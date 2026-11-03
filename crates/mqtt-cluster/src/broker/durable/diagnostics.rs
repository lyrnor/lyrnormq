//! Fixed-cardinality, logarithmically sampled acquisition diagnostics.
use std::{
    future::Future,
    io,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use crate::{diagnosis::Failure, rpc};

#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub(super) enum Stage {
    TakeMutate,
    TakeBarrier,
    TakeFence,
    TakeWrite,
    TakeFilters,
    TakeAuthority,
    TakeFinish,
    RecoveryMissing,
    RecoveryPoisoned,
    RecoveryStaleEpoch,
}
const STAGES: usize = Stage::RecoveryStaleEpoch as usize + 1;
struct Counters([AtomicU64; STAGES * 2]);
impl Counters {
    const fn new() -> Self {
        Self([const { AtomicU64::new(0) }; STAGES * 2])
    }
    fn next(&self, stage: Stage, failed: bool) -> (u64, bool) {
        let index = stage as usize * 2 + usize::from(failed);
        let count = self.0[index]
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        (count, count.is_power_of_two())
    }
}
static COUNTERS: Counters = Counters::new();

/// Only finite enums and booleans may cross the logging boundary, never Display
/// strings from errors (which can contain arbitrary peer or application data).
#[derive(Debug)]
struct ErrorSummary {
    kind: io::ErrorKind,
    rpc_code: Option<tonic::Code>,
    outcome_unknown: Option<bool>,
    not_submitted: bool,
    acquisition_conflict: bool,
    local_failure: Option<Failure>,
}
impl ErrorSummary {
    fn new(error: &io::Error) -> Self {
        let cause = error.get_ref();
        let rpc = cause.and_then(|cause| cause.downcast_ref::<rpc::RpcFailure>());
        Self {
            kind: error.kind(),
            rpc_code: rpc.map(|failure| failure.code),
            outcome_unknown: rpc.map(|failure| failure.outcome_unknown),
            not_submitted: rpc::not_submitted(error),
            acquisition_conflict: cause
                .is_some_and(|cause| cause.is::<mqtt_cluster_core::AcquisitionConflict>()),
            local_failure: cause
                .and_then(|cause| cause.downcast_ref::<Failure>())
                .copied(),
        }
    }
}

fn observe(stage: Stage, start: Instant, error: Option<&io::Error>) {
    let (count, sampled) = COUNTERS.next(stage, error.is_some());
    if sampled {
        let elapsed_us = start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        if let Some(error) = error {
            let summary = ErrorSummary::new(error);
            tracing::warn!(
                event = "cluster_acquire_stage", ?stage, outcome = "error", count, elapsed_us,
                kind = ?summary.kind, rpc_code = ?summary.rpc_code,
                outcome_unknown = ?summary.outcome_unknown,
                not_submitted = summary.not_submitted,
                acquisition_conflict = summary.acquisition_conflict,
                local_failure = ?summary.local_failure,
            );
        } else {
            tracing::warn!(
                event = "cluster_acquire_stage",
                ?stage,
                outcome = "success",
                count,
                elapsed_us
            );
        }
    }
}

pub(super) fn finish<T>(stage: Stage, start: Instant, result: io::Result<T>) -> io::Result<T> {
    observe(stage, start, result.as_ref().err());
    result
}

pub(super) fn recovery(stage: Stage, error: io::Error) -> io::Error {
    observe(stage, Instant::now(), Some(&error));
    error
}

pub(super) async fn measure<T>(
    stage: Stage,
    operation: impl Future<Output = io::Result<T>>,
) -> io::Result<T> {
    let start = Instant::now();
    finish(stage, start, operation.await)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Debug)]
    struct PrivateError;
    impl std::fmt::Display for PrivateError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("private-client-identity-and-payload")
        }
    }
    impl std::error::Error for PrivateError {}

    #[test]
    fn outcome_and_stage_counters_are_independent_and_logarithmic() {
        let counters = Counters::new();
        let mut sampled = Vec::new();
        for count in 1..=1024 {
            let observed = counters.next(Stage::TakeMutate, false);
            assert_eq!(observed.0, count);
            if observed.1 {
                sampled.push(count);
            }
        }
        assert_eq!(sampled, vec![1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]);
        assert_eq!(counters.next(Stage::TakeMutate, true), (1, true));
        assert_eq!(counters.next(Stage::TakeFinish, false), (1, true));
        assert_eq!(counters.next(Stage::RecoveryMissing, true), (1, true));
        assert_eq!(counters.next(Stage::RecoveryPoisoned, true), (1, true));
        assert_eq!(counters.next(Stage::RecoveryStaleEpoch, true), (1, true));
        assert_eq!(
            counters.0.len(),
            20,
            "fixed cardinality regardless of clients"
        );
    }

    #[test]
    fn diagnostic_completion_preserves_custom_and_os_error_semantics() {
        let error = io::Error::new(io::ErrorKind::Interrupted, PrivateError);
        let summary = ErrorSummary::new(&error);
        assert!(!format!("{summary:?}").contains("private-client"));
        let error = finish::<()>(Stage::TakeWrite, Instant::now(), Err(error)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert!(error.get_ref().unwrap().is::<PrivateError>());
        let error = finish::<()>(
            Stage::TakeFence,
            Instant::now(),
            Err(io::Error::from_raw_os_error(13)),
        )
        .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(13));
    }

    #[tokio::test]
    async fn measured_results_keep_values_and_cross_layer_error_markers() {
        let value = measure(Stage::TakeFinish, async {
            Ok::<_, io::Error>(vec![1, 2, 3])
        })
        .await
        .unwrap();
        assert_eq!(value, vec![1, 2, 3]);
        let error = measure::<()>(Stage::TakeMutate, async {
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                mqtt_cluster_core::AcquisitionConflict,
            ))
        })
        .await
        .unwrap_err();
        assert!(rpc::not_submitted(&error));
        assert!(
            error
                .get_ref()
                .unwrap()
                .is::<mqtt_cluster_core::AcquisitionConflict>()
        );
    }

    #[tokio::test]
    async fn measurement_does_not_spawn_or_detach_a_canceled_operation() {
        struct Dropped(std::sync::Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let guard = Dropped(dropped.clone());
        let operation = async move {
            let _guard = guard;
            std::future::pending::<io::Result<()>>().await
        };
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(5),
                measure(Stage::TakeBarrier, operation)
            )
            .await
            .is_err()
        );
        assert!(dropped.load(Ordering::SeqCst));
    }
}
