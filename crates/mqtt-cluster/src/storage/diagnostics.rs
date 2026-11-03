//! Sampled WAL/state-machine boundaries; log indices are numeric consensus metadata only.
use super::{Entry, Types};
use std::{
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};
#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub(super) enum Stage {
    Append,
    WalAppend,
    SaveCommitted,
    WalCommit,
    ApplyQueued,
    ApplyState,
}
#[derive(Clone, Copy)]
pub(super) struct Context {
    pub node: u64,
    pub entries: usize,
    pub first: Option<u64>,
    pub last: Option<u64>,
}
impl Context {
    pub(super) fn entries(node: u64, entries: &[Entry<Types>]) -> Self {
        Self {
            node,
            entries: entries.len(),
            first: entries.first().map(|e| e.log_id.index),
            last: entries.last().map(|e| e.log_id.index),
        }
    }
}
const STAGES: usize = 6;
static STARTS: [AtomicU64; STAGES] = [const { AtomicU64::new(0) }; STAGES];
static OUTCOMES: [AtomicU64; STAGES * 3] = [const { AtomicU64::new(0) }; STAGES * 3];
#[derive(Clone, Copy, Debug)]
#[repr(usize)]
enum Outcome {
    Success,
    Failure,
    Cancelled,
}
struct Timer {
    stage: Stage,
    context: Context,
    started: Instant,
    sequence: u64,
    finished: bool,
    trace_start_us: Option<u64>,
}
impl Timer {
    fn emit(&self, outcome: Outcome) {
        if let Some(start_us) = self.trace_start_us {
            let end_us = crate::profile::stamp_us();
            tracing::warn!(event="raft_storage_trace", stage=?self.stage, outcome=?outcome, node=self.context.node, entries=self.context.entries, first=?self.context.first, last=?self.context.last, sequence=self.sequence, start_us, end_us, "Raft storage correlated stage");
        }
        let count = OUTCOMES[self.stage as usize * 3 + outcome as usize]
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if count.is_power_of_two() {
            let elapsed_us = self.started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
            tracing::warn!(event="cluster_storage_timing", stage=?self.stage, outcome=?outcome, node=self.context.node, entries=self.context.entries, first=?self.context.first, last=?self.context.last, sequence=self.sequence, count, elapsed_us, "cluster storage stage timing");
        }
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        if !self.finished {
            self.emit(Outcome::Cancelled);
        }
    }
}
pub(super) async fn measure<T, E, F>(stage: Stage, context: Context, work: F) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    let sequence = STARTS[stage as usize]
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1);
    let mut timer = Timer {
        stage,
        context,
        started: Instant::now(),
        sequence,
        finished: false,
        trace_start_us: trace_allowed(crate::profile::enabled(), sequence)
            .then(crate::profile::stamp_us),
    };
    if sequence.is_power_of_two() {
        tracing::warn!(event="cluster_storage_start",stage=?stage,node=context.node,entries=context.entries,first=?context.first,last=?context.last,sequence,"cluster storage stage entry");
    }
    let result = work.await;
    timer.emit(if result.is_ok() {
        Outcome::Success
    } else {
        Outcome::Failure
    });
    timer.finished = true;
    result
}
/// Blocking flush work uses the same counters and monotonic clock as its async queue.
pub(super) fn measure_sync<T, E>(
    stage: Stage,
    context: Context,
    work: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let sequence = STARTS[stage as usize]
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1);
    let mut timer = Timer {
        stage,
        context,
        started: Instant::now(),
        sequence,
        finished: false,
        trace_start_us: trace_allowed(crate::profile::enabled(), sequence)
            .then(crate::profile::stamp_us),
    };
    if sequence.is_power_of_two() {
        tracing::warn!(event="cluster_storage_start",stage=?stage,node=context.node,entries=context.entries,first=?context.first,last=?context.last,sequence,"cluster storage stage entry");
    }
    let result = work();
    timer.emit(if result.is_ok() {
        Outcome::Success
    } else {
        Outcome::Failure
    });
    timer.finished = true;
    result
}
// First 4096 per-stage traces include numeric consensus context only. Existing
// sparse diagnostics retain their old sampling independently of this opt-in cap.
fn trace_allowed(enabled: bool, sequence: u64) -> bool {
    enabled && sequence != 0 && sequence <= 4096
}
static CALLBACKS: AtomicU64 = AtomicU64::new(0);
pub(super) fn flushed(context: Context) {
    if !crate::profile::enabled() {
        return;
    }
    let sequence = CALLBACKS.fetch_add(1, Ordering::Relaxed).saturating_add(1);
    if trace_allowed(true, sequence) {
        tracing::warn!(event="raft_storage_callback", node=context.node, entries=context.entries, first=?context.first, last=?context.last, sequence, stamp_us=crate::profile::stamp_us(), "Raft append callback issued");
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn opt_in_trace_budget_has_exact_boundaries() {
        assert!(!trace_allowed(false, 1));
        assert!(!trace_allowed(true, 0));
        assert!(trace_allowed(true, 1));
        assert!(trace_allowed(true, 4096));
        assert!(!trace_allowed(true, 4097));
        assert!(!trace_allowed(true, u64::MAX));
    }
    #[tokio::test]
    async fn timing_preserves_errors_success_and_cancellation() {
        let context = Context {
            node: 1,
            entries: 0,
            first: None,
            last: None,
        };
        assert_eq!(
            measure_sync(Stage::WalCommit, context, || Err::<u8, _>("sentinel")),
            Err("sentinel")
        );
        assert_eq!(
            measure(Stage::SaveCommitted, context, async { Ok::<_, ()>(7) }).await,
            Ok(7)
        );
        let mut pending = Box::pin(measure(
            Stage::ApplyQueued,
            context,
            std::future::pending::<Result<(), ()>>(),
        ));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(pending);
    }
}
