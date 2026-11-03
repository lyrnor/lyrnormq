//! Fixed-cardinality timing only; no command bodies, peer addresses or client identities.
use crate::{model::Command, rpc::ReadRequest};
use std::{
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub(crate) enum Stage {
    Write,
    RaftResult,
    ReadIndex,
    Read,
    RpcMutate,
    RpcRead,
}
#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub(crate) enum Kind {
    Acquire,
    Heartbeat,
    Register,
    Mutation,
    Metadata,
    Owner,
    Node,
    Applied,
    AppliedAck,
    Status,
    Authority,
}
impl Kind {
    pub(crate) fn command(command: &Command) -> Self {
        match command {
            Command::Acquire { .. } => Self::Acquire,
            Command::Heartbeat { .. } => Self::Heartbeat,
            Command::Register { .. } => Self::Register,
            _ => Self::Mutation,
        }
    }
    pub(crate) fn read(request: &ReadRequest) -> Self {
        match request {
            ReadRequest::Metadata => Self::Metadata,
            ReadRequest::Owner { .. } => Self::Owner,
            ReadRequest::Node { .. } => Self::Node,
            ReadRequest::Applied { .. } => Self::Applied,
            ReadRequest::AppliedAck { .. } => Self::AppliedAck,
            ReadRequest::Status => Self::Status,
        }
    }
}
#[derive(Clone, Copy, Debug)]
#[repr(usize)]
enum Outcome {
    Success,
    Failure,
    Cancelled,
}
const KINDS: usize = 11;
const OUTCOMES: usize = 3;
const COUNT: usize = 6 * KINDS * OUTCOMES;
static COUNTERS: [AtomicU64; COUNT] = [const { AtomicU64::new(0) }; COUNT];
fn sampled(count: u64) -> bool {
    count.is_power_of_two()
}
fn index(stage: Stage, kind: Kind, outcome: Outcome) -> usize {
    (stage as usize * KINDS + kind as usize) * OUTCOMES + outcome as usize
}
struct Timer {
    stage: Stage,
    kind: Kind,
    started: Instant,
    finished: bool,
}
impl Timer {
    fn emit(&self, outcome: Outcome) {
        let count = COUNTERS[index(self.stage, self.kind, outcome)]
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if sampled(count) {
            let elapsed_us = self.started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
            tracing::warn!(event="cluster_control_timing", stage=?self.stage, kind=?self.kind, outcome=?outcome, count, elapsed_us, "cluster control stage timing");
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
pub(crate) async fn measure<T, E, F>(stage: Stage, kind: Kind, work: F) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    let mut timer = Timer {
        stage,
        kind,
        started: Instant::now(),
        finished: false,
    };
    let result = work.await;
    timer.emit(if result.is_ok() {
        Outcome::Success
    } else {
        Outcome::Failure
    });
    timer.finished = true;
    result
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn instrumentation_returns_identical_success_and_error_and_allows_cancellation() {
        assert_eq!(
            measure(Stage::RpcRead, Kind::Status, async {
                Ok::<_, &'static str>(7)
            })
            .await,
            Ok(7)
        );
        assert_eq!(
            measure(Stage::RpcRead, Kind::Status, async {
                Err::<u8, _>("sentinel")
            })
            .await,
            Err("sentinel")
        );
        let mut pending = Box::pin(measure(
            Stage::RpcRead,
            Kind::Status,
            std::future::pending::<Result<(), ()>>(),
        ));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(pending);
    }
    #[test]
    fn labels_and_sampling_are_fixed_and_contain_no_command_identity() {
        let kind = Kind::command(&Command::Acquire {
            key: "secret-client".into(),
            node: 1,
            incarnation: "secret".into(),
            generation: 1,
            expected_epoch: 0,
        });
        assert_eq!(format!("{kind:?}"), "Acquire");
        assert_eq!((1..=1024).filter(|count| sampled(*count)).count(), 11);
        assert!(!sampled(0));
        assert_eq!(
            index(Stage::RpcRead, Kind::Authority, Outcome::Cancelled),
            COUNT - 1
        );
    }
}
