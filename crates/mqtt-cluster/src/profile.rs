//! Opt-in fixed-cardinality process metrics; no client identifiers or payloads.
use serde::Serialize;
use std::{
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::task::JoinHandle;

#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub(crate) enum Metric {
    ControlWrite,
    Validate,
    Encode,
    EnqueueWait,
    QueueResidence,
    BatchAssemble,
    BatchRaft,
    BatchSubmit,
    BatchWindow,
    ReplyWake,
    ReadIndex,
    ReadIndexRound,
    ReadIndexGroupSize,
    AppendRpcEncode,
    AppendRpcRoundtrip,
    AppendRpcDecode,
    WalSlotWait,
    WalSpawnWait,
    WalMutexWait,
    WalWork,
    WalEncode,
    WalMutation,
    WalFlushEnqueue,
    WalFlushWait,
    WalRead,
    ApplySerialWait,
    ApplyInputCheck,
    ApplySlotWait,
    ApplySpawnWait,
    ApplyMutexWait,
    ApplyMutate,
    ApplyHistory,
    CheckpointWait,
    CheckpointClone,
    CheckpointTotal,
    CheckpointEncode,
    CheckpointPersist,
    BatchSize,
    BatchBytes,
    QuorumDriver,
    QuorumRemoteEncode,
    QuorumRemoteBytes,
    QuorumTail,
    QuorumTailTimeout,
    ReplicaBatchSize,
    ReplicaRpc,
    DeliverBatchSize,
    DeliverRpc,
}
const METRICS: [Metric; 48] = [
    Metric::ControlWrite,
    Metric::Validate,
    Metric::Encode,
    Metric::EnqueueWait,
    Metric::QueueResidence,
    Metric::BatchAssemble,
    Metric::BatchRaft,
    Metric::BatchSubmit,
    Metric::BatchWindow,
    Metric::ReplyWake,
    Metric::ReadIndex,
    Metric::ReadIndexRound,
    Metric::ReadIndexGroupSize,
    Metric::AppendRpcEncode,
    Metric::AppendRpcRoundtrip,
    Metric::AppendRpcDecode,
    Metric::WalSlotWait,
    Metric::WalSpawnWait,
    Metric::WalMutexWait,
    Metric::WalWork,
    Metric::WalEncode,
    Metric::WalMutation,
    Metric::WalFlushEnqueue,
    Metric::WalFlushWait,
    Metric::WalRead,
    Metric::ApplySerialWait,
    Metric::ApplyInputCheck,
    Metric::ApplySlotWait,
    Metric::ApplySpawnWait,
    Metric::ApplyMutexWait,
    Metric::ApplyMutate,
    Metric::ApplyHistory,
    Metric::CheckpointWait,
    Metric::CheckpointClone,
    Metric::CheckpointTotal,
    Metric::CheckpointEncode,
    Metric::CheckpointPersist,
    Metric::BatchSize,
    Metric::BatchBytes,
    Metric::QuorumDriver,
    Metric::QuorumRemoteEncode,
    Metric::QuorumRemoteBytes,
    Metric::QuorumTail,
    Metric::QuorumTailTimeout,
    Metric::ReplicaBatchSize,
    Metric::ReplicaRpc,
    Metric::DeliverBatchSize,
    Metric::DeliverRpc,
];
const _: () = assert!(Metric::DeliverRpc as usize + 1 == METRICS.len());
struct Stats {
    count: AtomicU64,
    sum: AtomicU64,
    max: AtomicU64,
    buckets: [AtomicU64; 64],
}
impl Stats {
    const fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            max: AtomicU64::new(0),
            buckets: [const { AtomicU64::new(0) }; 64],
        }
    }
    fn observe(&self, value: u64) {
        let bucket = if value <= 1 {
            0
        } else {
            (64 - (value - 1).leading_zeros() as usize).min(63)
        };
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.max.fetch_max(value, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }
}
static STATS: [Stats; METRICS.len()] = [const { Stats::new() }; METRICS.len()];
pub(crate) fn enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var_os("RMQTT_RAFT_PROFILE").is_some_and(|v| v == "1"))
}
static ENABLED: OnceLock<bool> = OnceLock::new();
static ORIGIN: OnceLock<Instant> = OnceLock::new();
pub(crate) fn stamp_us() -> u64 {
    ORIGIN
        .get_or_init(Instant::now)
        .elapsed()
        .as_micros()
        .min(u128::from(u64::MAX)) as u64
}
pub(crate) fn observe(metric: Metric, value: u64) {
    if enabled() {
        STATS[metric as usize].observe(value);
    }
}
pub(crate) struct Timer {
    metric: Metric,
    start: Option<Instant>,
}
impl Timer {
    pub(crate) fn start(metric: Metric) -> Self {
        Self {
            metric,
            start: enabled().then(Instant::now),
        }
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        if let Some(start) = self.start {
            observe(
                self.metric,
                start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            );
        }
    }
}
#[derive(Serialize)]
struct Sample {
    metric: String,
    count: u64,
    sum: u64,
    max: u64,
    buckets: Vec<u64>,
}
fn emit(node: u64) {
    let samples: Vec<_> = METRICS
        .iter()
        .filter_map(|metric| {
            let s = &STATS[*metric as usize];
            let count = s.count.load(Ordering::Relaxed);
            (count != 0).then(|| Sample {
                metric: format!("{metric:?}"),
                count,
                sum: s.sum.load(Ordering::Relaxed),
                max: s.max.load(Ordering::Relaxed),
                buckets: s
                    .buckets
                    .iter()
                    .map(|v| v.load(Ordering::Relaxed))
                    .collect(),
            })
        })
        .collect();
    if let Ok(data) = serde_json::to_string(&samples) {
        tracing::warn!(event="raft_profile", node, stamp_us=stamp_us(), data=%data, "Raft processing cumulative metrics");
    }
}
pub(crate) struct Reporter {
    node: u64,
    task: Option<JoinHandle<()>>,
}
impl Reporter {
    pub(crate) fn start(node: u64) -> Self {
        let task = enabled().then(|| {
            tokio::spawn(async move {
                let mut ticks = tokio::time::interval(std::time::Duration::from_secs(1));
                ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    ticks.tick().await;
                    emit(node);
                }
            })
        });
        Self { node, task }
    }
}
impl Drop for Reporter {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            emit(self.node);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn histogram_boundaries_and_totals() {
        let stats = Stats::new();
        for n in [0, 1, 2, 3, 4, 5, 8, 9] {
            stats.observe(n);
        }
        assert_eq!(stats.count.load(Ordering::Relaxed), 8);
        assert_eq!(stats.sum.load(Ordering::Relaxed), 32);
        assert_eq!(stats.max.load(Ordering::Relaxed), 9);
        let counts: Vec<_> = stats.buckets[..5]
            .iter()
            .map(|v| v.load(Ordering::Relaxed))
            .collect();
        assert_eq!(counts, [2, 1, 2, 2, 1]);
    }
}
