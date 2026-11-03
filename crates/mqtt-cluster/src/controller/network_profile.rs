//! Limited numeric AppendEntries traces, including futures canceled by Raft deadlines.
use crate::{model::RaftRequest, profile};
use std::sync::atomic::{AtomicU64, Ordering};
static SEQUENCE: AtomicU64 = AtomicU64::new(0);
const LIMIT: u64 = 4096;
pub(super) struct Trace {
    node: u64,
    target: u64,
    sequence: u64,
    entries: usize,
    first: Option<u64>,
    last: Option<u64>,
    started_us: u64,
    outcome: &'static str,
}
impl Trace {
    pub(super) fn new(node: u64, target: u64, request: &RaftRequest) -> Option<Self> {
        if !profile::enabled() {
            return None;
        }
        let RaftRequest::Append(request) = request else {
            return None;
        };
        let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        if sequence > LIMIT {
            return None;
        }
        Some(Self {
            node,
            target,
            sequence,
            entries: request.entries.len(),
            first: request.entries.first().map(|e| e.log_id.index),
            last: request.entries.last().map(|e| e.log_id.index),
            started_us: profile::stamp_us(),
            outcome: "cancelled",
        })
    }
    pub(super) fn finish(&mut self, success: bool) {
        self.outcome = if success { "success" } else { "error" };
    }
}
impl Drop for Trace {
    fn drop(&mut self) {
        tracing::warn!(event="raft_append_rpc_profile", node=self.node, target=self.target,
            sequence=self.sequence, entries=self.entries, first=?self.first, last=?self.last,
            started_us=self.started_us, finished_us=profile::stamp_us(), outcome=self.outcome,
            "AppendEntries RPC timing");
    }
}
