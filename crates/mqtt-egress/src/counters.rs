use serde::Serialize;
use std::sync::{
    Arc, Mutex, PoisonError,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

pub(crate) const MAX_BYTES: usize = 16 * 1024 * 1024;
pub(crate) struct Counters {
    pub memory_changed: tokio::sync::watch::Sender<u64>,
    pub inflight_requests: AtomicUsize,
    pub bytes: AtomicUsize,
    pub queued: AtomicUsize,
    pub received: AtomicU64,
    pub matched: AtomicU64,
    pub dropped: AtomicU64,
    pub errors: AtomicU64,
}
impl Default for Counters {
    fn default() -> Self {
        let (memory_changed, _) = tokio::sync::watch::channel(0u64);
        Self {
            memory_changed,
            inflight_requests: AtomicUsize::default(),
            bytes: AtomicUsize::default(),
            queued: AtomicUsize::default(),
            received: AtomicU64::default(),
            matched: AtomicU64::default(),
            dropped: AtomicU64::default(),
            errors: AtomicU64::default(),
        }
    }
}
#[derive(Default)]
pub(crate) struct ActionCounters {
    #[cfg(test)]
    pub peak_lanes: AtomicUsize,
    #[cfg(test)]
    pub peak_jobs: AtomicUsize,
    pub inflight_requests: AtomicUsize,
    pub queued: AtomicUsize,
    pub delivered: AtomicU64,
    pub failed: AtomicU64,
    pub retried: AtomicU64,
    pub dropped: AtomicU64,
    pub last_error: Mutex<Option<String>>,
}
#[derive(Debug, Serialize)]
pub struct Status {
    pub inflight_requests: usize,
    pub mode: &'static str,
    pub running: bool,
    pub queued_events: usize,
    pub queued_bytes: usize,
    pub received: u64,
    pub matched: u64,
    pub dropped: u64,
    pub errors: u64,
    pub outbounds: Vec<ActionStatus>,
    pub durable_queue: mqtt_egress_log::QueueStats,
}
#[derive(Debug, Serialize)]
pub struct ActionStatus {
    pub inflight_requests: usize,
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub id: String,
    pub queued: usize,
    pub durable_queued: usize,
    pub delivered: u64,
    pub failed: u64,
    pub retried: u64,
    pub dropped: u64,
    pub last_error: Option<String>,
}
impl ActionCounters {
    pub fn view(&self, id: String, kind: &'static str) -> ActionStatus {
        ActionStatus {
            inflight_requests: self.inflight_requests.load(Ordering::Relaxed),
            kind,
            id,
            queued: self.queued.load(Ordering::Relaxed),
            durable_queued: 0,
            delivered: self.delivered.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            retried: self.retried.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            last_error: self
                .last_error
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone(),
        }
    }
}
pub(crate) struct Budget {
    counters: Arc<Counters>,
    bytes: usize,
    action: Option<Arc<ActionCounters>>,
    events: usize,
}
impl Budget {
    pub fn reserve(
        counters: &Arc<Counters>,
        bytes: usize,
        action: Option<Arc<ActionCounters>>,
    ) -> Option<Self> {
        let mut current = counters.bytes.load(Ordering::Relaxed);
        loop {
            let next = current.checked_add(bytes)?;
            if next > MAX_BYTES {
                return None;
            }
            match counters.bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(value) => current = value,
            }
        }
        counters.queued.fetch_add(1, Ordering::Relaxed);
        if let Some(action) = &action {
            action.queued.fetch_add(1, Ordering::Relaxed);
        }
        Some(Self {
            counters: Arc::clone(counters),
            bytes,
            action,
            events: 1,
        })
    }
}
impl Budget {
    pub fn metadata(counters: &Arc<Counters>, bytes: usize) -> Option<Self> {
        let mut budget = Self::reserve(counters, bytes, None)?;
        counters.queued.fetch_sub(1, Ordering::Relaxed);
        budget.events = 0;
        Some(budget)
    }
    pub fn set_events(&mut self, events: usize) {
        let extra = events.saturating_sub(self.events);
        self.counters.queued.fetch_add(extra, Ordering::Relaxed);
        if let Some(action) = &self.action {
            action.queued.fetch_add(extra, Ordering::Relaxed);
        }
        self.events = events;
    }
}
impl Drop for Budget {
    fn drop(&mut self) {
        self.counters.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        self.counters
            .queued
            .fetch_sub(self.events, Ordering::Relaxed);
        if let Some(action) = &self.action {
            action.queued.fetch_sub(self.events, Ordering::Relaxed);
        }
        self.counters
            .memory_changed
            .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
    }
}

/// Counts only an attempt that already owns a global request permit.
pub(crate) struct AttemptGuard {
    counters: Arc<Counters>,
    action: Arc<ActionCounters>,
}
impl AttemptGuard {
    pub fn new(counters: Arc<Counters>, action: Arc<ActionCounters>) -> Self {
        counters.inflight_requests.fetch_add(1, Ordering::Relaxed);
        action.inflight_requests.fetch_add(1, Ordering::Relaxed);
        Self { counters, action }
    }
}
impl Drop for AttemptGuard {
    fn drop(&mut self) {
        self.counters
            .inflight_requests
            .fetch_sub(1, Ordering::Relaxed);
        self.action
            .inflight_requests
            .fetch_sub(1, Ordering::Relaxed);
    }
}
