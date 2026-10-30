use serde::Serialize;
use std::sync::{
    Arc, Mutex, PoisonError,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

pub(crate) const MAX_BYTES: usize = 16 * 1024 * 1024;
#[derive(Default)]
pub(crate) struct Counters {
    pub bytes: AtomicUsize,
    pub queued: AtomicUsize,
    pub received: AtomicU64,
    pub matched: AtomicU64,
    pub dropped: AtomicU64,
    pub errors: AtomicU64,
}
#[derive(Default)]
pub(crate) struct ActionCounters {
    pub queued: AtomicUsize,
    pub delivered: AtomicU64,
    pub failed: AtomicU64,
    pub retried: AtomicU64,
    pub dropped: AtomicU64,
    pub last_error: Mutex<Option<String>>,
}
#[derive(Debug, Serialize)]
pub struct Status {
    pub mode: &'static str,
    pub queued_events: usize,
    pub queued_bytes: usize,
    pub received: u64,
    pub matched: u64,
    pub dropped: u64,
    pub errors: u64,
    pub actions: Vec<ActionStatus>,
}
#[derive(Debug, Serialize)]
pub struct ActionStatus {
    pub id: String,
    pub queued: usize,
    pub delivered: u64,
    pub failed: u64,
    pub retried: u64,
    pub dropped: u64,
    pub last_error: Option<String>,
}
impl ActionCounters {
    pub fn view(&self, id: String) -> ActionStatus {
        ActionStatus {
            id,
            queued: self.queued.load(Ordering::Relaxed),
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
        })
    }
}
impl Drop for Budget {
    fn drop(&mut self) {
        self.counters.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        self.counters.queued.fetch_sub(1, Ordering::Relaxed);
        if let Some(action) = &self.action {
            action.queued.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
