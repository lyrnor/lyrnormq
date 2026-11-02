use crate::{
    Outbound,
    counters::{ActionCounters, AttemptGuard, Budget, Counters},
};
use mqtt_egress_core::Sink;
use std::{
    sync::{Arc, Mutex, PoisonError, atomic::Ordering},
    time::Duration,
};
use tokio::{
    sync::{Semaphore, mpsc, watch},
    task::{JoinHandle, JoinSet},
};

pub(crate) const MAX_REQUEST: usize = 1024 * 1024;

pub(crate) struct Delivery {
    pub encoded: Vec<u8>,
    pub _budget: Budget,
    pub stats: Arc<ActionCounters>,
    pub counters: Arc<Counters>,
    done: bool,
}
impl Delivery {
    pub fn new(
        encoded: Vec<u8>,
        budget: Budget,
        stats: Arc<ActionCounters>,
        counters: Arc<Counters>,
    ) -> Self {
        Self {
            encoded,
            _budget: budget,
            stats,
            counters,
            done: false,
        }
    }
    fn complete(&mut self, success: bool, error: Option<&str>) {
        self.done = true;
        if success {
            self.stats.delivered.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.failed.fetch_add(1, Ordering::Relaxed);
            self.counters.errors.fetch_add(1, Ordering::Relaxed);
        }
        *self
            .stats
            .last_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = error.map(str::to_owned);
    }
}
impl Drop for Delivery {
    fn drop(&mut self) {
        if !self.done {
            self.stats.dropped.fetch_add(1, Ordering::Relaxed);
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}
#[derive(Clone)]
pub(crate) struct Target {
    pub sender: mpsc::Sender<Delivery>,
    pub stats: Arc<ActionCounters>,
}
pub(crate) struct Generation {
    pub cancel: watch::Sender<bool>,
    pub targets: std::collections::BTreeMap<String, Target>,
    pub tasks: Mutex<Vec<JoinHandle<()>>>,
}
impl Generation {
    pub fn cancel(&self) {
        self.cancel.send_replace(true);
    }
    pub async fn finish(&self) {
        self.cancel();
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap_or_else(PoisonError::into_inner));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        for mut task in tasks {
            if tokio::time::timeout_at(deadline, &mut task).await.is_err() {
                task.abort();
                let _ = task.await;
            }
        }
    }
}
pub(crate) fn spawn(
    action: Outbound,
    sink: Arc<dyn Sink>,
    global: Arc<Semaphore>,
    stats: Arc<ActionCounters>,
    cancel: watch::Receiver<bool>,
) -> (Target, JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel(action.queue_capacity);
    let target = Target {
        sender,
        stats: Arc::clone(&stats),
    };
    let task = tokio::spawn(run(action, sink, global, stats, cancel, receiver));
    (target, task)
}
async fn run(
    action: Outbound,
    sink: Arc<dyn Sink>,
    global: Arc<Semaphore>,
    stats: Arc<ActionCounters>,
    mut cancel: watch::Receiver<bool>,
    mut receiver: mpsc::Receiver<Delivery>,
) {
    let action = Arc::new(action);
    // The public configuration validator enforces these bounds. Keep this private
    // entry point bounded even if an internal caller bypasses validation.
    let concurrency = action.concurrency.clamp(1, crate::MAX_CONCURRENCY);
    let mut jobs = JoinSet::new();
    let mut pending = None;
    let mut cancelled = false;
    'collect: loop {
        if *cancel.borrow() || cancel.has_changed().is_err() {
            cancelled = true;
            break;
        }
        // Reserve a job slot before removing anything else from the input queue.
        // Jobs include permit wait and retry backoff, not only active sends.
        while jobs.len() >= concurrency {
            tokio::select! {biased;
                _ = cancel.changed() => { cancelled = true; break 'collect; }
                result = jobs.join_next() => record_join(result, &stats),
            }
        }
        while let Some(result) = jobs.try_join_next() {
            record_join(Some(result), &stats);
        }
        let first = if let Some(item) = pending.take() {
            Some(item)
        } else {
            tokio::select! {biased;
                _ = cancel.changed() => {cancelled = true; None},
                item = receiver.recv() => item,
            }
        };
        let Some(first) = first else {
            break;
        };
        let limits = sink.limits();
        let size = limits.overhead.checked_add(first.encoded.len());
        if limits.max_events == 0 || size.is_none() {
            let mut first = first;
            first.complete(false, Some("invalid_batch_limits"));
            continue;
        }
        let mut size = size.unwrap_or(usize::MAX);
        if size > limits.max_bytes {
            let mut first = first;
            first.complete(false, Some("request_too_large"));
            continue;
        }
        let mut batch = vec![first];
        let deadline = tokio::time::Instant::now() + Duration::from_millis(action.batch_wait_ms);
        while batch.len() < action.batch_size.min(limits.max_events) {
            let next = match receiver.try_recv() {
                Ok(item) => Some(item),
                Err(mpsc::error::TryRecvError::Disconnected) => None,
                Err(mpsc::error::TryRecvError::Empty) if action.batch_wait_ms > 0 => {
                    tokio::select! {biased;_ = cancel.changed()=>None,item=tokio::time::timeout_at(deadline,receiver.recv())=>item.ok().flatten()}
                }
                Err(mpsc::error::TryRecvError::Empty) => None,
            };
            let Some(item) = next else {
                break;
            };
            if size
                .checked_add(limits.separator_bytes)
                .and_then(|size| size.checked_add(item.encoded.len()))
                .is_none_or(|size| size > limits.max_bytes)
            {
                pending = Some(item);
                break;
            }
            size += limits.separator_bytes + item.encoded.len();
            batch.push(item);
        }
        if *cancel.borrow() || cancel.has_changed().is_err() {
            cancelled = true;
            break;
        }
        jobs.spawn(send_batch(
            Arc::clone(&action),
            Arc::clone(&sink),
            Arc::clone(&global),
            Arc::clone(&stats),
            cancel.clone(),
            batch,
            size,
        ));
    }
    // Drop the carry-over and queued records before returning. No detached child
    // outlives its generation, including during replacement or storage failure.
    drop(pending);
    drop(receiver);
    if cancelled {
        jobs.shutdown().await;
    } else {
        while !jobs.is_empty() {
            tokio::select! {biased;
                _ = cancel.changed() => { jobs.shutdown().await; break; }
                result = jobs.join_next() => record_join(result, &stats),
            }
        }
    }
}

fn record_join(result: Option<Result<(), tokio::task::JoinError>>, stats: &ActionCounters) {
    if result.is_some_and(|result| result.is_err()) {
        *stats
            .last_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some("worker_failed".into());
    }
}

async fn send_batch(
    action: Arc<Outbound>,
    sink: Arc<dyn Sink>,
    global: Arc<Semaphore>,
    stats: Arc<ActionCounters>,
    mut cancel: watch::Receiver<bool>,
    mut batch: Vec<Delivery>,
    size: usize,
) {
    let Some(first) = batch.first() else {
        return;
    };
    let counters = Arc::clone(&first.counters);
    let events: Vec<&[u8]> = batch.iter().map(|item| item.encoded.as_slice()).collect();
    let mut result = Err("transport".to_owned());
    for attempt in 0..action.max_attempts {
        if *cancel.borrow() {
            return;
        }
        if attempt > 0 {
            let scale = 1u64 << attempt.min(5);
            let jitter = (size as u64 + attempt as u64 * 37) % action.retry_base_ms.max(1);
            let delay = action
                .retry_base_ms
                .saturating_mul(scale)
                .saturating_add(jitter)
                .min(30000);
            tokio::select! {biased;_ = cancel.changed()=>return,_=tokio::time::sleep(Duration::from_millis(delay))=>{}}
        }
        // Every retry reacquires its own permit; backoff never holds the global
        // request budget. Scope both guards around this one adapter future.
        let permit = tokio::select! {biased;_ = cancel.changed()=>None,p=Arc::clone(&global).acquire_owned()=>p.ok()};
        let Some(global_permit) = permit else {
            return;
        };
        if *cancel.borrow() {
            return;
        }
        let retry = {
            let _active = AttemptGuard::new(Arc::clone(&counters), Arc::clone(&stats));
            if attempt > 0 {
                stats
                    .retried
                    .fetch_add(batch.len() as u64, Ordering::Relaxed);
            }
            let response = tokio::select! {biased;
                _ = cancel.changed() => return,
                result = tokio::time::timeout(Duration::from_millis(action.timeout_ms), sink.send(&events)) =>
                    match result { Ok(result) => result, Err(_) => Err(mqtt_egress_core::SendError {kind:mqtt_egress_core::FailureKind::Retryable,code:"timeout".into()}) },
            };
            match response {
                Ok(()) => {
                    result = Ok(());
                    false
                }
                Err(error) => {
                    result = Err(error.code);
                    error.kind == mqtt_egress_core::FailureKind::Retryable
                }
            }
        };
        drop(global_permit);
        if !retry {
            break;
        }
    }
    if *cancel.borrow() {
        return;
    }
    for item in &mut batch {
        match &result {
            Ok(()) => item.complete(true, None),
            Err(error) => item.complete(false, Some(error)),
        }
    }
}

#[cfg(test)]
#[path = "worker_tests.rs"]
mod tests;
