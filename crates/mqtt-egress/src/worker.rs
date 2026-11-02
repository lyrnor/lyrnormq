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
    pub client_id: Arc<str>,
    resident: Option<tokio::sync::OwnedSemaphorePermit>,
    pub _budget: Budget,
    pub stats: Arc<ActionCounters>,
    pub counters: Arc<Counters>,
    done: bool,
}
impl Delivery {
    pub fn new(
        client_id: &str,
        encoded: Vec<u8>,
        budget: Budget,
        stats: Arc<ActionCounters>,
        counters: Arc<Counters>,
    ) -> Self {
        Self {
            encoded,
            client_id: Arc::from(client_id),
            resident: None,
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
    sender: mpsc::Sender<Delivery>,
    resident: Arc<Semaphore>,
    pub stats: Arc<ActionCounters>,
}
impl Target {
    pub fn try_send(&self, mut delivery: Delivery) {
        let Ok(slot) = Arc::clone(&self.resident).try_acquire_owned() else {
            return;
        };
        delivery.resident = Some(slot);
        let _ = self.sender.try_send(delivery);
    }
    #[cfg(test)]
    async fn send(&self, mut delivery: Delivery) -> Result<(), mpsc::error::SendError<Delivery>> {
        let Ok(slot) = Arc::clone(&self.resident).acquire_owned().await else {
            return Err(mpsc::error::SendError(delivery));
        };
        delivery.resident = Some(slot);
        self.sender.send(delivery).await
    }
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
fn resident_limit(action: &Outbound, limits: &mqtt_egress_core::BatchLimits) -> usize {
    action
        .queue_capacity
        .saturating_add(
            action
                .concurrency
                .clamp(1, crate::MAX_CONCURRENCY)
                .saturating_mul(action.batch_size.min(limits.max_events)),
        )
        .saturating_add(1)
}

pub(crate) fn spawn(
    action: Outbound,
    sink: Arc<dyn Sink>,
    global: Arc<Semaphore>,
    stats: Arc<ActionCounters>,
    cancel: watch::Receiver<bool>,
) -> (Target, JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel(action.queue_capacity);
    let resident_limit = resident_limit(&action, &sink.limits());
    let target = Target {
        resident: Arc::new(Semaphore::new(resident_limit)),
        sender,
        stats: Arc::clone(&stats),
    };
    let task = tokio::spawn(scheduler::run(
        action, sink, global, stats, cancel, receiver,
    ));
    (target, task)
}
#[path = "worker_scheduler.rs"]
mod scheduler;

fn record_join(result: Option<Result<(), tokio::task::JoinError>>, stats: &ActionCounters) {
    if result.is_some_and(|result| result.is_err()) {
        *stats
            .last_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some("worker_failed".into());
    }
}

#[derive(Clone)]
struct AttemptLimits {
    global: Arc<Semaphore>,
    local: Arc<Semaphore>,
}

async fn send_batch(
    action: Arc<Outbound>,
    sink: Arc<dyn Sink>,
    attempts: AttemptLimits,
    stats: Arc<ActionCounters>,
    mut cancel: watch::Receiver<bool>,
    batch: &mut [Delivery],
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
        let local_permit = tokio::select! {biased;
            _ = cancel.changed() => None,
            permit = Arc::clone(&attempts.local).acquire_owned() => permit.ok(),
        };
        let Some(local_permit) = local_permit else {
            return;
        };
        let permit = tokio::select! {biased;_ = cancel.changed()=>None,p=Arc::clone(&attempts.global).acquire_owned()=>p.ok()};
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
        drop(local_permit);
        if !retry {
            break;
        }
    }
    if *cancel.borrow() {
        return;
    }
    for item in batch.iter_mut() {
        match &result {
            Ok(()) => item.complete(true, None),
            Err(error) => item.complete(false, Some(error)),
        }
    }
}

#[cfg(test)]
#[path = "worker_tests.rs"]
mod tests;
