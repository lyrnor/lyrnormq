use crate::{
    counters::Budget,
    runtime::{Inner, Snapshot},
    worker::Delivery,
};
use mqtt_rules::Event;
use serde::Serialize;
use std::{
    sync::{
        Arc,
        atomic::Ordering,
        mpsc::{Receiver, RecvTimeoutError},
    },
    time::Duration,
};

pub(crate) struct Work {
    pub event: Event,
    pub snapshot: Arc<Snapshot>,
    pub _budget: Budget,
    pub id: String,
    pub complete: bool,
    pub inner: Arc<Inner>,
}
impl Drop for Work {
    fn drop(&mut self) {
        if !self.complete {
            self.inner.counters.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}
#[derive(Serialize)]
struct WireEvent<'a> {
    event_id: &'a str,
    delivery_id: &'a str,
    rule_id: &'a str,
    topic: &'a str,
    client_id: &'a str,
    qos: u8,
    retain: bool,
    timestamp_ms: u64,
    data: &'a serde_json::Value,
}
pub(crate) fn run(inner: Arc<Inner>, receiver: Receiver<Work>) {
    while !inner.stopped.load(Ordering::Acquire) {
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(mut work) => {
                if *work.snapshot.generation.cancel.borrow() {
                    continue;
                }
                let mut cancelled = false;
                for (rule, compiled) in work.snapshot.candidates(&work.event.topic) {
                    if !rule.enabled || !compiled.matches_topic(&work.event.topic) {
                        continue;
                    }
                    if inner.stopped.load(Ordering::Acquire)
                        || *work.snapshot.generation.cancel.borrow()
                    {
                        cancelled = true;
                        break;
                    }
                    let output = match compiled.evaluate(&work.event) {
                        Ok(Some(value)) => value,
                        Ok(None) => continue,
                        Err(_) => {
                            inner.counters.errors.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };
                    inner.counters.matched.fetch_add(1, Ordering::Relaxed);
                    for action_id in &rule.action_ids {
                        let Some(target) = work.snapshot.generation.targets.get(action_id) else {
                            continue;
                        };
                        let delivery_id = format!("{}:{}:{}", work.id, rule.id, action_id);
                        let wire = WireEvent {
                            event_id: &work.id,
                            delivery_id: &delivery_id,
                            rule_id: &rule.id,
                            topic: &work.event.topic,
                            client_id: &work.event.client_id,
                            qos: work.event.qos,
                            retain: work.event.retain,
                            timestamp_ms: work.event.timestamp_ms,
                            data: &output,
                        };
                        let encoded = match serde_json::to_vec(&wire) {
                            Ok(bytes) if bytes.len() <= crate::worker::MAX_REQUEST => bytes,
                            _ => {
                                inner.counters.errors.fetch_add(1, Ordering::Relaxed);
                                target.stats.failed.fetch_add(1, Ordering::Relaxed);
                                *target
                                    .stats
                                    .last_error
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                    Some("request_too_large".into());
                                continue;
                            }
                        };
                        let Some(budget) = Budget::reserve(
                            &inner.counters,
                            encoded.len(),
                            Some(Arc::clone(&target.stats)),
                        ) else {
                            target.stats.dropped.fetch_add(1, Ordering::Relaxed);
                            inner.counters.dropped.fetch_add(1, Ordering::Relaxed);
                            continue;
                        };
                        let delivery = Delivery::new(
                            encoded,
                            budget,
                            Arc::clone(&target.stats),
                            Arc::clone(&inner.counters),
                        );
                        let _ = target.sender.try_send(delivery);
                    }
                }
                work.complete = !cancelled;
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
}
