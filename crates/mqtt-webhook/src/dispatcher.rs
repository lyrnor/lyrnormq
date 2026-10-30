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
    rule_id: &'a str,
    topic: &'a str,
    client_id: &'a str,
    qos: u8,
    retain: bool,
    timestamp_ms: u64,
    data: serde_json::Value,
}
pub(crate) fn run(inner: Arc<Inner>, receiver: Receiver<Work>) {
    while !inner.stopped.load(Ordering::Acquire) {
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(mut work) => {
                if *work.snapshot.generation.cancel.borrow() {
                    continue;
                }
                work.complete = true;
                for (rule, compiled) in &work.snapshot.rules {
                    if !rule.enabled || !compiled.matches_topic(&work.event.topic) {
                        continue;
                    }
                    if inner.stopped.load(Ordering::Acquire)
                        || *work.snapshot.generation.cancel.borrow()
                    {
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
                    let wire = WireEvent {
                        event_id: &work.id,
                        rule_id: &rule.id,
                        topic: &work.event.topic,
                        client_id: &work.event.client_id,
                        qos: work.event.qos,
                        retain: work.event.retain,
                        timestamp_ms: work.event.timestamp_ms,
                        data: output,
                    };
                    let encoded = match serde_json::to_vec(&wire) {
                        Ok(bytes) if bytes.len() <= crate::worker::MAX_REQUEST => bytes,
                        _ => {
                            inner.counters.errors.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };
                    for action_id in &rule.action_ids {
                        let Some(target) = work.snapshot.generation.targets.get(action_id) else {
                            continue;
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
                            encoded.clone(),
                            budget,
                            Arc::clone(&target.stats),
                            Arc::clone(&inner.counters),
                        );
                        let _ = target.sender.try_send(delivery);
                    }
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
}
