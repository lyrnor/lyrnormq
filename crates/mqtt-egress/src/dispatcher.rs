use crate::{
    counters::Budget,
    runtime::{Inner, Snapshot},
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
struct RuleResult<'a> {
    event_id: &'a str,
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
                    let targets: Vec<_> = rule
                        .outbound_ids
                        .iter()
                        .filter_map(|id| {
                            work.snapshot
                                .generation
                                .targets
                                .get(id)
                                .filter(|target| target.enabled)
                                .map(|target| (id, target))
                        })
                        .collect();
                    if targets.is_empty() {
                        continue;
                    }
                    let wire = RuleResult {
                        event_id: &work.id,
                        rule_id: &rule.id,
                        topic: &work.event.topic,
                        client_id: &work.event.client_id,
                        qos: work.event.qos,
                        retain: work.event.retain,
                        timestamp_ms: work.event.timestamp_ms,
                        data: &output,
                    };
                    let encoded = match serde_json::to_vec(&wire) {
                        Ok(bytes) if bytes.len() <= crate::durable_worker::MAX_REQUEST => bytes,
                        _ => {
                            inner.counters.errors.fetch_add(1, Ordering::Relaxed);
                            for (_, target) in targets {
                                target.stats.failed.fetch_add(1, Ordering::Relaxed);
                                *target
                                    .stats
                                    .last_error
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                    Some("request_too_large".into());
                            }
                            continue;
                        }
                    };
                    let mut consumers = Vec::with_capacity(targets.len());
                    let mut stats = Vec::with_capacity(targets.len());
                    let mut metadata_bytes =
                        work.event.client_id.len().saturating_add(rule.id.len());
                    for (action_id, target) in targets {
                        let delivery_id = format!("{}:{}:{}", work.id, rule.id, action_id);
                        let extra =
                            crate::result_wire::delivery_extra(&delivery_id).unwrap_or(usize::MAX);
                        if encoded
                            .len()
                            .checked_add(extra)
                            .is_none_or(|n| n > crate::durable_worker::MAX_REQUEST)
                        {
                            target.stats.failed.fetch_add(1, Ordering::Relaxed);
                            inner.counters.errors.fetch_add(1, Ordering::Relaxed);
                            *target
                                .stats
                                .last_error
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                Some("request_too_large".into());
                            continue;
                        }
                        metadata_bytes = metadata_bytes
                            .saturating_add(action_id.len())
                            .saturating_add(delivery_id.len())
                            .saturating_add(256);
                        consumers.push(mqtt_egress_log::NewConsumer {
                            outbound_id: action_id.clone(),
                            revision: target.revision,
                            delivery_id,
                        });
                        stats.push(Arc::clone(&target.stats));
                    }
                    if consumers.is_empty() {
                        continue;
                    }
                    let Some(mut budget) = Budget::reserve(
                        &inner.counters,
                        encoded.len().saturating_add(metadata_bytes),
                        None,
                    ) else {
                        for target in &stats {
                            target.dropped.fetch_add(1, Ordering::Relaxed);
                        }
                        inner
                            .counters
                            .dropped
                            .fetch_add(stats.len() as u64, Ordering::Relaxed);
                        continue;
                    };
                    budget.set_events(consumers.len());
                    let record = mqtt_egress_log::NewResult {
                        stream_id: rule.id.clone(),
                        client_id: work.event.client_id.clone(),
                        payload: encoded,
                        consumers,
                    };
                    inner
                        .queue
                        .enqueue(record, budget, stats, Arc::clone(&inner.counters));
                }
                work.complete = !cancelled;
                drop(work);
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
}
