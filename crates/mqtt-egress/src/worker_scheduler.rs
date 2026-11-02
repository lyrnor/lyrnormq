//! Exact-client FIFO lanes. Admission slots bound every resident event and lane.
use super::*;
use mqtt_egress_core::BatchLimits;
use std::collections::{HashMap, VecDeque};
use tokio::time::Instant;

struct Lane {
    pending: VecDeque<Delivery>,
    ready_at: Instant,
    busy: bool,
}

struct Lanes {
    lanes: HashMap<Arc<str>, Lane>,
    ready: VecDeque<Arc<str>>,
    wait: Duration,
}
impl Lanes {
    fn new(wait: Duration) -> Self {
        Self {
            lanes: HashMap::new(),
            ready: VecDeque::new(),
            wait,
        }
    }
    fn push(&mut self, item: Delivery) {
        let key = Arc::clone(&item.client_id);
        let lane = self.lanes.entry(Arc::clone(&key)).or_insert_with(|| Lane {
            pending: VecDeque::new(),
            ready_at: Instant::now(),
            busy: false,
        });
        if lane.pending.is_empty() {
            lane.ready_at = Instant::now() + self.wait;
            if !lane.busy {
                self.ready.push_back(key);
            }
        }
        lane.pending.push_back(item);
    }
    fn finished(&mut self, key: &Arc<str>) {
        if let Some(lane) = self.lanes.get_mut(key) {
            if lane.pending.is_empty() {
                self.lanes.remove(key);
            } else {
                lane.busy = false;
                self.ready.push_back(Arc::clone(key));
            }
        }
    }
    fn deadline(&self, count: usize) -> Option<Instant> {
        self.ready
            .iter()
            .filter_map(|key| self.lanes.get(key))
            .map(|lane| {
                if lane.pending.len() >= count {
                    Instant::now()
                } else {
                    lane.ready_at
                }
            })
            .min()
    }
    fn batch(
        &mut self,
        count: usize,
        limits: &BatchLimits,
    ) -> Option<(Arc<str>, Vec<Delivery>, usize)> {
        for _ in 0..self.ready.len() {
            let key = self.ready.pop_front()?;
            let Some(lane) = self.lanes.get_mut(&key) else {
                continue;
            };
            if lane.pending.len() < count && Instant::now() < lane.ready_at {
                self.ready.push_back(key);
                continue;
            }
            let mut batch = Vec::new();
            let mut size = limits.overhead;
            while batch.len() < count.max(1) {
                let Some(first) = lane.pending.front() else {
                    break;
                };
                let next_size = size
                    .checked_add(if batch.is_empty() {
                        0
                    } else {
                        limits.separator_bytes
                    })
                    .and_then(|n| n.checked_add(first.encoded.len()));
                if next_size.is_none_or(|n| n > limits.max_bytes) || limits.max_events == 0 {
                    if !batch.is_empty() {
                        break;
                    }
                    if let Some(mut invalid) = lane.pending.pop_front() {
                        let error = if next_size.is_none() || limits.max_events == 0 {
                            "invalid_batch_limits"
                        } else {
                            "request_too_large"
                        };
                        invalid.complete(false, Some(error));
                    }
                    continue;
                }
                size = next_size.unwrap_or(usize::MAX);
                if let Some(item) = lane.pending.pop_front() {
                    batch.push(item);
                }
            }
            if !batch.is_empty() {
                lane.busy = true;
                return Some((key, batch, size));
            }
            self.lanes.remove(&key);
        }
        None
    }
}

pub(super) async fn run(
    action: Outbound,
    sink: Arc<dyn Sink>,
    global: Arc<Semaphore>,
    stats: Arc<ActionCounters>,
    mut cancel: watch::Receiver<bool>,
    mut receiver: mpsc::Receiver<Delivery>,
) {
    let local = Arc::new(Semaphore::new(
        action.concurrency.clamp(1, crate::MAX_CONCURRENCY),
    ));
    let limits = sink.limits();
    let resident_limit = resident_limit(&action, &limits);
    let count = action.batch_size.min(limits.max_events).max(1);
    let mut lanes = Lanes::new(Duration::from_millis(action.batch_wait_ms));
    let action = Arc::new(action);
    let mut jobs = JoinSet::<(Arc<str>, Vec<Delivery>)>::new();
    let mut open = true;
    'run: loop {
        if *cancel.borrow() || cancel.has_changed().is_err() {
            break;
        }
        while let Some(result) = jobs.try_join_next() {
            match result {
                Ok((key, batch)) => {
                    lanes.finished(&key);
                    drop(batch);
                }
                Err(error) => {
                    record_join(Some(Err(error)), &stats);
                    break 'run;
                }
            }
        }
        // A bounded drain improves batching without starving completions/cancel.
        for _ in 0..256 {
            match receiver.try_recv() {
                Ok(item) => lanes.push(item),
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    open = false;
                    break;
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
            }
        }
        while let Some((key, mut batch, size)) = lanes.batch(count, &limits) {
            let action = Arc::clone(&action);
            let sink = Arc::clone(&sink);
            let global = Arc::clone(&global);
            let local = Arc::clone(&local);
            let stats = Arc::clone(&stats);
            let cancel = cancel.clone();
            jobs.spawn(async move {
                send_batch(
                    action,
                    sink,
                    AttemptLimits { global, local },
                    stats,
                    cancel,
                    &mut batch,
                    size,
                )
                .await;
                (key, batch)
            });
        }
        // Completed task results retain their batch permits until reaped. Thus
        // both live and completed jobs and every lane remain admission-bounded.
        debug_assert!(lanes.lanes.len() <= resident_limit);
        debug_assert!(jobs.len() <= resident_limit);
        #[cfg(test)]
        {
            stats
                .peak_lanes
                .fetch_max(lanes.lanes.len(), Ordering::Relaxed);
            stats.peak_jobs.fetch_max(jobs.len(), Ordering::Relaxed);
        }
        if !open && jobs.is_empty() && lanes.lanes.is_empty() {
            break;
        }
        let deadline = lanes.deadline(count);
        tokio::select! {biased;
            _ = cancel.changed() => break,
            result = jobs.join_next(), if !jobs.is_empty() => match result {
                Some(Ok((key, batch))) => { lanes.finished(&key); drop(batch); },
                Some(Err(error)) => { record_join(Some(Err(error)), &stats); break; }
                None => {}
            },
            item = receiver.recv(), if open => match item {
                Some(item) => lanes.push(item),
                None => open = false,
            },
            _ = async { match deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }} => {},
        }
    }
    drop(receiver);
    drop(lanes);
    // JoinSet owns every child; no network work outlives this generation.
    jobs.shutdown().await;
}
