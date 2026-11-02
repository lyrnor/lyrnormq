//! Protocol-independent journal-head scheduler and durable attempt transitions.
use crate::{
    Outbound,
    counters::{ActionCounters, AttemptGuard, Budget, Counters},
    durable::Queue,
};
use mqtt_egress_core::{FailureKind, Sink};
use mqtt_egress_log::{Completion, RecordMeta};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Semaphore, watch},
    task::{JoinHandle, JoinSet},
    time::Instant,
};

pub(crate) const MAX_REQUEST: usize = 1024 * 1024;

pub(crate) struct Target {
    pub stats: Arc<ActionCounters>,
    pub revision: u64,
    pub enabled: bool,
}
pub(crate) struct Generation {
    pub cancel: watch::Sender<bool>,
    pub targets: BTreeMap<String, Target>,
    pub tasks: Mutex<Vec<JoinHandle<()>>>,
}
impl Generation {
    pub fn cancel(&self) {
        self.cancel.send_replace(true);
    }
    pub async fn finish(&self) {
        self.cancel();
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap_or_else(PoisonError::into_inner));
        let deadline = Instant::now() + Duration::from_secs(2);
        for mut task in tasks {
            if tokio::time::timeout_at(deadline, &mut task).await.is_err() {
                task.abort();
                let _ = task.await;
            }
        }
    }
}
pub(crate) struct Context {
    pub queue: Arc<Queue>,
    pub global: Arc<Semaphore>,
    pub counters: Arc<Counters>,
    pub stopped: Arc<AtomicBool>,
}
pub(crate) fn spawn(
    action: Outbound,
    sink: Arc<dyn Sink>,
    stats: Arc<ActionCounters>,
    context: Arc<Context>,
    cancel: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(run(Arc::new(action), sink, stats, context, cancel))
}
fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}
fn last_error(stats: &ActionCounters, error: Option<&str>) {
    *stats
        .last_error
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = error.map(str::to_owned);
}
async fn run(
    action: Arc<Outbound>,
    sink: Arc<dyn Sink>,
    stats: Arc<ActionCounters>,
    context: Arc<Context>,
    mut cancel: watch::Receiver<bool>,
) {
    let mut changed = context.queue.changed.subscribe();
    let mut memory = context.counters.memory_changed.subscribe();
    let mut jobs = JoinSet::<(String, bool)>::new();
    let mut busy = BTreeSet::new();
    let mut fair_cursor = None;
    let mut scan = true;
    let mut next_due = None;
    let mut capacity_blocked = false;
    'run: loop {
        if *cancel.borrow() || context.stopped.load(Ordering::Acquire) {
            break;
        }
        while let Some(result) = jobs.try_join_next() {
            match result {
                Ok((client, capacity)) => {
                    busy.remove(&client);
                    capacity_blocked = capacity;
                    scan = true;
                }
                Err(_) => {
                    last_error(&stats, Some("worker_failed"));
                    context.stopped.store(true, Ordering::Release);
                    break 'run;
                }
            }
        }
        if scan && !capacity_blocked && jobs.len() < action.concurrency {
            scan = false;
            next_due = None;
            // One fair circular pass, never a fixed hash lane. Each bounded RPC
            // yields to IO/cancel; no idle polling or payload-wide scans.
            let start = fair_cursor;
            let mut cursor = start;
            let mut wrapped = start.is_none();
            loop {
                if context.queue.pending(&action.id) == 0 {
                    break;
                }
                let heads = tokio::select! {biased;_=cancel.changed()=>break 'run,heads=context.queue.heads(action.id.clone(),cursor,64)=>heads};
                let heads = match heads {
                    Ok(heads) => heads,
                    Err(_) => break 'run,
                };
                if heads.is_empty() {
                    if wrapped {
                        break;
                    } else {
                        wrapped = true;
                        cursor = None;
                        continue;
                    }
                }
                let mut reached_start = false;
                for head in heads {
                    if wrapped && start.is_some_and(|start| head.id > start) {
                        reached_start = true;
                        break;
                    }
                    cursor = Some(head.id);
                    if busy.contains(&head.client_id) {
                        continue;
                    }
                    if !head.inflight
                        && head.failures < action.max_attempts
                        && head.not_before_ms > now_ms()
                    {
                        next_due =
                            Some(next_due.map_or(head.not_before_ms, |due: u64| {
                                due.min(head.not_before_ms)
                            }));
                        continue;
                    }
                    busy.insert(head.client_id.clone());
                    fair_cursor = Some(head.id);
                    let job_action = Arc::clone(&action);
                    let sink = Arc::clone(&sink);
                    let stats = Arc::clone(&stats);
                    let context = Arc::clone(&context);
                    let cancel = cancel.clone();
                    jobs.spawn(async move {
                        let key = head.client_id.clone();
                        let capacity =
                            deliver(job_action, sink, stats, context, cancel, head).await;
                        (key, capacity)
                    });
                    if jobs.len() >= action.concurrency {
                        break;
                    }
                }
                if reached_start || jobs.len() >= action.concurrency {
                    break;
                }
            }
        }
        let wake_due = next_due.filter(|_| jobs.len() < action.concurrency && !capacity_blocked);
        tokio::select! {biased;
            _=cancel.changed()=>break,
            result=jobs.join_next(),if !jobs.is_empty()=>match result{Some(Ok((client,capacity)))=>{busy.remove(&client);capacity_blocked=capacity;scan=true;},Some(Err(_))=>{last_error(&stats,Some("worker_failed"));context.stopped.store(true,Ordering::Release);break},None=>{}},
            result=memory.changed(),if capacity_blocked=>{if result.is_err(){break}scan=true;capacity_blocked=false;},
            result=changed.changed()=>{if result.is_err(){break}scan=true;capacity_blocked=false;},
            _=async { match wake_due{Some(due)=>tokio::time::sleep(Duration::from_millis(due.saturating_sub(now_ms()).min(30000))).await,None=>std::future::pending::<()>().await}}=>{scan=true;next_due=None;},
        }
    }
    jobs.shutdown().await;
}
// true means a bounded body window could not be reserved: leave disk work intact.
async fn deliver(
    action: Arc<Outbound>,
    sink: Arc<dyn Sink>,
    stats: Arc<ActionCounters>,
    context: Arc<Context>,
    mut cancel: watch::Receiver<bool>,
    head: RecordMeta,
) -> bool {
    if head.failures >= action.max_attempts {
        return terminal(
            &context,
            &stats,
            vec![head.id],
            Completion::Failed,
            Some("retry_exhausted"),
            1,
        )
        .await;
    }
    if action.batch_wait_ms > 0 && !head.inflight && head.failures == 0 {
        tokio::select! {biased;_=cancel.changed()=>return false,_=tokio::time::sleep(Duration::from_millis(action.batch_wait_ms))=>{}}
    }
    let limits = sink.limits();
    let count = action.batch_size.min(limits.max_events).clamp(1, 64);
    let bytes = limits
        .max_bytes
        .saturating_sub(limits.overhead)
        .min(1024 * 1024);
    if limits.max_events == 0 || head.payload_len > bytes {
        return terminal(
            &context,
            &stats,
            vec![head.id],
            Completion::Failed,
            Some("request_too_large"),
            1,
        )
        .await;
    }
    let Some(plan_memory) = Budget::metadata(
        &context.counters,
        head.client_id
            .len()
            .saturating_add(256)
            .saturating_mul(count),
    ) else {
        return true;
    };
    let plan = tokio::select! {biased;_=cancel.changed()=>return false,plan=context.queue.plan(head.id,count,bytes)=>plan};
    let plan = match plan {
        Ok(plan) => plan,
        Err(_) => return false,
    };
    let mut packed = limits.overhead;
    let mut raw = 0usize;
    let mut take = 0;
    for record in &plan {
        let next = packed
            .checked_add(if take == 0 { 0 } else { limits.separator_bytes })
            .and_then(|n| n.checked_add(record.payload_len));
        if next.is_none_or(|n| n > limits.max_bytes) {
            break;
        }
        packed = next.unwrap_or(usize::MAX);
        raw = raw.saturating_add(record.payload_len);
        take += 1;
    }
    if take == 0 {
        return terminal(
            &context,
            &stats,
            vec![head.id],
            Completion::Failed,
            Some("request_too_large"),
            1,
        )
        .await;
    }
    drop(plan);
    drop(plan_memory);
    let reservation = raw.saturating_add(
        head.client_id
            .len()
            .saturating_add(256)
            .saturating_mul(take),
    );
    let Some(mut budget) =
        Budget::reserve(&context.counters, reservation, Some(Arc::clone(&stats)))
    else {
        return true;
    };
    let count = take;
    let bytes = raw.max(1);
    let batch = tokio::select! {biased;_=cancel.changed()=>return false,batch=context.queue.read(head.id,count,bytes)=>batch};
    let mut batch = match batch {
        Ok(batch) => batch,
        Err(_) => return false,
    };
    let mut size = limits.overhead;
    let mut take = 0;
    for record in &batch {
        let next = size
            .checked_add(if take == 0 { 0 } else { limits.separator_bytes })
            .and_then(|n| n.checked_add(record.payload.len()));
        if next.is_none_or(|n| n > limits.max_bytes) {
            break;
        }
        size = next.unwrap_or(usize::MAX);
        take += 1;
    }
    batch.truncate(take);
    if batch.is_empty() {
        return terminal(
            &context,
            &stats,
            vec![head.id],
            Completion::Failed,
            Some("request_too_large"),
            1,
        )
        .await;
    }
    budget.set_events(batch.len());
    let ids: Vec<_> = batch.iter().map(|r| r.meta.id).collect();
    if head.failures >= action.max_attempts {
        return terminal(
            &context,
            &stats,
            ids,
            Completion::Failed,
            Some("retry_exhausted"),
            batch.len(),
        )
        .await;
    }
    let permit = tokio::select! {biased;_=cancel.changed()=>return false,permit=Arc::clone(&context.global).acquire_owned()=>permit.ok()};
    let Some(permit) = permit else { return false };
    if *cancel.borrow() || context.stopped.load(Ordering::Acquire) {
        return false;
    }
    let begun = tokio::select! {biased;_=cancel.changed()=>return false,result=context.queue.begin(ids.clone())=>result};
    if begun.is_err() {
        return false;
    }
    let response = {
        let _active = AttemptGuard::new(Arc::clone(&context.counters), Arc::clone(&stats));
        if head.failures > 0 || head.inflight {
            stats
                .retried
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
        }
        let events: Vec<&[u8]> = batch
            .iter()
            .map(|record| record.payload.as_slice())
            .collect();
        tokio::select! {biased;_=cancel.changed()=>return false,result=tokio::time::timeout(Duration::from_millis(action.timeout_ms),sink.send(&events))=>match result{Ok(result)=>result,Err(_)=>Err(mqtt_egress_core::SendError{kind:FailureKind::Retryable,code:"timeout".into()})}}
    };
    drop(permit);
    match response {
        Ok(()) => {
            terminal(
                &context,
                &stats,
                ids,
                Completion::Delivered,
                None,
                batch.len(),
            )
            .await
        }
        Err(error) if error.kind == FailureKind::Permanent => {
            terminal(
                &context,
                &stats,
                ids,
                Completion::Failed,
                Some(&error.code),
                batch.len(),
            )
            .await
        }
        Err(error) => {
            // Persist the known failure even when this was the last attempt. A
            // crash between retry and terminal cannot reset the retry budget.
            let failures = head.failures.saturating_add(1);
            let scale = 1u64 << failures.min(5);
            let jitter = (size as u64 + u64::from(failures) * 37) % action.retry_base_ms.max(1);
            let delay = action
                .retry_base_ms
                .saturating_mul(scale)
                .saturating_add(jitter)
                .min(30000);
            let when = now_ms().saturating_add(delay);
            if context.queue.retry(ids.clone(), when).await.is_err() {
                return false;
            }
            if failures >= action.max_attempts {
                terminal(
                    &context,
                    &stats,
                    ids,
                    Completion::Failed,
                    Some(&error.code),
                    batch.len(),
                )
                .await
            } else {
                last_error(&stats, Some(&error.code));
                false
            }
        }
    }
}
async fn terminal(
    context: &Context,
    stats: &ActionCounters,
    ids: Vec<u64>,
    outcome: Completion,
    error: Option<&str>,
    count: usize,
) -> bool {
    if context.queue.complete(ids, outcome).await.is_ok() {
        match outcome {
            Completion::Delivered => {
                stats.delivered.fetch_add(count as u64, Ordering::Relaxed);
            }
            Completion::Failed => {
                stats.failed.fetch_add(count as u64, Ordering::Relaxed);
                context
                    .counters
                    .errors
                    .fetch_add(count as u64, Ordering::Relaxed);
            }
        }
        last_error(stats, error);
    }
    false
}

#[cfg(test)]
#[path = "durable_worker_tests.rs"]
mod tests;
