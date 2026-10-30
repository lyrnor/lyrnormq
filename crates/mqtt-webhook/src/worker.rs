use crate::{
    Action, Connector,
    counters::{ActionCounters, Budget, Counters},
};
use serde_json::json;
use std::{
    sync::{Arc, Mutex, PoisonError, atomic::Ordering},
    time::Duration,
};
use tokio::{
    sync::{Semaphore, mpsc, watch},
    task::JoinHandle,
};

pub(crate) const MAX_REQUEST: usize = 1024 * 1024;

pub(crate) fn headers(
    connector: &Connector,
) -> Result<reqwest::header::HeaderMap, crate::RuntimeError> {
    let mut headers = reqwest::header::HeaderMap::new();
    for (key, value) in &connector.headers {
        let name = reqwest::header::HeaderName::from_bytes(key.as_bytes())
            .map_err(|_| crate::RuntimeError::InvalidConfig)?;
        let mut value = reqwest::header::HeaderValue::from_str(value)
            .map_err(|_| crate::RuntimeError::InvalidConfig)?;
        value.set_sensitive(true);
        headers.insert(name, value);
    }
    Ok(headers)
}

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
    action: Action,
    connector: Connector,
    client: reqwest::Client,
    global: Arc<Semaphore>,
    stats: Arc<ActionCounters>,
    cancel: watch::Receiver<bool>,
) -> (Target, JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel(action.queue_capacity);
    let target = Target {
        sender,
        stats: Arc::clone(&stats),
    };
    let task = tokio::spawn(run(
        action, connector, client, global, stats, cancel, receiver,
    ));
    (target, task)
}
async fn run(
    action: Action,
    connector: Connector,
    client: reqwest::Client,
    global: Arc<Semaphore>,
    stats: Arc<ActionCounters>,
    mut cancel: watch::Receiver<bool>,
    mut receiver: mpsc::Receiver<Delivery>,
) {
    let headers = match headers(&connector) {
        Ok(headers) => headers,
        Err(_) => return,
    };
    let mut pending = None;
    loop {
        if *cancel.borrow() {
            break;
        }
        let first = if let Some(item) = pending.take() {
            Some(item)
        } else {
            tokio::select! {biased; _=cancel.changed()=>None,item=receiver.recv()=>item}
        };
        let Some(first) = first else {
            break;
        };
        let prefix = format!("{{\"action_id\":{},\"events\":[", json!(action.id));
        let mut size = prefix.len() + first.encoded.len() + 2;
        if size > MAX_REQUEST {
            let mut first = first;
            first.complete(false, Some("request_too_large"));
            continue;
        }
        let mut batch = vec![first];
        let deadline = tokio::time::Instant::now() + Duration::from_millis(action.batch_wait_ms);
        while batch.len() < action.batch_size {
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
            if size + 1 + item.encoded.len() > MAX_REQUEST {
                pending = Some(item);
                break;
            }
            size += 1 + item.encoded.len();
            batch.push(item);
        }
        if *cancel.borrow() {
            break;
        }
        let mut body = Vec::with_capacity(size);
        body.extend_from_slice(prefix.as_bytes());
        for (index, item) in batch.iter().enumerate() {
            if index > 0 {
                body.push(b',');
            }
            body.extend_from_slice(&item.encoded);
        }
        body.extend_from_slice(b"]}");
        let mut result = Err("transport".to_owned());
        for attempt in 0..action.max_attempts {
            if *cancel.borrow() {
                break;
            }
            if attempt > 0 {
                stats
                    .retried
                    .fetch_add(batch.len() as u64, Ordering::Relaxed);
                let scale = 1u64 << attempt.min(5);
                let jitter =
                    (body.len() as u64 + attempt as u64 * 37) % action.retry_base_ms.max(1);
                let delay = action
                    .retry_base_ms
                    .saturating_mul(scale)
                    .saturating_add(jitter)
                    .min(30000);
                tokio::select! {biased;_ = cancel.changed()=>break,_=tokio::time::sleep(Duration::from_millis(delay))=>{}}
            }
            let permit = tokio::select! {biased;_ = cancel.changed()=>None,p=Arc::clone(&global).acquire_owned()=>p.ok()};
            let Some(_permit) = permit else {
                break;
            };
            let mut request = client
                .post(&connector.url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .timeout(Duration::from_millis(action.timeout_ms))
                .body(body.clone());
            request = request.headers(headers.clone());
            let response =
                tokio::select! {biased;_ = cancel.changed()=>break,result=request.send()=>result};
            let retry = match response {
                Ok(response) => {
                    let code = response.status();
                    if code.is_success() {
                        result = Ok(());
                        false
                    } else {
                        result = Err(format!("http_{}", code.as_u16()));
                        code.as_u16() == 408 || code.as_u16() == 429 || code.is_server_error()
                    }
                }
                Err(error) => {
                    result = Err(if error.is_timeout() {
                        "timeout"
                    } else {
                        "transport"
                    }
                    .into());
                    true
                }
            };
            if !retry {
                break;
            }
        }
        if *cancel.borrow() {
            break;
        }
        for item in &mut batch {
            match &result {
                Ok(()) => item.complete(true, None),
                Err(error) => item.complete(false, Some(error)),
            }
        }
    }
}
