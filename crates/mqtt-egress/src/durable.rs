//! Bounded, single-owner blocking journal actor. No filesystem work runs on Tokio.
use crate::{
    RuntimeError,
    counters::{ActionCounters, Budget, Counters},
};
use mqtt_egress_log::{
    Binding, Completion, ConsumerMeta, ConsumerRecord, Journal, NewResult, QueueError,
    QueueOptions, QueueStats,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{mpsc, oneshot, watch};

type Reply<T> = oneshot::Sender<Result<T, QueueError>>;
enum Command {
    Bind(Vec<Binding>, Reply<BTreeMap<String, u64>>),
    Append(NewResult, Option<Reply<()>>, Option<Admission>),
    Plan(u64, usize, usize, Reply<Vec<ConsumerMeta>>),
    Heads(String, Option<u64>, usize, Reply<Vec<ConsumerMeta>>),
    Read(u64, usize, usize, Reply<Vec<ConsumerRecord>>),
    Begin(Vec<u64>, Reply<()>),
    Retry(Vec<u64>, u64, Reply<()>),
    Complete(Vec<u64>, Completion, Reply<()>),
    Close(oneshot::Sender<()>),
}
struct Admission {
    _budget: Budget,
    stats: Vec<Arc<ActionCounters>>,
    counters: Arc<Counters>,
    committed: bool,
}
impl Drop for Admission {
    fn drop(&mut self) {
        for stats in &self.stats {
            stats.queued.fetch_sub(1, Ordering::Relaxed);
            if !self.committed {
                stats.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
        if !self.committed {
            self.counters
                .dropped
                .fetch_add(self.stats.len() as u64, Ordering::Relaxed);
        }
    }
}

struct View {
    stats: QueueStats,
    pending: BTreeMap<String, usize>,
    #[cfg(test)]
    queries: usize,
}
pub(crate) struct Queue {
    sender: mpsc::Sender<Command>,
    view: Arc<Mutex<View>>,
    pub changed: watch::Sender<u64>,
    stopped: Arc<AtomicBool>,
    closed: AtomicBool,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}
impl Queue {
    pub async fn open(
        options: QueueOptions,
        bindings: Vec<Binding>,
        counters: Arc<Counters>,
        stopped: Arc<AtomicBool>,
    ) -> Result<(Arc<Self>, BTreeMap<String, u64>), RuntimeError> {
        let opened = tokio::task::spawn_blocking(move || {
            let mut journal = Journal::open(options)?;
            let revisions = journal.bind_all(&bindings)?;
            journal.migrate_legacy(crate::result_wire::resolve_legacy)?;
            Ok::<_, QueueError>((journal, revisions, bindings))
        })
        .await
        .map_err(|_| RuntimeError::Storage)?
        .map_err(|error| {
            #[cfg(test)]
            eprintln!("journal initialization failed: {error:?}");
            let _ = error;
            RuntimeError::Storage
        })?;
        let (journal, revisions, bindings) = opened;
        let view = Arc::new(Mutex::new(View {
            stats: journal.stats(),
            #[cfg(test)]
            queries: 0,
            pending: bindings
                .iter()
                .map(|b| (b.outbound_id.clone(), journal.pending_for(&b.outbound_id)))
                .collect(),
        }));
        let (changed, _) = watch::channel(0u64);
        let (sender, receiver) = mpsc::channel(128);
        let actor_view = Arc::clone(&view);
        let actor_changed = changed.clone();
        let actor_stopped = Arc::clone(&stopped);
        let thread = std::thread::Builder::new()
            .name("rmqtt-outbound-log".into())
            .spawn(move || {
                run(
                    journal,
                    receiver,
                    actor_view,
                    actor_changed,
                    actor_stopped,
                    counters,
                )
            })
            .map_err(|_| RuntimeError::Storage)?;
        Ok((
            Arc::new(Self {
                sender,
                view,
                changed,
                stopped,
                closed: AtomicBool::new(false),
                thread: Mutex::new(Some(thread)),
            }),
            revisions,
        ))
    }
    async fn request<T>(&self, make: impl FnOnce(Reply<T>) -> Command) -> Result<T, QueueError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(QueueError::Io);
        }
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(make(tx))
            .await
            .map_err(|_| QueueError::Io)?;
        rx.await.map_err(|_| QueueError::Io)?
    }
    pub async fn bind(&self, bindings: Vec<Binding>) -> Result<BTreeMap<String, u64>, QueueError> {
        self.request(|tx| Command::Bind(bindings, tx)).await
    }
    #[cfg(test)]
    pub fn append(&self, record: NewResult) -> Result<(), QueueError> {
        if self.stopped.load(Ordering::Acquire) || self.closed.load(Ordering::Acquire) {
            return Err(QueueError::Io);
        }
        let (tx, rx) = oneshot::channel();
        self.sender
            .blocking_send(Command::Append(record, Some(tx), None))
            .map_err(|_| QueueError::Io)?;
        rx.blocking_recv().map_err(|_| QueueError::Io)?
    }
    pub fn enqueue(
        &self,
        record: NewResult,
        budget: Budget,
        stats: Vec<Arc<ActionCounters>>,
        counters: Arc<Counters>,
    ) {
        for item in &stats {
            item.queued.fetch_add(1, Ordering::Relaxed);
        }
        let pending = Admission {
            _budget: budget,
            stats,
            counters,
            committed: false,
        };
        if self.stopped.load(Ordering::Acquire) || self.closed.load(Ordering::Acquire) {
            return;
        }
        let _ = self
            .sender
            .try_send(Command::Append(record, None, Some(pending)));
    }
    pub async fn plan(
        &self,
        id: u64,
        count: usize,
        bytes: usize,
    ) -> Result<Vec<ConsumerMeta>, QueueError> {
        self.request(|tx| Command::Plan(id, count, bytes, tx)).await
    }
    pub async fn heads(
        &self,
        outbound: String,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<ConsumerMeta>, QueueError> {
        self.request(|tx| Command::Heads(outbound, after, limit, tx))
            .await
    }
    pub async fn read(
        &self,
        id: u64,
        count: usize,
        bytes: usize,
    ) -> Result<Vec<ConsumerRecord>, QueueError> {
        self.request(|tx| Command::Read(id, count, bytes, tx)).await
    }
    pub async fn begin(&self, ids: Vec<u64>) -> Result<(), QueueError> {
        self.request(|tx| Command::Begin(ids, tx)).await
    }
    pub async fn retry(&self, ids: Vec<u64>, when: u64) -> Result<(), QueueError> {
        self.request(|tx| Command::Retry(ids, when, tx)).await
    }
    pub async fn complete(&self, ids: Vec<u64>, outcome: Completion) -> Result<(), QueueError> {
        self.request(|tx| Command::Complete(ids, outcome, tx)).await
    }
    pub fn stats(&self) -> QueueStats {
        self.view
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .stats
    }
    pub fn pending(&self, id: &str) -> usize {
        self.view
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pending
            .get(id)
            .copied()
            .unwrap_or(0)
    }
    #[cfg(test)]
    pub fn queries(&self) -> usize {
        self.view
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .queries
    }
    pub async fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            let (tx, rx) = oneshot::channel();
            if self.sender.send(Command::Close(tx)).await.is_ok() {
                let _ = rx.await;
            }
        }
        let thread = self
            .thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(thread) = thread {
            let _ = tokio::task::spawn_blocking(move || thread.join()).await;
        }
    }
}
fn fatal(error: &QueueError) -> bool {
    !matches!(
        error,
        QueueError::Full | QueueError::BindingConflict | QueueError::StaleBinding
    )
}
fn checked<T>(
    result: Result<T, QueueError>,
    stopped: &AtomicBool,
    counters: &Counters,
) -> Result<T, QueueError> {
    if matches!(&result, Err(QueueError::Full)) {
        counters.errors.fetch_add(1, Ordering::Relaxed);
    }
    if result.as_ref().is_err_and(fatal) {
        stopped.store(true, Ordering::Release);
        counters.errors.fetch_add(1, Ordering::Relaxed);
    }
    result
}
fn refresh(journal: &Journal, view: &Mutex<View>) {
    let mut view = view.lock().unwrap_or_else(PoisonError::into_inner);
    view.stats = journal.stats();
    for (id, pending) in &mut view.pending {
        *pending = journal.pending_for(id);
    }
}
fn run(
    mut journal: Journal,
    mut receiver: mpsc::Receiver<Command>,
    view: Arc<Mutex<View>>,
    changed: watch::Sender<u64>,
    stopped: Arc<AtomicBool>,
    counters: Arc<Counters>,
) {
    let mut carried = None;
    loop {
        let command = match carried.take().or_else(|| receiver.blocking_recv()) {
            Some(c) => c,
            None => break,
        };
        if let Command::Close(reply) = command {
            let _ = checked(journal.checkpoint(), &stopped, &counters);
            drop(journal);
            let _ = reply.send(());
            break;
        }
        if let Command::Append(first, reply, pending) = command {
            let batch =
                append::AppendBatch::gather(first, reply, pending, &mut receiver, &mut carried);
            batch.commit(&mut journal, &view, &changed, &stopped, &counters);
            continue;
        }
        let notify = matches!(
            &command,
            Command::Bind(_, _) | Command::Retry(_, _, _) | Command::Complete(_, _, _)
        );
        match command {
            Command::Bind(bindings, reply) => {
                let result = checked(journal.bind_all(&bindings), &stopped, &counters);
                if result.is_ok() {
                    let mut v = view.lock().unwrap_or_else(PoisonError::into_inner);
                    v.pending = bindings
                        .iter()
                        .map(|b| (b.outbound_id.clone(), journal.pending_for(&b.outbound_id)))
                        .collect();
                }
                let _ = reply.send(result);
            }
            Command::Heads(id, after, limit, reply) => {
                #[cfg(test)]
                {
                    view.lock().unwrap_or_else(PoisonError::into_inner).queries += 1;
                }
                let _ = reply.send(Ok(journal.consumer_heads(&id, after, limit)));
            }
            Command::Plan(id, count, bytes, reply) => {
                let _ = reply.send(checked(
                    journal.consumer_metas(id, count, bytes),
                    &stopped,
                    &counters,
                ));
            }
            Command::Read(id, count, bytes, reply) => {
                let _ = reply.send(checked(
                    journal.read_consumers(id, count, bytes),
                    &stopped,
                    &counters,
                ));
            }
            Command::Begin(mut ids, reply) => {
                let mut replies = vec![reply];
                while ids.len() < 64 {
                    match receiver.try_recv() {
                        Ok(Command::Begin(next, reply))
                            if ids.len().saturating_add(next.len()) <= 64 =>
                        {
                            ids.extend(next);
                            replies.push(reply);
                        }
                        Ok(other) => {
                            carried = Some(other);
                            break;
                        }
                        Err(_) => break,
                    }
                }
                let result = checked(journal.begin_consumers(&ids), &stopped, &counters);
                refresh(&journal, &view);
                for reply in replies {
                    let _ = reply.send(result);
                }
            }
            Command::Retry(ids, when, reply) => {
                let _ = reply.send(checked(
                    journal.retry_consumers(&ids, when),
                    &stopped,
                    &counters,
                ));
            }
            Command::Complete(mut ids, outcome, reply) => {
                let mut replies = vec![reply];
                while ids.len() < 64 {
                    match receiver.try_recv() {
                        Ok(Command::Complete(next, next_outcome, reply))
                            if ids.len().saturating_add(next.len()) <= 64
                                && matches!(
                                    (outcome, next_outcome),
                                    (Completion::Delivered, Completion::Delivered)
                                        | (Completion::Failed, Completion::Failed)
                                ) =>
                        {
                            ids.extend(next);
                            replies.push(reply);
                        }
                        Ok(other) => {
                            carried = Some(other);
                            break;
                        }
                        Err(_) => break,
                    }
                }
                let result = checked(
                    journal.complete_consumers(&ids, outcome),
                    &stopped,
                    &counters,
                );
                refresh(&journal, &view);
                for reply in replies {
                    let _ = reply.send(result);
                }
            }
            Command::Append(_, _, _) | Command::Close(_) => {}
        }
        refresh(&journal, &view);
        if notify || stopped.load(Ordering::Acquire) {
            changed.send_modify(|epoch| *epoch = epoch.wrapping_add(1));
        }
    }
}

/// Opaque canonical target binding; names and enablement do not change identity.
pub(crate) fn bindings(config: &crate::Config) -> Result<Vec<Binding>, RuntimeError> {
    config
        .outbounds
        .iter()
        .map(|outbound| {
            let mut bound = outbound.clone();
            bound.name.clear();
            bound.enabled = true;
            serde_json::to_vec(&bound)
                .map(|descriptor| Binding {
                    outbound_id: outbound.id.clone(),
                    descriptor,
                })
                .map_err(|_| RuntimeError::InvalidConfig)
        })
        .collect()
}

#[cfg(test)]
#[path = "durable_group_tests.rs"]
mod group_tests;

#[path = "durable_append.rs"]
mod append;
