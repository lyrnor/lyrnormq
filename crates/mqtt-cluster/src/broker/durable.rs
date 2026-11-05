//! Serialized replicated session transactions and forwarding ownership.
//! State lives on memory replicas; recovery requires surviving copies.
use super::*;
use crate::diagnosis::{self, Failure};
use crate::replica::{Intent, Journal, Recipient, Record, Request};
use mqtt_cluster_core::{DeliveryId, SessionCommit, SessionLease, SessionMutation};
use tokio::sync::{mpsc, oneshot};
mod diagnostics;
mod scheduler;
use diagnostics::{Stage, measure};

const REPLY_TIMEOUT: Duration = Duration::from_secs(8);
const WORK_TIMEOUT: Duration = Duration::from_secs(30);
const FORWARD_WORKERS: usize = 64;
/// Intents one forwarding worker may finish before it requeues the key.
const FORWARD_QUANTUM: usize = 256;
const HOST_BATCH: usize = 512;
const HOST_MAILBOX: usize = 2048;
const SESSION_MAILBOX: usize = 8;
const MAX_INTENTS: usize = 256;
const MAX_PAYLOAD: usize = 1024 * 1024;

#[derive(Debug)]
enum RecoveryRequired {
    Missing,
    Poisoned,
    StaleEpoch,
}
impl std::fmt::Display for RecoveryRequired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "replicated cache recovery required: {self:?}")
    }
}
impl std::error::Error for RecoveryRequired {}
fn recovery_required(error: &io::Error) -> bool {
    error.get_ref().is_some_and(|e| e.is::<RecoveryRequired>())
}
fn acquisition_cache_unchanged(error: &io::Error) -> bool {
    // A stale epoch proves no mutation by this request, but also disproves the
    // cached ownership assumption. Only genuine pre-submission failure restores it.
    rpc::not_submitted(error)
        && error
            .get_ref()
            .is_none_or(|cause| !cause.is::<mqtt_cluster_core::AcquisitionConflict>())
}
fn recovery_error(reason: RecoveryRequired) -> io::Error {
    let stage = match reason {
        RecoveryRequired::Missing => Stage::RecoveryMissing,
        RecoveryRequired::Poisoned => Stage::RecoveryPoisoned,
        RecoveryRequired::StaleEpoch => Stage::RecoveryStaleEpoch,
    };
    let error = io::Error::new(io::ErrorKind::WouldBlock, reason);
    diagnostics::recovery(stage, error)
}

struct Work {
    intents: Vec<Intent>,
    activation: Option<SessionLease>,
    activation_token: Arc<()>,
}
enum Operation {
    ForgetIfDisowned,
    Received {
        id: DeliveryId,
        reply: oneshot::Sender<io::Result<bool>>,
    },
    Release {
        generation: u64,
    },
    Activated {
        epoch: u64,
        revision: u64,
        token: Arc<()>,
    },
    Acquire {
        generation: u64,
        expected: u64,
        reply: oneshot::Sender<io::Result<SessionLease>>,
    },
    Recover {
        reply: oneshot::Sender<io::Result<()>>,
    },
    Commit {
        mutation: SessionMutation,
        reply: oneshot::Sender<io::Result<SessionCommit>>,
    },
    Host {
        message: ClusterMessage,
        reply: oneshot::Sender<io::Result<()>>,
    },
    Work {
        limit: usize,
        reply: oneshot::Sender<io::Result<Work>>,
    },
    Complete {
        completed: Vec<(DeliveryId, String)>,
        reply: oneshot::Sender<io::Result<()>>,
    },
}
struct Job {
    operation: Operation,
}
struct HostJob {
    message: ClusterMessage,
    reply: oneshot::Sender<io::Result<()>>,
}
pub(super) struct Durable {
    runtime: Weak<BrokerRuntime>,
    journal: Journal,
    actors: Mutex<BTreeMap<String, mpsc::Sender<Job>>>,
    scheduler: scheduler::Scheduler,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Acquisition {
    generation: u64,
    expected: u64,
}
struct Cell {
    pending: Option<Acquisition>,
    data_generation: u64,
    record: Option<Record>,
    poisoned: bool,
    activate: bool,
    activation_token: Arc<()>,
    forwarding: bool,
    stashed: Vec<HostJob>,
}
impl Cell {
    fn acquisition_failed(&mut self, previously_poisoned: bool, cache_unchanged: bool) {
        self.poisoned = previously_poisoned || !cache_unchanged;
    }
    fn request_activation(&mut self) {
        self.activate = true;
        self.activation_token = Arc::new(());
    }
    fn activated(&mut self, epoch: u64, revision: u64, token: &Arc<()>) {
        if Arc::ptr_eq(&self.activation_token, token)
            && self
                .record
                .as_ref()
                .is_some_and(|r| r.epoch == epoch && r.revision == revision)
        {
            self.activate = false;
        }
    }
    fn observe_generation(&mut self, generation: u64) {
        if self.data_generation != generation {
            self.record = None;
            self.poisoned = true;
            self.activate = false;
            self.data_generation = generation;
        }
    }
    fn forget_if_disowned(&mut self, metadata: &Metadata, key: &str, node: u64, incarnation: &str) {
        if metadata
            .durable
            .get(key)
            .is_none_or(|owner| owner.node != node || owner.incarnation != incarnation)
        {
            self.record = None;
            self.poisoned = true;
            self.activate = false;
        }
    }
}
pub(crate) fn client_key(client: &str) -> String {
    format!("c:{client}")
}
fn mailbox_capacity(key: &str) -> usize {
    if key.starts_with("h:") {
        HOST_MAILBOX
    } else {
        SESSION_MAILBOX
    }
}
/// Host jobs already waiting share one quorum write. A different operation stays
/// next, including anything that arrived behind it.
fn take_host_batch(
    message: ClusterMessage,
    reply: oneshot::Sender<io::Result<()>>,
    rx: &mut mpsc::Receiver<Job>,
) -> (Vec<HostJob>, Option<Job>) {
    let mut batch = Vec::with_capacity(HOST_BATCH);
    batch.push(HostJob { message, reply });
    let mut deferred = None;
    while batch.len() < HOST_BATCH {
        match rx.try_recv() {
            Ok(Job {
                operation: Operation::Host { message, reply },
            }) => batch.push(HostJob { message, reply }),
            Ok(job) => {
                deferred = Some(job);
                break;
            }
            Err(_) => break,
        }
    }
    (batch, deferred)
}
fn take_queued_hosts(rx: &mut mpsc::Receiver<Job>) -> (Vec<HostJob>, Option<Job>) {
    let mut batch = Vec::new();
    while batch.len() < HOST_BATCH {
        match rx.try_recv() {
            Ok(Job {
                operation: Operation::Host { message, reply },
            }) => batch.push(HostJob { message, reply }),
            Ok(job) => return (batch, Some(job)),
            Err(_) => break,
        }
    }
    (batch, None)
}
fn deliverable(cell: &Cell) -> bool {
    cell.record.as_ref().is_some_and(|record| {
        record.intents.iter().any(|intent| {
            intent
                .recipients
                .iter()
                .any(|recipient| !intent.completed.contains(&recipient.client_id))
        })
    })
}
async fn flush_forward(
    durable: &Durable,
    runtime: &BrokerRuntime,
    key: &str,
    cell: &mut Cell,
    rx: &mut mpsc::Receiver<Job>,
    prepared: Option<Record>,
) -> (io::Result<()>, Option<Job>) {
    let mut hosts = std::mem::take(&mut cell.stashed);
    let (queued, next) = take_queued_hosts(rx);
    hosts.extend(queued);
    let batch = if hosts.len() > HOST_BATCH {
        cell.stashed = hosts.split_off(HOST_BATCH);
        hosts
    } else {
        hosts
    };
    let stored = durable
        .host_batch(runtime, key, batch, cell, prepared)
        .await;
    (stored, next)
}
async fn drive(
    this: &Weak<Durable>,
    key: &str,
    cell: &mut Cell,
    rx: &mut mpsc::Receiver<Job>,
    mut job: Job,
) -> bool {
    loop {
        let Some(durable) = this.upgrade() else {
            return false;
        };
        let Some(runtime) = durable.runtime.upgrade() else {
            return false;
        };
        if runtime.stopped.load(Ordering::Acquire) {
            return false;
        }
        let generation = runtime
            .metadata
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .data_generation;
        cell.observe_generation(generation);
        cell.forget_if_disowned(
            &runtime
                .metadata
                .read()
                .unwrap_or_else(PoisonError::into_inner),
            key,
            runtime.config.node_id,
            &runtime.incarnation,
        );
        if cell.record.is_none() && !cell.stashed.is_empty() {
            cell.forwarding = false;
            Durable::reject_hosts(std::mem::take(&mut cell.stashed));
        }
        let deferred = match job.operation {
            Operation::Host { message, reply } => {
                if cell.forwarding {
                    let mut deferred = None;
                    cell.stashed.push(HostJob { message, reply });
                    while cell.stashed.len() < HOST_MAILBOX {
                        match rx.try_recv() {
                            Ok(Job {
                                operation: Operation::Host { message, reply },
                            }) => cell.stashed.push(HostJob { message, reply }),
                            Ok(job) => {
                                deferred = Some(job);
                                break;
                            }
                            Err(_) => break,
                        }
                    }
                    deferred
                } else {
                    let (batch, next) = take_host_batch(message, reply, rx);
                    let _ = durable.host_batch(&runtime, key, batch, cell, None).await;
                    next
                }
            }
            Operation::Complete { completed, reply } => match completion_record(&completed, cell) {
                Err(error) => {
                    let _ = reply.send(Err(error));
                    None
                }
                Ok(prepared) => {
                    let (stored, next) =
                        flush_forward(durable.as_ref(), &runtime, key, cell, rx, prepared).await;
                    let _ = reply.send(stored);
                    next
                }
            },
            operation => {
                durable
                    .apply_operation(&runtime, key, operation, cell)
                    .await;
                None
            }
        };
        match deferred {
            Some(next) => job = next,
            None => return true,
        }
    }
}
fn lease(key: &str, record: &Record) -> SessionLease {
    SessionLease {
        client_id: key.strip_prefix("c:").unwrap_or(key).to_owned(),
        epoch: record.epoch,
        revision: record.revision,
        snapshot: record.snapshot.clone(),
    }
}
fn acquired_record(mut record: Record, epoch: u64) -> io::Result<Record> {
    if record.epoch > epoch {
        return Err(unavailable());
    }
    if record.epoch < epoch {
        record.epoch = epoch;
        record.physical_revision = 0;
    }
    Ok(record)
}

fn completion_record(marks: &[(DeliveryId, String)], cell: &Cell) -> io::Result<Option<Record>> {
    if marks.is_empty() {
        return Ok(None);
    }
    let mut next = cell.record.clone().ok_or_else(unavailable)?;
    let mut matched = false;
    for (id, recipient) in marks {
        let Some(intent) = next.intents.iter_mut().find(|intent| &intent.id == id) else {
            continue;
        };
        intent.completed.insert(recipient.clone());
        matched = true;
    }
    if !matched {
        return Ok(None);
    }
    next.intents.retain(|intent| {
        intent
            .recipients
            .iter()
            .any(|recipient| !intent.completed.contains(&recipient.client_id))
    });
    Ok(Some(next))
}
enum Routed {
    Expired,
    Local(RecipientDelivery),
    Remote(String, RecipientDelivery),
}
struct Pending {
    mark_id: DeliveryId,
    client_id: String,
    routed: Routed,
}
fn route_recipient(
    runtime: &BrokerRuntime,
    recipient: &Recipient,
    intent: &Intent,
) -> io::Result<Routed> {
    if intent
        .message
        .expires_at
        .is_some_and(|at| at <= controller::now())
    {
        return Ok(Routed::Expired);
    }
    let mut delivery_id = intent.id.clone();
    delivery_id.target_generation = recipient.generation;
    let owner = runtime
        .metadata
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .durable
        .get(&client_key(&recipient.client_id))
        .cloned()
        .ok_or_else(unavailable)?;
    let delivery = RecipientDelivery {
        client_id: recipient.client_id.clone(),
        id: delivery_id,
        message: intent.message.clone(),
    };
    if owner.node == runtime.config.node_id && owner.incarnation == runtime.incarnation {
        return Ok(Routed::Local(delivery));
    }
    let address = runtime
        .config
        .durable
        .as_ref()
        .and_then(|group| group.replicas.iter().find(|peer| peer.id == owner.node))
        .map(|peer| peer.address.clone())
        .ok_or_else(unavailable)?;
    Ok(Routed::Remote(address, delivery))
}
fn note_failure(failure: &mut Option<io::Error>, error: io::Error) {
    if failure.is_none() {
        *failure = Some(error);
    }
}

impl Durable {
    pub fn new(runtime: Weak<BrokerRuntime>, journal: Journal) -> Self {
        Self {
            runtime,
            journal,
            actors: Mutex::new(BTreeMap::new()),
            scheduler: scheduler::Scheduler::default(),
        }
    }
    fn actor(self: &Arc<Self>, key: &str) -> io::Result<mpsc::Sender<Job>> {
        let mut actors = self.actors.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(actor) = actors.get(key) {
            return Ok(actor.clone());
        }
        // This bounds retained per-session actor identities, independently of
        // concurrent transaction admission. It matches the physical directory.
        if actors.len() >= 101_024 {
            return Err(unavailable());
        }
        let (tx, rx) = mpsc::channel(mailbox_capacity(key));
        actors.insert(key.to_owned(), tx.clone());
        self.spawn_actor(key.to_owned(), rx);
        Ok(tx)
    }
    fn spawn_actor(self: &Arc<Self>, key: String, mut rx: mpsc::Receiver<Job>) {
        let this = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut cell = Cell {
                pending: None,
                data_generation: 0,
                record: None,
                poisoned: false,
                activate: false,
                activation_token: Arc::new(()),
                forwarding: false,
                stashed: Vec::new(),
            };
            while let Some(job) = rx.recv().await {
                if !drive(&this, &key, &mut cell, &mut rx, job).await {
                    break;
                }
            }
        });
    }
    async fn apply_operation(
        self: &Arc<Self>,
        runtime: &BrokerRuntime,
        key: &str,
        operation: Operation,
        cell: &mut Cell,
    ) {
        match operation {
            Operation::Host { reply, .. } => {
                let _ = reply.send(Err(unavailable()));
            }
            Operation::ForgetIfDisowned => cell.forget_if_disowned(
                &runtime
                    .metadata
                    .read()
                    .unwrap_or_else(PoisonError::into_inner),
                key,
                runtime.config.node_id,
                &runtime.incarnation,
            ),
            Operation::Received { id, reply } => {
                let result = cell
                    .record
                    .as_ref()
                    .filter(|r| !cell.poisoned && Self::authoritative(runtime, key, r.epoch))
                    .map(|r| r.received_or_obsolete(&id))
                    .ok_or_else(unavailable);
                let _ = reply.send(result);
            }
            Operation::Release { generation } => {
                if runtime
                    .metadata
                    .read()
                    .unwrap_or_else(PoisonError::into_inner)
                    .durable
                    .get(key)
                    .is_some_and(|o| {
                        o.node == runtime.config.node_id
                            && o.incarnation == runtime.incarnation
                            && o.generation == generation
                    })
                {
                    cell.request_activation();
                    self.changed(key, cell);
                }
            }
            Operation::Activated {
                epoch,
                revision,
                token,
            } => {
                cell.activated(epoch, revision, &token);
            }
            Operation::Acquire {
                generation,
                expected,
                reply,
            } => {
                let result = self.take(runtime, key, generation, expected, cell).await;
                self.respond_acquisition(key, cell, reply, result);
            }
            Operation::Recover { reply } => {
                let result = self.recover_cell(runtime, key, cell).await;
                self.respond(key, cell, reply, result);
            }
            Operation::Commit { mutation, reply } => {
                let result = self.commit(runtime, key, mutation, cell).await;
                self.respond(key, cell, reply, result);
            }
            Operation::Work { limit, reply } => {
                if !deliverable(cell) && !cell.stashed.is_empty() {
                    let batch = std::mem::take(&mut cell.stashed);
                    let _ = self.host_batch(runtime, key, batch, cell, None).await;
                }
                let result = self.work(runtime, key, cell, limit);
                cell.forwarding = result.as_ref().is_ok_and(|state| !state.intents.is_empty());
                let _ = reply.send(result);
            }
            Operation::Complete { completed, reply } => {
                let _ = reply.send(self.complete(runtime, key, &completed, cell).await);
            }
        }
    }
    async fn send(self: &Arc<Self>, key: &str, operation: Operation) -> io::Result<()> {
        // The per-key mailbox is the only transaction admission boundary. Waiting
        // is cancellable until submission; a submitted job belongs to its actor,
        // even if the caller subsequently cancels its reply future.
        self.actor(key)?
            .send(Job { operation })
            .await
            .map_err(|_| diagnosis::reject(Failure::ActorClosed))
    }
    pub fn forget(self: &Arc<Self>, key: &str) {
        // Keep the channel/serialization identity; discard only obsolete payload cache.
        let present = self
            .actors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(key);
        if present {
            self.scheduler.schedule(key);
        }
    }
    async fn await_reply<T>(&self, rx: oneshot::Receiver<io::Result<T>>) -> io::Result<T> {
        tokio::time::timeout(REPLY_TIMEOUT, rx)
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())?
    }
    pub async fn release(self: &Arc<Self>, client: &str, generation: u64) -> io::Result<()> {
        self.send(&client_key(client), Operation::Release { generation })
            .await
    }
    pub async fn acquire(
        self: &Arc<Self>,
        key: String,
        generation: u64,
        expected: u64,
    ) -> io::Result<SessionLease> {
        let (reply, rx) = oneshot::channel();
        self.send(
            &key,
            Operation::Acquire {
                generation,
                expected,
                reply,
            },
        )
        .await?;
        self.await_reply(rx).await
    }
    pub async fn transaction(
        self: &Arc<Self>,
        mutation: SessionMutation,
    ) -> io::Result<SessionCommit> {
        let key = client_key(&mutation.client_id);
        let (reply, rx) = oneshot::channel();
        self.send(&key, Operation::Commit { mutation, reply })
            .await?;
        self.await_reply(rx).await
    }
    pub async fn enqueue(self: &Arc<Self>, message: ClusterMessage) -> io::Result<()> {
        let runtime = self.runtime.upgrade().ok_or_else(unavailable)?;
        let key = format!("h:{}:{}", runtime.config.node_id, runtime.incarnation);
        let (reply, rx) = oneshot::channel();
        self.send(&key, Operation::Host { message, reply }).await?;
        self.await_reply(rx).await
    }
    pub async fn received(self: &Arc<Self>, client: &str, id: DeliveryId) -> io::Result<bool> {
        let (reply, rx) = oneshot::channel();
        self.send(&client_key(client), Operation::Received { id, reply })
            .await?;
        self.await_reply(rx).await
    }
    pub async fn inspect(&self, runtime: &BrokerRuntime, client: &str) -> io::Result<SessionLease> {
        if !runtime.ready() {
            return Err(unavailable());
        }
        let key = client_key(client);
        let epoch = runtime
            .rpc()
            .await?
            .owner(key.clone())
            .await?
            .map_or(0, |owner| owner.epoch);
        let record =
            quorum::request(runtime, &self.journal, Request::Read { key: key.clone() }).await?;
        let mut result = record
            .as_ref()
            .map(|r| lease(&key, r))
            .unwrap_or(SessionLease {
                client_id: client.to_owned(),
                epoch: 0,
                revision: 0,
                snapshot: None,
            });
        result.epoch = epoch;
        Ok(result)
    }
    fn authoritative(runtime: &BrokerRuntime, key: &str, epoch: u64) -> bool {
        runtime.ready()
            && runtime
                .metadata
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .durable
                .get(key)
                .is_some_and(|owner| {
                    owner.node == runtime.config.node_id
                        && owner.incarnation == runtime.incarnation
                        && owner.epoch == epoch
                })
    }
    async fn take(
        &self,
        runtime: &BrokerRuntime,
        key: &str,
        generation: u64,
        expected: u64,
        cell: &mut Cell,
    ) -> io::Result<SessionLease> {
        measure(
            Stage::TakeFinish,
            self.take_inner(runtime, key, generation, expected, cell),
        )
        .await
    }
    async fn take_inner(
        &self,
        runtime: &BrokerRuntime,
        key: &str,
        generation: u64,
        expected: u64,
        cell: &mut Cell,
    ) -> io::Result<SessionLease> {
        if !runtime.ready() {
            return Err(unavailable());
        }
        let rpc = runtime.rpc().await?;
        // A lost reply may hide a committed ownership change. Invalidate cached
        // authority BEFORE submission, not after receiving the successful reply.
        let previously_poisoned = cell.poisoned;
        let prior_pending = cell.pending;
        cell.pending = Some(Acquisition {
            generation,
            expected,
        });
        cell.poisoned = true;
        let answer = measure(
            Stage::TakeMutate,
            rpc.mutate(Command::AcquireOnce {
                key: key.to_owned(),
                node: runtime.config.node_id,
                incarnation: runtime.incarnation.clone(),
                generation,
                expected_epoch: expected,
            }),
        )
        .await;
        let answer = match answer {
            Ok(answer) => answer,
            Err(error) => {
                if rpc::not_submitted(&error) {
                    cell.pending = prior_pending;
                    cell.acquisition_failed(
                        previously_poisoned,
                        acquisition_cache_unchanged(&error),
                    );
                    // A rejected older CONNECT must not destroy a still-valid
                    // cache or a previously unresolved acquisition. Numeric TCP
                    // generations do not define MQTT takeover order.
                    if prior_pending.is_none()
                        && !previously_poisoned
                        && error.get_ref().is_some_and(|cause| {
                            cause.is::<mqtt_cluster_core::AcquisitionConflict>()
                        })
                        && let Ok(Some(owner)) = rpc.owner(key.to_owned()).await
                        && owner.node == runtime.config.node_id
                        && owner.incarnation == runtime.incarnation
                        && cell
                            .record
                            .as_ref()
                            .is_some_and(|record| record.epoch == owner.epoch)
                        && Self::authoritative(runtime, key, owner.epoch)
                    {
                        cell.poisoned = false;
                    }
                }
                return Err(error);
            }
        };
        measure(Stage::TakeBarrier, runtime.barrier(answer.revision)).await?;
        let old = measure(
            Stage::TakeFence,
            quorum::request(
                runtime,
                &self.journal,
                Request::Fence {
                    key: key.to_owned(),
                    epoch: answer.revision,
                },
            ),
        )
        .await?;
        let record = acquired_record(
            old.unwrap_or_else(|| Record::empty(answer.revision)),
            answer.revision,
        )?;
        measure(
            Stage::TakeWrite,
            quorum::request(
                runtime,
                &self.journal,
                Request::Write {
                    key: key.to_owned(),
                    record: record.clone(),
                },
            ),
        )
        .await?;
        let authority_start = std::time::Instant::now();
        let authority = if !Self::authoritative(runtime, key, record.epoch) {
            Err(diagnosis::reject(Failure::TakeAuthority))
        } else {
            Ok(())
        };
        diagnostics::finish(Stage::TakeAuthority, authority_start, authority)?;
        let filters_differ = runtime
            .metadata
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .durable
            .get(key)
            .is_none_or(|owner| {
                owner.filters != record.filters
                    || owner.logical_generation != record.logical_generation
            });
        measure(Stage::TakeFilters, async {
            if filters_differ {
                runtime
                    .command(
                        Command::DurableFilters {
                            key: key.to_owned(),
                            node: runtime.config.node_id,
                            incarnation: runtime.incarnation.clone(),
                            epoch: record.epoch,
                            filters: record.filters.clone(),
                            logical_generation: record.logical_generation,
                        },
                        true,
                    )
                    .await?;
            }
            Ok(())
        })
        .await?;
        // Filter synchronization may await another control round trip. Ownership
        // can change during it, so the earlier post-write check is insufficient.
        if !Self::authoritative(runtime, key, record.epoch) {
            return Err(diagnosis::reject(Failure::TakeAuthority));
        }
        let lease = lease(key, &record);
        cell.record = Some(record);
        cell.pending = None;
        cell.poisoned = false;
        cell.activate = false;
        if generation == 0 && key.starts_with("c:") {
            cell.request_activation();
        }
        Ok(lease)
    }
    async fn recover(self: &Arc<Self>, key: &str) -> io::Result<()> {
        let (reply, rx) = oneshot::channel();
        self.send(key, Operation::Recover { reply }).await?;
        self.await_reply(rx).await
    }
    async fn recover_cell(
        &self,
        runtime: &BrokerRuntime,
        key: &str,
        cell: &mut Cell,
    ) -> io::Result<()> {
        // Work can become stale while Recover waits in the mailbox. Re-check
        // inside the same actor: a newer successful CONNECT/Commit needs no
        // fresh epoch, offline activation or connection fencing.
        if cell.pending.is_none()
            && !cell.poisoned
            && cell
                .record
                .as_ref()
                .is_some_and(|record| Self::authoritative(runtime, key, record.epoch))
        {
            return Ok(());
        }
        let acquisition = cell.pending.unwrap_or_else(|| Acquisition {
            generation: 0,
            expected: runtime
                .metadata
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .durable
                .get(key)
                .map_or(0, |owner| owner.epoch),
        });
        let result = self
            .take(
                runtime,
                key,
                acquisition.generation,
                acquisition.expected,
                cell,
            )
            .await;
        match result {
            Ok(_) => {}
            Err(error)
                if error
                    .get_ref()
                    .is_some_and(|cause| cause.is::<mqtt_cluster_core::AcquisitionConflict>()) =>
            {
                // This identity was definitely rejected. Reconcile with a
                // linearizable owner rather than guessing a new predecessor.
                let owner = runtime.rpc().await?.owner(key.to_owned()).await?;
                let Some(owner) = owner.filter(|owner| {
                    owner.node == runtime.config.node_id && owner.incarnation == runtime.incarnation
                }) else {
                    cell.pending = None;
                    cell.record = None;
                    cell.poisoned = true;
                    cell.activate = false;
                    return Err(error);
                };
                let confirmed = Acquisition {
                    generation: owner.acquired_from.map_or(0, |_| owner.generation),
                    // Legacy owners have no retry identity: one genuine fenced
                    // recovery is allowed, retained on every subsequent failure.
                    expected: owner.acquired_from.unwrap_or(owner.epoch),
                };
                cell.pending = Some(confirmed);
                self.take(runtime, key, confirmed.generation, confirmed.expected, cell)
                    .await?;
            }
            Err(error) => return Err(error),
        }
        if key.starts_with("c:") {
            cell.request_activation();
        }
        Ok(())
    }
    fn respond_acquisition(
        &self,
        key: &str,
        cell: &mut Cell,
        reply: oneshot::Sender<io::Result<SessionLease>>,
        result: io::Result<SessionLease>,
    ) {
        let acquired = result.is_ok();
        if reply.send(result).is_err() && acquired && key.starts_with("c:") {
            cell.request_activation();
        }
        self.changed(key, cell);
    }
    fn recipients(runtime: &BrokerRuntime, message: &ClusterMessage) -> Vec<Recipient> {
        runtime.durable_recipients(&message.topic)
    }
    fn message_rejected(message: &ClusterMessage) -> bool {
        message.retain
            || message.qos > 2
            || message.scope != ForwardScope::Ordinary
            || message.payload.len() > MAX_PAYLOAD
    }
    fn append(
        runtime: &BrokerRuntime,
        key: &str,
        record: &mut Record,
        publications: Vec<ClusterMessage>,
    ) -> io::Result<()> {
        for (index, message) in publications.into_iter().enumerate() {
            if Self::message_rejected(&message) {
                return Err(unavailable());
            }
            let recipients = Self::recipients(runtime, &message);
            if !recipients.is_empty() {
                record.intents.push(Intent {
                    id: DeliveryId {
                        source: key.to_owned(),
                        epoch: record.epoch,
                        revision: record.revision,
                        index: u32::try_from(index).map_err(|_| unavailable())?,
                        target_generation: 0,
                    },
                    message,
                    recipients,
                    completed: BTreeSet::new(),
                });
            }
        }
        if record.intents.len() > MAX_INTENTS || record.receipts.len() > 8192 {
            return Err(unavailable());
        }
        Ok(())
    }
    async fn store(
        &self,
        runtime: &BrokerRuntime,
        key: &str,
        mut next: Record,
        cell: &mut Cell,
    ) -> io::Result<()> {
        if cell.poisoned || !Self::authoritative(runtime, key, next.epoch) {
            return Err(diagnosis::reject(Failure::StoreAuthority));
        }
        next.physical_revision = next
            .physical_revision
            .checked_add(1)
            .ok_or_else(unavailable)?;
        // Unknown quorum outcomes poison both actor mutations and background completion writes.
        cell.poisoned = true;
        quorum::request(
            runtime,
            &self.journal,
            Request::Write {
                key: key.to_owned(),
                record: next.clone(),
            },
        )
        .await?;
        if !Self::authoritative(runtime, key, next.epoch) {
            return Err(diagnosis::reject(Failure::StoreAfterWriteAuthority));
        }
        cell.record = Some(next);
        cell.poisoned = false;
        Ok(())
    }
    async fn commit(
        &self,
        runtime: &BrokerRuntime,
        key: &str,
        mutation: SessionMutation,
        cell: &mut Cell,
    ) -> io::Result<SessionCommit> {
        let current = cell
            .record
            .as_ref()
            .ok_or_else(|| diagnosis::reject(Failure::CommitNoRecord))?;
        if cell.poisoned {
            return Err(diagnosis::reject(Failure::CommitPoisoned));
        }
        if mutation.epoch != current.epoch {
            return Err(diagnosis::reject(Failure::CommitEpoch));
        }
        if !Self::authoritative(runtime, key, current.epoch) {
            return Err(diagnosis::reject(Failure::CommitAuthority));
        }
        if mutation
            .receipt
            .as_ref()
            .is_some_and(|id| current.received_or_obsolete(id))
        {
            return Ok(SessionCommit {
                revision: current.revision,
                duplicate: true,
            });
        }
        if mutation.expected_revision != current.revision {
            return Err(diagnosis::reject(Failure::CommitRevision));
        }
        let mut next = current.clone();
        next.revision = next.revision.checked_add(1).ok_or_else(unavailable)?;
        if mutation.reset_session {
            next.logical_generation = next
                .logical_generation
                .checked_add(1)
                .ok_or_else(unavailable)?;
            next.receipts.clear();
        }
        next.snapshot = mutation.snapshot;
        next.filters = mutation.filters;
        if let Some(id) = mutation.receipt {
            next.receipts.insert(id);
        }
        Self::append(runtime, key, &mut next, mutation.publications)?;
        let filters_changed = current.filters != next.filters
            || current.logical_generation != next.logical_generation;
        self.store(runtime, key, next, cell).await?;
        let record = cell.record.as_ref().ok_or_else(unavailable)?;
        if filters_changed
            && runtime
                .command(
                    Command::DurableFilters {
                        key: key.to_owned(),
                        node: runtime.config.node_id,
                        incarnation: runtime.incarnation.clone(),
                        epoch: record.epoch,
                        filters: record.filters.clone(),
                        logical_generation: record.logical_generation,
                    },
                    true,
                )
                .await
                .is_err()
        {
            cell.poisoned = true;
            return Err(unavailable());
        }
        Ok(SessionCommit {
            revision: record.revision,
            duplicate: false,
        })
    }
    fn stage_record(
        runtime: &BrokerRuntime,
        key: &str,
        current: &Record,
        messages: &[ClusterMessage],
    ) -> io::Result<Record> {
        let mut next = current.clone();
        next.revision = next.revision.checked_add(1).ok_or_else(unavailable)?;
        Self::append(runtime, key, &mut next, messages.to_vec())?;
        Ok(next)
    }
    fn record_within_limit(record: &Record) -> bool {
        serde_json::to_vec(record).is_ok_and(|bytes| bytes.len() <= crate::replica::MAX_RECORD)
    }
    fn stage_fits(
        runtime: &BrokerRuntime,
        key: &str,
        current: &Record,
        messages: &[ClusterMessage],
    ) -> bool {
        Self::stage_record(runtime, key, current, messages)
            .is_ok_and(|record| Self::record_within_limit(&record))
    }
    /// Longest prefix whose canonical record stays inside the memory-replica limit.
    fn fitting_prefix(
        runtime: &BrokerRuntime,
        key: &str,
        current: &Record,
        messages: &[ClusterMessage],
    ) -> usize {
        if messages.is_empty() || Self::stage_fits(runtime, key, current, messages) {
            return messages.len();
        }
        let mut low = 0;
        let mut high = messages.len() - 1;
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            if Self::stage_fits(runtime, key, current, &messages[..mid]) {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        low
    }
    fn finish_hosts(&self, key: &str, cell: &Cell, jobs: Vec<HostJob>, result: io::Result<()>) {
        self.changed(key, cell);
        match result {
            Ok(()) => {
                for job in jobs {
                    let _ = job.reply.send(Ok(()));
                }
            }
            Err(error) => {
                let kind = error.kind();
                let text = error.to_string();
                for job in jobs {
                    let _ = job.reply.send(Err(io::Error::new(kind, text.clone())));
                }
            }
        }
    }
    fn reject_hosts(jobs: impl IntoIterator<Item = HostJob>) {
        for job in jobs {
            let _ = job.reply.send(Err(unavailable()));
        }
    }
    /// Messages that would grow the outbox past its intent or byte cap fail
    /// immediately. Accepted messages share one revision and one quorum write.
    async fn host_batch(
        &self,
        runtime: &BrokerRuntime,
        key: &str,
        batch: Vec<HostJob>,
        cell: &mut Cell,
        prepared: Option<Record>,
    ) -> io::Result<()> {
        let completion = prepared.is_some();
        if batch.is_empty() {
            return if let Some(record) = prepared {
                self.store(runtime, key, record, cell).await
            } else {
                Ok(())
            };
        }
        let current = if let Some(record) = prepared {
            record
        } else {
            if (cell.pending.is_some()
                || cell.poisoned
                || cell
                    .record
                    .as_ref()
                    .is_none_or(|record| !Self::authoritative(runtime, key, record.epoch)))
                && let Err(error) = self.recover_cell(runtime, key, cell).await
            {
                self.finish_hosts(key, cell, batch, Err(error));
                return Ok(());
            }
            let Some(current) = cell.record.clone() else {
                self.finish_hosts(key, cell, batch, Err(unavailable()));
                return Ok(());
            };
            current
        };
        let mut rejected = Vec::new();
        let mut admissible = Vec::new();
        let mut intents = current.intents.len();
        for job in batch {
            if Self::message_rejected(&job.message) {
                rejected.push(job);
                continue;
            }
            let adds_intent = !Self::recipients(runtime, &job.message).is_empty();
            if adds_intent && intents >= MAX_INTENTS {
                rejected.push(job);
                continue;
            }
            if adds_intent {
                intents += 1;
            }
            admissible.push(job);
        }
        let messages: Vec<ClusterMessage> =
            admissible.iter().map(|job| job.message.clone()).collect();
        let fit = Self::fitting_prefix(runtime, key, &current, &messages);
        let overflow = admissible.split_off(fit);
        rejected.extend(overflow);
        Self::reject_hosts(rejected);
        if admissible.is_empty() {
            if completion {
                return self.store(runtime, key, current, cell).await;
            }
            self.changed(key, cell);
            return Ok(());
        }
        let accepted_messages: Vec<ClusterMessage> =
            admissible.iter().map(|job| job.message.clone()).collect();
        let next = match Self::stage_record(runtime, key, &current, &accepted_messages) {
            Ok(record) if Self::record_within_limit(&record) => record,
            _ => {
                self.finish_hosts(key, cell, admissible, Err(unavailable()));
                if completion {
                    return self.store(runtime, key, current, cell).await;
                }
                return Ok(());
            }
        };
        let stored = self.store(runtime, key, next, cell).await;
        let reported = match &stored {
            Ok(()) => Ok(()),
            Err(error) => Err(io::Error::new(error.kind(), error.to_string())),
        };
        self.finish_hosts(key, cell, admissible, reported);
        if completion { stored } else { Ok(()) }
    }
    fn work(
        &self,
        runtime: &BrokerRuntime,
        key: &str,
        cell: &Cell,
        limit: usize,
    ) -> io::Result<Work> {
        if !runtime.ready() {
            return Err(unavailable());
        }
        let current = cell
            .record
            .as_ref()
            .ok_or_else(|| recovery_error(RecoveryRequired::Missing))?;
        if cell.poisoned {
            return Err(recovery_error(RecoveryRequired::Poisoned));
        }
        if !Self::authoritative(runtime, key, current.epoch) {
            return Err(recovery_error(RecoveryRequired::StaleEpoch));
        }
        let take = limit.min(FORWARD_QUANTUM);
        Ok(Work {
            intents: current.intents.iter().take(take).cloned().collect(),
            activation: (cell.activate && key.starts_with("c:")).then(|| lease(key, current)),
            activation_token: cell.activation_token.clone(),
        })
    }
    async fn complete(
        &self,
        runtime: &BrokerRuntime,
        key: &str,
        marks: &[(DeliveryId, String)],
        cell: &mut Cell,
    ) -> io::Result<()> {
        match completion_record(marks, cell)? {
            Some(next) => self.store(runtime, key, next, cell).await,
            None => Ok(()),
        }
    }
    async fn one(self: &Arc<Self>, key: String, limit: usize) -> io::Result<usize> {
        let runtime = self.runtime.upgrade().ok_or_else(unavailable)?;
        let (reply, rx) = oneshot::channel();
        self.send(&key, Operation::Work { limit, reply }).await?;
        let outcome = self.await_reply(rx).await;
        let state = match outcome {
            Ok(state) => state,
            Err(error) => {
                diagnosis::observe(Failure::BackgroundWorkReply);
                if !recovery_required(&error) {
                    return Err(error);
                }
                let owner = runtime
                    .metadata
                    .read()
                    .unwrap_or_else(PoisonError::into_inner)
                    .durable
                    .get(&key)
                    .cloned()
                    .ok_or_else(unavailable)?;
                if owner.node != runtime.config.node_id || owner.incarnation != runtime.incarnation
                {
                    return Err(unavailable());
                }
                diagnosis::observe(Failure::BackgroundReacquire);
                self.recover(&key).await?;
                // The next Work carries offline activation and its acknowledgement.
                return Ok(1);
            }
        };
        let activated = state.activation.is_some();
        if let Some(activation) = state.activation {
            let epoch = activation.epoch;
            let revision = activation.revision;
            runtime.local()?.activate_session(activation).await?;
            self.send(
                &key,
                Operation::Activated {
                    epoch,
                    revision,
                    token: state.activation_token,
                },
            )
            .await?;
        }
        if state.intents.is_empty() {
            return Ok(usize::from(activated));
        }
        // Same-client deliveries stay ordered. Different clients in one wave share a
        // destination batch. A failed client keeps its later intents queued.
        let mut queues: BTreeMap<String, VecDeque<Pending>> = BTreeMap::new();
        let mut failure = None;
        let mut blocked = BTreeSet::new();
        for intent in &state.intents {
            for recipient in &intent.recipients {
                if intent.completed.contains(&recipient.client_id)
                    || blocked.contains(&recipient.client_id)
                {
                    continue;
                }
                match route_recipient(&runtime, recipient, intent) {
                    Ok(routed) => queues
                        .entry(recipient.client_id.clone())
                        .or_default()
                        .push_back(Pending {
                            mark_id: intent.id.clone(),
                            client_id: recipient.client_id.clone(),
                            routed,
                        }),
                    Err(error) => {
                        blocked.insert(recipient.client_id.clone());
                        queues.remove(&recipient.client_id);
                        note_failure(&mut failure, error);
                    }
                }
            }
        }
        let batcher = runtime.delivery_batcher().await.ok().cloned();
        let mut marks = Vec::new();
        while queues.values().any(|queue| !queue.is_empty()) {
            let mut expired = Vec::new();
            let mut local = Vec::new();
            let mut remote: BTreeMap<String, Vec<(DeliveryId, String, RecipientDelivery)>> =
                BTreeMap::new();
            let clients: Vec<String> = queues.keys().cloned().collect();
            for client in clients {
                let Some(pending) = queues.get_mut(&client).and_then(VecDeque::pop_front) else {
                    continue;
                };
                match pending.routed {
                    Routed::Expired => expired.push((pending.mark_id, pending.client_id)),
                    Routed::Local(delivery) => {
                        local.push((pending.mark_id, pending.client_id, delivery));
                    }
                    Routed::Remote(address, delivery) => {
                        remote.entry(address).or_default().push((
                            pending.mark_id,
                            pending.client_id,
                            delivery,
                        ));
                    }
                }
            }
            for (mark_id, client_id) in expired {
                marks.push((mark_id, client_id));
            }
            enum Wave {
                One(DeliveryId, String, io::Result<()>),
                Many(Vec<(DeliveryId, String, io::Result<()>)>),
            }
            let mut joins = tokio::task::JoinSet::new();
            for (mark_id, client_id, delivery) in local {
                let runtime = Arc::clone(&runtime);
                joins.spawn(async move {
                    Wave::One(
                        mark_id,
                        client_id,
                        runtime.deliver_recipient(delivery).await,
                    )
                });
            }
            for (address, items) in remote {
                let Some(batcher) = batcher.clone() else {
                    for (_, client_id, _) in items {
                        queues.remove(&client_id);
                        note_failure(&mut failure, unavailable());
                    }
                    continue;
                };
                joins.spawn(async move {
                    let mut ids = Vec::with_capacity(items.len());
                    let mut deliveries = Vec::with_capacity(items.len());
                    for (mark_id, client_id, delivery) in items {
                        ids.push((mark_id, client_id));
                        deliveries.push(delivery);
                    }
                    let results = batcher.submit_many(&address, deliveries).await;
                    Wave::Many(
                        ids.into_iter()
                            .zip(results)
                            .map(|((mark_id, client_id), result)| (mark_id, client_id, result))
                            .collect(),
                    )
                });
            }
            while let Some(joined) = joins.join_next().await {
                let rows = match joined {
                    Ok(Wave::One(mark_id, client_id, result)) => {
                        vec![(mark_id, client_id, result)]
                    }
                    Ok(Wave::Many(rows)) => rows,
                    Err(_) => {
                        note_failure(&mut failure, unavailable());
                        continue;
                    }
                };
                for (mark_id, client_id, result) in rows {
                    match result {
                        Ok(()) => marks.push((mark_id, client_id)),
                        Err(error) => {
                            queues.remove(&client_id);
                            note_failure(&mut failure, error);
                        }
                    }
                }
            }
        }
        if !marks.is_empty() || key.starts_with("h:") {
            let (reply, rx) = oneshot::channel();
            self.send(
                &key,
                Operation::Complete {
                    completed: marks,
                    reply,
                },
            )
            .await?;
            self.await_reply(rx).await?;
        }
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(state.intents.len())
    }
    fn respond<T>(
        &self,
        key: &str,
        cell: &Cell,
        reply: oneshot::Sender<io::Result<T>>,
        result: io::Result<T>,
    ) {
        // Actor-side continuation runs after the quorum attempt even when the
        // network caller has cancelled its reply. Unknown outcomes retain work.
        self.changed(key, cell);
        let _ = reply.send(result);
    }
    #[cfg(test)]
    pub(super) fn metadata_event(update: &MetadataUpdate, old: &Metadata) -> bool {
        match update {
            MetadataUpdate::Progress { .. } => false,
            MetadataUpdate::Snapshot(_) => true,
            MetadataUpdate::Delta {
                durable,
                nodes,
                data_generation,
                excluded,
                repair,
                last_repair,
                ..
            } => {
                !durable.is_empty()
                    || *data_generation != old.data_generation
                    || *excluded != old.excluded
                    || *repair != old.repair
                    || *last_repair != old.last_repair
                    || nodes.iter().any(|(id, next)| {
                        old.nodes.get(id).map(|n| &n.incarnation)
                            != next.as_ref().map(|n| &n.incarnation)
                    })
            }
        }
    }
    fn changed(&self, key: &str, cell: &Cell) {
        if cell.poisoned
            || cell.activate
            || cell.record.as_ref().is_some_and(|r| !r.intents.is_empty())
        {
            self.scheduler.schedule(key);
        }
    }
    pub(super) fn wake(&self) {
        self.scheduler.wake();
    }
    pub(super) fn reconcile(
        &self,
        runtime: &BrokerRuntime,
        old: Option<&Metadata>,
        next: &Metadata,
    ) {
        // Heartbeat progress/lease timestamps do not change routing work. Avoid
        // a directory reconciliation unless ownership or readiness inputs changed.
        if let Some(old) = old {
            let same_nodes = old.nodes.len() == next.nodes.len()
                && old.nodes.iter().all(|(id, node)| {
                    next.nodes
                        .get(id)
                        .is_some_and(|n| n.incarnation == node.incarnation)
                });
            if old.durable == next.durable
                && same_nodes
                && old.data_generation == next.data_generation
                && old.excluded == next.excluded
                && old.repair == next.repair
                && old.last_repair == next.last_repair
            {
                return;
            }
        }
        let generation_changed = old.is_none_or(|old| {
            old.data_generation != next.data_generation
                || old.excluded != next.excluded
                || old.repair != next.repair
                || old.last_repair != next.last_repair
        });
        for (key, owner) in &next.durable {
            let ours =
                owner.node == runtime.config.node_id && owner.incarnation == runtime.incarnation;
            let live = next
                .nodes
                .get(&owner.node)
                .is_some_and(|n| n.incarnation == owner.incarnation);
            let changed = generation_changed
                || old.is_none_or(|old| {
                    old.durable.get(key) != Some(owner)
                        || old.nodes.get(&owner.node).map(|n| &n.incarnation)
                            != next.nodes.get(&owner.node).map(|n| &n.incarnation)
                });
            if changed && (ours || !live) {
                self.scheduler.schedule(key);
            }
        }
    }
    pub(super) fn owner_events(&self, runtime: &BrokerRuntime, keys: &[String]) {
        let metadata = runtime
            .metadata
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        for key in keys {
            if let Some(owner) = metadata.durable.get(key) {
                let ours = owner.node == runtime.config.node_id
                    && owner.incarnation == runtime.incarnation;
                let live = metadata
                    .nodes
                    .get(&owner.node)
                    .is_some_and(|n| n.incarnation == owner.incarnation);
                if ours || !live {
                    self.scheduler.schedule(key);
                }
            }
        }
    }
    pub(super) fn ready_again(&self, runtime: &BrokerRuntime) {
        let metadata = runtime
            .metadata
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        self.reconcile(runtime, None, &metadata);
    }
    async fn drain(self: &Arc<Self>, key: String) -> io::Result<()> {
        // A continuously backlogged key must give older waiting keys a turn.
        // Requeue without sleeping after a finite quantum; the scheduler keeps
        // the key running until finish(), so a second worker cannot overlap.
        let mut remaining = FORWARD_QUANTUM;
        loop {
            let runtime = self.runtime.upgrade().ok_or_else(unavailable)?;
            if runtime.stopped.load(Ordering::Acquire) {
                return Ok(());
            }
            if !runtime.ready() {
                return Err(unavailable());
            }
            let owner = runtime
                .metadata
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .durable
                .get(&key)
                .cloned();
            let Some(owner) = owner else {
                self.send(&key, Operation::ForgetIfDisowned).await?;
                return Ok(());
            };
            if owner.node != runtime.config.node_id || owner.incarnation != runtime.incarnation {
                let live = runtime
                    .metadata
                    .read()
                    .unwrap_or_else(PoisonError::into_inner)
                    .nodes
                    .get(&owner.node)
                    .is_some_and(|n| n.incarnation == owner.incarnation);
                if live {
                    self.send(&key, Operation::ForgetIfDisowned).await?;
                    return Ok(());
                }
                self.recover(&key).await?;
            }
            if remaining == 0 {
                self.scheduler.schedule(&key);
                return Ok(());
            }
            let taken = tokio::time::timeout(WORK_TIMEOUT, self.one(key.clone(), remaining))
                .await
                .map_err(|_| unavailable())??;
            if taken == 0 {
                return Ok(());
            }
            remaining = remaining.saturating_sub(taken);
        }
    }
    pub async fn run(self: Arc<Self>) {
        let mut jobs = tokio::task::JoinSet::new();
        loop {
            let Some(runtime) = self.runtime.upgrade() else {
                return;
            };
            if runtime.stopped.load(Ordering::Acquire) {
                return;
            }
            while jobs.len() < FORWARD_WORKERS {
                let Some(key) = self.scheduler.take() else {
                    break;
                };
                let this = self.clone();
                jobs.spawn(async move {
                    let success = this.drain(key.clone()).await.is_ok();
                    (key, success)
                });
            }
            let deadline = if jobs.len() < FORWARD_WORKERS {
                self.scheduler.deadline()
            } else {
                None
            };
            tokio::select! {
                _=self.scheduler.wake.notified()=>{},
                Some(joined)=jobs.join_next(), if !jobs.is_empty()=>{
                    if let Ok((key,success))=joined && self.scheduler.finish(key,success) {
                        // Overflow is a retained event, reconciled only as slots free.
                        self.ready_again(&runtime);
                    }
                },
                _=async { if let Some(at)=deadline {tokio::time::sleep_until(at).await} else {std::future::pending::<()>().await} }=>{},
            }
        }
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct RecipientDelivery {
    pub client_id: String,
    pub id: DeliveryId,
    pub message: ClusterMessage,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Debug, Default)]
    struct FullQueue(std::sync::atomic::AtomicUsize);
    impl LocalBroker for FullQueue {
        fn deliver_to(
            &self,
            _client: String,
            _id: DeliveryId,
            _message: ClusterMessage,
        ) -> ClusterFuture<'_, ()> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Box::pin(async {
                Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "recipient queue full",
                ))
            })
        }
        fn deliver(&self, _message: ClusterMessage) -> ClusterFuture<'_, ()> {
            Box::pin(async { Err(unavailable()) })
        }
        fn fence(&self, _client: String, _generation: u64) -> ClusterFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }
        fn fence_all(&self) -> ClusterFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }
        fn subscriptions(&self) -> ClusterFuture<'_, Vec<mqtt_cluster_core::ClientSubscriptions>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }
    #[tokio::test]
    async fn independent_mailboxes_do_not_share_a_transaction_quota() -> io::Result<()> {
        let (data, rx, path) = admission_fixture("no-global-quota")?;
        let mut receivers = vec![rx];
        for index in 0..513 {
            let key = format!("c:no-quota-{index}");
            let (tx, rx) = mpsc::channel(8);
            data.actors
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(key.clone(), tx);
            receivers.push(rx);
            let (reply, _) = oneshot::channel();
            data.send(&key, Operation::Work { limit: 1, reply }).await?;
        }
        assert_eq!(
            receivers.iter().map(mpsc::Receiver::len).sum::<usize>(),
            513
        );
        drop(receivers);
        drop(data);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[tokio::test]
    async fn full_mailbox_backpressure_outlives_former_admission_timeout() -> io::Result<()> {
        let (data, mut rx, path) = admission_fixture("no-admission-timeout")?;
        for generation in 0..8 {
            data.release("bounded", generation).await?;
        }
        let mut waiting = Box::pin(data.release("bounded", 8));
        assert!(
            tokio::time::timeout(Duration::from_millis(900), &mut waiting)
                .await
                .is_err(),
            "full mailbox must wait, not reject after750ms"
        );
        assert!(matches!(
            rx.recv().await.expect("first").operation,
            Operation::Release { generation: 0 }
        ));
        waiting.await?;
        for generation in 1..=8 {
            assert!(
                matches!(rx.recv().await.expect("FIFO").operation, Operation::Release {generation:g} if g==generation)
            );
        }
        drop(rx);
        drop(data);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    fn admission_fixture(
        label: &str,
    ) -> io::Result<(Arc<Durable>, mpsc::Receiver<Job>, std::path::PathBuf)> {
        let path = std::env::temp_dir().join(format!(
            "rmqtt-durable-{label}-{}-{}",
            std::process::id(),
            controller::now()
        ));
        let journal = Journal::open(&path, ("bounded-admission", 1), &[])?;
        let data = Arc::new(Durable::new(Weak::new(), journal));
        let (tx, rx) = mpsc::channel::<Job>(8);
        data.actors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(client_key("bounded"), tx);
        Ok((data, rx, path))
    }
    fn admission_mutation(bytes: usize) -> SessionMutation {
        SessionMutation {
            client_id: "bounded".into(),
            epoch: 1,
            expected_revision: 0,
            snapshot: Some(vec![1; bytes]),
            reset_session: false,
            filters: vec![],
            publications: vec![],
            receipt: None,
        }
    }

    #[tokio::test]
    async fn canceled_mailbox_wait_submits_nothing() -> io::Result<()> {
        let (data, mut rx, path) = admission_fixture("cancel-before-send")?;
        for generation in 0..8 {
            data.release("bounded", generation).await?;
        }
        let mut waiting = Box::pin(data.release("bounded", 8));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut waiting)
                .await
                .is_err()
        );
        drop(waiting);
        for generation in 0..8 {
            assert!(
                matches!(rx.recv().await.expect("FIFO").operation, Operation::Release {generation:g} if g==generation)
            );
        }
        assert!(
            rx.try_recv().is_err(),
            "canceled sender must not leave a job"
        );
        drop(rx);
        drop(data);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[tokio::test]
    async fn submitted_work_survives_canceled_reply() -> io::Result<()> {
        let (data, mut rx, path) = admission_fixture("cancel-after-send")?;
        let mut request = Box::pin(data.transaction(admission_mutation(4096)));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut request)
                .await
                .is_err()
        );
        drop(request);
        let job = rx.recv().await.expect("submitted job");
        assert!(matches!(job.operation, Operation::Commit {mutation,reply}
            if reply.is_closed() && mutation.snapshot.as_ref().is_some_and(|body| body.len()==4096)));
        drop(rx);
        drop(data);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[tokio::test]
    async fn work_and_received_use_the_same_fifo_backpressure() -> io::Result<()> {
        let (data, mut rx, path) = admission_fixture("background-backpressure")?;
        for generation in 0..8 {
            data.release("bounded", generation).await?;
        }
        let (reply, _) = oneshot::channel();
        let key = client_key("bounded");
        let mut work = Box::pin(data.send(&key, Operation::Work { limit: 1, reply }));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut work)
                .await
                .is_err()
        );
        assert!(matches!(
            rx.recv().await.expect("first").operation,
            Operation::Release { generation: 0 }
        ));
        work.await?;
        for generation in 1..8 {
            assert!(
                matches!(rx.recv().await.expect("FIFO").operation, Operation::Release {generation:g} if g==generation)
            );
        }
        assert!(matches!(
            rx.recv().await.expect("work").operation,
            Operation::Work { .. }
        ));
        for generation in 0..8 {
            data.release("bounded", generation).await?;
        }
        let id = DeliveryId {
            source: "c:source".into(),
            epoch: 1,
            revision: 1,
            index: 0,
            target_generation: 0,
        };
        let mut received = Box::pin(data.received("bounded", id));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut received)
                .await
                .is_err()
        );
        assert_eq!(rx.len(), 8, "Received waits behind the full mailbox");
        for generation in 0..8 {
            assert!(matches!(rx.recv().await.expect("FIFO").operation,
                Operation::Release {generation:g} if g == generation));
        }
        let consumer = async {
            let job = rx.recv().await.expect("received");
            let Operation::Received { reply, .. } = job.operation else {
                panic!("wrong operation")
            };
            reply.send(Ok(true)).expect("live reply");
        };
        let (result, ()) = tokio::join!(received, consumer);
        assert!(result?);
        drop(rx);
        drop(data);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[tokio::test]
    async fn queued_bodies_do_not_share_an_aggregate_byte_quota() -> io::Result<()> {
        let (data, rx, path) = admission_fixture("no-body-quota")?;
        let mut receivers = vec![rx];
        for index in 0..17 {
            let key = format!("c:body-{index}");
            let (tx, rx) = mpsc::channel(8);
            data.actors
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(key.clone(), tx);
            receivers.push(rx);
            let (reply, _) = oneshot::channel();
            data.send(
                &key,
                Operation::Commit {
                    mutation: admission_mutation(1024 * 1024),
                    reply,
                },
            )
            .await?;
        }
        assert_eq!(receivers.iter().map(mpsc::Receiver::len).sum::<usize>(), 17);
        drop(receivers);
        drop(data);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[tokio::test]
    async fn closed_mailbox_is_an_explicit_lifecycle_error() -> io::Result<()> {
        let (data, rx, path) = admission_fixture("closed-mailbox")?;
        drop(rx);
        let error = data.release("bounded", 1).await.expect_err("closed actor");
        assert!(matches!(
            error
                .get_ref()
                .and_then(|cause| cause.downcast_ref::<Failure>()),
            Some(Failure::ActorClosed)
        ));
        drop(data);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn retry_at_existing_epoch_keeps_physical_revision_and_session_data() {
        let mut record = Record::empty(7);
        record.physical_revision = 9;
        record.revision = 4;
        record.snapshot = Some(vec![1, 2]);
        record.logical_generation = 3;
        let repeated = acquired_record(record.clone(), 7).unwrap();
        assert_eq!(
            repeated.physical_revision, 9,
            "same-epoch retry must not roll durable version backward"
        );
        assert_eq!(repeated.snapshot, record.snapshot);
        assert_eq!(repeated.logical_generation, 3);
        assert_eq!(repeated.revision, 4);
        let advanced = acquired_record(record.clone(), 8).unwrap();
        assert_eq!(advanced.physical_revision, 0);
        assert!(
            acquired_record(record, 6).is_err(),
            "stale acquisition cannot rewrite a newer epoch"
        );
    }

    #[test]
    fn rejected_stale_epoch_does_not_restore_cached_authority() {
        let error = io::Error::new(
            io::ErrorKind::WouldBlock,
            mqtt_cluster_core::AcquisitionConflict,
        );
        assert!(rpc::not_submitted(&error));
        assert!(!acquisition_cache_unchanged(&error));
        assert!(!acquisition_cache_unchanged(&unavailable()));
    }

    #[test]
    fn safe_acquisition_rejection_restores_only_previous_cache_authority() {
        for (previously_poisoned, not_submitted, expected) in [
            (false, true, false),
            (true, true, true),
            (false, false, true),
            (true, false, true),
        ] {
            let mut cell = Cell {
                pending: None,
                data_generation: 1,
                record: Some(Record::empty(7)),
                poisoned: true,
                activate: false,
                activation_token: Arc::new(()),
                forwarding: false,
                stashed: Vec::new(),
            };
            cell.acquisition_failed(previously_poisoned, not_submitted);
            assert_eq!(
                cell.poisoned, expected,
                "safe rejection may restore an authoritative cache; unknown outcomes never do"
            );
            assert_eq!(cell.record.as_ref().map(|r| r.epoch), Some(7));
        }
    }

    #[test]
    fn only_explicit_cache_faults_request_recovery() {
        for reason in [
            RecoveryRequired::Missing,
            RecoveryRequired::Poisoned,
            RecoveryRequired::StaleEpoch,
        ] {
            assert!(recovery_required(&recovery_error(reason)));
        }
        for error in [
            unavailable(),
            diagnosis::reject(Failure::ActorClosed),
            io::Error::new(io::ErrorKind::Interrupted, "request canceled"),
            io::Error::new(io::ErrorKind::TimedOut, "reply lost"),
        ] {
            assert!(
                !recovery_required(&error),
                "transient/unknown RPC errors must not start an epoch"
            );
        }
        assert!(WORK_TIMEOUT > REPLY_TIMEOUT);
    }

    #[test]
    fn repair_generation_invalidates_cached_writer_before_any_operation() {
        let mut cell = Cell {
            pending: None,
            data_generation: 0,
            record: Some(Record::empty(1)),
            poisoned: false,
            activate: true,
            activation_token: Arc::new(()),
            forwarding: false,
            stashed: Vec::new(),
        };
        cell.observe_generation(1);
        assert!(cell.record.is_none() && cell.poisoned && !cell.activate);
        cell.record = Some(Record::empty(5));
        cell.poisoned = false;
        cell.observe_generation(1);
        assert!(cell.record.is_some() && !cell.poisoned);
        cell.observe_generation(2);
        assert!(cell.record.is_none() && cell.poisoned);
    }
    #[test]
    fn discarded_handoff_cache_does_not_clear_a_returned_owner() {
        let mut metadata = Metadata::default();
        let key = client_key("moving");
        let mut record = Record::empty(1);
        record.snapshot = Some(vec![1; 1024]);
        let mut cell = Cell {
            pending: None,
            data_generation: 0,
            record: Some(record),
            poisoned: false,
            activate: true,
            activation_token: Arc::new(()),
            forwarding: false,
            stashed: Vec::new(),
        };
        metadata.durable.insert(
            key.clone(),
            DurableOwner {
                acquired_from: None,
                node: 2,
                incarnation: "other".into(),
                generation: 1,
                epoch: 2,
                logical_generation: 0,
                filters: Vec::new(),
            },
        );
        cell.forget_if_disowned(&metadata, &key, 1, "local");
        assert!(cell.record.is_none());
        assert!(cell.poisoned);
        assert!(!cell.activate);
        cell.record = Some(Record::empty(3));
        cell.poisoned = false;
        let owner = metadata.durable.get_mut(&key).expect("owner");
        owner.node = 1;
        owner.incarnation = "local".into();
        owner.epoch = 3;
        cell.forget_if_disowned(&metadata, &key, 1, "local");
        assert_eq!(cell.record.as_ref().map(|r| r.epoch), Some(3));
        assert!(!cell.poisoned);
    }
    #[tokio::test]
    async fn committed_receipt_bypasses_full_recipient_queue() -> io::Result<()> {
        let path =
            std::env::temp_dir().join(format!("rmqtt-receipt-preflight-{}", std::process::id()));
        let config:crate::Config=serde_json::from_value(serde_json::json!({
            "role":"broker","cluster_id":"preflight","node_id":101,"listen":"127.0.0.1:7101",
            "data_dir":path,"token_file":path.join("token"),
            "controllers":[{"id":1,"address":"http://127.0.0.1:7001"},{"id":2,"address":"http://127.0.0.1:7002"},{"id":3,"address":"http://127.0.0.1:7003"}],
            "durable":{"replicas":[{"id":101,"address":"http://127.0.0.1:7101"},{"id":102,"address":"http://127.0.0.1:7102"},{"id":103,"address":"http://127.0.0.1:7103"}]}
        })).map_err(io::Error::other)?;
        let runtime = tokio::task::spawn_blocking(move || BrokerRuntime::new(config))
            .await
            .map_err(io::Error::other)??;
        let id = DeliveryId {
            source: "c:source".into(),
            epoch: 1,
            revision: 1,
            index: 0,
            target_generation: 0,
        };
        let journal = runtime.journal.as_ref().ok_or_else(unavailable)?.clone();
        let mut record = Record::empty(1);
        record.physical_revision = 1;
        record.receipts.insert(id.clone());
        journal
            .execute(Request::Write {
                key: client_key("recipient"),
                record,
            })
            .await?;
        let local = Arc::new(FullQueue::default());
        *runtime.local.lock().unwrap_or_else(PoisonError::into_inner) = Some(local.clone());
        runtime.initialized.store(true, Ordering::Release);
        *runtime
            .deadline
            .lock()
            .unwrap_or_else(PoisonError::into_inner) =
            tokio::time::Instant::now() + Duration::from_secs(60);
        {
            let mut metadata = runtime
                .metadata
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            metadata.data_mode = Some(DataMode::Memory(
                runtime
                    .config
                    .durable
                    .as_ref()
                    .expect("group")
                    .replicas
                    .iter()
                    .map(|p| (p.id, p.address.clone()))
                    .collect(),
            ));
            metadata.nodes.insert(
                101,
                Node {
                    id: 101,
                    incarnation: runtime.incarnation.clone(),
                    address: "http://127.0.0.1:7101".into(),
                    expires_at: 0,
                    max_lease_ms: 3000,
                    applied: 0,
                },
            );
            metadata.durable.insert(
                client_key("recipient"),
                DurableOwner {
                    acquired_from: None,
                    node: 101,
                    incarnation: runtime.incarnation.clone(),
                    generation: 1,
                    epoch: 1,
                    logical_generation: 0,
                    filters: vec!["events/#".into()],
                },
            );
            *runtime.recipients.write().unwrap() =
                crate::broker::recipients::RecipientIndex::from_metadata(&metadata);
        }
        assert!(!runtime.ready(), "a fresh unanchored disk cannot vote");
        let probe_key = client_key("unanchored-vote");
        assert!(
            quorum::request(
                &runtime,
                runtime.journal.as_ref().ok_or_else(unavailable)?,
                Request::Write {
                    key: probe_key.clone(),
                    record: Record::empty(1)
                }
            )
            .await
            .is_err()
        );
        assert!(
            runtime
                .journal
                .as_ref()
                .ok_or_else(unavailable)?
                .execute(Request::Read { key: probe_key })
                .await?
                .record
                .is_none(),
            "local bypass cannot vote before anchoring"
        );
        tokio::fs::write(
            path.join("token"),
            "data-replica-test-0123456789abcdef0123456789abcdef",
        )
        .await?;
        let rpc = ClientRpc::new(runtime.config.clone()).await?;
        let service = Service {
            control: None,
            broker: Some(runtime.clone()),
            rpc: rpc.clone(),
        };
        let request = rpc.authorize(rpc::encode(&ReplicaMessage {
            generation: 0,
            operation: Request::Read {
                key: client_key("recipient"),
            },
        })?)?;
        assert!(
            crate::rpc::wire::cluster_server::Cluster::replica(&service, request)
                .await
                .is_err(),
            "remote replica RPC cannot vote before anchoring"
        );
        {
            let mut metadata = runtime
                .metadata
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            metadata.disk_ids.insert(
                101,
                runtime
                    .journal
                    .as_ref()
                    .ok_or_else(unavailable)?
                    .disk_id()
                    .to_owned(),
            );
        }
        assert!(runtime.ready());
        {
            let mut metadata = runtime
                .metadata
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            let peers = match metadata.data_mode.take().expect("memory mode") {
                DataMode::Memory(peers) => peers,
                _ => panic!("expected RAM"),
            };
            metadata.data_mode = Some(DataMode::Durable(peers));
        }
        assert!(
            !runtime.ready(),
            "an old controller's SQLite mode never authorizes a RAM vote"
        );
        {
            let mut metadata = runtime
                .metadata
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            let peers = match metadata.data_mode.take().expect("old mode") {
                DataMode::Durable(peers) => peers,
                _ => panic!("expected historical mode"),
            };
            metadata.data_mode = Some(DataMode::Memory(peers));
        }
        assert!(runtime.ready());
        let request = rpc.authorize(rpc::encode(&ReplicaMessage {
            generation: 0,
            operation: Request::Read {
                key: client_key("recipient"),
            },
        })?)?;
        assert!(
            crate::rpc::wire::cluster_server::Cluster::replica(&service, request)
                .await
                .is_ok()
        );
        drop(service);
        // A preflight may race a clean replacement; the replicated commit guard must
        // discard its already-computed old-session proposal even after preflight.
        let mut late = id.clone();
        late.target_generation = 1;
        let mut record = Record::empty(1);
        record.revision = 8;
        record.logical_generation = 1;
        record.snapshot = Some(vec![1]);
        assert!(!record.received_or_obsolete(&late));
        record.logical_generation = 2;
        assert!(record.received_or_obsolete(&late));
        let mut cell = Cell {
            pending: None,
            data_generation: 0,
            record: Some(record),
            poisoned: false,
            activate: false,
            activation_token: Arc::new(()),
            forwarding: false,
            stashed: Vec::new(),
        };
        let result = runtime
            .data
            .as_ref()
            .ok_or_else(unavailable)?
            .commit(
                &runtime,
                &client_key("recipient"),
                SessionMutation {
                    client_id: "recipient".into(),
                    epoch: 1,
                    expected_revision: 8,
                    snapshot: Some(vec![9]),
                    reset_session: false,
                    filters: Vec::new(),
                    publications: Vec::new(),
                    receipt: Some(late),
                },
                &mut cell,
            )
            .await?;
        assert_eq!(
            result,
            SessionCommit {
                revision: 8,
                duplicate: true
            }
        );
        assert_eq!(
            cell.record.as_ref().and_then(|r| r.snapshot.as_deref()),
            Some([1].as_slice())
        );

        // Isolate the already-committed record-cache boundary from MQTT actor capacity.
        // The reply is loaded from the real reopened-format journal, not assumed success.
        let (tx, mut rx) = mpsc::channel::<Job>(1);
        runtime
            .data
            .as_ref()
            .ok_or_else(unavailable)?
            .actors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(client_key("recipient"), tx);
        let cache = tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                if let Operation::Received { id, reply } = job.operation {
                    let result = journal
                        .execute(Request::Read {
                            key: client_key("recipient"),
                        })
                        .await
                        .map(|r| r.record.is_some_and(|r| r.received_or_obsolete(&id)));
                    let _ = reply.send(result);
                }
            }
        });
        let result = runtime
            .deliver_recipient(RecipientDelivery {
                client_id: "recipient".into(),
                id,
                message: ClusterMessage {
                    topic: "events/test".into(),
                    payload: vec![1],
                    qos: 1,
                    retain: false,
                    properties: Default::default(),
                    expires_at: None,
                    publisher: Some("source".into()),
                    observer_client: "source".into(),
                    scope: ForwardScope::Ordinary,
                },
            })
            .await;
        cache.abort();
        let _ = cache.await;
        assert!(
            result.is_ok(),
            "a previously committed delivery cannot require queue capacity again"
        );
        assert_eq!(local.0.load(Ordering::Relaxed), 0);
        drop(runtime);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    fn queued_host(payload: u8) -> Job {
        let (reply, _) = oneshot::channel();
        Job {
            operation: Operation::Host {
                message: ClusterMessage {
                    topic: "events/batch".into(),
                    payload: vec![payload],
                    qos: 0,
                    retain: false,
                    properties: Default::default(),
                    expires_at: None,
                    publisher: None,
                    observer_client: String::new(),
                    scope: ForwardScope::Ordinary,
                },
                reply,
            },
        }
    }

    #[test]
    fn host_mailbox_is_wider_than_a_session_mailbox() {
        assert_eq!(super::mailbox_capacity("h:101:boot"), super::HOST_MAILBOX);
        assert_eq!(super::mailbox_capacity("c:device"), super::SESSION_MAILBOX);
        assert!(super::mailbox_capacity("h:101:boot") > super::mailbox_capacity("c:device"));
    }

    #[tokio::test]
    async fn host_batch_keeps_a_later_operation_in_order() {
        let (tx, mut rx) = mpsc::channel(8);
        let first = queued_host(1);
        tx.try_send(queued_host(2)).expect("queued");
        let (reply, _) = oneshot::channel();
        tx.try_send(Job {
            operation: Operation::Work { limit: 1, reply },
        })
        .expect("queued");
        tx.try_send(queued_host(3)).expect("queued");
        let Job {
            operation: Operation::Host { message, reply },
        } = first
        else {
            panic!("host");
        };
        let (batch, deferred) = super::take_host_batch(message, reply, &mut rx);
        assert_eq!(batch.len(), 2);
        assert!(matches!(
            deferred.expect("deferred").operation,
            Operation::Work { .. }
        ));
        assert!(matches!(
            rx.try_recv().expect("tail").operation,
            Operation::Host { .. }
        ));
    }

    #[tokio::test]
    async fn host_batch_stops_at_the_host_cap() {
        let extra = 16usize;
        let (tx, mut rx) = mpsc::channel(super::HOST_MAILBOX);
        let first = queued_host(0);
        for index in 1..super::HOST_BATCH + extra {
            tx.try_send(queued_host((index % 256) as u8))
                .expect("queued");
        }
        let Job {
            operation: Operation::Host { message, reply },
        } = first
        else {
            panic!("host");
        };
        let (batch, deferred) = super::take_host_batch(message, reply, &mut rx);
        assert_eq!(batch.len(), super::HOST_BATCH);
        assert!(deferred.is_none());
        assert_eq!(rx.len(), extra);
    }
}

#[cfg(test)]
mod event_tests;

#[cfg(test)]
mod recovery_tests;
