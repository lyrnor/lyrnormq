//! Serialized replicated session transactions and restartable forwarding ownership.
use super::*;
use crate::diagnosis::{self, Failure};
use crate::replica::{Intent, Journal, Recipient, Record, Request};
use mqtt_cluster_core::{DeliveryId, SessionCommit, SessionLease, SessionMutation};
use tokio::sync::{mpsc, oneshot};

struct Work {
    intent: Option<Intent>,
    activation: Option<SessionLease>,
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
    },
    Acquire {
        generation: u64,
        expected: u64,
        reply: oneshot::Sender<io::Result<SessionLease>>,
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
        reply: oneshot::Sender<io::Result<Work>>,
    },
    Complete {
        id: DeliveryId,
        recipient: String,
        reply: oneshot::Sender<io::Result<()>>,
    },
}
struct Job {
    operation: Operation,
    _permit: tokio::sync::OwnedSemaphorePermit,
    _bytes: tokio::sync::OwnedSemaphorePermit,
}
pub(super) struct Durable {
    runtime: Weak<BrokerRuntime>,
    journal: Journal,
    actors: Mutex<BTreeMap<String, mpsc::Sender<Job>>>,
    operations: Arc<Semaphore>,
    waiters: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
}
struct Cell {
    data_generation: u64,
    record: Option<Record>,
    poisoned: bool,
    activate: bool,
}
impl Cell {
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
fn lease(key: &str, record: &Record) -> SessionLease {
    SessionLease {
        client_id: key.strip_prefix("c:").unwrap_or(key).to_owned(),
        epoch: record.epoch,
        revision: record.revision,
        snapshot: record.snapshot.clone(),
    }
}
impl Durable {
    pub fn new(runtime: Weak<BrokerRuntime>, journal: Journal) -> Self {
        Self {
            runtime,
            journal,
            actors: Mutex::new(BTreeMap::new()),
            operations: Arc::new(Semaphore::new(32)),
            waiters: Arc::new(Semaphore::new(32)),
            bytes: Arc::new(Semaphore::new(16 * 1024 * 1024)),
        }
    }
    fn actor(self: &Arc<Self>, key: &str) -> io::Result<mpsc::Sender<Job>> {
        let mut actors = self.actors.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(actor) = actors.get(key) {
            return Ok(actor.clone());
        }
        if actors.len() >= 100_000 {
            return Err(unavailable());
        }
        let (tx, mut rx) = mpsc::channel(8);
        actors.insert(key.to_owned(), tx.clone());
        let this = Arc::downgrade(self);
        let key = key.to_owned();
        tokio::spawn(async move {
            let mut cell = Cell {
                data_generation: 0,
                record: None,
                poisoned: false,
                activate: false,
            };
            while let Some(job) = rx.recv().await {
                let Some(this) = this.upgrade() else {
                    break;
                };
                let Some(runtime) = this.runtime.upgrade() else {
                    break;
                };
                if runtime.stopped.load(Ordering::Acquire) {
                    break;
                }
                let generation = runtime
                    .metadata
                    .read()
                    .unwrap_or_else(PoisonError::into_inner)
                    .data_generation;
                cell.observe_generation(generation);
                match job.operation {
                    Operation::ForgetIfDisowned => cell.forget_if_disowned(
                        &runtime
                            .metadata
                            .read()
                            .unwrap_or_else(PoisonError::into_inner),
                        &key,
                        runtime.config.node_id,
                        &runtime.incarnation,
                    ),
                    Operation::Received { id, reply } => {
                        let result = cell
                            .record
                            .as_ref()
                            .filter(|r| {
                                !cell.poisoned && Self::authoritative(&runtime, &key, r.epoch)
                            })
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
                            .get(&key)
                            .is_some_and(|o| {
                                o.node == runtime.config.node_id
                                    && o.incarnation == runtime.incarnation
                                    && o.generation == generation
                            })
                        {
                            cell.activate = true;
                        }
                    }
                    Operation::Activated { epoch, revision } => {
                        if cell
                            .record
                            .as_ref()
                            .is_some_and(|r| r.epoch == epoch && r.revision == revision)
                        {
                            cell.activate = false;
                        }
                    }
                    Operation::Acquire {
                        generation,
                        expected,
                        reply,
                    } => {
                        let _ = reply.send(
                            this.take(&runtime, &key, generation, expected, &mut cell)
                                .await,
                        );
                    }
                    Operation::Commit { mutation, reply } => {
                        let _ = reply.send(this.commit(&runtime, &key, mutation, &mut cell).await);
                    }
                    Operation::Host { message, reply } => {
                        let _ = reply.send(this.host(&runtime, &key, message, &mut cell).await);
                    }
                    Operation::Work { reply } => {
                        let _ = reply.send(this.work(&runtime, &key, &cell));
                    }
                    Operation::Complete {
                        id,
                        recipient,
                        reply,
                    } => {
                        let _ = reply.send(
                            this.complete(&runtime, &key, &id, recipient, &mut cell)
                                .await,
                        );
                    }
                }
            }
        });
        Ok(tx)
    }
    fn operation_bytes(operation: &Operation) -> io::Result<u32> {
        let size = match operation {
            Operation::Commit { mutation, .. } => mutation
                .snapshot
                .as_ref()
                .map_or(0, Vec::len)
                .saturating_add(
                    mutation
                        .publications
                        .iter()
                        .map(|p| p.payload.len())
                        .sum::<usize>(),
                ),
            Operation::Host { message, .. } => message.payload.len(),
            _ => 0,
        };
        u32::try_from(size).map_err(|_| unavailable())
    }

    fn send(self: &Arc<Self>, key: &str, operation: Operation) -> io::Result<()> {
        let bytes = Self::operation_bytes(&operation)?;
        let permit = self
            .operations
            .clone()
            .try_acquire_owned()
            .map_err(|_| diagnosis::reject(Failure::BackgroundOperations))?;
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| diagnosis::reject(Failure::DurableBytes))?;
        self.actor(key)?
            .try_send(Job {
                operation,
                _permit: permit,
                _bytes: bytes,
            })
            .map_err(|error| {
                diagnosis::reject(match error {
                    mpsc::error::TrySendError::Full(_) => Failure::BackgroundActorQueue,
                    mpsc::error::TrySendError::Closed(_) => Failure::ActorClosed,
                })
            })
    }
    async fn send_wait(self: &Arc<Self>, key: &str, operation: Operation) -> io::Result<()> {
        // Charge the same16MiB body budget for both active and waiting work.
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(Self::operation_bytes(&operation)?)
            .map_err(|_| diagnosis::reject(Failure::DurableBytes))?;
        let actor = self.actor(key)?;
        // Never occupy global execution admission while waiting for an actor's
        // bounded eight-entry channel. The fast path submits without awaiting.
        match actor.clone().try_reserve_owned() {
            Ok(channel) => match self.operations.clone().try_acquire_owned() {
                Ok(active) => {
                    channel.send(Job {
                        operation,
                        _permit: active,
                        _bytes: bytes,
                    });
                    return Ok(());
                }
                Err(tokio::sync::TryAcquireError::Closed) => {
                    return Err(diagnosis::reject(Failure::DurableOperations));
                }
                Err(tokio::sync::TryAcquireError::NoPermits) => drop(channel),
            },
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(diagnosis::reject(Failure::ActorClosed));
            }
            Err(mpsc::error::TrySendError::Full(_)) => diagnosis::observe(Failure::ActorWaiting),
        }
        let waiting = self
            .waiters
            .clone()
            .try_acquire_owned()
            .map_err(|_| diagnosis::reject(Failure::DurableWaitQueueFull))?;
        diagnosis::observe(Failure::DurableWaiting);
        let (channel, active) = tokio::time::timeout(Duration::from_millis(750), async {
            let channel = actor
                .reserve_owned()
                .await
                .map_err(|_| diagnosis::reject(Failure::ActorClosed))?;
            let active = self
                .operations
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| diagnosis::reject(Failure::DurableOperations))?;
            Ok::<_, io::Error>((channel, active))
        })
        .await
        .map_err(|_| diagnosis::reject(Failure::DurableWaitTimeout))??;
        drop(waiting);
        channel.send(Job {
            operation,
            _permit: active,
            _bytes: bytes,
        });
        Ok(())
    }
    pub fn forget(self: &Arc<Self>, key: &str) {
        // Keep the channel/serialization identity; discard only obsolete payload cache.
        let present = self
            .actors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(key);
        if present {
            let _ = self.send(key, Operation::ForgetIfDisowned);
        }
    }
    async fn await_reply<T>(&self, rx: oneshot::Receiver<io::Result<T>>) -> io::Result<T> {
        tokio::time::timeout(Duration::from_secs(8), rx)
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())?
    }
    pub async fn release(self: &Arc<Self>, client: &str, generation: u64) -> io::Result<()> {
        self.send_wait(&client_key(client), Operation::Release { generation })
            .await
    }
    pub async fn acquire(
        self: &Arc<Self>,
        key: String,
        generation: u64,
        expected: u64,
    ) -> io::Result<SessionLease> {
        let (reply, rx) = oneshot::channel();
        self.send_wait(
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
        self.send_wait(&key, Operation::Commit { mutation, reply })
            .await?;
        self.await_reply(rx).await
    }
    pub async fn enqueue(self: &Arc<Self>, message: ClusterMessage) -> io::Result<()> {
        let runtime = self.runtime.upgrade().ok_or_else(unavailable)?;
        let key = format!("h:{}:{}", runtime.config.node_id, runtime.incarnation);
        let (reply, rx) = oneshot::channel();
        self.send_wait(&key, Operation::Host { message, reply })
            .await?;
        self.await_reply(rx).await
    }
    pub async fn received(self: &Arc<Self>, client: &str, id: DeliveryId) -> io::Result<bool> {
        let (reply, rx) = oneshot::channel();
        self.send_wait(&client_key(client), Operation::Received { id, reply })
            .await?;
        self.await_reply(rx).await
    }
    pub async fn inspect(&self, runtime: &BrokerRuntime, client: &str) -> io::Result<SessionLease> {
        if !runtime.ready() {
            return Err(unavailable());
        }
        let key = client_key(client);
        let metadata = runtime.rpc().await?.metadata().await?;
        let epoch = metadata.durable.get(&key).map_or(0, |o| o.epoch);
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
        if !runtime.ready() {
            return Err(unavailable());
        }
        let answer = runtime
            .rpc()
            .await?
            .mutate(Command::Acquire {
                key: key.to_owned(),
                node: runtime.config.node_id,
                incarnation: runtime.incarnation.clone(),
                generation,
                expected_epoch: expected,
            })
            .await?;
        // Acquisition is ambiguous until all following steps complete; no older cached write may resume.
        cell.poisoned = true;
        runtime.barrier(answer.revision).await?;
        let old = quorum::request(
            runtime,
            &self.journal,
            Request::Fence {
                key: key.to_owned(),
                epoch: answer.revision,
            },
        )
        .await?;
        let mut record = old.unwrap_or_else(|| Record::empty(answer.revision));
        record.epoch = answer.revision;
        record.physical_revision = 0;
        quorum::request(
            runtime,
            &self.journal,
            Request::Write {
                key: key.to_owned(),
                record: record.clone(),
            },
        )
        .await?;
        if !Self::authoritative(runtime, key, record.epoch) {
            return Err(diagnosis::reject(Failure::TakeAuthority));
        }
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
        let lease = lease(key, &record);
        cell.record = Some(record);
        cell.poisoned = false;
        cell.activate = generation == 0;
        Ok(lease)
    }
    fn recipients(runtime: &BrokerRuntime, message: &ClusterMessage) -> Vec<Recipient> {
        runtime
            .metadata
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .durable
            .iter()
            .filter(|(key, owner)| {
                key.starts_with("c:")
                    && owner
                        .filters
                        .iter()
                        .any(|filter| mqtt_router::matches_topic_filter(filter, &message.topic))
            })
            .filter_map(|(key, owner)| {
                key.strip_prefix("c:").map(|id| Recipient {
                    client_id: id.to_owned(),
                    generation: owner.logical_generation,
                })
            })
            .collect()
    }
    fn append(
        runtime: &BrokerRuntime,
        key: &str,
        record: &mut Record,
        publications: Vec<ClusterMessage>,
    ) -> io::Result<()> {
        for (index, message) in publications.into_iter().enumerate() {
            if message.retain
                || message.qos > 2
                || message.scope != ForwardScope::Ordinary
                || message.payload.len() > 1024 * 1024
            {
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
        if record.intents.len() > 256 || record.receipts.len() > 8192 {
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
    async fn host(
        &self,
        runtime: &BrokerRuntime,
        key: &str,
        message: ClusterMessage,
        cell: &mut Cell,
    ) -> io::Result<()> {
        if cell.poisoned
            || cell
                .record
                .as_ref()
                .is_none_or(|r| !Self::authoritative(runtime, key, r.epoch))
        {
            let expected = runtime
                .metadata
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .durable
                .get(key)
                .map_or(0, |o| o.epoch);
            // This runs inside the one record actor. Do not enqueue another Acquire to self.
            self.take(runtime, key, 0, expected, cell).await?;
        }
        let mut next = cell.record.clone().ok_or_else(unavailable)?;
        next.revision = next.revision.checked_add(1).ok_or_else(unavailable)?;
        Self::append(runtime, key, &mut next, vec![message])?;
        self.store(runtime, key, next, cell).await
    }
    fn work(&self, runtime: &BrokerRuntime, key: &str, cell: &Cell) -> io::Result<Work> {
        let current = cell.record.as_ref().ok_or_else(unavailable)?;
        if cell.poisoned || !Self::authoritative(runtime, key, current.epoch) {
            return Err(unavailable());
        }
        Ok(Work {
            intent: current.intents.first().cloned(),
            activation: (cell.activate && key.starts_with("c:")).then(|| lease(key, current)),
        })
    }
    async fn complete(
        &self,
        runtime: &BrokerRuntime,
        key: &str,
        id: &DeliveryId,
        recipient: String,
        cell: &mut Cell,
    ) -> io::Result<()> {
        let mut next = cell.record.clone().ok_or_else(unavailable)?;
        let Some(intent) = next.intents.iter_mut().find(|intent| &intent.id == id) else {
            return Ok(());
        };
        intent.completed.insert(recipient);
        next.intents.retain(|i| {
            i.recipients
                .iter()
                .any(|r| !i.completed.contains(&r.client_id))
        });
        self.store(runtime, key, next, cell).await
    }
    async fn one(self: &Arc<Self>, key: String) -> io::Result<()> {
        let runtime = self.runtime.upgrade().ok_or_else(unavailable)?;
        let (reply, rx) = oneshot::channel();
        self.send(&key, Operation::Work { reply })?;
        let outcome = self.await_reply(rx).await;
        let state = match outcome {
            Ok(state) => state,
            Err(_) => {
                diagnosis::observe(Failure::BackgroundWorkReply);
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
                let recovered = self.acquire(key.clone(), 0, owner.epoch).await?;
                if key.starts_with("c:") {
                    runtime.local()?.activate_session(recovered).await?;
                }
                return Ok(());
            }
        };
        if let Some(activation) = state.activation {
            let epoch = activation.epoch;
            let revision = activation.revision;
            runtime.local()?.activate_session(activation).await?;
            self.send_wait(&key, Operation::Activated { epoch, revision })
                .await?;
        }
        let Some(intent) = state.intent else {
            return Ok(());
        };
        for recipient in &intent.recipients {
            let target_generation = recipient.generation;
            let recipient = &recipient.client_id;
            let mut delivery_id = intent.id.clone();
            delivery_id.target_generation = target_generation;
            if intent.completed.contains(recipient) {
                continue;
            }
            if intent
                .message
                .expires_at
                .is_none_or(|at| at > controller::now())
            {
                let owner = runtime
                    .metadata
                    .read()
                    .unwrap_or_else(PoisonError::into_inner)
                    .durable
                    .get(&client_key(recipient))
                    .cloned()
                    .ok_or_else(unavailable)?;
                if owner.node == runtime.config.node_id && owner.incarnation == runtime.incarnation
                {
                    runtime
                        .deliver_recipient(RecipientDelivery {
                            client_id: recipient.clone(),
                            id: delivery_id.clone(),
                            message: intent.message.clone(),
                        })
                        .await?;
                } else {
                    let address = runtime
                        .config
                        .durable
                        .as_ref()
                        .and_then(|d| d.replicas.iter().find(|p| p.id == owner.node))
                        .map(|p| p.address.clone())
                        .ok_or_else(unavailable)?;
                    let rpc = runtime.rpc().await?;
                    let mut client = rpc.client(&address).await?;
                    client
                        .deliver_to(rpc.authorize(rpc::encode(&RecipientDelivery {
                            client_id: recipient.clone(),
                            id: delivery_id.clone(),
                            message: intent.message.clone(),
                        })?)?)
                        .await
                        .map_err(rpc::io_error)?;
                }
            }
            let (reply, rx) = oneshot::channel();
            self.send_wait(
                &key,
                Operation::Complete {
                    id: intent.id.clone(),
                    recipient: recipient.clone(),
                    reply,
                },
            )
            .await?;
            self.await_reply(rx).await?;
        }
        Ok(())
    }
    pub async fn run(self: Arc<Self>) {
        loop {
            let Some(runtime) = self.runtime.upgrade() else {
                return;
            };
            if runtime.stopped.load(Ordering::Acquire) {
                return;
            }
            let directory = runtime
                .metadata
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .durable
                .clone();
            if runtime.ready() {
                let mut jobs = tokio::task::JoinSet::new();
                for (key, owner) in directory {
                    if owner.node != runtime.config.node_id
                        || owner.incarnation != runtime.incarnation
                    {
                        self.forget(&key);
                    }
                    if jobs.len() >= 16 {
                        let _ = jobs.join_next().await;
                    }
                    let this = self.clone();
                    let runtime = runtime.clone();
                    jobs.spawn(async move {
                        let ours = owner.node == runtime.config.node_id
                            && owner.incarnation == runtime.incarnation;
                        let live = runtime
                            .metadata
                            .read()
                            .unwrap_or_else(PoisonError::into_inner)
                            .nodes
                            .get(&owner.node)
                            .is_some_and(|n| n.incarnation == owner.incarnation);
                        if !ours && !live {
                            let _ = this.acquire(key.clone(), 0, owner.epoch).await;
                        }
                        if ours || !live {
                            let _ =
                                tokio::time::timeout(Duration::from_secs(6), this.one(key)).await;
                        }
                    });
                }
                while jobs.join_next().await.is_some() {}
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
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
    async fn business_admission_waits_for_existing_operation_slot() -> io::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "rmqtt-durable-admission-{}-{}",
            std::process::id(),
            controller::now()
        ));
        let journal = Journal::open(&path, ("bounded-admission", 1), &[])?;
        let data = Arc::new(Durable::new(Weak::new(), journal));
        let (tx, mut rx) = mpsc::channel::<Job>(8);
        data.actors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(client_key("bounded"), tx);
        let held = data
            .operations
            .clone()
            .acquire_many_owned(32)
            .await
            .map_err(io::Error::other)?;
        let mutation = SessionMutation {
            client_id: "bounded".into(),
            epoch: 1,
            expected_revision: 0,
            snapshot: Some(vec![1]),
            reset_session: false,
            filters: vec![],
            publications: vec![],
            receipt: None,
        };
        let mut request = Box::pin(data.transaction(mutation));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut request)
                .await
                .is_err(),
            "a legitimate business operation must wait rather than fail-fast at transient global admission saturation"
        );
        assert!(rx.try_recv().is_err());
        drop(held);
        let consumer = async {
            let job = rx.recv().await.expect("queued operation");
            if let Operation::Commit { reply, .. } = job.operation {
                let _ = reply.send(Ok(SessionCommit {
                    revision: 1,
                    duplicate: false,
                }));
            } else {
                panic!("wrong operation");
            }
        };
        let (result, ()) = tokio::join!(request, consumer);
        assert_eq!(result?.revision, 1);
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
    async fn business_waits_are_finite_and_background_does_not_take_wait_budget() -> io::Result<()>
    {
        let (data, mut rx, path) = admission_fixture("finite-waits")?;
        let active = data
            .operations
            .clone()
            .acquire_many_owned(32)
            .await
            .map_err(io::Error::other)?;
        let waiting = data
            .waiters
            .clone()
            .acquire_many_owned(32)
            .await
            .map_err(io::Error::other)?;
        let full = data
            .transaction(admission_mutation(1))
            .await
            .expect_err("full waiting budget");
        assert!(matches!(
            full.get_ref().and_then(|e| e.downcast_ref::<Failure>()),
            Some(Failure::DurableWaitQueueFull)
        ));
        let (reply, _) = oneshot::channel();
        let skipped = data
            .send(&client_key("bounded"), Operation::Work { reply })
            .expect_err("background skips");
        assert!(matches!(
            skipped.get_ref().and_then(|e| e.downcast_ref::<Failure>()),
            Some(Failure::BackgroundOperations)
        ));
        assert_eq!(data.waiters.available_permits(), 0);
        drop(waiting);
        let expired = tokio::time::timeout(
            Duration::from_secs(2),
            data.transaction(admission_mutation(1)),
        )
        .await
        .map_err(io::Error::other)?
        .expect_err("finite deadline");
        assert!(matches!(
            expired.get_ref().and_then(|e| e.downcast_ref::<Failure>()),
            Some(Failure::DurableWaitTimeout)
        ));
        assert_eq!(data.waiters.available_permits(), 32);
        assert_eq!(data.operations.available_permits(), 0);
        assert_eq!(data.bytes.available_permits(), 16 * 1024 * 1024);
        assert!(rx.try_recv().is_err());
        drop(active);
        drop(rx);
        drop(data);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[tokio::test]
    async fn canceled_business_waits_release_body_channel_and_wait_admission() -> io::Result<()> {
        let (data, mut rx, path) = admission_fixture("canceled-wait")?;
        let active = data
            .operations
            .clone()
            .acquire_many_owned(32)
            .await
            .map_err(io::Error::other)?;
        let mut request = Box::pin(data.transaction(admission_mutation(2 * 1024 * 1024)));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut request)
                .await
                .is_err()
        );
        assert_eq!(data.bytes.available_permits(), 14 * 1024 * 1024);
        assert_eq!(data.waiters.available_permits(), 31);
        assert!(rx.try_recv().is_err());
        drop(request);
        assert_eq!(data.bytes.available_permits(), 16 * 1024 * 1024);
        assert_eq!(data.waiters.available_permits(), 32);
        assert!(rx.try_recv().is_err());
        drop(active);
        drop(rx);
        drop(data);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[tokio::test]
    async fn full_actor_channel_wait_does_not_hold_global_admission() -> io::Result<()> {
        let (data, mut rx, path) = admission_fixture("actor-wait")?;
        for _ in 0..8 {
            let (reply, _) = oneshot::channel();
            data.send(&client_key("bounded"), Operation::Work { reply })?;
        }
        assert_eq!(data.operations.available_permits(), 24);
        let mut request = Box::pin(data.release("bounded", 1));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut request)
                .await
                .is_err()
        );
        assert_eq!(
            data.operations.available_permits(),
            24,
            "full actor cannot monopolize global permits"
        );
        assert_eq!(data.waiters.available_permits(), 31);
        drop(rx.recv().await.expect("queued peek"));
        request.await?;
        assert_eq!(rx.len(), 8, "existing channel capacity unchanged");
        drop(rx);
        drop(data);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[tokio::test]
    async fn active_and_waiting_bodies_keep_the_original_sixteen_mib_budget() -> io::Result<()> {
        let (data, rx, path) = admission_fixture("body-bound")?;
        let bytes = data
            .bytes
            .clone()
            .acquire_many_owned(16 * 1024 * 1024)
            .await
            .map_err(io::Error::other)?;
        let rejected = data
            .transaction(admission_mutation(1))
            .await
            .expect_err("body budget exhausted");
        assert!(matches!(
            rejected.get_ref().and_then(|e| e.downcast_ref::<Failure>()),
            Some(Failure::DurableBytes)
        ));
        assert_eq!(data.waiters.available_permits(), 32);
        assert_eq!(data.operations.available_permits(), 32);
        drop(bytes);
        drop(rx);
        drop(data);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn repair_generation_invalidates_cached_writer_before_any_operation() {
        let mut cell = Cell {
            data_generation: 0,
            record: Some(Record::empty(1)),
            poisoned: false,
            activate: true,
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
            data_generation: 0,
            record: Some(record),
            poisoned: false,
            activate: true,
        };
        metadata.durable.insert(
            key.clone(),
            DurableOwner {
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
                    node: 101,
                    incarnation: runtime.incarnation.clone(),
                    generation: 1,
                    epoch: 1,
                    logical_generation: 0,
                    filters: vec!["events/#".into()],
                },
            );
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
            data_generation: 0,
            record: Some(record),
            poisoned: false,
            activate: false,
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
}
