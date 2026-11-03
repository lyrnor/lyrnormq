//! Serialized replicated session transactions and restartable forwarding ownership.
use super::*;
use crate::replica::{Intent, Journal, Record, Request};
use mqtt_cluster_core::{DeliveryId, SessionCommit, SessionLease, SessionMutation};
use tokio::sync::{mpsc, oneshot};

struct Work {
    intent: Option<Intent>,
    activation: Option<SessionLease>,
}
enum Operation {
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
    bytes: Arc<Semaphore>,
}
struct Cell {
    record: Option<Record>,
    poisoned: bool,
    activate: bool,
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
                match job.operation {
                    Operation::Received { id, reply } => {
                        let result = cell
                            .record
                            .as_ref()
                            .filter(|r| {
                                !cell.poisoned && Self::authoritative(&runtime, &key, r.epoch)
                            })
                            .map(|r| r.receipts.contains(&id))
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
    fn send(self: &Arc<Self>, key: &str, operation: Operation) -> io::Result<()> {
        let size = match &operation {
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
        let bytes = u32::try_from(size).map_err(|_| unavailable())?;
        let permit = self
            .operations
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable())?;
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| unavailable())?;
        self.actor(key)?
            .try_send(Job {
                operation,
                _permit: permit,
                _bytes: bytes,
            })
            .map_err(|_| unavailable())
    }
    async fn await_reply<T>(&self, rx: oneshot::Receiver<io::Result<T>>) -> io::Result<T> {
        tokio::time::timeout(Duration::from_secs(8), rx)
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())?
    }
    pub fn release(self: &Arc<Self>, client: &str, generation: u64) -> io::Result<()> {
        self.send(&client_key(client), Operation::Release { generation })
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
        )?;
        self.await_reply(rx).await
    }
    pub async fn transaction(
        self: &Arc<Self>,
        mutation: SessionMutation,
    ) -> io::Result<SessionCommit> {
        let key = client_key(&mutation.client_id);
        let (reply, rx) = oneshot::channel();
        self.send(&key, Operation::Commit { mutation, reply })?;
        self.await_reply(rx).await
    }
    pub async fn enqueue(self: &Arc<Self>, message: ClusterMessage) -> io::Result<()> {
        let runtime = self.runtime.upgrade().ok_or_else(unavailable)?;
        let key = format!("h:{}:{}", runtime.config.node_id, runtime.incarnation);
        let (reply, rx) = oneshot::channel();
        self.send(&key, Operation::Host { message, reply })?;
        self.await_reply(rx).await
    }
    pub async fn received(self: &Arc<Self>, client: &str, id: DeliveryId) -> io::Result<bool> {
        let (reply, rx) = oneshot::channel();
        self.send(&client_key(client), Operation::Received { id, reply })?;
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
            return Err(unavailable());
        }
        let filters_differ = runtime
            .metadata
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .durable
            .get(key)
            .is_none_or(|owner| owner.filters != record.filters);
        if filters_differ {
            runtime
                .command(
                    Command::DurableFilters {
                        key: key.to_owned(),
                        node: runtime.config.node_id,
                        incarnation: runtime.incarnation.clone(),
                        epoch: record.epoch,
                        filters: record.filters.clone(),
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
    fn recipients(runtime: &BrokerRuntime, message: &ClusterMessage) -> Vec<String> {
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
            .filter_map(|(key, _)| key.strip_prefix("c:").map(str::to_owned))
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
            return Err(unavailable());
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
            return Err(unavailable());
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
        let current = cell.record.as_ref().ok_or_else(unavailable)?;
        if cell.poisoned
            || mutation.epoch != current.epoch
            || !Self::authoritative(runtime, key, current.epoch)
        {
            return Err(unavailable());
        }
        if mutation
            .receipt
            .as_ref()
            .is_some_and(|id| current.receipts.contains(id))
        {
            return Ok(SessionCommit {
                revision: current.revision,
                duplicate: true,
            });
        }
        if mutation.expected_revision != current.revision {
            return Err(unavailable());
        }
        let mut next = current.clone();
        next.revision = next.revision.checked_add(1).ok_or_else(unavailable)?;
        next.snapshot = mutation.snapshot;
        next.filters = mutation.filters;
        if let Some(id) = mutation.receipt {
            next.receipts.insert(id);
        }
        Self::append(runtime, key, &mut next, mutation.publications)?;
        let filters_changed = current.filters != next.filters;
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
        next.intents
            .retain(|i| i.recipients.iter().any(|r| !i.completed.contains(r)));
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
            self.send(&key, Operation::Activated { epoch, revision })?;
        }
        let Some(intent) = state.intent else {
            return Ok(());
        };
        for recipient in &intent.recipients {
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
                            id: intent.id.clone(),
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
                            id: intent.id.clone(),
                            message: intent.message.clone(),
                        })?)?)
                        .await
                        .map_err(rpc::io_error)?;
                }
            }
            let (reply, rx) = oneshot::channel();
            self.send(
                &key,
                Operation::Complete {
                    id: intent.id.clone(),
                    recipient: recipient.clone(),
                    reply,
                },
            )?;
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
        let request = rpc.authorize(rpc::encode(&Request::Read {
            key: client_key("recipient"),
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
        let request = rpc.authorize(rpc::encode(&Request::Read {
            key: client_key("recipient"),
        })?)?;
        assert!(
            crate::rpc::wire::cluster_server::Cluster::replica(&service, request)
                .await
                .is_ok()
        );
        drop(service);
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
                        .map(|r| r.record.is_some_and(|r| r.receipts.contains(&id)));
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
