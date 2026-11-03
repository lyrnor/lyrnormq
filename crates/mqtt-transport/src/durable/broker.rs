//! Shared broker state for per-session actors (Phase4): client registry,
//! subscription index, global retained store, storage and publication fan-out.
use super::actor::{self, ActorCommand, ActorHandle};
use super::*;
use mqtt_router::{ConcurrentRetained, SharedMatch, SubscriptionIndex};
use mqtt_session::durable::{
    Clock, DeliveryScope, Publication, RetainedEntry, decode_retained_row, encode_retained_row,
};
use mqtt_storage::durable::{
    Contents, DurableError, RowChange, RowLimits, RowStore, Rows, StorageService,
};
use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{
    Mutex, PoisonError,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use tokio::sync::OwnedSemaphorePermit;

const SHARDS: usize = 64;
const STORAGE_QUEUE: usize = 4096;
const STORAGE_BATCH: usize = 1024;

#[derive(Clone, Debug)]
pub(super) struct RetainedMeta {
    pub(super) properties: Properties,
    pub(super) expires_at: Option<u64>,
}

type CommitOutcome = Result<(), Arc<DurableError>>;

/// Group-committing storage with async admission: a permit is held from submit
/// until completion, so the service queue never reports Full.
struct Storage {
    service: Mutex<Option<StorageService>>,
    permits: Arc<Semaphore>,
}
impl Storage {
    async fn reserve(&self) -> io::Result<OwnedSemaphorePermit> {
        Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| failure(ReasonCode::SERVER_UNAVAILABLE, "storage stopped"))
    }
    fn submit(
        &self,
        permit: OwnedSemaphorePermit,
        changes: Vec<RowChange>,
    ) -> io::Result<oneshot::Receiver<CommitOutcome>> {
        let (sender, receiver) = oneshot::channel();
        let service = self.service.lock().unwrap_or_else(PoisonError::into_inner);
        let service = service
            .as_ref()
            .ok_or_else(|| failure(ReasonCode::SERVER_UNAVAILABLE, "storage stopped"))?;
        service
            .try_submit(
                changes,
                Box::new(move |outcome| {
                    drop(permit);
                    let _ = sender.send(outcome);
                }),
            )
            .map_err(|error| failure(ReasonCode::SERVER_UNAVAILABLE, error.to_string()))?;
        Ok(receiver)
    }
}

struct Registry {
    shards: Vec<Mutex<HashMap<String, ActorHandle>>>,
}
impl Registry {
    fn new() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
        }
    }
    fn shard(&self, client: &str) -> std::sync::MutexGuard<'_, HashMap<String, ActorHandle>> {
        let mut hasher = DefaultHasher::new();
        client.hash(&mut hasher);
        let index = (hasher.finish() as usize) % SHARDS;
        self.shards[index]
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

pub(super) struct Broker {
    pub(super) config: BrokerConfig,
    pub(super) admission: crate::admission::Admission,
    pub(super) session_config: SessionConfig,
    pub(super) clock: Arc<dyn Clock>,
    pub(super) events: system_events::Events,
    pub(super) cluster_state: cluster::State,
    registry: Registry,
    actors: AtomicUsize,
    next_actor: AtomicU64,
    pub(super) index: SubscriptionIndex,
    pub(super) retained: ConcurrentRetained<RetainedMeta>,
    retained_order: Mutex<()>,
    storage: Option<Storage>,
    state_bytes: AtomicUsize,
    pub(super) fatal: watch::Sender<Option<String>>,
    alive: Mutex<Option<mpsc::Sender<()>>>,
    finished: tokio::sync::Mutex<mpsc::Receiver<()>>,
    handoffs: Mutex<Option<tokio::task::JoinSet<()>>>,
}

/// Restored partitions and retained entries produced on a blocking thread.
struct Restored {
    store: Option<RowStore>,
    partitions: Vec<(String, SessionEngine, Option<Vec<u8>>)>,
    retained: Vec<RetainedEntry>,
    /// Committed by the restore rows; dispatched once every actor exists.
    publications: Vec<Publication>,
}

fn storage_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

fn row_limits(config: &BrokerConfig) -> RowLimits {
    RowLimits {
        max_row_bytes: config.max_state_bytes,
        max_sessions: config.max_sessions,
        max_retained: config.max_retained,
        max_legacy_bytes: config.max_state_bytes,
    }
}

/// Open or migrate storage and restore every partition. Blocking. A `seed`
/// replaces container1 contents (tests inject a prepared legacy engine).
fn restore(
    config: &BrokerConfig,
    clock: Arc<dyn Clock>,
    seed: Option<SessionEngine>,
) -> io::Result<Restored> {
    let session_config = session_config(config);
    let access = Arc::clone(&config.access_control);
    let mut store = config
        .storage_path
        .as_ref()
        .map(|path| RowStore::open(path, row_limits(config)))
        .transpose()
        .map_err(storage_error)?;
    let mut partitions = Vec::new();
    let mut retained = Vec::new();
    let seeded = seed.is_some();
    let contents = match seed {
        Some(engine) => Some((Contents::Legacy(None), Some(engine))),
        None => store
            .as_ref()
            .map(RowStore::contents)
            .transpose()
            .map_err(storage_error)?
            .map(|contents| (contents, None)),
    };
    match contents {
        None => {}
        Some((Contents::Legacy(snapshot), seed)) => {
            let engine = match (seed, snapshot) {
                (Some(engine), _) => Ok(engine),
                (None, Some(bytes)) => SessionEngine::restore_with_clock(
                    session_config,
                    &bytes,
                    Arc::clone(&access),
                    Arc::clone(&clock),
                ),
                (None, None) => SessionEngine::new_with_clock(
                    session_config,
                    Arc::clone(&access),
                    Arc::clone(&clock),
                ),
            }
            .map_err(io::Error::other)?;
            let split = engine.into_partitions().map_err(io::Error::other)?;
            let mut rows = Rows::default();
            for (client, engine) in split.clients {
                let row = if engine.has_persistent_state() {
                    let bytes = engine.snapshot().map_err(io::Error::other)?;
                    rows.sessions.push((client.clone(), bytes.clone()));
                    Some(bytes)
                } else {
                    None
                };
                partitions.push((client, engine, row));
            }
            for entry in split.retained {
                rows.retained.push((
                    entry.topic.clone(),
                    encode_retained_row(session_config, entry.clone()).map_err(io::Error::other)?,
                ));
                retained.push(entry);
            }
            if let Some(store) = &mut store {
                if store.is_legacy() {
                    store.migrate(&rows).map_err(storage_error)?;
                } else {
                    let changes: Vec<RowChange> = rows
                        .sessions
                        .into_iter()
                        .map(|(client, bytes)| RowChange::PutSession(client, bytes))
                        .chain(
                            rows.retained
                                .into_iter()
                                .map(|(topic, bytes)| RowChange::PutRetained(topic, bytes)),
                        )
                        .collect();
                    store.apply(&changes).map_err(storage_error)?;
                }
            }
        }
        Some((Contents::Rows(rows), _)) => {
            for (client, bytes) in rows.sessions {
                let engine = SessionEngine::restore_partition(
                    session_config,
                    &bytes,
                    Arc::clone(&access),
                    Arc::clone(&clock),
                )
                .map_err(io::Error::other)?;
                partitions.push((client, engine, Some(bytes)));
            }
            for (_, bytes) in rows.retained {
                retained
                    .push(decode_retained_row(session_config, &bytes).map_err(io::Error::other)?);
            }
        }
    }
    let floor = partitions
        .iter()
        .map(|(_, engine, _)| engine.clock_floor())
        .max()
        .unwrap_or(0);
    let mut changes = Vec::new();
    let mut recovered = Vec::with_capacity(partitions.len());
    let mut publications = Vec::new();
    for (client, mut engine, row) in partitions {
        engine.observe_clock_floor(floor);
        if !seeded {
            engine.expire_sessions().map_err(model_error)?;
            engine.reconcile_authorization().map_err(model_error)?;
            engine.recover_wills().map_err(io::Error::other)?;
        }
        publications.extend(engine.take_publications());
        let current = if store.is_some() && engine.has_persistent_state() {
            Some(engine.snapshot().map_err(io::Error::other)?)
        } else {
            None
        };
        if current != row {
            changes.push(match &current {
                Some(bytes) => RowChange::PutSession(client.clone(), bytes.clone()),
                None => RowChange::DeleteSession(client.clone()),
            });
        }
        if !engine.is_vacant() {
            recovered.push((client, engine, current));
        }
    }
    if let Some(store) = &mut store
        && !changes.is_empty()
    {
        store.apply(&changes).map_err(storage_error)?;
    }
    Ok(Restored {
        store,
        partitions: recovered,
        retained,
        publications,
    })
}

impl Broker {
    pub(super) async fn start(
        config: BrokerConfig,
        clock: Arc<dyn Clock>,
        seed: Option<SessionEngine>,
    ) -> io::Result<Arc<Self>> {
        if let Some(publisher) = &config.publisher {
            publisher.claim()?;
        }
        if let Some(management) = &config.management {
            management.claim()?;
        }
        let restore_config = config.clone();
        let restore_clock = Arc::clone(&clock);
        let restored =
            tokio::task::spawn_blocking(move || restore(&restore_config, restore_clock, seed))
                .await
                .map_err(io::Error::other)??;
        if config.cluster.is_some()
            && (!restored.partitions.is_empty()
                || !restored.retained.is_empty()
                || !restored.publications.is_empty())
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "initial cluster gate requires an empty session/retained database",
            ));
        }
        let storage = restored
            .store
            .map(|store| StorageService::start(store, STORAGE_QUEUE, STORAGE_BATCH))
            .transpose()
            .map_err(storage_error)?
            .map(|service| Storage {
                service: Mutex::new(Some(service)),
                permits: Arc::new(Semaphore::new(STORAGE_QUEUE)),
            });
        let retained = ConcurrentRetained::new(config.max_retained, config.max_state_bytes)
            .map_err(io::Error::other)?;
        for entry in restored.retained {
            retained
                .apply(
                    entry.topic,
                    entry.payload,
                    entry.qos,
                    RetainedMeta {
                        properties: entry.properties,
                        expires_at: entry.expires_at,
                    },
                )
                .map_err(io::Error::other)?;
        }
        let (alive, finished) = mpsc::channel(1);
        let (fatal, _) = watch::channel(None);
        let admission = crate::admission::Admission::new(&config)?;
        let broker = Arc::new(Self {
            session_config: session_config(&config),
            admission,
            config,
            clock,
            events: system_events::Events::new()?,
            cluster_state: cluster::State::default(),
            registry: Registry::new(),
            actors: AtomicUsize::new(0),
            next_actor: AtomicU64::new(1),
            index: SubscriptionIndex::new(),
            retained,
            retained_order: Mutex::new(()),
            storage,
            state_bytes: AtomicUsize::new(0),
            fatal,
            alive: Mutex::new(Some(alive)),
            finished: tokio::sync::Mutex::new(finished),
            handoffs: Mutex::new(Some(tokio::task::JoinSet::new())),
        });
        broker.events.start(Arc::downgrade(&broker));
        // Index every restored subscription before any actor runs its first
        // tick, so recovered Wills route to subscribers spawned later.
        for (client, engine, _) in &restored.partitions {
            if let Some(filters) = engine.subscription_filters().remove(client) {
                broker.index.replace(client, &filters);
                if broker.config.cluster.is_some() {
                    broker
                        .cluster_state
                        .filters(client, &filters.into_iter().collect::<Vec<_>>());
                }
            }
        }
        for (client, engine, row) in restored.partitions {
            let mut shard = broker.registry.shard(&client);
            let handle = broker.spawn_actor(&client, engine, row)?;
            shard.insert(client, handle);
        }
        for publication in restored.publications {
            let _ = broker.apply_retained(&publication).await;
            if let Some(fanout) = broker.dispatch(Arc::new(publication), false) {
                let completing = Arc::clone(&broker);
                tokio::spawn(async move {
                    let _ = completing.complete(fanout).await;
                });
            }
        }
        broker.open_cluster().await?;
        if let Some(management) = &broker.config.management {
            management.attach(Arc::clone(&broker) as Arc<dyn crate::management::Source>)?;
        }
        if let Some(publisher) = &broker.config.publisher {
            publisher.attach(&broker);
        }
        Ok(broker)
    }

    fn alive_token(&self) -> io::Result<mpsc::Sender<()>> {
        self.alive
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .ok_or_else(|| failure(ReasonCode::SERVER_UNAVAILABLE, "broker stopping"))
    }

    /// Caller holds the client's registry shard.
    fn spawn_actor(
        self: &Arc<Self>,
        client: &str,
        engine: SessionEngine,
        row: Option<Vec<u8>>,
    ) -> io::Result<ActorHandle> {
        let reserved = self
            .actors
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < self.config.max_sessions).then_some(count + 1)
            });
        if reserved.is_err() {
            return Err(model_error(EngineError::Capacity));
        }
        let id = self.next_actor.fetch_add(1, Ordering::Relaxed);
        let alive = self.alive_token()?;
        Ok(actor::spawn(
            Arc::clone(self),
            client.to_owned(),
            id,
            engine,
            row,
            alive,
        ))
    }

    pub(super) fn handle(&self, client: &str) -> Option<ActorHandle> {
        self.registry.shard(client).get(client).cloned()
    }

    pub(super) fn contains(&self, client: &str) -> bool {
        self.registry.shard(client).contains_key(client)
    }

    pub(super) fn is_current(&self, client: &str, generation: u64) -> bool {
        self.handle(client)
            .is_some_and(|handle| handle.online.load(Ordering::Acquire) == generation)
    }

    fn get_or_spawn(self: &Arc<Self>, client: &str) -> io::Result<ActorHandle> {
        let mut shard = self.registry.shard(client);
        if let Some(handle) = shard.get(client)
            && !handle.sender.is_closed()
        {
            return Ok(handle.clone());
        }
        if let Some(stale) = shard.remove(client) {
            drop(stale);
        }
        let engine = SessionEngine::new_partition(
            self.session_config,
            Arc::clone(&self.config.access_control),
            Arc::clone(&self.clock),
        )
        .map_err(model_error)?;
        let handle = self.spawn_actor(client, engine, None)?;
        shard.insert(client.to_owned(), handle.clone());
        Ok(handle)
    }

    /// Remove an actor's registry entry only if it is still that actor and no
    /// command is waiting; the caller then exits.
    pub(super) fn retire(&self, client: &str, id: u64, inbox_empty: impl FnOnce() -> bool) -> bool {
        let mut shard = self.registry.shard(client);
        if !inbox_empty() {
            return false;
        }
        if shard.get(client).is_some_and(|handle| handle.id == id) {
            shard.remove(client);
        }
        true
    }

    pub(super) fn actor_exited(&self, client: &str, accounted: usize) {
        self.actors.fetch_sub(1, Ordering::AcqRel);
        self.state_bytes.fetch_sub(accounted, Ordering::AcqRel);
        if self.handle(client).is_none() {
            self.index.remove(client);
        }
    }

    /// Route one request to the client's actor, spawning it for CONNECT. An
    /// actor that exited between lookup and send is replaced.
    pub(super) async fn call(
        self: &Arc<Self>,
        client: &str,
        spawn: bool,
        request: Request,
        principal: Option<mqtt_auth::Principal>,
    ) -> io::Result<Reply> {
        let (reply, response) = oneshot::channel();
        let mut command = ActorCommand::Request {
            request: Box::new(request),
            principal,
            reply,
        };
        for _ in 0..4 {
            let handle = if spawn {
                self.get_or_spawn(client)?
            } else {
                match self.handle(client) {
                    Some(handle) => handle,
                    None => {
                        return Err(failure(
                            ReasonCode::SESSION_TAKEN_OVER,
                            "connection superseded",
                        ));
                    }
                }
            };
            match handle.sender.send(command).await {
                Ok(()) => {
                    return response.await.map_err(|_| {
                        failure(ReasonCode::SERVER_UNAVAILABLE, "session actor terminated")
                    })?;
                }
                Err(mpsc::error::SendError(returned)) => {
                    command = returned;
                    let mut shard = self.registry.shard(client);
                    if shard
                        .get(client)
                        .is_some_and(|current| current.id == handle.id)
                    {
                        shard.remove(client);
                    }
                    if !spawn {
                        return Err(failure(
                            ReasonCode::SESSION_TAKEN_OVER,
                            "connection superseded",
                        ));
                    }
                }
            }
        }
        Err(failure(
            ReasonCode::SERVER_UNAVAILABLE,
            "session actor unavailable",
        ))
    }

    /// Reserve the change in global live state; rejects growth beyond the cap.
    pub(super) fn account(&self, previous: usize, next: usize) -> io::Result<()> {
        if next <= previous {
            self.state_bytes
                .fetch_sub(previous - next, Ordering::AcqRel);
            return Ok(());
        }
        let growth = next - previous;
        self.state_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |total| {
                total
                    .checked_add(growth)
                    .filter(|sum| *sum <= self.config.max_state_bytes)
            })
            .map(|_| ())
            .map_err(|_| model_error(EngineError::Capacity))
    }

    /// Restored or freshly created state is admitted without the growth check.
    pub(super) fn add_state(&self, bytes: usize) {
        self.state_bytes.fetch_add(bytes, Ordering::AcqRel);
    }

    pub(super) fn persistent(&self) -> bool {
        self.storage.is_some()
    }

    /// Commit row changes; failure is fatal for the whole broker.
    pub(super) async fn commit(&self, changes: Vec<RowChange>) -> io::Result<()> {
        let Some(storage) = &self.storage else {
            return Ok(());
        };
        let permit = storage.reserve().await?;
        let outcome = storage.submit(permit, changes)?;
        self.await_commit(outcome).await
    }

    async fn await_commit(&self, outcome: oneshot::Receiver<CommitOutcome>) -> io::Result<()> {
        match outcome.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(self.fail(error.to_string())),
            Err(_) => Err(self.fail("storage completion dropped".into())),
        }
    }

    pub(super) fn fail(&self, message: String) -> io::Error {
        if let Some(publisher) = &self.config.publisher {
            publisher.stop();
        }
        if let Some(management) = &self.config.management {
            management.stop();
        }
        self.fatal.send_if_modified(|current| {
            if current.is_none() {
                *current = Some(message.clone());
                true
            } else {
                false
            }
        });
        failure(ReasonCode::SERVER_UNAVAILABLE, message)
    }

    /// Entries for SUBSCRIBE replay of every non-shared filter in `filters`.
    pub(super) fn retained_for(&self, filters: &[(String, u8)]) -> Vec<RetainedEntry> {
        let mut entries: BTreeMap<String, RetainedEntry> = BTreeMap::new();
        for (filter, _) in filters {
            if mqtt_router::split_shared_filter(filter).is_some() {
                continue;
            }
            for (message, meta) in self.retained.matching(filter, 2) {
                entries.insert(
                    message.topic.clone(),
                    RetainedEntry {
                        topic: message.topic,
                        payload: message.payload,
                        qos: message.qos,
                        properties: meta.properties,
                        expires_at: meta.expires_at,
                    },
                );
            }
        }
        entries.into_values().collect()
    }

    /// Apply a publication's retained side effect in memory and queue its row
    /// in the same order. The returned receiver reports durability.
    pub(super) async fn apply_retained(
        &self,
        publication: &Publication,
    ) -> io::Result<Option<oneshot::Receiver<CommitOutcome>>> {
        let Some(entry) = publication.retained_update() else {
            return Ok(None);
        };
        let row = match &self.storage {
            Some(_) if entry.payload.is_empty() => {
                Some(RowChange::DeleteRetained(entry.topic.clone()))
            }
            Some(_) => Some(RowChange::PutRetained(
                entry.topic.clone(),
                encode_retained_row(self.session_config, entry.clone()).map_err(model_error)?,
            )),
            None => None,
        };
        let permit = match &self.storage {
            Some(storage) => Some(storage.reserve().await?),
            None => None,
        };
        let _order = self
            .retained_order
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // Expired entries must not hold capacity until the periodic prune.
        let mut changes = Vec::new();
        if self.retained.len() >= self.config.max_retained {
            let now = self.clock.now_millis();
            changes.extend(
                self.retained
                    .prune(|meta| meta.expires_at.is_none_or(|at| at > now))
                    .into_iter()
                    .map(RowChange::DeleteRetained),
            );
        }
        let applied = self.retained.apply(
            entry.topic,
            entry.payload,
            entry.qos,
            RetainedMeta {
                properties: entry.properties,
                expires_at: entry.expires_at,
            },
        );
        if applied.is_ok() {
            changes.extend(row);
        }
        let outcome = match (&self.storage, permit) {
            (Some(storage), Some(permit)) if !changes.is_empty() => {
                Some(storage.submit(permit, changes)?)
            }
            _ => None,
        };
        applied.map_err(|error| model_error(EngineError::Retained(error)))?;
        Ok(outcome)
    }

    /// Wait for a retained row queued by [`Self::apply_retained`].
    pub(super) async fn retained_durable(
        &self,
        outcome: Option<oneshot::Receiver<CommitOutcome>>,
    ) -> io::Result<()> {
        match outcome {
            Some(outcome) => self.await_commit(outcome).await,
            None => Ok(()),
        }
    }

    /// Delete expired retained topics from memory and storage.
    pub(super) async fn prune_retained(&self) -> io::Result<()> {
        let now = self.clock.now_millis();
        let permit = match &self.storage {
            Some(storage) => Some(storage.reserve().await?),
            None => None,
        };
        let outcome = {
            let _order = self
                .retained_order
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let stale = self
                .retained
                .prune(|meta| meta.expires_at.is_none_or(|at| at > now));
            match (&self.storage, permit) {
                (Some(storage), Some(permit)) if !stale.is_empty() => Some(storage.submit(
                    permit,
                    stale.into_iter().map(RowChange::DeleteRetained).collect(),
                )?),
                _ => None,
            }
        };
        self.retained_durable(outcome).await
    }

    fn online_first(&self, members: Vec<String>) -> Vec<(String, ActorHandle)> {
        let mut online = Vec::new();
        let mut offline = Vec::new();
        for member in members {
            if let Some(handle) = self.handle(&member) {
                if handle.online.load(Ordering::Acquire) != 0 {
                    online.push((member, handle));
                } else {
                    offline.push((member, handle));
                }
            }
        }
        online.extend(offline);
        online
    }

    /// Send ordinary copies now, preserving per-publisher order. Shared groups
    /// and acknowledgements are returned for [`Self::complete`] when `wait` or
    /// QoS>0 requires member-by-member acceptance.
    pub(super) fn dispatch(&self, publication: Arc<Publication>, wait: bool) -> Option<Fanout> {
        self.dispatch_observed(publication, wait, None)
    }

    pub(super) fn dispatch_observed(
        &self,
        publication: Arc<Publication>,
        wait: bool,
        observer_client: Option<&str>,
    ) -> Option<Fanout> {
        self.dispatch_inner(publication, wait, observer_client, true)
    }
    pub(super) fn dispatch_received(
        &self,
        publication: Arc<Publication>,
        wait: bool,
    ) -> Option<Fanout> {
        self.dispatch_inner(publication, wait, None, false)
    }
    fn dispatch_inner(
        &self,
        publication: Arc<Publication>,
        wait: bool,
        observer_client: Option<&str>,
        origin: bool,
    ) -> Option<Fanout> {
        if publication.expired {
            return None;
        }
        if origin
            && let Some(observer) = &self.config.publication_observer
            && !publication
                .expires_at
                .is_some_and(|deadline| deadline <= self.clock.now_millis())
        {
            observer.observe(
                &publication.topic,
                &publication.payload,
                observer_client
                    .or(publication.publisher.as_deref())
                    .unwrap_or(""),
                publication.qos,
                publication.retain,
            );
        }
        let matches = self.index.matches(&publication.topic);
        let mut acks = Vec::new();
        if origin
            && let Some(remote) = self.forward_cluster(Arc::clone(&publication), observer_client)
        {
            acks.push(remote);
        }
        for client in matches.ordinary {
            let Some(handle) = self.handle(&client) else {
                continue;
            };
            let (ack, receiver) = if wait {
                let (ack, receiver) = oneshot::channel();
                (Some(ack), Some(receiver))
            } else {
                (None, None)
            };
            let admitted = handle
                .sender
                .try_send(ActorCommand::Deliver {
                    publication: Arc::clone(&publication),
                    scope: DeliveryScope::Ordinary,
                    ack,
                })
                .is_ok();
            // A dropped sender makes this receiver false in `complete`. Cluster
            // ingress must report pressure instead of acknowledging lost fanout.
            if (admitted || self.config.cluster.is_some())
                && let Some(receiver) = receiver
            {
                acks.push(receiver);
            }
        }
        let mut shared = Vec::new();
        for group in matches.shared {
            if wait || publication.qos > 0 {
                shared.push(group);
                continue;
            }
            for (member, handle) in self.online_first(group.members) {
                if handle.online.load(Ordering::Acquire) == 0 {
                    break;
                }
                if handle
                    .sender
                    .try_send(ActorCommand::Deliver {
                        publication: Arc::clone(&publication),
                        scope: DeliveryScope::Shared(group.group.clone()),
                        ack: None,
                    })
                    .is_ok()
                {
                    self.index.select(&group.group, &member);
                    break;
                }
            }
        }
        (!acks.is_empty() || !shared.is_empty()).then_some(Fanout {
            publication,
            acks,
            shared,
        })
    }

    /// Wait for ordinary acknowledgements, then offer each shared group to its
    /// members in rotation until one accepts.
    pub(super) async fn complete(&self, fanout: Fanout) -> io::Result<()> {
        let mut failed = false;
        for ack in fanout.acks {
            if !ack.await.unwrap_or(false) && self.config.cluster.is_some() {
                failed = true;
            }
        }
        for group in fanout.shared {
            for (member, handle) in self.online_first(group.members) {
                let (ack, accepted) = oneshot::channel();
                if handle
                    .sender
                    .send(ActorCommand::Deliver {
                        publication: Arc::clone(&fanout.publication),
                        scope: DeliveryScope::Shared(group.group.clone()),
                        ack: Some(ack),
                    })
                    .await
                    .is_err()
                {
                    continue;
                }
                if accepted.await.unwrap_or(false) {
                    self.index.select(&group.group, &member);
                    break;
                }
            }
        }
        if failed {
            Err(failure(
                ReasonCode::SERVER_UNAVAILABLE,
                "cluster delivery not accepted",
            ))
        } else {
            Ok(())
        }
    }

    /// Track bounded, authenticated CONNECT responsibilities through adoption or
    /// same-generation cleanup. Shutdown joins them BEFORE stopping actors.
    pub(super) fn spawn_handoff(
        &self,
        task: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> io::Result<()> {
        let mut handoffs = self.handoffs.lock().unwrap_or_else(PoisonError::into_inner);
        let tasks = handoffs
            .as_mut()
            .ok_or_else(|| failure(ReasonCode::SERVER_UNAVAILABLE, "broker stopping"))?;
        while let Some(result) = tasks.try_join_next() {
            result.map_err(|error| self.fail(format!("CONNECT handoff failed: {error}")))?;
        }
        if tasks.len() >= self.config.max_handshakes {
            return Err(failure(ReasonCode::SERVER_BUSY, "CONNECT cleanup capacity"));
        }
        tasks.spawn(task);
        Ok(())
    }

    /// Stop every actor after connections have closed, then drain storage.
    pub(super) async fn shutdown(&self) -> io::Result<()> {
        if let Some(publisher) = &self.config.publisher {
            publisher.drain().await;
        }
        self.events.shutdown().await;
        if let Some(cluster) = &self.config.cluster {
            let _ = tokio::time::timeout(Duration::from_secs(5), cluster.shutdown()).await;
        }
        if let Some(management) = &self.config.management {
            management.stop();
        }
        let handoffs = self
            .handoffs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let mut handoff_error = None;
        if let Some(mut tasks) = handoffs {
            while let Some(result) = tasks.join_next().await {
                if let Err(error) = result {
                    handoff_error = Some(io::Error::other(error));
                }
            }
        }
        for shard in &self.registry.shards {
            shard.lock().unwrap_or_else(PoisonError::into_inner).clear();
        }
        self.alive
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let mut finished = self.finished.lock().await;
        while finished.recv().await.is_some() {}
        let service = self.storage.as_ref().and_then(|storage| {
            storage
                .service
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
        });
        if let Some(service) = service {
            tokio::task::spawn_blocking(move || service.shutdown())
                .await
                .map_err(io::Error::other)?;
        }
        if let Some(error) = handoff_error {
            Err(error)
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    pub(super) fn storage_jobs(&self) -> usize {
        self.storage.as_ref().map_or(0, |storage| {
            STORAGE_QUEUE - storage.permits.available_permits()
        })
    }

    #[cfg(test)]
    pub(super) fn actor_count(&self) -> usize {
        self.actors.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn handles(&self) -> Vec<ActorHandle> {
        self.registry
            .shards
            .iter()
            .flat_map(|shard| {
                shard
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Run every actor's timer work now (tests drive manual clocks).
    #[cfg(test)]
    pub(super) async fn tick_all(&self) {
        for handle in self.handles() {
            let (done, finished) = oneshot::channel();
            if handle
                .sender
                .send(ActorCommand::Tick { done })
                .await
                .is_ok()
            {
                let _ = finished.await;
            }
        }
    }

    /// Install an online connection for `client` without a CONNECT.
    #[cfg(test)]
    pub(super) async fn attach(self: &Arc<Self>, client: &str, online: Online) {
        let handle = self.get_or_spawn(client).unwrap();
        let (done, finished) = oneshot::channel();
        assert!(
            handle
                .sender
                .send(ActorCommand::Attach {
                    client: client.to_owned(),
                    online,
                    done,
                })
                .await
                .is_ok()
        );
        finished.await.unwrap();
    }

    /// Block `client`'s actor; dropping the sender releases it.
    #[cfg(test)]
    pub(super) async fn stall(self: &Arc<Self>, client: &str) -> oneshot::Sender<()> {
        let handle = self.get_or_spawn(client).unwrap();
        let (release, stalled) = oneshot::channel();
        assert!(
            handle
                .sender
                .send(ActorCommand::Stall { release: stalled })
                .await
                .is_ok()
        );
        release
    }

    /// Commands waiting in `client`'s inbox.
    #[cfg(test)]
    pub(super) fn queued(&self, client: &str) -> usize {
        self.handle(client).map_or(0, |handle| {
            handle.sender.max_capacity() - handle.sender.capacity()
        })
    }
}

pub(super) struct Fanout {
    publication: Arc<Publication>,
    acks: Vec<oneshot::Receiver<bool>>,
    shared: Vec<SharedMatch>,
}

pub(super) fn subscribe_filters(frame: &Frame) -> Option<&[(String, u8)]> {
    match &frame.packet {
        Packet::Subscribe { filters, .. } => Some(filters),
        _ => None,
    }
}

impl crate::management::Source for Broker {
    fn state_bytes(&self) -> usize {
        self.state_bytes.load(Ordering::Acquire)
    }
    fn retained(
        &self,
        query: &crate::management::Query,
    ) -> crate::management::Page<crate::management::RetainedView> {
        let now = self.clock.now_millis();
        let mut page = crate::management::Page {
            items: Vec::with_capacity(query.limit),
            total: 0,
            offset: query.offset,
            limit: query.limit,
        };
        self.retained
            .visit_metadata(|topic, qos, payload_bytes, meta| {
                if meta.expires_at.is_some_and(|at| at <= now) || !topic.contains(&query.search) {
                    return;
                }
                if page.total >= query.offset && page.items.len() < query.limit {
                    page.items.push(crate::management::RetainedView {
                        topic: topic.into(),
                        qos,
                        payload_bytes,
                        expires_at_millis: meta.expires_at,
                    });
                }
                page.total += 1;
            });
        page
    }
}
