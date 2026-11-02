use crate::{
    Config, PublicConfig, RuntimeError,
    config::{MAX_CONFIG, Validated},
    counters::{ActionCounters, Budget, Counters, Status},
    dispatcher::{self, Work},
    durable::{self, Queue},
    durable_worker::{self, Generation, Target},
    persistence,
};
use mqtt_rules::{CompiledRule, Event, Rule};
use mqtt_storage::configuration::ConfigStore;
use mqtt_transport::PublicationObserver;
use std::{
    collections::BTreeMap,
    fmt, io,
    path::PathBuf,
    sync::{
        Arc, Mutex, PoisonError, RwLock, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, SyncSender},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Semaphore, watch};

pub(crate) struct Snapshot {
    pub config: Config,
    pub rules: Vec<(Rule, CompiledRule)>,
    exact: BTreeMap<String, Vec<usize>>,
    wildcard: Vec<usize>,
    pub generation: Arc<Generation>,
}
impl Snapshot {
    pub(crate) fn candidates<'a>(
        &'a self,
        topic: &str,
    ) -> impl Iterator<Item = &'a (Rule, CompiledRule)> {
        let first = topic.split('/').next().unwrap_or("");
        self.exact
            .get(first)
            .into_iter()
            .flatten()
            .chain(self.wildcard.iter())
            .map(|index| &self.rules[*index])
    }
}
pub(crate) struct Inner {
    store: Arc<crate::runtime_init::ConfigOwner>,
    pub snapshot: RwLock<Arc<Snapshot>>,
    senders: Vec<SyncSender<Work>>,
    pub counters: Arc<Counters>,
    pub stopped: Arc<AtomicBool>,
    pub queue: Arc<Queue>,
    sequence: AtomicU64,
    prefix: String,
    mutation: Arc<Semaphore>,
    tests: Arc<Semaphore>,
    outbound: Arc<Semaphore>,
    threads: Mutex<Vec<std::thread::JoinHandle<()>>>,
}
pub struct Runtime {
    inner: Arc<Inner>,
}
impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("mode", &"best_effort")
            .finish_non_exhaustive()
    }
}
struct Observer {
    inner: Weak<Inner>,
}
impl fmt::Debug for Observer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuleObserver").finish_non_exhaustive()
    }
}
impl PublicationObserver for Observer {
    fn observe(&self, topic: &str, payload: &[u8], client_id: &str, qos: u8, retain: bool) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        if inner.stopped.load(Ordering::Acquire) {
            return;
        }
        let snapshot = match inner.snapshot.try_read() {
            Ok(snapshot) => Arc::clone(&snapshot),
            Err(std::sync::TryLockError::Poisoned(error)) => Arc::clone(&error.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => {
                inner.counters.dropped.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        if !snapshot
            .candidates(topic)
            .any(|(_, compiled)| compiled.matches_topic(topic))
        {
            return;
        }
        inner.counters.received.fetch_add(1, Ordering::Relaxed);
        if payload.len() > 1024 * 1024 {
            inner.counters.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let bytes = payload
            .len()
            .saturating_add(topic.len())
            .saturating_add(client_id.len())
            .saturating_add(128);
        let Some(budget) = Budget::reserve(&inner.counters, bytes, None) else {
            inner.counters.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let sequence =
            match inner
                .sequence
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            {
                Ok(n) => n,
                Err(_) => {
                    inner.counters.dropped.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let event = Event {
            topic: topic.into(),
            payload: payload.to_vec(),
            client_id: client_id.into(),
            qos,
            retain,
            timestamp_ms: u64::try_from(millis).unwrap_or(u64::MAX),
        };
        let id = format!("{}-{sequence}", inner.prefix);
        let work = Work {
            event,
            snapshot,
            _budget: budget,
            id,
            complete: false,
            inner: Arc::clone(&inner),
        };
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        client_id.hash(&mut hasher);
        let shard = hasher.finish() as usize % inner.senders.len();
        let _ = inner.senders[shard].try_send(work);
    }
}
fn build_snapshot(
    validated: Validated,
    context: Arc<durable_worker::Context>,
    revisions: &BTreeMap<String, u64>,
) -> Arc<Snapshot> {
    let (cancel, _) = watch::channel(false);
    let mut targets = BTreeMap::new();
    let mut tasks = Vec::new();
    for action in &validated.config.outbounds {
        let stats = Arc::new(ActionCounters::default());
        let Some(sink) = validated.sinks.get(&action.id).cloned() else {
            continue;
        };
        let target = Target {
            stats: Arc::clone(&stats),
            revision: revisions.get(&action.id).copied().unwrap_or(0),
            enabled: action.enabled,
        };
        if action.enabled {
            tasks.push(durable_worker::spawn(
                action.clone(),
                sink,
                stats,
                Arc::clone(&context),
                cancel.subscribe(),
            ));
        }
        targets.insert(action.id.clone(), target);
    }
    let mut exact: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut wildcard = Vec::new();
    for (index, (rule, compiled)) in validated.rules.iter().enumerate() {
        if !rule.enabled {
            continue;
        }
        let first = compiled.topic_filter().split('/').next().unwrap_or("");
        if matches!(first, "+" | "#") {
            wildcard.push(index);
        } else {
            exact.entry(first.into()).or_default().push(index);
        }
    }
    Arc::new(Snapshot {
        config: validated.config,
        rules: validated.rules,
        exact,
        wildcard,
        generation: Arc::new(Generation {
            cancel,
            targets,
            tasks: Mutex::new(tasks),
        }),
    })
}
impl Runtime {
    /// Open configuration plus its sibling generic durable outbound journal.
    pub async fn open(path: PathBuf) -> io::Result<Arc<Self>> {
        let mut directory = path.as_os_str().to_os_string();
        directory.push(".outbound-queue");
        Self::open_with_queue(path, crate::QueueOptions::new(PathBuf::from(directory))).await
    }
    /// Open with explicit journal directory and bounded disk/record limits.
    pub async fn open_with_queue(
        path: PathBuf,
        options: crate::QueueOptions,
    ) -> io::Result<Arc<Self>> {
        let cleanup = tokio::runtime::Handle::try_current().map_err(io::Error::other)?;
        let (store, validated) = tokio::task::spawn_blocking(move || {
            let mut store =
                ConfigStore::open(&path, MAX_CONFIG).map_err(|_| RuntimeError::Storage)?;
            let bytes = store.load().map_err(|_| RuntimeError::Storage)?;
            let config = match bytes.as_deref() {
                Some(bytes) => persistence::decode(bytes)?,
                None => Config::default(),
            };
            let validated = config.validate()?;
            if bytes
                .as_ref()
                .is_none_or(|b| serde_json::from_slice::<Config>(b).is_err())
            {
                persistence::write(&mut store, &validated.config)?;
            }
            Ok::<_, RuntimeError>((store, validated))
        })
        .await
        .map_err(|_| io::Error::other("rule runtime initialization failed"))?
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        // Cancellation or any later startup error must not close SQLite on an
        // async worker. Transfer this guarded owner to Inner only on success.
        let store = Arc::new(crate::runtime_init::ConfigOwner::new(store, cleanup));
        let outbound = Arc::new(Semaphore::new(crate::config::GLOBAL_CONCURRENCY));
        let counters = Arc::new(Counters::default());
        let stopped = Arc::new(AtomicBool::new(false));
        let bindings = durable::bindings(&validated.config).map_err(io::Error::other)?;
        let (queue, revisions) = match Queue::open(
            options,
            bindings,
            Arc::clone(&counters),
            Arc::clone(&stopped),
        )
        .await
        {
            Ok(opened) => opened,
            Err(error) => {
                store.close().await;
                return Err(io::Error::other(error));
            }
        };
        let context = Arc::new(durable_worker::Context {
            codec: crate::result_wire::Codec::Rule,
            queue: Arc::clone(&queue),
            global: Arc::clone(&outbound),
            counters: Arc::clone(&counters),
            stopped: Arc::clone(&stopped),
        });
        let snapshot = build_snapshot(validated, context, &revisions);
        let (senders, receivers): (Vec<_>, Vec<_>) =
            (0..4).map(|_| mpsc::sync_channel(256)).unzip();
        let prefix = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let inner = Arc::new(Inner {
            store,
            snapshot: RwLock::new(snapshot),
            senders,
            counters,
            stopped,
            queue,
            sequence: AtomicU64::new(1),
            prefix,
            mutation: Arc::new(Semaphore::new(1)),
            tests: Arc::new(Semaphore::new(3)),
            outbound,
            threads: Mutex::new(Vec::new()),
        });
        for (index, receiver) in receivers.into_iter().enumerate() {
            let thread_inner = Arc::clone(&inner);
            let thread = std::thread::Builder::new()
                .name(format!("rmqtt-rule-{index}"))
                .spawn(move || dispatcher::run(thread_inner, receiver));
            let thread = match thread {
                Ok(thread) => thread,
                Err(error) => {
                    inner.stopped.store(true, Ordering::Release);
                    let cleanup = Arc::clone(&inner);
                    // This task owns cleanup if the initializing caller is
                    // cancelled, including when no dispatcher thread started.
                    let _ = tokio::spawn(async move { shutdown_inner(cleanup).await }).await;
                    return Err(error);
                }
            };
            inner
                .threads
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(thread);
        }
        Ok(Arc::new(Self { inner }))
    }
    pub fn observer(&self) -> Arc<dyn PublicationObserver> {
        Arc::new(Observer {
            inner: Arc::downgrade(&self.inner),
        })
    }
    pub async fn configuration(&self) -> PublicConfig {
        self.inner
            .snapshot
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .config
            .public()
    }
    pub async fn status(&self) -> Status {
        let snapshot = Arc::clone(
            &self
                .inner
                .snapshot
                .read()
                .unwrap_or_else(PoisonError::into_inner),
        );
        let c = &self.inner.counters;
        Status {
            inflight_requests: c.inflight_requests.load(Ordering::Relaxed),
            mode: "best_effort",
            running: !self.inner.stopped.load(Ordering::Acquire),
            queued_events: c.queued.load(Ordering::Relaxed),
            queued_bytes: c.bytes.load(Ordering::Relaxed),
            received: c.received.load(Ordering::Relaxed),
            matched: c.matched.load(Ordering::Relaxed),
            dropped: c.dropped.load(Ordering::Relaxed),
            errors: c.errors.load(Ordering::Relaxed),
            durable_queue: self.inner.queue.stats(),
            outbounds: snapshot
                .config
                .outbounds
                .iter()
                .map(|a| {
                    let mut view = snapshot
                        .generation
                        .targets
                        .get(&a.id)
                        .map(|target| target.stats.view(a.id.clone(), a.endpoint.kind()))
                        .unwrap_or_else(|| {
                            ActionCounters::default().view(a.id.clone(), a.endpoint.kind())
                        });
                    view.durable_queued = self.inner.queue.pending(&a.id);
                    view
                })
                .collect(),
        }
    }
    pub async fn replace(&self, config: Config) -> Result<PublicConfig, RuntimeError> {
        self.replace_using(config, persistence::write).await
    }
    #[cfg(test)]
    pub(crate) async fn replace_test_write(
        &self,
        config: Config,
        write: fn(&mut ConfigStore, &Config) -> Result<(), RuntimeError>,
    ) -> Result<PublicConfig, RuntimeError> {
        self.replace_using(config, write).await
    }
    async fn replace_using(
        &self,
        mut config: Config,
        write: fn(&mut ConfigStore, &Config) -> Result<(), RuntimeError>,
    ) -> Result<PublicConfig, RuntimeError> {
        if self.inner.stopped.load(Ordering::Acquire) {
            return Err(RuntimeError::Stopped);
        }
        let permit = Arc::clone(&self.inner.mutation)
            .try_acquire_owned()
            .map_err(|_| RuntimeError::Busy)?;
        let inner = Arc::clone(&self.inner);
        // The task owns its permit: caller cancellation cannot overlap config writes.
        tokio::spawn(async move {
            let _permit = permit;
            if inner.stopped.load(Ordering::Acquire) {
                return Err(RuntimeError::Stopped);
            }
            let previous = inner
                .snapshot
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .config
                .clone();
            config.restore_redacted(&previous)?;
            let validated = tokio::task::spawn_blocking(move || config.validate())
                .await
                .map_err(|_| RuntimeError::Storage)??;
            let bindings = durable::bindings(&validated.config)?;
            let revisions = match inner.queue.bind(bindings).await {
                Ok(revisions) => revisions,
                Err(mqtt_egress_log::QueueError::BindingConflict) => {
                    return Err(RuntimeError::Busy);
                }
                Err(_) => {
                    stop_after_storage_error(&inner).await;
                    return Err(RuntimeError::Storage);
                }
            };
            let store = Arc::clone(&inner.store);
            let saved = tokio::task::spawn_blocking(move || {
                let mut guard = store.lock().map_err(|_| RuntimeError::Storage)?;
                let store = guard.as_mut().ok_or(RuntimeError::Storage)?;
                write(store, &validated.config)?;
                Ok::<_, RuntimeError>(validated)
            })
            .await;
            let validated = match saved {
                Ok(Ok(validated)) => validated,
                _ => {
                    stop_after_storage_error(&inner).await;
                    return Err(RuntimeError::Storage);
                }
            };
            if inner.stopped.load(Ordering::Acquire) {
                return Err(RuntimeError::Stopped);
            }
            // Old claims are completely joined before the next generation can
            // replay the same journal head. Cancellation never completes records.
            let previous = Arc::clone(
                &inner
                    .snapshot
                    .read()
                    .unwrap_or_else(PoisonError::into_inner),
            );
            previous.generation.finish().await;
            let context = Arc::new(durable_worker::Context {
                codec: crate::result_wire::Codec::Rule,
                queue: Arc::clone(&inner.queue),
                global: Arc::clone(&inner.outbound),
                counters: Arc::clone(&inner.counters),
                stopped: Arc::clone(&inner.stopped),
            });
            let next = build_snapshot(validated, context, &revisions);
            let public = next.config.public();
            *inner
                .snapshot
                .write()
                .unwrap_or_else(PoisonError::into_inner) = next;
            Ok(public)
        })
        .await
        .map_err(|_| RuntimeError::Storage)?
    }
    pub async fn test(
        &self,
        sql: &str,
        event: Event,
    ) -> Result<Option<serde_json::Value>, RuntimeError> {
        if self.inner.stopped.load(Ordering::Acquire) {
            return Err(RuntimeError::Stopped);
        }
        if event.topic.is_empty()
            || event.topic.len() > 65535
            || event.topic.contains(['\0', '+', '#'])
            || event.client_id.len() > 65535
            || event.qos > 2
            || event.payload.len() > 1024 * 1024
        {
            return Err(RuntimeError::InvalidConfig);
        }
        let permit = Arc::clone(&self.inner.tests)
            .try_acquire_owned()
            .map_err(|_| RuntimeError::Busy)?;
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let rule = CompiledRule::compile(&sql).map_err(|_| RuntimeError::InvalidSql)?;
            if !rule.matches_topic(&event.topic) {
                return Ok(None);
            }
            rule.evaluate(&event).map_err(|_| RuntimeError::InvalidSql)
        })
        .await
        .map_err(|_| RuntimeError::InvalidSql)?
    }
    pub async fn shutdown(&self) {
        let inner = Arc::clone(&self.inner);
        // Cleanup continues even if the management/shutdown caller is cancelled.
        let _ = tokio::spawn(async move { shutdown_inner(inner).await }).await;
    }
}
async fn shutdown_inner(inner: Arc<Inner>) {
    inner.stopped.store(true, Ordering::Release);
    // A cancelled HTTP caller may still own a config mutation. Wait until its
    // commit/publish path ends before releasing database ownership.
    let _permit = inner.mutation.acquire().await;
    let generation = Arc::clone(
        &inner
            .snapshot
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .generation,
    );
    generation.finish().await;
    let threads =
        std::mem::take(&mut *inner.threads.lock().unwrap_or_else(PoisonError::into_inner));
    let store = Arc::clone(&inner.store);
    let _ = tokio::time::timeout(
        Duration::from_secs(3),
        tokio::task::spawn_blocking(move || {
            // SQLite close and ownership release run on the blocking pool.
            drop(store.lock().unwrap_or_else(PoisonError::into_inner).take());
            for thread in threads {
                let _ = thread.join();
            }
        }),
    )
    .await;
    inner.queue.close().await;
}

async fn stop_after_storage_error(inner: &Inner) {
    inner.stopped.store(true, Ordering::Release);
    inner.counters.errors.fetch_add(1, Ordering::Relaxed);
    let generation = Arc::clone(
        &inner
            .snapshot
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .generation,
    );
    // Do not return a storage failure while old network attempts still own
    // permits/buffers. The mutation task owns its permit even if its caller left.
    generation.finish().await;
}

impl Drop for Runtime {
    fn drop(&mut self) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let store = Arc::clone(&self.inner.store);
            handle.spawn_blocking(move || {
                drop(store.lock().unwrap_or_else(PoisonError::into_inner).take());
            });
        }
        self.inner.stopped.store(true, Ordering::Release);
        self.inner
            .snapshot
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .generation
            .cancel();
    }
}

#[cfg(test)]
#[path = "runtime_lifecycle_tests.rs"]
mod lifecycle_tests;
