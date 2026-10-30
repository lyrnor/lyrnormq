use crate::{
    Config, PublicConfig, RuntimeError,
    config::Validated,
    counters::{ActionCounters, Budget, Counters, Status},
    dispatcher::{self, Work},
    persistence,
    worker::{self, Generation},
};
use mqtt_rules::{CompiledRule, Event, Rule};
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
    pub generation: Arc<Generation>,
}
pub(crate) struct Inner {
    path: PathBuf,
    pub snapshot: RwLock<Arc<Snapshot>>,
    senders: Vec<SyncSender<Work>>,
    pub counters: Arc<Counters>,
    pub stopped: AtomicBool,
    sequence: AtomicU64,
    prefix: String,
    mutation: Arc<Semaphore>,
    tests: Arc<Semaphore>,
    outbound: Arc<Semaphore>,
    client: reqwest::Client,
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
        let snapshot = Arc::clone(
            &inner
                .snapshot
                .read()
                .unwrap_or_else(PoisonError::into_inner),
        );
        if !snapshot
            .rules
            .iter()
            .any(|(rule, compiled)| rule.enabled && compiled.matches_topic(topic))
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
    client: &reqwest::Client,
    outbound: &Arc<Semaphore>,
) -> Arc<Snapshot> {
    let (cancel, _) = watch::channel(false);
    let mut targets = BTreeMap::new();
    let mut tasks = Vec::new();
    for action in validated.config.actions.iter().filter(|a| a.enabled) {
        if let Some(connector) = validated
            .config
            .connectors
            .iter()
            .find(|c| c.id == action.connector_id)
        {
            let stats = Arc::new(ActionCounters::default());
            let (target, task) = worker::spawn(
                action.clone(),
                connector.clone(),
                client.clone(),
                Arc::clone(outbound),
                Arc::clone(&stats),
                cancel.subscribe(),
            );
            targets.insert(action.id.clone(), target);
            tasks.push(task);
        }
    }
    Arc::new(Snapshot {
        config: validated.config,
        rules: validated.rules,
        generation: Arc::new(Generation {
            cancel,
            targets,
            tasks: Mutex::new(tasks),
        }),
    })
}
impl Runtime {
    pub async fn open(path: PathBuf) -> io::Result<Arc<Self>> {
        let (path, validated, client) = tokio::task::spawn_blocking(move || {
            let path = if path.is_absolute() {
                path
            } else {
                std::env::current_dir()
                    .map_err(|_| RuntimeError::Storage)?
                    .join(path)
            };
            let config = persistence::read(&path)?;
            let validated = config.validate()?;
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .no_proxy()
                .pool_max_idle_per_host(1)
                .pool_idle_timeout(Duration::from_secs(30))
                .connect_timeout(Duration::from_secs(10))
                .build()
                .map_err(|_| RuntimeError::InvalidConfig)?;
            Ok::<_, RuntimeError>((path, validated, client))
        })
        .await
        .map_err(|_| io::Error::other("rule runtime initialization failed"))?
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let outbound = Arc::new(Semaphore::new(16));
        let snapshot = build_snapshot(validated, &client, &outbound);
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
            path,
            snapshot: RwLock::new(snapshot),
            senders,
            counters: Arc::new(Counters::default()),
            stopped: AtomicBool::new(false),
            sequence: AtomicU64::new(1),
            prefix,
            mutation: Arc::new(Semaphore::new(1)),
            tests: Arc::new(Semaphore::new(3)),
            outbound,
            client,
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
                    inner
                        .snapshot
                        .read()
                        .unwrap_or_else(PoisonError::into_inner)
                        .generation
                        .cancel();
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
            mode: "best_effort",
            queued_events: c.queued.load(Ordering::Relaxed),
            queued_bytes: c.bytes.load(Ordering::Relaxed),
            received: c.received.load(Ordering::Relaxed),
            matched: c.matched.load(Ordering::Relaxed),
            dropped: c.dropped.load(Ordering::Relaxed),
            errors: c.errors.load(Ordering::Relaxed),
            actions: snapshot
                .config
                .actions
                .iter()
                .map(|a| {
                    snapshot
                        .generation
                        .targets
                        .get(&a.id)
                        .map(|target| target.stats.view(a.id.clone()))
                        .unwrap_or_else(|| ActionCounters::default().view(a.id.clone()))
                })
                .collect(),
        }
    }
    pub async fn replace(&self, mut config: Config) -> Result<PublicConfig, RuntimeError> {
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
            let path = inner.path.clone();
            let validated = tokio::task::spawn_blocking(move || {
                let validated = config.validate()?;
                persistence::write(&path, &validated.config)?;
                Ok::<_, RuntimeError>(validated)
            })
            .await
            .map_err(|_| RuntimeError::Storage)??;
            if inner.stopped.load(Ordering::Acquire) {
                return Err(RuntimeError::Stopped);
            }
            let next = build_snapshot(validated, &inner.client, &inner.outbound);
            let public = next.config.public();
            let previous = std::mem::replace(
                &mut *inner
                    .snapshot
                    .write()
                    .unwrap_or_else(PoisonError::into_inner),
                next,
            );
            previous.generation.finish().await;
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
        self.inner.stopped.store(true, Ordering::Release);
        let generation = Arc::clone(
            &self
                .inner
                .snapshot
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .generation,
        );
        generation.finish().await;
        let threads = std::mem::take(
            &mut *self
                .inner
                .threads
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        if !threads.is_empty() {
            let _ = tokio::time::timeout(
                Duration::from_secs(3),
                tokio::task::spawn_blocking(move || {
                    for thread in threads {
                        let _ = thread.join();
                    }
                }),
            )
            .await;
        }
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        self.inner.stopped.store(true, Ordering::Release);
        self.inner
            .snapshot
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .generation
            .cancel();
    }
}
