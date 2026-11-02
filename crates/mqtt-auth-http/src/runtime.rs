use crate::{
    AclQuery, AuthorizationConfig, BackendConfig, CacheStatus, Config, Error, ProbeResult, Status,
    attempt::Attempt, cache::Cache, client::Client, persistence::Owner,
};
use mqtt_auth::{AccessAttempt, AccessControl, ConnectInfo, RemoteAccess};
use std::{
    fmt, io,
    path::PathBuf,
    sync::{
        Arc, OnceLock, PoisonError, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
pub(crate) struct Generation {
    pub id: u64,
    pub config: Config,
    pub authentication: Option<Client>,
    pub authorization: Option<Client>,
    _slot: OwnedSemaphorePermit,
}
impl Generation {
    fn build(id: u64, config: Config, slot: OwnedSemaphorePermit) -> Result<Self, Error> {
        let authentication = config
            .authentication
            .enabled
            .then(|| Client::build(&config.authentication, false))
            .transpose()?;
        let authorization = config
            .authorization
            .backend
            .enabled
            .then(|| Client::build(&config.authorization.backend, false))
            .transpose()?;
        Ok(Self {
            id,
            config,
            authentication,
            authorization,
            _slot: slot,
        })
    }
}
pub(crate) struct Shared {
    pub current: RwLock<Arc<Generation>>,
    pub cache: Arc<Cache>,
    pub stopped: AtomicBool,
    pub fallback: Arc<dyn AccessControl>,
    pub local_accounts: bool,
    pub attempt_bytes: AtomicUsize,
    pub attempts: Arc<Semaphore>,
}
pub struct Runtime {
    pub(crate) shared: Arc<Shared>,
    store: Arc<Owner>,
    update: Arc<Semaphore>,
    generations: Arc<Semaphore>,
}
impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpAccessRuntime")
            .field("stopped", &self.shared.stopped.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}
fn probe_slots() -> &'static Arc<Semaphore> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SLOTS.get_or_init(|| Arc::new(Semaphore::new(4)))
}
impl Runtime {
    pub async fn open(
        path: PathBuf,
        fallback: Arc<dyn AccessControl>,
        local_accounts: bool,
    ) -> io::Result<Arc<Self>> {
        let generations = Arc::new(Semaphore::new(4));
        let slot = generations
            .clone()
            .try_acquire_owned()
            .map_err(|_| io::Error::other(Error::Busy))?;
        let (store, prepared) = tokio::task::spawn_blocking(move || {
            let (store, bytes) = Owner::open(path)?;
            let prepared = (|| {
                let fresh = bytes.is_none();
                let config = match bytes {
                    Some(bytes) => Config::from_stored(&bytes)?,
                    None => Config::default(),
                };
                config.validate(local_accounts)?;
                let generation = Generation::build(1, config, slot)?;
                Ok::<_, Error>((generation, fresh))
            })();
            Ok::<_, Error>((store, prepared))
        })
        .await
        .map_err(|_| io::Error::other(Error::Storage))?
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let (generation, fresh) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                // Report validation/preparation failure only after the blocking
                // owner has released SQLite and its exclusive file lock. Drop
                // remains the cancellation fallback if this future is aborted.
                store.close().await;
                return Err(io::Error::new(io::ErrorKind::InvalidInput, error));
            }
        };
        let runtime = Arc::new(Self {
            shared: Arc::new(Shared {
                current: RwLock::new(Arc::new(generation)),
                cache: Arc::new(Cache::default()),
                stopped: AtomicBool::new(false),
                fallback,
                local_accounts,
                attempt_bytes: AtomicUsize::new(0),
                attempts: Arc::new(Semaphore::new(256)),
            }),
            store: Arc::new(store),
            update: Arc::new(Semaphore::new(1)),
            generations,
        });
        if fresh {
            let config = runtime.snapshot().config.clone();
            let bytes =
                serde_json::to_vec(&config).map_err(|_| io::Error::other(Error::InvalidConfig))?;
            runtime.store.save(bytes).await.map_err(io::Error::other)?;
        }
        Ok(runtime)
    }
    pub(crate) fn snapshot(&self) -> Arc<Generation> {
        self.shared
            .current
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
    pub fn is_stopped(&self) -> bool {
        self.shared.stopped.load(Ordering::Acquire)
    }
    #[cfg(test)]
    pub(crate) fn config_writes_for_test(&self) -> u64 {
        self.store.writes()
    }
    pub async fn configuration(&self) -> Config {
        self.snapshot().config.public()
    }
    pub async fn status(&self) -> Status {
        let g = self.snapshot();
        Status {
            authentication: g
                .authentication
                .as_ref()
                .map_or_else(Default::default, |c| c.counters.status(true)),
            authorization: g
                .authorization
                .as_ref()
                .map_or_else(Default::default, |c| c.counters.status(true)),
            cache: self.shared.cache.status(),
        }
    }
    pub async fn replace(&self, mut config: Config) -> Result<Config, Error> {
        if self.shared.stopped.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        let _update = self
            .update
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let old = self.snapshot();
        config.authentication.restore(&old.config.authentication)?;
        config
            .authorization
            .backend
            .restore(&old.config.authorization.backend)?;
        let local_accounts = self.shared.local_accounts;
        let slot = self
            .generations
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let id = old.id.checked_add(1).ok_or(Error::Stopped)?;
        let next = tokio::task::spawn_blocking(move || {
            config.validate(local_accounts)?;
            Generation::build(id, config, slot)
        })
        .await
        .map_err(|_| Error::InvalidConfig)??;
        let bytes = serde_json::to_vec(&next.config).map_err(|_| Error::InvalidConfig)?;
        let mut pending = PublishGuard {
            shared: self.shared.clone(),
            armed: true,
        };
        self.store.save(bytes).await?;
        if self.shared.stopped.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        let public = next.config.public();
        {
            let mut current = self
                .shared
                .current
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            self.shared.cache.clear();
            *current = Arc::new(next);
        }
        pending.armed = false;
        Ok(public)
    }
    pub async fn clear_cache(&self) -> CacheStatus {
        self.shared.cache.clear()
    }
    pub async fn probe_authentication(
        &self,
        mut config: BackendConfig,
        info: ConnectInfo<'_>,
    ) -> Result<ProbeResult, Error> {
        if self.shared.stopped.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        let _slot = probe_slots()
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        config.restore(&self.snapshot().config.authentication)?;
        let client = tokio::task::spawn_blocking(move || Client::build(&config, true))
            .await
            .map_err(|_| Error::InvalidConfig)??;
        client.authenticate(info, false).await
    }
    pub async fn probe_authorization(
        &self,
        mut config: AuthorizationConfig,
        query: AclQuery,
    ) -> Result<ProbeResult, Error> {
        if self.shared.stopped.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        if config.cache_ttl_seconds > 300 {
            return Err(Error::InvalidConfig);
        }
        let _slot = probe_slots()
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        config
            .backend
            .restore(&self.snapshot().config.authorization.backend)?;
        let client = tokio::task::spawn_blocking(move || Client::build(&config.backend, true))
            .await
            .map_err(|_| Error::InvalidConfig)??;
        client.authorize(&query, false).await
    }
    pub async fn shutdown(&self) {
        self.shared.stopped.store(true, Ordering::Release);
        self.shared.cache.clear();
        self.store.close().await;
    }
}
impl RemoteAccess for Runtime {
    fn begin(&self) -> Arc<dyn AccessAttempt> {
        Arc::new(Attempt::new(self.shared.clone(), self.snapshot()))
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        self.shared.stopped.store(true, Ordering::Release);
    }
}
struct PublishGuard {
    shared: Arc<Shared>,
    armed: bool,
}
impl Drop for PublishGuard {
    fn drop(&mut self) {
        if self.armed {
            self.shared.stopped.store(true, Ordering::Release);
            self.shared.cache.clear();
        }
    }
}
