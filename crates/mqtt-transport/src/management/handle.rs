use super::types::*;
use mqtt_session::durable::SessionEngine;
use std::{
    collections::BTreeMap,
    fmt, io,
    sync::{
        Arc, Mutex, PoisonError, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::sync::Semaphore;

pub(crate) trait Source: Send + Sync {
    fn state_bytes(&self) -> usize;
    fn retained(&self, query: &Query) -> Page<RetainedView>;
}
#[derive(Default)]
pub(crate) struct Observation {
    summary: Option<SessionView>,
    subscriptions: Vec<SubscriptionView>,
}
impl Observation {
    pub(crate) fn update(&mut self, engine: &SessionEngine, client: &str, filters_changed: bool) {
        let previously_known = self
            .summary
            .as_ref()
            .is_some_and(|s| s.protocol != "unknown");
        self.summary = engine.session_metadata(client).map(|s| {
            let known = s.online || previously_known;
            SessionView {
                client_id: s.client_id,
                online: s.online,
                protocol: if known {
                    s.protocol.into()
                } else {
                    "unknown".into()
                },
                principal: s.principal,
                persistent: s.persistent,
                subscriptions: s.subscriptions,
                queued: s.queued,
                inflight: s.inflight,
                receive_maximum: known.then_some(s.receive_maximum),
                session_expiry_interval: s.session_expiry_interval,
            }
        });
        if filters_changed || self.summary.is_none() {
            self.subscriptions = engine
                .subscription_metadata(client)
                .into_iter()
                .map(|s| SubscriptionView {
                    client_id: client.into(),
                    filter: s.filter,
                    qos: s.qos,
                    no_local: s.no_local,
                    retain_as_published: s.retain_as_published,
                    retain_handling: s.retain_handling,
                    identifier: s.identifier,
                })
                .collect();
        }
    }
}
type Observations = BTreeMap<String, (u64, Arc<Mutex<Observation>>)>;
struct Inner {
    source: Mutex<Option<Weak<dyn Source>>>,
    sessions: Mutex<Observations>,
    started: Mutex<Instant>,
    ready: AtomicBool,
    claimed: AtomicBool,
    requests: Arc<Semaphore>,
    messages_in: AtomicU64,
    messages_out: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
}
/// Opt-in observation handle. Queries use at most eight blocking tasks, fail
/// fast on overload, and never run a storage export or session transition.
#[derive(Clone)]
pub struct ManagementHandle {
    inner: Arc<Inner>,
}
impl fmt::Debug for ManagementHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagementHandle")
            .field("ready", &self.inner.ready.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}
impl Default for ManagementHandle {
    fn default() -> Self {
        Self::new()
    }
}
fn unavailable() -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, "management unavailable")
}
impl ManagementHandle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                source: Mutex::new(None),
                sessions: Mutex::new(BTreeMap::new()),
                started: Mutex::new(Instant::now()),
                ready: AtomicBool::new(false),
                claimed: AtomicBool::new(false),
                requests: Arc::new(Semaphore::new(8)),
                messages_in: AtomicU64::new(0),
                messages_out: AtomicU64::new(0),
                bytes_in: AtomicU64::new(0),
                bytes_out: AtomicU64::new(0),
            }),
        }
    }
    pub(crate) fn claim(&self) -> io::Result<()> {
        self.inner
            .claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "management handle already used",
                )
            })
    }
    pub(crate) fn attach(&self, source: Arc<dyn Source>) -> io::Result<()> {
        let mut slot = self
            .inner
            .source
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if slot.as_ref().and_then(Weak::upgrade).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "management handle already attached",
            ));
        }
        *slot = Some(Arc::downgrade(&source));
        *self
            .inner
            .started
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Instant::now();
        for counter in [
            &self.inner.messages_in,
            &self.inner.messages_out,
            &self.inner.bytes_in,
            &self.inner.bytes_out,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
        self.inner.ready.store(true, Ordering::Release);
        Ok(())
    }
    pub(crate) fn stop(&self) {
        self.inner.ready.store(false, Ordering::Release);
    }
    pub(crate) fn register(
        &self,
        client: &str,
        id: u64,
        engine: &SessionEngine,
    ) -> Arc<Mutex<Observation>> {
        let mut data = Observation::default();
        data.update(engine, client, true);
        let data = Arc::new(Mutex::new(data));
        self.inner
            .sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(client.into(), (id, Arc::clone(&data)));
        data
    }
    pub(crate) fn remove(&self, client: &str, id: u64) {
        let mut sessions = self
            .inner
            .sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if sessions.get(client).is_some_and(|entry| entry.0 == id) {
            sessions.remove(client);
        }
    }
    pub(crate) fn received(&self, publish: bool, bytes: usize) {
        self.inner
            .bytes_in
            .fetch_add(bytes as u64, Ordering::Relaxed);
        if publish {
            self.inner.messages_in.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(crate) fn sent(&self, publish: bool, bytes: usize) {
        self.inner
            .bytes_out
            .fetch_add(bytes as u64, Ordering::Relaxed);
        if publish {
            self.inner.messages_out.fetch_add(1, Ordering::Relaxed);
        }
    }
    async fn read<T: Send + 'static>(
        &self,
        read: impl FnOnce(&Self, &dyn Source) -> io::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        let permit = Arc::clone(&self.inner.requests)
            .try_acquire_owned()
            .map_err(|_| unavailable())?;
        if !self.inner.ready.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        let source = self
            .inner
            .source
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(unavailable)?;
        let handle = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            if !handle.inner.ready.load(Ordering::Acquire) {
                return Err(unavailable());
            }
            let result = read(&handle, &*source);
            if !handle.inner.ready.load(Ordering::Acquire) {
                return Err(unavailable());
            }
            result
        })
        .await
        .map_err(|_| io::Error::other("management query failed"))?
    }
    pub async fn overview(&self) -> io::Result<Overview> {
        self.read(|h, source| {
            let sessions = h
                .inner
                .sessions
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let (mut online, mut offline, mut subscriptions) = (0, 0, 0);
            for (_, data) in sessions.values() {
                let data = data.lock().unwrap_or_else(PoisonError::into_inner);
                if let Some(s) = &data.summary {
                    if s.online {
                        online += 1;
                    } else {
                        offline += 1;
                    }
                    subscriptions += s.subscriptions;
                }
            }
            drop(sessions);
            Ok(Overview {
                ready: true,
                uptime_seconds: h
                    .inner
                    .started
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .elapsed()
                    .as_secs(),
                online_sessions: online,
                offline_sessions: offline,
                subscriptions,
                retained_messages: source
                    .retained(&Query {
                        limit: 1,
                        ..Query::default()
                    })
                    .total,
                messages_in_total: h.inner.messages_in.load(Ordering::Relaxed),
                messages_out_total: h.inner.messages_out.load(Ordering::Relaxed),
                bytes_in_total: h.inner.bytes_in.load(Ordering::Relaxed),
                bytes_out_total: h.inner.bytes_out.load(Ordering::Relaxed),
                state_bytes: source.state_bytes(),
            })
        })
        .await
    }
    pub async fn sessions(&self, query: Query) -> io::Result<Page<SessionView>> {
        query.validate()?;
        self.read(move |h, _| {
            let sessions = h
                .inner
                .sessions
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let mut page = Page {
                items: Vec::with_capacity(query.limit),
                total: 0,
                offset: query.offset,
                limit: query.limit,
            };
            for (_, data) in sessions.values() {
                let data = data.lock().unwrap_or_else(PoisonError::into_inner);
                if let Some(s) = &data.summary
                    && query.matches(s)
                    && (s.client_id.contains(&query.search) || s.principal.contains(&query.search))
                {
                    if page.total >= query.offset && page.items.len() < query.limit {
                        page.items.push(s.clone());
                    }
                    page.total += 1;
                }
            }
            Ok(page)
        })
        .await
    }
    pub async fn session(&self, client_id: &str) -> io::Result<Option<SessionView>> {
        if client_id.len() > 65535 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid client identifier",
            ));
        }
        let client = client_id.to_owned();
        self.read(move |h, _| {
            let sessions = h
                .inner
                .sessions
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            Ok(sessions.get(&client).and_then(|(_, data)| {
                data.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .summary
                    .clone()
            }))
        })
        .await
    }
    pub async fn subscriptions(&self, query: Query) -> io::Result<Page<SubscriptionView>> {
        query.validate()?;
        self.read(move |h, _| {
            let sessions = h
                .inner
                .sessions
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let mut page = Page {
                items: Vec::with_capacity(query.limit),
                total: 0,
                offset: query.offset,
                limit: query.limit,
            };
            for (_, data) in sessions.values() {
                let data = data.lock().unwrap_or_else(PoisonError::into_inner);
                if let Some(s) = &data.summary
                    && query.matches(s)
                {
                    for subscription in &data.subscriptions {
                        if !subscription.client_id.contains(&query.search)
                            && !subscription.filter.contains(&query.search)
                        {
                            continue;
                        }
                        if page.total >= query.offset && page.items.len() < query.limit {
                            page.items.push(subscription.clone());
                        }
                        page.total += 1;
                    }
                }
            }
            Ok(page)
        })
        .await
    }
    pub async fn retained(&self, query: Query) -> io::Result<Page<RetainedView>> {
        query.validate()?;
        self.read(move |_, source| Ok(source.retained(&query)))
            .await
    }
}

#[cfg(test)]
#[path = "handle_tests.rs"]
mod tests;
