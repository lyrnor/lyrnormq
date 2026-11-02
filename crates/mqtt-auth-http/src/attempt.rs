use crate::{
    AclAction, AclQuery,
    cache::Receipt,
    client::{username, valid_username},
    runtime::{Generation, Shared},
};
use mqtt_auth::{
    AccessAttempt, AccessControl, AccessFuture, Action, AuthError, ConnectInfo, Principal,
    RemoteAccessError,
};
use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, PoisonError, atomic::Ordering},
    time::Duration,
};
use tokio::sync::OwnedSemaphorePermit;
struct State {
    answers: BTreeMap<AclQuery, Option<Receipt>>,
    error: Option<RemoteAccessError>,
    sealed: bool,
    reserved: usize,
}
struct Inner {
    shared: Arc<Shared>,
    generation: Arc<Generation>,
    epoch: u64,
    state: Mutex<State>,
    deadline: tokio::time::Instant,
    _permit: Option<OwnedSemaphorePermit>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        let bytes = self
            .state
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .reserved;
        self.shared.attempt_bytes.fetch_sub(bytes, Ordering::AcqRel);
    }
}
pub(crate) struct Attempt {
    inner: Arc<Inner>,
}
struct Policy {
    inner: Arc<Inner>,
}
impl fmt::Debug for Attempt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccessAttempt").finish_non_exhaustive()
    }
}
impl fmt::Debug for Policy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccessProbe").finish_non_exhaustive()
    }
}
impl Attempt {
    pub fn new(shared: Arc<Shared>, generation: Arc<Generation>) -> Self {
        let remote = generation.authentication.is_some() || generation.authorization.is_some();
        let permit = remote
            .then(|| shared.attempts.clone().try_acquire_owned().ok())
            .flatten();
        let error = (remote && permit.is_none()).then_some(RemoteAccessError::Busy);
        let epoch = shared.cache.epoch();
        Self {
            inner: Arc::new(Inner {
                shared,
                generation,
                epoch,
                state: Mutex::new(State {
                    answers: BTreeMap::new(),
                    error,
                    sealed: false,
                    reserved: 0,
                }),
                deadline: tokio::time::Instant::now() + Duration::from_secs(5),
                _permit: permit,
            }),
        }
    }
}
impl Inner {
    fn current(&self) -> bool {
        !self.shared.stopped.load(Ordering::Acquire)
            && self
                .shared
                .current
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .id
                == self.generation.id
            && (!self.generation.config.authorization.backend.enabled
                || self.shared.cache.epoch() == self.epoch)
    }
    fn deadline_expired(&self) -> bool {
        (self.generation.config.authentication.enabled
            || self.generation.config.authorization.backend.enabled)
            && tokio::time::Instant::now() >= self.deadline
    }
    fn reserve(&self, bytes: usize) -> bool {
        let mut n = self.shared.attempt_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = n.checked_add(bytes).filter(|n| *n <= 64 * 1024 * 1024) else {
                return false;
            };
            match self.shared.attempt_bytes.compare_exchange_weak(
                n,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => n = actual,
            }
        }
    }
    fn authorize(
        &self,
        principal: &Principal,
        client: &str,
        action: Action,
        resource: &str,
    ) -> bool {
        let config = &self.generation.config;
        if matches!(action, Action::Connect) {
            return if config.authentication.enabled
                && (config.authorization.backend.enabled || !self.shared.local_accounts)
            {
                username(principal).is_some()
            } else if self.shared.local_accounts {
                self.shared
                    .fallback
                    .authorize_for_client(principal, client, action, resource)
            } else if config.authentication.enabled {
                username(principal).is_some()
            } else {
                self.shared
                    .fallback
                    .authorize_for_client(principal, client, action, resource)
            };
        }
        if !config.authorization.backend.enabled {
            return if config.authentication.enabled && !self.shared.local_accounts {
                username(principal).is_some()
            } else {
                self.shared
                    .fallback
                    .authorize_for_client(principal, client, action, resource)
            };
        }
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.error.is_some() {
            return true;
        }
        let Some(username) = username(principal) else {
            state.error = Some(RemoteAccessError::Unavailable);
            return true;
        };
        let query = AclQuery {
            client_id: client.into(),
            username: username.into(),
            action: if matches!(action, Action::Publish) {
                AclAction::Publish
            } else {
                AclAction::Subscribe
            },
            topic: resource.into(),
        };
        if !query.validate() {
            state.error = Some(RemoteAccessError::Unavailable);
            return true;
        }
        if let Some(Some(receipt)) = state.answers.get(&query)
            && (state.sealed || receipt.valid())
        {
            return receipt.allowed;
        }
        if state.sealed {
            state.error = Some(RemoteAccessError::Unavailable);
            return true;
        }
        if !state.answers.contains_key(&query) {
            let bytes = query.size().saturating_mul(2);
            if state.answers.len() >= 4096
                || state.reserved.saturating_add(bytes) > 1024 * 1024
                || !self.reserve(bytes)
            {
                state.error = Some(RemoteAccessError::Busy);
                return true;
            }
            state.reserved += bytes;
            let receipt = self.shared.cache.lookup(self.generation.id, &query);
            state.answers.insert(query.clone(), receipt);
        }
        state
            .answers
            .get(&query)
            .and_then(Option::as_ref)
            .filter(|r| r.valid())
            .is_none_or(|r| r.allowed)
    }
}
impl AccessControl for Policy {
    fn reconciles_stored_authorization(&self) -> bool {
        !self.inner.generation.config.authorization.backend.enabled
    }
    fn authenticate(&self, info: ConnectInfo<'_>) -> Result<Principal, AuthError> {
        if self.inner.generation.config.authentication.enabled {
            Err(AuthError::BadCredentials)
        } else {
            self.inner.shared.fallback.authenticate(info)
        }
    }
    fn authorize(&self, p: &Principal, a: Action, r: &str) -> bool {
        self.inner
            .authorize(p, if matches!(a, Action::Connect) { r } else { "" }, a, r)
    }
    fn authorize_for_client(&self, p: &Principal, c: &str, a: Action, r: &str) -> bool {
        self.inner.authorize(p, c, a, r)
    }
}
impl AccessAttempt for Attempt {
    fn policy(&self) -> Arc<dyn AccessControl> {
        Arc::new(Policy {
            inner: self.inner.clone(),
        })
    }
    fn remote_authentication(&self) -> bool {
        self.inner.generation.config.authentication.enabled
    }
    fn authenticate<'a>(
        &'a self,
        info: ConnectInfo<'a>,
    ) -> AccessFuture<'a, Result<Principal, RemoteAccessError>> {
        Box::pin(async move {
            if !self.inner.current() {
                return Err(RemoteAccessError::Unavailable);
            }
            let Some(client) = &self.inner.generation.authentication else {
                return Err(RemoteAccessError::NotAuthorized);
            };
            let name = info
                .username
                .filter(|s| valid_username(s))
                .ok_or(RemoteAccessError::BadCredentials)?
                .to_owned();
            let result = client
                .authenticate(info, true)
                .await
                .map_err(|_| RemoteAccessError::BadCredentials)?;
            if !self.inner.current() {
                return Err(RemoteAccessError::Unavailable);
            }
            match result.result {
                "allow" => Ok(Principal {
                    name: format!("user:{name}"),
                }),
                "deny" => Err(RemoteAccessError::BadCredentials),
                _ => Err(if result.error_code == Some("busy") {
                    RemoteAccessError::Busy
                } else {
                    RemoteAccessError::Unavailable
                }),
            }
        })
    }
    fn pending(&self) -> bool {
        let s = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        s.error.is_some()
            || s.answers
                .values()
                .any(|r| r.as_ref().is_none_or(|r| !s.sealed && !r.valid()))
    }
    fn resolve(&self) -> AccessFuture<'_, Result<(), RemoteAccessError>> {
        Box::pin(async move {
            if !self.inner.current() || self.inner.deadline_expired() {
                return Err(RemoteAccessError::Unavailable);
            }
            let missing = {
                let s = self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if let Some(error) = s.error {
                    return Err(error);
                }
                s.answers
                    .iter()
                    .filter(|(_, r)| r.as_ref().is_none_or(|r| !r.valid()))
                    .map(|(q, _)| q.clone())
                    .collect::<Vec<_>>()
            };
            if missing.is_empty() {
                return Ok(());
            }
            let Some(client) = &self.inner.generation.authorization else {
                return Err(RemoteAccessError::Unavailable);
            };
            for batch in missing.chunks(16) {
                let mut tasks = tokio::task::JoinSet::new();
                for query in batch {
                    let shared = self.inner.shared.clone();
                    let generation = self.inner.generation.clone();
                    let query = query.clone();
                    let epoch = self.inner.epoch;
                    tasks.spawn(async move {
                        let client = generation
                            .authorization
                            .as_ref()
                            .ok_or(RemoteAccessError::Unavailable)?;
                        let receipt = shared
                            .cache
                            .resolve(
                                generation.id,
                                epoch,
                                query.clone(),
                                client,
                                generation.config.authorization.cache_ttl_seconds,
                            )
                            .await?;
                        Ok::<_, RemoteAccessError>((query, receipt))
                    });
                }
                while let Some(result) =
                    tokio::time::timeout_at(self.inner.deadline, tasks.join_next())
                        .await
                        .map_err(|_| RemoteAccessError::Unavailable)?
                {
                    let (query, receipt) = result.map_err(|_| RemoteAccessError::Unavailable)??;
                    self.inner
                        .state
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .answers
                        .insert(query, Some(receipt));
                }
            }
            let _ = client;
            if !self.inner.current() {
                return Err(RemoteAccessError::Unavailable);
            }
            Ok(())
        })
    }
    fn seal(&self) -> bool {
        let mut s = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if s.error.is_some() {
            return false;
        }
        if s.sealed {
            return true;
        }
        if !self.inner.current()
            || self.inner.deadline_expired()
            || s.answers.values().any(|r| {
                r.as_ref()
                    .is_none_or(|r| r.epoch != self.inner.epoch || !r.valid())
            })
        {
            return false;
        }
        s.sealed = true;
        true
    }
}
