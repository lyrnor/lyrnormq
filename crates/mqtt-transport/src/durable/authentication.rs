//! Blocking provider ownership and cancellation. Only short registry locks are
//! touched on the executor; the manager lock lives inside bounded blocking jobs.
use super::*;
use std::sync::{
    TryLockError,
    atomic::{AtomicBool, Ordering},
};
#[derive(Default)]
struct Entry {
    cancelled: AtomicBool,
    established: AtomicBool,
    username: Mutex<Option<String>>,
    handoff: Mutex<Option<oneshot::Sender<bool>>>,
}
impl Entry {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(handoff) = self
            .handoff
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            let _ = handoff.send(false);
        }
    }
}
#[derive(Default)]
pub(super) struct State {
    pub(super) manager: Mutex<enhanced::Manager>,
    pub(super) plain: remote_plain::State,
    entries: Mutex<HashMap<u64, Arc<Entry>>>,
}
impl State {
    fn entry(&self, generation: u64, limit: usize) -> io::Result<Arc<Entry>> {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(entry) = entries.get(&generation) {
            return Ok(entry.clone());
        }
        if entries.len() >= limit {
            return Err(failure(
                ReasonCode::SERVER_BUSY,
                "authentication state capacity",
            ));
        }
        let entry = Arc::new(Entry::default());
        entries.insert(generation, entry.clone());
        Ok(entry)
    }
    pub(super) fn username(&self, generation: u64) -> Option<String> {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&generation)
            .and_then(|entry| {
                entry
                    .username
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .clone()
            })
    }
    fn established(&self, generation: u64) -> bool {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&generation)
            .is_some_and(|entry| entry.established.load(Ordering::Acquire))
    }
    fn sweep(&self, manager: &mut enhanced::Manager) {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|generation, entry| {
                if entry.cancelled.load(Ordering::Acquire) {
                    manager.cancel(*generation);
                    false
                } else {
                    true
                }
            });
    }
    pub(super) fn cancel(&self, generation: u64) {
        self.plain.cancel(generation);
        if let Some(entry) = self
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&generation)
        {
            entry.cancel();
        }
        // Never enqueue cleanup and never wait behind a running hook. That job
        // sweeps cancelled state before releasing its manager/CPU ownership.
        match self.manager.try_lock() {
            Ok(mut manager) => self.sweep(&mut manager),
            Err(TryLockError::Poisoned(error)) => self.sweep(&mut error.into_inner()),
            Err(TryLockError::WouldBlock) => {}
        }
    }
}
impl Drop for State {
    fn drop(&mut self) {
        for entry in self
            .entries
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
        {
            entry.cancel();
        }
    }
}
struct CancelOnDrop {
    state: Arc<State>,
    entry: Arc<Entry>,
    generation: u64,
    armed: bool,
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.entry.cancel();
            self.state.cancel(self.generation);
        }
    }
}
impl Worker {
    pub(super) fn adopt_connection(&self, generation: u64) {
        let entry = self
            .auth
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&generation)
            .cloned();
        if let Some(entry) = entry
            && !entry.cancelled.load(Ordering::Acquire)
            && let Some(handoff) = entry
                .handoff
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
        {
            let _ = handoff.send(true);
        }
    }
    pub(super) fn release_handshake(&self) {
        if let Some(slot) = &self.handshake {
            slot.lock().unwrap_or_else(PoisonError::into_inner).take();
        }
    }
    pub(super) async fn authenticate(&self, mut request: Request) -> io::Result<Reply> {
        let generation = match &request {
            Request::Connect(request) => request.generation,
            Request::Authenticate { generation, .. } => *generation,
            Request::CancelAuthentication { generation } => {
                self.auth.cancel(*generation);
                return Ok(Reply::Done);
            }
            _ => return Err(io::Error::other("unexpected authentication request")),
        };
        let busy = || match &request {
            Request::Connect(request) => Ok(refusal(request, ReasonCode::SERVER_BUSY)),
            _ if self.auth.established(generation) => Err(failure(
                ReasonCode::QUOTA_EXCEEDED,
                "reauthentication admission exhausted",
            )),
            _ => Ok(Reply::FrameRejected(ReasonCode::SERVER_BUSY)),
        };
        let attempt = self
            .broker
            .config
            .remote_access
            .as_ref()
            .map(|remote| remote.begin());
        let remote = remote_plain::handles(self, &request, attempt.as_deref());
        let permit = if remote {
            if !self.broker.admission.charge_authentication() {
                return busy();
            }
            None
        } else {
            let Some(permit) = self.broker.admission.authentication() else {
                return busy();
            };
            Some(permit)
        };
        let entry = match self
            .auth
            .entry(generation, self.broker.config.max_connections)
        {
            Ok(entry) => entry,
            Err(_) => return busy(),
        };
        if let Request::Connect(request) = &mut request {
            request.access_attempt = attempt.clone();
        }
        let mut guard = CancelOnDrop {
            state: self.auth.clone(),
            entry: entry.clone(),
            generation,
            armed: true,
        };
        let broker = self.broker.clone();
        let auth = self.auth.clone();
        let job_entry = entry.clone();
        let step = if remote {
            self.remote_step(
                request,
                attempt.clone().ok_or_else(|| {
                    failure(ReasonCode::SERVER_UNAVAILABLE, "remote access stopped")
                })?,
            )
            .await?
        } else {
            tokio::task::spawn_blocking(move || -> io::Result<Authenticated> {
                let _permit = permit; // The JOB owns this even after its caller disappears.
                let mut manager = auth.manager.lock().unwrap_or_else(PoisonError::into_inner);
                auth.sweep(&mut manager);
                if job_entry.cancelled.load(Ordering::Acquire) {
                    return Err(failure(
                        ReasonCode::SERVER_UNAVAILABLE,
                        "authentication cancelled",
                    ));
                }
                let outcome = (|| {
                    let mut request = request;
                    if let Request::Connect(connect) = &mut request
                        && connect.options.version == ProtocolVersion::V5
                        && connect.client.is_empty()
                    {
                        connect.client = manager.assigned_identifier(|id| broker.contains(id))?;
                        connect.assigned = true;
                    }
                    let config = &broker.config;
                    let mut request =
                        match manager.process(request, config, |client, generation| {
                            broker.is_current(client, generation)
                        })? {
                            enhanced::Processed::Ready(Request::Connect(request)) => request,
                            enhanced::Processed::Ready(_) => {
                                return Err(io::Error::other("unprocessed authentication command"));
                            }
                            enhanced::Processed::Reply(reply) => {
                                return Ok(Authenticated::Reply(Box::new(reply)));
                            }
                        };
                    if let Some(reason) = connect_policy(&request, config) {
                        return Ok(Authenticated::Reply(Box::new(refusal(&request, reason))));
                    }
                    let authenticated = request.authenticated.take().map(Ok).unwrap_or_else(|| {
                        config.access_control.authenticate(ConnectInfo {
                            client_id: &request.client,
                            username: request.username.as_deref(),
                            password: request.password.as_ref().map(Password::as_bytes),
                        })
                    });
                    let principal = match authenticated {
                        Ok(principal) => principal,
                        Err(error) => {
                            return Ok(Authenticated::Reply(Box::new(refusal(
                                &request,
                                match error {
                                    AuthError::BadCredentials => {
                                        ReasonCode::BAD_USER_NAME_OR_PASSWORD
                                    }
                                    AuthError::NotAuthorized => ReasonCode::NOT_AUTHORIZED,
                                },
                            ))));
                        }
                    };
                    if request.authentication.is_some() {
                        manager.install(enhanced::Established::from_request(
                            &request,
                            principal.clone(),
                        ));
                    }
                    Ok(Authenticated::Connect {
                        request: Box::new(request),
                        principal,
                    })
                })();
                auth.sweep(&mut manager);
                if job_entry.cancelled.load(Ordering::Acquire) {
                    manager.cancel(generation);
                    return Err(failure(
                        ReasonCode::SERVER_UNAVAILABLE,
                        "authentication cancelled",
                    ));
                }
                outcome
            })
            .await
            .map_err(io::Error::other)??
        };
        if matches!(&step,Authenticated::Reply(reply) if matches!(**reply,Reply::AuthComplete(_)))
            && attempt.as_ref().is_some_and(|attempt| !attempt.seal())
        {
            return Err(failure(
                ReasonCode::SERVER_BUSY,
                "access policy changed during authentication",
            ));
        }
        if entry.cancelled.load(Ordering::Acquire) {
            return Err(failure(
                ReasonCode::SERVER_UNAVAILABLE,
                "authentication cancelled",
            ));
        }
        let reply = match step {
            Authenticated::Reply(reply) => *reply,
            Authenticated::Connect {
                mut request,
                principal,
            } => {
                if self.broker.config.cluster.is_some()
                    && (request.will.is_some()
                        || (!self.broker.durable_cluster()
                            && ((request.options.version == ProtocolVersion::V311
                                && !request.clean)
                                || request.options.session_expiry_interval != 0)))
                {
                    return Ok(refusal(
                        &request,
                        if request.options.version == ProtocolVersion::V311 {
                            ReasonCode::SERVER_UNAVAILABLE
                        } else {
                            ReasonCode::IMPLEMENTATION_SPECIFIC_ERROR
                        },
                    ));
                }
                if let Some(reason) = admit_will(&request, &principal).await {
                    return Ok(refusal(&request, reason));
                }
                if entry.cancelled.load(Ordering::Acquire) {
                    return Err(failure(
                        ReasonCode::SERVER_UNAVAILABLE,
                        "authentication cancelled",
                    ));
                }
                *entry
                    .username
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = if principal.name == "anonymous" {
                    None
                } else {
                    principal
                        .name
                        .strip_prefix("user:")
                        .map(str::to_owned)
                        .or_else(|| request.username.clone())
                };
                if let Some(cluster) = &self.broker.config.cluster {
                    if !cluster.ready() {
                        return Ok(refusal(&request, ReasonCode::SERVER_UNAVAILABLE));
                    }
                    self.broker.cluster_state.register(
                        &request.client,
                        &Online {
                            generation: request.generation,
                            wake: request.wake.clone(),
                            cancel: request.cancel.clone(),
                            close_reason: request.close_reason.clone(),
                        },
                    );
                    let claimed = tokio::time::timeout(Duration::from_secs(5), async {
                        if cluster.durable() {
                            for _ in 0..3 {
                                let inspected =
                                    cluster.inspect_session(request.client.clone()).await?;
                                let access = attempt
                                    .as_ref()
                                    .map(|attempt| attempt.admitted_policy())
                                    .unwrap_or_else(|| {
                                        Arc::clone(&self.broker.config.access_control)
                                    });
                                let mut candidate = match &inspected.snapshot {
                                    Some(snapshot) => SessionEngine::restore_partition(
                                        self.broker.session_config,
                                        snapshot,
                                        access,
                                        Arc::clone(&self.broker.clock),
                                    ),
                                    None => SessionEngine::new_partition(
                                        self.broker.session_config,
                                        access,
                                        Arc::clone(&self.broker.clock),
                                    ),
                                }
                                .map_err(model_error)?;
                                candidate
                                    .connect_with_options(
                                        &request.client,
                                        request.will.clone(),
                                        principal.clone(),
                                        request.options.clone(),
                                    )
                                    .map_err(model_error)?;
                                match cluster
                                    .acquire_session(
                                        request.client.clone(),
                                        request.generation,
                                        inspected.epoch,
                                    )
                                    .await
                                {
                                    Ok(lease) => {
                                        request.cluster_lease = Some(lease);
                                        return Ok(());
                                    }
                                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                                        continue;
                                    }
                                    Err(error) => return Err(error),
                                }
                            }
                            Err(failure(
                                ReasonCode::SERVER_UNAVAILABLE,
                                "session ownership changed during admission",
                            ))
                        } else {
                            cluster
                                .claim(request.client.clone(), request.generation)
                                .await
                        }
                    })
                    .await;
                    if !matches!(claimed, Ok(Ok(()))) || *request.cancel.borrow() {
                        self.broker
                            .cluster_state
                            .remove(&request.client, request.generation);
                        let _ = tokio::time::timeout(
                            Duration::from_secs(5),
                            cluster.release(request.client.clone(), request.generation),
                        )
                        .await;
                        let refusal_reason = match claimed {
                            Ok(Err(error))
                                if reason(&error) == Some(ReasonCode::NOT_AUTHORIZED) =>
                            {
                                ReasonCode::NOT_AUTHORIZED
                            }
                            _ => ReasonCode::SERVER_UNAVAILABLE,
                        };
                        return Ok(refusal(&request, refusal_reason));
                    }
                }
                let client = request.client.clone();
                let overload = refusal(&request, ReasonCode::SERVER_BUSY);
                let (adopt, adopted) = oneshot::channel();
                *entry.handoff.lock().unwrap_or_else(PoisonError::into_inner) = Some(adopt);
                let (response, reply) = oneshot::channel();
                let broker = self.broker.clone();
                let lifecycle = entry.clone();
                let slot = self.handshake.clone();
                let claimed_client = client.clone();
                let dispatch = self.broker.spawn_handoff(async move {
                    // The original pre-CONNACK slot bounds late-commit cleanup,
                    // even after all connection/Worker handles have disappeared.
                    let _slot = slot;
                    if lifecycle.cancelled.load(Ordering::Acquire) {
                        broker.release_cluster(&client, generation).await;
                        let _ = response.send(Err(failure(
                            ReasonCode::SERVER_UNAVAILABLE,
                            "authentication cancelled",
                        )));
                        return;
                    }
                    let outcome = broker
                        .call(&client, true, Request::Connect(*request), Some(principal))
                        .await;
                    let installed = matches!(
                        &outcome,
                        Ok(Reply::Connected(_)
                            | Reply::Assigned { .. }
                            | Reply::Authenticated { .. })
                    );
                    if installed {
                        lifecycle.established.store(true, Ordering::Release);
                    }
                    if !installed && let Some(cluster) = &broker.config.cluster {
                        broker.cluster_state.remove(&client, generation);
                        let _ = tokio::time::timeout(
                            Duration::from_secs(5),
                            cluster.release(client.clone(), generation),
                        )
                        .await;
                    }
                    let delivered = response.send(outcome).is_ok();
                    let adopted = if installed && delivered {
                        matches!(adopted.await, Ok(true))
                    } else {
                        false
                    };
                    if installed
                        && !adopted
                        && let Err(error) = broker
                            .call(
                                &client,
                                false,
                                Request::Disconnect {
                                    client: client.clone(),
                                    generation,
                                    graceful: false,
                                    expiry: None,
                                },
                                None,
                            )
                            .await
                        && reason(&error) != Some(ReasonCode::SESSION_TAKEN_OVER)
                    {
                        broker.fail(format!("cancelled CONNECT cleanup failed: {error}"));
                    }
                });
                if let Err(error) = dispatch {
                    self.broker
                        .release_cluster(&claimed_client, generation)
                        .await;
                    if reason(&error) == Some(ReasonCode::SERVER_BUSY) {
                        return Ok(overload);
                    }
                    return Err(error);
                }
                let reply = reply.await.map_err(|_| {
                    failure(ReasonCode::SERVER_UNAVAILABLE, "CONNECT handoff stopped")
                })??;
                // Internal test callers have no connection identity to adopt it.
                // Real sockets always hold a handshake slot and adopt explicitly
                // immediately after network records the resolved identity.
                if self.handshake.is_none() && !entry.cancelled.load(Ordering::Acquire) {
                    self.adopt_connection(generation);
                }
                reply
            }
        };
        if entry.cancelled.load(Ordering::Acquire) {
            return Err(failure(
                ReasonCode::SERVER_UNAVAILABLE,
                "authentication cancelled",
            ));
        }
        guard.armed = false;
        Ok(reply)
    }
}

/// A new Will is a future publish admitted exactly once, after credentials pass.
/// Expiry, recovery and control packets use the accepted non-probing snapshot.
async fn admit_will(
    request: &ConnectRequest,
    principal: &mqtt_auth::Principal,
) -> Option<ReasonCode> {
    let (Some(will), Some(attempt)) = (&request.will, &request.access_attempt) else {
        return None;
    };
    let unavailable = || Some(ReasonCode::SERVER_UNAVAILABLE);
    for _ in 0..2 {
        if *request.cancel.borrow()
            || request
                .deadline
                .is_some_and(|at| std::time::Instant::now() >= at)
        {
            return unavailable();
        }
        let allowed = attempt.policy().authorize_for_client(
            principal,
            &request.client,
            mqtt_auth::Action::Publish,
            &will.topic,
        );
        if !attempt.pending() {
            return if allowed {
                None // The actor seals all authentication and Will receipts together.
            } else if attempt.seal() {
                Some(ReasonCode::NOT_AUTHORIZED)
            } else {
                unavailable()
            };
        }
        let resolved = match request.deadline {
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                tokio::time::timeout(remaining, attempt.resolve())
                    .await
                    .ok()
                    .is_some_and(|result| result.is_ok())
            }
            None => attempt.resolve().await.is_ok(),
        };
        if !resolved {
            return unavailable();
        }
    }
    unavailable()
}
