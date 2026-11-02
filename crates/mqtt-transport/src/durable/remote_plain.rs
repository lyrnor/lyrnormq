//! Built-in PLAIN and ordinary credentials with HTTP outside provider-manager locks.
use super::*;
use mqtt_auth::{AccessAttempt, Action, Principal, RemoteAccessError};

#[derive(Default)]
pub(super) struct State {
    data: Mutex<Data>,
}
#[derive(Default)]
struct Data {
    pending: HashMap<u64, ConnectRequest>,
    established: HashMap<u64, enhanced::Established>,
    challenges: std::collections::HashSet<u64>,
}
impl State {
    pub(super) fn cancel(&self, generation: u64) {
        let mut data = self.data.lock().unwrap_or_else(PoisonError::into_inner);
        data.pending.remove(&generation);
        data.established.remove(&generation);
        data.challenges.remove(&generation);
    }
    fn contains(&self, generation: u64) -> bool {
        let data = self.data.lock().unwrap_or_else(PoisonError::into_inner);
        data.pending.contains_key(&generation) || data.established.contains_key(&generation)
    }
}
fn method(properties: &Properties) -> Option<&str> {
    properties.iter().find_map(|p| match p {
        Property::AuthenticationMethod(value) => Some(value.as_str()),
        _ => None,
    })
}
fn take_data(properties: &mut Properties) -> Option<Password> {
    let index = properties
        .iter()
        .position(|p| matches!(p, Property::AuthenticationData(_)))?;
    match properties.remove(index) {
        Property::AuthenticationData(bytes) => Some(Password::new(bytes)),
        _ => None,
    }
}
fn credentials<'a>(
    data: &'a [u8],
    expected: Option<&str>,
) -> Result<(&'a str, &'a [u8]), RemoteAccessError> {
    let text = std::str::from_utf8(data).map_err(|_| RemoteAccessError::BadCredentials)?;
    let mut parts = text.split('\0');
    let authorization = parts.next().ok_or(RemoteAccessError::BadCredentials)?;
    let username = parts.next().ok_or(RemoteAccessError::BadCredentials)?;
    let password = parts.next().ok_or(RemoteAccessError::BadCredentials)?;
    if parts.next().is_some()
        || username.is_empty()
        || username.len() > 256
        || password.is_empty()
        || (!authorization.is_empty() && authorization != username)
        || expected.is_some_and(|value| value != username)
    {
        return Err(RemoteAccessError::BadCredentials);
    }
    Ok((username, password.as_bytes()))
}
pub(super) fn handles(
    worker: &Worker,
    request: &Request,
    attempt: Option<&dyn AccessAttempt>,
) -> bool {
    let remote = attempt.is_some_and(AccessAttempt::remote_authentication);
    match request {
        Request::Connect(request) => match method(&request.properties) {
            None => remote,
            Some("PLAIN") => remote && worker.broker.config.remote_plain,
            _ => false,
        },
        Request::Authenticate {
            generation, frame, ..
        } => {
            worker.auth.plain.contains(*generation)
                || (remote
                    && worker.broker.config.remote_plain
                    && method(&frame.properties) == Some("PLAIN"))
        }
        _ => false,
    }
}
pub(super) fn remote_error(error: RemoteAccessError, established: bool) -> io::Error {
    let code = match (error, established) {
        (RemoteAccessError::BadCredentials, false) => ReasonCode::BAD_USER_NAME_OR_PASSWORD,
        (RemoteAccessError::BadCredentials | RemoteAccessError::NotAuthorized, _) => {
            ReasonCode::NOT_AUTHORIZED
        }
        (RemoteAccessError::Busy, true) => ReasonCode::QUOTA_EXCEEDED,
        (RemoteAccessError::Busy, false) => ReasonCode::SERVER_BUSY,
        (RemoteAccessError::Unavailable, false) => ReasonCode::SERVER_UNAVAILABLE,
        (RemoteAccessError::Unavailable, true) => ReasonCode::SERVER_BUSY,
    };
    failure(code, error.to_string())
}
impl Worker {
    async fn remote_identity(
        &self,
        attempt: &Arc<dyn AccessAttempt>,
        info: ConnectInfo<'_>,
    ) -> Result<Principal, RemoteAccessError> {
        if attempt.remote_authentication() {
            return attempt.authenticate(info).await;
        }
        // A PLAIN connection may survive disabling HTTP. Its next exchange uses
        // the actual local verifier, with CPU ownership surviving cancellation.
        let permit = self
            .broker
            .admission
            .authentication_task()
            .ok_or(RemoteAccessError::Busy)?;
        let access = Arc::clone(&self.broker.config.access_control);
        let client = info.client_id.to_owned();
        let username = info.username.map(str::to_owned);
        let password = info.password.map(|value| Password::new(value.to_vec()));
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            access
                .authenticate(ConnectInfo {
                    client_id: &client,
                    username: username.as_deref(),
                    password: password.as_ref().map(Password::as_bytes),
                })
                .map_err(|error| match error {
                    AuthError::BadCredentials => RemoteAccessError::BadCredentials,
                    AuthError::NotAuthorized => RemoteAccessError::NotAuthorized,
                })
        })
        .await
        .map_err(|_| RemoteAccessError::Unavailable)?
    }
    pub(super) async fn remote_step(
        &self,
        request: Request,
        attempt: Arc<dyn AccessAttempt>,
    ) -> io::Result<Authenticated> {
        match request {
            Request::Connect(mut request) => {
                if request.options.version == ProtocolVersion::V5 && request.client.is_empty() {
                    request.client = enhanced::Manager::default()
                        .assigned_identifier(|id| self.broker.contains(id))?;
                    request.assigned = true;
                }
                request.access_attempt = Some(Arc::clone(&attempt));
                if let Some(code) = connect_policy(&request, &self.broker.config) {
                    return Ok(Authenticated::Reply(Box::new(refusal(&request, code))));
                }
                let plain = method(&request.properties) == Some("PLAIN");
                if plain {
                    if request.options.version != ProtocolVersion::V5 || request.password.is_some()
                    {
                        return Ok(Authenticated::Reply(Box::new(Reply::FrameRejected(
                            ReasonCode::PROTOCOL_ERROR,
                        ))));
                    }
                    request.authentication = Some(enhanced::Completion::plain());
                    let data = take_data(&mut request.properties);
                    if let Some(data) = data {
                        return self.remote_plain_initial(request, data, attempt).await;
                    }
                    let deadline = request.deadline.get_or_insert_with(|| {
                        std::time::Instant::now() + self.broker.config.handshake_timeout
                    });
                    if std::time::Instant::now() >= *deadline {
                        return Err(remote_error(RemoteAccessError::Unavailable, false));
                    }
                    let frame = enhanced::plain_frame(0x18)?;
                    enhanced::fitting(
                        &frame,
                        &self.broker.config,
                        request.options.maximum_packet_size as usize,
                    )?;
                    let mut state = self
                        .auth
                        .plain
                        .data
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    if state.pending.len() + state.established.len()
                        >= self.broker.config.max_connections
                    {
                        return Err(remote_error(RemoteAccessError::Busy, false));
                    }
                    state.pending.insert(request.generation, request);
                    return Ok(Authenticated::Reply(Box::new(Reply::AuthContinue(frame))));
                }
                let deadline = request.deadline.unwrap_or_else(|| {
                    std::time::Instant::now() + self.broker.config.handshake_timeout
                });
                let identity = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    self.remote_identity(
                        &attempt,
                        ConnectInfo {
                            client_id: &request.client,
                            username: request.username.as_deref(),
                            password: request.password.as_ref().map(Password::as_bytes),
                        },
                    ),
                )
                .await
                .unwrap_or(Err(RemoteAccessError::Unavailable));
                match identity {
                    Ok(principal) => Ok(Authenticated::Connect {
                        request: Box::new(request),
                        principal,
                    }),
                    Err(error) => {
                        let code = reason(&remote_error(error, false))
                            .unwrap_or(ReasonCode::SERVER_UNAVAILABLE);
                        Ok(Authenticated::Reply(Box::new(refusal(&request, code))))
                    }
                }
            }
            Request::Authenticate {
                generation,
                mut frame,
                deadline,
            } => {
                if frame.version != ProtocolVersion::V5
                    || !matches!(frame.packet, Packet::Auth)
                    || method(&frame.properties) != Some("PLAIN")
                {
                    return Err(failure(
                        ReasonCode::PROTOCOL_ERROR,
                        "invalid PLAIN authentication sequence",
                    ));
                }
                let data = take_data(&mut frame.properties);
                let pending = self
                    .auth
                    .plain
                    .data
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .pending
                    .remove(&generation);
                let pending = if pending.is_none() {
                    self.auth
                        .manager
                        .try_lock()
                        .ok()
                        .and_then(|mut manager| manager.take_plain_pending(generation))
                } else {
                    pending
                };
                if let Some(mut request) = pending {
                    if frame.reason_code.value() != 0x18 {
                        return Err(failure(
                            ReasonCode::PROTOCOL_ERROR,
                            "invalid initial PLAIN continuation",
                        ));
                    }
                    request.access_attempt = Some(Arc::clone(&attempt));
                    return self
                        .remote_plain_initial(
                            request,
                            data.ok_or_else(|| {
                                remote_error(RemoteAccessError::BadCredentials, false)
                            })?,
                            attempt,
                        )
                        .await;
                }
                let established = self
                    .auth
                    .plain
                    .data
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .established
                    .get(&generation)
                    .cloned()
                    .or_else(|| {
                        self.auth
                            .manager
                            .try_lock()
                            .ok()
                            .and_then(|manager| manager.plain_established(generation))
                    })
                    .ok_or_else(|| {
                        failure(
                            ReasonCode::PROTOCOL_ERROR,
                            "PLAIN authentication was not negotiated",
                        )
                    })?;
                if !self.broker.is_current(&established.client, generation) {
                    return Err(failure(
                        ReasonCode::SESSION_TAKEN_OVER,
                        "connection superseded",
                    ));
                }
                match frame.reason_code.value() {
                    0x19 => {
                        let mut state = self
                            .auth
                            .plain
                            .data
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner);
                        if state.challenges.contains(&generation) {
                            return Err(failure(
                                ReasonCode::PROTOCOL_ERROR,
                                "PLAIN reauthentication already pending",
                            ));
                        }
                        if data.is_none() {
                            state.challenges.insert(generation);
                            let frame = enhanced::plain_frame(0x18)?;
                            enhanced::fitting(
                                &frame,
                                &self.broker.config,
                                established.peer_maximum,
                            )?;
                            return Ok(Authenticated::Reply(Box::new(Reply::AuthContinue(frame))));
                        }
                    }
                    0x18 => {
                        if !self
                            .auth
                            .plain
                            .data
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .challenges
                            .remove(&generation)
                        {
                            return Err(failure(
                                ReasonCode::PROTOCOL_ERROR,
                                "unsolicited PLAIN continuation",
                            ));
                        }
                    }
                    _ => {
                        return Err(failure(
                            ReasonCode::PROTOCOL_ERROR,
                            "invalid PLAIN authentication reason",
                        ));
                    }
                }
                let bytes =
                    data.ok_or_else(|| remote_error(RemoteAccessError::BadCredentials, true))?;
                let (username, password) =
                    credentials(bytes.as_bytes(), established.username.as_deref())
                        .map_err(|error| remote_error(error, true))?;
                let principal = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    self.remote_identity(
                        &attempt,
                        ConnectInfo {
                            client_id: &established.client,
                            username: Some(username),
                            password: Some(password),
                        },
                    ),
                )
                .await
                .unwrap_or(Err(RemoteAccessError::Unavailable))
                .map_err(|error| remote_error(error, true))?;
                if principal != established.principal
                    || !attempt.policy().authorize_for_client(
                        &principal,
                        &established.client,
                        Action::Connect,
                        &established.client,
                    )
                {
                    return Err(remote_error(RemoteAccessError::NotAuthorized, true));
                }
                let frame = enhanced::plain_frame(0)?;
                enhanced::fitting(&frame, &self.broker.config, established.peer_maximum)?;
                Ok(Authenticated::Reply(Box::new(Reply::AuthComplete(frame))))
            }
            _ => Err(failure(
                ReasonCode::PROTOCOL_ERROR,
                "unexpected remote authentication request",
            )),
        }
    }
    async fn remote_plain_initial(
        &self,
        mut request: ConnectRequest,
        bytes: Password,
        attempt: Arc<dyn AccessAttempt>,
    ) -> io::Result<Authenticated> {
        let (username, password) = match credentials(bytes.as_bytes(), request.username.as_deref())
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(Authenticated::Reply(Box::new(refusal(
                    &request,
                    reason(&remote_error(error, false))
                        .unwrap_or(ReasonCode::BAD_USER_NAME_OR_PASSWORD),
                ))));
            }
        };
        let deadline = request
            .deadline
            .unwrap_or_else(|| std::time::Instant::now() + self.broker.config.handshake_timeout);
        let identity = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.remote_identity(
                &attempt,
                ConnectInfo {
                    client_id: &request.client,
                    username: Some(username),
                    password: Some(password),
                },
            ),
        )
        .await
        .unwrap_or(Err(RemoteAccessError::Unavailable));
        let principal = match identity {
            Ok(value) => value,
            Err(error) => {
                return Ok(Authenticated::Reply(Box::new(refusal(
                    &request,
                    reason(&remote_error(error, false)).unwrap_or(ReasonCode::SERVER_UNAVAILABLE),
                ))));
            }
        };
        request.username = Some(username.to_owned());
        request.access_attempt = Some(attempt);
        let established = enhanced::Established::from_request(&request, principal.clone());
        self.auth
            .plain
            .data
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .established
            .insert(request.generation, established);
        Ok(Authenticated::Connect {
            request: Box::new(request),
            principal,
        })
    }
}

#[cfg(test)]
#[path = "remote_plain_tests.rs"]
mod tests;
