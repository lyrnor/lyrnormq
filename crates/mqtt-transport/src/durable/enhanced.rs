//! Bounded connection-local authentication state; one manager per connection,
//! driven on the blocking pool.
use super::*;
use mqtt_auth::{
    Action, EnhancedAuthError, EnhancedAuthStep, EnhancedConnectInfo, EnhancedExchange, Principal,
};
use std::time::Instant;

pub(super) struct Completion {
    method: String,
    data: Option<Password>,
}
pub(super) struct Established {
    generation: u64,
    client: String,
    username: Option<String>,
    principal: Principal,
    method: String,
    peer_maximum: usize,
}
impl Established {
    pub(super) fn from_request(request: &ConnectRequest, principal: Principal) -> Self {
        Self {
            generation: request.generation,
            client: request.client.clone(),
            username: request.username.clone(),
            principal,
            peer_maximum: request.options.maximum_packet_size as usize,
            method: request
                .authentication
                .as_ref()
                .map(|a| a.method.clone())
                .unwrap_or_default(),
        }
    }
}
struct Exchange {
    exchange: Box<dyn EnhancedExchange>,
    deadline: Instant,
    steps: u8,
}
struct Pending {
    request: Box<ConnectRequest>,
    state: Exchange,
}
#[derive(Default)]
pub(super) struct Manager {
    pending: HashMap<u64, Pending>,
    established: HashMap<u64, Established>,
    reauth: HashMap<u64, Exchange>,
}
pub(super) enum Processed {
    Ready(Request),
    Reply(Reply),
}
fn reason(error: EnhancedAuthError) -> ReasonCode {
    match error {
        EnhancedAuthError::UnsupportedMethod => ReasonCode::BAD_AUTHENTICATION_METHOD,
        EnhancedAuthError::BadCredentials => ReasonCode::BAD_USER_NAME_OR_PASSWORD,
        EnhancedAuthError::NotAuthorized => ReasonCode::NOT_AUTHORIZED,
        EnhancedAuthError::Protocol => ReasonCode::PROTOCOL_ERROR,
    }
}
fn reauthentication_reason(error: EnhancedAuthError) -> ReasonCode {
    match error {
        EnhancedAuthError::Protocol => ReasonCode::PROTOCOL_ERROR,
        _ => ReasonCode::NOT_AUTHORIZED,
    }
}
fn take_data(properties: &mut Properties) -> Option<Password> {
    let position = properties
        .iter()
        .position(|p| matches!(p, Property::AuthenticationData(_)))?;
    match properties.remove(position) {
        Property::AuthenticationData(data) => Some(Password::new(data)),
        _ => None,
    }
}
fn method(properties: &Properties) -> Option<&str> {
    properties.iter().find_map(|p| match p {
        Property::AuthenticationMethod(value) => Some(value.as_str()),
        _ => None,
    })
}
fn auth_frame(method: &str, code: u8, data: Option<Vec<u8>>) -> io::Result<Frame> {
    let mut frame = Frame::v5(Packet::Auth);
    frame.reason_code = ReasonCode::try_from(code).map_err(io::Error::other)?;
    frame
        .properties
        .push(Property::AuthenticationMethod(method.to_owned()));
    if let Some(data) = data {
        if data.len() > u16::MAX as usize {
            return Err(failure(
                ReasonCode::PACKET_TOO_LARGE,
                "authentication response exceeds limit",
            ));
        }
        frame.properties.push(Property::AuthenticationData(data));
    }
    Ok(frame)
}
fn before_deadline(at: Instant) -> io::Result<()> {
    if Instant::now() >= at {
        Err(failure(
            ReasonCode::NOT_AUTHORIZED,
            "authentication deadline exceeded",
        ))
    } else {
        Ok(())
    }
}
pub(super) fn success_frame(
    request: &ConnectRequest,
    present: bool,
    config: &BrokerConfig,
) -> io::Result<Frame> {
    let mut frame = network::accepted_connack(
        request.options.version,
        present,
        config,
        request.assigned.then_some(request.client.as_str()),
    );
    if let Some(authentication) = &request.authentication {
        frame.properties.push(Property::AuthenticationMethod(
            authentication.method.clone(),
        ));
        if let Some(data) = &authentication.data {
            frame
                .properties
                .push(Property::AuthenticationData(data.as_bytes().to_vec()));
        }
    }
    Ok(frame)
}
fn fitting(frame: &Frame, config: &BrokerConfig, peer: usize) -> io::Result<()> {
    let size = mqtt_protocol::encode_frame(frame)
        .map_err(io::Error::other)?
        .len();
    if size > config.max_packet_size.min(peer) {
        return Err(failure(
            ReasonCode::PACKET_TOO_LARGE,
            "authentication frame exceeds packet maximum",
        ));
    }
    Ok(())
}
impl Manager {
    pub(super) fn install(&mut self, established: Established) {
        self.established.insert(established.generation, established);
    }
    pub(super) fn assigned_identifier(&self, in_use: impl Fn(&str) -> bool) -> io::Result<String> {
        super::assigned_identifier_with(
            |id| {
                in_use(id)
                    || self
                        .pending
                        .values()
                        .any(|pending| pending.request.client == id)
            },
            |bytes| getrandom::getrandom(bytes).map_err(|e| io::Error::other(e.to_string())),
        )
    }
    /// `is_current(client, generation)` reports whether that connection still
    /// owns the client's session.
    pub(super) fn process(
        &mut self,
        request: Request,
        config: &BrokerConfig,
        is_current: impl Fn(&str, u64) -> bool,
    ) -> io::Result<Processed> {
        match request {
            Request::CancelAuthentication { generation } => {
                self.pending.remove(&generation);
                self.established.remove(&generation);
                self.reauth.remove(&generation);
                Ok(Processed::Reply(Reply::Done))
            }
            Request::Connect(mut request) => {
                if self.pending.contains_key(&request.generation)
                    || self.established.contains_key(&request.generation)
                {
                    return Err(failure(
                        ReasonCode::PROTOCOL_ERROR,
                        "CONNECT already received",
                    ));
                }
                let Some(method) = method(&request.properties).map(str::to_owned) else {
                    return Ok(Processed::Ready(Request::Connect(request)));
                };
                if request.password.is_some() {
                    return Ok(Processed::Reply(Reply::FrameRejected(
                        ReasonCode::PROTOCOL_ERROR,
                    )));
                }
                let Some(provider) = &config.enhanced_auth else {
                    return Ok(Processed::Reply(Reply::FrameRejected(
                        ReasonCode::BAD_AUTHENTICATION_METHOD,
                    )));
                };
                let deadline = request
                    .deadline
                    .unwrap_or_else(|| Instant::now() + config.handshake_timeout);
                request.deadline = Some(deadline);
                request.authentication = Some(Completion {
                    method: method.clone(),
                    data: None,
                });
                if let Some(reason) = connect_policy(&request, config) {
                    return Ok(Processed::Reply(Reply::FrameRejected(reason)));
                }
                before_deadline(deadline)?;
                let data = take_data(&mut request.properties);
                let started = provider.start(EnhancedConnectInfo {
                    client_id: &request.client,
                    username: request.username.as_deref(),
                    method: &method,
                    data: data.as_ref().map(Password::as_bytes),
                });
                drop(data);
                before_deadline(deadline)?;
                let (exchange, step) = match started {
                    Ok(result) => result,
                    Err(error) => return Ok(Processed::Reply(Reply::FrameRejected(reason(error)))),
                };
                self.initial_step(
                    request,
                    Exchange {
                        exchange,
                        deadline,
                        steps: 1,
                    },
                    step,
                    config,
                )
            }
            Request::Authenticate {
                generation,
                mut frame,
                deadline,
            } => {
                if frame.version != ProtocolVersion::V5 || !matches!(frame.packet, Packet::Auth) {
                    return Err(failure(ReasonCode::PROTOCOL_ERROR, "AUTH required"));
                }
                if let Some(mut pending) = self.pending.remove(&generation) {
                    let expected = pending
                        .request
                        .authentication
                        .as_ref()
                        .map(|a| a.method.as_str());
                    if method(&frame.properties) != expected || frame.reason_code.value() != 0x18 {
                        return Err(failure(
                            ReasonCode::PROTOCOL_ERROR,
                            "invalid initial authentication sequence",
                        ));
                    }
                    before_deadline(pending.state.deadline)?;
                    pending.state.steps += 1;
                    if pending.state.steps > 8 {
                        return Err(failure(
                            ReasonCode::NOT_AUTHORIZED,
                            "authentication round limit",
                        ));
                    }
                    let data = take_data(&mut frame.properties);
                    let step = pending
                        .state
                        .exchange
                        .step(data.as_ref().map(Password::as_bytes));
                    drop(data);
                    before_deadline(pending.state.deadline)?;
                    let step = match step {
                        Ok(step) => step,
                        Err(error) => {
                            return Ok(Processed::Reply(Reply::FrameRejected(reason(error))));
                        }
                    };
                    return self.initial_step(*pending.request, pending.state, step, config);
                }
                let established = self.established.get(&generation).ok_or_else(|| {
                    failure(ReasonCode::PROTOCOL_ERROR, "AUTH without negotiated method")
                })?;
                if !is_current(&established.client, generation) {
                    return Err(failure(
                        ReasonCode::SESSION_TAKEN_OVER,
                        "connection superseded",
                    ));
                }
                if method(&frame.properties) != Some(established.method.as_str()) {
                    return Err(failure(
                        ReasonCode::PROTOCOL_ERROR,
                        "authentication method changed or missing",
                    ));
                }
                let data = take_data(&mut frame.properties);
                let (state, step) = if frame.reason_code.value() == 0x19 {
                    if self.reauth.contains_key(&generation) {
                        return Err(failure(
                            ReasonCode::PROTOCOL_ERROR,
                            "reauthentication already pending",
                        ));
                    }
                    before_deadline(deadline)?;
                    let provider = config.enhanced_auth.as_ref().ok_or_else(|| {
                        failure(
                            ReasonCode::BAD_AUTHENTICATION_METHOD,
                            "authentication provider disabled",
                        )
                    })?;
                    let (exchange, step) = provider
                        .start(EnhancedConnectInfo {
                            client_id: &established.client,
                            username: established.username.as_deref(),
                            method: &established.method,
                            data: data.as_ref().map(Password::as_bytes),
                        })
                        .map_err(|e| {
                            failure(reauthentication_reason(e), "reauthentication refused")
                        })?;
                    (
                        Exchange {
                            exchange,
                            deadline,
                            steps: 1,
                        },
                        step,
                    )
                } else if frame.reason_code.value() == 0x18 {
                    let mut state = self.reauth.remove(&generation).ok_or_else(|| {
                        failure(ReasonCode::PROTOCOL_ERROR, "unsolicited AUTH continuation")
                    })?;
                    before_deadline(state.deadline)?;
                    state.steps += 1;
                    if state.steps > 8 {
                        return Err(failure(
                            ReasonCode::NOT_AUTHORIZED,
                            "authentication round limit",
                        ));
                    }
                    let step = state
                        .exchange
                        .step(data.as_ref().map(Password::as_bytes))
                        .map_err(|e| {
                            failure(reauthentication_reason(e), "reauthentication refused")
                        })?;
                    (state, step)
                } else {
                    return Err(failure(ReasonCode::PROTOCOL_ERROR, "invalid AUTH reason"));
                };
                drop(data);
                before_deadline(state.deadline)?;
                let (response, continuing) = match step {
                    EnhancedAuthStep::Continue { data } => {
                        (auth_frame(&established.method, 0x18, data)?, true)
                    }
                    EnhancedAuthStep::Complete { principal, data } => {
                        if principal != established.principal
                            || !config.access_control.authorize(
                                &principal,
                                Action::Connect,
                                &established.client,
                            )
                        {
                            return Err(failure(
                                ReasonCode::NOT_AUTHORIZED,
                                "reauthentication cannot change owner",
                            ));
                        }
                        (auth_frame(&established.method, 0, data)?, false)
                    }
                };
                // Network applies the negotiated peer cap; server resource bound is
                // checked before retaining any further exchange state.
                fitting(&response, config, established.peer_maximum)?;
                if continuing {
                    self.reauth.insert(generation, state);
                }
                Ok(Processed::Reply(if continuing {
                    Reply::AuthContinue(response)
                } else {
                    Reply::AuthComplete(response)
                }))
            }
            request => Ok(Processed::Ready(request)),
        }
    }
    fn initial_step(
        &mut self,
        mut request: ConnectRequest,
        state: Exchange,
        step: EnhancedAuthStep,
        config: &BrokerConfig,
    ) -> io::Result<Processed> {
        before_deadline(state.deadline)?;
        match step {
            EnhancedAuthStep::Continue { data } => {
                if self.pending.len() >= config.max_connections {
                    return Err(failure(
                        ReasonCode::SERVER_UNAVAILABLE,
                        "pending authentication capacity",
                    ));
                }
                let method = request
                    .authentication
                    .as_ref()
                    .map(|a| a.method.as_str())
                    .ok_or_else(|| io::Error::other("missing authentication method"))?;
                let frame = auth_frame(method, 0x18, data)?;
                fitting(&frame, config, request.options.maximum_packet_size as usize)?;
                self.pending.insert(
                    request.generation,
                    Pending {
                        request: Box::new(request),
                        state,
                    },
                );
                Ok(Processed::Reply(Reply::AuthContinue(frame)))
            }
            EnhancedAuthStep::Complete { principal, data } => {
                request.authenticated = Some(principal);
                let completion = request
                    .authentication
                    .as_mut()
                    .ok_or_else(|| io::Error::other("missing authentication method"))?;
                completion.data = data.map(Password::new);
                if let Some(code) = connect_policy(&request, config) {
                    return Ok(Processed::Reply(Reply::FrameRejected(code)));
                }
                Ok(Processed::Ready(Request::Connect(request)))
            }
        }
    }
}

#[cfg(test)]
mod tests;
