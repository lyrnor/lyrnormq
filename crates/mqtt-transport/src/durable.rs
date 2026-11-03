//! Per-session durable actors (Phase4): each client identifier is owned by one
//! tokio task that proposes, commits and installs its own partition before
//! exposing packets. Connection tasks perform socket IO only.
use crate::BrokerConfig;
use mqtt_auth::{AuthError, ConnectInfo};
use mqtt_protocol::{
    Frame, Packet, Password, Properties, Property, ProtocolVersion, ReasonCode, Will,
};
#[cfg(test)]
use mqtt_protocol::{ProtocolError, decode, encode};
use mqtt_session::durable::{
    ConnectionOptions, Delivery, EngineError, SessionConfig, SessionEngine, SystemClock,
};
use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};
#[cfg(test)]
use tokio::net::TcpStream;
use tokio::{
    net::TcpListener,
    sync::{Semaphore, mpsc, oneshot, watch},
    task::JoinSet,
    time::MissedTickBehavior,
};

mod actor;
mod aliases;
mod authentication;
mod broker;
mod enhanced;
pub(crate) mod export;
mod network;
mod operation_access;
mod publisher;
pub use publisher::{PublishHandle, PublishMessage};
mod remote_plain;
mod system_events;
#[cfg(test)]
mod system_events_tests;
#[cfg(test)]
use mqtt_session::durable::Clock;
#[cfg(test)]
use mqtt_storage::durable::SqliteStore;
#[cfg(test)]
use tokio::time::timeout;

use broker::Broker;

#[derive(Debug)]
struct WorkerFailure {
    reason: ReasonCode,
    message: String,
}
impl std::fmt::Display for WorkerFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for WorkerFailure {}
fn failure(reason: ReasonCode, message: impl Into<String>) -> io::Error {
    io::Error::other(WorkerFailure {
        reason,
        message: message.into(),
    })
}
fn reason(error: &io::Error) -> Option<ReasonCode> {
    error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<WorkerFailure>())
        .map(|failure| failure.reason)
}
fn model_error(error: EngineError) -> io::Error {
    let reason = match &error {
        EngineError::Mqtt5(reason) => *reason,
        EngineError::NotAuthorized => ReasonCode::NOT_AUTHORIZED,
        EngineError::Capacity
        | EngineError::Retained(mqtt_router::retained::RetainedError::CapacityExceeded) => {
            ReasonCode::QUOTA_EXCEEDED
        }
        EngineError::Qos(mqtt_session::SessionError::InflightFull) => {
            ReasonCode::RECEIVE_MAXIMUM_EXCEEDED
        }
        EngineError::Qos(mqtt_session::SessionError::MessageTooLarge) => {
            ReasonCode::PACKET_TOO_LARGE
        }
        _ => ReasonCode::PROTOCOL_ERROR,
    };
    failure(reason, error.to_string())
}
type ResultReply = Result<Reply, io::Error>;
enum Operation {
    ConnectFrameAt {
        frame: Frame,
        generation: u64,
        wake: watch::Sender<u64>,
        cancel: watch::Sender<bool>,
        close_reason: watch::Sender<Option<ReasonCode>>,
        deadline: std::time::Instant,
    },
    Authenticate {
        generation: u64,
        frame: Frame,
        deadline: std::time::Instant,
    },
    CancelAuthentication {
        generation: u64,
    },
    #[cfg(test)]
    ConnectFrame {
        frame: Frame,
        generation: u64,
        wake: watch::Sender<u64>,
        cancel: watch::Sender<bool>,
        close_reason: watch::Sender<Option<ReasonCode>>,
    },
    HandleFrameAt {
        client: String,
        generation: u64,
        frame: Frame,
        received_at_millis: u64,
    },
    PollDelivery {
        client: String,
        generation: u64,
    },
    BeginDelivery {
        client: String,
        generation: u64,
        delivery: Delivery,
    },
    DisconnectOptions {
        client: String,
        generation: u64,
        graceful: bool,
        expiry: Option<u32>,
    },
    #[cfg(test)]
    Connect {
        client: String,
        clean: bool,
        will: Option<Will>,
        username: Option<String>,
        password: Option<Password>,
        generation: u64,
        wake: watch::Sender<u64>,
        cancel: watch::Sender<bool>,
    },
    #[cfg(test)]
    Handle {
        client: String,
        generation: u64,
        packet: Packet,
    },
    #[cfg(test)]
    Poll {
        client: String,
        generation: u64,
    },
    Disconnect {
        client: String,
        generation: u64,
        graceful: bool,
    },
    /// Run every actor's timer work now.
    #[cfg(test)]
    Tick,
}
#[derive(Clone, Copy, Debug)]
enum ConnectRefusal {
    BadCredentials,
    NotAuthorized,
    ServerBusy,
}
impl ConnectRefusal {
    fn code(self) -> u8 {
        match self {
            Self::BadCredentials => 4,
            Self::NotAuthorized => 5,
            Self::ServerBusy => 3,
        }
    }
}
impl From<AuthError> for ConnectRefusal {
    fn from(error: AuthError) -> Self {
        match error {
            AuthError::BadCredentials => Self::BadCredentials,
            AuthError::NotAuthorized => Self::NotAuthorized,
        }
    }
}
enum Reply {
    AuthContinue(Frame),
    AuthComplete(Frame),
    Authenticated {
        client: String,
        frame: Frame,
    },
    FrameRejected(ReasonCode),
    Frames(Vec<Frame>),
    FramePolled(Option<Box<Frame>>),
    DeliveryPolled(Option<Box<Delivery>>),
    Connected(bool),
    Assigned {
        present: bool,
        client: String,
    },
    Rejected(ConnectRefusal),
    #[cfg(test)]
    Packets(Vec<Packet>),
    #[cfg(test)]
    Polled(Option<Packet>),
    Done,
}
struct Online {
    close_reason: Option<watch::Sender<Option<ReasonCode>>>,
    generation: u64,
    wake: watch::Sender<u64>,
    cancel: watch::Sender<bool>,
}

/// One connection's handle to the broker. Session requests are routed to the
/// owning actor; authentication state is connection-local.
#[derive(Clone)]
struct Worker {
    broker: Arc<Broker>,
    auth: Arc<authentication::State>,
    handshake: Option<Arc<Mutex<Option<tokio::sync::OwnedSemaphorePermit>>>>,
}

enum Authenticated {
    Reply(Box<Reply>),
    Connect {
        request: Box<ConnectRequest>,
        principal: mqtt_auth::Principal,
    },
}

impl Worker {
    fn new(broker: Arc<Broker>) -> Self {
        Self {
            broker,
            auth: Arc::new(authentication::State::default()),
            handshake: None,
        }
    }

    async fn request(&self, operation: Operation) -> io::Result<Reply> {
        #[cfg(test)]
        if let Operation::Tick = operation {
            self.broker.tick_all().await;
            return Ok(Reply::Done);
        }
        let request = normalize(operation)?;
        let client = match &request {
            Request::Connect(_)
            | Request::Authenticate { .. }
            | Request::CancelAuthentication { .. } => return self.authenticate(request).await,
            Request::Handle { client, .. }
            | Request::PollDelivery { client, .. }
            | Request::BeginDelivery { client, .. }
            | Request::Disconnect { client, .. } => client.clone(),
            #[cfg(test)]
            Request::Poll { client, .. } => client.clone(),
        };
        let disconnect = matches!(request, Request::Disconnect { .. });
        match self.broker.call(&client, false, request, None).await {
            // Only an absent or superseded session is a no-op; model errors stand.
            Err(error) if disconnect && reason(&error) == Some(ReasonCode::SESSION_TAKEN_OVER) => {
                Ok(Reply::Done)
            }
            result => result,
        }
    }
}
fn session_config(config: &BrokerConfig) -> SessionConfig {
    SessionConfig {
        max_sessions: config.max_sessions,
        max_retained: config.max_retained,
        max_subscriptions: config.max_subscriptions,
        max_queued: config.outbound_capacity,
        max_inflight: config.max_inflight,
        max_packet_size: config.max_packet_size,
        max_state_bytes: config.max_state_bytes,
    }
}
fn valid_generation(
    online: &HashMap<String, Online>,
    client: &str,
    generation: u64,
) -> io::Result<()> {
    if online
        .get(client)
        .is_some_and(|connection| connection.generation == generation)
    {
        Ok(())
    } else {
        Err(failure(
            ReasonCode::SESSION_TAKEN_OVER,
            "connection superseded",
        ))
    }
}
fn notify(engine: &SessionEngine, online: &HashMap<String, Online>) -> usize {
    let mut targets = 0;
    for (client, connection) in online {
        if !engine.delivery_may_be_ready(client) {
            continue;
        }
        targets += 1;
        connection
            .wake
            .send_modify(|version| *version = version.wrapping_add(1));
    }
    targets
}

#[derive(Clone)]
struct ConnectRequest {
    authenticated: Option<mqtt_auth::Principal>,
    access_attempt: Option<Arc<dyn mqtt_auth::AccessAttempt>>,
    authentication: Option<enhanced::Completion>,
    deadline: Option<std::time::Instant>,
    client: String,
    assigned: bool,
    clean: bool,
    will: Option<Will>,
    username: Option<String>,
    password: Option<Password>,
    options: ConnectionOptions,
    properties: Properties,
    generation: u64,
    wake: watch::Sender<u64>,
    cancel: watch::Sender<bool>,
    close_reason: Option<watch::Sender<Option<ReasonCode>>>,
}
#[derive(Clone)]
enum Request {
    Authenticate {
        generation: u64,
        frame: Frame,
        deadline: std::time::Instant,
    },
    CancelAuthentication {
        generation: u64,
    },
    Connect(ConnectRequest),
    Handle {
        client: String,
        generation: u64,
        frame: Frame,
        legacy: bool,
        received_at_millis: Option<u64>,
    },
    #[cfg(test)]
    Poll {
        client: String,
        generation: u64,
        legacy: bool,
    },
    PollDelivery {
        client: String,
        generation: u64,
    },
    BeginDelivery {
        client: String,
        generation: u64,
        delivery: Delivery,
    },
    Disconnect {
        client: String,
        generation: u64,
        graceful: bool,
        expiry: Option<u32>,
    },
}
fn normalize_connect(
    frame: Frame,
    generation: u64,
    wake: watch::Sender<u64>,
    cancel: watch::Sender<bool>,
    close_reason: watch::Sender<Option<ReasonCode>>,
    deadline: Option<std::time::Instant>,
) -> io::Result<Request> {
    Ok({
        let Packet::Connect {
            client_id,
            clean_session,
            will,
            username,
            password,
            ..
        } = frame.packet
        else {
            return Err(failure(ReasonCode::PROTOCOL_ERROR, "CONNECT required"));
        };
        let mut expiry = if frame.version == ProtocolVersion::V311 && !clean_session {
            u32::MAX
        } else {
            0
        };
        let mut receive_maximum = u16::MAX;
        let mut maximum_packet_size = u32::MAX;
        for property in &frame.properties {
            match property {
                Property::SessionExpiryInterval(value) => expiry = *value,
                Property::ReceiveMaximum(value) => receive_maximum = *value,
                Property::MaximumPacketSize(value) => maximum_packet_size = *value,
                _ => {}
            }
        }
        Request::Connect(ConnectRequest {
            authenticated: None,
            access_attempt: None,
            authentication: None,
            deadline,
            assigned: false,
            client: client_id,
            clean: clean_session,
            will,
            username,
            password,
            generation,
            wake,
            cancel,
            close_reason: Some(close_reason),
            properties: frame.properties,
            options: ConnectionOptions {
                version: frame.version,
                clean_start: clean_session,
                session_expiry_interval: expiry,
                receive_maximum,
                maximum_packet_size,
                will_properties: frame.will_properties,
            },
        })
    })
}
fn normalize(operation: Operation) -> io::Result<Request> {
    Ok(match operation {
        #[cfg(test)]
        Operation::Connect {
            client,
            clean,
            will,
            username,
            password,
            generation,
            wake,
            cancel,
        } => Request::Connect(ConnectRequest {
            authenticated: None,
            access_attempt: None,
            authentication: None,
            deadline: None,
            assigned: false,
            client,
            clean,
            will,
            username,
            password,
            generation,
            wake,
            cancel,
            close_reason: None,
            properties: Vec::new(),
            options: ConnectionOptions {
                version: ProtocolVersion::V311,
                clean_start: clean,
                session_expiry_interval: if clean { 0 } else { u32::MAX },
                receive_maximum: u16::MAX,
                maximum_packet_size: u32::MAX,
                will_properties: Vec::new(),
            },
        }),
        Operation::ConnectFrameAt {
            frame,
            generation,
            wake,
            cancel,
            close_reason,
            deadline,
        } => normalize_connect(
            frame,
            generation,
            wake,
            cancel,
            close_reason,
            Some(deadline),
        )?,
        #[cfg(test)]
        Operation::ConnectFrame {
            frame,
            generation,
            wake,
            cancel,
            close_reason,
        } => normalize_connect(frame, generation, wake, cancel, close_reason, None)?,
        Operation::Authenticate {
            generation,
            frame,
            deadline,
        } => Request::Authenticate {
            generation,
            frame,
            deadline,
        },
        Operation::CancelAuthentication { generation } => {
            Request::CancelAuthentication { generation }
        }
        #[cfg(test)]
        Operation::Handle {
            client,
            generation,
            packet,
        } => Request::Handle {
            client,
            generation,
            frame: Frame::v311(packet),
            legacy: true,
            received_at_millis: None,
        },
        #[cfg(test)]
        Operation::Poll { client, generation } => Request::Poll {
            client,
            generation,
            legacy: true,
        },
        Operation::HandleFrameAt {
            client,
            generation,
            frame,
            received_at_millis,
        } => Request::Handle {
            client,
            generation,
            frame,
            legacy: false,
            received_at_millis: Some(received_at_millis),
        },
        Operation::PollDelivery { client, generation } => {
            Request::PollDelivery { client, generation }
        }
        Operation::BeginDelivery {
            client,
            generation,
            delivery,
        } => Request::BeginDelivery {
            client,
            generation,
            delivery,
        },
        Operation::Disconnect {
            client,
            generation,
            graceful,
        } => Request::Disconnect {
            client,
            generation,
            graceful,
            expiry: None,
        },
        Operation::DisconnectOptions {
            client,
            generation,
            graceful,
            expiry,
        } => Request::Disconnect {
            client,
            generation,
            graceful,
            expiry,
        },
        #[cfg(test)]
        Operation::Tick => return Err(io::Error::other("tick is not a session request")),
    })
}
/// Allocate an identifier not used by any live actor or caller reservation.
fn assigned_identifier_with(
    in_use: impl Fn(&str) -> bool,
    mut entropy: impl FnMut(&mut [u8]) -> io::Result<()>,
) -> io::Result<String> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for _ in 0..16 {
        let mut bytes = [0u8; 11];
        entropy(&mut bytes)?;
        let mut candidate = String::with_capacity(23);
        candidate.push('r');
        for byte in bytes {
            candidate.push(char::from(HEX[usize::from(byte >> 4)]));
            candidate.push(char::from(HEX[usize::from(byte & 15)]));
        }
        if !in_use(&candidate) {
            return Ok(candidate);
        }
    }
    Err(failure(
        ReasonCode::SERVER_UNAVAILABLE,
        "identifier allocation exhausted",
    ))
}
#[cfg(test)]
fn assigned_identifier(
    engine: &SessionEngine,
    entropy: impl FnMut(&mut [u8]) -> io::Result<()>,
) -> io::Result<String> {
    assigned_identifier_with(|id| engine.client_id_in_use(id), entropy)
}
fn connect_policy(request: &ConnectRequest, config: &BrokerConfig) -> Option<ReasonCode> {
    if request.options.version == ProtocolVersion::V311 {
        return None;
    }
    // Refuse before installing a session when the configured resource cap cannot
    // carry our mandatory capability CONNACK. Never accept then fail its write.
    match enhanced::success_frame(request, false, config)
        .and_then(|frame| mqtt_protocol::encode_frame(&frame).map_err(io::Error::other))
    {
        Ok(bytes)
            if bytes.len() <= config.max_packet_size
                && bytes.len() <= request.options.maximum_packet_size as usize => {}
        _ => return Some(ReasonCode::PACKET_TOO_LARGE),
    }
    None
}
fn refusal(request: &ConnectRequest, reason: ReasonCode) -> Reply {
    if request.options.version == ProtocolVersion::V5 {
        Reply::FrameRejected(reason)
    } else {
        Reply::Rejected(
            if matches!(
                reason,
                ReasonCode::SERVER_BUSY | ReasonCode::SERVER_UNAVAILABLE
            ) {
                ConnectRefusal::ServerBusy
            } else if reason == ReasonCode::BAD_USER_NAME_OR_PASSWORD {
                ConnectRefusal::BadCredentials
            } else {
                ConnectRefusal::NotAuthorized
            },
        )
    }
}
fn frames_reply(frames: Vec<Frame>, legacy: bool) -> Reply {
    #[cfg(test)]
    if legacy {
        return Reply::Packets(frames.into_iter().map(|frame| frame.packet).collect());
    }
    let _ = legacy;
    Reply::Frames(frames)
}
#[cfg(test)]
fn poll_reply(frame: Option<Frame>, legacy: bool) -> Reply {
    if legacy {
        return Reply::Polled(frame.map(|frame| frame.packet));
    }
    Reply::FramePolled(frame.map(Box::new))
}

/// Start the broker state, restoring or migrating storage first.
async fn start(config: BrokerConfig) -> io::Result<Arc<Broker>> {
    Broker::start(config, Arc::new(SystemClock), None).await
}

pub(super) async fn serve(
    listener: TcpListener,
    config: BrokerConfig,
    shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    serve_with_tls(listener, config, None, None, shutdown).await
}

pub(super) async fn serve_tls(
    listener: TcpListener,
    config: BrokerConfig,
    tls: crate::TlsConfig,
    shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    serve_with_tls(listener, config, Some(tls), None, shutdown).await
}

pub(super) async fn serve_websocket(
    listener: TcpListener,
    config: BrokerConfig,
    tls: Option<crate::TlsConfig>,
    options: crate::WebSocketOptions,
    shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    serve_with_tls(listener, config, tls, Some(options), shutdown).await
}

async fn serve_with_tls(
    listener: TcpListener,
    config: BrokerConfig,
    tls: Option<crate::TlsConfig>,
    websocket: Option<crate::WebSocketOptions>,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    let broker = start(config.clone()).await?;
    let mut fatal = broker.fatal.subscribe();
    let permits = Arc::new(Semaphore::new(config.max_connections));
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut tasks = JoinSet::new();
    let mut generation = 0u64;
    let mut ticks = tokio::time::interval(Duration::from_secs(1));
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let result = loop {
        if *shutdown.borrow() {
            break Ok(());
        }
        tokio::select! {
            biased;
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break Ok(()); } }
            _ = fatal.changed() => {
                if let Some(message) = fatal.borrow().clone() { break Err(io::Error::other(message)); }
            }
            Some(joined) = tasks.join_next(), if !tasks.is_empty() => { if let Err(error) = joined { break Err(io::Error::other(error)); } }
            _ = ticks.tick() => { if let Err(error) = broker.prune_retained().await { break Err(error); } }
            accepted = listener.accept() => {
                let (stream, _) = match accepted { Ok(value) => value, Err(error) => break Err(error) };
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else { drop(stream); continue; };
                let Some(handshake)=broker.admission.handshake() else {drop(stream);continue;};
                generation = match generation.checked_add(1) { Some(value) => value, None => break Err(io::Error::other("connection generation exhausted")) };
                let mut worker = Worker::new(Arc::clone(&broker));
                worker.handshake=Some(Arc::new(Mutex::new(Some(handshake))));
                let config = config.clone();
                let shutdown = stop_rx.clone();
                let tls = tls.clone();
                let websocket = websocket.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    if stream.set_nodelay(true).is_err() { return; }
                    if let Some(tls) = tls {
                        let mut stopped = shutdown.clone();
                        let accepted = tokio::select! {
                            biased;
                            _ = stopped.changed() => return,
                            result = tokio::time::timeout(config.handshake_timeout, tls.acceptor.accept(stream)) => result,
                        };
                        if let Ok(Ok(stream)) = accepted {
                            let _ = accept_mqtt(stream, websocket, generation, worker, config, shutdown).await;
                        }
                    } else {
                        let _ = accept_mqtt(stream, websocket, generation, worker, config, shutdown).await;
                    }
                });
            }
        }
    };
    if let Some(publisher) = &broker.config.publisher {
        publisher.stop();
    }
    let _ = stop_tx.send(true);
    while let Some(joined) = tasks.join_next().await {
        if let Err(error) = joined {
            return Err(io::Error::other(error));
        }
    }
    let stopped = broker.shutdown().await;
    result.and(stopped)
}

async fn accept_mqtt<
    S: crate::io_stream::IoStream + tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
>(
    stream: S,
    websocket: Option<crate::WebSocketOptions>,
    generation: u64,
    worker: Worker,
    config: BrokerConfig,
    shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    if let Some(options) = websocket {
        let mut stopped = shutdown.clone();
        let stream = tokio::select! {
            biased;
            _=stopped.changed()=>return Ok(()),
            result=tokio::time::timeout(config.handshake_timeout,crate::websocket::accept(stream,&options,config.write_timeout))=>result.map_err(|_|io::Error::new(io::ErrorKind::TimedOut,"WebSocket upgrade timed out"))??,
        };
        connection(stream, generation, worker, config, shutdown).await
    } else {
        connection(stream, generation, worker, config, shutdown).await
    }
}

async fn connection<S: crate::io_stream::IoStream>(
    stream: S,
    generation: u64,
    worker: Worker,
    config: BrokerConfig,
    shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut identity = None;
    let result = network::connection_loop(
        stream,
        generation,
        &worker,
        &config,
        shutdown,
        &mut identity,
    )
    .await;
    let _ = worker
        .request(Operation::CancelAuthentication { generation })
        .await;
    if let Some(client) = identity {
        let _ = worker
            .request(Operation::Disconnect {
                client,
                generation,
                graceful: false,
            })
            .await;
    }
    result
}
#[cfg(test)]
fn discard_acknowledged(pending: &mut Option<Packet>, acknowledged: Option<u16>) {
    if let Some(id) = acknowledged {
        let pending_id = match pending.as_ref() {
            Some(Packet::Publish { packet_id, .. }) => *packet_id,
            Some(Packet::PubRel { packet_id }) => Some(*packet_id),
            _ => None,
        };
        if pending_id == Some(id) {
            *pending = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };
    use tokio::task::JoinHandle;

    pub(super) struct Database(pub(super) PathBuf);
    impl Database {
        pub(super) fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(1);
            let directory = std::env::temp_dir().join(format!(
                "rmqtt-worker-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&directory).unwrap();
            Self(directory.join("state.sqlite"))
        }
        pub(super) fn config(&self) -> BrokerConfig {
            BrokerConfig {
                storage_path: Some(self.0.clone()),
                outbound_capacity: 1,
                ..BrokerConfig::default()
            }
        }
    }
    impl Drop for Database {
        fn drop(&mut self) {
            if let Some(parent) = self.0.parent() {
                let _ = std::fs::remove_dir_all(parent);
            }
        }
    }
    pub(super) async fn worker(config: BrokerConfig) -> (Worker, JoinHandle<io::Result<()>>) {
        wrap(start(config).await.unwrap())
    }
    /// Start from a prepared legacy engine and pre-attached online connections,
    /// as the former worker tests injected them directly.
    pub(super) async fn seeded(
        config: BrokerConfig,
        engine: SessionEngine,
        clock: Arc<dyn mqtt_session::durable::Clock>,
        online: HashMap<String, Online>,
    ) -> (Worker, JoinHandle<io::Result<()>>) {
        let broker = Broker::start(config, clock, Some(engine)).await.unwrap();
        for (client, connection) in online {
            broker.attach(&client, connection).await;
        }
        wrap(broker)
    }
    pub(super) fn wrap(broker: Arc<Broker>) -> (Worker, JoinHandle<io::Result<()>>) {
        let worker = Worker::new(Arc::clone(&broker));
        let mut fatal = broker.fatal.subscribe();
        let handle = tokio::spawn(async move {
            let _ = fatal.wait_for(Option::is_some).await;
            match fatal.borrow().clone() {
                Some(message) => Err(io::Error::other(message)),
                None => Ok(()),
            }
        });
        (worker, handle)
    }
    /// The merged schema7 envelope of a stopped broker's database.
    pub(super) fn stored(path: &std::path::Path, config: &BrokerConfig) -> Option<Vec<u8>> {
        let config = BrokerConfig {
            storage_path: Some(path.to_path_buf()),
            ..config.clone()
        };
        export::export_state(&config).unwrap()
    }
    pub(super) async fn connect(
        worker: &Worker,
        client: &str,
        generation: u64,
        clean: bool,
    ) -> (bool, watch::Receiver<bool>) {
        let (wake, _) = watch::channel(0);
        let (cancel, receive) = watch::channel(false);
        let Reply::Connected(present) = worker
            .request(Operation::Connect {
                client: client.into(),
                clean,
                will: None,
                username: None,
                password: None,
                generation,
                wake,
                cancel,
            })
            .await
            .unwrap()
        else {
            panic!("unexpected response")
        };
        (present, receive)
    }
    pub(super) async fn handle(
        worker: &Worker,
        client: &str,
        generation: u64,
        packet: Packet,
    ) -> io::Result<Vec<Packet>> {
        let Reply::Packets(packets) = worker
            .request(Operation::Handle {
                client: client.into(),
                generation,
                packet,
            })
            .await?
        else {
            panic!("unexpected response")
        };
        Ok(packets)
    }
    pub(super) async fn poll(worker: &Worker, client: &str, generation: u64) -> Option<Packet> {
        let Reply::Polled(packet) = worker
            .request(Operation::Poll {
                client: client.into(),
                generation,
            })
            .await
            .unwrap()
        else {
            panic!("unexpected response")
        };
        packet
    }
    pub(super) async fn subscribe(worker: &Worker, client: &str, generation: u64) {
        assert_eq!(
            handle(
                worker,
                client,
                generation,
                Packet::Subscribe {
                    packet_id: 1,
                    filters: vec![("a".into(), 1)]
                }
            )
            .await
            .unwrap(),
            vec![Packet::SubAck {
                packet_id: 1,
                codes: vec![1]
            }]
        );
    }
    pub(super) fn publication(payload: &[u8], id: u16) -> Packet {
        Packet::Publish {
            topic: "a".into(),
            payload: payload.to_vec(),
            qos: 1,
            packet_id: Some(id),
            dup: false,
            retain: false,
        }
    }
    pub(super) async fn stop(worker: Worker, task: JoinHandle<io::Result<()>>) {
        let broker = Arc::clone(&worker.broker);
        drop(worker);
        timeout(Duration::from_secs(3), broker.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(broker.fatal.borrow().clone(), None);
        task.abort();
        let _ = task.await;
    }
    #[tokio::test]
    async fn committed_delivery_reopens_with_same_id_and_dup() {
        let db = Database::new();
        let (worker, task) = worker(db.config()).await;
        assert!(!connect(&worker, "sub", 1, false).await.0);
        subscribe(&worker, "sub", 1).await;
        connect(&worker, "pub", 2, true).await;
        assert_eq!(
            handle(&worker, "pub", 2, publication(b"persistent", 9))
                .await
                .unwrap(),
            vec![Packet::PubAck { packet_id: 9 }]
        );
        let original = poll(&worker, "sub", 1).await.unwrap();
        stop(worker, task).await;
        let (worker, task) = self::worker(db.config()).await;
        assert!(connect(&worker, "sub", 3, false).await.0);
        let replay = poll(&worker, "sub", 3).await.unwrap();
        let Packet::Publish {
            packet_id,
            topic,
            payload,
            qos,
            ..
        } = original
        else {
            panic!("PUBLISH expected")
        };
        assert_eq!(
            replay,
            Packet::Publish {
                packet_id,
                topic,
                payload,
                qos,
                dup: true,
                retain: false
            }
        );
        stop(worker, task).await;
    }
    #[tokio::test]
    async fn stale_generation_cleanup_cannot_delete_replacement_session() {
        let db = Database::new();
        let (worker, task) = worker(db.config()).await;
        let (_, cancel) = connect(&worker, "same", 1, false).await;
        subscribe(&worker, "same", 1).await;
        assert!(connect(&worker, "same", 2, false).await.0);
        assert!(*cancel.borrow());
        worker
            .request(Operation::Disconnect {
                client: "same".into(),
                generation: 1,
                graceful: false,
            })
            .await
            .unwrap();
        assert!(handle(&worker, "same", 1, Packet::PingReq).await.is_err());
        assert_eq!(
            handle(&worker, "same", 2, Packet::PingReq).await.unwrap(),
            vec![Packet::PingResp]
        );
        connect(&worker, "pub", 3, true).await;
        handle(&worker, "pub", 3, publication(b"alive", 1))
            .await
            .unwrap();
        assert!(
            matches!(poll(&worker,"same",2).await,Some(Packet::Publish {payload,..}) if payload==b"alive")
        );
        stop(worker, task).await;
    }
    #[tokio::test]
    async fn full_recipient_drops_only_its_own_copy() {
        let db = Database::new();
        let (worker, task) = worker(db.config()).await;
        connect(&worker, "b-full", 1, false).await;
        subscribe(&worker, "b-full", 1).await;
        connect(&worker, "pub", 2, true).await;
        handle(&worker, "pub", 2, publication(b"first", 1))
            .await
            .unwrap();
        connect(&worker, "a-empty", 3, false).await;
        subscribe(&worker, "a-empty", 3).await;
        // b-full rejects its copy; a-empty still receives the publication and the
        // preceding committed b-full message survives.
        assert!(
            handle(&worker, "pub", 2, publication(b"accepted", 2))
                .await
                .is_ok()
        );
        assert!(
            matches!(poll(&worker,"a-empty",3).await,Some(Packet::Publish {payload,..}) if payload==b"accepted")
        );
        assert!(
            matches!(poll(&worker,"b-full",1).await,Some(Packet::Publish {payload,..}) if payload==b"first")
        );
        stop(worker, task).await;
    }
    #[tokio::test]
    async fn ack_before_replay_is_queued_discards_obsolete_publish() {
        let db = Database::new();
        let (worker, task) = worker(db.config()).await;
        connect(&worker, "sub", 1, false).await;
        handle(
            &worker,
            "sub",
            1,
            Packet::Subscribe {
                packet_id: 1,
                filters: vec![("a".into(), 2)],
            },
        )
        .await
        .unwrap();
        connect(&worker, "pub", 2, true).await;
        let mut packet = publication(b"qos2", 7);
        if let Packet::Publish { qos, .. } = &mut packet {
            *qos = 2;
        }
        handle(&worker, "pub", 2, packet).await.unwrap();
        handle(&worker, "pub", 2, Packet::PubRel { packet_id: 7 })
            .await
            .unwrap();
        let original = poll(&worker, "sub", 1).await.unwrap();
        let Packet::Publish {
            packet_id: Some(id),
            ..
        } = original
        else {
            panic!("PUBLISH expected")
        };
        connect(&worker, "sub", 3, false).await;
        let mut pending = poll(&worker, "sub", 3).await;
        assert!(matches!(pending, Some(Packet::Publish { dup: true, .. })));
        // Select chooses an already buffered PUBREC before output.reserve.
        // The old PUBLISH is not yet in the writer queue; it must not be put
        // behind the new PUBREL reply after this state transition.
        let replies = handle(&worker, "sub", 3, Packet::PubRec { packet_id: id })
            .await
            .unwrap();
        discard_acknowledged(&mut pending, Some(id));
        assert_eq!(replies, vec![Packet::PubRel { packet_id: id }]);
        assert!(
            pending.is_none(),
            "obsolete PUBLISH would be sent after PUBREL"
        );
        assert!(poll(&worker, "sub", 3).await.is_none());
        // The incoming QoS2 namespace never invalidates outgoing replay.
        let mut pending = Some(Packet::PubRel { packet_id: id });
        discard_acknowledged(&mut pending, None);
        assert!(pending.is_some());
        let replies = handle(&worker, "sub", 3, Packet::PubComp { packet_id: id })
            .await
            .unwrap();
        discard_acknowledged(&mut pending, Some(id));
        assert!(replies.is_empty());
        assert!(
            pending.is_none(),
            "completed PUBREL must not remain queued locally"
        );
        stop(worker, task).await;
    }
    async fn assert_close_aborts_a_blocked_writer(result: io::Result<()>) {
        let expected_success = result.is_ok();
        // Tiny real TCP buffers keep a large frame blocked without arbitrary
        // sleeps; the bounded queue also contains a second frame. Closing must
        // interrupt this writer, not wait its 30-second per-frame deadline.
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_send_buffer_size(4096).unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(8).unwrap();
        let client_socket = tokio::net::TcpSocket::new_v4().unwrap();
        client_socket.set_recv_buffer_size(4096).unwrap();
        let client = client_socket
            .connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (_, writer) = server.into_split();
        let (output, receiver) = mpsc::channel(1);
        let (cancel, _) = watch::channel(false);
        let config = BrokerConfig {
            write_timeout: Duration::from_secs(30),
            max_packet_size: 2 * 1024 * 1024,
            ..BrokerConfig::default()
        };
        let (writer, _terminal) = network::test_writer(writer, receiver, config, cancel);
        let payload = vec![42; 1024 * 1024];
        output
            .send(network::test_output(publication(&payload, 1)))
            .await
            .unwrap();
        output
            .send(network::test_output(publication(&payload, 2)))
            .await
            .unwrap();
        drop(output);
        let closed = timeout(
            Duration::from_millis(300),
            network::finish_writer(writer, result, false, Duration::from_secs(30)),
        )
        .await
        .expect("closing a socket must cancel queued writes promptly");
        assert_eq!(closed.is_ok(), expected_success);
        drop(client);
    }
    #[tokio::test]
    async fn closing_connection_aborts_a_blocked_writer_instead_of_draining_queue() {
        assert_close_aborts_a_blocked_writer(Ok(())).await;
    }

    #[tokio::test]
    async fn established_version_error_does_not_flush_business_queue() {
        // A second CONNECT with protocol version5 produces this codec error
        // after the session is established. No rejection CONNACK was queued.
        assert_close_aborts_a_blocked_writer(Err(io::Error::other(
            ProtocolError::UnsupportedVersion,
        )))
        .await;
    }
    pub(super) async fn connect_with_will(
        worker: &Worker,
        client: &str,
        generation: u64,
        will: Option<Will>,
    ) -> watch::Receiver<bool> {
        let (wake, _) = watch::channel(0);
        let (cancel, receiver) = watch::channel(false);
        assert!(matches!(
            worker
                .request(Operation::Connect {
                    client: client.into(),
                    clean: true,
                    will,
                    username: None,
                    password: None,
                    generation,
                    wake,
                    cancel,
                })
                .await
                .unwrap(),
            Reply::Connected(false)
        ));
        receiver
    }
    pub(super) fn will(topic: &str, payload: &[u8], retain: bool) -> Will {
        Will {
            topic: topic.into(),
            payload: payload.to_vec(),
            qos: 1,
            retain,
        }
    }
    #[tokio::test]
    async fn graceful_will_clear_takeover_and_stale_cleanup_are_committed() {
        let db = Database::new();
        let (worker, task) = worker(db.config()).await;
        connect(&worker, "observer", 1, false).await;
        subscribe(&worker, "observer", 1).await;
        connect_with_will(&worker, "owner", 2, Some(will("a", b"normal", false))).await;
        worker
            .request(Operation::Disconnect {
                client: "owner".into(),
                generation: 2,
                graceful: true,
            })
            .await
            .unwrap();
        worker
            .request(Operation::Disconnect {
                client: "owner".into(),
                generation: 2,
                graceful: false,
            })
            .await
            .unwrap();
        assert!(poll(&worker, "observer", 1).await.is_none());
        let old_cancel =
            connect_with_will(&worker, "owner", 3, Some(will("a", b"old", false))).await;
        connect_with_will(&worker, "owner", 4, Some(will("a", b"new", false))).await;
        assert!(*old_cancel.borrow());
        let Some(Packet::Publish {
            packet_id: Some(id),
            payload,
            retain: false,
            ..
        }) = poll(&worker, "observer", 1).await
        else {
            panic!("old Will missing");
        };
        assert_eq!(payload, b"old");
        handle(&worker, "observer", 1, Packet::PubAck { packet_id: id })
            .await
            .unwrap();
        worker
            .request(Operation::Disconnect {
                client: "owner".into(),
                generation: 3,
                graceful: false,
            })
            .await
            .unwrap();
        assert!(poll(&worker, "observer", 1).await.is_none());
        worker
            .request(Operation::Disconnect {
                client: "owner".into(),
                generation: 4,
                graceful: true,
            })
            .await
            .unwrap();
        assert!(poll(&worker, "observer", 1).await.is_none());
        stop(worker, task).await;
    }
    #[tokio::test]
    async fn volatile_runtime_refuses_persistent_connect_and_keeps_global_retained() {
        let (worker, task) = worker(BrokerConfig::default()).await;
        let (wake, _) = watch::channel(0);
        let (cancel, _) = watch::channel(false);
        assert!(
            worker
                .request(Operation::Connect {
                    client: "persistent".into(),
                    clean: false,
                    will: None,
                    username: None,
                    password: None,
                    generation: 1,
                    wake,
                    cancel
                })
                .await
                .is_err()
        );
        connect(&worker, "writer", 2, true).await;
        let mut retained = publication(b"global", 1);
        if let Packet::Publish { retain, .. } = &mut retained {
            *retain = true;
        }
        handle(&worker, "writer", 2, retained).await.unwrap();
        worker
            .request(Operation::Disconnect {
                client: "writer".into(),
                generation: 2,
                graceful: true,
            })
            .await
            .unwrap();
        connect(&worker, "reader", 3, true).await;
        subscribe(&worker, "reader", 3).await;
        assert!(
            matches!(poll(&worker, "reader", 3).await, Some(Packet::Publish { payload, retain: true, .. }) if payload == b"global")
        );
        stop(worker, task).await;
    }
    #[tokio::test]
    async fn empty_poll_commits_and_notifies_pending_will_progress() {
        let db = Database::new();
        let config = BrokerConfig {
            max_retained: 1,
            ..db.config()
        };
        let mut engine = SessionEngine::new(session_config(&config)).unwrap();
        engine.connect("poller", true).unwrap();
        // Progress must wake an actual recipient, not an unrelated idle peer.
        engine
            .handle(
                "poller",
                Packet::Subscribe {
                    packet_id: 7,
                    filters: vec![("fresh".into(), 0)],
                },
            )
            .unwrap();
        let mut hold = publication(b"held", 1);
        if let Packet::Publish { topic, retain, .. } = &mut hold {
            *topic = "held".into();
            *retain = true;
        }
        engine.handle("poller", hold).unwrap();
        engine
            .connect_with_will("a-first", true, Some(will("fresh", b"retained Will", true)))
            .unwrap();
        engine
            .connect_with_will("z-delete", true, Some(will("held", b"", true)))
            .unwrap();
        engine.disconnect_with_reason("a-first", false).unwrap();
        // One retry pass visits a-first while full, then z-delete frees its slot.
        // The pending first Will can progress on the next, otherwise-empty poll.
        engine.disconnect_with_reason("z-delete", false).unwrap();
        let before = engine.snapshot().unwrap();
        assert!(std::str::from_utf8(&before).unwrap().contains("a-first"));
        let (wake, mut notified) = watch::channel(0);
        let (cancel, _) = watch::channel(false);
        let online = HashMap::from([(
            "poller".into(),
            Online {
                close_reason: None,
                generation: 1,
                wake,
                cancel,
            },
        )]);
        let (worker, task) = seeded(config.clone(), engine, Arc::new(SystemClock), online).await;
        // Each Will retries in its own actor until z-delete frees the slot.
        let delivered = timeout(Duration::from_secs(2), async {
            loop {
                if let Some(packet) = poll(&worker, "poller", 1).await {
                    break packet;
                }
                let _ = notified.changed().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            delivered,
            Packet::Publish { topic, payload, .. }
                if topic == "fresh" && payload == b"retained Will"
        ));
        stop(worker, task).await;
        let bytes = stored(&db.0, &config).unwrap();
        let after = std::str::from_utf8(&bytes).unwrap();
        assert!(after.contains("\"topic\":\"fresh\""));
        assert!(after.contains("\"wills\":[]"));
    }
    #[test]
    fn phase2g_allocator_bounds_collisions_and_never_uses_pending_will_identifier() {
        let config = BrokerConfig {
            outbound_capacity: 1,
            ..BrokerConfig::default()
        };
        let mut engine = SessionEngine::new(session_config(&config)).unwrap();
        let reserved = "r0000000000000000000000";
        engine.connect("target", true).unwrap();
        engine
            .handle(
                "target",
                Packet::Subscribe {
                    packet_id: 1,
                    filters: vec![("will".into(), 1)],
                },
            )
            .unwrap();
        engine
            .connect_with_will(
                reserved,
                true,
                Some(Will {
                    topic: "will".into(),
                    payload: vec![1],
                    qos: 1,
                    retain: false,
                }),
            )
            .unwrap();
        engine
            .handle(
                "target",
                Packet::Publish {
                    topic: "will".into(),
                    payload: vec![0],
                    qos: 0,
                    packet_id: None,
                    dup: false,
                    retain: false,
                },
            )
            .unwrap();
        engine.disconnect_with_reason(reserved, false).unwrap();
        assert!(engine.client_id_in_use(reserved));
        let before = engine.snapshot().unwrap();
        let mut attempts = 0;
        let result = assigned_identifier(&engine, |bytes| {
            attempts += 1;
            bytes.fill(0);
            Ok(())
        });
        assert!(result.is_err());
        assert_eq!(attempts, 16);
        assert_eq!(engine.snapshot().unwrap(), before);
        attempts = 0;
        let result = assigned_identifier(&engine, |bytes| {
            bytes.fill(0);
            bytes[10] = attempts;
            attempts += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(result, "r0000000000000000000001");
        assert_eq!(attempts, 2);
        let error = assigned_identifier(&engine, |_| Err(io::Error::other("entropy unavailable")));
        assert!(error.is_err());
        assert_eq!(engine.snapshot().unwrap(), before);
    }
    #[derive(Debug)]
    struct ManualClock(AtomicU64);
    impl Clock for ManualClock {
        fn now_millis(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }
    #[tokio::test]
    async fn expiry_tick_uses_restored_floor_and_empty_ticks_stay_quiet() {
        let db = Database::new();
        let config = db.config();
        // Keep the deadline beyond the real wall clock so a raw SystemClock
        // guard would incorrectly skip work after restoring the logical floor.
        let start = SystemClock.now_millis() + 86_400_000;
        let clock = Arc::new(ManualClock(AtomicU64::new(start)));
        let mut engine = SessionEngine::new_with_clock(
            session_config(&config),
            config.access_control.clone(),
            clock.clone(),
        )
        .unwrap();
        engine
            .connect_with_options(
                "expires",
                None,
                mqtt_auth::Principal::anonymous(),
                ConnectionOptions {
                    version: ProtocolVersion::V5,
                    clean_start: true,
                    session_expiry_interval: 1,
                    receive_maximum: 1,
                    maximum_packet_size: u32::MAX,
                    will_properties: Vec::new(),
                },
            )
            .unwrap();
        engine
            .disconnect_with_options("expires", true, None)
            .unwrap();
        clock.0.store(start + 1500, Ordering::SeqCst);
        engine.effective_now();
        let before = engine.snapshot().unwrap();
        clock.0.store(0, Ordering::SeqCst);
        let engine = SessionEngine::restore_with_clock(
            session_config(&config),
            &before,
            config.access_control.clone(),
            clock.clone(),
        )
        .unwrap();
        assert_eq!(engine.next_expiry_deadline(), Some(start + 1000));
        let (wake, mut notified) = watch::channel(0);
        let (cancel, _) = watch::channel(false);
        let online = HashMap::from([(
            "observer".into(),
            Online {
                close_reason: None,
                generation: 1,
                wake,
                cancel,
            },
        )]);
        let (worker, task) = seeded(config.clone(), engine, clock.clone(), online).await;
        worker.request(Operation::Tick).await.unwrap();
        // Expiry still commits below, but an unknown idle observer has no
        // delivery work and must not be woken merely for a removed session.
        assert!(!notified.has_changed().unwrap());
        notified.borrow_and_update();
        clock.0.store(start + 10_000, Ordering::SeqCst);
        worker.request(Operation::Tick).await.unwrap();
        assert!(!notified.has_changed().unwrap());
        stop(worker, task).await;
        let after = stored(&db.0, &config).unwrap();
        let after = std::str::from_utf8(&after).unwrap();
        // Phase4 persists the floor only inside session/Will rows; the expired
        // partition deleted its row, so no stale time observation remains.
        assert!(after.contains("\"sessions\":[]"));
    }
    #[tokio::test]
    async fn expiry_tick_commits_removal_without_any_client_reconnect() {
        let db = Database::new();
        let config = db.config();
        let clock = Arc::new(ManualClock(AtomicU64::new(0)));
        let mut engine = SessionEngine::new_with_clock(
            session_config(&config),
            config.access_control.clone(),
            clock.clone(),
        )
        .unwrap();
        engine
            .connect_with_options(
                "expires",
                None,
                mqtt_auth::Principal::anonymous(),
                ConnectionOptions {
                    version: ProtocolVersion::V5,
                    clean_start: true,
                    session_expiry_interval: 1,
                    receive_maximum: 1,
                    maximum_packet_size: u32::MAX,
                    will_properties: Vec::new(),
                },
            )
            .unwrap();
        engine
            .disconnect_with_options("expires", true, None)
            .unwrap();
        let mut store = SqliteStore::open(&db.0, config.max_state_bytes).unwrap();
        store.commit(&engine.snapshot().unwrap()).unwrap();
        drop(store);
        clock.0.store(1500, Ordering::SeqCst);
        let broker = Broker::start(config.clone(), clock.clone(), None)
            .await
            .unwrap();
        let (worker, task) = wrap(broker);
        assert!(matches!(
            worker.request(Operation::Tick).await.unwrap(),
            Reply::Done
        ));
        stop(worker, task).await;
        let bytes = stored(&db.0, &config).unwrap();
        assert!(
            std::str::from_utf8(&bytes)
                .unwrap()
                .contains("\"sessions\":[]")
        );
    }
}

#[cfg(test)]
#[path = "live_tests.rs"]
mod live_tests;

#[cfg(test)]
#[path = "auth_tests.rs"]
mod auth_tests;

#[cfg(test)]
#[path = "enhanced_tests.rs"]
mod enhanced_tests;

#[cfg(test)]
#[path = "durable/phase3_tests.rs"]
mod phase3_tests;

#[cfg(test)]
#[path = "durable/phase3a_tests.rs"]
mod phase3a_tests;

#[cfg(test)]
#[path = "durable/actor_tests.rs"]
mod actor_tests;

#[cfg(test)]
#[path = "durable/admission_tests.rs"]
mod admission_tests;

#[cfg(test)]
#[path = "durable/management_tests.rs"]
mod management_tests;

#[cfg(test)]
mod observer_tests;

#[cfg(test)]
mod operation_access_tests;

#[cfg(test)]
mod publisher_tests;
