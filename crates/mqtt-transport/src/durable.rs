//! Serialized durable worker: propose, commit, install, then expose packets.
use crate::BrokerConfig;
use mqtt_auth::{AuthError, ConnectInfo};
use mqtt_protocol::{Packet, Password, ProtocolError, Will, decode, encode};
use mqtt_session::durable::{EngineError, SessionConfig, SessionEngine};
use mqtt_storage::durable::SqliteStore;
use std::{collections::HashMap, io, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{Semaphore, mpsc, oneshot, watch},
    task::JoinSet,
    time::{Instant, timeout},
};

type ResultReply = Result<Reply, String>;
struct Command {
    operation: Operation,
    reply: oneshot::Sender<ResultReply>,
}
enum Operation {
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
    Handle {
        client: String,
        generation: u64,
        packet: Packet,
    },
    Poll {
        client: String,
        generation: u64,
    },
    Disconnect {
        client: String,
        generation: u64,
        graceful: bool,
    },
}
#[derive(Clone, Copy, Debug)]
enum ConnectRefusal {
    BadCredentials,
    NotAuthorized,
}
impl ConnectRefusal {
    fn code(self) -> u8 {
        match self {
            Self::BadCredentials => 4,
            Self::NotAuthorized => 5,
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
    Connected(bool),
    Rejected(ConnectRefusal),
    Packets(Vec<Packet>),
    Polled(Option<Packet>),
    Done,
}
struct Online {
    generation: u64,
    wake: watch::Sender<u64>,
    cancel: watch::Sender<bool>,
}
#[derive(Clone)]
struct Worker {
    sender: mpsc::Sender<Command>,
}
impl Worker {
    async fn request(&self, operation: Operation) -> io::Result<Reply> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(Command { operation, reply })
            .await
            .map_err(|_| io::Error::other("durable worker unavailable"))?;
        receive
            .await
            .map_err(|_| io::Error::other("durable worker terminated"))?
            .map_err(io::Error::other)
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
        Err(io::Error::other("connection superseded"))
    }
}
fn notify(online: &HashMap<String, Online>) {
    for connection in online.values() {
        connection
            .wake
            .send_modify(|version| *version = version.wrapping_add(1));
    }
}

fn run_worker(
    commands: mpsc::Receiver<Command>,
    config: BrokerConfig,
    ready: oneshot::Sender<Result<(), String>>,
) -> io::Result<()> {
    let startup = (|| {
        let mut store = config
            .storage_path
            .as_ref()
            .map(|path| SqliteStore::open(path, config.max_state_bytes))
            .transpose()
            .map_err(io::Error::other)?;
        let loaded = store
            .as_ref()
            .map(SqliteStore::load)
            .transpose()
            .map_err(io::Error::other)?
            .flatten();
        let mut engine = match loaded {
            Some(bytes) => SessionEngine::restore_with_access(
                session_config(&config),
                &bytes,
                Arc::clone(&config.access_control),
            ),
            None => SessionEngine::new_with_access(
                session_config(&config),
                Arc::clone(&config.access_control),
            ),
        }
        .map_err(io::Error::other)?;
        engine.reconcile_authorization().map_err(io::Error::other)?;
        engine.recover_wills().map_err(io::Error::other)?;
        let snapshot = engine.snapshot().map_err(io::Error::other)?;
        if let Some(store) = &mut store {
            store.commit(&snapshot).map_err(io::Error::other)?;
        }
        Ok::<_, io::Error>((store, engine))
    })();
    let (store, engine) = match startup {
        Ok(value) => {
            let _ = ready.send(Ok(()));
            value
        }
        Err(error) => {
            let _ = ready.send(Err(error.to_string()));
            return Err(error);
        }
    };
    run_commands(commands, config, store, engine, HashMap::new())
}

fn run_commands(
    mut commands: mpsc::Receiver<Command>,
    config: BrokerConfig,
    mut store: Option<SqliteStore>,
    mut engine: SessionEngine,
    mut online: HashMap<String, Online>,
) -> io::Result<()> {
    while let Some(command) = commands.blocking_recv() {
        // Reject bad credentials before copying potentially large durable state.
        // Password borrows and hashing stay on this bounded blocking worker.
        let mut principal = if let Operation::Connect {
            client,
            username,
            password,
            ..
        } = &command.operation
        {
            match config.access_control.authenticate(ConnectInfo {
                client_id: client,
                username: username.as_deref(),
                password: password.as_ref().map(Password::as_bytes),
            }) {
                Ok(principal) => Some(principal),
                Err(error) => {
                    let _ = command.reply.send(Ok(Reply::Rejected(error.into())));
                    continue;
                }
            }
        } else {
            None
        };
        // Every command mutates only a proposed clone. On model/quota errors the
        // full proposal is discarded; SQL failure additionally stops all service.
        let mut proposal = engine.clone();
        let mut commit_required = true;
        let mut notify_required = false;
        let mut retry_required = true;
        let mut registration = None;
        let mut removal = None;
        let proposed = (|| -> io::Result<Reply> {
            match command.operation {
                Operation::Connect {
                    client,
                    clean,
                    will,
                    username: _,
                    password: _,
                    generation,
                    wake,
                    cancel,
                } => {
                    let principal = principal
                        .take()
                        .ok_or_else(|| io::Error::other("missing authenticated identity"))?;
                    let result =
                        match proposal.connect_authenticated(&client, clean, will, principal) {
                            Ok(result) => result,
                            Err(EngineError::NotAuthorized) => {
                                // A rejected CONNECT cannot commit pruning, Will retry,
                                // clean reset or cancel an already established client.
                                commit_required = false;
                                retry_required = false;
                                return Ok(Reply::Rejected(ConnectRefusal::NotAuthorized));
                            }
                            Err(error) => return Err(io::Error::other(error)),
                        };
                    // Security refusal takes precedence over backend capability.
                    // Any old-Will/reset effects above are only on the proposal;
                    // rejecting persistence here discards that proposal entirely.
                    if !clean && config.storage_path.is_none() {
                        return Err(io::Error::new(
                            io::ErrorKind::Unsupported,
                            "persistent sessions require --storage-path",
                        ));
                    }
                    registration = Some((
                        client,
                        Online {
                            generation,
                            wake,
                            cancel,
                        },
                    ));
                    notify_required = true;
                    Ok(Reply::Connected(result.session_present))
                }
                Operation::Handle {
                    client,
                    generation,
                    packet,
                } => {
                    valid_generation(&online, &client, generation)?;
                    let replies = proposal.handle(&client, packet).map_err(io::Error::other)?;
                    notify_required = true;
                    Ok(Reply::Packets(replies))
                }
                Operation::Poll { client, generation } => {
                    valid_generation(&online, &client, generation)?;
                    let outcome = proposal
                        .poll_with_changes(&client)
                        .map_err(io::Error::other)?;
                    // Revocation can prune state without producing a packet.
                    commit_required = outcome.changed;
                    notify_required = outcome.changed;
                    Ok(Reply::Polled(outcome.packet))
                }
                Operation::Disconnect {
                    client,
                    generation,
                    graceful,
                } => {
                    if valid_generation(&online, &client, generation).is_err() {
                        commit_required = false;
                        retry_required = false;
                        return Ok(Reply::Done);
                    }
                    proposal
                        .disconnect_with_reason(&client, graceful)
                        .map_err(io::Error::other)?;
                    notify_required = true;
                    removal = Some(client);
                    Ok(Reply::Done)
                }
            }
        })();
        let proposed = proposed.and_then(|reply| {
            if retry_required {
                let pruned = proposal
                    .reconcile_authorization()
                    .map_err(io::Error::other)?;
                let retried = proposal.retry_wills().map_err(io::Error::other)?;
                if pruned || retried {
                    commit_required = true;
                    notify_required = true;
                }
            }
            Ok(reply)
        });
        let reply = match proposed {
            Err(error) => Err(error.to_string()),
            Ok(reply) => {
                if commit_required {
                    let bytes = match proposal.snapshot() {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            let _ = command.reply.send(Err(error.to_string()));
                            continue;
                        }
                    };
                    if let Some(store) = &mut store
                        && let Err(error) = store.commit(&bytes)
                    {
                        let message = error.to_string();
                        let _ = command.reply.send(Err(message.clone()));
                        for connection in online.values() {
                            let _ = connection.cancel.send(true);
                        }
                        return Err(io::Error::other(message));
                    }
                    engine = proposal;
                }
                if let Some((client, connection)) = registration
                    && let Some(previous) = online.insert(client, connection)
                {
                    let _ = previous.cancel.send(true);
                }
                if let Some(client) = removal {
                    online.remove(&client);
                }
                if notify_required {
                    notify(&online);
                }
                Ok(reply)
            }
        };
        let _ = command.reply.send(reply);
    }
    Ok(())
}

pub(super) async fn serve(
    listener: TcpListener,
    config: BrokerConfig,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    let (sender, receiver) = mpsc::channel(config.max_connections);
    let worker = Worker { sender };
    let (ready_tx, ready_rx) = oneshot::channel();
    let worker_config = config.clone();
    let mut worker_task =
        tokio::task::spawn_blocking(move || run_worker(receiver, worker_config, ready_tx));
    match ready_rx.await {
        Ok(Ok(())) => {}
        result => {
            let _ = worker_task.await;
            return Err(io::Error::other(match result {
                Ok(Err(message)) => message,
                _ => "durable worker startup failed".into(),
            }));
        }
    }
    let permits = Arc::new(Semaphore::new(config.max_connections));
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut tasks = JoinSet::new();
    let mut generation = 0u64;
    let mut worker_finished = false;
    let result = loop {
        if *shutdown.borrow() {
            break Ok(());
        }
        tokio::select! {
            biased;
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break Ok(()); } }
            joined = &mut worker_task => {
                worker_finished = true;
                break match joined { Ok(Err(error)) => Err(error), Err(error) => Err(io::Error::other(error)), Ok(Ok(())) => Err(io::Error::other("durable worker stopped unexpectedly")) };
            }
            Some(joined) = tasks.join_next(), if !tasks.is_empty() => { if let Err(error) = joined { break Err(io::Error::other(error)); } }
            accepted = listener.accept() => {
                let (stream, _) = match accepted { Ok(value) => value, Err(error) => break Err(error) };
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else { drop(stream); continue; };
                generation = match generation.checked_add(1) { Some(value) => value, None => break Err(io::Error::other("connection generation exhausted")) };
                let worker = worker.clone();
                let config = config.clone();
                let shutdown = stop_rx.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    let _ = connection(stream, generation, worker, config, shutdown).await;
                });
            }
        }
    };
    let _ = stop_tx.send(true);
    while let Some(joined) = tasks.join_next().await {
        if let Err(error) = joined {
            return Err(io::Error::other(error));
        }
    }
    drop(worker);
    if !worker_finished {
        worker_task.await.map_err(io::Error::other)??;
    }
    result
}

async fn write(
    writer: &mut OwnedWriteHalf,
    packet: &Packet,
    config: &BrokerConfig,
) -> io::Result<()> {
    let bytes = encode(packet).map_err(io::Error::other)?;
    if bytes.len() > config.max_packet_size {
        return Err(io::Error::other("outgoing packet exceeds limit"));
    }
    timeout(config.write_timeout, writer.write_all(&bytes))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "socket write timed out"))?
}

async fn connection(
    stream: TcpStream,
    generation: u64,
    worker: Worker,
    config: BrokerConfig,
    shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut identity = None;
    let result = connection_loop(
        stream,
        generation,
        &worker,
        &config,
        shutdown,
        &mut identity,
    )
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

async fn connection_loop(
    stream: TcpStream,
    generation: u64,
    worker: &Worker,
    config: &BrokerConfig,
    shutdown: watch::Receiver<bool>,
    identity: &mut Option<String>,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let (mut reader, writer) = stream.into_split();
    let (wake_tx, wake) = watch::channel(0u64);
    let (cancel_tx, cancel) = watch::channel(false);
    let (output, outgoing) = mpsc::channel::<Packet>(config.outbound_capacity);
    let writer_task = spawn_writer(writer, outgoing, config.clone(), cancel_tx.clone());
    let mut flush_rejection = false;
    let result = drive_connection(
        &mut reader,
        &output,
        ConnectionContext {
            generation,
            worker,
            config,
            shutdown,
            identity,
            flush_rejection: &mut flush_rejection,
            wake_tx,
            wake,
            cancel_tx,
            cancel,
        },
    )
    .await;
    drop(output);
    finish_writer(writer_task, result, flush_rejection).await
}

fn spawn_writer(
    mut writer: OwnedWriteHalf,
    mut outgoing: mpsc::Receiver<Packet>,
    config: BrokerConfig,
    cancel: watch::Sender<bool>,
) -> tokio::task::JoinHandle<io::Result<()>> {
    tokio::spawn(async move {
        while let Some(packet) = outgoing.recv().await {
            if let Err(error) = write(&mut writer, &packet, &config).await {
                let _ = cancel.send(true);
                return Err(error);
            }
        }
        Ok(())
    })
}

async fn finish_writer(
    writer_task: tokio::task::JoinHandle<io::Result<()>>,
    result: io::Result<()>,
    flush_rejection: bool,
) -> io::Result<()> {
    // A closing connection must not drain an arbitrarily slow queue after
    // takeover or shutdown. Cancelling a frame is safe because this socket is
    // closing and that frame will never be restarted on this stream. Durable
    // unacknowledged exchanges remain in the worker for reconnect replay.
    if flush_rejection {
        // Exactly one CONNACK was queued before CONNECT completed.
        let written = writer_task.await.map_err(io::Error::other)?;
        result.and(written)
    } else {
        writer_task.abort();
        let _ = writer_task.await;
        result
    }
}

struct ConnectionContext<'a> {
    generation: u64,
    worker: &'a Worker,
    config: &'a BrokerConfig,
    shutdown: watch::Receiver<bool>,
    identity: &'a mut Option<String>,
    flush_rejection: &'a mut bool,
    wake_tx: watch::Sender<u64>,
    wake: watch::Receiver<u64>,
    cancel_tx: watch::Sender<bool>,
    cancel: watch::Receiver<bool>,
}

async fn drive_connection(
    reader: &mut OwnedReadHalf,
    output: &mpsc::Sender<Packet>,
    context: ConnectionContext<'_>,
) -> io::Result<()> {
    let ConnectionContext {
        generation,
        worker,
        config,
        mut shutdown,
        identity,
        flush_rejection,
        wake_tx,
        mut wake,
        cancel_tx,
        mut cancel,
    } = context;
    let mut connected = false;
    let mut buffer = Vec::new();
    let mut scratch = [0u8; 8192];
    let mut expires = Some(Instant::now() + config.handshake_timeout);
    let mut keepalive = None;
    let mut pending = None;
    let mut needs_poll = false;

    loop {
        if *shutdown.borrow() || *cancel.borrow() {
            return Ok(());
        }
        if expires.is_some_and(|at| Instant::now() >= at) {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "MQTT receive deadline exceeded",
            ));
        }
        if let Some((packet, consumed)) = match decode(&buffer, config.max_packet_size) {
            Ok(value) => value,
            Err(error) => {
                if !connected
                    && matches!(error, ProtocolError::UnsupportedVersion)
                    && buffer.first() == Some(&0x10)
                {
                    output
                        .send(Packet::ConnAck {
                            session_present: false,
                            code: 1,
                        })
                        .await
                        .map_err(io::Error::other)?;
                    *flush_rejection = true;
                }
                return Err(io::Error::other(error));
            }
        } {
            buffer.drain(..consumed);
            let received = Instant::now();
            if !connected {
                let Packet::Connect {
                    client_id,
                    clean_session,
                    keep_alive,
                    will,
                    username,
                    password,
                } = packet
                else {
                    return Err(io::Error::other("CONNECT required first"));
                };
                let response = worker
                    .request(Operation::Connect {
                        client: client_id.clone(),
                        clean: clean_session,
                        will,
                        username,
                        password,
                        generation,
                        wake: wake_tx.clone(),
                        cancel: cancel_tx.clone(),
                    })
                    .await?;
                let session_present = match response {
                    Reply::Connected(present) => present,
                    Reply::Rejected(reason) => {
                        output
                            .send(Packet::ConnAck {
                                session_present: false,
                                code: reason.code(),
                            })
                            .await
                            .map_err(io::Error::other)?;
                        *flush_rejection = true;
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "CONNECT refused",
                        ));
                    }
                    _ => return Err(io::Error::other("invalid CONNECT response")),
                };
                *identity = Some(client_id);
                output
                    .send(Packet::ConnAck {
                        session_present,
                        code: 0,
                    })
                    .await
                    .map_err(io::Error::other)?;
                connected = true;
                keepalive = if keep_alive == 0 {
                    None
                } else {
                    Some(Duration::from_millis(u64::from(keep_alive) * 1500))
                };
            } else {
                if matches!(packet, Packet::Disconnect) {
                    let client = identity
                        .as_ref()
                        .ok_or_else(|| io::Error::other("missing connection identity"))?
                        .clone();
                    let Reply::Done = worker
                        .request(Operation::Disconnect {
                            client,
                            generation,
                            graceful: true,
                        })
                        .await?
                    else {
                        return Err(io::Error::other("invalid disconnect response"));
                    };
                    *identity = None;
                    return Ok(());
                }
                let client = identity
                    .as_ref()
                    .ok_or_else(|| io::Error::other("missing connection identity"))?
                    .clone();
                let acknowledged = match &packet {
                    Packet::PubAck { packet_id }
                    | Packet::PubRec { packet_id }
                    | Packet::PubComp { packet_id } => Some(*packet_id),
                    _ => None,
                };
                let Reply::Packets(replies) = worker
                    .request(Operation::Handle {
                        client,
                        generation,
                        packet,
                    })
                    .await?
                else {
                    return Err(io::Error::other("invalid packet response"));
                };
                discard_acknowledged(&mut pending, acknowledged);
                for packet in replies {
                    output.send(packet).await.map_err(io::Error::other)?;
                }
            }
            expires = keepalive.map(|duration| received + duration);
            needs_poll = true;
            tokio::task::yield_now().await;
            continue;
        }
        if connected && pending.is_none() && needs_poll {
            let client = identity
                .as_ref()
                .ok_or_else(|| io::Error::other("missing connection identity"))?
                .clone();
            let Reply::Polled(packet) = worker
                .request(Operation::Poll { client, generation })
                .await?
            else {
                return Err(io::Error::other("invalid poll response"));
            };
            pending = packet;
            needs_poll = false;
        }
        let remaining = config.max_packet_size.saturating_sub(buffer.len());
        if remaining == 0 {
            return Err(io::Error::other("receive buffer limit reached"));
        }
        let read_len = remaining.min(scratch.len());
        tokio::select! {
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { return Ok(()); } }
            _ = cancel.changed() => return Ok(()),
            _ = crate::packet_io::deadline(expires) => return Err(io::Error::new(io::ErrorKind::TimedOut,"MQTT receive deadline exceeded")),
            _ = wake.changed(), if connected => { needs_poll = true; }
            permit = output.reserve(), if pending.is_some() => {
                permit.map_err(io::Error::other)?.send(pending.take().ok_or_else(|| io::Error::other("missing pending output"))?);
                needs_poll = true;
            }
            read = reader.read(&mut scratch[..read_len]) => {
                let size = read?;
                if size == 0 { return Ok(()); }
                buffer.extend_from_slice(&scratch[..size]);
            }
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
        let (sender, receiver) = mpsc::channel(config.max_connections);
        let (ready_tx, ready_rx) = oneshot::channel();
        let handle = tokio::task::spawn_blocking(move || run_worker(receiver, config, ready_tx));
        ready_rx.await.unwrap().unwrap();
        (Worker { sender }, handle)
    }
    async fn connect(
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
    async fn handle(
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
    async fn poll(worker: &Worker, client: &str, generation: u64) -> Option<Packet> {
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
    async fn subscribe(worker: &Worker, client: &str, generation: u64) {
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
    fn publication(payload: &[u8], id: u16) -> Packet {
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
        drop(worker);
        timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
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
    async fn failed_fanout_proposal_never_installs_partial_queues() {
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
        // Routing a-empty first then b-full fails. No proposed a-empty delivery
        // may remain, and the preceding committed b-full message survives.
        assert!(
            handle(&worker, "pub", 2, publication(b"rejected", 2))
                .await
                .is_err()
        );
        assert!(poll(&worker, "a-empty", 3).await.is_none());
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
        let writer = spawn_writer(writer, receiver, config, cancel);
        let payload = vec![42; 1024 * 1024];
        output.send(publication(&payload, 1)).await.unwrap();
        output.send(publication(&payload, 2)).await.unwrap();
        drop(output);
        let closed = timeout(
            Duration::from_millis(300),
            finish_writer(writer, result, false),
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
    async fn connect_with_will(
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
    fn will(topic: &str, payload: &[u8], retain: bool) -> Will {
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
        let mut store = SqliteStore::open(&db.0, config.max_state_bytes).unwrap();
        store.commit(&before).unwrap();
        let (sender, receiver) = mpsc::channel(config.max_connections);
        let worker = Worker { sender };
        let (wake, mut notified) = watch::channel(0);
        let (cancel, _) = watch::channel(false);
        let online = HashMap::from([(
            "poller".into(),
            Online {
                generation: 1,
                wake,
                cancel,
            },
        )]);
        let task = tokio::task::spawn_blocking(move || {
            run_commands(receiver, config, Some(store), engine, online)
        });
        assert!(poll(&worker, "poller", 1).await.is_none());
        timeout(Duration::from_secs(1), notified.changed())
            .await
            .unwrap()
            .unwrap();
        stop(worker, task).await;
        let store = SqliteStore::open(&db.0, 64 * 1024 * 1024).unwrap();
        let bytes = store.load().unwrap().unwrap();
        let after = std::str::from_utf8(&bytes).unwrap();
        assert!(after.contains("\"topic\":\"fresh\""));
        assert!(after.contains("\"wills\":[]"));
    }
}

#[cfg(test)]
#[path = "live_tests.rs"]
mod live_tests;

#[cfg(test)]
#[path = "auth_tests.rs"]
mod auth_tests;
