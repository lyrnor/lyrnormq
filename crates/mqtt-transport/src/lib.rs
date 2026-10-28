//! Bounded Tokio TCP transport for the Phase 1 MQTT 3.1.1 QoS 0 subset.
use mqtt_protocol::{Packet, ProtocolError, decode, encode};
use mqtt_router::{Router, validate_filter};
use std::{
    collections::{HashMap, HashSet},
    future::pending,
    io,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc, watch},
    task::JoinSet,
    time::{Instant, sleep_until, timeout},
};

/// Limits apply per connection except for `max_connections`. Queue capacity counts
/// packets; maximum queued payload memory is bounded by capacity × packet size.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub max_packet_size: usize,
    pub max_connections: usize,
    pub outbound_capacity: usize,
    pub max_subscriptions: usize,
    pub handshake_timeout: Duration,
    pub write_timeout: Duration,
}
impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            max_packet_size: 1024 * 1024,
            max_connections: 1024,
            outbound_capacity: 64,
            max_subscriptions: 256,
            handshake_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(5),
        }
    }
}
struct Client {
    outgoing: mpsc::Sender<Arc<Packet>>,
    cancel: watch::Sender<bool>,
}
#[derive(Default)]
struct State {
    router: Router,
    clients: HashMap<u64, Client>,
    identities: HashMap<String, u64>,
}
fn lock(state: &Mutex<State>) -> io::Result<MutexGuard<'_, State>> {
    state
        .lock()
        .map_err(|_| io::Error::other("broker state poisoned"))
}
fn active(state: &State, generation: u64) -> io::Result<()> {
    if state.clients.contains_key(&generation) {
        Ok(())
    } else {
        Err(invalid("connection superseded"))
    }
}
fn remove(state: &mut State, generation: u64) {
    state.router.remove_client(generation);
    state.clients.remove(&generation);
    state.identities.retain(|_, current| *current != generation);
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
async fn write_packet(
    stream: &mut TcpStream,
    packet: &Packet,
    config: &BrokerConfig,
) -> io::Result<()> {
    let bytes = encode(packet).map_err(io::Error::other)?;
    if bytes.len() > config.max_packet_size {
        return Err(invalid("outgoing packet exceeds limit"));
    }
    timeout(config.write_timeout, stream.write_all(&bytes))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "client write timed out"))?
}
async fn deadline(at: Option<Instant>) {
    match at {
        Some(at) => sleep_until(at).await,
        None => pending::<()>().await,
    }
}

/// Serve until shutdown becomes true or its sender is dropped. All connection
/// tasks are joined before returning. Excess connections are closed immediately.
pub async fn serve(
    listener: TcpListener,
    config: BrokerConfig,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    if config.max_packet_size < 2
        || config.max_connections == 0
        || config.outbound_capacity == 0
        || config.max_subscriptions == 0
        || config.handshake_timeout.is_zero()
        || config.write_timeout.is_zero()
        || Instant::now()
            .checked_add(config.handshake_timeout)
            .is_none()
        || Instant::now().checked_add(config.write_timeout).is_none()
        || config.outbound_capacity > Semaphore::MAX_PERMITS
        || config.max_connections > Semaphore::MAX_PERMITS
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid broker resource limits",
        ));
    }
    let state = Arc::new(Mutex::new(State::default()));
    let permits = Arc::new(Semaphore::new(config.max_connections));
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut tasks = JoinSet::new();
    let mut generation = 0u64;
    let result = loop {
        if *shutdown.borrow() {
            break Ok(());
        }
        tokio::select! {
            biased;
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break Ok(()); } }
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(error) = result { break Err(io::Error::other(error)); }
            }
            accepted = listener.accept() => {
                let (stream, _) = match accepted { Ok(value) => value, Err(error) => break Err(error) };
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else { drop(stream); continue; };
                generation = match generation.checked_add(1) { Some(value) => value, None => break Err(io::Error::other("connection generation exhausted")) };
                let state = Arc::clone(&state);
                let config = config.clone();
                let stop = stop_rx.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    let _ = connection(stream, generation, &state, &config, stop).await;
                    if let Ok(mut state) = lock(&state) { remove(&mut state, generation); }
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
    result
}

async fn connection(
    mut stream: TcpStream,
    generation: u64,
    state: &Mutex<State>,
    config: &BrokerConfig,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let (outgoing, mut deliveries) = mpsc::channel(config.outbound_capacity);
    let (cancel_tx, mut cancel) = watch::channel(false);
    let mut connected = false;
    let mut subscriptions = HashSet::new();
    let mut keep_alive = None;
    let mut expires = Some(Instant::now() + config.handshake_timeout);
    let mut buffer = Vec::new();
    let mut scratch = [0u8; 8192];
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
        match decode(&buffer, config.max_packet_size) {
            Ok(Some((packet, consumed))) => {
                let received_at = Instant::now();
                buffer.drain(..consumed);
                if !connected {
                    let Packet::Connect {
                        client_id,
                        clean_session: true,
                        keep_alive: seconds,
                    } = packet
                    else {
                        return Err(invalid("CONNECT required first"));
                    };
                    {
                        let mut state = lock(state)?;
                        if let Some(old) = state.identities.get(&client_id).copied() {
                            if let Some(client) = state.clients.get(&old) {
                                let _ = client.cancel.send(true);
                            }
                            remove(&mut state, old);
                        }
                        state.identities.insert(client_id, generation);
                        state.clients.insert(
                            generation,
                            Client {
                                outgoing: outgoing.clone(),
                                cancel: cancel_tx.clone(),
                            },
                        );
                    }
                    write_packet(
                        &mut stream,
                        &Packet::ConnAck {
                            session_present: false,
                            code: 0,
                        },
                        config,
                    )
                    .await?;
                    connected = true;
                    keep_alive = if seconds == 0 {
                        None
                    } else {
                        Some(Duration::from_millis(u64::from(seconds) * 1500))
                    };
                } else {
                    match packet {
                        Packet::Publish { topic, payload } => {
                            let mut state = lock(state)?;
                            active(&state, generation)?;
                            let targets = state.router.matching_clients(&topic);
                            let packet = Arc::new(Packet::Publish { topic, payload });
                            let mut evict = Vec::new();
                            for target in targets {
                                if let Some(client) = state.clients.get(&target)
                                    && client.outgoing.try_send(Arc::clone(&packet)).is_err()
                                {
                                    let _ = client.cancel.send(true);
                                    evict.push(target);
                                }
                            }
                            for target in evict {
                                remove(&mut state, target);
                            }
                        }
                        Packet::Subscribe { packet_id, filters } => {
                            let codes = {
                                let mut state = lock(state)?;
                                active(&state, generation)?;
                                filters
                                    .into_iter()
                                    .map(|(filter, _)| {
                                        if (!subscriptions.contains(&filter)
                                            && subscriptions.len() >= config.max_subscriptions)
                                            || !state.router.subscribe(generation, filter.clone())
                                        {
                                            0x80
                                        } else {
                                            subscriptions.insert(filter);
                                            0
                                        }
                                    })
                                    .collect()
                            };
                            write_packet(&mut stream, &Packet::SubAck { packet_id, codes }, config)
                                .await?;
                        }
                        Packet::Unsubscribe { packet_id, filters } => {
                            if filters.iter().any(|filter| !validate_filter(filter)) {
                                return Err(invalid("invalid unsubscribe filter"));
                            }
                            {
                                let mut state = lock(state)?;
                                active(&state, generation)?;
                                for filter in filters {
                                    state.router.unsubscribe(generation, &filter);
                                    subscriptions.remove(&filter);
                                }
                            }
                            write_packet(&mut stream, &Packet::UnsubAck { packet_id }, config)
                                .await?;
                        }
                        Packet::PingReq => {
                            write_packet(&mut stream, &Packet::PingResp, config).await?
                        }
                        Packet::Disconnect => return Ok(()),
                        _ => {
                            return Err(invalid(
                                "unexpected client packet direction or repeated CONNECT",
                            ));
                        }
                    }
                }
                expires = keep_alive.map(|duration| received_at + duration);
                // Yield also under an endless stream of already-buffered packets.
                tokio::task::yield_now().await;
                continue;
            }
            Err(error) => {
                if !connected
                    && matches!(error, ProtocolError::UnsupportedVersion)
                    && buffer.first() == Some(&0x10)
                {
                    write_packet(
                        &mut stream,
                        &Packet::ConnAck {
                            session_present: false,
                            code: 1,
                        },
                        config,
                    )
                    .await?;
                }
                return Err(io::Error::new(io::ErrorKind::InvalidData, error));
            }
            Ok(None) => {}
        }
        let remaining = config.max_packet_size.saturating_sub(buffer.len());
        if remaining == 0 {
            return Err(invalid("receive buffer limit reached"));
        }
        let read_len = scratch.len().min(remaining);
        tokio::select! {
            biased;
            _ = shutdown.changed() => return Ok(()),
            _ = cancel.changed() => return Ok(()),
            _ = deadline(expires) => return Err(io::Error::new(io::ErrorKind::TimedOut, "MQTT receive deadline exceeded")),
            delivery = deliveries.recv(), if connected => {
                if let Some(packet) = delivery { write_packet(&mut stream, &packet, config).await?; }
            }
            read = stream.read(&mut scratch[..read_len]) => {
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
    use std::net::SocketAddr;
    use tokio::task::JoinHandle;

    async fn broker(
        config: BrokerConfig,
    ) -> (SocketAddr, watch::Sender<bool>, JoinHandle<io::Result<()>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(serve(listener, config, rx));
        (address, tx, handle)
    }
    async fn send(stream: &mut TcpStream, packet: Packet) {
        stream.write_all(&encode(&packet).unwrap()).await.unwrap();
    }
    async fn receive(stream: &mut TcpStream) -> Packet {
        timeout(Duration::from_secs(2), async {
            let mut bytes = Vec::new();
            loop {
                if let Some((packet, _)) = decode(&bytes, 4096).unwrap() {
                    return packet;
                }
                let mut byte = [0];
                stream.read_exact(&mut byte).await.unwrap();
                bytes.push(byte[0]);
            }
        })
        .await
        .unwrap()
    }
    async fn connected(address: SocketAddr, name: &str) -> TcpStream {
        let mut stream = TcpStream::connect(address).await.unwrap();
        send(
            &mut stream,
            Packet::Connect {
                client_id: name.into(),
                clean_session: true,
                keep_alive: 0,
            },
        )
        .await;
        assert_eq!(
            receive(&mut stream).await,
            Packet::ConnAck {
                session_present: false,
                code: 0
            }
        );
        stream
    }
    async fn closed(stream: &mut TcpStream) {
        let mut byte = [0];
        assert!(matches!(
            timeout(Duration::from_secs(2), stream.read(&mut byte)).await,
            Ok(Ok(0)) | Ok(Err(_))
        ));
    }
    async fn stop(tx: watch::Sender<bool>, handle: JoinHandle<io::Result<()>>) {
        tx.send(true).unwrap();
        timeout(Duration::from_secs(2), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    #[tokio::test]
    async fn zero_limits_rejected() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (_tx, rx) = watch::channel(false);
        let config = BrokerConfig {
            outbound_capacity: 0,
            ..BrokerConfig::default()
        };
        assert_eq!(
            serve(listener, config, rx).await.unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
    #[tokio::test]
    async fn handshake_timeout_and_connection_capacity() {
        let config = BrokerConfig {
            max_connections: 1,
            handshake_timeout: Duration::from_millis(100),
            ..BrokerConfig::default()
        };
        let (address, tx, handle) = broker(config).await;
        let mut first = connected(address, "one").await;
        let mut excess = TcpStream::connect(address).await.unwrap();
        closed(&mut excess).await;
        send(&mut first, Packet::Disconnect).await;
        closed(&mut first).await;
        // Completion of EOF precedes the task dropping its permit by a few instructions.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut idle = TcpStream::connect(address).await.unwrap();
        closed(&mut idle).await;
        stop(tx, handle).await;
    }
    #[tokio::test]
    async fn subscription_limit_and_replacement() {
        let (address, tx, handle) = broker(BrokerConfig {
            max_subscriptions: 2,
            ..BrokerConfig::default()
        })
        .await;
        let mut stream = connected(address, "subscriber").await;
        send(
            &mut stream,
            Packet::Subscribe {
                packet_id: 1,
                filters: vec![("a".into(), 2), ("b".into(), 1), ("c".into(), 0)],
            },
        )
        .await;
        assert_eq!(
            receive(&mut stream).await,
            Packet::SubAck {
                packet_id: 1,
                codes: vec![0, 0, 0x80]
            }
        );
        send(
            &mut stream,
            Packet::Subscribe {
                packet_id: 2,
                filters: vec![("a".into(), 0)],
            },
        )
        .await;
        assert_eq!(
            receive(&mut stream).await,
            Packet::SubAck {
                packet_id: 2,
                codes: vec![0]
            }
        );
        send(
            &mut stream,
            Packet::Unsubscribe {
                packet_id: 3,
                filters: vec!["b".into()],
            },
        )
        .await;
        assert_eq!(
            receive(&mut stream).await,
            Packet::UnsubAck { packet_id: 3 }
        );
        send(
            &mut stream,
            Packet::Subscribe {
                packet_id: 4,
                filters: vec![("c".into(), 0)],
            },
        )
        .await;
        assert_eq!(
            receive(&mut stream).await,
            Packet::SubAck {
                packet_id: 4,
                codes: vec![0]
            }
        );
        stop(tx, handle).await;
        closed(&mut stream).await;
    }
    #[tokio::test]
    async fn advertised_oversize_closed_before_payload() {
        let (address, tx, handle) = broker(BrokerConfig {
            max_packet_size: 32,
            ..BrokerConfig::default()
        })
        .await;
        let mut stream = connected(address, "small").await;
        stream.write_all(&[0x30, 100]).await.unwrap();
        closed(&mut stream).await;
        stop(tx, handle).await;
    }
    #[tokio::test]
    async fn takeover_removes_old_subscriptions_and_old_cleanup_is_safe() {
        let (address, tx, handle) = broker(BrokerConfig::default()).await;
        let mut old = connected(address, "same").await;
        send(
            &mut old,
            Packet::Subscribe {
                packet_id: 1,
                filters: vec![("old".into(), 0)],
            },
        )
        .await;
        receive(&mut old).await;
        let mut new = connected(address, "same").await;
        closed(&mut old).await;
        send(
            &mut new,
            Packet::Subscribe {
                packet_id: 1,
                filters: vec![("new".into(), 0)],
            },
        )
        .await;
        receive(&mut new).await;
        send(
            &mut new,
            Packet::Publish {
                topic: "new".into(),
                payload: vec![42],
            },
        )
        .await;
        assert_eq!(
            receive(&mut new).await,
            Packet::Publish {
                topic: "new".into(),
                payload: vec![42]
            }
        );
        stop(tx, handle).await;
    }
    #[tokio::test]
    async fn false_shutdown_notification_keeps_server_running() {
        let (address, tx, handle) = broker(BrokerConfig::default()).await;
        tx.send(false).unwrap();
        let mut stream = connected(address, "alive").await;
        send(&mut stream, Packet::PingReq).await;
        assert_eq!(receive(&mut stream).await, Packet::PingResp);
        stop(tx, handle).await;
    }
    #[tokio::test]
    async fn invalid_unsubscribe_filter_closes_connection() {
        let (address, tx, handle) = broker(BrokerConfig::default()).await;
        let mut stream = connected(address, "invalid").await;
        send(
            &mut stream,
            Packet::Unsubscribe {
                packet_id: 1,
                filters: vec!["a#".into()],
            },
        )
        .await;
        closed(&mut stream).await;
        stop(tx, handle).await;
    }
    #[test]
    fn removed_generation_cannot_mutate_routing() {
        let state = State::default();
        assert!(active(&state, 1).is_err());
    }
}
