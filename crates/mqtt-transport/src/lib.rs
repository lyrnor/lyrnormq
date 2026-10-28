//! Bounded Tokio TCP transport for MQTT 3.1.1 live and persistent QoS 0/1/2.
mod durable;
use mqtt_protocol::{Packet, ProtocolError, decode, encode};
use mqtt_router::{Router, validate_filter};
use mqtt_session::{AckAction, AckKind, IncomingQos2, OutgoingQos};
use std::{
    collections::{HashMap, HashSet},
    future::pending,
    io,
    num::NonZeroU16,
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
/// Each direction separately retains at most `max_inflight` QoS exchanges, with
/// each encoded message limited by `max_packet_size`. State is volatile and is
/// discarded on disconnect unless storage_path enables the SQLite-backed worker.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub storage_path: Option<std::path::PathBuf>,
    pub max_sessions: usize,
    pub max_state_bytes: usize,
    pub max_packet_size: usize,
    pub max_connections: usize,
    pub outbound_capacity: usize,
    pub max_subscriptions: usize,
    /// Independent limits for inbound QoS2 and outbound QoS1/2 exchanges.
    pub max_inflight: usize,
    pub handshake_timeout: Duration,
    pub write_timeout: Duration,
}
impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            storage_path: None,
            max_sessions: 1024,
            max_state_bytes: 64 * 1024 * 1024,
            max_packet_size: 1024 * 1024,
            max_connections: 1024,
            outbound_capacity: 64,
            max_subscriptions: 256,
            max_inflight: 32,
            handshake_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(5),
        }
    }
}
struct Delivery {
    publication: Arc<Packet>,
    qos: u8,
}
struct Client {
    outgoing: mpsc::Sender<Delivery>,
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
fn packet_identifier(id: Option<u16>) -> io::Result<NonZeroU16> {
    id.and_then(NonZeroU16::new)
        .ok_or_else(|| invalid("missing or zero packet identifier"))
}

/// The publication is shared among matching recipients. Each connection assigns
/// its own outbound identifier only when it drains a delivery from its queue.
fn route(state: &Mutex<State>, generation: u64, packet: Packet) -> io::Result<()> {
    let Packet::Publish { topic, qos, .. } = &packet else {
        return Err(invalid("routing requires a PUBLISH"));
    };
    let mut state = lock(state)?;
    active(&state, generation)?;
    let targets = state.router.matching_qos(topic);
    let published_qos = *qos;
    let publication = Arc::new(packet);
    let mut evict = Vec::new();
    for (target, granted_qos) in targets {
        if let Some(client) = state.clients.get(&target)
            && client
                .outgoing
                .try_send(Delivery {
                    publication: Arc::clone(&publication),
                    qos: published_qos.min(granted_qos),
                })
                .is_err()
        {
            let _ = client.cancel.send(true);
            evict.push(target);
        }
    }
    for target in evict {
        remove(&mut state, target);
    }
    Ok(())
}

async fn write_delivery(
    stream: &mut TcpStream,
    delivery: Delivery,
    outgoing: &mut OutgoingQos,
    config: &BrokerConfig,
) -> io::Result<()> {
    let Packet::Publish { topic, payload, .. } = delivery.publication.as_ref() else {
        return Err(invalid("delivery requires a PUBLISH"));
    };
    let mut packet = Packet::Publish {
        topic: topic.clone(),
        payload: payload.clone(),
        qos: delivery.qos,
        packet_id: if delivery.qos == 0 { None } else { Some(1) },
        dup: false,
        retain: false,
    };
    if delivery.qos != 0 {
        // The session retains an opaque encoded template, never reused as a
        // reconnect packet: this phase supports clean-session exchanges only.
        let bytes = encode(&packet).map_err(io::Error::other)?;
        let id = outgoing
            .start(delivery.qos, bytes)
            .map_err(io::Error::other)?;
        if let Packet::Publish { packet_id, .. } = &mut packet {
            *packet_id = Some(id.get());
        }
    }
    write_packet(stream, &packet, config).await
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
    if config.max_sessions == 0
        || config.max_state_bytes == 0
        || config.max_packet_size < 2
        || config.max_connections == 0
        || config.outbound_capacity == 0
        || config.max_subscriptions == 0
        || !(1..=65535).contains(&config.max_inflight)
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
    if config.storage_path.is_some() {
        return durable::serve(listener, config, shutdown).await;
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
    let mut incoming_qos2 = IncomingQos2::new(config.max_inflight, config.max_packet_size, 0);
    let mut outgoing_qos = OutgoingQos::new(config.max_inflight, config.max_packet_size);
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
                        publication @ Packet::Publish { .. } => {
                            let Packet::Publish {
                                qos,
                                packet_id,
                                dup,
                                ..
                            } = &publication
                            else {
                                return Err(invalid("PUBLISH expected"));
                            };
                            let (qos, packet_id, dup) = (*qos, *packet_id, *dup);
                            if qos == 2 {
                                {
                                    let state = lock(state)?;
                                    active(&state, generation)?;
                                }
                                let bytes = encode(&publication).map_err(io::Error::other)?;
                                incoming_qos2
                                    .publish(packet_identifier(packet_id)?, &bytes, dup)
                                    .map_err(io::Error::other)?;
                                write_packet(
                                    &mut stream,
                                    &Packet::PubRec {
                                        packet_id: packet_identifier(packet_id)?.get(),
                                    },
                                    config,
                                )
                                .await?;
                            } else {
                                route(state, generation, publication)?;
                                if qos == 1 {
                                    write_packet(
                                        &mut stream,
                                        &Packet::PubAck {
                                            packet_id: packet_identifier(packet_id)?.get(),
                                        },
                                        config,
                                    )
                                    .await?;
                                }
                            }
                        }
                        Packet::PubRel { packet_id } => {
                            {
                                let state = lock(state)?;
                                active(&state, generation)?;
                            }
                            let released =
                                incoming_qos2.pubrel(packet_identifier(Some(packet_id))?);
                            if let Some(bytes) = released.delivery {
                                let Some((publication, consumed)) =
                                    decode(&bytes, config.max_packet_size)
                                        .map_err(io::Error::other)?
                                else {
                                    return Err(invalid("incomplete stored PUBLISH"));
                                };
                                if consumed != bytes.len() {
                                    return Err(invalid("stored PUBLISH has trailing bytes"));
                                }
                                route(state, generation, publication)?;
                            }
                            write_packet(&mut stream, &Packet::PubComp { packet_id }, config)
                                .await?;
                        }
                        acknowledgement @ (Packet::PubAck { .. }
                        | Packet::PubRec { .. }
                        | Packet::PubComp { .. }) => {
                            let (packet_id, kind) = match acknowledgement {
                                Packet::PubAck { packet_id } => (packet_id, AckKind::PubAck),
                                Packet::PubRec { packet_id } => (packet_id, AckKind::PubRec),
                                Packet::PubComp { packet_id } => (packet_id, AckKind::PubComp),
                                _ => return Err(invalid("invalid acknowledgement")),
                            };
                            if outgoing_qos
                                .ack(packet_identifier(Some(packet_id))?, kind)
                                .map_err(io::Error::other)?
                                == AckAction::SendPubRel
                            {
                                write_packet(&mut stream, &Packet::PubRel { packet_id }, config)
                                    .await?;
                            }
                        }
                        Packet::Subscribe { packet_id, filters } => {
                            let codes = {
                                let mut state = lock(state)?;
                                active(&state, generation)?;
                                filters
                                    .into_iter()
                                    .map(|(filter, qos)| {
                                        if (!subscriptions.contains(&filter)
                                            && subscriptions.len() >= config.max_subscriptions)
                                            || !state.router.subscribe_qos(
                                                generation,
                                                filter.clone(),
                                                qos,
                                            )
                                        {
                                            0x80
                                        } else {
                                            subscriptions.insert(filter);
                                            qos
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
        // Fair selection prevents continuous queued traffic starving ACK/PING reads.
        tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            _ = cancel.changed() => return Ok(()),
            _ = deadline(expires) => return Err(io::Error::new(io::ErrorKind::TimedOut, "MQTT receive deadline exceeded")),
            delivery = deliveries.recv(), if connected && !outgoing_qos.is_full() => {
                if let Some(delivery) = delivery { write_delivery(&mut stream, delivery, &mut outgoing_qos, config).await?; }
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
        for config in [
            BrokerConfig {
                outbound_capacity: 0,
                ..BrokerConfig::default()
            },
            BrokerConfig {
                max_inflight: 0,
                ..BrokerConfig::default()
            },
            BrokerConfig {
                max_inflight: 65536,
                ..BrokerConfig::default()
            },
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (_tx, rx) = watch::channel(false);
            assert_eq!(
                serve(listener, config, rx).await.unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
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
                codes: vec![2, 1, 0x80]
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
                qos: 0,
                packet_id: None,
                dup: false,
                retain: false,
            },
        )
        .await;
        assert_eq!(
            receive(&mut new).await,
            Packet::Publish {
                topic: "new".into(),
                payload: vec![42],
                qos: 0,
                packet_id: None,
                dup: false,
                retain: false,
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
    fn publication(topic: &str, payload: &[u8], qos: u8, packet_id: Option<u16>) -> Packet {
        Packet::Publish {
            topic: topic.into(),
            payload: payload.to_vec(),
            qos,
            packet_id,
            dup: false,
            retain: false,
        }
    }
    async fn subscribe(stream: &mut TcpStream, topic: &str, qos: u8) {
        send(
            stream,
            Packet::Subscribe {
                packet_id: 1,
                filters: vec![(topic.into(), qos)],
            },
        )
        .await;
        assert_eq!(
            receive(stream).await,
            Packet::SubAck {
                packet_id: 1,
                codes: vec![qos]
            }
        );
    }
    async fn no_packet(stream: &mut TcpStream) {
        let mut byte = [0];
        assert!(
            timeout(Duration::from_millis(40), stream.read(&mut byte))
                .await
                .is_err()
        );
    }
    fn delivered_id(packet: Packet, expected_qos: u8, expected_payload: &[u8]) -> u16 {
        let Packet::Publish {
            qos,
            packet_id,
            payload,
            dup,
            retain,
            ..
        } = packet
        else {
            panic!("expected PUBLISH")
        };
        assert_eq!(qos, expected_qos);
        assert_eq!(payload, expected_payload);
        assert!(!dup);
        assert!(!retain);
        packet_id.unwrap()
    }
    #[tokio::test]
    async fn qos2_full_outgoing_window_keeps_ack_reads_live() {
        let (address, tx, handle) = broker(BrokerConfig {
            max_inflight: 1,
            ..BrokerConfig::default()
        })
        .await;
        let mut sub = connected(address, "qos2-sub").await;
        let mut publisher = connected(address, "qos2-pub").await;
        subscribe(&mut sub, "a", 2).await;
        send(&mut publisher, publication("a", b"first", 2, Some(55))).await;
        assert_eq!(
            receive(&mut publisher).await,
            Packet::PubRec { packet_id: 55 }
        );
        no_packet(&mut sub).await;
        let mut duplicate = publication("a", b"ignored duplicate", 2, Some(55));
        if let Packet::Publish { dup, .. } = &mut duplicate {
            *dup = true;
        }
        send(&mut publisher, duplicate).await;
        assert_eq!(
            receive(&mut publisher).await,
            Packet::PubRec { packet_id: 55 }
        );
        send(&mut publisher, Packet::PubRel { packet_id: 55 }).await;
        assert_eq!(
            receive(&mut publisher).await,
            Packet::PubComp { packet_id: 55 }
        );
        let first = delivered_id(receive(&mut sub).await, 2, b"first");
        assert_ne!(first, 55);
        send(&mut sub, Packet::PubRec { packet_id: first }).await;
        assert_eq!(receive(&mut sub).await, Packet::PubRel { packet_id: first });
        send(&mut sub, Packet::PubRec { packet_id: first }).await;
        assert_eq!(receive(&mut sub).await, Packet::PubRel { packet_id: first });
        // Duplicated incoming PUBREL returns PUBCOMP without another delivery.
        send(&mut publisher, Packet::PubRel { packet_id: 55 }).await;
        assert_eq!(
            receive(&mut publisher).await,
            Packet::PubComp { packet_id: 55 }
        );
        send(&mut publisher, publication("a", b"second", 1, Some(56))).await;
        assert_eq!(
            receive(&mut publisher).await,
            Packet::PubAck { packet_id: 56 }
        );
        no_packet(&mut sub).await;
        send(&mut sub, Packet::PubComp { packet_id: first }).await;
        let second = delivered_id(receive(&mut sub).await, 1, b"second");
        assert_ne!(first, second);
        send(&mut sub, Packet::PubAck { packet_id: second }).await;
        send(&mut sub, Packet::PingReq).await;
        assert_eq!(receive(&mut sub).await, Packet::PingResp);
        stop(tx, handle).await;
    }
    #[tokio::test]
    async fn inbound_outbound_packet_identifiers_are_independent() {
        let (address, tx, handle) = broker(BrokerConfig {
            max_inflight: 1,
            ..BrokerConfig::default()
        })
        .await;
        let mut stream = connected(address, "both-directions").await;
        subscribe(&mut stream, "a", 1).await;
        send(&mut stream, publication("a", b"one", 1, Some(99))).await;
        assert_eq!(receive(&mut stream).await, Packet::PubAck { packet_id: 99 });
        let outgoing = delivered_id(receive(&mut stream).await, 1, b"one");
        send(&mut stream, publication("a", b"two", 2, Some(outgoing))).await;
        assert_eq!(
            receive(&mut stream).await,
            Packet::PubRec {
                packet_id: outgoing
            }
        );
        send(
            &mut stream,
            Packet::PubRel {
                packet_id: outgoing,
            },
        )
        .await;
        assert_eq!(
            receive(&mut stream).await,
            Packet::PubComp {
                packet_id: outgoing
            }
        );
        no_packet(&mut stream).await;
        send(
            &mut stream,
            Packet::PubAck {
                packet_id: outgoing,
            },
        )
        .await;
        let next = delivered_id(receive(&mut stream).await, 1, b"two");
        assert_ne!(next, outgoing);
        send(&mut stream, Packet::PubAck { packet_id: next }).await;
        stop(tx, handle).await;
    }
    #[tokio::test]
    async fn incoming_qos2_window_limit_disconnects_without_acknowledging_excess() {
        let (address, tx, handle) = broker(BrokerConfig {
            max_inflight: 1,
            ..BrokerConfig::default()
        })
        .await;
        let mut stream = connected(address, "incoming-full").await;
        send(&mut stream, publication("a", b"one", 2, Some(1))).await;
        assert_eq!(receive(&mut stream).await, Packet::PubRec { packet_id: 1 });
        send(&mut stream, publication("a", b"two", 2, Some(2))).await;
        closed(&mut stream).await;
        stop(tx, handle).await;
    }
    #[tokio::test]
    async fn wrong_stage_ack_disconnects_and_unknown_pubrel_is_idempotent() {
        let (address, tx, handle) = broker(BrokerConfig::default()).await;
        let mut stream = connected(address, "wrong-stage").await;
        subscribe(&mut stream, "a", 2).await;
        send(&mut stream, Packet::PubRel { packet_id: 321 }).await;
        assert_eq!(
            receive(&mut stream).await,
            Packet::PubComp { packet_id: 321 }
        );
        send(&mut stream, publication("a", b"one", 2, Some(11))).await;
        assert_eq!(receive(&mut stream).await, Packet::PubRec { packet_id: 11 });
        send(&mut stream, Packet::PubRel { packet_id: 11 }).await;
        assert_eq!(
            receive(&mut stream).await,
            Packet::PubComp { packet_id: 11 }
        );
        let outgoing = delivered_id(receive(&mut stream).await, 2, b"one");
        send(
            &mut stream,
            Packet::PubComp {
                packet_id: outgoing,
            },
        )
        .await;
        closed(&mut stream).await;
        stop(tx, handle).await;
    }
    #[tokio::test]
    async fn qos_is_downgraded_to_publication_and_subscription_minimum() {
        let (address, tx, handle) = broker(BrokerConfig::default()).await;
        let mut sub = connected(address, "downgrade-sub").await;
        let mut publisher = connected(address, "downgrade-pub").await;
        subscribe(&mut sub, "a", 2).await;
        send(&mut publisher, publication("a", b"zero", 0, None)).await;
        assert_eq!(receive(&mut sub).await, publication("a", b"zero", 0, None));
        subscribe(&mut sub, "a", 0).await;
        send(&mut publisher, publication("a", b"two", 2, Some(5))).await;
        assert_eq!(
            receive(&mut publisher).await,
            Packet::PubRec { packet_id: 5 }
        );
        send(&mut publisher, Packet::PubRel { packet_id: 5 }).await;
        assert_eq!(
            receive(&mut publisher).await,
            Packet::PubComp { packet_id: 5 }
        );
        assert_eq!(receive(&mut sub).await, publication("a", b"two", 0, None));
        stop(tx, handle).await;
    }
    #[tokio::test]
    async fn continuous_qos0_delivery_does_not_starve_ping_reads() {
        // Drive a connection's real queue without router overflow so it remains
        // continuously ready; PING reads must compete fairly with socket writes.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(State::default()));
        let connection_state = Arc::clone(&state);
        let (stop_tx, stop_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            connection(
                stream,
                1,
                &connection_state,
                &BrokerConfig {
                    outbound_capacity: 1024,
                    ..BrokerConfig::default()
                },
                stop_rx,
            )
            .await
        });
        let mut stream = connected(address, "fairness").await;
        let sender = {
            lock(&state)
                .unwrap()
                .clients
                .get(&1)
                .unwrap()
                .outgoing
                .clone()
        };
        let message = Arc::new(publication("a", b"x", 0, None));
        for _ in 0..1024 {
            sender
                .try_send(Delivery {
                    publication: Arc::clone(&message),
                    qos: 0,
                })
                .unwrap_or_else(|_| panic!("queue unexpectedly full"));
        }
        let producer = tokio::spawn(async move {
            while sender
                .send(Delivery {
                    publication: Arc::clone(&message),
                    qos: 0,
                })
                .await
                .is_ok()
            {}
        });
        send(&mut stream, Packet::PingReq).await;
        let response = timeout(Duration::from_millis(500), async {
            loop {
                if receive(&mut stream).await == Packet::PingResp {
                    break;
                }
            }
        })
        .await;
        producer.abort();
        stop_tx.send(true).unwrap();
        timeout(Duration::from_secs(2), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            response.is_ok(),
            "continuous queued delivery starved PINGREQ"
        );
    }
    #[test]
    fn removed_generation_cannot_mutate_routing() {
        let state = State::default();
        assert!(active(&state, 1).is_err());
    }
}
