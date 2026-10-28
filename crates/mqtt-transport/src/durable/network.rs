//! Versioned socket IO; all business mutations remain on the shared worker.
use super::{Operation, Reply, Worker, WorkerFailure, failure};
use crate::BrokerConfig;
use mqtt_protocol::{
    Frame, Packet, Property, ProtocolError, ProtocolVersion, ReasonCode, decode_frame,
    encode_frame, peek_connect_version,
};
use std::{
    collections::BTreeMap,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Ticket {
    id: u16,
    serial: u64,
}
struct Credit {
    serial: u64,
    qos: u8,
    finishing: bool,
}
struct Credits {
    entries: BTreeMap<u16, Credit>,
    next: u64,
    limit: usize,
}
impl Credits {
    fn new(limit: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            next: 0,
            limit,
        }
    }
    fn register(&mut self, id: u16, qos: u8) -> io::Result<()> {
        if let Some(previous) = self.entries.get(&id) {
            if previous.qos != qos || previous.finishing {
                return Err(failure(
                    ReasonCode::PROTOCOL_ERROR,
                    "packet identifier reused before completion",
                ));
            }
            return Ok(());
        }
        if self.entries.len() >= self.limit {
            return Err(failure(
                ReasonCode::RECEIVE_MAXIMUM_EXCEEDED,
                "Receive Maximum exceeded",
            ));
        }
        self.next = self.next.checked_add(1).ok_or_else(|| {
            failure(
                ReasonCode::SERVER_UNAVAILABLE,
                "credit generation exhausted",
            )
        })?;
        self.entries.insert(
            id,
            Credit {
                serial: self.next,
                qos,
                finishing: false,
            },
        );
        Ok(())
    }
    fn completion(&mut self, frame: &Frame) -> Option<Ticket> {
        let (id, qos) = match frame.packet {
            Packet::PubAck { packet_id } => (packet_id, 1),
            Packet::PubComp { packet_id } => (packet_id, 2),
            Packet::PubRec { packet_id } if frame.reason_code.value() >= 128 => (packet_id, 2),
            _ => return None,
        };
        let credit = self.entries.get_mut(&id)?;
        if credit.qos != qos {
            return None;
        }
        credit.finishing = true;
        Some(Ticket {
            id,
            serial: credit.serial,
        })
    }
    fn complete(&mut self, ticket: Ticket) {
        if self
            .entries
            .get(&ticket.id)
            .is_some_and(|credit| credit.serial == ticket.serial)
        {
            self.entries.remove(&ticket.id);
        }
    }
}
type SharedCredits = Arc<Mutex<Credits>>;
fn locked(credits: &SharedCredits) -> io::Result<std::sync::MutexGuard<'_, Credits>> {
    credits
        .lock()
        .map_err(|_| failure(ReasonCode::SERVER_UNAVAILABLE, "credit ledger unavailable"))
}
pub(super) struct Output {
    frame: Frame,
    completion: Option<Ticket>,
    written: Option<oneshot::Sender<()>>,
}
fn outgoing(frame: Frame, credits: &SharedCredits) -> io::Result<Output> {
    let completion = locked(credits)?.completion(&frame);
    Ok(Output {
        frame,
        completion,
        written: None,
    })
}
fn frame_for(version: ProtocolVersion, packet: Packet) -> Frame {
    match version {
        ProtocolVersion::V311 => Frame::v311(packet),
        ProtocolVersion::V5 => Frame::v5(packet),
    }
}
fn protocol_reason(error: &ProtocolError) -> ReasonCode {
    match error {
        ProtocolError::Malformed(_) => ReasonCode::MALFORMED_PACKET,
        ProtocolError::ProtocolViolation(_) => ReasonCode::PROTOCOL_ERROR,
        ProtocolError::Mqtt5(reason) => *reason,
        ProtocolError::PacketTooLarge => ReasonCode::PACKET_TOO_LARGE,
        ProtocolError::UnsupportedVersion => ReasonCode::UNSUPPORTED_PROTOCOL_VERSION,
        ProtocolError::UnsupportedFeature(_) => ReasonCode::IMPLEMENTATION_SPECIFIC_ERROR,
    }
}
fn io_reason(error: &io::Error) -> ReasonCode {
    error
        .get_ref()
        .and_then(|value| value.downcast_ref::<WorkerFailure>())
        .map(|value| value.reason)
        .unwrap_or(if error.kind() == io::ErrorKind::TimedOut {
            ReasonCode::KEEP_ALIVE_TIMEOUT
        } else {
            ReasonCode::IMPLEMENTATION_SPECIFIC_ERROR
        })
}
async fn write_frame(
    writer: &mut OwnedWriteHalf,
    frame: &Frame,
    config: &BrokerConfig,
) -> io::Result<()> {
    let bytes = encode_frame(frame).map_err(io::Error::other)?;
    if bytes.len() > config.max_packet_size {
        return Err(failure(
            ReasonCode::PACKET_TOO_LARGE,
            "outgoing packet exceeds limit",
        ));
    }
    timeout(config.write_timeout, writer.write_all(&bytes))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "socket write timed out"))?
}
fn spawn_writer(
    mut writer: OwnedWriteHalf,
    mut output: mpsc::Receiver<Output>,
    mut terminal: watch::Receiver<Option<Frame>>,
    credits: SharedCredits,
    config: BrokerConfig,
    cancel: watch::Sender<bool>,
    failed: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<io::Result<()>> {
    tokio::spawn(async move {
        let result = async {
            loop {
                let final_frame = terminal.borrow_and_update().clone();
                if let Some(frame) = final_frame {
                    write_frame(&mut writer, &frame, &config).await?;
                    return Ok(());
                }
                tokio::select! {
                    biased;
                    changed=terminal.changed()=>{if changed.is_err(){return Ok(());}}
                    message=output.recv()=>{
                        let Some(message)=message else{return Ok(());};
                        // No select/cancellation inside a frame: terminal output may only
                        // follow a completed frame, never bytes from a partial PUBLISH.
                        write_frame(&mut writer,&message.frame,&config).await?;
                        if let Some(ticket)=message.completion {locked(&credits)?.complete(ticket);}
                        if let Some(written)=message.written {let _=written.send(());}
                    }
                }
            }
        }
        .await;
        if result.is_err() {
            failed.store(true, Ordering::Release);
            let _ = cancel.send(true);
        }
        result
    })
}
pub(super) fn connack(
    version: ProtocolVersion,
    present: bool,
    code: u8,
    config: &BrokerConfig,
) -> Frame {
    let mut frame = frame_for(
        version,
        Packet::ConnAck {
            session_present: present,
            code,
        },
    );
    if version == ProtocolVersion::V5 && code == 0 {
        frame.properties = vec![
            Property::ReceiveMaximum(config.max_inflight.min(u16::MAX as usize) as u16),
            Property::MaximumPacketSize(config.max_packet_size.min(u32::MAX as usize) as u32),
            Property::TopicAliasMaximum(0),
            Property::SharedSubscriptionAvailable(0),
            Property::SubscriptionIdentifierAvailable(0),
        ];
    }
    frame
}
struct Context<'a> {
    generation: u64,
    worker: &'a Worker,
    config: &'a BrokerConfig,
    identity: &'a mut Option<String>,
    version: &'a mut Option<ProtocolVersion>,
    connected: &'a mut bool,
    output: &'a mpsc::Sender<Output>,
    terminal: &'a watch::Sender<Option<Frame>>,
    credits: &'a SharedCredits,
    wake_tx: watch::Sender<u64>,
    wake: watch::Receiver<u64>,
    cancel_tx: watch::Sender<bool>,
    cancel: watch::Receiver<bool>,
    close_tx: watch::Sender<Option<ReasonCode>>,
    close: watch::Receiver<Option<ReasonCode>>,
    shutdown: watch::Receiver<bool>,
    flush: &'a mut bool,
}
fn terminal_frame(context: &Context<'_>, frame: Frame) {
    context.terminal.send_replace(Some(frame));
}
pub(super) async fn connection_loop(
    stream: TcpStream,
    generation: u64,
    worker: &Worker,
    config: &BrokerConfig,
    shutdown: watch::Receiver<bool>,
    identity: &mut Option<String>,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let (mut reader, writer) = stream.into_split();
    let (output, receiver) = mpsc::channel(config.outbound_capacity);
    let (terminal, terminal_rx) = watch::channel(None);
    let (wake_tx, wake) = watch::channel(0u64);
    let (cancel_tx, cancel) = watch::channel(false);
    let (close_tx, close) = watch::channel(None);
    let credits = Arc::new(Mutex::new(Credits::new(config.max_inflight)));
    let failed = Arc::new(AtomicBool::new(false));
    let writer = spawn_writer(
        writer,
        receiver,
        terminal_rx,
        credits.clone(),
        config.clone(),
        cancel_tx.clone(),
        failed.clone(),
    );
    let mut version = None;
    let mut connected = false;
    let mut flush = false;
    let result = drive(
        &mut reader,
        Context {
            generation,
            worker,
            config,
            identity,
            version: &mut version,
            connected: &mut connected,
            output: &output,
            terminal: &terminal,
            credits: &credits,
            wake_tx,
            wake,
            cancel_tx,
            cancel,
            close_tx,
            close,
            shutdown,
            flush: &mut flush,
        },
    )
    .await;
    // Only an actual queued terminal packet authorizes bounded writer completion.
    // Established errors never drain the ordinary outbound data queue.
    if let Err(error) = &result
        && connected
        && version == Some(ProtocolVersion::V5)
        && !failed.load(Ordering::Acquire)
        && !flush
    {
        let mut frame = Frame::v5(Packet::Disconnect);
        frame.reason_code = match io_reason(error) {
            ReasonCode::SERVER_UNAVAILABLE => ReasonCode::SERVER_SHUTTING_DOWN,
            ReasonCode::UNSUPPORTED_PROTOCOL_VERSION => ReasonCode::PROTOCOL_ERROR,
            reason => reason,
        };
        terminal.send_replace(Some(frame));
        flush = true;
    }
    drop(output);
    finish_writer(writer, result, flush, config.write_timeout).await
}
pub(super) async fn finish_writer(
    mut writer: tokio::task::JoinHandle<io::Result<()>>,
    result: io::Result<()>,
    flush: bool,
    write_timeout: Duration,
) -> io::Result<()> {
    if flush {
        match timeout(write_timeout.saturating_mul(2), &mut writer).await {
            Ok(Ok(written)) => {
                written?;
            }
            Ok(Err(error)) => return Err(io::Error::other(error)),
            Err(_) => {
                writer.abort();
                let _ = writer.await;
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "terminal frame write timed out",
                ));
            }
        }
    } else {
        writer.abort();
        let _ = writer.await;
    }
    result
}
#[cfg(test)]
pub(super) fn test_writer(
    writer: OwnedWriteHalf,
    output: mpsc::Receiver<Output>,
    config: BrokerConfig,
    cancel: watch::Sender<bool>,
) -> (
    tokio::task::JoinHandle<io::Result<()>>,
    watch::Sender<Option<Frame>>,
) {
    let (terminal, receiver) = watch::channel(None);
    let credits = Arc::new(Mutex::new(Credits::new(config.max_inflight)));
    (
        spawn_writer(
            writer,
            output,
            receiver,
            credits,
            config,
            cancel,
            Arc::new(AtomicBool::new(false)),
        ),
        terminal,
    )
}
#[cfg(test)]
pub(super) fn test_output(packet: Packet) -> Output {
    Output {
        frame: Frame::v311(packet),
        completion: None,
        written: None,
    }
}

fn discard_acknowledged(pending: &mut Option<Frame>, acknowledged: Option<u16>) {
    if let Some(id)=acknowledged && pending.as_ref().is_some_and(|frame|matches!(frame.packet,Packet::Publish {packet_id:Some(actual),..}|Packet::PubRel {packet_id:actual} if actual==id)) {*pending=None;}
}
async fn drive(reader: &mut OwnedReadHalf, mut context: Context<'_>) -> io::Result<()> {
    let mut buffer = Vec::new();
    let mut scratch = [0u8; 8192];
    let mut expires = Some(Instant::now() + context.config.handshake_timeout);
    let mut keepalive = None;
    let mut pending: Option<Frame> = None;
    let mut needs_poll = false;
    loop {
        if *context.shutdown.borrow() {
            return Ok(());
        }
        if *context.cancel.borrow() {
            if let Some(reason) = *context.close.borrow() {
                return Err(failure(reason, "connection closed by broker"));
            }
            return Err(failure(
                ReasonCode::SERVER_UNAVAILABLE,
                "connection writer unavailable",
            ));
        }
        if expires.is_some_and(|at| Instant::now() >= at) {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "MQTT receive deadline exceeded",
            ));
        }
        let decoded = match decode_frame(&buffer, context.config.max_packet_size, *context.version)
        {
            Ok(value) => value,
            Err(error) => {
                if !*context.connected {
                    if let Some(version) = peek_connect_version(&buffer) {
                        *context.version = Some(version);
                        if version == ProtocolVersion::V5 {
                            terminal_frame(
                                &context,
                                connack(
                                    version,
                                    false,
                                    protocol_reason(&error).value(),
                                    context.config,
                                ),
                            );
                            *context.flush = true;
                        }
                    } else if matches!(error, ProtocolError::UnsupportedVersion)
                        && buffer.first() == Some(&0x10)
                    {
                        terminal_frame(
                            &context,
                            connack(ProtocolVersion::V311, false, 1, context.config),
                        );
                        *context.flush = true;
                    }
                }
                return Err(failure(protocol_reason(&error), error.to_string()));
            }
        };
        if let Some((frame, consumed)) = decoded {
            buffer.drain(..consumed);
            let received = Instant::now();
            if !*context.connected {
                let Packet::Connect {
                    client_id,
                    keep_alive,
                    ..
                } = &frame.packet
                else {
                    return Err(failure(
                        ReasonCode::PROTOCOL_ERROR,
                        "CONNECT required first",
                    ));
                };
                let client = client_id.clone();
                let keep_alive = *keep_alive;
                let version = frame.version;
                let peer_limit = frame.properties.iter().find_map(|property| {
                    if let Property::MaximumPacketSize(value) = property {
                        Some(*value)
                    } else {
                        None
                    }
                });
                *context.version = Some(version);
                let response = context
                    .worker
                    .request(Operation::ConnectFrame {
                        frame,
                        generation: context.generation,
                        wake: context.wake_tx.clone(),
                        cancel: context.cancel_tx.clone(),
                        close_reason: context.close_tx.clone(),
                    })
                    .await?;
                let present = match response {
                    Reply::Connected(present) => present,
                    Reply::Rejected(reason) => {
                        terminal_frame(
                            &context,
                            connack(version, false, reason.code(), context.config),
                        );
                        *context.flush = true;
                        return Err(failure(ReasonCode::NOT_AUTHORIZED, "CONNECT refused"));
                    }
                    Reply::FrameRejected(reason) => {
                        if peer_limit.is_none_or(|limit| limit >= 5) {
                            terminal_frame(
                                &context,
                                connack(version, false, reason.value(), context.config),
                            );
                            *context.flush = true;
                        }
                        return Err(failure(reason, "CONNECT refused"));
                    }
                    _ => return Err(io::Error::other("invalid CONNECT response")),
                };
                *context.identity = Some(client);
                *context.connected = true;
                let (written_tx, written_rx) = oneshot::channel();
                let mut message = outgoing(
                    connack(version, present, 0, context.config),
                    context.credits,
                )?;
                message.written = Some(written_tx);
                context
                    .output
                    .send(message)
                    .await
                    .map_err(io::Error::other)?;
                // Accepted CONNACK must precede terminal output from pipelined input.
                written_rx
                    .await
                    .map_err(|_| failure(ReasonCode::SERVER_UNAVAILABLE, "CONNACK write failed"))?;
                keepalive = if keep_alive == 0 {
                    None
                } else {
                    Some(Duration::from_millis(u64::from(keep_alive) * 1500))
                };
            } else {
                let client = context
                    .identity
                    .as_ref()
                    .ok_or_else(|| io::Error::other("missing connection identity"))?
                    .clone();
                if matches!(frame.packet, Packet::Disconnect) {
                    let expiry = frame.properties.iter().find_map(|property| {
                        if let Property::SessionExpiryInterval(value) = property {
                            Some(*value)
                        } else {
                            None
                        }
                    });
                    let graceful = frame.reason_code != ReasonCode::DISCONNECT_WITH_WILL_MESSAGE;
                    let Reply::Done = context
                        .worker
                        .request(Operation::DisconnectOptions {
                            client,
                            generation: context.generation,
                            graceful,
                            expiry,
                        })
                        .await?
                    else {
                        return Err(io::Error::other("invalid disconnect response"));
                    };
                    *context.identity = None;
                    return Ok(());
                }
                if *context.version == Some(ProtocolVersion::V5)
                    && let Packet::Publish {
                        qos,
                        packet_id: Some(id),
                        ..
                    } = frame.packet
                    && qos > 0
                {
                    locked(context.credits)?.register(id, qos)?;
                }
                let acknowledged = match frame.packet {
                    Packet::PubAck { packet_id }
                    | Packet::PubRec { packet_id }
                    | Packet::PubComp { packet_id } => Some(packet_id),
                    _ => None,
                };
                let Reply::Frames(replies) = context
                    .worker
                    .request(Operation::HandleFrame {
                        client,
                        generation: context.generation,
                        frame,
                    })
                    .await?
                else {
                    return Err(io::Error::other("invalid packet response"));
                };
                discard_acknowledged(&mut pending, acknowledged);
                for frame in replies {
                    context
                        .output
                        .send(outgoing(frame, context.credits)?)
                        .await
                        .map_err(io::Error::other)?;
                }
            }
            expires = keepalive.map(|duration| received + duration);
            needs_poll = true;
            tokio::task::yield_now().await;
            continue;
        }
        if *context.connected && pending.is_none() && needs_poll {
            let client = context
                .identity
                .as_ref()
                .ok_or_else(|| io::Error::other("missing connection identity"))?
                .clone();
            let Reply::FramePolled(frame) = context
                .worker
                .request(Operation::PollFrame {
                    client,
                    generation: context.generation,
                })
                .await?
            else {
                return Err(io::Error::other("invalid poll response"));
            };
            pending = frame.map(|frame| *frame);
            needs_poll = false;
        }
        let remaining = context.config.max_packet_size.saturating_sub(buffer.len());
        if remaining == 0 {
            return Err(failure(
                ReasonCode::PACKET_TOO_LARGE,
                "receive buffer limit reached",
            ));
        }
        let read_len = remaining.min(scratch.len());
        tokio::select! {
            changed=context.shutdown.changed()=>{if changed.is_err()||*context.shutdown.borrow(){return Ok(());}}
            _=context.cancel.changed()=>{continue;}
            _=crate::packet_io::deadline(expires)=>return Err(io::Error::new(io::ErrorKind::TimedOut,"MQTT receive deadline exceeded")),
            _=context.wake.changed(),if *context.connected=>{needs_poll=true;}
            permit=context.output.reserve(),if pending.is_some()=>{
                let frame=pending.take().ok_or_else(||io::Error::other("missing pending output"))?;
                permit.map_err(io::Error::other)?.send(outgoing(frame,context.credits)?);needs_poll=true;
            }
            read=reader.read(&mut scratch[..read_len])=>{let size=read?;if size==0{return Ok(());}buffer.extend_from_slice(&scratch[..size]);}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn credits_hold_qos2_until_pubcomp_and_never_release_reused_id_from_stale_ack() {
        let mut ledger = Credits::new(1);
        ledger.register(1, 2).unwrap();
        assert!(
            ledger
                .completion(&Frame::v5(Packet::PubRec { packet_id: 1 }))
                .is_none()
        );
        assert!(ledger.register(2, 1).is_err());
        assert!(ledger.register(1, 1).is_err());
        assert!(ledger.register(1, 2).is_ok());
        let old = ledger
            .completion(&Frame::v5(Packet::PubComp { packet_id: 1 }))
            .unwrap();
        ledger.complete(old);
        ledger.register(1, 1).unwrap();
        ledger.complete(old);
        assert_eq!(ledger.entries.len(), 1);
        let current = ledger
            .completion(&Frame::v5(Packet::PubAck { packet_id: 1 }))
            .unwrap();
        ledger.complete(current);
        assert!(ledger.entries.is_empty());
    }
    async fn sockets() -> (tokio::net::TcpStream, OwnedWriteHalf) {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_send_buffer_size(4096).unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(8).unwrap();
        let peer = tokio::net::TcpSocket::new_v4().unwrap();
        peer.set_recv_buffer_size(4096).unwrap();
        let client = peer.connect(listener.local_addr().unwrap()).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (client, server.into_split().1)
    }
    fn large_frame() -> Frame {
        Frame::v5(Packet::Publish {
            topic: "writer/large".into(),
            payload: vec![42; 1024 * 1024],
            qos: 0,
            packet_id: None,
            dup: false,
            retain: false,
        })
    }
    #[tokio::test]
    async fn terminal_finishes_current_frame_then_discards_queue_and_unwritten_ack_credit() {
        let (mut client, writer) = sockets().await;
        let config = BrokerConfig {
            max_packet_size: 2 * 1024 * 1024,
            write_timeout: Duration::from_secs(3),
            ..BrokerConfig::default()
        };
        let credits = Arc::new(Mutex::new(Credits::new(1)));
        locked(&credits).unwrap().register(7, 1).unwrap();
        let (output, receiver) = mpsc::channel(2);
        let (terminal, terminal_rx) = watch::channel(None);
        let (cancel, _) = watch::channel(false);
        let publication = large_frame();
        let expected = encode_frame(&publication).unwrap();
        output
            .send(Output {
                frame: publication,
                completion: None,
                written: None,
            })
            .await
            .unwrap();
        output
            .send(outgoing(Frame::v5(Packet::PubAck { packet_id: 7 }), &credits).unwrap())
            .await
            .unwrap();
        assert_eq!(
            locked(&credits).unwrap().entries.len(),
            1,
            "enqueue is not ACK transmission"
        );
        let task = spawn_writer(
            writer,
            receiver,
            terminal_rx,
            credits.clone(),
            config,
            cancel,
            Arc::new(AtomicBool::new(false)),
        );
        let mut first = [0u8; 1];
        client.read_exact(&mut first).await.unwrap();
        assert_eq!(first[0], expected[0]);
        let mut final_frame = Frame::v5(Packet::Disconnect);
        final_frame.reason_code = ReasonCode::PROTOCOL_ERROR;
        let final_bytes = encode_frame(&final_frame).unwrap();
        terminal.send_replace(Some(final_frame));
        let mut rest = vec![0u8; expected.len() - 1];
        client.read_exact(&mut rest).await.unwrap();
        assert_eq!(rest, &expected[1..]);
        let mut actual = vec![0u8; final_bytes.len()];
        client.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, final_bytes);
        task.await.unwrap().unwrap();
        assert_eq!(client.read(&mut first).await.unwrap(), 0);
        assert_eq!(
            locked(&credits).unwrap().entries.len(),
            1,
            "discarded ACK cannot release receive credit"
        );
    }
    #[tokio::test]
    async fn actual_ack_write_releases_ticket() {
        let (mut client, writer) = sockets().await;
        let config = BrokerConfig::default();
        let credits = Arc::new(Mutex::new(Credits::new(1)));
        locked(&credits).unwrap().register(1, 2).unwrap();
        let (output, receiver) = mpsc::channel(1);
        let (_terminal, terminal_rx) = watch::channel(None);
        let (cancel, _) = watch::channel(false);
        let frame = Frame::v5(Packet::PubComp { packet_id: 1 });
        let expected = encode_frame(&frame).unwrap();
        output
            .send(outgoing(frame, &credits).unwrap())
            .await
            .unwrap();
        drop(output);
        assert_eq!(locked(&credits).unwrap().entries.len(), 1);
        let task = spawn_writer(
            writer,
            receiver,
            terminal_rx,
            credits.clone(),
            config,
            cancel,
            Arc::new(AtomicBool::new(false)),
        );
        let mut actual = vec![0u8; expected.len()];
        client.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, expected);
        task.await.unwrap().unwrap();
        assert!(locked(&credits).unwrap().entries.is_empty());
    }
    #[tokio::test]
    async fn terminal_timeout_bounds_blocked_current_frame_and_closes_instead_of_appending() {
        let (mut client, writer) = sockets().await;
        let config = BrokerConfig {
            max_packet_size: 2 * 1024 * 1024,
            write_timeout: Duration::from_millis(100),
            ..BrokerConfig::default()
        };
        let (output, receiver) = mpsc::channel(1);
        let (cancel, _) = watch::channel(false);
        output
            .send(Output {
                frame: large_frame(),
                completion: None,
                written: None,
            })
            .await
            .unwrap();
        let (task, terminal) = test_writer(writer, receiver, config, cancel);
        client.read_exact(&mut [0u8; 1]).await.unwrap();
        terminal.send_replace(Some(Frame::v5(Packet::Disconnect)));
        assert!(
            timeout(
                Duration::from_secs(1),
                finish_writer(task, Ok(()), true, Duration::from_millis(100))
            )
            .await
            .unwrap()
            .is_err()
        );
    }
    async fn received(client: &mut tokio::net::TcpStream) -> Frame {
        let mut bytes = Vec::new();
        loop {
            if let Some((frame, used)) =
                decode_frame(&bytes, 8192, Some(ProtocolVersion::V5)).unwrap()
            {
                assert_eq!(used, bytes.len());
                return frame;
            }
            let mut byte = [0u8; 1];
            client.read_exact(&mut byte).await.unwrap();
            bytes.push(byte[0]);
        }
    }
    fn connect_frame() -> Frame {
        Frame::v5(Packet::Connect {
            client_id: "mqtt5-network".into(),
            clean_session: true,
            keep_alive: 30,
            will: None,
            username: None,
            password: None,
        })
    }
    async fn server(
        config: BrokerConfig,
    ) -> (
        tokio::net::TcpStream,
        watch::Sender<bool>,
        tokio::task::JoinHandle<io::Result<()>>,
    ) {
        let (address, stop, task) = super::super::live_tests::broker(config).await;
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client
            .write_all(&encode_frame(&connect_frame()).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            received(&mut client).await.packet,
            Packet::ConnAck { code: 0, .. }
        ));
        (client, stop, task)
    }
    #[tokio::test]
    async fn accepted_connack_precedes_priority_disconnect_for_invalid_pipeline() {
        let (address, stop, task) = super::super::live_tests::broker(BrokerConfig::default()).await;
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut bytes = encode_frame(&connect_frame()).unwrap();
        bytes.extend([0xc1, 0]);
        client.write_all(&bytes).await.unwrap();
        assert!(matches!(
            received(&mut client).await.packet,
            Packet::ConnAck { code: 0, .. }
        ));
        let frame = received(&mut client).await;
        assert_eq!(frame.packet, Packet::Disconnect);
        assert_eq!(frame.reason_code, ReasonCode::MALFORMED_PACKET);
        super::super::live_tests::stop(stop, task).await;
    }
    fn incoming(id: u16, qos: u8, dup: bool) -> Frame {
        Frame::v5(Packet::Publish {
            topic: "credits/source".into(),
            payload: b"pending".to_vec(),
            qos,
            packet_id: Some(id),
            dup,
            retain: false,
        })
    }
    #[tokio::test]
    async fn receive_maximum_counts_qos2_duplicates_once_and_rejects_mixed_qos_identifier() {
        let (mut client, stop, task) = server(BrokerConfig {
            max_inflight: 1,
            ..BrokerConfig::default()
        })
        .await;
        client
            .write_all(&encode_frame(&incoming(1, 2, false)).unwrap())
            .await
            .unwrap();
        assert_eq!(
            received(&mut client).await.packet,
            Packet::PubRec { packet_id: 1 }
        );
        client
            .write_all(&encode_frame(&incoming(1, 2, true)).unwrap())
            .await
            .unwrap();
        assert_eq!(
            received(&mut client).await.packet,
            Packet::PubRec { packet_id: 1 }
        );
        client
            .write_all(&encode_frame(&Frame::v5(Packet::PingReq)).unwrap())
            .await
            .unwrap();
        assert_eq!(received(&mut client).await.packet, Packet::PingResp);
        client
            .write_all(&encode_frame(&incoming(1, 1, false)).unwrap())
            .await
            .unwrap();
        let frame = received(&mut client).await;
        assert_eq!(frame.packet, Packet::Disconnect);
        assert_eq!(frame.reason_code, ReasonCode::PROTOCOL_ERROR);
        super::super::live_tests::stop(stop, task).await;
    }
    #[tokio::test]
    async fn excess_inbound_qos2_sends_receive_maximum_reason() {
        let (mut client, stop, task) = server(BrokerConfig {
            max_inflight: 1,
            ..BrokerConfig::default()
        })
        .await;
        client
            .write_all(&encode_frame(&incoming(1, 2, false)).unwrap())
            .await
            .unwrap();
        assert_eq!(
            received(&mut client).await.packet,
            Packet::PubRec { packet_id: 1 }
        );
        client
            .write_all(&encode_frame(&incoming(2, 2, false)).unwrap())
            .await
            .unwrap();
        let frame = received(&mut client).await;
        assert_eq!(frame.packet, Packet::Disconnect);
        assert_eq!(frame.reason_code, ReasonCode::RECEIVE_MAXIMUM_EXCEEDED);
        super::super::live_tests::stop(stop, task).await;
    }
}
