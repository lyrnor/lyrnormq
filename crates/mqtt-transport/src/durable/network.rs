//! Versioned socket IO; all business mutations remain on the shared worker.
use super::{Operation, Reply, Worker, WorkerFailure, failure};
use crate::BrokerConfig;
use mqtt_protocol::{
    Frame, Packet, Property, ProtocolError, ProtocolVersion, ReasonCode, decode_frame,
    encode_frame, peek_connect_version,
};
use mqtt_session::durable::{Clock, Delivery, SystemClock};
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
struct Lease {
    client: String,
    generation: u64,
    worker: Worker,
    expires_at: Option<u64>,
    ticket: Option<u64>,
}
pub(super) struct Output {
    frame: Frame,
    lease: Option<Lease>,
    completion: Option<Ticket>,
    written: Option<oneshot::Sender<()>>,
}
fn outgoing(frame: Frame, credits: &SharedCredits) -> io::Result<Output> {
    let completion = locked(credits)?.completion(&frame);
    Ok(Output {
        frame,
        lease: None,
        completion,
        written: None,
    })
}
fn delivery_output(delivery: Delivery, client: String, generation: u64, worker: Worker) -> Output {
    Output {
        frame: delivery.frame,
        lease: Some(Lease {
            client,
            generation,
            worker,
            expires_at: delivery.expires_at,
            ticket: delivery.ticket,
        }),
        completion: None,
        written: None,
    }
}
async fn begin_output(mut message: Output) -> io::Result<Option<Output>> {
    let Some(lease) = message.lease.take() else {
        return Ok(Some(message));
    };
    let delivery = Delivery {
        frame: message.frame,
        expires_at: lease.expires_at,
        ticket: lease.ticket,
    };
    let Reply::FramePolled(frame) = lease
        .worker
        .request(Operation::BeginDelivery {
            client: lease.client,
            generation: lease.generation,
            delivery: delivery.clone(),
        })
        .await?
    else {
        return Err(io::Error::other("invalid delivery-start response"));
    };
    let Some(mut frame) = frame.map(|frame| *frame) else {
        return Ok(None);
    };
    delivery.refresh_remaining(&mut frame, SystemClock.now_millis());
    Ok(Some(Output {
        frame,
        lease: None,
        completion: message.completion,
        written: message.written,
    }))
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
                        let prepared=begin_output(message).await;
                        // A terminal signal arriving during the blocking worker roundtrip
                        // still wins before any new frame bytes are started.
                        let final_frame=terminal.borrow_and_update().clone();
                        if let Some(frame)=final_frame {write_frame(&mut writer,&frame,&config).await?;return Ok(());}
                        let Some(message)=prepared? else{continue;};
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
        lease: None,
        completion: None,
        written: None,
    }
}

fn discard_acknowledged(pending: &mut Option<Delivery>, acknowledged: Option<u16>) {
    let Some(id) = acknowledged else {
        return;
    };
    let obsolete = pending
        .as_ref()
        .is_some_and(|delivery| match delivery.frame.packet {
            Packet::Publish {
                packet_id: Some(actual),
                ..
            }
            | Packet::PubRel { packet_id: actual } => actual == id,
            _ => false,
        });
    if obsolete {
        *pending = None;
    }
}

async fn drive(reader: &mut OwnedReadHalf, mut context: Context<'_>) -> io::Result<()> {
    let mut buffer = Vec::new();
    let mut scratch = [0u8; 8192];
    let mut received_at_millis = 0;
    let mut expires = Some(Instant::now() + context.config.handshake_timeout);
    let mut keepalive = None;
    let mut pending: Option<Delivery> = None;
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
                    let graceful = frame.reason_code == ReasonCode::SUCCESS;
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
                    .request(Operation::HandleFrameAt {
                        client,
                        generation: context.generation,
                        frame,
                        received_at_millis,
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
            let Reply::DeliveryPolled(delivery) = context
                .worker
                .request(Operation::PollDelivery {
                    client,
                    generation: context.generation,
                })
                .await?
            else {
                return Err(io::Error::other("invalid poll response"));
            };
            pending = delivery.map(|delivery| *delivery);
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
                let delivery=pending.take().ok_or_else(||io::Error::other("missing pending output"))?;
                let client=context.identity.as_ref().ok_or_else(||io::Error::other("missing connection identity"))?.clone();
                permit.map_err(io::Error::other)?.send(delivery_output(delivery,client,context.generation,context.worker.clone()));needs_poll=true;
            }
            read=reader.read(&mut scratch[..read_len])=>{let size=read?;if size==0{return Ok(());}buffer.extend_from_slice(&scratch[..size]);received_at_millis=SystemClock.now_millis();}
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
                lease: None,
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
                lease: None,
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
    #[derive(Debug)]
    struct ManualClock(std::sync::atomic::AtomicU64);
    impl Clock for ManualClock {
        fn now_millis(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }
    #[tokio::test]
    async fn blocked_writer_expires_only_unstarted_delivery_and_keeps_started_replay() {
        use mqtt_auth::Principal;
        use mqtt_session::durable::{ConnectionOptions, SessionEngine};
        use mqtt_storage::durable::SqliteStore;
        let db = super::super::tests::Database::new();
        let config = BrokerConfig {
            storage_path: Some(db.0.clone()),
            max_packet_size: 2 * 1024 * 1024,
            outbound_capacity: 4,
            write_timeout: Duration::from_secs(3),
            ..BrokerConfig::default()
        };
        let initial_time = SystemClock.now_millis();
        let clock = Arc::new(ManualClock(std::sync::atomic::AtomicU64::new(initial_time)));
        let mut engine = SessionEngine::new_with_clock(
            super::super::session_config(&config),
            config.access_control.clone(),
            clock.clone(),
        )
        .unwrap();
        for client in ["target", "source"] {
            engine
                .connect_with_options(
                    client,
                    None,
                    Principal::anonymous(),
                    ConnectionOptions {
                        version: ProtocolVersion::V5,
                        clean_start: true,
                        session_expiry_interval: 60,
                        receive_maximum: 2,
                        will_properties: vec![],
                    },
                )
                .unwrap();
        }
        engine
            .handle_frame(
                "target",
                Frame::v5(Packet::Subscribe {
                    packet_id: 1,
                    filters: vec![("writer/expiry".into(), 1)],
                }),
            )
            .unwrap();
        for (id, payload) in [
            (40, vec![42; 1024 * 1024]),
            (41, b"must expire before first write".to_vec()),
        ] {
            let mut frame = Frame::v5(Packet::Publish {
                topic: "writer/expiry".into(),
                payload,
                qos: 1,
                packet_id: Some(id),
                dup: false,
                retain: false,
            });
            frame.properties = vec![Property::MessageExpiryInterval(1)];
            engine.handle_frame("source", frame).unwrap();
        }
        let mut store = SqliteStore::open(&db.0, config.max_state_bytes).unwrap();
        store.commit(&engine.snapshot().unwrap()).unwrap();
        let (sender, commands) = mpsc::channel(8);
        let worker = Worker { sender };
        let (wake, _) = watch::channel(0);
        let (cancel, _) = watch::channel(false);
        let online = std::collections::HashMap::from([(
            "target".into(),
            super::super::Online {
                generation: 1,
                wake,
                cancel,
                close_reason: None,
            },
        )]);
        let worker_config = config.clone();
        let worker_task = tokio::task::spawn_blocking(move || {
            super::super::run_commands(commands, worker_config, Some(store), engine, online)
        });
        let mut deliveries = Vec::new();
        for _ in 0..2 {
            let Reply::DeliveryPolled(Some(delivery)) = worker
                .request(Operation::PollDelivery {
                    client: "target".into(),
                    generation: 1,
                })
                .await
                .unwrap()
            else {
                panic!("delivery expected")
            };
            deliveries.push(*delivery);
        }
        let expected = encode_frame(&deliveries[0].frame).unwrap();
        let (mut client, writer) = sockets().await;
        let (output, receiver) = mpsc::channel(3);
        let (terminal, terminal_rx) = watch::channel(None);
        let (cancel, _) = watch::channel(false);
        let credits = Arc::new(Mutex::new(Credits::new(2)));
        for delivery in deliveries {
            output
                .send(delivery_output(
                    delivery,
                    "target".into(),
                    1,
                    worker.clone(),
                ))
                .await
                .unwrap();
        }
        output
            .send(outgoing(Frame::v5(Packet::PingResp), &credits).unwrap())
            .await
            .unwrap();
        let writer_task = spawn_writer(
            writer,
            receiver,
            terminal_rx,
            credits,
            config.clone(),
            cancel,
            Arc::new(AtomicBool::new(false)),
        );
        let mut first = [0u8; 1];
        client.read_exact(&mut first).await.unwrap();
        assert_eq!(first[0], expected[0]);
        // The first frame has started and is stalled by the small TCP window.
        // The second has only been reserved; no wall-clock sleeps are necessary.
        clock.0.store(initial_time + 1500, Ordering::SeqCst);
        let mut remaining = vec![0; expected.len() - 1];
        timeout(Duration::from_secs(3), client.read_exact(&mut remaining))
            .await
            .unwrap()
            .unwrap();
        let mut complete = first.to_vec();
        complete.extend(remaining);
        let sent = decode_frame(&complete, config.max_packet_size, Some(ProtocolVersion::V5))
            .unwrap()
            .unwrap()
            .0;
        assert!(matches!(sent.packet,Packet::Publish{payload,..} if payload==vec![42;1024*1024]));
        assert!(
            sent.properties
                .iter()
                .any(|p| matches!(p, Property::MessageExpiryInterval(0 | 1)))
        );
        assert_eq!(received(&mut client).await.packet, Packet::PingResp);
        drop(output);
        writer_task.await.unwrap().unwrap();
        drop(terminal);
        super::super::tests::stop(worker, worker_task).await;
        let store = SqliteStore::open(&db.0, config.max_state_bytes).unwrap();
        let bytes = store.load().unwrap().unwrap();
        let mut restored = SessionEngine::restore_with_clock(
            super::super::session_config(&config),
            &bytes,
            config.access_control.clone(),
            clock,
        )
        .unwrap();
        restored
            .connect_with_options(
                "target",
                None,
                Principal::anonymous(),
                ConnectionOptions {
                    version: ProtocolVersion::V5,
                    clean_start: false,
                    session_expiry_interval: 60,
                    receive_maximum: 2,
                    will_properties: vec![],
                },
            )
            .unwrap();
        let replay = restored
            .poll_frame_with_changes("target")
            .unwrap()
            .frame
            .unwrap();
        assert!(matches!(replay.packet, Packet::Publish { dup: true, .. }));
        assert!(
            replay
                .properties
                .contains(&Property::MessageExpiryInterval(0))
        );
        assert!(
            restored
                .poll_frame_with_changes("target")
                .unwrap()
                .frame
                .is_none()
        );
    }
    #[tokio::test]
    async fn nonzero_disconnect_reason_fires_will_while_normal_zero_cancels() {
        let (address, stop, task) = super::super::live_tests::broker(BrokerConfig::default()).await;
        let mut observer = TcpStream::connect(address).await.unwrap();
        let mut observer_connect = connect_frame();
        if let Packet::Connect { client_id, .. } = &mut observer_connect.packet {
            *client_id = "will-observer".into();
        }
        observer
            .write_all(&encode_frame(&observer_connect).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            received(&mut observer).await.packet,
            Packet::ConnAck { code: 0, .. }
        ));
        observer
            .write_all(
                &encode_frame(&Frame::v5(Packet::Subscribe {
                    packet_id: 1,
                    filters: vec![("will/reason".into(), 0)],
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            received(&mut observer).await.packet,
            Packet::SubAck { .. }
        ));
        for reason in [ReasonCode::SUCCESS, ReasonCode::UNSPECIFIED_ERROR] {
            let mut source = TcpStream::connect(address).await.unwrap();
            let mut connect = connect_frame();
            if let Packet::Connect {
                client_id, will, ..
            } = &mut connect.packet
            {
                *client_id = format!("will-source-{}", reason.value());
                *will = Some(mqtt_protocol::Will {
                    topic: "will/reason".into(),
                    payload: vec![reason.value()],
                    qos: 0,
                    retain: false,
                });
            }
            source
                .write_all(&encode_frame(&connect).unwrap())
                .await
                .unwrap();
            assert!(matches!(
                received(&mut source).await.packet,
                Packet::ConnAck { code: 0, .. }
            ));
            let mut disconnect = Frame::v5(Packet::Disconnect);
            disconnect.reason_code = reason;
            source
                .write_all(&encode_frame(&disconnect).unwrap())
                .await
                .unwrap();
            assert_eq!(source.read(&mut [0; 1]).await.unwrap(), 0);
            if reason == ReasonCode::SUCCESS {
                observer
                    .write_all(&encode_frame(&Frame::v5(Packet::PingReq)).unwrap())
                    .await
                    .unwrap();
                assert_eq!(received(&mut observer).await.packet, Packet::PingResp);
            } else {
                let message = timeout(Duration::from_secs(1), received(&mut observer))
                    .await
                    .expect("nonzero disconnect must publish Will");
                assert!(
                    matches!(message.packet,Packet::Publish{payload,..} if payload==vec![reason.value()])
                );
            }
        }
        super::super::live_tests::stop(stop, task).await;
    }
    #[tokio::test]
    async fn ingress_timestamp_survives_worker_residence_before_acknowledgement() {
        use mqtt_session::durable::{ConnectionOptions, SessionEngine};
        let config = BrokerConfig::default();
        let clock = Arc::new(ManualClock(std::sync::atomic::AtomicU64::new(2000)));
        let mut engine = SessionEngine::new_with_clock(
            super::super::session_config(&config),
            config.access_control.clone(),
            clock,
        )
        .unwrap();
        let mut online = std::collections::HashMap::new();
        for (client, generation) in [("target", 1), ("source", 2)] {
            engine
                .connect_with_options(
                    client,
                    None,
                    mqtt_auth::Principal::anonymous(),
                    ConnectionOptions {
                        version: ProtocolVersion::V5,
                        clean_start: true,
                        session_expiry_interval: 60,
                        receive_maximum: 1,
                        will_properties: vec![],
                    },
                )
                .unwrap();
            let (wake, _) = watch::channel(0);
            let (cancel, _) = watch::channel(false);
            online.insert(
                client.into(),
                super::super::Online {
                    generation,
                    wake,
                    cancel,
                    close_reason: None,
                },
            );
        }
        engine
            .handle_frame(
                "target",
                Frame::v5(Packet::Subscribe {
                    packet_id: 1,
                    filters: vec![("ingress".into(), 1)],
                }),
            )
            .unwrap();
        let (sender, commands) = mpsc::channel(4);
        let worker = Worker { sender };
        let task = tokio::task::spawn_blocking(move || {
            super::super::run_commands(commands, config, None, engine, online)
        });
        let mut frame = Frame::v5(Packet::Publish {
            topic: "ingress".into(),
            payload: b"already expired in mailbox".to_vec(),
            qos: 1,
            packet_id: Some(9),
            dup: false,
            retain: false,
        });
        frame.properties = vec![Property::MessageExpiryInterval(1)];
        let Reply::Frames(replies) = worker
            .request(Operation::HandleFrameAt {
                client: "source".into(),
                generation: 2,
                frame,
                received_at_millis: 1000,
            })
            .await
            .unwrap()
        else {
            panic!("frames expected")
        };
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].packet, Packet::PubAck { packet_id: 9 });
        assert!(matches!(
            worker
                .request(Operation::PollDelivery {
                    client: "target".into(),
                    generation: 1
                })
                .await
                .unwrap(),
            Reply::DeliveryPolled(None)
        ));
        super::super::tests::stop(worker, task).await;
    }
}
