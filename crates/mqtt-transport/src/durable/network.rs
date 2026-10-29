//! Versioned socket IO; business mutations run on the client's session actor.
use super::aliases::{InboundAliases, OutboundAliases};
use super::{Operation, Reply, Worker, WorkerFailure, failure};
mod authentication;
use crate::BrokerConfig;
use mqtt_protocol::{
    Frame, Packet, Property, ProtocolError, ProtocolVersion, ReasonCode, decode_frame,
    encode_frame, peek_connect_maximum_packet_size, peek_connect_version,
};
use mqtt_session::durable::{Clock, Delivery, SystemClock};
use std::{
    collections::BTreeMap,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
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
    shared_subscription: Option<(String, u64)>,
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
            shared_subscription: delivery.shared_subscription,
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
        shared_subscription: lease.shared_subscription,
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
    peer_maximum: usize,
) -> io::Result<()> {
    let bytes = encode_frame(frame).map_err(io::Error::other)?;
    if bytes.len() > config.max_packet_size.min(peer_maximum) {
        return Err(failure(
            ReasonCode::PACKET_TOO_LARGE,
            "outgoing packet exceeds limit",
        ));
    }
    timeout(config.write_timeout, writer.write_all(&bytes))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "socket write timed out"))?
}
struct WriterLimits {
    config: BrokerConfig,
    peer_maximum: Arc<AtomicUsize>,
    peer_alias_maximum: Arc<AtomicUsize>,
}
fn spawn_writer(
    mut writer: OwnedWriteHalf,
    mut output: mpsc::Receiver<Output>,
    mut terminal: watch::Receiver<Option<Frame>>,
    credits: SharedCredits,
    limits: WriterLimits,
    cancel: watch::Sender<bool>,
    failed: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<io::Result<()>> {
    let WriterLimits {
        config,
        peer_maximum,
        peer_alias_maximum,
    } = limits;
    let mut aliases = OutboundAliases::new(config.topic_alias_maximum);
    tokio::spawn(async move {
        let result = async {
            loop {
                let final_frame = terminal.borrow_and_update().clone();
                if let Some(frame) = final_frame {
                    write_frame(&mut writer, &frame, &config, peer_maximum.load(Ordering::Acquire)).await?;
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
                        if let Some(frame)=final_frame {write_frame(&mut writer,&frame,&config,peer_maximum.load(Ordering::Acquire)).await?;return Ok(());}
                        let Some(message)=prepared? else{continue;};
                        // No select/cancellation inside a frame: terminal output may only
                        // follow a completed frame, never bytes from a partial PUBLISH.
                        let prepared = aliases.prepare(message.frame, peer_alias_maximum.load(Ordering::Acquire) as u16, config.max_packet_size.min(peer_maximum.load(Ordering::Acquire)))?;
                        write_frame(&mut writer,&prepared.frame,&config,peer_maximum.load(Ordering::Acquire)).await?;
                        aliases.commit(prepared.binding);
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
            Property::TopicAliasMaximum(config.topic_alias_maximum),
            Property::SharedSubscriptionAvailable(1),
            Property::SubscriptionIdentifierAvailable(1),
        ];
    }
    frame
}
fn profile_refusal(config: &BrokerConfig) -> Frame {
    let (version, code) = config.protocol_mode.refusal();
    connack(version, false, code, config)
}
pub(super) fn accepted_connack(
    version: ProtocolVersion,
    present: bool,
    config: &BrokerConfig,
    assigned: Option<&str>,
) -> Frame {
    let mut frame = connack(version, present, 0, config);
    if version == ProtocolVersion::V5
        && let Some(client) = assigned
    {
        frame
            .properties
            .push(Property::AssignedClientIdentifier(client.into()));
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
    authenticating: &'a mut bool,
    output: &'a mpsc::Sender<Output>,
    terminal: &'a watch::Sender<Option<Frame>>,
    credits: &'a SharedCredits,
    peer_maximum: &'a Arc<AtomicUsize>,
    peer_alias_maximum: &'a Arc<AtomicUsize>,
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
    let peer_maximum = Arc::new(AtomicUsize::new(usize::MAX));
    let peer_alias_maximum = Arc::new(AtomicUsize::new(0));
    let writer = spawn_writer(
        writer,
        receiver,
        terminal_rx,
        credits.clone(),
        WriterLimits {
            config: config.clone(),
            peer_maximum: peer_maximum.clone(),
            peer_alias_maximum: peer_alias_maximum.clone(),
        },
        cancel_tx.clone(),
        failed.clone(),
    );
    let mut version = None;
    let mut connected = false;
    let mut authenticating = false;
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
            authenticating: &mut authenticating,
            output: &output,
            terminal: &terminal,
            credits: &credits,
            peer_maximum: &peer_maximum,
            peer_alias_maximum: &peer_alias_maximum,
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
        && (connected || authenticating)
        && version == Some(ProtocolVersion::V5)
        && !failed.load(Ordering::Acquire)
        && !flush
    {
        if connected {
            let mut frame = Frame::v5(Packet::Disconnect);
            frame.reason_code = match io_reason(error) {
                ReasonCode::SERVER_UNAVAILABLE => ReasonCode::SERVER_SHUTTING_DOWN,
                ReasonCode::UNSUPPORTED_PROTOCOL_VERSION => ReasonCode::PROTOCOL_ERROR,
                reason => reason,
            };
            terminal.send_replace(Some(frame));
            flush = true;
        } else {
            // MQTT-3.14.0-1 forbids DISCONNECT before a successful CONNACK.
            // An incomplete initial AUTH exchange can only refuse with CONNACK
            // or close silently when the reason/cap cannot encode that refusal.
            let refusal = connack(ProtocolVersion::V5, false, io_reason(error).value(), config);
            if encode_frame(&refusal).is_ok_and(|bytes| {
                bytes.len()
                    <= config
                        .max_packet_size
                        .min(peer_maximum.load(Ordering::Acquire))
            }) {
                terminal.send_replace(Some(refusal));
                flush = true;
            }
        }
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
            WriterLimits {
                config,
                peer_maximum: Arc::new(AtomicUsize::new(usize::MAX)),
                peer_alias_maximum: Arc::new(AtomicUsize::new(0)),
            },
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
    let mut handshake_keepalive = 0u16;
    let mut initial_auth = false;
    let mut reauth_deadline: Option<Instant> = None;
    let mut pending: Option<Delivery> = None;
    let mut aliases = InboundAliases::new(context.config.topic_alias_maximum);
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
        if reauth_deadline.is_some_and(|at| Instant::now() >= at) {
            return Err(failure(
                ReasonCode::NOT_AUTHORIZED,
                "reauthentication deadline exceeded",
            ));
        }
        if expires.is_some_and(|at| Instant::now() >= at) {
            if initial_auth {
                return Err(failure(
                    ReasonCode::NOT_AUTHORIZED,
                    "initial authentication deadline exceeded",
                ));
            }
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
                            // Only a known, valid property prefix establishes a
                            // peer cap, even when this profile rejects the version.
                            if let Some(maximum) = peek_connect_maximum_packet_size(&buffer) {
                                context
                                    .peer_maximum
                                    .store(maximum as usize, Ordering::Release);
                            }
                        }
                        if !context.config.protocol_mode.accepts(version) {
                            terminal_frame(&context, profile_refusal(context.config));
                            *context.flush = true;
                        } else if version == ProtocolVersion::V5 {
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
                        terminal_frame(&context, profile_refusal(context.config));
                        *context.flush = true;
                    }
                }
                return Err(failure(protocol_reason(&error), error.to_string()));
            }
        };
        if let Some((mut frame, consumed)) = decoded {
            buffer.drain(..consumed);
            let received = Instant::now();
            if !*context.connected {
                if initial_auth {
                    if matches!(frame.packet, Packet::Disconnect) {
                        return Ok(());
                    }
                    if !matches!(frame.packet, Packet::Auth) {
                        return Err(failure(
                            ReasonCode::PROTOCOL_ERROR,
                            "only AUTH allowed before CONNACK",
                        ));
                    }
                    let deadline = expires
                        .ok_or_else(|| io::Error::other("missing authentication deadline"))?;
                    let response = context
                        .worker
                        .request(Operation::Authenticate {
                            generation: context.generation,
                            frame,
                            deadline: deadline.into_std(),
                        })
                        .await?;
                    let accepted = authentication::handshake_reply(
                        &mut context,
                        response,
                        ProtocolVersion::V5,
                        String::new(),
                    )
                    .await?;
                    if !accepted {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    initial_auth = false;
                    keepalive = (handshake_keepalive != 0)
                        .then(|| Duration::from_millis(u64::from(handshake_keepalive) * 1500));
                } else {
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
                    handshake_keepalive = keep_alive;
                    let version = frame.version;
                    let peer_limit = frame.properties.iter().find_map(|property| {
                        if let Property::MaximumPacketSize(value) = property {
                            Some(*value)
                        } else {
                            None
                        }
                    });
                    context
                        .peer_maximum
                        .store(peer_limit.unwrap_or(u32::MAX) as usize, Ordering::Release);
                    let peer_alias_maximum = frame
                        .properties
                        .iter()
                        .find_map(|property| {
                            if let Property::TopicAliasMaximum(value) = property {
                                Some(*value)
                            } else {
                                None
                            }
                        })
                        .unwrap_or(0);
                    context
                        .peer_alias_maximum
                        .store(usize::from(peer_alias_maximum), Ordering::Release);
                    *context.version = Some(version);
                    if !context.config.protocol_mode.accepts(version) {
                        terminal_frame(&context, profile_refusal(context.config));
                        *context.flush = true;
                        return Err(failure(
                            ReasonCode::UNSUPPORTED_PROTOCOL_VERSION,
                            "protocol rejected by listener profile",
                        ));
                    }
                    let response = context
                        .worker
                        .request(Operation::ConnectFrameAt {
                            deadline: expires
                                .ok_or_else(|| io::Error::other("missing handshake deadline"))?
                                .into_std(),
                            frame,
                            generation: context.generation,
                            wake: context.wake_tx.clone(),
                            cancel: context.cancel_tx.clone(),
                            close_reason: context.close_tx.clone(),
                        })
                        .await?;
                    let accepted =
                        authentication::handshake_reply(&mut context, response, version, client)
                            .await?;
                    if !accepted {
                        initial_auth = true;
                        tokio::task::yield_now().await;
                        continue;
                    }
                    keepalive = if keep_alive == 0 {
                        None
                    } else {
                        Some(Duration::from_millis(u64::from(keep_alive) * 1500))
                    };
                }
            } else {
                if matches!(frame.packet, Packet::Auth) {
                    if frame.reason_code.value() == 0x19 && reauth_deadline.is_none() {
                        reauth_deadline = Some(Instant::now() + context.config.handshake_timeout);
                    }
                    let deadline = reauth_deadline
                        .unwrap_or_else(|| Instant::now() + context.config.handshake_timeout);
                    let response = context
                        .worker
                        .request(Operation::Authenticate {
                            generation: context.generation,
                            frame,
                            deadline: deadline.into_std(),
                        })
                        .await?;
                    let response = match response {
                        Reply::AuthContinue(frame) => frame,
                        Reply::AuthComplete(frame) => {
                            reauth_deadline = None;
                            frame
                        }
                        _ => return Err(io::Error::other("invalid reauthentication response")),
                    };
                    context
                        .output
                        .send(outgoing(response, context.credits)?)
                        .await
                        .map_err(io::Error::other)?;
                    expires = keepalive.map(|duration| received + duration);
                    continue;
                }
                aliases.normalize(&mut frame, context.config.max_packet_size)?;
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
            _=crate::packet_io::deadline(match (expires, reauth_deadline) { (Some(a),Some(b))=>Some(a.min(b)), (a,b)=>a.or(b) })=>{
                if reauth_deadline.is_some_and(|at|Instant::now()>=at) { return Err(failure(ReasonCode::NOT_AUTHORIZED,"reauthentication deadline exceeded")); }
                if initial_auth { return Err(failure(ReasonCode::NOT_AUTHORIZED,"initial authentication deadline exceeded")); }
                return Err(io::Error::new(io::ErrorKind::TimedOut,"MQTT receive deadline exceeded"));
            },
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
    #[test]
    fn phase2h_identifier_capability_preserves_handshake_size_and_other_limits() {
        let config = BrokerConfig::default();
        let frame = accepted_connack(ProtocolVersion::V5, false, &config, None);
        assert!(
            frame
                .properties
                .contains(&Property::SubscriptionIdentifierAvailable(1))
        );
        assert!(
            frame
                .properties
                .contains(&Property::SharedSubscriptionAvailable(1))
        );
        assert!(frame.properties.contains(&Property::ReceiveMaximum(32)));
        assert!(
            frame
                .properties
                .contains(&Property::MaximumPacketSize(1_048_576))
        );
        assert!(frame.properties.contains(&Property::TopicAliasMaximum(32)));
        assert_eq!(encode_frame(&frame).unwrap().len(), 20);
        let assigned = accepted_connack(
            ProtocolVersion::V5,
            false,
            &config,
            Some("r0123456789abcdef012345"),
        );
        assert_eq!(encode_frame(&assigned).unwrap().len(), 46);
        assert!(
            accepted_connack(ProtocolVersion::V311, false, &config, None)
                .properties
                .is_empty()
        );
    }
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
            WriterLimits {
                config,
                peer_maximum: Arc::new(AtomicUsize::new(usize::MAX)),
                peer_alias_maximum: Arc::new(AtomicUsize::new(0)),
            },
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
            WriterLimits {
                config,
                peer_maximum: Arc::new(AtomicUsize::new(usize::MAX)),
                peer_alias_maximum: Arc::new(AtomicUsize::new(0)),
            },
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
    async fn phase2g_empty_identifier_is_assigned_and_connection_uses_resolved_key() {
        let (address, stop, task) = super::super::live_tests::broker(BrokerConfig::default()).await;
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut connect = connect_frame();
        if let Packet::Connect { client_id, .. } = &mut connect.packet {
            client_id.clear();
        }
        client
            .write_all(&encode_frame(&connect).unwrap())
            .await
            .unwrap();
        let reply = received(&mut client).await;
        assert!(matches!(
            reply.packet,
            Packet::ConnAck {
                code: 0,
                session_present: false
            }
        ));
        let assigned: Vec<_> = reply
            .properties
            .iter()
            .filter_map(|p| {
                if let Property::AssignedClientIdentifier(id) = p {
                    Some(id)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].len(), 23);
        assert!(assigned[0].bytes().all(|b| b.is_ascii_alphanumeric()));
        client
            .write_all(&encode_frame(&Frame::v5(Packet::PingReq)).unwrap())
            .await
            .unwrap();
        assert_eq!(received(&mut client).await.packet, Packet::PingResp);
        super::super::live_tests::stop(stop, task).await;
    }
    #[tokio::test]
    async fn phase2g_positive_peer_maximum_is_accepted_at_exact_connack_boundary() {
        let config = BrokerConfig::default();
        let expected = encode_frame(&connack(ProtocolVersion::V5, false, 0, &config))
            .unwrap()
            .len();
        let (address, stop, task) = super::super::live_tests::broker(config).await;
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut connect = connect_frame();
        connect
            .properties
            .push(Property::MaximumPacketSize(expected as u32));
        client
            .write_all(&encode_frame(&connect).unwrap())
            .await
            .unwrap();
        let reply = received(&mut client).await;
        assert!(matches!(reply.packet, Packet::ConnAck { code: 0, .. }));
        assert_eq!(encode_frame(&reply).unwrap().len(), expected);
        super::super::live_tests::stop(stop, task).await;
    }
    #[tokio::test]
    async fn phase2g_connack_too_small_refuses_without_taking_over_old_connection() {
        let (mut old, stop, task) = server(BrokerConfig::default()).await;
        // The old connection remains alive when a new proposal cannot receive
        // the mandatory capability response. Exercise the actual worker path.
        let address = old.peer_addr().unwrap();
        for maximum in [1, 4, 5, 16] {
            let mut contender = tokio::net::TcpStream::connect(address).await.unwrap();
            let mut connect = connect_frame();
            connect
                .properties
                .push(Property::MaximumPacketSize(maximum));
            contender
                .write_all(&encode_frame(&connect).unwrap())
                .await
                .unwrap();
            if maximum >= 5 {
                let reply = received(&mut contender).await;
                assert_eq!(
                    reply.packet,
                    Packet::ConnAck {
                        session_present: false,
                        code: 0x95
                    }
                );
                assert!(encode_frame(&reply).unwrap().len() <= maximum as usize);
            }
            assert_eq!(
                contender.read_u8().await.unwrap_err().kind(),
                io::ErrorKind::UnexpectedEof
            );
            old.write_all(&encode_frame(&Frame::v5(Packet::PingReq)).unwrap())
                .await
                .unwrap();
            assert_eq!(received(&mut old).await.packet, Packet::PingResp);
        }
        super::super::live_tests::stop(stop, task).await;
    }
    #[tokio::test]
    async fn phase2g_writer_guard_checks_complete_data_and_control_frames_before_bytes() {
        let frames = [
            Frame::v5(Packet::Publish {
                topic: "guard".into(),
                payload: vec![7; 128],
                qos: 0,
                packet_id: None,
                dup: false,
                retain: false,
            }),
            Frame::v5(Packet::SubAck {
                packet_id: 1,
                codes: vec![0; 128],
            }),
            connack(ProtocolVersion::V5, false, 0x95, &BrokerConfig::default()),
            {
                let mut f = Frame::v5(Packet::Disconnect);
                f.reason_code = ReasonCode::SESSION_TAKEN_OVER;
                f
            },
        ];
        for frame in frames {
            let size = encode_frame(&frame).unwrap().len();
            for (maximum, fits) in [(size, true), (size - 1, false)] {
                let (mut peer, mut writer) = sockets().await;
                let outcome =
                    write_frame(&mut writer, &frame, &BrokerConfig::default(), maximum).await;
                if fits {
                    assert!(outcome.is_ok());
                    assert_eq!(received(&mut peer).await, frame);
                } else {
                    assert!(outcome.is_err());
                    drop(writer);
                    assert_eq!(
                        peer.read_u8().await.unwrap_err().kind(),
                        io::ErrorKind::UnexpectedEof
                    );
                }
            }
        }
    }
    #[tokio::test]
    async fn phase2g_assigned_connack_must_fit_and_explicit_identifier_has_no_assignment() {
        let config = BrokerConfig::default();
        let assigned_size = encode_frame(&accepted_connack(
            ProtocolVersion::V5,
            false,
            &config,
            Some("r0000000000000000000000"),
        ))
        .unwrap()
        .len();
        let (address, stop, task) = super::super::live_tests::broker(config).await;
        for (empty, maximum) in [
            (true, assigned_size - 1),
            (true, assigned_size),
            (false, assigned_size),
        ] {
            let mut peer = tokio::net::TcpStream::connect(address).await.unwrap();
            let mut connect = connect_frame();
            if empty
                && let Packet::Connect {
                    client_id,
                    clean_session,
                    ..
                } = &mut connect.packet
            {
                client_id.clear();
                *clean_session = false;
            }
            connect
                .properties
                .push(Property::MaximumPacketSize(maximum as u32));
            peer.write_all(&encode_frame(&connect).unwrap())
                .await
                .unwrap();
            let reply = received(&mut peer).await;
            assert!(encode_frame(&reply).unwrap().len() <= maximum);
            if empty && maximum < assigned_size {
                assert_eq!(
                    reply.packet,
                    Packet::ConnAck {
                        session_present: false,
                        code: 0x95
                    }
                );
            } else {
                assert!(matches!(reply.packet, Packet::ConnAck { code: 0, .. }));
                assert_eq!(
                    reply
                        .properties
                        .iter()
                        .filter(|p| matches!(p, Property::AssignedClientIdentifier(_)))
                        .count(),
                    usize::from(empty)
                );
            }
        }
        super::super::live_tests::stop(stop, task).await;
    }
    #[derive(Debug)]
    struct AssignedPolicy {
        seen: Arc<Mutex<Vec<String>>>,
    }
    impl mqtt_auth::AccessControl for AssignedPolicy {
        fn authenticate(
            &self,
            info: mqtt_auth::ConnectInfo<'_>,
        ) -> Result<mqtt_auth::Principal, mqtt_auth::AuthError> {
            self.seen.lock().unwrap().push(info.client_id.into());
            if info.client_id.starts_with('r') && info.client_id.len() == 23 {
                Ok(mqtt_auth::Principal {
                    name: "assigned-owner".into(),
                })
            } else {
                Err(mqtt_auth::AuthError::NotAuthorized)
            }
        }
        fn authorize(
            &self,
            _principal: &mqtt_auth::Principal,
            action: mqtt_auth::Action,
            resource: &str,
        ) -> bool {
            action != mqtt_auth::Action::Connect
                || (resource.starts_with('r') && resource.len() == 23)
        }
    }
    #[tokio::test]
    async fn phase2g_generated_key_is_used_for_authentication_and_authorization() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let config = BrokerConfig {
            access_control: Arc::new(AssignedPolicy { seen: seen.clone() }),
            ..BrokerConfig::default()
        };
        let (address, stop, task) = super::super::live_tests::broker(config).await;
        let mut peer = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut connect = connect_frame();
        if let Packet::Connect { client_id, .. } = &mut connect.packet {
            client_id.clear();
        }
        peer.write_all(&encode_frame(&connect).unwrap())
            .await
            .unwrap();
        let reply = received(&mut peer).await;
        assert!(matches!(reply.packet, Packet::ConnAck { code: 0, .. }));
        let id = reply
            .properties
            .iter()
            .find_map(|p| {
                if let Property::AssignedClientIdentifier(id) = p {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![id]);
        peer.write_all(&encode_frame(&Frame::v5(Packet::PingReq)).unwrap())
            .await
            .unwrap();
        assert_eq!(received(&mut peer).await.packet, Packet::PingResp);
        super::super::live_tests::stop(stop, task).await;
    }
    #[tokio::test]
    async fn phase2g_malformed_payload_obeys_already_valid_connect_maximum() {
        let (address, stop, task) = super::super::live_tests::broker(BrokerConfig::default()).await;
        for maximum in [1, 4, 5] {
            let mut peer = tokio::net::TcpStream::connect(address).await.unwrap();
            let mut connect = connect_frame();
            connect
                .properties
                .push(Property::MaximumPacketSize(maximum));
            let mut bytes = encode_frame(&connect).unwrap();
            assert!(bytes[1] < 127);
            bytes[1] += 1;
            bytes.push(0); // Valid properties followed by invalid trailing payload.
            peer.write_all(&bytes).await.unwrap();
            if maximum >= 5 {
                let reply = received(&mut peer).await;
                assert!(matches!(reply.packet, Packet::ConnAck { code: 0x81, .. }));
                assert!(encode_frame(&reply).unwrap().len() <= maximum as usize);
            }
            assert_eq!(
                peer.read_u8().await.unwrap_err().kind(),
                io::ErrorKind::UnexpectedEof
            );
        }
        super::super::live_tests::stop(stop, task).await;
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
                        maximum_packet_size: u32::MAX,
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
        let (worker, worker_task) =
            super::super::tests::seeded(config.clone(), engine, clock.clone(), online).await;
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
            WriterLimits {
                config: config.clone(),
                peer_maximum: Arc::new(AtomicUsize::new(usize::MAX)),
                peer_alias_maximum: Arc::new(AtomicUsize::new(0)),
            },
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
        let bytes = super::super::tests::stored(&db.0, &config).unwrap();
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
                    maximum_packet_size: u32::MAX,
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
            clock.clone(),
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
                        maximum_packet_size: u32::MAX,
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
        let (worker, task) = super::super::tests::seeded(config, engine, clock, online).await;
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
    fn alias_publish(topic: &str, payload: &[u8], qos: u8, alias: Option<u16>) -> Frame {
        let mut frame = Frame::v5(Packet::Publish {
            topic: topic.into(),
            payload: payload.to_vec(),
            qos,
            packet_id: (qos > 0).then_some(7),
            dup: false,
            retain: false,
        });
        if let Some(alias) = alias {
            frame.properties.push(Property::TopicAlias(alias));
        }
        frame
    }
    async fn subscribe_alias_test(client: &mut tokio::net::TcpStream, filter: &str, qos: u8) {
        client
            .write_all(
                &encode_frame(&Frame::v5(Packet::Subscribe {
                    packet_id: 1,
                    filters: vec![(filter.into(), qos)],
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            received(client).await.packet,
            Packet::SubAck { .. }
        ));
    }
    #[tokio::test]
    async fn topic_alias_qos2_binding_changes_do_not_replace_original_exchange() {
        let (mut client, stop, task) = server(BrokerConfig::default()).await;
        subscribe_alias_test(&mut client, "alias/#", 2).await;
        let original = alias_publish("alias/original", b"original", 2, Some(1));
        client
            .write_all(&encode_frame(&original).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            received(&mut client).await.packet,
            Packet::PubRec { packet_id: 7 }
        ));
        client
            .write_all(
                &encode_frame(&alias_publish("alias/rebound", b"later", 0, Some(1))).unwrap(),
            )
            .await
            .unwrap();
        assert!(
            matches!(received(&mut client).await.packet,Packet::Publish{topic,payload,..} if topic=="alias/rebound" && payload==b"later")
        );
        let mut duplicate = alias_publish("", b"must-not-replace", 2, Some(1));
        if let Packet::Publish { dup, .. } = &mut duplicate.packet {
            *dup = true;
        }
        client
            .write_all(&encode_frame(&duplicate).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            received(&mut client).await.packet,
            Packet::PubRec { packet_id: 7 }
        ));
        client
            .write_all(&encode_frame(&Frame::v5(Packet::PubRel { packet_id: 7 })).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            received(&mut client).await.packet,
            Packet::PubComp { packet_id: 7 }
        ));
        let delivered = received(&mut client).await;
        assert!(
            !delivered
                .properties
                .iter()
                .any(|p| matches!(p, Property::TopicAlias(_)))
        );
        let Packet::Publish {
            topic,
            payload,
            packet_id: Some(id),
            ..
        } = delivered.packet
        else {
            panic!("expected original publish")
        };
        assert_eq!(topic, "alias/original");
        assert_eq!(payload, b"original");
        client
            .write_all(&encode_frame(&Frame::v5(Packet::PubRec { packet_id: id })).unwrap())
            .await
            .unwrap();
        assert!(
            matches!(received(&mut client).await.packet,Packet::PubRel{packet_id} if packet_id==id)
        );
        client
            .write_all(&encode_frame(&Frame::v5(Packet::PubComp { packet_id: id })).unwrap())
            .await
            .unwrap();
        client
            .write_all(&encode_frame(&alias_publish("", b"reference", 0, Some(1))).unwrap())
            .await
            .unwrap();
        assert!(
            matches!(received(&mut client).await.packet,Packet::Publish{topic,..} if topic=="alias/rebound")
        );
        client
            .write_all(&encode_frame(&alias_publish("", b"unknown", 0, Some(2))).unwrap())
            .await
            .unwrap();
        assert_eq!(
            received(&mut client).await.reason_code,
            ReasonCode::TOPIC_ALIAS_INVALID
        );
        stop.send_replace(true);
        task.await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn topic_alias_writer_binds_reuses_replaces_and_keeps_input_map_independent() {
        let (address, stop, task) = super::super::live_tests::broker(BrokerConfig {
            topic_alias_maximum: 1,
            ..BrokerConfig::default()
        })
        .await;
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut connect = connect_frame();
        connect.properties.push(Property::TopicAliasMaximum(1));
        client
            .write_all(&encode_frame(&connect).unwrap())
            .await
            .unwrap();
        assert!(
            received(&mut client)
                .await
                .properties
                .contains(&Property::TopicAliasMaximum(1))
        );
        subscribe_alias_test(&mut client, "alias/#", 0).await;
        for (topic, expected) in [
            ("alias/first", "alias/first"),
            ("alias/first", ""),
            ("alias/second", "alias/second"),
            ("alias/first", "alias/first"),
        ] {
            client
                .write_all(&encode_frame(&alias_publish(topic, b"data", 0, None)).unwrap())
                .await
                .unwrap();
            let frame = received(&mut client).await;
            assert!(frame.properties.contains(&Property::TopicAlias(1)));
            assert!(matches!(frame.packet,Packet::Publish{topic,..} if topic==expected));
        }
        client
            .write_all(
                &encode_frame(&alias_publish("", b"input-map-is-empty", 0, Some(1))).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            received(&mut client).await.reason_code,
            ReasonCode::TOPIC_ALIAS_INVALID
        );
        stop.send_replace(true);
        task.await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn topic_alias_binding_cap_fallback_does_not_create_unsent_mapping() {
        let canonical = alias_publish("alias/long/topic", &[b'x'; 40], 0, None);
        let cap = encode_frame(&canonical).unwrap().len();
        let (address, stop, task) = super::super::live_tests::broker(BrokerConfig::default()).await;
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut connect = connect_frame();
        connect.properties = vec![
            Property::TopicAliasMaximum(1),
            Property::MaximumPacketSize(cap as u32),
        ];
        client
            .write_all(&encode_frame(&connect).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            received(&mut client).await.packet,
            Packet::ConnAck { code: 0, .. }
        ));
        subscribe_alias_test(&mut client, "alias/#", 0).await;
        for _ in 0..2 {
            client
                .write_all(&encode_frame(&canonical).unwrap())
                .await
                .unwrap();
            let frame = received(&mut client).await;
            assert_eq!(frame, canonical);
        }
        let short = alias_publish("alias/long/topic", b"short", 0, None);
        client
            .write_all(&encode_frame(&short).unwrap())
            .await
            .unwrap();
        let first = received(&mut client).await;
        assert!(first.properties.contains(&Property::TopicAlias(1)));
        assert!(matches!(first.packet,Packet::Publish{topic,..} if !topic.is_empty()));
        client
            .write_all(&encode_frame(&short).unwrap())
            .await
            .unwrap();
        let next = received(&mut client).await;
        assert!(next.properties.contains(&Property::TopicAlias(1)));
        assert!(matches!(next.packet,Packet::Publish{topic,..} if topic.is_empty()));
        stop.send_replace(true);
        task.await.unwrap().unwrap();
    }
}

#[cfg(test)]
#[path = "network/phase3_tests.rs"]
mod phase3_tests;
