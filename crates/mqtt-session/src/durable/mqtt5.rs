//! Version adaptation shares the existing router, incoming FSM and outbox.
use super::*;

#[derive(Clone, Debug)]
pub struct ConnectionOptions {
    pub version: ProtocolVersion,
    pub clean_start: bool,
    pub session_expiry_interval: u32,
    pub receive_maximum: u16,
    pub will_properties: Properties,
}
#[derive(Clone, Debug)]
pub struct Delivery {
    pub frame: Frame,
    pub expires_at: Option<u64>,
    pub ticket: Option<u64>,
}
impl Delivery {
    /// Recompute seconds immediately before writing, including time spent
    /// committing an already-accepted durable start intent.
    pub fn refresh_remaining(&self, frame: &mut Frame, now_millis: u64) {
        if frame.version == ProtocolVersion::V5 && matches!(frame.packet, Packet::Publish { .. }) {
            frame.properties =
                temporal::remaining_properties(&frame.properties, self.expires_at, now_millis);
        }
    }
}
#[derive(Debug)]
pub struct DeliveryPollOutcome {
    pub delivery: Option<Delivery>,
    pub changed: bool,
}
#[derive(Debug)]
pub struct FramePollOutcome {
    pub frame: Option<Frame>,
    pub changed: bool,
}
fn reason(code: u8) -> Result<ReasonCode, EngineError> {
    ReasonCode::try_from(code).map_err(|_| invalid_record("invalid reason code"))
}
pub(super) fn mqtt5_error(code: u8) -> EngineError {
    match reason(code) {
        Ok(code) => EngineError::Mqtt5(code),
        Err(error) => error,
    }
}
fn wrap(version: ProtocolVersion, packet: Packet, properties: Properties) -> Frame {
    if version == ProtocolVersion::V311 {
        Frame::v311(packet)
    } else {
        let mut frame = Frame::v5(packet);
        frame.properties = super::temporal::remaining_properties(&properties, None, 0);
        frame
    }
}
/// Preflight the complete recipient envelope, including both variable integers.
pub(super) fn check_delivery_size(
    limit: usize,
    version: ProtocolVersion,
    packet: &Packet,
    properties: &Properties,
) -> Result<(), EngineError> {
    let frame = wrap(version, packet.clone(), properties.clone());
    let bytes = encode_frame(&frame).map_err(|error| invalid_record(error.to_string()))?;
    if bytes.len() > limit {
        return Err(EngineError::Capacity);
    }
    Ok(())
}
impl SessionEngine {
    pub(super) fn check_resume_size(
        &self,
        session: &Session,
        version: ProtocolVersion,
    ) -> Result<(), EngineError> {
        for message in &session.queued {
            check_delivery_size(
                self.config.max_packet_size,
                version,
                &message.packet((message.qos != 0).then_some(1), false),
                &message.properties,
            )?;
        }
        for (id, state) in &session.outgoing.pending {
            if let OutgoingState::AwaitingAck { _message }
            | OutgoingState::AwaitingReceipt { _message } = state
            {
                let packet = decode_publish(_message, self.config.max_packet_size)?;
                check_delivery_size(
                    self.config.max_packet_size,
                    version,
                    &packet,
                    &session
                        .outgoing_properties
                        .get(id)
                        .cloned()
                        .unwrap_or_default(),
                )?;
            }
        }
        Ok(())
    }
    /// Only properties whose delivery semantics are implemented may enter records.
    pub(super) fn validate_properties(
        &self,
        packet: &Packet,
        properties: &Properties,
    ) -> Result<(), EngineError> {
        for property in properties {
            match property {
                Property::UserProperty(..)
                | Property::ContentType(_)
                | Property::PayloadFormatIndicator(_)
                | Property::ResponseTopic(_)
                | Property::CorrelationData(_)
                | Property::MessageExpiryInterval(_) => {}
                Property::TopicAlias(_) => return Err(mqtt5_error(0x94)),
                Property::SubscriptionIdentifier(_) => return Err(mqtt5_error(0x82)),
                _ => return Err(mqtt5_error(0x83)),
            }
        }
        if properties.is_empty() {
            return Ok(());
        }
        let frame = wrap(ProtocolVersion::V5, packet.clone(), properties.clone());
        let bytes = encode_frame(&frame).map_err(|e| invalid_record(e.to_string()))?;
        if bytes.len() > self.config.max_packet_size {
            return Err(EngineError::Capacity);
        }
        Ok(())
    }
    pub fn handle_frame(
        &mut self,
        client_id: &str,
        frame: Frame,
    ) -> Result<Vec<Frame>, EngineError> {
        self.handle_frame_at(client_id, frame, self.clock.now_millis())
    }
    /// Handles a frame using its original wall-clock receipt time for publication TTL.
    /// Future receipt times are clamped to the current clock; existing QoS2 state
    /// retains the deadline established by the first accepted PUBLISH.
    pub fn handle_frame_at(
        &mut self,
        client_id: &str,
        frame: Frame,
        received_at_millis: u64,
    ) -> Result<Vec<Frame>, EngineError> {
        self.require_online(client_id)?;
        let version = self
            .sessions
            .get(client_id)
            .ok_or(EngineError::UnknownSession)?
            .version;
        if frame.version != version {
            return Err(mqtt5_error(0x82));
        }
        let bytes = encode_frame(&frame).map_err(|error| match error {
            mqtt_protocol::ProtocolError::Mqtt5(code) => EngineError::Mqtt5(code),
            _ => mqtt5_error(0x82),
        })?;
        if bytes.len() > self.config.max_packet_size {
            return Err(mqtt5_error(0x95));
        }
        self.expire_sessions()?;
        if version == ProtocolVersion::V311 {
            return self
                .handle_packet(client_id, frame.packet, Vec::new())
                .map(|packets| packets.into_iter().map(Frame::v311).collect());
        }
        if matches!(frame.packet, Packet::Publish { .. }) {
            self.validate_properties(&frame.packet, &frame.properties)?;
        }
        match frame.packet {
            Packet::Subscribe { packet_id, filters } => {
                let identifier_unsupported = frame
                    .properties
                    .iter()
                    .any(|p| matches!(p, Property::SubscriptionIdentifier(_)));
                // CONNACK advertises both capabilities as unavailable. Their use
                // is a connection error, never a partial subscription grant.
                if identifier_unsupported {
                    return Err(mqtt5_error(0xa1));
                }
                if filters
                    .iter()
                    .any(|(filter, _)| filter.starts_with("$share/"))
                {
                    return Err(mqtt5_error(0x9e));
                }
                let mut codes = Vec::with_capacity(filters.len());
                for (index, (filter, qos)) in filters.into_iter().enumerate() {
                    let option = frame
                        .subscription_options
                        .get(index)
                        .copied()
                        .unwrap_or(qos);
                    let session = self
                        .sessions
                        .get(client_id)
                        .ok_or(EngineError::UnknownSession)?;
                    let denied = if option & 0x3c != 0 {
                        Some(0x83)
                    } else if !validate_filter(&filter) {
                        Some(0x8f)
                    } else if !self
                        .access
                        .authorize(&session.owner, Action::Subscribe, &filter)
                    {
                        Some(0x87)
                    } else if !session.subscriptions.contains_key(&filter)
                        && session.subscriptions.len() >= self.config.max_subscriptions
                    {
                        Some(0x97)
                    } else {
                        None
                    };
                    if let Some(code) = denied {
                        codes.push(code);
                        continue;
                    }
                    let replies = self.handle_packet(
                        client_id,
                        Packet::Subscribe {
                            packet_id,
                            filters: vec![(filter, qos)],
                        },
                        Vec::new(),
                    )?;
                    let granted = match replies.first() {
                        Some(Packet::SubAck { codes, .. }) => {
                            codes.first().copied().unwrap_or(0x80)
                        }
                        _ => return Err(invalid_record("missing subscription grant")),
                    };
                    codes.push(granted);
                }
                self.ensure_bounds()?;
                Ok(vec![Frame::v5(Packet::SubAck { packet_id, codes })])
            }
            Packet::Unsubscribe { packet_id, filters } => {
                let session = self
                    .sessions
                    .get(client_id)
                    .ok_or(EngineError::UnknownSession)?;
                let codes: Vec<ReasonCode> = filters
                    .iter()
                    .map(|filter| {
                        reason(if session.subscriptions.contains_key(filter) {
                            0
                        } else {
                            0x11
                        })
                    })
                    .collect::<Result<_, _>>()?;
                self.handle_packet(
                    client_id,
                    Packet::Unsubscribe { packet_id, filters },
                    Vec::new(),
                )?;
                let mut response = Frame::v5(Packet::UnsubAck { packet_id });
                response.reason_codes = codes;
                Ok(vec![response])
            }
            packet @ (Packet::PubAck { .. } | Packet::PubRec { .. } | Packet::PubComp { .. })
                if frame.reason_code.value() >= 0x80 =>
            {
                let (id, kind) = match packet {
                    Packet::PubAck { packet_id } => (packet_id, AckKind::PubAck),
                    Packet::PubRec { packet_id } => (packet_id, AckKind::PubRec),
                    Packet::PubComp { packet_id } => (packet_id, AckKind::PubComp),
                    _ => return Err(mqtt5_error(0x82)),
                };
                let id = identifier(id)?;
                let session = self
                    .sessions
                    .get_mut(client_id)
                    .ok_or(EngineError::UnknownSession)?;
                let valid = match (session.outgoing.pending.get(&id), kind) {
                    (None, _) => return Ok(Vec::new()),
                    (Some(OutgoingState::AwaitingAck { .. }), AckKind::PubAck)
                    | (Some(OutgoingState::AwaitingReceipt { .. }), AckKind::PubRec)
                    | (Some(OutgoingState::Released), AckKind::PubComp) => true,
                    _ => false,
                };
                if !valid {
                    return Err(EngineError::Qos(SessionError::UnexpectedAck));
                }
                session.outgoing.pending.remove(&id);
                session.outgoing.order.retain(|v| *v != id);
                session.replay.retain(|v| *v != id);
                session.outgoing.identifiers.release(id);
                session.outgoing_properties.remove(&id);
                session.outgoing_expiry.remove(&id);
                session.outgoing_started.remove(&id);
                session.delivery_tickets.remove(&id);
                session.sent.remove(&id);
                self.ensure_bounds()?;
                Ok(Vec::new())
            }
            Packet::PubRec { packet_id }
                if !self
                    .sessions
                    .get(client_id)
                    .ok_or(EngineError::UnknownSession)?
                    .outgoing
                    .pending
                    .contains_key(&identifier(packet_id)?) =>
            {
                let mut response = Frame::v5(Packet::PubRel { packet_id });
                response.reason_code = reason(0x92)?;
                Ok(vec![response])
            }
            Packet::PubRel { packet_id } => {
                let known = self
                    .sessions
                    .get(client_id)
                    .ok_or(EngineError::UnknownSession)?
                    .incoming
                    .pending
                    .contains_key(&identifier(packet_id)?);
                let packets =
                    self.handle_packet(client_id, Packet::PubRel { packet_id }, Vec::new())?;
                let mut frames: Vec<_> = packets.into_iter().map(Frame::v5).collect();
                if !known {
                    for response in &mut frames {
                        response.reason_code = reason(0x92)?;
                    }
                }
                Ok(frames)
            }
            Packet::Disconnect => {
                let expiry = frame.properties.iter().find_map(|p| {
                    if let Property::SessionExpiryInterval(v) = p {
                        Some(*v)
                    } else {
                        None
                    }
                });
                self.disconnect_with_options(
                    client_id,
                    frame.reason_code == ReasonCode::SUCCESS,
                    expiry,
                )?;
                Ok(Vec::new())
            }
            Packet::Auth => Err(mqtt5_error(0x82)),
            packet => self
                .handle_packet_at(client_id, packet, frame.properties, received_at_millis)
                .map(|packets| packets.into_iter().map(Frame::v5).collect()),
        }
    }
    pub fn poll_frame_with_changes(
        &mut self,
        client_id: &str,
    ) -> Result<FramePollOutcome, EngineError> {
        let poll = self.poll_delivery_with_changes(client_id)?;
        if let Some(delivery) = poll.delivery {
            let mut outcome = self.begin_delivery(client_id, delivery)?;
            outcome.changed |= poll.changed;
            Ok(outcome)
        } else {
            Ok(FramePollOutcome {
                frame: None,
                changed: poll.changed,
            })
        }
    }
    pub fn poll_delivery_with_changes(
        &mut self,
        client_id: &str,
    ) -> Result<DeliveryPollOutcome, EngineError> {
        self.require_online(client_id)?;
        let mut changed = self.expire_sessions()?;
        changed |= self.reconcile_authorization()?;
        let session = self
            .sessions
            .get_mut(client_id)
            .ok_or(EngineError::UnknownSession)?;
        let quota_available = session.sent.len() < usize::from(session.receive_maximum);
        let position = if quota_available {
            (!session.replay.is_empty()).then_some(0)
        } else {
            session.replay.iter().position(|id| {
                matches!(
                    session.outgoing.pending.get(id),
                    Some(OutgoingState::Released)
                )
            })
        };
        if let Some(position) = position {
            let id = *session
                .replay
                .get(position)
                .ok_or_else(|| invalid_record("replay position disappeared"))?;
            let packet = match session.outgoing.pending.get(&id) {
                Some(OutgoingState::Released) => Packet::PubRel {
                    packet_id: id.get(),
                },
                Some(OutgoingState::AwaitingAck { _message })
                | Some(OutgoingState::AwaitingReceipt { _message }) => {
                    let mut packet = decode_publish(_message, self.config.max_packet_size)?;
                    if let Packet::Publish { packet_id, dup, .. } = &mut packet {
                        *packet_id = Some(id.get());
                        *dup = true;
                    }
                    packet
                }
                None => return Err(invalid_record("replay references absent exchange")),
            };
            let properties = session
                .outgoing_properties
                .get(&id)
                .cloned()
                .unwrap_or_default();
            check_delivery_size(
                self.config.max_packet_size,
                session.version,
                &packet,
                &properties,
            )?;
            session.replay.remove(position);
            let publish = matches!(packet, Packet::Publish { .. });
            let ticket = if publish {
                session.sent.insert(id);
                Some(session.reserve_delivery(id)?)
            } else {
                None
            };
            let expires_at = session.outgoing_expiry.get(&id).copied();
            return Ok(DeliveryPollOutcome {
                delivery: Some(Delivery {
                    frame: wrap(session.version, packet, properties),
                    expires_at,
                    ticket,
                }),
                changed: true,
            });
        }
        if !quota_available || session.outgoing.is_full() {
            return Ok(DeliveryPollOutcome {
                delivery: None,
                changed,
            });
        }
        let Some(message) = session.queued.front() else {
            return Ok(DeliveryPollOutcome {
                delivery: None,
                changed,
            });
        };
        check_delivery_size(
            self.config.max_packet_size,
            session.version,
            &message.packet((message.qos != 0).then_some(1), false),
            &message.properties,
        )?;
        let message = session
            .queued
            .pop_front()
            .ok_or_else(|| invalid_record("queue disappeared"))?;
        let id = if message.qos == 0 {
            None
        } else {
            let template = encode(&message.packet(Some(1), false))
                .map_err(|e| invalid_record(e.to_string()))?;
            Some(session.outgoing.start(message.qos, template)?)
        };
        let packet = message.packet(id.map(NonZeroU16::get), false);
        let ticket = if let Some(id) = id {
            let bytes = encode(&packet).map_err(|e| invalid_record(e.to_string()))?;
            if let Some(
                OutgoingState::AwaitingAck { _message }
                | OutgoingState::AwaitingReceipt { _message },
            ) = session.outgoing.pending.get_mut(&id)
            {
                *_message = bytes;
            }
            session
                .outgoing_properties
                .insert(id, message.properties.clone());
            if let Some(deadline) = message.expires_at {
                session.outgoing_expiry.insert(id, deadline);
            }
            session.sent.insert(id);
            Some(session.reserve_delivery(id)?)
        } else {
            None
        };
        let delivery = Delivery {
            frame: wrap(session.version, packet, message.properties),
            expires_at: message.expires_at,
            ticket,
        };
        self.ensure_bounds()?;
        Ok(DeliveryPollOutcome {
            delivery: Some(delivery),
            changed: true,
        })
    }
    /// Commit this result before the writer begins its frame; a committed start
    /// intent is conservatively possibly-sent across a crash at the SQL/TCP gap.
    pub fn begin_delivery(
        &mut self,
        client_id: &str,
        delivery: Delivery,
    ) -> Result<FramePollOutcome, EngineError> {
        self.require_online(client_id)?;
        let previous_floor = self.clock_floor.get();
        let mut changed = self.expire_sessions()?;
        changed |= self.reconcile_authorization()?;
        let now = self.effective_now();
        let session = self
            .sessions
            .get_mut(client_id)
            .ok_or(EngineError::UnknownSession)?;
        if delivery.frame.version != session.version {
            return Ok(FramePollOutcome {
                frame: None,
                changed,
            });
        }
        let Packet::Publish {
            qos,
            packet_id,
            dup,
            ..
        } = &delivery.frame.packet
        else {
            return Ok(FramePollOutcome {
                frame: Some(delivery.frame),
                changed: changed || now != previous_floor,
            });
        };
        let mut frame = if *qos == 0 {
            if delivery.ticket.is_some() || temporal::is_expired(delivery.expires_at, now) {
                return Ok(FramePollOutcome {
                    frame: None,
                    changed,
                });
            }
            let mut frame = delivery.frame;
            frame.properties =
                temporal::remaining_properties(&frame.properties, delivery.expires_at, now);
            frame
        } else {
            let id = identifier(packet_id.ok_or_else(|| invalid_record("missing delivery ID"))?)?;
            if delivery.ticket.is_none()
                || session.delivery_tickets.get(&id).copied() != delivery.ticket
            {
                return Ok(FramePollOutcome {
                    frame: None,
                    changed,
                });
            }
            let bytes = match session.outgoing.pending.get(&id) {
                Some(OutgoingState::AwaitingAck { _message })
                | Some(OutgoingState::AwaitingReceipt { _message }) => _message,
                _ => {
                    return Ok(FramePollOutcome {
                        frame: None,
                        changed,
                    });
                }
            };
            let mut packet = decode_publish(bytes, self.config.max_packet_size)?;
            if let Packet::Publish { dup: actual, .. } = &mut packet {
                *actual = *dup;
            }
            let properties = session
                .outgoing_properties
                .get(&id)
                .cloned()
                .unwrap_or_default();
            let deadline = session.outgoing_expiry.get(&id).copied();
            if !session.outgoing_started.contains(&id) && temporal::is_expired(deadline, now) {
                session.retire_outgoing(id);
                return Ok(FramePollOutcome {
                    frame: None,
                    changed: true,
                });
            }
            changed |= session.outgoing_started.insert(id);
            wrap(
                session.version,
                packet,
                temporal::remaining_properties(&properties, deadline, now),
            )
        };
        if let Packet::Publish { topic, .. } = &frame.packet
            && (!self
                .access
                .authorize(&session.owner, Action::Connect, client_id)
                || !self
                    .access
                    .authorize(&session.owner, Action::Receive, topic))
        {
            return Ok(FramePollOutcome {
                frame: None,
                changed,
            });
        }
        if session.version == ProtocolVersion::V311 {
            frame.properties.clear();
        }
        let bytes = encode_frame(&frame).map_err(|e| invalid_record(e.to_string()))?;
        if bytes.len() > self.config.max_packet_size {
            return Err(EngineError::Capacity);
        }
        self.ensure_bounds()?;
        Ok(FramePollOutcome {
            frame: Some(frame),
            changed: changed || now != previous_floor,
        })
    }
}
