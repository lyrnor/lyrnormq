//! Version adaptation shares the existing router, incoming FSM and outbox.
use super::*;

#[derive(Clone, Debug)]
pub struct ConnectionOptions {
    pub version: ProtocolVersion,
    pub clean_start: bool,
    pub session_expiry_interval: u32,
    pub receive_maximum: u16,
    /// Maximum complete frame accepted by this connection; never persisted.
    pub maximum_packet_size: u32,
    pub will_properties: Properties,
}
#[derive(Clone, Debug)]
pub struct Delivery {
    pub frame: Frame,
    pub expires_at: Option<u64>,
    pub ticket: Option<u64>,
    /// Runtime-only shared group incarnation; never persisted.
    pub shared_subscription: Option<(String, u64)>,
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
fn wire_packet(version: ProtocolVersion, mut packet: Packet, replay: bool) -> Packet {
    if version == ProtocolVersion::V311
        && let Packet::Publish { retain, .. } = &mut packet
    {
        *retain = replay;
    }
    packet
}
/// Conservative wire-size ceiling for a Publish envelope. Always ≥ exact
/// `encode_frame` length; over-counting only forces the exact path more often.
pub(super) fn publish_frame_upper_bound(
    version: ProtocolVersion,
    packet: &Packet,
    properties: &[Property],
) -> Option<usize> {
    let Packet::Publish {
        topic,
        payload,
        qos,
        packet_id,
        ..
    } = packet
    else {
        return None;
    };
    let mut body = topic.len().saturating_add(payload.len()).saturating_add(2);
    if *qos > 0 {
        if packet_id.is_none() {
            return None;
        }
        body = body.saturating_add(2);
    }
    if version == ProtocolVersion::V5 {
        body = body.saturating_add(properties_wire_upper(properties));
    }
    Some(body.saturating_add(5))
}

fn properties_wire_upper(properties: &[Property]) -> usize {
    let mut bytes = 0usize;
    for property in properties {
        bytes = bytes.saturating_add(1);
        bytes = bytes.saturating_add(match property {
            Property::PayloadFormatIndicator(_) => 1,
            Property::MessageExpiryInterval(_) => 4,
            Property::ContentType(value)
            | Property::ResponseTopic(value)
            | Property::AssignedClientIdentifier(value)
            | Property::AuthenticationMethod(value)
            | Property::ResponseInformation(value)
            | Property::ServerReference(value)
            | Property::ReasonString(value) => value.len().saturating_add(2),
            Property::CorrelationData(value) | Property::AuthenticationData(value) => {
                value.len().saturating_add(2)
            }
            Property::SubscriptionIdentifier(value) => match *value {
                0..=127 => 1,
                128..=16_383 => 2,
                16_384..=2_097_151 => 3,
                _ => 4,
            },
            Property::TopicAlias(_)
            | Property::ReceiveMaximum(_)
            | Property::TopicAliasMaximum(_)
            | Property::ServerKeepAlive(_) => 2,
            Property::SessionExpiryInterval(_)
            | Property::WillDelayInterval(_)
            | Property::MaximumPacketSize(_) => 4,
            Property::UserProperty(key, value) => {
                key.len().saturating_add(value.len()).saturating_add(4)
            }
            Property::RequestProblemInformation(_)
            | Property::RequestResponseInformation(_)
            | Property::MaximumQos(_)
            | Property::RetainAvailable(_)
            | Property::WildcardSubscriptionAvailable(_)
            | Property::SubscriptionIdentifierAvailable(_)
            | Property::SharedSubscriptionAvailable(_) => 1,
        });
    }
    bytes.saturating_add(4)
}

/// Preflight the complete recipient envelope, including both variable integers.
pub(super) fn check_delivery_size(
    limit: usize,
    version: ProtocolVersion,
    packet: &Packet,
    properties: &[Property],
) -> Result<(), EngineError> {
    if publish_frame_upper_bound(version, packet, properties).is_some_and(|upper| upper <= limit) {
        return Ok(());
    }
    let frame = wrap(version, packet.clone(), properties.to_vec());
    let bytes = encode_frame(&frame).map_err(|error| invalid_record(error.to_string()))?;
    if bytes.len() > limit {
        return Err(EngineError::Capacity);
    }
    Ok(())
}
/// Only the current online connection can discard its recipient copy.
pub(super) fn exceeds_peer_limit(
    session: &Session,
    packet: &Packet,
    properties: &Properties,
    expires_at: Option<u64>,
    now: u64,
) -> Result<bool, EngineError> {
    if !session.online || session.maximum_packet_size == u32::MAX {
        return Ok(false);
    }
    let properties = temporal::remaining_properties(properties, expires_at, now);
    match check_delivery_size(
        session.maximum_packet_size as usize,
        session.version,
        packet,
        &properties,
    ) {
        Ok(()) => Ok(false),
        Err(EngineError::Capacity) => Ok(true),
        Err(error) => Err(error),
    }
}
/// Connection limits do not reject the source transaction or alter retained state.
/// Discard each recipient copy as completed, including old possibly-sent exchanges.
pub(super) fn discard_oversized(
    session: &mut Session,
    now: u64,
    server_limit: usize,
) -> Result<bool, EngineError> {
    if !session.online || session.maximum_packet_size == u32::MAX {
        return Ok(false);
    }
    let limit = session.maximum_packet_size as usize;
    let mut changed = false;
    for id in session.replay.clone() {
        let bytes = match session.outgoing.pending.get(&id) {
            Some(OutgoingState::AwaitingAck { _message })
            | Some(OutgoingState::AwaitingReceipt { _message }) => _message,
            Some(OutgoingState::Released) => continue,
            None => return Err(invalid_record("replay references absent exchange")),
        };
        let packet = decode_publish(bytes, server_limit)?;
        let properties = session
            .outgoing_properties
            .get(&id)
            .cloned()
            .unwrap_or_default();
        let properties = temporal::remaining_properties(
            &properties,
            session.outgoing_expiry.get(&id).copied(),
            now,
        );
        match check_delivery_size(limit, session.version, &packet, &properties) {
            Err(EngineError::Capacity) => {
                session.retire_outgoing(id);
                changed = true;
            }
            result => result?,
        }
    }
    let queued = session.queued.len();
    for _ in 0..queued {
        let message = session
            .queued
            .pop_front()
            .ok_or_else(|| invalid_record("queue disappeared"))?;
        let properties =
            temporal::remaining_properties(&message.properties, message.expires_at, now);
        match check_delivery_size(
            limit,
            session.version,
            &message.packet((message.qos != 0).then_some(1), false),
            &properties,
        ) {
            Err(EngineError::Capacity) => changed = true,
            Ok(()) => session.queued.push_back(message),
            Err(error) => return Err(error),
        }
    }
    Ok(changed)
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
    /// Client application metadata must not inject broker-owned identifiers.
    pub(super) fn validate_properties(
        &self,
        packet: &Packet,
        properties: &Properties,
    ) -> Result<(), EngineError> {
        if properties
            .iter()
            .any(|p| matches!(p, Property::SubscriptionIdentifier(_)))
        {
            return Err(mqtt5_error(0x82));
        }
        if properties.is_empty() {
            return Ok(());
        }
        self.validate_delivery_properties(packet, properties)?;
        if publish_frame_upper_bound(ProtocolVersion::V5, packet, properties)
            .is_some_and(|upper| upper <= self.config.max_packet_size)
        {
            return Ok(());
        }
        // `validate_delivery_properties` already proved the envelope encodes;
        // re-encode only when the upper bound exceeded the server limit.
        let bytes = encode_frame(&wrap(
            ProtocolVersion::V5,
            packet.clone(),
            properties.clone(),
        ))
        .map_err(|e| invalid_record(e.to_string()))?;
        if bytes.len() > self.config.max_packet_size {
            return Err(EngineError::Capacity);
        }
        Ok(())
    }
    /// Validate trusted recipient metadata without imposing a V5 wire envelope on
    /// a V311 delivery. Resume preflight enforces the newly chosen wire version.
    pub(super) fn validate_delivery_properties(
        &self,
        packet: &Packet,
        properties: &Properties,
    ) -> Result<(), EngineError> {
        for p in properties {
            match p {
                Property::UserProperty(..)
                | Property::ContentType(_)
                | Property::PayloadFormatIndicator(_)
                | Property::ResponseTopic(_)
                | Property::CorrelationData(_)
                | Property::MessageExpiryInterval(_)
                | Property::SubscriptionIdentifier(_) => {}
                Property::TopicAlias(_) => return Err(mqtt5_error(0x94)),
                _ => return Err(mqtt5_error(0x83)),
            }
        }
        // Empty properties: packet structure is checked by the accompanying
        // encode/`check_delivery_size` on every mutating path that trusts this.
        if properties.is_empty() {
            return Ok(());
        }
        encode_frame(&wrap(
            ProtocolVersion::V5,
            packet.clone(),
            properties.clone(),
        ))
        .map_err(|e| invalid_record(e.to_string()))?;
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
                let subscription_identifier = frame.properties.iter().find_map(|p| {
                    if let Property::SubscriptionIdentifier(n) = p {
                        Some(*n)
                    } else {
                        None
                    }
                });
                let mut codes = Vec::with_capacity(filters.len());
                for (index, (filter, qos)) in filters.into_iter().enumerate() {
                    let option = frame
                        .subscription_options
                        .get(index)
                        .copied()
                        .unwrap_or(qos);
                    let subscription = SubscriptionRecord {
                        filter,
                        qos,
                        no_local: option & 4 != 0,
                        retain_as_published: option & 8 != 0,
                        retain_handling: (option >> 4) & 3,
                        identifier: subscription_identifier,
                    };
                    codes.push(self.subscribe_entry(client_id, subscription)?);
                }
                self.ensure_bounds()?;
                Ok(vec![Frame::v5(Packet::SubAck { packet_id, codes })])
            }
            Packet::Unsubscribe { packet_id, filters } => {
                // Entry results observe the preceding removals, including a
                // repeated exact filter in the same packet (MQTT-3.10.4-6).
                // Validate the whole request before installing any removal;
                // malformed later entries must not partially change state.
                if filters
                    .iter()
                    .any(|filter| !validate_subscription_filter(filter))
                {
                    return Err(EngineError::InvalidPacket(
                        "invalid unsubscribe filter".into(),
                    ));
                }
                let mut codes = Vec::with_capacity(filters.len());
                for filter in filters {
                    let existed = self
                        .sessions
                        .get(client_id)
                        .ok_or(EngineError::UnknownSession)?
                        .subscriptions
                        .contains_key(&filter);
                    self.handle_packet(
                        client_id,
                        Packet::Unsubscribe {
                            packet_id,
                            filters: vec![filter],
                        },
                        Vec::new(),
                    )?;
                    codes.push(reason(if existed { 0 } else { 0x11 })?);
                }
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
                session.outgoing_retained_replay.remove(&id);
                session.outgoing_shared.remove(&id);
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
        let now = self.effective_now();
        let session = self
            .sessions
            .get_mut(client_id)
            .ok_or(EngineError::UnknownSession)?;
        changed |= discard_oversized(session, now, self.config.max_packet_size)?;
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
                    frame: wrap(
                        session.version,
                        wire_packet(
                            session.version,
                            packet,
                            session.outgoing_retained_replay.contains(&id),
                        ),
                        properties,
                    ),
                    expires_at,
                    ticket,
                    shared_subscription: None,
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
            if message.retained_replay {
                session.outgoing_retained_replay.insert(id);
            }
            if let Some(group) = &message.shared_subscription {
                session.outgoing_shared.insert(id, group.clone());
            }
            if let Some(deadline) = message.expires_at {
                session.outgoing_expiry.insert(id, deadline);
            }
            session.sent.insert(id);
            Some(session.reserve_delivery(id)?)
        } else {
            None
        };
        let delivery = Delivery {
            frame: wrap(
                session.version,
                wire_packet(session.version, packet, message.retained_replay),
                message.properties,
            ),
            expires_at: message.expires_at,
            ticket,
            shared_subscription: message.shared_subscription.as_ref().and_then(|key| {
                self.shared_epochs
                    .get(key)
                    .map(|epoch| (key.clone(), *epoch))
            }),
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
        // QoS0 without expiry/shared metadata cannot observe session or message
        // deadlines this call would act on; PollDelivery already expired.
        let qos0_fast = delivery.expires_at.is_none()
            && delivery.shared_subscription.is_none()
            && matches!(delivery.frame.packet, Packet::Publish { qos: 0, .. });
        let mut changed = if qos0_fast {
            false
        } else {
            self.expire_sessions()?
        };
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
            if delivery.ticket.is_some()
                || temporal::is_expired(delivery.expires_at, now)
                || delivery
                    .shared_subscription
                    .as_ref()
                    .is_some_and(|(key, epoch)| self.shared_epochs.get(key) != Some(epoch))
            {
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
                wire_packet(
                    session.version,
                    packet,
                    session.outgoing_retained_replay.contains(&id),
                ),
                temporal::remaining_properties(&properties, deadline, now),
            )
        };
        if let Packet::Publish { topic, .. } = &frame.packet
            && (!self.access.authorize_for_client(
                &session.owner,
                client_id,
                Action::Connect,
                client_id,
            ) || !self.access.authorize_for_client(
                &session.owner,
                client_id,
                Action::Receive,
                topic,
            ))
        {
            return Ok(FramePollOutcome {
                frame: None,
                changed,
            });
        }
        if session.version == ProtocolVersion::V311 {
            frame.properties.clear();
        }
        let peer_limit = session.maximum_packet_size as usize;
        let server_limit = self.config.max_packet_size;
        let under_both = publish_frame_upper_bound(frame.version, &frame.packet, &frame.properties)
            .is_some_and(|upper| upper <= server_limit && upper <= peer_limit);
        if !under_both {
            let bytes = encode_frame(&frame).map_err(|e| invalid_record(e.to_string()))?;
            if bytes.len() > server_limit {
                return Err(EngineError::Capacity);
            }
            if bytes.len() > peer_limit {
                if let Packet::Publish {
                    packet_id: Some(id),
                    ..
                } = frame.packet
                {
                    session.retire_outgoing(identifier(id)?);
                    changed = true;
                }
                return Ok(FramePollOutcome {
                    frame: None,
                    changed,
                });
            }
        }
        self.ensure_bounds()?;
        Ok(FramePollOutcome {
            frame: Some(frame),
            changed: changed || now != previous_floor,
        })
    }
}

#[cfg(test)]
mod upper_bound_tests {
    use super::*;
    use mqtt_protocol::{Packet, Property, encode_frame};

    fn publish(topic: &str, payload: &[u8], qos: u8) -> Packet {
        Packet::Publish {
            topic: topic.into(),
            payload: payload.to_vec(),
            qos,
            packet_id: (qos > 0).then_some(1),
            dup: false,
            retain: false,
        }
    }

    #[test]
    fn publish_frame_upper_bound_covers_exact_v311_and_v5() {
        let cases = [
            (ProtocolVersion::V311, publish("a/b", b"hello", 0), vec![]),
            (ProtocolVersion::V311, publish("a/b", b"hello", 1), vec![]),
            (ProtocolVersion::V5, publish("a/b", b"hello", 0), vec![]),
            (
                ProtocolVersion::V5,
                publish("t", b"x", 0),
                vec![
                    Property::ContentType("json".into()),
                    Property::UserProperty("k".into(), "v".into()),
                    Property::SubscriptionIdentifier(1),
                    Property::MessageExpiryInterval(60),
                ],
            ),
        ];
        for (version, packet, properties) in cases {
            let upper = publish_frame_upper_bound(version, &packet, &properties).expect("publish");
            let frame = wrap(version, packet, properties);
            let exact = encode_frame(&frame).unwrap().len();
            assert!(
                upper >= exact,
                "upper {upper} < exact {exact} for {version:?}"
            );
        }
    }

    #[test]
    fn check_delivery_size_accepts_when_upper_fits() {
        let packet = publish("sensors/1", &[0u8; 128], 0);
        check_delivery_size(1024, ProtocolVersion::V5, &packet, &[]).unwrap();
        assert!(matches!(
            check_delivery_size(8, ProtocolVersion::V5, &packet, &[]),
            Err(EngineError::Capacity)
        ));
    }
}
