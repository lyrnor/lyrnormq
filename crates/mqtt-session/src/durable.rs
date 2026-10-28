//! Durable session model, without IO. Every mutation is a **proposal**: callers
//! clone the engine, apply an event, call snapshot, durably commit those bytes,
//! then install the clone and send its packets. Discard a proposal on any error.
//! Neither method success nor serialized bytes alone establish durability.
use crate::outgoing::OutgoingState;
use crate::{AckAction, AckKind, IncomingQos2, OutgoingQos, SessionError};
use mqtt_auth::{AccessControl, Action, AllowAnonymous, Principal};
use mqtt_protocol::{
    Frame, Packet, Properties, Property, ProtocolVersion, ReasonCode, decode, encode, encode_frame,
};
use mqtt_router::retained::{RetainedError, RetainedStore};
use mqtt_router::{Router, validate_filter};
use records::{
    Envelope, Message, OutgoingRecord, RetainedRecord, SessionRecord, Stage, WillData, WillRecord,
};

mod authorization;
mod expiry;
mod lifecycle;
mod mqtt5;
mod records;
mod retention;
pub use expiry::{Clock, SystemClock};
pub use mqtt5::{ConnectionOptions, FramePollOutcome};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    error::Error,
    fmt,
    io::{self, Write},
    num::NonZeroU16,
    sync::Arc,
};

#[derive(Clone, Copy, Debug)]
pub struct SessionConfig {
    pub max_sessions: usize,
    pub max_retained: usize,
    pub max_subscriptions: usize,
    pub max_queued: usize,
    pub max_inflight: usize,
    pub max_packet_size: usize,
    pub max_state_bytes: usize,
}
#[derive(Debug)]
pub enum EngineError {
    InvalidConfig,
    Mqtt5(ReasonCode),
    Capacity,
    InvalidRecord(String),
    UnknownSession,
    OfflineSession,
    NotAuthorized,
    InvalidPacket(String),
    Qos(SessionError),
    Json(serde_json::Error),
    Retained(RetainedError),
}
impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mqtt5(code) => write!(f, "MQTT5 reason {:02x}", code.value()),
            Self::InvalidConfig => f.write_str("invalid durable session limits"),
            Self::Capacity => f.write_str("durable session capacity exceeded"),
            Self::InvalidRecord(s) => write!(f, "invalid durable record: {s}"),
            Self::UnknownSession => f.write_str("unknown session"),
            Self::OfflineSession => f.write_str("session is offline"),
            Self::NotAuthorized => f.write_str("operation is not authorized"),
            Self::InvalidPacket(s) => write!(f, "invalid session packet: {s}"),
            Self::Qos(error) => error.fmt(f),
            Self::Json(error) => error.fmt(f),
            Self::Retained(error) => error.fmt(f),
        }
    }
}
impl Error for EngineError {}
impl From<SessionError> for EngineError {
    fn from(error: SessionError) -> Self {
        Self::Qos(error)
    }
}
#[derive(Debug)]
pub struct ConnectResult {
    pub session_present: bool,
}

/// A poll can prune revoked deliveries without producing a network packet.
/// Commit whenever changed is true before installing the proposed model.
#[derive(Debug)]
pub struct PollOutcome {
    pub packet: Option<Packet>,
    pub changed: bool,
}

impl Message {
    fn packet(&self, id: Option<u16>, dup: bool) -> Packet {
        Packet::Publish {
            topic: self.topic.clone(),
            payload: self.payload.clone(),
            qos: self.qos,
            packet_id: id,
            dup,
            retain: self.retain,
        }
    }
}
#[derive(Clone)]
struct Session {
    owner: Principal,
    persistent: bool,
    online: bool,
    subscriptions: BTreeMap<String, u8>,
    incoming: IncomingQos2,
    outgoing: OutgoingQos,
    queued: VecDeque<Message>,
    replay: VecDeque<NonZeroU16>,
    version: ProtocolVersion,
    expiry: u32,
    expires_at: Option<u64>,
    receive_maximum: u16,
    sent: BTreeSet<NonZeroU16>,
    incoming_properties: BTreeMap<NonZeroU16, Properties>,
    outgoing_properties: BTreeMap<NonZeroU16, Properties>,
}
impl Session {
    fn new(config: SessionConfig, persistent: bool, owner: Principal) -> Self {
        Self {
            owner,
            persistent,
            online: true,
            subscriptions: BTreeMap::new(),
            incoming: IncomingQos2::new(config.max_inflight, config.max_packet_size, 0),
            outgoing: OutgoingQos::new(config.max_inflight, config.max_packet_size),
            queued: VecDeque::new(),
            replay: VecDeque::new(),
            version: ProtocolVersion::V311,
            expiry: if persistent { u32::MAX } else { 0 },
            expires_at: None,
            receive_maximum: u16::MAX,
            sent: BTreeSet::new(),
            incoming_properties: BTreeMap::new(),
            outgoing_properties: BTreeMap::new(),
        }
    }
}
#[derive(Clone)]
pub struct SessionEngine {
    access: Arc<dyn AccessControl>,
    clock: Arc<dyn Clock>,
    retained_properties: BTreeMap<String, Properties>,
    config: SessionConfig,
    sessions: BTreeMap<String, Session>,
    retained: RetainedStore,
    wills: BTreeMap<String, WillRecord>,
}

struct LimitedWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("snapshot byte limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn identifier(id: u16) -> Result<NonZeroU16, EngineError> {
    NonZeroU16::new(id).ok_or_else(|| EngineError::InvalidRecord("zero identifier".into()))
}
fn is_retained_capacity(error: &RetainedError) -> bool {
    matches!(error, RetainedError::CapacityExceeded)
}
fn invalid_record(message: impl Into<String>) -> EngineError {
    EngineError::InvalidRecord(message.into())
}
fn decode_publish(bytes: &[u8], limit: usize) -> Result<Packet, EngineError> {
    let decoded = decode(bytes, limit).map_err(|error| invalid_record(error.to_string()))?;
    match decoded {
        Some((packet @ Packet::Publish { .. }, consumed)) if consumed == bytes.len() => Ok(packet),
        _ => Err(invalid_record("stored message is not one complete PUBLISH")),
    }
}

impl SessionEngine {
    pub fn new(config: SessionConfig) -> Result<Self, EngineError> {
        Self::new_with_access(config, Arc::new(AllowAnonymous))
    }

    /// Configure a bounded, synchronous access policy; policy data is not persisted.
    pub fn new_with_access(
        config: SessionConfig,
        access: Arc<dyn AccessControl>,
    ) -> Result<Self, EngineError> {
        Self::new_with_clock(config, access, Arc::new(SystemClock))
    }
    pub fn new_with_clock(
        config: SessionConfig,
        access: Arc<dyn AccessControl>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, EngineError> {
        if config.max_sessions == 0
            || config.max_retained == 0
            || config.max_subscriptions == 0
            || config.max_queued == 0
            || !(1..=65535).contains(&config.max_inflight)
            || config.max_packet_size < 2
            || config.max_state_bytes == 0
        {
            return Err(EngineError::InvalidConfig);
        }
        let engine = Self {
            access,
            clock,
            retained_properties: BTreeMap::new(),
            config,
            sessions: BTreeMap::new(),
            retained: RetainedStore::new(config.max_retained, config.max_state_bytes)
                .map_err(EngineError::Retained)?,
            wills: BTreeMap::new(),
        };
        engine.ensure_bounds()?;
        Ok(engine)
    }

    /// Restore only known, validated records. Every restored session starts
    /// offline; connect reconstructs its replay cursor in original send order.
    pub fn restore(config: SessionConfig, bytes: &[u8]) -> Result<Self, EngineError> {
        Self::restore_with_access(config, bytes, Arc::new(AllowAnonymous))
    }

    /// Restore ownership without transferring legacy anonymous data to a new user.
    pub fn restore_with_access(
        config: SessionConfig,
        bytes: &[u8],
        access: Arc<dyn AccessControl>,
    ) -> Result<Self, EngineError> {
        Self::restore_with_clock(config, bytes, access, Arc::new(SystemClock))
    }
    pub fn restore_with_clock(
        config: SessionConfig,
        bytes: &[u8],
        access: Arc<dyn AccessControl>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, EngineError> {
        let mut engine = Self::new_with_clock(config, access, clock)?;
        if bytes.len() > config.max_state_bytes {
            return Err(EngineError::Capacity);
        }
        let (envelope, legacy) = records::decode(bytes)?;
        if envelope.sessions.len() > config.max_sessions
            || envelope.retained.len() > config.max_retained
            || envelope.wills.len() > config.max_sessions
        {
            return Err(EngineError::Capacity);
        }
        for record in envelope.sessions {
            engine.validate_client(&record.client_id)?;
            if engine.sessions.contains_key(&record.client_id) {
                return Err(invalid_record("duplicate client identifier"));
            }
            if record.subscriptions.len() > config.max_subscriptions
                || record.incoming.len() > config.max_inflight
                || record.outgoing.len() > config.max_inflight
                || record.queued.len() > config.max_queued
            {
                return Err(EngineError::Capacity);
            }
            Self::validate_owner(&record.owner)?;
            let mut session = Session::new(config, true, Principal { name: record.owner });
            if record.session_expiry_interval == 0
                || (record.session_expiry_interval == u32::MAX && record.expires_at.is_some())
            {
                return Err(invalid_record("invalid session expiry record"));
            }
            session.expiry = record.session_expiry_interval;
            session.expires_at = if session.expiry == u32::MAX {
                None
            } else {
                Some(record.expires_at.unwrap_or_else(|| {
                    engine
                        .clock
                        .now_millis()
                        .saturating_add(u64::from(session.expiry) * 1000)
                }))
            };
            session.online = false;
            for (filter, qos) in record.subscriptions {
                let packet = Packet::Subscribe {
                    packet_id: 1,
                    filters: vec![(filter.clone(), qos)],
                };
                let bytes = encode(&packet).map_err(|error| invalid_record(error.to_string()))?;
                if bytes.len() > config.max_packet_size {
                    return Err(EngineError::Capacity);
                }
                if qos > 2
                    || !validate_filter(&filter)
                    || session.subscriptions.insert(filter, qos).is_some()
                {
                    return Err(invalid_record("invalid or duplicate subscription"));
                }
            }
            for (id, bytes, properties) in record.incoming {
                let id = identifier(id)?;
                let packet = decode_publish(&bytes, config.max_packet_size)?;
                if legacy && matches!(packet, Packet::Publish { retain: true, .. }) {
                    return Err(invalid_record("retained input in legacy schema"));
                }
                if !matches!(packet,Packet::Publish {qos:2,packet_id:Some(actual),..} if actual==id.get())
                    || session.incoming.pending.contains_key(&id)
                {
                    return Err(invalid_record("invalid incoming QoS2 exchange"));
                }
                engine.validate_properties(&packet, &properties)?;
                session.incoming.publish(id, &bytes, false)?;
                session.incoming_properties.insert(id, properties);
            }
            for record in record.outgoing {
                let id = identifier(record.packet_id)?;
                if session.outgoing.pending.contains_key(&id) {
                    return Err(invalid_record("duplicate outgoing identifier"));
                }
                if legacy
                    && let Stage::Ack(bytes) | Stage::Receipt(bytes) = &record.stage
                    && matches!(
                        decode_publish(bytes, config.max_packet_size)?,
                        Packet::Publish { retain: true, .. }
                    )
                {
                    return Err(invalid_record("retained output in legacy schema"));
                }
                if matches!(record.stage, Stage::Comp) && !record.properties.is_empty() {
                    return Err(invalid_record("properties on released exchange"));
                }
                let state = match record.stage {
                    Stage::Ack(message) => {
                        Self::validate_outgoing_message(
                            &message,
                            1,
                            id.get(),
                            config.max_packet_size,
                        )?;
                        OutgoingState::AwaitingAck { _message: message }
                    }
                    Stage::Receipt(message) => {
                        Self::validate_outgoing_message(
                            &message,
                            2,
                            id.get(),
                            config.max_packet_size,
                        )?;
                        OutgoingState::AwaitingReceipt { _message: message }
                    }
                    Stage::Comp => OutgoingState::Released,
                };
                if let OutgoingState::AwaitingAck { _message }
                | OutgoingState::AwaitingReceipt { _message } = &state
                {
                    engine.validate_properties(
                        &decode_publish(_message, config.max_packet_size)?,
                        &record.properties,
                    )?;
                    session.outgoing_properties.insert(id, record.properties);
                }
                // Reserve the exact identifier in the existing allocator rather
                // than allocating a replacement and changing delivery identity.
                session.outgoing.identifiers.used[usize::from(id.get())] = true;
                session.outgoing.identifiers.count += 1;
                session.outgoing.pending.insert(id, state);
                session.outgoing.order.push_back(id);
            }
            for message in record.queued {
                engine.validate_message(&message)?;
                session.queued.push_back(message);
            }
            engine.sessions.insert(record.client_id, session);
        }
        let mut topics = std::collections::BTreeSet::new();
        for retained in envelope.retained {
            if retained.payload.is_empty() || !topics.insert(retained.topic.clone()) {
                return Err(invalid_record("empty or duplicate retained record"));
            }
            engine.validate_message(&Message {
                topic: retained.topic.clone(),
                payload: retained.payload.clone(),
                qos: retained.qos,
                retain: true,
                properties: retained.properties.clone(),
            })?;
            engine
                .retained_properties
                .insert(retained.topic.clone(), retained.properties);
            engine
                .retained
                .apply(retained.topic, retained.payload, retained.qos)
                .map_err(EngineError::Retained)?;
        }
        for record in envelope.wills {
            Self::validate_owner(&record.owner)?;
            if engine
                .sessions
                .get(&record.client_id)
                .is_some_and(|session| session.owner.name != record.owner)
            {
                return Err(invalid_record("session and Will ownership disagree"));
            }
            engine.validate_will(&record.client_id, &record.will.to_protocol())?;
            engine.validate_properties(
                &Message {
                    topic: record.will.topic.clone(),
                    payload: record.will.payload.clone(),
                    qos: record.will.qos,
                    retain: record.will.retain,
                    properties: Vec::new(),
                }
                .packet((record.will.qos != 0).then_some(1), false),
                &record.will.properties,
            )?;
            if engine
                .wills
                .insert(record.client_id.clone(), record)
                .is_some()
            {
                return Err(invalid_record("duplicate Will client identifier"));
            }
        }
        engine.ensure_bounds()?;
        Ok(engine)
    }

    fn validate_outgoing_message(
        bytes: &[u8],
        qos: u8,
        id: u16,
        limit: usize,
    ) -> Result<(), EngineError> {
        if !matches!(decode_publish(bytes,limit)?,Packet::Publish{qos:actual,packet_id:Some(actual_id),dup:false,..} if actual==qos && actual_id==id)
        {
            return Err(invalid_record(
                "outgoing stage and encoded message disagree",
            ));
        }
        Ok(())
    }
    fn validate_client(&self, client: &str) -> Result<(), EngineError> {
        let bytes = encode(&Packet::Connect {
            client_id: client.into(),
            clean_session: true,
            keep_alive: 0,
            will: None,
            username: None,
            password: None,
        })
        .map_err(|error| invalid_record(error.to_string()))?;
        if bytes.len() > self.config.max_packet_size {
            return Err(EngineError::Capacity);
        }
        Ok(())
    }
    fn validate_message(&self, message: &Message) -> Result<(), EngineError> {
        let id = if message.qos == 0 { None } else { Some(1) };
        self.validate_properties(&message.packet(id, false), &message.properties)?;
        let bytes = encode(&message.packet(id, false))
            .map_err(|error| invalid_record(error.to_string()))?;
        if bytes.len() > self.config.max_packet_size {
            return Err(EngineError::Capacity);
        }
        Ok(())
    }
    fn records(&self, persistent_only: bool) -> Result<Envelope, EngineError> {
        let mut records = Vec::new();
        for (client_id, session) in &self.sessions {
            if persistent_only && !session.persistent {
                continue;
            }
            let mut outgoing = Vec::new();
            for id in &session.outgoing.order {
                let stage = match session.outgoing.pending.get(id) {
                    Some(OutgoingState::AwaitingAck { _message }) => Stage::Ack(_message.clone()),
                    Some(OutgoingState::AwaitingReceipt { _message }) => {
                        Stage::Receipt(_message.clone())
                    }
                    Some(OutgoingState::Released) => Stage::Comp,
                    None => {
                        return Err(invalid_record("outgoing order references absent exchange"));
                    }
                };
                outgoing.push(OutgoingRecord {
                    packet_id: id.get(),
                    stage,
                    properties: session
                        .outgoing_properties
                        .get(id)
                        .cloned()
                        .unwrap_or_default(),
                });
            }
            records.push(SessionRecord {
                client_id: client_id.clone(),
                owner: session.owner.name.clone(),
                session_expiry_interval: session.expiry,
                expires_at: session.expires_at,
                subscriptions: session
                    .subscriptions
                    .iter()
                    .map(|(f, q)| (f.clone(), *q))
                    .collect(),
                incoming: session
                    .incoming
                    .pending
                    .iter()
                    .map(|(id, bytes)| {
                        (
                            id.get(),
                            bytes.clone(),
                            session
                                .incoming_properties
                                .get(id)
                                .cloned()
                                .unwrap_or_default(),
                        )
                    })
                    .collect(),
                outgoing,
                queued: session.queued.iter().cloned().collect(),
            });
        }
        Ok(Envelope {
            schema_version: 4,
            sessions: records,
            retained: self
                .retained
                .entries()
                .into_iter()
                .map(|message| RetainedRecord {
                    topic: message.topic.clone(),
                    payload: message.payload,
                    qos: message.qos,
                    properties: self
                        .retained_properties
                        .get(&message.topic)
                        .cloned()
                        .unwrap_or_default(),
                })
                .collect(),
            wills: self.wills.values().cloned().collect(),
        })
    }
    fn serialize(&self, persistent_only: bool) -> Result<Vec<u8>, EngineError> {
        let mut writer = LimitedWriter {
            bytes: Vec::new(),
            limit: self.config.max_state_bytes,
        };
        serde_json::to_writer(&mut writer, &self.records(persistent_only)?).map_err(|error| {
            if error.is_io() {
                EngineError::Capacity
            } else {
                EngineError::Json(error)
            }
        })?;
        Ok(writer.bytes)
    }
    fn ensure_bounds(&self) -> Result<(), EngineError> {
        self.serialize(false).map(|_| ())
    }

    /// Validate the complete live registry (including clean sessions), then emit
    /// the persistent subset. The worker must commit this before sending packets.
    pub fn snapshot(&self) -> Result<Vec<u8>, EngineError> {
        self.ensure_bounds()?;
        self.serialize(true)
    }
    fn require_online(&self, client: &str) -> Result<(), EngineError> {
        if self
            .sessions
            .get(client)
            .ok_or(EngineError::UnknownSession)?
            .online
        {
            Ok(())
        } else {
            Err(EngineError::OfflineSession)
        }
    }
    fn route(&mut self, message: Message) -> Result<(), EngineError> {
        self.expire_sessions()?;
        self.validate_message(&message)?;
        // Build the existing iterative trie from the bounded registry. It keeps
        // wildcard and overlap semantics in one implementation; future storage
        // layouts may retain this index across proposal clones for efficiency.
        let mut router = Router::default();
        let clients: Vec<String> = self.sessions.keys().cloned().collect();
        for (index, client) in clients.iter().enumerate() {
            if let Some(session) = self.sessions.get(client) {
                if !self
                    .access
                    .authorize(&session.owner, Action::Connect, client)
                {
                    continue;
                }
                for (filter, qos) in &session.subscriptions {
                    if self
                        .access
                        .authorize(&session.owner, Action::Subscribe, filter)
                    {
                        router.subscribe_qos(index as u64, filter.clone(), *qos);
                    }
                }
            }
        }
        let matches = router.matching_qos(&message.topic);
        // A later recipient's version must not discover an oversized envelope
        // after earlier recipients have already had copies appended.
        for &(index, grant) in &matches {
            let client = clients
                .get(index as usize)
                .ok_or_else(|| invalid_record("router returned absent client"))?;
            let target = self
                .sessions
                .get(client)
                .ok_or(EngineError::UnknownSession)?;
            if !self
                .access
                .authorize(&target.owner, Action::Receive, &message.topic)
            {
                continue;
            }
            let qos = message.qos.min(grant);
            if !target.online && qos == 0 {
                continue;
            }
            if target.queued.len() >= self.config.max_queued {
                return Err(EngineError::Capacity);
            }
            // An offline session retains the last runtime wire version. Reject
            // data it cannot drain, including volatile V5 persistent sessions.
            // Restored legacy sessions deliberately start as canonical V311.
            let packet = Message {
                qos,
                ..message.clone()
            }
            .packet((qos != 0).then_some(1), false);
            mqtt5::check_delivery_size(
                self.config.max_packet_size,
                target.version,
                &packet,
                &message.properties,
            )?;
        }
        for (index, grant) in matches {
            let client = clients
                .get(index as usize)
                .ok_or_else(|| invalid_record("router returned absent client"))?;
            let target = self
                .sessions
                .get_mut(client)
                .ok_or(EngineError::UnknownSession)?;
            if !self
                .access
                .authorize(&target.owner, Action::Receive, &message.topic)
            {
                continue;
            }
            let qos = message.qos.min(grant);
            if !target.online && qos == 0 {
                continue;
            }
            if target.queued.len() >= self.config.max_queued {
                return Err(EngineError::Capacity);
            }
            target.queued.push_back(Message {
                topic: message.topic.clone(),
                payload: message.payload.clone(),
                qos,
                retain: false,
                properties: message.properties.clone(),
            });
        }
        Ok(())
    }
    pub fn handle(&mut self, client_id: &str, packet: Packet) -> Result<Vec<Packet>, EngineError> {
        self.expire_sessions()?;
        self.handle_packet(client_id, packet, Vec::new())
    }
    fn handle_packet(
        &mut self,
        client_id: &str,
        packet: Packet,
        properties: Properties,
    ) -> Result<Vec<Packet>, EngineError> {
        self.require_online(client_id)?;
        let bytes =
            encode(&packet).map_err(|error| EngineError::InvalidPacket(error.to_string()))?;
        if bytes.len() > self.config.max_packet_size {
            return Err(EngineError::Capacity);
        }
        let owner = self
            .sessions
            .get(client_id)
            .ok_or(EngineError::UnknownSession)?
            .owner
            .clone();
        if let Packet::Publish { topic, .. } = &packet {
            self.require_access(&owner, Action::Publish, topic)?;
        }
        if let Packet::PubRel { packet_id } = &packet
            && let Some(pending) = self
                .sessions
                .get(client_id)
                .and_then(|session| session.incoming.pending.get(&identifier(*packet_id).ok()?))
            && let Packet::Publish { topic, .. } =
                decode_publish(pending, self.config.max_packet_size)?
        {
            self.require_access(&owner, Action::Publish, &topic)?;
        }
        self.reconcile_authorization()?;
        let mut replies = Vec::new();
        match packet {
            Packet::Subscribe { packet_id, filters } => {
                let session = self
                    .sessions
                    .get_mut(client_id)
                    .ok_or(EngineError::UnknownSession)?;
                let mut codes = Vec::new();
                for (filter, qos) in filters {
                    if !validate_filter(&filter)
                        || !self.access.authorize(&owner, Action::Subscribe, &filter)
                        || (!session.subscriptions.contains_key(&filter)
                            && session.subscriptions.len() >= self.config.max_subscriptions)
                    {
                        codes.push(128);
                        continue;
                    }
                    let retained_matches = self.retained.matching(&filter, qos);
                    for retained in &retained_matches {
                        if !self.access.authorize(&owner, Action::Connect, client_id)
                            || !self
                                .access
                                .authorize(&owner, Action::Receive, &retained.topic)
                        {
                            continue;
                        }
                        let properties = self
                            .retained_properties
                            .get(&retained.topic)
                            .cloned()
                            .unwrap_or_default();
                        let message = Message {
                            topic: retained.topic.clone(),
                            payload: retained.payload.clone(),
                            qos: retained.qos,
                            retain: true,
                            properties,
                        };
                        mqtt5::check_delivery_size(
                            self.config.max_packet_size,
                            session.version,
                            &message.packet((retained.qos != 0).then_some(1), false),
                            &message.properties,
                        )?;
                    }
                    session.subscriptions.insert(filter.clone(), qos);
                    for retained in retained_matches {
                        if !self.access.authorize(&owner, Action::Connect, client_id)
                            || !self
                                .access
                                .authorize(&owner, Action::Receive, &retained.topic)
                        {
                            continue;
                        }
                        if session.queued.len() >= self.config.max_queued {
                            return Err(EngineError::Capacity);
                        }
                        session.queued.push_back(Message {
                            topic: retained.topic.clone(),
                            payload: retained.payload,
                            qos: retained.qos,
                            retain: true,
                            properties: self
                                .retained_properties
                                .get(&retained.topic)
                                .cloned()
                                .unwrap_or_default(),
                        });
                    }
                    codes.push(qos);
                }
                replies.push(Packet::SubAck { packet_id, codes });
            }
            Packet::Unsubscribe { packet_id, filters } => {
                let session = self
                    .sessions
                    .get_mut(client_id)
                    .ok_or(EngineError::UnknownSession)?;
                for filter in filters {
                    if !validate_filter(&filter) {
                        return Err(EngineError::InvalidPacket(
                            "invalid unsubscribe filter".into(),
                        ));
                    }
                    session.subscriptions.remove(&filter);
                }
                replies.push(Packet::UnsubAck { packet_id });
            }
            Packet::Publish {
                topic,
                payload,
                qos,
                packet_id,
                dup,
                retain,
            } => {
                if qos == 2 {
                    let id = identifier(
                        packet_id.ok_or_else(|| invalid_record("missing incoming ID"))?,
                    )?;
                    let session = self
                        .sessions
                        .get_mut(client_id)
                        .ok_or(EngineError::UnknownSession)?;
                    let fresh = !session.incoming.pending.contains_key(&id);
                    session.incoming.publish(id, &bytes, dup)?;
                    if fresh {
                        session.incoming_properties.insert(id, properties);
                    }
                    replies.push(Packet::PubRec {
                        packet_id: id.get(),
                    });
                } else {
                    self.publish(Message {
                        topic,
                        payload,
                        qos,
                        retain,
                        properties,
                    })?;
                    if qos == 1 {
                        replies.push(Packet::PubAck {
                            packet_id: packet_id
                                .ok_or_else(|| invalid_record("missing QoS1 ID"))?,
                        });
                    }
                }
            }
            Packet::PubRel { packet_id } => {
                let properties = self
                    .sessions
                    .get_mut(client_id)
                    .ok_or(EngineError::UnknownSession)?
                    .incoming_properties
                    .remove(&identifier(packet_id)?)
                    .unwrap_or_default();
                let delivery = self
                    .sessions
                    .get_mut(client_id)
                    .ok_or(EngineError::UnknownSession)?
                    .incoming
                    .pubrel(identifier(packet_id)?)
                    .delivery;
                if let Some(bytes) = delivery {
                    let Packet::Publish {
                        topic,
                        payload,
                        qos,
                        retain,
                        ..
                    } = decode_publish(&bytes, self.config.max_packet_size)?
                    else {
                        return Err(invalid_record("invalid incoming message"));
                    };
                    self.publish(Message {
                        topic,
                        payload,
                        qos,
                        retain,
                        properties,
                    })?;
                }
                replies.push(Packet::PubComp { packet_id });
            }
            packet @ (Packet::PubAck { .. } | Packet::PubRec { .. } | Packet::PubComp { .. }) => {
                let (id, kind) = match packet {
                    Packet::PubAck { packet_id } => (packet_id, AckKind::PubAck),
                    Packet::PubRec { packet_id } => (packet_id, AckKind::PubRec),
                    Packet::PubComp { packet_id } => (packet_id, AckKind::PubComp),
                    _ => return Err(EngineError::InvalidPacket("invalid acknowledgement".into())),
                };
                let session = self
                    .sessions
                    .get_mut(client_id)
                    .ok_or(EngineError::UnknownSession)?;
                match session.outgoing.ack(identifier(id)?, kind)? {
                    AckAction::SendPubRel => {
                        session.replay.retain(|pending| pending.get() != id);
                        session.outgoing_properties.remove(&identifier(id)?);
                        replies.push(Packet::PubRel { packet_id: id });
                    }
                    AckAction::Complete => {
                        let id = identifier(id)?;
                        session.replay.retain(|pending| *pending != id);
                        session.sent.remove(&id);
                        session.outgoing_properties.remove(&id);
                    }
                    AckAction::Ignore => {}
                }
            }
            Packet::PingReq => replies.push(Packet::PingResp),
            _ => {
                return Err(EngineError::InvalidPacket(
                    "unexpected packet direction".into(),
                ));
            }
        }
        self.ensure_bounds()?;
        Ok(replies)
    }
    pub fn poll(&mut self, client_id: &str) -> Result<Option<Packet>, EngineError> {
        self.poll_with_changes(client_id)
            .map(|outcome| outcome.packet)
    }

    pub fn poll_with_changes(&mut self, client_id: &str) -> Result<PollOutcome, EngineError> {
        let outcome = self.poll_frame_with_changes(client_id)?;
        Ok(PollOutcome {
            packet: outcome.frame.map(|f| f.packet),
            changed: outcome.changed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> SessionConfig {
        SessionConfig {
            max_sessions: 8,
            max_retained: 8,
            max_subscriptions: 8,
            max_queued: 4,
            max_inflight: 4,
            max_packet_size: 4096,
            max_state_bytes: 64 * 1024,
        }
    }
    fn engine() -> SessionEngine {
        SessionEngine::new(config()).unwrap()
    }
    fn publish(topic: &str, payload: &[u8], qos: u8, id: u16) -> Packet {
        Packet::Publish {
            topic: topic.into(),
            payload: payload.to_vec(),
            qos,
            packet_id: if qos == 0 { None } else { Some(id) },
            dup: false,
            retain: false,
        }
    }
    fn event(engine: &mut SessionEngine, client: &str, packet: Packet) -> Vec<Packet> {
        let mut proposed = engine.clone();
        let replies = proposed.handle(client, packet).unwrap();
        proposed.snapshot().unwrap();
        *engine = proposed;
        replies
    }
    fn subscribe(engine: &mut SessionEngine, client: &str, topic: &str, qos: u8) {
        assert_eq!(
            event(
                engine,
                client,
                Packet::Subscribe {
                    packet_id: 1,
                    filters: vec![(topic.into(), qos)]
                }
            ),
            vec![Packet::SubAck {
                packet_id: 1,
                codes: vec![qos]
            }]
        );
    }
    fn incoming(
        engine: &mut SessionEngine,
        client: &str,
        topic: &str,
        payload: &[u8],
        qos: u8,
        id: u16,
    ) {
        let replies = event(engine, client, publish(topic, payload, qos, id));
        match qos {
            0 => assert!(replies.is_empty()),
            1 => assert_eq!(replies, vec![Packet::PubAck { packet_id: id }]),
            2 => {
                assert_eq!(replies, vec![Packet::PubRec { packet_id: id }]);
                assert_eq!(
                    event(engine, client, Packet::PubRel { packet_id: id }),
                    vec![Packet::PubComp { packet_id: id }]
                );
            }
            _ => unreachable!(),
        }
    }
    fn restored(engine: &SessionEngine) -> SessionEngine {
        SessionEngine::restore(config(), &engine.snapshot().unwrap()).unwrap()
    }
    fn outgoing_id(packet: &Packet) -> u16 {
        match packet {
            Packet::Publish {
                packet_id: Some(id),
                ..
            } => *id,
            _ => panic!("PUBLISH with ID required {packet:?}"),
        }
    }

    #[test]
    fn persistent_subscriptions_offline_queue_and_clean_reset() {
        let mut state = engine();
        assert!(!state.connect("sub", false).unwrap().session_present);
        subscribe(&mut state, "sub", "a/#", 2);
        state.disconnect("sub").unwrap();
        state.connect("pub", true).unwrap();
        incoming(&mut state, "pub", "a/b", b"offline", 1, 7);
        incoming(&mut state, "pub", "a/b", b"discard", 0, 0);
        let mut state = restored(&state);
        assert!(state.connect("sub", false).unwrap().session_present);
        assert!(
            matches!(state.poll("sub").unwrap(),Some(Packet::Publish{qos:1,dup:false,payload,..}) if payload==b"offline")
        );
        assert!(state.poll("sub").unwrap().is_none());
        assert!(!state.connect("sub", true).unwrap().session_present);
        assert!(state.poll("sub").unwrap().is_none());
        state.disconnect("sub").unwrap();
        assert!(
            !restored(&state)
                .connect("sub", false)
                .unwrap()
                .session_present
        );
    }

    #[test]
    fn outgoing_qos_replay_preserves_stages_ids_and_send_order() {
        let mut state = engine();
        state.connect("sub", false).unwrap();
        state.connect("pub", true).unwrap();
        subscribe(&mut state, "sub", "a", 2);
        incoming(&mut state, "pub", "a", b"qos1", 1, 9);
        let first = state.poll("sub").unwrap().unwrap();
        incoming(&mut state, "pub", "a", b"qos2", 2, 10);
        let second = state.poll("sub").unwrap().unwrap();
        incoming(&mut state, "pub", "a", b"released", 2, 11);
        let third = state.poll("sub").unwrap().unwrap();
        let (first_id, second_id, third_id) = (
            outgoing_id(&first),
            outgoing_id(&second),
            outgoing_id(&third),
        );
        event(
            &mut state,
            "sub",
            Packet::PubRec {
                packet_id: third_id,
            },
        );
        let mut state = restored(&state);
        assert!(state.connect("sub", false).unwrap().session_present);
        assert!(
            matches!(state.poll("sub").unwrap(),Some(Packet::Publish{qos:1,packet_id:Some(id),dup:true,..}) if id==first_id)
        );
        assert!(
            matches!(state.poll("sub").unwrap(),Some(Packet::Publish{qos:2,packet_id:Some(id),dup:true,..}) if id==second_id)
        );
        assert_eq!(
            state.poll("sub").unwrap(),
            Some(Packet::PubRel {
                packet_id: third_id
            })
        );
        assert!(state.poll("sub").unwrap().is_none());
        event(
            &mut state,
            "sub",
            Packet::PubAck {
                packet_id: first_id,
            },
        );
        event(
            &mut state,
            "sub",
            Packet::PubComp {
                packet_id: third_id,
            },
        );
        state.connect("pub", true).unwrap();
        incoming(&mut state, "pub", "a", b"fresh", 1, 12);
        let next = state.poll("sub").unwrap().unwrap();
        assert_ne!(outgoing_id(&next), second_id);
        assert!(matches!(next, Packet::Publish { dup: false, .. }));
    }

    #[test]
    fn incoming_qos2_survives_restart_and_commits_fanout_once() {
        let mut state = engine();
        state.connect("sub", false).unwrap();
        state.connect("pub", false).unwrap();
        subscribe(&mut state, "sub", "a", 1);
        assert_eq!(
            event(&mut state, "pub", publish("a", b"one", 2, 20)),
            vec![Packet::PubRec { packet_id: 20 }]
        );
        assert!(state.poll("sub").unwrap().is_none());
        let mut state = restored(&state);
        state.connect("pub", false).unwrap();
        assert_eq!(
            event(&mut state, "pub", Packet::PubRel { packet_id: 20 }),
            vec![Packet::PubComp { packet_id: 20 }]
        );
        let mut state = restored(&state);
        state.connect("pub", false).unwrap();
        state.connect("sub", false).unwrap();
        event(&mut state, "pub", Packet::PubRel { packet_id: 20 });
        let packet = state.poll("sub").unwrap().unwrap();
        assert!(matches!(packet,Packet::Publish{qos:1,ref payload,..} if payload==b"one"));
        assert!(state.poll("sub").unwrap().is_none());
        event(
            &mut state,
            "sub",
            Packet::PubAck {
                packet_id: outgoing_id(&packet),
            },
        );
        assert!(
            restored(&state)
                .connect("pub", false)
                .unwrap()
                .session_present
        );
    }

    #[test]
    fn failed_pubrel_proposal_preserves_source_and_all_recipient_outboxes() {
        let mut state = SessionEngine::new(SessionConfig {
            max_queued: 1,
            ..config()
        })
        .unwrap();
        state.connect("a", false).unwrap();
        state.connect("b", false).unwrap();
        state.connect("pub", false).unwrap();
        subscribe(&mut state, "a", "topic", 1);
        subscribe(&mut state, "b", "topic", 1);
        incoming(&mut state, "pub", "topic", b"fill", 1, 1);
        let packet = state.poll("a").unwrap().unwrap();
        event(
            &mut state,
            "a",
            Packet::PubAck {
                packet_id: outgoing_id(&packet),
            },
        );
        event(&mut state, "pub", publish("topic", b"transaction", 2, 2));
        let committed = state.snapshot().unwrap();
        let mut proposal = state.clone();
        assert!(
            proposal
                .handle("pub", Packet::PubRel { packet_id: 2 })
                .is_err()
        );
        assert_eq!(state.snapshot().unwrap(), committed);
        assert!(state.poll("a").unwrap().is_none());
        let fill = state.poll("b").unwrap().unwrap();
        event(
            &mut state,
            "b",
            Packet::PubAck {
                packet_id: outgoing_id(&fill),
            },
        );
        event(&mut state, "pub", Packet::PubRel { packet_id: 2 });
        assert!(
            matches!(state.poll("a").unwrap(),Some(Packet::Publish{ref payload,..}) if payload==b"transaction")
        );
        assert!(
            matches!(state.poll("b").unwrap(),Some(Packet::Publish{ref payload,..}) if payload==b"transaction")
        );
    }

    #[test]
    fn restore_rejects_schema_corruption_duplicate_ids_and_invalid_states() {
        let mut state = engine();
        state.connect("sub", false).unwrap();
        state.connect("pub", false).unwrap();
        subscribe(&mut state, "sub", "topic", 2);
        incoming(&mut state, "pub", "topic", b"message", 1, 3);
        state.poll("sub").unwrap();
        let value: serde_json::Value = serde_json::from_slice(&state.snapshot().unwrap()).unwrap();
        let mut unknown = value.clone();
        unknown["schema_version"] = 99.into();
        assert!(SessionEngine::restore(config(), &serde_json::to_vec(&unknown).unwrap()).is_err());
        for mutate in [0, 1, 2, 3, 4] {
            let mut value = value.clone();
            let sessions = value["sessions"].as_array_mut().unwrap();
            let target = sessions
                .iter_mut()
                .find(|session| session["client_id"] == "sub")
                .unwrap();
            match mutate {
                0 => target["outgoing"][0]["packet_id"] = 0.into(),
                1 => {
                    let duplicate = target["outgoing"][0].clone();
                    target["outgoing"].as_array_mut().unwrap().push(duplicate);
                }
                2 => target["subscriptions"][0][0] = "bad/#/filter".into(),
                3 => target["outgoing"][0]["stage"] = "unknown stage".into(),
                4 => target["unknown"] = true.into(),
                _ => unreachable!(),
            }
            assert!(
                SessionEngine::restore(config(), &serde_json::to_vec(&value).unwrap()).is_err(),
                "mutate={mutate}"
            );
        }
        assert!(SessionEngine::restore(config(), b"not JSON").is_err());
        assert!(
            SessionEngine::restore(
                SessionConfig {
                    max_state_bytes: 8,
                    ..config()
                },
                &state.snapshot().unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn total_state_limit_covers_clean_sessions_and_all_record_quotas() {
        let mut state = SessionEngine::new(SessionConfig {
            max_state_bytes: 500,
            ..config()
        })
        .unwrap();
        state.connect("clean-sub", true).unwrap();
        state.connect("pub", true).unwrap();
        subscribe(&mut state, "clean-sub", "topic", 1);
        assert!(
            state
                .handle("pub", publish("topic", &[1; 300], 1, 1))
                .is_err()
        );
        let mut state = SessionEngine::new(SessionConfig {
            max_sessions: 1,
            ..config()
        })
        .unwrap();
        state.connect("persistent", false).unwrap();
        state.disconnect("persistent").unwrap();
        assert!(state.connect("second", true).is_err());
        let mut state = engine();
        state.connect("pub", false).unwrap();
        for id in 1..=4 {
            event(&mut state, "pub", publish("a", b"pending", 2, id));
        }
        assert!(
            SessionEngine::restore(
                SessionConfig {
                    max_inflight: 3,
                    ..config()
                },
                &state.snapshot().unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn restore_rejects_subscription_exceeding_reduced_packet_quota() {
        let mut state = engine();
        state.connect("s", false).unwrap();
        subscribe(&mut state, "s", &"a".repeat(100), 1);
        let bytes = state.snapshot().unwrap();
        assert!(
            SessionEngine::restore(
                SessionConfig {
                    max_packet_size: 32,
                    ..config()
                },
                &bytes
            )
            .is_err()
        );
    }

    #[test]
    fn acknowledgement_before_replay_poll_does_not_leave_stale_cursor() {
        let mut state = engine();
        state.connect("sub", false).unwrap();
        state.connect("pub", true).unwrap();
        subscribe(&mut state, "sub", "a", 1);
        incoming(&mut state, "pub", "a", b"one", 1, 1);
        let id = outgoing_id(&state.poll("sub").unwrap().unwrap());
        let mut state = restored(&state);
        state.connect("sub", false).unwrap();
        event(&mut state, "sub", Packet::PubAck { packet_id: id });
        assert!(state.poll("sub").unwrap().is_none());
    }
}

#[cfg(test)]
mod phase2c_tests;

#[cfg(test)]
mod phase2d_tests;

#[cfg(test)]
mod phase2e_tests;
