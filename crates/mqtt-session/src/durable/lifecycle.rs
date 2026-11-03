//! Connection-bound Will lifecycle and capacity-safe deferred publication.
use super::{
    ConnectResult, ConnectionOptions, EngineError, Message, Session, SessionEngine, WillData,
    WillRecord, invalid_record,
};
use mqtt_auth::{Action, Principal};
#[cfg(test)]
use mqtt_protocol::Packet;
use mqtt_protocol::{Property, ProtocolVersion, Will};

impl SessionEngine {
    /// Earliest scheduled intent that still needs its first timer attempt.
    pub(super) fn next_will_deadline(&self) -> Option<u64> {
        self.wills
            .values()
            .filter(|will| will.pending && !will.timer_blocked)
            .filter_map(|will| will.due_at)
            .min()
    }

    pub(super) fn validate_will(&self, client: &str, will: &Will) -> Result<(), EngineError> {
        // Validate the stored publication, not a synthetic CONNECT enlarged by
        // an assigned key. The original inbound CONNECT was independently bounded.
        self.validate_client(client)?;
        self.validate_message(&Message {
            shared_subscription: None,
            retained_replay: false,
            expires_at: None,
            topic: will.topic.clone(),
            payload: will.payload.clone(),
            qos: will.qos,
            retain: will.retain,
            properties: Vec::new(),
        })
    }
    pub fn connect(
        &mut self,
        client_id: &str,
        clean_session: bool,
    ) -> Result<ConnectResult, EngineError> {
        self.connect_with_will(client_id, clean_session, None)
    }
    /// Proposed transition: the caller commits snapshot before CONNACK or takeover.
    pub fn connect_with_will(
        &mut self,
        client_id: &str,
        clean_session: bool,
        will: Option<Will>,
    ) -> Result<ConnectResult, EngineError> {
        self.connect_authenticated(client_id, clean_session, will, Principal::anonymous())
    }
    /// Authenticate externally first; ownership and Will authorization are checked
    /// here before any old connection, Will, or persistent session can be changed.
    pub fn connect_authenticated(
        &mut self,
        client_id: &str,
        clean_session: bool,
        will: Option<Will>,
        principal: Principal,
    ) -> Result<ConnectResult, EngineError> {
        self.connect_with_options(
            client_id,
            will,
            principal,
            ConnectionOptions {
                version: ProtocolVersion::V311,
                clean_start: clean_session,
                session_expiry_interval: if clean_session { 0 } else { u32::MAX },
                receive_maximum: u16::MAX,
                maximum_packet_size: u32::MAX,
                will_properties: Vec::new(),
            },
        )
    }
    pub fn connect_with_options(
        &mut self,
        client_id: &str,
        will: Option<Will>,
        principal: Principal,
        options: ConnectionOptions,
    ) -> Result<ConnectResult, EngineError> {
        if options.receive_maximum == 0 || options.maximum_packet_size == 0 {
            return Err(super::mqtt5::mqtt5_error(0x82));
        }
        if options.version == ProtocolVersion::V311 && !options.will_properties.is_empty() {
            return Err(super::mqtt5::mqtt5_error(0x82));
        }
        let mut will_properties = options.will_properties;
        if will.is_none() && !will_properties.is_empty() {
            return Err(super::mqtt5::mqtt5_error(0x82));
        }
        let mut delay_interval = 0;
        let mut delay_seen = false;
        for property in &will_properties {
            if let Property::WillDelayInterval(value) = property {
                if delay_seen {
                    return Err(super::mqtt5::mqtt5_error(0x82));
                }
                delay_seen = true;
                delay_interval = *value;
            }
        }
        will_properties.retain(|p| !matches!(p, Property::WillDelayInterval(_)));
        if let Some(will) = &will {
            let id = if will.qos == 0 { None } else { Some(1) };
            self.validate_properties(
                &Message {
                    shared_subscription: None,
                    retained_replay: false,
                    expires_at: None,
                    topic: will.topic.clone(),
                    payload: will.payload.clone(),
                    qos: will.qos,
                    retain: will.retain,
                    properties: Vec::new(),
                }
                .packet(id, false),
                &will_properties,
            )?;
        }
        if will
            .as_ref()
            .is_some_and(|will| super::authorization::reserved_system_topic(&will.topic))
        {
            return Err(EngineError::NotAuthorized);
        }
        self.expire_sessions()?;
        self.validate_client(client_id)?;
        Self::validate_owner(&principal.name).map_err(|_| EngineError::NotAuthorized)?;
        self.require_access(&principal, client_id, Action::Connect, client_id)?;
        if self
            .sessions
            .get(client_id)
            .is_some_and(|session| session.owner != principal)
            || self
                .wills
                .get(client_id)
                .is_some_and(|record| record.owner != principal.name)
        {
            return Err(EngineError::NotAuthorized);
        }
        if let Some(will) = &will {
            self.validate_will(client_id, will)?;
            self.require_access(&principal, client_id, Action::Publish, &will.topic)?;
        }
        if !options.clean_start
            && let Some(session) = self.sessions.get(client_id)
            && session.persistent
        {
            match self.check_resume_size(session, options.version) {
                Err(EngineError::Capacity) => return Err(super::mqtt5::mqtt5_error(0x83)),
                result => result?,
            }
        }
        let now = self.effective_now();
        let survives = !options.clean_start
            && self
                .sessions
                .get(client_id)
                .is_some_and(|session| session.persistent);
        let was_online = self
            .sessions
            .get(client_id)
            .is_some_and(|session| session.online);
        let cancellable = survives
            && self.wills.get(client_id).is_some_and(|record| {
                record.delay_interval > 0
                    && !record.timer_blocked
                    && if record.pending {
                        record.due_at.is_some_and(|deadline| deadline > now)
                    } else {
                        was_online
                    }
            });
        if cancellable {
            // Continuing the SAME session before the delay ends cancels the old
            // connection's Will, whether or not the replacement supplies a Will.
            self.wills.remove(client_id);
        } else if !survives
            && let Some(record) = self.wills.get_mut(client_id)
            && (record.pending || was_online)
        {
            // Clean Start or an expiry-zero takeover ends the original session.
            // A prior already-due intent keeps its earlier absolute deadline.
            record.pending = true;
            record.due_at = Some(record.due_at.map_or(now, |deadline| deadline.min(now)));
            record.timer_blocked = false;
        }
        if !survives {
            // Session end removes the old subscriptions and outboxes before
            // forced Will fanout. Discarded self-delivery cannot consume quota
            // or defer publication to genuine external recipients.
            self.sessions.remove(client_id);
        }
        if was_online && survives {
            self.disconnect_with_reason(client_id, false)?;
        } else {
            self.retry_wills()?;
        }
        // An older pending intent must survive a connection without a new Will:
        // the source may need to resume and drain its own full outbox first.
        // Never overwrite it, or bypass startup recovery of an active record.
        if self
            .wills
            .get(client_id)
            .is_some_and(|record| !record.pending || will.is_some())
        {
            return Err(EngineError::Capacity);
        }
        let present = !options.clean_start
            && self
                .sessions
                .get(client_id)
                .is_some_and(|session| session.persistent);
        if !present {
            if !self.sessions.contains_key(client_id)
                && self.sessions.len() >= self.config.max_sessions
            {
                return Err(EngineError::Capacity);
            }
            self.sessions.insert(
                client_id.into(),
                Box::new(Session::new(
                    self.config,
                    options.session_expiry_interval != 0,
                    principal.clone(),
                )),
            );
        }
        if let Some(will) = will {
            if self.wills.len() >= self.config.max_sessions {
                return Err(EngineError::Capacity);
            }
            self.wills.insert(
                client_id.into(),
                WillRecord {
                    client_id: client_id.into(),
                    owner: principal.name.clone(),
                    pending: false,
                    timer_blocked: false,
                    delay_interval,
                    due_at: None,
                    will: WillData {
                        properties: will_properties,
                        ..WillData::from(will)
                    },
                },
            );
        }
        let session = self
            .sessions
            .get_mut(client_id)
            .ok_or(EngineError::UnknownSession)?;
        session.online = true;
        session.version = options.version;
        session.expiry = options.session_expiry_interval;
        session.persistent = session.expiry != 0;
        session.expires_at = None;
        session.receive_maximum = options.receive_maximum;
        session.maximum_packet_size = if options.version == ProtocolVersion::V5 {
            options.maximum_packet_size
        } else {
            u32::MAX
        };
        session.sent.clear();
        session.replay = session.outgoing.order.clone();
        self.ensure_bounds()?;
        Ok(ConnectResult {
            session_present: present,
        })
    }
    pub fn disconnect(&mut self, client_id: &str) -> Result<(), EngineError> {
        self.disconnect_with_reason(client_id, true)
    }
    /// Graceful close clears the active Will; unexpected close retains a pending
    /// record if capacity temporarily prevents complete atomic fanout.
    pub fn disconnect_with_reason(
        &mut self,
        client_id: &str,
        graceful: bool,
    ) -> Result<(), EngineError> {
        self.disconnect_with_options(client_id, graceful, None)
    }
    pub fn disconnect_with_options(
        &mut self,
        client_id: &str,
        graceful: bool,
        expiry_override: Option<u32>,
    ) -> Result<(), EngineError> {
        if let Some(expiry) = expiry_override {
            let session = self
                .sessions
                .get_mut(client_id)
                .ok_or(EngineError::UnknownSession)?;
            if session.version != ProtocolVersion::V5 || (session.expiry == 0 && expiry != 0) {
                return Err(super::mqtt5::mqtt5_error(0x82));
            }
            session.expiry = expiry;
            session.persistent = expiry != 0;
        }
        let now = self.effective_now();
        let persistent = self
            .sessions
            .get(client_id)
            .ok_or(EngineError::UnknownSession)?
            .persistent;
        if persistent {
            let session = self
                .sessions
                .get_mut(client_id)
                .ok_or(EngineError::UnknownSession)?;
            session.online = false;
            session.expires_at = if session.expiry == u32::MAX {
                None
            } else {
                Some(now.saturating_add(u64::from(session.expiry) * 1000))
            };
            session.sent.clear();
            session.replay.clear();
            session.queued.retain(|message| message.qos != 0);
        } else {
            self.sessions.remove(client_id);
        }
        if graceful {
            // Do not clear an older already-deferred Will through a repeated close.
            if self.wills.get(client_id).is_some_and(|will| !will.pending) {
                self.wills.remove(client_id);
            }
        } else if let Some(record) = self.wills.get_mut(client_id)
            && !record.pending
        {
            let session_end = self
                .sessions
                .get(client_id)
                .map_or(Some(now), |session| session.expires_at);
            let delayed = now.saturating_add(u64::from(record.delay_interval) * 1000);
            record.pending = true;
            record.due_at = Some(session_end.map_or(delayed, |deadline| delayed.min(deadline)));
            record.timer_blocked = false;
        }
        self.prune_shared_state();
        if self.access.reconciles_stored_authorization() {
            self.retry_wills()?;
        }
        self.ensure_bounds()
    }
    /// Called once on startup after restore, before the startup commit and ready.
    pub fn recover_wills(&mut self) -> Result<(), EngineError> {
        self.anchor_recovered_wills();
        self.retry_wills()?;
        self.ensure_bounds()
    }
    /// Anchor active-at-crash intents once before asynchronous preparation.
    pub fn anchor_recovered_wills(&mut self) {
        let now = self.effective_now();
        for (client_id, record) in &mut self.wills {
            if !record.pending {
                // The crash instant is unknown. Only active-at-crash records get
                // a new conservative anchor; known schedules never slide.
                let session_end = self
                    .sessions
                    .get(client_id)
                    .map_or(Some(now), |session| session.expires_at);
                let delayed = now.saturating_add(u64::from(record.delay_interval) * 1000);
                record.pending = true;
                record.due_at = Some(session_end.map_or(delayed, |deadline| delayed.min(deadline)));
                record.timer_blocked = false;
            }
        }
    }
    /// Tries each pending Will on a separate bounded clone. A full recipient or
    /// retained store defers it unchanged; any other error is not swallowed.
    pub fn retry_wills(&mut self) -> Result<bool, EngineError> {
        let mut progressed = self.expire_sessions()?;
        // Legacy snapshots can contain Wills created before `$SYS` became
        // broker-owned. Retire their durable intent instead of forging events.
        let before = self.wills.len();
        self.wills
            .retain(|_, record| !super::authorization::reserved_system_topic(&record.will.topic));
        progressed |= self.wills.len() != before;
        progressed |= self.prune_wills();
        let pending: Vec<String> = self
            .wills
            .iter()
            .filter_map(|(id, record)| {
                (record.pending
                    && record
                        .due_at
                        .is_some_and(|deadline| deadline <= self.effective_now()))
                .then_some(id.clone())
            })
            .collect();
        for id in pending {
            if !self.access.reconciles_stored_authorization()
                && let Some(record) = self.wills.get(&id)
                && (!self.access.authorize_for_client(
                    &Principal {
                        name: record.owner.clone(),
                    },
                    &id,
                    Action::Connect,
                    &id,
                ) || !self.access.authorize_for_client(
                    &Principal {
                        name: record.owner.clone(),
                    },
                    &id,
                    Action::Publish,
                    &record.will.topic,
                ))
            {
                self.wills.remove(&id);
                progressed = true;
                continue;
            }
            let mut proposal = self.clone();
            let record = proposal
                .wills
                .remove(&id)
                .ok_or_else(|| invalid_record("pending Will disappeared"))?;
            let publication = Message {
                shared_subscription: None,
                retained_replay: false,
                expires_at: super::temporal::expiry_deadline(
                    &record.will.properties,
                    self.effective_now(),
                ),
                topic: record.will.topic,
                payload: record.will.payload,
                qos: record.will.qos,
                retain: record.will.retain,
                properties: record.will.properties,
            };
            let result = proposal
                .publish_from(publication, Some(&id))
                .and_then(|()| proposal.ensure_bounds());
            match result {
                Ok(()) => {
                    *self = proposal;
                    progressed = true;
                }
                Err(EngineError::Capacity) => {
                    if let Some(will) = self.wills.get_mut(&id)
                        && !will.timer_blocked
                    {
                        will.timer_blocked = true;
                        progressed = true;
                    }
                }
                Err(EngineError::Retained(error)) if super::is_retained_capacity(&error) => {
                    if let Some(will) = self.wills.get_mut(&id)
                        && !will.timer_blocked
                    {
                        will.timer_blocked = true;
                        progressed = true;
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Ok(progressed)
    }
}

#[cfg(test)]
#[path = "will_delay_tests.rs"]
mod will_delay_tests;
