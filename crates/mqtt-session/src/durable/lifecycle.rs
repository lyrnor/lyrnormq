//! Connection-bound Will lifecycle and capacity-safe deferred publication.
use super::{
    ConnectResult, EngineError, Message, Session, SessionEngine, WillData, WillRecord,
    invalid_record,
};
use mqtt_protocol::{Packet, Will, encode};

impl SessionEngine {
    pub(super) fn validate_will(&self, client: &str, will: &Will) -> Result<(), EngineError> {
        let bytes = encode(&Packet::Connect {
            client_id: client.into(),
            clean_session: true,
            keep_alive: 0,
            will: Some(will.clone()),
        })
        .map_err(|error| invalid_record(error.to_string()))?;
        if bytes.len() > self.config.max_packet_size {
            return Err(EngineError::Capacity);
        }
        self.validate_message(&Message {
            topic: will.topic.clone(),
            payload: will.payload.clone(),
            qos: will.qos,
            retain: will.retain,
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
        self.validate_client(client_id)?;
        if let Some(will) = &will {
            self.validate_will(client_id, will)?;
        }
        if self
            .sessions
            .get(client_id)
            .is_some_and(|session| session.online)
        {
            self.disconnect_with_reason(client_id, false)?;
        } else {
            self.retry_wills()?;
        }
        if self.wills.contains_key(client_id) {
            return Err(EngineError::Capacity);
        }
        let present = !clean_session
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
            self.sessions
                .insert(client_id.into(), Session::new(self.config, !clean_session));
        }
        if let Some(will) = will {
            if self.wills.len() >= self.config.max_sessions {
                return Err(EngineError::Capacity);
            }
            self.wills.insert(
                client_id.into(),
                WillRecord {
                    client_id: client_id.into(),
                    pending: false,
                    will: WillData::from(will),
                },
            );
        }
        let session = self
            .sessions
            .get_mut(client_id)
            .ok_or(EngineError::UnknownSession)?;
        session.online = true;
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
        } else if let Some(record) = self.wills.get_mut(client_id) {
            record.pending = true;
        }
        self.retry_wills()?;
        self.ensure_bounds()
    }
    /// Called once on startup after restore, before the startup commit and ready.
    pub fn recover_wills(&mut self) -> Result<(), EngineError> {
        for record in self.wills.values_mut() {
            record.pending = true;
        }
        self.retry_wills()?;
        self.ensure_bounds()
    }
    /// Tries each pending Will on a separate bounded clone. A full recipient or
    /// retained store defers it unchanged; any other error is not swallowed.
    pub fn retry_wills(&mut self) -> Result<bool, EngineError> {
        let pending: Vec<String> = self
            .wills
            .iter()
            .filter_map(|(id, record)| record.pending.then_some(id.clone()))
            .collect();
        let mut progressed = false;
        for id in pending {
            let mut proposal = self.clone();
            let record = proposal
                .wills
                .remove(&id)
                .ok_or_else(|| invalid_record("pending Will disappeared"))?;
            let publication = Message {
                topic: record.will.topic,
                payload: record.will.payload,
                qos: record.will.qos,
                retain: record.will.retain,
            };
            let result = proposal
                .publish(publication)
                .and_then(|()| proposal.ensure_bounds());
            match result {
                Ok(()) => {
                    *self = proposal;
                    progressed = true;
                }
                Err(EngineError::Capacity) => {}
                Err(EngineError::Retained(error)) if super::is_retained_capacity(&error) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(progressed)
    }
}
