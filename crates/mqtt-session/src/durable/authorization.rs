//! Authorization belongs to the same proposal as routing and outbox mutations.
use super::{EngineError, SessionEngine, decode_publish, invalid_record};
use crate::outgoing::OutgoingState;
use crate::profiling::{self, Operation};
use mqtt_auth::{Action, Principal};
use mqtt_protocol::Packet;

impl SessionEngine {
    /// Whether an accepted inbound QoS2 exchange still awaits PUBREL.
    /// This is runtime state inspection and does not authorize a new operation.
    pub fn has_incoming_exchange(&self, client_id: &str, packet_id: u16) -> bool {
        super::identifier(packet_id).ok().is_some_and(|id| {
            self.sessions
                .get(client_id)
                .is_some_and(|session| session.incoming.pending.contains_key(&id))
        })
    }

    /// Trusted stored principal for preparing an individual recipient ticket.
    pub fn principal(&self, client_id: &str) -> Option<Principal> {
        self.sessions
            .get(client_id)
            .map(|session| session.owner.clone())
    }

    /// Freshly authorize just the next application delivery. Unknown probe
    /// decisions are optimistic and the enclosing proposal must be discarded.
    pub(super) fn point_delivery_denied(&mut self, client_id: &str) -> Result<bool, EngineError> {
        if self.access.reconciles_stored_authorization() {
            return Ok(false);
        }
        let session = self
            .sessions
            .get(client_id)
            .ok_or(EngineError::UnknownSession)?;
        let quota = session.sent.len() < usize::from(session.receive_maximum);
        let replay = if quota {
            (!session.replay.is_empty()).then_some(0)
        } else {
            session.replay.iter().position(|id| {
                matches!(
                    session.outgoing.pending.get(id),
                    Some(OutgoingState::Released)
                )
            })
        };
        let candidate = if let Some(position) = replay {
            let id = *session
                .replay
                .get(position)
                .ok_or_else(|| invalid_record("replay position disappeared"))?;
            match session.outgoing.pending.get(&id) {
                Some(OutgoingState::Released) => None,
                Some(
                    OutgoingState::AwaitingAck { _message }
                    | OutgoingState::AwaitingReceipt { _message },
                ) => {
                    let Packet::Publish { topic, .. } =
                        decode_publish(_message, self.config.max_packet_size)?
                    else {
                        return Err(invalid_record("outgoing state is not PUBLISH"));
                    };
                    Some((Some(id), topic))
                }
                None => return Err(invalid_record("replay references absent exchange")),
            }
        } else if quota && !session.outgoing.is_full() {
            session
                .queued
                .front()
                .map(|message| (None, message.topic.clone()))
        } else {
            None
        };
        let Some((id, topic)) = candidate else {
            return Ok(false);
        };
        let owner = session.owner.clone();
        if self
            .access
            .authorize_for_client(&owner, client_id, Action::Connect, client_id)
            && self
                .access
                .authorize_for_client(&owner, client_id, Action::Receive, &topic)
        {
            return Ok(false);
        }
        let session = self
            .sessions
            .get_mut(client_id)
            .ok_or(EngineError::UnknownSession)?;
        match id {
            Some(id) => session.retire_outgoing(id),
            None => {
                session.queued.pop_front();
            }
        }
        self.ensure_bounds()?;
        Ok(true)
    }

    /// Bind a synchronous speculative policy to a proposal without changing rows.
    pub fn set_access_control(&mut self, access: std::sync::Arc<dyn mqtt_auth::AccessControl>) {
        self.access = access;
    }

    pub(super) fn validate_owner(owner: &str) -> Result<(), EngineError> {
        if owner.is_empty() || owner.len() > 65535 || owner.contains('\0') {
            return Err(invalid_record("invalid principal identity"));
        }
        Ok(())
    }

    pub(super) fn require_access(
        &self,
        principal: &Principal,
        client_id: &str,
        action: Action,
        resource: &str,
    ) -> Result<(), EngineError> {
        if self
            .access
            .authorize_for_client(principal, client_id, action, resource)
        {
            Ok(())
        } else {
            Err(EngineError::NotAuthorized)
        }
    }

    pub(super) fn prune_wills(&mut self) -> bool {
        if !self.access.reconciles_stored_authorization() {
            return false;
        }
        let access = &self.access;
        let before = self.wills.len();
        self.wills.retain(|client_id, record| {
            let owner = Principal {
                name: record.owner.clone(),
            };
            access.authorize_for_client(&owner, client_id, Action::Connect, client_id)
                && access.authorize_for_client(
                    &owner,
                    client_id,
                    Action::Publish,
                    &record.will.topic,
                )
        });
        self.wills.len() != before
    }

    /// Drop application data revoked by the current policy. Caller must commit
    /// even if this produces no packet. Incoming QoS2 retains its original topic
    /// for a fresh Publish check at PUBREL; Released outgoing states contain no
    /// payload and may finish the already accepted PUBREL/PUBCOMP handshake.
    pub fn reconcile_authorization(&mut self) -> Result<bool, EngineError> {
        if !self.access.reconciles_stored_authorization() {
            return Ok(false);
        }
        let _span = profiling::span(Operation::Authorization);
        let mut changed = self.prune_wills();
        let access = &self.access;
        for (client_id, session) in &mut self.sessions {
            let connected =
                access.authorize_for_client(&session.owner, client_id, Action::Connect, client_id);
            let subscriptions_before = session.subscriptions.len();
            session.subscriptions.retain(|filter, _| {
                connected
                    && access.authorize_for_client(
                        &session.owner,
                        client_id,
                        Action::Subscribe,
                        super::shared::acl_filter(filter),
                    )
            });
            let queued_before = session.queued.len();
            session.queued.retain(|message| {
                connected
                    && access.authorize_for_client(
                        &session.owner,
                        client_id,
                        Action::Receive,
                        &message.topic,
                    )
            });
            changed |= subscriptions_before != session.subscriptions.len()
                || queued_before != session.queued.len();
            let mut revoked = Vec::new();
            for (id, state) in &session.outgoing.pending {
                let bytes = match state {
                    OutgoingState::AwaitingAck { _message }
                    | OutgoingState::AwaitingReceipt { _message } => _message,
                    OutgoingState::Released => continue,
                };
                let Packet::Publish { topic, .. } =
                    decode_publish(bytes, self.config.max_packet_size)?
                else {
                    return Err(invalid_record("outgoing state is not a publication"));
                };
                if !connected
                    || !access.authorize_for_client(
                        &session.owner,
                        client_id,
                        Action::Receive,
                        &topic,
                    )
                {
                    revoked.push(*id);
                }
            }
            for id in revoked {
                session.outgoing.pending.remove(&id);
                session.outgoing.order.retain(|pending| *pending != id);
                session.replay.retain(|pending| *pending != id);
                session.outgoing.identifiers.release(id);
                session.outgoing_properties.remove(&id);
                session.outgoing_expiry.remove(&id);
                session.outgoing_started.remove(&id);
                session.outgoing_retained_replay.remove(&id);
                session.outgoing_shared.remove(&id);
                session.delivery_tickets.remove(&id);
                session.sent.remove(&id);
                changed = true;
            }
        }
        changed |= self.prune_shared_state();
        if changed {
            self.ensure_bounds()?;
        }
        Ok(changed)
    }
}

#[cfg(test)]
#[path = "point_authorization_tests.rs"]
mod tests;
