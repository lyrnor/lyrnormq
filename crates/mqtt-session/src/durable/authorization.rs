//! Authorization belongs to the same proposal as routing and outbox mutations.
use super::{EngineError, SessionEngine, decode_publish, invalid_record};
use crate::outgoing::OutgoingState;
use crate::profiling::{self, Operation};
use mqtt_auth::{Action, Principal};
use mqtt_protocol::Packet;

impl SessionEngine {
    pub(super) fn validate_owner(owner: &str) -> Result<(), EngineError> {
        if owner.is_empty() || owner.len() > 65535 || owner.contains('\0') {
            return Err(invalid_record("invalid principal identity"));
        }
        Ok(())
    }

    pub(super) fn require_access(
        &self,
        principal: &Principal,
        action: Action,
        resource: &str,
    ) -> Result<(), EngineError> {
        if self.access.authorize(principal, action, resource) {
            Ok(())
        } else {
            Err(EngineError::NotAuthorized)
        }
    }

    pub(super) fn prune_wills(&mut self) -> bool {
        let access = &self.access;
        let before = self.wills.len();
        self.wills.retain(|client_id, record| {
            let owner = Principal {
                name: record.owner.clone(),
            };
            access.authorize(&owner, Action::Connect, client_id)
                && access.authorize(&owner, Action::Publish, &record.will.topic)
        });
        self.wills.len() != before
    }

    /// Drop application data revoked by the current policy. Caller must commit
    /// even if this produces no packet. Incoming QoS2 retains its original topic
    /// for a fresh Publish check at PUBREL; Released outgoing states contain no
    /// payload and may finish the already accepted PUBREL/PUBCOMP handshake.
    pub fn reconcile_authorization(&mut self) -> Result<bool, EngineError> {
        let _span = profiling::span(Operation::Authorization);
        let mut changed = self.prune_wills();
        let access = &self.access;
        for (client_id, session) in &mut self.sessions {
            let connected = access.authorize(&session.owner, Action::Connect, client_id);
            let subscriptions_before = session.subscriptions.len();
            session.subscriptions.retain(|filter, _| {
                connected
                    && access.authorize(
                        &session.owner,
                        Action::Subscribe,
                        super::shared::acl_filter(filter),
                    )
            });
            let queued_before = session.queued.len();
            session.queued.retain(|message| {
                connected && access.authorize(&session.owner, Action::Receive, &message.topic)
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
                if !connected || !access.authorize(&session.owner, Action::Receive, &topic) {
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
        self.ensure_bounds()?;
        Ok(changed)
    }
}
