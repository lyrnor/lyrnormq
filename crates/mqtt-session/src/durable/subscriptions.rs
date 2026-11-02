//! Each SUBSCRIBE entry is applied sequentially within its enclosing proposal.
use super::*;
impl SessionEngine {
    pub(super) fn subscribe_entry(
        &mut self,
        client: &str,
        subscription: SubscriptionRecord,
    ) -> Result<u8, EngineError> {
        let session = self
            .sessions
            .get(client)
            .ok_or(EngineError::UnknownSession)?;
        if !validate_subscription_filter(&subscription.filter) {
            return Ok(0x8f);
        }
        if !self.access.authorize_for_client(
            &session.owner,
            client,
            Action::Subscribe,
            super::shared::acl_filter(&subscription.filter),
        ) {
            return Ok(0x87);
        }
        let existed = session.subscriptions.contains_key(&subscription.filter);
        if !existed && session.subscriptions.len() >= self.config.max_subscriptions {
            return Ok(0x97);
        }
        subscription.validate()?;
        let owner = session.owner.clone();
        let version = session.version;
        let qos = subscription.qos;
        let replay = split_shared_filter(&subscription.filter).is_none()
            && (subscription.retain_handling == 0
                || (subscription.retain_handling == 1 && !existed));
        let mut copies = Vec::new();
        if replay {
            for retained in self.retained.matching(&subscription.filter, qos) {
                if !self
                    .access
                    .authorize_for_client(&owner, client, Action::Connect, client)
                    || !self.access.authorize_for_client(
                        &owner,
                        client,
                        Action::Receive,
                        &retained.topic,
                    )
                {
                    continue;
                }
                let mut properties = self
                    .retained_properties
                    .get(&retained.topic)
                    .cloned()
                    .unwrap_or_default();
                if let Some(id) = subscription.identifier {
                    properties.push(Property::SubscriptionIdentifier(id));
                }
                let message = Message {
                    topic: retained.topic.clone(),
                    payload: retained.payload,
                    qos: retained.qos,
                    retain: true,
                    shared_subscription: None,
                    retained_replay: true,
                    expires_at: self.retained_expiry.get(&retained.topic).copied(),
                    properties,
                };
                mqtt5::check_delivery_size(
                    self.config.max_packet_size,
                    version,
                    &message.packet((message.qos != 0).then_some(1), false),
                    &message.properties,
                )?;
                copies.push(message);
            }
        }
        if split_shared_filter(&subscription.filter).is_some() {
            self.ensure_shared_epoch(&subscription.filter)?;
        }
        let now = self.effective_now();
        let session = self
            .sessions
            .get_mut(client)
            .ok_or(EngineError::UnknownSession)?;
        mqtt5::discard_oversized(session, now, self.config.max_packet_size)?;
        for message in copies {
            if mqtt5::exceeds_peer_limit(
                session,
                &message.packet((message.qos != 0).then_some(1), false),
                &message.properties,
                message.expires_at,
                now,
            )? {
                continue;
            }
            if session.queued.len() >= self.config.max_queued {
                return Err(EngineError::Capacity);
            }
            session.queued.push_back(message);
        }
        session
            .subscriptions
            .insert(subscription.filter.clone(), subscription);
        Ok(qos)
    }
}
