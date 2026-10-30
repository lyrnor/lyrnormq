//! Pure projections, excluding payloads, credentials and durable encoding.
use super::SessionEngine;
use mqtt_protocol::ProtocolVersion;

#[derive(Clone, Debug)]
pub struct SessionMetadata {
    pub client_id: String,
    pub online: bool,
    pub protocol: &'static str,
    pub principal: String,
    pub persistent: bool,
    pub subscriptions: usize,
    pub queued: usize,
    pub inflight: usize,
    pub receive_maximum: u16,
    pub session_expiry_interval: u32,
}
#[derive(Clone, Debug)]
pub struct SubscriptionMetadata {
    pub filter: String,
    pub qos: u8,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: u8,
    pub identifier: Option<u32>,
}
impl SessionEngine {
    /// Installed model metadata; this method neither expires nor mutates state.
    pub fn session_metadata(&self, client: &str) -> Option<SessionMetadata> {
        let session = self.sessions.get(client)?;
        Some(SessionMetadata {
            client_id: client.into(),
            online: session.online,
            protocol: match session.version {
                ProtocolVersion::V311 => "3.1.1",
                ProtocolVersion::V5 => "5.0",
            },
            principal: session.owner.name.clone(),
            persistent: session.persistent,
            subscriptions: session.subscriptions.len(),
            queued: session.queued.len(),
            inflight: session.outgoing.inflight_count(),
            receive_maximum: session.receive_maximum,
            session_expiry_interval: session.expiry,
        })
    }
    /// Ordered subscription metadata only; no queued or retained payload copies.
    pub fn subscription_metadata(&self, client: &str) -> Vec<SubscriptionMetadata> {
        self.sessions
            .get(client)
            .map(|s| {
                s.subscriptions
                    .values()
                    .map(|s| SubscriptionMetadata {
                        filter: s.filter.clone(),
                        qos: s.qos,
                        no_local: s.no_local,
                        retain_as_published: s.retain_as_published,
                        retain_handling: s.retain_handling,
                        identifier: s.identifier,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}
