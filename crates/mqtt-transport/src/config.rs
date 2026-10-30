//! Broker resource configuration.
use mqtt_auth::{AccessControl, AllowAnonymous};
use std::{sync::Arc, time::Duration};

/// Protocol family accepted by this listener. This is connection-local policy;
/// durable sessions can still be resumed by another listener/profile.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ProtocolMode {
    /// Accept levels4/5 and retain the legacy unsupported-level response.
    #[default]
    Both,
    /// Accept only MQTT3.1.1 and refuse other levels using legacy CONNACK1.
    V311,
    /// Accept only MQTT5 and refuse other levels using MQTT5 CONNACK84.
    V5,
}
impl ProtocolMode {
    pub(crate) fn accepts(self, version: mqtt_protocol::ProtocolVersion) -> bool {
        matches!(
            (self, version),
            (Self::Both, _)
                | (Self::V311, mqtt_protocol::ProtocolVersion::V311)
                | (Self::V5, mqtt_protocol::ProtocolVersion::V5)
        )
    }
    pub(crate) fn refusal(self) -> (mqtt_protocol::ProtocolVersion, u8) {
        match self {
            Self::Both | Self::V311 => (mqtt_protocol::ProtocolVersion::V311, 1),
            Self::V5 => (mqtt_protocol::ProtocolVersion::V5, 0x84),
        }
    }
}

/// Limits apply per connection except for `max_connections`. Queue capacity counts
/// packets; maximum queued payload memory is bounded by capacity × packet size.
/// Each direction separately retains at most `max_inflight` QoS exchanges, with
/// each encoded message limited by `max_packet_size`. State is volatile and is
/// discarded when a clean session ends. Global retained records survive client
/// disconnects even without SQLite; without storage_path all state is lost on
/// process termination. SQLite retains persistent sessions and global records.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// Optional nonblocking best-effort publication side channel.
    pub publication_observer: Option<Arc<dyn crate::PublicationObserver>>,
    /// Opt-in read-only state observation; no additional broker is created.
    pub management: Option<crate::management::ManagementHandle>,
    /// Explicit listener negotiation profile; never persisted with a session.
    pub protocol_mode: ProtocolMode,
    /// Immutable, bounded hooks executed on the serialized blocking worker.
    pub access_control: Arc<dyn AccessControl>,
    /// Optional connection-local enhanced authentication provider.
    pub enhanced_auth: Option<Arc<dyn mqtt_auth::EnhancedAuthenticator>>,
    pub storage_path: Option<std::path::PathBuf>,
    pub max_sessions: usize,
    pub max_retained: usize,
    pub max_state_bytes: usize,
    pub max_packet_size: usize,
    /// Advertised inbound Topic Alias maximum and local outbound entry budget.
    pub topic_alias_maximum: u16,
    pub max_connections: usize,
    /// Connections still awaiting a fully flushed successful CONNACK.
    pub max_handshakes: usize,
    /// Outstanding blocking credential/provider jobs, including cancelled callers.
    pub max_auth_tasks: usize,
    /// Global CONNECT/AUTH token refill; zero disables only rate throttling.
    pub auth_rate_per_second: u32,
    pub auth_burst: u32,
    pub outbound_capacity: usize,
    pub max_subscriptions: usize,
    /// Inbound QoS1/2 Receive Maximum and independent durable QoS windows.
    /// MQTT5 peer Receive Maximum further limits outbound PUBLISH per connection.
    pub max_inflight: usize,
    pub handshake_timeout: Duration,
    pub write_timeout: Duration,
}
impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            publication_observer: None,
            management: None,
            protocol_mode: ProtocolMode::Both,
            access_control: Arc::new(AllowAnonymous),
            enhanced_auth: None,
            storage_path: None,
            max_sessions: 1024,
            max_retained: 10_000,
            max_state_bytes: 64 * 1024 * 1024,
            max_packet_size: 1024 * 1024,
            topic_alias_maximum: 32,
            max_connections: 1024,
            max_handshakes: 128,
            max_auth_tasks: 4,
            auth_rate_per_second: 1000,
            auth_burst: 1000,
            outbound_capacity: 64,
            max_subscriptions: 256,
            max_inflight: 32,
            handshake_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(5),
        }
    }
}
