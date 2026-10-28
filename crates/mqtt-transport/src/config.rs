//! Broker resource configuration.
use mqtt_auth::{AccessControl, AllowAnonymous};
use std::{sync::Arc, time::Duration};

/// Limits apply per connection except for `max_connections`. Queue capacity counts
/// packets; maximum queued payload memory is bounded by capacity × packet size.
/// Each direction separately retains at most `max_inflight` QoS exchanges, with
/// each encoded message limited by `max_packet_size`. State is volatile and is
/// discarded when a clean session ends. Global retained records survive client
/// disconnects even without SQLite; without storage_path all state is lost on
/// process termination. SQLite retains persistent sessions and global records.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
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
            access_control: Arc::new(AllowAnonymous),
            enhanced_auth: None,
            storage_path: None,
            max_sessions: 1024,
            max_retained: 10_000,
            max_state_bytes: 64 * 1024 * 1024,
            max_packet_size: 1024 * 1024,
            topic_alias_maximum: 32,
            max_connections: 1024,
            outbound_capacity: 64,
            max_subscriptions: 256,
            max_inflight: 32,
            handshake_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(5),
        }
    }
}
