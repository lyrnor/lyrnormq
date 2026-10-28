//! Broker resource configuration.
use std::time::Duration;

/// Limits apply per connection except for `max_connections`. Queue capacity counts
/// packets; maximum queued payload memory is bounded by capacity × packet size.
/// Each direction separately retains at most `max_inflight` QoS exchanges, with
/// each encoded message limited by `max_packet_size`. State is volatile and is
/// discarded on disconnect unless storage_path enables the SQLite-backed worker.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub storage_path: Option<std::path::PathBuf>,
    pub max_sessions: usize,
    pub max_state_bytes: usize,
    pub max_packet_size: usize,
    pub max_connections: usize,
    pub outbound_capacity: usize,
    pub max_subscriptions: usize,
    /// Independent limits for inbound QoS2 and outbound QoS1/2 exchanges.
    pub max_inflight: usize,
    pub handshake_timeout: Duration,
    pub write_timeout: Duration,
}
impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            storage_path: None,
            max_sessions: 1024,
            max_state_bytes: 64 * 1024 * 1024,
            max_packet_size: 1024 * 1024,
            max_connections: 1024,
            outbound_capacity: 64,
            max_subscriptions: 256,
            max_inflight: 32,
            handshake_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(5),
        }
    }
}
