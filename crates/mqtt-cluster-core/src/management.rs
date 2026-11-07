//! Read-only directory projections for unified management surfaces.
use serde::Serialize;
use std::io;

/// Filtering applies before pagination. Empty filters mean all records.
#[derive(Clone, Debug, Default)]
pub struct ManagementQuery {
    pub offset: usize,
    pub limit: usize,
    pub search: String,
    pub state: String,
    pub protocol: String,
    pub node: Option<u64>,
}

impl ManagementQuery {
    pub fn validate(&self) -> io::Result<()> {
        if !(1..=100).contains(&self.limit)
            || self.offset > 1_000_000
            || self.search.len() > 256
            || !matches!(self.state.as_str(), "" | "online" | "offline")
            || !matches!(self.protocol.as_str(), "" | "3.1.1" | "5.0" | "unknown")
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid management query",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ManagementPage<T> {
    pub items: Vec<T>,
    pub total: usize,
    pub offset: usize,
    pub limit: usize,
}

/// Cluster-wide session/subscription/retained totals for overview cards.
#[derive(Clone, Debug, Serialize)]
pub struct ManagementCounts {
    pub online_sessions: usize,
    pub offline_sessions: usize,
    pub subscriptions: usize,
    pub retained_messages: usize,
}

/// Directory-backed session row. Protocol options stay unknown until a local
/// observation exists; `node_id` is the owning broker.
#[derive(Clone, Debug, Serialize)]
pub struct ManagementSession {
    pub client_id: String,
    pub online: bool,
    pub protocol: String,
    pub principal: String,
    pub persistent: bool,
    pub subscriptions: usize,
    pub queued: usize,
    pub inflight: usize,
    pub receive_maximum: Option<u16>,
    pub session_expiry_interval: u32,
    pub node_id: u64,
    /// True when fields beyond ownership/filters come from Controller metadata.
    pub directory: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ManagementSubscription {
    pub client_id: String,
    pub filter: String,
    pub qos: u8,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: u8,
    pub identifier: Option<u32>,
    pub node_id: u64,
    pub directory: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ManagementRetained {
    pub topic: String,
    pub qos: u8,
    pub payload_bytes: usize,
    pub expires_at_millis: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<u64>,
    pub directory: bool,
}
