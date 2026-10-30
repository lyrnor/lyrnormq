use serde::Serialize;
use std::io;

/// Process-lifetime MQTT counters and eventually consistent committed metadata.
#[derive(Clone, Debug, Serialize)]
pub struct Overview {
    pub ready: bool,
    pub uptime_seconds: u64,
    pub online_sessions: usize,
    pub offline_sessions: usize,
    pub subscriptions: usize,
    pub retained_messages: usize,
    pub messages_in_total: u64,
    pub messages_out_total: u64,
    pub bytes_in_total: u64,
    pub bytes_out_total: u64,
    pub state_bytes: usize,
}
#[derive(Clone, Debug, Serialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub total: usize,
    pub offset: usize,
    pub limit: usize,
}
/// Filtering applies before pagination. Empty filters mean all records.
#[derive(Clone, Debug)]
pub struct Query {
    pub offset: usize,
    pub limit: usize,
    pub search: String,
    pub state: String,
    pub protocol: String,
}
impl Default for Query {
    fn default() -> Self {
        Self {
            offset: 0,
            limit: 25,
            search: String::new(),
            state: String::new(),
            protocol: String::new(),
        }
    }
}
impl Query {
    pub(crate) fn validate(&self) -> io::Result<()> {
        if !(1..=100).contains(&self.limit)
            || self.offset > 1_000_000
            || self.search.len() > 256
            || !matches!(self.state.as_str(), "" | "online" | "offline")
            || !matches!(self.protocol.as_str(), "" | "3.1.1" | "5.0")
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid management query",
            ));
        }
        Ok(())
    }
    pub(crate) fn matches(&self, s: &SessionView) -> bool {
        (self.state.is_empty() || s.online == (self.state == "online"))
            && (self.protocol.is_empty() || self.protocol == s.protocol)
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct SessionView {
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
}
#[derive(Clone, Debug, Serialize)]
pub struct SubscriptionView {
    pub client_id: String,
    pub filter: String,
    pub qos: u8,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: u8,
    pub identifier: Option<u32>,
}
#[derive(Clone, Debug, Serialize)]
pub struct RetainedView {
    pub topic: String,
    pub qos: u8,
    pub payload_bytes: usize,
    pub expires_at_millis: Option<u64>,
}
