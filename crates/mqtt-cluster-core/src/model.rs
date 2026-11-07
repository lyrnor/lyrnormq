use mqtt_protocol::Properties;
use serde::{Deserialize, Serialize};

/// Internal message identity is assigned by the forwarding owner, not MQTT IDs.
#[derive(Clone, Serialize, Deserialize)]
pub struct ClusterMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
    pub properties: Properties,
    pub expires_at: Option<u64>,
    pub publisher: Option<String>,
    pub observer_client: String,
    pub scope: ForwardScope,
}
impl std::fmt::Debug for ClusterMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterMessage")
            .field("topic_bytes", &self.topic.len())
            .field("payload_bytes", &self.payload.len())
            .field("qos", &self.qos)
            .finish_non_exhaustive()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ForwardScope {
    Ordinary,
    Shared { group: String, filter: String },
}
impl ForwardScope {
    /// Full `$share/{group}/{filter}` key. Callers still validate the pair.
    pub fn shared_subscription_key(&self) -> Option<String> {
        match self {
            Self::Ordinary => None,
            Self::Shared { group, filter } => Some(format!("$share/{group}/{filter}")),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientSubscriptions {
    pub client_id: String,
    pub filters: Vec<String>,
}
