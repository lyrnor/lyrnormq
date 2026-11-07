//! Read-only cluster topology projection for management surfaces.
use serde::Serialize;

/// One logical member. Dual-role hosts merge controller + broker into one card.
#[derive(Clone, Debug, Serialize)]
pub struct TopologyNode {
    /// Display identity: controller id when cohosted, otherwise broker id.
    pub id: u64,
    /// Ordered role labels such as `controller` and/or `broker`.
    pub roles: Vec<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub controller_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_id: Option<u64>,
    pub address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub control_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker_address: Option<String>,
    pub applied: u64,
    pub expires_at: u64,
    pub max_lease_ms: u64,
    /// Signed remaining lease budget at sample time; negative means expired.
    pub lease_remaining_ms: i64,
    pub clients: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_id: Option<String>,
    pub excluded: bool,
    #[serde(rename = "self")]
    pub is_self: bool,
    pub leader: bool,
    /// Lease still valid relative to the sampling clock on this broker.
    pub online: bool,
}

/// Compact repair badge for the topology panel.
#[derive(Clone, Debug, Serialize)]
pub struct TopologyRepair {
    pub id: String,
    pub target: u64,
    pub generation: u64,
    pub sealed: bool,
    pub state: String,
}

/// Unified topology payload returned inside `/api/v1/overview`.
///
/// Standalone brokers synthesize a single-node view; clustered brokers project
/// watched metadata plus a best-effort Raft status sample.
#[derive(Clone, Debug, Serialize)]
pub struct TopologyView {
    pub revision: u64,
    pub leader: Option<u64>,
    pub term: u64,
    pub last_applied: u64,
    pub data_storage: String,
    pub data_generation: u64,
    pub clients: usize,
    pub routes: usize,
    pub self_node_id: u64,
    pub nodes: Vec<TopologyNode>,
    pub excluded: Vec<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repair: Option<TopologyRepair>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_repair: Option<TopologyRepair>,
}
