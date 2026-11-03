//! Opaque replicated session transactions; MQTT snapshot schema stays session-owned.
//! Persistence is a backend property; live memory-quorum acceptance is volatile.
use crate::ClusterMessage;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct SessionLease {
    pub client_id: String,
    pub epoch: u64,
    pub revision: u64,
    pub snapshot: Option<Vec<u8>>,
}
impl std::fmt::Debug for SessionLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionLease")
            .field("epoch", &self.epoch)
            .field("revision", &self.revision)
            .field("snapshot_bytes", &self.snapshot.as_ref().map(Vec::len))
            .finish_non_exhaustive()
    }
}

/// Stable source event identity, distinct from a reusable MQTT Packet Identifier.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DeliveryId {
    pub source: String,
    pub epoch: u64,
    pub revision: u64,
    pub index: u32,
    /// Recipient logical session, unchanged by ownership migration alone.
    #[serde(default)]
    pub target_generation: u64,
}

#[derive(Clone)]
pub struct SessionMutation {
    pub client_id: String,
    pub epoch: u64,
    pub expected_revision: u64,
    pub snapshot: Option<Vec<u8>>,
    /// True only for a successful CONNECT with Session Present=false.
    pub reset_session: bool,
    pub filters: Vec<String>,
    pub publications: Vec<ClusterMessage>,
    pub receipt: Option<DeliveryId>,
}
impl std::fmt::Debug for SessionMutation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionMutation")
            .field("epoch", &self.epoch)
            .field("expected_revision", &self.expected_revision)
            .field("reset_session", &self.reset_session)
            .field("snapshot_bytes", &self.snapshot.as_ref().map(Vec::len))
            .field("publications", &self.publications.len())
            .field("has_receipt", &self.receipt.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionCommit {
    pub revision: u64,
    pub duplicate: bool,
}
