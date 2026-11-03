//! Leader-local monotonic observation of a durable maximum outstanding grant.
use crate::model::Node;
use std::time::Duration;
use tokio::time::Instant;
pub(crate) struct LeaseObservation {
    incarnation: String,
    token: u64,
    since: Instant,
}
impl LeaseObservation {
    pub fn new(node: &Node, now: Instant) -> Self {
        Self {
            incarnation: node.incarnation.clone(),
            token: node.expires_at,
            since: now,
        }
    }
    pub fn expired(&mut self, node: &Node, now: Instant) -> bool {
        if self.incarnation != node.incarnation || self.token != node.expires_at {
            *self = Self::new(node, now);
        }
        now.duration_since(self.since) >= Duration::from_millis(node.max_lease_ms)
    }
}
