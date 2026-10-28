//! Incoming QoS2 ownership and single-delivery transitions.
use crate::SessionError;
use std::{
    collections::{BTreeMap, VecDeque},
    num::NonZeroU16,
};

/// PUBREC is required for both first and repeated PUBLISH, irrespective of DUP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishResult {
    PubRec {
        packet_id: NonZeroU16,
        newly_accepted: bool,
    },
}
/// Send PUBCOMP for every PUBREL. `delivery` occurs once for an accepted exchange.
#[derive(Debug, PartialEq, Eq)]
pub struct ReleaseResult {
    pub packet_id: NonZeroU16,
    pub delivery: Option<Vec<u8>>,
}

/// Incoming QoS2 method A: retain ownership on PUBLISH and initiate delivery on
/// PUBREL. Messages are opaque encoded application messages, including any topic.
/// Maximum retained bytes are `max_inflight * max_message_size` (plus metadata).
/// Completed IDs form only a bounded diagnostic history, not a deduplication
/// requirement: an unknown PUBREL also returns PUBCOMP without delivery.
#[derive(Clone)]
pub struct IncomingQos2 {
    pub(super) pending: BTreeMap<NonZeroU16, Vec<u8>>,
    completed: VecDeque<NonZeroU16>,
    max_inflight: usize,
    max_message_size: usize,
    history_limit: usize,
}
impl IncomingQos2 {
    pub fn new(max_inflight: usize, max_message_size: usize, history_limit: usize) -> Self {
        Self {
            pending: BTreeMap::new(),
            completed: VecDeque::new(),
            max_inflight: max_inflight.min(65535),
            max_message_size,
            history_limit: history_limit.min(65535),
        }
    }
    /// DUP is intentionally not part of identity. Repeated IDs while awaiting
    /// PUBREL always receive PUBREC and never replace the accepted message.
    /// A PUBLISH after completion starts a new exchange even with DUP set.
    pub fn publish(
        &mut self,
        id: NonZeroU16,
        message: &[u8],
        _dup: bool,
    ) -> Result<PublishResult, SessionError> {
        if self.pending.contains_key(&id) {
            return Ok(PublishResult::PubRec {
                packet_id: id,
                newly_accepted: false,
            });
        }
        if message.len() > self.max_message_size {
            return Err(SessionError::MessageTooLarge);
        }
        if self.pending.len() >= self.max_inflight {
            return Err(SessionError::InflightFull);
        }
        self.completed.retain(|old| *old != id);
        self.pending.insert(id, message.to_vec());
        Ok(PublishResult::PubRec {
            packet_id: id,
            newly_accepted: true,
        })
    }
    /// This mutation and onward delivery intent MUST be durably committed
    /// together before PUBCOMP. The durable model and transport worker provide
    /// that transaction boundary; using this primitive alone supplies no IO or
    /// application-side exactly-once guarantee.
    pub fn pubrel(&mut self, id: NonZeroU16) -> ReleaseResult {
        let delivery = self.pending.remove(&id);
        if delivery.is_some() && self.history_limit != 0 {
            self.completed.retain(|old| *old != id);
            if self.completed.len() == self.history_limit {
                self.completed.pop_front();
            }
            self.completed.push_back(id);
        }
        ReleaseResult {
            packet_id: id,
            delivery,
        }
    }
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
    pub fn completed_count(&self) -> usize {
        self.completed.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn id(value: u16) -> NonZeroU16 {
        NonZeroU16::new(value).unwrap()
    }
    #[test]
    fn qos2_delivery_once_and_reused_id_is_new_even_with_dup() {
        let mut state = IncomingQos2::new(2, 10, 2);
        assert_eq!(
            state.publish(id(1), b"first", false),
            Ok(PublishResult::PubRec {
                packet_id: id(1),
                newly_accepted: true
            })
        );
        assert_eq!(
            state.publish(id(1), b"changed", true),
            Ok(PublishResult::PubRec {
                packet_id: id(1),
                newly_accepted: false
            })
        );
        assert_eq!(state.pending_count(), 1);
        assert_eq!(state.pubrel(id(1)).delivery, Some(b"first".to_vec()));
        assert_eq!(state.pubrel(id(1)).delivery, None);
        state.publish(id(1), b"new", true).unwrap();
        assert_eq!(state.completed_count(), 0);
        assert_eq!(state.pubrel(id(1)).delivery, Some(b"new".to_vec()));
    }
    #[test]
    fn bounded_window_bytes_and_completion_history() {
        let mut state = IncomingQos2::new(1, 3, 1);
        assert_eq!(
            state.publish(id(1), b"long", false),
            Err(SessionError::MessageTooLarge)
        );
        state.publish(id(1), b"one", false).unwrap();
        assert_eq!(
            state.publish(id(2), b"two", false),
            Err(SessionError::InflightFull)
        );
        // A duplicate must still be acknowledged at a full window.
        assert!(
            state
                .publish(id(1), b"ignored large duplicate", false)
                .is_ok()
        );
        assert_eq!(state.pubrel(id(1)).delivery, Some(b"one".to_vec()));
        state.publish(id(2), b"two", false).unwrap();
        assert_eq!(state.pubrel(id(2)).delivery, Some(b"two".to_vec()));
        assert_eq!(state.completed_count(), 1);
        assert_eq!(state.pubrel(id(1)).delivery, None);
        assert_eq!(state.pubrel(id(99)).delivery, None);
        assert_eq!(state.pending_count(), 0);
    }
}
