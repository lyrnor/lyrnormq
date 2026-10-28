//! Bounded in-memory session primitives. Not yet integrated with the broker.
use std::{
    collections::{BTreeMap, VecDeque},
    error::Error,
    fmt,
    num::NonZeroU16,
};

/// Errors leave previously accepted messages unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionError {
    InflightFull,
    MessageTooLarge,
}
impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InflightFull => "inflight window full",
            Self::MessageTooLarge => "message exceeds session limit",
        })
    }
}
impl Error for SessionError {}

/// Allocates identifiers from 1..=65535, never allocating an in-use identifier.
/// The caller releases only after the matching protocol exchange completes:
/// PUBACK for outgoing QoS1, PUBCOMP for outgoing QoS2.
pub struct PacketIdentifiers {
    used: Box<[bool; 65536]>,
    next: u16,
    count: usize,
    limit: usize,
}
impl PacketIdentifiers {
    pub fn new(limit: usize) -> Self {
        Self {
            used: Box::new([false; 65536]),
            next: 1,
            count: 0,
            limit: limit.min(65535),
        }
    }
    pub fn allocate(&mut self) -> Result<NonZeroU16, SessionError> {
        if self.count >= self.limit {
            return Err(SessionError::InflightFull);
        }
        loop {
            let candidate = self.next;
            self.next = if candidate == u16::MAX {
                1
            } else {
                candidate + 1
            };
            if !self.used[usize::from(candidate)] {
                self.used[usize::from(candidate)] = true;
                self.count += 1;
                // candidate is always in 1..=65535.
                if let Some(id) = NonZeroU16::new(candidate) {
                    return Ok(id);
                }
            }
        }
    }
    /// Returns false if this identifier was already free.
    pub fn release(&mut self, id: NonZeroU16) -> bool {
        let occupied = &mut self.used[usize::from(id.get())];
        if !*occupied {
            return false;
        }
        *occupied = false;
        self.count -= 1;
        true
    }
    pub fn in_use(&self) -> usize {
        self.count
    }
}

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
pub struct IncomingQos2 {
    pending: BTreeMap<NonZeroU16, Vec<u8>>,
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
    /// together before a future persistent runtime sends PUBCOMP. This crate
    /// supplies no durable transaction or crash-safe exactly-once guarantee.
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
    fn identifiers_exhaust_reuse_and_release() {
        let mut ids = PacketIdentifiers::new(usize::MAX);
        for n in 1..=65535 {
            assert_eq!(ids.allocate(), Ok(id(n)));
        }
        assert_eq!(ids.allocate(), Err(SessionError::InflightFull));
        assert!(ids.release(id(1234)));
        assert!(!ids.release(id(1234)));
        assert_eq!(ids.allocate(), Ok(id(1234)));
        assert_eq!(ids.in_use(), 65535);
        assert_eq!(
            PacketIdentifiers::new(0).allocate(),
            Err(SessionError::InflightFull)
        );
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
