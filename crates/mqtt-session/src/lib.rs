//! Bounded MQTT 3.1.1 QoS primitives integrated by the Phase 2a clean-session transport.
//! Incoming QoS2 delivers on PUBREL; outgoing QoS1/QoS2 releases identifiers only
//! after PUBACK/PUBCOMP. The transport drives packet IO and acknowledgement order.
//! State is volatile: persistence, reconnect retransmission and crash recovery
//! require a future durable transaction/outbox implementation.
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
    InvalidQos,
    UnexpectedAck,
}
impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InflightFull => "inflight window full",
            Self::MessageTooLarge => "message exceeds session limit",
            Self::InvalidQos => "outgoing exchange requires QoS 1 or 2",
            Self::UnexpectedAck => "acknowledgement does not match outgoing exchange stage",
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

/// Acknowledgements received for a broker-originated exchange. Incoming QoS2
/// PUBREL uses a separate identifier namespace and is handled by `IncomingQos2`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckKind {
    PubAck,
    PubRec,
    PubComp,
}

/// Action after a valid outgoing acknowledgement. The caller writes PUBREL with
/// fixed flags 0x2, including when `SendPubRel` results from a repeated PUBREC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckAction {
    Complete,
    SendPubRel,
    Ignore,
}

enum OutgoingState {
    AwaitingAck { _message: Vec<u8> },
    AwaitingReceipt { _message: Vec<u8> },
    Released,
}

/// Bounded outgoing QoS1/QoS2 exchanges for one clean-session connection.
///
/// An identifier remains allocated until PUBACK (QoS1) or PUBCOMP (QoS2). QoS2
/// drops the stored encoded PUBLISH at PUBREC: subsequent PUBREC packets cause
/// PUBREL, never another PUBLISH. Pending bytes are bounded by
/// `min(max_inflight, 65535) * max_message_size`, plus bounded metadata.
///
/// This object supplies no timers, persistence or reconnect recovery. The caller
/// serializes packet IO and must not claim durable delivery from these mutations.
/// Messages are opaque encoded PUBLISH data, retained only for the current stage;
/// no retransmission inspection API or durable schema is provided in this phase.
pub struct OutgoingQos {
    identifiers: PacketIdentifiers,
    pending: BTreeMap<NonZeroU16, OutgoingState>,
    max_message_size: usize,
    max_inflight: usize,
}
impl OutgoingQos {
    /// A zero window allows no exchange; values above 65535 are capped to the
    /// protocol identifier space. Transport configuration rejects zero windows.
    pub fn new(max_inflight: usize, max_message_size: usize) -> Self {
        Self {
            identifiers: PacketIdentifiers::new(max_inflight),
            pending: BTreeMap::new(),
            max_message_size,
            max_inflight: max_inflight.min(65535),
        }
    }

    /// Start an outgoing exchange before writing its first PUBLISH. Rejects
    /// QoS0/invalid QoS, oversize encoded messages or a full window without
    /// allocating an ID or changing previously accepted exchanges.
    pub fn start(&mut self, qos: u8, message: Vec<u8>) -> Result<NonZeroU16, SessionError> {
        if !matches!(qos, 1 | 2) {
            return Err(SessionError::InvalidQos);
        }
        if message.len() > self.max_message_size {
            return Err(SessionError::MessageTooLarge);
        }
        let id = self.identifiers.allocate()?;
        let state = if qos == 1 {
            OutgoingState::AwaitingAck { _message: message }
        } else {
            OutgoingState::AwaitingReceipt { _message: message }
        };
        self.pending.insert(id, state);
        Ok(id)
    }

    /// Advance a broker-originated exchange. Unknown identifiers are ignored;
    /// a known identifier with the wrong ACK kind returns `UnexpectedAck` and
    /// preserves the exchange and its identifier. Duplicate PUBREC after the
    /// first PUBREC requests PUBREL again while retaining the identifier.
    pub fn ack(&mut self, id: NonZeroU16, kind: AckKind) -> Result<AckAction, SessionError> {
        match (self.pending.get(&id), kind) {
            (None, _) => Ok(AckAction::Ignore),
            (Some(OutgoingState::AwaitingAck { .. }), AckKind::PubAck)
            | (Some(OutgoingState::Released), AckKind::PubComp) => {
                self.pending.remove(&id);
                self.identifiers.release(id);
                Ok(AckAction::Complete)
            }
            (Some(OutgoingState::AwaitingReceipt { .. }), AckKind::PubRec) => {
                self.pending.insert(id, OutgoingState::Released);
                Ok(AckAction::SendPubRel)
            }
            (Some(OutgoingState::Released), AckKind::PubRec) => Ok(AckAction::SendPubRel),
            _ => Err(SessionError::UnexpectedAck),
        }
    }

    pub fn inflight_count(&self) -> usize {
        self.pending.len()
    }

    /// The transport may keep reading acknowledgements while withholding new
    /// queued deliveries when this returns true.
    pub fn is_full(&self) -> bool {
        self.pending.len() >= self.max_inflight
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
    fn outgoing_qos1_completes_once_and_unknown_acks_are_ignored() {
        let mut outgoing = OutgoingQos::new(1, 8);
        let first = outgoing.start(1, b"message".to_vec()).unwrap();
        assert_eq!(first, id(1));
        assert!(outgoing.is_full());
        assert_eq!(outgoing.inflight_count(), 1);
        assert_eq!(
            outgoing.ack(id(500), AckKind::PubRec),
            Ok(AckAction::Ignore)
        );
        assert_eq!(outgoing.inflight_count(), 1);
        assert_eq!(
            outgoing.ack(first, AckKind::PubAck),
            Ok(AckAction::Complete)
        );
        assert!(!outgoing.is_full());
        assert_eq!(outgoing.inflight_count(), 0);
        assert_eq!(outgoing.ack(first, AckKind::PubAck), Ok(AckAction::Ignore));
    }

    #[test]
    fn outgoing_qos2_repeated_pubrec_keeps_id_until_pubcomp() {
        let mut outgoing = OutgoingQos::new(1, 8);
        let first = outgoing.start(2, b"message".to_vec()).unwrap();
        assert_eq!(
            outgoing.ack(first, AckKind::PubComp),
            Err(SessionError::UnexpectedAck)
        );
        assert_eq!(
            outgoing.ack(first, AckKind::PubAck),
            Err(SessionError::UnexpectedAck)
        );
        assert_eq!(
            outgoing.ack(first, AckKind::PubRec),
            Ok(AckAction::SendPubRel)
        );
        assert!(matches!(
            outgoing.pending.get(&first),
            Some(OutgoingState::Released)
        ));
        for _ in 0..3 {
            assert_eq!(
                outgoing.ack(first, AckKind::PubRec),
                Ok(AckAction::SendPubRel)
            );
            assert!(outgoing.is_full());
            assert_eq!(outgoing.start(1, vec![]), Err(SessionError::InflightFull));
        }
        assert_eq!(
            outgoing.ack(first, AckKind::PubAck),
            Err(SessionError::UnexpectedAck)
        );
        assert_eq!(outgoing.inflight_count(), 1);
        assert_eq!(
            outgoing.ack(first, AckKind::PubComp),
            Ok(AckAction::Complete)
        );
        assert_eq!(outgoing.ack(first, AckKind::PubComp), Ok(AckAction::Ignore));
        assert_eq!(outgoing.inflight_count(), 0);
    }

    #[test]
    fn outgoing_errors_preserve_previous_state_and_id_allocation() {
        let mut outgoing = OutgoingQos::new(2, 3);
        assert_eq!(outgoing.start(0, vec![]), Err(SessionError::InvalidQos));
        assert_eq!(outgoing.start(3, vec![]), Err(SessionError::InvalidQos));
        assert_eq!(outgoing.start(255, vec![]), Err(SessionError::InvalidQos));
        assert_eq!(
            outgoing.start(1, vec![0; 4]),
            Err(SessionError::MessageTooLarge)
        );
        assert_eq!(outgoing.inflight_count(), 0);
        let first = outgoing.start(1, vec![1; 3]).unwrap();
        assert_eq!(first, id(1));
        assert_eq!(
            outgoing.ack(first, AckKind::PubRec),
            Err(SessionError::UnexpectedAck)
        );
        assert_eq!(
            outgoing.ack(first, AckKind::PubComp),
            Err(SessionError::UnexpectedAck)
        );
        let second = outgoing.start(2, vec![2; 3]).unwrap();
        assert_eq!(second, id(2));
        assert_eq!(outgoing.start(2, vec![]), Err(SessionError::InflightFull));
        assert_eq!(outgoing.inflight_count(), 2);
        assert_eq!(
            outgoing.ack(first, AckKind::PubAck),
            Ok(AckAction::Complete)
        );
        assert_eq!(
            outgoing.ack(second, AckKind::PubRec),
            Ok(AckAction::SendPubRel)
        );
        assert_eq!(
            outgoing.ack(second, AckKind::PubComp),
            Ok(AckAction::Complete)
        );
        assert_eq!(outgoing.inflight_count(), 0);
        assert_eq!(outgoing.identifiers.in_use(), 0);
        assert_eq!(
            OutgoingQos::new(0, 3).start(1, vec![]),
            Err(SessionError::InflightFull)
        );
        assert!(OutgoingQos::new(0, 3).is_full());
        assert!(OutgoingQos::new(1, 0).start(1, vec![]).is_ok());
    }

    #[test]
    fn outgoing_identifiers_never_reused_until_final_ack() {
        let mut outgoing = OutgoingQos::new(usize::MAX, 0);
        for value in 1..=65535 {
            assert_eq!(outgoing.start(2, vec![]), Ok(id(value)));
        }
        assert!(outgoing.is_full());
        assert_eq!(
            outgoing.ack(id(1000), AckKind::PubRec),
            Ok(AckAction::SendPubRel)
        );
        assert_eq!(outgoing.start(1, vec![]), Err(SessionError::InflightFull));
        assert_eq!(
            outgoing.ack(id(1000), AckKind::PubComp),
            Ok(AckAction::Complete)
        );
        assert_eq!(outgoing.start(1, vec![]), Ok(id(1000)));
        assert!(outgoing.is_full());
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
