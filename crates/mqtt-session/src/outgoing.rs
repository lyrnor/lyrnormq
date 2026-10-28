//! Outgoing QoS acknowledgement state machines.
use crate::{PacketIdentifiers, SessionError};
use std::{
    collections::{BTreeMap, VecDeque},
    num::NonZeroU16,
};

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

#[derive(Clone)]
pub(super) enum OutgoingState {
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
/// Messages are opaque encoded PUBLISH data retained for the current stage.
/// The durable module snapshots/restores this state and drives reconnect replay;
/// the primitive itself neither commits storage nor retransmits packets.
#[derive(Clone)]
pub struct OutgoingQos {
    pub(super) identifiers: PacketIdentifiers,
    pub(super) pending: BTreeMap<NonZeroU16, OutgoingState>,
    max_message_size: usize,
    max_inflight: usize,
    pub(super) order: VecDeque<NonZeroU16>,
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
            order: VecDeque::new(),
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
        self.order.push_back(id);
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
                self.order.retain(|pending| *pending != id);
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
}
