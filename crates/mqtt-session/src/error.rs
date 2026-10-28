//! Session operation errors.
use std::{error::Error, fmt};

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
