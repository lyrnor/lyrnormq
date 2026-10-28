//! Typed packet validation and codec errors.

use std::{error::Error, fmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    Malformed(String),
    ProtocolViolation(String),
    Mqtt5(crate::ReasonCode),
    UnsupportedVersion,
    UnsupportedFeature(String),
    PacketTooLarge,
}
impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mqtt5(reason) => write!(f, "MQTT5 error 0x{:02x}", reason.value()),
            Self::ProtocolViolation(s) => write!(f, "MQTT protocol violation: {s}"),
            Self::Malformed(s) => write!(f, "malformed MQTT packet: {s}"),
            Self::UnsupportedVersion => write!(f, "unsupported MQTT version"),
            Self::UnsupportedFeature(s) => write!(f, "unsupported MQTT feature: {s}"),
            Self::PacketTooLarge => write!(f, "MQTT packet exceeds limit"),
        }
    }
}
impl Error for ProtocolError {}
