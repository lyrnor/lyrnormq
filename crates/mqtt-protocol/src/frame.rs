//! Connection-version-aware packet envelopes.
use crate::{Packet, Properties, ReasonCode};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolVersion {
    V311,
    V5,
}

/// Wire metadata is deliberately separate from the stable normalized packet API.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub version: ProtocolVersion,
    pub packet: Packet,
    pub properties: Properties,
    pub will_properties: Properties,
    pub subscription_options: Vec<u8>,
    pub reason_code: ReasonCode,
    pub reason_codes: Vec<ReasonCode>,
}
impl Frame {
    pub fn v311(packet: Packet) -> Self {
        Self::new(ProtocolVersion::V311, packet)
    }
    pub fn v5(packet: Packet) -> Self {
        Self::new(ProtocolVersion::V5, packet)
    }
    fn new(version: ProtocolVersion, packet: Packet) -> Self {
        Self {
            version,
            packet,
            properties: Vec::new(),
            will_properties: Vec::new(),
            subscription_options: Vec::new(),
            reason_code: ReasonCode::SUCCESS,
            reason_codes: Vec::new(),
        }
    }
}
