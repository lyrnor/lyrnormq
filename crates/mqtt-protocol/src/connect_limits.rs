//! Diagnostic-only CONNECT limit discovery using the shared property parser.
use crate::{Property, properties, wire::Reader};

/// Read a trustworthy MQTT5 CONNECT packet-size limit before parsing its payload.
///
/// Returns `Some(u32::MAX)` when a complete valid property block omits the limit,
/// and `None` for other versions, malformed/incomplete prefixes or properties.
/// Later payload bytes need not be complete or valid. The declared packet boundary
/// is respected even when the input also contains another packet. This inspection
/// does not accept a connection or replace normal framing/protocol validation.
pub fn peek_connect_maximum_packet_size(input: &[u8]) -> Option<u32> {
    let mut fixed = Reader::new(input);
    if fixed.byte().ok()? != 0x10 {
        return None;
    }
    let remaining = fixed.varint().ok()? as usize;
    let end = fixed.pos.checked_add(remaining)?.min(input.len());
    let mut body = Reader::new(input.get(fixed.pos..end)?);
    if body.take(7).ok()? != b"\x00\x04MQTT\x05" {
        return None;
    }
    let flags = body.byte().ok()?;
    if flags & 1 != 0 || flags & 0x18 == 0x18 || (flags & 4 == 0 && flags & 0x38 != 0) {
        return None;
    }
    body.u16().ok()?;
    let properties = properties::read(&mut body, 1).ok()?;
    Some(
        properties
            .into_iter()
            .find_map(|property| match property {
                Property::MaximumPacketSize(value) => Some(value),
                _ => None,
            })
            .unwrap_or(u32::MAX),
    )
}

#[cfg(test)]
mod tests;
