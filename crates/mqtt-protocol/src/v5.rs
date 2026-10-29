//! Incremental versioned MQTT framing. Policy and feature support belong to runtime.
use crate::codec::{validate_id, validate_string, validate_topic};
use crate::properties::{self, Property};
use crate::reason::valid_reason;
use crate::wire::{Reader, append, binary, malformed, string, varint, violation};
use crate::{Frame, Packet, Password, ProtocolError, ProtocolVersion, ReasonCode, Will};

/// Inspect only a complete CONNECT protocol-name/version prefix, without allocating
/// or accepting the packet. Useful for correctly framing a failure response.
pub fn peek_connect_version(input: &[u8]) -> Option<ProtocolVersion> {
    if input.first().copied() != Some(0x10) {
        return None;
    }
    let mut remaining = 0usize;
    let mut header = 1usize;
    let mut finished = false;
    for index in 0..4 {
        let byte = *input.get(1 + index)?;
        header += 1;
        remaining |= usize::from(byte & 127) << (7 * index);
        if byte & 128 == 0 {
            if index > 0 && byte == 0 {
                return None;
            }
            finished = true;
            break;
        }
    }
    if !finished || remaining < 7 {
        return None;
    }
    let prefix = input.get(header..header.checked_add(7)?)?;
    if &prefix[..6] != b"\x00\x04MQTT" {
        return None;
    }
    match prefix[6] {
        4 => Some(ProtocolVersion::V311),
        5 => Some(ProtocolVersion::V5),
        _ => None,
    }
}

fn bounds(input: &[u8], max: usize) -> Result<Option<(u8, usize, usize)>, ProtocolError> {
    if input.is_empty() {
        return Ok(None);
    }
    if max < 2 {
        return Err(ProtocolError::PacketTooLarge);
    }
    check_flags(input[0])?;
    let mut length = 0usize;
    let mut header = 1;
    for index in 0..4 {
        let Some(byte) = input.get(index + 1).copied() else {
            return if input.len() >= max {
                Err(ProtocolError::PacketTooLarge)
            } else {
                Ok(None)
            };
        };
        header += 1;
        length |= usize::from(byte & 127) << (7 * index);
        if byte & 128 == 0 {
            if index > 0 && byte == 0 {
                return Err(malformed("noncanonical Remaining Length"));
            }
            let total = header + length;
            if total > max {
                return Err(ProtocolError::PacketTooLarge);
            }
            return if input.len() < total {
                Ok(None)
            } else {
                Ok(Some((input[0], header, total)))
            };
        }
    }
    Err(malformed("Remaining Length exceeds four bytes"))
}
fn check_flags(header: u8) -> Result<u8, ProtocolError> {
    let kind = header >> 4;
    let flags = header & 15;
    if kind == 0 {
        return Err(malformed("reserved packet type"));
    }
    if kind == 3 {
        let qos = (flags >> 1) & 3;
        if qos == 3 || (qos == 0 && flags & 8 != 0) {
            return Err(malformed("invalid PUBLISH flags"));
        }
    } else if flags != if matches!(kind, 6 | 8 | 10) { 2 } else { 0 } {
        return Err(malformed("invalid fixed header flags"));
    }
    Ok(kind)
}
fn reason(kind: u8, value: u8) -> Result<ReasonCode, ProtocolError> {
    let reason = ReasonCode::try_from(value)?;
    if !valid_reason(kind, value) {
        return Err(violation("Reason Code invalid for packet"));
    }
    Ok(reason)
}
fn option(filter: &str, qos: u8, value: u8) -> Result<(), ProtocolError> {
    if value & 0xc0 != 0 {
        return Err(malformed("reserved subscription option bits"));
    }
    if qos > 2 || value & 3 != qos || (value >> 4) & 3 == 3 {
        return Err(violation("invalid subscription options"));
    }
    if filter.starts_with("$share/") && value & 4 != 0 {
        return Err(violation("No Local is forbidden on shared subscriptions"));
    }
    Ok(())
}
fn topic(topic: &str, properties: &[Property]) -> Result<(), ProtocolError> {
    if topic.is_empty() {
        if properties
            .iter()
            .any(|p| matches!(p, Property::TopicAlias(_)))
        {
            return Ok(());
        }
        return Err(violation("empty Topic Name without Topic Alias"));
    }
    validate_topic(topic)
}

pub fn decode_frame(
    input: &[u8],
    max: usize,
    version: Option<ProtocolVersion>,
) -> Result<Option<(Frame, usize)>, ProtocolError> {
    let Some((header, offset, total)) = bounds(input, max)? else {
        return Ok(None);
    };
    let kind = check_flags(header)?;
    let selected = if kind == 1 {
        let mut prefix = Reader::new(&input[offset..total]);
        if prefix.string()? != "MQTT" {
            return Err(malformed("invalid protocol name"));
        }
        let actual = match prefix.byte()? {
            4 => ProtocolVersion::V311,
            5 => ProtocolVersion::V5,
            _ => return Err(ProtocolError::UnsupportedVersion),
        };
        if version.is_some_and(|expected| actual != expected) {
            return Err(violation("CONNECT changes negotiated version"));
        }
        actual
    } else {
        version.unwrap_or(ProtocolVersion::V311)
    };
    if selected == ProtocolVersion::V311 {
        return crate::decode(input, max).map(|packet| packet.map(|(p, n)| (Frame::v311(p), n)));
    }
    let mut r = Reader::new(&input[offset..total]);
    let mut frame = Frame::v5(Packet::PingReq);
    frame.packet = match kind {
        1 => {
            if r.string()? != "MQTT" || r.byte()? != 5 {
                return Err(malformed("invalid CONNECT prefix"));
            }
            let flags = r.byte()?;
            if flags & 1 != 0 || flags & 0x18 == 0x18 || (flags & 4 == 0 && flags & 0x38 != 0) {
                return Err(violation("invalid CONNECT flags"));
            }
            let keep_alive = r.u16()?;
            frame.properties = properties::read(&mut r, 1)?;
            let client_id = r.string()?;
            let will = if flags & 4 != 0 {
                frame.will_properties = properties::read(&mut r, 0)?;
                let topic = r.string()?;
                validate_topic(&topic)?;
                Some(Will {
                    topic,
                    payload: r.binary()?,
                    qos: (flags >> 3) & 3,
                    retain: flags & 32 != 0,
                })
            } else {
                None
            };
            let username = if flags & 128 != 0 {
                Some(r.string()?)
            } else {
                None
            };
            let password = if flags & 64 != 0 {
                Some(Password::new(r.binary()?))
            } else {
                None
            };
            Packet::Connect {
                client_id,
                clean_session: flags & 2 != 0,
                keep_alive,
                will,
                username,
                password,
            }
        }
        2 => {
            let flags = r.byte()?;
            let code = r.byte()?;
            reason(2, code)?;
            if flags > 1 || (flags == 1 && code != 0) {
                return Err(violation("invalid CONNACK flags"));
            }
            frame.properties = properties::read(&mut r, 2)?;
            Packet::ConnAck {
                session_present: flags == 1,
                code,
            }
        }
        3 => {
            let topic_name = r.string()?;
            let qos = (header >> 1) & 3;
            let packet_id = if qos > 0 { Some(r.id()?) } else { None };
            frame.properties = properties::read(&mut r, 3)?;
            topic(&topic_name, &frame.properties)?;
            Packet::Publish {
                topic: topic_name,
                payload: r.rest()?,
                qos,
                packet_id,
                dup: header & 8 != 0,
                retain: header & 1 != 0,
            }
        }
        4..=7 => {
            let packet_id = r.id()?;
            if !r.done() {
                frame.reason_code = reason(kind, r.byte()?)?;
            }
            if !r.done() {
                frame.properties = properties::read(&mut r, kind)?;
            }
            match kind {
                4 => Packet::PubAck { packet_id },
                5 => Packet::PubRec { packet_id },
                6 => Packet::PubRel { packet_id },
                _ => Packet::PubComp { packet_id },
            }
        }
        8 => {
            let packet_id = r.id()?;
            frame.properties = properties::read(&mut r, 8)?;
            let mut filters = Vec::new();
            while !r.done() {
                let filter = r.string()?;
                if filter.is_empty() {
                    return Err(violation("empty topic filter"));
                }
                let options = r.byte()?;
                option(&filter, options & 3, options)?;
                filters.push((filter, options & 3));
                frame.subscription_options.push(options);
            }
            if filters.is_empty() {
                return Err(violation("empty SUBSCRIBE"));
            }
            Packet::Subscribe { packet_id, filters }
        }
        9 => {
            let packet_id = r.id()?;
            frame.properties = properties::read(&mut r, 9)?;
            let codes = r.rest()?;
            if codes.is_empty() {
                return Err(violation("empty SUBACK"));
            }
            for code in &codes {
                reason(9, *code)?;
            }
            Packet::SubAck { packet_id, codes }
        }
        10 => {
            let packet_id = r.id()?;
            frame.properties = properties::read(&mut r, 10)?;
            let mut filters = Vec::new();
            while !r.done() {
                let filter = r.string()?;
                if filter.is_empty() {
                    return Err(violation("empty topic filter"));
                }
                filters.push(filter);
            }
            if filters.is_empty() {
                return Err(violation("empty UNSUBSCRIBE"));
            }
            Packet::Unsubscribe { packet_id, filters }
        }
        11 => {
            let packet_id = r.id()?;
            frame.properties = properties::read(&mut r, 11)?;
            while !r.done() {
                frame.reason_codes.push(reason(11, r.byte()?)?);
            }
            if frame.reason_codes.is_empty() {
                return Err(violation("empty UNSUBACK"));
            }
            Packet::UnsubAck { packet_id }
        }
        12 => Packet::PingReq,
        13 => Packet::PingResp,
        14 | 15 => {
            if !r.done() {
                frame.reason_code = reason(kind, r.byte()?)?;
            }
            if !r.done() {
                frame.properties = properties::read(&mut r, kind)?;
            }
            if kind == 14 {
                Packet::Disconnect
            } else {
                Packet::Auth
            }
        }
        _ => return Err(malformed("reserved packet type")),
    };
    if !r.done() {
        return Err(malformed("trailing packet bytes"));
    }
    if kind == 15 {
        // Even the abbreviated AUTH forms must carry the mandatory method.
        properties::validate(&frame.properties, kind)?;
    }
    Ok(Some((frame, total)))
}

fn clean_metadata(frame: &Frame, kind: u8) -> Result<(), ProtocolError> {
    if (!matches!(kind, 4..=7 | 14 | 15) && frame.reason_code != ReasonCode::SUCCESS)
        || (kind != 11 && !frame.reason_codes.is_empty())
        || (kind != 8 && !frame.subscription_options.is_empty())
        || (kind != 1 && !frame.will_properties.is_empty())
    {
        return Err(violation("metadata attached to wrong packet"));
    }
    Ok(())
}
/// Encode one frame. Property validity is independent of runtime feature policy.
pub fn encode_frame(frame: &Frame) -> Result<Vec<u8>, ProtocolError> {
    if frame.version == ProtocolVersion::V311 {
        if !frame.properties.is_empty()
            || !frame.will_properties.is_empty()
            || !frame.subscription_options.is_empty()
            || !frame.reason_codes.is_empty()
            || frame.reason_code != ReasonCode::SUCCESS
        {
            return Err(violation("MQTT311 frame cannot carry MQTT5 metadata"));
        }
        return crate::encode(&frame.packet);
    }
    let mut out = Vec::new();
    let kind = match &frame.packet {
        Packet::Connect { .. } => 1,
        Packet::ConnAck { .. } => 2,
        Packet::Publish { .. } => 3,
        Packet::PubAck { .. } => 4,
        Packet::PubRec { .. } => 5,
        Packet::PubRel { .. } => 6,
        Packet::PubComp { .. } => 7,
        Packet::Subscribe { .. } => 8,
        Packet::SubAck { .. } => 9,
        Packet::Unsubscribe { .. } => 10,
        Packet::UnsubAck { .. } => 11,
        Packet::PingReq => 12,
        Packet::PingResp => 13,
        Packet::Disconnect => 14,
        Packet::Auth => 15,
    };
    clean_metadata(frame, kind)?;
    if kind == 15 {
        properties::validate(&frame.properties, kind)?;
    }
    let header = match &frame.packet {
        Packet::Connect {
            client_id,
            clean_session,
            keep_alive,
            will,
            username,
            password,
        } => {
            string(&mut out, "MQTT")?;
            let mut flags = u8::from(*clean_session) << 1;
            if let Some(will) = will {
                if will.qos > 2 {
                    return Err(violation("invalid Will QoS"));
                }
                flags |= 4 | (will.qos << 3) | (u8::from(will.retain) << 5);
            } else if !frame.will_properties.is_empty() {
                return Err(violation("Will Properties without Will"));
            }
            if username.is_some() {
                flags |= 128;
            }
            if password.is_some() {
                flags |= 64;
            }
            append(&mut out, &[5, flags])?;
            append(&mut out, &keep_alive.to_be_bytes())?;
            properties::write(&mut out, &frame.properties, 1)?;
            string(&mut out, client_id)?;
            if let Some(will) = will {
                properties::write(&mut out, &frame.will_properties, 0)?;
                validate_topic(&will.topic)?;
                string(&mut out, &will.topic)?;
                binary(&mut out, &will.payload)?;
            }
            if let Some(username) = username {
                string(&mut out, username)?;
            }
            if let Some(password) = password {
                binary(&mut out, password.as_bytes())?;
            }
            0x10
        }
        Packet::ConnAck {
            session_present,
            code,
        } => {
            reason(2, *code)?;
            if *session_present && *code != 0 {
                return Err(violation("invalid CONNACK flags"));
            }
            append(&mut out, &[u8::from(*session_present), *code])?;
            properties::write(&mut out, &frame.properties, 2)?;
            0x20
        }
        Packet::Publish {
            topic: topic_name,
            payload,
            qos,
            packet_id,
            dup,
            retain,
        } => {
            if *qos > 2 || (*qos == 0 && (*dup || packet_id.is_some())) {
                return Err(violation("invalid PUBLISH flags or ID"));
            }
            topic(topic_name, &frame.properties)?;
            string(&mut out, topic_name)?;
            if *qos > 0 {
                let id = packet_id.ok_or_else(|| violation("missing PUBLISH ID"))?;
                validate_id(id)?;
                append(&mut out, &id.to_be_bytes())?;
            }
            properties::write(&mut out, &frame.properties, 3)?;
            append(&mut out, payload)?;
            0x30 | (*qos << 1) | (u8::from(*dup) << 3) | u8::from(*retain)
        }
        Packet::PubAck { packet_id }
        | Packet::PubRec { packet_id }
        | Packet::PubRel { packet_id }
        | Packet::PubComp { packet_id } => {
            validate_id(*packet_id)?;
            reason(kind, frame.reason_code.value())?;
            append(&mut out, &packet_id.to_be_bytes())?;
            if frame.reason_code != ReasonCode::SUCCESS || !frame.properties.is_empty() {
                append(&mut out, &[frame.reason_code.value()])?;
                if !frame.properties.is_empty() {
                    properties::write(&mut out, &frame.properties, kind)?;
                }
            }
            if kind == 6 { 0x62 } else { kind << 4 }
        }
        Packet::Subscribe { packet_id, filters } => {
            validate_id(*packet_id)?;
            if filters.is_empty() {
                return Err(violation("empty SUBSCRIBE"));
            }
            if !frame.subscription_options.is_empty()
                && frame.subscription_options.len() != filters.len()
            {
                return Err(violation("subscription option count mismatch"));
            }
            append(&mut out, &packet_id.to_be_bytes())?;
            properties::write(&mut out, &frame.properties, 8)?;
            for (index, (filter, qos)) in filters.iter().enumerate() {
                validate_string(filter)?;
                if filter.is_empty() {
                    return Err(violation("empty topic filter"));
                }
                let value = frame
                    .subscription_options
                    .get(index)
                    .copied()
                    .unwrap_or(*qos);
                option(filter, *qos, value)?;
                string(&mut out, filter)?;
                append(&mut out, &[value])?;
            }
            0x82
        }
        Packet::SubAck { packet_id, codes } => {
            validate_id(*packet_id)?;
            if codes.is_empty() {
                return Err(violation("empty SUBACK"));
            }
            for code in codes {
                reason(9, *code)?;
            }
            append(&mut out, &packet_id.to_be_bytes())?;
            properties::write(&mut out, &frame.properties, 9)?;
            append(&mut out, codes)?;
            0x90
        }
        Packet::Unsubscribe { packet_id, filters } => {
            validate_id(*packet_id)?;
            if filters.is_empty() {
                return Err(violation("empty UNSUBSCRIBE"));
            }
            append(&mut out, &packet_id.to_be_bytes())?;
            properties::write(&mut out, &frame.properties, 10)?;
            for filter in filters {
                if filter.is_empty() {
                    return Err(violation("empty topic filter"));
                }
                string(&mut out, filter)?;
            }
            0xa2
        }
        Packet::UnsubAck { packet_id } => {
            validate_id(*packet_id)?;
            if frame.reason_codes.is_empty() {
                return Err(violation("empty UNSUBACK"));
            }
            append(&mut out, &packet_id.to_be_bytes())?;
            properties::write(&mut out, &frame.properties, 11)?;
            for code in &frame.reason_codes {
                reason(11, code.value())?;
                append(&mut out, &[code.value()])?;
            }
            0xb0
        }
        Packet::PingReq | Packet::PingResp => {
            if !frame.properties.is_empty() {
                return Err(violation("PING cannot carry properties"));
            }
            kind << 4
        }
        Packet::Disconnect | Packet::Auth => {
            reason(kind, frame.reason_code.value())?;
            if frame.reason_code != ReasonCode::SUCCESS || !frame.properties.is_empty() {
                append(&mut out, &[frame.reason_code.value()])?;
                if !frame.properties.is_empty() {
                    properties::write(&mut out, &frame.properties, kind)?;
                }
            }
            kind << 4
        }
    };
    let mut bytes = vec![header];
    varint(&mut bytes, out.len())?;
    bytes.extend(out);
    Ok(bytes)
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod topic_alias_tests;

#[cfg(test)]
mod conformance_tests;

#[cfg(test)]
mod property_conformance_tests;

#[cfg(test)]
mod property_table_tests;

#[cfg(test)]
mod payload_role_tests;
