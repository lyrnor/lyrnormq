//! Incremental wire decoding, encoding and protocol validation.

use crate::{Packet, Password, ProtocolError, Will};

fn malformed(s: &str) -> ProtocolError {
    ProtocolError::Malformed(s.into())
}
fn unsupported(s: &str) -> ProtocolError {
    ProtocolError::UnsupportedFeature(s.into())
}
fn valid_utf8(s: &str) -> bool {
    !s.chars().any(|c| {
        c == '\0' || (0xfdd0..=0xfdef).contains(&(c as u32)) || (c as u32 & 0xffff) >= 0xfffe
    })
}
struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], ProtocolError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| malformed("length overflow"))?;
        let value = self
            .bytes
            .get(self.pos..end)
            .ok_or_else(|| malformed("truncated field"))?;
        self.pos = end;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8, ProtocolError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, ProtocolError> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }
    fn id(&mut self) -> Result<u16, ProtocolError> {
        let id = self.u16()?;
        validate_id(id)?;
        Ok(id)
    }
    fn string(&mut self) -> Result<String, ProtocolError> {
        let n = self.u16()? as usize;
        let s = std::str::from_utf8(self.take(n)?).map_err(|_| malformed("invalid UTF-8"))?;
        validate_string(s)?;
        Ok(s.to_owned())
    }
    fn done(&self) -> bool {
        self.pos == self.bytes.len()
    }
}

/// Decode one frame without consuming partial input. The limit includes fixed header bytes.
/// Unsupported features fail explicitly; callers must close the connection on errors.
pub fn decode(
    input: &[u8],
    max_packet_size: usize,
) -> Result<Option<(Packet, usize)>, ProtocolError> {
    if input.is_empty() {
        return Ok(None);
    }
    if max_packet_size < 2 {
        return Err(ProtocolError::PacketTooLarge);
    }
    let header = input[0];
    let kind = header >> 4;
    let flags = header & 15;
    if kind == 0 || kind == 15 {
        return Err(malformed("reserved packet type"));
    }
    let expected = if kind == 6 || kind == 8 || kind == 10 {
        2
    } else {
        0
    };
    if kind != 3 && flags != expected {
        return Err(malformed("invalid fixed header flags"));
    }
    if kind == 3 {
        let qos = (flags >> 1) & 3;
        if qos == 3 || (qos == 0 && flags & 8 != 0) {
            return Err(malformed("invalid PUBLISH flags"));
        }
    }
    let mut remaining = 0usize;
    let mut header_len = 1;
    for i in 0..4 {
        let Some(&b) = input.get(1 + i) else {
            if input.len() >= max_packet_size {
                return Err(ProtocolError::PacketTooLarge);
            }
            return Ok(None);
        };
        header_len += 1;
        remaining += ((b & 127) as usize) << (7 * i);
        if b & 128 == 0 {
            break;
        }
        if i == 3 {
            return Err(malformed("remaining length exceeds four bytes"));
        }
    }
    let total = header_len + remaining;
    if total > max_packet_size {
        return Err(ProtocolError::PacketTooLarge);
    }
    if input.len() < total {
        return Ok(None);
    }
    let mut r = Reader {
        bytes: &input[header_len..total],
        pos: 0,
    };
    let packet = match kind {
        1 => {
            if r.string()? != "MQTT" {
                return Err(malformed("invalid protocol name"));
            }
            if r.byte()? != 4 {
                return Err(ProtocolError::UnsupportedVersion);
            }
            let flags = r.byte()?;
            if flags & 1 != 0
                || flags & 0x18 == 0x18
                || (flags & 4 == 0 && flags & 0x38 != 0)
                || (flags & 0x40 != 0 && flags & 0x80 == 0)
            {
                return Err(malformed("invalid CONNECT flags"));
            }
            let keep_alive = r.u16()?;
            let client_id = r.string()?;
            if client_id.is_empty() {
                return Err(unsupported("server-assigned client identifier"));
            }
            let will = if flags & 4 != 0 {
                let topic = r.string()?;
                validate_topic(&topic)?;
                let length = usize::from(r.u16()?);
                let payload = r.take(length)?.to_vec();
                Some(Will {
                    topic,
                    payload,
                    qos: (flags >> 3) & 3,
                    retain: flags & 0x20 != 0,
                })
            } else {
                None
            };
            let username = if flags & 0x80 != 0 {
                Some(r.string()?)
            } else {
                None
            };
            let password = if flags & 0x40 != 0 {
                let length = usize::from(r.u16()?);
                Some(Password::new(r.take(length)?.to_vec()))
            } else {
                None
            };
            Packet::Connect {
                username,
                password,
                client_id,
                clean_session: flags & 2 != 0,
                keep_alive,
                will,
            }
        }
        2 => {
            let ack = r.byte()?;
            let code = r.byte()?;
            if ack > 1 || code > 5 || (code != 0 && ack != 0) {
                return Err(malformed("invalid CONNACK"));
            }
            Packet::ConnAck {
                session_present: ack == 1,
                code,
            }
        }
        3 => {
            let topic = r.string()?;
            validate_topic(&topic)?;
            let qos = (flags >> 1) & 3;
            let packet_id = if qos == 0 { None } else { Some(r.id()?) };
            let payload = r.take(r.bytes.len() - r.pos)?.to_vec();
            Packet::Publish {
                topic,
                payload,
                qos,
                packet_id,
                dup: flags & 8 != 0,
                retain: flags & 1 != 0,
            }
        }
        4 => Packet::PubAck { packet_id: r.id()? },
        5 => Packet::PubRec { packet_id: r.id()? },
        6 => Packet::PubRel { packet_id: r.id()? },
        7 => Packet::PubComp { packet_id: r.id()? },
        8 => {
            let packet_id = r.id()?;
            let mut filters = Vec::new();
            while !r.done() {
                let filter = r.string()?;
                if filter.is_empty() {
                    return Err(malformed("empty topic filter"));
                }
                let qos = r.byte()?;
                if qos > 2 {
                    return Err(malformed("invalid subscription QoS"));
                }
                filters.push((filter, qos));
            }
            if filters.is_empty() {
                return Err(malformed("empty subscription"));
            }
            Packet::Subscribe { packet_id, filters }
        }
        9 => {
            let packet_id = r.id()?;
            let codes = r.take(r.bytes.len() - r.pos)?.to_vec();
            if codes.is_empty() || codes.iter().any(|c| !matches!(c, 0 | 1 | 2 | 128)) {
                return Err(malformed("invalid SUBACK codes"));
            }
            Packet::SubAck { packet_id, codes }
        }
        10 => {
            let packet_id = r.id()?;
            let mut filters = Vec::new();
            while !r.done() {
                let filter = r.string()?;
                if filter.is_empty() {
                    return Err(malformed("empty unsubscribe filter"));
                }
                filters.push(filter);
            }
            if filters.is_empty() {
                return Err(malformed("empty unsubscribe"));
            }
            Packet::Unsubscribe { packet_id, filters }
        }
        11 => Packet::UnsubAck { packet_id: r.id()? },
        12 => Packet::PingReq,
        13 => Packet::PingResp,
        14 => Packet::Disconnect,
        _ => return Err(malformed("reserved packet type")),
    };
    if !r.done() {
        return Err(malformed("trailing bytes"));
    }
    Ok(Some((packet, total)))
}
fn string(out: &mut Vec<u8>, s: &str) -> Result<(), ProtocolError> {
    let n = u16::try_from(s.len()).map_err(|_| malformed("string too long"))?;
    if !valid_utf8(s) {
        return Err(malformed("forbidden UTF-8 code point"));
    }
    out.extend(n.to_be_bytes());
    out.extend(s.as_bytes());
    Ok(())
}
pub(super) fn validate_string(value: &str) -> Result<(), ProtocolError> {
    if value.len() > u16::MAX as usize {
        return Err(malformed("string too long"));
    }
    if !valid_utf8(value) {
        return Err(malformed("forbidden UTF-8 code point"));
    }
    Ok(())
}
pub(super) fn validate_topic(topic: &str) -> Result<(), ProtocolError> {
    validate_string(topic)?;
    if topic.is_empty() || topic.contains(['+', '#']) {
        return Err(malformed("invalid topic name"));
    }
    Ok(())
}
pub(super) fn validate_id(id: u16) -> Result<(), ProtocolError> {
    if id == 0 {
        return Err(malformed("zero packet identifier"));
    }
    Ok(())
}
/// Validate owned packet values without encoding and decoding them again. Payload
/// bytes are opaque; checking metadata must never clone an application payload.
fn validate_packet(packet: &Packet) -> Result<(), ProtocolError> {
    match packet {
        Packet::Connect {
            client_id,
            will,
            username,
            password,
            ..
        } => {
            validate_string(client_id)?;
            if let Some(username) = username {
                validate_string(username)?;
            }
            if let Some(password) = password {
                if username.is_none() {
                    return Err(malformed("password requires username"));
                }
                if password.as_bytes().len() > usize::from(u16::MAX) {
                    return Err(malformed("password too long"));
                }
            }
            if let Some(will) = will {
                validate_topic(&will.topic)?;
                if will.qos > 2 {
                    return Err(malformed("invalid Will QoS"));
                }
                if will.payload.len() > usize::from(u16::MAX) {
                    return Err(malformed("Will payload too long"));
                }
            }
            if client_id.is_empty() {
                return Err(unsupported("server-assigned client identifier"));
            }
        }
        Packet::ConnAck {
            session_present,
            code,
        } => {
            if *code > 5 || (*code != 0 && *session_present) {
                return Err(malformed("invalid CONNACK"));
            }
        }
        Packet::Publish {
            topic,
            payload,
            qos,
            packet_id,
            dup,
            ..
        } => {
            if *qos > 2 || (*qos == 0 && (*dup || packet_id.is_some())) {
                return Err(malformed("invalid PUBLISH flags or packet identifier"));
            }
            if *qos != 0 {
                validate_id(packet_id.ok_or_else(|| malformed("missing packet identifier"))?)?;
            }
            validate_topic(topic)?;
            // Reject impossible wire lengths before allocating a second payload.
            let variable_size = 2 + topic.len() + if *qos == 0 { 0 } else { 2 };
            if payload.len() > 268_435_455 - variable_size {
                return Err(ProtocolError::PacketTooLarge);
            }
        }
        Packet::PubAck { packet_id }
        | Packet::PubRec { packet_id }
        | Packet::PubRel { packet_id }
        | Packet::PubComp { packet_id }
        | Packet::UnsubAck { packet_id } => validate_id(*packet_id)?,
        Packet::Subscribe { packet_id, filters } => {
            validate_id(*packet_id)?;
            if filters.is_empty() {
                return Err(malformed("empty subscription"));
            }
            for (filter, qos) in filters {
                validate_string(filter)?;
                if filter.is_empty() {
                    return Err(malformed("empty topic filter"));
                }
                if *qos > 2 {
                    return Err(malformed("invalid subscription QoS"));
                }
            }
        }
        Packet::SubAck { packet_id, codes } => {
            validate_id(*packet_id)?;
            if codes.is_empty() || codes.iter().any(|c| !matches!(c, 0 | 1 | 2 | 128)) {
                return Err(malformed("invalid SUBACK codes"));
            }
        }
        Packet::Unsubscribe { packet_id, filters } => {
            validate_id(*packet_id)?;
            if filters.is_empty() {
                return Err(malformed("empty unsubscribe"));
            }
            for filter in filters {
                validate_string(filter)?;
                if filter.is_empty() {
                    return Err(malformed("empty unsubscribe filter"));
                }
            }
        }
        Packet::PingReq | Packet::PingResp | Packet::Disconnect => {}
        Packet::Auth => return Err(unsupported("AUTH requires MQTT5")),
    }
    Ok(())
}
/// Encode a supported packet, validating the same invariants as the decoder.
pub fn encode(packet: &Packet) -> Result<Vec<u8>, ProtocolError> {
    validate_packet(packet)?;
    let mut body = Vec::new();
    let header = match packet {
        Packet::Connect {
            client_id,
            clean_session,
            keep_alive,
            will,
            username,
            password,
        } => {
            string(&mut body, "MQTT")?;
            let mut flags = if *clean_session { 2 } else { 0 };
            if let Some(will) = will {
                flags |= 4 | (will.qos << 3) | (u8::from(will.retain) << 5);
            }
            if username.is_some() {
                flags |= 0x80;
            }
            if password.is_some() {
                flags |= 0x40;
            }
            body.extend([4, flags]);
            body.extend(keep_alive.to_be_bytes());
            string(&mut body, client_id)?;
            if let Some(will) = will {
                string(&mut body, &will.topic)?;
                let length = u16::try_from(will.payload.len())
                    .map_err(|_| malformed("Will payload too long"))?;
                body.extend(length.to_be_bytes());
                body.extend(&will.payload);
            }
            if let Some(username) = username {
                string(&mut body, username)?;
            }
            if let Some(password) = password {
                let bytes = password.as_bytes();
                let length =
                    u16::try_from(bytes.len()).map_err(|_| malformed("password too long"))?;
                body.extend(length.to_be_bytes());
                body.extend(bytes);
            }
            0x10
        }
        Packet::ConnAck {
            session_present,
            code,
        } => {
            body.extend([u8::from(*session_present), *code]);
            0x20
        }
        Packet::Publish {
            topic,
            payload,
            qos,
            packet_id,
            dup,
            retain,
        } => {
            string(&mut body, topic)?;
            if let Some(id) = packet_id {
                body.extend(id.to_be_bytes());
            }
            body.extend(payload);
            0x30 | (u8::from(*dup) << 3) | (*qos << 1) | u8::from(*retain)
        }
        Packet::PubAck { packet_id } => {
            body.extend(packet_id.to_be_bytes());
            0x40
        }
        Packet::PubRec { packet_id } => {
            body.extend(packet_id.to_be_bytes());
            0x50
        }
        Packet::PubRel { packet_id } => {
            body.extend(packet_id.to_be_bytes());
            0x62
        }
        Packet::PubComp { packet_id } => {
            body.extend(packet_id.to_be_bytes());
            0x70
        }
        Packet::Subscribe { packet_id, filters } => {
            body.extend(packet_id.to_be_bytes());
            for (filter, qos) in filters {
                string(&mut body, filter)?;
                body.push(*qos);
            }
            0x82
        }
        Packet::SubAck { packet_id, codes } => {
            body.extend(packet_id.to_be_bytes());
            body.extend(codes);
            0x90
        }
        Packet::Unsubscribe { packet_id, filters } => {
            body.extend(packet_id.to_be_bytes());
            for filter in filters {
                string(&mut body, filter)?;
            }
            0xa2
        }
        Packet::UnsubAck { packet_id } => {
            body.extend(packet_id.to_be_bytes());
            0xb0
        }
        Packet::PingReq => 0xc0,
        Packet::PingResp => 0xd0,
        Packet::Disconnect => 0xe0,
        Packet::Auth => return Err(unsupported("AUTH requires MQTT5")),
    };
    if body.len() > 268_435_455 {
        return Err(ProtocolError::PacketTooLarge);
    }
    let mut out = vec![header];
    let mut n = body.len();
    loop {
        let mut b = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            b |= 128;
        }
        out.push(b);
        if n == 0 {
            break;
        }
    }
    out.extend(body);
    Ok(out)
}
