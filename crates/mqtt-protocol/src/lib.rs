//! Incremental, allocation-bounded MQTT 3.1.1 Phase 1 packet codec.
use std::{error::Error, fmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    Connect {
        client_id: String,
        clean_session: bool,
        keep_alive: u16,
    },
    ConnAck {
        session_present: bool,
        code: u8,
    },
    Publish {
        topic: String,
        payload: Vec<u8>,
    },
    Subscribe {
        packet_id: u16,
        filters: Vec<(String, u8)>,
    },
    SubAck {
        packet_id: u16,
        codes: Vec<u8>,
    },
    Unsubscribe {
        packet_id: u16,
        filters: Vec<String>,
    },
    UnsubAck {
        packet_id: u16,
    },
    PingReq,
    PingResp,
    Disconnect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    Malformed(String),
    UnsupportedVersion,
    UnsupportedFeature(String),
    PacketTooLarge,
}
impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(s) => write!(f, "malformed MQTT packet: {s}"),
            Self::UnsupportedVersion => write!(f, "unsupported MQTT version"),
            Self::UnsupportedFeature(s) => write!(f, "unsupported MQTT feature: {s}"),
            Self::PacketTooLarge => write!(f, "MQTT packet exceeds limit"),
        }
    }
}
impl Error for ProtocolError {}
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
        if id == 0 {
            return Err(malformed("zero packet identifier"));
        }
        Ok(id)
    }
    fn string(&mut self) -> Result<String, ProtocolError> {
        let n = self.u16()? as usize;
        let s = std::str::from_utf8(self.take(n)?).map_err(|_| malformed("invalid UTF-8"))?;
        if !valid_utf8(s) {
            return Err(malformed("forbidden UTF-8 code point"));
        }
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
        if qos != 0 || flags & 1 != 0 {
            return Err(unsupported("QoS or retained publish"));
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
            if flags & 0xfc != 0 {
                return Err(unsupported("Will or credentials"));
            }
            if flags & 2 == 0 {
                return Err(unsupported("persistent session"));
            }
            let keep_alive = r.u16()?;
            let client_id = r.string()?;
            if client_id.is_empty() {
                return Err(unsupported("server-assigned client identifier"));
            }
            Packet::Connect {
                client_id,
                clean_session: true,
                keep_alive,
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
            if topic.is_empty() || topic.contains(['+', '#']) {
                return Err(malformed("invalid topic name"));
            }
            let payload = r.take(r.bytes.len() - r.pos)?.to_vec();
            Packet::Publish { topic, payload }
        }
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
        _ => return Err(unsupported("QoS acknowledgement")),
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
/// Encode a supported packet, validating the same invariants as the decoder.
pub fn encode(packet: &Packet) -> Result<Vec<u8>, ProtocolError> {
    let mut body = Vec::new();
    let header = match packet {
        Packet::Connect {
            client_id,
            clean_session,
            keep_alive,
        } => {
            string(&mut body, "MQTT")?;
            body.extend([4, if *clean_session { 2 } else { 0 }]);
            body.extend(keep_alive.to_be_bytes());
            string(&mut body, client_id)?;
            0x10
        }
        Packet::ConnAck {
            session_present,
            code,
        } => {
            body.extend([u8::from(*session_present), *code]);
            0x20
        }
        Packet::Publish { topic, payload } => {
            string(&mut body, topic)?;
            body.extend(payload);
            0x30
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
    decode(&out, out.len())?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn packets() -> Vec<Packet> {
        vec![
            Packet::Connect {
                client_id: "client-1".into(),
                clean_session: true,
                keep_alive: 30,
            },
            Packet::ConnAck {
                session_present: false,
                code: 0,
            },
            Packet::Publish {
                topic: "sensors/温度".into(),
                payload: vec![0, 255, 1],
            },
            Packet::Subscribe {
                packet_id: 42,
                filters: vec![("a/+".into(), 2), ("#".into(), 1)],
            },
            Packet::SubAck {
                packet_id: 42,
                codes: vec![0, 128],
            },
            Packet::Unsubscribe {
                packet_id: 65535,
                filters: vec!["#".into()],
            },
            Packet::UnsubAck { packet_id: 65535 },
            Packet::PingReq,
            Packet::PingResp,
            Packet::Disconnect,
        ]
    }
    #[test]
    fn all_packets_roundtrip_and_fragment() {
        for p in packets() {
            let bytes = encode(&p).unwrap();
            for n in 0..bytes.len() {
                assert_eq!(decode(&bytes[..n], 65536).unwrap(), None, "{p:?}, {n}");
            }
            assert_eq!(decode(&bytes, 65536).unwrap(), Some((p, bytes.len())));
        }
    }
    #[test]
    fn concatenation_consumes_one_packet() {
        assert_eq!(
            decode(&[0xc0, 0, 0xe0, 0], 2).unwrap(),
            Some((Packet::PingReq, 2))
        );
    }
    #[test]
    fn multibyte_remaining_length() {
        for n in [127, 128, 16383, 16384, 65536] {
            let p = Packet::Publish {
                topic: "a".into(),
                payload: vec![42; n],
            };
            let bytes = encode(&p).unwrap();
            assert_eq!(decode(&bytes, bytes.len()).unwrap(), Some((p, bytes.len())));
            assert_eq!(
                decode(&bytes, bytes.len() - 1),
                Err(ProtocolError::PacketTooLarge)
            );
        }
    }
    #[test]
    fn reject_malformed_headers_and_lengths() {
        for bytes in [
            vec![0, 0],
            vec![0xf0, 0],
            vec![0xc1, 0],
            vec![0x80, 0],
            vec![0x3e, 0],
            vec![0x38, 0],
            vec![0xc0, 1, 0],
            vec![0xc0, 128, 128, 128, 128],
        ] {
            assert!(
                matches!(decode(&bytes, 65536), Err(ProtocolError::Malformed(_))),
                "{bytes:?}"
            );
        }
        assert_eq!(
            decode(&[0x30, 255, 127], 1024),
            Err(ProtocolError::PacketTooLarge)
        );
        assert_eq!(decode(&[0x30, 128], 2), Err(ProtocolError::PacketTooLarge));
    }
    #[test]
    fn reject_invalid_payloads() {
        for bytes in [
            vec![0x30, 2, 0, 0],
            vec![0x30, 3, 0, 1, b'+'],
            vec![0x30, 3, 0, 1, 0],
            vec![0x30, 3, 0, 1, 255],
            vec![0x82, 2, 0, 1],
            vec![0x82, 6, 0, 0, 0, 1, b'a', 0],
            vec![0x82, 6, 0, 1, 0, 1, b'a', 3],
            vec![0xa2, 2, 0, 1],
            vec![0xb0, 2, 0, 0],
            vec![0x90, 3, 0, 1, 3],
            vec![0x20, 2, 1, 1],
            vec![0x20, 2, 0, 6],
        ] {
            assert!(decode(&bytes, 65536).is_err(), "{bytes:?}");
        }
    }
    #[test]
    fn connect_rejections_and_version() {
        let p = Packet::Connect {
            client_id: "c".into(),
            clean_session: true,
            keep_alive: 0,
        };
        let base = encode(&p).unwrap();
        let mut b = base.clone();
        b[8] = 5;
        assert_eq!(decode(&b, 1024), Err(ProtocolError::UnsupportedVersion));
        for flags in [1, 0x1a, 0x22, 0x42] {
            let mut b = base.clone();
            b[9] = flags;
            assert!(matches!(decode(&b, 1024), Err(ProtocolError::Malformed(_))));
        }
        for flags in [0, 6, 0x82, 0xc2] {
            let mut b = base.clone();
            b[9] = flags;
            assert!(matches!(
                decode(&b, 1024),
                Err(ProtocolError::UnsupportedFeature(_))
            ));
        }
    }
    #[test]
    fn unsupported_publish_features() {
        for h in [0x31, 0x32, 0x34] {
            assert!(matches!(
                decode(&[h, 0], 1024),
                Err(ProtocolError::UnsupportedFeature(_))
            ));
        }
    }
    #[test]
    fn encoder_checks_invariants() {
        assert!(
            encode(&Packet::Publish {
                topic: "bad/#".into(),
                payload: vec![]
            })
            .is_err()
        );
        assert!(
            encode(&Packet::Publish {
                topic: "\u{fdd0}".into(),
                payload: vec![]
            })
            .is_err()
        );
        assert!(encode(&Packet::UnsubAck { packet_id: 0 }).is_err());
        assert!(
            encode(&Packet::Subscribe {
                packet_id: 1,
                filters: vec![]
            })
            .is_err()
        );
        assert!(
            encode(&Packet::Publish {
                topic: "a".repeat(65536),
                payload: vec![]
            })
            .is_err()
        );
    }
    #[test]
    fn arbitrary_short_inputs_do_not_panic() {
        for a in 0..=255 {
            for b in 0..=255 {
                let _ = decode(&[a, b], 4096);
            }
        }
    }
}
