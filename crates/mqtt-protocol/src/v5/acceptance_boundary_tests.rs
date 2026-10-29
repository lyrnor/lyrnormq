//! Independent normative tables and CONNECT flag correlations, not runtime roles.
use super::*;

// Literal Tables3-1,3-4..3-11. Endpoint-specific "Sent by" restrictions are a
// separate connection-aware acceptance gate; this is symmetric packet syntax.
const CASES: &[(u8, &[u8])] = &[
    (
        2,
        &[
            0, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 140, 144, 149, 151, 153, 154,
            155, 156, 157, 159,
        ],
    ),
    (4, &[0, 16, 128, 131, 135, 144, 145, 151, 153]),
    (5, &[0, 16, 128, 131, 135, 144, 145, 151, 153]),
    (6, &[0, 146]),
    (7, &[0, 146]),
    (9, &[0, 1, 2, 128, 131, 135, 143, 145, 151, 158, 161, 162]),
    (11, &[0, 17, 128, 131, 135, 143, 145]),
    (
        14,
        &[
            0, 4, 128, 129, 130, 131, 135, 137, 139, 141, 142, 143, 144, 147, 148, 149, 150, 151,
            152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162,
        ],
    ),
    (15, &[0, 24, 25]),
];

fn literal_reason_wire(kind: u8, code: u8) -> Vec<u8> {
    match kind {
        2 => vec![0x20, 3, 0, code, 0],
        4..=7 => vec![
            (kind << 4) | if kind == 6 { 2 } else { 0 },
            4,
            0,
            7,
            code,
            0,
        ],
        9 | 11 => vec![kind << 4, 4, 0, 7, 0, code],
        14 => vec![0xe0, 2, code, 0],
        15 => {
            let mut bytes = vec![0xf0, 10, code, 8, 0x15, 0, 5];
            bytes.extend_from_slice(b"PLAIN");
            bytes
        }
        _ => unreachable!(),
    }
}
fn reason_frame(kind: u8, code: u8) -> Result<Frame, ProtocolError> {
    let packet = match kind {
        2 => Packet::ConnAck {
            session_present: false,
            code,
        },
        4 => Packet::PubAck { packet_id: 7 },
        5 => Packet::PubRec { packet_id: 7 },
        6 => Packet::PubRel { packet_id: 7 },
        7 => Packet::PubComp { packet_id: 7 },
        9 => Packet::SubAck {
            packet_id: 7,
            codes: vec![code],
        },
        11 => Packet::UnsubAck { packet_id: 7 },
        14 => Packet::Disconnect,
        15 => Packet::Auth,
        _ => unreachable!(),
    };
    let mut frame = Frame::v5(packet);
    if kind == 11 {
        frame.reason_codes.push(ReasonCode::try_from(code)?);
    } else if matches!(kind, 4..=7 | 14 | 15) {
        frame.reason_code = ReasonCode::try_from(code)?;
    }
    if kind == 15 {
        frame
            .properties
            .push(Property::AuthenticationMethod("PLAIN".into()));
    }
    Ok(frame)
}

#[test]
fn every_reason_byte_is_checked_against_its_literal_packet_table() {
    for &(kind, allowed) in CASES {
        for code in 0..=u8::MAX {
            let wire = literal_reason_wire(kind, code);
            let decoded = decode_frame(&wire, 4096, Some(ProtocolVersion::V5));
            let frame = reason_frame(kind, code);
            let encoded = frame.as_ref().map_err(Clone::clone).and_then(encode_frame);
            let expected = allowed.contains(&code);
            assert_eq!(decoded.is_ok(), expected, "decode kind={kind} code={code}");
            assert_eq!(encoded.is_ok(), expected, "encode kind={kind} code={code}");
            if expected {
                let (decoded, used) = decoded.unwrap().unwrap();
                assert_eq!(used, wire.len());
                assert_eq!(decoded, frame.unwrap());
                let encoded = encoded.unwrap();
                let (again, used) = decode_frame(&encoded, 4096, Some(ProtocolVersion::V5))
                    .unwrap()
                    .unwrap();
                assert_eq!(again, decoded);
                assert_eq!(used, encoded.len());
            }
        }
    }
}

fn literal_connect(flags: u8) -> Vec<u8> {
    let mut body = b"\x00\x04MQTT\x05".to_vec();
    body.extend_from_slice(&[flags, 0, 30, 0, 0, 1, b'c']);
    if flags & 4 != 0 {
        // Will Properties length0, Topic "t", arbitrary binary Will payload.
        body.extend_from_slice(b"\x00\x00\x01t\x00\x02\x00\xff");
    }
    if flags & 128 != 0 {
        body.extend_from_slice(b"\x00\x01u");
    }
    if flags & 64 != 0 {
        body.extend_from_slice(b"\x00\x02\x00\xff");
    }
    let mut bytes = vec![0x10, u8::try_from(body.len()).unwrap()];
    bytes.extend(body);
    bytes
}

#[test]
fn connect_flag_matrix_correlates_will_fields_and_independent_v5_credentials() {
    for flags in 0..=u8::MAX {
        let will = flags & 4 != 0;
        let will_qos = (flags >> 3) & 3;
        let expected = flags & 1 == 0 && will_qos != 3 && (will || flags & 0x38 == 0);
        let wire = literal_connect(flags);
        let result = decode_frame(&wire, 4096, None);
        assert_eq!(result.is_ok(), expected, "flags={flags:02x}");
        if expected {
            let (frame, consumed) = result.unwrap().unwrap();
            assert_eq!(consumed, wire.len());
            assert_eq!(encode_frame(&frame).unwrap(), wire);
            let Packet::Connect {
                will: value,
                username,
                password,
                ..
            } = &frame.packet
            else {
                panic!()
            };
            assert_eq!(value.is_some(), will);
            assert_eq!(username.is_some(), flags & 128 != 0);
            assert_eq!(password.is_some(), flags & 64 != 0);
            if let Some(value) = value {
                assert_eq!(value.qos, will_qos);
                assert_eq!(value.retain, flags & 32 != 0);
                assert_eq!(value.payload, b"\x00\xff");
            }
            // A complete frame with a truncated required field is malformed,
            // whereas a stream prefix with the original length is incomplete.
            let mut malformed = wire.clone();
            malformed.pop();
            malformed[1] -= 1;
            assert!(decode_frame(&malformed, 4096, None).is_err());
            for cut in 0..wire.len() {
                assert!(decode_frame(&wire[..cut], 4096, None).unwrap().is_none());
            }
        }
    }
}
