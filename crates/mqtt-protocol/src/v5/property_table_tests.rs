//! Independent Table2-4 oracle: literal property bytes and packet columns.
use super::*;
use crate::wire::Reader;

#[derive(Debug)]
struct Cell {
    property: Property,
    bytes: &'static [u8],
    contexts: &'static [u8],
}
fn table() -> Vec<Cell> {
    // Context0 denotes Will Properties. Packet columns are authored from Table2-4,
    // not derived from Property::id(), properties::permitted or its dispatch table.
    vec![
        Cell {
            property: Property::PayloadFormatIndicator(0),
            bytes: b"\x01\x00",
            contexts: &[0, 3],
        },
        Cell {
            property: Property::MessageExpiryInterval(7),
            bytes: b"\x02\x00\x00\x00\x07",
            contexts: &[0, 3],
        },
        Cell {
            property: Property::ContentType("x".into()),
            bytes: b"\x03\x00\x01x",
            contexts: &[0, 3],
        },
        Cell {
            property: Property::ResponseTopic("r".into()),
            bytes: b"\x08\x00\x01r",
            contexts: &[0, 3],
        },
        Cell {
            property: Property::CorrelationData(vec![0, 255]),
            bytes: b"\x09\x00\x02\x00\xff",
            contexts: &[0, 3],
        },
        Cell {
            property: Property::SubscriptionIdentifier(128),
            bytes: b"\x0b\x80\x01",
            contexts: &[3, 8],
        },
        Cell {
            property: Property::SessionExpiryInterval(7),
            bytes: b"\x11\x00\x00\x00\x07",
            contexts: &[1, 2, 14],
        },
        Cell {
            property: Property::AssignedClientIdentifier("i".into()),
            bytes: b"\x12\x00\x01i",
            contexts: &[2],
        },
        Cell {
            property: Property::ServerKeepAlive(7),
            bytes: b"\x13\x00\x07",
            contexts: &[2],
        },
        Cell {
            property: Property::AuthenticationMethod("m".into()),
            bytes: b"\x15\x00\x01m",
            contexts: &[1, 2, 15],
        },
        Cell {
            property: Property::AuthenticationData(vec![0, 255]),
            bytes: b"\x16\x00\x02\x00\xff",
            contexts: &[1, 2, 15],
        },
        Cell {
            property: Property::RequestProblemInformation(0),
            bytes: b"\x17\x00",
            contexts: &[1],
        },
        Cell {
            property: Property::WillDelayInterval(7),
            bytes: b"\x18\x00\x00\x00\x07",
            contexts: &[0],
        },
        Cell {
            property: Property::RequestResponseInformation(0),
            bytes: b"\x19\x00",
            contexts: &[1],
        },
        Cell {
            property: Property::ResponseInformation("r".into()),
            bytes: b"\x1a\x00\x01r",
            contexts: &[2],
        },
        Cell {
            property: Property::ServerReference("s".into()),
            bytes: b"\x1c\x00\x01s",
            contexts: &[2, 14],
        },
        Cell {
            property: Property::ReasonString("r".into()),
            bytes: b"\x1f\x00\x01r",
            contexts: &[2, 4, 5, 6, 7, 9, 11, 14, 15],
        },
        Cell {
            property: Property::ReceiveMaximum(7),
            bytes: b"\x21\x00\x07",
            contexts: &[1, 2],
        },
        Cell {
            property: Property::TopicAliasMaximum(0),
            bytes: b"\x22\x00\x00",
            contexts: &[1, 2],
        },
        Cell {
            property: Property::TopicAlias(7),
            bytes: b"\x23\x00\x07",
            contexts: &[3],
        },
        Cell {
            property: Property::MaximumQos(1),
            bytes: b"\x24\x01",
            contexts: &[2],
        },
        Cell {
            property: Property::RetainAvailable(1),
            bytes: b"\x25\x01",
            contexts: &[2],
        },
        Cell {
            property: Property::UserProperty("k".into(), "v".into()),
            bytes: b"\x26\x00\x01k\x00\x01v",
            contexts: &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 15],
        },
        Cell {
            property: Property::MaximumPacketSize(7),
            bytes: b"\x27\x00\x00\x00\x07",
            contexts: &[1, 2],
        },
        Cell {
            property: Property::WildcardSubscriptionAvailable(1),
            bytes: b"\x28\x01",
            contexts: &[2],
        },
        Cell {
            property: Property::SubscriptionIdentifierAvailable(1),
            bytes: b"\x29\x01",
            contexts: &[2],
        },
        Cell {
            property: Property::SharedSubscriptionAvailable(1),
            bytes: b"\x2a\x01",
            contexts: &[2],
        },
    ]
}
fn vint(mut n: usize) -> Vec<u8> {
    let mut result = Vec::new();
    loop {
        let digit = (n % 128) as u8;
        n /= 128;
        result.push(digit | if n == 0 { 0 } else { 128 });
        if n == 0 {
            return result;
        }
    }
}
fn field(bytes: &[u8]) -> Vec<u8> {
    let mut out = vint(bytes.len());
    out.extend(bytes);
    out
}
fn inputs(cell: &Cell, context: u8) -> (Vec<Property>, Vec<u8>) {
    let mut properties = Vec::new();
    let mut bytes = Vec::new();
    // Supply other required fields so an invalid-context result cannot be merely
    // an absent-method error. The target property itself is never duplicated.
    if (context == 15 && cell.bytes[0] != 0x15)
        || (matches!(context, 1 | 2) && cell.bytes[0] == 0x16)
    {
        properties.push(Property::AuthenticationMethod("m".into()));
        bytes.extend(b"\x15\x00\x01m");
    }
    properties.push(cell.property.clone());
    bytes.extend(cell.bytes);
    (properties, bytes)
}
fn model(context: u8, properties: Vec<Property>) -> Frame {
    let packet = match context {
        0 | 1 => Packet::Connect {
            client_id: "c".into(),
            clean_session: true,
            keep_alive: 30,
            will: (context == 0).then(|| Will {
                topic: "t".into(),
                payload: vec![b'w'],
                qos: 0,
                retain: false,
            }),
            username: None,
            password: None,
        },
        2 => Packet::ConnAck {
            session_present: false,
            code: 0,
        },
        3 => Packet::Publish {
            topic: "t".into(),
            payload: vec![b'x'],
            qos: 0,
            packet_id: None,
            dup: false,
            retain: false,
        },
        4 => Packet::PubAck { packet_id: 1 },
        5 => Packet::PubRec { packet_id: 1 },
        6 => Packet::PubRel { packet_id: 1 },
        7 => Packet::PubComp { packet_id: 1 },
        8 => Packet::Subscribe {
            packet_id: 1,
            filters: vec![("t".into(), 0)],
        },
        9 => Packet::SubAck {
            packet_id: 1,
            codes: vec![0],
        },
        10 => Packet::Unsubscribe {
            packet_id: 1,
            filters: vec!["t".into()],
        },
        11 => Packet::UnsubAck { packet_id: 1 },
        12 => Packet::PingReq,
        13 => Packet::PingResp,
        14 => Packet::Disconnect,
        15 => Packet::Auth,
        _ => unreachable!(),
    };
    let mut f = Frame::v5(packet);
    if context == 0 {
        f.will_properties = properties;
    } else {
        f.properties = properties;
    }
    if context == 8 {
        f.subscription_options = vec![0];
    }
    if context == 11 {
        f.reason_codes = vec![ReasonCode::SUCCESS];
    }
    if context == 15 {
        f.reason_code = ReasonCode::try_from(0x18).unwrap();
    }
    f
}
fn packet(context: u8, bytes: &[u8]) -> Vec<u8> {
    let p = field(bytes);
    let (header, mut body) = match context {
        0 => {
            let mut b = b"\x00\x04MQTT\x05\x06\x00\x1e\x00\x00\x01c".to_vec();
            b.extend(p);
            b.extend(b"\x00\x01t\x00\x01w");
            (0x10, b)
        }
        1 => {
            let mut b = b"\x00\x04MQTT\x05\x02\x00\x1e".to_vec();
            b.extend(p);
            b.extend(b"\x00\x01c");
            (0x10, b)
        }
        2 => {
            let mut b = vec![0, 0];
            b.extend(p);
            (0x20, b)
        }
        3 => {
            let mut b = b"\x00\x01t".to_vec();
            b.extend(p);
            b.push(b'x');
            (0x30, b)
        }
        4..=7 => {
            let mut b = vec![0, 1, 0];
            b.extend(p);
            (if context == 6 { 0x62 } else { context << 4 }, b)
        }
        8 | 10 => {
            let mut b = vec![0, 1];
            b.extend(p);
            b.extend(b"\x00\x01t");
            if context == 8 {
                b.push(0);
            }
            ((context << 4) | 2, b)
        }
        9 | 11 => {
            let mut b = vec![0, 1];
            b.extend(p);
            b.push(0);
            (context << 4, b)
        }
        12 | 13 => (context << 4, p),
        14 | 15 => {
            let mut b = vec![if context == 15 { 0x18 } else { 0 }];
            b.extend(p);
            (context << 4, b)
        }
        _ => unreachable!(),
    };
    let mut out = vec![header];
    out.extend(vint(body.len()));
    out.append(&mut body);
    out
}
#[test]
fn table_2_4_all_property_packet_columns_are_checked_in_both_wire_directions() {
    let cells = table();
    assert_eq!(cells.len(), 27);
    let mut accepted = 0;
    let mut rejected = 0;
    for cell in cells {
        for context in 0..=15 {
            let (props, raw) = inputs(&cell, context);
            let f = model(context, props.clone());
            let wire = packet(context, &raw);
            let mut encoded = Vec::new();
            let result = properties::write(&mut encoded, &props, context);
            let property_wire = field(&raw);
            let mut r = Reader::new(&property_wire);
            if cell.contexts.contains(&context) {
                accepted += 1;
                result.unwrap();
                assert_eq!(encoded, property_wire, "{cell:?} context{context}");
                assert_eq!(properties::read(&mut r, context).unwrap(), props);
                assert!(r.done());
                assert_eq!(encode_frame(&f).unwrap(), wire, "{cell:?} context{context}");
                assert_eq!(
                    decode_frame(&wire, 4096, Some(ProtocolVersion::V5)).unwrap(),
                    Some((f, wire.len()))
                );
            } else {
                rejected += 1;
                assert!(
                    matches!(result, Err(ProtocolError::Malformed(_))),
                    "{cell:?} context{context}: {result:?}"
                );
                assert!(matches!(
                    properties::read(&mut r, context),
                    Err(ProtocolError::Malformed(_))
                ));
                assert!(encode_frame(&f).is_err(), "{cell:?} context{context}");
                assert!(
                    decode_frame(&wire, 4096, Some(ProtocolVersion::V5)).is_err(),
                    "{cell:?} context{context}"
                );
            }
        }
    }
    assert_eq!(accepted, 64);
    assert_eq!(rejected, 368);
    assert_eq!(accepted + rejected, 432);
}
#[test]
fn table_2_4_each_wire_type_rejects_a_truncated_value_in_every_allowed_column() {
    for cell in table() {
        for &context in cell.contexts {
            let (_, mut raw) = inputs(&cell, context);
            raw.pop();
            let data = field(&raw);
            let mut r = Reader::new(&data);
            assert!(
                matches!(
                    properties::read(&mut r, context),
                    Err(ProtocolError::Malformed(_))
                ),
                "{cell:?} context{context}"
            );
            assert!(decode_frame(&packet(context, &raw), 4096, Some(ProtocolVersion::V5)).is_err());
        }
    }
}
#[test]
fn table_2_4_unassigned_and_multibyte_property_identifiers_are_malformed() {
    let known: Vec<_> = table().iter().map(|c| c.bytes[0] as usize).collect();
    for id in (0..=127).chain([128, 16384, 268435455]) {
        if known.contains(&id) {
            continue;
        }
        for context in 0..=15 {
            let mut raw = if context == 15 {
                b"\x15\x00\x01m".to_vec()
            } else {
                Vec::new()
            };
            raw.extend(vint(id));
            raw.extend([0, 0, 0, 0]);
            let data = field(&raw);
            let mut r = Reader::new(&data);
            assert!(
                matches!(
                    properties::read(&mut r, context),
                    Err(ProtocolError::Malformed(_))
                ),
                "id{id} context{context}"
            );
        }
    }
}
fn content_wire(value: &[u8]) -> Vec<u8> {
    let mut bytes = vec![3];
    bytes.extend((value.len() as u16).to_be_bytes());
    bytes.extend(value);
    packet(3, &bytes)
}
fn assert_string_policy(value: &str, accepted: bool) {
    let p = Property::ContentType(value.into());
    let wire = content_wire(value.as_bytes());
    let f = model(3, vec![p]);
    if accepted {
        assert_eq!(encode_frame(&f).unwrap(), wire);
        assert_eq!(
            decode_frame(&wire, 4096, Some(ProtocolVersion::V5)).unwrap(),
            Some((f, wire.len()))
        );
    } else {
        assert!(matches!(encode_frame(&f), Err(ProtocolError::Malformed(_))));
        assert!(matches!(
            decode_frame(&wire, 4096, Some(ProtocolVersion::V5)),
            Err(ProtocolError::Malformed(_))
        ));
    }
}
#[test]
fn utf8_receiver_policy_accepts_optional_controls_and_rejects_noncharacters() {
    // Section1.5.4 lets receivers reject these optional disallowed code points.
    // This codec accepts non-NUL C0/C1 controls, but elects to reject noncharacters.
    for cp in (1..=31).chain(127..=159) {
        assert_string_policy(&char::from_u32(cp).unwrap().to_string(), true);
    }
    for cp in 0xfdd0..=0xfdef {
        assert_string_policy(&char::from_u32(cp).unwrap().to_string(), false);
    }
    for plane in 0..=16 {
        for tail in [0xfffe, 0xffff] {
            assert_string_policy(
                &char::from_u32((plane << 16) | tail).unwrap().to_string(),
                false,
            );
        }
    }
}
#[test]
fn utf8_mandatory_invalid_scalars_nul_and_byte_preserving_bom_are_separate() {
    assert_string_policy("\0", false);
    for bad in [
        &b"\xed\xa0\x80"[..],
        &b"\xed\xbf\xbf"[..],
        &b"\xc0\x80"[..],
        &b"\xf4\x90\x80\x80"[..],
    ] {
        assert!(matches!(
            decode_frame(&content_wire(bad), 4096, Some(ProtocolVersion::V5)),
            Err(ProtocolError::Malformed(_))
        ));
    }
    for valid in ["\u{feff}", "a\u{feff}b", "é", "e\u{301}", " x "] {
        assert_string_policy(valid, true);
    }
    assert_ne!(
        content_wire("é".as_bytes()),
        content_wire("e\u{301}".as_bytes())
    );
}
