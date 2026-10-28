//! Wire syntax is independent of the transport's connection-local alias maps.
use super::*;

fn publication(topic: &str, alias: u16, qos: u8) -> Frame {
    let mut frame = Frame::v5(Packet::Publish {
        topic: topic.into(),
        payload: b"payload\0\xff".to_vec(),
        qos,
        packet_id: (qos != 0).then_some(19),
        dup: false,
        retain: true,
    });
    frame.properties = vec![
        Property::UserProperty("ordered".into(), "first".into()),
        Property::TopicAlias(alias),
        Property::UserProperty("ordered".into(), "last".into()),
    ];
    frame
}

fn round_trip(frame: &Frame) -> Vec<u8> {
    let wire = encode_frame(frame).unwrap();
    let (decoded, consumed) = decode_frame(&wire, wire.len(), Some(ProtocolVersion::V5))
        .unwrap()
        .unwrap();
    assert_eq!(decoded, *frame);
    assert_eq!(consumed, wire.len());
    wire
}

fn packet(header: u8, body: &[u8]) -> Vec<u8> {
    let mut wire = vec![header];
    varint(&mut wire, body.len()).unwrap();
    wire.extend_from_slice(body);
    wire
}

#[test]
fn binding_reference_rebinding_and_qos_flags_remain_wire_only() {
    for qos in 0..=2 {
        for alias in [1, 32, u16::MAX] {
            for topic in ["设备/温度", "", "changed/topic", ""] {
                let mut frame = publication(topic, alias, qos);
                if let Packet::Publish { dup, .. } = &mut frame.packet {
                    *dup = qos != 0;
                }
                round_trip(&frame);
            }
        }
    }
    // The codec has no connection table: an empty reference remains empty. Even
    // a newly connected peer's unknown reference has valid syntax; transport
    // must reject its missing binding rather than silently inventing a topic.
    let frame = publication("", 1, 1);
    let wire = round_trip(&frame);
    assert_eq!(
        decode_frame(&wire, wire.len(), Some(ProtocolVersion::V5))
            .unwrap()
            .unwrap()
            .0,
        frame
    );
}

#[test]
fn zero_duplicate_and_missing_alias_have_distinct_protocol_errors() {
    let zero = packet(0x30, &[0, 0, 3, 0x23, 0, 0]);
    assert!(matches!(
        decode_frame(&zero, zero.len(), Some(ProtocolVersion::V5)),
        Err(ProtocolError::Mqtt5(code)) if code.value() == 0x94
    ));
    assert!(matches!(
        encode_frame(&publication("topic", 0, 0)),
        Err(ProtocolError::Mqtt5(code)) if code.value() == 0x94
    ));
    let duplicate = packet(0x30, &[0, 1, b't', 6, 0x23, 0, 1, 0x23, 0, 2]);
    assert!(matches!(
        decode_frame(&duplicate, duplicate.len(), Some(ProtocolVersion::V5)),
        Err(ProtocolError::ProtocolViolation(_))
    ));
    let mut duplicate = publication("t", 1, 0);
    duplicate.properties.push(Property::TopicAlias(2));
    assert!(matches!(
        encode_frame(&duplicate),
        Err(ProtocolError::ProtocolViolation(_))
    ));
    let missing = packet(0x30, &[0, 0, 0]);
    assert!(matches!(
        decode_frame(&missing, missing.len(), Some(ProtocolVersion::V5)),
        Err(ProtocolError::ProtocolViolation(_))
    ));
}

#[test]
fn alias_syntax_does_not_bypass_utf8_or_topic_validation() {
    for topic in ["wild/+", "wild/#", "bad\0topic"] {
        assert!(encode_frame(&publication(topic, 1, 0)).is_err());
    }
    let invalid_utf8 = packet(0x30, &[0, 1, 0xff, 3, 0x23, 0, 1]);
    assert!(matches!(
        decode_frame(&invalid_utf8, invalid_utf8.len(), Some(ProtocolVersion::V5)),
        Err(ProtocolError::Malformed(_))
    ));
    let wildcard = packet(0x30, &[0, 1, b'+', 3, 0x23, 0, 1]);
    assert!(decode_frame(&wildcard, wildcard.len(), Some(ProtocolVersion::V5)).is_err());
}

#[test]
fn complete_packet_limits_include_alias_and_both_variable_integer_widths() {
    for payload_length in [90, 91, 92, 93, 127, 128, 16_383, 16_384] {
        for property_length in [0, 119, 120, 121, 122] {
            let mut frame = publication("设备/long/topic", u16::MAX, 2);
            if let Packet::Publish { payload, .. } = &mut frame.packet {
                *payload = vec![0xa5; payload_length];
            }
            frame
                .properties
                .push(Property::ContentType("x".repeat(property_length)));
            let wire = round_trip(&frame);
            for prefix in [0, 1, 2, 3, wire.len() - 1] {
                assert_eq!(
                    decode_frame(&wire[..prefix], wire.len(), Some(ProtocolVersion::V5)).unwrap(),
                    None
                );
            }
            assert!(matches!(
                decode_frame(&wire, wire.len() - 1, Some(ProtocolVersion::V5)),
                Err(ProtocolError::PacketTooLarge)
            ));
        }
    }
}

#[test]
fn coalesced_bind_and_reference_keep_distinct_packet_boundaries() {
    let binding = publication("bound/topic", 1, 1);
    let reference = publication("", 1, 2);
    let first = encode_frame(&binding).unwrap();
    let second = encode_frame(&reference).unwrap();
    let mut joined = first.clone();
    joined.extend_from_slice(&second);
    let (decoded, used) = decode_frame(&joined, first.len(), Some(ProtocolVersion::V5))
        .unwrap()
        .unwrap();
    assert_eq!(decoded, binding);
    assert_eq!(used, first.len());
    assert_eq!(
        decode_frame(&joined[used..], second.len(), Some(ProtocolVersion::V5))
            .unwrap()
            .unwrap(),
        (reference, second.len())
    );
}

#[test]
fn alias_maximum_is_connect_connack_only_and_will_alias_is_forbidden() {
    for maximum in [0, 1, 32, u16::MAX] {
        let mut connect = Frame::v5(Packet::Connect {
            client_id: "alias-client".into(),
            clean_session: true,
            keep_alive: 0,
            will: None,
            username: None,
            password: None,
        });
        connect
            .properties
            .push(Property::TopicAliasMaximum(maximum));
        round_trip(&connect);
        let mut connack = Frame::v5(Packet::ConnAck {
            session_present: false,
            code: 0,
        });
        connack
            .properties
            .push(Property::TopicAliasMaximum(maximum));
        round_trip(&connack);
        if let Packet::Connect { will, .. } = &mut connect.packet {
            *will = Some(Will {
                topic: "will".into(),
                payload: vec![],
                qos: 1,
                retain: false,
            });
        }
        connect.will_properties.push(Property::TopicAlias(1));
        assert!(matches!(
            encode_frame(&connect),
            Err(ProtocolError::Malformed(_))
        ));
    }
    let mut publish = publication("t", 1, 0);
    publish.properties.push(Property::TopicAliasMaximum(32));
    assert!(matches!(
        encode_frame(&publish),
        Err(ProtocolError::Malformed(_))
    ));

    // Attack the decoder directly: the encoder refuses to construct this Will.
    let mut body = b"\x00\x04MQTT\x05\x0e\x00\x00\x00".to_vec();
    string(&mut body, "client").unwrap();
    body.extend_from_slice(&[3, 0x23, 0, 1]);
    string(&mut body, "will/topic").unwrap();
    binary(&mut body, b"will").unwrap();
    let invalid = packet(0x10, &body);
    assert!(matches!(
        decode_frame(&invalid, invalid.len(), None),
        Err(ProtocolError::Malformed(_))
    ));

    let mut body = b"\x00\x04MQTT\x05\x02\x00\x00".to_vec();
    body.extend_from_slice(&[6, 0x22, 0, 32, 0x22, 0, 1]);
    string(&mut body, "client").unwrap();
    let duplicate = packet(0x10, &body);
    assert!(matches!(
        decode_frame(&duplicate, duplicate.len(), None),
        Err(ProtocolError::ProtocolViolation(_))
    ));
}
