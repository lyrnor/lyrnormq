use super::*;
use crate::{Properties, Property};

fn connect() -> Frame {
    Frame::v5(Packet::Connect {
        client_id: "test".into(),
        clean_session: true,
        keep_alive: 10,
        will: None,
        username: None,
        password: None,
    })
}
fn publish() -> Frame {
    Frame::v5(Packet::Publish {
        topic: "a/b".into(),
        payload: b"payload\0\xff".to_vec(),
        qos: 1,
        packet_id: Some(45),
        dup: false,
        retain: true,
    })
}
fn round_trip(frame: Frame) {
    let bytes = encode_frame(&frame).unwrap();
    let (decoded, n) = decode_frame(&bytes, bytes.len(), Some(frame.version))
        .unwrap()
        .unwrap();
    assert_eq!(n, bytes.len());
    assert_eq!(frame, decoded);
    for prefix in 0..bytes.len() {
        assert_eq!(
            decode_frame(&bytes[..prefix], bytes.len(), Some(frame.version)).unwrap(),
            None,
            "prefix {prefix}"
        );
    }
    assert!(matches!(
        decode_frame(&bytes, bytes.len() - 1, Some(frame.version)),
        Err(ProtocolError::PacketTooLarge)
    ));
}
#[test]
fn connect_properties_binary_password_and_will_round_trip() {
    let mut frame = connect();
    frame.properties = vec![
        Property::SessionExpiryInterval(60),
        Property::ReceiveMaximum(3),
        Property::MaximumPacketSize(65536),
        Property::TopicAliasMaximum(0),
        Property::RequestProblemInformation(1),
        Property::RequestResponseInformation(0),
        Property::UserProperty("k".into(), "one".into()),
        Property::UserProperty("k".into(), "two".into()),
    ];
    if let Packet::Connect {
        will,
        username,
        password,
        ..
    } = &mut frame.packet
    {
        *will = Some(Will {
            topic: "will/status".into(),
            payload: vec![0, 255],
            qos: 2,
            retain: true,
        });
        *username = Some("name".into());
        *password = Some(Password::new(vec![0, 255]));
    }
    frame.will_properties = vec![
        Property::WillDelayInterval(0),
        Property::ContentType("binary".into()),
        Property::PayloadFormatIndicator(0),
    ];
    round_trip(frame);
    let mut frame = connect();
    if let Packet::Connect { password, .. } = &mut frame.packet {
        *password = Some(Password::new(vec![0, 255]));
    }
    assert!(crate::encode(&frame.packet).is_err()); // v311 requires username.
    round_trip(frame);
}
#[test]
fn all_base_frames_round_trip() {
    let mut frames = vec![
        connect(),
        publish(),
        Frame::v5(Packet::ConnAck {
            session_present: true,
            code: 0,
        }),
        Frame::v5(Packet::PubAck { packet_id: 1 }),
        Frame::v5(Packet::PubRec { packet_id: 2 }),
        Frame::v5(Packet::PubRel { packet_id: 3 }),
        Frame::v5(Packet::PubComp { packet_id: 4 }),
        Frame::v5(Packet::SubAck {
            packet_id: 6,
            codes: vec![0, 1, 2, 0x87, 0x9e, 0xa1],
        }),
        Frame::v5(Packet::Unsubscribe {
            packet_id: 7,
            filters: vec!["a/#".into(), "b/+".into()],
        }),
        Frame::v5(Packet::PingReq),
        Frame::v5(Packet::PingResp),
        Frame::v5(Packet::Disconnect),
        Frame::v5(Packet::Auth),
    ];
    let mut sub = Frame::v5(Packet::Subscribe {
        packet_id: 5,
        filters: vec![("a/#".into(), 2), ("b/+".into(), 1)],
    });
    sub.subscription_options = vec![0x2e, 0x15];
    frames.push(sub);
    let mut unsub = Frame::v5(Packet::UnsubAck { packet_id: 7 });
    unsub.reason_codes = vec![ReasonCode::SUCCESS, ReasonCode::NO_SUBSCRIPTION_EXISTED];
    frames.push(unsub);
    for frame in frames {
        round_trip(frame);
    }
}
#[test]
fn every_property_type_and_context_round_trip() {
    let mut conn = connect();
    conn.properties = vec![
        Property::AuthenticationMethod("method".into()),
        Property::AuthenticationData(vec![0, 255]),
    ];
    round_trip(conn);
    let mut ack = Frame::v5(Packet::ConnAck {
        session_present: false,
        code: 0,
    });
    ack.properties = vec![
        Property::SessionExpiryInterval(12),
        Property::AssignedClientIdentifier("generated".into()),
        Property::ServerKeepAlive(15),
        Property::AuthenticationMethod("method".into()),
        Property::AuthenticationData(vec![0, 1]),
        Property::ResponseInformation("response".into()),
        Property::ServerReference("host".into()),
        Property::ReasonString("ok".into()),
        Property::ReceiveMaximum(1),
        Property::TopicAliasMaximum(0),
        Property::MaximumQos(1),
        Property::RetainAvailable(1),
        Property::MaximumPacketSize(1),
        Property::WildcardSubscriptionAvailable(1),
        Property::SubscriptionIdentifierAvailable(0),
        Property::SharedSubscriptionAvailable(0),
    ];
    round_trip(ack);
    let mut p = publish();
    p.properties = vec![
        Property::PayloadFormatIndicator(0),
        Property::MessageExpiryInterval(12),
        Property::ContentType("application/octet-stream".into()),
        Property::ResponseTopic("reply/to".into()),
        Property::CorrelationData(vec![0, 255]),
        Property::SubscriptionIdentifier(1),
        Property::SubscriptionIdentifier(268435455),
        Property::TopicAlias(1),
        Property::UserProperty("".into(), "".into()),
    ];
    round_trip(p);
    for packet in [
        Packet::PubAck { packet_id: 1 },
        Packet::PubRec { packet_id: 1 },
        Packet::PubRel { packet_id: 1 },
        Packet::PubComp { packet_id: 1 },
        Packet::SubAck {
            packet_id: 1,
            codes: vec![0],
        },
    ] {
        let mut f = Frame::v5(packet);
        f.properties = vec![
            Property::ReasonString("reason".into()),
            Property::UserProperty("k".into(), "v".into()),
        ];
        round_trip(f);
    }
    let mut f = Frame::v5(Packet::Subscribe {
        packet_id: 1,
        filters: vec![("a".into(), 0)],
    });
    f.subscription_options = vec![0];
    f.properties = vec![Property::SubscriptionIdentifier(1234)];
    round_trip(f);
    let mut f = Frame::v5(Packet::Disconnect);
    f.reason_code = ReasonCode::SERVER_BUSY;
    f.properties = vec![
        Property::SessionExpiryInterval(0),
        Property::ServerReference("elsewhere".into()),
        Property::ReasonString("reason".into()),
    ];
    round_trip(f);
    let mut f = Frame::v5(Packet::Auth);
    f.reason_code = ReasonCode::CONTINUE_AUTHENTICATION;
    f.properties = vec![
        Property::AuthenticationMethod("method".into()),
        Property::AuthenticationData(vec![0, 255]),
    ];
    round_trip(f);
}
#[test]
fn singleton_context_and_value_validation_is_symmetric() {
    let invalid: Vec<Properties> = vec![
        vec![Property::ReceiveMaximum(0)],
        vec![Property::ReceiveMaximum(1), Property::ReceiveMaximum(2)],
        vec![Property::MaximumPacketSize(0)],
        vec![Property::RequestProblemInformation(2)],
        vec![Property::ContentType("bad-context".into())],
        vec![Property::AuthenticationData(vec![1])],
    ];
    for properties in invalid {
        let mut c = connect();
        c.properties = properties;
        assert!(encode_frame(&c).is_err());
    }
    for properties in [
        vec![Property::TopicAlias(0)],
        vec![Property::SubscriptionIdentifier(0)],
        vec![Property::SubscriptionIdentifier(268435456)],
        vec![Property::ResponseTopic("a/#".into())],
        vec![Property::PayloadFormatIndicator(2)],
        vec![
            Property::ContentType("a".into()),
            Property::ContentType("b".into()),
        ],
    ] {
        let mut p = publish();
        p.properties = properties;
        assert!(encode_frame(&p).is_err());
    }
    let mut c = Frame::v5(Packet::ConnAck {
        session_present: false,
        code: 0,
    });
    c.properties = vec![Property::MaximumQos(2)];
    assert!(encode_frame(&c).is_err());
    let mut s = Frame::v5(Packet::Subscribe {
        packet_id: 1,
        filters: vec![("a".into(), 0)],
    });
    s.properties = vec![
        Property::SubscriptionIdentifier(1),
        Property::SubscriptionIdentifier(2),
    ];
    assert!(encode_frame(&s).is_err());
    // Raw CONNECT with duplicate Receive Maximum and zero value must also fail.
    for properties in [vec![0x21, 0, 0], vec![0x21, 0, 1, 0x21, 0, 2]] {
        let mut body = b"\0\x04MQTT\x05\x02\0\0".to_vec();
        body.push(properties.len() as u8);
        body.extend(properties);
        body.extend(b"\0\x01x");
        let mut wire = vec![0x10, body.len() as u8];
        wire.extend(body);
        assert!(matches!(
            decode_frame(&wire, 1024, None),
            Err(ProtocolError::ProtocolViolation(_))
        ));
    }
    for properties in [vec![0xff, 0x01], vec![3, 0, 1, b'x']] {
        let mut body = b"\0\x04MQTT\x05\x02\0\0".to_vec();
        body.push(properties.len() as u8);
        body.extend(properties);
        body.extend(b"\0\x01x");
        let mut wire = vec![0x10, body.len() as u8];
        wire.extend(body);
        assert!(matches!(
            decode_frame(&wire, 1024, None),
            Err(ProtocolError::Malformed(_))
        ));
    }
}
#[test]
fn omitted_ack_reason_and_properties_forms_decode() {
    for header in [0x40, 0x50, 0x62, 0x70] {
        for wire in [
            vec![header, 2, 0, 1],
            vec![header, 3, 0, 1, 0],
            vec![header, 4, 0, 1, 0, 0],
        ] {
            let frame = decode_frame(&wire, 100, Some(ProtocolVersion::V5))
                .unwrap()
                .unwrap()
                .0;
            assert_eq!(frame.reason_code, ReasonCode::SUCCESS);
            assert!(frame.properties.is_empty());
        }
    }
    for header in [0xe0, 0xf0] {
        for wire in [vec![header, 0], vec![header, 1, 0], vec![header, 2, 0, 0]] {
            assert_eq!(
                decode_frame(&wire, 100, Some(ProtocolVersion::V5))
                    .unwrap()
                    .unwrap()
                    .0
                    .reason_code,
                ReasonCode::SUCCESS
            );
        }
    }
    let mut negative = Frame::v5(Packet::PubRec { packet_id: 4 });
    negative.reason_code = ReasonCode::NOT_AUTHORIZED;
    round_trip(negative);
}
#[test]
fn reason_codes_are_checked_per_packet_in_both_directions() {
    let cases = [
        (
            Packet::ConnAck {
                session_present: false,
                code: 139,
            },
            0,
        ),
        (
            Packet::SubAck {
                packet_id: 1,
                codes: vec![146],
            },
            0,
        ),
        (Packet::PubAck { packet_id: 1 }, 146),
        (Packet::PubRel { packet_id: 1 }, 135),
        (Packet::Auth, 135),
    ];
    for (packet, code) in cases {
        let mut f = Frame::v5(packet);
        f.reason_code = ReasonCode::try_from(code).unwrap();
        assert!(encode_frame(&f).is_err());
    }
    for wire in [
        vec![0x20, 3, 0, 139, 0],
        vec![0x40, 3, 0, 1, 146],
        vec![0x90, 4, 0, 1, 0, 146],
        vec![0xf0, 1, 135],
    ] {
        assert!(matches!(
            decode_frame(&wire, 100, Some(ProtocolVersion::V5)),
            Err(ProtocolError::ProtocolViolation(_))
        ));
    }
    for code in 0..=255 {
        assert_eq!(
            ReasonCode::try_from(code).is_ok(),
            matches!(code,0..=2|4|16|17|24|25|128..=162)
        );
    }
}
#[test]
fn malicious_lengths_headers_and_noncanonical_integers_rejected() {
    for wire in [
        vec![0xe1, 0],
        vec![0xf1, 0],
        vec![0x30, 0],
        vec![0x30, 0x80, 0],
        vec![0xe0, 2, 0, 0x80],
        vec![0xe0, 3, 0, 0x80, 0],
        vec![0x40, 2, 0, 0],
        vec![0xc0, 1, 0],
        vec![0x30, 5, 0, 1, b'a', 127, 0],
        vec![0x30, 6, 0, 1, b'a', 2, 0x0b, 0x80],
    ] {
        assert!(
            decode_frame(&wire, 1024, Some(ProtocolVersion::V5)).is_err(),
            "{wire:?}"
        );
    }
    assert!(
        decode_frame(
            &[0x30, 0xff, 0xff, 0xff, 0x7f],
            1024,
            Some(ProtocolVersion::V5)
        )
        .is_err()
    );
    assert!(
        decode_frame(
            &[0x30, 0xff, 0xff, 0xff, 0xff],
            1024,
            Some(ProtocolVersion::V5)
        )
        .is_err()
    );
    assert!(decode_frame(&[0x30, 0x80], 2, Some(ProtocolVersion::V5)).is_err());
}
#[test]
fn metadata_does_not_silently_disappear() {
    let mut p = Frame::v311(Packet::PingReq);
    p.properties = vec![Property::UserProperty("k".into(), "v".into())];
    assert!(encode_frame(&p).is_err());
    for packet in [Packet::PingReq, Packet::PubAck { packet_id: 1 }] {
        let mut f = Frame::v5(packet);
        f.will_properties = vec![Property::WillDelayInterval(0)];
        assert!(encode_frame(&f).is_err());
    }
    let mut f = connect();
    f.will_properties = vec![Property::WillDelayInterval(0)];
    assert!(encode_frame(&f).is_err());
    let mut f = publish();
    f.reason_code = ReasonCode::NOT_AUTHORIZED;
    assert!(encode_frame(&f).is_err());
    assert!(crate::encode(&Packet::Auth).is_err());
}
#[test]
fn subscription_options_and_topic_alias_syntax() {
    for options in [3, 0x30, 0x40, 0x80] {
        let mut f = Frame::v5(Packet::Subscribe {
            packet_id: 1,
            filters: vec![("a".into(), options & 3)],
        });
        f.subscription_options = vec![options];
        assert!(encode_frame(&f).is_err());
    }
    let mut f = Frame::v5(Packet::Subscribe {
        packet_id: 1,
        filters: vec![("$share/g/a".into(), 0)],
    });
    f.subscription_options = vec![4];
    assert!(encode_frame(&f).is_err());
    let mut f = publish();
    if let Packet::Publish { topic, .. } = &mut f.packet {
        topic.clear();
    }
    assert!(encode_frame(&f).is_err());
    f.properties = vec![Property::TopicAlias(1)];
    round_trip(f);
}
#[test]
fn negotiated_version_and_bounded_prefix_inspection() {
    let bytes = encode_frame(&connect()).unwrap();
    for n in 0..9 {
        assert_eq!(peek_connect_version(&bytes[..n]), None);
    }
    assert_eq!(peek_connect_version(&bytes[..9]), Some(ProtocolVersion::V5));
    assert_eq!(peek_connect_version(&[0xc0, 0]), None);
    assert_eq!(peek_connect_version(b"\x10\x80\0\0\x04MQTT\x05"), None);
    assert!(matches!(
        decode_frame(&bytes, 1024, Some(ProtocolVersion::V311)),
        Err(ProtocolError::ProtocolViolation(_))
    ));
    let legacy = Frame::v311(Packet::Connect {
        client_id: "legacy".into(),
        clean_session: true,
        keep_alive: 0,
        will: None,
        username: None,
        password: None,
    });
    let bytes = encode_frame(&legacy).unwrap();
    assert_eq!(peek_connect_version(&bytes), Some(ProtocolVersion::V311));
    round_trip(legacy);
    assert_eq!(
        decode_frame(&[0xc0, 0], 100, None)
            .unwrap()
            .unwrap()
            .0
            .version,
        ProtocolVersion::V311
    );
}
#[test]
fn concatenation_consumes_exactly_one_frame_and_password_debug_is_redacted() {
    let mut c = connect();
    if let Packet::Connect { password, .. } = &mut c.packet {
        *password = Some(Password::new(b"private-password-marker".to_vec()));
    }
    let mut bytes = encode_frame(&c).unwrap();
    let n = bytes.len();
    bytes.extend([0xc0, 0]);
    assert_eq!(decode_frame(&bytes, 1024, None).unwrap().unwrap().1, n);
    assert_eq!(
        decode_frame(&bytes[n..], 1024, Some(ProtocolVersion::V5))
            .unwrap()
            .unwrap()
            .0
            .packet,
        Packet::PingReq
    );
    c.properties = vec![
        Property::AuthenticationMethod("opaque".into()),
        Property::AuthenticationData(b"private-authentication-marker".to_vec()),
    ];
    let debug = format!("{c:?}");
    assert!(!debug.contains("private-authentication-marker"));
    assert!(!debug.contains("112, 114, 105"));
    assert!(!debug.contains("private-password-marker"));
    assert!(!debug.contains("112, 114, 105"));
}

#[test]
fn zero_topic_alias_has_specific_wire_reason_and_invalid_header_fails_early() {
    let wire = b"\x30\x07\0\x01a\x03\x23\0\0";
    assert!(matches!(decode_frame(wire,100,Some(ProtocolVersion::V5)),
        Err(ProtocolError::Mqtt5(reason)) if reason==ReasonCode::TOPIC_ALIAS_INVALID));
    assert!(matches!(
        decode_frame(&[0xe1], 100, Some(ProtocolVersion::V5)),
        Err(ProtocolError::Malformed(_))
    ));
}
