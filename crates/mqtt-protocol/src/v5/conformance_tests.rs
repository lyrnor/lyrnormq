//! Clause-oriented codec regressions; packet legality is independent of providers.
use super::*;

#[test]
fn auth_requires_method_even_when_reason_and_properties_are_omitted() {
    // OASIS 3.15.2.2.2 and MQTT-4.12.0-5: AUTH has no legal method-less form.
    // A remembered connection method does not permit omitting the wire property.
    for wire in [
        vec![0xf0, 0],
        vec![0xf0, 1, 0],
        vec![0xf0, 2, 0x18, 0],
        vec![0xf0, 6, 0x18, 4, 0x16, 0, 1, 42],
    ] {
        assert!(
            matches!(
                decode_frame(&wire, 1024, Some(ProtocolVersion::V5)),
                Err(ProtocolError::ProtocolViolation(_))
            ),
            "accepted missing Authentication Method: {wire:?}"
        );
    }
    for code in [0, 0x18, 0x19] {
        let mut frame = Frame::v5(Packet::Auth);
        frame.reason_code = ReasonCode::try_from(code).unwrap();
        assert!(matches!(
            encode_frame(&frame),
            Err(ProtocolError::ProtocolViolation(_))
        ));
        frame
            .properties
            .push(Property::AuthenticationData(vec![42]));
        assert!(matches!(
            encode_frame(&frame),
            Err(ProtocolError::ProtocolViolation(_))
        ));
        frame
            .properties
            .insert(0, Property::AuthenticationMethod("PLAIN".into()));
        let bytes = encode_frame(&frame).unwrap();
        assert_eq!(
            decode_frame(&bytes, 1024, Some(ProtocolVersion::V5)).unwrap(),
            Some((frame, bytes.len()))
        );
    }
}

#[test]
fn utf8_bom_is_preserved_and_malformed_scalars_and_nul_are_rejected() {
    // MQTT-1.5.4-1/-2/-3 and MQTT-1.5.7-1. Neither UTF-8 fields nor
    // pair members may silently normalize or strip a byte-order mark.
    let marker = "\u{feff}topic/\u{feff}value";
    let mut frame = Frame::v5(Packet::Publish {
        topic: marker.into(),
        payload: vec![0, 255],
        qos: 0,
        packet_id: None,
        dup: false,
        retain: false,
    });
    frame.properties = vec![
        Property::UserProperty(marker.into(), marker.into()),
        Property::ContentType(marker.into()),
        Property::ResponseTopic(marker.into()),
    ];
    let bytes = encode_frame(&frame).unwrap();
    assert_eq!(
        decode_frame(&bytes, bytes.len(), Some(ProtocolVersion::V5)).unwrap(),
        Some((frame, bytes.len()))
    );
    for invalid in [
        vec![0],
        vec![0xc0, 0x80],
        vec![0xed, 0xa0, 0x80],
        vec![0xf4, 0x90, 0x80, 0x80],
        vec![0x80],
    ] {
        // Independent wire fixture: Topic Name length, invalid scalar bytes, empty Properties.
        let mut body = vec![0, invalid.len() as u8];
        body.extend(&invalid);
        body.push(0);
        let mut wire = vec![0x30, body.len() as u8];
        wire.extend(body);
        assert!(
            matches!(
                decode_frame(&wire, 1024, Some(ProtocolVersion::V5)),
                Err(ProtocolError::Malformed(_))
            ),
            "{invalid:?}"
        );
    }
    for (name, value) in [("bad\0name", "value"), ("name", "bad\0value")] {
        let mut frame = Frame::v5(Packet::Publish {
            topic: "valid".into(),
            payload: vec![],
            qos: 0,
            packet_id: None,
            dup: false,
            retain: false,
        });
        frame.properties = vec![Property::UserProperty(name.into(), value.into())];
        assert!(matches!(
            encode_frame(&frame),
            Err(ProtocolError::Malformed(_))
        ));
    }
}

#[test]
fn every_reserved_fixed_header_flag_is_rejected_without_waiting_for_payload() {
    // MQTT-2.1.3-1. Fixed header validation must not depend on a complete
    // declared body, otherwise malformed peers can occupy bounded connections.
    for kind in [1u8, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15] {
        let required = if [6, 8, 10].contains(&kind) { 2 } else { 0 };
        for flags in 0..16 {
            if flags != required {
                assert!(
                    matches!(
                        decode_frame(&[(kind << 4) | flags], 1024, Some(ProtocolVersion::V5)),
                        Err(ProtocolError::Malformed(_))
                    ),
                    "kind={kind} flags={flags}"
                );
            }
        }
    }
}

#[test]
fn connect_conditional_fields_and_order_match_independent_wire_fixtures() {
    // MQTT-3.1.2-3/-9/-11/-12/-13/-16/-17/-18/-19 and MQTT-3.1.3-1.
    fn wire(flags: u8, tail: &[u8]) -> Vec<u8> {
        let mut body = b"\0\x04MQTT\x05".to_vec();
        body.extend([flags, 0, 0, 0, 0, 1, b'c']);
        body.extend(tail);
        let mut bytes = vec![0x10, body.len() as u8];
        bytes.extend(body);
        bytes
    }
    let will = [0, 0, 1, b'w', 0, 2, 0, 255];
    let user = [0, 1, b'u'];
    let password = [0, 2, 0, 255];
    for (flags, tail) in [
        (2, vec![]),
        (0x82, user.to_vec()),
        (0x42, password.to_vec()),
        (0x36, will.to_vec()),
        (0xf6, [will.as_slice(), &user, &password].concat()),
    ] {
        let bytes = wire(flags, &tail);
        let decoded = decode_frame(&bytes, 1024, Some(ProtocolVersion::V5))
            .unwrap()
            .unwrap()
            .0;
        let Packet::Connect {
            client_id,
            clean_session,
            keep_alive,
            will,
            username,
            password,
        } = &decoded.packet
        else {
            panic!("CONNECT fixture")
        };
        assert_eq!(client_id, "c");
        assert!(*clean_session);
        assert_eq!(*keep_alive, 0);
        assert_eq!(
            will.as_ref()
                .map(|w| (w.topic.as_str(), w.payload.as_slice(), w.qos, w.retain)),
            (flags & 4 != 0).then_some(("w", &[0, 255][..], 2, true))
        );
        assert_eq!(username.as_deref(), (flags & 128 != 0).then_some("u"));
        assert_eq!(
            password.as_ref().map(Password::as_bytes),
            (flags & 64 != 0).then_some(&[0, 255][..])
        );
        assert_eq!(encode_frame(&decoded).unwrap(), bytes);
    }
    for (flags, tail) in [
        (3, vec![]),
        (0x1e, will.to_vec()),
        (0x12, vec![]),
        (0x22, vec![]),
        (6, vec![]),
        (6, vec![0]),
        (6, vec![0, 0, 1, b'w']),
        (0x82, vec![]),
        (0x42, vec![]),
        (2, user.to_vec()),
        (2, password.to_vec()),
    ] {
        let bytes = wire(flags, &tail);
        assert!(
            decode_frame(&bytes, 1024, Some(ProtocolVersion::V5)).is_err(),
            "flags={flags:02x} tail={tail:?}"
        );
    }
}

#[test]
fn subscribe_and_unsubscribe_require_nonempty_valid_utf8_filter_lists() {
    // MQTT-3.8.3-1/-2 and MQTT-3.10.3-1/-2, independently framed bodies.
    for (header, body) in [
        (0x82, vec![0, 1, 0]),
        (0xa2, vec![0, 1, 0]),
        (0x82, vec![0, 1, 0, 0, 0, 0]),
        (0xa2, vec![0, 1, 0, 0, 0]),
        (0x82, vec![0, 1, 0, 0, 1, 0, 0]),
        (0xa2, vec![0, 1, 0, 0, 1, 0]),
        (0x82, vec![0, 1, 0, 0, 1, 0xff, 0]),
        (0xa2, vec![0, 1, 0, 0, 1, 0xff]),
    ] {
        let mut bytes = vec![header, body.len() as u8];
        bytes.extend(body);
        assert!(
            decode_frame(&bytes, 1024, Some(ProtocolVersion::V5)).is_err(),
            "{bytes:?}"
        );
    }
    for packet in [
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![],
        },
        Packet::Unsubscribe {
            packet_id: 1,
            filters: vec![],
        },
    ] {
        assert!(encode_frame(&Frame::v5(packet)).is_err());
    }
}
