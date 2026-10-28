//! Specification-authored property fixtures, independent of the implementation allow-list.
use super::*;

#[derive(Clone, Copy, Debug)]
enum Context {
    Connect,
    Will,
    Connack,
    Publish,
    Disconnect,
    Auth,
}

fn vint(mut value: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let digit = (value % 128) as u8;
        value /= 128;
        out.push(digit | if value > 0 { 128 } else { 0 });
        if value == 0 {
            return out;
        }
    }
}
fn bytes(context: Context, properties: &[u8]) -> Vec<u8> {
    let mut p = vint(properties.len());
    p.extend(properties);
    let (header, mut body) = match context {
        Context::Connect => {
            let mut body = b"\x00\x04MQTT\x05\x02\x00\x1e".to_vec();
            body.extend(p);
            body.extend(b"\x00\x01c");
            (0x10, body)
        }
        Context::Will => {
            let mut body = b"\x00\x04MQTT\x05\x06\x00\x1e\x00\x00\x01c".to_vec();
            body.extend(p);
            body.extend(b"\x00\x01t\x00\x01w");
            (0x10, body)
        }
        Context::Connack => {
            let mut body = vec![0, 0];
            body.extend(p);
            (0x20, body)
        }
        Context::Publish => {
            let mut body = b"\x00\x01t".to_vec();
            body.extend(p);
            body.push(b'x');
            (0x30, body)
        }
        Context::Disconnect => {
            let mut body = vec![0];
            body.extend(p);
            (0xe0, body)
        }
        Context::Auth => {
            let mut body = vec![0x18];
            body.extend(p);
            (0xf0, body)
        }
    };
    let mut out = vec![header];
    out.extend(vint(body.len()));
    out.append(&mut body);
    out
}
fn frame(context: Context, properties: Vec<Property>) -> Frame {
    let packet = match context {
        Context::Connect | Context::Will => Packet::Connect {
            client_id: "c".into(),
            clean_session: true,
            keep_alive: 30,
            will: matches!(context, Context::Will).then(|| Will {
                topic: "t".into(),
                payload: vec![b'w'],
                qos: 0,
                retain: false,
            }),
            username: None,
            password: None,
        },
        Context::Connack => Packet::ConnAck {
            session_present: false,
            code: 0,
        },
        Context::Publish => Packet::Publish {
            topic: "t".into(),
            payload: vec![b'x'],
            qos: 0,
            packet_id: None,
            dup: false,
            retain: false,
        },
        Context::Disconnect => Packet::Disconnect,
        Context::Auth => Packet::Auth,
    };
    let mut frame = Frame::v5(packet);
    if matches!(context, Context::Auth) {
        frame.reason_code = ReasonCode::try_from(0x18).unwrap();
    }
    if matches!(context, Context::Will) {
        frame.will_properties = properties;
    } else {
        frame.properties = properties;
    }
    frame
}
fn method() -> (Property, Vec<u8>) {
    (
        Property::AuthenticationMethod("m".into()),
        b"\x15\x00\x01m".to_vec(),
    )
}
fn fixture(property: Property, wire: &[u8]) -> (Property, Vec<u8>) {
    (property, wire.to_vec())
}
fn cases(context: Context) -> Vec<(Property, Vec<u8>)> {
    // Values and literal IDs are authored from individual OASIS section3 property entries.
    match context {
        Context::Connect => vec![
            fixture(Property::SessionExpiryInterval(1), b"\x11\x00\x00\x00\x01"),
            fixture(Property::ReceiveMaximum(1), b"\x21\x00\x01"),
            fixture(Property::MaximumPacketSize(1), b"\x27\x00\x00\x00\x01"),
            fixture(Property::TopicAliasMaximum(0), b"\x22\x00\x00"),
            fixture(Property::RequestResponseInformation(0), b"\x19\x00"),
            fixture(Property::RequestProblemInformation(0), b"\x17\x00"),
            method(),
            fixture(
                Property::AuthenticationData(vec![0, 255]),
                b"\x16\x00\x02\x00\xff",
            ),
        ],
        Context::Will => vec![
            fixture(Property::WillDelayInterval(1), b"\x18\x00\x00\x00\x01"),
            fixture(Property::PayloadFormatIndicator(0), b"\x01\x00"),
            fixture(Property::MessageExpiryInterval(1), b"\x02\x00\x00\x00\x01"),
            fixture(Property::ContentType("x".into()), b"\x03\x00\x01x"),
            fixture(Property::ResponseTopic("r".into()), b"\x08\x00\x01r"),
            fixture(
                Property::CorrelationData(vec![0, 255]),
                b"\x09\x00\x02\x00\xff",
            ),
        ],
        Context::Connack => vec![
            fixture(Property::SessionExpiryInterval(1), b"\x11\x00\x00\x00\x01"),
            fixture(Property::ReceiveMaximum(1), b"\x21\x00\x01"),
            fixture(Property::MaximumQos(1), b"\x24\x01"),
            fixture(Property::RetainAvailable(1), b"\x25\x01"),
            fixture(Property::MaximumPacketSize(1), b"\x27\x00\x00\x00\x01"),
            fixture(
                Property::AssignedClientIdentifier("c".into()),
                b"\x12\x00\x01c",
            ),
            fixture(Property::TopicAliasMaximum(0), b"\x22\x00\x00"),
            fixture(Property::ServerKeepAlive(0), b"\x13\x00\x00"),
            fixture(Property::ReasonString("r".into()), b"\x1f\x00\x01r"),
            fixture(Property::WildcardSubscriptionAvailable(1), b"\x28\x01"),
            fixture(Property::SubscriptionIdentifierAvailable(1), b"\x29\x01"),
            fixture(Property::SharedSubscriptionAvailable(1), b"\x2a\x01"),
            fixture(Property::ResponseInformation("r".into()), b"\x1a\x00\x01r"),
            fixture(Property::ServerReference("s".into()), b"\x1c\x00\x01s"),
            method(),
            fixture(
                Property::AuthenticationData(vec![0, 255]),
                b"\x16\x00\x02\x00\xff",
            ),
        ],
        Context::Publish => vec![
            fixture(Property::PayloadFormatIndicator(0), b"\x01\x00"),
            fixture(Property::MessageExpiryInterval(1), b"\x02\x00\x00\x00\x01"),
            fixture(Property::TopicAlias(1), b"\x23\x00\x01"),
            fixture(Property::ContentType("x".into()), b"\x03\x00\x01x"),
            fixture(Property::ResponseTopic("r".into()), b"\x08\x00\x01r"),
            fixture(
                Property::CorrelationData(vec![0, 255]),
                b"\x09\x00\x02\x00\xff",
            ),
        ],
        Context::Disconnect => vec![
            fixture(Property::SessionExpiryInterval(1), b"\x11\x00\x00\x00\x01"),
            fixture(Property::ServerReference("s".into()), b"\x1c\x00\x01s"),
            fixture(Property::ReasonString("r".into()), b"\x1f\x00\x01r"),
        ],
        Context::Auth => vec![
            method(),
            fixture(
                Property::AuthenticationData(vec![0, 255]),
                b"\x16\x00\x02\x00\xff",
            ),
            fixture(Property::ReasonString("r".into()), b"\x1f\x00\x01r"),
        ],
    }
}
fn contextual(context: Context, property: &Property) -> bool {
    matches!(context, Context::Auth) && !matches!(property, Property::AuthenticationMethod(_))
        || matches!(context, Context::Connect | Context::Connack)
            && matches!(property, Property::AuthenticationData(_))
}
fn singleton(context: Context) {
    for (property, raw) in cases(context) {
        let (m, mw) = method();
        let mut fields = Vec::new();
        let mut values = Vec::new();
        if contextual(context, &property) {
            fields.extend(mw);
            values.push(m);
        }
        fields.extend(&raw);
        values.push(property.clone());
        let valid = frame(context, values.clone());
        let wire = bytes(context, &fields);
        assert_eq!(
            encode_frame(&valid).unwrap(),
            wire,
            "literal positive {context:?} {property:?}"
        );
        assert_eq!(
            decode_frame(&wire, 4096, Some(ProtocolVersion::V5)).unwrap(),
            Some((valid, wire.len()))
        );
        fields.extend(&raw);
        values.push(property);
        assert!(
            matches!(
                decode_frame(&bytes(context, &fields), 4096, Some(ProtocolVersion::V5)),
                Err(ProtocolError::ProtocolViolation(_))
            ),
            "literal duplicate {context:?} {values:?}"
        );
        assert!(matches!(
            encode_frame(&frame(context, values)),
            Err(ProtocolError::ProtocolViolation(_))
        ));
    }
}
#[test]
fn connect_singleton_property_counterexamples() {
    singleton(Context::Connect);
}
#[test]
fn will_singleton_property_counterexamples() {
    singleton(Context::Will);
}
#[test]
fn connack_singleton_property_counterexamples() {
    singleton(Context::Connack);
}
#[test]
fn publish_singleton_property_counterexamples() {
    singleton(Context::Publish);
}
#[test]
fn disconnect_singleton_property_counterexamples() {
    singleton(Context::Disconnect);
}
#[test]
fn auth_singleton_property_counterexamples() {
    singleton(Context::Auth);
}

#[test]
fn boolean_nonzero_and_topic_value_constraints_are_independent_wire_cases() {
    for (context, property, raw) in [
        (
            Context::Connect,
            Property::RequestProblemInformation(2),
            b"\x17\x02".as_slice(),
        ),
        (
            Context::Connect,
            Property::RequestResponseInformation(2),
            b"\x19\x02".as_slice(),
        ),
        (
            Context::Connect,
            Property::ReceiveMaximum(0),
            b"\x21\x00\x00".as_slice(),
        ),
        (
            Context::Connect,
            Property::MaximumPacketSize(0),
            b"\x27\x00\x00\x00\x00".as_slice(),
        ),
        (
            Context::Will,
            Property::PayloadFormatIndicator(2),
            b"\x01\x02".as_slice(),
        ),
        (
            Context::Connack,
            Property::MaximumQos(2),
            b"\x24\x02".as_slice(),
        ),
        (
            Context::Connack,
            Property::RetainAvailable(2),
            b"\x25\x02".as_slice(),
        ),
        (
            Context::Connack,
            Property::WildcardSubscriptionAvailable(2),
            b"\x28\x02".as_slice(),
        ),
        (
            Context::Connack,
            Property::SubscriptionIdentifierAvailable(2),
            b"\x29\x02".as_slice(),
        ),
        (
            Context::Connack,
            Property::SharedSubscriptionAvailable(2),
            b"\x2a\x02".as_slice(),
        ),
        (
            Context::Connack,
            Property::ReceiveMaximum(0),
            b"\x21\x00\x00".as_slice(),
        ),
        (
            Context::Connack,
            Property::MaximumPacketSize(0),
            b"\x27\x00\x00\x00\x00".as_slice(),
        ),
        (
            Context::Publish,
            Property::PayloadFormatIndicator(2),
            b"\x01\x02".as_slice(),
        ),
        (
            Context::Publish,
            Property::ResponseTopic("".into()),
            b"\x08\x00\x00".as_slice(),
        ),
        (
            Context::Will,
            Property::ResponseTopic("+".into()),
            b"\x08\x00\x01+".as_slice(),
        ),
    ] {
        assert!(
            matches!(
                encode_frame(&frame(context, vec![property.clone()])),
                Err(ProtocolError::ProtocolViolation(_))
            ),
            "{context:?} {property:?}"
        );
        assert!(
            matches!(
                decode_frame(&bytes(context, raw), 4096, Some(ProtocolVersion::V5)),
                Err(ProtocolError::ProtocolViolation(_))
            ),
            "{context:?} {property:?}"
        );
    }
}

#[test]
fn typed_truncation_and_wrong_context_are_malformed_not_partial_success() {
    for (context, raw) in [
        (Context::Connect, b"\x21\x01".as_slice()), // Two-byte ReceiveMaximum lacks a byte.
        (Context::Connack, b"\x27\x00\x00\x01".as_slice()), // Four-byte MaximumPacketSize lacks a byte.
        (Context::Will, b"\x03\x00\x02x".as_slice()),       // Declared UTF8 length exceeds field.
        (Context::Publish, b"\x09\x00\x02x".as_slice()),    // Declared Binary length exceeds field.
        (Context::Auth, b"\x15\x00\x02m".as_slice()),
        (Context::Disconnect, b"\x11\x00\x00\x01".as_slice()),
    ] {
        assert!(matches!(
            decode_frame(&bytes(context, raw), 4096, Some(ProtocolVersion::V5)),
            Err(ProtocolError::Malformed(_))
        ));
    }
    for (context, property, raw) in [
        (
            Context::Connect,
            Property::TopicAlias(1),
            b"\x23\x00\x01".as_slice(),
        ),
        (
            Context::Will,
            Property::ReceiveMaximum(1),
            b"\x21\x00\x01".as_slice(),
        ),
        (
            Context::Connack,
            Property::WillDelayInterval(1),
            b"\x18\x00\x00\x00\x01".as_slice(),
        ),
        (
            Context::Publish,
            Property::ServerKeepAlive(1),
            b"\x13\x00\x01".as_slice(),
        ),
        (
            Context::Disconnect,
            Property::ContentType("x".into()),
            b"\x03\x00\x01x".as_slice(),
        ),
        (
            Context::Auth,
            Property::SessionExpiryInterval(1),
            b"\x11\x00\x00\x00\x01".as_slice(),
        ),
    ] {
        let mut values = vec![property];
        let mut fields = raw.to_vec();
        if matches!(context, Context::Auth) {
            // Isolate the misplaced property from AUTH's independent method requirement.
            values.insert(0, Property::AuthenticationMethod("PLAIN".into()));
            fields.splice(0..0, b"\x15\x00\x05PLAIN".iter().copied());
        }
        assert!(matches!(
            encode_frame(&frame(context, values)),
            Err(ProtocolError::Malformed(_))
        ));
        assert!(matches!(
            decode_frame(&bytes(context, &fields), 4096, Some(ProtocolVersion::V5)),
            Err(ProtocolError::Malformed(_))
        ));
    }
}

#[test]
fn repeatable_user_pairs_keep_order_while_publish_identifiers_keep_multiplicity() {
    for context in [
        Context::Connect,
        Context::Will,
        Context::Connack,
        Context::Publish,
        Context::Disconnect,
        Context::Auth,
    ] {
        let mut fields = Vec::new();
        let mut values = Vec::new();
        if matches!(context, Context::Auth) {
            let (p, w) = method();
            fields.extend(w);
            values.push(p);
        }
        fields.extend(b"\x26\x00\x01k\x00\x01a\x26\x00\x01k\x00\x01b");
        values.extend([
            Property::UserProperty("k".into(), "a".into()),
            Property::UserProperty("k".into(), "b".into()),
        ]);
        let expected = frame(context, values);
        let wire = bytes(context, &fields);
        assert_eq!(encode_frame(&expected).unwrap(), wire);
        assert_eq!(
            decode_frame(&wire, 4096, Some(ProtocolVersion::V5)).unwrap(),
            Some((expected, wire.len()))
        );
    }
    let expected = frame(
        Context::Publish,
        vec![
            Property::SubscriptionIdentifier(127),
            Property::SubscriptionIdentifier(127),
            Property::SubscriptionIdentifier(128),
        ],
    );
    let wire = bytes(Context::Publish, b"\x0b\x7f\x0b\x7f\x0b\x80\x01");
    assert_eq!(encode_frame(&expected).unwrap(), wire);
    assert_eq!(
        decode_frame(&wire, 4096, Some(ProtocolVersion::V5)).unwrap(),
        Some((expected, wire.len()))
    );
    // packet ID2 + properties5 + filter4 = RemainingLength11.
    let wire = vec![0x82, 11, 0, 1, 4, 0x0b, 1, 0x0b, 2, 0, 1, b't', 0];
    assert!(matches!(
        decode_frame(&wire, 4096, Some(ProtocolVersion::V5)),
        Err(ProtocolError::ProtocolViolation(_))
    ));
    let mut request = Frame::v5(Packet::Subscribe {
        packet_id: 1,
        filters: vec![("t".into(), 0)],
    });
    request.properties = vec![
        Property::SubscriptionIdentifier(1),
        Property::SubscriptionIdentifier(2),
    ];
    assert!(matches!(
        encode_frame(&request),
        Err(ProtocolError::ProtocolViolation(_))
    ));
}
