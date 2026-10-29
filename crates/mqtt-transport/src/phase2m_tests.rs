//! Exact connection-context acceptance, independent of codec property tables.
use super::*;

fn basic(client: &str, version: ProtocolVersion) -> Frame {
    let packet = Packet::Connect {
        client_id: client.into(),
        clean_session: true,
        keep_alive: 0,
        will: None,
        username: None,
        password: None,
    };
    match version {
        ProtocolVersion::V311 => Frame::v311(packet),
        ProtocolVersion::V5 => Frame::v5(packet),
    }
}

#[tokio::test]
async fn mqtt_3_1_2_1_wrong_protocol_names_close_silently_without_version_guessing() {
    let (address, shutdown, task) = broker(BrokerConfig::default()).await;
    // Literal fields, not an encoder that would refuse the malformed name first.
    for wire in [
        b"\x10\x0e\x00\x04mqtt\x05\x02\x00\x00\x00\x00\x01c".as_slice(),
        b"\x10\x0e\x00\x04MQTQ\x05\x02\x00\x00\x00\x00\x01c".as_slice(),
        b"\x10\x0e\x00\x04mqtt\x04\x02\x00\x00\x00\x02cc".as_slice(),
    ] {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(wire).await.unwrap();
        closed(&mut stream).await;
    }
    stop(shutdown, task).await;
}

#[tokio::test]
async fn unsupported_levels_preserve_legacy_refusal_and_valid_versions_remain_usable() {
    let (address, shutdown, task) = broker(BrokerConfig::default()).await;
    for level in [0, 3, 6, 255] {
        let mut wire = b"\x10\x0f\x00\x04MQTT\x04\x02\x00\x00\x00\x03cid".to_vec();
        wire[8] = level;
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(&wire).await.unwrap();
        let mut reply = [0; 4];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(reply, [0x20, 2, 0, 1]);
        closed(&mut stream).await;
    }
    for version in [ProtocolVersion::V311, ProtocolVersion::V5] {
        let mut stream = TcpStream::connect(address).await.unwrap();
        write(&mut stream, basic("valid", version)).await;
        if version == ProtocolVersion::V311 {
            let mut bytes = [0; 4];
            stream.read_exact(&mut bytes).await.unwrap();
            assert_eq!(bytes, [0x20, 2, 0, 0]);
        } else {
            assert_eq!(
                read(&mut stream).await.packet,
                Packet::ConnAck {
                    session_present: false,
                    code: 0
                }
            );
        }
        write(
            &mut stream,
            if version == ProtocolVersion::V5 {
                Frame::v5(Packet::Disconnect)
            } else {
                Frame::v311(Packet::Disconnect)
            },
        )
        .await;
        closed(&mut stream).await;
    }
    stop(shutdown, task).await;
}

#[tokio::test]
async fn mqtt_3_1_3_8_no_extra_client_identifier_exclusion_policy_is_configured() {
    let (address, shutdown, task) = broker(BrokerConfig::default()).await;
    // Valid non-alphanumeric names and the maximum UTF8-string byte length are
    // admitted. No new restriction is added merely to manufacture CONNACK85.
    for name in [
        "a".into(),
        " a /+ # ".into(),
        "客户-🦀-\u{feff}".into(),
        "z".repeat(65535),
    ] {
        let mut stream = TcpStream::connect(address).await.unwrap();
        write(&mut stream, basic(&name, ProtocolVersion::V5)).await;
        assert_eq!(
            read(&mut stream).await.packet,
            Packet::ConnAck {
                session_present: false,
                code: 0
            }
        );
        write(&mut stream, Frame::v5(Packet::Disconnect)).await;
        closed(&mut stream).await;
    }
    let mut empty = TcpStream::connect(address).await.unwrap();
    write(&mut empty, basic("", ProtocolVersion::V5)).await;
    let reply = read(&mut empty).await;
    assert!(
        reply
            .properties
            .iter()
            .any(|p| matches!(p, Property::AssignedClientIdentifier(value) if !value.is_empty()))
    );
    write(&mut empty, Frame::v5(Packet::Disconnect)).await;
    closed(&mut empty).await;
    stop(shutdown, task).await;
}

#[test]
fn mqtt_3_2_2_8_connack_constructor_is_guarded_for_every_numeric_reason() {
    // Literal Table3-1 values, intentionally independent of valid_reason().
    let legal = [
        0, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a, 0x8c, 0x90, 0x95,
        0x97, 0x99, 0x9a, 0x9b, 0x9c, 0x9d, 0x9f,
    ];
    for value in 0..=255 {
        let frame = super::super::network::connack(
            ProtocolVersion::V5,
            false,
            value,
            &BrokerConfig::default(),
        );
        let encoded = mqtt_protocol::encode_frame(&frame);
        assert_eq!(
            encoded.is_ok(),
            legal.contains(&value),
            "reason {value:02x}"
        );
        if let Ok(bytes) = encoded {
            assert_eq!(bytes[3], value);
            let (decoded, consumed) =
                mqtt_protocol::decode_frame(&bytes, 4096, Some(ProtocolVersion::V5))
                    .unwrap()
                    .unwrap();
            assert_eq!(consumed, bytes.len());
            assert_eq!(
                decoded.packet,
                Packet::ConnAck {
                    session_present: false,
                    code: value
                }
            );
        }
    }
}

fn real_plain_config() -> BrokerConfig {
    // Public synthetic low-cost fixture, never a recommended production hash.
    let policy = mqtt_auth::StaticPolicy::from_json(br##"{"allow_anonymous":false,"users":[{"username":"alice","password_hash":"$argon2id$v=19$m=32,t=1,p=1$bXF0dC10ZXN0LXNhbHQtMDE$9nKCHYN7W3NFFVjg+4h4JqpV8p5TDVUKGWdEeVvnJYg","client_id_prefix":"","publish":["#"],"subscribe":["#"]}] }"##).unwrap();
    let mut config = BrokerConfig {
        access_control: Arc::new(policy),
        ..BrokerConfig::default()
    };
    config.enhanced_auth = Some(Arc::new(PlainAuthenticator::new(
        config.access_control.clone(),
    )));
    config
}

#[tokio::test]
async fn mqtt_3_15_2_2_4_auth_diagnostic_text_cannot_override_exchange_outcomes() {
    let (address, shutdown, task) = broker(real_plain_config()).await;
    for text in [
        "Authentication failed; disconnect now",
        "{\"reason\":135,\"success\":false}",
        "诊断 🦀 \u{feff}",
        "",
    ] {
        let mut stream = TcpStream::connect(address).await.unwrap();
        write(&mut stream, connect("diagnostic", None)).await;
        assert_eq!(read(&mut stream).await.reason_code.value(), 0x18);
        let mut answer = auth(0x18, Some(b"\0alice\0alice-test-password"));
        answer.properties.push(Property::ReasonString(text.into()));
        write(&mut stream, answer).await;
        let accepted = read(&mut stream).await;
        assert_eq!(
            accepted.packet,
            Packet::ConnAck {
                session_present: false,
                code: 0
            }
        );
        method(&accepted);
        let mut start = auth(0x19, None);
        start.properties.push(Property::ReasonString(text.into()));
        write(&mut stream, start).await;
        assert_eq!(read(&mut stream).await.reason_code.value(), 0x18);
        let mut finish = auth(0x18, Some(b"\0alice\0alice-test-password"));
        finish.properties.push(Property::ReasonString(text.into()));
        write(&mut stream, finish).await;
        let success = read(&mut stream).await;
        assert_eq!(success.packet, Packet::Auth);
        assert_eq!(success.reason_code.value(), 0);
        method(&success);
        write(&mut stream, Frame::v5(Packet::PingReq)).await;
        assert_eq!(read(&mut stream).await.packet, Packet::PingResp);
        // A diagnostic claiming success must not authenticate bad credentials.
        let mut bad = auth(0x19, Some(b"\0alice\0wrong"));
        bad.properties
            .push(Property::ReasonString("success; reason=0".into()));
        write(&mut stream, bad).await;
        let refused = read(&mut stream).await;
        assert_eq!(refused.packet, Packet::Disconnect);
        assert_eq!(refused.reason_code, ReasonCode::NOT_AUTHORIZED);
        closed(&mut stream).await;
        let mut initial = TcpStream::connect(address).await.unwrap();
        write(&mut initial, connect("bad-initial", None)).await;
        assert_eq!(read(&mut initial).await.reason_code.value(), 0x18);
        let mut wrong = auth(0x18, Some(b"\0alice\0wrong"));
        wrong
            .properties
            .push(Property::ReasonString("success; reason=0".into()));
        write(&mut initial, wrong).await;
        assert_eq!(
            read(&mut initial).await.packet,
            Packet::ConnAck {
                session_present: false,
                code: 0x86
            }
        );
        closed(&mut initial).await;
    }
    stop(shutdown, task).await;
}
