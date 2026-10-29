//! Version negotiation must not mutate an existing authenticated connection.
use super::*;
use std::sync::atomic::Ordering;

fn basic(client: &str) -> Frame {
    Frame::v5(Packet::Connect {
        client_id: client.into(),
        clean_session: true,
        keep_alive: 0,
        will: None,
        username: None,
        password: None,
    })
}

#[tokio::test]
async fn unknown_version_refusal_does_not_takeover_publish_pipeline_or_fire_will() {
    let (address, shutdown, task) = broker(BrokerConfig::default()).await;
    let mut owner = TcpStream::connect(address).await.unwrap();
    write(&mut owner, basic("same-owner")).await;
    assert!(matches!(
        read(&mut owner).await.packet,
        Packet::ConnAck { code: 0, .. }
    ));
    let mut observer = TcpStream::connect(address).await.unwrap();
    write(&mut observer, basic("observer")).await;
    assert!(matches!(
        read(&mut observer).await.packet,
        Packet::ConnAck { code: 0, .. }
    ));
    write(
        &mut observer,
        Frame::v5(Packet::Subscribe {
            packet_id: 9,
            filters: vec![("version/#".into(), 1)],
        }),
    )
    .await;
    assert_eq!(
        read(&mut observer).await.packet,
        Packet::SubAck {
            packet_id: 9,
            codes: vec![1]
        }
    );

    for level in [0, 3, 6, 255] {
        let mut replacement = basic("same-owner");
        if let Packet::Connect { will, .. } = &mut replacement.packet {
            *will = Some(mqtt_protocol::Will {
                topic: "version/will".into(),
                payload: b"must-not-fire".to_vec(),
                qos: 1,
                retain: true,
            });
        }
        let mut wire = mqtt_protocol::encode_frame(&replacement).unwrap();
        // RemainingLength fits one byte: FixedHeader2 + Name6 => version offset8.
        assert!(wire[1] < 128);
        assert_eq!(wire[8], 5);
        wire[8] = level;
        wire.extend(
            mqtt_protocol::encode_frame(&Frame::v5(Packet::Publish {
                topic: "version/pipeline".into(),
                payload: b"must-not-route".to_vec(),
                qos: 1,
                packet_id: Some(17),
                retain: true,
                dup: false,
            }))
            .unwrap(),
        );
        let mut rejected = TcpStream::connect(address).await.unwrap();
        rejected.write_all(&wire).await.unwrap();
        let mut refusal = [0; 4];
        rejected.read_exact(&mut refusal).await.unwrap();
        assert_eq!(refusal, [0x20, 2, 0, 1]);
        closed(&mut rejected).await;
        write(&mut owner, Frame::v5(Packet::PingReq)).await;
        assert_eq!(read(&mut owner).await.packet, Packet::PingResp);
        write(&mut observer, Frame::v5(Packet::PingReq)).await;
        assert_eq!(read(&mut observer).await.packet, Packet::PingResp);
        let mut byte = [0];
        assert!(
            timeout(Duration::from_millis(25), observer.read(&mut byte))
                .await
                .is_err()
        );
    }
    // An extra retained subscription detects illicit retained pipeline/Will writes.
    write(
        &mut observer,
        Frame::v5(Packet::Subscribe {
            packet_id: 10,
            filters: vec![("version/#".into(), 1)],
        }),
    )
    .await;
    assert_eq!(
        read(&mut observer).await.packet,
        Packet::SubAck {
            packet_id: 10,
            codes: vec![1]
        }
    );
    write(&mut observer, Frame::v5(Packet::PingReq)).await;
    assert_eq!(read(&mut observer).await.packet, Packet::PingResp);
    let mut byte = [0];
    assert!(
        timeout(Duration::from_millis(25), observer.read(&mut byte))
            .await
            .is_err()
    );
    write(&mut owner, Frame::v5(Packet::Disconnect)).await;
    closed(&mut owner).await;
    stop(shutdown, task).await;
}

#[tokio::test]
async fn explicit_protocol_profiles_select_acceptance_and_refusal_wire_family() {
    use crate::ProtocolMode;
    for mode in [ProtocolMode::Both, ProtocolMode::V311, ProtocolMode::V5] {
        let config = BrokerConfig {
            protocol_mode: mode,
            ..BrokerConfig::default()
        };
        let (address, shutdown, task) = broker(config).await;
        for version in [ProtocolVersion::V311, ProtocolVersion::V5] {
            let mut request = basic("profile");
            request.version = version;
            let mut stream = TcpStream::connect(address).await.unwrap();
            write(&mut stream, request).await;
            let accepted = mode == ProtocolMode::Both
                || (mode == ProtocolMode::V311 && version == ProtocolVersion::V311)
                || (mode == ProtocolMode::V5 && version == ProtocolVersion::V5);
            if accepted {
                if version == ProtocolVersion::V311 {
                    let mut bytes = [0; 4];
                    stream.read_exact(&mut bytes).await.unwrap();
                    assert_eq!(bytes, [0x20, 2, 0, 0]);
                } else {
                    assert!(matches!(
                        read(&mut stream).await.packet,
                        Packet::ConnAck { code: 0, .. }
                    ));
                }
                let mut disconnect = Frame::v5(Packet::Disconnect);
                disconnect.version = version;
                write(&mut stream, disconnect).await;
            } else if mode == ProtocolMode::V5 {
                let mut bytes = [0; 5];
                stream.read_exact(&mut bytes).await.unwrap();
                assert_eq!(bytes, [0x20, 3, 0, 0x84, 0]);
            } else {
                let mut bytes = [0; 4];
                stream.read_exact(&mut bytes).await.unwrap();
                assert_eq!(bytes, [0x20, 2, 0, 1]);
            }
            closed(&mut stream).await;
        }
        for level in [0, 3, 6, 255] {
            let mut wire = b"\x10\x0f\x00\x04MQTT\x04\x02\x00\x00\x00\x03cid".to_vec();
            wire[8] = level;
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream.write_all(&wire).await.unwrap();
            if mode == ProtocolMode::V5 {
                let mut bytes = [0; 5];
                stream.read_exact(&mut bytes).await.unwrap();
                assert_eq!(bytes, [0x20, 3, 0, 0x84, 0]);
            } else {
                let mut bytes = [0; 4];
                stream.read_exact(&mut bytes).await.unwrap();
                assert_eq!(bytes, [0x20, 2, 0, 1]);
            }
            closed(&mut stream).await;
        }
        // Incorrect protocol names never establish a version, in any profile.
        let mut malformed = TcpStream::connect(address).await.unwrap();
        malformed
            .write_all(b"\x10\x0e\x00\x04mqtt\x05\x02\x00\x00\x00\x00\x01c")
            .await
            .unwrap();
        closed(&mut malformed).await;
        stop(shutdown, task).await;
    }
}

#[tokio::test]
async fn rejected_known_protocol_profile_respects_known_peer_packet_caps() {
    use crate::ProtocolMode;
    let (address, shutdown, task) = broker(BrokerConfig {
        protocol_mode: ProtocolMode::V311,
        ..BrokerConfig::default()
    })
    .await;
    for cap in [1, 3, 4, 5] {
        let mut request = basic("capped");
        request.properties.push(Property::MaximumPacketSize(cap));
        let mut stream = TcpStream::connect(address).await.unwrap();
        write(&mut stream, request).await;
        if cap >= 4 {
            let mut bytes = [0; 4];
            stream.read_exact(&mut bytes).await.unwrap();
            assert_eq!(bytes, [0x20, 2, 0, 1]);
        }
        closed(&mut stream).await;
    }
    for cap in [3, 4] {
        let mut request = basic("capped");
        request.properties.push(Property::MaximumPacketSize(cap));
        let mut malformed = mqtt_protocol::encode_frame(&request).unwrap();
        assert_eq!(&malformed[18..20], &[0, 6]);
        malformed[18..20].copy_from_slice(&[0x7f, 0xff]);
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(&malformed).await.unwrap();
        if cap == 4 {
            let mut bytes = [0; 4];
            stream.read_exact(&mut bytes).await.unwrap();
            assert_eq!(bytes, [0x20, 2, 0, 1]);
        }
        closed(&mut stream).await;
    }
    stop(shutdown, task).await;
    // An unsupported future version has no trusted MQTT5 property layout. Only
    // the local cap applies; the strict5 minimum refusal is five complete bytes.
    for cap in [4, 5] {
        let (address, shutdown, task) = broker(BrokerConfig {
            protocol_mode: ProtocolMode::V5,
            max_packet_size: cap,
            ..BrokerConfig::default()
        })
        .await;
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"\x10\x0f\x00\x04MQTT\x06\x02\x00\x00\x00\x03cid")
            .await
            .unwrap();
        // The advertised whole packet exceeds these server caps, so the normal
        // resource guard may close before protocol selection. Never partial bytes.
        closed(&mut stream).await;
        stop(shutdown, task).await;
    }
}

async fn read_version(stream: &mut TcpStream, version: ProtocolVersion) -> Frame {
    timeout(Duration::from_secs(2), async {
        let mut bytes = Vec::new();
        loop {
            if let Some((frame, _)) =
                mqtt_protocol::decode_frame(&bytes, 4096, Some(version)).unwrap()
            {
                return frame;
            }
            let mut byte = [0];
            stream.read_exact(&mut byte).await.unwrap();
            bytes.push(byte[0]);
        }
    })
    .await
    .unwrap()
}
#[derive(Debug)]
struct CountAuthentication(Arc<std::sync::atomic::AtomicUsize>);
impl mqtt_auth::AccessControl for CountAuthentication {
    fn authenticate(
        &self,
        _: mqtt_auth::ConnectInfo<'_>,
    ) -> Result<mqtt_auth::Principal, mqtt_auth::AuthError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(mqtt_auth::Principal::anonymous())
    }
    fn authorize(&self, _: &mqtt_auth::Principal, _: mqtt_auth::Action, _: &str) -> bool {
        true
    }
}
#[tokio::test]
async fn rejected_profile_is_gated_before_authentication_takeover_and_will_registration() {
    use crate::ProtocolMode;
    for (mode, accepted, rejected) in [
        (ProtocolMode::V5, ProtocolVersion::V5, ProtocolVersion::V311),
        (
            ProtocolMode::V311,
            ProtocolVersion::V311,
            ProtocolVersion::V5,
        ),
    ] {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (address, shutdown, task) = broker(BrokerConfig {
            protocol_mode: mode,
            access_control: Arc::new(CountAuthentication(calls.clone())),
            ..BrokerConfig::default()
        })
        .await;
        let mut owner = TcpStream::connect(address).await.unwrap();
        let mut original = basic("same-owner");
        original.version = accepted;
        write(&mut owner, original).await;
        assert!(matches!(
            read_version(&mut owner, accepted).await.packet,
            Packet::ConnAck { code: 0, .. }
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let mut replacement = basic("same-owner");
        replacement.version = rejected;
        if let Packet::Connect { will, .. } = &mut replacement.packet {
            *will = Some(mqtt_protocol::Will {
                topic: "profile/will".into(),
                payload: b"must-not-exist".to_vec(),
                qos: 1,
                retain: true,
            });
        }
        let mut wire = mqtt_protocol::encode_frame(&replacement).unwrap();
        let mut publish = Frame::v5(Packet::Publish {
            topic: "profile/pipeline".into(),
            payload: b"must-not-route".to_vec(),
            qos: 1,
            packet_id: Some(3),
            dup: false,
            retain: true,
        });
        publish.version = rejected;
        wire.extend(mqtt_protocol::encode_frame(&publish).unwrap());
        let mut attempt = TcpStream::connect(address).await.unwrap();
        attempt.write_all(&wire).await.unwrap();
        if mode == ProtocolMode::V5 {
            let mut reply = [0; 5];
            attempt.read_exact(&mut reply).await.unwrap();
            assert_eq!(reply, [0x20, 3, 0, 0x84, 0]);
        } else {
            let mut reply = [0; 4];
            attempt.read_exact(&mut reply).await.unwrap();
            assert_eq!(reply, [0x20, 2, 0, 1]);
        }
        closed(&mut attempt).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let mut ping = Frame::v5(Packet::PingReq);
        ping.version = accepted;
        write(&mut owner, ping).await;
        assert_eq!(
            read_version(&mut owner, accepted).await.packet,
            Packet::PingResp
        );
        let mut observer = TcpStream::connect(address).await.unwrap();
        let mut request = basic("observer");
        request.version = accepted;
        write(&mut observer, request).await;
        assert!(matches!(
            read_version(&mut observer, accepted).await.packet,
            Packet::ConnAck { code: 0, .. }
        ));
        let mut subscribe = Frame::v5(Packet::Subscribe {
            packet_id: 7,
            filters: vec![("profile/#".into(), 1)],
        });
        subscribe.version = accepted;
        write(&mut observer, subscribe).await;
        assert_eq!(
            read_version(&mut observer, accepted).await.packet,
            Packet::SubAck {
                packet_id: 7,
                codes: vec![1]
            }
        );
        let mut ping = Frame::v5(Packet::PingReq);
        ping.version = accepted;
        write(&mut observer, ping).await;
        assert_eq!(
            read_version(&mut observer, accepted).await.packet,
            Packet::PingResp
        );
        let mut byte = [0];
        assert!(
            timeout(Duration::from_millis(25), observer.read(&mut byte))
                .await
                .is_err()
        );
        let mut disconnect = Frame::v5(Packet::Disconnect);
        disconnect.version = accepted;
        write(&mut owner, disconnect).await;
        closed(&mut owner).await;
        stop(shutdown, task).await;
    }
}

#[tokio::test]
async fn durable_session_can_resume_after_listener_profile_changes_from_311_to_5() {
    use crate::ProtocolMode;
    let database = super::super::tests::Database::new();
    let mut config = database.config();
    config.protocol_mode = ProtocolMode::V311;
    let (address, shutdown, task) = broker(config.clone()).await;
    let mut owner = TcpStream::connect(address).await.unwrap();
    let mut request = basic("durable-profile");
    request.version = ProtocolVersion::V311;
    if let Packet::Connect { clean_session, .. } = &mut request.packet {
        *clean_session = false;
    }
    write(&mut owner, request).await;
    assert_eq!(
        read_version(&mut owner, ProtocolVersion::V311).await.packet,
        Packet::ConnAck {
            session_present: false,
            code: 0
        }
    );
    write(
        &mut owner,
        Frame::v311(Packet::Subscribe {
            packet_id: 9,
            filters: vec![("profile/queued".into(), 1)],
        }),
    )
    .await;
    assert_eq!(
        read_version(&mut owner, ProtocolVersion::V311).await.packet,
        Packet::SubAck {
            packet_id: 9,
            codes: vec![1]
        }
    );
    write(&mut owner, Frame::v311(Packet::Disconnect)).await;
    closed(&mut owner).await;
    let mut publisher = TcpStream::connect(address).await.unwrap();
    let mut request = basic("publisher");
    request.version = ProtocolVersion::V311;
    write(&mut publisher, request).await;
    assert!(matches!(
        read_version(&mut publisher, ProtocolVersion::V311)
            .await
            .packet,
        Packet::ConnAck { code: 0, .. }
    ));
    write(
        &mut publisher,
        Frame::v311(Packet::Publish {
            topic: "profile/queued".into(),
            payload: b"before-profile-change".to_vec(),
            qos: 1,
            packet_id: Some(71),
            dup: false,
            retain: false,
        }),
    )
    .await;
    assert_eq!(
        read_version(&mut publisher, ProtocolVersion::V311)
            .await
            .packet,
        Packet::PubAck { packet_id: 71 }
    );
    write(&mut publisher, Frame::v311(Packet::Disconnect)).await;
    closed(&mut publisher).await;
    stop(shutdown, task).await;

    config.protocol_mode = ProtocolMode::V5;
    let (address, shutdown, task) = broker(config).await;
    let mut resumed = TcpStream::connect(address).await.unwrap();
    let mut request = basic("durable-profile");
    if let Packet::Connect { clean_session, .. } = &mut request.packet {
        *clean_session = false;
    }
    request.properties.push(Property::SessionExpiryInterval(60));
    write(&mut resumed, request).await;
    assert_eq!(
        read(&mut resumed).await.packet,
        Packet::ConnAck {
            session_present: true,
            code: 0
        }
    );
    let message = read(&mut resumed).await;
    let Packet::Publish {
        topic,
        payload,
        qos,
        packet_id: Some(identifier),
        ..
    } = message.packet
    else {
        panic!("missing recovered publication")
    };
    assert_eq!(topic, "profile/queued");
    assert_eq!(payload, b"before-profile-change");
    assert_eq!(qos, 1);
    write(
        &mut resumed,
        Frame::v5(Packet::PubAck {
            packet_id: identifier,
        }),
    )
    .await;
    write(&mut resumed, Frame::v5(Packet::PingReq)).await;
    assert_eq!(read(&mut resumed).await.packet, Packet::PingResp);
    write(&mut resumed, Frame::v5(Packet::Disconnect)).await;
    closed(&mut resumed).await;
    stop(shutdown, task).await;
}
