//! Clause evidence for packet identifiers and non-semantic diagnostics.
use super::*;
use mqtt_protocol::Will;

#[derive(Debug)]
struct FixedClock;
impl Clock for FixedClock {
    fn now_millis(&self) -> u64 {
        1000
    }
}
fn config() -> SessionConfig {
    SessionConfig {
        max_sessions: 8,
        max_retained: 8,
        max_subscriptions: 8,
        max_queued: 16,
        max_inflight: 16,
        max_packet_size: 4096,
        max_state_bytes: 1 << 20,
    }
}
fn options() -> ConnectionOptions {
    ConnectionOptions {
        version: ProtocolVersion::V5,
        clean_start: false,
        session_expiry_interval: 100,
        receive_maximum: 16,
        maximum_packet_size: u32::MAX,
        will_properties: Vec::new(),
    }
}
fn connect(engine: &mut SessionEngine) {
    engine
        .connect_with_options("client", None, Principal::anonymous(), options())
        .unwrap();
}
fn engine() -> SessionEngine {
    let mut engine =
        SessionEngine::new_with_clock(config(), Arc::new(AllowAnonymous), Arc::new(FixedClock))
            .unwrap();
    connect(&mut engine);
    engine
        .handle_frame(
            "client",
            Frame::v5(Packet::Subscribe {
                packet_id: 90,
                filters: vec![("topic/#".into(), 2)],
            }),
        )
        .unwrap();
    engine
}
fn restore(engine: &SessionEngine) -> SessionEngine {
    let mut result = SessionEngine::restore_with_clock(
        config(),
        &engine.snapshot().unwrap(),
        Arc::new(AllowAnonymous),
        Arc::new(FixedClock),
    )
    .unwrap();
    connect(&mut result);
    result
}
fn publish(engine: &mut SessionEngine, qos: u8, packet_id: u16) -> Vec<Frame> {
    engine
        .handle_frame(
            "client",
            Frame::v5(Packet::Publish {
                topic: "topic/value".into(),
                payload: packet_id.to_be_bytes().to_vec(),
                qos,
                packet_id: Some(packet_id),
                dup: false,
                retain: false,
            }),
        )
        .unwrap()
}
fn next(engine: &mut SessionEngine) -> Frame {
    engine
        .poll_frame_with_changes("client")
        .unwrap()
        .frame
        .unwrap()
}
fn outgoing_id(frame: &Frame) -> u16 {
    let Packet::Publish {
        packet_id: Some(id),
        ..
    } = frame.packet
    else {
        panic!("expected PUBLISH")
    };
    id
}
fn handle(engine: &mut SessionEngine, packet: Packet) -> Vec<Frame> {
    engine.handle_frame("client", Frame::v5(packet)).unwrap()
}

#[test]
fn mqtt_2_2_1_5_and_6_response_identifiers_preserve_boundaries() {
    for id in [1, 32768, u16::MAX] {
        for qos in [1, 2] {
            let mut engine = engine();
            let sub = handle(
                &mut engine,
                Packet::Subscribe {
                    packet_id: id,
                    filters: vec![("topic/#".into(), 2)],
                },
            );
            assert_eq!(
                sub[0].packet,
                Packet::SubAck {
                    packet_id: id,
                    codes: vec![2]
                }
            );
            let ack = publish(&mut engine, qos, id);
            assert_eq!(
                ack[0].packet,
                if qos == 1 {
                    Packet::PubAck { packet_id: id }
                } else {
                    Packet::PubRec { packet_id: id }
                }
            );
            if qos == 2 {
                assert_eq!(
                    handle(&mut engine, Packet::PubRel { packet_id: id })[0].packet,
                    Packet::PubComp { packet_id: id }
                );
            }
            let output = next(&mut engine);
            let allocated = outgoing_id(&output);
            if qos == 2 {
                assert_eq!(
                    handle(
                        &mut engine,
                        Packet::PubRec {
                            packet_id: allocated
                        }
                    )[0]
                    .packet,
                    Packet::PubRel {
                        packet_id: allocated
                    }
                );
                handle(
                    &mut engine,
                    Packet::PubComp {
                        packet_id: allocated,
                    },
                );
            } else {
                handle(
                    &mut engine,
                    Packet::PubAck {
                        packet_id: allocated,
                    },
                );
            }
            let unsub = handle(
                &mut engine,
                Packet::Unsubscribe {
                    packet_id: id,
                    filters: vec!["topic/#".into()],
                },
            );
            assert_eq!(unsub.len(), 1);
            assert_eq!(unsub[0].packet, Packet::UnsubAck { packet_id: id });
            assert_eq!(unsub[0].reason_codes[0].value(), 0);
        }
    }
}

#[test]
fn mqtt_2_2_1_4_live_identifiers_survive_restart_until_final_ack() {
    let mut engine = engine();
    publish(&mut engine, 1, 100);
    assert_eq!(outgoing_id(&next(&mut engine)), 1);
    publish(&mut engine, 2, 101);
    handle(&mut engine, Packet::PubRel { packet_id: 101 });
    assert_eq!(outgoing_id(&next(&mut engine)), 2);
    let mut engine = restore(&engine);
    assert_eq!(outgoing_id(&next(&mut engine)), 1);
    assert_eq!(outgoing_id(&next(&mut engine)), 2);
    handle(&mut engine, Packet::PubAck { packet_id: 1 });
    assert_eq!(
        handle(&mut engine, Packet::PubRec { packet_id: 2 })[0].packet,
        Packet::PubRel { packet_id: 2 }
    );
    // A fresh allocator restored with live ID2 must reuse free ID1, never ID2.
    let mut engine = restore(&engine);
    publish(&mut engine, 1, 102);
    assert_eq!(next(&mut engine).packet, Packet::PubRel { packet_id: 2 });
    assert_eq!(outgoing_id(&next(&mut engine)), 1);
    handle(&mut engine, Packet::PubComp { packet_id: 2 });
    let mut engine = restore(&engine);
    assert_eq!(outgoing_id(&next(&mut engine)), 1);
    publish(&mut engine, 1, 103);
    assert_eq!(outgoing_id(&next(&mut engine)), 2);
}

#[test]
fn mqtt_2_2_1_separate_directions_allow_same_id_without_aliasing_exchanges() {
    let mut engine = engine();
    publish(&mut engine, 2, 100);
    handle(&mut engine, Packet::PubRel { packet_id: 100 });
    assert_eq!(outgoing_id(&next(&mut engine)), 1);
    assert_eq!(
        publish(&mut engine, 2, 1)[0].packet,
        Packet::PubRec { packet_id: 1 }
    );
    assert_eq!(engine.sessions["client"].incoming.pending_count(), 1);
    assert_eq!(engine.sessions["client"].outgoing.inflight_count(), 1);
    assert_eq!(
        handle(&mut engine, Packet::PubRel { packet_id: 1 })[0].packet,
        Packet::PubComp { packet_id: 1 }
    );
    assert_eq!(engine.sessions["client"].incoming.pending_count(), 0);
    assert_eq!(engine.sessions["client"].outgoing.inflight_count(), 1);
    assert_eq!(
        handle(&mut engine, Packet::PubRec { packet_id: 1 })[0].packet,
        Packet::PubRel { packet_id: 1 }
    );
    handle(&mut engine, Packet::PubComp { packet_id: 1 });
    assert_eq!(engine.sessions["client"].outgoing.inflight_count(), 0);
    assert_eq!(outgoing_id(&next(&mut engine)), 2);
}

#[test]
fn mqtt_2_2_1_wrong_stage_ack_preserves_live_identifier_and_original_state() {
    for qos in [1, 2] {
        for released in [false, true] {
            if qos == 1 && released {
                continue;
            }
            let mut base = engine();
            publish(&mut base, qos, 100);
            if qos == 2 {
                handle(&mut base, Packet::PubRel { packet_id: 100 });
            }
            let id = outgoing_id(&next(&mut base));
            if released {
                handle(&mut base, Packet::PubRec { packet_id: id });
            }
            let wrong = if qos == 1 {
                vec![
                    Packet::PubRec { packet_id: id },
                    Packet::PubComp { packet_id: id },
                ]
            } else if released {
                vec![Packet::PubAck { packet_id: id }]
            } else {
                vec![
                    Packet::PubAck { packet_id: id },
                    Packet::PubComp { packet_id: id },
                ]
            };
            for packet in wrong {
                let mut engine = base.clone();
                let before = engine.snapshot().unwrap();
                assert!(matches!(
                    engine.handle_frame("client", Frame::v5(packet)),
                    Err(EngineError::Qos(SessionError::UnexpectedAck))
                ));
                assert_eq!(engine.snapshot().unwrap(), before);
                assert_eq!(engine.sessions["client"].outgoing.identifiers.in_use(), 1);
            }
        }
    }
}

fn diagnostic_equivalence(base: &SessionEngine, packet: Packet, reason_code: u8) {
    let mut plain = base.clone();
    let mut frame = Frame::v5(packet);
    frame.reason_code = ReasonCode::try_from(reason_code).unwrap();
    if matches!(frame.packet, Packet::Disconnect) {
        frame.properties.push(Property::SessionExpiryInterval(30));
    }
    let expected = plain.handle_frame("client", frame.clone()).unwrap();
    let snapshot = plain.snapshot().unwrap();
    for text in [
        "Success",
        "0x80 change session owner",
        "Packet Identifier not found",
        "消息:正常断开",
    ] {
        let mut engine = base.clone();
        let mut annotated = frame.clone();
        annotated
            .properties
            .push(Property::ReasonString(text.into()));
        assert_eq!(engine.handle_frame("client", annotated).unwrap(), expected);
        assert_eq!(
            engine.snapshot().unwrap(),
            snapshot,
            "diagnostic text must not affect state"
        );
    }
}

#[test]
fn mqtt_diagnostic_reason_strings_do_not_control_pubrec_pubrel_pubcomp_state() {
    let mut outgoing = engine();
    publish(&mut outgoing, 2, 100);
    handle(&mut outgoing, Packet::PubRel { packet_id: 100 });
    let id = outgoing_id(&next(&mut outgoing));
    for code in [0, 0x80] {
        diagnostic_equivalence(&outgoing, Packet::PubRec { packet_id: id }, code);
    }
    handle(&mut outgoing, Packet::PubRec { packet_id: id });
    for code in [0, 0x92] {
        diagnostic_equivalence(&outgoing, Packet::PubComp { packet_id: id }, code);
    }
    let mut incoming = engine();
    publish(&mut incoming, 2, 65535);
    for code in [0, 0x92] {
        diagnostic_equivalence(&incoming, Packet::PubRel { packet_id: 65535 }, code);
    }
    diagnostic_equivalence(&incoming, Packet::PubRel { packet_id: 32768 }, 0);
}

#[test]
fn mqtt_diagnostic_reason_string_does_not_change_disconnect_expiry_or_will() {
    let mut base = engine();
    base.connect_with_options(
        "client",
        Some(Will {
            topic: "will/value".into(),
            payload: b"last".to_vec(),
            qos: 1,
            retain: true,
        }),
        Principal::anonymous(),
        options(),
    )
    .unwrap();
    for code in [0, 4] {
        diagnostic_equivalence(&base, Packet::Disconnect, code);
    }
    let mut normal = base.clone();
    handle(&mut normal, Packet::Disconnect);
    assert!(normal.retained.is_empty());
    let mut abnormal = base;
    let mut frame = Frame::v5(Packet::Disconnect);
    frame.reason_code = ReasonCode::try_from(4).unwrap();
    abnormal.handle_frame("client", frame).unwrap();
    assert_eq!(abnormal.retained.matching("will/#", 1).len(), 1);
}

#[test]
fn mqtt_2_2_1_5_incoming_qos2_identifiers_and_original_survive_restore() {
    for id in [1, u16::MAX] {
        let mut original = engine();
        assert_eq!(
            publish(&mut original, 2, id)[0].packet,
            Packet::PubRec { packet_id: id }
        );
        let mut restored = restore(&original);
        let duplicate = Frame::v5(Packet::Publish {
            topic: "topic/replacement".into(),
            payload: b"must not replace".to_vec(),
            qos: 2,
            packet_id: Some(id),
            dup: true,
            retain: false,
        });
        assert_eq!(
            restored.handle_frame("client", duplicate).unwrap()[0].packet,
            Packet::PubRec { packet_id: id }
        );
        assert_eq!(
            handle(&mut restored, Packet::PubRel { packet_id: id })[0].packet,
            Packet::PubComp { packet_id: id }
        );
        let output = next(&mut restored);
        let Packet::Publish { topic, payload, .. } = output.packet else {
            panic!("expected original PUBLISH")
        };
        assert_eq!(topic, "topic/value");
        assert_eq!(payload, id.to_be_bytes());
        let repeated = handle(&mut restored, Packet::PubRel { packet_id: id });
        assert_eq!(repeated[0].packet, Packet::PubComp { packet_id: id });
        assert_eq!(repeated[0].reason_code.value(), 0x92);
        assert!(
            restored
                .poll_frame_with_changes("client")
                .unwrap()
                .frame
                .is_none()
        );
    }
}
