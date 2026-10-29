//! Scoped evidence for RAP, ordered topics and the optional payload validator.
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
        max_sessions: 12,
        max_subscriptions: 16,
        max_queued: 32,
        max_inflight: 16,
        max_packet_size: 8192,
        max_state_bytes: 1 << 20,
        max_retained: 16,
    }
}
fn options() -> ConnectionOptions {
    ConnectionOptions {
        version: ProtocolVersion::V5,
        clean_start: false,
        session_expiry_interval: u32::MAX,
        receive_maximum: 16,
        maximum_packet_size: u32::MAX,
        will_properties: Vec::new(),
    }
}
fn connect(e: &mut SessionEngine, id: &str) {
    e.connect_with_options(id, None, Principal::anonymous(), options())
        .unwrap();
}
fn engine() -> SessionEngine {
    SessionEngine::new_with_clock(config(), Arc::new(AllowAnonymous), Arc::new(FixedClock)).unwrap()
}
fn restored(e: &SessionEngine) -> SessionEngine {
    SessionEngine::restore_with_clock(
        config(),
        &e.snapshot().unwrap(),
        Arc::new(AllowAnonymous),
        Arc::new(FixedClock),
    )
    .unwrap()
}
fn subscribe(e: &mut SessionEngine, id: &str, topic: &str, flags: u8, sid: u32) {
    let mut f = Frame::v5(Packet::Subscribe {
        packet_id: 55,
        filters: vec![(topic.into(), flags & 3)],
    });
    f.subscription_options = vec![flags];
    f.properties.push(Property::SubscriptionIdentifier(sid));
    let out = e.handle_frame(id, f).unwrap();
    assert!(matches!(&out[0].packet, Packet::SubAck { codes, .. } if codes == &vec![flags & 3]));
}
fn publish(e: &mut SessionEngine, source: &str, qos: u8, id: u16, value: &[u8], retain: bool) {
    e.handle_frame(
        source,
        Frame::v5(Packet::Publish {
            topic: "ordered/value".into(),
            payload: value.to_vec(),
            qos,
            packet_id: (qos != 0).then_some(id),
            dup: false,
            retain,
        }),
    )
    .unwrap();
}
fn next(e: &mut SessionEngine, id: &str) -> Frame {
    e.poll_frame_with_changes(id).unwrap().frame.unwrap()
}
fn data(f: &Frame) -> (Vec<u8>, u16, bool, bool) {
    let Packet::Publish {
        payload,
        packet_id,
        dup,
        retain,
        ..
    } = &f.packet
    else {
        panic!("PUBLISH expected");
    };
    (payload.clone(), packet_id.unwrap_or(0), *dup, *retain)
}
fn ack(e: &mut SessionEngine, id: &str, f: &Frame) {
    let Packet::Publish {
        qos,
        packet_id: Some(n),
        ..
    } = f.packet
    else {
        return;
    };
    if qos == 1 {
        e.handle_frame(id, Frame::v5(Packet::PubAck { packet_id: n }))
            .unwrap();
    } else {
        let out = e
            .handle_frame(id, Frame::v5(Packet::PubRec { packet_id: n }))
            .unwrap();
        assert!(matches!(out[0].packet, Packet::PubRel { packet_id } if packet_id == n));
        e.handle_frame(id, Frame::v5(Packet::PubComp { packet_id: n }))
            .unwrap();
    }
}
fn identifier(f: &Frame) -> u32 {
    f.properties
        .iter()
        .find_map(|p| {
            if let Property::SubscriptionIdentifier(n) = p {
                Some(*n)
            } else {
                None
            }
        })
        .unwrap()
}

#[test]
fn rap_single_and_mixed_ordinary_policy_matrix_preserves_source_and_ids() {
    for source_retain in [false, true] {
        for rap_a in [false, true] {
            for rap_b in [false, true] {
                let mut e = engine();
                connect(&mut e, "pub");
                connect(&mut e, "sub");
                subscribe(
                    &mut e,
                    "sub",
                    "ordered/#",
                    0x21 | (u8::from(rap_a) << 3),
                    11,
                );
                subscribe(
                    &mut e,
                    "sub",
                    "ordered/+",
                    0x21 | (u8::from(rap_b) << 3),
                    22,
                );
                publish(&mut e, "pub", 1, 9, b"live", source_retain);
                let split = source_retain && rap_a != rap_b;
                let mut actual = Vec::new();
                for _ in 0..if split { 2 } else { 1 } {
                    let out = next(&mut e, "sub");
                    let mut ids: Vec<_> = out
                        .properties
                        .iter()
                        .filter_map(|p| {
                            if let Property::SubscriptionIdentifier(n) = p {
                                Some(*n)
                            } else {
                                None
                            }
                        })
                        .collect();
                    ids.sort_unstable();
                    actual.push((data(&out).3, ids));
                    ack(&mut e, "sub", &out);
                }
                let mut expected = if split {
                    vec![(rap_a, vec![11]), (rap_b, vec![22])]
                } else {
                    vec![(source_retain && (rap_a || rap_b), vec![11, 22])]
                };
                actual.sort();
                expected.sort();
                assert_eq!(actual, expected);
                assert!(e.poll_frame_with_changes("sub").unwrap().frame.is_none());
            }
        }
    }
}

#[test]
fn rap_shared_and_ordinary_copies_keep_each_subscription_choice() {
    for source_retain in [false, true] {
        let mut e = engine();
        connect(&mut e, "pub");
        connect(&mut e, "sub");
        subscribe(&mut e, "sub", "ordered/#", 0x21, 1);
        subscribe(&mut e, "sub", "$share/a/ordered/#", 0x29, 2);
        subscribe(&mut e, "sub", "$share/b/ordered/+", 0x21, 3);
        publish(&mut e, "pub", 1, 9, b"three", source_retain);
        let mut choices = BTreeMap::new();
        for _ in 0..3 {
            let f = next(&mut e, "sub");
            choices.insert(identifier(&f), data(&f).3);
            ack(&mut e, "sub", &f);
        }
        assert_eq!(
            choices,
            BTreeMap::from([(1, false), (2, source_retain), (3, false)])
        );
        assert!(e.poll_frame_with_changes("sub").unwrap().frame.is_none());
    }
}

#[test]
fn retained_replay_remains_retained_for_both_rap_choices_without_shared_replay() {
    let mut e = engine();
    connect(&mut e, "pub");
    connect(&mut e, "sub");
    publish(&mut e, "pub", 1, 1, b"stored", true);
    subscribe(&mut e, "sub", "ordered/#", 1, 1);
    subscribe(&mut e, "sub", "ordered/+", 9, 2);
    subscribe(&mut e, "sub", "$share/no-replay/ordered/#", 9, 3);
    for sid in [1, 2] {
        let f = next(&mut e, "sub");
        assert_eq!(identifier(&f), sid);
        assert!(data(&f).3);
        ack(&mut e, "sub", &f);
    }
    assert!(e.poll_frame_with_changes("sub").unwrap().frame.is_none());
}

#[test]
fn mqtt_4_6_order_survives_identifier_wrap_restore_and_receive_maximum_one() {
    for qos in [1, 2] {
        let mut e = engine();
        connect(&mut e, "pub");
        connect(&mut e, "sub");
        subscribe(&mut e, "sub", "ordered/#", qos, 1);
        // Advance the real allocator without fabricating private cursor state.
        let ids = &mut e.sessions.get_mut("sub").unwrap().outgoing.identifiers;
        for _ in 0..65534 {
            let id = ids.allocate().unwrap();
            assert!(ids.release(id));
        }
        for (id, payload) in [(60000, b"first".as_slice()), (3, b"second".as_slice())] {
            publish(&mut e, "pub", qos, id, payload, false);
            if qos == 2 {
                e.handle_frame("pub", Frame::v5(Packet::PubRel { packet_id: id }))
                    .unwrap();
            }
        }
        let first = next(&mut e, "sub");
        let second = next(&mut e, "sub");
        assert_eq!((data(&first).1, data(&second).1), (65535, 1));
        let mut e = restored(&e);
        e.connect_with_options(
            "sub",
            None,
            Principal::anonymous(),
            ConnectionOptions {
                receive_maximum: 1,
                ..options()
            },
        )
        .unwrap();
        let replay = next(&mut e, "sub");
        assert_eq!(data(&replay), (b"first".to_vec(), 65535, true, false));
        assert!(e.poll_frame_with_changes("sub").unwrap().frame.is_none());
        ack(&mut e, "sub", &replay);
        let replay = next(&mut e, "sub");
        assert_eq!(data(&replay), (b"second".to_vec(), 1, true, false));
        ack(&mut e, "sub", &replay);
    }
}

#[test]
fn mqtt_4_6_interleaved_sources_preserve_same_source_qos_order_across_recovery() {
    let mut e = engine();
    for id in ["a", "b", "sub"] {
        connect(&mut e, id);
    }
    subscribe(&mut e, "sub", "ordered/#", 2, 1);
    publish(&mut e, "a", 2, 60000, b"a-first", false);
    publish(&mut e, "b", 1, 9, b"b-first", false);
    publish(&mut e, "a", 2, 3, b"a-second", false);
    let mut e = restored(&e);
    for id in ["a", "b", "sub"] {
        connect(&mut e, id);
    }
    e.handle_frame("a", Frame::v5(Packet::PubRel { packet_id: 60000 }))
        .unwrap();
    publish(&mut e, "b", 1, 2, b"b-second", false);
    e.handle_frame("a", Frame::v5(Packet::PubRel { packet_id: 3 }))
        .unwrap();
    let mut payloads = Vec::new();
    for _ in 0..4 {
        let f = next(&mut e, "sub");
        payloads.push(data(&f).0);
        ack(&mut e, "sub", &f);
    }
    assert_eq!(
        payloads,
        [
            b"b-first".to_vec(),
            b"a-first".to_vec(),
            b"b-second".to_vec(),
            b"a-second".to_vec()
        ]
    );
}

#[test]
fn mqtt_4_6_pubrel_resume_does_not_reorder_later_publish_payloads() {
    let mut e = engine();
    connect(&mut e, "pub");
    connect(&mut e, "sub");
    subscribe(&mut e, "sub", "ordered/#", 2, 1);
    for (id, payload) in [
        (8, b"first".as_slice()),
        (1, b"second".as_slice()),
        (7, b"third".as_slice()),
    ] {
        publish(&mut e, "pub", 2, id, payload, false);
        e.handle_frame("pub", Frame::v5(Packet::PubRel { packet_id: id }))
            .unwrap();
    }
    let first = next(&mut e, "sub");
    let second = next(&mut e, "sub");
    let third = next(&mut e, "sub");
    let first_id = data(&first).1;
    e.handle_frame(
        "sub",
        Frame::v5(Packet::PubRec {
            packet_id: first_id,
        }),
    )
    .unwrap();
    let mut e = restored(&e);
    e.connect_with_options(
        "sub",
        None,
        Principal::anonymous(),
        ConnectionOptions {
            receive_maximum: 1,
            ..options()
        },
    )
    .unwrap();
    assert!(
        matches!(next(&mut e, "sub").packet, Packet::PubRel { packet_id } if packet_id == first_id)
    );
    let replay_second = next(&mut e, "sub");
    assert_eq!(data(&replay_second).0, b"second");
    assert_eq!(data(&replay_second).1, data(&second).1);
    e.handle_frame(
        "sub",
        Frame::v5(Packet::PubComp {
            packet_id: first_id,
        }),
    )
    .unwrap();
    assert!(e.poll_frame_with_changes("sub").unwrap().frame.is_none());
    ack(&mut e, "sub", &replay_second);
    let replay_third = next(&mut e, "sub");
    assert_eq!(data(&replay_third).0, b"third");
    assert_eq!(data(&replay_third).1, data(&third).1);
    ack(&mut e, "sub", &replay_third);
}

#[test]
fn pfi_optional_validation_policy_preserves_client_payload_for_publish_and_will() {
    for indicator in [None, Some(0), Some(1)] {
        for payload in [b"valid utf8".as_slice(), b"\xff\x00".as_slice()] {
            let mut e = engine();
            connect(&mut e, "sub");
            connect(&mut e, "pub");
            subscribe(&mut e, "sub", "ordered/#", 1, 1);
            let properties: Vec<_> = indicator
                .map(Property::PayloadFormatIndicator)
                .into_iter()
                .collect();
            let mut f = Frame::v5(Packet::Publish {
                topic: "ordered/value".into(),
                payload: payload.to_vec(),
                qos: 1,
                packet_id: Some(9),
                dup: false,
                retain: true,
            });
            f.properties = properties.clone();
            e.handle_frame("pub", f).unwrap();
            let out = next(&mut e, "sub");
            assert_eq!(data(&out).0, payload);
            assert!(properties.iter().all(|p| out.properties.contains(p)));
            ack(&mut e, "sub", &out);
            let mut opts = options();
            opts.will_properties = properties.clone();
            e.connect_with_options(
                "will-source",
                Some(Will {
                    topic: "ordered/will".into(),
                    payload: payload.to_vec(),
                    qos: 1,
                    retain: true,
                }),
                Principal::anonymous(),
                opts,
            )
            .unwrap();
            e.disconnect_with_reason("will-source", false).unwrap();
            let out = next(&mut e, "sub");
            assert_eq!(data(&out).0, payload);
            assert!(properties.iter().all(|p| out.properties.contains(p)));
            ack(&mut e, "sub", &out);
            connect(&mut e, "late");
            subscribe(&mut e, "late", "ordered/#", 1, 2);
            for _ in 0..2 {
                let out = next(&mut e, "late");
                assert_eq!(data(&out).0, payload);
                assert!(data(&out).3);
                assert!(properties.iter().all(|p| out.properties.contains(p)));
                ack(&mut e, "late", &out);
            }
        }
    }
}
