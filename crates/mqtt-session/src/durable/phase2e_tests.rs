use super::*;
use mqtt_auth::{AuthError, ConnectInfo};
use std::sync::atomic::{AtomicU64, Ordering};
#[derive(Debug, Default)]
struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
impl TestClock {
    fn set(&self, now: u64) {
        self.0.store(now, Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct Open;
impl AccessControl for Open {
    fn authenticate(&self, _: ConnectInfo<'_>) -> Result<Principal, AuthError> {
        Ok(Principal::anonymous())
    }
    fn authorize(&self, _: &Principal, _: Action, _: &str) -> bool {
        true
    }
}
fn config() -> SessionConfig {
    SessionConfig {
        max_sessions: 16,
        max_retained: 16,
        max_subscriptions: 16,
        max_queued: 16,
        max_inflight: 8,
        max_packet_size: 4096,
        max_state_bytes: 1024 * 1024,
    }
}
fn state() -> (SessionEngine, Arc<TestClock>) {
    let clock = Arc::new(TestClock::default());
    (
        SessionEngine::new_with_clock(config(), Arc::new(Open), clock.clone()).unwrap(),
        clock,
    )
}
fn options(clean: bool, expiry: u32, maximum: u16) -> ConnectionOptions {
    ConnectionOptions {
        version: ProtocolVersion::V5,
        clean_start: clean,
        session_expiry_interval: expiry,
        receive_maximum: maximum,
        maximum_packet_size: u32::MAX,
        will_properties: Vec::new(),
    }
}
fn connect(s: &mut SessionEngine, id: &str, clean: bool, expiry: u32, max: u16) -> bool {
    s.connect_with_options(
        id,
        None,
        Principal::anonymous(),
        options(clean, expiry, max),
    )
    .unwrap()
    .session_present
}
fn frame(packet: Packet) -> Frame {
    Frame::v5(packet)
}
fn subscribe(s: &mut SessionEngine, id: &str, topic: &str, qos: u8) {
    let result = s
        .handle_frame(
            id,
            frame(Packet::Subscribe {
                packet_id: 1,
                filters: vec![(topic.into(), qos)],
            }),
        )
        .unwrap();
    assert!(matches!(&result[0].packet,Packet::SubAck{codes,..} if codes==&[qos]));
}
fn publication(topic: &str, payload: &[u8], qos: u8, id: u16, retain: bool) -> Packet {
    Packet::Publish {
        topic: topic.into(),
        payload: payload.to_vec(),
        qos,
        packet_id: if qos == 0 { None } else { Some(id) },
        dup: false,
        retain,
    }
}
fn publish(s: &mut SessionEngine, topic: &str, qos: u8, id: u16) {
    s.handle("p", publication(topic, b"data", qos, id, false))
        .unwrap();
    if qos == 2 {
        s.handle("p", Packet::PubRel { packet_id: id }).unwrap();
    }
}
fn next(s: &mut SessionEngine, id: &str) -> Frame {
    s.poll_frame_with_changes(id).unwrap().frame.unwrap()
}
fn id(frame: &Frame) -> u16 {
    match frame.packet {
        Packet::Publish {
            packet_id: Some(id),
            ..
        }
        | Packet::PubRel { packet_id: id } => id,
        _ => panic!("expected ID {frame:?}"),
    }
}
fn metadata() -> Properties {
    vec![
        Property::UserProperty("k".into(), "one".into()),
        Property::UserProperty("k".into(), "two".into()),
        Property::ContentType("application/test".into()),
        Property::PayloadFormatIndicator(0),
        Property::ResponseTopic("response/a".into()),
        Property::CorrelationData(vec![0, 255]),
    ]
}

/// Convert our current output to an actual old schema3 fixture before further v1/2 downgrade.
pub(super) fn legacy_schema3(mut json: serde_json::Value) -> serde_json::Value {
    json = super::phase2h_tests::schema5(json);
    json["schema_version"] = 3.into();
    json.as_object_mut().unwrap().remove("clock_floor");
    for session in json["sessions"].as_array_mut().unwrap() {
        session
            .as_object_mut()
            .unwrap()
            .remove("session_expiry_interval");
        session.as_object_mut().unwrap().remove("expires_at");
        for incoming in session["incoming"].as_array_mut().unwrap() {
            incoming.as_array_mut().unwrap().pop();
            incoming.as_array_mut().unwrap().pop();
        }
        for outgoing in session["outgoing"].as_array_mut().unwrap() {
            outgoing.as_object_mut().unwrap().remove("properties");
            outgoing.as_object_mut().unwrap().remove("expires_at");
            outgoing.as_object_mut().unwrap().remove("started");
        }
        for queued in session["queued"].as_array_mut().unwrap() {
            queued.as_object_mut().unwrap().remove("properties");
            queued.as_object_mut().unwrap().remove("expires_at");
        }
    }
    for retained in json["retained"].as_array_mut().unwrap() {
        retained.as_object_mut().unwrap().remove("properties");
        retained.as_object_mut().unwrap().remove("expires_at");
    }
    for will in json["wills"].as_array_mut().unwrap() {
        will["will"].as_object_mut().unwrap().remove("properties");
        will.as_object_mut().unwrap().remove("delay_interval");
        will.as_object_mut().unwrap().remove("due_at");
    }
    json
}
#[test]
fn clean_start_and_expiry_are_independent_including_resumed_zero() {
    let (mut s, _) = state();
    s.connect("p", true).unwrap();
    for clean in [false, true] {
        for expiry in [0, 10, u32::MAX] {
            assert!(!connect(&mut s, "a", true, u32::MAX, 8));
            subscribe(&mut s, "a", "data", 1);
            s.disconnect("a").unwrap();
            publish(&mut s, "data", 1, 1);
            assert_eq!(connect(&mut s, "a", clean, expiry, 8), !clean);
            assert_eq!(
                s.poll_frame_with_changes("a").unwrap().frame.is_some(),
                !clean
            );
            s.disconnect("a").unwrap();
            assert_eq!(s.sessions.contains_key("a"), expiry != 0);
            if expiry != 0 {
                connect(&mut s, "a", true, 0, 8);
                s.disconnect("a").unwrap();
            }
        }
    }
}
#[test]
fn finite_deadline_only_runs_offline_and_releases_owner_and_subscriptions() {
    let (mut s, clock) = state();
    clock.set(1234);
    connect(&mut s, "a", true, 1, 8);
    subscribe(&mut s, "a", "data", 1);
    clock.set(10000);
    assert!(!s.expire_sessions().unwrap());
    s.disconnect("a").unwrap();
    assert_eq!(s.next_expiry_deadline(), Some(11000));
    clock.set(10999);
    assert!(!s.expire_sessions().unwrap());
    clock.set(11000);
    assert!(s.expire_sessions().unwrap());
    assert!(!s.sessions.contains_key("a"));
    assert!(
        !s.connect_with_options(
            "a",
            None,
            Principal {
                name: "another".into()
            },
            options(false, 1, 8)
        )
        .unwrap()
        .session_present
    );
    assert!(s.sessions["a"].subscriptions.is_empty());
}
#[test]
fn restored_offline_deadline_does_not_extend_and_active_crash_starts_conservatively() {
    let (mut s, clock) = state();
    clock.set(1000);
    connect(&mut s, "offline", true, 2, 8);
    connect(&mut s, "active", true, 2, 8);
    connect(&mut s, "infinite", true, u32::MAX, 8);
    s.disconnect("offline").unwrap();
    let bytes = s.snapshot().unwrap();
    clock.set(2500);
    let restored =
        SessionEngine::restore_with_clock(config(), &bytes, Arc::new(Open), clock.clone()).unwrap();
    assert_eq!(restored.sessions["offline"].expires_at, Some(3000));
    assert_eq!(restored.sessions["active"].expires_at, Some(4500));
    let bytes = restored.snapshot().unwrap();
    clock.set(4000);
    let mut restored =
        SessionEngine::restore_with_clock(config(), &bytes, Arc::new(Open), clock.clone()).unwrap();
    assert_eq!(restored.sessions["active"].expires_at, Some(4500));
    assert!(restored.expire_sessions().unwrap());
    assert!(!restored.sessions.contains_key("offline"));
    assert!(restored.sessions.contains_key("active"));
    clock.set(u64::MAX);
    restored.expire_sessions().unwrap();
    assert!(restored.sessions.contains_key("infinite"));
}
#[test]
fn disconnect_expiry_override_zero_and_invalid_zero_to_positive() {
    let (mut s, clock) = state();
    connect(&mut s, "zero", true, 0, 8);
    let before = s.snapshot().unwrap();
    assert!(
        matches!(s.disconnect_with_options("zero",true,Some(1)),Err(EngineError::Mqtt5(code)) if code.value()==0x82)
    );
    assert_eq!(s.snapshot().unwrap(), before);
    connect(&mut s, "positive", true, 100, 8);
    clock.set(1234);
    s.disconnect_with_options("positive", true, Some(1))
        .unwrap();
    assert_eq!(s.sessions["positive"].expires_at, Some(2234));
    connect(&mut s, "positive", false, 100, 8);
    s.disconnect_with_options("positive", true, Some(0))
        .unwrap();
    assert!(!s.sessions.contains_key("positive"));
}
#[test]
fn receive_maximum_one_is_held_through_pubrec_until_pubcomp_and_controls_progress() {
    let (mut s, _) = state();
    s.connect("p", true).unwrap();
    connect(&mut s, "a", true, 10, 1);
    subscribe(&mut s, "a", "data", 2);
    publish(&mut s, "data", 2, 10);
    publish(&mut s, "data", 1, 11);
    let first = next(&mut s, "a");
    assert!(s.poll_frame_with_changes("a").unwrap().frame.is_none());
    let replies = s
        .handle_frame(
            "a",
            frame(Packet::PubRec {
                packet_id: id(&first),
            }),
        )
        .unwrap();
    assert!(matches!(replies[0].packet, Packet::PubRel { .. }));
    assert!(s.poll_frame_with_changes("a").unwrap().frame.is_none());
    assert_eq!(
        s.handle_frame("a", frame(Packet::PingReq)).unwrap()[0].packet,
        Packet::PingResp
    );
    s.handle_frame(
        "a",
        frame(Packet::PubComp {
            packet_id: id(&first),
        }),
    )
    .unwrap();
    assert!(matches!(
        next(&mut s, "a").packet,
        Packet::Publish { qos: 1, .. }
    ));
}
#[test]
fn reconnect_lower_quota_replays_order_but_released_control_bypasses_blocked_publish() {
    let (mut s, _) = state();
    s.connect("p", true).unwrap();
    connect(&mut s, "a", true, 10, 8);
    subscribe(&mut s, "a", "data", 2);
    publish(&mut s, "data", 1, 10);
    let first = next(&mut s, "a");
    publish(&mut s, "data", 2, 11);
    let second = next(&mut s, "a");
    publish(&mut s, "data", 2, 12);
    let third = next(&mut s, "a");
    s.handle_frame(
        "a",
        frame(Packet::PubRec {
            packet_id: id(&third),
        }),
    )
    .unwrap();
    s.disconnect("a").unwrap();
    assert!(connect(&mut s, "a", false, 10, 1));
    let replay = next(&mut s, "a");
    assert_eq!(id(&replay), id(&first));
    assert!(matches!(replay.packet, Packet::Publish { dup: true, .. }));
    assert_eq!(
        next(&mut s, "a").packet,
        Packet::PubRel {
            packet_id: id(&third)
        }
    );
    assert!(s.poll_frame_with_changes("a").unwrap().frame.is_none());
    s.handle_frame(
        "a",
        frame(Packet::PubComp {
            packet_id: id(&third),
        }),
    )
    .unwrap();
    assert!(s.poll_frame_with_changes("a").unwrap().frame.is_none());
    s.handle_frame(
        "a",
        frame(Packet::PubAck {
            packet_id: id(&first),
        }),
    )
    .unwrap();
    assert_eq!(id(&next(&mut s, "a")), id(&second));
}
#[test]
fn negative_ack_releases_exchange_metadata_and_connection_quota() {
    let (mut s, _) = state();
    s.connect("p", true).unwrap();
    connect(&mut s, "a", true, 10, 1);
    subscribe(&mut s, "a", "data", 2);
    for qos in [1, 2] {
        publish(&mut s, "data", qos, 20);
        publish(&mut s, "data", 1, 21);
        let first = next(&mut s, "a");
        let mut ack = frame(if qos == 1 {
            Packet::PubAck {
                packet_id: id(&first),
            }
        } else {
            Packet::PubRec {
                packet_id: id(&first),
            }
        });
        ack.reason_code = ReasonCode::NOT_AUTHORIZED;
        s.handle_frame("a", ack).unwrap();
        let next = next(&mut s, "a");
        s.handle_frame(
            "a",
            frame(Packet::PubAck {
                packet_id: id(&next),
            }),
        )
        .unwrap();
    }
    publish(&mut s, "data", 2, 22);
    let exchange = next(&mut s, "a");
    let before = s.snapshot().unwrap();
    let mut wrong = frame(Packet::PubComp {
        packet_id: id(&exchange),
    });
    wrong.reason_code = ReasonCode::PACKET_IDENTIFIER_NOT_FOUND;
    assert!(matches!(
        s.handle_frame("a", wrong),
        Err(EngineError::Qos(SessionError::UnexpectedAck))
    ));
    assert_eq!(s.snapshot().unwrap(), before);
    s.handle_frame(
        "a",
        frame(Packet::PubRec {
            packet_id: id(&exchange),
        }),
    )
    .unwrap();
    let mut final_ack = frame(Packet::PubComp {
        packet_id: id(&exchange),
    });
    final_ack.reason_code = ReasonCode::PACKET_IDENTIFIER_NOT_FOUND;
    s.handle_frame("a", final_ack).unwrap();
    assert!(s.sessions["a"].outgoing.pending.is_empty());
    assert!(s.sessions["a"].outgoing_properties.is_empty());
}
#[test]
fn metadata_survives_duplicate_incoming_restart_retained_and_cross_version_replay() {
    let (mut s, clock) = state();
    connect(&mut s, "p", true, u32::MAX, 8);
    connect(&mut s, "a", true, u32::MAX, 8);
    subscribe(&mut s, "a", "data", 2);
    let mut source = frame(publication("data", b"original", 2, 33, true));
    source.properties = metadata();
    s.handle_frame("p", source.clone()).unwrap();
    source.properties = vec![Property::UserProperty(
        "forged".into(),
        "replacement".into(),
    )];
    if let Packet::Publish { dup, .. } = &mut source.packet {
        *dup = true;
    }
    s.handle_frame("p", source).unwrap();
    let mut s =
        SessionEngine::restore_with_clock(config(), &s.snapshot().unwrap(), Arc::new(Open), clock)
            .unwrap();
    connect(&mut s, "p", false, u32::MAX, 8);
    s.handle_frame("p", frame(Packet::PubRel { packet_id: 33 }))
        .unwrap();
    connect(&mut s, "a", false, u32::MAX, 8);
    let received = next(&mut s, "a");
    assert_eq!(received.properties, metadata());
    let packet_id = id(&received);
    s.disconnect("a").unwrap();
    s.connect("a", false).unwrap();
    let legacy = next(&mut s, "a");
    assert!(legacy.properties.is_empty());
    assert_eq!(id(&legacy), packet_id);
    s.disconnect("a").unwrap();
    connect(&mut s, "a", false, u32::MAX, 8);
    assert_eq!(next(&mut s, "a").properties, metadata());
    connect(&mut s, "later", true, 0, 8);
    subscribe(&mut s, "later", "data", 1);
    let retained = next(&mut s, "later");
    assert_eq!(retained.properties, metadata());
    assert!(matches!(
        retained.packet,
        Packet::Publish { retain: true, .. }
    ));
}
#[test]
fn will_metadata_and_expiry_zero_disconnection_keep_global_retained() {
    let (mut s, _) = state();
    connect(&mut s, "a", true, 0, 8);
    subscribe(&mut s, "a", "status", 1);
    let mut opts = options(true, 0, 8);
    opts.will_properties = metadata();
    opts.will_properties.push(Property::WillDelayInterval(0));
    let will = mqtt_protocol::Will {
        topic: "status".into(),
        payload: b"bye".to_vec(),
        qos: 1,
        retain: true,
    };
    s.connect_with_options("source", Some(will.clone()), Principal::anonymous(), opts)
        .unwrap();
    s.disconnect_with_options("source", false, None).unwrap();
    assert_eq!(next(&mut s, "a").properties, metadata());
    assert!(!s.sessions.contains_key("source"));
}
#[test]
fn unsupported_properties_subscription_options_and_unknown_ack_reasons_are_explicit() {
    let (mut s, _) = state();
    connect(&mut s, "a", true, 10, 8);
    let mut sub = frame(Packet::Subscribe {
        packet_id: 1,
        filters: vec![("data".into(), 1)],
    });
    sub.subscription_options = vec![5];
    assert_eq!(
        s.handle_frame("a", sub).unwrap()[0].packet,
        Packet::SubAck {
            packet_id: 1,
            codes: vec![1]
        }
    );
    assert_eq!(
        s.handle_frame("a", frame(Packet::PubRec { packet_id: 999 }))
            .unwrap()[0]
            .reason_code
            .value(),
        0x92
    );
    assert_eq!(
        s.handle_frame("a", frame(Packet::PubRel { packet_id: 999 }))
            .unwrap()[0]
            .reason_code
            .value(),
        0x92
    );
}
#[test]
fn schema3_migration_and_schema4_property_expiry_corruption_are_strict() {
    let (mut s, clock) = state();
    s.connect("p", false).unwrap();
    s.connect("a", false).unwrap();
    s.handle(
        "a",
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![("data".into(), 1)],
        },
    )
    .unwrap();
    publish(&mut s, "data", 1, 10);
    s.poll("a").unwrap();
    let old = legacy_schema3(serde_json::from_slice(&s.snapshot().unwrap()).unwrap());
    let restored = SessionEngine::restore_with_clock(
        config(),
        &serde_json::to_vec(&old).unwrap(),
        Arc::new(Open),
        clock.clone(),
    )
    .unwrap();
    assert!(
        restored
            .sessions
            .values()
            .all(|session| session.expiry == u32::MAX)
    );
    let json: serde_json::Value = serde_json::from_slice(&restored.snapshot().unwrap()).unwrap();
    assert_eq!(json["schema_version"], 7);
    for variant in 0..4 {
        let mut corrupt = json.clone();
        match variant {
            0 => {
                corrupt["sessions"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("session_expiry_interval");
            }
            1 => corrupt["sessions"][0]["expires_at"] = 1.into(),
            2 => corrupt["sessions"][0]["session_expiry_interval"] = 0.into(),
            _ => {
                corrupt["sessions"][0]["outgoing"][0]["properties"] =
                    serde_json::json!([{"TopicAlias":1}])
            }
        };
        assert!(
            SessionEngine::restore_with_clock(
                config(),
                &serde_json::to_vec(&corrupt).unwrap(),
                Arc::new(Open),
                clock.clone()
            )
            .is_err()
        );
    }
}

#[test]
fn early_pubrec_after_reconnect_does_not_duplicate_automatic_pubrel() {
    let (mut s, _) = state();
    s.connect("p", true).unwrap();
    connect(&mut s, "a", true, 10, 8);
    subscribe(&mut s, "a", "data", 2);
    publish(&mut s, "data", 1, 10);
    let first = next(&mut s, "a");
    publish(&mut s, "data", 2, 11);
    let second = next(&mut s, "a");
    s.disconnect("a").unwrap();
    connect(&mut s, "a", false, 10, 1);
    let reply = s
        .handle_frame(
            "a",
            frame(Packet::PubRec {
                packet_id: id(&second),
            }),
        )
        .unwrap();
    assert_eq!(
        reply[0].packet,
        Packet::PubRel {
            packet_id: id(&second)
        }
    );
    assert_eq!(id(&next(&mut s, "a")), id(&first));
    assert!(s.poll_frame_with_changes("a").unwrap().frame.is_none());
}

fn sized_state(limit: usize) -> SessionEngine {
    SessionEngine::new_with_clock(
        SessionConfig {
            max_packet_size: limit,
            ..config()
        },
        Arc::new(Open),
        Arc::new(TestClock::default()),
    )
    .unwrap()
}
fn seed_large_legacy(s: &mut SessionEngine, payload: usize, pending: bool) {
    s.connect("p", true).unwrap();
    if pending {
        s.connect_with_will(
            "a",
            false,
            Some(mqtt_protocol::Will {
                topic: "status".into(),
                payload: b"old-will".to_vec(),
                qos: 1,
                retain: false,
            }),
        )
        .unwrap();
    } else {
        s.connect("a", false).unwrap();
    }
    s.handle(
        "a",
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![("a".into(), 1)],
        },
    )
    .unwrap();
    if !pending {
        s.disconnect("a").unwrap();
    }
    s.handle("p", publication("a", &vec![42; payload], 1, 10, false))
        .unwrap();
    if pending {
        s.poll("a").unwrap().unwrap();
    }
}
#[test]
fn bridge_resume_size_boundaries_preserve_legacy_and_allow_drain_or_clean_reset() {
    for (limit, payload, v5_size) in [(64, 57, 65), (128, 121, 129), (129, 122, 131)] {
        let packet = publication("a", &vec![42; payload], 1, 1, false);
        assert_eq!(encode(&packet).unwrap().len(), limit);
        assert_eq!(encode_frame(&frame(packet)).unwrap().len(), v5_size);
        for pending in [false, true] {
            let mut s = sized_state(limit);
            seed_large_legacy(&mut s, payload, pending);
            let before = s.snapshot().unwrap();
            // Real legacy schema3 remains valid even if a future5 envelope cannot fit.
            let legacy = legacy_schema3(serde_json::from_slice(&before).unwrap());
            let mut restored = SessionEngine::restore_with_clock(
                SessionConfig {
                    max_packet_size: limit,
                    ..config()
                },
                &serde_json::to_vec(&legacy).unwrap(),
                Arc::new(Open),
                Arc::new(TestClock::default()),
            )
            .unwrap();
            restored.recover_wills().unwrap();
            let restored_before = restored.snapshot().unwrap();
            assert!(
                matches!(restored.connect_with_options("a",None,Principal::anonymous(),options(false,u32::MAX,8)),Err(EngineError::Mqtt5(code)) if code.value()==0x83)
            );
            assert_eq!(restored.snapshot().unwrap(), restored_before);
            assert!(restored.connect("a", false).unwrap().session_present);
            assert_eq!(
                encode(&restored.poll("a").unwrap().unwrap()).unwrap().len(),
                limit
            );
            assert!(matches!(
                s.connect_with_options(
                    "a",
                    None,
                    Principal {
                        name: "intruder".into()
                    },
                    options(false, u32::MAX, 8)
                ),
                Err(EngineError::NotAuthorized)
            ));
            assert!(
                matches!(s.connect_with_options("a",None,Principal::anonymous(),options(false,u32::MAX,8)),Err(EngineError::Mqtt5(code)) if code.value()==0x83),
                "limit={limit}, pending={pending}"
            );
            assert_eq!(s.snapshot().unwrap(), before);
            assert_eq!(s.sessions["a"].online, pending);
            assert!(s.connect("a", false).unwrap().session_present);
            let delivery = s.poll("a").unwrap().unwrap();
            assert_eq!(encode(&delivery).unwrap().len(), limit);
            let Packet::Publish {
                packet_id: Some(packet_id),
                dup,
                ..
            } = delivery
            else {
                panic!("publish required")
            };
            assert_eq!(dup, pending);
            s.handle("a", Packet::PubAck { packet_id }).unwrap();
            s.disconnect("a").unwrap();
            assert!(connect(&mut s, "a", false, u32::MAX, 8));
            assert!(s.poll_frame_with_changes("a").unwrap().frame.is_none());
            let mut s = sized_state(limit);
            seed_large_legacy(&mut s, payload, pending);
            assert!(!connect(&mut s, "a", true, u32::MAX, 8));
            assert!(s.poll_frame_with_changes("a").unwrap().frame.is_none());
        }
    }
}
#[test]
fn bridge_live_size_preflight_happens_before_any_recipient_queue_or_source_ack() {
    let mut s = sized_state(64);
    s.connect("p", true).unwrap();
    s.connect("a", false).unwrap();
    s.handle(
        "a",
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![("a".into(), 1)],
        },
    )
    .unwrap();
    connect(&mut s, "b", true, u32::MAX, 8);
    subscribe(&mut s, "b", "a", 1);
    let before = s.snapshot().unwrap();
    let mut proposal = s.clone();
    assert!(matches!(
        proposal.handle("p", publication("a", &[42; 57], 1, 10, true)),
        Err(EngineError::Capacity)
    ));
    assert!(proposal.sessions["a"].queued.is_empty());
    assert!(proposal.sessions["b"].queued.is_empty());
    assert_eq!(s.snapshot().unwrap(), before);
}
#[test]
fn bridge_retained_replay_size_preflight_does_not_install_subscription() {
    let mut s = sized_state(64);
    s.connect("p", true).unwrap();
    s.handle("p", publication("a", &[42; 57], 1, 10, true))
        .unwrap();
    connect(&mut s, "r", true, u32::MAX, 8);
    let before = s.snapshot().unwrap();
    let mut proposal = s.clone();
    assert!(matches!(
        proposal.handle_frame(
            "r",
            frame(Packet::Subscribe {
                packet_id: 1,
                filters: vec![("a".into(), 1)]
            })
        ),
        Err(EngineError::Capacity)
    ));
    assert!(proposal.sessions["r"].subscriptions.is_empty());
    assert!(proposal.sessions["r"].queued.is_empty());
    assert_eq!(s.snapshot().unwrap(), before);
}
#[test]
fn bridge_final_poll_guard_preserves_queue_id_and_replay_cursor_on_overflow() {
    for pending in [false, true] {
        let mut s = sized_state(64);
        seed_large_legacy(&mut s, 57, pending);
        s.connect("a", false).unwrap();
        s.sessions.get_mut("a").unwrap().version = ProtocolVersion::V5;
        let before = s.snapshot().unwrap();
        let queued = s.sessions["a"].queued.len();
        let replay = s.sessions["a"].replay.clone();
        let count = s.sessions["a"].outgoing.inflight_count();
        assert!(matches!(
            s.poll_frame_with_changes("a"),
            Err(EngineError::Capacity)
        ));
        assert_eq!(s.snapshot().unwrap(), before);
        assert_eq!(s.sessions["a"].queued.len(), queued);
        assert_eq!(s.sessions["a"].replay, replay);
        assert_eq!(s.sessions["a"].outgoing.inflight_count(), count);
        assert!(s.sessions["a"].sent.is_empty());
    }
}

#[test]
fn advertised_unsupported_subscription_features_close_before_any_filter_install() {
    let (mut s, _) = state();
    s.connect("p", true).unwrap();
    s.handle("p", publication("data", b"retained", 1, 10, true))
        .unwrap();
    connect(&mut s, "a", true, u32::MAX, 8);
    let before = s.snapshot().unwrap();
    let request = frame(Packet::Subscribe {
        packet_id: 1,
        filters: vec![("data".into(), 1), ("$share/g/data".into(), 5)],
    });
    assert!(
        matches!(s.handle_frame("a",request),Err(EngineError::Mqtt5(code)) if code.value()==0x82)
    );
    assert_eq!(s.snapshot().unwrap(), before);
    assert!(s.sessions["a"].subscriptions.is_empty());
    assert!(s.sessions["a"].queued.is_empty());
}

#[test]
fn bridge_expired_offline_data_does_not_block_new_version_admission() {
    let clock = Arc::new(TestClock::default());
    let mut s = SessionEngine::new_with_clock(
        SessionConfig {
            max_packet_size: 64,
            ..config()
        },
        Arc::new(Open),
        clock.clone(),
    )
    .unwrap();
    s.connect("p", true).unwrap();
    connect(&mut s, "a", true, 1, 8);
    subscribe(&mut s, "a", "a", 1);
    s.disconnect("a").unwrap();
    // Restore resets the runtime version to canonical311 while preserving the
    // finite deadline. Only that known version can accept this legacy envelope.
    let mut s = SessionEngine::restore_with_clock(
        SessionConfig {
            max_packet_size: 64,
            ..config()
        },
        &s.snapshot().unwrap(),
        Arc::new(Open),
        clock.clone(),
    )
    .unwrap();
    s.connect("p", true).unwrap();
    s.handle("p", publication("a", &[42; 57], 1, 10, false))
        .unwrap();
    assert_eq!(s.sessions["a"].queued.len(), 1);
    clock.set(1000);
    assert!(!connect(&mut s, "a", false, 1, 8));
    assert!(s.poll_frame_with_changes("a").unwrap().frame.is_none());
}

#[test]
fn required_schema4_deadline_cannot_be_omitted_for_active_offline_or_infinite_session() {
    let (mut s, clock) = state();
    connect(&mut s, "active", true, 60, 8);
    connect(&mut s, "offline", true, 60, 8);
    connect(&mut s, "infinite", true, u32::MAX, 8);
    s.disconnect("offline").unwrap();
    let json: serde_json::Value = serde_json::from_slice(&s.snapshot().unwrap()).unwrap();
    for client in ["active", "offline", "infinite"] {
        let mut bad = json.clone();
        let record = bad["sessions"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|record| record["client_id"] == client)
            .unwrap();
        record.as_object_mut().unwrap().remove("expires_at");
        assert!(
            SessionEngine::restore_with_clock(
                config(),
                &serde_json::to_vec(&bad).unwrap(),
                Arc::new(Open),
                clock.clone()
            )
            .is_err(),
            "missing deadline accepted for {client}"
        );
    }
    clock.set(30000);
    let restored = SessionEngine::restore_with_clock(
        config(),
        &serde_json::to_vec(&json).unwrap(),
        Arc::new(Open),
        clock.clone(),
    )
    .unwrap();
    assert_eq!(restored.sessions["active"].expires_at, Some(90000));
    assert_eq!(restored.sessions["offline"].expires_at, Some(60000));
    assert_eq!(restored.sessions["infinite"].expires_at, None);
    let legacy = legacy_schema3(json);
    assert!(
        SessionEngine::restore_with_clock(
            config(),
            &serde_json::to_vec(&legacy).unwrap(),
            Arc::new(Open),
            clock
        )
        .is_ok()
    );
}
#[test]
fn known_offline_v5_recipient_rejects_oversized_bridge_before_outbox_insert() {
    let mut s = sized_state(64);
    s.connect("p", true).unwrap();
    connect(&mut s, "a", true, 60, 8);
    subscribe(&mut s, "a", "a", 1);
    s.disconnect("a").unwrap();
    let packet = publication("a", &[42; 57], 1, 10, false);
    assert_eq!(encode(&packet).unwrap().len(), 64);
    assert_eq!(encode_frame(&frame(packet.clone())).unwrap().len(), 65);
    let before = s.snapshot().unwrap();
    let mut proposal = s.clone();
    assert!(matches!(
        proposal.handle("p", packet),
        Err(EngineError::Capacity)
    ));
    assert!(proposal.sessions["a"].queued.is_empty());
    assert_eq!(s.snapshot().unwrap(), before);
    assert!(connect(&mut s, "a", false, 60, 8));
    assert!(s.poll_frame_with_changes("a").unwrap().frame.is_none());
}
