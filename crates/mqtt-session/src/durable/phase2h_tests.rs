use super::*;
fn config() -> SessionConfig {
    SessionConfig {
        max_sessions: 16,
        max_retained: 16,
        max_subscriptions: 16,
        max_queued: 16,
        max_inflight: 16,
        max_packet_size: 8192,
        max_state_bytes: 262144,
    }
}
fn connect(e: &mut SessionEngine, id: &str) {
    e.connect_with_options(
        id,
        None,
        Principal::anonymous(),
        ConnectionOptions {
            version: ProtocolVersion::V5,
            clean_start: false,
            session_expiry_interval: u32::MAX,
            receive_maximum: 32,
            maximum_packet_size: u32::MAX,
            will_properties: vec![],
        },
    )
    .unwrap();
}
fn subscribe(e: &mut SessionEngine, id: &str, filter: &str, options: u8, identifier: Option<u32>) {
    let mut f = Frame::v5(Packet::Subscribe {
        packet_id: 1,
        filters: vec![(filter.into(), options & 3)],
    });
    f.subscription_options = vec![options];
    if let Some(n) = identifier {
        f.properties.push(Property::SubscriptionIdentifier(n));
    }
    let r = e.handle_frame(id, f).unwrap();
    assert!(matches!(&r[0].packet,Packet::SubAck{codes,..} if codes==&vec![options&3]));
}
fn publish(e: &mut SessionEngine, id: &str, retain: bool) {
    e.handle_frame(
        id,
        Frame::v5(Packet::Publish {
            topic: "a/x".into(),
            payload: b"value".to_vec(),
            qos: 1,
            packet_id: Some(2),
            dup: false,
            retain,
        }),
    )
    .unwrap();
}
#[test]
fn no_local_excludes_match_before_ids_and_qos() {
    let mut e = SessionEngine::new(config()).unwrap();
    connect(&mut e, "a");
    subscribe(&mut e, "a", "a/#", 6, Some(7));
    publish(&mut e, "a", false);
    assert!(e.poll_frame_with_changes("a").unwrap().frame.is_none());
}
#[test]
fn rap_and_identifier_live_copy() {
    let mut e = SessionEngine::new(config()).unwrap();
    connect(&mut e, "a");
    connect(&mut e, "b");
    subscribe(&mut e, "b", "a/#", 9, Some(9));
    publish(&mut e, "a", true);
    let f = e.poll_frame_with_changes("b").unwrap().frame.unwrap();
    assert!(matches!(f.packet, Packet::Publish { retain: true, .. }));
    assert_eq!(f.properties, vec![Property::SubscriptionIdentifier(9)]);
}
#[test]
fn retain_handling_two_suppresses_replay() {
    let mut e = SessionEngine::new(config()).unwrap();
    connect(&mut e, "a");
    connect(&mut e, "b");
    publish(&mut e, "a", true);
    subscribe(&mut e, "b", "a/#", 0x21, None);
    assert!(e.poll_frame_with_changes("b").unwrap().frame.is_none());
}

/// Construct only genuine legacy-representable fixtures, never drop new semantics.
pub(super) fn schema5(mut v: serde_json::Value) -> serde_json::Value {
    assert_eq!(v["schema_version"], 7);
    for session in v["sessions"].as_array_mut().unwrap() {
        for sub in session["subscriptions"].as_array().unwrap() {
            assert!(mqtt_router::split_shared_filter(sub["filter"].as_str().unwrap()).is_none());
        }
        for field in ["queued", "outgoing"] {
            for message in session[field].as_array_mut().unwrap() {
                assert!(
                    message
                        .as_object_mut()
                        .unwrap()
                        .remove("shared_subscription")
                        .unwrap()
                        .is_null()
                );
            }
        }
    }
    v["schema_version"] = 5.into();
    for session in v["sessions"].as_array_mut().unwrap() {
        for sub in session["subscriptions"].as_array_mut().unwrap() {
            assert_eq!(sub["no_local"], false);
            assert_eq!(sub["retain_as_published"], false);
            assert_eq!(sub["retain_handling"], 0);
            assert!(sub["identifier"].is_null());
            *sub = serde_json::json!([sub["filter"], sub["qos"]]);
        }
        for q in session["queued"].as_array_mut().unwrap() {
            let marker = q
                .as_object_mut()
                .unwrap()
                .remove("retained_replay")
                .unwrap();
            assert_eq!(marker, q["retain"]);
        }
        for q in session["outgoing"].as_array_mut().unwrap() {
            let marker = q
                .as_object_mut()
                .unwrap()
                .remove("retained_replay")
                .unwrap();
            let retained = q["stage"]
                .as_object()
                .and_then(|o| o.values().next())
                .and_then(|bytes| bytes.as_array())
                .and_then(|a| a.first())
                .and_then(|v| v.as_u64())
                .is_some_and(|b| b & 1 != 0);
            assert_eq!(marker, retained);
        }
    }
    v
}
fn next(e: &mut SessionEngine, id: &str) -> Frame {
    e.poll_frame_with_changes(id).unwrap().frame.unwrap()
}
fn identifiers(f: &Frame) -> Vec<u32> {
    f.properties
        .iter()
        .filter_map(|p| {
            if let Property::SubscriptionIdentifier(n) = p {
                Some(*n)
            } else {
                None
            }
        })
        .collect()
}
fn ack(e: &mut SessionEngine, id: &str, f: &Frame) {
    if let Packet::Publish {
        qos,
        packet_id: Some(n),
        ..
    } = f.packet
    {
        if qos == 1 {
            e.handle_frame(id, Frame::v5(Packet::PubAck { packet_id: n }))
                .unwrap();
        } else if qos == 2 {
            e.handle_frame(id, Frame::v5(Packet::PubRec { packet_id: n }))
                .unwrap();
            e.handle_frame(id, Frame::v5(Packet::PubComp { packet_id: n }))
                .unwrap();
        }
    }
}
#[test]
fn overlap_aggregates_eligible_qos_rap_and_duplicate_identifiers() {
    let mut e = SessionEngine::new(config()).unwrap();
    connect(&mut e, "a");
    subscribe(&mut e, "a", "a/#", 6, Some(1));
    subscribe(&mut e, "a", "+/x", 9, Some(7));
    subscribe(&mut e, "a", "a/+", 1, Some(7));
    publish(&mut e, "a", true);
    let mut retains = Vec::new();
    for _ in 0..2 {
        let f = next(&mut e, "a");
        let Packet::Publish { qos, retain, .. } = f.packet else {
            panic!("PUBLISH")
        };
        assert_eq!(qos, 1);
        assert_eq!(identifiers(&f), vec![7]);
        retains.push(retain);
        ack(&mut e, "a", &f);
    }
    retains.sort_unstable();
    assert_eq!(retains, vec![false, true]);
    assert!(e.poll_frame_with_changes("a").unwrap().frame.is_none());
}
#[test]
fn replacement_clears_identifier_and_unsubscribe_removes_record() {
    let mut e = SessionEngine::new(config()).unwrap();
    connect(&mut e, "a");
    connect(&mut e, "b");
    subscribe(&mut e, "b", "a/#", 1, Some(268435455));
    publish(&mut e, "a", false);
    let f = next(&mut e, "b");
    assert_eq!(identifiers(&f), vec![268435455]);
    ack(&mut e, "b", &f);
    subscribe(&mut e, "b", "a/#", 1, None);
    publish(&mut e, "a", false);
    let f = next(&mut e, "b");
    assert!(identifiers(&f).is_empty());
    ack(&mut e, "b", &f);
    e.handle_frame(
        "b",
        Frame::v5(Packet::Unsubscribe {
            packet_id: 3,
            filters: vec!["a/#".into()],
        }),
    )
    .unwrap();
    assert!(e.sessions["b"].subscriptions.is_empty());
}
#[test]
fn retain_handling_replace_and_replay_identifiers_are_entry_local() {
    let mut e = SessionEngine::new(config()).unwrap();
    connect(&mut e, "a");
    connect(&mut e, "b");
    publish(&mut e, "a", true);
    subscribe(&mut e, "b", "a/#", 0x11, Some(10));
    let f = next(&mut e, "b");
    assert_eq!(identifiers(&f), vec![10]);
    assert!(matches!(f.packet, Packet::Publish { retain: true, .. }));
    ack(&mut e, "b", &f);
    subscribe(&mut e, "b", "a/#", 0x11, Some(11));
    assert!(e.poll_frame_with_changes("b").unwrap().frame.is_none());
    subscribe(&mut e, "b", "a/+", 0x01, Some(12));
    let f = next(&mut e, "b");
    assert_eq!(identifiers(&f), vec![12]);
    ack(&mut e, "b", &f);
    subscribe(&mut e, "b", "a/#", 0x01, None);
    let f = next(&mut e, "b");
    assert!(identifiers(&f).is_empty());
    ack(&mut e, "b", &f);
    let mut packet = Frame::v5(Packet::Subscribe {
        packet_id: 6,
        filters: vec![("a/x".into(), 1), ("a/x".into(), 1)],
    });
    packet.subscription_options = vec![0x11, 0x11];
    packet.properties = vec![Property::SubscriptionIdentifier(13)];
    e.handle_frame("b", packet).unwrap();
    let f = next(&mut e, "b");
    assert_eq!(identifiers(&f), vec![13]);
    ack(&mut e, "b", &f);
    assert!(e.poll_frame_with_changes("b").unwrap().frame.is_none());
}
#[test]
fn no_local_does_not_suppress_subscription_triggered_retained_replay() {
    let mut e = SessionEngine::new(config()).unwrap();
    connect(&mut e, "a");
    publish(&mut e, "a", true);
    subscribe(&mut e, "a", "a/#", 5, Some(1));
    let f = next(&mut e, "a");
    assert!(matches!(f.packet, Packet::Publish { retain: true, .. }));
    assert_eq!(identifiers(&f), vec![1]);
}
#[test]
fn pending_qos2_keeps_publisher_identity_after_restart() {
    let mut e = SessionEngine::new(config()).unwrap();
    connect(&mut e, "a");
    connect(&mut e, "b");
    subscribe(&mut e, "a", "a/#", 6, Some(1));
    subscribe(&mut e, "b", "a/#", 2, Some(2));
    e.handle_frame(
        "a",
        Frame::v5(Packet::Publish {
            topic: "a/x".into(),
            payload: b"q2".to_vec(),
            qos: 2,
            packet_id: Some(8),
            dup: false,
            retain: false,
        }),
    )
    .unwrap();
    let mut e = SessionEngine::restore(config(), &e.snapshot().unwrap()).unwrap();
    connect(&mut e, "a");
    connect(&mut e, "b");
    e.handle_frame("a", Frame::v5(Packet::PubRel { packet_id: 8 }))
        .unwrap();
    assert!(e.poll_frame_with_changes("a").unwrap().frame.is_none());
    let f = next(&mut e, "b");
    assert_eq!(identifiers(&f), vec![2]);
    assert!(matches!(f.packet, Packet::Publish { qos: 2, .. }));
}
#[test]
fn will_origin_obeys_no_local_and_retained_as_published() {
    let mut e = SessionEngine::new(config()).unwrap();
    connect(&mut e, "a");
    connect(&mut e, "b");
    subscribe(&mut e, "a", "a/#", 5, Some(1));
    subscribe(&mut e, "b", "a/#", 9, Some(2));
    e.connect_with_options(
        "a",
        Some(mqtt_protocol::Will {
            topic: "a/x".into(),
            payload: b"will".to_vec(),
            qos: 1,
            retain: true,
        }),
        Principal::anonymous(),
        ConnectionOptions {
            version: ProtocolVersion::V5,
            clean_start: false,
            session_expiry_interval: u32::MAX,
            receive_maximum: 32,
            maximum_packet_size: u32::MAX,
            will_properties: vec![],
        },
    )
    .unwrap();
    e.disconnect_with_reason("a", false).unwrap();
    assert!(e.sessions["a"].queued.is_empty());
    let f = next(&mut e, "b");
    assert_eq!(identifiers(&f), vec![2]);
    assert!(matches!(f.packet, Packet::Publish { retain: true, .. }));
    assert!(e.retained_properties["a/x"].is_empty());
}
#[test]
fn snapshot_delivery_choice_survives_option_replacement_and_v311_bridge() {
    let mut e = SessionEngine::new(config()).unwrap();
    connect(&mut e, "a");
    connect(&mut e, "b");
    subscribe(&mut e, "b", "a/#", 9, Some(9));
    publish(&mut e, "a", true);
    subscribe(&mut e, "b", "a/x", 1, Some(10));
    subscribe(&mut e, "b", "a/#", 0x21, None);
    let mut e = SessionEngine::restore(config(), &e.snapshot().unwrap()).unwrap();
    e.connect("b", false).unwrap();
    let live = e.poll("b").unwrap().unwrap();
    let replay = e.poll("b").unwrap().unwrap();
    assert!(matches!(live, Packet::Publish { retain: false, .. }));
    assert!(matches!(replay, Packet::Publish { retain: true, .. }));
    let mut e = SessionEngine::restore(config(), &e.snapshot().unwrap()).unwrap();
    connect(&mut e, "b");
    let f = next(&mut e, "b");
    assert_eq!(identifiers(&f), vec![9]);
    assert!(matches!(
        f.packet,
        Packet::Publish {
            retain: true,
            dup: true,
            ..
        }
    ));
    let f = next(&mut e, "b");
    assert_eq!(identifiers(&f), vec![10]);
    assert!(matches!(
        f.packet,
        Packet::Publish {
            retain: true,
            dup: true,
            ..
        }
    ));
}
#[test]
fn strict_schema6_options_markers_and_old5_metadata() {
    let mut e = SessionEngine::new(config()).unwrap();
    connect(&mut e, "a");
    connect(&mut e, "b");
    subscribe(&mut e, "b", "a/#", 1, None);
    publish(&mut e, "a", false);
    let original: serde_json::Value = serde_json::from_slice(&e.snapshot().unwrap()).unwrap();
    assert_eq!(original["schema_version"], 7);
    for key in [
        "no_local",
        "retain_as_published",
        "retain_handling",
        "identifier",
    ] {
        let mut bad = original.clone();
        bad["sessions"][1]["subscriptions"][0]
            .as_object_mut()
            .unwrap()
            .remove(key);
        assert!(
            SessionEngine::restore(config(), &serde_json::to_vec(&bad).unwrap()).is_err(),
            "{key}"
        );
    }
    for (key, value) in [
        ("retain_handling", serde_json::json!(3)),
        ("identifier", serde_json::json!(0)),
        ("identifier", serde_json::json!(268435456)),
    ] {
        let mut bad = original.clone();
        bad["sessions"][1]["subscriptions"][0][key] = value;
        assert!(SessionEngine::restore(config(), &serde_json::to_vec(&bad).unwrap()).is_err());
    }
    let mut bad = original.clone();
    bad["sessions"][1]["queued"][0]
        .as_object_mut()
        .unwrap()
        .remove("retained_replay");
    assert!(SessionEngine::restore(config(), &serde_json::to_vec(&bad).unwrap()).is_err());
    let old = schema5(original);
    let restored = SessionEngine::restore(config(), &serde_json::to_vec(&old).unwrap()).unwrap();
    assert!(!restored.sessions["b"].subscriptions["a/#"].no_local);
    let mut smuggled = old.clone();
    smuggled["sessions"][1]["queued"][0]["properties"] =
        serde_json::json!([{"SubscriptionIdentifier":7}]);
    assert!(SessionEngine::restore(config(), &serde_json::to_vec(&smuggled).unwrap()).is_err());
    let mut bad = old;
    bad["sessions"][1]["queued"][0]
        .as_object_mut()
        .unwrap()
        .remove("expires_at");
    assert!(SessionEngine::restore(config(), &serde_json::to_vec(&bad).unwrap()).is_err());
}
#[test]
fn peer_size_counts_ids_without_queue_poison() {
    let mut cfg = config();
    cfg.max_queued = 1;
    let mut e = SessionEngine::new(cfg).unwrap();
    connect(&mut e, "a");
    connect(&mut e, "b");
    subscribe(&mut e, "b", "a/#", 1, Some(268435455));
    subscribe(&mut e, "b", "+/x", 1, Some(268435455));
    e.sessions.get_mut("b").unwrap().maximum_packet_size = 20;
    publish(&mut e, "a", false);
    assert!(e.sessions["b"].queued.is_empty());
    e.handle_frame(
        "a",
        Frame::v5(Packet::Publish {
            topic: "a/x".into(),
            payload: vec![],
            qos: 0,
            packet_id: None,
            dup: false,
            retain: false,
        }),
    )
    .unwrap();
    assert_eq!(e.sessions["b"].queued.len(), 1);
    let f = next(&mut e, "b");
    assert_eq!(identifiers(&f), vec![268435455, 268435455]);
    assert!(encode_frame(&f).unwrap().len() <= 20);
}
