use super::*;
#[derive(Debug)]
struct FixedClock;
impl Clock for FixedClock {
    fn now_millis(&self) -> u64 {
        0
    }
}
fn engine(c: SessionConfig) -> SessionEngine {
    SessionEngine::new_with_clock(c, Arc::new(AllowAnonymous), Arc::new(FixedClock)).unwrap()
}
fn config() -> SessionConfig {
    SessionConfig {
        max_sessions: 16,
        max_retained: 16,
        max_subscriptions: 16,
        max_queued: 16,
        max_inflight: 16,
        max_packet_size: 8192,
        max_state_bytes: 524288,
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
fn subscribe(e: &mut SessionEngine, id: &str, filter: &str, qos: u8, sid: u32) {
    let mut f = Frame::v5(Packet::Subscribe {
        packet_id: 1,
        filters: vec![(filter.into(), qos)],
    });
    f.properties.push(Property::SubscriptionIdentifier(sid));
    let r = e.handle_frame(id, f).unwrap();
    assert!(matches!(&r[0].packet,Packet::SubAck{codes,..} if codes==&vec![qos]));
}
fn publish(e: &mut SessionEngine, qos: u8, retain: bool) {
    let f = Frame::v5(Packet::Publish {
        topic: "work/a".into(),
        payload: b"data".to_vec(),
        qos,
        packet_id: (qos != 0).then_some(9),
        dup: false,
        retain,
    });
    e.handle_frame("source", f).unwrap();
    if qos == 2 {
        e.handle_frame("source", Frame::v5(Packet::PubRel { packet_id: 9 }))
            .unwrap();
    }
}
fn drain(e: &mut SessionEngine, id: &str) -> Vec<Frame> {
    let mut frames = vec![];
    while let Some(f) = e.poll_frame_with_changes(id).unwrap().frame {
        if let Packet::Publish {
            qos,
            packet_id: Some(n),
            ..
        } = f.packet
        {
            if qos == 1 {
                e.handle_frame(id, Frame::v5(Packet::PubAck { packet_id: n }))
                    .unwrap();
            } else {
                e.handle_frame(id, Frame::v5(Packet::PubRec { packet_id: n }))
                    .unwrap();
                e.handle_frame(id, Frame::v5(Packet::PubComp { packet_id: n }))
                    .unwrap();
            }
        }
        frames.push(f);
    }
    frames
}
#[test]
fn shared_round_robin_one_copy_and_no_retained_replay() {
    let mut e = engine(config());
    for id in ["source", "a", "b"] {
        connect(&mut e, id);
    }
    publish(&mut e, 1, true);
    for id in ["a", "b"] {
        subscribe(&mut e, id, "$share/g/work/#", 1, 10);
    }
    assert!(drain(&mut e, "a").is_empty());
    assert!(drain(&mut e, "b").is_empty());
    for _ in 0..4 {
        publish(&mut e, 1, false);
    }
    assert_eq!(drain(&mut e, "a").len(), 2);
    assert_eq!(drain(&mut e, "b").len(), 2);
}
#[test]
fn shared_group_and_ordinary_copies_are_independent() {
    let mut e = engine(config());
    connect(&mut e, "source");
    connect(&mut e, "a");
    for (f, id) in [
        ("$share/g/work/#", 10),
        ("$share/h/work/#", 11),
        ("work/#", 12),
    ] {
        subscribe(&mut e, "a", f, 1, id);
    }
    publish(&mut e, 1, false);
    let out = drain(&mut e, "a");
    assert_eq!(out.len(), 3);
    let mut ids: Vec<_> = out.iter().map(|f| f.properties.clone()).collect();
    ids.sort_by_key(|p| format!("{p:?}"));
    assert_eq!(
        ids,
        vec![
            vec![Property::SubscriptionIdentifier(10)],
            vec![Property::SubscriptionIdentifier(11)],
            vec![Property::SubscriptionIdentifier(12)]
        ]
    );
}
#[test]
fn last_member_unsubscribe_prunes_unstarted_group_delivery() {
    let mut e = engine(config());
    connect(&mut e, "source");
    connect(&mut e, "a");
    subscribe(&mut e, "a", "$share/g/work/#", 1, 10);
    publish(&mut e, 1, false);
    e.handle_frame(
        "a",
        Frame::v5(Packet::Unsubscribe {
            packet_id: 2,
            filters: vec!["$share/g/work/#".into()],
        }),
    )
    .unwrap();
    assert!(drain(&mut e, "a").is_empty());
}
fn unsubscribe(e: &mut SessionEngine, id: &str, filter: &str) {
    e.handle_frame(
        id,
        Frame::v5(Packet::Unsubscribe {
            packet_id: 2,
            filters: vec![filter.into()],
        }),
    )
    .unwrap();
}
#[test]
fn qos0_polled_lease_does_not_survive_group_recreation() {
    let mut e = engine(config());
    for id in ["source", "a"] {
        connect(&mut e, id);
    }
    subscribe(&mut e, "a", "$share/g/work/#", 0, 10);
    publish(&mut e, 0, false);
    let old = e.poll_delivery_with_changes("a").unwrap().delivery.unwrap();
    let epoch = old.shared_subscription.clone().unwrap().1;
    unsubscribe(&mut e, "a", "$share/g/work/#");
    assert!(e.shared_epochs.is_empty());
    subscribe(&mut e, "a", "$share/g/work/#", 0, 10);
    assert_ne!(e.shared_epochs["$share/g/work/#"], epoch);
    assert!(e.begin_delivery("a", old).unwrap().frame.is_none());
    publish(&mut e, 0, false);
    assert_eq!(drain(&mut e, "a").len(), 1);
}
#[test]
fn shared_full_member_is_skipped_and_all_full_is_atomic() {
    let mut c = config();
    c.max_queued = 1;
    let mut e = engine(c);
    for id in ["source", "a", "b"] {
        connect(&mut e, id);
    }
    for id in ["a", "b"] {
        subscribe(&mut e, id, "$share/g/work/#", 1, 10);
    }
    publish(&mut e, 1, false);
    // Pin cursor before a so the full member is examined first again.
    e.shared_cursors.clear();
    publish(&mut e, 1, false);
    assert_eq!(e.sessions["a"].queued.len(), 1);
    assert_eq!(e.sessions["b"].queued.len(), 1);
    let snapshot = e.snapshot().unwrap();
    let cursor = e.shared_cursors.clone();
    assert!(matches!(
        e.route(
            Message {
                topic: "work/a".into(),
                payload: vec![],
                qos: 1,
                retain: false,
                retained_replay: false,
                shared_subscription: None,
                expires_at: None,
                properties: vec![]
            },
            Some("source")
        ),
        Err(EngineError::Capacity)
    ));
    assert_eq!(e.snapshot().unwrap(), snapshot);
    assert_eq!(e.shared_cursors, cursor);
}
#[test]
fn ordinary_and_two_groups_count_against_same_queue() {
    let mut c = config();
    c.max_queued = 2;
    let mut e = engine(c);
    for id in ["source", "a"] {
        connect(&mut e, id);
    }
    for f in ["work/#", "$share/g/work/#", "$share/h/work/#"] {
        subscribe(&mut e, "a", f, 1, 10);
    }
    let before = e.snapshot().unwrap();
    assert!(matches!(
        e.handle_frame(
            "source",
            Frame::v5(Packet::Publish {
                topic: "work/a".into(),
                payload: vec![],
                qos: 1,
                packet_id: Some(9),
                dup: false,
                retain: false
            })
        ),
        Err(EngineError::Capacity)
    ));
    assert_eq!(e.snapshot().unwrap(), before);
    assert!(e.shared_cursors.is_empty());
}
#[test]
fn online_preferred_then_offline_qos_and_peer_size_skip() {
    let mut e = engine(config());
    for id in ["source", "a", "b"] {
        connect(&mut e, id);
    }
    for id in ["a", "b"] {
        subscribe(&mut e, id, "$share/g/work/#", 1, 10);
    }
    e.disconnect("a").unwrap();
    publish(&mut e, 1, false);
    assert_eq!(drain(&mut e, "b").len(), 1);
    assert!(e.sessions["a"].queued.is_empty());
    e.sessions.get_mut("b").unwrap().maximum_packet_size = 2;
    publish(&mut e, 1, false);
    assert_eq!(e.sessions["a"].queued.len(), 1);
    publish(&mut e, 0, false);
    assert_eq!(e.sessions["a"].queued.len(), 1);
}
#[test]
fn shared_qos2_selected_owner_resumes_and_last_member_keeps_started_exchange() {
    let mut e = engine(config());
    for id in ["source", "a", "b"] {
        connect(&mut e, id);
    }
    for id in ["a", "b"] {
        subscribe(&mut e, id, "$share/g/work/#", 2, 10);
    }
    publish(&mut e, 2, false);
    let first = e.poll_frame_with_changes("a").unwrap().frame.unwrap();
    let Packet::Publish {
        packet_id: Some(id),
        qos: 2,
        ..
    } = first.packet
    else {
        panic!()
    };
    e.handle_frame("a", Frame::v5(Packet::PubRec { packet_id: id }))
        .unwrap();
    unsubscribe(&mut e, "a", "$share/g/work/#");
    unsubscribe(&mut e, "b", "$share/g/work/#");
    let mut e = SessionEngine::restore(config(), &e.snapshot().unwrap()).unwrap();
    connect(&mut e, "a");
    connect(&mut e, "b");
    assert!(drain(&mut e, "b").is_empty());
    let replay = e.poll_frame_with_changes("a").unwrap().frame.unwrap();
    assert!(matches!(replay.packet,Packet::PubRel{packet_id} if packet_id==id));
    e.handle_frame("a", Frame::v5(Packet::PubComp { packet_id: id }))
        .unwrap();
    assert!(e.sessions["a"].outgoing_shared.is_empty());
}
#[test]
fn shared_schema7_strict_markers_and_schema6_migration() {
    let mut e = engine(config());
    for id in ["source", "a"] {
        connect(&mut e, id);
    }
    subscribe(&mut e, "a", "$share/g/work/#", 1, 10);
    publish(&mut e, 1, false);
    let good: serde_json::Value = serde_json::from_slice(&e.snapshot().unwrap()).unwrap();
    assert_eq!(good["schema_version"], 7);
    for marker in [
        serde_json::json!("bad"),
        serde_json::json!("$share//work/#"),
    ] {
        let mut bad = good.clone();
        bad["sessions"][0]["queued"][0]["shared_subscription"] = marker;
        assert!(SessionEngine::restore(config(), &serde_json::to_vec(&bad).unwrap()).is_err());
    }
    let mut old = good.clone();
    old["schema_version"] = 6.into();
    old["sessions"][0]["queued"][0]
        .as_object_mut()
        .unwrap()
        .remove("shared_subscription");
    assert!(SessionEngine::restore(config(), &serde_json::to_vec(&old).unwrap()).is_err()); // old schema cannot contain shared membership
    old["sessions"][0]["subscriptions"][0]["filter"] = "work/#".into();
    let restored = SessionEngine::restore(config(), &serde_json::to_vec(&old).unwrap()).unwrap();
    assert!(
        restored.sessions["a"].queued[0]
            .shared_subscription
            .is_none()
    );
    let mut missing = good;
    missing["sessions"][0]["queued"][0]
        .as_object_mut()
        .unwrap()
        .remove("shared_subscription");
    assert!(SessionEngine::restore(config(), &serde_json::to_vec(&missing).unwrap()).is_err());
}
#[test]
fn shared_311_creation_rejected_but_existing_membership_resumes() {
    let mut e = engine(config());
    for id in ["source", "a"] {
        connect(&mut e, id);
    }
    subscribe(&mut e, "a", "$share/g/work/#", 1, 10);
    e.disconnect("a").unwrap();
    e.connect("a", false).unwrap();
    let out = e
        .handle(
            "a",
            Packet::Subscribe {
                packet_id: 1,
                filters: vec![("$share/new/work/#".into(), 1)],
            },
        )
        .unwrap();
    assert!(matches!(&out[0],Packet::SubAck{codes,..} if codes==&vec![0x80]));
    publish(&mut e, 1, false);
    let packet = e.poll("a").unwrap().unwrap();
    let Packet::Publish {
        packet_id: Some(n), ..
    } = packet
    else {
        panic!()
    };
    e.handle("a", Packet::PubAck { packet_id: n }).unwrap();
    assert!(e.poll("a").unwrap().is_none());
    e.handle(
        "a",
        Packet::Unsubscribe {
            packet_id: 2,
            filters: vec!["$share/g/work/#".into()],
        },
    )
    .unwrap();
    assert!(e.shared_epochs.is_empty());
}
#[test]
fn clean_start_last_member_ends_old_group_incarnation() {
    let mut e = engine(config());
    for id in ["source", "a"] {
        connect(&mut e, id);
    }
    subscribe(&mut e, "a", "$share/g/work/#", 0, 10);
    publish(&mut e, 0, false);
    let old = e.poll_delivery_with_changes("a").unwrap().delivery.unwrap();
    e.connect_with_options(
        "a",
        None,
        Principal::anonymous(),
        ConnectionOptions {
            version: ProtocolVersion::V5,
            clean_start: true,
            session_expiry_interval: u32::MAX,
            receive_maximum: 32,
            maximum_packet_size: u32::MAX,
            will_properties: vec![],
        },
    )
    .unwrap();
    assert!(e.shared_epochs.is_empty());
    subscribe(&mut e, "a", "$share/g/work/#", 0, 10);
    assert!(e.begin_delivery("a", old).unwrap().frame.is_none());
}
#[test]
fn shared_qos1_restart_keeps_recipient_and_negative_ack_never_reassigns() {
    let mut e = engine(config());
    for id in ["source", "a", "b"] {
        connect(&mut e, id);
    }
    for id in ["a", "b"] {
        subscribe(&mut e, id, "$share/g/work/#", 1, 10);
    }
    publish(&mut e, 1, false);
    let frame = e.poll_frame_with_changes("a").unwrap().frame.unwrap();
    let Packet::Publish {
        packet_id: Some(id),
        ..
    } = frame.packet
    else {
        panic!()
    };
    let mut e = SessionEngine::restore(config(), &e.snapshot().unwrap()).unwrap();
    connect(&mut e, "a");
    connect(&mut e, "b");
    let retry = e.poll_frame_with_changes("a").unwrap().frame.unwrap();
    assert!(matches!(retry.packet,Packet::Publish{packet_id:Some(n),dup:true,..} if n==id));
    assert!(drain(&mut e, "b").is_empty());
    let mut ack = Frame::v5(Packet::PubAck { packet_id: id });
    ack.reason_code = ReasonCode::try_from(0x80).unwrap();
    e.handle_frame("a", ack).unwrap();
    assert!(e.sessions["a"].outgoing_shared.is_empty());
    assert!(drain(&mut e, "b").is_empty());
}
#[test]
fn expired_last_member_prunes_group_and_unstarted_backlog() {
    let mut e = engine(config());
    for id in ["source", "a"] {
        connect(&mut e, id);
    }
    subscribe(&mut e, "a", "$share/g/work/#", 1, 10);
    publish(&mut e, 1, false);
    e.disconnect("a").unwrap();
    e.sessions.get_mut("a").unwrap().expiry = 1;
    e.sessions.get_mut("a").unwrap().expires_at = Some(0);
    e.expire_sessions().unwrap();
    assert!(!e.sessions.contains_key("a"));
    assert!(e.shared_epochs.is_empty());
    assert!(e.shared_cursors.is_empty());
}
#[test]
fn shared_literal_dollar_topic_is_not_recursively_unwrapped_or_matched_by_hash() {
    let mut e = engine(config());
    for id in ["source", "a", "b"] {
        connect(&mut e, id);
    }
    subscribe(&mut e, "a", "$share/g/$share/inner/#", 1, 10);
    subscribe(&mut e, "b", "$share/g/#", 1, 11);
    e.handle_frame(
        "source",
        Frame::v5(Packet::Publish {
            topic: "$share/inner/x".into(),
            payload: vec![],
            qos: 1,
            packet_id: Some(9),
            dup: false,
            retain: false,
        }),
    )
    .unwrap();
    assert_eq!(drain(&mut e, "a").len(), 1);
    assert!(drain(&mut e, "b").is_empty());
}
