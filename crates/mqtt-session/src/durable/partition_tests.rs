use super::*;
use mqtt_protocol::Will;

fn config(max_queued: usize) -> SessionConfig {
    SessionConfig {
        max_sessions: 16,
        max_retained: 16,
        max_subscriptions: 8,
        max_queued,
        max_inflight: 8,
        max_packet_size: 4096,
        max_state_bytes: 128 * 1024,
    }
}
fn partition(max_queued: usize) -> SessionEngine {
    SessionEngine::new_partition(
        config(max_queued),
        Arc::new(AllowAnonymous),
        Arc::new(SystemClock),
    )
    .unwrap()
}
fn publish(topic: &str, payload: &[u8], qos: u8, retain: bool, id: u16) -> Packet {
    Packet::Publish {
        topic: topic.into(),
        payload: payload.to_vec(),
        qos,
        retain,
        dup: false,
        packet_id: (qos != 0).then_some(id),
    }
}
fn subscribe(engine: &mut SessionEngine, id: &str, filter: &str, qos: u8) {
    engine
        .handle(
            id,
            Packet::Subscribe {
                packet_id: 1,
                filters: vec![(filter.into(), qos)],
            },
        )
        .unwrap();
}
fn connect_v5(engine: &mut SessionEngine, id: &str) {
    engine
        .connect_with_options(
            id,
            None,
            Principal::anonymous(),
            ConnectionOptions {
                version: ProtocolVersion::V5,
                clean_start: true,
                session_expiry_interval: 0,
                receive_maximum: 32,
                maximum_packet_size: u32::MAX,
                will_properties: vec![],
            },
        )
        .unwrap();
}
fn subscribe_v5(engine: &mut SessionEngine, id: &str, filter: &str, options: u8) {
    let mut frame = Frame::v5(Packet::Subscribe {
        packet_id: 1,
        filters: vec![(filter.into(), options & 3)],
    });
    frame.subscription_options = vec![options];
    let replies = engine.handle_frame(id, frame).unwrap();
    assert!(
        matches!(&replies[0].packet, Packet::SubAck { codes, .. } if codes == &vec![options & 3])
    );
}
fn drain(engine: &mut SessionEngine, id: &str) -> Vec<Packet> {
    let mut packets = Vec::new();
    while let Some(packet) = engine.poll(id).unwrap() {
        if let Packet::Publish {
            qos: 1,
            packet_id: Some(mid),
            ..
        } = packet
        {
            engine
                .handle(id, Packet::PubAck { packet_id: mid })
                .unwrap();
        }
        packets.push(packet);
    }
    packets
}

#[test]
fn partition_publish_emits_publications_without_local_routing() {
    let mut engine = partition(4);
    engine.connect("pub", true).unwrap();
    subscribe(&mut engine, "pub", "a/#", 1);
    engine
        .handle("pub", publish("a/b", b"x", 1, true, 7))
        .unwrap();
    assert_eq!(engine.poll("pub").unwrap(), None);
    assert!(engine.retained_entries().is_empty());
    let publications = engine.take_publications();
    assert_eq!(publications.len(), 1);
    assert_eq!(publications[0].publisher.as_deref(), Some("pub"));
    assert_eq!(
        publications[0].retained_update().map(|entry| entry.payload),
        Some(b"x".to_vec())
    );
    assert!(engine.take_publications().is_empty());
}

#[test]
fn deliver_admits_per_recipient_and_full_queue_rejects_only_that_copy() {
    let mut source = partition(4);
    source.connect("pub", true).unwrap();
    source
        .handle("pub", publish("a/b", b"x", 1, false, 1))
        .unwrap();
    let publication = source.take_publications().remove(0);
    let mut open = partition(4);
    open.connect("open", true).unwrap();
    subscribe(&mut open, "open", "a/#", 1);
    let mut full = partition(1);
    full.connect("full", false).unwrap();
    subscribe(&mut full, "full", "a/#", 1);
    full.disconnect("full").unwrap();
    assert!(
        full.deliver(&publication, &DeliveryScope::Ordinary)
            .unwrap()
    );
    let before = full.snapshot().unwrap();
    let mut proposal = full.clone();
    assert!(matches!(
        proposal.deliver(&publication, &DeliveryScope::Ordinary),
        Err(EngineError::Capacity)
    ));
    assert_eq!(full.snapshot().unwrap(), before);
    assert!(
        open.deliver(&publication, &DeliveryScope::Ordinary)
            .unwrap()
    );
    assert_eq!(drain(&mut open, "open").len(), 1);
}

#[test]
fn deliver_respects_no_local_and_unmatched_filters() {
    let mut engine = partition(4);
    connect_v5(&mut engine, "self");
    subscribe_v5(&mut engine, "self", "a/#", 1 | 4);
    let local = Publication {
        topic: "a/b".into(),
        payload: b"x".to_vec(),
        qos: 1,
        retain: false,
        properties: Vec::new(),
        expires_at: None,
        publisher: Some("self".into()),
        expired: false,
    };
    assert!(!engine.deliver(&local, &DeliveryScope::Ordinary).unwrap());
    let other = Publication {
        publisher: Some("other".into()),
        ..local.clone()
    };
    assert!(engine.deliver(&other, &DeliveryScope::Ordinary).unwrap());
    let unmatched = Publication {
        topic: "b/c".into(),
        ..other
    };
    assert!(
        !engine
            .deliver(&unmatched, &DeliveryScope::Ordinary)
            .unwrap()
    );
    let expired = Publication {
        expired: true,
        ..local
    };
    assert!(!engine.deliver(&expired, &DeliveryScope::Ordinary).unwrap());
}

#[test]
fn delivery_scope_separates_ordinary_and_shared_copies() {
    let mut engine = partition(8);
    connect_v5(&mut engine, "m");
    subscribe_v5(&mut engine, "m", "a/#", 1);
    subscribe_v5(&mut engine, "m", "$share/g/a/#", 1);
    subscribe_v5(&mut engine, "m", "$share/h/a/#", 1);
    let publication = Publication {
        topic: "a/b".into(),
        payload: b"x".to_vec(),
        qos: 1,
        retain: false,
        properties: Vec::new(),
        expires_at: None,
        publisher: None,
        expired: false,
    };
    assert!(
        engine
            .deliver(&publication, &DeliveryScope::Ordinary)
            .unwrap()
    );
    assert_eq!(drain(&mut engine, "m").len(), 1);
    assert!(
        engine
            .deliver(&publication, &DeliveryScope::Shared("$share/g/a/#".into()))
            .unwrap()
    );
    assert_eq!(drain(&mut engine, "m").len(), 1);
    assert!(
        !engine
            .deliver(&publication, &DeliveryScope::Shared("$share/x/a/#".into()))
            .unwrap()
    );
    let filters = engine.subscription_filters();
    assert_eq!(filters["m"].len(), 3);
}

#[test]
fn partition_routing_matches_the_legacy_engine_for_one_subscriber() {
    let packets = [
        publish("a/b", b"q0", 0, false, 0),
        publish("a/b", b"q1", 1, false, 1),
        publish("a/c", b"q2", 2, false, 2),
        publish("a/b", b"retained", 1, true, 3),
        publish("x/y", b"miss", 1, false, 4),
    ];
    let mut legacy = SessionEngine::new(config(16)).unwrap();
    legacy.connect("pub", true).unwrap();
    legacy.connect("sub", true).unwrap();
    subscribe(&mut legacy, "sub", "a/+", 1);
    let mut source = partition(16);
    source.connect("pub", true).unwrap();
    let mut sink = partition(16);
    sink.connect("sub", true).unwrap();
    subscribe(&mut sink, "sub", "a/+", 1);
    for packet in packets {
        legacy.handle("pub", packet.clone()).unwrap();
        if let Packet::Publish {
            qos: 2,
            packet_id: Some(id),
            ..
        } = packet
        {
            legacy
                .handle("pub", Packet::PubRel { packet_id: id })
                .unwrap();
        }
        source.handle("pub", packet.clone()).unwrap();
        if let Packet::Publish {
            qos: 2,
            packet_id: Some(id),
            ..
        } = packet
        {
            source
                .handle("pub", Packet::PubRel { packet_id: id })
                .unwrap();
        }
        for publication in source.take_publications() {
            sink.deliver(&publication, &DeliveryScope::Ordinary)
                .unwrap();
        }
    }
    assert_eq!(drain(&mut sink, "sub"), drain(&mut legacy, "sub"));
}

#[test]
fn staged_retained_replays_on_subscribe_and_clears_before_snapshot() {
    let mut engine = partition(8);
    engine.connect("s", false).unwrap();
    let before = engine.snapshot().unwrap();
    engine
        .stage_retained(vec![RetainedEntry {
            topic: "a/b".into(),
            payload: b"r".to_vec(),
            qos: 1,
            properties: Vec::new(),
            expires_at: None,
        }])
        .unwrap();
    subscribe(&mut engine, "s", "a/#", 1);
    engine.clear_staged_retained().unwrap();
    assert!(engine.retained_entries().is_empty());
    let replay = drain(&mut engine, "s");
    assert!(
        matches!(&replay[..], [Packet::Publish { retain: true, payload, .. }] if payload == b"r")
    );
    assert_ne!(engine.snapshot().unwrap(), before);
    let restored = SessionEngine::restore_partition(
        config(8),
        &engine.snapshot().unwrap(),
        Arc::new(AllowAnonymous),
        Arc::new(SystemClock),
    )
    .unwrap();
    assert!(restored.is_partition());
    assert!(restored.retained_entries().is_empty());
}

#[test]
fn legacy_engine_splits_into_restorable_partitions() {
    let mut legacy = SessionEngine::new(config(8)).unwrap();
    legacy.connect("a", false).unwrap();
    subscribe(&mut legacy, "a", "t/#", 1);
    legacy
        .connect_with_will(
            "b",
            false,
            Some(Will {
                topic: "will/b".into(),
                payload: b"w".to_vec(),
                qos: 1,
                retain: false,
            }),
        )
        .unwrap();
    legacy.connect("pub", true).unwrap();
    legacy
        .handle("pub", publish("t/x", b"keep", 1, true, 1))
        .unwrap();
    legacy.disconnect_with_reason("a", false).unwrap();
    let floor = legacy.clock_floor();
    let split = legacy.into_partitions().unwrap();
    assert_eq!(split.clock_floor, floor);
    assert_eq!(split.retained.len(), 1);
    assert_eq!(split.retained[0].payload, b"keep");
    let ids: Vec<_> = split.clients.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, ["a", "b", "pub"]);
    for (id, engine) in split.clients {
        assert!(engine.is_partition());
        assert!(engine.retained_entries().is_empty());
        assert!(engine.clock_floor() >= floor);
        if engine.has_persistent_state() {
            let row = engine.snapshot().unwrap();
            let mut restored = SessionEngine::restore_partition(
                config(8),
                &row,
                Arc::new(AllowAnonymous),
                Arc::new(SystemClock),
            )
            .unwrap();
            if id == "a" {
                restored.connect("a", false).unwrap();
                assert_eq!(drain(&mut restored, "a").len(), 1);
            }
        } else {
            assert_eq!(id, "pub");
        }
    }
}

#[test]
fn restore_partition_rejects_multi_client_and_retained_rows() {
    let mut legacy = SessionEngine::new(config(8)).unwrap();
    legacy.connect("a", false).unwrap();
    legacy.connect("b", false).unwrap();
    let access: Arc<dyn AccessControl> = Arc::new(AllowAnonymous);
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    assert!(matches!(
        SessionEngine::restore_partition(
            config(8),
            &legacy.snapshot().unwrap(),
            access.clone(),
            clock.clone()
        ),
        Err(EngineError::InvalidRecord(_))
    ));
    let row = encode_retained_row(
        config(8),
        RetainedEntry {
            topic: "r".into(),
            payload: b"p".to_vec(),
            qos: 0,
            properties: Vec::new(),
            expires_at: None,
        },
    )
    .unwrap();
    assert!(matches!(
        SessionEngine::restore_partition(config(8), &row, access, clock),
        Err(EngineError::InvalidRecord(_))
    ));
}

#[test]
fn retained_rows_round_trip_one_record() {
    let entry = RetainedEntry {
        topic: "a/b".into(),
        payload: b"value".to_vec(),
        qos: 1,
        properties: vec![Property::ContentType("text/plain".into())],
        expires_at: None,
    };
    let row = encode_retained_row(config(8), entry.clone()).unwrap();
    assert_eq!(decode_retained_row(config(8), &row).unwrap(), entry);
    let mut session = SessionEngine::new(config(8)).unwrap();
    session.connect("s", false).unwrap();
    assert!(decode_retained_row(config(8), &session.snapshot().unwrap()).is_err());
}

#[test]
fn live_state_bytes_track_the_last_bounds_check() {
    let mut engine = partition(8);
    let empty = engine.live_state_bytes();
    assert!(empty > 0);
    engine.connect("s", false).unwrap();
    subscribe(&mut engine, "s", "a/#", 1);
    assert!(engine.live_state_bytes() > empty);
    assert!(engine.has_persistent_state());
    engine.disconnect("s").unwrap();
    assert!(!engine.is_vacant());
}
