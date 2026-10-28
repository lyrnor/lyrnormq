use super::*;

fn config() -> SessionConfig {
    SessionConfig {
        max_sessions: 8,
        max_retained: 8,
        max_subscriptions: 8,
        max_queued: 16,
        max_inflight: 8,
        max_packet_size: 1024,
        max_state_bytes: 65536,
    }
}
fn connect(e: &mut SessionEngine, id: &str, clean: bool, cap: u32) {
    e.connect_with_options(
        id,
        None,
        Principal::anonymous(),
        ConnectionOptions {
            version: ProtocolVersion::V5,
            clean_start: clean,
            session_expiry_interval: u32::MAX,
            receive_maximum: 1,
            maximum_packet_size: cap,
            will_properties: vec![],
        },
    )
    .unwrap();
}
fn subscribed(cap: u32) -> SessionEngine {
    let mut e = SessionEngine::new(config()).unwrap();
    connect(&mut e, "src", true, u32::MAX);
    connect(&mut e, "sub", true, cap);
    e.handle_frame(
        "sub",
        Frame::v5(Packet::Subscribe {
            packet_id: 1,
            filters: vec![("a".into(), 2)],
        }),
    )
    .unwrap();
    e
}
fn publish(e: &mut SessionEngine, bytes: usize, qos: u8, retained: bool) {
    publish_properties(e, bytes, qos, retained, Vec::new());
}
fn publish_properties(
    e: &mut SessionEngine,
    bytes: usize,
    qos: u8,
    retained: bool,
    properties: Properties,
) {
    let p = Packet::Publish {
        topic: "a".into(),
        payload: vec![7; bytes],
        qos,
        packet_id: (qos > 0).then_some(23),
        dup: false,
        retain: retained,
    };
    let mut frame = Frame::v5(p);
    frame.properties = properties;
    e.handle_frame("src", frame).unwrap();
    if qos == 2 {
        e.handle_frame("src", Frame::v5(Packet::PubRel { packet_id: 23 }))
            .unwrap();
    }
}
fn wire_size(bytes: usize, qos: u8) -> u32 {
    encode_frame(&Frame::v5(Packet::Publish {
        topic: "a".into(),
        payload: vec![7; bytes],
        qos,
        packet_id: (qos > 0).then_some(1),
        dup: false,
        retain: false,
    }))
    .unwrap()
    .len() as u32
}
#[test]
fn exact_limit_accepts_and_plus_one_discards_all_qos() {
    for qos in 0..=2 {
        let mut e = subscribed(wire_size(4, qos));
        publish(&mut e, 5, qos, false);
        publish(&mut e, 4, qos, false);
        let poll = e.poll_frame_with_changes("sub").unwrap();
        assert!(poll.changed);
        let f = poll.frame.unwrap();
        assert_eq!(encode_frame(&f).unwrap().len() as u32, wire_size(4, qos));
        assert!(matches!(f.packet,Packet::Publish {payload,..} if payload.len()==4));
    }
}
#[test]
fn smaller_cap_resume_discards_started_and_releases_credit() {
    for qos in 1..=2 {
        let mut e = subscribed(u32::MAX);
        publish(&mut e, 50, qos, false);
        let old = e.poll_frame_with_changes("sub").unwrap().frame.unwrap();
        let id = match old.packet {
            Packet::Publish {
                packet_id: Some(id),
                ..
            } => id,
            _ => panic!(),
        };
        e.disconnect("sub").unwrap();
        publish(&mut e, 1, 1, false);
        connect(&mut e, "sub", false, wire_size(1, 1));
        let f = e.poll_frame_with_changes("sub").unwrap().frame.unwrap();
        assert!(matches!(f.packet,Packet::Publish {payload,..} if payload.len()==1));
        let s = &e.sessions["sub"];
        assert!(!s.outgoing.pending.contains_key(&identifier(id).unwrap()));
        assert_eq!(s.sent.len(), 1);
    }
}
#[test]
fn peer_discard_preserves_retained_and_another_recipient() {
    let mut e = subscribed(20);
    connect(&mut e, "other", true, u32::MAX);
    e.handle_frame(
        "other",
        Frame::v5(Packet::Subscribe {
            packet_id: 1,
            filters: vec![("a".into(), 2)],
        }),
    )
    .unwrap();
    publish(&mut e, 50, 1, true);
    assert!(e.poll_frame_with_changes("sub").unwrap().frame.is_none());
    assert!(e.poll_frame_with_changes("other").unwrap().frame.is_some());
    assert_eq!(e.retained.len(), 1);
    e.handle_frame(
        "sub",
        Frame::v5(Packet::Subscribe {
            packet_id: 2,
            filters: vec![("a".into(), 2)],
        }),
    )
    .unwrap();
    assert!(e.poll_frame_with_changes("sub").unwrap().frame.is_none());
    assert_eq!(e.retained.len(), 1);
}

#[test]
fn restore_and_v311_reconnect_reset_connection_limit() {
    let mut e = subscribed(20);
    let snapshot = e.snapshot().unwrap();
    let text = String::from_utf8(snapshot.clone()).unwrap();
    assert!(!text.contains("maximum_packet_size"));
    let mut r = SessionEngine::restore(config(), &snapshot).unwrap();
    assert_eq!(r.sessions["sub"].maximum_packet_size, u32::MAX);
    r.connect("sub", false).unwrap();
    assert_eq!(r.sessions["sub"].maximum_packet_size, u32::MAX);
    e.disconnect("sub").unwrap();
    publish(&mut e, 50, 1, false);
    connect(&mut e, "sub", false, u32::MAX);
    assert!(e.poll_frame_with_changes("sub").unwrap().frame.is_some());
}

#[test]
fn begin_rechecks_reserved_data_and_retires_every_sidecar() {
    for qos in 0..=2 {
        let mut e = subscribed(u32::MAX);
        publish_properties(
            &mut e,
            50,
            qos,
            false,
            vec![Property::MessageExpiryInterval(100)],
        );
        let d = e
            .poll_delivery_with_changes("sub")
            .unwrap()
            .delivery
            .unwrap();
        if qos > 0 {
            assert_eq!(e.sessions["sub"].outgoing_expiry.len(), 1);
        }
        e.sessions.get_mut("sub").unwrap().maximum_packet_size = 20;
        assert!(e.begin_delivery("sub", d).unwrap().frame.is_none());
        let s = &e.sessions["sub"];
        assert!(
            s.outgoing.pending.is_empty()
                && s.outgoing.order.is_empty()
                && s.sent.is_empty()
                && s.outgoing_properties.is_empty()
                && s.outgoing_started.is_empty()
                && s.outgoing_expiry.is_empty()
                && s.delivery_tickets.is_empty()
        );
    }
}

#[test]
fn smaller_limit_retains_pubrel_only_recovery() {
    let mut e = subscribed(u32::MAX);
    publish(&mut e, 50, 2, false);
    let f = e.poll_frame_with_changes("sub").unwrap().frame.unwrap();
    let id = match f.packet {
        Packet::Publish {
            packet_id: Some(id),
            ..
        } => id,
        _ => panic!(),
    };
    e.handle_frame("sub", Frame::v5(Packet::PubRec { packet_id: id }))
        .unwrap();
    e.disconnect("sub").unwrap();
    connect(&mut e, "sub", false, 20);
    let f = e.poll_frame_with_changes("sub").unwrap().frame.unwrap();
    assert!(matches!(f.packet,Packet::PubRel {packet_id} if packet_id==id));
    e.handle_frame("sub", Frame::v5(Packet::PubComp { packet_id: id }))
        .unwrap();
    assert!(e.sessions["sub"].outgoing.pending.is_empty());
}

#[test]
fn packet_and_property_length_integer_boundaries_use_complete_size() {
    for bytes in [119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 250] {
        for metadata in [0, 120, 121, 122, 123, 124, 125, 126, 127, 128] {
            let mut f = Frame::v5(Packet::Publish {
                topic: "a".into(),
                payload: vec![7; bytes],
                qos: 0,
                packet_id: None,
                dup: false,
                retain: false,
            });
            f.properties = vec![Property::UserProperty("k".into(), "v".repeat(metadata))];
            let len = encode_frame(&f).unwrap().len() as u32;
            for (cap, expected) in [(len, true), (len - 1, false)] {
                let mut e = subscribed(cap);
                e.handle_frame("src", f.clone()).unwrap();
                assert_eq!(
                    e.poll_frame_with_changes("sub").unwrap().frame.is_some(),
                    expected,
                    "bytes={bytes} metadata={metadata} cap={cap}"
                );
            }
        }
    }
}

#[test]
fn allocator_collision_query_includes_will_without_session() {
    let mut e = SessionEngine::new(config()).unwrap();
    assert!(!e.client_id_in_use("x"));
    e.connect_with_options(
        "x",
        Some(mqtt_protocol::Will {
            topic: "a".into(),
            payload: vec![1],
            qos: 0,
            retain: false,
        }),
        Principal::anonymous(),
        ConnectionOptions {
            version: ProtocolVersion::V5,
            clean_start: true,
            session_expiry_interval: 0,
            receive_maximum: 1,
            maximum_packet_size: u32::MAX,
            will_properties: vec![],
        },
    )
    .unwrap();
    assert!(e.client_id_in_use("x"));
    e.sessions.remove("x");
    assert!(e.client_id_in_use("x"));
    e.wills.remove("x");
    assert!(!e.client_id_in_use("x"));
}

#[test]
fn generated_key_does_not_enlarge_synthetic_will_connect() {
    let mut limits = config();
    limits.max_packet_size = 64;
    let mut e = SessionEngine::new(limits).unwrap();
    let will = mqtt_protocol::Will {
        topic: "a".into(),
        payload: vec![1; 35],
        qos: 0,
        retain: false,
    };
    // Real empty-ID MQTT5 CONNECT fits64, and so does the eventual Will PUBLISH.
    let mut input = Frame::v5(Packet::Connect {
        client_id: String::new(),
        clean_session: true,
        keep_alive: 0,
        will: Some(will.clone()),
        username: None,
        password: None,
    });
    input.properties = vec![Property::MaximumPacketSize(100)];
    assert!(encode_frame(&input).unwrap().len() <= 64);
    e.connect_with_options(
        "r1234567890123456789012",
        Some(will),
        Principal::anonymous(),
        ConnectionOptions {
            version: ProtocolVersion::V5,
            clean_start: true,
            session_expiry_interval: 0,
            receive_maximum: 1,
            maximum_packet_size: 100,
            will_properties: vec![],
        },
    )
    .unwrap();
}

#[test]
fn maximum_zero_rejected_before_existing_state_changes() {
    let mut e = subscribed(u32::MAX);
    let before = e.snapshot().unwrap();
    let error = e
        .connect_with_options(
            "sub",
            None,
            Principal::anonymous(),
            ConnectionOptions {
                version: ProtocolVersion::V5,
                clean_start: true,
                session_expiry_interval: 0,
                receive_maximum: 1,
                maximum_packet_size: 0,
                will_properties: vec![],
            },
        )
        .unwrap_err();
    assert!(matches!(error,EngineError::Mqtt5(code) if code.value()==0x82));
    assert_eq!(before, e.snapshot().unwrap());
}
