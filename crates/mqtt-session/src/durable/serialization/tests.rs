//! Independent old owned-Envelope oracle against borrowed/counting production paths.
use super::super::*;
use super::{CountingWriter, check_live_bounds, encode as encode_projection, oracle};
use mqtt_protocol::Will;
use std::io::Write;
use std::sync::Arc;

#[derive(Debug)]
struct FixedClock;
#[derive(Debug)]
struct Permit;
impl AccessControl for Permit {
    fn authenticate(
        &self,
        _: mqtt_auth::ConnectInfo<'_>,
    ) -> Result<Principal, mqtt_auth::AuthError> {
        Ok(Principal::anonymous())
    }
    fn authorize(&self, _: &Principal, _: Action, _: &str) -> bool {
        true
    }
}
impl Clock for FixedClock {
    fn now_millis(&self) -> u64 {
        1000
    }
}
fn config() -> SessionConfig {
    SessionConfig {
        max_sessions: 16,
        max_retained: 16,
        max_subscriptions: 32,
        max_queued: 32,
        max_inflight: 16,
        max_packet_size: 65536,
        max_state_bytes: 2 * 1024 * 1024,
    }
}
fn options() -> ConnectionOptions {
    ConnectionOptions {
        version: ProtocolVersion::V5,
        clean_start: false,
        session_expiry_interval: 300,
        receive_maximum: 16,
        maximum_packet_size: u32::MAX,
        will_properties: Vec::new(),
    }
}
fn engine() -> SessionEngine {
    SessionEngine::new_with_clock(config(), Arc::new(Permit), Arc::new(FixedClock)).unwrap()
}
fn publish(e: &mut SessionEngine, topic: &str, qos: u8, id: u16, retain: bool) {
    e.handle(
        "pub",
        Packet::Publish {
            topic: topic.into(),
            payload: vec![0, 1, 8, 9, 10, 13, 31, 80, 128, 255],
            qos,
            packet_id: (qos != 0).then_some(id),
            dup: false,
            retain,
        },
    )
    .unwrap();
    if qos == 2 {
        e.handle("pub", Packet::PubRel { packet_id: id }).unwrap();
    }
}
fn rich() -> SessionEngine {
    let mut e = engine();
    for id in ["pub", "sub"] {
        e.connect_with_options(id, None, Principal::anonymous(), options())
            .unwrap();
    }
    e.connect("clean\"\\雪😀", true).unwrap();
    e.handle(
        "sub",
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![("s/#".into(), 2)],
        },
    )
    .unwrap();
    e.handle(
        "clean\"\\雪😀",
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![("live-only/#".into(), 0)],
        },
    )
    .unwrap();
    publish(&mut e, "s/ack", 1, 10, false);
    let ack = e
        .poll_delivery_with_changes("sub")
        .unwrap()
        .delivery
        .unwrap();
    e.begin_delivery("sub", ack).unwrap();
    publish(&mut e, "s/receipt", 2, 11, false);
    let receipt = e
        .poll_delivery_with_changes("sub")
        .unwrap()
        .delivery
        .unwrap();
    e.begin_delivery("sub", receipt).unwrap();
    publish(&mut e, "s/comp", 2, 12, false);
    let comp = e
        .poll_delivery_with_changes("sub")
        .unwrap()
        .delivery
        .unwrap();
    let comp = e.begin_delivery("sub", comp).unwrap().frame.unwrap();
    let Packet::Publish {
        packet_id: Some(id),
        ..
    } = comp.packet
    else {
        panic!("expected publish");
    };
    e.handle("sub", Packet::PubRec { packet_id: id }).unwrap();
    publish(&mut e, "s/queued\"\\雪😀", 1, 13, false);
    publish(&mut e, "s/queued0", 0, 0, false);
    publish(&mut e, "s/queued2", 2, 16, false);
    let mut input = Frame::v5(Packet::Publish {
        topic: "s/incoming".into(),
        payload: vec![0, 255],
        qos: 2,
        packet_id: Some(14),
        dup: false,
        retain: false,
    });
    input.properties = vec![
        Property::MessageExpiryInterval(60),
        Property::UserProperty("key\"\\雪".into(), "value\n\t😀".into()),
    ];
    e.handle_frame("pub", input).unwrap();
    publish(&mut e, "retained/\"\\雪😀", 1, 15, true);
    publish(&mut e, "retained0", 0, 0, true);
    publish(&mut e, "retained2", 2, 17, true);
    let mut o = options();
    o.will_properties = vec![
        Property::WillDelayInterval(10),
        Property::MessageExpiryInterval(60),
        Property::UserProperty("will\"".into(), "雪\\\n".into()),
    ];
    e.connect_with_options(
        "will",
        Some(Will {
            topic: "will/雪".into(),
            payload: vec![0, 255, 80],
            qos: 1,
            retain: true,
        }),
        Principal {
            name: "owner\"\\\n雪😀".into(),
        },
        o,
    )
    .unwrap();
    e.connect_with_options(
        "will-active",
        Some(Will {
            topic: "will/active".into(),
            payload: vec![80],
            qos: 2,
            retain: false,
        }),
        Principal::anonymous(),
        options(),
    )
    .unwrap();
    e.disconnect_with_options("will", false, None).unwrap();
    e.wills.get_mut("will").unwrap().timer_blocked = true;
    // Populate non-default metadata while retaining an entry with fallback []/nulls.
    let session = e.sessions.get_mut("sub").unwrap();
    let ids: Vec<_> = session.outgoing.order.iter().copied().collect();
    session.outgoing_properties.insert(
        ids[0],
        vec![
            Property::ContentType("text/雪\"".into()),
            Property::CorrelationData(vec![0, 255]),
            Property::UserProperty("k\\".into(), "v\t😀".into()),
        ],
    );
    session.outgoing_retained_replay.insert(ids[0]);
    session
        .outgoing_shared
        .insert(ids[1], "$share/g/s/#".into());
    session.outgoing_expiry.insert(ids[1], 61000);
    e.retained_properties.insert(
        "retained/\"\\雪😀".into(),
        vec![
            Property::MessageExpiryInterval(60),
            Property::UserProperty("retained".into(), "value\n😀".into()),
        ],
    );
    e.retained_expiry.insert("retained/\"\\雪😀".into(), 61000);
    e
}
fn same_result(a: Result<Vec<u8>, EngineError>, b: Result<Vec<u8>, EngineError>) {
    match (a, b) {
        (Ok(a), Ok(b)) => assert_eq!(a, b),
        (Err(a), Err(b)) => {
            assert_eq!(std::mem::discriminant(&a), std::mem::discriminant(&b));
            assert_eq!(a.to_string(), b.to_string());
        }
        (a, b) => panic!("different results: {a:?}, {b:?}"),
    }
}
fn check(e: &SessionEngine) {
    for persistent in [false, true] {
        same_result(
            encode_projection(e, persistent),
            oracle::serialize(e, persistent),
        );
    }
    same_result(e.snapshot(), oracle::snapshot(e));
    let old = oracle::bounds(e);
    let new = check_live_bounds(e);
    assert_eq!(old.is_ok(), new.is_ok());
    if let (Err(old), Err(new)) = (old, new) {
        assert_eq!(std::mem::discriminant(&old), std::mem::discriminant(&new));
    }
}

#[test]
fn borrowed_schema_bytes_equal_owned_oracle_for_all_stages_and_metadata() {
    let e = rich();
    check(&e);
    let value: serde_json::Value =
        serde_json::from_slice(&encode_projection(&e, true).unwrap()).unwrap();
    let sub = value["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["client_id"] == "sub")
        .unwrap();
    let out = sub["outgoing"].as_array().unwrap();
    assert!(out[0]["stage"]["Ack"].is_array());
    assert!(out[1]["stage"]["Receipt"].is_array());
    assert_eq!(out[2]["stage"], "Comp");
    assert!(sub["expires_at"].is_null());
    assert_eq!(out[2]["properties"], serde_json::json!([]));
    assert_eq!(
        sub["queued"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["qos"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![1, 0, 2]
    );
    assert_eq!(
        value["retained"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["qos"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![1, 0, 2]
    );
    assert_eq!(value["wills"][1]["pending"], false);
    assert!(value["wills"][0].get("timer_blocked").is_none());
    assert_eq!(value["wills"][0]["pending"], true);
    let persistent = encode_projection(&e, true).unwrap();
    let live = encode_projection(&e, false).unwrap();
    assert_ne!(persistent, live);
    assert!(!String::from_utf8(persistent).unwrap().contains("live-only"));
    assert!(String::from_utf8(live).unwrap().contains("live-only"));
}

#[test]
fn exact_limits_match_owned_oracle_for_escaped_strings_and_counting() {
    let mut e = rich();
    for persistent in [false, true] {
        let length = oracle::serialize(&e, persistent).unwrap().len();
        for limit in [1, length - 1, length, length + 1] {
            e.config.max_state_bytes = limit;
            same_result(
                encode_projection(&e, persistent),
                oracle::serialize(&e, persistent),
            );
            same_result(e.snapshot(), oracle::snapshot(&e));
        }
        e.config.max_state_bytes = config().max_state_bytes;
    }
    let mut small = engine();
    small.connect("escaped\"\\雪😀", false).unwrap();
    let length = oracle::serialize(&small, false).unwrap().len();
    for limit in 0..=length + 1 {
        small.config.max_state_bytes = limit;
        same_result(
            encode_projection(&small, false),
            oracle::serialize(&small, false),
        );
        assert_eq!(
            check_live_bounds(&small).is_ok(),
            oracle::bounds(&small).is_ok()
        );
    }
}

#[test]
fn invalid_order_priority_and_projection_match_even_before_json_prefix_fits() {
    let mut e = rich();
    e.sessions
        .get_mut("sub")
        .unwrap()
        .outgoing
        .order
        .push_back(identifier(65535).unwrap());
    e.config.max_state_bytes = 1;
    for persistent in [false, true] {
        same_result(
            encode_projection(&e, persistent),
            oracle::serialize(&e, persistent),
        );
    }
    assert!(matches!(
        encode_projection(&e, true),
        Err(EngineError::InvalidRecord(_))
    ));
    same_result(e.snapshot(), oracle::snapshot(&e));
    // A corrupt clean-only record is not part of serialize(true), but must still
    // fail snapshot's complete-live check before any persistent output.
    let mut e = engine();
    e.connect("clean", true).unwrap();
    e.sessions
        .get_mut("clean")
        .unwrap()
        .outgoing
        .order
        .push_back(identifier(3).unwrap());
    e.config.max_state_bytes = 1;
    assert!(matches!(
        encode_projection(&e, true),
        Err(EngineError::Capacity)
    ));
    assert!(matches!(e.snapshot(), Err(EngineError::InvalidRecord(_))));
    check(&e);
}

#[test]
fn existing_orphan_validation_still_precedes_capacity_for_complete_live_snapshot() {
    let mut e = engine();
    e.connect("clean", true).unwrap();
    e.sessions
        .get_mut("clean")
        .unwrap()
        .incoming_expiry
        .insert(identifier(3).unwrap(), 100);
    e.config.max_state_bytes = 1;
    assert!(matches!(e.snapshot(), Err(EngineError::InvalidRecord(_))));
    same_result(e.snapshot(), oracle::snapshot(&e));
}

#[test]
fn counting_writer_rejects_whole_chunks_without_partial_advance_or_overflow() {
    let mut w = CountingWriter { count: 0, limit: 3 };
    assert_eq!(w.write(b"ab").unwrap(), 2);
    assert!(w.write(b"cd").is_err());
    assert_eq!(w.count, 2);
    assert_eq!(w.write(b"c").unwrap(), 1);
    assert_eq!(w.count, 3);
    assert!(w.write(b"d").is_err());
    assert_eq!(w.count, 3);
    let mut w = CountingWriter {
        count: usize::MAX - 1,
        limit: usize::MAX,
    };
    assert!(w.write(b"ab").is_err());
    assert_eq!(w.count, usize::MAX - 1);
    assert_eq!(w.write(b"c").unwrap(), 1);
    assert_eq!(w.count, usize::MAX);
}

#[test]
fn normalized_recovery_and_reconnected_replay_keep_identical_schema_bytes() {
    let mut e = rich();
    // Metadata on an Ack payload must agree with its origin for strict recovery.
    let s = e.sessions.get_mut("sub").unwrap();
    s.outgoing_retained_replay.clear();
    s.outgoing_shared.clear();
    s.outgoing_properties.clear();
    s.outgoing_expiry.clear();
    let bytes = e.snapshot().unwrap();
    let mut restored =
        SessionEngine::restore_with_clock(config(), &bytes, Arc::new(Permit), Arc::new(FixedClock))
            .unwrap();
    check(&restored);
    restored
        .connect_with_options("sub", None, Principal::anonymous(), options())
        .unwrap();
    assert!(restored.delivery_may_be_ready("sub"));
    check(&restored);
    assert_eq!(
        restored.snapshot().unwrap(),
        oracle::snapshot(&restored).unwrap()
    );
}
