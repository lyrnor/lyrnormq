use super::*;
use crate::durable::{SessionConfig, SessionEngine};
use mqtt_protocol::Packet;
fn config() -> SessionConfig {
    SessionConfig {
        max_sessions: 8,
        max_retained: 8,
        max_subscriptions: 8,
        max_queued: 8,
        max_inflight: 8,
        max_packet_size: 512,
        max_state_bytes: 32768,
    }
}
fn snapshot() -> serde_json::Value {
    let mut e = SessionEngine::new(config()).unwrap();
    e.connect("sub", false).unwrap();
    e.connect("pub", false).unwrap();
    e.handle(
        "sub",
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![("a".into(), 1)],
        },
    )
    .unwrap();
    for packet_id in 1..=2 {
        e.handle(
            "pub",
            Packet::Publish {
                topic: "a".into(),
                payload: vec![packet_id as u8],
                qos: 1,
                packet_id: Some(packet_id),
                dup: false,
                retain: true,
            },
        )
        .unwrap();
    }
    e.poll("sub").unwrap();
    e.handle(
        "pub",
        Packet::Publish {
            topic: "a".into(),
            payload: vec![3],
            qos: 2,
            packet_id: Some(3),
            dup: false,
            retain: false,
        },
    )
    .unwrap();
    e.connect_with_will(
        "will",
        true,
        Some(Will {
            topic: "a".into(),
            payload: vec![4],
            qos: 1,
            retain: false,
        }),
    )
    .unwrap();
    serde_json::from_slice(&e.snapshot().unwrap()).unwrap()
}
fn restore(v: &serde_json::Value) -> Result<SessionEngine, EngineError> {
    SessionEngine::restore(config(), &serde_json::to_vec(v).unwrap())
}
#[test]
fn schema5_new_fields_are_required_even_when_nullable() {
    let valid = snapshot();
    assert_eq!(valid["schema_version"], 6);
    assert!(restore(&valid).is_ok());
    for (pointer, key) in [
        ("", "clock_floor"),
        ("/sessions/1/queued/0", "expires_at"),
        ("/sessions/1/outgoing/0", "expires_at"),
        ("/sessions/1/outgoing/0", "started"),
        ("/retained/0", "expires_at"),
        ("/wills/0", "due_at"),
        ("/wills/0", "delay_interval"),
    ] {
        let mut bad = valid.clone();
        assert!(
            bad.pointer_mut(pointer)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove(key)
                .is_some()
        );
        assert!(restore(&bad).is_err(), "{pointer}/{key}");
    }
    for malformed in [
        serde_json::Value::Null,
        serde_json::json!(-1),
        serde_json::json!("1000"),
    ] {
        let mut bad = valid.clone();
        bad["clock_floor"] = malformed;
        assert!(restore(&bad).is_err());
    }
    let mut bad = valid.clone();
    bad["sessions"][0]["incoming"][0]
        .as_array_mut()
        .unwrap()
        .pop();
    assert!(restore(&bad).is_err());
    let mut bad = valid.clone();
    bad["wills"][0]["pending"] = true.into();
    assert!(restore(&bad).is_err());
    let mut bad = valid.clone();
    bad["sessions"][1]["queued"][0]["expires_at"] = Some(100u64).into();
    assert!(restore(&bad).is_err());
    let mut bad = valid.clone();
    bad["sessions"][1]["outgoing"][0]["stage"] = "Comp".into();
    bad["sessions"][1]["outgoing"][0]["started"] = false.into();
    assert!(restore(&bad).is_err());
}
fn downgrade4(mut value: serde_json::Value) -> serde_json::Value {
    value = crate::durable::phase2h_tests::schema5(value);
    value["schema_version"] = 4.into();
    value.as_object_mut().unwrap().remove("clock_floor");
    for s in value["sessions"].as_array_mut().unwrap() {
        for incoming in s["incoming"].as_array_mut().unwrap() {
            assert_eq!(incoming.as_array().unwrap().len(), 4);
            incoming.as_array_mut().unwrap().pop();
        }
        for outgoing in s["outgoing"].as_array_mut().unwrap() {
            outgoing.as_object_mut().unwrap().remove("expires_at");
            outgoing.as_object_mut().unwrap().remove("started");
        }
        for queued in s["queued"].as_array_mut().unwrap() {
            queued.as_object_mut().unwrap().remove("expires_at");
        }
    }
    for r in value["retained"].as_array_mut().unwrap() {
        r.as_object_mut().unwrap().remove("expires_at");
    }
    for w in value["wills"].as_array_mut().unwrap() {
        w.as_object_mut().unwrap().remove("delay_interval");
        w.as_object_mut().unwrap().remove("due_at");
    }
    value
}
#[test]
fn schema4_migration_preserves_state_and_rejects_unsupported_legacy_metadata() {
    let old = downgrade4(snapshot());
    let (decoded, _) = decode(&serde_json::to_vec(&old).unwrap()).unwrap();
    assert_eq!(decoded.clock_floor, 0);
    let restored = restore(&old).unwrap();
    let next: serde_json::Value = serde_json::from_slice(&restored.snapshot().unwrap()).unwrap();
    assert_eq!(next["schema_version"], 6);
    assert_eq!(next["sessions"][1]["outgoing"][0]["started"], true);
    assert_eq!(
        next["sessions"][0]["incoming"][0].as_array().unwrap().len(),
        4
    );
    assert!(next["sessions"][1]["queued"][0]["expires_at"].is_null());
    assert!(next["wills"][0].get("timer_blocked").is_none());
    let mut bad = old.clone();
    bad["sessions"][0]
        .as_object_mut()
        .unwrap()
        .remove("expires_at");
    assert!(restore(&bad).is_err());
    for pointer in [
        "/sessions/0/incoming/0/2",
        "/sessions/1/outgoing/0/properties",
        "/sessions/1/queued/0/properties",
        "/retained/0/properties",
        "/wills/0/will/properties",
    ] {
        let mut bad = old.clone();
        *bad.pointer_mut(pointer).unwrap() = serde_json::json!([{"MessageExpiryInterval":10}]);
        assert!(restore(&bad).is_err(), "{pointer}");
    }
}
