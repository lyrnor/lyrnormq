use super::*;
use mqtt_protocol::Will;

fn config() -> SessionConfig {
    SessionConfig {
        max_sessions: 16,
        max_retained: 16,
        max_subscriptions: 8,
        max_queued: 16,
        max_inflight: 8,
        max_packet_size: 4096,
        max_state_bytes: 128 * 1024,
    }
}
fn state() -> SessionEngine {
    SessionEngine::new(config()).unwrap()
}
fn event(state: &mut SessionEngine, id: &str, packet: Packet) -> Vec<Packet> {
    let mut proposal = state.clone();
    let replies = proposal.handle(id, packet).unwrap();
    proposal.snapshot().unwrap();
    *state = proposal;
    replies
}
fn publication(topic: &str, payload: &[u8], qos: u8, retain: bool, id: u16) -> Packet {
    Packet::Publish {
        topic: topic.into(),
        payload: payload.to_vec(),
        qos,
        retain,
        dup: false,
        packet_id: (qos != 0).then_some(id),
    }
}
fn subscribe(state: &mut SessionEngine, id: &str, filter: &str, qos: u8) {
    assert_eq!(
        event(
            state,
            id,
            Packet::Subscribe {
                packet_id: 1,
                filters: vec![(filter.into(), qos)]
            }
        ),
        vec![Packet::SubAck {
            packet_id: 1,
            codes: vec![qos]
        }]
    );
}
fn complete(state: &mut SessionEngine, id: &str, packet: &Packet) {
    match packet {
        Packet::Publish {
            qos: 1,
            packet_id: Some(mid),
            ..
        } => {
            event(state, id, Packet::PubAck { packet_id: *mid });
        }
        Packet::Publish {
            qos: 2,
            packet_id: Some(mid),
            ..
        } => {
            assert_eq!(
                event(state, id, Packet::PubRec { packet_id: *mid }),
                vec![Packet::PubRel { packet_id: *mid }]
            );
            event(state, id, Packet::PubComp { packet_id: *mid });
        }
        Packet::Publish { qos: 0, .. } => {}
        _ => panic!("unexpected publication {packet:?}"),
    }
}
fn check(packet: &Packet, payload: &[u8], qos: u8, retain: bool) {
    assert!(
        matches!(packet,Packet::Publish {payload:actual,qos:actual_qos,retain:actual_retain,..} if actual==payload&&*actual_qos==qos&&*actual_retain==retain),
        "{packet:?}"
    );
}
fn outgoing_id(packet: &Packet) -> u16 {
    match packet {
        Packet::Publish {
            packet_id: Some(id),
            ..
        } => *id,
        _ => panic!("missing ID"),
    }
}
fn will(payload: &[u8], qos: u8, retain: bool) -> Will {
    Will {
        topic: "status/client".into(),
        payload: payload.to_vec(),
        qos,
        retain,
    }
}
fn restored(state: &SessionEngine) -> SessionEngine {
    SessionEngine::restore(config(), &state.snapshot().unwrap()).unwrap()
}

#[test]
fn retained_updates_delete_and_replay_flags_are_independent_of_sessions() {
    let mut state = state();
    state.connect("pub", true).unwrap();
    state.connect("live", true).unwrap();
    subscribe(&mut state, "live", "a/#", 2);
    event(&mut state, "pub", publication("a/b", b"first", 1, true, 1));
    let live = state.poll("live").unwrap().unwrap();
    check(&live, b"first", 1, false);
    complete(&mut state, "live", &live);
    state.connect("replay", true).unwrap();
    subscribe(&mut state, "replay", "a/+", 2);
    let replay = state.poll("replay").unwrap().unwrap();
    check(&replay, b"first", 1, true);
    complete(&mut state, "replay", &replay);
    event(
        &mut state,
        "pub",
        publication("a/b", b"ordinary", 0, false, 0),
    );
    assert_eq!(state.retained.entries()[0].payload, b"first");
    for id in ["live", "replay"] {
        let packet = state.poll(id).unwrap().unwrap();
        check(&packet, b"ordinary", 0, false);
    }
    event(
        &mut state,
        "pub",
        publication("a/b", b"updated", 0, true, 0),
    );
    assert_eq!(state.retained.entries()[0].payload, b"updated");
    for id in ["live", "replay"] {
        state.poll(id).unwrap();
    }
    state.disconnect("pub").unwrap();
    state.connect("pub", true).unwrap();
    assert_eq!(state.retained.len(), 1);
    event(&mut state, "pub", publication("a/b", b"", 1, true, 2));
    for id in ["live", "replay"] {
        let packet = state.poll(id).unwrap().unwrap();
        check(&packet, b"", 1, false);
        complete(&mut state, id, &packet);
    }
    assert!(state.retained.is_empty());
    state.connect("later", true).unwrap();
    subscribe(&mut state, "later", "#", 2);
    assert!(state.poll("later").unwrap().is_none());
}

#[test]
fn retained_qos2_updates_only_at_pubrel_and_repeats_do_not_deliver_twice() {
    let mut state = state();
    state.connect("pub", false).unwrap();
    state.connect("sub", false).unwrap();
    subscribe(&mut state, "sub", "a", 0);
    assert_eq!(
        event(&mut state, "pub", publication("a", b"stored", 2, true, 7)),
        vec![Packet::PubRec { packet_id: 7 }]
    );
    assert!(state.retained.is_empty());
    assert!(state.poll("sub").unwrap().is_none());
    let mut state = restored(&state);
    state.connect("pub", false).unwrap();
    state.connect("sub", false).unwrap();
    event(&mut state, "pub", Packet::PubRel { packet_id: 7 });
    check(&state.poll("sub").unwrap().unwrap(), b"stored", 0, false);
    event(&mut state, "pub", Packet::PubRel { packet_id: 7 });
    assert!(state.poll("sub").unwrap().is_none());
    assert_eq!(state.retained.entries()[0].payload, b"stored");
    state.connect("late", true).unwrap();
    subscribe(&mut state, "late", "a", 1);
    check(&state.poll("late").unwrap().unwrap(), b"stored", 1, true);
}

#[test]
fn retained_resubscription_sequence_system_topics_and_reconnect_keep_flags() {
    let mut state = state();
    state.connect("pub", true).unwrap();
    event(&mut state, "pub", publication("a/b", b"a", 1, true, 1));
    event(&mut state, "pub", publication("$SYS/x", b"sys", 0, true, 0));
    state.connect("sub", false).unwrap();
    subscribe(&mut state, "sub", "#", 1);
    let first = state.poll("sub").unwrap().unwrap();
    check(&first, b"a", 1, true);
    let mut state = restored(&state);
    state.connect("sub", false).unwrap();
    let replay = state.poll("sub").unwrap().unwrap();
    assert!(matches!(
        replay,
        Packet::Publish {
            dup: true,
            retain: true,
            ..
        }
    ));
    assert_eq!(outgoing_id(&first), outgoing_id(&replay));
    complete(&mut state, "sub", &replay);
    subscribe(&mut state, "sub", "#", 0);
    check(&state.poll("sub").unwrap().unwrap(), b"a", 0, true);
    subscribe(&mut state, "sub", "$SYS/#", 2);
    check(&state.poll("sub").unwrap().unwrap(), b"sys", 0, true);
    event(
        &mut state,
        "sub",
        Packet::Subscribe {
            packet_id: 2,
            filters: vec![("a/#".into(), 0), ("a/+".into(), 1)],
        },
    );
    check(&state.poll("sub").unwrap().unwrap(), b"a", 0, true);
    let last = state.poll("sub").unwrap().unwrap();
    check(&last, b"a", 1, true);
    complete(&mut state, "sub", &last);
}

#[test]
fn full_replay_and_retained_store_failures_discard_the_whole_proposal() {
    let mut state = SessionEngine::new(SessionConfig {
        max_queued: 1,
        max_retained: 2,
        ..config()
    })
    .unwrap();
    state.connect("pub", true).unwrap();
    event(&mut state, "pub", publication("a", b"one", 0, true, 0));
    event(&mut state, "pub", publication("b", b"two", 0, true, 0));
    state.connect("sub", false).unwrap();
    let before = state.snapshot().unwrap();
    let mut proposal = state.clone();
    assert!(
        proposal
            .handle(
                "sub",
                Packet::Subscribe {
                    packet_id: 1,
                    filters: vec![("#".into(), 1)]
                }
            )
            .is_err()
    );
    assert_eq!(state.snapshot().unwrap(), before);
    assert!(state.poll("sub").unwrap().is_none());
    let mut proposal = state.clone();
    assert!(
        proposal
            .handle("pub", publication("c", b"over", 1, true, 1))
            .is_err()
    );
    assert_eq!(state.snapshot().unwrap(), before);
}

#[test]
fn graceful_will_is_silent_and_abnormal_binary_will_uses_each_qos() {
    for qos in 0..=2 {
        let mut state = state();
        state.connect("sub", false).unwrap();
        subscribe(&mut state, "sub", "status/#", 2);
        state
            .connect_with_will("src", true, Some(will(&[0, 255, 0x80], qos, false)))
            .unwrap();
        state.disconnect_with_reason("src", true).unwrap();
        assert!(state.wills.is_empty());
        assert!(state.poll("sub").unwrap().is_none());
        state
            .connect_with_will("src", true, Some(will(&[0, 255, 0x80], qos, false)))
            .unwrap();
        state.disconnect_with_reason("src", false).unwrap();
        let packet = state.poll("sub").unwrap().unwrap();
        check(&packet, &[0, 255, 0x80], qos, false);
        complete(&mut state, "sub", &packet);
        assert!(state.poll("sub").unwrap().is_none());
        assert!(state.wills.is_empty());
    }
}

#[test]
fn takeover_publishes_old_will_once_then_installs_and_clears_new_will() {
    let mut state = state();
    state.connect("sub", false).unwrap();
    subscribe(&mut state, "sub", "status/#", 2);
    state
        .connect_with_will("same", true, Some(will(b"old", 1, true)))
        .unwrap();
    state
        .connect_with_will("same", true, Some(will(b"new", 1, true)))
        .unwrap();
    let packet = state.poll("sub").unwrap().unwrap();
    check(&packet, b"old", 1, false);
    complete(&mut state, "sub", &packet);
    assert!(state.poll("sub").unwrap().is_none());
    assert_eq!(state.retained.entries()[0].payload, b"old");
    state.disconnect_with_reason("same", true).unwrap();
    assert!(state.wills.is_empty());
    assert!(state.poll("sub").unwrap().is_none());
    state.connect("later", true).unwrap();
    subscribe(&mut state, "later", "status/#", 2);
    check(&state.poll("later").unwrap().unwrap(), b"old", 1, true);
}

#[test]
fn full_outbox_defers_will_without_partial_retained_update_and_retries_once() {
    let mut state = SessionEngine::new(SessionConfig {
        max_queued: 1,
        ..config()
    })
    .unwrap();
    state.connect("sub", false).unwrap();
    subscribe(&mut state, "sub", "status/#", 1);
    state.disconnect("sub").unwrap();
    state.connect("pub", true).unwrap();
    event(
        &mut state,
        "pub",
        publication("status/client", b"fills", 1, false, 1),
    );
    state
        .connect_with_will("src", true, Some(will(b"will", 1, true)))
        .unwrap();
    state.disconnect_with_reason("src", false).unwrap();
    assert!(state.wills["src"].pending);
    assert!(state.retained.is_empty());
    assert!(!state.retry_wills().unwrap());
    let mut rejected = state.clone();
    assert!(
        rejected
            .connect_with_will("src", true, Some(will(b"overwrite", 1, false)))
            .is_err()
    );
    let mut state = SessionEngine::restore(
        SessionConfig {
            max_queued: 1,
            ..config()
        },
        &state.snapshot().unwrap(),
    )
    .unwrap();
    state.connect("sub", false).unwrap();
    let fill = state.poll("sub").unwrap().unwrap();
    check(&fill, b"fills", 1, false);
    assert!(state.retry_wills().unwrap());
    assert!(state.wills.is_empty());
    assert_eq!(state.retained.entries()[0].payload, b"will");
    let packet = state.poll("sub").unwrap().unwrap();
    check(&packet, b"will", 1, false);
    assert!(!state.retry_wills().unwrap());
    assert!(state.poll("sub").unwrap().is_none());
}

#[test]
fn clean_source_active_will_survives_snapshot_and_publishes_on_recovery() {
    let mut state = state();
    state.connect("sub", false).unwrap();
    subscribe(&mut state, "sub", "status/#", 1);
    state.disconnect("sub").unwrap();
    state
        .connect_with_will("clean", true, Some(will(b"crashed", 1, true)))
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&state.snapshot().unwrap()).unwrap();
    assert_eq!(json["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(json["wills"].as_array().unwrap().len(), 1);
    let mut state = restored(&state);
    state.recover_wills().unwrap();
    state.recover_wills().unwrap();
    assert!(state.wills.is_empty());
    assert!(state.connect("sub", false).unwrap().session_present);
    check(&state.poll("sub").unwrap().unwrap(), b"crashed", 1, false);
    assert!(state.poll("sub").unwrap().is_none());
    state.connect("new", true).unwrap();
    subscribe(&mut state, "new", "status/#", 1);
    check(&state.poll("new").unwrap().unwrap(), b"crashed", 1, true);
}

#[test]
fn retained_empty_will_deletes_and_persistent_source_can_queue_its_own_will() {
    let mut state = state();
    state.connect("pub", true).unwrap();
    event(
        &mut state,
        "pub",
        publication("status/client", b"old", 0, true, 0),
    );
    state
        .connect_with_will("self", false, Some(will(b"", 1, true)))
        .unwrap();
    subscribe(&mut state, "self", "status/#", 1);
    let replay = state.poll("self").unwrap().unwrap();
    complete(&mut state, "self", &replay);
    state.disconnect_with_reason("self", false).unwrap();
    assert!(state.retained.is_empty());
    state.connect("self", false).unwrap();
    let packet = state.poll("self").unwrap().unwrap();
    check(&packet, b"", 1, false);
    complete(&mut state, "self", &packet);
    assert!(state.poll("self").unwrap().is_none());
}

#[test]
fn schema1_migrates_explicitly_and_schema2_globals_are_required_and_validated() {
    let mut state = state();
    state.connect("sub", false).unwrap();
    state.connect("pub", false).unwrap();
    subscribe(&mut state, "sub", "a", 1);
    event(&mut state, "pub", publication("a", b"legacy", 1, false, 1));
    event(&mut state, "pub", publication("a", b"pending", 2, false, 2));
    let mut legacy: serde_json::Value = serde_json::from_slice(&state.snapshot().unwrap()).unwrap();
    legacy["schema_version"] = 1.into();
    legacy.as_object_mut().unwrap().remove("retained");
    legacy.as_object_mut().unwrap().remove("wills");
    for session in legacy["sessions"].as_array_mut().unwrap() {
        for message in session["queued"].as_array_mut().unwrap() {
            message.as_object_mut().unwrap().remove("retain");
        }
    }
    let mut restored =
        SessionEngine::restore(config(), &serde_json::to_vec(&legacy).unwrap()).unwrap();
    assert!(restored.retained.is_empty());
    assert!(restored.wills.is_empty());
    restored.connect("sub", false).unwrap();
    check(&restored.poll("sub").unwrap().unwrap(), b"legacy", 1, false);
    let json: serde_json::Value = serde_json::from_slice(&restored.snapshot().unwrap()).unwrap();
    assert_eq!(json["schema_version"], 2);
    for field in ["retained", "wills"] {
        let mut bad = json.clone();
        bad.as_object_mut().unwrap().remove(field);
        assert!(SessionEngine::restore(config(), &serde_json::to_vec(&bad).unwrap()).is_err());
    }
    let mut bad = json.clone();
    bad["retained"] = serde_json::json!([{"topic":"a","payload":[],"qos":0}]);
    assert!(SessionEngine::restore(config(), &serde_json::to_vec(&bad).unwrap()).is_err());
    let mut bad = json.clone();
    bad["retained"] = serde_json::json!([{"topic":"a","payload":[1],"qos":0},{"topic":"a","payload":[2],"qos":1}]);
    assert!(SessionEngine::restore(config(), &serde_json::to_vec(&bad).unwrap()).is_err());
    let mut bad = json;
    bad["wills"] = serde_json::json!([{"client_id":"x","pending":false,"will":{"topic":"bad/+","payload":[0],"qos":0,"retain":false}}]);
    assert!(SessionEngine::restore(config(), &serde_json::to_vec(&bad).unwrap()).is_err());
}

#[test]
fn retained_and_clean_will_bytes_participate_in_live_and_snapshot_limits() {
    let mut state = SessionEngine::new(SessionConfig {
        max_state_bytes: 512,
        ..config()
    })
    .unwrap();
    state.connect("pub", true).unwrap();
    let before = state.snapshot().unwrap();
    let mut proposal = state.clone();
    assert!(
        proposal
            .handle("pub", publication("a", &[255; 200], 0, true, 0))
            .is_err()
    );
    assert_eq!(state.snapshot().unwrap(), before);
    let mut proposal = state.clone();
    assert!(
        proposal
            .connect_with_will("clean", true, Some(will(&[255; 200], 1, true)))
            .is_err()
    );
    assert_eq!(state.snapshot().unwrap(), before);
    let mut state = SessionEngine::new(config()).unwrap();
    state
        .connect_with_will("x", true, Some(will(b"valid", 0, false)))
        .unwrap();
    let mut json: serde_json::Value = serde_json::from_slice(&state.snapshot().unwrap()).unwrap();
    let duplicate = json["wills"][0].clone();
    json["wills"].as_array_mut().unwrap().push(duplicate);
    assert!(SessionEngine::restore(config(), &serde_json::to_vec(&json).unwrap()).is_err());
}

#[test]
fn schema1_rejects_retained_wire_that_old_codec_could_not_store() {
    for qos in [1, 2] {
        let mut state = state();
        state.connect("sub", false).unwrap();
        state.connect("pub", true).unwrap();
        subscribe(&mut state, "sub", "a", qos);
        event(
            &mut state,
            "pub",
            publication("a", b"published", qos, false, 4),
        );
        if qos == 2 {
            event(&mut state, "pub", Packet::PubRel { packet_id: 4 });
        }
        let first = state.poll("sub").unwrap().unwrap();
        let mut retained_wire = first.clone();
        if let Packet::Publish { retain, .. } = &mut retained_wire {
            *retain = true;
        }
        let bytes = encode(&retained_wire).unwrap();
        let mut json: serde_json::Value =
            serde_json::from_slice(&state.snapshot().unwrap()).unwrap();
        let session = json["sessions"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|session| session["client_id"] == "sub")
            .unwrap();
        session["outgoing"][0]["stage"] = if qos == 1 {
            serde_json::json!({"Ack":bytes})
        } else {
            serde_json::json!({"Receipt":bytes})
        };
        assert!(SessionEngine::restore(config(), &serde_json::to_vec(&json).unwrap()).is_ok());
        json["schema_version"] = 1.into();
        json.as_object_mut().unwrap().remove("retained");
        json.as_object_mut().unwrap().remove("wills");
        assert!(SessionEngine::restore(config(), &serde_json::to_vec(&json).unwrap()).is_err());
    }
}
