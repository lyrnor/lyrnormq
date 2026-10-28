use super::*;
use mqtt_auth::{AuthError, ConnectInfo};
use mqtt_protocol::Will;

#[derive(Debug, Default)]
struct Policy {
    publish: Vec<&'static str>,
    subscribe: Vec<&'static str>,
    receive: Vec<&'static str>,
    clients: Vec<&'static str>,
}
impl AccessControl for Policy {
    fn authenticate(&self, _: ConnectInfo<'_>) -> Result<Principal, AuthError> {
        Err(AuthError::BadCredentials)
    }
    fn authorize(&self, _: &Principal, action: Action, resource: &str) -> bool {
        let denied = match action {
            Action::Connect => &self.clients,
            Action::Publish => &self.publish,
            Action::Subscribe => &self.subscribe,
            Action::Receive => &self.receive,
        };
        !denied.iter().any(|rule| *rule == "*" || *rule == resource)
    }
}
fn config() -> SessionConfig {
    SessionConfig {
        max_sessions: 16,
        max_retained: 16,
        max_subscriptions: 16,
        max_queued: 8,
        max_inflight: 8,
        max_packet_size: 4096,
        max_state_bytes: 256 * 1024,
    }
}
fn state() -> SessionEngine {
    SessionEngine::new_with_access(config(), Arc::new(Policy::default())).unwrap()
}
fn owner(name: &str) -> Principal {
    Principal {
        name: format!("user:{name}"),
    }
}
fn connect(state: &mut SessionEngine, id: &str, user: &str, clean: bool) {
    state
        .connect_authenticated(id, clean, None, owner(user))
        .unwrap();
}
fn publish(topic: &str, qos: u8, id: u16, retain: bool) -> Packet {
    Packet::Publish {
        topic: topic.into(),
        payload: b"data".to_vec(),
        qos,
        packet_id: (qos != 0).then_some(id),
        dup: false,
        retain,
    }
}
fn subscribe(state: &mut SessionEngine, id: &str, filter: &str) {
    assert_eq!(
        state
            .handle(
                id,
                Packet::Subscribe {
                    packet_id: 1,
                    filters: vec![(filter.into(), 2)]
                }
            )
            .unwrap(),
        vec![Packet::SubAck {
            packet_id: 1,
            codes: vec![2]
        }]
    );
}
fn send(state: &mut SessionEngine, id: &str, topic: &str, qos: u8, packet_id: u16) {
    state
        .handle(id, publish(topic, qos, packet_id, false))
        .unwrap();
    if qos == 2 {
        state.handle(id, Packet::PubRel { packet_id }).unwrap();
    }
}
fn will(topic: &str) -> Will {
    Will {
        topic: topic.into(),
        payload: b"will".to_vec(),
        qos: 1,
        retain: true,
    }
}
fn id(packet: Packet) -> u16 {
    match packet {
        Packet::Publish {
            packet_id: Some(id),
            ..
        } => id,
        _ => panic!("expected QoS publication"),
    }
}
fn snapshot(state: &SessionEngine) -> serde_json::Value {
    serde_json::from_slice(&state.snapshot().unwrap()).unwrap()
}

#[test]
fn ownership_blocks_live_takeover_and_clean_reset_before_old_will_changes() {
    let mut state = state();
    state
        .connect_authenticated("same", false, Some(will("status")), owner("alice"))
        .unwrap();
    subscribe(&mut state, "same", "data/#");
    let before = state.snapshot().unwrap();
    for clean in [false, true] {
        assert!(matches!(
            state.connect_authenticated("same", clean, None, owner("bob")),
            Err(EngineError::NotAuthorized)
        ));
        assert_eq!(state.snapshot().unwrap(), before);
        assert!(state.sessions["same"].online);
        assert!(!state.wills["same"].pending);
    }
    assert!(
        state
            .connect_authenticated("same", false, None, owner("alice"))
            .unwrap()
            .session_present
    );
}

#[test]
fn ownership_of_pending_clean_will_survives_without_session_record() {
    let mut state = SessionEngine::new_with_access(
        SessionConfig {
            max_queued: 1,
            ..config()
        },
        Arc::new(Policy::default()),
    )
    .unwrap();
    connect(&mut state, "sub", "reader", false);
    connect(&mut state, "pub", "writer", true);
    subscribe(&mut state, "sub", "status");
    send(&mut state, "pub", "status", 1, 1);
    state
        .connect_authenticated("source", true, Some(will("status")), owner("alice"))
        .unwrap();
    state.disconnect_with_reason("source", false).unwrap();
    assert!(!state.sessions.contains_key("source"));
    assert!(state.wills["source"].pending);
    let before = state.snapshot().unwrap();
    for clean in [false, true] {
        assert!(matches!(
            state.connect_authenticated("source", clean, None, owner("bob")),
            Err(EngineError::NotAuthorized)
        ));
        assert_eq!(state.snapshot().unwrap(), before);
    }
    state
        .connect_authenticated("source", true, None, owner("alice"))
        .unwrap();
    assert!(state.wills["source"].pending);
}

#[test]
fn denied_new_will_and_client_prefix_do_not_mutate_existing_connection() {
    let mut state = state();
    state
        .connect_authenticated("same", false, Some(will("status")), owner("alice"))
        .unwrap();
    state.access = Arc::new(Policy {
        publish: vec!["denied"],
        ..Policy::default()
    });
    let before = state.snapshot().unwrap();
    assert!(matches!(
        state.connect_authenticated("same", true, Some(will("denied")), owner("alice")),
        Err(EngineError::NotAuthorized)
    ));
    assert_eq!(state.snapshot().unwrap(), before);
    assert!(state.sessions["same"].online);
    state.access = Arc::new(Policy {
        clients: vec!["same"],
        ..Policy::default()
    });
    assert!(matches!(
        state.connect_authenticated("same", true, None, owner("alice")),
        Err(EngineError::NotAuthorized)
    ));
    assert_eq!(state.snapshot().unwrap(), before);
}

#[test]
fn denied_publish_never_acknowledges_or_updates_deletes_retained_or_qos2() {
    let mut state = state();
    connect(&mut state, "source", "alice", false);
    state
        .handle("source", publish("secret", 1, 1, true))
        .unwrap();
    state.access = Arc::new(Policy {
        publish: vec!["secret"],
        ..Policy::default()
    });
    let before = state.snapshot().unwrap();
    for qos in 0..=2 {
        for delete in [false, true] {
            let mut packet = publish("secret", qos, 2, true);
            if delete && let Packet::Publish { payload, .. } = &mut packet {
                payload.clear();
            }
            assert!(matches!(
                state.handle("source", packet),
                Err(EngineError::NotAuthorized)
            ));
            assert_eq!(state.snapshot().unwrap(), before);
        }
    }
}

#[test]
fn subscribe_partial_denial_and_retained_receive_checks_are_independent() {
    let mut state = state();
    connect(&mut state, "source", "alice", false);
    connect(&mut state, "reader", "bob", false);
    for topic in ["secret/one", "data/one", "data/private"] {
        state.handle("source", publish(topic, 1, 1, true)).unwrap();
    }
    state.access = Arc::new(Policy {
        subscribe: vec!["secret/#"],
        receive: vec!["data/private"],
        ..Policy::default()
    });
    assert_eq!(
        state
            .handle(
                "reader",
                Packet::Subscribe {
                    packet_id: 7,
                    filters: vec![("secret/#".into(), 2), ("data/#".into(), 1)]
                }
            )
            .unwrap(),
        vec![Packet::SubAck {
            packet_id: 7,
            codes: vec![128, 1]
        }]
    );
    assert!(
        !state.sessions["reader"]
            .subscriptions
            .contains_key("secret/#")
    );
    assert!(
        matches!(state.poll("reader").unwrap(), Some(Packet::Publish { topic, retain: true, .. }) if topic == "data/one")
    );
    assert!(state.poll("reader").unwrap().is_none());
    state.disconnect("reader").unwrap();
    send(&mut state, "source", "data/private", 1, 2);
    assert!(state.sessions["reader"].queued.is_empty());
    send(&mut state, "source", "data/one", 1, 3);
    assert_eq!(state.sessions["reader"].queued.len(), 1);
}

#[test]
fn revoked_incoming_qos2_pubrel_retains_original_payload_and_has_no_effect() {
    let mut state = state();
    connect(&mut state, "source", "alice", false);
    state
        .handle("source", publish("secret", 2, 9, true))
        .unwrap();
    let mut state = SessionEngine::restore_with_access(
        config(),
        &state.snapshot().unwrap(),
        Arc::new(Policy {
            publish: vec!["secret"],
            ..Policy::default()
        }),
    )
    .unwrap();
    connect(&mut state, "source", "alice", false);
    let before = state.snapshot().unwrap();
    assert!(matches!(
        state.handle("source", Packet::PubRel { packet_id: 9 }),
        Err(EngineError::NotAuthorized)
    ));
    assert_eq!(state.snapshot().unwrap(), before);
    assert!(state.retained.is_empty());
    assert_eq!(
        state
            .handle("source", Packet::PubRel { packet_id: 99 })
            .unwrap(),
        vec![Packet::PubComp { packet_id: 99 }]
    );
    assert_eq!(state.sessions["source"].incoming.pending.len(), 1);
}

#[test]
fn empty_poll_reports_revoked_queued_and_inflight_deletion_and_frees_ids() {
    let mut state = state();
    connect(&mut state, "source", "alice", true);
    connect(&mut state, "reader", "bob", false);
    subscribe(&mut state, "reader", "data");
    send(&mut state, "source", "data", 1, 1);
    let first = id(state.poll("reader").unwrap().unwrap());
    send(&mut state, "source", "data", 2, 2);
    let second = id(state.poll("reader").unwrap().unwrap());
    send(&mut state, "source", "data", 1, 3);
    let mut state = SessionEngine::restore_with_access(
        config(),
        &state.snapshot().unwrap(),
        Arc::new(Policy {
            receive: vec!["*"],
            ..Policy::default()
        }),
    )
    .unwrap();
    connect(&mut state, "reader", "bob", false);
    let result = state.poll_with_changes("reader").unwrap();
    assert!(result.packet.is_none());
    assert!(result.changed);
    let session = &state.sessions["reader"];
    assert!(
        session.queued.is_empty()
            && session.outgoing.pending.is_empty()
            && session.replay.is_empty()
    );
    assert!(session.outgoing.order.is_empty());
    assert_eq!(session.outgoing.identifiers.in_use(), 0);
    assert!(!session.outgoing.identifiers.used[usize::from(first)]);
    assert!(!session.outgoing.identifiers.used[usize::from(second)]);
    assert!(!state.poll_with_changes("reader").unwrap().changed);
    let restored = SessionEngine::restore_with_access(
        config(),
        &state.snapshot().unwrap(),
        Arc::new(Policy::default()),
    )
    .unwrap();
    assert!(restored.sessions["reader"].outgoing.pending.is_empty());
}

#[test]
fn revoked_payloads_are_cancelled_but_released_qos2_can_finish_without_payload() {
    let mut state = state();
    connect(&mut state, "source", "alice", true);
    connect(&mut state, "reader", "bob", false);
    subscribe(&mut state, "reader", "data");
    send(&mut state, "source", "data", 2, 1);
    let released_id = id(state.poll("reader").unwrap().unwrap());
    state
        .handle(
            "reader",
            Packet::PubRec {
                packet_id: released_id,
            },
        )
        .unwrap();
    let mut state = SessionEngine::restore_with_access(
        config(),
        &state.snapshot().unwrap(),
        Arc::new(Policy {
            receive: vec!["*"],
            subscribe: vec!["data"],
            ..Policy::default()
        }),
    )
    .unwrap();
    connect(&mut state, "reader", "bob", false);
    let result = state.poll_with_changes("reader").unwrap();
    assert_eq!(
        result.packet,
        Some(Packet::PubRel {
            packet_id: released_id
        })
    );
    assert!(result.changed);
    assert_eq!(state.sessions["reader"].outgoing.identifiers.in_use(), 1);
    assert!(state.sessions["reader"].subscriptions.is_empty());
    state
        .handle(
            "reader",
            Packet::PubComp {
                packet_id: released_id,
            },
        )
        .unwrap();
    assert_eq!(state.sessions["reader"].outgoing.identifiers.in_use(), 0);
}

#[test]
fn revoked_wills_are_removed_on_recovery_without_changing_global_retained_data() {
    for persistent in [false, true] {
        let mut state = state();
        connect(&mut state, "publisher", "writer", true);
        state
            .handle("publisher", publish("status", 1, 1, true))
            .unwrap();
        state
            .connect_authenticated("source", !persistent, Some(will("status")), owner("alice"))
            .unwrap();
        let bytes = state.snapshot().unwrap();
        let mut state = SessionEngine::restore_with_access(
            config(),
            &bytes,
            Arc::new(Policy {
                publish: vec!["status"],
                ..Policy::default()
            }),
        )
        .unwrap();
        state.recover_wills().unwrap();
        assert!(state.wills.is_empty());
        assert_eq!(state.retained.entries()[0].payload, b"data");
        assert!(!state.retry_wills().unwrap());
    }
}

#[test]
fn legacy_schema2_preserves_retained_wills_send_order_and_binds_only_anonymous() {
    let mut state = state();
    state.connect("source", false).unwrap();
    state.connect("reader", false).unwrap();
    subscribe(&mut state, "reader", "data");
    for qos in [1, 2, 2] {
        send(&mut state, "source", "data", qos, 10);
        let packet = state.poll("reader").unwrap().unwrap();
        if state.sessions["reader"].outgoing.inflight_count() == 3 {
            state
                .handle(
                    "reader",
                    Packet::PubRec {
                        packet_id: id(packet),
                    },
                )
                .unwrap();
        }
    }
    state
        .handle("source", publish("retained", 1, 1, true))
        .unwrap();
    state
        .connect_with_will("will", true, Some(will("status")))
        .unwrap();
    let mut json = super::phase2e_tests::legacy_schema3(snapshot(&state));
    json["schema_version"] = 2.into();
    for key in ["sessions", "wills"] {
        for record in json[key].as_array_mut().unwrap() {
            record.as_object_mut().unwrap().remove("owner");
        }
    }
    let mut restored = SessionEngine::restore_with_access(
        config(),
        &serde_json::to_vec(&json).unwrap(),
        Arc::new(Policy::default()),
    )
    .unwrap();
    for clean in [false, true] {
        assert!(matches!(
            restored.connect_authenticated("reader", clean, None, owner("alice")),
            Err(EngineError::NotAuthorized)
        ));
    }
    restored.connect("reader", false).unwrap();
    assert!(matches!(
        restored.poll("reader").unwrap(),
        Some(Packet::Publish {
            packet_id: Some(1),
            dup: true,
            qos: 1,
            ..
        })
    ));
    assert!(matches!(
        restored.poll("reader").unwrap(),
        Some(Packet::Publish {
            packet_id: Some(2),
            dup: true,
            qos: 2,
            ..
        })
    ));
    assert_eq!(
        restored.poll("reader").unwrap(),
        Some(Packet::PubRel { packet_id: 3 })
    );
    let json = snapshot(&restored);
    assert_eq!(json["schema_version"], 5);
    assert_eq!(json["retained"][0]["topic"], "retained");
    assert_eq!(json["wills"][0]["owner"], "anonymous");
    for record in json["sessions"].as_array().unwrap() {
        assert_eq!(record["owner"], "anonymous");
    }
}

#[test]
fn schema3_requires_valid_matching_owner_and_legacy_schemas_reject_owner_fields() {
    let mut state = state();
    state
        .connect_authenticated("source", false, Some(will("status")), owner("alice"))
        .unwrap();
    let json = snapshot(&state);
    for key in ["sessions", "wills"] {
        for bad_owner in [
            serde_json::Value::Null,
            serde_json::json!(""),
            serde_json::json!("bad\u{0}name"),
            serde_json::json!("x".repeat(65536)),
        ] {
            let mut bad = json.clone();
            bad[key][0]["owner"] = bad_owner;
            assert!(SessionEngine::restore(config(), &serde_json::to_vec(&bad).unwrap()).is_err());
        }
        let mut bad = json.clone();
        bad[key][0].as_object_mut().unwrap().remove("owner");
        assert!(SessionEngine::restore(config(), &serde_json::to_vec(&bad).unwrap()).is_err());
    }
    let mut bad = json.clone();
    bad["wills"][0]["owner"] = "user:bob".into();
    assert!(SessionEngine::restore(config(), &serde_json::to_vec(&bad).unwrap()).is_err());
    let mut bad = super::phase2e_tests::legacy_schema3(json);
    bad["schema_version"] = 2.into();
    assert!(SessionEngine::restore(config(), &serde_json::to_vec(&bad).unwrap()).is_err());
    for name in ["".to_owned(), "bad\0name".to_owned(), "x".repeat(65536)] {
        assert!(matches!(
            state.connect_authenticated("new", true, None, Principal { name }),
            Err(EngineError::NotAuthorized)
        ));
    }
}
