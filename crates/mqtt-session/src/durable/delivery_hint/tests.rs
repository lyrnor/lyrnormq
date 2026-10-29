use super::super::*;

fn engine() -> SessionEngine {
    let mut engine = SessionEngine::new(SessionConfig {
        max_sessions: 4,
        max_retained: 4,
        max_subscriptions: 4,
        max_queued: 4,
        max_inflight: 4,
        max_packet_size: 4096,
        max_state_bytes: 65536,
    })
    .unwrap();
    engine.connect("client", false).unwrap();
    engine
}
fn message() -> Message {
    Message {
        topic: "hint/value".into(),
        payload: b"pending".to_vec(),
        qos: 1,
        retain: false,
        retained_replay: false,
        shared_subscription: None,
        expires_at: None,
        properties: Vec::new(),
    }
}

#[test]
fn hint_is_exactly_online_and_queue_or_replay_not_outgoing() {
    let mut e = engine();
    assert!(!e.delivery_may_be_ready("missing"));
    assert!(!e.delivery_may_be_ready("client"));
    let id = identifier(1).unwrap();
    e.sessions
        .get_mut("client")
        .unwrap()
        .outgoing
        .pending
        .insert(id, OutgoingState::Released);
    assert!(
        !e.delivery_may_be_ready("client"),
        "an ACK-waiting exchange alone is not replay"
    );
    e.sessions
        .get_mut("client")
        .unwrap()
        .queued
        .push_back(message());
    assert!(e.delivery_may_be_ready("client"));
    e.sessions.get_mut("client").unwrap().online = false;
    assert!(!e.delivery_may_be_ready("client"));
    let s = e.sessions.get_mut("client").unwrap();
    s.queued.clear();
    s.replay.push_back(id);
    assert!(!e.delivery_may_be_ready("client"));
    e.sessions.get_mut("client").unwrap().online = true;
    assert!(e.delivery_may_be_ready("client"));
}

#[derive(Debug)]
struct UnobservedClock;
impl Clock for UnobservedClock {
    fn now_millis(&self) -> u64 {
        panic!("hint must not observe time")
    }
}
#[derive(Debug)]
struct UnobservedPolicy;
impl AccessControl for UnobservedPolicy {
    fn authenticate(
        &self,
        _: mqtt_auth::ConnectInfo<'_>,
    ) -> Result<Principal, mqtt_auth::AuthError> {
        panic!("hint must not authenticate")
    }
    fn authorize(&self, _: &Principal, _: Action, _: &str) -> bool {
        panic!("hint must not authorize")
    }
}

#[test]
fn expired_denied_and_oversized_queue_stays_conservatively_true_without_effects() {
    let mut e = engine();
    let mut pending = message();
    pending.expires_at = Some(0);
    pending.properties.push(Property::MessageExpiryInterval(0));
    pending.payload = vec![0; 8192];
    e.sessions
        .get_mut("client")
        .unwrap()
        .queued
        .push_back(pending);
    e.clock = Arc::new(UnobservedClock);
    e.access = Arc::new(UnobservedPolicy);
    let floor = e.clock_floor.get();
    for _ in 0..2 {
        assert!(e.delivery_may_be_ready("client"));
    }
    assert_eq!(e.clock_floor.get(), floor);
    let s = e.sessions.get("client").unwrap();
    assert_eq!(s.queued.len(), 1);
    assert_eq!(s.queued[0].payload.len(), 8192);
    assert_eq!(s.queued[0].expires_at, Some(0));
    assert!(s.replay.is_empty());
}

#[test]
fn full_receive_window_does_not_hide_released_pubrel_replay() {
    let mut e = engine();
    let sent = identifier(1).unwrap();
    let released = identifier(2).unwrap();
    let s = e.sessions.get_mut("client").unwrap();
    s.receive_maximum = 1;
    s.sent.insert(sent);
    s.replay.extend([sent, released]);
    s.outgoing.pending.insert(
        sent,
        OutgoingState::AwaitingAck {
            _message: Vec::new(),
        },
    );
    s.outgoing.pending.insert(released, OutgoingState::Released);
    assert!(e.delivery_may_be_ready("client"));
    // A conservative hint deliberately does not inspect even replay validity.
    let s = e.sessions.get_mut("client").unwrap();
    s.replay.retain(|id| *id != released);
    assert!(e.delivery_may_be_ready("client"));
}
