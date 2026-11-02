use super::*;
use crate::durable::{Clock, ConnectionOptions, SessionConfig};
use mqtt_auth::{AccessControl, AuthError, ConnectInfo};
use mqtt_protocol::{Frame, Packet, Property, ProtocolVersion, Will};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Default)]
struct PointPolicy {
    calls: Mutex<Vec<(String, Action, String)>>,
    denied: Mutex<Vec<(String, Action, String)>>,
}
impl AccessControl for PointPolicy {
    fn authenticate(&self, _: ConnectInfo<'_>) -> Result<Principal, AuthError> {
        Err(AuthError::BadCredentials)
    }
    fn authorize(&self, _: &Principal, _: Action, _: &str) -> bool {
        panic!("context-free authorization must not be used")
    }
    fn authorize_for_client(
        &self,
        owner: &Principal,
        client: &str,
        action: Action,
        resource: &str,
    ) -> bool {
        assert_eq!(owner.name, format!("user:{client}"));
        self.calls
            .lock()
            .unwrap()
            .push((client.into(), action, resource.into()));
        !self
            .denied
            .lock()
            .unwrap()
            .contains(&(client.into(), action, resource.into()))
    }
    fn reconciles_stored_authorization(&self) -> bool {
        false
    }
}
fn config() -> SessionConfig {
    SessionConfig {
        max_sessions: 16,
        max_retained: 16,
        max_subscriptions: 16,
        max_queued: 16,
        max_inflight: 16,
        max_packet_size: 4096,
        max_state_bytes: 128 * 1024,
    }
}
fn connect(engine: &mut SessionEngine, id: &str, will: Option<Will>) {
    engine
        .connect_with_options(
            id,
            will,
            Principal {
                name: format!("user:{id}"),
            },
            ConnectionOptions {
                version: ProtocolVersion::V5,
                clean_start: false,
                session_expiry_interval: u32::MAX,
                receive_maximum: u16::MAX,
                maximum_packet_size: u32::MAX,
                will_properties: Vec::new(),
            },
        )
        .unwrap();
}
fn subscribe(engine: &mut SessionEngine, id: &str, filter: &str) {
    engine
        .handle_frame(
            id,
            Frame::v5(Packet::Subscribe {
                packet_id: 1,
                filters: vec![(filter.into(), 1)],
            }),
        )
        .unwrap();
}
fn publish(engine: &mut SessionEngine, topic: &str, retain: bool) {
    engine
        .handle_frame(
            "source",
            Frame::v5(Packet::Publish {
                topic: topic.into(),
                payload: b"body".to_vec(),
                qos: 1,
                packet_id: Some(9),
                dup: false,
                retain,
            }),
        )
        .unwrap();
}
#[test]
fn ordinary_enqueue_is_quarantined_then_only_the_attempted_denied_record_retires() {
    let policy = Arc::new(PointPolicy::default());
    let mut engine = SessionEngine::new_with_access(config(), policy.clone()).unwrap();
    connect(&mut engine, "source", None);
    connect(&mut engine, "target", None);
    subscribe(&mut engine, "target", "sensor/#");
    policy
        .denied
        .lock()
        .unwrap()
        .push(("target".into(), Action::Receive, "sensor/first".into()));
    policy.calls.lock().unwrap().clear();
    publish(&mut engine, "sensor/first", false);
    publish(&mut engine, "sensor/second", false);
    assert_eq!(engine.sessions["target"].queued.len(), 2);
    assert!(
        !policy
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|(_, action, _)| *action == Action::Receive)
    );
    assert!(
        engine
            .poll_delivery_with_changes("target")
            .unwrap()
            .delivery
            .is_none()
    );
    assert_eq!(engine.sessions["target"].queued.len(), 1);
    assert_eq!(engine.sessions["target"].subscriptions.len(), 1);
    let delivery = engine
        .poll_delivery_with_changes("target")
        .unwrap()
        .delivery
        .unwrap();
    assert!(
        matches!(delivery.frame.packet,Packet::Publish{ref topic,..} if topic=="sensor/second")
    );
    assert_eq!(engine.principal("target").unwrap().name, "user:target");
}
#[test]
fn point_policy_does_not_sweep_other_topics_or_prequery_retained_delivery() {
    let policy = Arc::new(PointPolicy::default());
    let mut engine = SessionEngine::new_with_access(config(), policy.clone()).unwrap();
    connect(
        &mut engine,
        "source",
        Some(Will {
            topic: "will/source".into(),
            payload: vec![1],
            qos: 1,
            retain: false,
        }),
    );
    connect(&mut engine, "target", None);
    publish(&mut engine, "retained/one", true);
    policy.calls.lock().unwrap().clear();
    subscribe(&mut engine, "target", "retained/#");
    assert_eq!(engine.sessions["target"].queued.len(), 1);
    assert!(
        !policy
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|(_, action, _)| *action == Action::Receive)
    );
    policy.calls.lock().unwrap().clear();
    assert!(!engine.reconcile_authorization().unwrap());
    assert!(policy.calls.lock().unwrap().is_empty());
    assert_eq!(engine.wills.len(), 1);
    let delivery = engine
        .poll_delivery_with_changes("target")
        .unwrap()
        .delivery
        .unwrap();
    let packet_id = match delivery.frame.packet {
        Packet::Publish {
            packet_id: Some(id),
            ..
        } => id,
        _ => panic!("expected publication"),
    };
    engine.begin_delivery("target", delivery).unwrap();
    policy.calls.lock().unwrap().clear();
    engine
        .handle_frame("target", Frame::v5(Packet::PubAck { packet_id }))
        .unwrap();
    assert!(
        policy.calls.lock().unwrap().is_empty(),
        "ACK must not refresh unrelated ACL or Will"
    );
}
#[test]
fn shared_selection_still_skips_a_denied_member() {
    let policy = Arc::new(PointPolicy::default());
    let mut engine = SessionEngine::new_with_access(config(), policy.clone()).unwrap();
    connect(&mut engine, "source", None);
    connect(&mut engine, "a", None);
    connect(&mut engine, "b", None);
    subscribe(&mut engine, "a", "$share/group/sensor/#");
    subscribe(&mut engine, "b", "$share/group/sensor/#");
    policy
        .denied
        .lock()
        .unwrap()
        .push(("a".into(), Action::Receive, "sensor/one".into()));
    publish(&mut engine, "sensor/one", false);
    assert_eq!(engine.sessions["a"].queued.len(), 0);
    assert_eq!(engine.sessions["b"].queued.len(), 1);
}
#[derive(Debug)]
struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}
#[test]
fn recovery_anchor_is_once_and_only_due_will_checks_remote_publish() {
    let policy = Arc::new(PointPolicy::default());
    let clock = Arc::new(TestClock(AtomicU64::new(1_000)));
    let mut engine =
        SessionEngine::new_with_clock(config(), policy.clone(), clock.clone()).unwrap();
    engine
        .connect_with_options(
            "source",
            Some(Will {
                topic: "will/source".into(),
                payload: vec![1],
                qos: 1,
                retain: false,
            }),
            Principal {
                name: "user:source".into(),
            },
            ConnectionOptions {
                version: ProtocolVersion::V5,
                clean_start: false,
                session_expiry_interval: u32::MAX,
                receive_maximum: u16::MAX,
                maximum_packet_size: u32::MAX,
                will_properties: vec![Property::WillDelayInterval(10)],
            },
        )
        .unwrap();
    engine.anchor_recovered_wills();
    let due = engine.wills["source"].due_at;
    clock.0.store(5_000, Ordering::Relaxed);
    engine.anchor_recovered_wills();
    assert_eq!(engine.wills["source"].due_at, due);
    policy.calls.lock().unwrap().clear();
    engine.retry_wills().unwrap();
    assert!(policy.calls.lock().unwrap().is_empty());
    policy
        .denied
        .lock()
        .unwrap()
        .push(("source".into(), Action::Publish, "will/source".into()));
    clock.0.store(11_000, Ordering::Relaxed);
    engine.retry_wills().unwrap();
    assert!(engine.wills.is_empty());
    assert_eq!(
        *policy.calls.lock().unwrap(),
        vec![
            ("source".into(), Action::Connect, "source".into()),
            ("source".into(), Action::Publish, "will/source".into())
        ]
    );
}

#[test]
fn due_will_cannot_bypass_a_revoked_local_connect_permission() {
    let policy = Arc::new(PointPolicy::default());
    let mut engine = SessionEngine::new_with_access(config(), policy.clone()).unwrap();
    connect(
        &mut engine,
        "source",
        Some(Will {
            topic: "will/private".into(),
            payload: vec![1],
            qos: 1,
            retain: true,
        }),
    );
    engine.anchor_recovered_wills();
    policy
        .denied
        .lock()
        .unwrap()
        .push(("source".into(), Action::Connect, "source".into()));
    policy.calls.lock().unwrap().clear();
    engine.retry_wills().unwrap();
    assert!(
        engine.retained_entries().is_empty(),
        "revoked owner must not publish retained Will"
    );
    assert!(engine.wills.is_empty());
    assert_eq!(
        *policy.calls.lock().unwrap(),
        vec![("source".into(), Action::Connect, "source".into())]
    );
}
