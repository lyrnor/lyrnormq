use super::*;
use crate::durable::{Clock, SessionConfig};
use mqtt_auth::{AccessControl, AuthError, ConnectInfo};
use mqtt_protocol::{Frame, ReasonCode};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Debug)]
struct ManualClock(AtomicU64);
impl Clock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
impl ManualClock {
    fn set(&self, value: u64) {
        self.0.store(value, Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct Permit;
impl AccessControl for Permit {
    fn authenticate(&self, _: ConnectInfo<'_>) -> Result<Principal, AuthError> {
        Err(AuthError::BadCredentials)
    }
    fn authorize(&self, _: &Principal, _: Action, resource: &str) -> bool {
        resource != "forbidden"
    }
}
fn config() -> SessionConfig {
    SessionConfig {
        max_sessions: 8,
        max_retained: 4,
        max_subscriptions: 8,
        max_queued: 2,
        max_inflight: 4,
        max_packet_size: 4096,
        max_state_bytes: 65536,
    }
}
fn state() -> (SessionEngine, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock(AtomicU64::new(1000)));
    (
        SessionEngine::new_with_clock(config(), Arc::new(Permit), clock.clone()).unwrap(),
        clock,
    )
}
fn options(clean: bool, expiry: u32, delay: u32) -> ConnectionOptions {
    ConnectionOptions {
        version: ProtocolVersion::V5,
        clean_start: clean,
        session_expiry_interval: expiry,
        receive_maximum: 4,
        maximum_packet_size: u32::MAX,
        will_properties: vec![Property::WillDelayInterval(delay)],
    }
}
fn will(payload: &[u8]) -> Will {
    Will {
        topic: "will/test".into(),
        payload: payload.into(),
        qos: 1,
        retain: false,
    }
}
fn connect_will(e: &mut SessionEngine, clean: bool, expiry: u32, delay: u32) {
    e.connect_with_options(
        "source",
        Some(will(b"old")),
        Principal::anonymous(),
        options(clean, expiry, delay),
    )
    .unwrap();
}
fn resume(e: &mut SessionEngine, clean: bool, expiry: u32) -> Result<ConnectResult, EngineError> {
    let mut o = options(clean, expiry, 0);
    o.will_properties.clear();
    e.connect_with_options("source", None, Principal::anonymous(), o)
}
fn observer(e: &mut SessionEngine) {
    e.connect("observer", false).unwrap();
    e.handle(
        "observer",
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![("will/#".into(), 1)],
        },
    )
    .unwrap();
}
fn payload(e: &mut SessionEngine) -> Option<Vec<u8>> {
    e.poll("observer").unwrap().map(|p| match p {
        Packet::Publish { payload, .. } => payload,
        other => panic!("unexpected {other:?}"),
    })
}

#[test]
fn delayed_will_waits_until_exact_due_and_exposes_due_to_idle_timer() {
    let (mut e, clock) = state();
    observer(&mut e);
    connect_will(&mut e, true, 60, 5);
    e.disconnect_with_reason("source", false).unwrap();
    assert_eq!(e.wills["source"].due_at, Some(6000));
    assert_eq!(e.next_will_deadline(), Some(6000));
    clock.set(5999);
    assert!(!e.retry_wills().unwrap());
    assert_eq!(payload(&mut e), None);
    clock.set(6000);
    assert_eq!(e.next_will_deadline(), Some(6000));
    assert!(e.retry_wills().unwrap());
    assert_eq!(payload(&mut e), Some(b"old".to_vec()));
    assert!(!e.wills.contains_key("source"));
}
#[test]
fn session_expiry_caps_delay_and_zero_expiry_fires_immediately() {
    for (expiry, expected) in [(0, 1000), (2, 3000), (u32::MAX, 6000)] {
        let (mut e, clock) = state();
        observer(&mut e);
        connect_will(&mut e, true, expiry, 5);
        e.disconnect_with_reason("source", false).unwrap();
        if expected == 1000 {
            assert_eq!(payload(&mut e), Some(b"old".to_vec()));
        } else {
            assert_eq!(e.wills["source"].due_at, Some(expected));
            clock.set(expected);
            e.retry_wills().unwrap();
            assert_eq!(payload(&mut e), Some(b"old".to_vec()));
        }
    }
}
#[test]
fn pre_due_same_session_resume_and_active_takeover_cancel_old_will() {
    for disconnected in [false, true] {
        let (mut e, clock) = state();
        observer(&mut e);
        connect_will(&mut e, true, 60, 5);
        if disconnected {
            e.disconnect_with_reason("source", false).unwrap();
            clock.set(2000);
        }
        assert!(resume(&mut e, false, 60).unwrap().session_present);
        assert!(!e.wills.contains_key("source"));
        clock.set(7000);
        e.retry_wills().unwrap();
        assert_eq!(payload(&mut e), None);
    }
}
#[test]
fn clean_start_or_zero_old_session_ends_and_forces_will_on_takeover() {
    for (clean, old_expiry) in [(true, 60), (false, 0)] {
        let (mut e, _) = state();
        observer(&mut e);
        connect_will(&mut e, true, old_expiry, 30);
        assert!(!resume(&mut e, clean, 60).unwrap().session_present);
        assert_eq!(payload(&mut e), Some(b"old".to_vec()));
    }
}
#[test]
fn clean_start_ends_an_offline_delayed_session_early() {
    let (mut e, clock) = state();
    observer(&mut e);
    connect_will(&mut e, true, 60, 30);
    e.disconnect_with_reason("source", false).unwrap();
    clock.set(2000);
    resume(&mut e, true, 60).unwrap();
    assert_eq!(payload(&mut e), Some(b"old".to_vec()));
}
#[test]
fn normal_disconnect_cancels_but_non_graceful_override_zero_accelerates() {
    let (mut e, _) = state();
    observer(&mut e);
    connect_will(&mut e, true, 60, 30);
    e.disconnect_with_options("source", true, Some(0)).unwrap();
    assert!(e.wills.is_empty());
    assert_eq!(payload(&mut e), None);
    connect_will(&mut e, true, 60, 30);
    e.disconnect_with_options("source", false, Some(0)).unwrap();
    assert_eq!(payload(&mut e), Some(b"old".to_vec()));
}
#[test]
fn known_schedule_survives_restarts_and_due_boundary_cannot_cancel() {
    let (mut e, clock) = state();
    observer(&mut e);
    connect_will(&mut e, true, 60, 5);
    e.disconnect_with_reason("source", false).unwrap();
    for now in [2500, 5000] {
        clock.set(now);
        e = SessionEngine::restore_with_clock(
            config(),
            &e.snapshot().unwrap(),
            Arc::new(Permit),
            clock.clone(),
        )
        .unwrap();
        e.recover_wills().unwrap();
        assert_eq!(e.wills["source"].due_at, Some(6000));
    }
    clock.set(6000);
    resume(&mut e, false, 60).unwrap();
    assert!(!e.wills.contains_key("source"));
    e.connect("observer", false).unwrap();
    assert_eq!(payload(&mut e), Some(b"old".to_vec()));
}
#[test]
fn active_crash_starts_delay_once_capped_by_restored_session_deadline() {
    let (mut e, clock) = state();
    observer(&mut e);
    connect_will(&mut e, true, 3, 8);
    clock.set(4000);
    let mut restored = SessionEngine::restore_with_clock(
        config(),
        &e.snapshot().unwrap(),
        Arc::new(Permit),
        clock.clone(),
    )
    .unwrap();
    restored.recover_wills().unwrap();
    assert_eq!(restored.wills["source"].due_at, Some(7000));
    clock.set(5000);
    let mut again = SessionEngine::restore_with_clock(
        config(),
        &restored.snapshot().unwrap(),
        Arc::new(Permit),
        clock.clone(),
    )
    .unwrap();
    again.recover_wills().unwrap();
    assert_eq!(again.wills["source"].due_at, Some(7000));
    clock.set(7000);
    again.retry_wills().unwrap();
    again.connect("observer", false).unwrap();
    assert_eq!(payload(&mut again), Some(b"old".to_vec()));
}
#[test]
fn capacity_block_marker_is_a_change_and_retries_on_progress_without_due_reset() {
    let (mut e, clock) = state();
    observer(&mut e);
    connect_will(&mut e, true, 60, 0);
    let queue = &mut e.sessions.get_mut("observer").unwrap().queued;
    for _ in 0..2 {
        queue.push_back(Message {
            shared_subscription: None,
            retained_replay: false,
            topic: "will/fill".into(),
            payload: b"fill".to_vec(),
            qos: 0,
            retain: false,
            properties: vec![],
            expires_at: None,
        });
    }
    e.wills.get_mut("source").unwrap().pending = true;
    e.wills.get_mut("source").unwrap().due_at = Some(1000);
    assert!(e.retry_wills().unwrap());
    assert!(e.wills["source"].timer_blocked);
    assert_eq!(e.next_will_deadline(), None);
    assert!(!e.retry_wills().unwrap());
    clock.set(2000);
    resume(&mut e, false, 60).unwrap();
    e.disconnect_with_reason("source", false).unwrap();
    assert_eq!(e.wills["source"].due_at, Some(1000));
    assert_eq!(payload(&mut e), Some(b"fill".to_vec()));
    assert!(e.retry_wills().unwrap());
    assert!(!e.wills.contains_key("source"));
}
#[test]
fn rejected_owner_or_forbidden_new_will_cannot_cancel_delayed_intent() {
    let (mut e, _) = state();
    observer(&mut e);
    connect_will(&mut e, true, 60, 10);
    e.disconnect_with_reason("source", false).unwrap();
    let before = e.snapshot().unwrap();
    assert!(matches!(
        e.connect_with_options(
            "source",
            None,
            Principal {
                name: "other".into()
            },
            {
                let mut o = options(false, 60, 0);
                o.will_properties.clear();
                o
            }
        ),
        Err(EngineError::NotAuthorized)
    ));
    assert_eq!(before, e.snapshot().unwrap());
    let mut forbidden = will(b"new");
    forbidden.topic = "forbidden".into();
    assert!(matches!(
        e.connect_with_options(
            "source",
            Some(forbidden),
            Principal::anonymous(),
            options(false, 60, 10)
        ),
        Err(EngineError::NotAuthorized)
    ));
    assert_eq!(before, e.snapshot().unwrap());
}
#[test]
fn delayed_will_can_be_replaced_on_cancelled_resume_without_old_publication() {
    let (mut e, clock) = state();
    observer(&mut e);
    connect_will(&mut e, true, 60, 10);
    e.disconnect_with_reason("source", false).unwrap();
    clock.set(2000);
    e.connect_with_options(
        "source",
        Some(will(b"new")),
        Principal::anonymous(),
        options(false, 60, 3),
    )
    .unwrap();
    e.disconnect_with_reason("source", false).unwrap();
    assert_eq!(e.wills["source"].due_at, Some(5000));
    clock.set(5000);
    e.retry_wills().unwrap();
    assert_eq!(payload(&mut e), Some(b"new".to_vec()));
    assert_eq!(payload(&mut e), None);
}

#[test]
fn expired_original_session_is_not_cancellable_by_same_id_resume() {
    let (mut e, clock) = state();
    observer(&mut e);
    connect_will(&mut e, true, 2, 20);
    e.disconnect_with_reason("source", false).unwrap();
    clock.set(3000);
    assert!(!resume(&mut e, false, 60).unwrap().session_present);
    assert_eq!(payload(&mut e), Some(b"old".to_vec()));
}
#[test]
fn will_delay_uint_max_is_finite_and_normal_disconnect_cancels_it() {
    let (mut e, _) = state();
    connect_will(&mut e, true, u32::MAX, u32::MAX);
    e.disconnect_with_reason("source", false).unwrap();
    assert_eq!(
        e.wills["source"].due_at,
        Some(1000 + u64::from(u32::MAX) * 1000)
    );
    resume(&mut e, false, u32::MAX).unwrap();
    assert!(e.wills.is_empty());
}
#[test]
fn nonzero_client_disconnect_reason_schedules_will_and_obeys_delay() {
    for reason in [
        ReasonCode::UNSPECIFIED_ERROR,
        ReasonCode::DISCONNECT_WITH_WILL_MESSAGE,
    ] {
        let (mut e, clock) = state();
        observer(&mut e);
        connect_will(&mut e, true, 60, 3);
        let mut disconnect = Frame::v5(Packet::Disconnect);
        disconnect.reason_code = reason;
        e.handle_frame("source", disconnect).unwrap();
        assert_eq!(e.wills["source"].due_at, Some(4000));
        assert_eq!(payload(&mut e), None);
        clock.set(4000);
        e.retry_wills().unwrap();
        assert_eq!(payload(&mut e), Some(b"old".to_vec()));
    }
}
fn timed_options(delay: u32, ttl: u32) -> ConnectionOptions {
    let mut o = options(true, 60, delay);
    o.will_properties.push(Property::MessageExpiryInterval(ttl));
    o
}
#[test]
fn will_message_ttl_starts_at_due_publication_not_at_connect_or_disconnect() {
    let (mut e, clock) = state();
    observer(&mut e);
    e.connect_with_options(
        "source",
        Some(will(b"timed")),
        Principal::anonymous(),
        timed_options(5, 2),
    )
    .unwrap();
    e.disconnect_with_reason("source", false).unwrap();
    clock.set(5900);
    e = SessionEngine::restore_with_clock(
        config(),
        &e.snapshot().unwrap(),
        Arc::new(Permit),
        clock.clone(),
    )
    .unwrap();
    e.recover_wills().unwrap();
    assert_eq!(e.wills["source"].due_at, Some(6000));
    clock.set(6000);
    e.retry_wills().unwrap();
    assert_eq!(
        e.sessions["observer"].queued.front().unwrap().expires_at,
        Some(8000)
    );
    clock.set(7001);
    let mut o = options(false, u32::MAX, 0);
    o.will_properties.clear();
    e.connect_with_options("observer", None, Principal::anonymous(), o)
        .unwrap();
    let f = e
        .poll_frame_with_changes("observer")
        .unwrap()
        .frame
        .unwrap();
    assert!(f.properties.contains(&Property::MessageExpiryInterval(1)));
    assert!(
        !f.properties
            .iter()
            .any(|p| matches!(p, Property::WillDelayInterval(_)))
    );
}
#[test]
fn capacity_defer_does_not_start_will_message_ttl() {
    let (mut e, clock) = state();
    observer(&mut e);
    for _ in 0..2 {
        e.sessions
            .get_mut("observer")
            .unwrap()
            .queued
            .push_back(Message {
                shared_subscription: None,
                retained_replay: false,
                topic: "will/fill".into(),
                payload: b"fill".to_vec(),
                qos: 0,
                retain: false,
                properties: vec![],
                expires_at: None,
            });
    }
    e.connect_with_options(
        "source",
        Some(will(b"timed")),
        Principal::anonymous(),
        timed_options(2, 1),
    )
    .unwrap();
    e.disconnect_with_reason("source", false).unwrap();
    clock.set(3000);
    assert!(e.retry_wills().unwrap());
    assert!(e.wills["source"].timer_blocked);
    clock.set(10000);
    assert!(!e.retry_wills().unwrap());
    assert!(e.wills.contains_key("source"));
    assert_eq!(payload(&mut e), Some(b"fill".to_vec()));
    e.retry_wills().unwrap();
    assert_eq!(
        e.sessions["observer"].queued.back().unwrap().expires_at,
        Some(11000)
    );
}
#[test]
fn zero_ttl_retained_will_replaces_old_value_without_forwarding_expired_payload() {
    let (mut e, clock) = state();
    observer(&mut e);
    e.connect("publisher", true).unwrap();
    e.handle(
        "publisher",
        Packet::Publish {
            topic: "will/test".into(),
            payload: b"prior".to_vec(),
            qos: 0,
            packet_id: None,
            dup: false,
            retain: true,
        },
    )
    .unwrap();
    assert_eq!(payload(&mut e), Some(b"prior".to_vec()));
    let mut w = will(b"expired");
    w.retain = true;
    e.connect_with_options(
        "source",
        Some(w),
        Principal::anonymous(),
        timed_options(2, 0),
    )
    .unwrap();
    e.disconnect_with_reason("source", false).unwrap();
    clock.set(3000);
    e.retry_wills().unwrap();
    assert!(e.retained.is_empty());
    assert_eq!(payload(&mut e), None);
    assert!(!e.wills.contains_key("source"));
}

fn fill_source_self_queue(e: &mut SessionEngine, topic: &str) {
    e.handle(
        "source",
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![("#".into(), 1)],
        },
    )
    .unwrap();
    e.connect("filler", true).unwrap();
    for id in 1..=2 {
        e.handle(
            "filler",
            Packet::Publish {
                topic: topic.into(),
                payload: b"fill".to_vec(),
                qos: 1,
                packet_id: Some(id),
                dup: false,
                retain: false,
            },
        )
        .unwrap();
    }
    assert_eq!(e.sessions["source"].queued.len(), 2);
}

#[test]
fn clean_start_drops_full_old_self_queue_before_forced_will_fanout() {
    for offline in [false, true] {
        for replacement in [false, true] {
            let (mut e, _) = state();
            observer(&mut e);
            connect_will(&mut e, true, 60, 30);
            fill_source_self_queue(&mut e, "fill");
            if offline {
                e.disconnect_with_reason("source", false).unwrap();
            }
            let mut o = options(true, 60, 30);
            let new_will = replacement.then(|| will(b"new"));
            if !replacement {
                o.will_properties.clear();
            }
            let connected = e
                .connect_with_options("source", new_will, Principal::anonymous(), o)
                .unwrap();
            assert!(!connected.session_present);
            assert!(e.sessions["source"].queued.is_empty());
            assert!(e.sessions["source"].subscriptions.is_empty());
            assert!(e.sessions["source"].outgoing.pending.is_empty());
            assert_eq!(payload(&mut e), Some(b"old".to_vec()));
            assert_eq!(payload(&mut e), None);
            assert!(!e.retry_wills().unwrap());
            assert_eq!(e.wills.contains_key("source"), replacement);
            if replacement {
                assert_eq!(e.wills["source"].will.payload, b"new");
            }
        }
    }
}

#[test]
fn clean_start_preserves_real_external_capacity_deferral() {
    for replacement in [false, true] {
        let (mut e, _) = state();
        observer(&mut e);
        connect_will(&mut e, true, 60, 30);
        fill_source_self_queue(&mut e, "will/fill");
        let mut o = options(true, 60, 30);
        if !replacement {
            o.will_properties.clear();
        }
        let result = e.connect_with_options(
            "source",
            replacement.then(|| will(b"new")),
            Principal::anonymous(),
            o,
        );
        if replacement {
            assert!(matches!(result, Err(EngineError::Capacity)));
        } else {
            assert!(!result.unwrap().session_present);
        }
        assert!(e.wills["source"].pending);
        assert!(e.wills["source"].timer_blocked);
        assert_eq!(e.wills["source"].will.payload, b"old");
        assert_eq!(payload(&mut e), Some(b"fill".to_vec()));
        assert!(e.retry_wills().unwrap());
        assert_eq!(payload(&mut e), Some(b"fill".to_vec()));
        assert_eq!(payload(&mut e), Some(b"old".to_vec()));
        assert_eq!(payload(&mut e), None);
        assert!(!e.wills.contains_key("source"));
    }
}

#[test]
fn zero_expiry_takeover_drops_full_old_self_queue_before_will_fanout() {
    for clean in [false, true] {
        for replacement in [false, true] {
            let (mut e, _) = state();
            observer(&mut e);
            connect_will(&mut e, true, 0, 30);
            fill_source_self_queue(&mut e, "fill");
            let mut o = options(clean, 60, 30);
            if !replacement {
                o.will_properties.clear();
            }
            assert!(
                !e.connect_with_options(
                    "source",
                    replacement.then(|| will(b"new")),
                    Principal::anonymous(),
                    o
                )
                .unwrap()
                .session_present
            );
            assert!(e.sessions["source"].subscriptions.is_empty());
            assert!(e.sessions["source"].queued.is_empty());
            assert_eq!(payload(&mut e), Some(b"old".to_vec()));
            assert_eq!(payload(&mut e), None);
            assert_eq!(e.wills.contains_key("source"), replacement);
        }
    }
}

#[test]
fn rollback_recovery_anchors_active_will_to_persisted_effective_clock() {
    let (mut e, clock) = state();
    observer(&mut e);
    connect_will(&mut e, true, 60, 3);
    clock.set(5000);
    e.expire_sessions().unwrap();
    let snapshot = e.snapshot().unwrap();
    clock.set(2000);
    let mut restored =
        SessionEngine::restore_with_clock(config(), &snapshot, Arc::new(Permit), clock.clone())
            .unwrap();
    restored.recover_wills().unwrap();
    assert_eq!(restored.wills["source"].due_at, Some(8000));
    clock.set(7000);
    assert!(!restored.retry_wills().unwrap());
    clock.set(8000);
    assert!(restored.retry_wills().unwrap());
    assert!(!restored.wills.contains_key("source"));
}

#[test]
fn rollback_cannot_cancel_an_already_observed_due_will_on_resume() {
    let (mut e, clock) = state();
    observer(&mut e);
    connect_will(&mut e, true, 60, 3);
    e.disconnect_with_reason("source", false).unwrap();
    clock.set(4000);
    e.expire_sessions().unwrap();
    clock.set(2000);
    resume(&mut e, false, 60).unwrap();
    assert_eq!(payload(&mut e), Some(b"old".to_vec()));
    assert!(!e.wills.contains_key("source"));
}
