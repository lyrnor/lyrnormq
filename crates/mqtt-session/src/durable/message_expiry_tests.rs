use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
#[derive(Debug)]
struct Manual(AtomicU64);
impl Clock for Manual {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
impl Manual {
    fn set(&self, now: u64) {
        self.0.store(now, Ordering::SeqCst)
    }
}
fn config() -> SessionConfig {
    SessionConfig {
        max_sessions: 8,
        max_retained: 8,
        max_subscriptions: 8,
        max_queued: 8,
        max_inflight: 8,
        max_packet_size: 1024,
        max_state_bytes: 32768,
    }
}
fn state() -> (SessionEngine, Arc<Manual>) {
    let clock = Arc::new(Manual(AtomicU64::new(1000)));
    let mut e =
        SessionEngine::new_with_clock(config(), Arc::new(AllowAnonymous), clock.clone()).unwrap();
    connect(&mut e, "pub", true);
    connect(&mut e, "sub", true);
    (e, clock)
}
fn connect(e: &mut SessionEngine, id: &str, clean: bool) {
    e.connect_with_options(
        id,
        None,
        Principal::anonymous(),
        ConnectionOptions {
            version: ProtocolVersion::V5,
            clean_start: clean,
            session_expiry_interval: 60,
            receive_maximum: 1,
            will_properties: vec![],
        },
    )
    .unwrap();
}
fn subscribe(e: &mut SessionEngine, filter: &str) {
    e.handle_frame(
        "sub",
        Frame::v5(Packet::Subscribe {
            packet_id: 1,
            filters: vec![(filter.into(), 2)],
        }),
    )
    .unwrap();
}
fn publication(qos: u8, id: u16, ttl: Option<u32>, retain: bool) -> Frame {
    let mut f = Frame::v5(Packet::Publish {
        topic: "a".into(),
        payload: vec![id as u8],
        qos,
        packet_id: (qos != 0).then_some(id),
        dup: false,
        retain,
    });
    if let Some(ttl) = ttl {
        f.properties.push(Property::MessageExpiryInterval(ttl));
    }
    f
}
fn publish(e: &mut SessionEngine, qos: u8, id: u16, ttl: Option<u32>, retain: bool) {
    e.handle_frame("pub", publication(qos, id, ttl, retain))
        .unwrap();
}
#[test]
fn message_expiry_offline_queue_and_retained_share_original_deadline() {
    let (mut e, c) = state();
    subscribe(&mut e, "a");
    e.disconnect("sub").unwrap();
    publish(&mut e, 1, 1, Some(2), true);
    assert_eq!(e.next_expiry_deadline(), Some(3000));
    c.set(2999);
    connect(&mut e, "sub", false);
    let f = e.poll_frame_with_changes("sub").unwrap().frame.unwrap();
    assert!(f.properties.contains(&Property::MessageExpiryInterval(1)));
    c.set(3000);
    assert!(e.expire_sessions().unwrap());
    assert!(e.retained.is_empty());
    assert_eq!(e.sessions["sub"].outgoing.pending.len(), 1);
}
#[test]
fn message_expiry_zero_removes_retained_predecessor_without_fanout() {
    let (mut e, _) = state();
    publish(&mut e, 0, 1, None, true);
    subscribe(&mut e, "a");
    assert!(e.poll_frame_with_changes("sub").unwrap().frame.is_some());
    publish(&mut e, 1, 2, Some(0), true);
    assert!(e.retained.is_empty());
    assert!(e.poll_frame_with_changes("sub").unwrap().frame.is_none());
}
#[test]
fn message_expiry_qos2_duplicate_never_renews_and_expired_pubrel_completes() {
    let (mut e, c) = state();
    subscribe(&mut e, "a");
    publish(&mut e, 2, 1, Some(1), true);
    c.set(1500);
    let mut duplicate = publication(2, 1, Some(100), true);
    if let Packet::Publish { dup, .. } = &mut duplicate.packet {
        *dup = true;
    }
    e.handle_frame("pub", duplicate).unwrap();
    c.set(2000);
    let reply = e
        .handle_frame("pub", Frame::v5(Packet::PubRel { packet_id: 1 }))
        .unwrap();
    assert!(matches!(reply[0].packet, Packet::PubComp { packet_id: 1 }));
    assert!(e.poll_frame_with_changes("sub").unwrap().frame.is_none());
    assert!(e.retained.is_empty());
    assert!(e.sessions["pub"].incoming_expiry.is_empty());
}
#[test]
fn message_expiry_restored_deadlines_do_not_restart() {
    let (mut e, c) = state();
    subscribe(&mut e, "a");
    e.disconnect("sub").unwrap();
    publish(&mut e, 1, 1, Some(2), true);
    let bytes = e.snapshot().unwrap();
    c.set(3001);
    let mut r =
        SessionEngine::restore_with_clock(config(), &bytes, Arc::new(AllowAnonymous), c).unwrap();
    assert!(r.expire_sessions().unwrap());
    connect(&mut r, "sub", false);
    assert!(r.poll_frame_with_changes("sub").unwrap().frame.is_none());
    assert!(r.retained.is_empty());
}
fn release_source(e: &mut SessionEngine, qos: u8, id: u16) {
    if qos == 2 {
        e.handle_frame("pub", Frame::v5(Packet::PubRel { packet_id: id }))
            .unwrap();
    }
}
#[test]
fn delivery_expiry_before_begin_retires_reserved_id_ticket_and_quota() {
    for qos in [1, 2] {
        let (mut e, c) = state();
        subscribe(&mut e, "a");
        publish(&mut e, qos, 1, Some(1), false);
        release_source(&mut e, qos, 1);
        let d = e
            .poll_delivery_with_changes("sub")
            .unwrap()
            .delivery
            .unwrap();
        let id = identifier(match d.frame.packet {
            Packet::Publish {
                packet_id: Some(id),
                ..
            } => id,
            _ => panic!(),
        })
        .unwrap();
        assert!(!e.sessions["sub"].outgoing_started.contains(&id));
        assert!(e.snapshot().is_ok());
        c.set(2000);
        let result = e.begin_delivery("sub", d).unwrap();
        assert!(result.frame.is_none() && result.changed);
        let s = &e.sessions["sub"];
        assert!(
            s.outgoing.pending.is_empty()
                && s.outgoing_expiry.is_empty()
                && s.delivery_tickets.is_empty()
                && s.sent.is_empty()
        );
        assert_eq!(e.next_expiry_deadline(), None);
    }
}
#[test]
fn delivery_started_qos_retries_survive_expiry_and_publish_properties_reach_zero() {
    for qos in [1, 2] {
        let (mut e, c) = state();
        subscribe(&mut e, "a");
        publish(&mut e, qos, 1, Some(1), false);
        release_source(&mut e, qos, 1);
        let first = e.poll_frame_with_changes("sub").unwrap().frame.unwrap();
        let id = match first.packet {
            Packet::Publish {
                packet_id: Some(id),
                ..
            } => id,
            _ => panic!(),
        };
        c.set(2001);
        e.expire_sessions().unwrap();
        assert_eq!(e.sessions["sub"].outgoing.pending.len(), 1);
        let bytes = e.snapshot().unwrap();
        let mut r =
            SessionEngine::restore_with_clock(config(), &bytes, Arc::new(AllowAnonymous), c)
                .unwrap();
        connect(&mut r, "sub", false);
        let f = r.poll_frame_with_changes("sub").unwrap().frame.unwrap();
        assert!(
            matches!(f.packet,Packet::Publish{packet_id:Some(actual),dup:true,..} if actual==id)
        );
        assert!(f.properties.contains(&Property::MessageExpiryInterval(0)));
        if qos == 2 {
            r.handle_frame("sub", Frame::v5(Packet::PubRec { packet_id: id }))
                .unwrap();
            assert!(r.sessions["sub"].outgoing_expiry.is_empty());
            assert!(r.snapshot().is_ok());
        }
    }
}
#[test]
fn delivery_stale_retired_ticket_cannot_start_a_later_reservation() {
    let (mut e, _) = state();
    subscribe(&mut e, "a");
    publish(&mut e, 1, 1, Some(10), false);
    let old = e
        .poll_delivery_with_changes("sub")
        .unwrap()
        .delivery
        .unwrap();
    let id = match old.frame.packet {
        Packet::Publish {
            packet_id: Some(id),
            ..
        } => id,
        _ => panic!(),
    };
    e.handle_frame("sub", Frame::v5(Packet::PubAck { packet_id: id }))
        .unwrap();
    publish(&mut e, 1, 2, Some(10), false);
    let new = e
        .poll_delivery_with_changes("sub")
        .unwrap()
        .delivery
        .unwrap();
    assert_ne!(old.ticket, new.ticket);
    assert!(e.begin_delivery("sub", old).unwrap().frame.is_none());
    assert!(e.begin_delivery("sub", new).unwrap().frame.is_some());
}
#[test]
fn delivery_qos0_and_cross_version_waiters_expire_before_first_write() {
    for legacy in [false, true] {
        let (mut e, c) = state();
        if legacy {
            e.connect("sub", false).unwrap();
        }
        subscribe_for_version(&mut e, legacy);
        publish(&mut e, 0, 1, Some(1), false);
        let d = e
            .poll_delivery_with_changes("sub")
            .unwrap()
            .delivery
            .unwrap();
        assert!(d.ticket.is_none());
        if legacy {
            assert!(d.frame.properties.is_empty());
        }
        c.set(2000);
        assert!(e.begin_delivery("sub", d).unwrap().frame.is_none());
    }
}
fn subscribe_for_version(e: &mut SessionEngine, legacy: bool) {
    let p = Packet::Subscribe {
        packet_id: 1,
        filters: vec![("a".into(), 2)],
    };
    if legacy {
        e.handle("sub", p).unwrap();
    } else {
        e.handle_frame("sub", Frame::v5(p)).unwrap();
    }
}
#[test]
fn delivery_refresh_after_start_commit_decreases_without_cancelling_started_qos() {
    let (mut e, c) = state();
    subscribe(&mut e, "a");
    publish(&mut e, 1, 1, Some(2), false);
    let d = e
        .poll_delivery_with_changes("sub")
        .unwrap()
        .delivery
        .unwrap();
    let mut f = e.begin_delivery("sub", d.clone()).unwrap().frame.unwrap();
    c.set(2500);
    d.refresh_remaining(&mut f, c.now_millis());
    assert!(f.properties.contains(&Property::MessageExpiryInterval(1)));
    c.set(3001);
    d.refresh_remaining(&mut f, c.now_millis());
    assert!(f.properties.contains(&Property::MessageExpiryInterval(0)));
    c.set(1000);
    d.refresh_remaining(&mut f, c.now_millis());
    assert!(f.properties.contains(&Property::MessageExpiryInterval(0)));
    assert_eq!(e.sessions["sub"].outgoing.pending.len(), 1);
}
#[test]
fn retained_replay_inherits_remaining_lifetime_and_replacement_clears_deadline() {
    let (mut e, c) = state();
    publish(&mut e, 0, 1, Some(3), true);
    c.set(2500);
    subscribe(&mut e, "a");
    let f = e.poll_frame_with_changes("sub").unwrap().frame.unwrap();
    assert!(f.properties.contains(&Property::MessageExpiryInterval(2)));
    c.set(4000);
    e.expire_sessions().unwrap();
    assert!(
        e.retained.is_empty() && e.retained_expiry.is_empty() && e.retained_properties.is_empty()
    );
    publish(&mut e, 0, 2, Some(3), true);
    publish(&mut e, 0, 3, None, true);
    assert!(e.retained_expiry.is_empty());
}
#[test]
fn ingress_wait_counts_toward_expiry_and_preserves_ack_and_retained_removal() {
    for qos in [0, 1, 2] {
        let (mut e, c) = state();
        publish(&mut e, 0, 9, None, true);
        subscribe(&mut e, "a");
        assert!(e.poll_frame_with_changes("sub").unwrap().frame.is_some());
        c.set(10000);
        let replies = e
            .handle_frame_at("pub", publication(qos, 2, Some(1), true), 1000)
            .unwrap();
        match qos {
            0 => assert!(replies.is_empty()),
            1 => assert!(matches!(replies[0].packet, Packet::PubAck { packet_id: 2 })),
            2 => {
                assert!(matches!(replies[0].packet, Packet::PubRec { packet_id: 2 }));
                assert!(!e.retained.is_empty());
                release_source(&mut e, 2, 2);
            }
            _ => unreachable!(),
        }
        assert!(e.retained.is_empty());
        assert!(e.poll_frame_with_changes("sub").unwrap().frame.is_none());
    }
}
#[test]
fn ingress_future_timestamp_clamps_and_duplicate_does_not_reset_deadline() {
    let (mut e, c) = state();
    e.handle_frame_at("pub", publication(2, 1, Some(2), false), u64::MAX)
        .unwrap();
    let id = NonZeroU16::new(1).unwrap();
    assert_eq!(e.sessions["pub"].incoming_expiry[&id], 3000);
    c.set(2000);
    let mut duplicate = publication(2, 1, Some(100), false);
    if let Packet::Publish { dup, .. } = &mut duplicate.packet {
        *dup = true;
    }
    e.handle_frame_at("pub", duplicate, 1900).unwrap();
    assert_eq!(e.sessions["pub"].incoming_expiry[&id], 3000);
}
#[test]
fn nonzero_disconnect_adapter_marks_active_will_pending() {
    let (mut e, _) = state();
    e.connect_with_options(
        "will",
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
            session_expiry_interval: 60,
            receive_maximum: 1,
            will_properties: vec![],
        },
    )
    .unwrap();
    subscribe(&mut e, "a");
    let mut f = Frame::v5(Packet::Disconnect);
    f.reason_code = ReasonCode::try_from(0x80).unwrap();
    e.handle_frame("will", f).unwrap();
    assert!(e.poll_frame_with_changes("sub").unwrap().frame.is_some());
}
