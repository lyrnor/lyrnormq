//! Regression evidence for unambiguous mixed-RAP live forwarding.
use super::*;
fn config() -> SessionConfig {
    SessionConfig {
        max_sessions: 8,
        max_subscriptions: 16,
        max_queued: 16,
        max_inflight: 16,
        max_packet_size: 8192,
        max_state_bytes: 1 << 20,
        max_retained: 16,
    }
}
fn options(version: ProtocolVersion) -> ConnectionOptions {
    ConnectionOptions {
        version,
        clean_start: false,
        session_expiry_interval: u32::MAX,
        receive_maximum: 16,
        maximum_packet_size: u32::MAX,
        will_properties: Vec::new(),
    }
}
fn connect(e: &mut SessionEngine, id: &str) {
    e.connect_with_options(
        id,
        None,
        Principal::anonymous(),
        options(ProtocolVersion::V5),
    )
    .unwrap();
}
fn sub(e: &mut SessionEngine, filter: &str, flags: u8, sid: Option<u32>) {
    let mut f = Frame::v5(Packet::Subscribe {
        packet_id: 5,
        filters: vec![(filter.into(), flags & 3)],
    });
    f.subscription_options = vec![flags];
    f.properties = sid
        .map(Property::SubscriptionIdentifier)
        .into_iter()
        .collect();
    e.handle_frame("sub", f).unwrap();
}
fn publication(retain: bool, qos: u8) -> Frame {
    Frame::v5(Packet::Publish {
        topic: "a/x".into(),
        payload: b"live".to_vec(),
        qos,
        packet_id: (qos != 0).then_some(9),
        dup: false,
        retain,
    })
}
fn publish(e: &mut SessionEngine, retain: bool, qos: u8) {
    e.handle_frame("pub", publication(retain, qos)).unwrap();
    if qos == 2 {
        e.handle_frame("pub", Frame::v5(Packet::PubRel { packet_id: 9 }))
            .unwrap();
    }
}
fn frames(e: &mut SessionEngine) -> Vec<Frame> {
    let mut out = Vec::new();
    while let Some(f) = e.poll_frame_with_changes("sub").unwrap().frame {
        out.push(f);
    }
    out
}
fn shape(f: &Frame) -> (bool, u8, Vec<u32>) {
    let Packet::Publish { retain, qos, .. } = f.packet else {
        panic!("PUBLISH")
    };
    let ids = f
        .properties
        .iter()
        .filter_map(|p| {
            if let Property::SubscriptionIdentifier(n) = p {
                Some(*n)
            } else {
                None
            }
        })
        .collect();
    (retain, qos, ids)
}
#[test]
fn mixed_rap_retained_live_copies_respect_each_subscription_and_identifier() {
    let mut e = SessionEngine::new(config()).unwrap();
    connect(&mut e, "pub");
    connect(&mut e, "sub");
    sub(&mut e, "a/#", 0x21, Some(11));
    sub(&mut e, "a/+", 0x29, Some(22));
    publish(&mut e, true, 1);
    let mut actual: Vec<_> = frames(&mut e).iter().map(shape).collect();
    actual.sort();
    assert_eq!(actual, vec![(false, 1, vec![11]), (true, 1, vec![22])]);
}

#[test]
fn mixed_rap_each_copy_respects_its_grant_and_duplicate_id_occurrence() {
    let mut e = SessionEngine::new(config()).unwrap();
    connect(&mut e, "pub");
    connect(&mut e, "sub");
    sub(&mut e, "a/#", 0x20, Some(7));
    sub(&mut e, "a/+", 0x2a, Some(7));
    sub(&mut e, "+/x", 0x21, None);
    publish(&mut e, true, 2);
    let mut actual: Vec<_> = frames(&mut e).iter().map(shape).collect();
    actual.sort();
    assert_eq!(
        actual,
        vec![(false, 0, vec![7]), (false, 1, vec![]), (true, 2, vec![7])]
    );
}

#[test]
fn homogeneous_rap_and_nonretained_source_keep_single_copy_with_all_identifiers() {
    for source_retain in [false, true] {
        for rap_a in [false, true] {
            for rap_b in [false, true] {
                if source_retain && rap_a != rap_b {
                    continue;
                }
                let mut e = SessionEngine::new(config()).unwrap();
                connect(&mut e, "pub");
                connect(&mut e, "sub");
                sub(&mut e, "a/#", 0x21 | (u8::from(rap_a) << 3), Some(7));
                sub(&mut e, "a/+", 0x22 | (u8::from(rap_b) << 3), Some(7));
                publish(&mut e, source_retain, 2);
                assert_eq!(
                    frames(&mut e).iter().map(shape).collect::<Vec<_>>(),
                    vec![(source_retain && rap_a, 2, vec![7, 7])]
                );
            }
        }
    }
}

#[test]
fn no_local_filter_is_removed_before_mixed_rap_detection() {
    let mut e = SessionEngine::new(config()).unwrap();
    connect(&mut e, "sub");
    sub(&mut e, "a/#", 0x25, Some(1));
    sub(&mut e, "a/+", 0x29, Some(2));
    e.handle_frame("sub", publication(true, 1)).unwrap();
    assert_eq!(
        frames(&mut e).iter().map(shape).collect::<Vec<_>>(),
        vec![(true, 1, vec![2])]
    );
}

#[test]
fn ordinary_mixed_rap_and_shared_copies_are_independent() {
    let mut e = SessionEngine::new(config()).unwrap();
    connect(&mut e, "pub");
    connect(&mut e, "sub");
    sub(&mut e, "a/#", 0x21, Some(1));
    sub(&mut e, "a/+", 0x29, Some(2));
    sub(&mut e, "$share/g/a/#", 0x29, Some(3));
    publish(&mut e, true, 1);
    let mut actual: Vec<_> = frames(&mut e).iter().map(shape).collect();
    actual.sort();
    assert_eq!(
        actual,
        vec![(false, 1, vec![1]), (true, 1, vec![2]), (true, 1, vec![3])]
    );
}

#[test]
fn mixed_rap_copy_metadata_survives_offline_restore_and_subscription_replacement() {
    let mut e = SessionEngine::new(config()).unwrap();
    connect(&mut e, "pub");
    connect(&mut e, "sub");
    sub(&mut e, "a/#", 0x21, Some(11));
    sub(&mut e, "a/+", 0x29, Some(22));
    e.disconnect_with_reason("sub", true).unwrap();
    publish(&mut e, true, 1);
    let bytes = e.snapshot().unwrap();
    let mut e = SessionEngine::restore(config(), &bytes).unwrap();
    connect(&mut e, "sub");
    sub(&mut e, "a/#", 0x29, Some(99));
    sub(&mut e, "a/+", 0x21, None);
    let mut actual: Vec<_> = frames(&mut e).iter().map(shape).collect();
    actual.sort();
    assert_eq!(actual, vec![(false, 1, vec![11]), (true, 1, vec![22])]);
}

#[test]
fn mixed_rap_peer_size_discards_only_the_oversized_copy() {
    let mut e = SessionEngine::new(config()).unwrap();
    connect(&mut e, "pub");
    e.connect_with_options(
        "sub",
        None,
        Principal::anonymous(),
        ConnectionOptions {
            maximum_packet_size: 20,
            ..options(ProtocolVersion::V5)
        },
    )
    .unwrap();
    sub(&mut e, "a/#", 0x21, Some(1));
    sub(&mut e, "a/+", 0x29, Some(268435455));
    let mut f = publication(true, 1);
    if let Packet::Publish { payload, .. } = &mut f.packet {
        *payload = b"1234567".to_vec();
    }
    e.handle_frame("pub", f).unwrap();
    assert_eq!(
        frames(&mut e).iter().map(shape).collect::<Vec<_>>(),
        vec![(false, 1, vec![1])]
    );
}

#[derive(Debug)]
struct FixedClock;
impl Clock for FixedClock {
    fn now_millis(&self) -> u64 {
        1000
    }
}
fn fixed_engine(config: SessionConfig) -> SessionEngine {
    SessionEngine::new_with_clock(config, Arc::new(AllowAnonymous), Arc::new(FixedClock)).unwrap()
}
fn source_message() -> Message {
    Message {
        topic: "a/x".into(),
        payload: b"live".to_vec(),
        qos: 1,
        retain: true,
        shared_subscription: None,
        retained_replay: false,
        expires_at: None,
        properties: Vec::new(),
    }
}
#[test]
fn mixed_rap_ordinary_and_shared_reservations_fail_without_partial_outbox_or_cursor() {
    for maximum in [1, 2] {
        let mut e = fixed_engine(SessionConfig {
            max_queued: maximum,
            ..config()
        });
        connect(&mut e, "pub");
        connect(&mut e, "sub");
        sub(&mut e, "a/#", 0x21, Some(1));
        sub(&mut e, "a/+", 0x29, Some(2));
        if maximum == 2 {
            sub(&mut e, "$share/g/a/#", 0x21, Some(3));
        }
        let before = e.snapshot().unwrap();
        let cursors = e.shared_cursors.clone();
        assert!(matches!(
            e.route(source_message(), Some("pub")),
            Err(EngineError::Capacity)
        ));
        assert_eq!(e.snapshot().unwrap(), before);
        assert_eq!(e.shared_cursors, cursors);
        assert!(frames(&mut e).is_empty());
    }
}

#[test]
fn mixed_rap_qos2_repeat_pubrel_does_not_duplicate_the_pair() {
    let mut e = fixed_engine(config());
    connect(&mut e, "pub");
    connect(&mut e, "sub");
    sub(&mut e, "a/#", 0x22, Some(1));
    sub(&mut e, "a/+", 0x2a, Some(2));
    publish(&mut e, true, 2);
    e.handle_frame("pub", Frame::v5(Packet::PubRel { packet_id: 9 }))
        .unwrap();
    let mut actual: Vec<_> = frames(&mut e).iter().map(shape).collect();
    actual.sort();
    assert_eq!(actual, vec![(false, 2, vec![1]), (true, 2, vec![2])]);
    let session = &e.sessions["sub"];
    assert_eq!(session.outgoing.pending.len(), 2);
}

#[test]
fn new_captured_pair_survives_v311_then_v5_resume_without_retagging() {
    let mut e = fixed_engine(config());
    connect(&mut e, "pub");
    connect(&mut e, "sub");
    sub(&mut e, "a/#", 0x21, Some(11));
    sub(&mut e, "a/+", 0x29, Some(22));
    publish(&mut e, true, 1);
    e.disconnect_with_reason("sub", true).unwrap();
    e.connect_with_options(
        "sub",
        None,
        Principal::anonymous(),
        options(ProtocolVersion::V311),
    )
    .unwrap();
    let legacy = frames(&mut e);
    assert_eq!(legacy.len(), 2);
    assert!(
        legacy
            .iter()
            .all(|f| !shape(f).0 && f.properties.is_empty())
    );
    let bytes = e.snapshot().unwrap();
    let mut e = SessionEngine::restore_with_clock(
        config(),
        &bytes,
        Arc::new(AllowAnonymous),
        Arc::new(FixedClock),
    )
    .unwrap();
    connect(&mut e, "sub");
    let mut replay: Vec<_> = frames(&mut e).iter().map(shape).collect();
    replay.sort();
    assert_eq!(replay, vec![(false, 1, vec![11]), (true, 1, vec![22])]);
}

#[test]
fn v311_recipient_retains_existing_ordinary_collapse_policy() {
    let mut e = fixed_engine(config());
    connect(&mut e, "pub");
    connect(&mut e, "sub");
    sub(&mut e, "a/#", 0x21, Some(11));
    sub(&mut e, "a/+", 0x29, Some(22));
    e.disconnect_with_reason("sub", true).unwrap();
    e.connect_with_options(
        "sub",
        None,
        Principal::anonymous(),
        options(ProtocolVersion::V311),
    )
    .unwrap();
    publish(&mut e, true, 1);
    let legacy = frames(&mut e);
    assert_eq!(legacy.len(), 1);
    assert_eq!(shape(&legacy[0]), (false, 1, vec![]));
}

#[derive(Debug)]
struct DenyRapOne;
impl AccessControl for DenyRapOne {
    fn authenticate(
        &self,
        _info: mqtt_auth::ConnectInfo<'_>,
    ) -> Result<Principal, mqtt_auth::AuthError> {
        Ok(Principal::anonymous())
    }
    fn authorize(&self, _principal: &Principal, action: Action, resource: &str) -> bool {
        action != Action::Subscribe || resource != "a/+"
    }
}
#[test]
fn subscribe_acl_filter_is_removed_before_mixed_rap_detection() {
    let mut e = fixed_engine(config());
    connect(&mut e, "pub");
    connect(&mut e, "sub");
    sub(&mut e, "a/#", 0x21, Some(1));
    sub(&mut e, "a/+", 0x29, Some(2));
    e.access = Arc::new(DenyRapOne);
    e.route(source_message(), Some("pub")).unwrap();
    assert_eq!(
        frames(&mut e).iter().map(shape).collect::<Vec<_>>(),
        vec![(false, 1, vec![1])]
    );
}

#[test]
fn mixed_rap_pair_obeys_receive_maximum_one_with_distinct_packet_identifiers() {
    let mut e = fixed_engine(config());
    connect(&mut e, "pub");
    e.connect_with_options(
        "sub",
        None,
        Principal::anonymous(),
        ConnectionOptions {
            receive_maximum: 1,
            ..options(ProtocolVersion::V5)
        },
    )
    .unwrap();
    sub(&mut e, "a/#", 0x21, Some(1));
    sub(&mut e, "a/+", 0x29, Some(2));
    publish(&mut e, true, 1);
    let first = e.poll_frame_with_changes("sub").unwrap().frame.unwrap();
    assert!(e.poll_frame_with_changes("sub").unwrap().frame.is_none());
    let Packet::Publish {
        packet_id: Some(id),
        ..
    } = first.packet
    else {
        panic!("identifier")
    };
    e.handle_frame("sub", Frame::v5(Packet::PubAck { packet_id: id }))
        .unwrap();
    let second = e.poll_frame_with_changes("sub").unwrap().frame.unwrap();
    let Packet::Publish {
        packet_id: Some(next),
        ..
    } = second.packet
    else {
        panic!("identifier")
    };
    assert_ne!(id, next);
    let mut choices = vec![shape(&first), shape(&second)];
    choices.sort();
    assert_eq!(choices, vec![(false, 1, vec![1]), (true, 1, vec![2])]);
}

#[derive(Debug)]
struct AdvanceClock(std::sync::atomic::AtomicU64);
impl Clock for AdvanceClock {
    fn now_millis(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}
#[test]
fn offline_mixed_rap_pair_keeps_one_source_expiry_and_both_expire() {
    let clock = Arc::new(AdvanceClock(std::sync::atomic::AtomicU64::new(1000)));
    let mut e =
        SessionEngine::new_with_clock(config(), Arc::new(AllowAnonymous), clock.clone()).unwrap();
    connect(&mut e, "pub");
    connect(&mut e, "sub");
    sub(&mut e, "a/#", 0x21, Some(1));
    sub(&mut e, "a/+", 0x29, Some(2));
    e.disconnect_with_reason("sub", true).unwrap();
    let mut f = publication(true, 1);
    f.properties.push(Property::MessageExpiryInterval(1));
    e.handle_frame("pub", f).unwrap();
    let queue = &e.sessions["sub"].queued;
    assert_eq!(queue.len(), 2);
    assert!(queue.iter().all(|message| message.expires_at == Some(2000)));
    clock.0.store(2000, std::sync::atomic::Ordering::SeqCst);
    connect(&mut e, "sub");
    assert!(frames(&mut e).is_empty());
}

#[test]
fn captured_single_copy_is_not_split_or_retagged_after_options_become_mixed() {
    let mut e = fixed_engine(config());
    connect(&mut e, "pub");
    connect(&mut e, "sub");
    sub(&mut e, "a/#", 0x29, Some(11));
    sub(&mut e, "a/+", 0x29, Some(22));
    publish(&mut e, true, 1);
    sub(&mut e, "a/#", 0x21, Some(11));
    let mut e = SessionEngine::restore_with_clock(
        config(),
        &e.snapshot().unwrap(),
        Arc::new(AllowAnonymous),
        Arc::new(FixedClock),
    )
    .unwrap();
    connect(&mut e, "sub");
    assert_eq!(
        frames(&mut e).iter().map(shape).collect::<Vec<_>>(),
        vec![(true, 1, vec![11, 22])]
    );
}
