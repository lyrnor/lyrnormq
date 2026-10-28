use super::*;
fn publication(topic: &str, alias: Option<u16>) -> Frame {
    let mut frame = Frame::v5(Packet::Publish {
        topic: topic.into(),
        payload: b"body".to_vec(),
        qos: 1,
        packet_id: Some(7),
        dup: false,
        retain: true,
    });
    frame.properties = vec![
        Property::UserProperty("x".into(), "one".into()),
        Property::SubscriptionIdentifier(17),
        Property::UserProperty("x".into(), "two".into()),
    ];
    if let Some(alias) = alias {
        frame.properties.push(Property::TopicAlias(alias));
    }
    frame
}
fn topic(frame: &Frame) -> &str {
    match &frame.packet {
        Packet::Publish { topic, .. } => topic,
        _ => panic!("publish required"),
    }
}
fn reason(error: io::Error) -> ReasonCode {
    error
        .get_ref()
        .unwrap()
        .downcast_ref::<super::super::WorkerFailure>()
        .unwrap()
        .reason
}
#[test]
fn input_bind_reference_replace_preserves_every_other_field() {
    let mut aliases = InboundAliases::new(2);
    let mut first = publication("one/topic", Some(2));
    aliases.normalize(&mut first, 4096).unwrap();
    assert_eq!(first, publication("one/topic", None));
    let mut reference = publication("", Some(2));
    aliases.normalize(&mut reference, 4096).unwrap();
    assert_eq!(reference, first);
    let mut rebound = publication("two/topic", Some(2));
    aliases.normalize(&mut rebound, 4096).unwrap();
    let mut reference = publication("", Some(2));
    aliases.normalize(&mut reference, 4096).unwrap();
    assert_eq!(reference, publication("two/topic", None));
    assert_eq!(aliases.topics.len(), 1);
}
#[test]
fn input_zero_range_unknown_and_independent_connection_fail94() {
    let mut aliases = InboundAliases::new(2);
    for (name, id) in [("topic", 0), ("topic", 3), ("", 1)] {
        assert_eq!(
            reason(
                aliases
                    .normalize(&mut publication(name, Some(id)), 4096)
                    .unwrap_err()
            ),
            ReasonCode::TOPIC_ALIAS_INVALID
        );
    }
    aliases
        .normalize(&mut publication("topic", Some(1)), 4096)
        .unwrap();
    assert!(
        InboundAliases::new(2)
            .normalize(&mut publication("", Some(1)), 4096)
            .is_err()
    );
    assert!(
        InboundAliases::new(0)
            .normalize(&mut publication("topic", Some(1)), 4096)
            .is_err()
    );
    let mut sparse = InboundAliases::new(u16::MAX);
    sparse
        .normalize(&mut publication("topic", Some(u16::MAX)), 4096)
        .unwrap();
    assert_eq!(sparse.topics.len(), 1);
}
#[test]
fn input_expansion_is_quota97_not_false_wire95() {
    let mut aliases = InboundAliases::new(1);
    let long = "t".repeat(100);
    aliases
        .normalize(&mut publication(&long, Some(1)), 1024)
        .unwrap();
    assert!(encode_frame(&publication("", Some(1))).unwrap().len() < 80);
    assert_eq!(
        reason(
            aliases
                .normalize(&mut publication("", Some(1)), 80)
                .unwrap_err()
        ),
        ReasonCode::QUOTA_EXCEEDED
    );
}
#[test]
fn input_duplicate_rejected_and_nonpublishes_are_unchanged() {
    let mut aliases = InboundAliases::new(2);
    let mut duplicate = publication("t", Some(1));
    duplicate.properties.push(Property::TopicAlias(2));
    assert_eq!(
        reason(aliases.normalize(&mut duplicate, 4096).unwrap_err()),
        ReasonCode::PROTOCOL_ERROR
    );
    let mut ping = Frame::v5(Packet::PingReq);
    aliases.normalize(&mut ping, 4096).unwrap();
    assert_eq!(ping, Frame::v5(Packet::PingReq));
}
#[test]
fn output_binding_is_not_visible_until_written_commit() {
    let mut aliases = OutboundAliases::new(2);
    let original = publication("long/output/topic", None);
    let first = aliases.prepare(original.clone(), 2, 4096).unwrap();
    assert_eq!(topic(&first.frame), "long/output/topic");
    assert!(first.binding.is_some());
    let still_first = aliases.prepare(original.clone(), 2, 4096).unwrap();
    assert!(!topic(&still_first.frame).is_empty());
    aliases.commit(first.binding);
    let reference = aliases.prepare(original.clone(), 2, 4096).unwrap();
    assert_eq!(topic(&reference.frame), "");
    assert!(reference.binding.is_none());
    let mut inbound = InboundAliases::new(2);
    let mut bound = still_first.frame;
    inbound.normalize(&mut bound, 4096).unwrap();
    let mut referenced = reference.frame;
    inbound.normalize(&mut referenced, 4096).unwrap();
    assert_eq!(referenced, original);
}
#[test]
fn output_exact_cap_fallback_does_not_install_phantom_binding() {
    for padding in [0, 90, 100, 16_340, 16_350] {
        let mut original = publication("a/topic/long/enough", None);
        if let Packet::Publish { payload, .. } = &mut original.packet {
            *payload = vec![0; padding];
        }
        let cap = encode_frame(&original).unwrap().len();
        let mut aliases = OutboundAliases::new(1);
        let fallback = aliases.prepare(original.clone(), 1, cap).unwrap();
        assert_eq!(fallback.frame, original);
        assert!(fallback.binding.is_none());
        aliases.commit(fallback.binding);
        assert!(aliases.topics.is_empty());
        let binding = aliases.prepare(original.clone(), 1, cap + 5).unwrap();
        assert!(binding.binding.is_some());
        assert!(!topic(&binding.frame).is_empty());
        aliases.commit(binding.binding);
        let compressed = aliases.prepare(original, 1, cap).unwrap();
        assert!(topic(&compressed.frame).is_empty());
        assert!(encode_frame(&compressed.frame).unwrap().len() <= cap);
    }
}
#[test]
fn output_bounded_round_robin_replacement_and_separate_directions() {
    let mut aliases = OutboundAliases::new(2);
    for name in ["first/long/topic", "second/long/topic", "third/long/topic"] {
        let frame = aliases
            .prepare(publication(name, None), u16::MAX, 4096)
            .unwrap();
        aliases.commit(frame.binding);
    }
    assert_eq!(aliases.topics.len(), 2);
    assert_eq!(aliases.topics[&1], "third/long/topic");
    assert_eq!(aliases.topics[&2], "second/long/topic");
    assert!(
        !topic(
            &aliases
                .prepare(publication("first/long/topic", None), 2, 4096)
                .unwrap()
                .frame
        )
        .is_empty()
    );
    assert!(
        InboundAliases::new(2)
            .normalize(&mut publication("", Some(1)), 4096)
            .is_err()
    );
    let other = OutboundAliases::new(2);
    assert!(
        !topic(
            &other
                .prepare(publication("second/long/topic", None), 2, 4096)
                .unwrap()
                .frame
        )
        .is_empty()
    );
}
#[test]
fn output_zero_budget_v311_controls_short_reference_and_property_order() {
    let mut aliases = OutboundAliases::new(1);
    let original = publication("a", None);
    let bound = aliases.prepare(original.clone(), 1, 4096).unwrap();
    aliases.commit(bound.binding);
    assert_eq!(
        aliases.prepare(original.clone(), 1, 4096).unwrap().frame,
        original
    );
    assert_eq!(
        aliases.prepare(original.clone(), 0, 4096).unwrap().frame,
        original
    );
    assert_eq!(
        OutboundAliases::new(0)
            .prepare(original.clone(), 1, 4096)
            .unwrap()
            .frame,
        original
    );
    let legacy = Frame::v311(Packet::PingResp);
    assert_eq!(
        aliases.prepare(legacy.clone(), 1, 4096).unwrap().frame,
        legacy
    );
    let mut invalid = publication("t", Some(1));
    assert_eq!(
        reason(aliases.prepare(invalid.clone(), 1, 4096).err().unwrap()),
        ReasonCode::PROTOCOL_ERROR
    );
    invalid.properties.clear();
    let prepared = aliases.prepare(invalid, 1, 4096).unwrap();
    assert!(
        prepared
            .frame
            .properties
            .iter()
            .any(|p| matches!(p, Property::TopicAlias(1)))
    );
}
