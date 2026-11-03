//! Reserved broker namespace cannot be forged by MQTT clients.
use super::*;
use mqtt_protocol::Will;

#[derive(Debug)]
struct FixedClock;
impl Clock for FixedClock {
    fn now_millis(&self) -> u64 {
        1_000
    }
}
fn config() -> SessionConfig {
    SessionConfig {
        max_sessions: 8,
        max_retained: 8,
        max_subscriptions: 8,
        max_queued: 16,
        max_inflight: 8,
        max_packet_size: 4096,
        max_state_bytes: 128 * 1024,
    }
}
fn engine() -> SessionEngine {
    SessionEngine::new_with_clock(config(), Arc::new(AllowAnonymous), Arc::new(FixedClock)).unwrap()
}
fn connect(
    engine: &mut SessionEngine,
    client: &str,
    version: ProtocolVersion,
    will: Option<Will>,
) -> Result<ConnectResult, EngineError> {
    engine.connect_with_options(
        client,
        will,
        Principal::anonymous(),
        ConnectionOptions {
            version,
            clean_start: false,
            session_expiry_interval: u32::MAX,
            receive_maximum: u16::MAX,
            maximum_packet_size: u32::MAX,
            will_properties: vec![],
        },
    )
}
fn frame(version: ProtocolVersion, topic: &str, qos: u8) -> Frame {
    let packet = Packet::Publish {
        topic: topic.into(),
        payload: b"spoof".to_vec(),
        qos,
        packet_id: (qos > 0).then_some(1),
        dup: false,
        retain: true,
    };
    match version {
        ProtocolVersion::V311 => Frame::v311(packet),
        ProtocolVersion::V5 => Frame::v5(packet),
    }
}
#[test]
fn reserved_client_publish_is_denied_before_retained_or_qos_state() {
    for version in [ProtocolVersion::V311, ProtocolVersion::V5] {
        for topic in ["$SYS", "$SYS/events/client/connected", "$SYS/"] {
            for qos in 0..=2 {
                let mut engine = engine();
                connect(&mut engine, "attacker", version, None).unwrap();
                let before = engine.snapshot().unwrap();
                assert!(matches!(
                    engine.handle_frame("attacker", frame(version, topic, qos)),
                    Err(EngineError::NotAuthorized)
                ));
                assert_eq!(engine.snapshot().unwrap(), before);
                assert!(engine.retained.is_empty());
                assert!(!engine.has_incoming_exchange("attacker", 1));
            }
        }
    }
}
#[test]
fn reserved_connect_will_cannot_replace_existing_connection() {
    for version in [ProtocolVersion::V311, ProtocolVersion::V5] {
        for topic in ["$SYS", "$SYS/events/client/disconnected"] {
            let mut engine = engine();
            connect(&mut engine, "device", version, None).unwrap();
            let before = engine.snapshot().unwrap();
            assert!(matches!(
                connect(
                    &mut engine,
                    "device",
                    version,
                    Some(Will {
                        topic: topic.into(),
                        payload: b"spoof".to_vec(),
                        qos: 1,
                        retain: true,
                    })
                ),
                Err(EngineError::NotAuthorized)
            ));
            assert_eq!(engine.snapshot().unwrap(), before);
            assert!(engine.sessions["device"].online);
            assert!(engine.wills.is_empty());
        }
    }
}
#[test]
fn namespace_guard_does_not_reserve_other_dollar_topics_or_sys_prefixes() {
    for topic in [
        "$SYSfoo/events",
        "$OTHER/events",
        "$sys/events",
        "ordinary/events",
    ] {
        let mut engine = engine();
        connect(
            &mut engine,
            "client",
            ProtocolVersion::V5,
            Some(Will {
                topic: topic.into(),
                payload: vec![],
                qos: 0,
                retain: false,
            }),
        )
        .unwrap();
        engine
            .handle_frame("client", frame(ProtocolVersion::V5, topic, 0))
            .unwrap();
        assert_eq!(engine.retained.len(), 1);
    }
}
#[test]
fn system_subscriptions_remain_available_and_core_qos2_finishes() {
    let mut engine = engine();
    connect(&mut engine, "client", ProtocolVersion::V5, None).unwrap();
    let replies = engine
        .handle_frame(
            "client",
            Frame::v5(Packet::Subscribe {
                packet_id: 2,
                filters: vec![("$SYS/events/client/#".into(), 1)],
            }),
        )
        .unwrap();
    assert!(matches!(&replies[0].packet, Packet::SubAck { codes, .. } if codes == &[1]));
    engine
        .handle_frame("client", frame(ProtocolVersion::V5, "ordinary/topic", 2))
        .unwrap();
    let mut duplicate = frame(ProtocolVersion::V5, "ordinary/topic", 2);
    if let Packet::Publish { dup, .. } = &mut duplicate.packet {
        *dup = true;
    }
    engine.handle_frame("client", duplicate).unwrap();
    let replies = engine
        .handle_frame("client", Frame::v5(Packet::PubRel { packet_id: 1 }))
        .unwrap();
    assert!(matches!(
        replies[0].packet,
        Packet::PubComp { packet_id: 1 }
    ));
}
#[test]
fn restored_legacy_reserved_will_is_retired_without_publishing() {
    let mut engine = engine();
    connect(
        &mut engine,
        "client",
        ProtocolVersion::V5,
        Some(Will {
            topic: "ordinary/topic".into(),
            payload: b"spoof".to_vec(),
            qos: 1,
            retain: true,
        }),
    )
    .unwrap();
    // Simulate a valid old durable record created before the reserved namespace.
    engine.wills.get_mut("client").unwrap().will.topic = "$SYS/events/client/connected".into();
    let bytes = engine.snapshot().unwrap();
    let mut restored = SessionEngine::restore_with_clock(
        config(),
        &bytes,
        Arc::new(AllowAnonymous),
        Arc::new(FixedClock),
    )
    .unwrap();
    assert!(restored.retry_wills().unwrap());
    assert!(restored.retained.is_empty());
    assert!(restored.wills.is_empty());
    // The changed proposal serializes without the Will, so a committed row
    // cannot resurrect its intent after another restart.
    let committed = restored.snapshot().unwrap();
    let again = SessionEngine::restore_with_clock(
        config(),
        &committed,
        Arc::new(AllowAnonymous),
        Arc::new(FixedClock),
    )
    .unwrap();
    assert!(again.wills.is_empty());
}

#[test]
fn legacy_accepted_system_qos2_exchange_can_complete_after_restore() {
    let mut engine = engine();
    connect(&mut engine, "client", ProtocolVersion::V5, None).unwrap();
    engine
        .handle_frame("client", frame(ProtocolVersion::V5, "old/topic", 2))
        .unwrap();
    // The old version could accept this publication before the namespace was
    // reserved. Preserve the accepted exchange rather than hanging its sender.
    let packet = frame(ProtocolVersion::V5, "$SYS/legacy", 2).packet;
    engine
        .sessions
        .get_mut("client")
        .unwrap()
        .incoming
        .pending
        .insert(identifier(1).unwrap(), encode(&packet).unwrap());
    let bytes = engine.snapshot().unwrap();
    let mut restored = SessionEngine::restore_with_clock(
        config(),
        &bytes,
        Arc::new(AllowAnonymous),
        Arc::new(FixedClock),
    )
    .unwrap();
    connect(&mut restored, "client", ProtocolVersion::V5, None).unwrap();
    let mut duplicate = frame(ProtocolVersion::V5, "$SYS/legacy", 2);
    if let Packet::Publish { dup, .. } = &mut duplicate.packet {
        *dup = true;
    }
    assert!(matches!(
        restored.handle_frame("client", duplicate).unwrap()[0].packet,
        Packet::PubRec { packet_id: 1 }
    ));
    assert!(matches!(
        restored
            .handle_frame("client", Frame::v5(Packet::PubRel { packet_id: 1 }))
            .unwrap()[0]
            .packet,
        Packet::PubComp { packet_id: 1 }
    ));
    // Once completed, reuse is a new publication and must be rejected.
    assert!(matches!(
        restored.handle_frame("client", frame(ProtocolVersion::V5, "$SYS/legacy", 2)),
        Err(EngineError::NotAuthorized)
    ));
}
