//! Numbered-clause conformance regressions independent of feature acceptance tests.
use super::*;

#[derive(Debug)]
struct FixedClock;
impl Clock for FixedClock {
    fn now_millis(&self) -> u64 {
        1000
    }
}

fn engine() -> SessionEngine {
    let mut engine = SessionEngine::new_with_clock(
        SessionConfig {
            max_sessions: 16,
            max_retained: 16,
            max_subscriptions: 16,
            max_queued: 16,
            max_inflight: 16,
            max_packet_size: 8192,
            max_state_bytes: 524288,
        },
        Arc::new(AllowAnonymous),
        Arc::new(FixedClock),
    )
    .unwrap();
    engine
        .connect_with_options(
            "client",
            None,
            Principal::anonymous(),
            ConnectionOptions {
                version: ProtocolVersion::V5,
                clean_start: false,
                session_expiry_interval: u32::MAX,
                receive_maximum: 16,
                maximum_packet_size: u32::MAX,
                will_properties: Vec::new(),
            },
        )
        .unwrap();
    engine
}

#[test]
fn mqtt_3_10_4_6_duplicate_unsubscribe_filters_observe_prior_removals() {
    for filter in ["topic/#", "$share/group/topic/#"] {
        let mut engine = engine();
        engine
            .handle_frame(
                "client",
                Frame::v5(Packet::Subscribe {
                    packet_id: 1,
                    filters: vec![(filter.into(), 1)],
                }),
            )
            .unwrap();
        let replies = engine
            .handle_frame(
                "client",
                Frame::v5(Packet::Unsubscribe {
                    packet_id: 2,
                    filters: vec![filter.into(), filter.into(), "missing/#".into()],
                }),
            )
            .unwrap();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].packet, Packet::UnsubAck { packet_id: 2 });
        assert_eq!(
            replies[0]
                .reason_codes
                .iter()
                .map(|code| code.value())
                .collect::<Vec<_>>(),
            vec![0, 0x11, 0x11]
        );
        assert!(engine.sessions["client"].subscriptions.is_empty());
    }
}

#[test]
fn mqtt_unsubscribe_invalid_later_filter_preserves_existing_memberships() {
    let mut engine = engine();
    engine
        .handle_frame(
            "client",
            Frame::v5(Packet::Subscribe {
                packet_id: 1,
                filters: vec![("topic/#".into(), 1), ("$share/group/topic/#".into(), 1)],
            }),
        )
        .unwrap();
    let before = engine.snapshot().unwrap();
    assert!(
        engine
            .handle_frame(
                "client",
                Frame::v5(Packet::Unsubscribe {
                    packet_id: 2,
                    filters: vec!["topic/#".into(), "topic/#/invalid".into()],
                })
            )
            .is_err()
    );
    assert_eq!(engine.snapshot().unwrap(), before);
}

#[test]
fn mqtt_3_10_4_3_duplicate_unsubscribe_preserves_started_qos_obligations() {
    for filter in ["topic/#", "$share/group/topic/#"] {
        for qos in [1, 2] {
            let mut engine = engine();
            engine
                .handle_frame(
                    "client",
                    Frame::v5(Packet::Subscribe {
                        packet_id: 1,
                        filters: vec![(filter.into(), qos)],
                    }),
                )
                .unwrap();
            engine
                .handle_frame(
                    "client",
                    Frame::v5(Packet::Publish {
                        topic: "topic/item".into(),
                        payload: b"owned".to_vec(),
                        qos,
                        packet_id: Some(100),
                        dup: false,
                        retain: false,
                    }),
                )
                .unwrap();
            if qos == 2 {
                engine
                    .handle_frame("client", Frame::v5(Packet::PubRel { packet_id: 100 }))
                    .unwrap();
            }
            let delivery = engine
                .poll_delivery_with_changes("client")
                .unwrap()
                .delivery
                .unwrap();
            let sent = engine
                .begin_delivery("client", delivery)
                .unwrap()
                .frame
                .unwrap();
            let Packet::Publish {
                packet_id: Some(id),
                qos: actual,
                ..
            } = sent.packet
            else {
                panic!("expected publish")
            };
            assert_eq!(actual, qos);
            let response = engine
                .handle_frame(
                    "client",
                    Frame::v5(Packet::Unsubscribe {
                        packet_id: 2,
                        filters: vec![filter.into(), filter.into()],
                    }),
                )
                .unwrap();
            assert_eq!(
                response[0]
                    .reason_codes
                    .iter()
                    .map(|c| c.value())
                    .collect::<Vec<_>>(),
                vec![0, 17]
            );
            assert_eq!(engine.sessions["client"].outgoing.inflight_count(), 1);
            if qos == 1 {
                engine
                    .handle_frame("client", Frame::v5(Packet::PubAck { packet_id: id }))
                    .unwrap();
            } else {
                let release = engine
                    .handle_frame("client", Frame::v5(Packet::PubRec { packet_id: id }))
                    .unwrap();
                assert_eq!(release[0].packet, Packet::PubRel { packet_id: id });
                engine
                    .handle_frame("client", Frame::v5(Packet::PubComp { packet_id: id }))
                    .unwrap();
            }
            assert_eq!(engine.sessions["client"].outgoing.inflight_count(), 0);
        }
    }
}
