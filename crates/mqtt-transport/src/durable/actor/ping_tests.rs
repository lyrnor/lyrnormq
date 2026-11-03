//! PING must retain model-side expiry changes even when housekeeping is quiet.
use super::*;
use crate::durable::durable_cluster_tests::Bridge;
use std::sync::atomic::AtomicU64;

#[derive(Debug, Default)]
struct Clock(AtomicU64);
impl mqtt_session::durable::Clock for Clock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

#[tokio::test]
async fn ping_commits_expiry_already_performed_inside_handle_frame() {
    let clock = Arc::new(Clock::default());
    let config = BrokerConfig {
        cluster: Some(Arc::new(Bridge::default())),
        ..BrokerConfig::default()
    };
    let broker = Broker::start(config.clone(), clock.clone(), None)
        .await
        .unwrap();
    let mut engine = SessionEngine::new_with_clock(
        session_config(&config),
        config.access_control.clone(),
        clock.clone(),
    )
    .unwrap();
    engine
        .connect_with_options(
            "target",
            None,
            Principal::anonymous(),
            ConnectionOptions {
                version: ProtocolVersion::V5,
                clean_start: true,
                session_expiry_interval: 60,
                receive_maximum: 1,
                maximum_packet_size: u32::MAX,
                will_properties: Vec::new(),
            },
        )
        .unwrap();
    engine
        .handle_frame(
            "target",
            Frame::v5(Packet::Subscribe {
                packet_id: 1,
                filters: vec![("a".into(), 1)],
            }),
        )
        .unwrap();
    engine
        .deliver(
            &Publication {
                topic: "a".into(),
                payload: b"expires".to_vec(),
                qos: 1,
                retain: false,
                properties: vec![Property::MessageExpiryInterval(1)],
                expires_at: Some(1000),
                publisher: None,
                expired: false,
            },
            &DeliveryScope::Ordinary,
        )
        .unwrap();
    assert_eq!(engine.next_expiry_deadline(), Some(1000));
    let (wake, _) = watch::channel(0);
    let (cancel, _) = watch::channel(false);
    let online = HashMap::from([(
        "target".to_owned(),
        Online {
            generation: 1,
            wake,
            cancel,
            close_reason: None,
        },
    )]);
    clock.0.store(1001, Ordering::Release);
    let mut effects = Effects::default();
    let reply = apply(
        &mut engine,
        Request::Handle {
            client: "target".into(),
            generation: 1,
            frame: Frame::v5(Packet::PingReq),
            legacy: false,
            received_at_millis: None,
        },
        None,
        &online,
        &broker,
        &mut effects,
    )
    .unwrap();
    assert!(matches!(reply, Reply::Frames(_)));
    assert!(
        effects.commit,
        "expiry performed by handle_frame still requires commit"
    );
    housekeeping(&mut engine, &mut effects).unwrap();
    assert!(effects.commit);
    assert!(
        engine
            .poll_frame_with_changes("target")
            .unwrap()
            .frame
            .is_none()
    );
}
