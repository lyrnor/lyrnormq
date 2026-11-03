use super::*;
use mqtt_cluster_core::{ClusterBridge, ClusterFuture, ClusterMessage, LocalBroker};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
struct Bridge {
    local: Mutex<Option<Arc<dyn LocalBroker>>>,
    ready: AtomicBool,
    fails: AtomicBool,
    forwards: AtomicUsize,
    subscriptions: Mutex<Vec<(String, Vec<String>)>>,
    claims: Mutex<Vec<(String, u64)>>,
}
impl std::fmt::Debug for Bridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestBridge").finish_non_exhaustive()
    }
}
impl ClusterBridge for Bridge {
    fn ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
    fn open(&self, local: Arc<dyn LocalBroker>) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            *self.local.lock().unwrap() = Some(local);
            self.ready.store(true, Ordering::Release);
            Ok(())
        })
    }
    fn claim(&self, client_id: String, generation: u64) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            self.claims.lock().unwrap().push((client_id, generation));
            Ok(())
        })
    }
    fn release(&self, _: String, _: u64) -> ClusterFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn subscriptions(&self, client_id: String, filters: Vec<String>) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            self.subscriptions
                .lock()
                .unwrap()
                .push((client_id, filters));
            Ok(())
        })
    }
    fn forward(&self, _: ClusterMessage) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            self.forwards.fetch_add(1, Ordering::Relaxed);
            if self.fails.load(Ordering::Acquire) {
                Err(io::Error::other("replica unavailable"))
            } else {
                Ok(())
            }
        })
    }
    fn shutdown(&self) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            self.ready.store(false, Ordering::Release);
            Ok(())
        })
    }
}
fn config(bridge: Arc<Bridge>) -> BrokerConfig {
    BrokerConfig {
        cluster: Some(bridge),
        ..BrokerConfig::default()
    }
}
#[tokio::test]
async fn ready_and_remote_failure_gate_mqtt_ack_and_http_without_source_sessions() {
    let bridge = Arc::new(Bridge::default());
    let publisher = crate::PublishHandle::new();
    let (worker, task) = tests::worker(BrokerConfig {
        publisher: Some(publisher.clone()),
        ..config(bridge.clone())
    })
    .await;
    tests::connect(&worker, "publisher", 1, true).await;
    tests::handle(&worker, "publisher", 1, tests::publication(b"ok", 1))
        .await
        .unwrap();
    assert_eq!(bridge.forwards.load(Ordering::Relaxed), 1);
    bridge.fails.store(true, Ordering::Release);
    assert!(
        tests::handle(&worker, "publisher", 1, tests::publication(b"fail", 2))
            .await
            .is_err()
    );
    assert!(
        publisher
            .publish(crate::PublishMessage {
                topic: "a".into(),
                payload: b"http".to_vec(),
                qos: 1,
                retain: false,
                source: "http".into()
            })
            .await
            .is_err()
    );
    assert_eq!(worker.broker.actor_count(), 1);
    bridge.ready.store(false, Ordering::Release);
    assert!(
        tests::handle(&worker, "publisher", 1, Packet::PingReq)
            .await
            .is_err()
    );
    tests::stop(worker, task).await;
}
#[tokio::test]
async fn subscription_watermark_and_actor_independent_fencing_preserve_new_generation() {
    let bridge = Arc::new(Bridge::default());
    let (worker, task) = tests::worker(config(bridge.clone())).await;
    let (_, old) = tests::connect(&worker, "client", 1, true).await;
    tests::subscribe(&worker, "client", 1).await;
    assert_eq!(
        bridge.subscriptions.lock().unwrap().last().unwrap().1,
        vec!["a"]
    );
    let local = bridge.local.lock().unwrap().clone().unwrap();
    local.fence("client".into(), 1).await.unwrap();
    assert!(*old.borrow());
    let (_, new) = tests::connect(&worker, "client", 2, true).await;
    local.fence("client".into(), 1).await.unwrap();
    assert!(!*new.borrow());
    local.fence_all().await.unwrap();
    assert!(*new.borrow());
    assert_eq!(bridge.claims.lock().unwrap().len(), 2);
    tests::stop(worker, task).await;
}
#[tokio::test]
async fn cluster_features_are_explicit_and_standalone_capabilities_unchanged() {
    let bridge = Arc::new(Bridge::default());
    let (worker, task) = tests::worker(config(bridge)).await;
    tests::connect(&worker, "client", 1, true).await;
    let mut retained = tests::publication(b"retained", 1);
    if let Packet::Publish { retain, .. } = &mut retained {
        *retain = true;
    }
    assert_eq!(
        reason(
            &tests::handle(&worker, "client", 1, retained)
                .await
                .unwrap_err()
        ),
        Some(ReasonCode::RETAIN_NOT_SUPPORTED)
    );
    let frames = worker
        .request(Operation::HandleFrameAt {
            client: "client".into(),
            generation: 1,
            received_at_millis: SystemClock.now_millis(),
            frame: Frame::v311(Packet::Subscribe {
                packet_id: 2,
                filters: vec![("$share/g/a".into(), 1)],
            }),
        })
        .await;
    let Reply::Frames(frames) = frames.unwrap() else {
        panic!("frames expected")
    };
    assert_eq!(frames[0].version, ProtocolVersion::V311);
    assert!(matches!(&frames[0].packet, Packet::SubAck { codes, .. } if codes == &[0x80]));
    assert!(worker.broker.retained_for(&[("#".into(), 2)]).is_empty());
    let frame = network::connack(ProtocolVersion::V5, false, 0, &worker.broker.config);
    assert!(
        frame
            .properties
            .contains(&Property::SharedSubscriptionAvailable(0))
    );
    assert!(frame.properties.contains(&Property::RetainAvailable(0)));
    tests::stop(worker, task).await;
    let standalone = network::connack(ProtocolVersion::V5, false, 0, &BrokerConfig::default());
    assert!(
        standalone
            .properties
            .contains(&Property::SharedSubscriptionAvailable(1))
    );
    assert!(
        !standalone
            .properties
            .contains(&Property::RetainAvailable(0))
    );
}
#[tokio::test]
async fn incoming_cluster_delivery_without_reflection() {
    let bridge = Arc::new(Bridge::default());
    let (worker, task) = tests::worker(config(bridge.clone())).await;
    tests::connect(&worker, "subscriber", 1, true).await;
    tests::subscribe(&worker, "subscriber", 1).await;
    let local = bridge.local.lock().unwrap().clone().unwrap();
    local
        .deliver(ClusterMessage {
            topic: "a".into(),
            payload: b"remote".to_vec(),
            qos: 1,
            retain: false,
            properties: vec![],
            expires_at: None,
            publisher: Some("remote-publisher".into()),
            observer_client: "remote-publisher".into(),
            scope: mqtt_cluster_core::ForwardScope::Ordinary,
        })
        .await
        .unwrap();
    assert!(
        matches!(tests::poll(&worker,"subscriber",1).await,Some(Packet::Publish{payload,..}) if payload==b"remote")
    );
    assert_eq!(bridge.forwards.load(Ordering::Relaxed), 0);
    tests::stop(worker, task).await;
}
