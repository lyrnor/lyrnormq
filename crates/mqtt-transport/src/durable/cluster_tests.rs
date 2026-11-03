use super::*;
use mqtt_cluster_core::{ClusterBridge, ClusterFuture, ClusterMessage, LocalBroker};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
struct Bridge {
    local: Mutex<Option<Arc<dyn LocalBroker>>>,
    ready: AtomicBool,
    fails: AtomicBool,
    hold: AtomicBool,
    resume: tokio::sync::Notify,
    forwards: AtomicUsize,
    subscriptions: Mutex<Vec<(String, u64, Vec<String>)>>,
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
    fn subscriptions(
        &self,
        client_id: String,
        generation: u64,
        filters: Vec<String>,
    ) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            let owner = self
                .claims
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find(|(client, _)| client == &client_id)
                .map(|(_, generation)| *generation);
            if owner != Some(generation) {
                return Err(io::Error::other("stale subscription generation"));
            }
            self.subscriptions
                .lock()
                .unwrap()
                .push((client_id, generation, filters));
            Ok(())
        })
    }
    fn forward(&self, _: ClusterMessage) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            self.forwards.fetch_add(1, Ordering::Relaxed);
            if self.hold.load(Ordering::Acquire) {
                self.resume.notified().await;
            }
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
async fn connect_frame(worker: &Worker, frame: Frame, generation: u64) -> Reply {
    let (wake, _) = watch::channel(0);
    let (cancel, _) = watch::channel(false);
    let (close_reason, _) = watch::channel(None);
    worker
        .request(Operation::ConnectFrame {
            frame,
            generation,
            wake,
            cancel,
            close_reason,
        })
        .await
        .unwrap()
}
fn connect_packet() -> Packet {
    Packet::Connect {
        client_id: "v5".into(),
        clean_session: true,
        keep_alive: 0,
        will: None,
        username: None,
        password: None,
    }
}
#[tokio::test]
async fn persistent_requests_and_bad_credentials_never_claim_client_id() {
    let bridge = Arc::new(Bridge::default());
    let (worker, task) = tests::worker(config(bridge.clone())).await;
    let mut persistent = Frame::v5(connect_packet());
    persistent
        .properties
        .push(Property::SessionExpiryInterval(1));
    assert!(!matches!(
        connect_frame(&worker, persistent, 1).await,
        Reply::Connected(_) | Reply::Assigned { .. } | Reply::Authenticated { .. }
    ));
    let mut bad_credentials = Frame::v5(connect_packet());
    if let Packet::Connect { username, .. } = &mut bad_credentials.packet {
        *username = Some("invalid".into());
    }
    assert!(!matches!(
        connect_frame(&worker, bad_credentials, 2).await,
        Reply::Connected(_) | Reply::Assigned { .. } | Reply::Authenticated { .. }
    ));
    assert!(bridge.claims.lock().unwrap().is_empty());
    let mut nonpersistent = Frame::v5(connect_packet());
    if let Packet::Connect { clean_session, .. } = &mut nonpersistent.packet {
        *clean_session = false;
    }
    assert!(matches!(
        connect_frame(&worker, nonpersistent, 3).await,
        Reply::Connected(_) | Reply::Assigned { .. } | Reply::Authenticated { .. }
    ));
    assert_eq!(bridge.claims.lock().unwrap().len(), 1);
    tests::stop(worker, task).await;
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
        bridge.subscriptions.lock().unwrap().last().unwrap().2,
        vec!["a"]
    );
    let local = bridge.local.lock().unwrap().clone().unwrap();
    local.fence("client".into(), 1).await.unwrap();
    assert!(*old.borrow());
    let (_, new) = tests::connect(&worker, "client", 2, true).await;
    tests::subscribe(&worker, "client", 2).await;
    let updates = bridge.subscriptions.lock().unwrap().len();
    assert!(
        tests::handle(
            &worker,
            "client",
            1,
            Packet::Subscribe {
                packet_id: 9,
                filters: vec![("stale".into(), 1)],
            }
        )
        .await
        .is_err()
    );
    assert_eq!(bridge.subscriptions.lock().unwrap().len(), updates);
    assert_eq!(bridge.subscriptions.lock().unwrap().last().unwrap().1, 2);
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
    connect_frame(&worker, Frame::v5(connect_packet()), 2).await;
    let Reply::Frames(frames) = worker
        .request(Operation::HandleFrameAt {
            client: "v5".into(),
            generation: 2,
            received_at_millis: SystemClock.now_millis(),
            frame: Frame::v5(Packet::Subscribe {
                packet_id: 1,
                filters: vec![("$share/g/a".into(), 1)],
            }),
        })
        .await
        .unwrap()
    else {
        panic!("frames expected")
    };
    assert!(matches!(&frames[0].packet, Packet::SubAck { codes, .. } if codes == &[0x9e]));
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

#[tokio::test]
async fn forwarding_capacity_is_held_until_real_work_finishes() {
    let bridge = Arc::new(Bridge::default());
    bridge.hold.store(true, Ordering::Release);
    let (worker, task) = tests::worker(config(bridge.clone())).await;
    let publication = Arc::new(mqtt_session::durable::Publication {
        topic: "a".into(),
        payload: vec![0; 128],
        qos: 0,
        retain: false,
        properties: vec![],
        expires_at: None,
        publisher: Some("source".into()),
        expired: false,
    });
    let mut accepted = Vec::new();
    for _ in 0..32 {
        accepted.push(
            worker
                .broker
                .forward_cluster(publication.clone(), None)
                .unwrap(),
        );
    }
    let rejected = worker.broker.forward_cluster(publication, None).unwrap();
    assert!(!rejected.await.unwrap());
    tokio::time::timeout(Duration::from_secs(1), async {
        while bridge.forwards.load(Ordering::Acquire) != 32 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // A caller disappearing cannot release admission while the actual bridge task runs.
    drop(accepted.pop());
    assert!(
        !worker
            .broker
            .forward_cluster(
                Arc::new(mqtt_session::durable::Publication {
                    topic: "a".into(),
                    payload: vec![],
                    qos: 0,
                    retain: false,
                    properties: vec![],
                    expires_at: None,
                    publisher: None,
                    expired: false,
                }),
                None
            )
            .unwrap()
            .await
            .unwrap()
    );
    bridge.hold.store(false, Ordering::Release);
    bridge.resume.notify_waiters();
    for ack in accepted {
        assert!(ack.await.unwrap());
    }
    tokio::task::yield_now().await;
    assert!(
        worker
            .broker
            .forward_cluster(
                Arc::new(mqtt_session::durable::Publication {
                    topic: "a".into(),
                    payload: vec![],
                    qos: 0,
                    retain: false,
                    properties: vec![],
                    expires_at: None,
                    publisher: None,
                    expired: false,
                }),
                None
            )
            .unwrap()
            .await
            .unwrap()
    );
    tests::stop(worker, task).await;
}

#[tokio::test]
async fn remote_ingress_rejects_full_local_mailbox_instead_of_false_acceptance() {
    let bridge = Arc::new(Bridge::default());
    let (worker, task) = tests::worker(config(bridge.clone())).await;
    tests::connect(&worker, "subscriber", 1, true).await;
    tests::subscribe(&worker, "subscriber", 1).await;
    let handle = worker.broker.handle("subscriber").unwrap();
    let mut permits = Vec::new();
    for _ in 0..handle.sender.capacity() {
        permits.push(handle.sender.clone().reserve_owned().await.unwrap());
    }
    let local = bridge.local.lock().unwrap().clone().unwrap();
    assert!(
        local
            .deliver(ClusterMessage {
                topic: "a".into(),
                payload: b"remote".to_vec(),
                qos: 1,
                retain: false,
                properties: vec![],
                expires_at: None,
                publisher: Some("remote".into()),
                observer_client: "remote".into(),
                scope: mqtt_cluster_core::ForwardScope::Ordinary,
            })
            .await
            .is_err()
    );
    drop(permits);
    drop(handle);
    tests::stop(worker, task).await;
}

#[tokio::test]
async fn no_local_remote_delivery_is_successful_filtering() {
    let bridge = Arc::new(Bridge::default());
    let (worker, task) = tests::worker(config(bridge.clone())).await;
    connect_frame(&worker, Frame::v5(connect_packet()), 1).await;
    let mut subscription = Frame::v5(Packet::Subscribe {
        packet_id: 1,
        filters: vec![("a".into(), 1)],
    });
    subscription.subscription_options = vec![0x05];
    worker
        .request(Operation::HandleFrameAt {
            client: "v5".into(),
            generation: 1,
            frame: subscription,
            received_at_millis: SystemClock.now_millis(),
        })
        .await
        .unwrap();
    let local = bridge.local.lock().unwrap().clone().unwrap();
    local
        .deliver(ClusterMessage {
            topic: "a".into(),
            payload: b"remote".to_vec(),
            qos: 1,
            retain: false,
            properties: vec![],
            expires_at: None,
            publisher: Some("v5".into()),
            observer_client: "v5".into(),
            scope: mqtt_cluster_core::ForwardScope::Ordinary,
        })
        .await
        .unwrap();
    assert!(tests::poll(&worker, "v5", 1).await.is_none());
    tests::stop(worker, task).await;
}
