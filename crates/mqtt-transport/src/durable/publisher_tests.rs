use super::*;
use crate::{PublishHandle, PublishMessage};
use mqtt_auth::{AccessControl, Action, Principal};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Default)]
struct RejectSource(AtomicUsize);
impl AccessControl for RejectSource {
    fn authenticate(&self, _: ConnectInfo<'_>) -> Result<Principal, AuthError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(AuthError::BadCredentials)
    }
    fn authorize(&self, _: &Principal, _: Action, _: &str) -> bool {
        self.0.fetch_add(1, Ordering::Relaxed);
        false
    }
}
fn message(topic: &str, qos: u8, retain: bool) -> PublishMessage {
    PublishMessage {
        topic: topic.into(),
        payload: b"downlink".to_vec(),
        qos,
        retain,
        source: "http".into(),
    }
}
#[tokio::test]
async fn privileged_handle_fences_start_stop_and_never_calls_source_authentication_or_acl() {
    let publisher = PublishHandle::new();
    assert_eq!(
        publisher
            .publish(message("a", 1, true))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    let access = Arc::new(RejectSource::default());
    let broker = Broker::start(
        BrokerConfig {
            publisher: Some(publisher.clone()),
            access_control: access.clone(),
            ..BrokerConfig::default()
        },
        Arc::new(SystemClock),
        None,
    )
    .await
    .unwrap();
    publisher.publish(message("a", 1, true)).await.unwrap();
    assert_eq!(
        broker.retained_for(&[("a".into(), 2)])[0].payload,
        b"downlink"
    );
    assert_eq!(access.0.load(Ordering::Relaxed), 0);
    assert_eq!(broker.actor_count(), 0);
    broker.shutdown().await.unwrap();
    assert_eq!(
        publisher
            .publish(message("a", 0, false))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
}
#[tokio::test]
async fn input_validation_and_budgets_precede_all_side_effects() {
    let publisher = PublishHandle::new();
    let broker = Broker::start(
        BrokerConfig {
            publisher: Some(publisher.clone()),
            max_packet_size: 32,
            ..BrokerConfig::default()
        },
        Arc::new(SystemClock),
        None,
    )
    .await
    .unwrap();
    for topic in ["", "a/+", "a/#", "bad\0topic", "a\u{ffff}"] {
        assert_eq!(
            publisher
                .publish(message(topic, 0, false))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    for topic in ["$SYS", "$SYS/events/client/connected"] {
        assert_eq!(
            publisher
                .publish(message(topic, 0, false))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
    }
    let mut oversized = message("a", 1, true);
    oversized.payload = vec![0; 33];
    assert_eq!(
        publisher.publish(oversized).await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert!(broker.retained_for(&[("#".into(), 2)]).is_empty());
    publisher
        .publish(message("$SYSfoo", 0, false))
        .await
        .unwrap();
    let slots = publisher
        .inner
        .slots
        .clone()
        .acquire_many_owned(32)
        .await
        .unwrap();
    assert_eq!(
        publisher
            .publish(message("a", 0, true))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    drop(slots);
    let bytes = publisher
        .inner
        .bytes
        .clone()
        .acquire_many_owned(16 * 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(
        publisher
            .publish(message("a", 0, true))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    drop(bytes);
    assert!(broker.retained_for(&[("#".into(), 2)]).is_empty());
    assert_eq!(publisher.inner.slots.available_permits(), 32);
    broker.shutdown().await.unwrap();
}
#[tokio::test]
async fn qos_retained_offline_and_broker_origin_reuse_existing_session_path() {
    let database = tests::Database::new();
    let publisher = PublishHandle::new();
    let config = BrokerConfig {
        publisher: Some(publisher.clone()),
        ..database.config()
    };
    let (worker, task) = tests::worker(config).await;
    tests::connect(&worker, "receiver", 1, false).await;
    tests::subscribe(&worker, "receiver", 1).await;
    for qos in 0..=2 {
        publisher.publish(message("a", qos, false)).await.unwrap();
        let packet = tests::poll(&worker, "receiver", 1).await.unwrap();
        let Packet::Publish {
            qos: granted,
            packet_id,
            ref payload,
            ..
        } = packet
        else {
            panic!("not publish")
        };
        assert_eq!(granted, qos.min(1));
        assert_eq!(payload, b"downlink");
        if let Some(packet_id) = packet_id {
            tests::handle(&worker, "receiver", 1, Packet::PubAck { packet_id })
                .await
                .unwrap();
        }
    }
    worker
        .request(Operation::Disconnect {
            client: "receiver".into(),
            generation: 1,
            graceful: true,
        })
        .await
        .unwrap();
    publisher.publish(message("a", 1, true)).await.unwrap();
    tests::stop(worker, task).await;
    let publisher = PublishHandle::new();
    let (worker, task) = tests::worker(BrokerConfig {
        publisher: Some(publisher.clone()),
        ..database.config()
    })
    .await;
    assert!(tests::connect(&worker, "receiver", 2, false).await.0);
    assert!(
        matches!(tests::poll(&worker, "receiver", 2).await, Some(Packet::Publish { ref payload, .. }) if payload == b"downlink")
    );
    assert_eq!(
        worker.broker.retained_for(&[("a".into(), 2)])[0].payload,
        b"downlink"
    );
    let mut deletion = message("a", 1, true);
    deletion.payload.clear();
    publisher.publish(deletion).await.unwrap();
    assert!(worker.broker.retained_for(&[("a".into(), 2)]).is_empty());
    tests::stop(worker, task).await;
}

type Observation = (String, Vec<u8>, String, u8, bool);
#[derive(Debug)]
struct Observer(mpsc::Sender<Observation>);
impl crate::PublicationObserver for Observer {
    fn observe(&self, topic: &str, payload: &[u8], source: &str, qos: u8, retain: bool) {
        let _ = self
            .0
            .try_send((topic.into(), payload.into(), source.into(), qos, retain));
    }
}
async fn connect_v5(worker: &Worker, client: &str, generation: u64) {
    let (wake, _) = watch::channel(0);
    let (cancel, _) = watch::channel(false);
    let (close_reason, _) = watch::channel(None);
    assert!(matches!(
        worker
            .request(Operation::ConnectFrame {
                frame: Frame::v5(Packet::Connect {
                    client_id: client.into(),
                    clean_session: true,
                    keep_alive: 0,
                    will: None,
                    username: None,
                    password: None
                }),
                generation,
                wake,
                cancel,
                close_reason,
            })
            .await
            .unwrap(),
        Reply::Connected(_)
    ));
}
#[tokio::test]
async fn broker_origin_preserves_no_local_shared_delivery_and_observer_identity() {
    let publisher = PublishHandle::new();
    let (sender, mut observations) = mpsc::channel(8);
    let (worker, task) = tests::worker(BrokerConfig {
        publisher: Some(publisher.clone()),
        publication_observer: Some(Arc::new(Observer(sender))),
        ..BrokerConfig::default()
    })
    .await;
    connect_v5(&worker, "http", 1).await;
    connect_v5(&worker, "shared", 2).await;
    let mut subscription = Frame::v5(Packet::Subscribe {
        packet_id: 1,
        filters: vec![("a".into(), 2)],
    });
    subscription.subscription_options = vec![6]; // QoS2 + No Local.
    worker
        .request(Operation::HandleFrameAt {
            client: "http".into(),
            generation: 1,
            received_at_millis: 0,
            frame: subscription,
        })
        .await
        .unwrap();
    worker
        .request(Operation::HandleFrameAt {
            client: "shared".into(),
            generation: 2,
            received_at_millis: 0,
            frame: Frame::v5(Packet::Subscribe {
                packet_id: 1,
                filters: vec![("$share/group/a".into(), 1)],
            }),
        })
        .await
        .unwrap();
    publisher.publish(message("a", 2, false)).await.unwrap();
    assert_eq!(
        observations.try_recv().unwrap(),
        ("a".into(), b"downlink".to_vec(), "http".into(), 2, false)
    );
    assert!(observations.try_recv().is_err());
    for (client, generation, qos) in [("http", 1, 2), ("shared", 2, 1)] {
        let Reply::DeliveryPolled(Some(delivery)) = worker
            .request(Operation::PollDelivery {
                client: client.into(),
                generation,
            })
            .await
            .unwrap()
        else {
            panic!("publication missing")
        };
        assert!(
            matches!(delivery.frame.packet, Packet::Publish { qos: actual, ref payload, .. } if actual==qos && payload==b"downlink")
        );
    }
    assert_eq!(worker.broker.actor_count(), 2);
    tests::stop(worker, task).await;
}
#[tokio::test]
async fn packet_size_boundaries_are_rejected() {
    for (mode, payload_size) in [
        (crate::ProtocolMode::V311, 27),
        (crate::ProtocolMode::Both, 26),
    ] {
        let publisher = PublishHandle::new();
        let broker = Broker::start(
            BrokerConfig {
                publisher: Some(publisher.clone()),
                protocol_mode: mode,
                max_packet_size: 32,
                ..BrokerConfig::default()
            },
            Arc::new(SystemClock),
            None,
        )
        .await
        .unwrap();
        let mut boundary = message("a", 0, false);
        boundary.payload.resize(payload_size, 0);
        publisher.publish(boundary).await.unwrap();
        let mut overflow = message("a", 0, false);
        overflow.payload.resize(payload_size + 1, 0);
        assert_eq!(
            publisher.publish(overflow).await.unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        broker.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn caller_cancellation_releases_ingress_budget_but_not_storage_commit_ownership() {
    let database = tests::Database::new();
    let publisher = PublishHandle::new();
    let broker = Broker::start(
        BrokerConfig {
            publisher: Some(publisher.clone()),
            ..database.config()
        },
        Arc::new(SystemClock),
        None,
    )
    .await
    .unwrap();
    // Hold an independent real writer transaction so retained durability remains
    // pending after its in-memory side effect. No production blocking IO is added.
    let writer = rusqlite::Connection::open(&database.0).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    let publishing = publisher.clone();
    let task = tokio::spawn(async move { publishing.publish(message("a", 1, true)).await });
    timeout(Duration::from_secs(2), async {
        while broker.retained_for(&[("a".into(), 2)]).is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(publisher.inner.slots.available_permits(), 31);
    task.abort();
    let _ = task.await;
    assert_eq!(publisher.inner.slots.available_permits(), 32);
    assert_eq!(publisher.inner.bytes.available_permits(), 16 * 1024 * 1024);
    assert_eq!(broker.storage_jobs(), 1);
    writer.execute_batch("COMMIT").unwrap();
    timeout(Duration::from_secs(2), async {
        while broker.storage_jobs() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        broker.retained_for(&[("a".into(), 2)])[0].payload,
        b"downlink"
    );
    broker.shutdown().await.unwrap();
}
