//! Committed actor-state and durable recovery observations.
use super::tests::{Database, connect, handle, publication, stop, subscribe, worker};
use super::*;
use crate::management::{ManagementHandle, Query};
#[tokio::test]
async fn installed_sessions_subscriptions_retained_and_restart_are_observed() {
    let db = Database::new();
    let management = ManagementHandle::new();
    let config = BrokerConfig {
        management: Some(management.clone()),
        ..db.config()
    };
    let (worker, task) = worker(config.clone()).await;
    connect(&worker, "observer", 1, false).await;
    subscribe(&worker, "observer", 1).await;
    let session = management.session("observer").await.unwrap().unwrap();
    assert!(session.online);
    assert_eq!(session.subscriptions, 1);
    assert_eq!(session.protocol, "3.1.1");
    let subscriptions = management.subscriptions(Query::default()).await.unwrap();
    assert_eq!(subscriptions.total, 1);
    let mut retained = publication(b"metadata-only", 2);
    if let Packet::Publish { retain, .. } = &mut retained {
        *retain = true;
    }
    handle(&worker, "observer", 1, retained).await.unwrap();
    assert_eq!(
        management.retained(Query::default()).await.unwrap().items[0].payload_bytes,
        13
    );
    worker
        .request(Operation::Disconnect {
            client: "observer".into(),
            generation: 1,
            graceful: true,
        })
        .await
        .unwrap();
    assert_eq!(management.overview().await.unwrap().offline_sessions, 1);
    stop(worker, task).await;
    assert!(management.overview().await.is_err());
    let recovered = ManagementHandle::new();
    let (worker, task) = super::tests::worker(BrokerConfig {
        management: Some(recovered.clone()),
        ..config
    })
    .await;
    let session = recovered.session("observer").await.unwrap().unwrap();
    assert!(!session.online);
    assert_eq!(session.protocol, "unknown");
    assert_eq!(session.receive_maximum, None);
    assert_eq!(
        recovered
            .subscriptions(Query::default())
            .await
            .unwrap()
            .total,
        1
    );
    assert_eq!(recovered.retained(Query::default()).await.unwrap().total, 1);
    assert_eq!(recovered.overview().await.unwrap().messages_in_total, 0);
    connect(&worker, "observer", 2, false).await;
    assert_eq!(
        recovered
            .session("observer")
            .await
            .unwrap()
            .unwrap()
            .protocol,
        "3.1.1"
    );
    stop(worker, task).await;
}

#[derive(Debug)]
struct ManualClock(std::sync::atomic::AtomicU64);
impl mqtt_session::durable::Clock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}
#[tokio::test]
async fn retained_reads_hide_expiry_without_copying_or_mutating_payloads() {
    let clock = Arc::new(ManualClock(std::sync::atomic::AtomicU64::new(10_000)));
    let management = ManagementHandle::new();
    let broker = Broker::start(
        BrokerConfig {
            management: Some(management.clone()),
            ..BrokerConfig::default()
        },
        clock.clone(),
        None,
    )
    .await
    .unwrap();
    let publication = mqtt_session::durable::Publication {
        topic: "expiry".into(),
        payload: vec![1; 512_000],
        qos: 1,
        retain: true,
        properties: vec![Property::MessageExpiryInterval(1)],
        expires_at: Some(11_000),
        publisher: None,
        expired: false,
    };
    let committed = broker.apply_retained(&publication).await.unwrap();
    broker.retained_durable(committed).await.unwrap();
    let page = management.retained(Query::default()).await.unwrap();
    assert_eq!(page.total, 1);
    assert_eq!(page.items[0].payload_bytes, 512_000);
    clock.0.store(11_000, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        management.retained(Query::default()).await.unwrap().total,
        0
    );
    assert_eq!(management.overview().await.unwrap().retained_messages, 0);
    // A read neither prunes the shared store nor mutates its timers.
    assert_eq!(broker.retained.len(), 1);
    broker.shutdown().await.unwrap();
}
