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
