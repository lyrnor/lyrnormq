use super::*;
use crate::PublicationObserver;
use mqtt_session::durable::Publication;
#[derive(Debug)]
struct Observer(mpsc::Sender<(String, Vec<u8>, String, u8, bool)>);
impl PublicationObserver for Observer {
    fn observe(&self, topic: &str, payload: &[u8], client_id: &str, qos: u8, retain: bool) {
        let _ = self
            .0
            .try_send((topic.into(), payload.into(), client_id.into(), qos, retain));
    }
}
#[tokio::test]
async fn hook_observes_unsubscribed_publications_but_never_expired_ones() {
    let (tx, mut rx) = mpsc::channel(1);
    let broker = Broker::start(
        BrokerConfig {
            publication_observer: Some(Arc::new(Observer(tx))),
            ..BrokerConfig::default()
        },
        Arc::new(SystemClock),
        None,
    )
    .await
    .unwrap();
    let mut publication = Publication {
        topic: "devices/a".into(),
        payload: b"value".to_vec(),
        qos: 1,
        retain: true,
        properties: vec![],
        expires_at: None,
        publisher: Some("source".into()),
        expired: false,
    };
    assert!(
        broker
            .dispatch(Arc::new(publication.clone()), false)
            .is_none()
    );
    assert_eq!(
        rx.try_recv().unwrap(),
        (
            "devices/a".into(),
            b"value".to_vec(),
            "source".into(),
            1,
            true
        )
    );
    publication.expires_at = Some(1);
    broker.dispatch(Arc::new(publication.clone()), false);
    assert!(rx.try_recv().is_err());
    publication.expired = true;
    broker.dispatch(Arc::new(publication), false);
    assert!(rx.try_recv().is_err());
    broker.shutdown().await.unwrap();
}
#[tokio::test]
async fn full_observer_does_not_fail_publisher_or_require_recipients() {
    let (tx, mut rx) = mpsc::channel(1);
    let (worker, task) = tests::worker(BrokerConfig {
        publication_observer: Some(Arc::new(Observer(tx))),
        ..BrokerConfig::default()
    })
    .await;
    tests::connect(&worker, "publisher", 1, true).await;
    for _ in 0..3 {
        let mut publication = tests::publication(b"message", 0);
        if let Packet::Publish { packet_id, qos, .. } = &mut publication {
            *packet_id = None;
            *qos = 0;
        }
        tests::handle(&worker, "publisher", 1, publication)
            .await
            .unwrap();
    }
    assert_eq!(rx.try_recv().unwrap().2, "publisher");
    assert!(rx.try_recv().is_err());
    tests::stop(worker, task).await;
}
