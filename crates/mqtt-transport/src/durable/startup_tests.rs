use super::*;
use mqtt_cluster_core::{ClusterBridge, ClusterFuture, ClusterMessage, LocalBroker};

#[derive(Debug)]
struct WaitingBridge {
    entered: tokio::sync::Notify,
}
impl ClusterBridge for WaitingBridge {
    fn startup_timeout(&self) -> Duration {
        Duration::from_secs(625)
    }
    fn ready(&self) -> bool {
        false
    }
    fn open(&self, _: Arc<dyn LocalBroker>) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            self.entered.notify_one();
            std::future::pending().await
        })
    }
    fn claim(&self, _: String, _: u64) -> ClusterFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn release(&self, _: String, _: u64) -> ClusterFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn subscriptions(&self, _: String, _: u64, _: Vec<String>) -> ClusterFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn forward(&self, _: ClusterMessage) -> ClusterFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn shutdown(&self) -> ClusterFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn pending_cluster_startup_survives_forwarding_deadline_and_cancels() {
    let bridge = Arc::new(WaitingBridge {
        entered: tokio::sync::Notify::new(),
    });
    let config = BrokerConfig {
        cluster: Some(bridge.clone()),
        ..BrokerConfig::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (shutdown, stopped) = watch::channel(false);
    let task = tokio::spawn(serve(listener, config, stopped));
    timeout(Duration::from_secs(2), bridge.entered.notified())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5500)).await;
    assert!(
        !task.is_finished(),
        "startup incorrectly inherited forwarding deadline"
    );
    shutdown.send(true).unwrap();
    timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn pending_cluster_startup_cancels_before_forwarding_deadline() {
    let bridge = Arc::new(WaitingBridge {
        entered: tokio::sync::Notify::new(),
    });
    let config = BrokerConfig {
        cluster: Some(bridge.clone()),
        ..BrokerConfig::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (shutdown, stopped) = watch::channel(false);
    let task = tokio::spawn(serve(listener, config, stopped));
    timeout(Duration::from_secs(2), bridge.entered.notified())
        .await
        .unwrap();
    shutdown.send(true).unwrap();
    timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
