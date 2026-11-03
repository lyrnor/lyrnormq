//! Give final lifecycle publications a bounded opportunity to reach outbounds.
use std::time::Duration;

/// MQTT producers and the lifecycle consumer must already have stopped.
/// This is a best-effort drain; a destination outage cannot prevent shutdown.
pub(crate) async fn drain(runtime: &mqtt_egress::Runtime) {
    let drained = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let status = runtime.status().await;
            if status.queued_events == 0
                && status.inflight_requests == 0
                && status.durable_queue.pending_records == 0
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if drained.is_err() {
        tracing::warn!(
            event = "rules_shutdown_drain_timeout",
            "outbound drain timed out; durable pending deliveries remain recoverable"
        );
    }
}
