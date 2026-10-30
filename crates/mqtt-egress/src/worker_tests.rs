use super::*;
use mqtt_egress_core::{BatchLimits, FailureKind, SendError};
use std::sync::atomic::AtomicUsize;
struct Fake {
    calls: AtomicUsize,
    permanent: bool,
}
impl Sink for Fake {
    fn limits(&self) -> BatchLimits {
        BatchLimits {
            max_bytes: 4,
            overhead: 0,
            max_events: 1,
            separator_bytes: 0,
        }
    }
    fn send<'a>(
        &'a self,
        events: &'a [&'a [u8]],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SendError>> + Send + 'a>>
    {
        Box::pin(async move {
            assert_eq!(events.len(), 1);
            assert!(events[0].len() <= 4);
            let n = self.calls.fetch_add(1, Ordering::Relaxed);
            if self.permanent || n == 0 {
                Err(SendError {
                    kind: if self.permanent {
                        FailureKind::Permanent
                    } else {
                        FailureKind::Retryable
                    },
                    code: "fake_failure".into(),
                })
            } else {
                Ok(())
            }
        })
    }
}
#[tokio::test]
async fn generic_worker_obeys_non_http_capabilities_and_error_classes() {
    for permanent in [false, true] {
        let sink = Arc::new(Fake {
            calls: AtomicUsize::new(0),
            permanent,
        });
        let stats = Arc::new(ActionCounters::default());
        let counters = Arc::new(Counters::default());
        let (cancel, rx) = watch::channel(false);
        let action = crate::Outbound {
            id: "fake".into(),
            name: "Fake".into(),
            enabled: true,
            endpoint: crate::Endpoint::Webhook(mqtt_webhook::WebhookConfig {
                url: "http://unused/".into(),
                headers: Default::default(),
            }),
            delivery: crate::DeliveryPolicy {
                batch_size: 50,
                batch_wait_ms: 0,
                queue_capacity: 4,
                timeout_ms: 100,
                max_attempts: 3,
                retry_base_ms: 10,
            },
        };
        let (target, task) = spawn(
            action,
            sink.clone(),
            Arc::new(Semaphore::new(1)),
            stats.clone(),
            rx,
        );
        for _ in 0..2 {
            let budget = Budget::reserve(&counters, 2, Some(stats.clone())).unwrap();
            target
                .sender
                .send(Delivery::new(
                    b"{}".to_vec(),
                    budget,
                    stats.clone(),
                    counters.clone(),
                ))
                .await
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while stats.delivered.load(Ordering::Relaxed) + stats.failed.load(Ordering::Relaxed) < 2
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            sink.calls.load(Ordering::Relaxed),
            if permanent { 2 } else { 3 }
        );
        assert_eq!(
            stats.retried.load(Ordering::Relaxed),
            if permanent { 0 } else { 1 }
        );
        cancel.send_replace(true);
        task.await.unwrap();
    }
}

struct BoundedFake {
    calls: AtomicUsize,
    invalid: bool,
}
impl Sink for BoundedFake {
    fn limits(&self) -> BatchLimits {
        BatchLimits {
            max_bytes: 8,
            overhead: if self.invalid { usize::MAX } else { 0 },
            max_events: if self.invalid { 0 } else { 1 },
            separator_bytes: 0,
        }
    }
    fn send<'a>(
        &'a self,
        _events: &'a [&'a [u8]],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SendError>> + Send + 'a>>
    {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Box::pin(std::future::pending())
    }
}
#[tokio::test]
async fn worker_enforces_time_budget_and_rejects_invalid_adapter_limits() {
    for invalid in [false, true] {
        let sink = Arc::new(BoundedFake {
            calls: AtomicUsize::new(0),
            invalid,
        });
        let stats = Arc::new(ActionCounters::default());
        let counters = Arc::new(Counters::default());
        let (cancel, rx) = watch::channel(false);
        let action = crate::Outbound {
            id: "fake".into(),
            name: "Fake".into(),
            enabled: true,
            endpoint: crate::Endpoint::Webhook(mqtt_webhook::WebhookConfig {
                url: "http://unused/".into(),
                headers: Default::default(),
            }),
            delivery: crate::DeliveryPolicy {
                batch_size: 50,
                batch_wait_ms: 0,
                queue_capacity: 4,
                timeout_ms: 100,
                max_attempts: 2,
                retry_base_ms: 10,
            },
        };
        let (target, task) = spawn(
            action,
            sink.clone(),
            Arc::new(Semaphore::new(1)),
            stats.clone(),
            rx,
        );
        let budget = Budget::reserve(&counters, 2, Some(stats.clone())).unwrap();
        target
            .sender
            .send(Delivery::new(
                b"{}".to_vec(),
                budget,
                stats.clone(),
                counters.clone(),
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while stats.failed.load(Ordering::Relaxed) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            sink.calls.load(Ordering::Relaxed),
            if invalid { 0 } else { 2 }
        );
        assert_eq!(
            stats.retried.load(Ordering::Relaxed),
            if invalid { 0 } else { 1 }
        );
        cancel.send_replace(true);
        task.await.unwrap();
    }
}
