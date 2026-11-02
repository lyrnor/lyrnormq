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
                concurrency: 1,
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
                .send(Delivery::new(
                    "same-client",
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
                concurrency: 1,
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
            .send(Delivery::new(
                "same-client",
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

struct HeldSink {
    entered: tokio::sync::mpsc::Sender<()>,
    release: Arc<Semaphore>,
}
impl Sink for HeldSink {
    fn limits(&self) -> BatchLimits {
        BatchLimits {
            max_bytes: 1024,
            overhead: 0,
            max_events: 1,
            separator_bytes: 0,
        }
    }
    fn send<'a>(
        &'a self,
        _events: &'a [&'a [u8]],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SendError>> + Send + 'a>>
    {
        Box::pin(async move {
            let _ = self.entered.send(()).await;
            let permit = self.release.acquire().await;
            if let Ok(permit) = permit {
                permit.forget();
            }
            Ok(())
        })
    }
}
fn action(concurrency: usize) -> crate::Outbound {
    crate::Outbound {
        id: "held".into(),
        name: "Held".into(),
        enabled: true,
        endpoint: crate::Endpoint::Webhook(mqtt_webhook::WebhookConfig {
            url: "http://unused/".into(),
            headers: Default::default(),
        }),
        delivery: crate::DeliveryPolicy {
            concurrency,
            batch_size: 1,
            batch_wait_ms: 0,
            queue_capacity: 16,
            timeout_ms: 10000,
            max_attempts: 1,
            retry_base_ms: 10,
        },
    }
}
async fn enqueue(target: &Target, counters: &Arc<Counters>, count: usize) {
    for index in 0..count {
        let budget = Budget::reserve(counters, 2, Some(target.stats.clone())).unwrap();
        target
            .send(Delivery::new(
                &format!("client-{index}"),
                b"{}".to_vec(),
                budget,
                target.stats.clone(),
                counters.clone(),
            ))
            .await
            .unwrap();
    }
}
#[tokio::test]
async fn concurrent_jobs_reach_local_cap_and_cancel_without_budget_leaks() {
    let (entered, mut requests) = tokio::sync::mpsc::channel(64);
    let sink = Arc::new(HeldSink {
        entered,
        release: Arc::new(Semaphore::new(0)),
    });
    let counters = Arc::new(Counters::default());
    let stats = Arc::new(ActionCounters::default());
    let global = Arc::new(Semaphore::new(16));
    let (cancel, rx) = watch::channel(false);
    let (target, task) = spawn(action(3), sink, global.clone(), stats.clone(), rx);
    enqueue(&target, &counters, 8).await;
    for _ in 0..3 {
        tokio::time::timeout(Duration::from_secs(1), requests.recv())
            .await
            .expect("outbound did not reach concurrency")
            .unwrap();
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), requests.recv())
            .await
            .is_err()
    );
    assert_eq!(stats.inflight_requests.load(Ordering::Relaxed), 3);
    assert_eq!(counters.inflight_requests.load(Ordering::Relaxed), 3);
    cancel.send_replace(true);
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stats.dropped.load(Ordering::Relaxed), 8);
    assert_eq!(stats.queued.load(Ordering::Relaxed), 0);
    assert_eq!(counters.bytes.load(Ordering::Relaxed), 0);
    assert_eq!(counters.queued.load(Ordering::Relaxed), 0);
    assert_eq!(global.available_permits(), 16);
    assert_eq!(stats.inflight_requests.load(Ordering::Relaxed), 0);
    assert_eq!(counters.inflight_requests.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn global_attempt_cap_is_shared_and_waiting_jobs_are_not_inflight() {
    let counters = Arc::new(Counters::default());
    let global = Arc::new(Semaphore::new(2));
    let (entered, mut requests) = tokio::sync::mpsc::channel(64);
    let sink = Arc::new(HeldSink {
        entered,
        release: Arc::new(Semaphore::new(0)),
    });
    let mut owners = Vec::new();
    for _ in 0..2 {
        let stats = Arc::new(ActionCounters::default());
        let (cancel, rx) = watch::channel(false);
        let (target, task) = spawn(action(3), sink.clone(), global.clone(), stats.clone(), rx);
        enqueue(&target, &counters, 6).await;
        owners.push((target, task, stats, cancel));
    }
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(1), requests.recv())
            .await
            .unwrap()
            .unwrap();
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), requests.recv())
            .await
            .is_err()
    );
    assert_eq!(counters.inflight_requests.load(Ordering::Relaxed), 2);
    assert_eq!(
        owners
            .iter()
            .map(|(_, _, stats, _)| stats.inflight_requests.load(Ordering::Relaxed))
            .sum::<usize>(),
        2
    );
    for (_, _, _, cancel) in &owners {
        cancel.send_replace(true);
    }
    for (_, task, stats, _) in owners {
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stats.dropped.load(Ordering::Relaxed), 6);
        assert_eq!(stats.inflight_requests.load(Ordering::Relaxed), 0);
    }
    assert_eq!(counters.inflight_requests.load(Ordering::Relaxed), 0);
    assert_eq!(counters.bytes.load(Ordering::Relaxed), 0);
    assert_eq!(counters.queued.load(Ordering::Relaxed), 0);
    assert_eq!(global.available_permits(), 2);
}

struct RetrySink {
    entered: tokio::sync::mpsc::Sender<u8>,
    retries: AtomicUsize,
}
impl Sink for RetrySink {
    fn limits(&self) -> BatchLimits {
        BatchLimits {
            max_bytes: 16,
            max_events: 1,
            overhead: 0,
            separator_bytes: 0,
        }
    }
    fn send<'a>(
        &'a self,
        events: &'a [&'a [u8]],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SendError>> + Send + 'a>>
    {
        Box::pin(async move {
            let value = events[0][0];
            let _ = self.entered.send(value).await;
            if value == b'0' && self.retries.fetch_add(1, Ordering::Relaxed) == 0 {
                Err(SendError {
                    kind: FailureKind::Retryable,
                    code: "retry_me".into(),
                })
            } else {
                Ok(())
            }
        })
    }
}
#[tokio::test]
async fn retry_backoff_releases_global_slot_and_does_not_block_another_job() {
    let (entered, mut requests) = tokio::sync::mpsc::channel(8);
    let sink = Arc::new(RetrySink {
        entered,
        retries: AtomicUsize::new(0),
    });
    let counters = Arc::new(Counters::default());
    let stats = Arc::new(ActionCounters::default());
    let global = Arc::new(Semaphore::new(1));
    let (cancel, rx) = watch::channel(false);
    let mut policy = action(1);
    policy.max_attempts = 2;
    policy.retry_base_ms = 1000;
    let (target, task) = spawn(policy, sink, global.clone(), stats.clone(), rx);
    for value in *b"01" {
        if value == b'1' {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), requests.recv())
                    .await
                    .unwrap()
                    .unwrap(),
                b'0'
            );
            let budget = Budget::reserve(&counters, 1, Some(stats.clone())).unwrap();
            target
                .send(Delivery::new(
                    "client-48",
                    vec![b'2'],
                    budget,
                    stats.clone(),
                    counters.clone(),
                ))
                .await
                .unwrap();
        }
        let budget = Budget::reserve(&counters, 1, Some(stats.clone())).unwrap();
        target
            .send(Delivery::new(
                &format!("client-{value}"),
                vec![value],
                budget,
                stats.clone(),
                counters.clone(),
            ))
            .await
            .unwrap();
    }
    let healthy = tokio::time::timeout(Duration::from_secs(1), requests.recv())
        .await
        .expect("healthy job waited for retry backoff")
        .unwrap();
    assert_eq!(healthy, b'1');
    assert!(
        tokio::time::timeout(Duration::from_millis(30), requests.recv())
            .await
            .is_err(),
        "same-client successor escaped its predecessor retry backoff"
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while stats.delivered.load(Ordering::Relaxed) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(global.available_permits(), 1);
    assert_eq!(stats.inflight_requests.load(Ordering::Relaxed), 0);
    assert_eq!(stats.retried.load(Ordering::Relaxed), 0);
    cancel.send_replace(true);
    task.await.unwrap();
    assert_eq!(stats.dropped.load(Ordering::Relaxed), 2);
    assert_eq!(stats.failed.load(Ordering::Relaxed), 0);
    assert_eq!(counters.bytes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn one_job_drains_accepted_deliveries_sequentially_after_sender_closes() {
    let (entered, mut requests) = tokio::sync::mpsc::channel(8);
    let release = Arc::new(Semaphore::new(0));
    let sink = Arc::new(HeldSink {
        entered,
        release: release.clone(),
    });
    let counters = Arc::new(Counters::default());
    let stats = Arc::new(ActionCounters::default());
    let (_cancel, rx) = watch::channel(false);
    let (target, task) = spawn(
        action(1),
        sink,
        Arc::new(Semaphore::new(16)),
        stats.clone(),
        rx,
    );
    enqueue(&target, &counters, 3).await;
    drop(target);
    for _ in 0..3 {
        tokio::time::timeout(Duration::from_secs(1), requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), requests.recv())
                .await
                .is_err()
        );
        release.add_permits(1);
    }
    task.await.unwrap();
    assert_eq!(stats.delivered.load(Ordering::Relaxed), 3);
    assert_eq!(stats.dropped.load(Ordering::Relaxed), 0);
    assert_eq!(stats.inflight_requests.load(Ordering::Relaxed), 0);
    assert_eq!(counters.bytes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn same_client_does_not_overlap_even_with_concurrency_three() {
    let (entered, mut requests) = tokio::sync::mpsc::channel(8);
    let release = Arc::new(Semaphore::new(0));
    let sink = Arc::new(HeldSink {
        entered,
        release: release.clone(),
    });
    let counters = Arc::new(Counters::default());
    let stats = Arc::new(ActionCounters::default());
    let (cancel, rx) = watch::channel(false);
    let (target, task) = spawn(
        action(3),
        sink,
        Arc::new(Semaphore::new(16)),
        stats.clone(),
        rx,
    );
    for _ in 0..3 {
        let budget = Budget::reserve(&counters, 2, Some(stats.clone())).unwrap();
        target
            .send(Delivery::new(
                "one",
                b"{}".to_vec(),
                budget,
                stats.clone(),
                counters.clone(),
            ))
            .await
            .unwrap();
    }
    requests.recv().await.unwrap();
    let overlap = tokio::time::timeout(Duration::from_millis(40), requests.recv()).await;
    cancel.send_replace(true);
    task.await.unwrap();
    assert!(
        overlap.is_err(),
        "one client's successor started before its predecessor completed"
    );
    assert_eq!(stats.queued.load(Ordering::Relaxed), 0);
    assert_eq!(counters.bytes.load(Ordering::Relaxed), 0);
}

struct FastSink;
impl Sink for FastSink {
    fn limits(&self) -> BatchLimits {
        BatchLimits {
            max_bytes: 1024,
            max_events: 1,
            overhead: 0,
            separator_bytes: 0,
        }
    }
    fn send<'a>(
        &'a self,
        _: &'a [&'a [u8]],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SendError>> + Send + 'a>>
    {
        Box::pin(std::future::ready(Ok(())))
    }
}

#[tokio::test]
async fn immediate_success_client_churn_bounds_completed_jobs_and_lane_registry() {
    let counters = Arc::new(Counters::default());
    let stats = Arc::new(ActionCounters::default());
    let (_cancel, rx) = watch::channel(false);
    let mut policy = action(4);
    policy.queue_capacity = 4;
    let cap = resident_limit(&policy, &FastSink.limits());
    let (target, task) = spawn(
        policy,
        Arc::new(FastSink),
        Arc::new(Semaphore::new(16)),
        stats.clone(),
        rx,
    );
    for index in 0..5000 {
        let client = format!("one-shot-{index}");
        let budget = Budget::reserve(&counters, client.len() + 2, Some(stats.clone())).unwrap();
        target
            .send(Delivery::new(
                &client,
                b"{}".to_vec(),
                budget,
                stats.clone(),
                counters.clone(),
            ))
            .await
            .unwrap();
    }
    drop(target);
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stats.delivered.load(Ordering::Relaxed), 5000);
    assert!(stats.peak_lanes.load(Ordering::Relaxed) > 0);
    assert!(stats.peak_jobs.load(Ordering::Relaxed) > 0);
    assert!(stats.peak_lanes.load(Ordering::Relaxed) <= cap);
    assert!(stats.peak_jobs.load(Ordering::Relaxed) <= cap);
    assert_eq!(counters.bytes.load(Ordering::Relaxed), 0);
    assert_eq!(stats.queued.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn resident_admission_bounds_one_held_client_and_cancel_releases_slots() {
    let counters = Arc::new(Counters::default());
    let stats = Arc::new(ActionCounters::default());
    let (entered, mut requests) = mpsc::channel(8);
    let sink = Arc::new(HeldSink {
        entered,
        release: Arc::new(Semaphore::new(0)),
    });
    let mut policy = action(1);
    policy.queue_capacity = 2;
    let cap = resident_limit(&policy, &sink.limits());
    let (cancel, rx) = watch::channel(false);
    let (target, task) = spawn(
        policy,
        sink,
        Arc::new(Semaphore::new(16)),
        stats.clone(),
        rx,
    );
    for _ in 0..cap {
        let budget = Budget::reserve(&counters, 3, Some(stats.clone())).unwrap();
        target
            .send(Delivery::new(
                "a",
                b"{}".to_vec(),
                budget,
                stats.clone(),
                counters.clone(),
            ))
            .await
            .unwrap();
    }
    requests.recv().await.unwrap();
    assert_eq!(target.resident.available_permits(), 0);
    assert_eq!(stats.inflight_requests.load(Ordering::Relaxed), 1);
    for _ in 0..3 {
        let budget = Budget::reserve(&counters, 3, Some(stats.clone())).unwrap();
        target.try_send(Delivery::new(
            "a",
            b"{}".to_vec(),
            budget,
            stats.clone(),
            counters.clone(),
        ));
    }
    assert_eq!(stats.queued.load(Ordering::Relaxed), cap);
    assert_eq!(stats.dropped.load(Ordering::Relaxed), 3);
    cancel.send_replace(true);
    task.await.unwrap();
    assert_eq!(target.resident.available_permits(), cap);
    assert_eq!(stats.dropped.load(Ordering::Relaxed), cap as u64 + 3);
    assert_eq!(counters.bytes.load(Ordering::Relaxed), 0);
    assert_eq!(stats.queued.load(Ordering::Relaxed), 0);
}

struct PanicSink;
impl Sink for PanicSink {
    fn limits(&self) -> BatchLimits {
        FastSink.limits()
    }
    fn send<'a>(
        &'a self,
        _: &'a [&'a [u8]],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SendError>> + Send + 'a>>
    {
        Box::pin(async { panic!("intentional adapter panic") })
    }
}
#[tokio::test]
async fn panicked_child_stops_scheduler_and_releases_every_lane_and_permit() {
    let counters = Arc::new(Counters::default());
    let stats = Arc::new(ActionCounters::default());
    let (_cancel, rx) = watch::channel(false);
    let policy = action(2);
    let cap = resident_limit(&policy, &PanicSink.limits());
    let global = Arc::new(Semaphore::new(16));
    let (target, task) = spawn(
        policy,
        Arc::new(PanicSink),
        global.clone(),
        stats.clone(),
        rx,
    );
    for _ in 0..6 {
        let budget = Budget::reserve(&counters, 3, Some(stats.clone())).unwrap();
        target.try_send(Delivery::new(
            "a",
            b"{}".to_vec(),
            budget,
            stats.clone(),
            counters.clone(),
        ));
    }
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stats.last_error.lock().unwrap().as_deref(),
        Some("worker_failed")
    );
    assert_eq!(target.resident.available_permits(), cap);
    assert_eq!(global.available_permits(), 16);
    assert_eq!(stats.dropped.load(Ordering::Relaxed), 6);
    assert_eq!(stats.queued.load(Ordering::Relaxed), 0);
    assert_eq!(stats.inflight_requests.load(Ordering::Relaxed), 0);
    assert_eq!(counters.bytes.load(Ordering::Relaxed), 0);
}
