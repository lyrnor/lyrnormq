use super::*;
use mqtt_egress_log::{Binding, NewConsumer, NewResult, QueueOptions};
struct Fixture(std::path::PathBuf);
impl Fixture {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
                "rmqtt-kafka-state-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
async fn open(f: &Fixture, counters: Arc<Counters>, stopped: Arc<AtomicBool>) -> (Arc<Queue>, u64) {
    let (q, r) = Queue::open(
        QueueOptions::new(f.0.clone()),
        vec![Binding {
            outbound_id: "send".into(),
            descriptor: vec![1],
        }],
        counters,
        stopped,
    )
    .await
    .unwrap();
    (q, r["send"])
}
async fn append(q: Arc<Queue>, revision: u64) {
    tokio::task::spawn_blocking(move || {
        q.append(NewResult {
            stream_id: "rule".into(),
            client_id: "client".into(),
            payload: vec![1, 2, 3],
            consumers: vec![NewConsumer {
                outbound_id: "send".into(),
                revision,
                delivery_id: "stable-delivery".into(),
            }],
        })
    })
    .await
    .unwrap()
    .unwrap();
}
fn action() -> Outbound {
    Outbound {
        id: "send".into(),
        name: "Send".into(),
        enabled: true,
        endpoint: crate::Endpoint::Webhook(mqtt_webhook::WebhookConfig {
            url: "http://127.0.0.1:1".into(),
            headers: BTreeMap::new(),
        }),
        delivery: crate::DeliveryPolicy {
            concurrency: 2,
            batch_size: 1,
            batch_wait_ms: 0,
            queue_capacity: 8,
            timeout_ms: 100,
            max_attempts: 2,
            retry_base_ms: 10,
        },
    }
}
fn context(q: Arc<Queue>, counters: Arc<Counters>, stopped: Arc<AtomicBool>) -> Arc<Context> {
    Arc::new(Context {
        queue: q,
        global: Arc::new(Semaphore::new(16)),
        counters,
        stopped,
        codec: crate::result_wire::Codec::Opaque,
    })
}
struct NativeDeadline {
    calls: AtomicUsize,
}
use std::sync::atomic::AtomicUsize;
impl Sink for NativeDeadline {
    fn limits(&self) -> mqtt_egress_core::BatchLimits {
        mqtt_egress_core::BatchLimits {
            max_bytes: 1024,
            max_events: 1,
            overhead: 0,
            separator_bytes: 0,
        }
    }
    fn manages_timeout(&self) -> bool {
        true
    }
    fn send<'a>(
        &'a self,
        _: &'a [&'a [u8]],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), mqtt_egress_core::SendError>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(180)).await;
            Ok(())
        })
    }
}
#[tokio::test]
async fn native_delivery_deadline_is_not_cancelled_by_outer_http_timeout() {
    let f = Fixture::new();
    let c = Arc::new(Counters::default());
    let stopped = Arc::new(AtomicBool::new(false));
    let (q, r) = open(&f, c.clone(), stopped.clone()).await;
    append(q.clone(), r).await;
    let sink = Arc::new(NativeDeadline {
        calls: AtomicUsize::new(0),
    });
    let stats = Arc::new(ActionCounters::default());
    let (cancel, rx) = watch::channel(false);
    let task = spawn(
        action(),
        sink.clone(),
        stats.clone(),
        context(q.clone(), c, stopped),
        rx,
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while q.stats().pending_records > 0 || stats.delivered.load(Ordering::Relaxed) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancel.send_replace(true);
    task.await.unwrap();
    assert_eq!(sink.calls.load(Ordering::Relaxed), 1);
    assert_eq!(stats.delivered.load(Ordering::Relaxed), 1);
    assert_eq!(stats.retried.load(Ordering::Relaxed), 0);
    q.close().await;
}
struct AcceptedThenLocalFailure {
    queue: Arc<Queue>,
    accepted: tokio::sync::mpsc::Sender<()>,
}
impl Sink for AcceptedThenLocalFailure {
    fn manages_timeout(&self) -> bool {
        true
    }
    fn limits(&self) -> mqtt_egress_core::BatchLimits {
        mqtt_egress_core::BatchLimits {
            max_bytes: 1024,
            max_events: 1,
            overhead: 0,
            separator_bytes: 0,
        }
    }
    fn send<'a>(
        &'a self,
        _: &'a [&'a [u8]],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), mqtt_egress_core::SendError>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.accepted.send(()).await.unwrap();
            self.queue.close().await;
            Ok(())
        })
    }
}
#[tokio::test]
async fn accepted_remote_delivery_with_failed_local_complete_replays_stable_identifier() {
    let f = Fixture::new();
    let c = Arc::new(Counters::default());
    let stopped = Arc::new(AtomicBool::new(false));
    let (q, r) = open(&f, c.clone(), stopped.clone()).await;
    append(q.clone(), r).await;
    let head = q.heads("send".into(), None, 1).await.unwrap().remove(0);
    let (sent, mut got) = tokio::sync::mpsc::channel(1);
    let sink = Arc::new(AcceptedThenLocalFailure {
        queue: q.clone(),
        accepted: sent,
    });
    let stats = Arc::new(ActionCounters::default());
    let (cancel, rx) = watch::channel(false);
    let task = spawn(
        action(),
        sink,
        stats.clone(),
        context(q.clone(), c.clone(), stopped.clone()),
        rx,
    );
    tokio::time::timeout(Duration::from_secs(3), got.recv())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    cancel.send_replace(true);
    assert_eq!(stats.delivered.load(Ordering::Relaxed), 0);
    drop(q);
    let (q, _) = open(&f, c.clone(), Arc::new(AtomicBool::new(false))).await;
    let recovered = q.heads("send".into(), None, 1).await.unwrap().remove(0);
    assert_eq!(recovered.id, head.id);
    assert_eq!(recovered.delivery_id, "stable-delivery");
    assert!(recovered.inflight);
    assert_eq!(recovered.failures, 0);
    let replay = Arc::new(NativeDeadline {
        calls: AtomicUsize::new(0),
    });
    let stats = Arc::new(ActionCounters::default());
    let (cancel, rx) = watch::channel(false);
    let task = spawn(
        action(),
        replay.clone(),
        stats.clone(),
        context(q.clone(), c, Arc::new(AtomicBool::new(false))),
        rx,
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while q.stats().pending_records > 0 || stats.delivered.load(Ordering::Relaxed) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancel.send_replace(true);
    task.await.unwrap();
    assert_eq!(replay.calls.load(Ordering::Relaxed), 1);
    assert_eq!(stats.delivered.load(Ordering::Relaxed), 1);
    q.close().await;
}

struct RetryClient {
    attempts: AtomicUsize,
    started: tokio::sync::mpsc::Sender<Vec<u8>>,
}
impl Sink for RetryClient {
    fn limits(&self) -> mqtt_egress_core::BatchLimits {
        mqtt_egress_core::BatchLimits {
            max_bytes: 1024,
            max_events: 1,
            overhead: 0,
            separator_bytes: 0,
        }
    }
    fn manages_timeout(&self) -> bool {
        true
    }
    fn send<'a>(
        &'a self,
        events: &'a [&'a [u8]],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), mqtt_egress_core::SendError>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.started.send(events[0].to_vec()).await.unwrap();
            if events[0] == b"a1" && self.attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                Err(mqtt_egress_core::SendError {
                    kind: FailureKind::Retryable,
                    code: "retry".into(),
                })
            } else {
                Ok(())
            }
        })
    }
}
#[tokio::test]
async fn durable_client_retry_blocks_its_successor_but_other_client_progresses() {
    let f = Fixture::new();
    let counters = Arc::new(Counters::default());
    let stopped = Arc::new(AtomicBool::new(false));
    let (q, revision) = open(&f, counters.clone(), stopped.clone()).await;
    let queue = q.clone();
    tokio::task::spawn_blocking(move || {
        for (client, payload) in [("A", b"a1"), ("A", b"a2"), ("B", b"b1")] {
            queue
                .append(NewResult {
                    stream_id: "rule".into(),
                    client_id: client.into(),
                    payload: payload.to_vec(),
                    consumers: vec![NewConsumer {
                        outbound_id: "send".into(),
                        revision,
                        delivery_id: format!("{client}-{}", String::from_utf8_lossy(payload)),
                    }],
                })
                .unwrap();
        }
    })
    .await
    .unwrap();
    let (started, mut received) = tokio::sync::mpsc::channel(8);
    let sink = Arc::new(RetryClient {
        attempts: AtomicUsize::new(0),
        started,
    });
    let stats = Arc::new(ActionCounters::default());
    let mut policy = action();
    policy.retry_base_ms = 500;
    let ctx = context(q.clone(), counters, stopped);
    let ctx = Arc::new(Context {
        global: Arc::new(Semaphore::new(1)),
        queue: ctx.queue.clone(),
        counters: ctx.counters.clone(),
        stopped: ctx.stopped.clone(),
        codec: ctx.codec,
    });
    let (cancel, rx) = watch::channel(false);
    let task = spawn(policy, sink, stats.clone(), ctx, rx);
    let mut first = vec![
        received.recv().await.unwrap(),
        received.recv().await.unwrap(),
    ];
    first.sort();
    assert_eq!(first, vec![b"a1".to_vec(), b"b1".to_vec()]);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), received.recv())
            .await
            .is_err()
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), received.recv())
            .await
            .unwrap()
            .unwrap(),
        b"a1"
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), received.recv())
            .await
            .unwrap()
            .unwrap(),
        b"a2"
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while q.stats().pending_records != 0 || stats.delivered.load(Ordering::Relaxed) != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancel.send_replace(true);
    task.await.unwrap();
    assert_eq!(stats.delivered.load(Ordering::Relaxed), 3);
    assert_eq!(stats.retried.load(Ordering::Relaxed), 1);
    q.close().await;
}

struct ShutdownGate {
    gate: Arc<Semaphore>,
    entered: watch::Sender<bool>,
}
impl Sink for ShutdownGate {
    fn limits(&self) -> mqtt_egress_core::BatchLimits {
        mqtt_egress_core::BatchLimits {
            max_bytes: 1024,
            max_events: 1,
            overhead: 0,
            separator_bytes: 0,
        }
    }
    fn send<'a>(
        &'a self,
        _: &'a [&'a [u8]],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), mqtt_egress_core::SendError>> + Send + 'a>,
    > {
        Box::pin(async { Ok(()) })
    }
    fn shutdown(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), mqtt_egress_core::SendError>> + Send + '_>,
    > {
        Box::pin(async move {
            self.entered.send_replace(true);
            let _permit = self.gate.acquire().await.unwrap();
            Ok(())
        })
    }
}
#[tokio::test]
async fn generation_finish_waits_for_native_cleanup_before_returning() {
    let (cancel, _) = watch::channel(false);
    let (entered, mut seen) = watch::channel(false);
    let gate = Arc::new(Semaphore::new(0));
    let generation = Arc::new(Generation {
        cancel,
        targets: BTreeMap::new(),
        tasks: Mutex::new(Vec::new()),
        sinks: Mutex::new(vec![Arc::new(ShutdownGate {
            gate: gate.clone(),
            entered,
        })]),
    });
    let finishing = generation.clone();
    let task = tokio::spawn(async move { finishing.finish().await });
    seen.wait_for(|v| *v).await.unwrap();
    assert!(!task.is_finished());
    gate.add_permits(1);
    task.await.unwrap().unwrap();
}
