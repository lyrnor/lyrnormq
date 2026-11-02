use super::*;
use mqtt_egress_log::{Binding, NewRecord, QueueOptions};
use std::sync::atomic::AtomicUsize;
use tokio::sync::mpsc;

struct OpaqueSink {
    attempts: AtomicUsize,
    fail_first: bool,
    sent: mpsc::Sender<Vec<Vec<u8>>>,
}
impl Sink for OpaqueSink {
    fn limits(&self) -> mqtt_egress_core::BatchLimits {
        mqtt_egress_core::BatchLimits {
            max_bytes: 1024,
            max_events: 64,
            overhead: 0,
            separator_bytes: 0,
        }
    }
    fn send<'a>(
        &'a self,
        events: &'a [&'a [u8]],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), mqtt_egress_core::SendError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let first = self.attempts.fetch_add(1, Ordering::Relaxed) == 0;
            self.sent
                .send(events.iter().map(|e| e.to_vec()).collect())
                .await
                .unwrap();
            if first && self.fail_first {
                Err(mqtt_egress_core::SendError {
                    kind: FailureKind::Retryable,
                    code: "opaque-retry".into(),
                })
            } else {
                Ok(())
            }
        })
    }
}
static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
struct Fixture(std::path::PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "rmqtt-generic-durable-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn action() -> Outbound {
    Outbound {
        id: "opaque-destination".into(),
        name: "Opaque".into(),
        enabled: true,
        endpoint: crate::Endpoint::Webhook(mqtt_webhook::WebhookConfig {
            url: "http://127.0.0.1:1/unused".into(),
            headers: BTreeMap::new(),
        }),
        delivery: crate::DeliveryPolicy {
            concurrency: 2,
            batch_size: 1,
            batch_wait_ms: 0,
            queue_capacity: 4,
            timeout_ms: 100,
            max_attempts: 2,
            retry_base_ms: 10,
        },
    }
}
async fn queue(
    f: &Fixture,
    counters: Arc<Counters>,
    stopped: Arc<AtomicBool>,
) -> (Arc<Queue>, u64) {
    let (q, revisions) = Queue::open(
        QueueOptions::new(f.0.clone()),
        vec![Binding {
            outbound_id: "opaque-destination".into(),
            descriptor: vec![0, 255, 1, 42],
        }],
        counters,
        stopped,
    )
    .await
    .unwrap();
    let rev = revisions["opaque-destination"];
    (q, rev)
}
async fn append(q: Arc<Queue>, revision: u64, payload: Vec<u8>) {
    tokio::task::spawn_blocking(move || {
        q.append(NewRecord {
            outbound_id: "opaque-destination".into(),
            revision,
            client_id: "client-a".into(),
            payload,
        })
    })
    .await
    .unwrap()
    .unwrap();
}
async fn drained(q: &Queue) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while q.stats().pending_records != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn generic_durable_sink_retries_raw_opaque_bytes_and_commits_completion() {
    let f = Fixture::new();
    let counters = Arc::new(Counters::default());
    let stopped = Arc::new(AtomicBool::new(false));
    let (q, revision) = queue(&f, counters.clone(), stopped.clone()).await;
    let payload = vec![0, 255, 13, 10, 0, 128];
    append(q.clone(), revision, payload.clone()).await;
    let (sent, mut received) = mpsc::channel(4);
    let sink = Arc::new(OpaqueSink {
        attempts: AtomicUsize::new(0),
        fail_first: true,
        sent,
    });
    let stats = Arc::new(ActionCounters::default());
    let (cancel, rx) = watch::channel(false);
    let context = Arc::new(Context {
        queue: q.clone(),
        global: Arc::new(Semaphore::new(16)),
        counters: counters.clone(),
        stopped,
    });
    let task = spawn(action(), sink, stats.clone(), context, rx);
    for _ in 0..2 {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), received.recv())
                .await
                .unwrap()
                .unwrap(),
            vec![payload.clone()]
        );
    }
    drained(&q).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while stats.delivered.load(Ordering::Relaxed) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancel.send_replace(true);
    task.await.unwrap();
    assert_eq!(stats.delivered.load(Ordering::Relaxed), 1);
    assert_eq!(stats.retried.load(Ordering::Relaxed), 1);
    assert_eq!(counters.bytes.load(Ordering::Relaxed), 0);
    q.close().await;
    let (q, _) = queue(&f, counters, Arc::new(AtomicBool::new(false))).await;
    assert_eq!(q.stats().pending_records, 0);
    q.close().await;
}

#[tokio::test]
async fn uncertain_generic_attempt_replays_after_reopen_without_exhausting_budget() {
    let f = Fixture::new();
    let counters = Arc::new(Counters::default());
    let stopped = Arc::new(AtomicBool::new(false));
    let (q, revision) = queue(&f, counters.clone(), stopped.clone()).await;
    let payload = vec![255, 0, 7];
    append(q.clone(), revision, payload.clone()).await;
    let head = q
        .heads("opaque-destination".into(), None, 1)
        .await
        .unwrap()
        .remove(0);
    q.begin(vec![head.id]).await.unwrap();
    q.close().await;
    drop(q);
    let (q, _) = queue(&f, counters.clone(), stopped.clone()).await;
    let (sent, mut received) = mpsc::channel(4);
    let sink = Arc::new(OpaqueSink {
        attempts: AtomicUsize::new(0),
        fail_first: false,
        sent,
    });
    let stats = Arc::new(ActionCounters::default());
    let (cancel, rx) = watch::channel(false);
    let mut policy = action();
    policy.max_attempts = 1;
    let context = Arc::new(Context {
        queue: q.clone(),
        global: Arc::new(Semaphore::new(16)),
        counters,
        stopped,
    });
    let task = spawn(policy, sink, stats.clone(), context, rx);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), received.recv())
            .await
            .unwrap()
            .unwrap(),
        vec![payload]
    );
    drained(&q).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while stats.delivered.load(Ordering::Relaxed) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancel.send_replace(true);
    task.await.unwrap();
    assert_eq!(stats.delivered.load(Ordering::Relaxed), 1);
    assert_eq!(stats.failed.load(Ordering::Relaxed), 0);
    assert_eq!(stats.retried.load(Ordering::Relaxed), 1);
    q.close().await;
}

#[tokio::test]
async fn idle_outbounds_wait_for_mutations_and_future_retry_deadlines_without_polling() {
    let f = Fixture::new();
    let counters = Arc::new(Counters::default());
    let stopped = Arc::new(AtomicBool::new(false));
    let (q, _) = queue(&f, counters.clone(), stopped.clone()).await;
    let revisions = q
        .bind(vec![
            Binding {
                outbound_id: "opaque-destination".into(),
                descriptor: vec![1],
            },
            Binding {
                outbound_id: "second-destination".into(),
                descriptor: vec![2],
            },
        ])
        .await
        .unwrap();
    for id in ["opaque-destination", "second-destination"] {
        let queue = q.clone();
        let revision = revisions[id];
        let id = id.to_owned();
        tokio::task::spawn_blocking(move || {
            queue.append(NewRecord {
                outbound_id: id,
                revision,
                client_id: "future".into(),
                payload: vec![255],
            })
        })
        .await
        .unwrap()
        .unwrap();
    }
    for id in ["opaque-destination", "second-destination"] {
        let head = q.heads(id.into(), None, 1).await.unwrap().remove(0);
        q.begin(vec![head.id]).await.unwrap();
        q.retry(vec![head.id], now_ms() + 60000).await.unwrap();
    }
    let (sent, mut received) = mpsc::channel(4);
    let sink = Arc::new(OpaqueSink {
        attempts: AtomicUsize::new(0),
        fail_first: false,
        sent,
    });
    let (cancel, rx) = watch::channel(false);
    let context = Arc::new(Context {
        queue: q.clone(),
        global: Arc::new(Semaphore::new(16)),
        counters,
        stopped,
    });
    let one = spawn(
        action(),
        sink.clone(),
        Arc::new(ActionCounters::default()),
        context.clone(),
        rx.clone(),
    );
    let mut second = action();
    second.id = "second-destination".into();
    let two = spawn(
        second,
        sink,
        Arc::new(ActionCounters::default()),
        context,
        rx,
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while q.queries() < 6 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let before = q.queries();
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(
        q.queries(),
        before,
        "idle workers repeatedly queried the journal"
    );
    assert!(received.try_recv().is_err());
    cancel.send_replace(true);
    one.await.unwrap();
    two.await.unwrap();
    q.close().await;
}

#[tokio::test]
async fn memory_release_without_journal_mutation_resumes_durable_head() {
    let f = Fixture::new();
    let counters = Arc::new(Counters::default());
    let stopped = Arc::new(AtomicBool::new(false));
    let (q, revision) = queue(&f, counters.clone(), stopped.clone()).await;
    append(q.clone(), revision, vec![255, 0, 1]).await;
    let held = Budget::reserve(&counters, crate::counters::MAX_BYTES, None).unwrap();
    let (sent, mut received) = mpsc::channel(4);
    let sink = Arc::new(OpaqueSink {
        attempts: AtomicUsize::new(0),
        fail_first: false,
        sent,
    });
    let stats = Arc::new(ActionCounters::default());
    let (cancel, rx) = watch::channel(false);
    let context = Arc::new(Context {
        queue: q.clone(),
        global: Arc::new(Semaphore::new(16)),
        counters,
        stopped,
    });
    let task = spawn(action(), sink, stats.clone(), context, rx);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(received.try_recv().is_err());
    assert_eq!(q.stats().pending_records, 1);
    drop(held);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), received.recv())
            .await
            .unwrap()
            .unwrap(),
        vec![vec![255, 0, 1]]
    );
    drained(&q).await;
    cancel.send_replace(true);
    task.await.unwrap();
    q.close().await;
}

struct LargeSink(OpaqueSink);
impl Sink for LargeSink {
    fn limits(&self) -> mqtt_egress_core::BatchLimits {
        mqtt_egress_core::BatchLimits {
            max_bytes: 1024 * 1024,
            max_events: 1,
            overhead: 0,
            separator_bytes: 0,
        }
    }
    fn send<'a>(
        &'a self,
        events: &'a [&'a [u8]],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), mqtt_egress_core::SendError>> + Send + 'a>,
    > {
        self.0.send(events)
    }
}
#[tokio::test]
async fn metadata_release_does_not_spin_when_payload_capacity_is_still_unavailable() {
    let f = Fixture::new();
    let counters = Arc::new(Counters::default());
    let stopped = Arc::new(AtomicBool::new(false));
    let (q, revision) = queue(&f, counters.clone(), stopped.clone()).await;
    let payload = vec![255; 65536];
    append(q.clone(), revision, payload.clone()).await;
    let held = Budget::reserve(&counters, crate::counters::MAX_BYTES - 4096, None).unwrap();
    let (sent, mut received) = mpsc::channel(4);
    let sink = Arc::new(LargeSink(OpaqueSink {
        attempts: AtomicUsize::new(0),
        fail_first: false,
        sent,
    }));
    let stats = Arc::new(ActionCounters::default());
    let (cancel, rx) = watch::channel(false);
    let context = Arc::new(Context {
        queue: q.clone(),
        global: Arc::new(Semaphore::new(16)),
        counters,
        stopped,
    });
    let mut policy = action();
    policy.concurrency = 1;
    let task = spawn(policy, sink, stats, context, rx);
    tokio::time::timeout(Duration::from_secs(1), async {
        while q.queries() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    let queries = q.queries();
    assert!(received.try_recv().is_err());
    drop(held);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), received.recv())
            .await
            .unwrap()
            .unwrap(),
        vec![payload]
    );
    drained(&q).await;
    cancel.send_replace(true);
    task.await.unwrap();
    q.close().await;
    assert!(
        queries <= 8,
        "metadata-only budget releases retriggered {queries} actor queries"
    );
}
