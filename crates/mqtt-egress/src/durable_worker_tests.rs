use super::*;
use mqtt_egress_log::{Binding, NewConsumer, NewResult, QueueOptions};
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
        q.append(NewResult {
            stream_id: "opaque-stream".into(),
            client_id: "client-a".into(),
            payload,
            consumers: vec![NewConsumer {
                outbound_id: "opaque-destination".into(),
                revision,
                delivery_id: "opaque-delivery".into(),
            }],
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
        codec: crate::result_wire::Codec::Opaque,
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
        codec: crate::result_wire::Codec::Opaque,
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
            queue.append(NewResult {
                stream_id: "opaque-stream".into(),
                client_id: "future".into(),
                payload: vec![255],
                consumers: vec![NewConsumer {
                    outbound_id: id,
                    revision,
                    delivery_id: "future-delivery".into(),
                }],
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
    // Read-only barrier ensures the actor has published the last retry epoch
    // before workers subscribe; do not depend on a reply/notification race.
    q.heads("opaque-destination".into(), None, 1).await.unwrap();
    let startup_queries = q.queries();
    let (sent, mut received) = mpsc::channel(4);
    let sink = Arc::new(OpaqueSink {
        attempts: AtomicUsize::new(0),
        fail_first: false,
        sent,
    });
    let (cancel, rx) = watch::channel(false);
    let context = Arc::new(Context {
        codec: crate::result_wire::Codec::Opaque,
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
        while q.queries() < startup_queries + 4 {
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
        codec: crate::result_wire::Codec::Opaque,
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
        codec: crate::result_wire::Codec::Opaque,
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

struct LaneSink {
    entered: mpsc::Sender<Vec<Vec<u8>>>,
    release: Arc<Semaphore>,
}
impl Sink for LaneSink {
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
            self.entered
                .send(events.iter().map(|v| v.to_vec()).collect())
                .await
                .unwrap();
            if events[0] == b"held" {
                self.release.acquire().await.unwrap().forget();
            }
            Ok(())
        })
    }
}
async fn shared_append(q: Arc<Queue>, stream: &str, payload: Vec<u8>, consumers: Vec<NewConsumer>) {
    let record = NewResult {
        stream_id: stream.into(),
        client_id: "client-a".into(),
        payload,
        consumers,
    };
    tokio::task::spawn_blocking(move || q.append(record))
        .await
        .unwrap()
        .unwrap();
}
fn reference(outbound: &str, revision: u64, id: &str) -> NewConsumer {
    NewConsumer {
        outbound_id: outbound.into(),
        revision,
        delivery_id: id.into(),
    }
}
#[tokio::test]
async fn separate_rule_streams_do_not_mix_batches_or_block_one_another() {
    let f = Fixture::new();
    let counters = Arc::new(Counters::default());
    let stopped = Arc::new(AtomicBool::new(false));
    let (q, revision) = queue(&f, counters.clone(), stopped.clone()).await;
    shared_append(
        q.clone(),
        "rule-a",
        b"held".to_vec(),
        vec![reference("opaque-destination", revision, "a-0")],
    )
    .await;
    shared_append(
        q.clone(),
        "rule-b",
        b"free".to_vec(),
        vec![reference("opaque-destination", revision, "b-0")],
    )
    .await;
    let (entered, mut received) = mpsc::channel(8);
    let release = Arc::new(Semaphore::new(0));
    let sink = Arc::new(LaneSink {
        entered,
        release: release.clone(),
    });
    let stats = Arc::new(ActionCounters::default());
    let (cancel, rx) = watch::channel(false);
    let mut policy = action();
    policy.batch_size = 64;
    policy.timeout_ms = 5000;
    let context = Arc::new(Context {
        queue: q.clone(),
        global: Arc::new(Semaphore::new(16)),
        counters,
        stopped,
        codec: crate::result_wire::Codec::Opaque,
    });
    let task = spawn(policy, sink, stats.clone(), context, rx);
    let mut initial = Vec::new();
    for _ in 0..2 {
        initial.push(
            tokio::time::timeout(Duration::from_secs(2), received.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert!(initial.iter().all(|batch| batch.len() == 1));
    assert!(initial.contains(&vec![b"held".to_vec()]));
    assert!(initial.contains(&vec![b"free".to_vec()]));
    tokio::time::timeout(Duration::from_secs(2), async {
        while q.stats().pending_records != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    shared_append(
        q.clone(),
        "rule-a",
        b"tail".to_vec(),
        vec![reference("opaque-destination", revision, "a-1")],
    )
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(40), received.recv())
            .await
            .is_err()
    );
    release.add_permits(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), received.recv())
            .await
            .unwrap()
            .unwrap(),
        vec![b"tail".to_vec()]
    );
    drained(&q).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while stats.delivered.load(Ordering::Relaxed) != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancel.send_replace(true);
    task.await.unwrap();
    assert_eq!(q.stats().pending_results, 0);
    assert_eq!(stats.delivered.load(Ordering::Relaxed), 3);
    q.close().await;
}

#[tokio::test]
async fn one_consumer_completion_retains_shared_body_for_another_after_reopen() {
    let f = Fixture::new();
    let counters = Arc::new(Counters::default());
    let stopped = Arc::new(AtomicBool::new(false));
    let (q, revision) = queue(&f, counters.clone(), stopped.clone()).await;
    let revisions = q
        .bind(vec![
            Binding {
                outbound_id: "opaque-destination".into(),
                descriptor: vec![0, 255, 1, 42],
            },
            Binding {
                outbound_id: "other".into(),
                descriptor: vec![9],
            },
        ])
        .await
        .unwrap();
    let payload = vec![0, 255, 99];
    shared_append(
        q.clone(),
        "shared-rule",
        payload.clone(),
        vec![
            reference("opaque-destination", revision, "first-consumer"),
            reference("other", revisions["other"], "second-consumer"),
        ],
    )
    .await;
    assert_eq!(q.stats().pending_results, 1);
    assert_eq!(q.stats().pending_records, 2);
    assert_eq!(q.stats().pending_bytes, 3);
    let first = q
        .heads("opaque-destination".into(), None, 1)
        .await
        .unwrap()
        .remove(0);
    let second = q.heads("other".into(), None, 1).await.unwrap().remove(0);
    assert_eq!(first.result_id, second.result_id);
    q.begin(vec![first.id]).await.unwrap();
    q.complete(vec![first.id], Completion::Delivered)
        .await
        .unwrap();
    q.begin(vec![second.id]).await.unwrap();
    q.retry(vec![second.id], 123456).await.unwrap();
    q.close().await;
    drop(q);
    let (q, _) = Queue::open(
        QueueOptions::new(f.0.clone()),
        vec![
            Binding {
                outbound_id: "opaque-destination".into(),
                descriptor: vec![0, 255, 1, 42],
            },
            Binding {
                outbound_id: "other".into(),
                descriptor: vec![9],
            },
        ],
        counters,
        stopped,
    )
    .await
    .unwrap();
    assert_eq!(q.stats().pending_results, 1);
    assert_eq!(q.stats().pending_records, 1);
    assert_eq!(q.pending("opaque-destination"), 0);
    let remaining = q.heads("other".into(), None, 1).await.unwrap().remove(0);
    assert_eq!(remaining.id, second.id);
    assert_eq!(remaining.delivery_id, "second-consumer");
    assert_eq!(remaining.failures, 1);
    assert_eq!(remaining.not_before_ms, 123456);
    assert_eq!(
        q.read(remaining.id, 1, 1024).await.unwrap()[0].payload,
        payload
    );
    q.begin(vec![remaining.id]).await.unwrap();
    q.complete(vec![remaining.id], Completion::Failed)
        .await
        .unwrap();
    assert_eq!(q.stats().pending_results, 0);
    assert_eq!(q.stats().pending_records, 0);
    assert_eq!(q.stats().pending_bytes, 0);
    q.close().await;
}
