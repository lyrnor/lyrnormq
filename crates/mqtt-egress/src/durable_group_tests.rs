use super::*;
use mqtt_egress_log::NewConsumer;
use std::{
    path::PathBuf,
    sync::atomic::AtomicU64,
    time::{SystemTime, UNIX_EPOCH},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
                "lyrnormq-runtime-group-{}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed),
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
struct Actor {
    fixture: Fixture,
    journal: Journal,
    sender: mpsc::Sender<Command>,
    receiver: mpsc::Receiver<Command>,
    view: Arc<Mutex<View>>,
    changed: watch::Sender<u64>,
    changes: watch::Receiver<u64>,
    stopped: Arc<AtomicBool>,
    counters: Arc<Counters>,
    revision: u64,
}
impl Actor {
    fn new(max_records: usize) -> Self {
        let fixture = Fixture::new();
        let mut options = QueueOptions::new(fixture.0.clone());
        options.max_records = max_records;
        let mut journal = Journal::open(options).unwrap();
        let revision = journal
            .bind_all(&[Binding {
                outbound_id: "x".into(),
                descriptor: vec![1],
            }])
            .unwrap()["x"];
        let view = Arc::new(Mutex::new(View {
            stats: journal.stats(),
            pending: BTreeMap::from([("x".into(), 0)]),
            queries: 0,
        }));
        let (sender, receiver) = mpsc::channel(512);
        let (changed, changes) = watch::channel(0);
        Self {
            fixture,
            journal,
            sender,
            receiver,
            view,
            changed,
            changes,
            stopped: Arc::new(AtomicBool::new(false)),
            counters: Arc::new(Counters::default()),
            revision,
        }
    }
    fn record(&self, n: usize, payload: usize) -> NewResult {
        NewResult {
            stream_id: "rule".into(),
            client_id: "client".into(),
            payload: vec![(n % 251) as u8; payload],
            consumers: vec![NewConsumer {
                outbound_id: "x".into(),
                revision: self.revision,
                delivery_id: format!("delivery-{n}"),
            }],
        }
    }
    fn append(&self, n: usize) -> oneshot::Receiver<Result<(), QueueError>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .try_send(Command::Append(self.record(n, 1), Some(tx), None))
            .unwrap();
        rx
    }
    fn run(self) -> (Fixture, Arc<Mutex<View>>, u64, Arc<Counters>) {
        let Self {
            fixture,
            journal,
            sender,
            receiver,
            view,
            changed,
            changes,
            stopped,
            counters,
            ..
        } = self;
        drop(sender);
        run(
            journal,
            receiver,
            Arc::clone(&view),
            changed,
            stopped,
            Arc::clone(&counters),
        );
        let epoch = *changes.borrow();
        (fixture, view, epoch, counters)
    }
}
#[test]
fn contiguous_appends_larger_than_one_frame_share_one_actor_commit() {
    let a = Actor::new(512);
    let replies: Vec<_> = (0..96).map(|n| a.append(n)).collect();
    let (f, view, epoch, _) = a.run();
    assert_eq!(
        epoch, 1,
        "96 queued appends must be one group, not two durable actor batches"
    );
    for r in replies {
        assert_eq!(r.blocking_recv().unwrap(), Ok(()));
    }
    assert_eq!(view.lock().unwrap().stats.pending_results, 96);
    let reopened = Journal::open(QueueOptions::new(f.0.clone())).unwrap();
    assert_eq!(reopened.stats().pending_records, 96);
    let rows = reopened.read_consumers(2, 64, 1024).unwrap();
    assert_eq!(rows.len(), 64);
    for (n, r) in rows.iter().enumerate() {
        assert_eq!(r.payload, vec![n as u8]);
    }
}
#[test]
fn append_gather_has_a_finite_256_result_boundary() {
    let a = Actor::new(512);
    let replies: Vec<_> = (0..300).map(|n| a.append(n)).collect();
    let (_, view, epoch, _) = a.run();
    assert_eq!(epoch, 2);
    assert_eq!(view.lock().unwrap().stats.pending_results, 300);
    for r in replies {
        assert_eq!(r.blocking_recv().unwrap(), Ok(()));
    }
}
#[test]
fn read_is_an_ordering_barrier_between_contiguous_append_groups() {
    let a = Actor::new(512);
    let mut replies: Vec<_> = (0..32).map(|n| a.append(n)).collect();
    let (tx, read) = oneshot::channel();
    a.sender.try_send(Command::Read(2, 64, 1024, tx)).unwrap();
    replies.extend((32..96).map(|n| a.append(n)));
    let (_, _, epoch, _) = a.run();
    assert_eq!(epoch, 2);
    let rows = read.blocking_recv().unwrap().unwrap();
    assert_eq!(rows.len(), 32);
    for r in replies {
        assert_eq!(r.blocking_recv().unwrap(), Ok(()));
    }
}
#[test]
fn full_group_falls_back_to_per_frame_durable_prefix() {
    let a = Actor::new(64);
    let replies: Vec<_> = (0..96).map(|n| a.append(n)).collect();
    let (f, view, _, counters) = a.run();
    for (n, r) in replies.into_iter().enumerate() {
        assert_eq!(
            r.blocking_recv().unwrap(),
            if n < 64 {
                Ok(())
            } else {
                Err(QueueError::Full)
            }
        );
    }
    assert_eq!(view.lock().unwrap().stats.pending_results, 64);
    assert_eq!(counters.errors.load(Ordering::Relaxed), 1);
    let reopened = Journal::open(QueueOptions::new(f.0.clone())).unwrap();
    assert_eq!(reopened.stats().pending_results, 64);
}
#[test]
fn cancelled_reply_does_not_cancel_durable_append_and_close_remains_a_barrier() {
    let a = Actor::new(512);
    let before = a.append(0);
    drop(before);
    let (tx, closed) = oneshot::channel();
    a.sender.try_send(Command::Close(tx)).unwrap();
    let after = a.append(1);
    let (f, _, epoch, _) = a.run();
    closed.blocking_recv().unwrap();
    assert!(after.blocking_recv().is_err());
    assert_eq!(epoch, 1);
    let reopened = Journal::open(QueueOptions::new(f.0.clone())).unwrap();
    assert_eq!(reopened.stats().pending_results, 1);
}

#[test]
fn full_fallback_releases_admission_and_marks_only_rejected_deliveries_dropped() {
    let a = Actor::new(64);
    let action = Arc::new(ActionCounters::default());
    let mut replies = Vec::new();
    for n in 0..96 {
        let budget = Budget::reserve(&a.counters, 1, None).unwrap();
        action.queued.fetch_add(1, Ordering::Relaxed);
        let admission = Admission {
            _budget: budget,
            stats: vec![Arc::clone(&action)],
            counters: Arc::clone(&a.counters),
            committed: false,
        };
        let (tx, rx) = oneshot::channel();
        a.sender
            .try_send(Command::Append(a.record(n, 1), Some(tx), Some(admission)))
            .unwrap();
        replies.push(rx);
    }
    assert_eq!(a.counters.bytes.load(Ordering::Relaxed), 96);
    assert_eq!(action.queued.load(Ordering::Relaxed), 96);
    let (_, _, _, counters) = a.run();
    for (n, reply) in replies.into_iter().enumerate() {
        assert_eq!(
            reply.blocking_recv().unwrap(),
            if n < 64 {
                Ok(())
            } else {
                Err(QueueError::Full)
            }
        );
    }
    assert_eq!(counters.bytes.load(Ordering::Relaxed), 0);
    assert_eq!(counters.queued.load(Ordering::Relaxed), 0);
    assert_eq!(action.queued.load(Ordering::Relaxed), 0);
    assert_eq!(action.dropped.load(Ordering::Relaxed), 32);
    assert_eq!(counters.dropped.load(Ordering::Relaxed), 32);
}

#[test]
fn metadata_estimate_and_raw_bytes_bound_collection_independently() {
    let a = Actor::new(512);
    let mut replies = Vec::new();
    for n in 0..48 {
        let mut record = a.record(n, 1);
        record.client_id = "c".repeat(65535);
        let (tx, rx) = oneshot::channel();
        a.sender
            .try_send(Command::Append(record, Some(tx), None))
            .unwrap();
        replies.push(rx);
    }
    let (_, view, epoch, _) = a.run();
    assert_eq!(
        epoch, 2,
        "conservative16MiB metadata limit must split collection before64results"
    );
    assert_eq!(view.lock().unwrap().stats.pending_results, 48);
    for reply in replies {
        assert_eq!(reply.blocking_recv().unwrap(), Ok(()));
    }

    let a = Actor::new(512);
    let mut replies = Vec::new();
    for n in 0..9 {
        let (tx, rx) = oneshot::channel();
        a.sender
            .try_send(Command::Append(a.record(n, 1024 * 1024), Some(tx), None))
            .unwrap();
        replies.push(rx);
    }
    let (_, view, epoch, _) = a.run();
    assert_eq!(epoch, 2);
    assert_eq!(view.lock().unwrap().stats.pending_results, 9);
    for reply in replies {
        assert_eq!(reply.blocking_recv().unwrap(), Ok(()));
    }
}

#[test]
fn later_stale_binding_rejection_does_not_fall_back_or_acknowledge_earlier_frames() {
    let a = Actor::new(512);
    let mut replies: Vec<_> = (0..64).map(|n| a.append(n)).collect();
    let mut stale = a.record(64, 1);
    stale.consumers[0].revision += 100;
    let (tx, rx) = oneshot::channel();
    a.sender
        .try_send(Command::Append(stale, Some(tx), None))
        .unwrap();
    replies.push(rx);
    let (f, view, epoch, _) = a.run();
    assert_eq!(epoch, 1);
    for reply in replies {
        assert_eq!(
            reply.blocking_recv().unwrap(),
            Err(QueueError::StaleBinding)
        );
    }
    assert_eq!(view.lock().unwrap().stats.pending_results, 0);
    let reopened = Journal::open(QueueOptions::new(f.0.clone())).unwrap();
    assert_eq!(reopened.stats().pending_results, 0);
}

#[tokio::test]
async fn lone_append_does_not_wait_for_more_traffic_and_acknowledged_body_recovers() {
    let f = Fixture::new();
    let (queue, revisions) = Queue::open(
        QueueOptions::new(f.0.clone()),
        vec![Binding {
            outbound_id: "x".into(),
            descriptor: vec![1],
        }],
        Arc::new(Counters::default()),
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .unwrap();
    let record = NewResult {
        stream_id: "rule".into(),
        client_id: "client".into(),
        payload: b"durable before acknowledgement".to_vec(),
        consumers: vec![NewConsumer {
            outbound_id: "x".into(),
            revision: revisions["x"],
            delivery_id: "id".into(),
        }],
    };
    let (tx, rx) = oneshot::channel();
    queue
        .sender
        .send(Command::Append(record, Some(tx), None))
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), rx)
            .await
            .unwrap()
            .unwrap(),
        Ok(())
    );
    assert_eq!(queue.stats().pending_results, 1);
    queue.close().await;
    let reopened = Journal::open(QueueOptions::new(f.0.clone())).unwrap();
    assert_eq!(
        reopened.read_consumers(2, 1, 1024).unwrap()[0].payload,
        b"durable before acknowledgement"
    );
}
