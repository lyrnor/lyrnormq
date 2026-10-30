use super::*;
use std::sync::Barrier;
struct Empty;
impl Source for Empty {
    fn state_bytes(&self) -> usize {
        0
    }
    fn retained(&self, q: &Query) -> Page<RetainedView> {
        Page {
            items: Vec::new(),
            total: 0,
            offset: q.offset,
            limit: q.limit,
        }
    }
}
fn attached() -> (ManagementHandle, Arc<dyn Source>) {
    let h = ManagementHandle::new();
    h.claim().unwrap();
    let source: Arc<dyn Source> = Arc::new(Empty);
    h.attach(Arc::clone(&source)).unwrap();
    (h, source)
}
#[tokio::test]
async fn query_bounds_and_stop_are_enforced() {
    let (h, _source) = attached();
    assert_eq!(h.overview().await.unwrap().online_sessions, 0);
    for query in [
        Query {
            limit: 0,
            ..Query::default()
        },
        Query {
            offset: 1_000_001,
            ..Query::default()
        },
        Query {
            search: "x".repeat(257),
            ..Query::default()
        },
        Query {
            state: "invalid".into(),
            ..Query::default()
        },
        Query {
            protocol: "6".into(),
            ..Query::default()
        },
    ] {
        assert_eq!(
            h.sessions(query).await.unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
    assert!(h.claim().is_err());
    h.stop();
    assert_eq!(
        h.overview().await.unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eight_query_jobs_hold_permits_after_caller_cancellation() {
    let (h, _source) = attached();
    let ready = Arc::new(Barrier::new(9));
    let release = Arc::new(Barrier::new(9));
    let mut jobs = Vec::new();
    for _ in 0..8 {
        let h = h.clone();
        let ready = Arc::clone(&ready);
        let release = Arc::clone(&release);
        jobs.push(tokio::spawn(async move {
            h.read(move |_, _| {
                ready.wait();
                release.wait();
                Ok(())
            })
            .await
        }));
    }
    tokio::task::spawn_blocking(move || ready.wait())
        .await
        .unwrap();
    assert_eq!(
        h.overview().await.unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    for job in &jobs {
        job.abort();
    }
    assert_eq!(
        h.overview().await.unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    tokio::task::spawn_blocking(move || release.wait())
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while h.inner.requests.available_permits() != 8 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(h.overview().await.is_ok());
}
#[tokio::test]
async fn metadata_paging_is_ordered_and_does_not_copy_queued_payloads() {
    let (h, _source) = attached();
    let config = mqtt_session::durable::SessionConfig {
        max_sessions: 4,
        max_retained: 4,
        max_subscriptions: 4,
        max_queued: 4,
        max_inflight: 4,
        max_packet_size: 1024,
        max_state_bytes: 65536,
    };
    let mut engine = SessionEngine::new(config).unwrap();
    for (id, name) in [(1, "z"), (2, "a"), (3, "m")] {
        engine.connect(name, false).unwrap();
        h.register(name, id, &engine);
    }
    let page = h
        .sessions(Query {
            offset: 1,
            limit: 1,
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(page.total, 3);
    assert_eq!(page.items[0].client_id, "m");
    h.remove("m", 999);
    assert!(h.session("m").await.unwrap().is_some());
    h.remove("m", 3);
    assert!(h.session("m").await.unwrap().is_none());
    let page = h
        .sessions(Query {
            search: "a".into(),
            ..Query::default()
        })
        .await
        .unwrap();
    // Anonymous principal also contains 'a', proving principal search is applied.
    assert_eq!(page.total, 2);
    assert_eq!(
        h.sessions(Query {
            state: "offline".into(),
            ..Query::default()
        })
        .await
        .unwrap()
        .total,
        0
    );
}

#[tokio::test]
async fn busy_and_unavailable_have_stable_typed_error_causes() {
    fn classify(error: &io::Error) -> ManagementError {
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        *error
            .get_ref()
            .unwrap()
            .downcast_ref::<ManagementError>()
            .unwrap()
    }
    let unattached = ManagementHandle::new();
    assert_eq!(
        classify(&unattached.overview().await.unwrap_err()),
        ManagementError::Unavailable
    );
    let (h, _source) = attached();
    let all_permits = Arc::clone(&h.inner.requests)
        .try_acquire_many_owned(8)
        .unwrap();
    assert_eq!(
        classify(&h.overview().await.unwrap_err()),
        ManagementError::Busy
    );
    drop(all_permits);
    assert!(h.overview().await.is_ok());
    let _all_permits = Arc::clone(&h.inner.requests)
        .try_acquire_many_owned(8)
        .unwrap();
    h.stop();
    assert_eq!(
        classify(&h.overview().await.unwrap_err()),
        ManagementError::Unavailable
    );
}
