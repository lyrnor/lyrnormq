use super::*;
use rpc::wire::{
    Envelope,
    cluster_server::{Cluster, ClusterServer},
};
use std::sync::atomic::AtomicUsize;
use tonic::{Request, Response, Status};
#[derive(Clone)]
struct ReadSpy {
    metadata: Arc<Mutex<Metadata>>,
    mutation_errors: Arc<Mutex<VecDeque<Status>>>,
    mutation_calls: Arc<AtomicUsize>,
    stall_mutation: Arc<AtomicBool>,
    full: Arc<AtomicUsize>,
    applied: Arc<AtomicUsize>,
    invalidate: Option<Arc<BrokerRuntime>>,
    acks: Arc<AtomicUsize>,
    last_ack: Arc<AtomicU64>,
    heartbeats: Arc<AtomicU64>,
    hold_first: Arc<AtomicBool>,
    release: Arc<Notify>,
}
#[tonic::async_trait]
impl Cluster for ReadSpy {
    type WatchStream = tokio_stream::wrappers::ReceiverStream<Result<Envelope, Status>>;
    async fn read(&self, request: Request<Envelope>) -> Result<Response<Envelope>, Status> {
        let value: serde_json::Value = serde_json::from_slice(&request.into_inner().data).unwrap();
        let body = if value == "Metadata" {
            self.full.fetch_add(1, Ordering::Relaxed);
            rpc::encode(&*self.metadata.lock().unwrap()).unwrap()
        } else if let Some(request) = value.get("Node") {
            let id = request["node"].as_u64().unwrap();
            rpc::encode(&self.metadata.lock().unwrap().nodes.get(&id)).unwrap()
        } else if let Some(request) = value.get("Applied") {
            self.applied.fetch_add(1, Ordering::Relaxed);
            let revision = request["revision"].as_u64().unwrap();
            if let Some(runtime) = &self.invalidate {
                *runtime.deadline.lock().unwrap() = tokio::time::Instant::now();
            }
            rpc::encode(&revision).unwrap()
        } else if let Some(request) = value.get("AppliedAck") {
            self.acks.fetch_add(1, Ordering::SeqCst);
            let revision = request["revision"].as_u64().unwrap();
            self.last_ack.store(revision, Ordering::SeqCst);
            if self.hold_first.swap(false, Ordering::SeqCst) {
                self.release.notified().await;
            }
            rpc::encode(&revision).unwrap()
        } else {
            return Err(Status::unimplemented("read kind"));
        };
        Ok(Response::new(body))
    }
    async fn raft(&self, _: Request<Envelope>) -> Result<Response<Envelope>, Status> {
        Err(Status::unimplemented("raft"))
    }
    async fn mutate(&self, request: Request<Envelope>) -> Result<Response<Envelope>, Status> {
        self.mutation_calls.fetch_add(1, Ordering::SeqCst);
        if self.stall_mutation.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
        if let Some(status) = self.mutation_errors.lock().unwrap().pop_front() {
            return Err(status);
        }
        let value: serde_json::Value = serde_json::from_slice(&request.into_inner().data).unwrap();
        if let Some(heartbeat) = value.get("Heartbeat") {
            let applied = heartbeat["applied"].as_u64().unwrap();
            self.heartbeats.store(applied, Ordering::SeqCst);
            return Ok(Response::new(
                rpc::encode(&Answer {
                    revision: applied + 100,
                    error: None,
                    lease_ms: 3000,
                    results: Vec::new(),
                })
                .unwrap(),
            ));
        }
        Err(Status::unimplemented("mutate"))
    }
    async fn watch(&self, _: Request<Envelope>) -> Result<Response<Self::WatchStream>, Status> {
        Err(Status::unimplemented("watch"))
    }
    async fn repair(&self, _: Request<Envelope>) -> Result<Response<Envelope>, Status> {
        Err(Status::unimplemented("repair"))
    }
    async fn replica(&self, _: Request<Envelope>) -> Result<Response<Envelope>, Status> {
        Err(Status::unimplemented("replica"))
    }
    async fn deliver_to(&self, _: Request<Envelope>) -> Result<Response<Envelope>, Status> {
        Err(Status::unimplemented("deliver_to"))
    }
    async fn deliver(&self, _: Request<Envelope>) -> Result<Response<Envelope>, Status> {
        Err(Status::unimplemented("deliver"))
    }
}
type SpyServer = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;
async fn spy_fixture(
    label: &str,
    invalidate: bool,
) -> io::Result<(Arc<BrokerRuntime>, ReadSpy, SpyServer, std::path::PathBuf)> {
    let path = std::env::temp_dir().join(format!(
        "rmqtt-applied-spy-{label}-{invalidate}-{}-{}",
        std::process::id(),
        controller::now()
    ));
    tokio::fs::create_dir_all(&path).await?;
    tokio::fs::write(path.join("token"), "a".repeat(32)).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let config=serde_json::from_value(serde_json::json!({"role":"broker","cluster_id":"applied-test","node_id":101,"listen":"127.0.0.1:7101","data_dir":path,"token_file":path.join("token"),"controllers":[{"id":1,"address":format!("http://{address}")},{"id":2,"address":"http://127.0.0.1:65001"},{"id":3,"address":"http://127.0.0.1:65002"}]})).map_err(io::Error::other)?;
    let runtime = BrokerRuntime::new(config)?;
    runtime.initialized.store(true, Ordering::Release);
    *runtime.deadline.lock().unwrap() = tokio::time::Instant::now() + Duration::from_secs(20);
    let node = Node {
        id: 101,
        incarnation: runtime.incarnation.clone(),
        address: "http://127.0.0.1:7101".into(),
        expires_at: 0,
        max_lease_ms: 3000,
        applied: 7,
    };
    runtime.metadata.write().unwrap().nodes.insert(101, node);
    runtime.metadata.write().unwrap().data_mode = Some(DataMode::Legacy);
    let spy = ReadSpy {
        metadata: Arc::new(Mutex::new(runtime.metadata.read().unwrap().clone())),
        mutation_errors: Arc::new(Mutex::new(VecDeque::new())),
        mutation_calls: Arc::new(AtomicUsize::new(0)),
        stall_mutation: Arc::new(AtomicBool::new(false)),
        full: Arc::new(AtomicUsize::new(0)),
        applied: Arc::new(AtomicUsize::new(0)),
        invalidate: invalidate.then(|| runtime.clone()),
        acks: Arc::new(AtomicUsize::new(0)),
        last_ack: Arc::new(AtomicU64::new(0)),
        heartbeats: Arc::new(AtomicU64::new(0)),
        hold_first: Arc::new(AtomicBool::new(false)),
        release: Arc::new(Notify::new()),
    };
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ClusterServer::new(spy.clone()))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    Ok((runtime, spy, server, path))
}
async fn barrier_fixture(invalidate: bool) -> io::Result<()> {
    let (runtime, spy, server, path) = spy_fixture("barrier", invalidate).await?;
    let result = runtime.barrier(7).await;
    server.abort();
    let _ = server.await;
    drop(runtime);
    tokio::fs::remove_dir_all(path).await?;
    if invalidate {
        assert!(
            result.is_err(),
            "expired local lease must reject a positive RPC"
        );
    } else {
        result?;
    }
    assert_eq!(
        spy.full.load(Ordering::Relaxed),
        0,
        "barrier must never download full metadata"
    );
    assert_eq!(spy.applied.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn applied_barrier_never_downloads_directory() -> io::Result<()> {
    barrier_fixture(false).await
}
#[tokio::test]
async fn applied_barrier_rechecks_local_lease_after_reply() -> io::Result<()> {
    barrier_fixture(true).await
}

#[tokio::test]
async fn installation_ack_sender_coalesces_and_repushes_same_revision() -> io::Result<()> {
    let (runtime, spy, server, path) = spy_fixture("coalescing", false).await?;
    spy.hold_first.store(true, Ordering::SeqCst);
    runtime.metadata.write().unwrap().revision = 1;
    runtime.announce_applied();
    let sender = tokio::spawn(runtime.clone().applied_ack_loop());
    tokio::time::timeout(Duration::from_secs(2), async {
        while spy.acks.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(io::Error::other)?;
    for revision in 2..=5000 {
        runtime.metadata.write().unwrap().revision = revision;
        runtime.announce_applied();
    }
    spy.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while spy.last_ack.load(Ordering::SeqCst) != 5000 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(io::Error::other)?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        spy.acks.load(Ordering::SeqCst),
        2,
        "one in-flight RPC plus one latest update, not one task per revision"
    );
    runtime.announce_applied();
    tokio::time::timeout(Duration::from_secs(2), async {
        while spy.acks.load(Ordering::SeqCst) != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(io::Error::other)?;
    runtime.stopped.store(true, Ordering::Release);
    sender.abort();
    let _ = sender.await;
    server.abort();
    let _ = server.await;
    drop(runtime);
    tokio::fs::remove_dir_all(path).await?;
    assert_eq!(spy.full.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn heartbeat_cannot_announce_revision_before_installation_completes() -> io::Result<()> {
    let (runtime, spy, server, path) = spy_fixture("watermark", false).await?;
    runtime.installed.send_replace(7);
    // Canonical merge has run, but an asynchronous reconciliation is still pending.
    runtime.metadata.write().unwrap().revision = 99;
    let mut installed = runtime.installed.subscribe();
    let task = tokio::spawn(runtime.clone().heartbeat_loop());
    tokio::time::timeout(Duration::from_secs(2), installed.changed())
        .await
        .map_err(io::Error::other)?
        .map_err(io::Error::other)?;
    task.abort();
    let _ = task.await;
    server.abort();
    let _ = server.await;
    assert_eq!(
        spy.heartbeats.load(Ordering::SeqCst),
        7,
        "heartbeat must use completed installed watermark"
    );
    assert_eq!(
        *runtime.installed.borrow(),
        7,
        "repushing progress cannot publish pending canonical revision"
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            runtime.watch_reconnect.notified()
        )
        .await
        .is_err(),
        "a healthy incremental stream must not be reset by a newer heartbeat revision"
    );
    drop(runtime);
    tokio::fs::remove_dir_all(path).await?;
    Ok(())
}

#[tokio::test]
async fn registration_recovery_reads_only_one_node() -> io::Result<()> {
    let (runtime, spy, server, path) = spy_fixture("node-recovery", false).await?;
    let rpc = runtime.rpc().await?;
    runtime.recover_registration(rpc).await?;
    assert_eq!(
        spy.full.load(Ordering::Relaxed),
        0,
        "transient heartbeat failure must not read full directory"
    );
    server.abort();
    let _ = server.await;
    drop(runtime);
    tokio::fs::remove_dir_all(path).await?;
    Ok(())
}

async fn three_seed_rpc(
    runtime: &BrokerRuntime,
    spy: &ReadSpy,
) -> io::Result<(ClientRpc, Vec<SpyServer>)> {
    let mut config = (*runtime.config).clone();
    let mut servers = Vec::new();
    for peer in config.controllers.iter_mut().skip(1) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        peer.address = format!("http://{}", listener.local_addr()?);
        servers.push(tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(ClusterServer::new(spy.clone()))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        ));
    }
    Ok((ClientRpc::new(Arc::new(config)).await?, servers))
}
fn test_heartbeat() -> Command {
    Command::Heartbeat {
        node: 101,
        incarnation: "test".into(),
        applied: 0,
        expires_at: 0,
        lease_ms: 3000,
    }
}
#[tokio::test]
async fn ambiguous_mutation_reply_is_not_resubmitted_to_other_seeds() -> io::Result<()> {
    let (runtime, spy, server, path) = spy_fixture("unknown-write", false).await?;
    let (rpc, servers) = three_seed_rpc(&runtime, &spy).await?;
    spy.mutation_errors
        .lock()
        .unwrap()
        .push_back(Status::unavailable("reply lost after apply"));
    let error = rpc.mutate(test_heartbeat()).await.unwrap_err();
    assert!(!rpc::not_submitted(&error));
    assert_eq!(spy.mutation_calls.load(Ordering::SeqCst), 1);
    assert!(error.to_string().contains("reply lost after apply"));
    // A definite follower rejection is safe to redirect to the next seed.
    spy.mutation_errors
        .lock()
        .unwrap()
        .push_back(Status::failed_precondition(
            "metadata not submitted: not current leader",
        ));
    rpc.mutate(test_heartbeat()).await?;
    assert_eq!(spy.mutation_calls.load(Ordering::SeqCst), 3);
    server.abort();
    for task in servers {
        task.abort();
    }
    drop(runtime);
    tokio::fs::remove_dir_all(path).await?;
    Ok(())
}
#[tokio::test]
async fn mutation_deadline_covers_all_seeds() -> io::Result<()> {
    let (runtime, spy, server, path) = spy_fixture("write-deadline", false).await?;
    let (rpc, servers) = three_seed_rpc(&runtime, &spy).await?;
    spy.stall_mutation.store(true, Ordering::SeqCst);
    let start = tokio::time::Instant::now();
    let error = rpc.mutate(test_heartbeat()).await.unwrap_err();
    assert!(start.elapsed() < Duration::from_millis(2500));
    assert!(!rpc::not_submitted(&error));
    assert_eq!(spy.mutation_calls.load(Ordering::SeqCst), 1);
    server.abort();
    for task in servers {
        task.abort();
    }
    drop(runtime);
    tokio::fs::remove_dir_all(path).await?;
    Ok(())
}

#[tokio::test]
async fn dead_initial_seed_does_not_block_safe_controller_discovery() -> io::Result<()> {
    let (runtime, spy, server, path) = spy_fixture("dead-first", false).await?;
    let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let dead_address = format!("http://{}", dead.local_addr()?);
    drop(dead);
    let mut config = (*runtime.config).clone();
    config.controllers[1].address = config.controllers[0].address.clone();
    config.controllers[0].address = dead_address;
    let rpc = ClientRpc::new(Arc::new(config)).await?;
    let start = tokio::time::Instant::now();
    rpc.mutate(test_heartbeat()).await?;
    assert!(start.elapsed() < Duration::from_secs(2));
    assert_eq!(spy.mutation_calls.load(Ordering::SeqCst), 1);
    let node = rpc.node(101).await?;
    assert!(node.is_some());
    server.abort();
    let _ = server.await;
    drop(runtime);
    tokio::fs::remove_dir_all(path).await?;
    Ok(())
}

#[tokio::test]
async fn more_than_old_broker_command_limit_waits_instead_of_rejecting() -> io::Result<()> {
    let (runtime, _spy, server, path) = spy_fixture("no-command-quota", false).await?;
    let mut pending: Vec<_> = (0..257)
        .map(|_| Box::pin(runtime.command(test_heartbeat(), false)))
        .collect();
    std::future::poll_fn(|cx| {
        for request in &mut pending {
            assert!(
                request.as_mut().poll(cx).is_pending(),
                "in-flight command must not be capacity-rejected"
            );
        }
        std::task::Poll::Ready(())
    })
    .await;
    drop(pending);
    server.abort();
    let _ = server.await;
    drop(runtime);
    tokio::fs::remove_dir_all(path).await?;
    Ok(())
}

#[test]
fn legacy_receipts_accept_more_than_8192_pending_deliveries() {
    let mut receipts = Receipts::default();
    for sequence in 0..8193 {
        assert!(
            receipts.begin(&(101, "inc".into(), sequence)).is_ok(),
            "pending deliveries must not impose admission quota"
        );
    }
    assert_eq!(receipts.values.len(), 8193);
    assert!(receipts.order.is_empty());
    assert!(
        receipts.begin(&(101, "inc".into(), 0)).is_err(),
        "duplicate pending delivery remains rejected"
    );
}
#[test]
fn legacy_completed_receipts_are_bounded_without_evicting_pending() {
    let mut receipts = Receipts::default();
    for sequence in 0..9000 {
        receipts.values.insert((101, "inc".into(), sequence), false);
    }
    for sequence in 0..8500 {
        receipts.finish((101, "inc".into(), sequence), true);
    }
    assert_eq!(receipts.order.len(), 8192);
    assert_eq!(receipts.values.len(), 8192 + 500);
    for sequence in 8500..9000 {
        assert_eq!(
            receipts.values.get(&(101, "inc".into(), sequence)),
            Some(&false)
        );
    }
    assert!(
        !receipts.begin(&(101, "inc".into(), 8499)).unwrap(),
        "completed duplicate is acknowledged without redelivery"
    );
}

#[tokio::test]
async fn legacy_receipt_guard_cleans_cancellation_without_deleting_a_retry() -> io::Result<()> {
    let (runtime, _spy, server, path) = spy_fixture("receipt-guard", false).await?;
    let key = (101, "inc".to_owned(), 1);
    assert!(runtime.receipts.lock().unwrap().begin(&key)?);
    let guard = ReceiptGuard {
        runtime: &runtime,
        key: key.clone(),
        active: true,
    };
    drop(guard); // Cancellation before finalization removes only the pending attempt.
    assert!(!runtime.receipts.lock().unwrap().values.contains_key(&key));
    assert!(runtime.receipts.lock().unwrap().begin(&key)?);
    let mut guard = ReceiptGuard {
        runtime: &runtime,
        key: key.clone(),
        active: true,
    };
    {
        let mut receipts = runtime.receipts.lock().unwrap();
        receipts.finish(key.clone(), true);
        guard.active = false;
        // Simulate completed-cache eviction and a later attempt at the same identity.
        receipts.values.remove(&key);
        receipts.order.clear();
        assert!(receipts.begin(&key)?);
    }
    drop(guard);
    assert_eq!(
        runtime.receipts.lock().unwrap().values.get(&key),
        Some(&false)
    );
    runtime.receipts.lock().unwrap().finish(key.clone(), false);
    assert!(!runtime.receipts.lock().unwrap().values.contains_key(&key));
    server.abort();
    let _ = server.await;
    drop(runtime);
    tokio::fs::remove_dir_all(path).await?;
    Ok(())
}
