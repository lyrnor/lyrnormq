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
    full: Arc<AtomicUsize>,
    applied: Arc<AtomicUsize>,
    invalidate: Option<Arc<BrokerRuntime>>,
    acks: Arc<AtomicUsize>,
    last_ack: Arc<AtomicU64>,
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
    async fn mutate(&self, _: Request<Envelope>) -> Result<Response<Envelope>, Status> {
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
        full: Arc::new(AtomicUsize::new(0)),
        applied: Arc::new(AtomicUsize::new(0)),
        invalidate: invalidate.then(|| runtime.clone()),
        acks: Arc::new(AtomicUsize::new(0)),
        last_ack: Arc::new(AtomicU64::new(0)),
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
