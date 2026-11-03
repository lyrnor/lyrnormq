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
async fn barrier_fixture(invalidate: bool) -> io::Result<()> {
    let path = std::env::temp_dir().join(format!(
        "rmqtt-applied-spy-{invalidate}-{}-{}",
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
    };
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ClusterServer::new(spy.clone()))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
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
