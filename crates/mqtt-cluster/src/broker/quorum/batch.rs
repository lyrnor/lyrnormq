//! Cross-key replica writes share one RPC per destination. Each request is still judged alone.
use crate::broker::{REPLICA_BATCH_LIMIT, ReplicaBatchMessage, ReplicaBatchReply, ReplicaMessage};
use crate::profile::{self, Metric, Timer};
use crate::replica::{Reply, Request};
use crate::rpc::{self, ClientRpc, wire};
use std::{
    collections::{BTreeMap, VecDeque},
    io,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    sync::{Notify, Semaphore, mpsc, oneshot},
    task::JoinHandle,
};

const QUEUE_CAPACITY: usize = 8192;
const MAX_IN_FLIGHT: usize = 4;
const MAX_BATCH_BYTES: usize = 2 * 1024 * 1024;
const MAX_PEERS: usize = 64;

struct Item {
    generation: u64,
    request: Request,
    reply: oneshot::Sender<io::Result<Reply>>,
}
struct Outbound {
    items: Vec<Item>,
    bytes: Vec<u8>,
}
#[derive(Clone)]
struct Peer {
    tx: mpsc::Sender<Item>,
    legacy: Arc<AtomicBool>,
}
struct Inner {
    rpc: ClientRpc,
    peers: Mutex<BTreeMap<String, Peer>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    gate: Option<Arc<Notify>>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        let tasks = {
            let mut guard = self.tasks.lock().unwrap_or_else(PoisonError::into_inner);
            std::mem::take(&mut *guard)
        };
        for task in tasks {
            task.abort();
        }
    }
}

#[derive(Clone)]
pub(in crate::broker) struct ReplicaBatcher {
    inner: Arc<Inner>,
}
impl ReplicaBatcher {
    pub(in crate::broker) fn new(rpc: ClientRpc) -> Self {
        Self::create(rpc, None)
    }
    fn create(rpc: ClientRpc, gate: Option<Arc<Notify>>) -> Self {
        Self {
            inner: Arc::new(Inner {
                rpc,
                peers: Mutex::new(BTreeMap::new()),
                tasks: Mutex::new(Vec::new()),
                gate,
            }),
        }
    }
    pub(in crate::broker) async fn submit(
        &self,
        address: &str,
        generation: u64,
        request: Request,
    ) -> io::Result<Reply> {
        let peer = self.peer(address)?;
        if peer.legacy.load(Ordering::Acquire) {
            return send_one(&self.inner.rpc, address, generation, request).await;
        }
        let rx = queue(&peer, generation, request)?;
        rx.await.map_err(|_| peer_failed())?
    }
    #[cfg(test)]
    fn enqueue(
        &self,
        address: &str,
        generation: u64,
        request: Request,
    ) -> io::Result<oneshot::Receiver<io::Result<Reply>>> {
        let peer = self.peer(address)?;
        queue(&peer, generation, request)
    }
    fn peer(&self, address: &str) -> io::Result<Peer> {
        let mut peers = self
            .inner
            .peers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(peer) = peers.get(address) {
            return Ok(peer.clone());
        }
        if peers.len() >= MAX_PEERS {
            return Err(peer_failed());
        }
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        let legacy = Arc::new(AtomicBool::new(false));
        let peer = Peer {
            tx,
            legacy: Arc::clone(&legacy),
        };
        peers.insert(address.to_owned(), peer.clone());
        drop(peers);
        let task = tokio::spawn(run_peer(
            self.inner.rpc.clone(),
            address.to_owned(),
            rx,
            legacy,
            self.inner.gate.clone(),
        ));
        self.inner
            .tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(task);
        Ok(peer)
    }
}

fn queue(
    peer: &Peer,
    generation: u64,
    request: Request,
) -> io::Result<oneshot::Receiver<io::Result<Reply>>> {
    let (tx, rx) = oneshot::channel();
    peer.tx
        .try_send(Item {
            generation,
            request,
            reply: tx,
        })
        .map_err(|_| peer_failed())?;
    Ok(rx)
}
fn peer_failed() -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, "replica write unavailable")
}
fn fail_all(items: Vec<Item>, message: &str) {
    for item in items {
        let _ = item.reply.send(Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            message.to_owned(),
        )));
    }
}

enum BatchError {
    Unimplemented,
    Failed(String),
}

async fn run_peer(
    rpc: ClientRpc,
    address: String,
    mut incoming: mpsc::Receiver<Item>,
    legacy: Arc<AtomicBool>,
    gate: Option<Arc<Notify>>,
) {
    if let Some(gate) = &gate {
        gate.notified().await;
    }
    let limit = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
    let mut held = VecDeque::new();
    loop {
        let Some(first) = next_item(&mut held, &mut incoming).await else {
            break;
        };
        if legacy.load(Ordering::Acquire) {
            let result = send_one(&rpc, &address, first.generation, first.request).await;
            let _ = first.reply.send(result);
            continue;
        }
        let (outbound, rest) = take_batch(first, &mut incoming);
        held.extend(rest);
        let Ok(permit) = Arc::clone(&limit).acquire_owned().await else {
            fail_all(outbound.items, "replica write unavailable");
            continue;
        };
        let task_rpc = rpc.clone();
        let task_address = address.clone();
        let task_legacy = Arc::clone(&legacy);
        tokio::spawn(async move {
            let _permit = permit;
            deliver(task_rpc, task_address, task_legacy, outbound).await;
        });
    }
}
async fn next_item(held: &mut VecDeque<Item>, incoming: &mut mpsc::Receiver<Item>) -> Option<Item> {
    if let Some(item) = held.pop_front() {
        Some(item)
    } else {
        incoming.recv().await
    }
}
fn take_batch(first: Item, incoming: &mut mpsc::Receiver<Item>) -> (Outbound, VecDeque<Item>) {
    let generation = first.generation;
    let mut batch = Vec::with_capacity(REPLICA_BATCH_LIMIT);
    batch.push(first);
    let mut deferred = VecDeque::new();
    while batch.len() < REPLICA_BATCH_LIMIT {
        match incoming.try_recv() {
            Ok(item) if item.generation == generation => batch.push(item),
            Ok(item) => {
                deferred.push_back(item);
                break;
            }
            Err(_) => break,
        }
    }
    let (bytes, keep) = match encode_fitting(generation, &batch) {
        Ok((bytes, keep)) if keep > 0 && keep <= batch.len() => (bytes, keep),
        _ => {
            while batch.len() > 1 {
                if let Some(item) = batch.pop() {
                    deferred.push_front(item);
                }
            }
            return (
                Outbound {
                    items: batch,
                    bytes: Vec::new(),
                },
                deferred,
            );
        }
    };
    if keep < batch.len() {
        let mut overflow = VecDeque::from(batch.split_off(keep));
        overflow.append(&mut deferred);
        deferred = overflow;
    }
    (
        Outbound {
            items: batch,
            bytes,
        },
        deferred,
    )
}
fn encode_fitting(generation: u64, batch: &[Item]) -> io::Result<(Vec<u8>, usize)> {
    if batch.is_empty() {
        return Err(peer_failed());
    }
    let _timer = Timer::start(Metric::QuorumRemoteEncode);
    let mut body = serde_json::to_vec(&ReplicaBatchMessage {
        generation,
        operations: Vec::new(),
    })
    .map_err(io::Error::other)?;
    // {"generation":N,"operations":[]} ends in `]}`. Items are inserted before that suffix.
    if body.len() < 2 || &body[body.len() - 2..] != b"]}" {
        return Err(io::Error::other("replica batch encoding"));
    }
    body.truncate(body.len() - 2);
    let mut keep = 0usize;
    for item in batch {
        let encoded = serde_json::to_vec(&item.request).map_err(io::Error::other)?;
        let extra = encoded.len() + usize::from(keep > 0);
        if keep > 0 && body.len() + extra + 2 > MAX_BATCH_BYTES {
            break;
        }
        if keep > 0 {
            body.push(b',');
        }
        body.extend_from_slice(&encoded);
        keep += 1;
    }
    if keep == 0 {
        return Err(io::Error::other("replica batch encoding"));
    }
    body.extend_from_slice(b"]}");
    Ok((body, keep))
}

async fn deliver(rpc: ClientRpc, address: String, legacy: Arc<AtomicBool>, outbound: Outbound) {
    if outbound.items.is_empty()
        || outbound.bytes.is_empty()
        || outbound.bytes.len() > rpc::MAX_WIRE
    {
        fail_all(outbound.items, "replica write unavailable");
        return;
    }
    let expected = outbound.items.len();
    match send_batch(&rpc, &address, outbound.bytes, expected).await {
        Ok(results) => {
            for (item, result) in outbound.items.into_iter().zip(results) {
                let reply = match result {
                    Some(reply) => Ok(reply),
                    None => Err(peer_failed()),
                };
                let _ = item.reply.send(reply);
            }
        }
        Err(BatchError::Unimplemented) => {
            legacy.store(true, Ordering::Release);
            for item in outbound.items {
                let result = send_one(&rpc, &address, item.generation, item.request).await;
                let _ = item.reply.send(result);
            }
        }
        Err(BatchError::Failed(message)) => fail_all(outbound.items, &message),
    }
}
async fn send_batch(
    rpc: &ClientRpc,
    address: &str,
    bytes: Vec<u8>,
    expected: usize,
) -> Result<Vec<Option<Reply>>, BatchError> {
    let mut client = rpc
        .client(address)
        .await
        .map_err(|error| BatchError::Failed(error.to_string()))?;
    profile::observe(Metric::QuorumRemoteBytes, bytes.len() as u64);
    profile::observe(Metric::ReplicaBatchSize, expected as u64);
    profile::observe(Metric::ReplicaRpc, 1);
    let response = client
        .replica_batch(
            rpc.authorize(wire::Envelope { data: bytes.into() })
                .map_err(|error| BatchError::Failed(error.to_string()))?,
        )
        .await
        .map_err(|status| {
            if status.code() == tonic::Code::Unimplemented {
                BatchError::Unimplemented
            } else {
                BatchError::Failed(status.to_string())
            }
        })?;
    let reply: ReplicaBatchReply = rpc::decode(response.into_inner())
        .map_err(|error| BatchError::Failed(error.to_string()))?;
    if reply.results.len() != expected {
        return Err(BatchError::Failed("replica write unavailable".to_owned()));
    }
    Ok(reply.results)
}
async fn send_one(
    rpc: &ClientRpc,
    address: &str,
    generation: u64,
    request: Request,
) -> io::Result<Reply> {
    let envelope = rpc::encode(&ReplicaMessage {
        generation,
        operation: request,
    })?;
    let mut client = rpc.client(address).await?;
    profile::observe(Metric::ReplicaRpc, 1);
    let response = client
        .replica(rpc.authorize(envelope)?)
        .await
        .map_err(rpc::io_error)?;
    rpc::decode(response.into_inner()).map_err(rpc::io_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica::{Journal, Record};
    use mqtt_cluster_core::ClusterBridge;
    use rpc::wire::cluster_server::{Cluster, ClusterServer};
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };
    use tonic::{Request as RpcRequest, Response, Status};

    fn item(request: Request) -> Item {
        Item {
            generation: 1,
            request,
            reply: oneshot::channel().0,
        }
    }
    fn read(key: &str) -> Request {
        Request::Read { key: key.into() }
    }
    fn write_record(key: &str, record: Record) -> Request {
        Request::Write {
            key: key.into(),
            record,
        }
    }
    fn record(epoch: u64, physical: u64, payload: &[u8]) -> Record {
        let mut value = Record::empty(epoch);
        value.physical_revision = physical;
        value.snapshot = Some(payload.to_vec());
        value
    }

    #[test]
    fn encoded_batch_matches_the_owned_message_and_respects_two_mib() {
        let small = vec![
            item(read("c:a")),
            item(read("c:b")),
            item(write_record("c:c", record(1, 1, b"payload"))),
        ];
        let (bytes, keep) = encode_fitting(9, &small).unwrap();
        assert_eq!(keep, 3);
        let operations: Vec<_> = small.iter().map(|item| item.request.clone()).collect();
        let expected = serde_json::to_vec(&ReplicaBatchMessage {
            generation: 9,
            operations,
        })
        .unwrap();
        assert_eq!(bytes, expected);

        let mut payload = vec![0u8; 256 * 1024];
        let split_at = loop {
            let request = write_record("c:big", record(1, 1, &payload));
            let pair = [item(request.clone()), item(request)];
            let (encoded, keep) = encode_fitting(3, &pair).unwrap();
            if keep == 1 && encoded.len() <= MAX_BATCH_BYTES {
                let both = serde_json::to_vec(&ReplicaBatchMessage {
                    generation: 3,
                    operations: vec![pair[0].request.clone(), pair[1].request.clone()],
                })
                .unwrap();
                assert!(both.len() > MAX_BATCH_BYTES);
                break payload.len();
            }
            payload.resize(payload.len() + 128 * 1024, 0);
            assert!(payload.len() <= 2 * 1024 * 1024);
        };
        let big = write_record("c:big", record(1, 1, &vec![0u8; split_at]));
        let mixed = [item(big.clone()), item(big.clone()), item(read("c:tail"))];
        let (first, keep) = encode_fitting(3, &mixed).unwrap();
        assert_eq!(keep, 1);
        assert!(first.len() <= MAX_BATCH_BYTES);
        let (second, keep) = encode_fitting(3, &mixed[1..]).unwrap();
        assert_eq!(keep, 2);
        assert!(second.len() <= MAX_BATCH_BYTES);
        let (tx, mut rx) = mpsc::channel(8);
        tx.try_send(Item {
            generation: 5,
            request: big.clone(),
            reply: oneshot::channel().0,
        })
        .unwrap();
        tx.try_send(Item {
            generation: 5,
            request: big,
            reply: oneshot::channel().0,
        })
        .unwrap();
        tx.try_send(Item {
            generation: 5,
            request: read("c:tail"),
            reply: oneshot::channel().0,
        })
        .unwrap();
        let first = rx.try_recv().unwrap();
        let (outbound, deferred) = take_batch(first, &mut rx);
        assert_eq!(outbound.items.len(), 1);
        assert!(outbound.bytes.len() <= MAX_BATCH_BYTES);
        assert_eq!(deferred.len(), 2);
        let mut deferred = deferred;
        let next = deferred.pop_front().unwrap();
        let (rest_tx, mut rest_rx) = mpsc::channel(8);
        for item in deferred {
            rest_tx.try_send(item).unwrap();
        }
        let (rest, none) = take_batch(next, &mut rest_rx);
        assert_eq!(rest.items.len(), 2);
        assert!(none.is_empty());
        assert!(rest.bytes.len() <= MAX_BATCH_BYTES);
    }

    #[derive(Clone)]
    struct Script {
        journal: Option<Journal>,
        fail_batch: bool,
        legacy: bool,
        block: bool,
        release: Arc<AtomicBool>,
        wait: Arc<Notify>,
        seen: Arc<Mutex<Vec<(u64, usize)>>>,
        singles: Arc<AtomicUsize>,
        inflight: Arc<AtomicUsize>,
        max_inflight: Arc<AtomicUsize>,
    }
    impl Script {
        fn open() -> Self {
            Self {
                journal: None,
                fail_batch: false,
                legacy: false,
                block: false,
                release: Arc::new(AtomicBool::new(false)),
                wait: Arc::new(Notify::new()),
                seen: Arc::default(),
                singles: Arc::default(),
                inflight: Arc::default(),
                max_inflight: Arc::default(),
            }
        }
    }
    #[tonic::async_trait]
    impl Cluster for Script {
        type WatchStream = tokio_stream::wrappers::ReceiverStream<Result<wire::Envelope, Status>>;
        async fn read(
            &self,
            _: RpcRequest<wire::Envelope>,
        ) -> Result<Response<wire::Envelope>, Status> {
            Err(Status::unimplemented("read"))
        }
        async fn raft(
            &self,
            _: RpcRequest<wire::Envelope>,
        ) -> Result<Response<wire::Envelope>, Status> {
            Err(Status::unimplemented("raft"))
        }
        async fn mutate(
            &self,
            _: RpcRequest<wire::Envelope>,
        ) -> Result<Response<wire::Envelope>, Status> {
            Err(Status::unimplemented("mutate"))
        }
        async fn watch(
            &self,
            _: RpcRequest<wire::Envelope>,
        ) -> Result<Response<Self::WatchStream>, Status> {
            Err(Status::unimplemented("watch"))
        }
        async fn repair(
            &self,
            _: RpcRequest<wire::Envelope>,
        ) -> Result<Response<wire::Envelope>, Status> {
            Err(Status::unimplemented("repair"))
        }
        async fn replica(
            &self,
            request: RpcRequest<wire::Envelope>,
        ) -> Result<Response<wire::Envelope>, Status> {
            self.singles.fetch_add(1, Ordering::SeqCst);
            let message: ReplicaMessage = rpc::decode(request.into_inner())?;
            let reply = if let Some(journal) = &self.journal {
                journal
                    .execute_at(message.generation, message.operation)
                    .await
                    .map_err(|_| Status::unavailable("replica"))?
            } else {
                Reply { record: None }
            };
            Ok(Response::new(rpc::encode(&reply)?))
        }
        async fn replica_batch(
            &self,
            request: RpcRequest<wire::Envelope>,
        ) -> Result<Response<wire::Envelope>, Status> {
            if self.legacy {
                return Err(Status::unimplemented("replica_batch"));
            }
            let message: ReplicaBatchMessage = rpc::decode(request.into_inner())?;
            self.seen
                .lock()
                .unwrap()
                .push((message.generation, message.operations.len()));
            if self.block {
                let now = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_inflight.fetch_max(now, Ordering::SeqCst);
                loop {
                    let notified = self.wait.notified();
                    if self.release.load(Ordering::Acquire) {
                        break;
                    }
                    notified.await;
                }
                self.inflight.fetch_sub(1, Ordering::SeqCst);
            }
            if self.fail_batch {
                return Err(Status::unavailable("batch down"));
            }
            let results = if let Some(journal) = &self.journal {
                journal
                    .execute_many(message.generation, message.operations)
                    .await
                    .map_err(|_| Status::unavailable("replica"))?
                    .into_iter()
                    .map(Result::ok)
                    .collect()
            } else {
                vec![Some(Reply { record: None }); message.operations.len()]
            };
            Ok(Response::new(rpc::encode(&ReplicaBatchReply { results })?))
        }
        async fn deliver_to(
            &self,
            _: RpcRequest<wire::Envelope>,
        ) -> Result<Response<wire::Envelope>, Status> {
            Err(Status::unimplemented("deliver_to"))
        }
        async fn deliver_batch(
            &self,
            _: RpcRequest<wire::Envelope>,
        ) -> Result<Response<wire::Envelope>, Status> {
            Err(Status::unimplemented("deliver_batch"))
        }
        async fn deliver(
            &self,
            _: RpcRequest<wire::Envelope>,
        ) -> Result<Response<wire::Envelope>, Status> {
            Err(Status::unimplemented("deliver"))
        }
    }

    fn temp(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "rmqtt-replica-batch-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }
    async fn client_rpc(path: &PathBuf) -> ClientRpc {
        tokio::fs::create_dir_all(path).await.unwrap();
        tokio::fs::write(path.join("token"), "a".repeat(32))
            .await
            .unwrap();
        let config = serde_json::from_value(serde_json::json!({
            "role": "broker",
            "cluster_id": "batch-test",
            "node_id": 101,
            "listen": "127.0.0.1:7101",
            "data_dir": path,
            "token_file": path.join("token"),
            "controllers": [
                {"id": 1, "address": "http://127.0.0.1:65001"},
                {"id": 2, "address": "http://127.0.0.1:65002"},
                {"id": 3, "address": "http://127.0.0.1:65003"}
            ]
        }))
        .unwrap();
        ClientRpc::new(std::sync::Arc::new(config)).await.unwrap()
    }
    async fn serve(service: Script) -> (String, JoinHandle<Result<(), tonic::transport::Error>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(
                    ClusterServer::new(service)
                        .max_decoding_message_size(rpc::MAX_WIRE)
                        .max_encoding_message_size(rpc::MAX_WIRE),
                )
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        (address, server)
    }
    fn gated(rpc: ClientRpc) -> (ReplicaBatcher, Arc<Notify>) {
        let gate = Arc::new(Notify::new());
        let batcher = ReplicaBatcher::create(rpc, Some(Arc::clone(&gate)));
        (batcher, gate)
    }
    async fn finish(server: JoinHandle<Result<(), tonic::transport::Error>>) {
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn different_generations_are_split_and_same_generation_is_coalesced() {
        let path = temp("generations");
        let rpc = client_rpc(&path).await;
        let mut script = Script::open();
        script.journal = None;
        let (address, server) = serve(script.clone()).await;
        let (batcher, gate) = gated(rpc);
        let mut waits = Vec::new();
        for index in 0..10 {
            waits.push(
                batcher
                    .enqueue(&address, 7, read(&format!("c:{index}")))
                    .unwrap(),
            );
        }
        for index in 0..10 {
            waits.push(
                batcher
                    .enqueue(&address, 9, read(&format!("d:{index}")))
                    .unwrap(),
            );
        }
        gate.notify_one();
        for wait in waits {
            tokio::time::timeout(Duration::from_secs(5), wait)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        let mut seen = script.seen.lock().unwrap().clone();
        seen.sort();
        assert_eq!(seen, vec![(7, 10), (9, 10)]);
        drop(batcher);
        finish(server).await;
        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn batch_stops_at_sixty_four_items() {
        let path = temp("sixty-four");
        let rpc = client_rpc(&path).await;
        let script = Script::open();
        let (address, server) = serve(script.clone()).await;
        let (batcher, gate) = gated(rpc);
        let mut waits = Vec::new();
        for index in 0..70 {
            waits.push(
                batcher
                    .enqueue(&address, 4, read(&format!("c:{index}")))
                    .unwrap(),
            );
        }
        gate.notify_one();
        for wait in waits {
            tokio::time::timeout(Duration::from_secs(5), wait)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        let mut seen = script.seen.lock().unwrap().clone();
        seen.sort();
        assert_eq!(seen, vec![(4, 6), (4, 64)]);
        drop(batcher);
        finish(server).await;
        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn queue_full_fails_without_waiting_for_the_peer() {
        let path = temp("full");
        let rpc = client_rpc(&path).await;
        let (batcher, _gate) = gated(rpc);
        let mut accepted = 0usize;
        let mut failed = None;
        for _ in 0..QUEUE_CAPACITY + 8 {
            let started = Instant::now();
            match batcher.enqueue("http://127.0.0.1:9", 1, read("c:q")) {
                Ok(_) => accepted += 1,
                Err(_) => {
                    failed = Some(started.elapsed());
                    break;
                }
            }
        }
        assert_eq!(accepted, QUEUE_CAPACITY);
        assert!(failed.unwrap() < Duration::from_millis(200));
        drop(batcher);
        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn unimplemented_falls_back_to_single_replica_and_stays_there() {
        let path = temp("legacy");
        let rpc = client_rpc(&path).await;
        let mut script = Script::open();
        script.legacy = true;
        let (address, server) = serve(script.clone()).await;
        let batcher = ReplicaBatcher::new(rpc);
        batcher.submit(&address, 1, read("c:a")).await.unwrap();
        batcher.submit(&address, 1, read("c:b")).await.unwrap();
        assert_eq!(script.singles.load(Ordering::SeqCst), 2);
        assert!(script.seen.lock().unwrap().is_empty());
        drop(batcher);
        finish(server).await;
        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn whole_batch_failure_still_lets_the_other_peer_satisfy_w2() {
        let path = temp("w2");
        let rpc = client_rpc(&path).await;
        let mut bad = Script::open();
        bad.fail_batch = true;
        let good = Script::open();
        let (bad_address, bad_server) = serve(bad).await;
        let (good_address, good_server) = serve(good.clone()).await;
        let batcher = ReplicaBatcher::new(rpc);
        let request = read("c:w2");
        let started = tokio::time::Instant::now();
        let good_batcher = batcher.clone();
        let bad_batcher = batcher.clone();
        let good_request = request.clone();
        let decision = tokio::time::timeout(
            Duration::from_secs(5),
            super::super::driver::start(
                vec![
                    Box::pin(async { Ok(Reply { record: None }) }),
                    Box::pin(
                        async move { good_batcher.submit(&good_address, 1, good_request).await },
                    ),
                    Box::pin(async move { bad_batcher.submit(&bad_address, 1, request).await }),
                ],
                started,
            ),
        )
        .await
        .expect("quorum")
        .expect("driver");
        assert!(decision.is_ok());
        assert_eq!(good.seen.lock().unwrap().as_slice(), [(1, 1)]);
        drop(batcher);
        finish(bad_server).await;
        finish(good_server).await;
        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn at_most_four_batch_rpcs_are_in_flight() {
        let path = temp("inflight");
        let rpc = client_rpc(&path).await;
        let mut script = Script::open();
        script.block = true;
        let (address, server) = serve(script.clone()).await;
        let batcher = ReplicaBatcher::new(rpc);
        let mut waits = Vec::new();
        for index in 0..256 {
            waits.push(
                batcher
                    .enqueue(&address, 1, read(&format!("c:{index}")))
                    .unwrap(),
            );
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while script.max_inflight.load(Ordering::SeqCst) < 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(script.max_inflight.load(Ordering::SeqCst), 4);
        script.release.store(true, Ordering::Release);
        script.wait.notify_waiters();
        for wait in waits {
            tokio::time::timeout(Duration::from_secs(5), wait)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        let seen = script.seen.lock().unwrap().clone();
        assert!(seen.iter().all(|(_, len)| *len <= 64));
        assert_eq!(seen.iter().map(|(_, len)| *len).sum::<usize>(), 256);
        assert!(seen.len() >= 4);
        drop(batcher);
        finish(server).await;
        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn one_batch_returns_each_operation_independently() {
        let path = temp("independent");
        let rpc = client_rpc(&path).await;
        let journal = Journal::open(
            &path.join("journal"),
            ("batch-test", 101),
            &[(101, "http://127.0.0.1:7101".into())],
        )
        .unwrap();
        journal
            .execute(write_record("old", record(5, 4, b"seed")))
            .await
            .unwrap();
        journal
            .execute(write_record("epoch", record(5, 1, b"seed")))
            .await
            .unwrap();
        journal
            .execute(write_record("conflict", record(5, 2, b"seed")))
            .await
            .unwrap();
        let mut script = Script::open();
        script.journal = Some(journal.clone());
        let (address, server) = serve(script.clone()).await;
        let (batcher, gate) = gated(rpc);
        let operations = [
            write_record("ok", record(5, 1, b"new")),
            read("ok"),
            write_record("old", record(5, 3, b"older")),
            write_record("epoch", record(4, 9, b"low")),
            write_record("conflict", record(5, 2, b"other")),
        ];
        let mut waits = Vec::new();
        for operation in operations {
            waits.push(batcher.enqueue(&address, 0, operation).unwrap());
        }
        gate.notify_one();
        let mut results = Vec::new();
        for wait in waits {
            results.push(
                tokio::time::timeout(Duration::from_secs(5), wait)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert!(results[0].is_ok());
        assert_eq!(
            results[1]
                .as_ref()
                .unwrap()
                .record
                .as_ref()
                .unwrap()
                .snapshot
                .as_deref(),
            Some(b"new".as_slice())
        );
        assert!(results[2].is_err());
        assert!(results[3].is_err());
        assert!(results[4].is_err());
        assert_eq!(script.seen.lock().unwrap().as_slice(), [(0, 5)]);
        assert_eq!(
            journal
                .execute(read("old"))
                .await
                .unwrap()
                .record
                .unwrap()
                .snapshot
                .as_deref(),
            Some(b"seed".as_slice())
        );
        drop(journal);
        drop(script);
        drop(batcher);
        finish(server).await;
        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn service_rejects_an_empty_or_oversized_batch_and_maps_each_result() {
        let path = temp("service");
        tokio::fs::create_dir_all(&path).await.unwrap();
        tokio::fs::write(path.join("token"), "b".repeat(32))
            .await
            .unwrap();
        let config = serde_json::from_value(serde_json::json!({
            "role": "broker",
            "cluster_id": "batch-service",
            "node_id": 101,
            "listen": "127.0.0.1:7101",
            "data_dir": path,
            "token_file": path.join("token"),
            "controllers": [
                {"id": 1, "address": "http://127.0.0.1:65001"},
                {"id": 2, "address": "http://127.0.0.1:65002"},
                {"id": 3, "address": "http://127.0.0.1:65003"}
            ],
            "durable": {"replicas": [
                {"id": 101, "address": "http://127.0.0.1:7101"},
                {"id": 102, "address": "http://127.0.0.1:7102"},
                {"id": 103, "address": "http://127.0.0.1:7103"}
            ]}
        }))
        .unwrap();
        let runtime = crate::broker::BrokerRuntime::new(config).unwrap();
        runtime.initialized.store(true, Ordering::Release);
        *runtime.deadline.lock().unwrap() = tokio::time::Instant::now() + Duration::from_secs(60);
        {
            let mut metadata = runtime.metadata.write().unwrap();
            metadata.data_mode = Some(crate::model::DataMode::Memory(vec![
                (101, "http://127.0.0.1:7101".into()),
                (102, "http://127.0.0.1:7102".into()),
                (103, "http://127.0.0.1:7103".into()),
            ]));
            metadata
                .disk_ids
                .insert(101, runtime.journal.as_ref().unwrap().disk_id().to_owned());
            metadata.nodes.insert(
                101,
                crate::model::Node {
                    id: 101,
                    incarnation: runtime.incarnation.clone(),
                    address: "http://127.0.0.1:7101".into(),
                    expires_at: crate::controller::now() + 60_000,
                    max_lease_ms: 3000,
                    applied: 0,
                },
            );
        }
        assert!(runtime.ready());
        runtime
            .journal
            .as_ref()
            .unwrap()
            .execute(write_record("old", record(5, 4, b"seed")))
            .await
            .unwrap();
        let rpc = ClientRpc::new(runtime_config(&runtime)).await.unwrap();
        let service = crate::controller::Service {
            control: None,
            broker: Some(Arc::clone(&runtime)),
            rpc: rpc.clone(),
        };
        let mixed = ReplicaBatchMessage {
            generation: 0,
            operations: vec![
                write_record("ok", record(5, 1, b"new")),
                write_record("old", record(5, 3, b"older")),
            ],
        };
        let response = Cluster::replica_batch(
            &service,
            rpc.authorize(rpc::encode(&mixed).unwrap()).unwrap(),
        )
        .await
        .unwrap();
        let reply: ReplicaBatchReply = rpc::decode(response.into_inner()).unwrap();
        assert!(reply.results[0].is_some());
        assert!(reply.results[1].is_none());
        for operations in [Vec::new(), vec![read("c:x"); 65]] {
            let status = Cluster::replica_batch(
                &service,
                rpc.authorize(
                    rpc::encode(&ReplicaBatchMessage {
                        generation: 0,
                        operations,
                    })
                    .unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap_err();
            assert_eq!(status.code(), tonic::Code::InvalidArgument);
        }
        drop(runtime);
        let _ = std::fs::remove_dir_all(path);
    }

    fn runtime_config(runtime: &crate::broker::BrokerRuntime) -> Arc<crate::config::Config> {
        Arc::clone(&runtime.config)
    }
}
