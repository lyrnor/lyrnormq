//! Cross-connection deliveries share one RPC per destination. Each recipient is still judged alone.
use super::durable::RecipientDelivery;
use super::{DELIVER_BATCH_LIMIT, DeliverBatchMessage, DeliverBatchReply};
use crate::profile::{self, Metric};
use crate::rpc::{self, ClientRpc, wire};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
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
const MAX_IN_FLIGHT: usize = 32;
const MAX_BATCH_BYTES: usize = 2 * 1024 * 1024;
const MAX_PEERS: usize = 64;

pub(crate) fn batch_allowed(len: usize) -> bool {
    (1..=DELIVER_BATCH_LIMIT).contains(&len)
}

struct Item {
    delivery: RecipientDelivery,
    reply: oneshot::Sender<io::Result<()>>,
}
struct Outbound {
    items: Vec<Item>,
    bytes: Vec<u8>,
}
#[derive(Clone)]
struct Peer {
    tx: mpsc::Sender<Item>,
    legacy: Arc<AtomicBool>,
    gather: Arc<Mutex<()>>,
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
pub(in crate::broker) struct DeliveryBatcher {
    inner: Arc<Inner>,
}
impl DeliveryBatcher {
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
    /// Queue every delivery before the first await so the sender can fill one batch.
    pub(in crate::broker) async fn submit_many(
        &self,
        address: &str,
        deliveries: Vec<RecipientDelivery>,
    ) -> Vec<io::Result<()>> {
        if deliveries.is_empty() {
            return Vec::new();
        }
        let peer = match self.peer(address) {
            Ok(peer) => peer,
            Err(error) => {
                let kind = error.kind();
                let text = error.to_string();
                return std::iter::repeat_with(|| Err(io::Error::new(kind, text.clone())))
                    .take(deliveries.len())
                    .collect();
            }
        };
        if peer.legacy.load(Ordering::Acquire) {
            let mut results = Vec::with_capacity(deliveries.len());
            for delivery in deliveries {
                results.push(send_one(&self.inner.rpc, address, delivery).await);
            }
            return results;
        }
        let queued = {
            let _gather = peer.gather.lock().unwrap_or_else(PoisonError::into_inner);
            deliveries
                .into_iter()
                .map(|delivery| queue(&peer, delivery))
                .collect::<Vec<_>>()
        };
        let mut results = Vec::with_capacity(queued.len());
        for queued in queued {
            match queued {
                Ok(rx) => results.push(match rx.await {
                    Ok(result) => result,
                    Err(_) => Err(peer_failed()),
                }),
                Err(error) => results.push(Err(error)),
            }
        }
        results
    }
    #[cfg(test)]
    fn enqueue(
        &self,
        address: &str,
        delivery: RecipientDelivery,
    ) -> io::Result<oneshot::Receiver<io::Result<()>>> {
        let peer = self.peer(address)?;
        queue(&peer, delivery)
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
        let gather = Arc::new(Mutex::new(()));
        let peer = Peer {
            tx,
            legacy: Arc::clone(&legacy),
            gather: Arc::clone(&gather),
        };
        peers.insert(address.to_owned(), peer.clone());
        drop(peers);
        let task = tokio::spawn(run_peer(
            self.inner.rpc.clone(),
            address.to_owned(),
            rx,
            legacy,
            gather,
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
    delivery: RecipientDelivery,
) -> io::Result<oneshot::Receiver<io::Result<()>>> {
    let (tx, rx) = oneshot::channel();
    peer.tx
        .try_send(Item {
            delivery,
            reply: tx,
        })
        .map_err(|_| peer_failed())?;
    Ok(rx)
}
fn peer_failed() -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, "recipient unavailable")
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
    gather: Arc<Mutex<()>>,
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
            let result = send_one(&rpc, &address, first.delivery).await;
            let _ = first.reply.send(result);
            continue;
        }
        let (outbound, rest) = {
            let _gather = gather.lock().unwrap_or_else(PoisonError::into_inner);
            take_batch(first, &mut incoming)
        };
        held.extend(rest);
        let Ok(permit) = Arc::clone(&limit).acquire_owned().await else {
            fail_all(outbound.items, "recipient unavailable");
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
    let mut batch = Vec::with_capacity(DELIVER_BATCH_LIMIT);
    batch.push(first);
    while batch.len() < DELIVER_BATCH_LIMIT {
        match incoming.try_recv() {
            Ok(item) => batch.push(item),
            Err(_) => break,
        }
    }
    let (bytes, keep) = match encode_fitting(&batch) {
        Ok((bytes, keep)) if keep > 0 && keep <= batch.len() => (bytes, keep),
        _ => {
            while batch.len() > 1 {
                if let Some(item) = batch.pop() {
                    // Rebuilt below; the failed encode sends nothing.
                    let _ = item.reply.send(Err(peer_failed()));
                }
            }
            return (
                Outbound {
                    items: batch,
                    bytes: Vec::new(),
                },
                VecDeque::new(),
            );
        }
    };
    let deferred = if keep < batch.len() {
        VecDeque::from(batch.split_off(keep))
    } else {
        VecDeque::new()
    };
    (
        Outbound {
            items: batch,
            bytes,
        },
        deferred,
    )
}
fn encode_fitting(batch: &[Item]) -> io::Result<(Vec<u8>, usize)> {
    if batch.is_empty() {
        return Err(peer_failed());
    }
    let mut body = serde_json::to_vec(&DeliverBatchMessage {
        deliveries: Vec::new(),
    })
    .map_err(io::Error::other)?;
    if body.len() < 2 || &body[body.len() - 2..] != b"]}" {
        return Err(io::Error::other("deliver batch encoding"));
    }
    body.truncate(body.len() - 2);
    let mut keep = 0usize;
    for item in batch {
        let encoded = serde_json::to_vec(&item.delivery).map_err(io::Error::other)?;
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
        return Err(io::Error::other("deliver batch encoding"));
    }
    body.extend_from_slice(b"]}");
    Ok((body, keep))
}

async fn deliver(rpc: ClientRpc, address: String, legacy: Arc<AtomicBool>, outbound: Outbound) {
    if outbound.items.is_empty()
        || outbound.bytes.is_empty()
        || outbound.bytes.len() > rpc::MAX_WIRE
    {
        fail_all(outbound.items, "recipient unavailable");
        return;
    }
    let expected = outbound.items.len();
    match send_batch(&rpc, &address, outbound.bytes, expected).await {
        Ok(results) => {
            for (item, ok) in outbound.items.into_iter().zip(results) {
                let reply = if ok { Ok(()) } else { Err(peer_failed()) };
                let _ = item.reply.send(reply);
            }
        }
        Err(BatchError::Unimplemented) => {
            legacy.store(true, Ordering::Release);
            for item in outbound.items {
                let result = send_one(&rpc, &address, item.delivery).await;
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
) -> Result<Vec<bool>, BatchError> {
    let mut client = rpc
        .delivery_client(address)
        .await
        .map_err(|error| BatchError::Failed(error.to_string()))?;
    profile::observe(Metric::DeliverBatchSize, expected as u64);
    profile::observe(Metric::DeliverRpc, 1);
    let response = client
        .deliver_batch(
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
    let reply: DeliverBatchReply = rpc::decode(response.into_inner())
        .map_err(|error| BatchError::Failed(error.to_string()))?;
    if reply.results.len() != expected {
        return Err(BatchError::Failed("recipient unavailable".to_owned()));
    }
    Ok(reply.results)
}
async fn send_one(rpc: &ClientRpc, address: &str, delivery: RecipientDelivery) -> io::Result<()> {
    let envelope = rpc::encode(&delivery)?;
    let mut client = rpc.delivery_client(address).await?;
    profile::observe(Metric::DeliverRpc, 1);
    client
        .deliver_to(rpc.authorize(envelope)?)
        .await
        .map_err(rpc::io_error)?;
    Ok(())
}

/// One client stays ordered. The first failure leaves every later item unattempted.
async fn attempt_in_order<T, F, Fut>(items: Vec<T>, mut attempt: F) -> Vec<bool>
where
    F: FnMut(T) -> Fut,
    Fut: Future<Output = io::Result<()>>,
{
    let mut results = Vec::with_capacity(items.len());
    let mut failed = false;
    for item in items {
        if failed {
            results.push(false);
            continue;
        }
        match attempt(item).await {
            Ok(()) => results.push(true),
            Err(_) => {
                failed = true;
                results.push(false);
            }
        }
    }
    results
}

pub(crate) async fn deliver_many(
    broker: Arc<super::BrokerRuntime>,
    deliveries: Vec<RecipientDelivery>,
) -> Vec<bool> {
    let mut out = vec![false; deliveries.len()];
    let mut groups: BTreeMap<String, Vec<(usize, RecipientDelivery)>> = BTreeMap::new();
    for (index, delivery) in deliveries.into_iter().enumerate() {
        groups
            .entry(delivery.client_id.clone())
            .or_default()
            .push((index, delivery));
    }
    let mut joins = tokio::task::JoinSet::new();
    for (_, items) in groups {
        let broker = Arc::clone(&broker);
        joins.spawn(async move {
            let mut indices = Vec::with_capacity(items.len());
            let mut bodies = Vec::with_capacity(items.len());
            for (index, delivery) in items {
                indices.push(index);
                bodies.push(delivery);
            }
            let flags = attempt_in_order(bodies, |delivery| {
                let broker = Arc::clone(&broker);
                async move { broker.deliver_recipient(delivery).await }
            })
            .await;
            indices.into_iter().zip(flags).collect::<Vec<_>>()
        });
    }
    while let Some(joined) = joins.join_next().await {
        if let Ok(results) = joined {
            for (index, ok) in results {
                if let Some(slot) = out.get_mut(index) {
                    *slot = ok;
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::wire::cluster_server::{Cluster, ClusterServer};
    use mqtt_cluster_core::{ClusterMessage, DeliveryId, ForwardScope};
    use std::{
        path::PathBuf,
        sync::atomic::AtomicUsize,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };
    use tonic::{Request as RpcRequest, Response, Status};

    #[derive(Clone)]
    struct Script {
        legacy: bool,
        fail: bool,
        mask: Option<Vec<bool>>,
        seen: Arc<Mutex<Vec<usize>>>,
        singles: Arc<AtomicUsize>,
    }
    impl Script {
        fn open() -> Self {
            Self {
                legacy: false,
                fail: false,
                mask: None,
                seen: Arc::default(),
                singles: Arc::default(),
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
            _: RpcRequest<wire::Envelope>,
        ) -> Result<Response<wire::Envelope>, Status> {
            Err(Status::unimplemented("replica"))
        }
        async fn replica_batch(
            &self,
            _: RpcRequest<wire::Envelope>,
        ) -> Result<Response<wire::Envelope>, Status> {
            Err(Status::unimplemented("replica_batch"))
        }
        async fn deliver_to(
            &self,
            _: RpcRequest<wire::Envelope>,
        ) -> Result<Response<wire::Envelope>, Status> {
            self.singles.fetch_add(1, Ordering::SeqCst);
            Ok(Response::new(wire::Envelope::default()))
        }
        async fn deliver_batch(
            &self,
            request: RpcRequest<wire::Envelope>,
        ) -> Result<Response<wire::Envelope>, Status> {
            if self.legacy {
                return Err(Status::unimplemented("deliver_batch"));
            }
            if self.fail {
                return Err(Status::unavailable("deliver down"));
            }
            let message: DeliverBatchMessage = rpc::decode(request.into_inner())?;
            self.seen.lock().unwrap().push(message.deliveries.len());
            let results = self
                .mask
                .clone()
                .filter(|mask| mask.len() == message.deliveries.len())
                .unwrap_or_else(|| vec![true; message.deliveries.len()]);
            Ok(Response::new(rpc::encode(&DeliverBatchReply { results })?))
        }
        async fn deliver(
            &self,
            _: RpcRequest<wire::Envelope>,
        ) -> Result<Response<wire::Envelope>, Status> {
            Err(Status::unimplemented("deliver"))
        }
        async fn forward_batch(
            &self,
            _: RpcRequest<wire::Envelope>,
        ) -> Result<Response<wire::Envelope>, Status> {
            Err(Status::unimplemented("forward_batch"))
        }
    }

    fn delivery(client: &str, n: u64) -> RecipientDelivery {
        RecipientDelivery {
            client_id: client.into(),
            id: DeliveryId {
                source: format!("h:{n}"),
                epoch: 1,
                revision: n,
                index: 0,
                target_generation: 1,
            },
            message: ClusterMessage {
                topic: "t".into(),
                payload: n.to_le_bytes().to_vec(),
                qos: 0,
                retain: false,
                properties: Default::default(),
                expires_at: None,
                publisher: None,
                observer_client: String::new(),
                scope: ForwardScope::Ordinary,
            },
        }
    }
    fn item(delivery: RecipientDelivery) -> Item {
        Item {
            delivery,
            reply: oneshot::channel().0,
        }
    }
    fn temp(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "lyrnormq-deliver-batch-{label}-{}-{}",
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
            "cluster_id": "deliver-batch-test",
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
        ClientRpc::new(Arc::new(config)).await.unwrap()
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
    fn gated(rpc: ClientRpc) -> (DeliveryBatcher, Arc<Notify>) {
        let gate = Arc::new(Notify::new());
        let batcher = DeliveryBatcher::create(rpc, Some(Arc::clone(&gate)));
        (batcher, gate)
    }
    async fn finish(server: JoinHandle<Result<(), tonic::transport::Error>>) {
        server.abort();
        let _ = server.await;
    }

    #[test]
    fn empty_and_oversized_batches_are_rejected() {
        assert!(!batch_allowed(0));
        assert!(batch_allowed(1));
        assert!(batch_allowed(DELIVER_BATCH_LIMIT));
        assert!(!batch_allowed(DELIVER_BATCH_LIMIT + 1));
    }

    #[test]
    fn encoded_batch_matches_the_owned_message_and_respects_two_mib() {
        let batch = vec![item(delivery("a", 1)), item(delivery("b", 2))];
        let (bytes, keep) = encode_fitting(&batch).unwrap();
        assert_eq!(keep, 2);
        let parsed: DeliverBatchMessage = serde_json::from_slice(&bytes).unwrap();
        let owned = serde_json::to_vec(&DeliverBatchMessage {
            deliveries: batch.iter().map(|item| item.delivery.clone()).collect(),
        })
        .unwrap();
        assert_eq!(bytes, owned);
        assert_eq!(parsed.deliveries.len(), 2);
        let mut huge = delivery("c", 3);
        huge.message.payload = vec![1; 1_200_000];
        let (bytes, keep) =
            encode_fitting(&[item(delivery("a", 1)), item(huge), item(delivery("b", 2))]).unwrap();
        assert_eq!(keep, 1);
        assert!(bytes.len() <= MAX_BATCH_BYTES);
    }

    #[tokio::test]
    async fn batch_stops_at_sixty_four_items() {
        let path = temp("sixty-four");
        let rpc = client_rpc(&path).await;
        let script = Script::open();
        let seen = Arc::clone(&script.seen);
        let (address, server) = serve(script).await;
        let (batcher, gate) = gated(rpc);
        let mut waits = Vec::new();
        for index in 0..70 {
            waits.push(batcher.enqueue(&address, delivery("c", index)).unwrap());
        }
        gate.notify_one();
        for wait in waits {
            tokio::time::timeout(Duration::from_secs(5), wait)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        let mut sizes = seen.lock().unwrap().clone();
        sizes.sort();
        assert_eq!(sizes, vec![6, 64]);
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
            match batcher.enqueue("http://127.0.0.1:9", delivery("c", 1)) {
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
    async fn unimplemented_falls_back_to_single_deliver_and_stays_there() {
        let path = temp("legacy");
        let rpc = client_rpc(&path).await;
        let mut script = Script::open();
        script.legacy = true;
        let singles = Arc::clone(&script.singles);
        let seen = Arc::clone(&script.seen);
        let (address, server) = serve(script).await;
        let (batcher, gate) = gated(rpc);
        let mut waits = Vec::new();
        for index in 0..2 {
            waits.push(batcher.enqueue(&address, delivery("c", index)).unwrap());
        }
        gate.notify_one();
        for wait in waits {
            tokio::time::timeout(Duration::from_secs(5), wait)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        assert!(seen.lock().unwrap().is_empty());
        assert_eq!(singles.load(Ordering::SeqCst), 2);
        let again = batcher.enqueue(&address, delivery("c", 3)).unwrap();
        tokio::time::timeout(Duration::from_secs(5), again)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(seen.lock().unwrap().is_empty());
        assert_eq!(singles.load(Ordering::SeqCst), 3);
        drop(batcher);
        finish(server).await;
        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn whole_batch_failure_fails_each_waiter() {
        let path = temp("down");
        let rpc = client_rpc(&path).await;
        let mut script = Script::open();
        script.fail = true;
        let (address, server) = serve(script).await;
        let (batcher, gate) = gated(rpc);
        let mut waits = Vec::new();
        for index in 0..3 {
            waits.push(batcher.enqueue(&address, delivery("c", index)).unwrap());
        }
        gate.notify_one();
        for wait in waits {
            let result = tokio::time::timeout(Duration::from_secs(5), wait)
                .await
                .unwrap()
                .unwrap();
            assert!(result.is_err());
        }
        drop(batcher);
        finish(server).await;
        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn one_batch_returns_each_delivery_independently() {
        let path = temp("items");
        let rpc = client_rpc(&path).await;
        let mut script = Script::open();
        script.mask = Some(vec![true, false, true]);
        let (address, server) = serve(script).await;
        let (batcher, gate) = gated(rpc);
        let mut waits = Vec::new();
        for index in 0..3 {
            waits.push(
                batcher
                    .enqueue(&address, delivery(&format!("c{index}"), index))
                    .unwrap(),
            );
        }
        gate.notify_one();
        let mut results = Vec::new();
        for wait in waits {
            results.push(
                tokio::time::timeout(Duration::from_secs(5), wait)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_ok(),
            );
        }
        assert_eq!(results, vec![true, false, true]);
        drop(batcher);
        finish(server).await;
        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn submit_many_coalesces_a_burst() {
        let path = temp("burst");
        let rpc = client_rpc(&path).await;
        let script = Script::open();
        let seen = Arc::clone(&script.seen);
        let (address, server) = serve(script).await;
        let (batcher, gate) = gated(rpc);
        let batcher_task = batcher.clone();
        let address_task = address.clone();
        let submitted = tokio::spawn(async move {
            let deliveries = (0..10).map(|index| delivery("c", index)).collect();
            batcher_task.submit_many(&address_task, deliveries).await
        });
        tokio::task::yield_now().await;
        gate.notify_one();
        let results = tokio::time::timeout(Duration::from_secs(5), submitted)
            .await
            .unwrap()
            .unwrap();
        assert!(results.iter().all(|result| result.is_ok()));
        assert_eq!(seen.lock().unwrap().as_slice(), &[10]);
        drop(batcher);
        finish(server).await;
        let _ = std::fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn failed_same_client_item_skips_the_rest() {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        let flags = attempt_in_order(vec!["a", "b", "c"], |name| {
            let seen = Arc::clone(&seen);
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                if name == "b" {
                    Err(io::Error::other("stopped"))
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert_eq!(flags, vec![true, false, false]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
