//! Readonly metadata replica, conservative leases and direct broker forwarding.
use crate::{
    config::{Config, Role},
    controller::{self, Service},
    model::*,
    rpc::{self, ClientRpc, wire},
};
use mqtt_cluster_core::{ClusterBridge, ClusterFuture, ClusterMessage, ForwardScope, LocalBroker};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt, io,
    sync::{
        Arc, Mutex, PoisonError, RwLock, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{OnceCell, Semaphore, watch};
#[derive(Default)]
struct Receipts {
    values: BTreeMap<(u64, String, u64), bool>,
    order: VecDeque<(u64, String, u64)>,
}
pub struct BrokerRuntime {
    config: Arc<Config>,
    incarnation: String,
    rpc: OnceCell<ClientRpc>,
    local: Mutex<Option<Arc<dyn LocalBroker>>>,
    metadata: RwLock<Metadata>,
    deadline: Mutex<tokio::time::Instant>,
    opened: AtomicBool,
    stopped: AtomicBool,
    initialized: AtomicBool,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    sequence: AtomicU64,
    slots: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    receipts: Mutex<Receipts>,
    weak: Weak<Self>,
}
impl fmt::Debug for BrokerRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerRuntime")
            .field("node_id", &self.config.node_id)
            .field("ready", &self.ready())
            .finish_non_exhaustive()
    }
}
fn unavailable() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "cluster metadata or peer unavailable",
    )
}
impl BrokerRuntime {
    pub fn new(config: Config) -> io::Result<Arc<Self>> {
        config.validate()?;
        if config.role != Role::Broker {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "broker role required",
            ));
        }
        let mut random = [0u8; 16];
        getrandom::getrandom(&mut random).map_err(io::Error::other)?;
        let incarnation = random.iter().map(|b| format!("{b:02x}")).collect();
        Ok(Arc::new_cyclic(|weak| Self {
            config: Arc::new(config),
            incarnation,
            rpc: OnceCell::new(),
            local: Mutex::new(None),
            metadata: RwLock::new(Metadata::default()),
            deadline: Mutex::new(tokio::time::Instant::now()),
            opened: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            initialized: AtomicBool::new(false),
            tasks: Mutex::new(Vec::new()),
            sequence: AtomicU64::new(1),
            slots: Arc::new(Semaphore::new(32)),
            bytes: Arc::new(Semaphore::new(16 * 1024 * 1024)),
            receipts: Mutex::new(Receipts::default()),
            weak: weak.clone(),
        }))
    }
    async fn rpc(&self) -> io::Result<&ClientRpc> {
        self.rpc
            .get_or_try_init(|| ClientRpc::new(self.config.clone()))
            .await
    }
    fn local(&self) -> io::Result<Arc<dyn LocalBroker>> {
        self.local
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .ok_or_else(unavailable)
    }
    async fn barrier(&self, revision: u64) -> io::Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if !self.ready() {
                    return Err(unavailable());
                }
                let metadata = self.rpc().await?.metadata().await?;
                if metadata.nodes.values().all(|node| node.applied >= revision) {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| unavailable())?
    }
    async fn command(&self, command: Command, barrier: bool) -> io::Result<()> {
        if !self.ready() {
            return Err(unavailable());
        }
        let _slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable())?;
        let answer = self.rpc().await?.mutate(command).await?;
        if barrier {
            self.barrier(answer.revision).await?;
        }
        Ok(())
    }
    async fn install(&self, next: Metadata) -> io::Result<()> {
        let old = self
            .metadata
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if next.revision < old.revision {
            return Err(unavailable());
        }
        let local = self.local()?;
        for (client, previous) in &old.clients {
            if previous.node == self.config.node_id
                && previous.incarnation == self.incarnation
                && !next.clients.get(client).is_some_and(|c| {
                    c.node == previous.node
                        && c.incarnation == previous.incarnation
                        && c.generation == previous.generation
                })
            {
                local.fence(client.clone(), previous.generation).await?;
            }
        }
        if !next
            .nodes
            .get(&self.config.node_id)
            .is_some_and(|n| n.incarnation == self.incarnation)
        {
            local.fence_all().await?;
            self.initialized.store(false, Ordering::Release);
        }
        *self
            .metadata
            .write()
            .unwrap_or_else(PoisonError::into_inner) = next;
        self.initialized.store(true, Ordering::Release);
        Ok(())
    }
    async fn watch_loop(self: Arc<Self>) {
        while !self.stopped.load(Ordering::Acquire) {
            let rpc = match self.rpc().await {
                Ok(r) => r.clone(),
                Err(_) => return,
            };
            let mut stream = None;
            for peer in &self.config.controllers {
                let Ok(mut client) = rpc.client(&peer.address).await else {
                    continue;
                };
                let Ok(request) = rpc.authorize(wire::Envelope { data: Vec::new() }) else {
                    continue;
                };
                if let Ok(response) = client.watch(request).await {
                    stream = Some(response.into_inner());
                    break;
                }
            }
            if let Some(mut stream) = stream {
                while !self.stopped.load(Ordering::Acquire) {
                    match stream.message().await {
                        Ok(Some(message)) => match rpc::decode(message) {
                            Ok(metadata) => {
                                if self.install(metadata).await.is_err() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        },
                        _ => break,
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    async fn heartbeat_loop(self: Arc<Self>) {
        while !self.stopped.load(Ordering::Acquire) {
            let start = tokio::time::Instant::now();
            let applied = self
                .metadata
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .revision;
            let command = Command::Heartbeat {
                node: self.config.node_id,
                incarnation: self.incarnation.clone(),
                applied,
                expires_at: controller::now().saturating_add(self.config.lease_ms),
            };
            if let Ok(rpc) = self.rpc().await
                && let Ok(answer) = rpc.mutate(command).await
            {
                let lease = self.config.lease_ms.min(answer.lease_ms);
                if lease >= 1000 {
                    *self.deadline.lock().unwrap_or_else(PoisonError::into_inner) =
                        start + Duration::from_millis(lease.saturating_sub(100));
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    async fn lease_loop(self: Arc<Self>) {
        let mut was_ready = false;
        while !self.stopped.load(Ordering::Acquire) {
            let ready = self.ready();
            if was_ready
                && !ready
                && let Ok(local) = self.local()
            {
                let _ = local.fence_all().await;
            }
            was_ready = ready;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    pub(crate) async fn deliver(&self, forward: Forward) -> io::Result<()> {
        if !self.ready() {
            return Err(unavailable());
        }
        let _slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable())?;
        if forward.message.payload.len() > 1024 * 1024
            || forward.message.qos > 2
            || forward.message.retain
            || forward.message.scope != ForwardScope::Ordinary
            || !mqtt_router::validate_topic_filter(&forward.message.topic)
            || forward.message.topic.contains(['+', '#'])
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid cluster delivery",
            ));
        }
        if !self
            .metadata
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .nodes
            .get(&forward.node)
            .is_some_and(|node| node.incarnation == forward.incarnation)
        {
            return Err(unavailable());
        }
        if let Some(publisher) = &forward.message.publisher {
            let metadata = self.metadata.read().unwrap_or_else(PoisonError::into_inner);
            if !metadata.clients.get(publisher).is_some_and(|owner| {
                owner.node == forward.node && owner.incarnation == forward.incarnation
            }) {
                return Err(unavailable());
            }
        }
        let bytes = u32::try_from(forward.message.payload.len()).map_err(|_| unavailable())?;
        let _bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| unavailable())?;
        let key = (forward.node, forward.incarnation, forward.sequence);
        {
            let mut receipts = self.receipts.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(done) = receipts.values.get(&key) {
                return if *done { Ok(()) } else { Err(unavailable()) };
            }
            if receipts.values.len() >= 8192 {
                if let Some(old) = receipts.order.pop_front() {
                    receipts.values.remove(&old);
                } else {
                    return Err(unavailable());
                }
            }
            receipts.values.insert(key.clone(), false);
        }
        let guard = ReceiptGuard {
            runtime: self,
            key: key.clone(),
        };
        let result = tokio::time::timeout(
            Duration::from_secs(4),
            self.local()?.deliver(forward.message),
        )
        .await
        .unwrap_or_else(|_| Err(unavailable()));
        let mut receipts = self.receipts.lock().unwrap_or_else(PoisonError::into_inner);
        if result.is_ok() {
            receipts.values.insert(key.clone(), true);
            receipts.order.push_back(key);
        } else {
            receipts.values.remove(&key);
        }
        drop(receipts);
        drop(guard);
        result
    }
}
impl ClusterBridge for BrokerRuntime {
    fn ready(&self) -> bool {
        !self.stopped.load(Ordering::Acquire)
            && self.initialized.load(Ordering::Acquire)
            && tokio::time::Instant::now()
                < *self.deadline.lock().unwrap_or_else(PoisonError::into_inner)
            && self
                .metadata
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .nodes
                .get(&self.config.node_id)
                .is_some_and(|n| n.incarnation == self.incarnation)
    }
    fn open(&self, local: Arc<dyn LocalBroker>) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            if self.opened.swap(true, Ordering::AcqRel) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "cluster runtime already opened",
                ));
            }
            *self.local.lock().unwrap_or_else(PoisonError::into_inner) = Some(local);
            let rpc = self.rpc().await?;
            tokio::time::timeout(Duration::from_secs(12), async {
                loop {
                    if rpc
                        .mutate(Command::Register {
                            node: self.config.node_id,
                            incarnation: self.incarnation.clone(),
                            address: self.config.address(),
                            expires_at: controller::now() + self.config.lease_ms,
                        })
                        .await
                        .is_ok()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .map_err(|_| unavailable())?;
            let this = self.weak.upgrade().ok_or_else(unavailable)?;
            let tasks = vec![
                tokio::spawn(this.clone().watch_loop()),
                tokio::spawn(this.clone().heartbeat_loop()),
                tokio::spawn(this.lease_loop()),
            ];
            self.tasks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend(tasks);
            tokio::time::timeout(Duration::from_secs(10), async {
                while !self.ready() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .map_err(|_| unavailable())?;
            for subscription in self.local()?.subscriptions().await? {
                self.claim(subscription.client_id.clone(), 1).await?;
                self.subscriptions(subscription.client_id, 1, subscription.filters)
                    .await?;
            }
            Ok(())
        })
    }
    fn claim(&self, client_id: String, generation: u64) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            self.command(
                Command::Claim {
                    node: self.config.node_id,
                    incarnation: self.incarnation.clone(),
                    client: client_id,
                    generation,
                },
                true,
            )
            .await
        })
    }
    fn release(&self, client_id: String, generation: u64) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            self.command(
                Command::Release {
                    node: self.config.node_id,
                    incarnation: self.incarnation.clone(),
                    client: client_id,
                    generation,
                },
                false,
            )
            .await
        })
    }
    fn subscriptions(
        &self,
        client_id: String,
        generation: u64,
        filters: Vec<String>,
    ) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            self.command(
                Command::Filters {
                    generation,
                    node: self.config.node_id,
                    incarnation: self.incarnation.clone(),
                    client: client_id,
                    filters,
                },
                true,
            )
            .await
        })
    }
    fn forward(&self, message: ClusterMessage) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            if !self.ready() {
                return Err(unavailable());
            }
            let _slot = self
                .slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| unavailable())?;
            let bytes = u32::try_from(message.payload.len()).map_err(|_| unavailable())?;
            let _bytes = self
                .bytes
                .clone()
                .try_acquire_many_owned(bytes)
                .map_err(|_| unavailable())?;
            let targets = {
                let metadata = self.metadata.read().unwrap_or_else(PoisonError::into_inner);
                let nodes: BTreeSet<_> = metadata
                    .routes
                    .iter()
                    .filter(|(filter, _)| mqtt_router::matches_topic_filter(filter, &message.topic))
                    .flat_map(|(_, nodes)| nodes)
                    .filter(|node| **node != self.config.node_id)
                    .copied()
                    .collect();
                nodes
                    .into_iter()
                    .map(|node| metadata.nodes.get(&node).cloned().ok_or_else(unavailable))
                    .collect::<io::Result<Vec<_>>>()?
            };
            let sequence = self
                .sequence
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
                .map_err(|_| unavailable())?;
            let forward = Forward {
                node: self.config.node_id,
                incarnation: self.incarnation.clone(),
                sequence,
                message,
            };
            let rpc = self.rpc().await?;
            tokio::time::timeout(Duration::from_secs(5), async {
                for target in targets {
                    let mut okay = false;
                    for _ in 0..2 {
                        if !self.ready() {
                            return Err(unavailable());
                        }
                        let mut client = rpc.client(&target.address).await?;
                        if client
                            .deliver(rpc.authorize(rpc::encode(&forward)?)?)
                            .await
                            .is_ok()
                        {
                            okay = true;
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    if !okay {
                        return Err(unavailable());
                    }
                }
                Ok(())
            })
            .await
            .map_err(|_| unavailable())?
        })
    }
    fn shutdown(&self) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            self.stopped.store(true, Ordering::Release);
            let tasks =
                std::mem::take(&mut *self.tasks.lock().unwrap_or_else(PoisonError::into_inner));
            for task in tasks {
                task.abort();
                let _ = task.await;
            }
            if let Ok(local) = self.local() {
                let _ = local.fence_all().await;
            }
            Ok(())
        })
    }
}
pub async fn serve_broker(
    runtime: Arc<BrokerRuntime>,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    let rpc = runtime.rpc().await?.clone();
    let service = wire::cluster_server::ClusterServer::new(Service {
        control: None,
        broker: Some(runtime.clone()),
        rpc,
    })
    .max_decoding_message_size(rpc::MAX_WIRE)
    .max_encoding_message_size(rpc::MAX_WIRE);
    let mut server = tonic::transport::Server::builder()
        .concurrency_limit_per_connection(32)
        .timeout(Duration::from_secs(5));
    if let Some(tls) = rpc::server_tls(&runtime.config).await? {
        server = server.tls_config(tls).map_err(io::Error::other)?;
    }
    server
        .add_service(service)
        .serve_with_shutdown(runtime.config.listen, async move {
            let _ = shutdown.wait_for(|s| *s).await;
        })
        .await
        .map_err(io::Error::other)
}

struct ReceiptGuard<'a> {
    runtime: &'a BrokerRuntime,
    key: (u64, String, u64),
}
impl Drop for ReceiptGuard<'_> {
    fn drop(&mut self) {
        let mut receipts = self
            .runtime
            .receipts
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if receipts.values.get(&self.key) == Some(&false) {
            receipts.values.remove(&self.key);
        }
    }
}
