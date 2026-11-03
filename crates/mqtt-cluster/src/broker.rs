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
use tokio::sync::{Notify, OnceCell, watch};
pub(crate) mod durable;
mod progress;
mod quorum;
pub(crate) mod repair;
mod startup;
mod updates;
#[derive(Default)]
struct Receipts {
    values: BTreeMap<(u64, String, u64), bool>,
    order: VecDeque<(u64, String, u64)>,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ReplicaMessage {
    pub generation: u64,
    pub operation: crate::replica::Request,
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
    receipts: Mutex<Receipts>,
    watch_reconnect: Notify,
    installed: watch::Sender<u64>,
    weak: Weak<Self>,
    pub(crate) journal: Option<crate::replica::Journal>,
    data: Option<Arc<durable::Durable>>,
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
        let journal = config
            .durable
            .as_ref()
            .map(|d| {
                let members = d
                    .replicas
                    .iter()
                    .map(|p| (p.id, p.address.clone()))
                    .collect::<Vec<_>>();
                crate::replica::Journal::open(
                    &config.data_dir,
                    (&config.cluster_id, config.node_id),
                    &members,
                )
            })
            .transpose()?;
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
            receipts: Mutex::new(Receipts::default()),
            watch_reconnect: Notify::new(),
            installed: watch::channel(0).0,
            weak: weak.clone(),
            data: journal
                .as_ref()
                .map(|j| Arc::new(durable::Durable::new(weak.clone(), j.clone()))),
            journal,
        }))
    }
    pub(crate) fn control_ready(&self) -> bool {
        if self.stopped.load(Ordering::Acquire)
            || !self.initialized.load(Ordering::Acquire)
            || tokio::time::Instant::now()
                >= *self.deadline.lock().unwrap_or_else(PoisonError::into_inner)
        {
            return false;
        }
        let metadata = self.metadata.read().unwrap_or_else(PoisonError::into_inner);
        if !startup::storage_matches(&self.config, &metadata) {
            return false;
        }
        metadata
            .nodes
            .get(&self.config.node_id)
            .is_some_and(|n| n.incarnation == self.incarnation)
            && self.journal.as_ref().is_none_or(|journal| {
                metadata
                    .disk_ids
                    .get(&self.config.node_id)
                    .is_some_and(|id| id == journal.disk_id())
            })
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
            if !self.ready() {
                return Err(unavailable());
            }
            let applied = self.rpc().await?.applied(revision).await?;
            if applied < revision || !self.ready() {
                return Err(unavailable());
            }
            Ok(())
        })
        .await
        .map_err(|_| unavailable())?
    }
    async fn command(&self, command: Command, barrier: bool) -> io::Result<()> {
        if !self.ready() {
            return Err(unavailable());
        }
        let answer = self.rpc().await?.mutate(command).await?;
        if barrier {
            self.barrier(answer.revision).await?;
        }
        Ok(())
    }
    async fn install(&self, next: Metadata, forwarding_event: bool) -> io::Result<()> {
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
        let mut discard = Vec::new();
        for (key, previous) in &old.durable {
            if previous.node == self.config.node_id
                && previous.incarnation == self.incarnation
                && next.durable.get(key).is_none_or(|owner| {
                    owner.node != self.config.node_id || owner.incarnation != self.incarnation
                })
            {
                discard.push(key.clone());
            }
            if previous.node == self.config.node_id
                && previous.incarnation == self.incarnation
                && next.durable.get(key).is_none_or(|n| {
                    n.epoch != previous.epoch
                        || n.node != previous.node
                        || n.incarnation != previous.incarnation
                })
                && let Some(client) = key.strip_prefix("c:")
            {
                local.fence(client.to_owned(), previous.generation).await?;
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
        if let Some(data) = &self.data {
            for key in discard {
                data.forget(&key);
            }
        }
        self.reconcile_repair().await?;
        self.initialized.store(true, Ordering::Release);
        self.announce_applied();
        if forwarding_event && let Some(data) = &self.data {
            let metadata = self.metadata.read().unwrap_or_else(PoisonError::into_inner);
            data.reconcile(self, Some(&old), &metadata);
        }
        Ok(())
    }
    async fn watch_loop(self: Arc<Self>) {
        while !self.stopped.load(Ordering::Acquire) {
            let rpc = match self.rpc().await {
                Ok(r) => r.clone(),
                Err(_) => return,
            };
            let mut stream = None;
            for address in rpc.controller_addresses() {
                let Ok(mut client) = rpc.client(&address).await else {
                    continue;
                };
                let Ok(request) = rpc.authorize(wire::Envelope { data: Vec::new() }) else {
                    continue;
                };
                if let Ok(response) = client.watch(request).await {
                    rpc.remember_controller(address.clone());
                    stream = Some(response.into_inner());
                    break;
                }
            }
            if let Some(mut stream) = stream {
                while !self.stopped.load(Ordering::Acquire) {
                    let next = tokio::select! {
                        next = tokio::time::timeout(Duration::from_millis(self.config.lease_ms / 2), stream.message()) => next,
                        _ = self.watch_reconnect.notified() => break,
                    };
                    match next {
                        Ok(Ok(Some(message))) => match rpc::decode::<MetadataUpdate>(message) {
                            Ok(MetadataUpdate::Progress { revision }) => {
                                if self
                                    .metadata
                                    .read()
                                    .unwrap_or_else(PoisonError::into_inner)
                                    .revision
                                    != revision
                                {
                                    break;
                                }
                            }
                            Ok(update) => {
                                if self.install_update(update).await.is_err() {
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
            let applied = *self.installed.borrow();
            let command = Command::Heartbeat {
                node: self.config.node_id,
                incarnation: self.incarnation.clone(),
                applied,
                lease_ms: self.config.lease_ms,
                expires_at: controller::now().saturating_add(self.config.lease_ms),
            };
            if let Ok(rpc) = self.rpc().await {
                match rpc.mutate(command).await {
                    Ok(answer) => {
                        self.repush_applied();
                        let lease = self.config.lease_ms.min(answer.lease_ms);
                        if lease >= 1000 {
                            *self.deadline.lock().unwrap_or_else(PoisonError::into_inner) =
                                start + Duration::from_millis(lease.saturating_sub(100));
                        }
                    }
                    Err(_) => {
                        let _ = self.recover_registration(rpc).await;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    async fn recover_registration(&self, rpc: &ClientRpc) -> io::Result<()> {
        // Only an authoritative quorum read may prove that this incarnation was removed.
        // Never replace another incarnation, and never resurrect the fenced local sessions.
        let node = rpc.node(self.config.node_id).await?;
        if node.is_some() {
            return Ok(());
        }
        self.initialized.store(false, Ordering::Release);
        *self.deadline.lock().unwrap_or_else(PoisonError::into_inner) = tokio::time::Instant::now();
        self.local()?.fence_all().await?;
        rpc.mutate(Command::Register {
            node: self.config.node_id,
            incarnation: self.incarnation.clone(),
            address: self.config.address(),
            disk_id: self.journal.as_ref().map(|j| j.disk_id().to_owned()),
            memory: self.config.durable.is_some(),
            replicas: self.config.durable.as_ref().map(|d| {
                d.replicas
                    .iter()
                    .map(|p| (p.id, p.address.clone()))
                    .collect()
            }),
            expires_at: 0,
            lease_ms: self.config.lease_ms,
        })
        .await?;
        self.watch_reconnect.notify_one();
        // A fresh watch snapshot and subsequent confirmed heartbeat are both required for ready.
        Ok(())
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
            if !was_ready
                && ready
                && let Some(data) = &self.data
            {
                data.ready_again(&self);
            }
            was_ready = ready;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    pub(crate) async fn deliver_recipient(
        &self,
        delivery: durable::RecipientDelivery,
    ) -> io::Result<()> {
        let key = durable::client_key(&delivery.client_id);
        if !self.ready()
            || self.data.is_none()
            || !self
                .metadata
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .durable
                .get(&key)
                .is_some_and(|owner| {
                    owner.node == self.config.node_id && owner.incarnation == self.incarnation
                })
        {
            return Err(unavailable());
        }
        if self
            .data
            .as_ref()
            .ok_or_else(unavailable)?
            .received(&delivery.client_id, delivery.id.clone())
            .await?
        {
            return Ok(());
        }
        self.local()?
            .deliver_to(delivery.client_id, delivery.id, delivery.message)
            .await
    }
    pub(crate) async fn deliver(&self, forward: Forward) -> io::Result<()> {
        if !self.ready() {
            return Err(unavailable());
        }
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
    fn startup_timeout(&self) -> Duration {
        if self.journal.is_some() {
            Duration::from_secs(625)
        } else {
            Duration::from_secs(5)
        }
    }
    fn inspect_session(
        &self,
        client_id: String,
    ) -> ClusterFuture<'_, mqtt_cluster_core::SessionLease> {
        Box::pin(async move {
            self.data
                .as_ref()
                .ok_or_else(unavailable)?
                .inspect(self, &client_id)
                .await
        })
    }
    fn acquire_session(
        &self,
        client_id: String,
        generation: u64,
        expected_epoch: u64,
    ) -> ClusterFuture<'_, mqtt_cluster_core::SessionLease> {
        Box::pin(async move {
            self.data
                .as_ref()
                .ok_or_else(unavailable)?
                .acquire(durable::client_key(&client_id), generation, expected_epoch)
                .await
        })
    }
    fn commit_session(
        &self,
        mutation: mqtt_cluster_core::SessionMutation,
    ) -> ClusterFuture<'_, mqtt_cluster_core::SessionCommit> {
        Box::pin(async move {
            self.data
                .as_ref()
                .ok_or_else(unavailable)?
                .transaction(mutation)
                .await
        })
    }

    fn durable(&self) -> bool {
        self.journal.is_some()
    }
    fn ready(&self) -> bool {
        if !self.control_ready() {
            return false;
        }
        let metadata = self.metadata.read().unwrap_or_else(PoisonError::into_inner);
        metadata.repair.is_none()
            && !metadata.excluded.contains(&self.config.node_id)
            && self
                .journal
                .as_ref()
                .is_none_or(|j| j.voting_ready(metadata.data_generation))
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
            self.register_startup().await?;
            let this = self.weak.upgrade().ok_or_else(unavailable)?;
            let mut tasks = vec![
                tokio::spawn(this.clone().watch_loop()),
                tokio::spawn(this.clone().heartbeat_loop()),
                tokio::spawn(this.clone().applied_ack_loop()),
                tokio::spawn(this.lease_loop()),
            ];
            if let Some(data) = &self.data {
                tasks.push(tokio::spawn(data.clone().run()));
            }
            self.tasks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend(tasks);
            tokio::time::timeout(Duration::from_secs(10), async {
                // Repair maintenance intentionally keeps data voting unavailable.
                // Donors still need to start and serve authenticated repair RPC.
                while !self.control_ready() {
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
            if let Some(data) = &self.data {
                return data.release(&client_id, generation).await;
            }
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
            if let Some(data) = &self.data {
                return data.enqueue(message).await;
            }
            if !self.ready() {
                return Err(unavailable());
            }
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
            if let Some(data) = &self.data {
                data.wake();
            }
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
        .max_concurrent_streams(None)
        .timeout(Duration::from_secs(60));
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

#[cfg(test)]
mod control_tests;
