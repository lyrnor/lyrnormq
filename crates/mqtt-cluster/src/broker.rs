//! Readonly metadata replica, conservative leases and direct broker forwarding.
use crate::{
    config::{Config, Role},
    controller::{self, Service},
    model::*,
    rpc::{self, ClientRpc, wire},
};
use mqtt_cluster_core::{
    ClusterBridge, ClusterFuture, ClusterMessage, ForwardScope, LocalBroker, SharedConfigKind,
    SharedConfigSink,
};
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
pub(crate) mod delivery_batch;
pub(crate) mod durable;
pub(crate) mod forward_batch;
mod progress;
mod quorum;
mod recipients;
pub(crate) mod repair;
mod startup;
mod updates;
#[derive(Default)]
struct Receipts {
    values: BTreeMap<(u64, String, u64), bool>,
    order: VecDeque<(u64, String, u64)>,
}
impl Receipts {
    fn begin(&mut self, key: &(u64, String, u64)) -> io::Result<bool> {
        if let Some(done) = self.values.get(key) {
            return if *done { Ok(false) } else { Err(unavailable()) };
        }
        self.values.insert(key.clone(), false);
        Ok(true)
    }
    fn finish(&mut self, key: (u64, String, u64), success: bool) {
        if success {
            self.values.insert(key.clone(), true);
            self.order.push_back(key);
            // Only completed identities occupy this bounded deduplication cache.
            // Pending work is never rejected or evicted to enforce a count limit.
            while self.order.len() > 8192 {
                if let Some(old) = self.order.pop_front() {
                    self.values.remove(&old);
                }
            }
        } else {
            self.values.remove(&key);
        }
    }
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ReplicaMessage {
    pub generation: u64,
    pub operation: crate::replica::Request,
}
pub(crate) const REPLICA_BATCH_LIMIT: usize = 64;
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ReplicaBatchMessage {
    pub generation: u64,
    pub operations: Vec<crate::replica::Request>,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ReplicaBatchReply {
    pub results: Vec<Option<crate::replica::Reply>>,
}
pub(crate) const DELIVER_BATCH_LIMIT: usize = 64;
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct DeliverBatchMessage {
    pub deliveries: Vec<durable::RecipientDelivery>,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct DeliverBatchReply {
    pub results: Vec<bool>,
}
pub(crate) const FORWARD_BATCH_LIMIT: usize = 128;
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ForwardBatchMessage {
    pub forwards: Vec<Forward>,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ForwardBatchReply {
    pub results: Vec<bool>,
}
pub struct BrokerRuntime {
    config: Arc<Config>,
    incarnation: String,
    rpc: OnceCell<ClientRpc>,
    local: Mutex<Option<Arc<dyn LocalBroker>>>,
    metadata: RwLock<Metadata>,
    // Acquire metadata before this lock for both publication and lookup.
    recipients: RwLock<recipients::RecipientIndex>,
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
    replica_batches: OnceCell<quorum::batch::ReplicaBatcher>,
    delivery_batches: OnceCell<delivery_batch::DeliveryBatcher>,
    forward_batches: OnceCell<forward_batch::ForwardBatcher>,
    config_sink: Mutex<Option<Arc<dyn SharedConfigSink>>>,
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
                match d.backend {
                    crate::config::DataBackend::Memory => crate::replica::Journal::open(
                        &config.data_dir,
                        (&config.cluster_id, config.node_id),
                        &members,
                    ),
                    crate::config::DataBackend::Rocks => crate::replica::Journal::open_with(
                        &config.data_dir,
                        (&config.cluster_id, config.node_id),
                        &members,
                        crate::replica::StoreKind::Rocks,
                    ),
                }
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
            recipients: RwLock::new(recipients::RecipientIndex::default()),
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
            replica_batches: OnceCell::new(),
            delivery_batches: OnceCell::new(),
            forward_batches: OnceCell::new(),
            config_sink: Mutex::new(None),
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
    fn not_ready_reason(&self) -> Option<&'static str> {
        if !self.control_ready() {
            if self.stopped.load(Ordering::Acquire) {
                return Some("stopped");
            }
            if !self.initialized.load(Ordering::Acquire) {
                return Some("metadata not installed");
            }
            if tokio::time::Instant::now()
                >= *self.deadline.lock().unwrap_or_else(PoisonError::into_inner)
            {
                return Some("heartbeat lease expired");
            }
            let metadata = self.metadata.read().unwrap_or_else(PoisonError::into_inner);
            if !startup::storage_matches(&self.config, &metadata) {
                return Some("storage membership mismatch");
            }
            if self.journal.as_ref().is_some_and(|journal| {
                !metadata
                    .disk_ids
                    .get(&self.config.node_id)
                    .is_some_and(|id| id == journal.disk_id())
            }) {
                return Some("disk identity mismatch");
            }
            return Some("node not registered");
        }
        let metadata = self.metadata.read().unwrap_or_else(PoisonError::into_inner);
        if metadata.repair.is_some() {
            return Some("repair in progress");
        }
        if metadata.excluded.contains(&self.config.node_id) {
            return Some("node excluded");
        }
        if self
            .journal
            .as_ref()
            .is_some_and(|journal| !journal.voting_ready(metadata.data_generation))
        {
            return Some("replica not voting");
        }
        None
    }
    async fn rpc(&self) -> io::Result<&ClientRpc> {
        self.rpc
            .get_or_try_init(|| ClientRpc::new(self.config.clone()))
            .await
    }
    async fn replica_batcher(&self) -> io::Result<&quorum::batch::ReplicaBatcher> {
        self.replica_batches
            .get_or_try_init(|| async {
                Ok(quorum::batch::ReplicaBatcher::new(
                    self.rpc().await?.clone(),
                ))
            })
            .await
    }
    pub(in crate::broker) async fn delivery_batcher(
        &self,
    ) -> io::Result<&delivery_batch::DeliveryBatcher> {
        self.delivery_batches
            .get_or_try_init(|| async {
                Ok(delivery_batch::DeliveryBatcher::new(
                    self.rpc().await?.clone(),
                ))
            })
            .await
    }
    async fn forward_batcher(&self) -> io::Result<&forward_batch::ForwardBatcher> {
        self.forward_batches
            .get_or_try_init(|| async {
                Ok(forward_batch::ForwardBatcher::new(
                    self.rpc().await?.clone(),
                ))
            })
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
    async fn publish_shared_configs(&self, old: &Metadata, next: &Metadata) -> io::Result<()> {
        self.publish_shared_config_pair(
            (old.rules_config.clone(), old.access_config.clone()),
            (next.rules_config.clone(), next.access_config.clone()),
        )
        .await
    }
    async fn publish_shared_config_pair(
        &self,
        old: (Option<ClusterConfigDoc>, Option<ClusterConfigDoc>),
        next: (Option<ClusterConfigDoc>, Option<ClusterConfigDoc>),
    ) -> io::Result<()> {
        let sink = self
            .config_sink
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let Some(sink) = sink else {
            return Ok(());
        };
        if old.0 != next.0
            && let Some(doc) = next.0
        {
            sink.apply_rules(doc.generation, doc.document).await?;
        }
        if old.1 != next.1
            && let Some(doc) = next.1
        {
            sink.apply_access(doc.generation, doc.document).await?;
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
        let next_recipients = recipients::RecipientIndex::from_metadata(&next);
        self.publish_shared_configs(&old, &next).await?;
        {
            let mut metadata = self
                .metadata
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            let mut recipients = self
                .recipients
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            *metadata = next;
            *recipients = next_recipients;
        }
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
        let mut failed = false;
        while !self.stopped.load(Ordering::Acquire) {
            let rpc = match self.rpc().await {
                Ok(r) => r.clone(),
                Err(error) => {
                    if !failed {
                        tracing::error!(
                            event = "cluster_watch_failed",
                            node = self.config.node_id,
                            error = %error,
                            "cluster metadata watch failed"
                        );
                        failed = true;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let mut stream = None;
            for address in rpc.controller_addresses() {
                let Ok(mut client) = rpc.client(&address).await else {
                    continue;
                };
                let Ok(request) = rpc.authorize(wire::Envelope::default()) else {
                    continue;
                };
                if let Ok(response) = client.watch(request).await {
                    rpc.remember_controller(address.clone());
                    stream = Some(response.into_inner());
                    break;
                }
            }
            if let Some(mut stream) = stream {
                if failed {
                    tracing::info!(
                        event = "cluster_watch_restored",
                        node = self.config.node_id,
                        "cluster metadata watch restored"
                    );
                    failed = false;
                }
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
                                if let Err(error) = self.install_update(update).await {
                                    if !failed {
                                        tracing::error!(
                                            event = "cluster_metadata_install_failed",
                                            node = self.config.node_id,
                                            error = %error,
                                            "cluster metadata install failed"
                                        );
                                        failed = true;
                                    }
                                    break;
                                }
                            }
                            Err(error) => {
                                if !failed {
                                    tracing::error!(
                                        event = "cluster_watch_decode_failed",
                                        node = self.config.node_id,
                                        error = %error,
                                        "cluster metadata watch decode failed"
                                    );
                                    failed = true;
                                }
                                break;
                            }
                        },
                        Ok(Err(error)) => {
                            if !failed {
                                tracing::error!(
                                    event = "cluster_watch_failed",
                                    node = self.config.node_id,
                                    error = %error,
                                    "cluster metadata watch disconnected"
                                );
                                failed = true;
                            }
                            break;
                        }
                        Ok(Ok(None)) => {
                            if !failed {
                                tracing::error!(
                                    event = "cluster_watch_failed",
                                    node = self.config.node_id,
                                    "cluster metadata watch disconnected"
                                );
                                failed = true;
                            }
                            break;
                        }
                        Err(_) => break,
                    }
                }
            } else if !failed {
                tracing::error!(
                    event = "cluster_watch_failed",
                    node = self.config.node_id,
                    "cluster metadata watch could not reach a controller"
                );
                failed = true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    async fn heartbeat_loop(self: Arc<Self>) {
        let mut failed = false;
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
            match self.rpc().await {
                Ok(rpc) => match rpc.mutate(command).await {
                    Ok(answer) => {
                        if failed {
                            tracing::info!(
                                event = "cluster_heartbeat_restored",
                                node = self.config.node_id,
                                "cluster heartbeat restored"
                            );
                            failed = false;
                        }
                        self.repush_applied();
                        let lease = self.config.lease_ms.min(answer.lease_ms);
                        if lease >= 1000 {
                            *self.deadline.lock().unwrap_or_else(PoisonError::into_inner) =
                                start + Duration::from_millis(lease.saturating_sub(100));
                        }
                    }
                    Err(error) => {
                        let recovery = self.recover_registration(rpc).await.err();
                        if !failed {
                            tracing::error!(
                                event = "cluster_heartbeat_failed",
                                node = self.config.node_id,
                                error = %error,
                                recovery = recovery.as_ref().map(ToString::to_string).unwrap_or_default(),
                                "cluster heartbeat failed"
                            );
                            failed = true;
                        }
                    }
                },
                Err(error) => {
                    if !failed {
                        tracing::error!(
                            event = "cluster_heartbeat_failed",
                            node = self.config.node_id,
                            error = %error,
                            "cluster heartbeat failed"
                        );
                        failed = true;
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
            rocks: self
                .config
                .durable
                .as_ref()
                .is_some_and(|durable| durable.backend == crate::config::DataBackend::Rocks),
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
        let mut reported: Option<Option<&'static str>> = None;
        while !self.stopped.load(Ordering::Acquire) {
            let reason = self.not_ready_reason();
            let ready = reason.is_none();
            if reported != Some(reason) {
                let revision = self
                    .metadata
                    .read()
                    .unwrap_or_else(PoisonError::into_inner)
                    .revision;
                match reason {
                    None => tracing::info!(
                        event = "cluster_ready",
                        node = self.config.node_id,
                        revision,
                        "cluster ready"
                    ),
                    Some(reason) => tracing::warn!(
                        event = "cluster_not_ready",
                        node = self.config.node_id,
                        revision,
                        reason,
                        "cluster not ready"
                    ),
                }
                reported = Some(reason);
            }
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
        // A committed receipt must not occupy MQTT queue capacity on retry.
        // `commit` also treats the same identity as a duplicate, but only after
        // the local actor has already tried to enqueue.
        if self
            .data
            .as_ref()
            .ok_or_else(unavailable)?
            .received(&delivery.client_id, delivery.id.clone())
            .await
            .unwrap_or(false)
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
            // Memory-replica sessions are durable owners, not Claim entries.
            // QoS 0 forwarding still has to prove this node owns the publisher.
            if !publisher_owned(&metadata, publisher, forward.node, &forward.incarnation) {
                return Err(unavailable());
            }
        }
        let key = (forward.node, forward.incarnation, forward.sequence);
        {
            let mut receipts = self.receipts.lock().unwrap_or_else(PoisonError::into_inner);
            if !receipts.begin(&key)? {
                return Ok(());
            }
        }
        let mut guard = ReceiptGuard {
            runtime: self,
            key: key.clone(),
            active: true,
        };
        let result = tokio::time::timeout(
            Duration::from_secs(4),
            self.local()?.deliver(forward.message),
        )
        .await
        .unwrap_or_else(|_| Err(unavailable()));
        let mut receipts = self.receipts.lock().unwrap_or_else(PoisonError::into_inner);
        receipts.finish(key, result.is_ok());
        // Disarm under the same lock: a completed entry may be evicted and retried
        // before this response drops its guard. Never delete a subsequent delivery.
        guard.active = false;
        drop(receipts);
        drop(guard);
        result
    }
    /// One best-effort RPC per remote node. Callers deliver local subscribers themselves.
    async fn forward_remote(&self, message: ClusterMessage) -> io::Result<()> {
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
        let addresses = targets.into_iter().map(|node| node.address).collect();
        self.send_forward(addresses, message, false).await
    }
    /// Indexed durable subscribers. QoS 0 already delivered local matches.
    fn remote_forward_addresses(&self, topic: &str) -> Vec<String> {
        let metadata = self.metadata.read().unwrap_or_else(PoisonError::into_inner);
        let matched = self
            .recipients
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .matching(topic);
        recipients::RecipientIndex::remote_addresses(
            &metadata,
            &matched,
            self.config.node_id,
            &self.incarnation,
        )
    }
    async fn send_forward(
        &self,
        addresses: Vec<String>,
        message: ClusterMessage,
        delivery_channel: bool,
    ) -> io::Result<()> {
        if addresses.is_empty() {
            return Ok(());
        }
        let sequence = self
            .sequence
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| unavailable())?;
        let forward = Forward {
            node: self.config.node_id,
            incarnation: self.incarnation.clone(),
            sequence,
            message,
        };
        let rpc = self.rpc().await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            for address in addresses {
                let mut okay = false;
                for _ in 0..2 {
                    if !self.ready() {
                        return Err(unavailable());
                    }
                    let mut client = if delivery_channel {
                        rpc.delivery_client(&address).await?
                    } else {
                        rpc.client(&address).await?
                    };
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
    }
    async fn enqueue_forwards(
        &self,
        addresses: Vec<String>,
        message: ClusterMessage,
    ) -> io::Result<()> {
        if addresses.is_empty() {
            return Ok(());
        }
        let sequence = self
            .sequence
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| unavailable())?;
        let forward = Forward {
            node: self.config.node_id,
            incarnation: self.incarnation.clone(),
            sequence,
            message,
        };
        let batcher = self.forward_batcher().await?;
        let mut waits = Vec::with_capacity(addresses.len());
        for address in &addresses {
            waits.push(batcher.enqueue(address, forward.clone())?);
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            for wait in waits {
                wait.await.map_err(|_| unavailable())??;
            }
            Ok(())
        })
        .await
        .map_err(|_| unavailable())?
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
        self.not_ready_reason().is_none()
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
            .map_err(|_| {
                tracing::error!(
                    event = "cluster_control_timeout",
                    node = self.config.node_id,
                    reason = self.not_ready_reason().unwrap_or("control not ready"),
                    "cluster control did not become ready within 10s"
                );
                unavailable()
            })?;
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
            self.forward_remote(message).await
        })
    }
    fn commit_retained(&self, message: ClusterMessage) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            self.data
                .as_ref()
                .ok_or_else(unavailable)?
                .commit_retained(message)
                .await
        })
    }
    fn matching_retained(&self, filters: Vec<String>) -> ClusterFuture<'_, Vec<ClusterMessage>> {
        Box::pin(async move {
            self.data
                .as_ref()
                .ok_or_else(unavailable)?
                .matching_retained(filters)
                .await
        })
    }
    fn forward_ephemeral(&self, message: ClusterMessage) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            if !self.ready() {
                return Err(unavailable());
            }
            let addresses = self.remote_forward_addresses(&message.topic);
            self.enqueue_forwards(addresses, message).await
        })
    }
    fn put_shared_config(
        &self,
        kind: SharedConfigKind,
        expected_generation: u64,
        document: String,
    ) -> ClusterFuture<'_, u64> {
        Box::pin(async move {
            if !self.ready() {
                return Err(unavailable());
            }
            let answer = self
                .rpc()
                .await?
                .mutate(Command::PutClusterConfig {
                    kind: ClusterConfigKind::from(kind),
                    expected_generation,
                    document: document.clone(),
                })
                .await?;
            if let Some(error) = answer.error {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, error));
            }
            self.barrier(answer.revision).await?;
            let generation = expected_generation.checked_add(1).ok_or_else(unavailable)?;
            let sink = self
                .config_sink
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            if let Some(sink) = sink {
                match kind {
                    SharedConfigKind::Rules => sink.apply_rules(generation, document).await?,
                    SharedConfigKind::Access => sink.apply_access(generation, document).await?,
                }
            }
            Ok(generation)
        })
    }
    fn shared_config_generation(&self, kind: SharedConfigKind) -> Option<u64> {
        let metadata = self.metadata.read().unwrap_or_else(PoisonError::into_inner);
        match kind {
            SharedConfigKind::Rules => metadata.rules_config.as_ref().map(|doc| doc.generation),
            SharedConfigKind::Access => metadata.access_config.as_ref().map(|doc| doc.generation),
        }
    }
    fn set_shared_config_sink(&self, sink: Arc<dyn SharedConfigSink>) {
        *self
            .config_sink
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(sink);
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
/// A forwarded publish is accepted from the node that owns the publisher,
/// whether that ownership is a connected Claim or a durable session record.
fn publisher_owned(metadata: &Metadata, publisher: &str, node: u64, incarnation: &str) -> bool {
    metadata
        .clients
        .get(publisher)
        .is_some_and(|owner| owner.node == node && owner.incarnation == incarnation)
        || metadata
            .durable
            .get(&durable::client_key(publisher))
            .is_some_and(|owner| owner.node == node && owner.incarnation == incarnation)
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
    active: bool,
}
impl Drop for ReceiptGuard<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
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

#[cfg(test)]
mod publisher_owned_tests {
    use super::{durable, publisher_owned};
    use crate::model::{Client, DurableOwner, Metadata};

    fn durable_owner(node: u64) -> DurableOwner {
        DurableOwner {
            node,
            incarnation: "inc".into(),
            generation: 1,
            epoch: 3,
            filters: Vec::new(),
            logical_generation: 0,
            acquired_from: None,
        }
    }

    #[test]
    fn durable_session_owner_can_forward_without_a_claim() {
        let mut metadata = Metadata::default();
        metadata
            .durable
            .insert(durable::client_key("pub"), durable_owner(7));
        assert!(publisher_owned(&metadata, "pub", 7, "inc"));
        assert!(!publisher_owned(&metadata, "pub", 8, "inc"));
        assert!(!publisher_owned(&metadata, "pub", 7, "other"));
        assert!(!publisher_owned(&metadata, "other", 7, "inc"));
    }

    #[test]
    fn claimed_client_can_forward_from_its_node() {
        let mut metadata = Metadata::default();
        metadata.clients.insert(
            "pub".into(),
            Client {
                node: 7,
                incarnation: "inc".into(),
                generation: 1,
                filters: Vec::new(),
            },
        );
        assert!(publisher_owned(&metadata, "pub", 7, "inc"));
        assert!(!publisher_owned(&metadata, "pub", 7, "other"));
    }
}
