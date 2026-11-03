//! Three-voter metadata Raft, durable state and snapshot replication service.
use std::sync::PoisonError;

use crate::{
    config::{Config, Role},
    model::*,
    rpc::{self, ClientRpc, ReadRequest, wire},
    storage::Store,
};
use mqtt_cluster_core::ClusterBridge;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::RPCOption;
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};
use serde::de::DeserializeOwned;
use std::{
    collections::BTreeMap,
    io,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Semaphore, watch};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};
pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis().min(u128::from(u64::MAX)) as u64)
}
struct Network {
    rpc: ClientRpc,
}
struct Peer {
    rpc: ClientRpc,
    address: String,
}
impl RaftNetworkFactory<Types> for Network {
    type Network = Peer;
    async fn new_client(&mut self, _target: u64, node: &BasicNode) -> Peer {
        Peer {
            rpc: self.rpc.clone(),
            address: node.addr.clone(),
        }
    }
}
impl Peer {
    #[allow(clippy::result_large_err)] // Required by the OpenRaft network trait.
    async fn send<T: DeserializeOwned, E: std::error::Error>(
        &self,
        request: RaftRequest,
    ) -> Result<T, RPCError<u64, BasicNode, RaftError<u64, E>>> {
        let result = async {
            let mut client = self.rpc.client(&self.address).await?;
            let reply = client
                .raft(self.rpc.authorize(rpc::encode(&request)?)?)
                .await
                .map_err(rpc::io_error)?;
            let envelope = reply.into_inner();
            serde_json::from_slice(&envelope.data).map_err(io::Error::other)
        }
        .await;
        result.map_err(|error: io::Error| RPCError::Network(NetworkError::new(&error)))
    }
}
impl RaftNetwork<Types> for Peer {
    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<Types>,
        _option: RPCOption,
    ) -> Result<openraft::raft::AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>>
    {
        self.send(RaftRequest::Append(rpc)).await
    }
    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<openraft::raft::VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.send(RaftRequest::Vote(rpc)).await
    }
    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<Types>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        self.send(RaftRequest::Snapshot(rpc)).await
    }
}
pub(crate) struct Control {
    raft: Raft,
    store: Store,
    rpc: ClientRpc,
    slots: Arc<Semaphore>,
    watchers: Arc<Semaphore>,
    lease_ms: u64,
}
impl Control {
    async fn leader(&self) -> Result<(), Status> {
        tokio::time::timeout(Duration::from_secs(1), self.raft.ensure_linearizable())
            .await
            .map_err(|_| Status::unavailable("quorum deadline"))?
            .map_err(|_| Status::unavailable("not current leader"))?;
        Ok(())
    }
    async fn write(&self, mut command: Command) -> Result<Answer, Status> {
        let _slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("metadata write capacity"))?;
        validate(&command)?;
        if serde_json::to_vec(&command)
            .map_err(|_| Status::invalid_argument("invalid metadata command"))?
            .len()
            > 512 * 1024
        {
            return Err(Status::resource_exhausted("metadata command byte limit"));
        }
        match &mut command {
            Command::Register {
                node,
                expires_at,
                lease_ms,
                ..
            }
            | Command::Heartbeat {
                node,
                expires_at,
                lease_ms,
                ..
            } => {
                *lease_ms = self.lease_ms;
                let previous = self
                    .store
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .metadata
                    .nodes
                    .get(node)
                    .map_or(0, |n| n.expires_at);
                *expires_at = now()
                    .saturating_add(self.lease_ms)
                    .max(previous.saturating_add(1));
            }
            _ => {}
        }
        let result = tokio::time::timeout(Duration::from_secs(2), self.raft.client_write(command))
            .await
            .map_err(|_| Status::unavailable("metadata commit deadline"))?
            .map_err(|_| Status::unavailable("not current leader"))?;
        let mut answer = result.data;
        answer.lease_ms = self.lease_ms;
        Ok(answer)
    }
    pub(crate) async fn raft_request(
        &self,
        request: RaftRequest,
    ) -> Result<wire::Envelope, Status> {
        match request {
            RaftRequest::Append(r) => rpc::encode(
                &self
                    .raft
                    .append_entries(r)
                    .await
                    .map_err(|_| Status::unavailable("Raft append failed"))?,
            )
            .map_err(|_| Status::internal("Raft encoding failed")),
            RaftRequest::Vote(r) => rpc::encode(
                &self
                    .raft
                    .vote(r)
                    .await
                    .map_err(|_| Status::unavailable("Raft vote failed"))?,
            )
            .map_err(|_| Status::internal("Raft encoding failed")),
            RaftRequest::Snapshot(r) => {
                if r.data.len() > 1024 * 1024
                    || r.offset
                        .checked_add(r.data.len() as u64)
                        .is_none_or(|end| end > 16 * 1024 * 1024)
                {
                    return Err(Status::resource_exhausted("snapshot chunk limit"));
                }
                rpc::encode(
                    &self
                        .raft
                        .install_snapshot(r)
                        .await
                        .map_err(|_| Status::unavailable("Raft snapshot failed"))?,
                )
                .map_err(|_| Status::internal("Raft encoding failed"))
            }
        }
    }
}
fn validate(command: &Command) -> Result<(), Status> {
    let text = |s: &str, max: usize| !s.is_empty() && s.len() <= max && !s.contains('\0');
    let okay = match command {
        Command::RepairBegin {
            target,
            old_disk,
            new_disk,
            ..
        } => *target > 0 && text(old_disk, 32) && text(new_disk, 32),
        Command::RepairSeal {
            id,
            generation,
            manifest,
        } => text(id, 128) && *generation > 0 && manifest.valid(),
        Command::RepairCommit { id, generation, .. } | Command::RepairAbort { id, generation } => {
            text(id, 128) && *generation > 0
        }
        Command::Acquire {
            key,
            node,
            incarnation,
            ..
        } => {
            *node > 0
                && text(key, 65538)
                && (key.starts_with("c:") || key.starts_with("h:"))
                && text(incarnation, 64)
        }
        Command::DurableFilters {
            key,
            node,
            incarnation,
            epoch,
            filters,
            ..
        } => {
            *node > 0
                && *epoch > 0
                && text(key, 65538)
                && text(incarnation, 64)
                && filters.len() <= 256
                && filters
                    .iter()
                    .all(|f| mqtt_router::validate_topic_filter(f) && !f.starts_with("$share/"))
        }
        Command::Register {
            node,
            incarnation,
            address,
            ..
        } => {
            *node > 0
                && text(incarnation, 64)
                && text(address, 2048)
                && (address.starts_with("http://127.0.0.1:")
                    || address.starts_with("http://[::1]:")
                    || address.starts_with("https://"))
        }
        Command::Heartbeat {
            node, incarnation, ..
        }
        | Command::Expire {
            node, incarnation, ..
        } => *node > 0 && text(incarnation, 64),
        Command::Claim {
            node,
            incarnation,
            client,
            generation,
        }
        | Command::Release {
            node,
            incarnation,
            client,
            generation,
        } => *node > 0 && *generation > 0 && text(incarnation, 64) && text(client, 65535),
        Command::Filters {
            generation,
            node,
            incarnation,
            client,
            filters,
        } => {
            *node > 0
                && text(incarnation, 64)
                && text(client, 65535)
                && *generation > 0
                && filters.len() <= 256
                && filters
                    .iter()
                    .all(|f| mqtt_router::validate_topic_filter(f) && !f.starts_with("$share/"))
        }
    };
    if okay {
        Ok(())
    } else {
        Err(Status::invalid_argument("invalid metadata command"))
    }
}
pub(crate) type WatchStream = Pin<Box<dyn Stream<Item = Result<wire::Envelope, Status>> + Send>>;
#[derive(Clone)]
pub(crate) struct Service {
    pub(crate) control: Option<Arc<Control>>,
    pub(crate) broker: Option<Arc<crate::broker::BrokerRuntime>>,
    pub(crate) rpc: ClientRpc,
}
#[tonic::async_trait]
impl wire::cluster_server::Cluster for Service {
    async fn raft(
        &self,
        request: Request<wire::Envelope>,
    ) -> Result<Response<wire::Envelope>, Status> {
        self.rpc.check(&request)?;
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("not a controller"))?;
        Ok(Response::new(
            control
                .raft_request(rpc::decode(request.into_inner())?)
                .await?,
        ))
    }
    async fn mutate(
        &self,
        request: Request<wire::Envelope>,
    ) -> Result<Response<wire::Envelope>, Status> {
        self.rpc.check(&request)?;
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("not a controller"))?;
        let answer = control.write(rpc::decode(request.into_inner())?).await?;
        Ok(Response::new(
            rpc::encode(&answer).map_err(|_| Status::internal("metadata encoding"))?,
        ))
    }
    async fn read(
        &self,
        request: Request<wire::Envelope>,
    ) -> Result<Response<wire::Envelope>, Status> {
        self.rpc.check(&request)?;
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("not a controller"))?;
        let request: ReadRequest = rpc::decode(request.into_inner())?;
        let envelope = match request {
            ReadRequest::Metadata => {
                control.leader().await?;
                rpc::encode(&control.store.metadata())
            }
            ReadRequest::Status => {
                let m = control.raft.metrics().borrow().clone();
                let s = control.store.metadata();
                rpc::encode(&ClusterStatus {
                    leader: m.current_leader,
                    term: m.current_term,
                    last_applied: m.last_applied.map_or(0, |id| id.index),
                    nodes: s.nodes.len(),
                    clients: s.clients.len()
                        + s.durable.keys().filter(|key| key.starts_with("c:")).count(),
                    routes: s.routes.len(),
                    data_generation: s.data_generation,
                    excluded: s.excluded.iter().copied().collect(),
                    repair: s.repair.as_ref().map(|r| r.summary("active")),
                    last_repair: s.last_repair.as_ref().map(|r| {
                        r.repair.summary(match r.outcome {
                            crate::repair_state::RepairOutcome::Committed => "committed",
                            crate::repair_state::RepairOutcome::Aborted => "aborted",
                        })
                    }),
                })
            }
        }
        .map_err(|_| Status::internal("metadata encoding"))?;
        Ok(Response::new(envelope))
    }
    type WatchStream = WatchStream;
    async fn watch(
        &self,
        request: Request<wire::Envelope>,
    ) -> Result<Response<WatchStream>, Status> {
        self.rpc.check(&request)?;
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("not a controller"))?;
        control.leader().await?;
        let permit = control
            .watchers
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("watch capacity"))?;
        let control = control.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let mut changes = control.store.changed.subscribe();
        let mut leadership = control.raft.metrics();
        tokio::spawn(async move {
            let _permit = permit;
            let mut previous: Option<Metadata> = None;
            loop {
                let metadata = control.store.metadata();
                let update = match &previous {
                    Some(old) => MetadataUpdate::between(old, &metadata),
                    None => MetadataUpdate::Snapshot(metadata.clone()),
                };
                previous = Some(metadata);
                let envelope = rpc::encode(&update)
                    .map_err(|_| Status::resource_exhausted("metadata snapshot capacity"));
                if tx.send(envelope).await.is_err() {
                    break;
                }
                loop {
                    tokio::select! { result = changes.changed() => { if result.is_err() { return; } }, result = leadership.changed() => { if result.is_err() { return; } }, _ = tokio::time::sleep(Duration::from_millis(control.lease_ms / 4)) => {
                        if let Some(old) = &previous {
                            let progress = rpc::encode(&MetadataUpdate::Progress { revision: old.revision }).map_err(|_| Status::internal("metadata progress encoding"));
                            if tx.send(progress).await.is_err() { return; }
                        }
                    } }
                    if leadership.borrow().current_leader != Some(control.rpc.config_node()) {
                        let _ = tx
                            .send(Err(Status::unavailable("controller leader changed")))
                            .await;
                        return;
                    }
                    if previous
                        .as_ref()
                        .is_none_or(|old| old.revision != *changes.borrow())
                    {
                        break;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
    async fn replica(
        &self,
        request: Request<wire::Envelope>,
    ) -> Result<Response<wire::Envelope>, Status> {
        self.rpc.check(&request)?;
        let broker = self
            .broker
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("not a data replica"))?;
        if !broker.ready() {
            return Err(Status::unavailable("unregistered replica disk"));
        }
        let journal = broker
            .journal
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("not a data replica"))?;
        let reply = journal
            .execute(rpc::decode(request.into_inner())?)
            .await
            .map_err(|_| Status::unavailable("replica write unavailable"))?;
        Ok(Response::new(rpc::encode(&reply).map_err(|_| {
            Status::resource_exhausted("replica response capacity")
        })?))
    }
    async fn deliver_to(
        &self,
        request: Request<wire::Envelope>,
    ) -> Result<Response<wire::Envelope>, Status> {
        self.rpc.check(&request)?;
        let broker = self
            .broker
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("not a Broker"))?;
        broker
            .deliver_recipient(rpc::decode(request.into_inner())?)
            .await
            .map_err(|_| Status::unavailable("recipient unavailable"))?;
        Ok(Response::new(wire::Envelope { data: Vec::new() }))
    }
    async fn deliver(
        &self,
        request: Request<wire::Envelope>,
    ) -> Result<Response<wire::Envelope>, Status> {
        self.rpc.check(&request)?;
        let broker = self
            .broker
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("not a broker"))?;
        broker
            .deliver(rpc::decode(request.into_inner())?)
            .await
            .map_err(|_| Status::unavailable("broker delivery rejected"))?;
        Ok(Response::new(wire::Envelope { data: Vec::new() }))
    }
}
pub async fn run_controller(config: Config, mut shutdown: watch::Receiver<bool>) -> io::Result<()> {
    config.validate()?;
    if config.role != Role::Controller {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "controller role required",
        ));
    }
    let config = Arc::new(config);
    let store = Store::open(&config.data_dir, &config.cluster_id, config.node_id).await?;
    let rpc = ClientRpc::new(config.clone()).await?;
    let settings = Arc::new(
        openraft::Config {
            heartbeat_interval: 150,
            max_payload_entries: 8,
            election_timeout_min: 500,
            election_timeout_max: 900,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(1000),
            snapshot_max_chunk_size: 1024 * 1024,
            max_in_snapshot_log_to_keep: 1000,
            ..Default::default()
        }
        .validate()
        .map_err(io::Error::other)?,
    );
    let raft = Raft::new(
        config.node_id,
        settings,
        Network { rpc: rpc.clone() },
        store.clone(),
        store.clone(),
    )
    .await
    .map_err(io::Error::other)?;
    let control = Arc::new(Control {
        raft,
        store,
        rpc: rpc.clone(),
        slots: Arc::new(Semaphore::new(32)),
        watchers: Arc::new(Semaphore::new(128)),
        lease_ms: config.lease_ms,
    });
    if config.bootstrap
        && !control
            .raft
            .is_initialized()
            .await
            .map_err(io::Error::other)?
    {
        let members: BTreeMap<_, _> = config
            .controllers
            .iter()
            .map(|c| (c.id, BasicNode::new(c.address.clone())))
            .collect();
        control
            .raft
            .initialize(members)
            .await
            .map_err(io::Error::other)?;
    }
    let expiry_control = control.clone();
    let expiry_config = config.clone();
    let mut expiry_shutdown = shutdown.clone();
    let expiry = tokio::spawn(async move {
        let mut leader_term = None;
        let mut observed = BTreeMap::new();
        loop {
            tokio::select! {
                _ = expiry_shutdown.changed() => break,
                _ = tokio::time::sleep(Duration::from_millis(200)) => {
                    let metrics = expiry_control.raft.metrics().borrow().clone();
                    if metrics.current_leader != Some(expiry_config.node_id) { leader_term = None; observed.clear(); continue; }
                    if leader_term != Some(metrics.current_term) { leader_term = Some(metrics.current_term); observed.clear(); }
                    let metadata = expiry_control.store.metadata();
                    observed.retain(|id, _| metadata.nodes.contains_key(id));
                    for n in metadata.nodes.into_values() {
                        let instant = tokio::time::Instant::now();
                        let seen = observed.entry(n.id).or_insert_with(|| crate::lease::LeaseObservation::new(&n, instant));
                        if seen.expired(&n, instant) {
                            let _ = expiry_control.write(Command::Expire { node:n.id, incarnation:n.incarnation, expires_at:n.expires_at }).await;
                        }
                    }
                }
            }
        }
    });
    let service = wire::cluster_server::ClusterServer::new(Service {
        control: Some(control.clone()),
        broker: None,
        rpc,
    })
    .max_decoding_message_size(rpc::MAX_WIRE)
    .max_encoding_message_size(rpc::MAX_WIRE);
    let mut server = tonic::transport::Server::builder()
        .concurrency_limit_per_connection(32)
        .timeout(Duration::from_secs(5));
    if let Some(tls) = rpc::server_tls(&config).await? {
        server = server.tls_config(tls).map_err(io::Error::other)?;
    }
    let result = server
        .add_service(service)
        .serve_with_shutdown(config.listen, async move {
            let _ = shutdown.wait_for(|stopped| *stopped).await;
        })
        .await
        .map_err(io::Error::other);
    expiry.abort();
    let _ = expiry.await;
    control.raft.shutdown().await.map_err(io::Error::other)?;
    result
}
pub async fn status(config: &Config) -> io::Result<ClusterStatus> {
    config.validate()?;
    ClientRpc::new(Arc::new(config.clone()))
        .await?
        .status()
        .await
}
