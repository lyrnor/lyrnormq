//! Three-voter metadata Raft, durable state and snapshot replication service.
use crate::profile::{Metric, Timer};
use std::sync::PoisonError;
mod admission;
mod applied;
mod batching;
pub(crate) mod diagnostics;
mod network_profile;
mod readindex;
use diagnostics::{Kind, Stage, measure};
const MAX_REPLICATION_ENTRIES: u64 = 24;

use crate::{
    config::{Config, Role},
    model::*,
    rpc::{self, ClientRpc, ReadRequest, wire},
    storage::Store,
};
use mqtt_cluster_core::ClusterBridge;
use openraft::BasicNode;
use openraft::async_runtime::WatchReceiver;
use openraft::errors::{NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetworkFactory};
use openraft_legacy::network_v1::{
    ChunkedSnapshotReceiver, InstallSnapshotError, InstallSnapshotRequest, InstallSnapshotResponse,
    RaftNetwork,
};
use serde::de::DeserializeOwned;
use std::{
    collections::BTreeMap,
    io,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::watch;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};
pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis().min(u128::from(u64::MAX)) as u64)
}
#[cfg(test)]
fn applied_ready(metadata: &Metadata, revision: u64) -> bool {
    metadata.revision >= revision
        && !metadata.nodes.is_empty()
        && metadata.nodes.values().all(|node| node.applied >= revision)
}
struct Network {
    rpc: ClientRpc,
}
#[derive(Clone, Copy)]
enum Lane {
    Replication,
    Heartbeat,
}
struct Peer {
    target: u64,
    rpc: ClientRpc,
    address: String,
    lane: Lane,
}
impl Network {
    fn peer(&self, target: u64, node: &BasicNode, lane: Lane) -> Peer {
        Peer {
            target,
            rpc: self.rpc.clone(),
            address: node.addr.clone(),
            lane,
        }
    }
}
impl RaftNetworkFactory<Types> for Network {
    type Network = openraft_legacy::network_v1::Adapter<Types, Peer, std::io::Cursor<Vec<u8>>>;
    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        self.peer(target, node, Lane::Replication).into_v2()
    }
    async fn new_heartbeat_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        // Lease heartbeats must not wait behind a log-replication RPC on the same HTTP/2 connection.
        self.peer(target, node, Lane::Heartbeat).into_v2()
    }
}
impl Peer {
    #[allow(clippy::result_large_err)] // Required by the OpenRaft network trait.
    async fn send<T: DeserializeOwned, E: std::error::Error>(
        &self,
        request: RaftRequest,
    ) -> Result<T, RPCError<Types, RaftError<Types, E>>> {
        let is_append = matches!(request, RaftRequest::Append(_));
        let mut trace = network_profile::Trace::new(self.rpc.config_node(), self.target, &request);
        let result = async {
            let mut client = match self.lane {
                Lane::Heartbeat => self.rpc.heartbeat_client(&self.address).await?,
                Lane::Replication => self.rpc.client(&self.address).await?,
            };
            let request = {
                let _timer = is_append.then(|| Timer::start(Metric::AppendRpcEncode));
                self.rpc.authorize(rpc::encode(&request)?)?
            };
            let reply = {
                let _timer = is_append.then(|| Timer::start(Metric::AppendRpcRoundtrip));
                client.raft(request).await.map_err(rpc::io_error)?
            };
            let _timer = is_append.then(|| Timer::start(Metric::AppendRpcDecode));
            let envelope = reply.into_inner();
            serde_json::from_slice(&envelope.data).map_err(io::Error::other)
        }
        .await;
        if let Some(trace) = &mut trace {
            trace.finish(result.is_ok());
        }
        result.map_err(|error: io::Error| RPCError::Network(NetworkError::new(&error)))
    }
}
impl RaftNetwork<Types> for Peer {
    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<Types>,
        _option: RPCOption,
    ) -> Result<openraft::raft::AppendEntriesResponse<Types>, RPCError<Types, RaftError<Types>>>
    {
        self.send(RaftRequest::Append(rpc)).await
    }
    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<Types>,
        _option: RPCOption,
    ) -> Result<openraft::raft::VoteResponse<Types>, RPCError<Types, RaftError<Types>>> {
        self.send(RaftRequest::Vote(rpc)).await
    }
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<Types>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<Types>,
        RPCError<Types, RaftError<Types, InstallSnapshotError>>,
    > {
        self.send(RaftRequest::Snapshot(rpc)).await
    }
}
pub(crate) struct Control {
    raft: Raft,
    store: Store,
    rpc: ClientRpc,
    progress: applied::Progress,
    lease_ms: u64,
    writes: batching::Writer,
    reads: readindex::Reader,
}
impl Control {
    async fn leader(&self) -> Result<(), Status> {
        let _timer = Timer::start(Metric::ReadIndex);
        measure(Stage::ReadIndex, Kind::Authority, self.reads.check()).await
    }
    async fn write(&self, mut command: Command) -> Result<Answer, Status> {
        let _timer = Timer::start(Metric::ControlWrite);
        measure(Stage::Write, Kind::command(&command), async {
            {
                let _timer = Timer::start(Metric::Validate);
                validate(&command)?;
            }
            if let Command::RepairCommit {
                id,
                generation,
                proofs,
            } = &mut command
            {
                self.leader().await?;
                *proofs = crate::broker::repair::verify_commit(
                    &self.rpc,
                    &self.store.metadata(),
                    id,
                    *generation,
                )
                .await
                .map_err(|_| Status::unavailable("repair proof verification failed"))?;
            }
            let bytes = {
                let _timer = Timer::start(Metric::Encode);
                serde_json::to_vec(&command)
                    .map_err(|_| Status::invalid_argument("invalid metadata command"))?
                    .len()
            };
            if bytes > 512 * 1024 {
                return Err(Status::resource_exhausted("metadata command byte limit"));
            }
            self.writes.write(command).await
        })
        .await
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
    if crate::model::valid_command(command) {
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
        let decoded = rpc::decode(request.into_inner())?;
        let mut incoming = network_profile::Incoming::begin(self.rpc.config_node(), &decoded);
        let response = control.raft_request(decoded).await;
        if let Some(trace) = incoming.as_mut() {
            trace.finish(response.is_ok());
        }
        Ok(Response::new(response?))
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
        measure(Stage::Read, Kind::read(&request), async {
            let envelope = match request {
                ReadRequest::Metadata => {
                    control.leader().await?;
                    rpc::encode(&control.store.metadata())
                }
                ReadRequest::Applied { revision } => rpc::encode(&control.applied(revision).await?),
                ReadRequest::AppliedAck {
                    node,
                    incarnation,
                    revision,
                } => rpc::encode(&control.acknowledge(node, &incarnation, revision)?),
                ReadRequest::Owner { key } => {
                    if key.len() <= 2
                        || key.len() > 65538
                        || key.contains('\0')
                        || !(key.starts_with("c:") || key.starts_with("h:"))
                    {
                        return Err(Status::invalid_argument("invalid owner key"));
                    }
                    control.leader().await?;
                    let owner = control
                        .store
                        .state
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .metadata
                        .durable
                        .get(&key)
                        .cloned();
                    rpc::encode(&owner)
                }
                ReadRequest::Node { node } => {
                    control.leader().await?;
                    let value = control
                        .store
                        .state
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .metadata
                        .nodes
                        .get(&node)
                        .cloned();
                    rpc::encode(&value)
                }
                ReadRequest::Status => {
                    let m = control.raft.metrics().borrow_watched().clone();
                    let state = control
                        .store
                        .state
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    let s = &state.metadata;
                    rpc::encode(&ClusterStatus {
                        data_storage: s
                            .data_mode
                            .as_ref()
                            .map_or("none", DataMode::storage_name)
                            .to_owned(),
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
        })
        .await
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
        let control = control.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let mut changes = control.store.changed.subscribe();
        let mut leadership = control.raft.metrics();
        tokio::spawn(async move {
            let mut base = None;
            let mut progress = tokio::time::interval(Duration::from_millis(control.lease_ms / 4));
            progress.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                if leadership.borrow_watched().current_leader != Some(control.rpc.config_node()) {
                    let _ = tx
                        .send(Err(Status::unavailable("controller leader changed")))
                        .await;
                    return;
                }
                for update in control.store.watch_updates(base) {
                    let revision = match &update {
                        MetadataUpdate::Progress { revision }
                        | MetadataUpdate::Delta { revision, .. } => *revision,
                        MetadataUpdate::Snapshot(metadata) => metadata.revision,
                    };
                    let envelope = rpc::encode(&update)
                        .map_err(|_| Status::resource_exhausted("metadata update capacity"));
                    if tx.send(envelope).await.is_err() {
                        return;
                    }
                    base = Some(revision);
                }
                if base
                    != Some(
                        control
                            .store
                            .state
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .metadata
                            .revision,
                    )
                {
                    continue;
                }
                loop {
                    tokio::select! {
                        result = changes.changed() => { if result.is_err() { return; } },
                        result = leadership.changed() => { if result.is_err() { return; } },
                        _ = progress.tick() => {
                            if let Some(revision) = base {
                                let envelope = rpc::encode(&MetadataUpdate::Progress { revision }).map_err(|_| Status::internal("metadata progress encoding"));
                                if tx.send(envelope).await.is_err() { return; }
                            }
                        }
                    }
                    if leadership.borrow_watched().current_leader != Some(control.rpc.config_node())
                    {
                        let _ = tx
                            .send(Err(Status::unavailable("controller leader changed")))
                            .await;
                        return;
                    }
                    if base
                        != Some(
                            control
                                .store
                                .state
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner)
                                .metadata
                                .revision,
                        )
                    {
                        break;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
    async fn repair(
        &self,
        request: Request<wire::Envelope>,
    ) -> Result<Response<wire::Envelope>, Status> {
        self.rpc.check(&request)?;
        let broker = self
            .broker
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("not a repair data endpoint"))?;
        let reply = broker
            .repair_request(rpc::decode(request.into_inner())?)
            .await
            .map_err(|_| Status::unavailable("repair operation unavailable"))?;
        Ok(Response::new(rpc::encode(&reply).map_err(|_| {
            Status::resource_exhausted("repair response capacity")
        })?))
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
        let message: crate::broker::ReplicaMessage = rpc::decode(request.into_inner())?;
        let reply = journal
            .execute_at(message.generation, message.operation)
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
        Ok(Response::new(wire::Envelope::default()))
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
        Ok(Response::new(wire::Envelope::default()))
    }
}
pub async fn run_controller(config: Config, mut shutdown: watch::Receiver<bool>) -> io::Result<()> {
    config.validate()?;
    let _profile = crate::profile::Reporter::start(config.node_id);
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
            max_payload_entries: MAX_REPLICATION_ENTRIES,
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
    let writes = batching::start(raft.clone(), store.clone(), config.node_id, config.lease_ms);
    let control = Arc::new(Control {
        reads: readindex::start(raft.clone()),
        raft,
        store,
        writes,
        rpc: rpc.clone(),
        progress: applied::Progress::default(),
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
                    let metrics = expiry_control.raft.metrics().borrow_watched().clone();
                    if metrics.current_leader != Some(expiry_config.node_id) { leader_term = None; observed.clear(); continue; }
                    if leader_term != Some(metrics.current_term) { leader_term = Some(metrics.current_term); observed.clear(); }
                    let expired = {
                        let state = expiry_control.store.state.lock().unwrap_or_else(PoisonError::into_inner);
                        observed.retain(|id, _| state.metadata.nodes.contains_key(id));
                        state.metadata.nodes.values().filter_map(|node| {
                            let instant = tokio::time::Instant::now();
                            let seen = observed.entry(node.id).or_insert_with(|| crate::lease::LeaseObservation::new(node, instant));
                            seen.expired(node, instant).then(|| (node.id, node.incarnation.clone(), node.expires_at))
                        }).collect::<Vec<_>>()
                    };
                    for (node, incarnation, expires_at) in expired {
                        let _ = expiry_control.write(Command::Expire { node, incarnation, expires_at }).await;
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
        .max_concurrent_streams(None)
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

#[cfg(test)]
mod control_tests {
    use super::*;
    #[test]
    fn barrier_needs_committed_revision_and_every_registered_node() {
        let mut metadata = Metadata {
            revision: 7,
            ..Metadata::default()
        };
        assert!(!applied_ready(&metadata, 7));
        for (id, applied) in [(101, 7), (102, 7), (103, 6)] {
            metadata.nodes.insert(
                id,
                Node {
                    id,
                    incarnation: format!("n{id}"),
                    address: "http://127.0.0.1:1".into(),
                    expires_at: 0,
                    max_lease_ms: 3000,
                    applied,
                },
            );
        }
        assert!(
            !applied_ready(&metadata, 7),
            "two applied nodes are not an all-node visibility barrier"
        );
        metadata.nodes.get_mut(&103).unwrap().applied = 7;
        assert!(applied_ready(&metadata, 7));
        assert!(!applied_ready(&metadata, 8));
    }
    #[tokio::test]
    async fn applied_waiter_wakes_on_same_revision_heartbeat_and_rejects_new_term() -> io::Result<()>
    {
        let path = std::env::temp_dir().join(format!(
            "rmqtt-control-progress-{}-{}",
            std::process::id(),
            now()
        ));
        tokio::fs::create_dir_all(&path).await?;
        tokio::fs::write(path.join("token"), "b".repeat(32)).await?;
        let config: Config = serde_json::from_value(serde_json::json!({"role":"controller","cluster_id":"progress-test","node_id":1,"listen":"127.0.0.1:64000","data_dir":path,"token_file":path.join("token"),"controllers":[{"id":1,"address":"http://127.0.0.1:64000"},{"id":2,"address":"http://127.0.0.1:64001"},{"id":3,"address":"http://127.0.0.1:64002"}]})).map_err(io::Error::other)?;
        let store = Store::open(&path, "progress-test", 1).await?;
        let rpc = ClientRpc::new(Arc::new(config)).await?;
        let raft = Raft::new(
            1,
            Arc::new(
                openraft::Config::default()
                    .validate()
                    .map_err(io::Error::other)?,
            ),
            Network { rpc: rpc.clone() },
            store.clone(),
            store.clone(),
        )
        .await
        .map_err(io::Error::other)?;
        raft.initialize(BTreeMap::from([(1, BasicNode::new("127.0.0.1:64000"))]))
            .await
            .map_err(io::Error::other)?;
        tokio::time::timeout(Duration::from_secs(3), async {
            while raft.metrics().borrow_watched().current_leader != Some(1) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(io::Error::other)?;
        // Drain initialization entries before injecting test-only progress data.
        // Otherwise an in-flight State::apply clone can overwrite the fixture.
        raft.client_write(Command::Heartbeat {
            node: u64::MAX,
            incarnation: "fixture".into(),
            applied: 0,
            expires_at: 0,
            lease_ms: 3000,
        })
        .await
        .map_err(io::Error::other)?;
        let writes = batching::start(raft.clone(), store.clone(), 1, 3000);
        let control = Arc::new(Control {
            reads: readindex::start(raft.clone()),
            raft,
            store,
            writes,
            rpc,
            progress: applied::Progress::default(),
            lease_ms: 3000,
        });
        control
            .write(Command::Register {
                node: 1,
                incarnation: "fixture".into(),
                address: "http://127.0.0.1:64000".into(),
                replicas: None,
                memory: false,
                disk_id: None,
                expires_at: 0,
                lease_ms: 3000,
            })
            .await
            .map_err(io::Error::other)?;
        let before_external_batch = serde_json::to_vec(&control.store.metadata()).unwrap();
        let before_log = control.store.state.lock().unwrap().last_applied;
        let rejection = control
            .write(Command::Batch {
                commands: vec![Command::Heartbeat {
                    node: 1,
                    incarnation: "fixture".into(),
                    applied: 0,
                    expires_at: 0,
                    lease_ms: 3000,
                }],
            })
            .await
            .unwrap_err();
        assert_eq!(rejection.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            serde_json::to_vec(&control.store.metadata()).unwrap(),
            before_external_batch
        );
        assert_eq!(control.store.state.lock().unwrap().last_applied, before_log);
        // Hold real state-machine application: no response may precede apply.
        let application = control
            .store
            .test_apply_gate()
            .acquire_owned()
            .await
            .unwrap();
        let waiting = tokio::spawn({
            let control = control.clone();
            async move {
                control
                    .write(Command::Claim {
                        node: 1,
                        incarnation: "fixture".into(),
                        client: "no-early".into(),
                        generation: 1,
                    })
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !waiting.is_finished(),
            "reply preceded state-machine application"
        );
        assert!(!control.store.metadata().clients.contains_key("no-early"));
        drop(application);
        waiting
            .await
            .map_err(io::Error::other)?
            .map_err(io::Error::other)?;
        // Cancel514 queued replies while the one worker retains all accepted work.
        let before_submissions = control
            .raft
            .metrics()
            .borrow_watched()
            .last_applied
            .unwrap()
            .index;
        let commands = (0..514).map(|i| {
            if i % 2 == 0 {
                Command::Heartbeat {
                    node: 1,
                    incarnation: "fixture".into(),
                    applied: 0,
                    expires_at: 0,
                    lease_ms: 3000,
                }
            } else {
                Command::Claim {
                    node: 1,
                    incarnation: "fixture".into(),
                    client: format!("quota-{i}"),
                    generation: 1,
                }
            }
        });
        let mut writes: Vec<_> = commands
            .map(|command| Box::pin(control.write(command)))
            .collect();
        // This deliberately polls all sends in one task before cancelling replies.
        // Disable only the test harness's cooperative poll budget, so Pending means
        // waiting for the reply, rather than an unpolled enqueue after128 sends.
        tokio::task::coop::unconstrained(std::future::poll_fn(|cx| {
            for request in &mut writes {
                assert!(
                    request.as_mut().poll(cx).is_pending(),
                    "submission must not be capacity-rejected"
                );
            }
            std::task::Poll::Ready(())
        }))
        .await;
        drop(writes);
        let mut changes = control.store.changed.subscribe();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let done = control
                    .store
                    .state
                    .lock()
                    .unwrap()
                    .metadata
                    .clients
                    .keys()
                    .filter(|key| key.starts_with("quota-"))
                    .count()
                    == 257;
                if done {
                    break;
                }
                changes.changed().await.unwrap();
            }
        })
        .await
        .expect("all accepted child commands must apply after caller cancellation");
        let last = control
            .store
            .state
            .lock()
            .unwrap()
            .last_applied
            .unwrap()
            .index;
        eprintln!(
            "514 canceled requests applied using {} durable Raft entries",
            last - before_submissions
        );
        assert!(
            last - before_submissions <= 10,
            "514 queued requests should share at most10 WAL entries, observed{}",
            last - before_submissions
        );
        {
            let mut state = control.store.state.lock().unwrap();
            state.metadata = Metadata {
                revision: 7,
                ..Metadata::default()
            };
            for (id, applied) in [(101, 7), (102, 6)] {
                state.metadata.nodes.insert(
                    id,
                    Node {
                        id,
                        incarnation: format!("n{id}"),
                        address: "http://127.0.0.1:1".into(),
                        expires_at: 0,
                        max_lease_ms: 3000,
                        applied,
                    },
                );
            }
        }
        // Pending barriers beyond the former1024 cap must wait for installation.
        let mut pending: Vec<_> = (0..1025).map(|_| Box::pin(control.applied(7))).collect();
        std::future::poll_fn(|cx| {
            for request in &mut pending {
                assert!(
                    request.as_mut().poll(cx).is_pending(),
                    "barrier must wait, not capacity-reject"
                );
            }
            std::task::Poll::Ready(())
        })
        .await;
        drop(pending);
        let waiter = tokio::spawn({
            let control = control.clone();
            async move { control.applied(7).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        control
            .store
            .state
            .lock()
            .unwrap()
            .metadata
            .nodes
            .get_mut(&102)
            .unwrap()
            .applied = 7;
        control.store.changed.send_replace(7);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), waiter)
                .await
                .expect("same-revision heartbeat must wake waiter")
                .map_err(io::Error::other)?
                .map_err(io::Error::other)?,
            7
        );
        // Installation ACKs complete the next barrier before a persisted
        // Heartbeat advances Node.applied, with no Raft log append.
        control.store.state.lock().unwrap().metadata.revision = 8;
        let before_ack = control.raft.metrics().borrow_watched().last_applied;
        let waiter = tokio::spawn({
            let control = control.clone();
            async move { control.applied(8).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        control
            .acknowledge(101, "n101", 8)
            .map_err(io::Error::other)?;
        assert!(!waiter.is_finished());
        control
            .acknowledge(102, "n102", 8)
            .map_err(io::Error::other)?;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), waiter)
                .await
                .expect("volatile ACK must wake waiter")
                .map_err(io::Error::other)?
                .map_err(io::Error::other)?,
            8
        );
        assert_eq!(
            control.store.state.lock().unwrap().metadata.nodes[&102].applied,
            7
        );
        assert_eq!(
            control.raft.metrics().borrow_watched().last_applied,
            before_ack
        );
        {
            let mut state = control.store.state.lock().unwrap();
            state.metadata.revision = 9;
            state.metadata.nodes.get_mut(&101).unwrap().applied = 9;
        }
        let waiter = tokio::spawn({
            let control = control.clone();
            async move { control.applied(9).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        let term = control.raft.metrics().borrow_watched().current_term;
        let last = control.store.state.lock().unwrap().last_applied;
        control
            .raft
            .append_entries(openraft::raft::AppendEntriesRequest {
                vote: openraft::Vote::new_committed(term + 1, 2),
                prev_log_id: last,
                entries: vec![],
                leader_commit: last,
            })
            .await
            .map_err(io::Error::other)?;
        // append_entries completion may precede publication of the metrics watch.
        let mut metrics = control.raft.metrics();
        tokio::time::timeout(Duration::from_secs(1), async {
            while metrics.borrow_watched().current_term <= term {
                metrics.changed().await.unwrap();
            }
        })
        .await
        .expect("higher vote must publish its term");
        let result = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("higher term must reject waiter")
            .map_err(io::Error::other)?;
        assert!(result.is_err(), "new term cannot complete an old barrier");
        control.raft.shutdown().await.map_err(io::Error::other)?;
        drop(control);
        tokio::fs::remove_dir_all(path).await?;
        Ok(())
    }
    #[test]
    fn replication_batch_of_maximal_commands_fits_existing_wire_bound() {
        let mut command = Command::Filters {
            node: u64::MAX,
            incarnation: "i".repeat(64),
            client: "c".repeat(65535),
            generation: u64::MAX,
            filters: vec!["f".repeat(65535); 7],
        };
        let command_limit = 512 * 1024;
        let excess = serde_json::to_vec(&command).unwrap().len() - command_limit;
        if let Command::Filters { filters, .. } = &mut command {
            let last = filters.last_mut().unwrap();
            last.truncate(last.len() - excess);
        }
        validate(&command).unwrap();
        assert_eq!(serde_json::to_vec(&command).unwrap().len(), command_limit);
        let leader = crate::model::committed_leader_id(u64::MAX, u64::MAX);
        let last = openraft::LogId::new(leader, u64::MAX);
        let entries = (0..MAX_REPLICATION_ENTRIES)
            .map(|i| openraft::Entry {
                log_id: openraft::LogId::new(leader, u64::MAX - MAX_REPLICATION_ENTRIES + i),
                payload: openraft::EntryPayload::Normal(command.clone()),
            })
            .collect();
        let envelope = rpc::encode(&RaftRequest::Append(openraft::raft::AppendEntriesRequest {
            vote: openraft::Vote::new_committed(u64::MAX, u64::MAX),
            prev_log_id: Some(last),
            leader_commit: Some(last),
            entries,
        }))
        .unwrap();
        assert!(envelope.data.len() <= rpc::MAX_WIRE);
        assert!(envelope.data.len() > 12 * 1024 * 1024);
    }
}
