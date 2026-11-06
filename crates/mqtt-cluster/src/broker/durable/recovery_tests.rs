//! Controlled RPC fault injection, not a substitute for real Raft process tests.
use super::*;
use rpc::wire::{
    Envelope,
    cluster_server::{Cluster, ClusterServer},
};
use std::sync::atomic::AtomicUsize;
use tonic::{Response, Status};

#[derive(Clone)]
struct Peer {
    runtime: Weak<BrokerRuntime>,
    journal: Journal,
    calls: Arc<Mutex<Vec<(u64, u64)>>>,
    lost_replies: Arc<AtomicUsize>,
    pre_submit_errors: Arc<AtomicUsize>,
    barrier_errors: Arc<AtomicUsize>,
    write_errors: Arc<AtomicUsize>,
    barrier_calls: Arc<AtomicUsize>,
    takeover_on_barrier: Arc<AtomicUsize>,
}
fn consume(counter: &AtomicUsize) -> bool {
    counter
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
            value.checked_sub(1)
        })
        .is_ok()
}
#[tonic::async_trait]
impl Cluster for Peer {
    type WatchStream = tokio_stream::wrappers::ReceiverStream<Result<Envelope, Status>>;
    async fn read(&self, request: tonic::Request<Envelope>) -> Result<Response<Envelope>, Status> {
        let request: rpc::ReadRequest = rpc::decode(request.into_inner())?;
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| Status::unavailable("stopped"))?;
        let value = match request {
            rpc::ReadRequest::Applied { revision } => {
                let count = self.barrier_calls.fetch_add(1, Ordering::SeqCst) + 1;
                if self.takeover_on_barrier.load(Ordering::SeqCst) == count {
                    let mut metadata = runtime.metadata.write().unwrap();
                    let key = client_key("filter-race");
                    let expected_epoch = metadata.durable[&key].epoch;
                    let answer = metadata.apply(Command::AcquireOnce {
                        key,
                        node: 102,
                        incarnation: "other".into(),
                        generation: 99,
                        expected_epoch,
                    });
                    assert!(answer.error.is_none());
                    *runtime.recipients.write().unwrap() =
                        crate::broker::recipients::RecipientIndex::from_metadata(&metadata);
                }
                if consume(&self.barrier_errors) {
                    return Err(Status::unavailable("injected barrier failure"));
                }
                rpc::encode(&revision)
            }
            rpc::ReadRequest::Owner { key } => {
                rpc::encode(&runtime.metadata.read().unwrap().durable.get(&key).cloned())
            }
            _ => return Err(Status::unimplemented("read kind")),
        }?;
        Ok(Response::new(value))
    }
    async fn mutate(
        &self,
        request: tonic::Request<Envelope>,
    ) -> Result<Response<Envelope>, Status> {
        let command: Command = rpc::decode(request.into_inner())?;
        if let Command::Acquire {
            generation,
            expected_epoch,
            ..
        }
        | Command::AcquireOnce {
            generation,
            expected_epoch,
            ..
        } = &command
        {
            self.calls
                .lock()
                .unwrap()
                .push((*generation, *expected_epoch));
        }
        if consume(&self.pre_submit_errors) {
            return Err(Status::resource_exhausted("injected rejected submission"));
        }
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| Status::unavailable("stopped"))?;
        let answer = {
            let mut metadata = runtime.metadata.write().unwrap();
            let answer = metadata.apply(command);
            *runtime.recipients.write().unwrap() =
                crate::broker::recipients::RecipientIndex::from_metadata(&metadata);
            answer
        };
        if consume(&self.lost_replies) {
            return Err(Status::unavailable("injected lost committed reply"));
        }
        Ok(Response::new(rpc::encode(&answer)?))
    }
    async fn replica(
        &self,
        request: tonic::Request<Envelope>,
    ) -> Result<Response<Envelope>, Status> {
        let message: ReplicaMessage = rpc::decode(request.into_inner())?;
        if matches!(message.operation, Request::Write { .. }) && consume(&self.write_errors) {
            return Err(Status::unavailable("injected partial write"));
        }
        let reply = self
            .journal
            .execute_at(message.generation, message.operation)
            .await
            .map_err(|_| Status::unavailable("replica"))?;
        Ok(Response::new(rpc::encode(&reply)?))
    }
    async fn replica_batch(
        &self,
        _: tonic::Request<Envelope>,
    ) -> Result<Response<Envelope>, Status> {
        Err(Status::unimplemented("replica_batch"))
    }
    async fn raft(&self, _: tonic::Request<Envelope>) -> Result<Response<Envelope>, Status> {
        Err(Status::unimplemented("raft"))
    }
    async fn watch(
        &self,
        _: tonic::Request<Envelope>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        Err(Status::unimplemented("watch"))
    }
    async fn repair(&self, _: tonic::Request<Envelope>) -> Result<Response<Envelope>, Status> {
        Err(Status::unimplemented("repair"))
    }
    async fn deliver_to(&self, _: tonic::Request<Envelope>) -> Result<Response<Envelope>, Status> {
        Err(Status::unimplemented("deliver_to"))
    }
    async fn deliver_batch(
        &self,
        _: tonic::Request<Envelope>,
    ) -> Result<Response<Envelope>, Status> {
        Err(Status::unimplemented("deliver_batch"))
    }
    async fn deliver(&self, _: tonic::Request<Envelope>) -> Result<Response<Envelope>, Status> {
        Err(Status::unimplemented("deliver"))
    }
    async fn forward_batch(
        &self,
        _: tonic::Request<Envelope>,
    ) -> Result<Response<Envelope>, Status> {
        Err(Status::unimplemented("forward_batch"))
    }
}
struct Fixture {
    runtime: Arc<BrokerRuntime>,
    peer: Peer,
    peers: Vec<Journal>,
    servers: Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
    path: std::path::PathBuf,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.runtime.stopped.store(true, Ordering::Release);
        for server in &self.servers {
            server.abort();
        }
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
impl Fixture {
    async fn new(label: &str) -> io::Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "rmqtt-recovery-{label}-{}-{}",
            std::process::id(),
            controller::now()
        ));
        tokio::fs::create_dir_all(&path).await?;
        tokio::fs::write(path.join("token"), "q".repeat(32)).await?;
        let mut listeners = Vec::new();
        for _ in 0..3 {
            listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await?);
        }
        let addresses = listeners
            .iter()
            .map(|listener| {
                listener
                    .local_addr()
                    .map(|address| format!("http://{address}"))
            })
            .collect::<io::Result<Vec<_>>>()?;
        let config=serde_json::from_value(serde_json::json!({
            "role":"broker","cluster_id":"recovery","node_id":101,
            "listen":listeners[0].local_addr()?,"data_dir":path.join("local"),"token_file":path.join("token"),
            "controllers":[{"id":1,"address":addresses[1]},{"id":2,"address":"http://127.0.0.1:65001"},{"id":3,"address":"http://127.0.0.1:65002"}],
            "durable":{"replicas":[{"id":101,"address":addresses[0]},{"id":102,"address":addresses[1]},{"id":103,"address":addresses[2]}]}
        })).map_err(io::Error::other)?;
        let runtime = BrokerRuntime::new(config)?;
        runtime.initialized.store(true, Ordering::Release);
        *runtime.deadline.lock().unwrap() = tokio::time::Instant::now() + Duration::from_secs(60);
        let members = runtime
            .config
            .durable
            .as_ref()
            .unwrap()
            .replicas
            .iter()
            .map(|peer| (peer.id, peer.address.clone()))
            .collect::<Vec<_>>();
        {
            let mut metadata = runtime.metadata.write().unwrap();
            metadata.data_mode = Some(DataMode::Memory(members.clone()));
            metadata
                .disk_ids
                .insert(101, runtime.journal.as_ref().unwrap().disk_id().into());
            metadata.nodes.insert(
                101,
                Node {
                    id: 101,
                    incarnation: runtime.incarnation.clone(),
                    address: addresses[0].clone(),
                    expires_at: controller::now() + 60000,
                    max_lease_ms: 3000,
                    applied: 0,
                },
            );
        }
        assert!(runtime.ready());
        let mut peers = vec![runtime.journal.as_ref().unwrap().clone()];
        peers.push(Journal::open(
            &path.join("peer102"),
            ("recovery", 102),
            &members,
        )?);
        peers.push(Journal::open(
            &path.join("peer103"),
            ("recovery", 103),
            &members,
        )?);
        let peer = Peer {
            runtime: Arc::downgrade(&runtime),
            journal: peers[0].clone(),
            calls: Arc::default(),
            lost_replies: Arc::new(AtomicUsize::new(0)),
            pre_submit_errors: Arc::new(AtomicUsize::new(0)),
            barrier_errors: Arc::new(AtomicUsize::new(0)),
            write_errors: Arc::new(AtomicUsize::new(0)),
            barrier_calls: Arc::new(AtomicUsize::new(0)),
            takeover_on_barrier: Arc::new(AtomicUsize::new(0)),
        };
        let mut servers = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let mut service = peer.clone();
            service.journal = peers[index].clone();
            servers.push(tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(
                        ClusterServer::new(service)
                            .max_decoding_message_size(rpc::MAX_WIRE)
                            .max_encoding_message_size(rpc::MAX_WIRE),
                    )
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
            ));
        }
        Ok(Self {
            runtime,
            peer,
            peers,
            servers,
            path,
        })
    }
    fn data(&self) -> Arc<Durable> {
        self.runtime.data.as_ref().unwrap().clone()
    }
    async fn work(&self, key: &str) -> io::Result<Work> {
        let (reply, rx) = oneshot::channel();
        self.data()
            .send(
                key,
                Operation::Work {
                    limit: super::FORWARD_QUANTUM,
                    reply,
                },
            )
            .await?;
        rx.await.map_err(io::Error::other)?
    }
    fn owner(&self, key: &str) -> DurableOwner {
        self.runtime.metadata.read().unwrap().durable[key].clone()
    }
}

#[tokio::test]
async fn lost_committed_acquire_reply_recovers_the_same_identity_and_epoch() -> io::Result<()> {
    let fixture = Fixture::new("lost-reply").await?;
    let key = client_key("lost");
    fixture.peer.lost_replies.store(1, Ordering::SeqCst);
    assert!(fixture.data().acquire(key.clone(), 42, 0).await.is_err());
    let epoch = fixture.owner(&key).epoch;
    assert!(
        fixture
            .data()
            .one(key.clone(), super::FORWARD_QUANTUM)
            .await?
            > 0
    );
    assert_eq!(
        *fixture.peer.calls.lock().unwrap(),
        vec![(42, 0), (42, 0)],
        "background must retry the original identity"
    );
    assert_eq!(
        fixture.owner(&key).epoch,
        epoch,
        "lost reply must not manufacture another owner epoch"
    );
    assert!(
        fixture.work(&key).await?.activation.is_some(),
        "failed admission recovers offline work"
    );
    Ok(())
}

#[tokio::test]
async fn repeated_barrier_failures_do_not_change_the_pending_predecessor() -> io::Result<()> {
    let fixture = Fixture::new("barrier-retry").await?;
    let key = client_key("barrier");
    fixture.peer.barrier_errors.store(1, Ordering::SeqCst);
    assert!(fixture.data().acquire(key.clone(), 42, 0).await.is_err());
    let epoch = fixture.owner(&key).epoch;
    for _ in 0..2 {
        fixture.peer.barrier_errors.store(1, Ordering::SeqCst);
        assert!(
            fixture
                .data()
                .one(key.clone(), super::FORWARD_QUANTUM)
                .await
                .is_err()
        );
        assert_eq!(fixture.owner(&key).epoch, epoch);
    }
    assert!(
        fixture
            .data()
            .one(key.clone(), super::FORWARD_QUANTUM)
            .await?
            > 0
    );
    assert_eq!(*fixture.peer.calls.lock().unwrap(), vec![(42, 0); 4]);
    assert!(fixture.work(&key).await?.activation.is_some());
    Ok(())
}

#[tokio::test]
async fn partial_same_epoch_write_recovery_preserves_the_highest_physical_version() -> io::Result<()>
{
    let fixture = Fixture::new("partial-write").await?;
    let key = client_key("partial");
    fixture.peer.write_errors.store(2, Ordering::SeqCst);
    assert!(fixture.data().acquire(key.clone(), 42, 0).await.is_err());
    let epoch = fixture.owner(&key).epoch;
    // Inject an already-completed same-epoch body from a partial prior quorum
    // outcome. Recovery must fence/read it, never overwrite version9 with0.
    let mut record = Record::empty(epoch);
    record.physical_revision = 9;
    record.snapshot = Some(vec![7, 8]);
    record.logical_generation = 3;
    for journal in fixture.peers.iter().take(2) {
        journal
            .execute(Request::Write {
                key: key.clone(),
                record: record.clone(),
            })
            .await?;
    }
    assert!(
        fixture
            .data()
            .one(key.clone(), super::FORWARD_QUANTUM)
            .await?
            > 0
    );
    assert_eq!(fixture.owner(&key).epoch, epoch);
    let restored = fixture.peers[0]
        .execute(Request::Read { key: key.clone() })
        .await?
        .record
        .unwrap();
    assert_eq!(restored.physical_revision, 9);
    assert_eq!(restored.snapshot, Some(vec![7, 8]));
    assert_eq!(restored.logical_generation, 3);
    assert_eq!(*fixture.peer.calls.lock().unwrap(), vec![(42, 0); 2]);
    Ok(())
}

#[tokio::test]
async fn stale_recover_job_does_not_fence_a_new_successful_admission() -> io::Result<()> {
    let fixture = Fixture::new("stale-work").await?;
    let key = client_key("stale");
    fixture.peer.lost_replies.store(1, Ordering::SeqCst);
    assert!(fixture.data().acquire(key.clone(), 42, 0).await.is_err());
    assert!(fixture.work(&key).await.is_err());
    // Work reported Missing, but its Recover job arrives AFTER a newer CONNECT
    // has completed. The private actor operation must re-check current state.
    let previous = fixture.owner(&key).epoch;
    fixture.data().acquire(key.clone(), 43, previous).await?;
    let latest = fixture.owner(&key).epoch;
    fixture.data().recover(&key).await?;
    assert_eq!(fixture.owner(&key).epoch, latest);
    assert_eq!(fixture.owner(&key).generation, 43);
    assert_eq!(fixture.peer.calls.lock().unwrap().len(), 2);
    assert!(fixture.work(&key).await?.activation.is_none());
    Ok(())
}

#[tokio::test]
async fn rejected_new_admission_restores_prior_unknown_identity() -> io::Result<()> {
    let fixture = Fixture::new("rejected-new").await?;
    let key = client_key("prior");
    fixture.peer.lost_replies.store(1, Ordering::SeqCst);
    assert!(fixture.data().acquire(key.clone(), 42, 0).await.is_err());
    let epoch = fixture.owner(&key).epoch;
    let error = fixture
        .data()
        .acquire(key.clone(), 43, 0)
        .await
        .unwrap_err();
    assert!(
        error
            .get_ref()
            .unwrap()
            .is::<mqtt_cluster_core::AcquisitionConflict>()
    );
    fixture.data().recover(&key).await?;
    assert_eq!(
        *fixture.peer.calls.lock().unwrap(),
        vec![(42, 0), (43, 0), (42, 0)]
    );
    assert_eq!(fixture.owner(&key).epoch, epoch);
    Ok(())
}

#[tokio::test]
async fn unknown_rejected_attempt_reconciles_to_the_committed_identity_without_new_epoch()
-> io::Result<()> {
    let fixture = Fixture::new("reconcile-local").await?;
    let key = client_key("reconcile");
    fixture.peer.lost_replies.store(1, Ordering::SeqCst);
    assert!(fixture.data().acquire(key.clone(), 42, 0).await.is_err());
    let epoch = fixture.owner(&key).epoch;
    fixture.peer.lost_replies.store(1, Ordering::SeqCst);
    assert!(fixture.data().acquire(key.clone(), 43, 0).await.is_err());
    fixture.data().recover(&key).await?;
    assert_eq!(
        *fixture.peer.calls.lock().unwrap(),
        vec![(42, 0), (43, 0), (43, 0), (42, 0)]
    );
    assert_eq!(fixture.owner(&key).epoch, epoch);
    Ok(())
}

#[tokio::test]
async fn a_delayed_old_acquisition_cannot_replace_a_new_foreign_owner() -> io::Result<()> {
    let fixture = Fixture::new("foreign-owner").await?;
    let key = client_key("foreign");
    fixture.peer.lost_replies.store(1, Ordering::SeqCst);
    assert!(fixture.data().acquire(key.clone(), 42, 0).await.is_err());
    let previous = fixture.owner(&key).epoch;
    {
        let mut metadata = fixture.runtime.metadata.write().unwrap();
        metadata.nodes.insert(
            102,
            Node {
                id: 102,
                incarnation: "other".into(),
                address: fixture.runtime.config.durable.as_ref().unwrap().replicas[1]
                    .address
                    .clone(),
                expires_at: controller::now() + 60000,
                max_lease_ms: 3000,
                applied: previous,
            },
        );
        let answer = metadata.apply(Command::AcquireOnce {
            key: key.clone(),
            node: 102,
            incarnation: "other".into(),
            generation: 3,
            expected_epoch: previous,
        });
        assert!(answer.error.is_none());
        *fixture.runtime.recipients.write().unwrap() =
            crate::broker::recipients::RecipientIndex::from_metadata(&metadata);
    }
    let foreign = fixture.owner(&key).epoch;
    assert!(fixture.data().recover(&key).await.is_err());
    assert_eq!(fixture.owner(&key).epoch, foreign);
    assert_eq!(fixture.owner(&key).node, 102);
    assert_eq!(*fixture.peer.calls.lock().unwrap(), vec![(42, 0), (42, 0)]);
    Ok(())
}

#[tokio::test]
async fn genuinely_uncertain_data_commit_creates_only_one_recovery_identity() -> io::Result<()> {
    let fixture = Fixture::new("data-unknown").await?;
    let key = client_key("commit");
    let lease = fixture.data().acquire(key.clone(), 42, 0).await?;
    fixture.peer.write_errors.store(2, Ordering::SeqCst);
    assert!(
        fixture
            .data()
            .transaction(SessionMutation {
                client_id: "commit".into(),
                epoch: lease.epoch,
                expected_revision: lease.revision,
                snapshot: Some(vec![9]),
                reset_session: false,
                filters: vec![],
                publications: vec![],
                receipt: None
            })
            .await
            .is_err()
    );
    fixture.peer.barrier_errors.store(1, Ordering::SeqCst);
    assert!(
        fixture
            .data()
            .one(key.clone(), super::FORWARD_QUANTUM)
            .await
            .is_err()
    );
    let recovered = fixture.owner(&key).epoch;
    assert!(recovered > lease.epoch);
    assert!(
        fixture
            .data()
            .one(key.clone(), super::FORWARD_QUANTUM)
            .await?
            > 0
    );
    assert_eq!(fixture.owner(&key).epoch, recovered);
    assert_eq!(
        *fixture.peer.calls.lock().unwrap(),
        vec![(42, 0), (0, lease.epoch), (0, lease.epoch)]
    );
    assert!(fixture.work(&key).await?.activation.is_some());
    Ok(())
}

#[tokio::test]
async fn canceled_success_reply_activates_offline_but_live_admission_does_not() -> io::Result<()> {
    let fixture = Fixture::new("canceled-admission").await?;
    let key = client_key("canceled");
    let (reply, rx) = oneshot::channel();
    drop(rx);
    fixture
        .data()
        .send(
            &key,
            Operation::Acquire {
                generation: 42,
                expected: 0,
                reply,
            },
        )
        .await?;
    assert!(fixture.work(&key).await?.activation.is_some());
    let live = client_key("live");
    fixture.data().acquire(live.clone(), 43, 0).await?;
    assert!(fixture.work(&live).await?.activation.is_none());
    Ok(())
}

#[tokio::test]
async fn host_acquisition_retry_reuses_the_original_identity() -> io::Result<()> {
    let fixture = Fixture::new("host-unknown").await?;
    let message = ClusterMessage {
        topic: "events/test".into(),
        payload: vec![1],
        qos: 0,
        retain: false,
        properties: Default::default(),
        expires_at: None,
        publisher: None,
        observer_client: "host".into(),
        scope: ForwardScope::Ordinary,
    };
    fixture.peer.lost_replies.store(1, Ordering::SeqCst);
    assert!(fixture.data().enqueue(message.clone()).await.is_err());
    let key = format!(
        "h:{}:{}",
        fixture.runtime.config.node_id, fixture.runtime.incarnation
    );
    let epoch = fixture.owner(&key).epoch;
    fixture.data().enqueue(message).await?;
    assert_eq!(*fixture.peer.calls.lock().unwrap(), vec![(0, 0), (0, 0)]);
    assert_eq!(fixture.owner(&key).epoch, epoch);
    Ok(())
}

#[tokio::test]
async fn confirmed_rejected_old_connect_does_not_poison_a_healthy_current_session() -> io::Result<()>
{
    let fixture = Fixture::new("healthy-rejection").await?;
    let key = client_key("healthy");
    fixture.data().acquire(key.clone(), 42, 0).await?;
    let epoch = fixture.owner(&key).epoch;
    assert!(fixture.data().acquire(key.clone(), 43, 0).await.is_err());
    fixture.data().recover(&key).await?;
    assert_eq!(fixture.owner(&key).epoch, epoch);
    assert_eq!(fixture.peer.calls.lock().unwrap().len(), 2);
    assert!(fixture.work(&key).await?.activation.is_none());
    Ok(())
}

#[tokio::test]
async fn ownership_change_during_filter_barrier_cannot_install_a_stale_cache() -> io::Result<()> {
    let fixture = Fixture::new("filter-race").await?;
    let key = client_key("filter-race");
    {
        let mut metadata = fixture.runtime.metadata.write().unwrap();
        metadata.revision = 1;
        metadata.nodes.insert(
            102,
            Node {
                id: 102,
                incarnation: "other".into(),
                address: fixture.runtime.config.durable.as_ref().unwrap().replicas[1]
                    .address
                    .clone(),
                expires_at: controller::now() + 60000,
                max_lease_ms: 3000,
                applied: 0,
            },
        );
    }
    let mut record = Record::empty(1);
    record.filters = vec!["events/#".into()];
    for journal in fixture.peers.iter().take(2) {
        journal
            .execute(Request::Write {
                key: key.clone(),
                record: record.clone(),
            })
            .await?;
    }
    fixture.peer.takeover_on_barrier.store(2, Ordering::SeqCst);
    assert!(
        fixture.data().acquire(key.clone(), 42, 0).await.is_err(),
        "ownership must be rechecked after the final filter barrier"
    );
    assert_eq!(fixture.owner(&key).node, 102);
    Ok(())
}

#[tokio::test]
async fn a_new_unknown_admission_supersedes_the_old_pending_only_with_its_own_identity()
-> io::Result<()> {
    let fixture = Fixture::new("new-unknown").await?;
    let key = client_key("supersede");
    fixture.peer.lost_replies.store(1, Ordering::SeqCst);
    assert!(fixture.data().acquire(key.clone(), 42, 0).await.is_err());
    let first = fixture.owner(&key).epoch;
    fixture.peer.lost_replies.store(1, Ordering::SeqCst);
    assert!(
        fixture
            .data()
            .acquire(key.clone(), 43, first)
            .await
            .is_err()
    );
    let current = fixture.owner(&key).epoch;
    assert!(current > first);
    fixture.data().recover(&key).await?;
    assert_eq!(
        *fixture.peer.calls.lock().unwrap(),
        vec![(42, 0), (43, first), (43, first)]
    );
    assert_eq!(fixture.owner(&key).epoch, current);
    Ok(())
}

#[tokio::test]
async fn a_proven_pre_submit_rejection_does_not_discard_prior_pending() -> io::Result<()> {
    let fixture = Fixture::new("safe-rejection").await?;
    let key = client_key("safe");
    fixture.peer.lost_replies.store(1, Ordering::SeqCst);
    assert!(fixture.data().acquire(key.clone(), 42, 0).await.is_err());
    let first = fixture.owner(&key).epoch;
    fixture.peer.pre_submit_errors.store(1, Ordering::SeqCst);
    let error = fixture
        .data()
        .acquire(key.clone(), 43, first)
        .await
        .unwrap_err();
    assert!(rpc::not_submitted(&error));
    fixture.data().recover(&key).await?;
    assert_eq!(
        *fixture.peer.calls.lock().unwrap(),
        vec![(42, 0), (43, first), (42, 0)]
    );
    assert_eq!(fixture.owner(&key).epoch, first);
    Ok(())
}

fn host_key(fixture: &Fixture) -> String {
    format!(
        "h:{}:{}",
        fixture.runtime.config.node_id, fixture.runtime.incarnation
    )
}

fn host_message(payload: impl Into<Vec<u8>>) -> ClusterMessage {
    ClusterMessage {
        topic: "events/batch".into(),
        payload: payload.into(),
        qos: 0,
        retain: false,
        properties: Default::default(),
        expires_at: None,
        publisher: None,
        observer_client: "host".into(),
        scope: ForwardScope::Ordinary,
    }
}

fn host_job(
    fixture: &Fixture,
    payload: impl Into<Vec<u8>>,
) -> (HostJob, oneshot::Receiver<io::Result<()>>) {
    let message = host_message(payload);
    // Mirrors admission: the caller resolves recipients before queueing.
    let recipients = fixture.runtime.durable_recipients(&message.topic);
    let (reply, wait) = oneshot::channel();
    (
        HostJob {
            message,
            recipients,
            reply,
        },
        wait,
    )
}

fn empty_cell() -> Cell {
    Cell {
        pending: None,
        data_generation: 0,
        record: None,
        poisoned: false,
        activate: false,
        activation_token: Arc::new(()),
        forwarding: false,
        stashed: Vec::new(),
    }
}

fn subscribe(fixture: &Fixture, client: &str) {
    let mut metadata = fixture.runtime.metadata.write().unwrap();
    metadata.durable.insert(
        client_key(client),
        DurableOwner {
            node: fixture.runtime.config.node_id,
            incarnation: fixture.runtime.incarnation.clone(),
            generation: 1,
            epoch: 1,
            filters: vec!["events/#".into()],
            logical_generation: 1,
            acquired_from: None,
        },
    );
    *fixture.runtime.recipients.write().unwrap() =
        crate::broker::recipients::RecipientIndex::from_metadata(&metadata);
}

async fn recv_host(wait: oneshot::Receiver<io::Result<()>>) -> io::Result<()> {
    tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .map_err(|_| unavailable())?
        .map_err(io::Error::other)?
}

#[tokio::test]
async fn one_queued_host_batch_stores_once_in_input_order() -> io::Result<()> {
    let fixture = Fixture::new("host-batch-once").await?;
    subscribe(&fixture, "subscriber");
    let key = host_key(&fixture);
    let (tx, rx) = mpsc::channel(super::HOST_MAILBOX);
    let mut waits = Vec::new();
    let count = 10u8;
    for index in 0..count {
        let (reply, wait) = oneshot::channel();
        tx.try_send(Job {
            operation: Operation::Host {
                message: host_message(vec![index]),
                recipients: vec![Recipient {
                    client_id: "subscriber".into(),
                    generation: 1,
                }],
                reply,
            },
        })
        .expect("queue");
        waits.push(wait);
    }
    drop(tx);
    fixture.data().spawn_actor(key.clone(), rx);
    for wait in waits {
        recv_host(wait).await?;
    }
    let record = fixture.peers[0]
        .execute(Request::Read { key: key.clone() })
        .await?
        .record
        .expect("stored record");
    assert_eq!(record.revision, 1);
    assert_eq!(record.physical_revision, 1);
    assert_eq!(record.intents.len(), usize::from(count));
    for (index, intent) in record.intents.iter().enumerate() {
        assert_eq!(intent.id.source, key);
        assert_eq!(intent.id.epoch, record.epoch);
        assert_eq!(intent.id.revision, record.revision);
        assert_eq!(intent.id.index, u32::try_from(index).expect("index"));
        assert_eq!(
            intent.message.payload,
            vec![u8::try_from(index).expect("payload")]
        );
        if index > 0 {
            assert!(record.intents[index - 1].id < intent.id);
        }
    }
    Ok(())
}

#[tokio::test]
async fn batched_completion_is_idempotent_until_every_recipient_is_done() -> io::Result<()> {
    let fixture = Fixture::new("complete-batch").await?;
    subscribe(&fixture, "a");
    subscribe(&fixture, "b");
    let key = host_key(&fixture);
    let data = fixture.data();
    let mut cell = empty_cell();
    let (job, wait) = host_job(&fixture, b"one".to_vec());
    let _ = data
        .host_batch(&fixture.runtime, &key, vec![job], &mut cell, None)
        .await;
    recv_host(wait).await?;
    let intent = cell
        .record
        .as_ref()
        .expect("intent")
        .intents
        .first()
        .expect("intent")
        .clone();
    assert_eq!(intent.recipients.len(), 2);
    let id = intent.id.clone();
    data.complete(
        &fixture.runtime,
        &key,
        &[(id.clone(), "a".into())],
        &mut cell,
    )
    .await?;
    let record = cell.record.as_ref().expect("record");
    assert_eq!(record.intents.len(), 1);
    assert!(record.intents[0].completed.contains("a"));
    assert!(!record.intents[0].completed.contains("b"));
    data.complete(
        &fixture.runtime,
        &key,
        &[(id.clone(), "a".into())],
        &mut cell,
    )
    .await?;
    let record = cell.record.as_ref().expect("record");
    assert_eq!(record.intents.len(), 1);
    assert_eq!(record.intents[0].completed.len(), 1);
    let unchanged = record.physical_revision;
    let unknown = DeliveryId {
        index: id.index.saturating_add(1),
        ..id.clone()
    };
    data.complete(&fixture.runtime, &key, &[(unknown, "a".into())], &mut cell)
        .await?;
    assert_eq!(
        cell.record.as_ref().expect("record").physical_revision,
        unchanged
    );
    data.complete(
        &fixture.runtime,
        &key,
        &[(id.clone(), "a".into()), (id.clone(), "b".into())],
        &mut cell,
    )
    .await?;
    assert!(cell.record.as_ref().expect("record").intents.is_empty());
    let cleared = cell.record.as_ref().expect("record").physical_revision;
    data.complete(&fixture.runtime, &key, &[(id, "b".into())], &mut cell)
        .await?;
    assert_eq!(
        cell.record.as_ref().expect("record").physical_revision,
        cleared
    );
    Ok(())
}

#[tokio::test]
async fn a_full_outbox_parks_new_hosts_until_an_intent_slot_frees() -> io::Result<()> {
    let fixture = Fixture::new("host-full").await?;
    subscribe(&fixture, "subscriber");
    let key = host_key(&fixture);
    {
        let mut metadata = fixture.runtime.metadata.write().unwrap();
        metadata.durable.insert(
            key.clone(),
            DurableOwner {
                node: fixture.runtime.config.node_id,
                incarnation: fixture.runtime.incarnation.clone(),
                generation: 0,
                epoch: 1,
                filters: Vec::new(),
                logical_generation: 0,
                acquired_from: None,
            },
        );
    }
    let mut record = Record::empty(1);
    record.revision = 4;
    record.physical_revision = 7;
    for index in 0..super::MAX_INTENTS {
        record.intents.push(Intent {
            id: DeliveryId {
                source: key.clone(),
                epoch: 1,
                revision: 4,
                index: u32::try_from(index).expect("index"),
                target_generation: 0,
            },
            message: host_message(vec![1]),
            recipients: vec![Recipient {
                client_id: "subscriber".into(),
                generation: 1,
            }],
            completed: BTreeSet::new(),
        });
    }
    let committed = record.intents.clone();
    let mut cell = empty_cell();
    cell.record = Some(record);
    let (job, mut wait) = host_job(&fixture, b"overflow".to_vec());
    let _ = fixture
        .data()
        .host_batch(&fixture.runtime, &key, vec![job], &mut cell, None)
        .await;
    assert!(wait.try_recv().is_err());
    assert_eq!(cell.stashed.len(), 1);
    assert!(cell.forwarding);
    assert!(!cell.poisoned);
    let record = cell.record.as_ref().expect("record");
    assert_eq!(record.physical_revision, 7);
    assert_eq!(record.revision, 4);
    assert_eq!(record.intents.len(), committed.len());
    assert_eq!(record.intents[0].id, committed[0].id);
    assert_eq!(
        record.intents.last().expect("last").id,
        committed.last().expect("last").id
    );
    let parked = std::mem::take(&mut cell.stashed);
    cell.forwarding = false;
    cell.record.as_mut().expect("record").intents.clear();
    let _ = fixture
        .data()
        .host_batch(&fixture.runtime, &key, parked, &mut cell, None)
        .await;
    recv_host(wait).await?;
    let record = cell.record.as_ref().expect("record");
    assert_eq!(record.intents.len(), 1);
    assert_eq!(record.intents[0].message.payload, b"overflow");
    Ok(())
}

#[tokio::test]
async fn a_failed_host_quorum_fails_every_message_and_poisons() -> io::Result<()> {
    let fixture = Fixture::new("host-poison").await?;
    subscribe(&fixture, "subscriber");
    let key = host_key(&fixture);
    let data = fixture.data();
    let mut cell = empty_cell();
    let (warm, warm_wait) = host_job(&fixture, b"warm".to_vec());
    let _ = data
        .host_batch(&fixture.runtime, &key, vec![warm], &mut cell, None)
        .await;
    recv_host(warm_wait).await?;
    assert!(!cell.poisoned);
    let revision = cell.record.as_ref().expect("record").revision;
    let physical = cell.record.as_ref().expect("record").physical_revision;
    let intents = cell.record.as_ref().expect("record").intents.len();
    fixture.peer.write_errors.store(4, Ordering::SeqCst);
    let mut jobs = Vec::new();
    let mut waits = Vec::new();
    for index in 0..4u8 {
        let (job, wait) = host_job(&fixture, vec![index]);
        jobs.push(job);
        waits.push(wait);
    }
    let _ = data
        .host_batch(&fixture.runtime, &key, jobs, &mut cell, None)
        .await;
    for wait in waits {
        assert!(recv_host(wait).await.is_err());
    }
    assert!(cell.poisoned);
    let record = cell.record.as_ref().expect("record");
    assert_eq!(record.revision, revision);
    assert_eq!(record.physical_revision, physical);
    assert_eq!(record.intents.len(), intents);
    Ok(())
}

#[tokio::test]
async fn host_batch_parks_the_suffix_past_the_record_byte_budget() -> io::Result<()> {
    let fixture = Fixture::new("host-bytes").await?;
    subscribe(&fixture, "subscriber");
    let key = host_key(&fixture);
    let data = fixture.data();
    let mut cell = empty_cell();
    let (warm, warm_wait) = host_job(&fixture, b"warm".to_vec());
    let _ = data
        .host_batch(&fixture.runtime, &key, vec![warm], &mut cell, None)
        .await;
    recv_host(warm_wait).await?;
    let physical = cell.record.as_ref().expect("record").physical_revision;
    let payload = vec![255u8; 1024 * 1024];
    let mut jobs = Vec::new();
    let mut waits = Vec::new();
    for _ in 0..3 {
        let (job, wait) = host_job(&fixture, payload.clone());
        jobs.push(job);
        waits.push(wait);
    }
    let _ = data
        .host_batch(&fixture.runtime, &key, jobs, &mut cell, None)
        .await;
    let mut stored = 0;
    let mut parked = 0;
    for mut wait in waits {
        match wait.try_recv() {
            Ok(Ok(())) => stored += 1,
            Ok(Err(error)) => return Err(error),
            Err(_) => parked += 1,
        }
    }
    assert!(
        stored >= 1 && parked >= 1,
        "stored={stored} parked={parked}"
    );
    assert_eq!(cell.stashed.len(), parked);
    assert!(!cell.poisoned);
    let record = cell.record.as_ref().expect("record");
    assert_eq!(record.physical_revision, physical + 1);
    assert_eq!(record.intents.len(), 1 + stored);
    Ok(())
}
