//! Independent transport transaction checks; real replica/process acceptance is external.
use super::*;
use mqtt_cluster_core::{
    ClusterBridge, ClusterFuture, ClusterMessage, DeliveryId, LocalBroker, SessionCommit,
    SessionLease, SessionMutation,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[derive(Default)]
struct Record {
    epoch: u64,
    revision: u64,
    snapshot: Option<Vec<u8>>,
    receipts: BTreeSet<DeliveryId>,
}
#[derive(Default)]
struct Data {
    records: BTreeMap<String, Record>,
    mutations: Vec<SessionMutation>,
}
#[derive(Default)]
pub(super) struct Bridge {
    data: Arc<Mutex<Data>>,
    local: Mutex<Option<Arc<dyn LocalBroker>>>,
    ready: AtomicBool,
    fail_after_write: AtomicBool,
    hold: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    acquisitions: AtomicUsize,
    host: AtomicUsize,
}
impl std::fmt::Debug for Bridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicatedTransportMock")
    }
}
impl Bridge {
    fn with_data(data: Arc<Mutex<Data>>) -> Arc<Self> {
        Arc::new(Self {
            data,
            ..Self::default()
        })
    }
    fn local(&self) -> Arc<dyn LocalBroker> {
        self.local.lock().unwrap().clone().unwrap()
    }
}
impl ClusterBridge for Bridge {
    fn durable(&self) -> bool {
        true
    }
    fn ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
    fn open(&self, local: Arc<dyn LocalBroker>) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            *self.local.lock().unwrap() = Some(local);
            self.ready.store(true, Ordering::Release);
            Ok(())
        })
    }
    fn inspect_session(&self, client_id: String) -> ClusterFuture<'_, SessionLease> {
        Box::pin(async move {
            let data = self.data.lock().unwrap();
            let record = data.records.get(&client_id);
            Ok(SessionLease {
                client_id,
                epoch: record.map_or(0, |r| r.epoch),
                revision: record.map_or(0, |r| r.revision),
                snapshot: record.and_then(|r| r.snapshot.clone()),
            })
        })
    }
    fn acquire_session(
        &self,
        client_id: String,
        _: u64,
        expected_epoch: u64,
    ) -> ClusterFuture<'_, SessionLease> {
        Box::pin(async move {
            let mut data = self.data.lock().unwrap();
            let record = data.records.entry(client_id.clone()).or_default();
            if record.epoch != expected_epoch {
                return Err(io::Error::new(io::ErrorKind::WouldBlock, "epoch changed"));
            }
            record.epoch += 1;
            self.acquisitions.fetch_add(1, Ordering::AcqRel);
            Ok(SessionLease {
                client_id,
                epoch: record.epoch,
                revision: record.revision,
                snapshot: record.snapshot.clone(),
            })
        })
    }
    fn commit_session(&self, mutation: SessionMutation) -> ClusterFuture<'_, SessionCommit> {
        Box::pin(async move {
            if self.hold.load(Ordering::Acquire) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            let mut data = self.data.lock().unwrap();
            let record = data
                .records
                .get_mut(&mutation.client_id)
                .ok_or_else(|| io::Error::other("missing record"))?;
            if record.epoch != mutation.epoch {
                return Err(io::Error::other("stale epoch"));
            }
            if mutation
                .receipt
                .as_ref()
                .is_some_and(|receipt| record.receipts.contains(receipt))
            {
                return Ok(SessionCommit {
                    revision: record.revision,
                    duplicate: true,
                });
            }
            if record.revision != mutation.expected_revision {
                return Err(io::Error::other("stale revision"));
            }
            record.revision += 1;
            record.snapshot = mutation.snapshot.clone();
            if let Some(receipt) = &mutation.receipt {
                record.receipts.insert(receipt.clone());
            }
            let revision = record.revision;
            data.mutations.push(mutation);
            if self.fail_after_write.swap(false, Ordering::AcqRel) {
                return Err(io::Error::other("lost commit response"));
            }
            Ok(SessionCommit {
                revision,
                duplicate: false,
            })
        })
    }
    fn claim(&self, _: String, _: u64) -> ClusterFuture<'_, ()> {
        Box::pin(async { panic!("durable mode must acquire snapshot authority") })
    }
    fn release(&self, _: String, _: u64) -> ClusterFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn subscriptions(&self, _: String, _: u64, _: Vec<String>) -> ClusterFuture<'_, ()> {
        Box::pin(async { panic!("directory filters must share session commit") })
    }
    fn forward(&self, _: ClusterMessage) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            self.host.fetch_add(1, Ordering::AcqRel);
            Ok(())
        })
    }
    fn shutdown(&self) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            self.ready.store(false, Ordering::Release);
            Ok(())
        })
    }
}
fn config(bridge: Arc<Bridge>) -> BrokerConfig {
    BrokerConfig {
        cluster: Some(bridge),
        ..BrokerConfig::default()
    }
}
fn message(payload: &[u8]) -> ClusterMessage {
    super::cluster::message(
        &mqtt_session::durable::Publication {
            topic: "a".into(),
            payload: payload.into(),
            qos: 1,
            retain: false,
            properties: Vec::new(),
            expires_at: None,
            publisher: None,
            expired: false,
        },
        Some("source"),
    )
}
fn receipt() -> DeliveryId {
    DeliveryId {
        source: "original-source".into(),
        epoch: 2,
        revision: 4,
        index: 0,
        target_generation: 0,
    }
}

#[tokio::test]
async fn mqtt_ack_waits_for_atomic_snapshot_and_source_intent_without_local_fanout() {
    let bridge = Arc::new(Bridge::default());
    let (worker, task) = tests::worker(config(bridge.clone())).await;
    tests::connect(&worker, "source", 1, false).await;
    tests::subscribe(&worker, "source", 1).await;
    bridge.hold.store(true, Ordering::Release);
    let pending = {
        let worker = worker.clone();
        tokio::spawn(async move {
            tests::handle(&worker, "source", 1, tests::publication(b"durable", 7)).await
        })
    };
    bridge.entered.notified().await;
    assert!(!pending.is_finished());
    bridge.hold.store(false, Ordering::Release);
    bridge.resume.notify_one();
    assert_eq!(
        pending.await.unwrap().unwrap(),
        vec![Packet::PubAck { packet_id: 7 }]
    );
    {
        let data = bridge.data.lock().unwrap();
        let publication = data
            .mutations
            .iter()
            .find(|m| !m.publications.is_empty())
            .unwrap();
        assert!(publication.snapshot.is_some());
        assert_eq!(publication.publications[0].payload, b"durable");
    }
    assert_eq!(bridge.host.load(Ordering::Acquire), 0);
    assert!(
        tests::poll(&worker, "source", 1).await.is_none(),
        "local delivery must come through journal, not precommit fanout"
    );
    tests::stop(worker, task).await;
}

#[tokio::test]
async fn durable_receipt_discards_duplicate_proposal_even_after_outgoing_ack() {
    let bridge = Arc::new(Bridge::default());
    let (worker, task) = tests::worker(config(bridge.clone())).await;
    tests::connect(&worker, "target", 1, false).await;
    tests::subscribe(&worker, "target", 1).await;
    let local = bridge.local();
    local
        .deliver_to("target".into(), receipt(), message(b"once"))
        .await
        .unwrap();
    let Some(Packet::Publish {
        packet_id: Some(id),
        payload,
        ..
    }) = tests::poll(&worker, "target", 1).await
    else {
        panic!("missing durable delivery")
    };
    assert_eq!(payload, b"once");
    tests::handle(&worker, "target", 1, Packet::PubAck { packet_id: id })
        .await
        .unwrap();
    local
        .deliver_to("target".into(), receipt(), message(b"once"))
        .await
        .unwrap();
    assert!(tests::poll(&worker, "target", 1).await.is_none());
    assert_eq!(
        bridge.data.lock().unwrap().records["target"].receipts.len(),
        1
    );
    tests::stop(worker, task).await;
}

#[tokio::test]
async fn unknown_commit_fences_actor_and_reconnect_restores_committed_qos_two_state() {
    let bridge = Arc::new(Bridge::default());
    let (worker, task) = tests::worker(config(bridge.clone())).await;
    let (_, cancel) = tests::connect(&worker, "source", 1, false).await;
    let publish = Packet::Publish {
        topic: "a".into(),
        payload: b"qos2".to_vec(),
        qos: 2,
        retain: false,
        dup: false,
        packet_id: Some(17),
    };
    assert_eq!(
        tests::handle(&worker, "source", 1, publish).await.unwrap(),
        vec![Packet::PubRec { packet_id: 17 }]
    );
    bridge.fail_after_write.store(true, Ordering::Release);
    assert!(
        tests::handle(&worker, "source", 1, Packet::PubRel { packet_id: 17 })
            .await
            .is_err()
    );
    assert!(*cancel.borrow());
    let count = bridge.data.lock().unwrap().mutations.len();
    assert!(
        tests::handle(&worker, "source", 1, Packet::PingReq)
            .await
            .is_err()
    );
    assert_eq!(bridge.data.lock().unwrap().mutations.len(), count);
    assert!(tests::connect(&worker, "source", 2, false).await.0);
    assert_eq!(
        tests::handle(&worker, "source", 2, Packet::PubRel { packet_id: 17 })
            .await
            .unwrap(),
        vec![Packet::PubComp { packet_id: 17 }]
    );
    assert_eq!(
        bridge
            .data
            .lock()
            .unwrap()
            .mutations
            .iter()
            .map(|m| m.publications.len())
            .sum::<usize>(),
        1
    );
    tests::stop(worker, task).await;
}

#[tokio::test]
async fn another_worker_recovers_subscription_and_same_packet_id_without_old_disk() {
    let bridge = Arc::new(Bridge::default());
    let (worker, task) = tests::worker(config(bridge.clone())).await;
    tests::connect(&worker, "device", 1, false).await;
    tests::subscribe(&worker, "device", 1).await;
    bridge
        .local()
        .deliver_to("device".into(), receipt(), message(b"recover"))
        .await
        .unwrap();
    let Some(Packet::Publish {
        packet_id: Some(id),
        ..
    }) = tests::poll(&worker, "device", 1).await
    else {
        panic!("missing publish")
    };
    tests::stop(worker, task).await;
    let replacement = Bridge::with_data(bridge.data.clone());
    let (worker, task) = tests::worker(config(replacement.clone())).await;
    assert!(tests::connect(&worker, "device", 1, false).await.0);
    let Some(Packet::Publish {
        packet_id,
        dup,
        payload,
        ..
    }) = tests::poll(&worker, "device", 1).await
    else {
        panic!("missing restored publish")
    };
    assert_eq!(packet_id, Some(id));
    assert!(dup);
    assert_eq!(payload, b"recover");
    tests::stop(worker, task).await;
}

#[tokio::test]
async fn offline_activation_receives_durable_queue_before_client_reconnect() {
    let bridge = Arc::new(Bridge::default());
    let (worker, task) = tests::worker(config(bridge.clone())).await;
    tests::connect(&worker, "device", 1, false).await;
    tests::subscribe(&worker, "device", 1).await;
    tests::stop(worker, task).await;
    let replacement = Bridge::with_data(bridge.data.clone());
    let (worker, task) = tests::worker(config(replacement.clone())).await;
    let previous = replacement.inspect_session("device".into()).await.unwrap();
    let lease = replacement
        .acquire_session("device".into(), 0, previous.epoch)
        .await
        .unwrap();
    replacement.local().activate_session(lease).await.unwrap();
    replacement
        .local()
        .deliver_to("device".into(), receipt(), message(b"offline"))
        .await
        .unwrap();
    assert!(tests::connect(&worker, "device", 1, false).await.0);
    assert!(
        matches!(tests::poll(&worker,"device",1).await,Some(Packet::Publish{payload,..}) if payload==b"offline")
    );
    tests::stop(worker, task).await;
}

#[derive(Debug)]
struct Named;
impl mqtt_auth::AccessControl for Named {
    fn authenticate(
        &self,
        info: mqtt_auth::ConnectInfo<'_>,
    ) -> Result<mqtt_auth::Principal, mqtt_auth::AuthError> {
        Ok(mqtt_auth::Principal {
            name: format!("user:{}", info.username.unwrap_or("anonymous")),
        })
    }
    fn authorize(&self, _: &mqtt_auth::Principal, _: mqtt_auth::Action, _: &str) -> bool {
        true
    }
}
async fn named_connect(
    worker: &Worker,
    username: &str,
    generation: u64,
    clean: bool,
) -> (Reply, watch::Receiver<bool>) {
    let (wake, _) = watch::channel(0);
    let (cancel, closed) = watch::channel(false);
    (
        worker
            .request(Operation::Connect {
                client: "owned-device".into(),
                clean,
                will: None,
                username: Some(username.into()),
                password: None,
                generation,
                wake,
                cancel,
            })
            .await
            .unwrap(),
        closed,
    )
}
#[tokio::test]
async fn wrong_username_never_acquires_or_fences_the_current_session() {
    let owner = Arc::new(Bridge::default());
    let (worker, task) = tests::worker(BrokerConfig {
        access_control: Arc::new(Named),
        ..config(owner.clone())
    })
    .await;
    let (reply, closed) = named_connect(&worker, "alice", 1, false).await;
    assert!(matches!(reply, Reply::Connected(false)));
    let stranger = Bridge::with_data(owner.data.clone());
    let (other, other_task) = tests::worker(BrokerConfig {
        access_control: Arc::new(Named),
        ..config(stranger.clone())
    })
    .await;
    for clean in [false, true] {
        let (reply, _) = named_connect(&other, "bob", 2, clean).await;
        assert!(matches!(
            reply,
            Reply::Rejected(ConnectRefusal::NotAuthorized)
        ));
    }
    assert_eq!(stranger.acquisitions.load(Ordering::Acquire), 0);
    assert!(!*closed.borrow());
    assert_eq!(
        tests::handle(&worker, "owned-device", 1, Packet::PingReq)
            .await
            .unwrap(),
        vec![Packet::PingResp]
    );
    tests::stop(other, other_task).await;
    tests::stop(worker, task).await;
}

#[tokio::test]
async fn replaced_epoch_cannot_publish_or_accept_recipient_receipts() {
    let owner = Arc::new(Bridge::default());
    let (worker, task) = tests::worker(config(owner.clone())).await;
    let (_, closed) = tests::connect(&worker, "device", 1, false).await;
    tests::subscribe(&worker, "device", 1).await;
    let replacement = Bridge::with_data(owner.data.clone());
    let (other, other_task) = tests::worker(config(replacement)).await;
    assert!(tests::connect(&other, "device", 1, false).await.0);
    let count = owner.data.lock().unwrap().mutations.len();
    assert!(
        owner
            .local()
            .deliver_to("device".into(), receipt(), message(b"stale"))
            .await
            .is_err()
    );
    assert!(*closed.borrow());
    assert_eq!(owner.data.lock().unwrap().mutations.len(), count);
    assert!(
        tests::handle(
            &worker,
            "device",
            1,
            tests::publication(b"stale-source", 22)
        )
        .await
        .is_err()
    );
    assert_eq!(owner.data.lock().unwrap().mutations.len(), count);
    tests::stop(other, other_task).await;
    tests::stop(worker, task).await;
}

#[tokio::test]
async fn recovered_filter_cache_is_removed_after_unsubscribe() {
    let bridge = Arc::new(Bridge::default());
    let (worker, task) = tests::worker(config(bridge.clone())).await;
    tests::connect(&worker, "device", 1, false).await;
    tests::subscribe(&worker, "device", 1).await;
    assert!(tests::connect(&worker, "device", 2, false).await.0);
    assert_eq!(
        bridge.local().subscriptions().await.unwrap()[0].filters,
        vec!["a"]
    );
    tests::handle(
        &worker,
        "device",
        2,
        Packet::Unsubscribe {
            packet_id: 9,
            filters: vec!["a".into()],
        },
    )
    .await
    .unwrap();
    assert!(
        bridge.local().subscriptions().await.unwrap().is_empty(),
        "unsubscribed recovery filters remain cached"
    );
    tests::stop(worker, task).await;
}

#[derive(Debug, Default)]
struct Clock(std::sync::atomic::AtomicU64);
impl mqtt_session::durable::Clock for Clock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}
#[tokio::test]
async fn recovered_filter_cache_is_removed_when_session_expires() {
    let bridge = Arc::new(Bridge::default());
    let clock = Arc::new(Clock::default());
    let broker = Broker::start(config(bridge.clone()), clock.clone(), None)
        .await
        .unwrap();
    let (worker, task) = tests::wrap(broker);
    let (wake, _) = watch::channel(0);
    let (cancel, _) = watch::channel(false);
    let (close_reason, _) = watch::channel(None);
    let mut frame = Frame::v5(Packet::Connect {
        client_id: "device".into(),
        clean_session: true,
        keep_alive: 0,
        will: None,
        username: None,
        password: None,
    });
    frame.properties.push(Property::SessionExpiryInterval(1));
    assert!(matches!(
        worker
            .request(Operation::ConnectFrame {
                frame,
                generation: 1,
                wake,
                cancel,
                close_reason
            })
            .await
            .unwrap(),
        Reply::Connected(false)
    ));
    assert!(matches!(
        worker
            .request(Operation::HandleFrameAt {
                client: "device".into(),
                generation: 1,
                frame: Frame::v5(Packet::Subscribe {
                    packet_id: 1,
                    filters: vec![("a".into(), 1)]
                }),
                received_at_millis: 0
            })
            .await
            .unwrap(),
        Reply::Frames(_)
    ));
    worker
        .request(Operation::Disconnect {
            client: "device".into(),
            generation: 1,
            graceful: true,
        })
        .await
        .unwrap();
    let inspected = bridge.inspect_session("device".into()).await.unwrap();
    let lease = bridge
        .acquire_session("device".into(), 0, inspected.epoch)
        .await
        .unwrap();
    bridge.local().activate_session(lease).await.unwrap();
    assert_eq!(
        bridge.local().subscriptions().await.unwrap()[0].filters,
        vec!["a"]
    );
    clock.0.store(1001, Ordering::Release);
    worker.request(Operation::Tick).await.unwrap();
    assert!(
        bridge.local().subscriptions().await.unwrap().is_empty(),
        "expired recovery filters remain cached"
    );
    assert!(!tests::connect(&worker, "device", 2, false).await.0);
    assert!(
        bridge
            .data
            .lock()
            .unwrap()
            .mutations
            .last()
            .unwrap()
            .reset_session,
        "expired session reconnect creates a new logical generation"
    );
    tests::stop(worker, task).await;
}

#[tokio::test]
async fn authoritative_tombstone_deletes_a_preexisting_local_mirror_row() {
    let database = tests::Database::new();
    let stored = database.config();
    let (worker, task) = tests::worker(stored.clone()).await;
    tests::connect(&worker, "device", 1, false).await;
    tests::subscribe(&worker, "device", 1).await;
    tests::stop(worker, task).await;
    let limits = mqtt_storage::durable::RowLimits {
        max_row_bytes: stored.max_state_bytes,
        max_sessions: stored.max_sessions,
        max_retained: stored.max_retained,
        max_legacy_bytes: stored.max_state_bytes,
    };
    let mqtt_storage::durable::Contents::Rows(rows) =
        mqtt_storage::durable::read_contents(&database.0, limits).unwrap()
    else {
        panic!("expected rows")
    };
    assert_eq!(rows.sessions.len(), 1);
    let bridge = Arc::new(Bridge::default());
    let (worker, task) = tests::worker(BrokerConfig {
        cluster: Some(bridge.clone()),
        ..stored
    })
    .await;
    let lease = bridge.acquire_session("device".into(), 0, 0).await.unwrap();
    assert!(lease.snapshot.is_none());
    bridge.local().activate_session(lease).await.unwrap();
    assert!(!tests::connect(&worker, "device", 1, true).await.0);
    tests::stop(worker, task).await;
    let mqtt_storage::durable::Contents::Rows(rows) =
        mqtt_storage::durable::read_contents(&database.0, limits).unwrap()
    else {
        panic!("expected rows")
    };
    assert!(
        rows.sessions.is_empty(),
        "local mirror resurrects a session deleted by the authoritative tombstone"
    );
}

#[tokio::test]
async fn only_a_new_logical_session_resets_recipient_generation() {
    let bridge = Arc::new(Bridge::default());
    let (worker, task) = tests::worker(config(bridge.clone())).await;
    assert!(!tests::connect(&worker, "device", 1, false).await.0);
    assert!(
        bridge
            .data
            .lock()
            .unwrap()
            .mutations
            .last()
            .unwrap()
            .reset_session,
        "first logical session must reset"
    );
    tests::subscribe(&worker, "device", 1).await;
    assert!(
        !bridge
            .data
            .lock()
            .unwrap()
            .mutations
            .last()
            .unwrap()
            .reset_session,
        "subscription is not a new session"
    );
    assert!(tests::connect(&worker, "device", 2, false).await.0);
    assert!(
        !bridge
            .data
            .lock()
            .unwrap()
            .mutations
            .last()
            .unwrap()
            .reset_session,
        "ownership migration alone must preserve logical session"
    );
    assert!(!tests::connect(&worker, "device", 3, true).await.0);
    assert!(
        bridge
            .data
            .lock()
            .unwrap()
            .mutations
            .last()
            .unwrap()
            .reset_session,
        "Clean Start creates a new logical session"
    );
    tests::subscribe(&worker, "device", 3).await;
    bridge
        .local()
        .deliver_to("device".into(), receipt(), message(b"message"))
        .await
        .unwrap();
    assert!(
        !bridge
            .data
            .lock()
            .unwrap()
            .mutations
            .last()
            .unwrap()
            .reset_session,
        "delivery receipt cannot reset a logical session"
    );
    tests::stop(worker, task).await;
}

#[tokio::test]
async fn ping_keeps_replicated_revision_unchanged_for_both_versions() {
    for version in [ProtocolVersion::V311, ProtocolVersion::V5] {
        let bridge = Arc::new(Bridge::default());
        let (worker, task) = tests::worker(config(bridge.clone())).await;
        let (wake, _) = watch::channel(0);
        let (cancel, _) = watch::channel(false);
        let (close_reason, _) = watch::channel(None);
        let mut frame = Frame::v311(Packet::Connect {
            client_id: "ping".into(),
            clean_session: false,
            keep_alive: 30,
            will: None,
            username: None,
            password: None,
        });
        frame.version = version;
        if version == ProtocolVersion::V5 {
            frame.properties.push(Property::SessionExpiryInterval(60));
        }
        worker
            .request(Operation::ConnectFrame {
                frame,
                generation: 1,
                wake,
                cancel,
                close_reason,
            })
            .await
            .unwrap();
        let before = bridge.data.lock().unwrap().mutations.len();
        for _ in 0..3 {
            let mut frame = Frame::v311(Packet::PingReq);
            frame.version = version;
            let Reply::Frames(frames) = worker
                .request(Operation::HandleFrameAt {
                    client: "ping".into(),
                    generation: 1,
                    frame,
                    received_at_millis: 0,
                })
                .await
                .unwrap()
            else {
                panic!("unexpected response")
            };
            assert_eq!(frames.len(), 1);
            assert_eq!(frames[0].packet, Packet::PingResp);
            assert_eq!(frames[0].version, version);
        }
        assert_eq!(
            bridge.data.lock().unwrap().mutations.len(),
            before,
            "PING must not write a replicated snapshot or increase revision"
        );
        assert!(
            tests::handle(&worker, "ping", 2, Packet::PingReq)
                .await
                .is_err()
        );
        bridge.ready.store(false, Ordering::Release);
        assert!(
            tests::handle(&worker, "ping", 1, Packet::PingReq)
                .await
                .is_err()
        );
        tests::stop(worker, task).await;
    }
}
