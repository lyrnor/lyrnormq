use crate::{model::*, storage::Store};
use openraft::{
    Entry, EntryPayload, LogId, RaftSnapshotBuilder, Vote,
    storage::{RaftLogStorage, RaftStateMachine},
};
use std::io;
fn path() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "rmqtt-cluster-store-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}
fn register(node: u64) -> Command {
    Command::Register {
        node,
        incarnation: format!("node-{node}"),
        address: format!("http://127.0.0.1:{}", 10000 + node),
        replicas: None,
        expires_at: 100,
        lease_ms: 3000,
    }
}
#[test]
fn ownership_release_is_fenced_and_expiry_is_conditional() {
    let mut m = Metadata::default();
    assert!(m.apply(register(1)).error.is_none());
    assert!(m.apply(register(2)).error.is_none());
    for node in [1, 2] {
        assert!(
            m.apply(Command::Claim {
                node,
                incarnation: format!("node-{node}"),
                client: "device".into(),
                generation: node
            })
            .error
            .is_none()
        );
    }
    m.apply(Command::Release {
        node: 1,
        incarnation: "node-1".into(),
        client: "device".into(),
        generation: 1,
    });
    assert_eq!(m.clients["device"].node, 2);
    m.apply(Command::Expire {
        node: 2,
        incarnation: "node-2".into(),
        expires_at: 99,
    });
    assert!(m.nodes.contains_key(&2));
    m.apply(Command::Expire {
        node: 2,
        incarnation: "node-2".into(),
        expires_at: 100,
    });
    assert!(!m.clients.contains_key("device"));
}
#[tokio::test]
async fn durable_vote_commit_state_snapshot_and_exclusive_directory()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = path();
    let mut store = Store::open(&dir, "test", 1).await?;
    assert_eq!(
        Store::open(&dir, "test", 1)
            .await
            .err()
            .ok_or("missing lock rejection")?
            .kind(),
        io::ErrorKind::WouldBlock
    );
    let vote = Vote::new(7, 1);
    store.save_vote(&vote).await?;
    let id = LogId::new(openraft::CommittedLeaderId::new(7, 1), 3);
    store.save_committed(Some(id)).await?;
    let entry = Entry {
        log_id: id,
        payload: EntryPayload::Normal(register(1)),
    };
    store.apply([entry]).await?;
    let snapshot = store.build_snapshot().await?;
    assert_eq!(snapshot.meta.last_log_id, Some(id));
    drop(snapshot);
    drop(store);
    let mut store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.read_vote().await?, Some(vote));
    assert_eq!(store.read_committed().await?, Some(id));
    assert_eq!(store.applied_state().await?.0, Some(id));
    assert!(store.metadata().nodes.contains_key(&1));
    assert!(store.get_current_snapshot().await?.is_some());
    drop(store);
    assert!(Store::open(&dir, "wrong", 1).await.is_err());
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[test]
fn subscription_generation_and_delta_gap_are_fenced() {
    let mut m = Metadata::default();
    m.apply(register(1));
    let claim = Command::Claim {
        node: 1,
        incarnation: "node-1".into(),
        client: "device".into(),
        generation: 2,
    };
    m.apply(claim.clone());
    m.apply(Command::Filters {
        node: 1,
        incarnation: "node-1".into(),
        client: "device".into(),
        generation: 2,
        filters: vec!["devices/+".into()],
    });
    m.apply(claim);
    assert_eq!(m.clients["device"].filters, vec!["devices/+".to_owned()]);
    assert!(
        m.apply(Command::Filters {
            node: 1,
            incarnation: "node-1".into(),
            client: "device".into(),
            generation: 1,
            filters: vec![]
        })
        .error
        .is_some()
    );
    assert!(
        m.apply(Command::Claim {
            node: 1,
            incarnation: "node-1".into(),
            client: "device".into(),
            generation: 1
        })
        .error
        .is_some()
    );
    let old = m.clone();
    m.apply(register(2));
    let delta = MetadataUpdate::between(&old, &m);
    let merged = delta.clone().merge(&old).expect("merge");
    assert_eq!(merged.nodes, m.nodes);
    assert_eq!(merged.routes, m.routes);
    assert!(delta.merge(&m).is_err());
}

struct StoreBuilder;
struct StoreGuard(std::path::PathBuf);
impl Drop for StoreGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
impl openraft::testing::StoreBuilder<Types, Store, Store, StoreGuard> for StoreBuilder {
    async fn build(&self) -> Result<(StoreGuard, Store, Store), openraft::StorageError<u64>> {
        let dir = path();
        let store = Store::open(&dir, "conformance", 1)
            .await
            .map_err(|error| openraft::StorageIOError::write_state_machine(&error))?;
        Ok((StoreGuard(dir), store.clone(), store))
    }
}
#[test]
fn openraft_storage_conformance() -> Result<(), Box<dyn std::error::Error>> {
    openraft::testing::Suite::<Types, Store, Store, StoreBuilder, StoreGuard>::test_all(
        StoreBuilder,
    )?;
    Ok(())
}

#[test]
fn mixed_controller_leases_preserve_all_outstanding_grants() {
    let mut metadata = Metadata::default();
    metadata.apply(register(1));
    let long: Command=serde_json::from_value(serde_json::json!({"Heartbeat":{"node":1,"incarnation":"node-1","applied":0,"expires_at":200,"lease_ms":30000}})).expect("long grant");
    let short: Command=serde_json::from_value(serde_json::json!({"Heartbeat":{"node":1,"incarnation":"node-1","applied":0,"expires_at":201,"lease_ms":1000}})).expect("short grant");
    metadata.apply(long);
    metadata.apply(short);
    let node = serde_json::to_value(&metadata.nodes[&1]).expect("node");
    assert_eq!(node["max_lease_ms"], 30000);
}

#[test]
fn mixed_lease_election_waits_old_maximum_and_tracks_new_grants_monotonically() {
    let mut metadata = Metadata::default();
    metadata.apply(register(1));
    let long:Command=serde_json::from_value(serde_json::json!({"Heartbeat":{"node":1,"incarnation":"node-1","applied":0,"expires_at":200,"lease_ms":30000}})).expect("grant");
    metadata.apply(long);
    let start = tokio::time::Instant::now();
    let mut observed = crate::lease::LeaseObservation::new(&metadata.nodes[&1], start);
    assert!(!observed.expired(
        &metadata.nodes[&1],
        start + std::time::Duration::from_millis(1001)
    ));
    assert!(observed.expired(
        &metadata.nodes[&1],
        start + std::time::Duration::from_millis(30000)
    ));
    let short:Command=serde_json::from_value(serde_json::json!({"Heartbeat":{"node":1,"incarnation":"node-1","applied":0,"expires_at":201,"lease_ms":1000}})).expect("grant");
    metadata.apply(short);
    let short_at = start + std::time::Duration::from_millis(31000);
    assert!(!observed.expired(&metadata.nodes[&1], short_at));
    assert!(!observed.expired(
        &metadata.nodes[&1],
        short_at + std::time::Duration::from_millis(1001)
    ));
    assert!(observed.expired(
        &metadata.nodes[&1],
        short_at + std::time::Duration::from_millis(30000)
    ));
    let legacy:Node=serde_json::from_value(serde_json::json!({"id":1,"incarnation":"legacy","address":"http://127.0.0.1:1","expires_at":10,"applied":0})).expect("legacy node");
    assert_eq!(legacy.max_lease_ms, 30000);
}

#[test]
fn durable_directory_preserves_interest_and_rejects_mixed_membership() {
    let members = vec![
        (1, "http://127.0.0.1:10001".into()),
        (2, "http://127.0.0.1:10002".into()),
        (3, "http://127.0.0.1:10003".into()),
    ];
    let mut metadata = Metadata::default();
    let mut first = register(1);
    if let Command::Register { replicas, .. } = &mut first {
        *replicas = Some(members.clone());
    }
    assert!(metadata.apply(first).error.is_none());
    assert!(metadata.apply(register(2)).error.is_some());
    let mut wrong = register(2);
    if let Command::Register { replicas, .. } = &mut wrong {
        let mut peers = members.clone();
        peers[0].1 = "http://127.0.0.1:10999".into();
        *replicas = Some(peers);
    }
    assert!(metadata.apply(wrong).error.is_some());
    assert!(
        metadata
            .apply(Command::Acquire {
                key: "c:device".into(),
                node: 1,
                incarnation: "node-1".into(),
                generation: 1,
                expected_epoch: 0
            })
            .error
            .is_none()
    );
    let epoch = metadata.durable["c:device"].epoch;
    assert!(
        metadata
            .apply(Command::Acquire {
                key: "c:device".into(),
                node: 1,
                incarnation: "node-1".into(),
                generation: 2,
                expected_epoch: 0
            })
            .error
            .is_some()
    );
    assert!(
        metadata
            .apply(Command::DurableFilters {
                key: "c:device".into(),
                node: 1,
                incarnation: "node-1".into(),
                epoch,
                filters: vec!["events/+".into()]
            })
            .error
            .is_none()
    );
    metadata.apply(Command::Expire {
        node: 1,
        incarnation: "node-1".into(),
        expires_at: 100,
    });
    assert_eq!(
        metadata.durable["c:device"].filters,
        vec!["events/+".to_owned()]
    );
    assert!(metadata.routes.contains_key("events/+"));
    let legacy: Metadata = serde_json::from_value(
        serde_json::json!({"revision":0,"nodes":{},"clients":{},"routes":{}}),
    )
    .expect("old metadata");
    assert_eq!(legacy.data_mode, Some(DataMode::Legacy));
}
