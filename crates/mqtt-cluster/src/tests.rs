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
        expires_at: 100,
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
