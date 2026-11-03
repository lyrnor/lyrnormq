use super::*;
use openraft::{CommittedLeaderId, EntryPayload};
static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
fn directory() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "control-log-tests-{}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn entry(index: u64, node: u64) -> Entry<Types> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(Command::Register {
            node,
            incarnation: format!("node-{node}"),
            address: format!("http://127.0.0.1:{}", 10000 + node),
            replicas: None,
            memory: false,
            disk_id: None,
            expires_at: 1000,
            lease_ms: 3000,
        }),
    }
}
#[tokio::test]
async fn ordinary_apply_is_logged_without_full_state_blob() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    let first = entry(0, 1);
    prime(&store, vec![first.clone()], Some(first.log_id)).await?;
    store.apply([first]).await?;
    let db = Connection::open(dir.join("metadata.sqlite"))?;
    let saved: Option<Vec<u8>> = db
        .query_row("SELECT data FROM meta WHERE key='state'", [], |r| r.get(0))
        .optional()?;
    assert!(
        saved.is_none(),
        "ordinary apply must not rewrite full state"
    );
    drop(db);
    drop(store);
    let store = Store::open(&dir, "test", 1).await?;
    assert!(store.metadata().nodes.contains_key(&1));
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
#[tokio::test]
async fn history_returns_sparse_delta_and_heartbeat_wakes_waiters()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    let mut changed = store.changed.subscribe();
    store.apply([entry(0, 1)]).await?;
    changed.borrow_and_update();
    assert!(
        matches!(
            store.watch_updates(Some(0)).as_slice(),
            [MetadataUpdate::Delta {
                base: 0,
                revision: 1,
                ..
            }]
        ),
        "ordinary update must be sparse"
    );
    let heartbeat = Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
        payload: EntryPayload::Normal(Command::Heartbeat {
            node: 1,
            incarnation: "node-1".into(),
            applied: 1,
            expires_at: 2000,
            lease_ms: 3000,
        }),
    };
    store.apply([heartbeat]).await?;
    assert!(changed.has_changed()?);
    assert_eq!(store.metadata().revision, 1);
    drop(changed);
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[allow(clippy::result_large_err)] // Same error as the storage trait under test.
async fn prime(
    store: &Store,
    entries: Vec<Entry<Types>>,
    committed: Option<LogId<u64>>,
) -> Result<(), StorageError<u64>> {
    store
        .run(move |db| {
            let tx = db.transaction().map_err(db_err)?;
            for entry in entries {
                tx.execute(
                    "INSERT INTO logs(idx,data) VALUES(?1,?2)",
                    params![entry.log_id.index as i64, log::encode_entry(&entry)?],
                )
                .map_err(db_err)?;
            }
            if let Some(id) = committed {
                put(&tx, "committed", &Some(id))?;
            }
            tx.commit().map_err(db_err)
        })
        .await
}
#[tokio::test]
async fn committed_tail_replays_but_speculative_tail_does_not()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    let first = entry(0, 1);
    prime(&store, vec![first.clone()], Some(first.log_id)).await?;
    store.apply([first]).await?;
    prime(&store, vec![entry(1, 2)], None).await?;
    drop(store);
    let mut store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.metadata().nodes.len(), 1);
    store.save_committed(Some(entry(1, 2).log_id)).await?;
    drop(store);
    let store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.metadata().nodes.len(), 2);
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
#[tokio::test]
async fn missing_committed_log_and_unknown_format_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let store = Store::open(&dir, "test", 1).await?;
    prime(
        &store,
        vec![entry(0, 1), entry(2, 3)],
        Some(entry(2, 3).log_id),
    )
    .await?;
    drop(store);
    assert!(Store::open(&dir, "test", 1).await.is_err());
    let db = Connection::open(dir.join("metadata.sqlite"))?;
    put(&db, "control_log_format", &999u64)?;
    drop(db);
    assert!(Store::open(&dir, "test", 1).await.is_err());
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
#[tokio::test]
async fn checkpoint_then_committed_tail_survives_purge_and_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    let entries = vec![entry(0, 1), entry(1, 2)];
    prime(&store, entries.clone(), Some(entries[1].log_id)).await?;
    store.apply(entries).await?;
    let snapshot = store.build_snapshot().await?;
    assert_eq!(snapshot.meta.last_log_id, Some(entry(1, 2).log_id));
    let tail = entry(2, 3);
    prime(&store, vec![tail.clone()], Some(tail.log_id)).await?;
    store.apply([tail]).await?;
    store.purge(entry(1, 2).log_id).await?;
    drop(snapshot);
    drop(store);
    let mut store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.metadata().nodes.len(), 3);
    // Purging a committed suffix without a pre-existing checkpoint first writes one.
    store.purge(entry(2, 3).log_id).await?;
    drop(store);
    let mut store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.metadata().nodes.len(), 3);
    assert_eq!(
        store
            .get_current_snapshot()
            .await?
            .ok_or("missing snapshot")?
            .meta
            .last_log_id,
        Some(entry(2, 3).log_id)
    );
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
#[tokio::test]
async fn direct_sparse_apply_checkpoints_without_injecting_log_membership()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    store.apply([entry(7, 1)]).await?;
    assert!(store.try_get_log_entries(0..=7).await?.is_empty());
    drop(store);
    let store = Store::open(&dir, "test", 1).await?;
    assert!(store.metadata().nodes.contains_key(&1));
    assert_eq!(
        store.state.lock().unwrap().last_applied,
        Some(entry(7, 1).log_id)
    );
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
#[tokio::test]
async fn legacy_state_remains_checkpoint_while_new_log_tail_recovers()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    std::fs::create_dir_all(&dir)?;
    let db = Connection::open(dir.join("metadata.sqlite"))?;
    db.execute_batch("CREATE TABLE meta(key TEXT PRIMARY KEY,data BLOB NOT NULL); CREATE TABLE logs(idx INTEGER PRIMARY KEY,data BLOB NOT NULL)")?;
    let mut state = State::default();
    state.apply(entry(4, 1));
    put(&db, "identity", &("test", 1u64))?;
    put(&db, "state", &state)?;
    let original = encode(&state)?;
    drop(db);
    let mut store = Store::open(&dir, "test", 1).await?;
    let tail = entry(5, 2);
    prime(&store, vec![tail.clone()], Some(tail.log_id)).await?;
    store.apply([tail]).await?;
    let db = Connection::open(dir.join("metadata.sqlite"))?;
    assert_eq!(
        get::<State>(&db, "state")?
            .ok_or("missing legacy checkpoint")?
            .metadata
            .nodes
            .len(),
        1
    );
    assert_eq!(
        encode(&get::<State>(&db, "state")?.ok_or("missing state")?)?,
        original
    );
    drop(db);
    drop(store);
    let store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.metadata().nodes.len(), 2);
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
#[tokio::test]
async fn snapshot_install_is_durable_before_publication_and_following_tail()
-> Result<(), Box<dyn std::error::Error>> {
    let source = directory();
    let target = directory();
    let mut leader = Store::open(&source, "test", 1).await?;
    leader.apply([entry(0, 1)]).await?;
    let snapshot = leader.build_snapshot().await?;
    let mut follower = Store::open(&target, "test", 2).await?;
    follower
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await?;
    let tail = entry(1, 2);
    prime(&follower, vec![tail.clone()], Some(tail.log_id)).await?;
    follower.apply([tail]).await?;
    drop(follower);
    drop(leader);
    let follower = Store::open(&target, "test", 2).await?;
    assert_eq!(follower.metadata().nodes.len(), 2);
    drop(follower);
    std::fs::remove_dir_all(target)?;
    std::fs::remove_dir_all(source)?;
    Ok(())
}
#[tokio::test]
async fn history_is_bounded_and_batch_contiguous_with_snapshot_fallback()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    let entries = (0..65).map(|i| entry(i, i + 1)).collect::<Vec<_>>();
    prime(&store, entries.clone(), Some(entries[64].log_id)).await?;
    store.apply(entries).await?;
    assert_eq!(store.watch_updates(Some(0)).len(), 64);
    assert!(matches!(
        store.watch_updates(Some(64)).as_slice(),
        [MetadataUpdate::Delta {
            base: 64,
            revision: 65,
            ..
        }]
    ));
    let entries = (65..300).map(|i| entry(i, i + 1)).collect::<Vec<_>>();
    prime(&store, entries.clone(), Some(entries[234].log_id)).await?;
    store.apply(entries).await?;
    assert!(matches!(
        store.watch_updates(Some(0)).as_slice(),
        [MetadataUpdate::Snapshot(_)]
    ));
    assert!(store.history.lock().unwrap().updates_len_for_test() <= 256);
    assert!(matches!(
        store.watch_updates(Some(300)).as_slice(),
        [MetadataUpdate::Progress { revision: 300 }]
    ));
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
#[tokio::test]
async fn cancelled_apply_keeps_serialized_continuation_until_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let store = Store::open(&dir, "test", 1).await?;
    let inner = store.inner.clone();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let blocker = std::thread::spawn(move || {
        let _db = inner.db.lock().unwrap();
        let _ = entered_tx.send(());
        release_rx.recv().unwrap();
    });
    entered_rx.await?;
    let mut first = store.clone();
    let task = tokio::spawn(async move { first.apply([entry(0, 1)]).await });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while store.apply_gate.available_permits() != 0 {
            tokio::task::yield_now().await
        }
    })
    .await?;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        store.apply_gate.available_permits(),
        0,
        "caller cancellation must not release active serialized operation"
    );
    release_tx.send(())?;
    blocker.join().unwrap();
    let mut second = store.clone();
    second.apply([entry(1, 2)]).await?;
    assert_eq!(store.metadata().nodes.len(), 2);
    drop(second);
    drop(store);
    let store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.metadata().nodes.len(), 2);
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn new_snapshot_checksum_cannot_be_dropped_or_payload_changed()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    store.apply([entry(0, 1)]).await?;
    store
        .run(|db| {
            let mut saved: SavedSnapshot =
                get(db, "snapshot")?.ok_or_else(|| db_err("missing snapshot"))?;
            saved.digest = None;
            put(db, "snapshot", &saved)
        })
        .await?;
    assert!(store.get_current_snapshot().await.is_err());
    drop(store);
    assert!(Store::open(&dir, "test", 1).await.is_err());
    std::fs::remove_dir_all(dir)?;
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    store.apply([entry(0, 1)]).await?;
    store
        .run(|db| {
            let mut saved: SavedSnapshot =
                get(db, "snapshot")?.ok_or_else(|| db_err("missing snapshot"))?;
            let mut state: State = decode(&saved.data)?;
            state.metadata.revision += 1;
            saved.data = encode(&state)?;
            put(db, "snapshot", &saved)
        })
        .await?;
    assert!(store.get_current_snapshot().await.is_err());
    drop(store);
    assert!(Store::open(&dir, "test", 1).await.is_err());
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
#[tokio::test]
async fn old_unsigned_snapshot_loads_but_new_marker_forbids_legacy_downgrade()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    store.apply([entry(0, 1)]).await?;
    drop(store);
    let db = Connection::open(dir.join("metadata.sqlite"))?;
    let mut saved: serde_json::Value = serde_json::from_slice(
        &get::<serde_json::Value>(&db, "snapshot")?
            .ok_or("missing snapshot")?
            .to_string()
            .into_bytes(),
    )?;
    saved.as_object_mut().ok_or("object")?.remove("format");
    saved.as_object_mut().ok_or("object")?.remove("digest");
    put(&db, "snapshot", &saved)?;
    assert!(
        Store::open(&dir, "test", 1).await.is_err(),
        "new-record marker cannot downgrade into unsigned legacy"
    );
    db.execute("DELETE FROM meta WHERE key='snapshot_record_version'", [])?;
    drop(db);
    let store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.metadata().nodes.len(), 1);
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
#[tokio::test]
async fn snapshot_install_io_failure_rolls_back_record_marker_and_ram()
-> Result<(), Box<dyn std::error::Error>> {
    let left = directory();
    let right = directory();
    let mut source = Store::open(&left, "test", 1).await?;
    source.apply([entry(0, 1), entry(1, 2)]).await?;
    let snapshot = source.build_snapshot().await?;
    let mut target = Store::open(&right, "test", 2).await?;
    target.apply([entry(0, 1)]).await?;
    target.run(|db| {db.execute_batch("CREATE TRIGGER fail_snapshot_marker BEFORE UPDATE ON meta WHEN NEW.key='snapshot_record_version' BEGIN SELECT RAISE(ABORT,'injected checkpoint failure'); END;").map_err(db_err)}).await?;
    assert!(
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .is_err()
    );
    assert_eq!(target.metadata().nodes.len(), 1);
    assert_eq!(
        target
            .get_current_snapshot()
            .await?
            .ok_or("snapshot")?
            .meta
            .last_log_id,
        Some(entry(0, 1).log_id)
    );
    drop(target);
    drop(source);
    let target = Store::open(&right, "test", 2).await?;
    assert_eq!(target.metadata().nodes.len(), 1);
    drop(target);
    std::fs::remove_dir_all(left)?;
    std::fs::remove_dir_all(right)?;
    Ok(())
}

#[tokio::test]
async fn commit_watermark_cannot_regress_and_unsnapshotted_committed_tail_cannot_truncate()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    let entries = vec![entry(0, 1), entry(1, 2)];
    prime(&store, entries.clone(), Some(entries[1].log_id)).await?;
    store.apply(entries).await?;
    assert!(store.save_committed(None).await.is_err());
    assert!(
        store
            .save_committed(Some(entry(0, 1).log_id))
            .await
            .is_err()
    );
    assert!(store.truncate(entry(1, 2).log_id).await.is_err());
    drop(store);
    let store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.metadata().nodes.len(), 2);
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn direct_apply_cannot_ack_with_a_missing_higher_committed_tail()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    store.save_committed(Some(entry(3, 4).log_id)).await?;
    assert!(store.apply([entry(0, 1)]).await.is_err());
    assert!(store.metadata().nodes.is_empty());
    drop(store);
    assert!(Store::open(&dir, "test", 1).await.is_err());
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn older_or_out_of_order_apply_cannot_roll_ram_back_behind_durable_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    store.apply([entry(7, 1)]).await?;
    let before = encode(&*store.state.lock().unwrap())?;
    assert!(
        store.apply([entry(3, 2)]).await.is_err(),
        "older direct apply must not replace RAM with an undurable candidate"
    );
    assert_eq!(encode(&*store.state.lock().unwrap())?, before);
    assert!(
        store.apply([entry(9, 2), entry(8, 3)]).await.is_err(),
        "batch must be strictly ordered"
    );
    assert_eq!(encode(&*store.state.lock().unwrap())?, before);
    drop(store);
    let store = Store::open(&dir, "test", 1).await?;
    assert_eq!(encode(&*store.state.lock().unwrap())?, before);
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn valid_json_committed_log_payload_change_is_detected_before_replay()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    let first = entry(0, 1);
    prime(&store, vec![first.clone()], Some(first.log_id)).await?;
    store.apply([first]).await?;
    let tail = entry(1, 2);
    prime(&store, vec![tail.clone()], Some(tail.log_id)).await?;
    drop(store);
    let db = Connection::open(dir.join("metadata.sqlite"))?;
    let bytes: Vec<u8> = db.query_row("SELECT data FROM logs WHERE idx=1", [], |row| row.get(0))?;
    let mut value: serde_json::Value = serde_json::from_slice(&bytes)?;
    if value.get("entry").is_some() {
        value["entry"]["payload"]["Normal"]["Register"]["expires_at"] = serde_json::json!(9000);
    } else {
        value["payload"]["Normal"]["Register"]["expires_at"] = serde_json::json!(9000);
    }
    db.execute(
        "UPDATE logs SET data=?1 WHERE idx=1",
        [serde_json::to_vec(&value)?],
    )?;
    drop(db);
    assert!(
        Store::open(&dir, "test", 1).await.is_err(),
        "valid-JSON payload corruption must not silently change replayed state"
    );
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn legacy_raw_log_migration_is_atomic_and_never_replays_uncommitted_tail()
-> Result<(), Box<dyn std::error::Error>> {
    for format in [None, Some(1u64)] {
        let dir = directory();
        let store = Store::open(&dir, "test", 1).await?;
        drop(store);
        let db = Connection::open(dir.join("metadata.sqlite"))?;
        if let Some(format) = format {
            put(&db, "control_log_format", &format)?;
        } else {
            db.execute("DELETE FROM meta WHERE key='control_log_format'", [])?;
        }
        let first = entry(0, 1);
        let tail = entry(1, 2);
        db.execute("INSERT INTO logs(idx,data) VALUES(0,?1)", [encode(&first)?])?;
        db.execute("INSERT INTO logs(idx,data) VALUES(1,?1)", [encode(&tail)?])?;
        put(&db, "committed", &Some(first.log_id))?;
        drop(db);
        let mut store = Store::open(&dir, "test", 1).await?;
        assert_eq!(store.metadata().nodes.len(), 1);
        assert!(store.metadata().nodes.contains_key(&1));
        assert_eq!(store.get_log_state().await?.last_log_id, Some(tail.log_id));
        assert_eq!(store.try_get_log_entries(0..=1).await?.len(), 2);
        let db = Connection::open(dir.join("metadata.sqlite"))?;
        assert_eq!(get::<u64>(&db, "control_log_format")?, Some(2));
        let bytes: Vec<u8> = db.query_row("SELECT data FROM logs WHERE idx=0", [], |r| r.get(0))?;
        let stored: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(stored["format"], 2);
        assert_eq!(stored["digest"].as_str().ok_or("digest")?.len(), 64);
        assert_eq!(log::decode_entry(0, &bytes)?.log_id, first.log_id);
        drop(db);
        drop(store);
        std::fs::remove_dir_all(dir)?;
    }
    Ok(())
}
#[tokio::test]
async fn legacy_migration_failure_rolls_back_wrapped_rows_and_format_marker()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let store = Store::open(&dir, "test", 1).await?;
    drop(store);
    let db = Connection::open(dir.join("metadata.sqlite"))?;
    put(&db, "control_log_format", &1u64)?;
    let raw = encode(&entry(0, 1))?;
    db.execute("INSERT INTO logs(idx,data) VALUES(0,?1)", [&raw])?;
    db.execute(
        "INSERT INTO logs(idx,data) VALUES(1,?1)",
        [b"{}".as_slice()],
    )?;
    drop(db);
    assert!(Store::open(&dir, "test", 1).await.is_err());
    let db = Connection::open(dir.join("metadata.sqlite"))?;
    assert_eq!(get::<u64>(&db, "control_log_format")?, Some(1));
    let unchanged: Vec<u8> = db.query_row("SELECT data FROM logs WHERE idx=0", [], |r| r.get(0))?;
    assert_eq!(unchanged, raw);
    drop(db);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
#[tokio::test]
async fn all_new_log_read_paths_reject_missing_hash_unknown_version_or_bare_entry()
-> Result<(), Box<dyn std::error::Error>> {
    for kind in ["missing-hash", "unknown-version", "bare-entry"] {
        let dir = directory();
        let mut store = Store::open(&dir, "test", 1).await?;
        let first = entry(0, 1);
        prime(&store, vec![first.clone()], Some(first.log_id)).await?;
        store.apply([first]).await?;
        let tail = entry(1, 2);
        prime(&store, vec![tail.clone()], Some(tail.log_id)).await?;
        let db = Connection::open(dir.join("metadata.sqlite"))?;
        let bytes: Vec<u8> = db.query_row("SELECT data FROM logs WHERE idx=1", [], |r| r.get(0))?;
        let mut value: serde_json::Value = serde_json::from_slice(&bytes)?;
        match kind {
            "missing-hash" => {
                value.as_object_mut().ok_or("object")?.remove("digest");
            }
            "unknown-version" => value["format"] = serde_json::json!(999),
            _ => value = serde_json::to_value(&tail)?,
        }
        db.execute(
            "UPDATE logs SET data=?1 WHERE idx=1",
            [serde_json::to_vec(&value)?],
        )?;
        drop(db);
        assert!(store.get_log_state().await.is_err());
        assert!(store.try_get_log_entries(0..=1).await.is_err());
        assert!(store.apply([tail]).await.is_err());
        assert_eq!(store.metadata().nodes.len(), 1);
        drop(store);
        assert!(Store::open(&dir, "test", 1).await.is_err());
        std::fs::remove_dir_all(dir)?;
    }
    Ok(())
}
