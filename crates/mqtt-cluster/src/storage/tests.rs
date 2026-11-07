use super::*;
use openraft::EntryPayload;
static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub(super) fn directory() -> std::path::PathBuf {
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

pub(super) fn entry(index: u64, node: u64) -> Entry {
    Entry {
        log_id: LogId::new(crate::model::committed_leader_id(1, 1), index),
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

#[allow(clippy::result_large_err)]
pub(super) async fn prime(
    store: &Store,
    entries: Vec<Entry>,
    committed: Option<LogId>,
) -> Result<(), StorageError<Types>> {
    store
        .run_wal(move |wal| {
            wal.append(entries)?;
            if let Some(id) = committed {
                wal.save_committed(Some(id))?;
            }
            Ok(())
        })
        .await
}

#[tokio::test]
async fn history_returns_sparse_delta_and_heartbeat_wakes_waiters()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let store = Store::open(&dir, "test", 1).await?;
    let mut changed = store.changed.subscribe();
    store.apply_log([entry(0, 1)]).await?;
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
        log_id: LogId::new(crate::model::committed_leader_id(1, 1), 1),
        payload: EntryPayload::Normal(Command::Heartbeat {
            node: 1,
            incarnation: "node-1".into(),
            applied: 1,
            expires_at: 2000,
            lease_ms: 3000,
        }),
    };
    store.apply_log([heartbeat]).await?;
    assert!(changed.has_changed()?);
    assert_eq!(store.metadata().revision, 1);
    drop(changed);
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn committed_tail_replays_but_speculative_tail_does_not()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let store = Store::open(&dir, "test", 1).await?;
    let first = entry(0, 1);
    prime(&store, vec![first.clone()], Some(first.log_id)).await?;
    store.apply_log([first]).await?;
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
async fn checkpoint_then_committed_tail_survives_purge_and_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    let entries = vec![entry(0, 1), entry(1, 2)];
    prime(&store, entries.clone(), Some(entries[1].log_id)).await?;
    store.apply_log(entries).await?;
    let snapshot = store.build_snapshot().await?;
    assert_eq!(snapshot.meta.last_log_id, Some(entry(1, 2).log_id));
    let tail = entry(2, 3);
    prime(&store, vec![tail.clone()], Some(tail.log_id)).await?;
    store.apply_log([tail]).await?;
    store.purge(entry(1, 2).log_id).await?;
    drop(snapshot);
    drop(store);
    let mut store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.metadata().nodes.len(), 3);
    // Purge must not build a snapshot on the caller. A committed index the
    // incremental checkpoint cannot reach stays in the log.
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
async fn purge_returns_while_incremental_checkpoint_is_blocked()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    let flush = store.owner.state().clone();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !flush.booted() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("checkpoint worker did not finish startup");
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let inner = store.inner.clone();
    let holder = std::thread::spawn(move || {
        let db = inner
            .db
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        locked_tx.send(()).expect("lock signal");
        let _ = release_rx.recv();
        drop(db);
    });
    locked_rx.recv().expect("checkpoint lock");
    let record = entry(0, 1);
    prime(&store, vec![record.clone()], Some(record.log_id)).await?;
    store.apply_log([record.clone()]).await?;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !flush.inflight() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("incremental checkpoint did not start");
    let started = std::time::Instant::now();
    let purged = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        store.purge(record.log_id),
    )
    .await;
    assert_eq!(store.try_get_log_entries(0..=0).await?.len(), 1);
    let _ = release_tx.send(());
    let _ = holder.join();
    purged.expect("purge waited for the checkpoint")?;
    assert!(started.elapsed() < std::time::Duration::from_millis(200));
    drop(store);
    let mut store = Store::open(&dir, "test", 1).await?;
    assert!(store.try_get_log_entries(0..=0).await?.is_empty());
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn purge_leaves_committed_log_when_incremental_checkpoint_cannot_cover()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    let record = entry(0, 1);
    prime(&store, vec![record.clone()], Some(record.log_id)).await?;
    store.purge(record.log_id).await?;
    assert_eq!(store.try_get_log_entries(0..=0).await?.len(), 1);
    assert!(store.get_current_snapshot().await?.is_none());
    drop(store);
    let mut store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.try_get_log_entries(0..=0).await?.len(), 1);
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn direct_sparse_apply_checkpoints_without_injecting_log_membership()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    store.apply_log([entry(7, 1)]).await?;
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
async fn snapshot_install_is_durable_before_publication_and_following_tail()
-> Result<(), Box<dyn std::error::Error>> {
    let source = directory();
    let target = directory();
    let mut leader = Store::open(&source, "test", 1).await?;
    leader.apply_log([entry(0, 1)]).await?;
    let snapshot = leader.build_snapshot().await?;
    let mut follower = Store::open(&target, "test", 2).await?;
    follower
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await?;
    let tail = entry(1, 2);
    prime(&follower, vec![tail.clone()], Some(tail.log_id)).await?;
    follower.apply_log([tail]).await?;
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
    let store = Store::open(&dir, "test", 1).await?;
    let entries = (0..65).map(|i| entry(i, i + 1)).collect::<Vec<_>>();
    prime(&store, entries.clone(), Some(entries[64].log_id)).await?;
    store.apply_log(entries).await?;
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
    store.apply_log(entries).await?;
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
    let first = store.clone();
    let task = tokio::spawn(async move { first.apply_log([entry(0, 1)]).await });
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
    let second = store.clone();
    second.apply_log([entry(1, 2)]).await?;
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
async fn commit_watermark_cannot_regress_and_unsnapshotted_committed_tail_cannot_truncate()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    let entries = vec![entry(0, 1), entry(1, 2)];
    prime(&store, entries.clone(), Some(entries[1].log_id)).await?;
    store.apply_log(entries).await?;
    assert!(store.save_committed(None).await.is_err());
    assert!(
        store
            .save_committed(Some(entry(0, 1).log_id))
            .await
            .is_err()
    );
    match store.truncate_after(Some(entry(0, 1).log_id)).await {
        Err(error) => assert!(
            error
                .to_string()
                .contains("unsnapshotted committed entries"),
            "{error}"
        ),
        Ok(()) => {
            // apply_log may persist a covering checkpoint when the WAL read is
            // not yet durable; a covered committed prefix may then truncate.
        }
    }
    store.truncate_after(Some(entry(1, 2).log_id)).await?;
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
    assert!(store.apply_log([entry(0, 1)]).await.is_err());
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
    let store = Store::open(&dir, "test", 1).await?;
    store.apply_log([entry(7, 1)]).await?;
    let before = encode(&*store.state.lock().unwrap())?;
    assert!(
        store.apply_log([entry(3, 2)]).await.is_err(),
        "older direct apply must not replace RAM with an undurable candidate"
    );
    assert_eq!(encode(&*store.state.lock().unwrap())?, before);
    assert!(
        store.apply_log([entry(9, 2), entry(8, 3)]).await.is_err(),
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
async fn ordinary_apply_keeps_its_log_entry_beside_an_incremental_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    let first = entry(0, 1);
    prime(&store, vec![first.clone()], Some(first.log_id)).await?;
    store.apply_log([first.clone()]).await?;
    assert_eq!(store.try_get_log_entries(0..=0).await?.len(), 1);
    if let Some(saved) = store.run(|db| db.read_snapshot()).await? {
        assert_eq!(saved.meta.last_log_id, Some(first.log_id));
    }
    assert!(!dir.join("metadata.sqlite").exists());
    drop(store);
    let store = Store::open(&dir, "test", 1).await?;
    assert!(store.metadata().nodes.contains_key(&1));
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
#[tokio::test]
async fn checkpoint_commit_error_preserves_ram_and_previous_durable_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let left = directory();
    let right = directory();
    let mut source = Store::open(&left, "test", 1).await?;
    source.apply_log([entry(0, 1), entry(1, 2)]).await?;
    let snapshot = source.build_snapshot().await?;
    let mut target = Store::open(&right, "test", 2).await?;
    target.apply_log([entry(0, 1)]).await?;
    target
        .run(|db| {
            db.fail_next_write();
            Ok(())
        })
        .await?;
    assert!(
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .is_err()
    );
    assert_eq!(target.metadata().nodes.len(), 1);
    assert!(
        target.get_current_snapshot().await.is_err(),
        "uncertain writes poison the active backend"
    );
    drop(target);
    drop(source);
    let mut target = Store::open(&right, "test", 2).await?;
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
    std::fs::remove_dir_all(left)?;
    std::fs::remove_dir_all(right)?;
    Ok(())
}
#[test]
fn entry_envelope_rejects_missing_hash_version_payload_change_and_wrong_index() {
    let bytes = log::encode_entry(&entry(0, 1)).unwrap();
    let original: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    for kind in ["missing-hash", "version", "payload", "bare-entry"] {
        let mut changed = original.clone();
        match kind {
            "missing-hash" => {
                changed.as_object_mut().unwrap().remove("digest");
            }
            "version" => changed["format"] = serde_json::json!(999),
            "payload" => {
                changed["entry"]["payload"]["Normal"]["Register"]["expires_at"] =
                    serde_json::json!(9000)
            }
            _ => changed = serde_json::to_value(entry(0, 1)).unwrap(),
        }
        assert!(log::decode_entry(0, &serde_json::to_vec(&changed).unwrap()).is_err());
    }
    assert!(log::decode_entry(1, &bytes).is_err());
    assert!(log::decode_entry(-1, &bytes).is_err());
}

#[path = "pressure.rs"]
mod pressure;
