//! Stable WAL/checkpoint ordering and complete shutdown regression gates.
use super::tests::{directory, entry};
use super::*;
#[allow(clippy::result_large_err)]
async fn append_through_store(
    store: &mut Store,
    entries: Vec<Entry<Types>>,
) -> Result<(), StorageError<u64>> {
    let inner = store.inner.clone();
    store
        .run_wal(move |wal| log::append_checked(wal, &inner.db, entries))
        .await
}

#[tokio::test]
async fn repeated_prefix_is_idempotent_but_committed_conflict_requires_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    let first = vec![entry(0, 1), entry(1, 2)];
    append_through_store(&mut store, first.clone()).await?;
    store.save_committed(Some(first[1].log_id)).await?;
    append_through_store(&mut store, first.clone()).await?;
    let mut replacement = entry(1, 3);
    replacement.log_id.leader_id = openraft::CommittedLeaderId::new(2, 1);
    assert!(
        append_through_store(&mut store, vec![replacement.clone()])
            .await
            .is_err()
    );
    assert_eq!(
        encode(&store.try_get_log_entries(..).await?)?,
        encode(&first)?
    );
    store.apply(first).await?;
    store.build_snapshot().await?;
    append_through_store(&mut store, vec![replacement.clone()]).await?;
    assert_eq!(
        encode(&store.try_get_log_entries(1..=1).await?)?,
        encode(&vec![replacement])?
    );
    drop(store);
    let store = Store::open(&dir, "test", 1).await?;
    assert_eq!(
        store.metadata().nodes.len(),
        2,
        "checkpoint remains business-state authority"
    );
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[test]
fn final_store_drop_defers_all_io_and_retains_directory_lock_until_cleanup()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()?;
    let dir = directory();
    runtime.block_on(async {
        let store = Store::open(&dir, "test", 1).await?;
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocked = tokio::task::spawn_blocking(move || {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        entered_rx.await?;
        drop(store);
        let probe = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.join("controller.lock"))?;
        let retained = probe.try_lock().is_err();
        drop(probe);
        release_tx.send(())?;
        blocked.await?;
        assert!(
            retained,
            "async final drop must defer closing and retain directory ownership while cleanup waits"
        );
        let store = Store::open(&dir, "test", 1).await?;
        drop(store);
        // Opening again also waits for the preceding cleanup rather than racing it.
        let store = Store::open(&dir, "test", 1).await?;
        drop(store);
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    drop(runtime); // Runtime shutdown joins the one pending blocking cleanup.
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn checkpoint_committed_before_wal_floor_recovers_after_crash()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let store = Store::open(&dir, "test", 1).await?;
    store
        .run(|db| {
            let mut next = State::default();
            next.apply(entry(7, 1));
            db.write_snapshot(&make_snapshot(&next)?)
        })
        .await?;
    assert_eq!(store.run_wal(|wal| wal.committed()).await?, None);
    drop(store);
    let mut store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.metadata().nodes.len(), 1);
    assert_eq!(store.read_committed().await?, Some(entry(7, 1).log_id));
    assert!(store.try_get_log_entries(..).await?.is_empty());
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
