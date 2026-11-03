//! Integration gates for the authoritative WAL and inert legacy SQLite source.
use super::tests::{directory, entry};
use super::*;

#[tokio::test]
async fn raft_mutations_do_not_write_sqlite_log_or_watermark_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    store.run(|db| {
        db.execute_batch("CREATE TABLE logs(idx INTEGER PRIMARY KEY,data BLOB NOT NULL); CREATE TRIGGER forbid_raft_log_insert BEFORE INSERT ON logs BEGIN SELECT RAISE(ABORT,'legacy Raft writes forbidden'); END; CREATE TRIGGER forbid_raft_meta_insert BEFORE INSERT ON meta WHEN NEW.key IN ('vote','committed','purged') BEGIN SELECT RAISE(ABORT,'legacy Raft writes forbidden'); END;").map_err(db_err)
    }).await?;
    store.save_vote(&Vote::new(3, 1)).await?;
    super::tests::prime(&store, vec![entry(0, 1)], Some(entry(0, 1).log_id)).await?;
    store.apply([entry(0, 1)]).await?;
    store.build_snapshot().await?;
    store.purge(entry(0, 1).log_id).await?;
    assert_eq!(store.read_vote().await?, Some(Vote::new(3, 1)));
    drop(store);
    let mut store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.read_vote().await?, Some(Vote::new(3, 1)));
    assert_eq!(store.read_committed().await?, Some(entry(0, 1).log_id));
    assert_eq!(
        store.get_log_state().await?.last_purged_log_id,
        Some(entry(0, 1).log_id)
    );
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn completed_migration_marker_forbids_missing_wal_fallback()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let store = Store::open(&dir, "test", 1).await?;
    drop(store);
    let db = Connection::open(dir.join("metadata.sqlite"))?;
    assert_eq!(get::<u64>(&db, "raft_wal_migration")?, Some(1));
    drop(db);
    std::fs::remove_dir_all(dir.join("raft-wal"))?;
    assert!(Store::open(&dir, "test", 1).await.is_err());
    assert!(
        !dir.join("raft-wal").exists(),
        "missing durable WAL must not be silently recreated"
    );
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn published_import_before_marker_reopens_only_if_source_matches()
-> Result<(), Box<dyn std::error::Error>> {
    for conflict in [false, true] {
        let dir = directory();
        let db = super::tests::legacy_database(&dir)?;
        let first = entry(0, 1);
        db.execute(
            "INSERT INTO logs(idx,data) VALUES(0,?1)",
            [log::encode_entry(&first)?],
        )?;
        put(&db, "committed", &Some(first.log_id))?;
        db.execute_batch("CREATE TRIGGER fail_migration_marker BEFORE INSERT ON meta WHEN NEW.key='raft_wal_migration' BEGIN SELECT RAISE(ABORT,'injected crash before marker'); END;")?;
        drop(db);
        assert!(Store::open(&dir, "test", 1).await.is_err());
        assert!(
            dir.join("raft-wal").is_dir(),
            "complete import was already atomically published"
        );
        let db = Connection::open(dir.join("metadata.sqlite"))?;
        assert_eq!(get::<u64>(&db, "raft_wal_migration")?, None);
        db.execute_batch("DROP TRIGGER fail_migration_marker;")?;
        if conflict {
            db.execute(
                "UPDATE logs SET data=?1 WHERE idx=0",
                [log::encode_entry(&entry(0, 2))?],
            )?;
        }
        drop(db);
        let reopened = Store::open(&dir, "test", 1).await;
        if conflict {
            assert!(
                reopened.is_err(),
                "published WAL must never be replaced from conflicting SQL"
            );
        } else {
            let store = reopened?;
            assert_eq!(store.metadata().nodes.len(), 1);
            drop(store);
            let db = Connection::open(dir.join("metadata.sqlite"))?;
            assert_eq!(get::<u64>(&db, "raft_wal_migration")?, Some(1));
        }
        std::fs::remove_dir_all(dir)?;
    }
    Ok(())
}

#[tokio::test]
async fn completed_migration_ignores_stale_or_corrupt_sqlite_raft_backup()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    super::tests::prime(&store, vec![entry(0, 1)], Some(entry(0, 1).log_id)).await?;
    store.apply([entry(0, 1)]).await?;
    drop(store);
    let db = Connection::open(dir.join("metadata.sqlite"))?;
    db.execute_batch("CREATE TABLE logs(idx INTEGER PRIMARY KEY,data BLOB NOT NULL)")?;
    db.execute(
        "INSERT INTO logs(idx,data) VALUES(0,?1)",
        [b"corrupt inert backup".as_slice()],
    )?;
    put(&db, "committed", &Some(entry(100, 100).log_id))?;
    put(&db, "vote", &Vote::new(100, 100))?;
    put(&db, "purged", &entry(100, 100).log_id)?;
    put(&db, "control_log_format", &999u64)?;
    drop(db);
    let mut store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.metadata().nodes.len(), 1);
    assert_eq!(store.read_committed().await?, Some(entry(0, 1).log_id));
    assert_eq!(store.read_vote().await?, None);
    assert_eq!(store.get_log_state().await?.last_purged_log_id, None);
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn incomplete_staging_is_retryable_only_after_legacy_source_validation()
-> Result<(), Box<dyn std::error::Error>> {
    for corrupt in [false, true] {
        let dir = directory();
        let db = super::tests::legacy_database(&dir)?;
        let bytes = if corrupt {
            b"invalid".to_vec()
        } else {
            log::encode_entry(&entry(0, 1))?
        };
        db.execute("INSERT INTO logs(idx,data) VALUES(0,?1)", [bytes])?;
        put(&db, "committed", &Some(entry(0, 1).log_id))?;
        drop(db);
        let staging = dir.join("raft-wal.staging");
        std::fs::create_dir(&staging)?;
        std::fs::write(staging.join("partial"), b"torn import")?;
        let opened = Store::open(&dir, "test", 1).await;
        if corrupt {
            assert!(opened.is_err());
            assert!(
                staging.join("partial").exists(),
                "invalid source cannot replace even staging output"
            );
        } else {
            let store = opened?;
            assert_eq!(store.metadata().nodes.len(), 1);
            drop(store);
            assert!(!staging.exists());
        }
        std::fs::remove_dir_all(dir)?;
    }
    Ok(())
}

#[tokio::test]
async fn checkpoint_committed_before_wal_floor_recovers_after_crash()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let store = Store::open(&dir, "test", 1).await?;
    // Emulate a crash precisely after the SQLite install transaction committed
    // and before the independent WAL committed-floor update.
    store
        .run(|db| {
            let mut next = State::default();
            next.apply(entry(7, 1));
            recovery::put_snapshot(db, &make_snapshot(&next)?)
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

#[tokio::test]
async fn checkpoint_only_legacy_import_retries_marker_after_effective_commit_floor()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let db = super::tests::legacy_database(&dir)?;
    let mut next = State::default();
    next.apply(entry(7, 1));
    recovery::put_snapshot(&db, &make_snapshot(&next)?)?;
    db.execute_batch("CREATE TRIGGER fail_migration_marker BEFORE INSERT ON meta WHEN NEW.key='raft_wal_migration' BEGIN SELECT RAISE(ABORT,'injected crash'); END;")?;
    drop(db);
    assert!(Store::open(&dir, "test", 1).await.is_err());
    let db = Connection::open(dir.join("metadata.sqlite"))?;
    assert_eq!(
        get::<Option<LogId<u64>>>(&db, "committed")?.flatten(),
        Some(entry(7, 1).log_id)
    );
    db.execute_batch("DROP TRIGGER fail_migration_marker")?;
    drop(db);
    let mut store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.read_committed().await?, Some(entry(7, 1).log_id));
    assert!(store.try_get_log_entries(..).await?.is_empty());
    assert_eq!(store.metadata().nodes.len(), 1);
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn migrated_purged_prefix_vote_and_speculative_tail_keep_exact_identities()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let db = super::tests::legacy_database(&dir)?;
    let mut next = State::default();
    for index in 0..=4 {
        next.apply(entry(index, index + 1));
    }
    recovery::put_snapshot(&db, &make_snapshot(&next)?)?;
    put(&db, "purged", &entry(4, 5).log_id)?;
    put(&db, "vote", &Vote::new_committed(8, 2))?;
    put(&db, "committed", &Some(entry(5, 6).log_id))?;
    for item in [entry(5, 6), entry(6, 7)] {
        db.execute(
            "INSERT INTO logs(idx,data) VALUES(?1,?2)",
            params![item.log_id.index as i64, log::encode_entry(&item)?],
        )?;
    }
    drop(db);
    let mut store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.metadata().nodes.len(), 6);
    assert!(!store.metadata().nodes.contains_key(&7));
    assert_eq!(store.read_vote().await?, Some(Vote::new_committed(8, 2)));
    assert_eq!(
        store.get_log_state().await?.last_purged_log_id,
        Some(entry(4, 5).log_id)
    );
    assert_eq!(
        store.get_log_state().await?.last_log_id,
        Some(entry(6, 7).log_id)
    );
    assert_eq!(store.try_get_log_entries(..).await?.len(), 2);
    drop(store);
    let store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.metadata().nodes.len(), 6);
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[test]
fn legacy_purged_prefix_requires_verified_checkpoint_coverage()
-> Result<(), Box<dyn std::error::Error>> {
    for kind in ["missing", "behind", "conflicting-id"] {
        let dir = directory();
        let mut db = super::tests::legacy_database(&dir)?;
        put(&db, "purged", &entry(4, 1).log_id)?;
        if kind != "missing" {
            let mut checkpoint = State::default();
            let mut item = entry(if kind == "behind" { 3 } else { 4 }, 1);
            if kind == "conflicting-id" {
                item.log_id.leader_id = openraft::CommittedLeaderId::new(2, 1);
            }
            checkpoint.apply(item);
            recovery::put_snapshot(&db, &make_snapshot(&checkpoint)?)?;
        }
        assert!(
            recovery::recover_legacy(&mut db).is_err(),
            "unrecoverable purged source: {kind}"
        );
        assert!(!dir.join("raft-wal").exists());
        drop(db);
        std::fs::remove_dir_all(dir)?;
    }
    Ok(())
}

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
