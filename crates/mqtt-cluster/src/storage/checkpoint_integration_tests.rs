//! Controller checkpoint layout, exchange migration and durable ownership gates.
use super::tests::{directory, entry, prime};
use super::*;

#[tokio::test]
async fn fresh_controller_uses_bbolt_without_creating_sqlite()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    prime(&store, vec![entry(0, 1)], Some(entry(0, 1).log_id)).await?;
    store.apply_log([entry(0, 1)]).await?;
    store.build_snapshot().await?;
    assert!(dir.join("controller.bbolt").is_file());
    assert!(dir.join("controller.bbolt.required").is_file());
    assert!(!dir.join("metadata.sqlite").exists());
    assert!(!dir.join("metadata.sqlite-wal").exists());
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
#[tokio::test]
async fn legacy_sqlite_requires_explicit_export_instead_of_runtime_migration()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    std::fs::create_dir_all(&dir)?;
    std::fs::write(
        dir.join("metadata.sqlite"),
        b"legacy state must remain untouched",
    )?;
    assert!(Store::open(&dir, "test", 1).await.is_err());
    assert_eq!(
        std::fs::read(dir.join("metadata.sqlite"))?,
        b"legacy state must remain untouched"
    );
    assert!(!dir.join("controller.bbolt").exists());
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

fn exchange(
    state: Option<&State>,
    snapshot: Option<&SavedSnapshot>,
    version: Option<u64>,
) -> serde_json::Value {
    serde_json::json!({"format":1,"identity":["test",1],"state":state,"snapshot":snapshot,"snapshot_record_version":version})
}
fn seed_exchange(
    dir: &Path,
    value: &serde_json::Value,
    entries: Vec<Entry>,
    committed: Option<LogId>,
    purged: Option<LogId>,
) -> io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let mut wal = wal::Wal::open(&dir.join("raft-wal"), "test", 1)?;
    if let Some(purged) = purged {
        wal.purge(purged)?;
    }
    wal.append(entries)?;
    wal.save_committed(committed)?;
    drop(wal);
    std::fs::write(dir.join("metadata.sqlite"), b"immutable legacy source")?;
    std::fs::write(
        dir.join("controller-checkpoint.export.json"),
        encode(value)?,
    )
}
fn state(index: u64) -> State {
    let mut state = State::default();
    for index in 0..=index {
        state.apply(entry(index, index + 1));
    }
    state
}
#[tokio::test]
async fn explicit_export_import_preserves_source_and_replays_only_committed_tail()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let source = state(0);
    let mut saved = make_snapshot(&source)?;
    saved.data = serde_json::to_vec_pretty(&source)?;
    saved.digest = Some(recovery::snapshot_digest(&saved.meta, &saved.data)?);
    let value = exchange(Some(&source), Some(&saved), Some(1));
    seed_exchange(
        &dir,
        &value,
        vec![entry(1, 2), entry(2, 3)],
        Some(entry(1, 2).log_id),
        Some(entry(0, 1).log_id),
    )?;
    let original = std::fs::read(dir.join("metadata.sqlite"))?;
    let original_export = std::fs::read(dir.join("controller-checkpoint.export.json"))?;
    let mut store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.metadata().nodes.len(), 2);
    assert!(!store.metadata().nodes.contains_key(&3));
    // Recovery applies only the committed tail. The flush then persists that
    // recovered state; the speculative entry after the commit stays out.
    let snapshot = store.build_snapshot().await?;
    assert_eq!(snapshot.meta.last_log_id, Some(entry(1, 2).log_id));
    assert!(dir.join("controller.bbolt.required").is_file());
    assert!(!dir.join("controller-checkpoint.staging").exists());
    drop(store);
    let store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.metadata().nodes.len(), 2);
    assert_eq!(std::fs::read(dir.join("metadata.sqlite"))?, original);
    assert_eq!(
        std::fs::read(dir.join("controller-checkpoint.export.json"))?,
        original_export
    );
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
#[tokio::test]
async fn invalid_export_is_rejected_before_replacing_interrupted_staging()
-> Result<(), Box<dyn std::error::Error>> {
    for kind in [
        "format",
        "identity",
        "checksum",
        "snapshot-version",
        "checkpoint-conflict",
    ] {
        let dir = directory();
        let source = state(0);
        let snapshot = make_snapshot(&source)?;
        let mut value = exchange(Some(&source), Some(&snapshot), Some(1));
        match kind {
            "format" => value["format"] = serde_json::json!(999),
            "identity" => value["identity"] = serde_json::json!(["other", 1]),
            "checksum" => value["snapshot"]["digest"] = serde_json::json!("bad"),
            "snapshot-version" => value["snapshot_record_version"] = serde_json::json!(2),
            _ => value["state"]["metadata"]["revision"] = serde_json::json!(9),
        }
        seed_exchange(&dir, &value, vec![], None, None)?;
        let staging = dir.join("controller-checkpoint.staging");
        std::fs::create_dir(&staging)?;
        std::fs::write(staging.join("partial"), b"preserve invalid-input evidence")?;
        assert!(
            Store::open(&dir, "test", 1).await.is_err(),
            "invalid export {kind}"
        );
        assert!(staging.join("partial").is_file());
        assert!(!dir.join("controller.bbolt").exists());
        std::fs::remove_dir_all(dir)?;
    }
    Ok(())
}
#[tokio::test]
async fn incomplete_staging_retries_from_valid_export_without_touching_source()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let source = state(0);
    seed_exchange(
        &dir,
        &exchange(Some(&source), None, None),
        vec![],
        None,
        None,
    )?;
    let staging = dir.join("controller-checkpoint.staging");
    std::fs::create_dir(&staging)?;
    std::fs::write(
        staging.join("controller.bbolt"),
        b"torn unpublished checkpoint",
    )?;
    let store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.metadata().nodes.len(), 1);
    assert!(!staging.exists());
    assert_eq!(
        std::fs::read(dir.join("metadata.sqlite"))?,
        b"immutable legacy source"
    );
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
#[tokio::test]
async fn published_bbolt_before_required_marker_is_recovered_without_reimport()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let source = state(0);
    seed_exchange(
        &dir,
        &exchange(Some(&source), None, None),
        vec![],
        None,
        None,
    )?;
    // Fail the marker temp-file publication only after the validated bbolt file
    // has been flushed, closed, renamed and parent-directory synced.
    std::fs::create_dir(dir.join("controller.bbolt.required.tmp"))?;
    assert!(Store::open(&dir, "test", 1).await.is_err());
    assert!(dir.join("controller.bbolt").is_file());
    assert!(!dir.join("controller.bbolt.required").exists());
    std::fs::remove_dir(dir.join("controller.bbolt.required.tmp"))?;
    std::fs::write(
        dir.join("controller-checkpoint.export.json"),
        b"stale broken exchange",
    )?;
    let store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.metadata().nodes.len(), 1);
    assert!(dir.join("controller.bbolt.required").is_file());
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
#[tokio::test]
async fn missing_required_bbolt_never_reimports_stale_exchange()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let store = Store::open(&dir, "test", 1).await?;
    store.apply_log([entry(7, 1)]).await?;
    // This old identity-only exchange cannot describe the acknowledged sparse
    // checkpoint; direct trait apply deliberately need not add a Raft log entry.
    std::fs::write(
        dir.join("controller-checkpoint.export.json"),
        encode(&exchange(None, None, None))?,
    )?;
    drop(store);
    std::fs::remove_file(dir.join("controller.bbolt"))?;
    assert!(Store::open(&dir, "test", 1).await.is_err());
    assert!(!dir.join("controller.bbolt").exists());
    assert!(!dir.join("controller-checkpoint.staging").exists());
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
#[tokio::test]
async fn existing_bbolt_ignores_stale_sqlite_and_invalid_exchange()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let store = Store::open(&dir, "test", 1).await?;
    store.apply_log([entry(0, 1)]).await?;
    drop(store);
    std::fs::write(dir.join("metadata.sqlite"), b"invalid inert SQLite")?;
    std::fs::write(
        dir.join("controller-checkpoint.export.json"),
        b"invalid inert exchange",
    )?;
    let store = Store::open(&dir, "test", 1).await?;
    assert_eq!(store.metadata().nodes.len(), 1);
    assert_eq!(
        std::fs::read(dir.join("metadata.sqlite"))?,
        b"invalid inert SQLite"
    );
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
#[tokio::test]
async fn partial_fresh_store_and_invalid_existing_bbolt_are_never_reinitialized()
-> Result<(), Box<dyn std::error::Error>> {
    for kind in ["bbolt-only", "wal-only", "corrupt-bbolt-with-export"] {
        let dir = directory();
        std::fs::create_dir_all(&dir)?;
        if kind == "bbolt-only" {
            drop(checkpoint::Checkpoint::open(&dir, "test", 1)?);
        } else if kind == "wal-only" {
            drop(wal::Wal::open(&dir.join("raft-wal"), "test", 1)?);
        } else {
            seed_exchange(&dir, &exchange(None, None, None), vec![], None, None)?;
            std::fs::write(dir.join("controller.bbolt"), [])?;
        }
        assert!(
            Store::open(&dir, "test", 1).await.is_err(),
            "partial store {kind}"
        );
        if kind == "wal-only" {
            assert!(!dir.join("controller.bbolt").exists());
        }
        if kind == "bbolt-only" {
            assert!(!dir.join("raft-wal").exists());
        }
        if kind == "corrupt-bbolt-with-export" {
            assert_eq!(std::fs::metadata(dir.join("controller.bbolt"))?.len(), 0);
        }
        std::fs::remove_dir_all(dir)?;
    }
    Ok(())
}
#[tokio::test]
async fn required_marker_identity_version_and_size_are_checked()
-> Result<(), Box<dyn std::error::Error>> {
    for marker in [
        serde_json::json!({"format":2,"identity":["test",1]}),
        serde_json::json!({"format":1,"identity":["other",1]}),
        serde_json::json!({"format":1,"identity":["test",2]}),
    ] {
        let dir = directory();
        let store = Store::open(&dir, "test", 1).await?;
        drop(store);
        std::fs::write(dir.join("controller.bbolt.required"), encode(&marker)?)?;
        assert!(Store::open(&dir, "test", 1).await.is_err());
        std::fs::remove_dir_all(dir)?;
    }
    let dir = directory();
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join("controller.bbolt.required"), vec![b' '; 4097])?;
    assert!(Store::open(&dir, "test", 1).await.is_err());
    assert!(!dir.join("controller.bbolt").exists());
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
#[tokio::test]
async fn exchange_cannot_cover_missing_committed_or_purged_input()
-> Result<(), Box<dyn std::error::Error>> {
    for purged in [false, true] {
        let dir = directory();
        seed_exchange(
            &dir,
            &exchange(None, None, None),
            vec![],
            Some(entry(4, 1).log_id),
            purged.then_some(entry(4, 1).log_id),
        )?;
        assert!(Store::open(&dir, "test", 1).await.is_err());
        assert!(!dir.join("controller-checkpoint.staging").exists());
        assert!(!dir.join("controller.bbolt").exists());
        std::fs::remove_dir_all(dir)?;
    }
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn dangling_checkpoint_symlink_is_not_overwritten_from_exchange()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    seed_exchange(&dir, &exchange(None, None, None), vec![], None, None)?;
    std::os::unix::fs::symlink(
        dir.join("missing-checkpoint-target"),
        dir.join("controller.bbolt"),
    )?;
    let result = Store::open(&dir, "test", 1).await;
    assert!(
        result.is_err(),
        "a present invalid checkpoint must not be treated as an absent migration destination"
    );
    assert!(
        std::fs::symlink_metadata(dir.join("controller.bbolt"))?
            .file_type()
            .is_symlink()
    );
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn newer_oversized_legacy_state_does_not_replace_existing_staging()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let older = state(0);
    let snapshot = make_snapshot(&older)?;
    let mut newer = state(1);
    newer.metadata.nodes.get_mut(&1).ok_or("node")?.incarnation =
        "x".repeat(recovery::MAX_SNAPSHOT);
    assert!(
        make_snapshot(&newer).is_err(),
        "fixture must exceed the checkpoint wire bound"
    );
    seed_exchange(
        &dir,
        &exchange(Some(&newer), Some(&snapshot), Some(1)),
        vec![],
        None,
        None,
    )?;
    let staging = dir.join("controller-checkpoint.staging");
    std::fs::create_dir(&staging)?;
    std::fs::write(
        staging.join("partial"),
        b"preserve existing staging until all selected input is validated",
    )?;
    assert!(Store::open(&dir, "test", 1).await.is_err());
    assert!(
        staging.join("partial").is_file(),
        "an oversized selected legacy State must be rejected before deleting staging"
    );
    assert!(!dir.join("controller.bbolt").exists());
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
