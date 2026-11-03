//! Controller checkpoint layout, exchange migration and durable ownership gates.
use super::tests::{directory, entry, prime};
use super::*;

#[tokio::test]
async fn fresh_controller_uses_bbolt_without_creating_sqlite()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "test", 1).await?;
    prime(&store, vec![entry(0, 1)], Some(entry(0, 1).log_id)).await?;
    store.apply([entry(0, 1)]).await?;
    store.build_snapshot().await?;
    assert!(dir.join("controller.bbolt").is_file());
    assert!(dir.join("controller.bbolt.required").is_file());
    assert!(!dir.join("metadata.sqlite").exists());
    assert!(!dir.join("metadata.sqlite-wal").exists());
    drop(store);
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
