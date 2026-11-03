use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn dir() -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "rmqtt-checkpoint-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&p).unwrap();
    p
}
#[test]
fn keyed_checkpoint_reopens_and_deletes_stale_records() {
    let p = dir();
    let mut db = Checkpoint::open(&p, "test", 1).unwrap();
    assert!(db.read_snapshot().unwrap().is_none());
    let mut state = State::default();
    state.metadata.routes.insert("one".into(), [1, 2].into());
    state.metadata.routes.insert("delete".into(), [2].into());
    db.write_snapshot(&super::super::make_snapshot(&state).unwrap())
        .unwrap();
    state.metadata.routes.remove("delete");
    state.metadata.routes.insert("one".into(), [3].into());
    db.write_snapshot(&super::super::make_snapshot(&state).unwrap())
        .unwrap();
    drop(db);
    let db = Checkpoint::open(&p, "test", 1).unwrap();
    let got = recovery::snapshot_state(&db.read_snapshot().unwrap().unwrap()).unwrap();
    assert_eq!(encode(&got).unwrap(), encode(&state).unwrap());
    drop(db);
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn wrong_identity_and_empty_existing_file_rejected() {
    let p = dir();
    let db = Checkpoint::open(&p, "test", 1).unwrap();
    drop(db);
    assert!(Checkpoint::open(&p, "other", 1).is_err());
    assert!(Checkpoint::open(&p, "test", 2).is_err());
    std::fs::remove_dir_all(&p).unwrap();
    std::fs::create_dir(&p).unwrap();
    std::fs::write(p.join("controller.bbolt"), []).unwrap();
    assert!(Checkpoint::open(&p, "test", 1).is_err());
    assert_eq!(
        std::fs::metadata(p.join("controller.bbolt")).unwrap().len(),
        0
    );
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn corrupt_record_rejected_and_poisoned() {
    let p = dir();
    let mut db = Checkpoint::open(&p, "test", 1).unwrap();
    let saved = super::super::make_snapshot(&State::default()).unwrap();
    db.write_snapshot(&saved).unwrap();
    db.db
        .update(|tx| {
            tx.bucket(b"metadata")?
                .unwrap()
                .put(b"checkpoint", b"corrupt")
        })
        .unwrap();
    assert!(db.read_snapshot().is_err());
    assert!(db.write_snapshot(&saved).is_err());
    drop(db);
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn noncanonical_valid_snapshot_normalizes_and_bad_digest_is_rejected() {
    let p = dir();
    let mut db = Checkpoint::open(&p, "test", 1).unwrap();
    let mut saved = super::super::make_snapshot(&State::default()).unwrap();
    saved.data = serde_json::to_vec_pretty(&State::default()).unwrap();
    saved.digest = Some(recovery::snapshot_digest(&saved.meta, &saved.data).unwrap());
    db.write_snapshot(&saved).unwrap();
    let got = db.read_snapshot().unwrap().unwrap();
    assert_eq!(got.data, encode(&State::default()).unwrap());
    assert_eq!(got.meta, saved.meta);
    saved.digest = Some("wrong".into());
    assert!(db.write_snapshot(&saved).is_err());
    assert!(db.read_snapshot().unwrap().is_some());
    drop(db);
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn deleting_empty_checkpoint_header_does_not_restore_fresh_identity() {
    let p = dir();
    let mut db = Checkpoint::open(&p, "test", 1).unwrap();
    db.write_snapshot(&super::super::make_snapshot(&State::default()).unwrap())
        .unwrap();
    db.db
        .update(|tx| tx.bucket(b"metadata")?.unwrap().delete(b"checkpoint"))
        .unwrap();
    assert!(db.read_snapshot().is_err());
    drop(db);
    assert!(Checkpoint::open(&p, "test", 1).is_err());
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn injected_write_failure_poison_requires_reopen_and_preserves_checkpoint() {
    let p = dir();
    let mut db = Checkpoint::open(&p, "test", 1).unwrap();
    let saved = super::super::make_snapshot(&State::default()).unwrap();
    db.write_snapshot(&saved).unwrap();
    db.fail_next_write();
    assert!(db.write_snapshot(&saved).is_err());
    assert!(db.read_snapshot().is_err());
    drop(db);
    let db = Checkpoint::open(&p, "test", 1).unwrap();
    assert!(db.read_snapshot().unwrap().is_some());
    drop(db);
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn oversized_file_and_oversized_record_fail_without_reinitializing() {
    let p = dir();
    let file = p.join("controller.bbolt");
    std::fs::File::create(&file)
        .unwrap()
        .set_len(FILE_LIMIT + 1)
        .unwrap();
    assert!(Checkpoint::open(&p, "test", 1).is_err());
    assert_eq!(std::fs::metadata(&file).unwrap().len(), FILE_LIMIT + 1);
    assert!(unpack(b"nodes", b"1", &vec![0; RECORD_LIMIT + 33]).is_err());
    std::fs::remove_dir_all(p).unwrap();
}
