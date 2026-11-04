use super::*;
use openraft::EntryPayload;
use std::{path::PathBuf, sync::atomic::AtomicU64};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "rmqtt-raft-wal-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn id(index: u64) -> LogId {
    LogId::new(crate::model::committed_leader_id(1, 1), index)
}
fn entry(index: u64) -> Entry {
    Entry {
        log_id: id(index),
        payload: EntryPayload::Blank,
    }
}
fn chunks(path: &Path) -> Vec<PathBuf> {
    let mut files = std::fs::read_dir(path)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "wal"))
        .collect::<Vec<_>>();
    files.sort();
    files
}
#[test]
fn durable_append_vote_commit_reopen_and_snapshot_only_floor() {
    let p = path();
    let mut wal = Wal::open(&p, "test", 1).unwrap();
    wal.append(vec![entry(0), entry(1)]).unwrap();
    wal.save_vote(Vote::new(2, 1)).unwrap();
    wal.save_committed(Some(id(1))).unwrap();
    assert_eq!(wal.entries(0, u64::MAX).unwrap().len(), 2);
    drop(wal);
    let mut wal = Wal::open(&p, "test", 1).unwrap();
    assert_eq!(wal.vote().unwrap(), Some(Vote::new(2, 1)));
    assert_eq!(wal.committed().unwrap(), Some(id(1)));
    assert_eq!(wal.state().unwrap().last_log_id, Some(id(1)));
    assert_eq!(wal.entries(0, 1).unwrap().len(), 2);
    wal.save_committed(Some(id(1))).unwrap();
    drop(wal);
    std::fs::remove_dir_all(&p).unwrap();
    let p = path();
    let mut wal = Wal::open(&p, "test", 1).unwrap();
    wal.save_committed(Some(id(7))).unwrap();
    assert_eq!(wal.state().unwrap().last_log_id, None);
    assert_eq!(wal.state().unwrap().last_purged_log_id, None);
    drop(wal);
    let wal = Wal::open(&p, "test", 1).unwrap();
    assert_eq!(wal.committed().unwrap(), Some(id(7)));
    assert!(wal.entries(0, 7).unwrap().is_empty());
    drop(wal);
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn next_append_keeps_commit_watermark_across_reopen() {
    let p = path();
    let mut wal = Wal::open(&p, "test", 1).unwrap();
    wal.append(vec![entry(0)]).unwrap();
    wal.save_committed(Some(id(0))).unwrap();
    wal.append(vec![entry(1)]).unwrap();
    drop(wal);
    let wal = Wal::open(&p, "test", 1).unwrap();
    assert_eq!(wal.committed().unwrap(), Some(id(0)));
    assert_eq!(wal.state().unwrap().last_log_id, Some(id(1)));
    drop(wal);
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn suffix_truncate_then_append_and_purge_survive_reopen() {
    let p = path();
    let mut wal = Wal::open(&p, "test", 1).unwrap();
    wal.append((0..5).map(entry).collect()).unwrap();
    wal.truncate(id(3)).unwrap();
    assert_eq!(wal.entries(0, 9).unwrap().len(), 3);
    wal.append(vec![entry(3), entry(4)]).unwrap();
    wal.purge(id(1)).unwrap();
    drop(wal);
    let wal = Wal::open(&p, "test", 1).unwrap();
    assert_eq!(wal.state().unwrap().last_purged_log_id, Some(id(1)));
    assert_eq!(
        wal.entries(0, 9)
            .unwrap()
            .iter()
            .map(|e| e.log_id.index)
            .collect::<Vec<_>>(),
        [2, 3, 4]
    );
    drop(wal);
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn identity_mismatch_and_existing_empty_directory_never_initialize() {
    let p = path();
    let wal = Wal::open(&p, "test", 1).unwrap();
    drop(wal);
    assert!(Wal::open(&p, "wrong", 1).is_err());
    assert!(Wal::open(&p, "test", 2).is_err());
    assert!(Wal::open(&p, "test", 1).is_ok());
    std::fs::remove_dir_all(p).unwrap();
    let p = path();
    std::fs::create_dir(&p).unwrap();
    std::fs::write(p.join("unrelated"), b"keep").unwrap();
    assert!(Wal::open(&p, "test", 1).is_err());
    assert_eq!(std::fs::read(p.join("unrelated")).unwrap(), b"keep");
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn acknowledged_single_chunk_truncation_never_silently_reinitializes() {
    for length in [0, 8] {
        let p = path();
        let mut wal = Wal::open(&p, "test", 1).unwrap();
        wal.append(vec![entry(0)]).unwrap();
        wal.save_committed(Some(id(0))).unwrap();
        drop(wal);
        let files = chunks(&p);
        assert_eq!(files.len(), 1);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&files[0])
            .unwrap()
            .set_len(length)
            .unwrap();
        assert!(Wal::open(&p, "test", 1).is_err());
        assert_eq!(std::fs::metadata(&files[0]).unwrap().len(), length);
        std::fs::remove_dir_all(p).unwrap();
    }
}
#[test]
fn payload_bit_flip_and_partial_tail_fail_closed_without_modification() {
    for torn in [false, true] {
        let p = path();
        let mut wal = Wal::open(&p, "test", 1).unwrap();
        wal.append(vec![entry(0)]).unwrap();
        drop(wal);
        let f = chunks(&p).pop().unwrap();
        let mut bytes = std::fs::read(&f).unwrap();
        if torn {
            bytes.pop();
        } else {
            let n = bytes.len() / 2;
            bytes[n] ^= 1;
        }
        std::fs::write(&f, &bytes).unwrap();
        assert!(Wal::open(&p, "test", 1).is_err());
        assert_eq!(std::fs::read(f).unwrap(), bytes);
        std::fs::remove_dir_all(p).unwrap();
    }
}
#[test]
fn callback_failure_poison_prevents_every_later_success() {
    let p = path();
    let mut wal = Wal::open(&p, "test", 1).unwrap();
    wal.fail_next_callback = true;
    assert!(wal.append(vec![entry(0)]).is_err());
    assert!(wal.entries(0, 1).is_err());
    assert!(wal.vote().is_err());
    assert!(wal.committed().is_err());
    assert!(wal.state().is_err());
    assert!(wal.save_committed(Some(id(0))).is_err());
    assert!(wal.save_vote(Vote::new(2, 1)).is_err());
    assert!(wal.append(vec![entry(1)]).is_err());
    drop(wal);
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn unsupported_index_rejected_before_mutation_and_reopen_still_works() {
    let p = path();
    let mut wal = Wal::open(&p, "test", 1).unwrap();
    assert!(wal.append(vec![entry(i64::MAX as u64 + 1)]).is_err());
    assert!(wal.append(vec![entry(u64::MAX), entry(0)]).is_err());
    assert!(wal.save_committed(Some(id(u64::MAX))).is_err());
    wal.append(vec![entry(0)]).unwrap();
    drop(wal);
    let wal = Wal::open(&p, "test", 1).unwrap();
    assert_eq!(wal.entries(0, 1).unwrap().len(), 1);
    drop(wal);
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn corrupt_length_is_rejected_before_allocation() {
    use raft_log::codeq::Decode;
    assert!(Payload::decode(std::io::Cursor::new(u32::MAX.to_be_bytes())).is_err());
    assert!(Metadata::decode(std::io::Cursor::new(u32::MAX.to_be_bytes())).is_err());
    assert!(LogKey::decode(std::io::Cursor::new(u32::MAX.to_be_bytes())).is_err());
}
#[test]
fn oversized_existing_chunk_rejected_without_reading_or_truncating() {
    let p = path();
    let wal = Wal::open(&p, "test", 1).unwrap();
    drop(wal);
    let f = chunks(&p).pop().unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&f)
        .unwrap()
        .set_len(MAX_CHUNK_BYTES + 1)
        .unwrap();
    assert!(Wal::open(&p, "test", 1).is_err());
    assert_eq!(std::fs::metadata(f).unwrap().len(), MAX_CHUNK_BYTES + 1);
    std::fs::remove_dir_all(p).unwrap();
}

#[test]
fn actual_chunk_rotation_remains_readable_with_finite_recovery_bounds() {
    let p = path();
    let mut wal = Wal::open(&p, "test", 1).unwrap();
    let entries = (0..20)
        .map(|index| Entry {
            log_id: id(index),
            payload: EntryPayload::Normal(crate::model::Command::Register {
                node: 1,
                incarnation: "rotation".into(),
                address: "a".repeat(500 * 1024),
                expires_at: 1000,
                lease_ms: 1000,
                replicas: None,
                memory: true,
                disk_id: None,
            }),
        })
        .collect();
    wal.append(entries).unwrap();
    wal.save_committed(Some(id(19))).unwrap();
    drop(wal);
    assert!(chunks(&p).len() > 1);
    for file in chunks(&p) {
        assert!(std::fs::metadata(file).unwrap().len() <= MAX_CHUNK_BYTES);
    }
    let wal = Wal::open(&p, "test", 1).unwrap();
    assert_eq!(wal.entries(0, u64::MAX).unwrap().len(), 20);
    assert_eq!(wal.committed().unwrap(), Some(id(19)));
    drop(wal);
    std::fs::remove_dir_all(p).unwrap();
    let p = path();
    let mut wal = Wal::open(&p, "test", 1).unwrap();
    wal.append((0..5000).map(entry).collect()).unwrap();
    wal.save_committed(Some(id(4999))).unwrap();
    drop(wal);
    assert!(chunks(&p).len() > 1);
    let wal = Wal::open(&p, "test", 1).unwrap();
    assert_eq!(wal.entries(0, u64::MAX).unwrap().len(), 5000);
    assert_eq!(wal.committed().unwrap(), Some(id(4999)));
    drop(wal);
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn excessive_small_records_rejected_before_native_replay() {
    let p = path();
    std::fs::create_dir(&p).unwrap();
    let mut cfg = (*config(&p).unwrap()).clone();
    cfg.wal.chunk_max_records = Some(CHUNK_RECORDS * 2);
    let mut native = RaftLog::<WalTypes>::open(Arc::new(cfg)).unwrap();
    native
        .save_user_data(Some(Metadata {
            format: 1,
            cluster: "test".into(),
            node: 1,
            committed: None,
        }))
        .unwrap();
    for term in 1..MAX_CHUNK_RECORDS + 2 {
        native.save_vote(VoteKey(Vote::new(term, 1))).unwrap();
    }
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    native
        .flush(
            true,
            Some(Completion {
                sender: tx,
                injected_error: false,
            }),
        )
        .unwrap();
    rx.recv().unwrap().unwrap();
    drop(native);
    let before = chunks(&p)
        .into_iter()
        .map(|f| {
            let bytes = std::fs::read(&f).unwrap();
            (f, bytes)
        })
        .collect::<Vec<_>>();
    assert!(Wal::open(&p, "test", 1).is_err());
    for (file, bytes) in before {
        assert_eq!(std::fs::read(file).unwrap(), bytes);
    }
    std::fs::remove_dir_all(p).unwrap();
}
