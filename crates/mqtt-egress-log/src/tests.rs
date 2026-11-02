use super::*;
use std::{
    fs,
    io::{Seek, SeekFrom, Write},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(1);
struct Fixture {
    path: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "rmqtt-egress-log-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        Self { path }
    }
    fn options(&self) -> QueueOptions {
        let mut o = QueueOptions::new(self.path.clone());
        o.segment_bytes = 4096;
        o.max_bytes = 1024 * 1024;
        o.max_records = 1000;
        o
    }
    fn open(&self) -> Journal {
        Journal::open(self.options()).unwrap()
    }
    fn dir(&self) -> PathBuf {
        let root = fs::canonicalize(&self.path).unwrap();
        let n = crate::storage::manifest(&root).unwrap().unwrap();
        crate::storage::generation(&root, n)
    }
    fn segment(&self, last: bool) -> PathBuf {
        let dir = self.dir();
        let files = crate::storage::files(&dir, 1024 * 1024).unwrap();
        crate::storage::segment(
            &dir,
            if last {
                files.last().unwrap().0
            } else {
                files.first().unwrap().0
            },
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
fn bind(j: &mut Journal, id: &str) -> u64 {
    j.bind_all(&[Binding {
        outbound_id: id.into(),
        descriptor: b"opaque-target-with-no-protocol".to_vec(),
    }])
    .unwrap()[id]
}
fn input(id: &str, revision: u64, client: &str, payload: &[u8]) -> NewRecord {
    NewRecord {
        outbound_id: id.into(),
        revision,
        client_id: client.into(),
        payload: payload.to_vec(),
    }
}
fn append(j: &mut Journal, revision: u64, client: &str, payload: &[u8]) -> u64 {
    j.append_batch(vec![input("target", revision, client, payload)])
        .unwrap()[0]
}

#[test]
fn options_are_finite() {
    let f = Fixture::new();
    for (segment, max, records) in [
        (0, 65536, 8),
        (4095, 65536, 8),
        (4096, 16383, 8),
        (4096, 65536, 0),
        (4096, 65536, 1_000_001),
        (4096, 1024 * 1024 * 1024 * 1024 + 1, 8),
    ] {
        let mut o = f.options();
        o.segment_bytes = segment;
        o.max_bytes = max;
        o.max_records = records;
        assert_eq!(Journal::open(o).unwrap_err(), QueueError::InvalidOptions);
    }
}
#[test]
fn generic_binary_records_survive_restart_with_stable_identifiers() {
    let f = Fixture::new();
    let mut j = f.open();
    let rev = bind(&mut j, "target");
    let ids = j
        .append_batch(vec![
            input("target", rev, "client-a", &[0, 255, 1, 254]),
            input("target", rev, "client-a", b"another future adapter payload"),
            input("target", rev, "client-b", b"not HTTP"),
        ])
        .unwrap();
    assert_eq!(ids, [1, 2, 3]);
    drop(j);
    let mut j = f.open();
    assert_eq!(j.stats().recovered_records, 3);
    let records = j.read_batch(1, 64, 1024).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].payload, [0, 255, 1, 254]);
    assert_eq!(records[1].meta.id, 2);
    assert_eq!(j.pending_for("target"), 3);
    assert_eq!(bind(&mut j, "target"), rev);
}
#[test]
fn indexed_heads_are_exact_client_fifo_and_cursor_bounded() {
    let f = Fixture::new();
    let mut j = f.open();
    let rev = bind(&mut j, "target");
    for client in ["a", "a", "b", "a", "c"] {
        append(&mut j, rev, client, b"{} ");
    }
    assert_eq!(
        j.heads("target", None, 64)
            .iter()
            .map(|m| m.id)
            .collect::<Vec<_>>(),
        [1, 3, 5]
    );
    assert_eq!(j.heads("target", Some(1), 1)[0].id, 3);
    assert!(j.heads("target", Some(5), 1).is_empty());
    j.complete(&[1], Completion::Delivered).unwrap();
    assert_eq!(
        j.heads("target", None, 64)
            .iter()
            .map(|m| m.id)
            .collect::<Vec<_>>(),
        [2, 3, 5]
    );
    assert!(j.read_batch(4, 64, 1024).is_err());
}
#[test]
fn retry_and_uncertain_inflight_state_survive_reopen_and_checkpoint() {
    let f = Fixture::new();
    let mut j = f.open();
    let rev = bind(&mut j, "target");
    let a = append(&mut j, rev, "a", b"first");
    let b = append(&mut j, rev, "a", b"second");
    j.begin_attempt(&[a]).unwrap();
    j.retry(&[a], u64::MAX - 2).unwrap();
    j.begin_attempt(&[a]).unwrap();
    j.checkpoint().unwrap();
    drop(j);
    let mut j = f.open();
    let head = j.heads("target", None, 64).remove(0);
    assert_eq!(head.failures, 1);
    assert_eq!(head.not_before_ms, u64::MAX - 2);
    assert!(head.inflight);
    assert_eq!(j.read_batch(a, 64, 1024).unwrap().len(), 1);
    j.retry(&[a], 123).unwrap();
    assert_eq!(j.heads("target", None, 64)[0].failures, 2);
    j.complete(&[a], Completion::Failed).unwrap();
    assert_eq!(j.heads("target", None, 64)[0].id, b);
    drop(j);
    assert_eq!(f.open().heads("target", None, 64)[0].id, b);
}
#[test]
fn binding_changes_are_atomic_and_blocked_only_while_pending() {
    let f = Fixture::new();
    let mut j = f.open();
    let rev = bind(&mut j, "target");
    let id = append(&mut j, rev, "a", b"x");
    assert_eq!(j.bind_all(&[]).unwrap_err(), QueueError::BindingConflict);
    assert_eq!(
        j.bind_all(&[Binding {
            outbound_id: "target".into(),
            descriptor: b"changed".to_vec()
        }])
        .unwrap_err(),
        QueueError::BindingConflict
    );
    assert_eq!(j.pending_for("target"), 1);
    j.complete(&[id], Completion::Delivered).unwrap();
    let new = j
        .bind_all(&[Binding {
            outbound_id: "target".into(),
            descriptor: b"changed".to_vec(),
        }])
        .unwrap()["target"];
    assert!(new > rev);
    assert_eq!(
        j.append_batch(vec![input("target", rev, "a", b"stale")])
            .unwrap_err(),
        QueueError::StaleBinding
    );
    drop(j);
    let mut reopened = f.open();
    assert_eq!(
        reopened
            .bind_all(&[Binding {
                outbound_id: "target".into(),
                descriptor: b"changed".to_vec()
            }])
            .unwrap()["target"],
        new
    );
}
#[test]
fn invalid_group_never_partially_appends_and_identifiers_do_not_reuse() {
    let f = Fixture::new();
    let mut j = f.open();
    let rev = bind(&mut j, "target");
    assert_eq!(
        j.append_batch(vec![
            input("target", rev, "a", b"ok"),
            input("target", rev + 1, "b", b"bad")
        ])
        .unwrap_err(),
        QueueError::StaleBinding
    );
    assert_eq!(j.stats().pending_records, 0);
    let id = append(&mut j, rev, "a", b"ok");
    j.complete(&[id], Completion::Delivered).unwrap();
    j.checkpoint().unwrap();
    drop(j);
    let mut j = f.open();
    assert!(append(&mut j, rev, "a", b"next") > id);
    assert_eq!(
        j.complete(&[999], Completion::Delivered),
        Err(QueueError::InvalidOptions)
    );
    assert_eq!(j.begin_attempt(&[id, id]), Err(QueueError::InvalidOptions));
}
#[test]
fn full_count_and_physical_capacity_allow_terminal_progress_and_reclaim() {
    let f = Fixture::new();
    let mut options = f.options();
    options.max_bytes = 65536;
    options.max_records = 8;
    let mut j = Journal::open(options.clone()).unwrap();
    let rev = bind(&mut j, "target");
    let mut ids = Vec::new();
    for _ in 0..8 {
        ids.push(append(&mut j, rev, "a", &[7; 1024]));
    }
    assert_eq!(
        j.append_batch(vec![input("target", rev, "b", b"full")])
            .unwrap_err(),
        QueueError::Full
    );
    for _ in 0..100 {
        j.begin_attempt(&ids).unwrap();
        j.retry(&ids, 42).unwrap();
    }
    j.complete(&ids, Completion::Delivered).unwrap();
    j.checkpoint().unwrap();
    assert!(j.stats().disk_bytes < 8192);
    assert_eq!(j.stats().pending_records, 0);
    assert!(append(&mut j, rev, "a", b"progress") > ids[7]);
    drop(j);
    assert_eq!(Journal::open(options).unwrap().stats().pending_records, 1);
}
#[test]
fn sustained_delivery_reclaims_completed_segments_without_accumulation() {
    let f = Fixture::new();
    let mut o = f.options();
    o.max_bytes = 65536;
    let mut j = Journal::open(o).unwrap();
    let rev = bind(&mut j, "target");
    for _ in 0..1000 {
        let id = append(&mut j, rev, "same", &[3; 1024]);
        j.begin_attempt(&[id]).unwrap();
        j.complete(&[id], Completion::Delivered).unwrap();
        assert!(j.stats().disk_bytes <= 65536);
    }
    assert_eq!(j.stats().pending_records, 0);
    drop(j);
    let mut j = f.open();
    assert_eq!(j.stats().pending_records, 0);
    assert!(append(&mut j, rev, "a", b"after") > 1000);
}
#[test]
fn checkpoint_preserves_live_payloads_states_and_reclaims_old_generation() {
    let f = Fixture::new();
    let mut j = f.open();
    let rev = bind(&mut j, "target");
    let a = append(&mut j, rev, "a", &[9; 16000]);
    let b = append(&mut j, rev, "b", &[8; 16000]);
    let c = append(&mut j, rev, "a", &[7; 16000]);
    j.complete(&[b], Completion::Failed).unwrap();
    j.begin_attempt(&[a]).unwrap();
    j.retry(&[a], 789).unwrap();
    let before = f.dir();
    j.checkpoint().unwrap();
    assert!(!before.exists());
    assert_eq!(j.stats().pending_records, 2);
    assert_eq!(j.read_batch(a, 64, 32000).unwrap()[0].payload, [9; 16000]);
    drop(j);
    let mut j = f.open();
    assert_eq!(j.heads("target", None, 64)[0].not_before_ms, 789);
    j.complete(&[a], Completion::Delivered).unwrap();
    assert_eq!(j.read_batch(c, 64, 32000).unwrap()[0].payload, [7; 16000]);
}
#[test]
fn latest_incomplete_header_and_body_truncate_only_uncommitted_tail() {
    for partial in [vec![1, 2, 3], {
        let mut b = vec![0; 20];
        b[..4].copy_from_slice(b"REQJ");
        b[4] = 1;
        b[8..12].copy_from_slice(&1024u32.to_le_bytes());
        b
    }] {
        let f = Fixture::new();
        let mut j = f.open();
        let rev = bind(&mut j, "target");
        let id = append(&mut j, rev, "a", b"accepted");
        drop(j);
        let path = f.segment(true);
        let before = fs::metadata(&path).unwrap().len();
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&partial).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let mut j = f.open();
        assert_eq!(fs::metadata(path).unwrap().len(), before);
        assert_eq!(j.read_batch(id, 1, 100).unwrap()[0].payload, b"accepted");
    }
}
#[test]
fn checksum_corruption_in_complete_latest_frame_fails_closed() {
    let f = Fixture::new();
    let mut j = f.open();
    let rev = bind(&mut j, "target");
    append(&mut j, rev, "a", b"accepted");
    drop(j);
    let path = f.segment(true);
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    file.write_all(&[123]).unwrap();
    file.sync_all().unwrap();
    assert_eq!(Journal::open(f.options()).unwrap_err(), QueueError::Corrupt);
}
#[test]
fn sealed_segment_incomplete_tail_is_corruption_not_repair() {
    let f = Fixture::new();
    let mut j = f.open();
    let rev = bind(&mut j, "target");
    append(&mut j, rev, "a", &[1; 8000]);
    append(&mut j, rev, "b", &[2; 8000]);
    drop(j);
    let first = f.segment(false);
    let file = fs::OpenOptions::new().write(true).open(first).unwrap();
    let len = file.metadata().unwrap().len();
    file.set_len(len - 1).unwrap();
    file.sync_all().unwrap();
    assert_eq!(Journal::open(f.options()).unwrap_err(), QueueError::Corrupt);
}
#[test]
fn corrupt_frame_length_and_version_fail_before_large_allocation() {
    for field in [4, 8] {
        let f = Fixture::new();
        let mut j = f.open();
        let rev = bind(&mut j, "target");
        append(&mut j, rev, "a", b"accepted");
        drop(j);
        let path = f.segment(false);
        let mut file = fs::OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start(field)).unwrap();
        file.write_all(&[255; 4]).unwrap();
        file.sync_all().unwrap();
        assert_eq!(Journal::open(f.options()).unwrap_err(), QueueError::Corrupt);
    }
}
#[test]
fn exclusive_ownership_and_foreign_files_fail_closed() {
    let f = Fixture::new();
    let j = f.open();
    assert_eq!(Journal::open(f.options()).unwrap_err(), QueueError::Locked);
    drop(j);
    let file = crate::storage::private_file(&f.path.join("foreign"), true).unwrap();
    drop(file);
    assert_eq!(Journal::open(f.options()).unwrap_err(), QueueError::Corrupt);
}
#[cfg(unix)]
#[test]
fn private_files_reject_symlinks_hardlinks_and_insecure_permissions() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let f = Fixture::new();
    let mut j = f.open();
    let rev = bind(&mut j, "target");
    append(&mut j, rev, "a", b"private");
    drop(j);
    let path = f.segment(true);
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let alias = f.path.join("alias");
    fs::hard_link(&path, &alias).unwrap();
    assert_eq!(Journal::open(f.options()).unwrap_err(), QueueError::Io);
    fs::remove_file(alias).unwrap();
    let original = f.path.join("CURRENT.tmp");
    fs::rename(&path, &original).unwrap();
    symlink(&original, &path).unwrap();
    assert_eq!(Journal::open(f.options()).unwrap_err(), QueueError::Io);
    fs::remove_file(&path).unwrap();
    fs::rename(original, &path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(Journal::open(f.options()).unwrap_err(), QueueError::Io);
}
#[cfg(unix)]
#[test]
fn queue_directory_symlink_is_rejected_but_system_temp_parent_is_supported() {
    use std::os::unix::fs::symlink;
    let f = Fixture::new();
    drop(f.open());
    let alias = f.path.with_extension("symlink");
    symlink(&f.path, &alias).unwrap();
    let mut options = f.options();
    options.directory = alias.clone();
    assert_eq!(Journal::open(options).unwrap_err(), QueueError::Io);
    fs::remove_file(alias).unwrap();
    assert!(Journal::open(f.options()).is_ok());
}
#[test]
fn orphan_prepared_generation_is_removed_without_replacing_current_data() {
    let f = Fixture::new();
    let mut j = f.open();
    let rev = bind(&mut j, "target");
    let id = append(&mut j, rev, "a", b"durable");
    drop(j);
    let extra = crate::storage::generation(&fs::canonicalize(&f.path).unwrap(), 99);
    crate::storage::directory(&extra).unwrap();
    drop(crate::storage::private_file(&crate::storage::segment(&extra, 1), true).unwrap());
    let mut j = f.open();
    assert!(!extra.exists());
    assert_eq!(j.read_batch(id, 1, 100).unwrap()[0].payload, b"durable");
}

#[test]
fn missing_manifest_never_discards_accepted_generation() {
    let f = Fixture::new();
    let mut j = f.open();
    let rev = bind(&mut j, "target");
    append(&mut j, rev, "a", b"accepted");
    drop(j);
    let segment = f.segment(true);
    let before = fs::read(&segment).unwrap();
    fs::remove_file(f.path.join("CURRENT")).unwrap();
    assert_eq!(Journal::open(f.options()).unwrap_err(), QueueError::Corrupt);
    assert_eq!(fs::read(segment).unwrap(), before);
}
#[test]
fn corrupted_complete_frame_length_cannot_be_misclassified_as_partial_tail() {
    let f = Fixture::new();
    let mut j = f.open();
    let rev = bind(&mut j, "target");
    append(&mut j, rev, "a", &[1; 8000]);
    drop(j);
    let path = f.segment(true);
    let mut bytes = fs::read(&path).unwrap();
    let original = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    bytes[8..12].copy_from_slice(&(original + 1000).to_le_bytes());
    fs::write(&path, &bytes).unwrap();
    assert_eq!(Journal::open(f.options()).unwrap_err(), QueueError::Corrupt);
    assert_eq!(fs::read(path).unwrap(), bytes);
}

#[test]
fn payload_bitflip_after_open_is_not_sent_or_blessed_by_compaction() {
    for checkpoint in [false, true] {
        let f = Fixture::new();
        let mut j = f.open();
        let rev = bind(&mut j, "target");
        let body = b"unique opaque payload cannot be corrupted";
        let id = append(&mut j, rev, "a", body);
        let path = f.segment(true);
        let mut bytes = fs::read(&path).unwrap();
        let offset = bytes.windows(body.len()).position(|w| w == body).unwrap();
        bytes[offset] ^= 0x80;
        fs::write(&path, bytes).unwrap();
        if checkpoint {
            assert_eq!(j.checkpoint(), Err(QueueError::Corrupt));
        } else {
            assert_eq!(j.read_batch(id, 1, 100).err(), Some(QueueError::Corrupt));
        }
    }
}

#[test]
fn uncertain_append_or_terminal_commit_poison_owner_and_replay_actual_prefix() {
    use crate::model::Fault;
    for point in [Fault::AfterWrite, Fault::AfterSync] {
        let f = Fixture::new();
        let mut j = f.open();
        let rev = bind(&mut j, "target");
        j.fault = Some(point);
        assert_eq!(
            j.append_batch(vec![input("target", rev, "a", b"unknown outcome")])
                .unwrap_err(),
            QueueError::Io
        );
        assert_eq!(j.begin_attempt(&[1]), Err(QueueError::Io));
        drop(j);
        let mut j = f.open();
        assert_eq!(j.stats().pending_records, 1);
        assert_eq!(
            j.read_batch(1, 1, 100).unwrap()[0].payload,
            b"unknown outcome"
        );
        j.fault = Some(Fault::AfterSync);
        assert_eq!(j.complete(&[1], Completion::Delivered), Err(QueueError::Io));
        drop(j);
        let mut j = f.open();
        assert_eq!(j.stats().pending_records, 0);
        assert!(append(&mut j, rev, "a", b"next") > 1);
    }
}
#[test]
fn checkpoint_crash_before_and_after_manifest_switch_preserves_exact_records() {
    use crate::model::Fault;
    for point in [Fault::BeforePublish, Fault::AfterPublish] {
        let f = Fixture::new();
        let mut j = f.open();
        let rev = bind(&mut j, "target");
        let a = append(&mut j, rev, "a", b"keep");
        let b = append(&mut j, rev, "b", b"remove");
        j.complete(&[b], Completion::Failed).unwrap();
        j.begin_attempt(&[a]).unwrap();
        j.retry(&[a], 123456789).unwrap();
        j.fault = Some(point);
        assert_eq!(j.checkpoint(), Err(QueueError::Io));
        assert_eq!(j.begin_attempt(&[a]), Err(QueueError::Io));
        drop(j);
        let mut j = f.open();
        assert_eq!(j.stats().pending_records, 1);
        let record = j.read_batch(a, 1, 100).unwrap().remove(0);
        assert_eq!(record.payload, b"keep");
        assert_eq!(record.meta.failures, 1);
        assert_eq!(record.meta.not_before_ms, 123456789);
        assert!(append(&mut j, rev, "c", b"after recovery") > b);
        let root = fs::canonicalize(&f.path).unwrap();
        assert_eq!(
            fs::read_dir(root)
                .unwrap()
                .filter(|e| e
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("generation-"))
                .count(),
            1
        );
    }
}
#[test]
fn bounded_batch_input_and_escaped_client_identifiers_preserve_reclaim_progress() {
    let f = Fixture::new();
    let mut o = f.options();
    o.max_bytes = 65536;
    o.max_records = 100;
    let mut j = Journal::open(o).unwrap();
    let rev = bind(&mut j, "target");
    assert_eq!(
        j.append_batch((0..65).map(|_| input("target", rev, "a", b"x")).collect())
            .unwrap_err(),
        QueueError::InvalidOptions
    );
    assert_eq!(
        j.append_batch(vec![input("target", rev, "a", &vec![0; 1024 * 1024 + 1])])
            .unwrap_err(),
        QueueError::InvalidOptions
    );
    let escaped = "\0\n\t\\\"".repeat(10);
    let mut ids = Vec::new();
    loop {
        match j.append_batch(vec![input("target", rev, &escaped, &[4; 256])]) {
            Ok(v) => ids.extend(v),
            Err(QueueError::Full) => break,
            Err(e) => panic!("unexpected {e}"),
        }
    }
    assert!(!ids.is_empty());
    j.checkpoint().unwrap();
    for chunk in ids.chunks(64) {
        j.begin_attempt(chunk).unwrap();
        j.retry(chunk, 999).unwrap();
        j.complete(chunk, Completion::Failed).unwrap();
    }
    j.checkpoint().unwrap();
    assert_eq!(j.stats().pending_records, 0);
    assert!(j.stats().disk_bytes <= 65536);
}

#[test]
fn metadata_preview_and_read_batch_select_identical_prefix_without_payload_reads() {
    let f = Fixture::new();
    let mut j = f.open();
    let rev = bind(&mut j, "target");
    let ids = j
        .append_batch(vec![
            input("target", rev, "a", b"123"),
            input("target", rev, "a", b"12345"),
            input("target", rev, "a", b"1234567"),
        ])
        .unwrap();
    assert_eq!(
        j.batch_metas(ids[0], 64, 8)
            .unwrap()
            .iter()
            .map(|m| m.id)
            .collect::<Vec<_>>(),
        ids[..2]
    );
    let metas = j.batch_metas(ids[0], 64, 8).unwrap();
    let loaded = j.read_batch(ids[0], 64, 8).unwrap();
    assert_eq!(
        metas,
        loaded.iter().map(|r| r.meta.clone()).collect::<Vec<_>>()
    );
    j.begin_attempt(&[ids[0]]).unwrap();
    assert_eq!(j.batch_metas(ids[0], 64, 100).unwrap().len(), 1);
    let path = f.segment(false);
    let original = fs::read(&path).unwrap();
    let mut corrupted = original.clone();
    let offset = corrupted.windows(3).position(|w| w == b"123").unwrap();
    corrupted[offset] = b'9';
    fs::write(&path, corrupted).unwrap();
    assert_eq!(j.batch_metas(ids[0], 1, 100).unwrap().len(), 1);
    assert_eq!(
        j.read_batch(ids[0], 1, 100).err(),
        Some(QueueError::Corrupt)
    );
}
#[test]
fn writer_reclaims_before_reader_segment_limit_and_reopens_its_own_output() {
    let f = Fixture::new();
    let mut j = f.open();
    let rev = bind(&mut j, "target");
    let mut ids = Vec::new();
    for n in 0..crate::storage::MAX_SEGMENTS - 2 {
        ids.push(append(&mut j, rev, &format!("client{n}"), &[4; 4096]));
    }
    let original = f.dir();
    for _ in 0..100 {
        j.begin_attempt(&ids[..1]).unwrap();
        j.retry(&ids[..1], 1).unwrap();
    }
    assert_ne!(original, f.dir());
    assert!(
        crate::storage::files(&f.dir(), 1024 * 1024).unwrap().len() <= crate::storage::MAX_SEGMENTS
    );
    drop(j);
    let mut j = f.open();
    assert_eq!(j.stats().pending_records, ids.len());
    for id in ids {
        j.complete(&[id], Completion::Delivered).unwrap();
    }
    j.checkpoint().unwrap();
    assert_eq!(j.stats().pending_records, 0);
}

#[test]
fn malformed_orphan_file_and_root_directory_counts_fail_without_deletion() {
    let f = Fixture::new();
    drop(f.open());
    let root = fs::canonicalize(&f.path).unwrap();
    let orphan = crate::storage::generation(&root, 99);
    crate::storage::directory(&orphan).unwrap();
    for n in 1..=crate::storage::MAX_SEGMENTS + 1 {
        drop(
            crate::storage::private_file(&crate::storage::segment(&orphan, n as u64), true)
                .unwrap(),
        );
    }
    assert_eq!(Journal::open(f.options()).unwrap_err(), QueueError::Corrupt);
    assert_eq!(
        fs::read_dir(&orphan).unwrap().count(),
        crate::storage::MAX_SEGMENTS + 1
    );
    let f = Fixture::new();
    drop(f.open());
    let root = fs::canonicalize(&f.path).unwrap();
    for n in 2..20 {
        crate::storage::directory(&crate::storage::generation(&root, n)).unwrap();
    }
    assert_eq!(Journal::open(f.options()).unwrap_err(), QueueError::Corrupt);
    assert_eq!(
        fs::read_dir(root)
            .unwrap()
            .filter(|e| e
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("generation-"))
            .count(),
        19
    );
}

#[test]
fn terminal_delivery_evicts_historical_outbound_and_client_index_keys() {
    let f = Fixture::new();
    let mut j = f.open();
    for n in 0..40 {
        let name = format!("outbound-{n}");
        let rev = bind(&mut j, &name);
        let id = j
            .append_batch(vec![input(&name, rev, "one-shot-client", b"x")])
            .unwrap()[0];
        j.complete(&[id], Completion::Delivered).unwrap();
        assert_eq!(j.retained_indexes(), (0, 0, 0));
    }
}

#[test]
fn persistent_metadata_vectors_reject_excess_elements_while_deserializing() {
    use crate::model::{StoredBinding, StoredRecord, Transaction};
    let binding = StoredBinding {
        outbound_id: "target".into(),
        revision: 1,
        descriptor: b"opaque".to_vec(),
    };
    let too_many = Transaction::Bind {
        bindings: vec![binding.clone(); 33],
        next_revision: 2,
    };
    assert!(
        serde_json::from_slice::<Transaction>(&serde_json::to_vec(&too_many).unwrap()).is_err()
    );
    let mut huge = binding;
    huge.descriptor = vec![255; 65537];
    let too_large = Transaction::Bind {
        bindings: vec![huge],
        next_revision: 2,
    };
    assert!(
        serde_json::from_slice::<Transaction>(&serde_json::to_vec(&too_large).unwrap()).is_err()
    );
    let too_many = Transaction::Begin { ids: vec![1; 65] };
    assert!(
        serde_json::from_slice::<Transaction>(&serde_json::to_vec(&too_many).unwrap()).is_err()
    );
    let record = StoredRecord {
        meta: RecordMeta {
            id: 1,
            outbound_id: "target".into(),
            revision: 1,
            client_id: "a".into(),
            failures: 0,
            not_before_ms: 0,
            inflight: false,
            payload_len: 0,
        },
        checksum: 0,
    };
    let too_many = Transaction::Append {
        records: vec![record; 65],
        next_id: 2,
    };
    assert!(
        serde_json::from_slice::<Transaction>(&serde_json::to_vec(&too_many).unwrap()).is_err()
    );
}

#[test]
fn one_rule_result_fanout_stores_body_only_once() {
    let f = Fixture::new();
    let mut j = f.open();
    let revisions = j
        .bind_all(&[
            Binding {
                outbound_id: "x".into(),
                descriptor: vec![1],
            },
            Binding {
                outbound_id: "y".into(),
                descriptor: vec![2],
            },
        ])
        .unwrap();
    let body = b"one-rule-result-must-not-be-copied-per-destination";
    j.append_results(vec![NewResult {
        stream_id: "rule".into(),
        client_id: "source".into(),
        payload: body.to_vec(),
        consumers: vec![
            NewConsumer {
                outbound_id: "x".into(),
                revision: revisions["x"],
                delivery_id: "delivery-x".into(),
            },
            NewConsumer {
                outbound_id: "y".into(),
                revision: revisions["y"],
                delivery_id: "delivery-y".into(),
            },
        ],
    }])
    .unwrap();
    assert_eq!(j.stats().pending_records, 2);
    assert_eq!(j.stats().pending_bytes, body.len() as u64);
}
