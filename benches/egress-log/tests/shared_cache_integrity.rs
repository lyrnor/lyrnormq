//! Public-API acceptance: optimizations must not hide changed durable bytes/files.
use mqtt_egress_log::{
    Binding, Completion, Journal, NewConsumer, NewResult, QueueError, QueueOptions,
};
use std::{
    collections::BTreeMap,
    fs::{self, File, FileTimes, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
const BODY: &[u8] = b"independent-cache-integrity-payload-\x00\xff\xfe-distinct";
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "journal-cache-integrity-{}-{stamp}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&path).unwrap();
        Self(path)
    }
    fn options(&self) -> QueueOptions {
        let mut o = QueueOptions::new(self.0.clone());
        o.segment_bytes = 1024 * 1024;
        o.max_bytes = 128 * 1024 * 1024;
        o
    }
    fn open(&self) -> Journal {
        Journal::open(self.options()).unwrap()
    }
    fn segment(&self) -> PathBuf {
        let generation = fs::read_dir(&self.0)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("generation-")
            })
            .unwrap();
        let mut files: Vec<_> = fs::read_dir(generation)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        files.sort();
        files.pop().unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn bind(j: &mut Journal) -> BTreeMap<String, u64> {
    j.bind_all(&["x", "y"].map(|id| Binding {
        outbound_id: id.into(),
        descriptor: format!("opaque-{id}").into_bytes(),
    }))
    .unwrap()
}
fn result(revisions: &BTreeMap<String, u64>, index: usize, body: &[u8]) -> NewResult {
    NewResult {
        stream_id: format!("rule-{}", index % 3),
        client_id: format!("client-{index}"),
        payload: body.to_vec(),
        consumers: ["x", "y"]
            .into_iter()
            .map(|id| NewConsumer {
                outbound_id: id.into(),
                revision: revisions[id],
                delivery_id: format!("{index}:{id}"),
            })
            .collect(),
    }
}
fn warm() -> (Fixture, Journal, Vec<u64>) {
    let f = Fixture::new();
    let mut j = f.open();
    let revisions = bind(&mut j);
    let ids = j
        .append_results(vec![result(&revisions, 0, BODY)])
        .unwrap()
        .remove(0)
        .consumer_ids;
    assert_eq!(j.read_consumers(ids[0], 1, 1024).unwrap()[0].payload, BODY);
    (f, j, ids)
}
fn offset(path: &Path) -> u64 {
    let bytes = fs::read(path).unwrap();
    bytes.windows(BODY.len()).position(|b| b == BODY).unwrap() as u64
}
fn corrupt(path: &Path, at: u64) {
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(at)).unwrap();
    file.write_all(b"!").unwrap();
    file.sync_all().unwrap();
}
fn assert_closed(j: &Journal, id: u64) {
    assert!(
        matches!(
            j.read_consumers(id, 1, 1024),
            Err(QueueError::Io | QueueError::Corrupt)
        ),
        "cached payload must not bypass changed-file validation"
    );
}

#[test]
fn same_length_mutation_after_first_destination_is_not_hidden() {
    let (f, j, ids) = warm();
    let path = f.segment();
    corrupt(&path, offset(&path));
    assert_closed(&j, ids[1]);
    drop(j);
    assert!(matches!(
        Journal::open(f.options()),
        Err(QueueError::Corrupt)
    ));
}
#[test]
fn restoring_mtime_after_mutation_does_not_revalidate_cached_bytes() {
    let (f, j, ids) = warm();
    let path = f.segment();
    let mtime = fs::metadata(&path).unwrap().modified().unwrap();
    corrupt(&path, offset(&path));
    File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_times(FileTimes::new().set_modified(mtime))
        .unwrap();
    assert_closed(&j, ids[1]);
}
#[test]
fn truncated_body_after_warm_read_fails_closed() {
    let (f, j, ids) = warm();
    let path = f.segment();
    OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(offset(&path) + 1)
        .unwrap();
    assert_closed(&j, ids[1]);
}
#[cfg(unix)]
#[test]
fn permissions_change_after_warm_read_fails_closed() {
    use std::os::unix::fs::PermissionsExt;
    let (f, j, ids) = warm();
    let path = f.segment();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert_closed(&j, ids[1]);
}
#[cfg(unix)]
#[test]
fn hardlink_after_warm_read_fails_closed() {
    let (f, j, ids) = warm();
    let path = f.segment();
    fs::hard_link(&path, f.0.join("other-link")).unwrap();
    assert_closed(&j, ids[1]);
}
#[cfg(unix)]
#[test]
fn symlink_replacement_after_warm_read_fails_closed() {
    use std::os::unix::fs::symlink;
    let (f, j, ids) = warm();
    let path = f.segment();
    let saved = f.0.join("saved-private-segment");
    fs::rename(&path, &saved).unwrap();
    symlink(&saved, &path).unwrap();
    assert_closed(&j, ids[1]);
}
#[test]
fn different_inode_at_same_path_cannot_return_old_cached_body() {
    let (f, j, ids) = warm();
    let path = f.segment();
    let saved = f.0.join("saved-private-segment");
    fs::rename(&path, &saved).unwrap();
    fs::copy(&saved, &path).unwrap();
    corrupt(&path, offset(&path));
    assert_closed(&j, ids[1]);
}
#[test]
fn partial_completion_retry_checkpoint_and_reopen_preserve_remaining_reference() {
    let (f, mut j, ids) = warm();
    j.begin_consumers(&[ids[0]]).unwrap();
    j.complete_consumers(&[ids[0]], Completion::Delivered)
        .unwrap();
    j.begin_consumers(&[ids[1]]).unwrap();
    j.retry_consumers(&[ids[1]], 1234567).unwrap();
    let before = j.consumer_heads("y", None, 64).remove(0);
    assert_eq!(before.failures, 1);
    assert_eq!(before.not_before_ms, 1234567);
    j.checkpoint().unwrap();
    assert_eq!(j.read_consumers(ids[1], 1, 1024).unwrap()[0].payload, BODY);
    assert_eq!(j.stats().pending_results, 1);
    assert_eq!(j.stats().pending_records, 1);
    drop(j);
    let mut j = f.open();
    let after = j.consumer_heads("y", None, 64).remove(0);
    assert_eq!(before, after);
    assert!(j.consumer_heads("x", None, 64).is_empty());
    assert_eq!(j.read_consumers(ids[1], 1, 1024).unwrap()[0].payload, BODY);
    j.complete_consumers(&[ids[1]], Completion::Failed).unwrap();
    assert_eq!(j.stats().pending_results, 0);
    assert_eq!(j.stats().pending_bytes, 0);
    drop(j);
    assert_eq!(f.open().stats().pending_records, 0);
}
#[test]
fn cache_pressure_does_not_lose_rule_bodies_or_consumer_heads() {
    let f = Fixture::new();
    let mut j = f.open();
    let revisions = bind(&mut j);
    let mut ids = Vec::new();
    // More than both the frozen 8MiB body allowance and 8192-entry allowance.
    for start in (0..8448).step_by(64) {
        let records = (start..start + 64)
            .map(|n| {
                let mut body = vec![0x53; 1056];
                body[..8].copy_from_slice(&(n as u64).to_le_bytes());
                result(&revisions, n, &body)
            })
            .collect();
        ids.extend(
            j.append_results(records)
                .unwrap()
                .into_iter()
                .map(|r| r.consumer_ids),
        );
    }
    for outbound in 0..2 {
        for (n, consumer_ids) in ids.iter().enumerate() {
            let records = j.read_consumers(consumer_ids[outbound], 1, 2048).unwrap();
            assert_eq!(records[0].payload.len(), 1056);
            assert_eq!(records[0].payload[..8], (n as u64).to_le_bytes());
            assert_eq!(records[0].meta.stream_id, format!("rule-{}", n % 3));
        }
    }
    assert_eq!(j.stats().pending_records, 16896);
    assert_eq!(j.stats().pending_results, 8448);
}
#[test]
fn empty_body_fanout_survives_checkpoint_and_restart() {
    let f = Fixture::new();
    let mut j = f.open();
    let revisions = bind(&mut j);
    let ids = j
        .append_results(vec![result(&revisions, 0, &[])])
        .unwrap()
        .remove(0)
        .consumer_ids;
    assert!(
        j.read_consumers(ids[0], 1, 1).unwrap()[0]
            .payload
            .is_empty()
    );
    j.checkpoint().unwrap();
    drop(j);
    let j = f.open();
    assert!(
        j.read_consumers(ids[1], 1, 1).unwrap()[0]
            .payload
            .is_empty()
    );
    assert_eq!(j.stats().pending_records, 2);
}

#[test]
fn legal_append_after_warm_read_preserves_old_and_new_result_bytes() {
    let (_f, mut j, ids) = warm();
    let revisions = bind(&mut j);
    let added = j
        .append_results(vec![result(&revisions, 1, b"new-result")])
        .unwrap()
        .remove(0)
        .consumer_ids;
    assert_eq!(j.read_consumers(ids[1], 1, 1024).unwrap()[0].payload, BODY);
    assert_eq!(
        j.read_consumers(added[0], 1, 1024).unwrap()[0].payload,
        b"new-result"
    );
}

#[test]
fn legal_append_does_not_bless_preexisting_external_body_mutation() {
    let (f, mut j, ids) = warm();
    let path = f.segment();
    corrupt(&path, offset(&path));
    let revisions = bind(&mut j);
    j.append_results(vec![result(&revisions, 1, b"new-result")])
        .unwrap();
    assert_closed(&j, ids[1]);
}
