//! Independent acceptance for multi-frame durability, not a multi-frame transaction.
use mqtt_egress_log::{
    Binding, Completion, Journal, NewConsumer, NewResult, QueueError, QueueOptions,
};
use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!(
            "group-commit-integrity-{}-{n}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut b = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            b.mode(0o700);
        }
        b.create(&p).unwrap();
        Self(p)
    }
    fn options(&self, segment: u64, max_records: usize) -> QueueOptions {
        let mut o = QueueOptions::new(self.0.clone());
        o.segment_bytes = segment;
        o.max_bytes = 128 * 1024 * 1024;
        o.max_records = max_records;
        o
    }
    fn segments(&self) -> BTreeMap<String, Vec<u8>> {
        let mut m = BTreeMap::new();
        for generation in fs::read_dir(&self.0).unwrap() {
            let generation = generation.unwrap().path();
            if generation.is_dir() {
                for file in fs::read_dir(generation).unwrap() {
                    let file = file.unwrap().path();
                    m.insert(
                        file.file_name().unwrap().to_string_lossy().into_owned(),
                        fs::read(file).unwrap(),
                    );
                }
            }
        }
        m
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn setup(f: &Fixture, segment: u64, max_records: usize) -> (Journal, BTreeMap<String, u64>) {
    let mut j = Journal::open(f.options(segment, max_records)).unwrap();
    let r = j
        .bind_all(&["x", "y"].map(|id| Binding {
            outbound_id: id.into(),
            descriptor: format!("binding-{id}").into_bytes(),
        }))
        .unwrap();
    (j, r)
}
fn row(r: &BTreeMap<String, u64>, i: usize, size: usize) -> NewResult {
    NewResult {
        stream_id: format!("rule-{}", i % 3),
        client_id: format!("client-{i}"),
        payload: vec![i as u8; size],
        consumers: ["x", "y"]
            .map(|id| NewConsumer {
                outbound_id: id.into(),
                revision: r[id],
                delivery_id: format!("event-{i}-{id}"),
            })
            .to_vec(),
    }
}
#[test]
fn later_stale_binding_is_rejected_before_any_frame_or_id_is_consumed() {
    let f = Fixture::new();
    let (mut j, r) = setup(&f, 1048576, 100);
    let before = f.segments();
    let mut bad = row(&r, 2, 100);
    bad.consumers[0].revision += 1;
    assert_eq!(
        j.append_result_groups(&[vec![row(&r, 1, 100)], vec![bad]])
            .unwrap_err(),
        QueueError::StaleBinding
    );
    assert_eq!(j.stats().pending_records, 0);
    assert_eq!(f.segments(), before);
    let ids = j.append_results(vec![row(&r, 1, 100)]).unwrap();
    assert_eq!(ids[0].result_id, 1);
    assert_eq!(ids[0].consumer_ids, [2, 3]);
}
#[test]
fn later_capacity_failure_is_zero_append_and_smaller_fallback_can_commit() {
    let f = Fixture::new();
    let (mut j, r) = setup(&f, 1048576, 3);
    let before = f.segments();
    assert_eq!(
        j.append_result_groups(&[vec![row(&r, 1, 100)], vec![row(&r, 2, 100)]])
            .unwrap_err(),
        QueueError::Full
    );
    assert_eq!(j.stats().pending_results, 0);
    assert_eq!(f.segments(), before);
    let ids = j.append_results(vec![row(&r, 1, 100)]).unwrap();
    assert_eq!(ids[0].result_id, 1);
    assert_eq!(
        j.append_results(vec![row(&r, 2, 100)]).unwrap_err(),
        QueueError::Full
    );
    j.complete_consumers(&ids[0].consumer_ids, Completion::Delivered)
        .unwrap();
    assert_eq!(
        j.append_results(vec![row(&r, 2, 100)]).unwrap()[0].result_id,
        4
    );
}
#[test]
fn shape_and_aggregate_payload_bounds_do_not_append_prefix() {
    let f = Fixture::new();
    let (mut j, r) = setup(&f, 1048576, 100);
    let before = f.segments();
    for groups in [
        vec![],
        vec![vec![]],
        (0..5).map(|i| vec![row(&r, i, 1)]).collect(),
        vec![(0..65).map(|i| row(&r, i, 1)).collect()],
    ] {
        assert!(j.append_result_groups(&groups).is_err());
        assert_eq!(f.segments(), before);
    }
    let groups = vec![
        (0..5).map(|i| row(&r, i, 1024 * 1024)).collect(),
        (5..9).map(|i| row(&r, i, 1024 * 1024)).collect(),
    ];
    assert!(matches!(
        j.append_result_groups(&groups),
        Err(QueueError::Full | QueueError::InvalidOptions)
    ));
    assert_eq!(f.segments(), before);
    assert_eq!(j.stats().pending_results, 0);
}
#[test]
fn successful_group_survives_cross_segment_reopen_with_identical_consumer_ids() {
    let f = Fixture::new();
    let (mut j, r) = setup(&f, 4096, 100);
    let groups: Vec<_> = (0..4).map(|i| vec![row(&r, i, 3000)]).collect();
    let ids = j.append_result_groups(&groups).unwrap();
    assert_eq!(ids.len(), 4);
    assert!(f.segments().len() >= 4);
    drop(j);
    let j = Journal::open(f.options(4096, 100)).unwrap();
    assert_eq!(j.stats().recovered_records, 8);
    for (i, frame) in ids.iter().enumerate() {
        for id in &frame[0].consumer_ids {
            let rows = j.read_consumers(*id, 1, 4096).unwrap();
            assert_eq!(rows[0].payload, vec![i as u8; 3000]);
            assert_eq!(rows[0].meta.result_id, frame[0].result_id);
        }
    }
}
#[test]
fn group_and_existing_separate_frame_api_emit_identical_durable_bytes() {
    let a = Fixture::new();
    let b = Fixture::new();
    let (mut ja, ra) = setup(&a, 1048576, 100);
    let (mut jb, rb) = setup(&b, 1048576, 100);
    let groups: Vec<_> = (0..4).map(|i| vec![row(&ra, i, 1000)]).collect();
    assert_eq!(ja.append_data_syncs(), 0);
    assert_eq!(jb.append_data_syncs(), 0);
    let ids = ja.append_result_groups(&groups).unwrap();
    for (i, frame) in ids.iter().enumerate() {
        let old = jb.append_results(vec![row(&rb, i, 1000)]).unwrap();
        assert_eq!(frame[0].result_id, old[0].result_id);
        assert_eq!(frame[0].consumer_ids, old[0].consumer_ids);
    }
    assert_eq!(a.segments(), b.segments());
    assert_eq!(ja.append_data_syncs(), 1);
    assert_eq!(jb.append_data_syncs(), 4);
    ja.checkpoint().unwrap();
    assert_eq!(ja.append_data_syncs(), 1);
}
#[test]
fn grouped_append_preserves_cached_prefix_through_retry_partial_completion_and_checkpoint() {
    let f = Fixture::new();
    let (mut j, r) = setup(&f, 1048576, 100);
    let old = j.append_results(vec![row(&r, 9, 100)]).unwrap().remove(0);
    j.read_consumers(old.consumer_ids[0], 1, 100).unwrap();
    j.append_result_groups(&[vec![row(&r, 1, 100)], vec![row(&r, 2, 100)]])
        .unwrap();
    j.begin_consumers(&[old.consumer_ids[0]]).unwrap();
    j.retry_consumers(&[old.consumer_ids[0]], 123).unwrap();
    j.complete_consumers(&[old.consumer_ids[0]], Completion::Delivered)
        .unwrap();
    assert_eq!(
        j.read_consumers(old.consumer_ids[1], 1, 100).unwrap()[0].payload,
        vec![9; 100]
    );
    j.checkpoint().unwrap();
    drop(j);
    let j = Journal::open(f.options(1048576, 100)).unwrap();
    assert_eq!(j.stats().pending_records, 5);
    assert_eq!(
        j.read_consumers(old.consumer_ids[1], 1, 100).unwrap()[0].payload,
        vec![9; 100]
    );
}
#[test]
fn uncertain_group_tail_fixture_recovers_complete_frames_only() {
    // Generate valid frame bytes, then model an unacknowledged interrupted final
    // frame. Deliberately truncating previously acknowledged data is NOT a claim
    // that acknowledged data may be lost under the real sync contract.
    let f = Fixture::new();
    let (mut j, r) = setup(&f, 1048576, 100);
    let ids = j
        .append_result_groups(&[
            vec![row(&r, 1, 1000)],
            vec![row(&r, 2, 1000)],
            vec![row(&r, 3, 1000)],
        ])
        .unwrap();
    drop(j);
    let generation = fs::read_dir(&f.0)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.is_dir())
        .unwrap();
    let file = fs::read_dir(generation)
        .unwrap()
        .map(|e| e.unwrap().path())
        .max()
        .unwrap();
    let handle = fs::OpenOptions::new().write(true).open(file).unwrap();
    let len = handle.metadata().unwrap().len();
    handle.set_len(len - 50).unwrap();
    handle.sync_all().unwrap();
    drop(handle);
    let j = Journal::open(f.options(1048576, 100)).unwrap();
    assert_eq!(j.stats().pending_results, 2);
    assert_eq!(j.stats().recovered_records, 4);
    assert_eq!(
        j.read_consumers(ids[0][0].consumer_ids[0], 1, 2000)
            .unwrap()[0]
            .payload,
        vec![1; 1000]
    );
    assert!(
        j.read_consumers(ids[2][0].consumer_ids[0], 1, 2000)
            .is_err()
    );
}

#[test]
fn live_byte_budget_rejection_does_not_publish_an_earlier_frame() {
    let f = Fixture::new();
    let mut options = f.options(4096, 100);
    options.max_bytes = 32 * 1024;
    let mut j = Journal::open(options).unwrap();
    let revisions = j
        .bind_all(&["x", "y"].map(|id| Binding {
            outbound_id: id.into(),
            descriptor: format!("binding-{id}").into_bytes(),
        }))
        .unwrap();
    let before = f.segments();
    let groups: Vec<_> = (0..3).map(|i| vec![row(&revisions, i, 2000)]).collect();
    assert_eq!(
        j.append_result_groups(&groups).unwrap_err(),
        QueueError::Full
    );
    assert_eq!(f.segments(), before);
    assert_eq!(j.stats().pending_results, 0);
    assert_eq!(
        j.append_results(vec![row(&revisions, 0, 2000)]).unwrap()[0].result_id,
        1
    );
}
