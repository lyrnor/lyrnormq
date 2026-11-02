use super::*;
fn result(revision: u64, n: usize, payload: usize) -> NewResult {
    NewResult {
        stream_id: "rule".into(),
        client_id: format!("client-{n}"),
        payload: vec![n as u8; payload],
        consumers: vec![NewConsumer {
            outbound_id: "target".into(),
            revision,
            delivery_id: format!("delivery-{n}"),
        }],
    }
}
#[test]
fn four_frames_share_one_sync_and_recover_all_acknowledged_results() {
    let f = Fixture::new();
    let mut o = f.options();
    o.segment_bytes = 1024 * 1024;
    o.max_bytes = 8 * 1024 * 1024;
    let mut j = Journal::open(o.clone()).unwrap();
    let rev = bind(&mut j, "target");
    let groups: Vec<_> = (0..4).map(|i| vec![result(rev, i, 128)]).collect();
    let before = j.append_data_syncs();
    let ids = j.append_result_groups(&groups).unwrap();
    assert_eq!(j.append_data_syncs() - before, 1);
    assert_eq!(ids.len(), 4);
    assert_eq!(j.stats().pending_results, 4);
    let next = j.next_id;
    drop(j);
    let j = Journal::open(o).unwrap();
    assert_eq!(j.next_id, next);
    for (i, g) in ids.iter().enumerate() {
        assert_eq!(
            j.read_consumers(g[0].consumer_ids[0], 1, 1024).unwrap()[0].payload,
            vec![i as u8; 128]
        );
    }
}
#[test]
fn later_invalid_frame_and_aggregate_quota_reject_without_prefix_or_ids() {
    let f = Fixture::new();
    let mut j = f.open();
    let rev = bind(&mut j, "target");
    let before = j.stats();
    let next = j.next_id;
    let mut groups = vec![vec![result(rev, 0, 16)], vec![result(rev + 1, 1, 16)]];
    assert_eq!(
        j.append_result_groups(&groups).unwrap_err(),
        QueueError::StaleBinding
    );
    assert_eq!(j.stats().disk_bytes, before.disk_bytes);
    assert_eq!(j.next_id, next);
    assert_eq!(j.stats().pending_results, 0);
    groups[1][0].consumers[0].revision = rev;
    j.options.max_records = 1;
    assert_eq!(
        j.append_result_groups(&groups).unwrap_err(),
        QueueError::Full
    );
    assert_eq!(j.stats().disk_bytes, before.disk_bytes);
    assert_eq!(j.next_id, next);
    assert_eq!(j.stats().pending_results, 0);
}
#[test]
fn crossing_segments_syncs_each_dirty_segment_before_ack() {
    let f = Fixture::new();
    let mut j = f.open();
    let rev = bind(&mut j, "target");
    let groups: Vec<_> = (0..4).map(|i| vec![result(rev, i, 3000)]).collect();
    let before = j.append_data_syncs();
    let ids = j.append_result_groups(&groups).unwrap();
    assert_eq!(j.append_data_syncs() - before, 4);
    drop(j);
    let j = f.open();
    assert_eq!(j.stats().pending_results, 4);
    for (i, g) in ids.iter().enumerate() {
        assert_eq!(
            j.read_consumers(g[0].consumer_ids[0], 1, 4096).unwrap()[0].payload,
            vec![i as u8; 3000]
        );
    }
}

#[test]
fn four_single_calls_sync_four_times_and_encoding_is_identical_to_group_commit() {
    let a = Fixture::new();
    let b = Fixture::new();
    let mut o = a.options();
    o.segment_bytes = 1024 * 1024;
    o.max_bytes = 8 * 1024 * 1024;
    let mut x = Journal::open(o.clone()).unwrap();
    o.directory = b.path.clone();
    let mut y = Journal::open(o).unwrap();
    let rx = bind(&mut x, "target");
    let ry = bind(&mut y, "target");
    assert_eq!(rx, ry);
    let groups: Vec<_> = (0..4).map(|i| vec![result(rx, i, 128)]).collect();
    let sx = x.append_data_syncs();
    let sy = y.append_data_syncs();
    x.append_result_groups(&groups).unwrap();
    for g in &groups {
        y.append_results(g.clone()).unwrap();
    }
    assert_eq!(x.append_data_syncs() - sx, 1);
    assert_eq!(y.append_data_syncs() - sy, 4);
    assert_eq!(
        fs::read(storage_segment(&x)).unwrap(),
        fs::read(storage_segment(&y)).unwrap()
    );
}
fn storage_segment(j: &Journal) -> PathBuf {
    crate::storage::segment(&j.dir, j.segment)
}
#[test]
fn uncertain_group_failures_poison_without_exposing_logical_state_and_recover_prefix() {
    for (fault, recovered) in [
        (crate::model::Fault::AfterWrite, 1),
        (crate::model::Fault::AfterGroupSecondWrite, 2),
        (crate::model::Fault::AfterSync, 4),
    ] {
        let f = Fixture::new();
        let mut o = f.options();
        o.segment_bytes = 1024 * 1024;
        o.max_bytes = 8 * 1024 * 1024;
        let mut j = Journal::open(o.clone()).unwrap();
        let rev = bind(&mut j, "target");
        let next = j.next_id;
        let groups: Vec<_> = (0..4).map(|i| vec![result(rev, i, 64)]).collect();
        j.fault = Some(fault);
        assert_eq!(j.append_result_groups(&groups).unwrap_err(), QueueError::Io);
        assert_eq!(j.stats().pending_results, 0);
        assert_eq!(j.next_id, next);
        assert_eq!(j.append_result_groups(&groups).unwrap_err(), QueueError::Io);
        assert_eq!(j.consumer_metas(2, 1, 1024).unwrap_err(), QueueError::Io);
        drop(j);
        let j = Journal::open(o).unwrap();
        assert_eq!(j.stats().pending_results, recovered);
        assert_eq!(j.next_id, next + (recovered as u64) * 2);
        for i in 0..recovered {
            assert_eq!(
                j.read_consumers(next + 1 + 2 * i as u64, 1, 1024).unwrap()[0].payload,
                vec![i as u8; 64]
            );
        }
    }
}
#[test]
fn torn_unacknowledged_second_frame_recovers_only_complete_prefix() {
    let f = Fixture::new();
    let mut o = f.options();
    o.segment_bytes = 1024 * 1024;
    o.max_bytes = 8 * 1024 * 1024;
    let mut j = Journal::open(o.clone()).unwrap();
    let rev = bind(&mut j, "target");
    let groups: Vec<_> = (0..4).map(|i| vec![result(rev, i, 128)]).collect();
    j.fault = Some(crate::model::Fault::AfterGroupSecondWrite);
    assert_eq!(j.append_result_groups(&groups).unwrap_err(), QueueError::Io);
    let path = storage_segment(&j);
    drop(j);
    let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
    let len = file.metadata().unwrap().len();
    file.set_len(len - 12).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let mut j = Journal::open(o).unwrap();
    assert_eq!(j.stats().pending_results, 1);
    assert_eq!(j.next_id, 3);
    let ids = j.append_results(vec![result(rev, 9, 16)]).unwrap();
    assert_eq!(ids[0].result_id, 3);
}
#[test]
fn rotation_sync_failure_recovers_uncertain_synced_prefix_and_no_later_frames() {
    for (fault, recovered) in [
        (crate::model::Fault::AfterSync, 1),
        (crate::model::Fault::AfterGroupSecondWrite, 2),
    ] {
        let f = Fixture::new();
        let mut j = f.open();
        let rev = bind(&mut j, "target");
        let groups: Vec<_> = (0..4).map(|i| vec![result(rev, i, 3000)]).collect();
        j.fault = Some(fault);
        assert_eq!(j.append_result_groups(&groups).unwrap_err(), QueueError::Io);
        assert_eq!(j.append_data_syncs(), 1);
        assert_eq!(j.stats().pending_results, 0);
        assert_eq!(j.consumer_metas(2, 1, 4096).unwrap_err(), QueueError::Io);
        drop(j);
        let j = f.open();
        assert_eq!(j.stats().pending_results, recovered);
        for i in 0..recovered {
            assert_eq!(
                j.read_consumers(2 + i as u64 * 2, 1, 4096).unwrap()[0].payload,
                vec![i as u8; 3000]
            );
        }
    }
}

#[test]
fn invalid_group_and_raw_bounds_are_preflighted_without_consuming_ids() {
    let f = Fixture::new();
    let mut o = f.options();
    o.max_bytes = 64 * 1024 * 1024;
    let mut j = Journal::open(o).unwrap();
    let rev = bind(&mut j, "target");
    let next = j.next_id;
    let disk = j.stats().disk_bytes;
    for groups in [
        vec![],
        vec![vec![]],
        (0..5).map(|i| vec![result(rev, i, 1)]).collect(),
        vec![(0..65).map(|i| result(rev, i, 1)).collect()],
    ] {
        assert_eq!(
            j.append_result_groups(&groups).unwrap_err(),
            QueueError::InvalidOptions
        );
    }
    let groups: Vec<_> = (0..2)
        .map(|g| {
            (0..5)
                .map(|i| result(rev, g * 5 + i, 1024 * 1024))
                .collect()
        })
        .collect();
    assert_eq!(
        j.append_result_groups(&groups).unwrap_err(),
        QueueError::Full
    );
    assert_eq!(j.stats().disk_bytes, disk);
    assert_eq!(j.next_id, next);
    assert_eq!(j.stats().pending_results, 0);
}
#[test]
fn checkpoint_preflight_cannot_orphan_any_group_frame() {
    let f = Fixture::new();
    let mut o = f.options();
    o.max_bytes = 32768;
    let mut j = Journal::open(o.clone()).unwrap();
    let rev = bind(&mut j, "target");
    let prior = j.append_results(vec![result(rev, 99, 0)]).unwrap();
    while j.stats().disk_bytes < o.max_bytes - 512 {
        j.begin_consumers(&prior[0].consumer_ids).unwrap();
    }
    let generation = j.generation;
    let next = j.next_id;
    let groups: Vec<_> = (0..4).map(|i| vec![result(rev, i, 0)]).collect();
    let ids = j.append_result_groups(&groups).unwrap();
    assert!(j.generation > generation);
    assert_eq!(ids[0][0].result_id, next);
    assert_eq!(j.stats().pending_results, 5);
    drop(j);
    let j = Journal::open(o).unwrap();
    assert_eq!(j.stats().pending_results, 5);
    assert_eq!(
        j.read_consumers(prior[0].consumer_ids[0], 1, 1024).unwrap()[0]
            .payload
            .len(),
        0
    );
}
#[test]
fn segment_count_preflight_checkpoints_before_writing_group() {
    let f = Fixture::new();
    let mut j = f.open();
    let rev = bind(&mut j, "target");
    let count = crate::storage::MAX_SEGMENTS - 3;
    for i in 0..count {
        j.append_results(vec![result(rev, i, 4096)]).unwrap();
    }
    let generation = j.generation;
    let groups: Vec<_> = (0..4).map(|i| vec![result(rev, 100 + i, 3000)]).collect();
    j.append_result_groups(&groups).unwrap();
    assert!(j.generation > generation);
    assert!(j.segment <= crate::storage::MAX_SEGMENTS as u64);
    drop(j);
    let j = f.open();
    assert_eq!(j.stats().pending_results, count + 4);
}

#[test]
fn aggregate_metadata_bound_rejects_before_any_frame_or_id_is_committed() {
    let f = Fixture::new();
    let mut o = f.options();
    o.max_bytes = 1024 * 1024 * 1024;
    let mut j = Journal::open(o).unwrap();
    let rev = bind(&mut j, "target");
    let client = "\0".repeat(65535);
    let groups: Vec<_> = (0..2)
        .map(|g| {
            (0..64)
                .map(|i| {
                    let mut r = result(rev, g * 64 + i, 0);
                    r.client_id = client.clone();
                    r
                })
                .collect()
        })
        .collect();
    let disk = j.stats().disk_bytes;
    let next = j.next_id;
    assert_eq!(
        j.append_result_groups(&groups).unwrap_err(),
        QueueError::Full
    );
    assert_eq!(j.stats().disk_bytes, disk);
    assert_eq!(j.next_id, next);
    assert_eq!(j.stats().pending_results, 0);
}
#[cfg(unix)]
#[test]
fn grouped_writes_preserve_prevalidated_cached_prefix_across_same_segment_and_rotations() {
    for target in [4096, 1024 * 1024] {
        let f = Fixture::new();
        let mut o = f.options();
        o.segment_bytes = target;
        o.max_bytes = 8 * 1024 * 1024;
        let mut j = Journal::open(o).unwrap();
        let revs = j
            .bind_all(&[
                Binding {
                    outbound_id: "target".into(),
                    descriptor: vec![1],
                },
                Binding {
                    outbound_id: "other".into(),
                    descriptor: vec![2],
                },
            ])
            .unwrap();
        let mut old = result(revs["target"], 99, 16);
        old.consumers.push(NewConsumer {
            outbound_id: "other".into(),
            revision: revs["other"],
            delivery_id: "other-old".into(),
        });
        let ids = j.append_results(vec![old]).unwrap();
        j.read_consumers(ids[0].consumer_ids[0], 1, 1024).unwrap();
        let reads = crate::storage::test_payload_reads();
        let payload = if target == 4096 { 3000 } else { 128 };
        let groups: Vec<_> = (0..4)
            .map(|i| vec![result(revs["target"], i, payload)])
            .collect();
        j.append_result_groups(&groups).unwrap();
        j.begin_consumers(&[ids[0].consumer_ids[0]]).unwrap();
        j.complete_consumers(&[ids[0].consumer_ids[0]], Completion::Delivered)
            .unwrap();
        assert_eq!(
            j.read_consumers(ids[0].consumer_ids[1], 1, 1024).unwrap()[0].payload,
            vec![99; 16]
        );
        assert_eq!(crate::storage::test_payload_reads(), reads);
    }
}
#[test]
fn checkpoint_full_is_uncertain_poison_not_a_retryable_preflight_full() {
    let f = Fixture::new();
    let mut o = f.options();
    o.segment_bytes = 65536;
    o.max_bytes = 16 * 1024 * 1024;
    o.max_records = 3000;
    let mut j = Journal::open(o.clone()).unwrap();
    let rev = bind(&mut j, "target");
    for g in 0..32 {
        j.append_results((0..64).map(|i| result(rev, g * 64 + i, 0)).collect())
            .unwrap();
    }
    drop(j);
    o.segment_bytes = 4096;
    let mut j = Journal::open(o.clone()).unwrap();
    let mut n = 2048;
    while j.segment < (crate::storage::MAX_SEGMENTS - 2) as u64 {
        j.append_results(vec![result(rev, n, 0)]).unwrap();
        n += 1;
    }
    let before = j.stats().pending_results;
    let generation = j.generation;
    let groups: Vec<_> = (0..4)
        .map(|g| {
            (0..64)
                .map(|i| result(rev, 10000 + g * 64 + i, 0))
                .collect()
        })
        .collect();
    assert_eq!(
        j.append_result_groups(&groups).unwrap_err(),
        QueueError::Full
    );
    assert!(j.poisoned);
    assert_eq!(j.generation, generation);
    assert_eq!(j.stats().pending_results, before);
    // Runtime may attempt quota fallback on Full, but poisoned owner must reject it.
    assert_eq!(
        j.append_results(groups[0].clone()).unwrap_err(),
        QueueError::Io
    );
    drop(j);
    let j = Journal::open(o).unwrap();
    assert_eq!(j.stats().pending_results, before);
}

#[test]
fn append_sync_counter_excludes_state_checkpoint_and_reopen_initialization() {
    let f = Fixture::new();
    let mut j = f.open();
    assert_eq!(j.append_data_syncs(), 0);
    let rev = bind(&mut j, "target");
    assert_eq!(j.append_data_syncs(), 0);
    let ids = j.append_results(vec![result(rev, 0, 16)]).unwrap();
    assert_eq!(j.append_data_syncs(), 1);
    j.begin_consumers(&ids[0].consumer_ids).unwrap();
    j.retry_consumers(&ids[0].consumer_ids, 42).unwrap();
    j.checkpoint().unwrap();
    assert_eq!(j.append_data_syncs(), 1);
    drop(j);
    let j = f.open();
    assert_eq!(j.append_data_syncs(), 0);
}
