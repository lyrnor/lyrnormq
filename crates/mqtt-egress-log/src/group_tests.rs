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
    let before = j.group_data_syncs;
    let ids = j.append_result_groups(&groups).unwrap();
    assert_eq!(j.group_data_syncs - before, 1);
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
    let before = j.group_data_syncs;
    let ids = j.append_result_groups(&groups).unwrap();
    assert_eq!(j.group_data_syncs - before, 4);
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
