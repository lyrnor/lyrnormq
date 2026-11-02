use super::*;
fn targets(j: &mut Journal) -> std::collections::BTreeMap<String, u64> {
    j.bind_all(&[
        Binding {
            outbound_id: "x".into(),
            descriptor: vec![1],
        },
        Binding {
            outbound_id: "y".into(),
            descriptor: vec![2],
        },
    ])
    .unwrap()
}
fn result(
    revs: &std::collections::BTreeMap<String, u64>,
    stream: &str,
    client: &str,
    body: &[u8],
    outputs: &[&str],
) -> NewResult {
    NewResult {
        stream_id: stream.into(),
        client_id: client.into(),
        payload: body.to_vec(),
        consumers: outputs
            .iter()
            .map(|out| NewConsumer {
                outbound_id: (*out).into(),
                revision: revs[*out],
                delivery_id: format!("{stream}:{client}:{out}"),
            })
            .collect(),
    }
}
fn old(
    revs: &std::collections::BTreeMap<String, u64>,
    stream: u8,
    event: u8,
    out: &str,
    body: &[u8],
) -> NewRecord {
    let mut payload = vec![stream, event, out.as_bytes()[0]];
    payload.extend_from_slice(body);
    input(out, revs[out], "source", &payload)
}
fn resolve(record: &Record) -> Result<LegacyResult, QueueError> {
    if record.payload.len() < 4 {
        return Err(QueueError::Corrupt);
    }
    Ok(LegacyResult {
        stream_id: format!("rule-{}", record.payload[0]),
        result_key: format!("event-{}", record.payload[1]),
        payload: record.payload[3..].to_vec(),
        delivery_id: format!("delivery-{}", record.payload[2]),
    })
}
fn frames_to_v1(f: &Fixture) {
    for (_, _) in crate::storage::files(&f.dir(), 1024 * 1024).unwrap() {}
    for path in fs::read_dir(f.dir()).unwrap() {
        let path = path.unwrap().path();
        let mut bytes = fs::read(&path).unwrap();
        let mut offset = 0;
        while offset < bytes.len() {
            let size =
                u32::from_le_bytes(bytes[offset + 8..offset + 12].try_into().unwrap()) as usize;
            bytes[offset + 4] = 1;
            let crc = crate::format::checksum(&bytes[offset..offset + 16]);
            bytes[offset + 16..offset + 20].copy_from_slice(&crc.to_le_bytes());
            offset += 24 + size;
        }
        fs::write(path, bytes).unwrap();
    }
}
#[test]
fn shared_reference_completion_retry_and_checkpoint_keep_other_consumer_body() {
    let f = Fixture::new();
    let mut j = f.open();
    let revs = targets(&mut j);
    let body = b"unique-shared-body-for-two-independent-consumers";
    let ids = j
        .append_results(vec![result(&revs, "A", "source", body, &["x", "y"])])
        .unwrap()
        .remove(0);
    let x = ids.consumer_ids[0];
    let y = ids.consumer_ids[1];
    assert_eq!(j.stats().pending_results, 1);
    assert_eq!(j.stats().pending_records, 2);
    assert_eq!(j.stats().pending_bytes, body.len() as u64);
    let bytes: Vec<_> = fs::read_dir(f.dir())
        .unwrap()
        .flat_map(|p| fs::read(p.unwrap().path()).unwrap())
        .collect();
    assert_eq!(bytes.windows(body.len()).filter(|w| *w == body).count(), 1);
    j.begin_consumers(&[x]).unwrap();
    j.retry_consumers(&[x], 987654321).unwrap();
    assert_eq!(j.consumer_heads("x", None, 64)[0].failures, 1);
    assert_eq!(j.consumer_heads("y", None, 64)[0].failures, 0);
    j.complete_consumers(&[x], Completion::Failed).unwrap();
    assert_eq!(j.stats().pending_results, 1);
    assert!(j.consumer_heads("x", None, 64).is_empty());
    j.checkpoint().unwrap();
    drop(j);
    let mut j = f.open();
    let r = j.read_consumers(y, 64, 1024).unwrap();
    assert_eq!(r[0].payload, body);
    assert_eq!(r[0].meta.result_id, ids.result_id);
    assert_eq!(r[0].meta.delivery_id, "A:source:y");
    assert_eq!(j.stats().recovered_records, 1);
    j.complete_consumers(&[y], Completion::Delivered).unwrap();
    assert_eq!(j.stats().pending_bytes, 0);
    assert_eq!(j.stats().pending_results, 0);
    j.checkpoint().unwrap();
    drop(j);
    let j = f.open();
    assert_eq!(j.stats().pending_records, 0);
}
#[test]
fn same_client_different_streams_have_independent_heads_and_homogeneous_batches() {
    let f = Fixture::new();
    let mut j = f.open();
    let revs = targets(&mut j);
    let ids = j
        .append_results(vec![
            result(&revs, "A", "source", b"a1", &["x"]),
            result(&revs, "B", "source", b"b1", &["x"]),
            result(&revs, "A", "source", b"a2", &["x"]),
        ])
        .unwrap();
    let heads = j.consumer_heads("x", None, 64);
    assert_eq!(heads.len(), 2);
    let a = ids[0].consumer_ids[0];
    let b = ids[1].consumer_ids[0];
    assert_eq!(
        j.consumer_metas(a, 64, 1024)
            .unwrap()
            .iter()
            .map(|m| m.stream_id.as_str())
            .collect::<Vec<_>>(),
        ["A", "A"]
    );
    j.retry_consumers(&[a], u64::MAX).unwrap();
    assert_eq!(j.consumer_heads("x", Some(a), 64)[0].id, b);
    assert_eq!(j.consumer_metas(b, 64, 1024).unwrap()[0].not_before_ms, 0);
    j.complete_consumers(&[b], Completion::Delivered).unwrap();
    assert_eq!(j.stats().pending_results, 2);
    assert_eq!(j.consumer_heads("x", None, 64).len(), 1);
}
#[test]
fn shared_fanout_capacity_and_validation_are_atomic() {
    let f = Fixture::new();
    let mut options = f.options();
    options.max_records = 1;
    let mut j = Journal::open(options).unwrap();
    let revs = targets(&mut j);
    assert!(matches!(
        j.append_results(vec![result(&revs, "A", "source", b"a", &["x", "y"])]),
        Err(QueueError::Full)
    ));
    assert_eq!(j.stats().pending_records, 0);
    assert!(matches!(
        j.append_results(vec![result(&revs, "A", "source", b"a", &["x", "x"])]),
        Err(QueueError::InvalidOptions)
    ));
    assert_eq!(j.stats().pending_results, 0);
    let ids = j
        .append_results(vec![result(&revs, "A", "source", b"a", &["x"])])
        .unwrap();
    assert_eq!(ids[0].result_id, 1);
    assert_eq!(ids[0].consumer_ids, [2]);
    assert!(matches!(j.bind_all(&[]), Err(QueueError::BindingConflict)));
    j.complete_consumers(&[2], Completion::Delivered).unwrap();
    j.bind_all(&[]).unwrap();
}
#[test]
fn v1_migration_deduplicates_body_and_preserves_sparse_consumer_state_ids() {
    let f = Fixture::new();
    let mut j = f.open();
    let revs = targets(&mut j);
    let ids = j
        .append_batch(vec![
            old(&revs, b'A', 1, "x", b"same-result"),
            old(&revs, b'A', 1, "y", b"same-result"),
            old(&revs, b'B', 1, "x", b"completed-result"),
        ])
        .unwrap();
    j.begin_attempt(&[ids[0]]).unwrap();
    j.retry(&[ids[0]], 987654321).unwrap();
    j.begin_attempt(&[ids[1]]).unwrap();
    j.complete(&[ids[2]], Completion::Delivered).unwrap();
    let old_next = ids[2] + 1;
    drop(j);
    frames_to_v1(&f);
    let mut j = f.open();
    j.migrate_legacy(resolve).unwrap();
    assert_eq!(j.stats().pending_results, 1);
    assert_eq!(j.stats().pending_records, 2);
    assert_eq!(j.stats().pending_bytes, 11);
    let x = j.consumer_heads("x", None, 64).remove(0);
    let y = j.consumer_heads("y", None, 64).remove(0);
    assert_eq!(x.id, ids[0]);
    assert_eq!(y.id, ids[1]);
    assert!(x.result_id >= old_next);
    assert_eq!(x.result_id, y.result_id);
    assert_eq!(x.failures, 1);
    assert_eq!(x.not_before_ms, 987654321);
    assert!(!x.inflight);
    assert!(y.inflight);
    assert_eq!(y.failures, 0);
    assert_eq!(x.delivery_id, "delivery-120");
    assert_eq!(y.delivery_id, "delivery-121");
    assert!(j.heads("x", None, 64).is_empty());
    j.checkpoint().unwrap();
    drop(j);
    let mut j = f.open();
    j.migrate_legacy(|_| panic!("already upgraded")).unwrap();
    assert_eq!(
        j.read_consumers(y.id, 1, 100).unwrap()[0].payload,
        b"same-result"
    );
    let id = j
        .append_results(vec![result(&revs, "A", "source", b"next", &["y"])])
        .unwrap()[0]
        .consumer_ids[0];
    assert!(id > y.id);
}
#[test]
fn migration_partial_fanout_does_not_recreate_already_completed_destinations() {
    let f = Fixture::new();
    let mut j = f.open();
    let revs = targets(&mut j);
    let ids = j
        .append_batch(vec![
            old(&revs, b'A', 1, "x", b"body"),
            old(&revs, b'A', 1, "y", b"body"),
        ])
        .unwrap();
    j.complete(&[ids[0]], Completion::Delivered).unwrap();
    j.migrate_legacy(resolve).unwrap();
    assert_eq!(j.stats().pending_results, 1);
    assert_eq!(j.pending_for("x"), 0);
    assert_eq!(j.pending_for("y"), 1);
    assert!(j.consumer_heads("x", None, 1).is_empty());
}
#[test]
fn conflicting_or_malformed_legacy_resolution_preserves_original_files() {
    for malformed in [false, true] {
        let f = Fixture::new();
        let mut j = f.open();
        let revs = targets(&mut j);
        j.append_batch(vec![
            old(&revs, b'A', 1, "x", b"body"),
            old(&revs, b'A', 1, "y", b"different"),
        ])
        .unwrap();
        let current = fs::read(f.path.join("CURRENT")).unwrap();
        let segment = f.segment(true);
        let before = fs::read(&segment).unwrap();
        let result = j.migrate_legacy(|r| {
            if malformed {
                Err(QueueError::Corrupt)
            } else {
                resolve(r)
            }
        });
        assert!(matches!(result, Err(QueueError::Corrupt)));
        assert_eq!(fs::read(f.path.join("CURRENT")).unwrap(), current);
        assert_eq!(fs::read(segment).unwrap(), before);
        drop(j);
        let j = f.open();
        assert_eq!(j.stats().pending_records, 2);
        assert_eq!(j.heads("x", None, 64).len(), 1);
    }
}
#[test]
fn migration_before_or_after_manifest_failure_recovers_complete_old_or_new_state() {
    for fault in [
        crate::model::Fault::BeforePublish,
        crate::model::Fault::AfterPublish,
    ] {
        let f = Fixture::new();
        let mut j = f.open();
        let revs = targets(&mut j);
        j.append_batch(vec![
            old(&revs, b'A', 1, "x", b"body"),
            old(&revs, b'A', 1, "y", b"body"),
        ])
        .unwrap();
        j.fault = Some(fault);
        assert!(matches!(j.migrate_legacy(resolve), Err(QueueError::Io)));
        drop(j);
        let mut j = f.open();
        assert_eq!(j.stats().pending_records, 2);
        if fault == crate::model::Fault::BeforePublish {
            assert_eq!(j.heads("x", None, 1).len(), 1);
            j.migrate_legacy(resolve).unwrap();
        } else {
            assert!(j.heads("x", None, 1).is_empty());
        }
        assert_eq!(j.stats().pending_results, 1);
        assert_eq!(j.consumer_heads("x", None, 64).len(), 1);
        assert_eq!(j.consumer_heads("y", None, 64).len(), 1);
    }
}
#[test]
fn mixed_legacy_and_shared_migration_preserves_fifo_and_legacy_api_remains_usable() {
    let f = Fixture::new();
    let mut j = f.open();
    let revs = targets(&mut j);
    let legacy = j
        .append_batch(vec![old(&revs, b'A', 1, "x", b"before")])
        .unwrap()[0];
    let shared = j
        .append_results(vec![result(&revs, "rule-65", "source", b"after", &["x"])])
        .unwrap()[0]
        .consumer_ids[0];
    j.migrate_legacy(resolve).unwrap();
    assert_eq!(j.consumer_heads("x", None, 64)[0].id, legacy);
    assert_eq!(
        j.consumer_metas(legacy, 64, 1024)
            .unwrap()
            .iter()
            .map(|m| m.id)
            .collect::<Vec<_>>(),
        [legacy, shared]
    );
    assert_eq!(j.retained_indexes().0, 0);
    assert_eq!(j.retained_indexes().1, 0);
    j.checkpoint().unwrap();
    drop(j);
    let mut j = f.open();
    assert_eq!(
        j.consumer_metas(legacy, 64, 1024)
            .unwrap()
            .iter()
            .map(|m| m.id)
            .collect::<Vec<_>>(),
        [legacy, shared]
    );
    let old_again = j
        .append_batch(vec![input("x", revs["x"], "source", b"compat")])
        .unwrap()[0];
    assert_eq!(j.heads("x", None, 1)[0].id, old_again);
}
#[test]
fn uncertain_shared_append_and_completion_recover_actual_durable_prefix() {
    let f = Fixture::new();
    let mut j = f.open();
    let revs = targets(&mut j);
    j.fault = Some(crate::model::Fault::AfterSync);
    assert!(matches!(
        j.append_results(vec![result(&revs, "A", "source", b"body", &["x", "y"])]),
        Err(QueueError::Io)
    ));
    drop(j);
    let mut j = f.open();
    let x = j.consumer_heads("x", None, 1)[0].id;
    j.fault = Some(crate::model::Fault::AfterSync);
    assert!(matches!(
        j.complete_consumers(&[x], Completion::Delivered),
        Err(QueueError::Io)
    ));
    drop(j);
    let j = f.open();
    assert_eq!(j.stats().pending_results, 1);
    assert_eq!(j.pending_for("x"), 0);
    assert_eq!(j.pending_for("y"), 1);
}

#[test]
fn near_full_v1_migration_fails_closed_until_explicit_capacity_increase() {
    let f = Fixture::new();
    let mut options = f.options();
    options.max_bytes = 65536;
    let mut j = Journal::open(options.clone()).unwrap();
    let revs = targets(&mut j);
    j.append_batch(
        (0..25)
            .map(|event| old(&revs, b'A', event, "x", b"tiny"))
            .collect(),
    )
    .unwrap();
    let current = fs::read(f.path.join("CURRENT")).unwrap();
    assert!(matches!(j.migrate_legacy(resolve), Err(QueueError::Full)));
    assert_eq!(fs::read(f.path.join("CURRENT")).unwrap(), current);
    drop(j);
    options.max_bytes = 262144;
    let mut j = Journal::open(options).unwrap();
    assert_eq!(j.stats().pending_records, 25);
    j.migrate_legacy(resolve).unwrap();
    assert_eq!(j.stats().pending_records, 25);
    assert_eq!(j.stats().pending_results, 25);
}
#[test]
fn combined_legacy_and_shared_capacity_is_rejected_before_any_frame_write() {
    let f = Fixture::new();
    let mut options = f.options();
    options.max_records = 1;
    let mut j = Journal::open(options).unwrap();
    let revs = targets(&mut j);
    let ids = j
        .append_results(vec![result(&revs, "A", "source", b"body", &["x"])])
        .unwrap();
    let before = j.stats().disk_bytes;
    assert!(matches!(
        j.append_batch(vec![input("x", revs["x"], "old", b"legacy")]),
        Err(QueueError::Full)
    ));
    assert_eq!(j.stats().disk_bytes, before);
    j.complete_consumers(&ids[0].consumer_ids, Completion::Delivered)
        .unwrap();
}
#[test]
fn new_consumer_vectors_and_shared_result_vectors_are_bounded_during_decode() {
    let c = crate::model::StoredConsumer {
        id: 2,
        outbound_id: "x".into(),
        revision: 1,
        delivery_id: "id".into(),
        failures: 0,
        not_before_ms: 0,
        inflight: false,
    };
    let stored = crate::model::StoredResult {
        meta: crate::model::ResultMeta {
            id: 1,
            stream_id: "A".into(),
            client_id: "c".into(),
            payload_len: 0,
        },
        checksum: crate::format::checksum(&[]),
        consumers: vec![c.clone(); 33],
    };
    let json = serde_json::to_vec(&crate::model::Transaction::Results {
        results: vec![stored],
        next_id: 3,
    })
    .unwrap();
    assert!(serde_json::from_slice::<crate::model::Transaction>(&json).is_err());
    let stored = crate::model::StoredResult {
        meta: crate::model::ResultMeta {
            id: 1,
            stream_id: "A".into(),
            client_id: "c".into(),
            payload_len: 0,
        },
        checksum: crate::format::checksum(&[]),
        consumers: vec![c],
    };
    let json = serde_json::to_vec(&crate::model::Transaction::Results {
        results: vec![stored; 65],
        next_id: 3,
    })
    .unwrap();
    assert!(serde_json::from_slice::<crate::model::Transaction>(&json).is_err());
}
#[test]
fn shared_live_corruption_is_not_read_or_rechecksummed_by_checkpoint() {
    let f = Fixture::new();
    let mut j = f.open();
    let revs = targets(&mut j);
    let ids = j
        .append_results(vec![result(
            &revs,
            "A",
            "source",
            b"shared-body-crc",
            &["x", "y"],
        )])
        .unwrap();
    let segment = f.segment(true);
    let mut bytes = fs::read(&segment).unwrap();
    let offset = bytes
        .windows(b"shared-body-crc".len())
        .position(|w| w == b"shared-body-crc")
        .unwrap();
    bytes[offset] ^= 1;
    fs::write(segment, bytes).unwrap();
    assert!(matches!(
        j.read_consumers(ids[0].consumer_ids[0], 1, 100),
        Err(QueueError::Corrupt)
    ));
    assert!(matches!(j.checkpoint(), Err(QueueError::Corrupt)));
}

#[test]
fn sustained_shared_delivery_reclaims_segments_and_all_stream_consumer_indexes() {
    let f = Fixture::new();
    let mut options = f.options();
    options.max_bytes = 65536;
    let mut j = Journal::open(options.clone()).unwrap();
    let revs = targets(&mut j);
    let mut previous = 0;
    let mut decreases = 0;
    for n in 0..100 {
        let ids = j
            .append_results(vec![result(
                &revs,
                &format!("stream-{n}"),
                &format!("client-{n}"),
                &[0x55; 256],
                &["x", "y"],
            )])
            .unwrap();
        j.complete_consumers(&[ids[0].consumer_ids[0]], Completion::Delivered)
            .unwrap();
        assert_eq!(j.stats().pending_results, 1);
        j.complete_consumers(&[ids[0].consumer_ids[1]], Completion::Delivered)
            .unwrap();
        assert_eq!(j.shared.retained_indexes(), (0, 0, 0, 0));
        assert_eq!(j.retained_indexes(), (0, 0, 0));
        let bytes = j.stats().disk_bytes;
        if bytes < previous {
            decreases += 1
        }
        previous = bytes;
    }
    assert!(decreases > 0);
    j.checkpoint().unwrap();
    drop(j);
    let j = Journal::open(options).unwrap();
    assert_eq!(j.stats().pending_records, 0);
    assert_eq!(j.stats().pending_results, 0);
    assert!(j.stats().disk_bytes < 4096);
}
