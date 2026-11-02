use super::*;
fn shared(j: &mut Journal, body: &[u8]) -> ResultIds {
    let revs = j
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
    j.append_results(vec![NewResult {
        stream_id: "A".into(),
        client_id: "source".into(),
        payload: body.to_vec(),
        consumers: ["x", "y"]
            .map(|out| NewConsumer {
                outbound_id: out.into(),
                revision: revs[out],
                delivery_id: format!("A:{out}"),
            })
            .to_vec(),
    }])
    .unwrap()
    .remove(0)
}
#[test]
fn destinations_share_one_verified_physical_body_read() {
    let f = Fixture::new();
    let mut j = f.open();
    let ids = shared(&mut j, b"one disk read across independent destinations");
    let before = crate::storage::test_payload_reads();
    for _ in 0..2 {
        for id in &ids.consumer_ids {
            assert_eq!(
                j.read_consumers(*id, 1, 1024).unwrap()[0].payload,
                b"one disk read across independent destinations"
            );
        }
    }
    assert_eq!(crate::storage::test_payload_reads() - before, 1);
    assert_eq!(j.reads.lock().unwrap().stats().3, 3);
}

#[test]
fn crc_slicing_matches_independent_bitwise_reference_and_standard_vector() {
    fn bitwise(bytes: &[u8]) -> u32 {
        let mut c = !0u32;
        for b in bytes {
            c ^= u32::from(*b);
            for _ in 0..8 {
                c = if c & 1 == 1 {
                    (c >> 1) ^ 0xedb88320
                } else {
                    c >> 1
                };
            }
        }
        !c
    }
    assert_eq!(crate::format::checksum(b"123456789"), 0xcbf43926);
    assert_eq!(crate::format::checksum(b""), 0);
    let data: Vec<_> = (0..8192).map(|v| ((v * 17 + 31) % 256) as u8).collect();
    for n in [
        1, 7, 8, 9, 15, 16, 17, 31, 32, 63, 64, 65, 255, 256, 257, 8191, 8192,
    ] {
        assert_eq!(crate::format::checksum(&data[..n]), bitwise(&data[..n]));
    }
}
#[test]
fn shared_body_cache_survives_partial_ack_retry_but_not_generation_or_reopen() {
    let f = Fixture::new();
    let mut j = f.open();
    let ids = shared(&mut j, b"shared");
    j.read_consumers(ids.consumer_ids[0], 1, 1024).unwrap();
    j.complete_consumers(&[ids.consumer_ids[0]], Completion::Delivered)
        .unwrap();
    let reads = crate::storage::test_payload_reads();
    j.read_consumers(ids.consumer_ids[1], 1, 1024).unwrap();
    assert_eq!(crate::storage::test_payload_reads(), reads + 1); // state append changes fingerprint
    j.begin_consumers(&[ids.consumer_ids[1]]).unwrap();
    j.retry_consumers(&[ids.consumer_ids[1]], 42).unwrap();
    assert_eq!(j.consumer_heads("y", None, 1)[0].failures, 1);
    j.checkpoint().unwrap();
    assert_eq!(j.reads.lock().unwrap().stats().0, 0);
    let before = crate::storage::test_payload_reads();
    assert_eq!(
        j.read_consumers(ids.consumer_ids[1], 1, 1024).unwrap()[0].payload,
        b"shared"
    );
    assert_eq!(crate::storage::test_payload_reads(), before + 1);
    drop(j);
    let mut j = f.open();
    assert_eq!(j.reads.lock().unwrap().stats().0, 0);
    assert_eq!(j.consumer_heads("y", None, 1)[0].failures, 1);
    j.read_consumers(ids.consumer_ids[1], 1, 1024).unwrap();
    j.complete_consumers(&[ids.consumer_ids[1]], Completion::Delivered)
        .unwrap();
    assert_eq!(j.reads.lock().unwrap().stats().0, 0);
}

fn cache_dataset(inputs: usize, payload: usize, batch: usize) -> (Fixture, Journal, Vec<u64>) {
    let f = Fixture::new();
    let mut options = f.options();
    options.max_bytes = 128 * 1024 * 1024;
    options.max_records = 20000;
    if payload == 0 {
        options.segment_bytes = 1024 * 1024;
    }
    let mut j = Journal::open(options).unwrap();
    let revs = j
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
    let mut ids = Vec::new();
    for start in (0..inputs).step_by(batch) {
        let records = (start..(start + batch).min(inputs))
            .map(|n| NewResult {
                stream_id: "A".into(),
                client_id: format!("client-{}", start / batch),
                payload: vec![(n % 251) as u8; payload],
                consumers: ["x", "y"]
                    .map(|out| NewConsumer {
                        outbound_id: out.into(),
                        revision: revs[out],
                        delivery_id: format!("{n}:{out}"),
                    })
                    .to_vec(),
            })
            .collect();
        ids.extend(
            j.append_results(records)
                .unwrap()
                .into_iter()
                .map(|id| id.consumer_ids[0]),
        );
    }
    (f, j, ids)
}
#[test]
fn cache_payload_and_descriptor_limits_are_enforced_and_evicted_bodies_reread() {
    let (_f, j, ids) = cache_dataset(200, 65536, 4);
    for head in ids.iter().step_by(4) {
        let rows = j.read_consumers(*head, 4, 4 * 65536).unwrap();
        assert_eq!(rows.len(), 4);
    }
    let (entries, bytes, descriptors, _, tickets) = j.reads.lock().unwrap().stats();
    assert_eq!(bytes, 8 * 1024 * 1024);
    assert_eq!(entries, 128);
    assert_eq!(entries, tickets);
    assert_eq!(descriptors, 16);
    let before = crate::storage::test_payload_reads();
    assert_eq!(
        j.read_consumers(ids[0], 4, 4 * 65536).unwrap()[0].payload,
        vec![0; 65536]
    );
    assert_eq!(crate::storage::test_payload_reads(), before + 4);
    assert_eq!(j.reads.lock().unwrap().stats().1, 8 * 1024 * 1024);
}
#[test]
fn cache_empty_bodies_obey_entry_limit_without_unbounded_eviction_tickets() {
    let (_f, mut j, ids) = cache_dataset(8193, 0, 64);
    for head in ids.iter().step_by(64) {
        j.read_consumers(*head, 64, 1).unwrap();
    }
    let (entries, bytes, descriptors, _, tickets) = j.reads.lock().unwrap().stats();
    assert_eq!(entries, 8192);
    assert_eq!(entries, tickets);
    assert_eq!(bytes, 0);
    assert!(descriptors <= 16);
    let mut complete = Vec::new();
    for head in ids.iter().take(32) {
        let y = j
            .shared
            .bodies
            .get(&j.shared.consumers[head].0)
            .unwrap()
            .consumers
            .iter()
            .find(|id| **id != *head)
            .copied()
            .unwrap();
        complete.extend([*head, y]);
    }
    j.complete_consumers(&complete, Completion::Delivered)
        .unwrap();
    let stats = j.reads.lock().unwrap().stats();
    assert_eq!(stats.0, 8192 - 31);
    assert_eq!(stats.0, stats.4);
    j.reads.get_mut().unwrap().clear();
    let stats = j.reads.lock().unwrap().stats();
    assert_eq!(stats.0, 0);
    assert_eq!(stats.4, 0);
}
