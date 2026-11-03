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
#[test]
fn missing_or_rebound_business_record_cannot_validate_snapshot() {
    for remove in [true, false] {
        let p = dir();
        let mut db = Checkpoint::open(&p, "test", 1).unwrap();
        let mut state = State::default();
        state.metadata.routes.insert("one".into(), [1].into());
        db.write_snapshot(&super::super::make_snapshot(&state).unwrap())
            .unwrap();
        db.db
            .update(|tx| {
                let bucket = tx.bucket(b"routes")?.unwrap();
                let row = bucket.get(&business_key(b"one"))?.unwrap();
                bucket.delete(&business_key(b"one"))?;
                if !remove {
                    bucket.put(&business_key(b"two"), &row)?;
                }
                Ok(())
            })
            .unwrap();
        assert!(db.read_snapshot().is_err());
        drop(db);
        assert!(Checkpoint::open(&p, "test", 1).is_err());
        std::fs::remove_dir_all(p).unwrap();
    }
}
#[test]
fn metadata_budget_remains_enforced_before_transaction() {
    let p = dir();
    let mut db = Checkpoint::open(&p, "test", 1).unwrap();
    let mut state = State::default();
    state
        .metadata
        .routes
        .insert("k".repeat(METADATA_LIMIT), [1].into());
    let saved = super::super::make_snapshot(&state).unwrap();
    assert!(db.write_snapshot(&saved).is_err());
    assert!(db.read_snapshot().unwrap().is_none());
    drop(db);
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn multipage_realistic_metadata_roundtrips_all_rows() {
    use crate::model::{Client, DurableOwner, Node};
    let p = dir();
    let mut db = Checkpoint::open(&p, "test", 1).unwrap();
    let mut state = State::default();
    for n in 1..=3 {
        state.metadata.nodes.insert(
            n,
            Node {
                id: n,
                incarnation: format!("inc-{n}"),
                address: format!("127.0.0.1:{}", 19000 + n),
                expires_at: 10000,
                max_lease_ms: 30000,
                applied: 2000,
            },
        );
    }
    for c in 0..10 {
        let filters = (0..210)
            .map(|i| format!("gate/{c}/{i}"))
            .collect::<Vec<_>>();
        state.metadata.clients.insert(
            format!("client-{c}"),
            Client {
                node: 1,
                incarnation: "inc-1".into(),
                generation: 1,
                filters: filters.clone(),
            },
        );
        state.metadata.durable.insert(
            format!("c:client-{c}"),
            DurableOwner {
                node: 1,
                incarnation: "inc-1".into(),
                generation: 1,
                epoch: 1,
                filters: filters.clone(),
                logical_generation: 1,
            },
        );
        for filter in filters {
            state.metadata.routes.insert(filter, [1].into());
        }
    }
    let full = state.clone();
    for count in [100, 256, 512, 768, 1024, 1280, 1536, 1792, 2048, 2100] {
        state = full.clone();
        state.metadata.routes = full
            .metadata
            .routes
            .iter()
            .take(count)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        state.metadata.revision = count as u64;
        let saved = super::super::make_snapshot(&state).unwrap();
        db.write_snapshot(&saved).unwrap();
        db.db
            .view(|tx| {
                let bucket = tx.bucket(b"routes")?.unwrap();
                for key in state.metadata.routes.keys() {
                    assert!(
                        bucket.get(&business_key(key.as_bytes()))?.is_some(),
                        "missing direct get count={count} key={key}"
                    );
                }
                let mut prev: Option<Vec<u8>> = None;
                let mut rows = 0;
                bucket.for_each(|key, _| {
                    if let Some(old) = prev.as_ref() {
                        assert!(
                            old.as_slice() < key,
                            "iterator nonmonotonic count={count} row={rows}"
                        );
                    }
                    prev = Some(key.to_vec());
                    rows += 1;
                    Ok(())
                })?;
                assert_eq!(rows, state.metadata.routes.len(), "count={count}");
                Ok(())
            })
            .unwrap();
        assert_eq!(
            db.read_snapshot().unwrap().unwrap().data,
            saved.data,
            "count={count}"
        );
    }
    let saved = super::super::make_snapshot(&state).unwrap();
    drop(db);
    let db = Checkpoint::open(&p, "test", 1).unwrap();
    assert_eq!(db.read_snapshot().unwrap().unwrap().data, saved.data);
    drop(db);
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn long_business_keys_roundtrip_and_delete() {
    use crate::model::Client;
    let p = dir();
    let mut db = Checkpoint::open(&p, "test", 1).unwrap();
    let mut state = State::default();
    state.metadata.routes.insert("r".repeat(32769), [1].into());
    state.metadata.routes.insert("t".repeat(65535), [1].into());
    state.metadata.clients.insert(
        format!("c:{}", "x".repeat(65535)),
        Client {
            node: 1,
            incarnation: "i".into(),
            generation: 1,
            filters: vec![],
        },
    );
    let saved = super::super::make_snapshot(&state).unwrap();
    db.write_snapshot(&saved).unwrap();
    assert_eq!(db.read_snapshot().unwrap().unwrap().data, saved.data);
    drop(db);
    let mut db = Checkpoint::open(&p, "test", 1).unwrap();
    assert_eq!(db.read_snapshot().unwrap().unwrap().data, saved.data);
    state.metadata.routes.clear();
    state.metadata.clients.clear();
    let saved = super::super::make_snapshot(&state).unwrap();
    db.write_snapshot(&saved).unwrap();
    drop(db);
    let db = Checkpoint::open(&p, "test", 1).unwrap();
    assert_eq!(db.read_snapshot().unwrap().unwrap().data, saved.data);
    drop(db);
    std::fs::remove_dir_all(p).unwrap();
}

#[test]
fn physical_collision_and_logical_envelope_bounds_fail_closed() {
    let mut rows = BTreeMap::new();
    let hash = business_key(b"a");
    insert_row(&mut rows, hash.clone(), vec![1]).unwrap();
    assert!(insert_row(&mut rows, hash, vec![2]).is_err());
    let value = business_record(b"routes", b"a", b"[]").unwrap();
    assert!(business_unpack(b"routes", &business_key(b"b"), &value).is_err());
    let mut invalid = value;
    invalid[..4].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(business_unpack(b"routes", &business_key(b"a"), &invalid).is_err());
}
#[test]
fn native_bbolt_sorted_hash_growth_preserves_old_keys() {
    let p = dir();
    let db = bbolt::Db::open(p.join("native.bbolt"), 0o600, None).unwrap();
    db.update(|tx| {
        tx.create_bucket(b"rows")?;
        Ok(())
    })
    .unwrap();
    let full = (0..10)
        .flat_map(|c| (0..210).map(move |i| format!("gate/{c}/{i}")))
        .collect::<std::collections::BTreeSet<_>>();
    for count in [100, 256, 512, 768, 1024, 1280, 1536, 1792, 2048, 2100] {
        let rows = full
            .iter()
            .take(count)
            .map(|k| {
                (
                    business_key(k.as_bytes()),
                    business_record(b"routes", k.as_bytes(), b"[1]").unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        db.update(|tx| {
            let bucket = tx.bucket(b"rows")?.unwrap();
            for (key, value) in &rows {
                if bucket.get(key)?.as_deref() != Some(value) {
                    bucket.put(key, value)?;
                }
            }
            Ok(())
        })
        .unwrap();
        db.view(|tx| {
            let bucket = tx.bucket(b"rows")?.unwrap();
            for key in rows.keys() {
                assert!(
                    bucket.get(key)?.is_some(),
                    "native missing after growth count={count}"
                );
            }
            Ok(())
        })
        .unwrap();
    }
    drop(db);
    std::fs::remove_dir_all(p).unwrap();
}
