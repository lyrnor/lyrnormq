use super::*;
use crate::replica::store::Journal;
use mqtt_cluster_core::DeliveryId;
use std::time::{SystemTime, UNIX_EPOCH};

fn path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "rmqtt-memory-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn journal(path: &Path) -> Journal {
    Journal::open(
        path,
        ("memory-test", 101),
        &[(101, "http://127.0.0.1:7101".into())],
    )
    .unwrap()
}
fn token(generation: u64) -> RepairToken {
    RepairToken {
        id: format!("repair-{generation}"),
        generation,
    }
}
fn record(epoch: u64, physical: u64, payload: &[u8]) -> Record {
    let mut r = Record::empty(epoch);
    r.physical_revision = physical;
    r.snapshot = Some(payload.to_vec());
    r
}
async fn write(j: &Journal, key: &str, r: Record) {
    j.execute(Request::Write {
        key: key.into(),
        record: r,
    })
    .await
    .unwrap();
}
async fn read(j: &Journal, key: &str) -> Option<Record> {
    j.execute(Request::Read { key: key.into() })
        .await
        .unwrap()
        .record
}

#[tokio::test]
async fn no_body_files_and_reopen_loses_all_data_and_identity() {
    let p = path("volatile");
    let j = journal(&p);
    let old = j.disk_id().to_owned();
    write(&j, "client", record(1, 1, b"private-payload-secret")).await;
    assert_eq!(
        read(&j, "client").await.unwrap().snapshot,
        Some(b"private-payload-secret".to_vec())
    );
    let names = std::fs::read_dir(&p)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(
        names
            .iter()
            .all(|s| s == "data.lock" || s == "memory.identity")
    );
    for n in names {
        assert!(
            !std::fs::read(p.join(n))
                .unwrap()
                .windows(22)
                .any(|w| w == b"private-payload-secret")
        );
    }
    drop(j);
    let j = journal(&p);
    assert_ne!(j.disk_id(), old);
    assert!(read(&j, "client").await.is_none());
    assert!(j.voting_ready(0));
    drop(j);
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn legacy_sqlite_is_refused_and_never_modified() {
    let p = path("legacy");
    std::fs::create_dir_all(&p).unwrap();
    std::fs::write(p.join("data.sqlite"), b"preserved old state").unwrap();
    assert!(Journal::open(&p, ("memory-test", 101), &[]).is_err());
    assert_eq!(
        std::fs::read(p.join("data.sqlite")).unwrap(),
        b"preserved old state"
    );
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn identity_and_directory_ownership_are_guarded() {
    let p = path("identity");
    let j = journal(&p);
    assert!(Journal::open(&p, ("memory-test", 101), &[]).is_err());
    drop(j);
    assert!(Journal::open(&p, ("other-cluster", 101), &[]).is_err());
    std::fs::remove_dir_all(p).unwrap();
}
#[tokio::test]
async fn fence_returns_previous_complete_value_and_prevents_stale_writes() {
    let p = path("fence");
    let j = journal(&p);
    write(&j, "client", record(3, 1, b"accepted")).await;
    let fenced = j
        .execute(Request::Fence {
            key: "client".into(),
            epoch: 9,
        })
        .await
        .unwrap()
        .record
        .unwrap();
    assert_eq!(fenced.snapshot, Some(b"accepted".to_vec()));
    assert!(
        j.execute(Request::Write {
            key: "client".into(),
            record: record(8, 100, b"stale")
        })
        .await
        .is_err()
    );
    write(&j, "client", record(9, 0, b"new-owner")).await;
    assert_eq!(read(&j, "client").await.unwrap().epoch, 9);
    drop(j);
    std::fs::remove_dir_all(p).unwrap();
}
#[tokio::test]
async fn same_version_is_idempotent_only_for_identical_values() {
    let p = path("version");
    let j = journal(&p);
    write(&j, "client", record(2, 5, b"original")).await;
    write(&j, "client", record(2, 5, b"original")).await;
    assert!(
        j.execute(Request::Write {
            key: "client".into(),
            record: record(2, 5, b"conflict")
        })
        .await
        .is_err()
    );
    assert!(
        j.execute(Request::Write {
            key: "client".into(),
            record: record(2, 4, b"old")
        })
        .await
        .is_err()
    );
    assert_eq!(
        read(&j, "client").await.unwrap().snapshot,
        Some(b"original".to_vec())
    );
    drop(j);
    std::fs::remove_dir_all(p).unwrap();
}
#[tokio::test]
async fn freeze_blocks_all_old_generation_operations_and_resumes_exactly() {
    let p = path("freeze");
    let j = journal(&p);
    write(&j, "client", record(1, 1, b"stable")).await;
    let a = j.freeze(token(1)).await.unwrap();
    assert!(!j.voting_ready(0));
    assert!(a.manifest.is_some());
    for r in [
        Request::Read {
            key: "client".into(),
        },
        Request::Fence {
            key: "client".into(),
            epoch: 2,
        },
        Request::Write {
            key: "client".into(),
            record: record(1, 2, b"late"),
        },
    ] {
        assert!(j.execute_at(0, r).await.is_err());
    }
    j.resume(token(1), false).await.unwrap();
    assert!(j.voting_ready(1));
    assert!(!j.voting_ready(0));
    assert!(
        j.execute(Request::Write {
            key: "client".into(),
            record: record(1, 2, b"late")
        })
        .await
        .is_err()
    );
    assert_eq!(
        j.execute_at(
            1,
            Request::Read {
                key: "client".into()
            }
        )
        .await
        .unwrap()
        .record
        .unwrap()
        .snapshot,
        Some(b"stable".to_vec())
    );
    assert!(j.resume(token(1), false).await.is_ok());
    drop(j);
    std::fs::remove_dir_all(p).unwrap();
}
#[tokio::test]
async fn paged_copy_installs_typed_values_and_fence_only_entries_atomically() {
    let p = path("source");
    let q = path("target");
    let source = journal(&p);
    let target = journal(&q);
    for i in 0..70 {
        write(&source, &format!("client-{i:03}"), record(1, 1, b"message")).await;
    }
    source
        .execute(Request::Fence {
            key: "tombstone".into(),
            epoch: 7,
        })
        .await
        .unwrap();
    let frozen = source.freeze(token(1)).await.unwrap();
    let expected = frozen.manifest.unwrap();
    assert_eq!(expected.records, 71);
    target
        .begin_import(token(1), expected.clone())
        .await
        .unwrap();
    let first = source.export_page(token(1), None, 64).await.unwrap();
    assert_eq!(first.entries.len(), 64);
    assert!(!first.done);
    target
        .append_page(token(1), 0, first.entries.clone())
        .await
        .unwrap();
    target
        .append_page(token(1), 0, first.entries)
        .await
        .unwrap();
    assert!(target.finish_import(token(1)).await.is_err());
    let second = source.export_page(token(1), first.next, 64).await.unwrap();
    assert!(second.done);
    target
        .append_page(token(1), 1, second.entries)
        .await
        .unwrap();
    let installed = target.finish_import(token(1)).await.unwrap();
    assert!(installed.installed);
    assert_eq!(installed.manifest, Some(expected));
    assert!(!target.voting_ready(0));
    target.resume(token(1), false).await.unwrap();
    assert!(target.voting_ready(1));
    assert!(
        target
            .execute_at(
                1,
                Request::Write {
                    key: "tombstone".into(),
                    record: record(6, 8, b"stale")
                }
            )
            .await
            .is_err()
    );
    assert_eq!(
        target
            .execute_at(
                1,
                Request::Read {
                    key: "client-069".into()
                }
            )
            .await
            .unwrap()
            .record
            .unwrap()
            .snapshot,
        Some(b"message".to_vec())
    );
    drop(source);
    drop(target);
    std::fs::remove_dir_all(p).unwrap();
    std::fs::remove_dir_all(q).unwrap();
}
#[tokio::test]
async fn invalid_page_and_wrong_token_leave_no_partial_import() {
    let p = path("badpage-source");
    let q = path("badpage-target");
    let s = journal(&p);
    let t = journal(&q);
    write(&s, "a", record(1, 1, b"source")).await;
    write(&t, "old", record(1, 1, b"old")).await;
    let m = s.freeze(token(1)).await.unwrap().manifest.unwrap();
    t.begin_import(token(1), m).await.unwrap();
    let page = s.export_page(token(1), None, 64).await.unwrap();
    assert!(
        t.append_page(token(2), 0, page.entries.clone())
            .await
            .is_err()
    );
    let mut bad = page.entries.clone();
    bad.push(RepairEntry {
        key: "b".into(),
        min_epoch: 0,
        record: None,
    });
    assert!(t.append_page(token(1), 0, bad).await.is_err());
    assert_eq!(t.status().await.unwrap().next_page, 0);
    t.append_page(token(1), 0, page.entries.clone())
        .await
        .unwrap();
    let mut changed = page.entries;
    changed[0].min_epoch = 2;
    assert!(t.append_page(token(1), 0, changed).await.is_err());
    t.finish_import(token(1)).await.unwrap();
    t.resume(token(1), false).await.unwrap();
    assert!(
        t.execute_at(1, Request::Read { key: "old".into() })
            .await
            .unwrap()
            .record
            .is_none()
    );
    drop(s);
    drop(t);
    std::fs::remove_dir_all(p).unwrap();
    std::fs::remove_dir_all(q).unwrap();
}
#[tokio::test]
async fn restarted_learner_loses_staging_and_cannot_claim_installed_proof() {
    let p = path("lost-staging");
    let j = journal(&p);
    let old = j.disk_id().to_owned();
    let expected = RepairManifest {
        records: 1,
        bytes: 500,
        digest: "0".repeat(64),
    };
    j.begin_import(token(1), expected).await.unwrap();
    drop(j);
    let j = journal(&p);
    assert_ne!(j.disk_id(), old);
    let status = j.status().await.unwrap();
    assert!(!status.installed);
    assert!(status.manifest.is_none());
    assert!(j.finish_import(token(1)).await.is_err());
    drop(j);
    std::fs::remove_dir_all(p).unwrap();
}
#[tokio::test]
async fn cached_quota_accounting_tracks_replacements_and_keys() {
    let p = path("accounting");
    let store = MemoryStore::open(&p, ("quota", 1), &[]).unwrap();
    for i in 0..100 {
        store
            .execute(
                0,
                Request::Write {
                    key: format!("key-{i}"),
                    record: record(1, 1, b"larger-value"),
                },
            )
            .unwrap();
    }
    for i in 0..100 {
        store
            .execute(
                0,
                Request::Write {
                    key: format!("key-{i}"),
                    record: record(1, 2, b"x"),
                },
            )
            .unwrap();
    }
    let state = store.state.lock().unwrap();
    let computed = state
        .records
        .iter()
        .map(|(k, v)| v.charge(k))
        .sum::<usize>();
    assert_eq!(state.bytes, computed);
    assert!(state.bytes > 100 * "x".len());
    drop(state);
    let too_large = record(1, 3, &vec![0; 2 * 1024 * 1024 + 1]);
    assert!(
        store
            .execute(
                0,
                Request::Write {
                    key: "key-0".into(),
                    record: too_large
                }
            )
            .is_err()
    );
    assert_eq!(store.state.lock().unwrap().bytes, computed);
    drop(store);
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn receipt_watermarks_remain_bounded_and_accept_legacy_shape() {
    let a = DeliveryId {
        source: "source".into(),
        epoch: 2,
        revision: 8,
        index: 0,
        target_generation: 1,
    };
    let mut b = a.clone();
    b.revision = 9;
    let bytes = serde_json::to_vec(&vec![a.clone(), b.clone()]).unwrap();
    let mut r: Receipts = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(r.len(), 1);
    assert!(r.contains(&a));
    assert!(r.contains(&b));
    r.clear();
    assert_eq!(r.len(), 0);
}

#[test]
fn physical_record_headroom_admits_internal_keys_beyond_100k_clients_but_stays_bounded() {
    let p = path("record-headroom");
    let store = MemoryStore::open(
        &p,
        ("memory-test", 101),
        &[(101, "http://127.0.0.1:7101".into())],
    )
    .unwrap();
    {
        let mut state = store.state.lock().unwrap();
        for client in 0..100_000 {
            let key = format!("c:{client:06}");
            state.bytes += key.len();
            state.records.insert(
                key,
                Entry {
                    min_epoch: 1,
                    value: None,
                },
            );
        }
    }
    assert!(
        store
            .execute(
                0,
                Request::Fence {
                    key: "h:internal".into(),
                    epoch: 1
                }
            )
            .is_ok(),
        "100k client records must leave finite internal-record headroom"
    );
    {
        let mut state = store.state.lock().unwrap();
        for internal in state.records.len()..101_024 {
            let key = format!("h:{internal:06}");
            state.bytes += key.len();
            state.records.insert(
                key,
                Entry {
                    min_epoch: 1,
                    value: None,
                },
            );
        }
    }
    assert_eq!(store.state.lock().unwrap().records.len(), 101_024);
    assert!(
        store
            .execute(
                0,
                Request::Fence {
                    key: "c:000000".into(),
                    epoch: 2
                }
            )
            .is_ok(),
        "existing records can still advance at the physical cap"
    );
    assert!(
        store
            .execute(
                0,
                Request::Fence {
                    key: "h:overflow".into(),
                    epoch: 1
                }
            )
            .is_err()
    );
    let mut manifest = RepairManifest {
        records: 101_024,
        bytes: 0,
        digest: "0".repeat(64),
    };
    assert!(validate_manifest(&manifest).is_ok());
    manifest.records += 1;
    assert!(validate_manifest(&manifest).is_err());
    drop(store);
    std::fs::remove_dir_all(p).unwrap();
}
