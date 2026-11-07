use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use super::*;
const CLIENTS: usize = 16_384;
const BATCH: usize = 64;

fn command_entry(index: u64, command: Command) -> Entry {
    Entry {
        log_id: LogId::new(crate::model::committed_leader_id(1, 1), index),
        payload: EntryPayload::Normal(command),
    }
}

async fn apply_one(store: &mut Store, entry: Entry) -> Result<(), Box<dyn std::error::Error>> {
    prime(store, vec![entry.clone()], Some(entry.log_id)).await?;
    let answers = store.apply_log([entry]).await?;
    for answer in answers {
        if let Some(error) = answer.error {
            return Err(error.into());
        }
        for child in answer.results {
            if let Some(error) = child.error {
                return Err(error.into());
            }
        }
    }
    Ok(())
}

fn assert_round_trip(store: &Store) {
    let metadata = store.metadata();
    assert_eq!(metadata.clients.len(), 2, "released client must stay gone");
    assert!(!metadata.clients.contains_key("c1"));
    assert_eq!(metadata.clients["c0"].filters, vec!["sensor/#".to_owned()]);
    assert!(metadata.clients["c2"].filters.is_empty());
    assert!(
        metadata
            .routes
            .get("sensor/#")
            .is_some_and(|nodes| nodes.contains(&1))
    );
    assert_eq!(metadata.nodes[&1].expires_at, 9_000);
    assert_eq!(metadata.nodes[&1].applied, 4);
}

#[tokio::test]
async fn incremental_checkpoint_round_trips_release_filter_and_heartbeat()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "pressure", 1).await?;
    apply_one(&mut store, entry(0, 1)).await?;
    apply_one(
        &mut store,
        command_entry(
            1,
            Command::Batch {
                commands: (0..3)
                    .map(|id| Command::Claim {
                        node: 1,
                        incarnation: "node-1".into(),
                        client: format!("c{id}"),
                        generation: 1,
                    })
                    .collect(),
            },
        ),
    )
    .await?;
    apply_one(
        &mut store,
        command_entry(
            2,
            Command::Release {
                node: 1,
                incarnation: "node-1".into(),
                client: "c1".into(),
                generation: 1,
            },
        ),
    )
    .await?;
    apply_one(
        &mut store,
        command_entry(
            3,
            Command::Filters {
                generation: 1,
                node: 1,
                incarnation: "node-1".into(),
                client: "c0".into(),
                filters: vec!["sensor/#".into()],
            },
        ),
    )
    .await?;
    let heartbeat = Command::Heartbeat {
        node: 1,
        incarnation: "node-1".into(),
        applied: 4,
        expires_at: 9_000,
        lease_ms: 3_000,
    };
    let heartbeat = command_entry(4, heartbeat);
    apply_one(&mut store, heartbeat.clone()).await?;
    let snapshot = store.build_snapshot().await?;
    assert_eq!(snapshot.meta.last_log_id, Some(heartbeat.log_id));
    assert_round_trip(&store);
    drop(snapshot);
    drop(store);
    let store = Store::open(&dir, "pressure", 1).await?;
    assert_round_trip(&store);
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn incremental_flush_does_not_hold_the_wal_lock_across_16k_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "pressure", 1).await?;
    apply_one(&mut store, entry(0, 1)).await?;
    let stop = Arc::new(AtomicBool::new(false));
    let probe = tokio::spawn({
        let store = store.clone();
        let stop = Arc::clone(&stop);
        async move {
            let mut peak = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let started = std::time::Instant::now();
                store.run_wal(|wal| wal.state()).await?;
                let elapsed = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
                peak = peak.max(elapsed);
            }
            Ok::<u64, StorageError<Types>>(peak)
        }
    });
    let mut failure = None;
    for batch in 0..(CLIENTS / BATCH) {
        let commands = (0..BATCH)
            .map(|offset| {
                let id = batch * BATCH + offset;
                Command::Claim {
                    node: 1,
                    incarnation: "node-1".into(),
                    client: format!("c{id}"),
                    generation: 1,
                }
            })
            .collect();
        let entry = command_entry(batch as u64 + 1, Command::Batch { commands });
        if let Err(error) = apply_one(&mut store, entry).await {
            failure = Some(error);
            break;
        }
    }
    if failure.is_none()
        && let Err(error) = store.build_snapshot().await
    {
        failure = Some(error.into());
    }
    stop.store(true, Ordering::Relaxed);
    let waited = match probe.await {
        Ok(Ok(peak)) => peak,
        Ok(Err(error)) => return Err(error.into()),
        Err(error) => return Err(error.into()),
    };
    if let Some(error) = failure {
        return Err(error);
    }
    // The probe shares the mutex with append fsync. One sync on a shared CI
    // disk can pass 100ms. A checkpoint that kept the mutex across this flush
    // would block the probe for the whole 16384-client write, which is seconds.
    assert!(
        waited < 1_000_000,
        "Raft WAL wait peaked at {waited}us while incremental checkpoints flushed {CLIENTS} clients"
    );
    println!("raft wal wait peak {waited}us");
    assert_eq!(store.metadata().clients.len(), CLIENTS);
    drop(store);
    let store = Store::open(&dir, "pressure", 1).await?;
    assert_eq!(store.metadata().clients.len(), CLIENTS);
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
