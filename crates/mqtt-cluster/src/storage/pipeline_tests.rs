//! Multiple outstanding Raft batches retain durable prefix and child-result semantics.
use super::tests::{directory, entry};
use super::*;
use openraft::EntryPayload;
use std::time::Duration;

fn acquire(client: &str, generation: u64, expected_epoch: u64) -> Command {
    Command::AcquireOnce {
        key: format!("c:{client}"),
        node: 1,
        incarnation: "node-1".into(),
        generation,
        expected_epoch,
    }
}

fn filters(client: &str, epoch: u64, generation: u64, filter: &str) -> Command {
    Command::DurableFilters {
        key: format!("c:{client}"),
        node: 1,
        incarnation: "node-1".into(),
        epoch,
        filters: vec![filter.into()],
        logical_generation: generation,
    }
}

fn entries() -> Vec<Entry> {
    let mut registration = entry(0, 1);
    if let EntryPayload::Normal(Command::Register {
        replicas,
        memory,
        disk_id,
        ..
    }) = &mut registration.payload
    {
        *replicas = Some(
            (1..=3)
                .map(|node| (node, format!("http://127.0.0.1:{}", 10000 + node)))
                .collect(),
        );
        *memory = true;
        *disk_id = Some(format!("{:032x}", 1));
    }
    let commands = [
        vec![acquire("a", 1, 0), acquire("b", 1, 0)],
        // A retry in the next log entry retains its original child epoch.
        vec![acquire("a", 1, 0), filters("a", 2, 11, "a/+")],
        vec![
            Command::Claim {
                node: 1,
                incarnation: "wrong".into(),
                client: "rejected".into(),
                generation: 1,
            },
            acquire("c", 1, 0),
        ],
        vec![acquire("b", 2, 3), filters("b", 6, 22, "b/#")],
    ];
    let mut result = vec![registration];
    result.extend(commands.into_iter().enumerate().map(|(index, commands)| {
        let mut value = entry(index as u64 + 1, 1);
        value.payload = EntryPayload::Normal(Command::Batch { commands });
        value
    }));
    result
}

#[allow(clippy::result_large_err)]
async fn append(store: &Store, entries: Vec<Entry>) -> Result<(), StorageError<Types>> {
    let inner = store.inner.clone();
    store
        .run_wal(move |wal| log::append_checked(wal, &inner.db, entries))
        .await
}

fn child_outcomes(answers: &[Answer]) -> Vec<Vec<(u64, bool)>> {
    answers
        .iter()
        .map(|answer| {
            assert!(
                answer.error.is_none(),
                "batch envelope must stay successful"
            );
            answer
                .results
                .iter()
                .map(|child| (child.revision, child.error.is_some()))
                .collect()
        })
        .collect()
}

fn assert_complete(metadata: &Metadata) {
    assert_eq!(metadata.revision, 7);
    assert_eq!(metadata.durable.len(), 3);
    assert!(
        metadata.clients.is_empty(),
        "failed child must not claim a client"
    );
    for (client, epoch, generation, filter) in [
        ("c:a", 2, 11, Some("a/+")),
        ("c:b", 6, 22, Some("b/#")),
        ("c:c", 5, 0, None),
    ] {
        let owner = &metadata.durable[client];
        assert_eq!(owner.epoch, epoch);
        assert_eq!(owner.logical_generation, generation);
        assert_eq!(owner.filters.first().map(String::as_str), filter);
    }
}

#[tokio::test]
async fn four_batch_apply_range_preserves_fifo_child_results_and_sparse_watch()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "pipeline", 1).await?;
    let entries = entries();
    append(&store, entries.clone()).await?;
    store.save_committed(Some(entries[4].log_id)).await?;
    // An append/commit is not yet the applied result or a published state update.
    assert_eq!(store.metadata().revision, 0);
    assert_eq!(store.state.lock().unwrap().last_applied, None);
    let answers = store.apply_log(entries.clone()).await?;
    assert_eq!(answers.len(), 5);
    assert_eq!(answers[0].revision, 1);
    assert_eq!(
        child_outcomes(&answers[1..]),
        vec![
            vec![(2, false), (3, false)],
            vec![(2, false), (4, false)],
            vec![(4, true), (5, false)],
            vec![(6, false), (7, false)],
        ]
    );
    assert_complete(&store.metadata());
    assert_eq!(
        store.state.lock().unwrap().last_applied,
        Some(entries[4].log_id)
    );
    let updates = store.watch_updates(Some(0));
    assert_eq!(updates.len(), 5);
    let mut watched = Metadata::default();
    for update in updates {
        assert!(matches!(update, MetadataUpdate::Delta { .. }));
        update.merge_in_place(&mut watched)?;
    }
    assert_eq!(encode(&watched)?, encode(&store.metadata())?);
    drop(store);
    let store = Store::open(&dir, "pipeline", 1).await?;
    assert_complete(&store.metadata());
    assert_eq!(
        store.state.lock().unwrap().last_applied,
        Some(entries[4].log_id)
    );
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn committed_pipeline_prefix_replays_without_speculative_batches_before_and_after_purge()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "pipeline", 1).await?;
    let entries = entries();
    append(&store, entries.clone()).await?;
    store.save_committed(Some(entries[2].log_id)).await?;
    // No RAM apply/checkpoint: reopening must recover only the committed prefix.
    drop(store);
    let mut store = Store::open(&dir, "pipeline", 1).await?;
    let metadata = store.metadata();
    assert_eq!(metadata.revision, 4);
    assert_eq!(metadata.durable.len(), 2);
    assert_eq!(metadata.durable["c:a"].epoch, 2);
    assert_eq!(metadata.durable["c:a"].filters, vec!["a/+"]);
    assert_eq!(metadata.durable["c:b"].epoch, 3);
    assert_eq!(metadata.durable["c:b"].logical_generation, 0);
    assert!(!metadata.durable.contains_key("c:c"));
    assert_eq!(
        store.state.lock().unwrap().last_applied,
        Some(entries[2].log_id)
    );
    assert_eq!(store.try_get_log_entries(3..=4).await?.len(), 2);
    let snapshot = store.build_snapshot().await?;
    assert_eq!(snapshot.meta.last_log_id, Some(entries[2].log_id));
    store.purge(entries[2].log_id).await?;
    drop(snapshot);
    drop(store);
    let mut store = Store::open(&dir, "pipeline", 1).await?;
    assert_eq!(encode(&store.metadata())?, encode(&metadata)?);
    assert_eq!(store.try_get_log_entries(..).await?.len(), 2);
    store.save_committed(Some(entries[4].log_id)).await?;
    // Crash boundary after durable commit, before applying the remaining batches.
    drop(store);
    let mut store = Store::open(&dir, "pipeline", 1).await?;
    assert_complete(&store.metadata());
    let before = encode(&store.metadata())?;
    let mut recovered = store.metadata();
    assert_eq!(recovered.apply(acquire("a", 1, 0)).revision, 2);
    assert_eq!(
        encode(&recovered)?,
        before,
        "retry must not advance an epoch"
    );
    store.build_snapshot().await?;
    store.purge(entries[4].log_id).await?;
    drop(store);
    let mut store = Store::open(&dir, "pipeline", 1).await?;
    assert_eq!(encode(&store.metadata())?, before);
    assert!(store.try_get_log_entries(..).await?.is_empty());
    assert_eq!(store.read_committed().await?, Some(entries[4].log_id));
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}

#[tokio::test]
async fn canceled_apply_caller_keeps_first_range_owned_and_blocks_later_results_until_io()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = directory();
    let mut store = Store::open(&dir, "pipeline", 1).await?;
    let entries = entries();
    append(&store, entries.clone()).await?;
    store.save_committed(Some(entries[4].log_id)).await?;
    store.apply_log(vec![entries[0].clone()]).await?;
    let slots = store.inner.slots.clone().acquire_many_owned(32).await?;
    let first = tokio::spawn({
        let store = store.clone();
        let first_entries = entries[1..=2].to_vec();
        async move { store.apply_log(first_entries).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.apply_gate.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let mut second = tokio::spawn({
        let store = store.clone();
        let second_entries = entries[3..=4].to_vec();
        async move { store.apply_log(second_entries).await }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut second)
            .await
            .is_err(),
        "later apply response must not bypass the owned blocked prefix"
    );
    assert_eq!(store.metadata().revision, 1);
    assert_eq!(
        store.state.lock().unwrap().last_applied,
        Some(entries[0].log_id)
    );
    drop(slots);
    let answers = tokio::time::timeout(Duration::from_secs(2), second).await???;
    assert_eq!(
        child_outcomes(&answers),
        vec![vec![(4, true), (5, false)], vec![(6, false), (7, false)]]
    );
    assert_complete(&store.metadata());
    assert_eq!(
        store.state.lock().unwrap().last_applied,
        Some(entries[4].log_id)
    );
    drop(store);
    let store = Store::open(&dir, "pipeline", 1).await?;
    assert_complete(&store.metadata());
    drop(store);
    std::fs::remove_dir_all(dir)?;
    Ok(())
}
