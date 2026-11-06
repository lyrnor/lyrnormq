use super::*;

fn owner(filters: &[&str], generation: u64) -> DurableOwner {
    DurableOwner {
        node: 101,
        incarnation: "disk-a".into(),
        generation: 7,
        epoch: 8,
        logical_generation: generation,
        filters: filters.iter().map(|s| (*s).into()).collect(),
        acquired_from: None,
    }
}

// Independent pre-optimization implementation is the semantic oracle.
fn scan(metadata: &Metadata, topic: &str) -> Vec<(String, u64)> {
    metadata
        .durable
        .iter()
        .filter(|(key, owner)| {
            key.starts_with("c:")
                && owner
                    .filters
                    .iter()
                    .any(|filter| mqtt_router::matches_topic_filter(filter, topic))
        })
        .map(|(key, owner)| (key[2..].to_owned(), owner.logical_generation))
        .collect()
}

fn indexed(index: &RecipientIndex, topic: &str) -> Vec<(String, u64)> {
    index
        .matching(topic)
        .into_iter()
        .map(|r| (r.client_id, r.generation))
        .collect()
}

#[test]
fn snapshot_matches_old_scan_for_wildcards_system_topics_and_literal_shared_prefix() {
    let mut metadata = Metadata::default();
    for (client, filters) in [
        (
            "z",
            vec!["sensors/+/reading", "sensors/#", "sensors/+/reading"],
        ),
        ("a", vec!["#", "+/events"]),
        ("system", vec!["$SYS/events/#"]),
        ("literal", vec!["$share/team/topic", "$share/+/other"]),
        ("empty-level", vec!["/+/", "a//b", "a/#"]),
        ("invalid", vec!["x/#/y", "a+", ""]),
        ("none", vec![]),
    ] {
        metadata
            .durable
            .insert(format!("c:{client}"), owner(&filters, 11));
    }
    metadata
        .durable
        .insert("system-owner".into(), owner(&["#"], 99));
    let index = RecipientIndex::from_metadata(&metadata);
    for topic in [
        "sensors/room/reading",
        "sensors",
        "sensors/",
        "x/events",
        "$SYS/events/client/connected",
        "$OTHER/events",
        "topic",
        "$share/team/topic",
        "$share/team/other",
        "/room/",
        "a//b",
        "a",
        "",
        "bad/+",
        "bad/#",
        "bad\0topic",
    ] {
        assert_eq!(indexed(&index, topic), scan(&metadata, topic), "{topic:?}");
    }
}

#[test]
fn replacement_delete_and_generation_changes_are_incremental_and_deduplicated() {
    let mut metadata = Metadata::default();
    let mut index = RecipientIndex::default();
    // Update order differs from lexical client order; results must retain old ordering.
    for (key, filters) in [
        ("c:z", vec!["a/#", "a/+"]),
        ("c:a", vec!["a/b"]),
        ("c:m", vec!["#"]),
    ] {
        let next = owner(&filters, 1);
        index.replace(key, Some(&next));
        metadata.durable.insert(key.into(), next);
    }
    assert_eq!(indexed(&index, "a/b"), scan(&metadata, "a/b"));
    for (key, next) in [
        ("c:z", Some(owner(&["a/#", "a/+"], 22))),
        ("c:a", Some(owner(&["$SYS/#"], 33))),
        ("c:m", None),
        ("c:z", Some(owner(&[], 44))),
        ("c:a", None),
        ("c:fresh", Some(owner(&["a/#"], 55))),
    ] {
        index.replace(key, next.as_ref());
        if let Some(next) = next {
            metadata.durable.insert(key.into(), next);
        } else {
            metadata.durable.remove(key);
        }
        for topic in ["a", "a/b", "$SYS/events/x", "unmatched"] {
            assert_eq!(indexed(&index, topic), scan(&metadata, topic));
        }
        assert!(
            index.slots.len() <= 3,
            "removed numeric slots must be reusable"
        );
    }
}

#[test]
fn unsubscribed_directory_has_no_indexed_owners_and_churn_does_not_accumulate_slots() {
    let mut metadata = Metadata::default();
    for n in 0..10_000 {
        metadata.durable.insert(format!("c:{n}"), owner(&[], n));
    }
    let mut index = RecipientIndex::from_metadata(&metadata);
    assert!(index.entries.is_empty());
    assert!(index.slots.is_empty());
    assert!(indexed(&index, "$SYS/events/client/connected").is_empty());
    for n in 0..10_000 {
        let key = format!("c:temporary-{n}");
        index.replace(&key, Some(&owner(&["device/+"], n)));
        assert_eq!(indexed(&index, "device/one"), vec![(key[2..].into(), n)]);
        index.replace(&key, None);
        assert!(index.entries.is_empty());
        assert!(index.slots.len() <= 1);
    }
}

#[test]
fn generated_filter_combinations_match_scan_before_and_after_sparse_changes() {
    let filters = [
        "#",
        "+",
        "a",
        "a/#",
        "a/+",
        "a//+",
        "/#",
        "$SYS/#",
        "$share/g/#",
    ];
    let topics = [
        "a",
        "a/b",
        "a/",
        "a//c",
        "b",
        "/",
        "/b",
        "$SYS/online",
        "$share/g/x",
    ];
    let mut metadata = Metadata::default();
    for n in 0..81 {
        metadata.durable.insert(
            format!("c:{n:03}"),
            owner(&[filters[n / 9], filters[n % 9]], n as u64),
        );
    }
    let mut index = RecipientIndex::from_metadata(&metadata);
    for n in 0..81 {
        let key = format!("c:{n:03}");
        let next = owner(&[filters[(n + 3) % 9]], 1000 + n as u64);
        index.replace(&key, Some(&next));
        metadata.durable.insert(key, next);
        for topic in topics {
            assert_eq!(indexed(&index, topic), scan(&metadata, topic));
        }
    }
}

#[derive(Debug)]
struct Local;
impl mqtt_cluster_core::LocalBroker for Local {
    fn deliver(
        &self,
        _: mqtt_cluster_core::ClusterMessage,
    ) -> mqtt_cluster_core::ClusterFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn fence(&self, _: String, _: u64) -> mqtt_cluster_core::ClusterFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn fence_all(&self) -> mqtt_cluster_core::ClusterFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
    fn subscriptions(
        &self,
    ) -> mqtt_cluster_core::ClusterFuture<'_, Vec<mqtt_cluster_core::ClientSubscriptions>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

#[tokio::test]
async fn runtime_snapshot_and_sparse_delta_publish_coherent_index_and_reject_gaps()
-> std::io::Result<()> {
    use crate::{broker::BrokerRuntime, model::MetadataUpdate};
    use std::sync::Arc;
    let config = serde_json::from_value(serde_json::json!({
        "role":"broker", "cluster_id":"index-test", "node_id":101,
        "listen":"127.0.0.1:7101", "data_dir":"unused-index-test", "token_file":"unused-token",
        "controllers":[{"id":1,"address":"http://127.0.0.1:7001"},{"id":2,"address":"http://127.0.0.1:7002"},{"id":3,"address":"http://127.0.0.1:7003"}]
    })).map_err(std::io::Error::other)?;
    let runtime = BrokerRuntime::new(config)?;
    *runtime.local.lock().unwrap() = Some(Arc::new(Local));
    let mut metadata = Metadata {
        revision: 1,
        ..Metadata::default()
    };
    metadata.durable.insert("c:z".into(), owner(&["a/#"], 1));
    runtime
        .install_update(MetadataUpdate::Snapshot(metadata.clone()))
        .await?;
    let slot = runtime.recipients.read().unwrap().entries["c:z"].slot;
    for (key, next_owner) in [
        ("c:a", Some(owner(&["a/+"], 2))),
        ("c:z", Some(owner(&["a/#"], 3))),
        ("c:no-subscriptions", Some(owner(&[], 4))),
        ("c:a", Some(owner(&["$SYS/#"], 5))),
        ("c:z", None),
    ] {
        let mut next = metadata.clone();
        next.revision += 1;
        if let Some(owner) = next_owner {
            next.durable.insert(key.into(), owner);
        } else {
            next.durable.remove(key);
        }
        let delta = MetadataUpdate::between(&metadata, &next);
        runtime.install_update(delta.clone()).await?;
        for topic in ["a", "a/b", "$SYS/events/x"] {
            let actual: Vec<_> = runtime
                .durable_recipients(topic)
                .into_iter()
                .map(|r| (r.client_id, r.generation))
                .collect();
            assert_eq!(actual, scan(&next, topic));
        }
        if next.durable.contains_key("c:z") {
            assert_eq!(
                runtime.recipients.read().unwrap().entries["c:z"].slot,
                slot,
                "sparse updates must not rebuild/reassign unaffected indexed owners"
            );
        }
        assert!(
            runtime.install_update(delta).await.is_err(),
            "replayed delta is a gap"
        );
        assert_eq!(runtime.metadata.read().unwrap().revision, next.revision);
        metadata = next;
    }
    let mut snapshot = Metadata {
        revision: metadata.revision + 1,
        ..Metadata::default()
    };
    snapshot
        .durable
        .insert("c:new".into(), owner(&["fresh/#"], 100));
    runtime
        .install_update(MetadataUpdate::Snapshot(snapshot.clone()))
        .await?;
    assert!(runtime.durable_recipients("a/b").is_empty());
    assert!(runtime.durable_recipients("$SYS/events/x").is_empty());
    let actual: Vec<_> = runtime
        .durable_recipients("fresh/x")
        .into_iter()
        .map(|r| (r.client_id, r.generation))
        .collect();
    assert_eq!(actual, scan(&snapshot, "fresh/x"));
    // An older snapshot must not replace either half of the published view.
    assert!(
        runtime
            .install_update(MetadataUpdate::Snapshot(metadata))
            .await
            .is_err()
    );
    assert_eq!(runtime.metadata.read().unwrap().revision, snapshot.revision);
    assert_eq!(runtime.durable_recipients("fresh/x").len(), 1);
    Ok(())
}

#[test]
fn indexed_match_forwards_only_remote_owners() {
    let mut metadata = Metadata::default();
    metadata.durable.insert(
        "c:local".into(),
        DurableOwner {
            node: 101,
            incarnation: "self".into(),
            generation: 1,
            epoch: 1,
            logical_generation: 1,
            filters: vec!["sensors/room".into()],
            acquired_from: None,
        },
    );
    metadata.durable.insert(
        "c:remote".into(),
        DurableOwner {
            node: 102,
            incarnation: "other".into(),
            generation: 1,
            epoch: 1,
            logical_generation: 1,
            filters: vec!["sensors/room".into()],
            acquired_from: None,
        },
    );
    metadata.nodes.insert(
        101,
        crate::model::Node {
            id: 101,
            incarnation: "self".into(),
            address: "http://127.0.0.1:1".into(),
            expires_at: 1,
            max_lease_ms: 3000,
            applied: 1,
        },
    );
    metadata.nodes.insert(
        102,
        crate::model::Node {
            id: 102,
            incarnation: "other".into(),
            address: "http://127.0.0.1:2".into(),
            expires_at: 1,
            max_lease_ms: 3000,
            applied: 1,
        },
    );
    let index = RecipientIndex::from_metadata(&metadata);
    let matched = index.matching("sensors/room");
    assert_eq!(
        RecipientIndex::remote_addresses(&metadata, &matched, 101, "self"),
        vec!["http://127.0.0.1:2".to_owned()]
    );
}
