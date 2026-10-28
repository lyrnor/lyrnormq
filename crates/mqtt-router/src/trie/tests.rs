use super::*;
use crate::filter::matches;

#[test]
fn routing_dedup_and_cleanup() {
    let mut router = Router::default();
    for filter in ["a/#", "a/+", "a/b"] {
        assert!(router.subscribe(1, filter.into()));
    }
    assert!(router.subscribe(2, "a/b".into()));
    assert!(router.subscribe(1, "a/b".into()));
    assert_eq!(router.subscription_count(1), 3);
    assert_eq!(router.matching_clients("a/b"), vec![1, 2]);
    router.unsubscribe(1, "a/#");
    assert_eq!(router.matching_clients("a"), Vec::<u64>::new());
    router.remove_client(1);
    assert_eq!(router.subscription_count(1), 0);
    assert_eq!(router.matching_clients("a/b"), vec![2]);
    router.remove_client(2);
    router.remove_client(2);
    assert!(router.matching_clients("a/b").is_empty());
    assert_eq!(router.nodes[0].children.len(), 0);
    assert!(router.subscribe(3, "x/y".into()));
    assert_eq!(router.matching_clients("x/y"), vec![3]);
}
#[test]
fn trie_agrees_with_independent_matching() {
    let filters = [
        "#", "+", "+/+", "a", "a/#", "a/+", "/#", "$SYS/#", "$SYS/+", "a//+", "a/$x",
    ];
    let mut router = Router::default();
    for (client, filter) in filters.iter().enumerate() {
        assert!(router.subscribe(client as u64, (*filter).into()));
    }
    for topic in [
        "a", "a/", "a/b", "a//b", "/", "//", "$SYS", "$SYS/", "$SYS/a", "a/$x",
    ] {
        let expected: Vec<_> = filters
            .iter()
            .enumerate()
            .filter(|(_, f)| matches(f, topic))
            .map(|(id, _)| id as u64)
            .collect();
        assert_eq!(router.matching_clients(topic), expected, "{topic}");
    }
}
#[test]
fn deep_topic_is_iterative_and_nodes_are_reused() {
    let mut router = Router::default();
    let topic = "/".repeat(65534);
    assert!(router.subscribe(1, topic.clone()));
    assert_eq!(router.matching_clients(&topic), vec![1]);
    router.remove_client(1);
    let count = router.nodes.len();
    assert!(router.subscribe(2, topic.clone()));
    assert_eq!(router.nodes.len(), count);
    router.remove_client(2);
}
#[test]
fn overlapping_qos_uses_maximum_and_replacement_updates_grant() {
    let mut router = Router::default();
    assert!(router.subscribe_qos(1, "a/#".into(), 0));
    assert!(router.subscribe_qos(1, "a/+".into(), 2));
    assert!(router.subscribe_qos(1, "a/b".into(), 1));
    assert!(router.subscribe_qos(2, "a/#".into(), 1));
    assert_eq!(router.matching_qos("a/b"), vec![(1, 2), (2, 1)]);
    assert_eq!(router.matching_clients("a/b"), vec![1, 2]);
    assert_eq!(router.matching_qos("a"), vec![(1, 0), (2, 1)]);
    assert!(router.subscribe_qos(1, "a/+".into(), 0));
    assert_eq!(router.subscription_count(1), 3);
    assert_eq!(router.matching_qos("a/b"), vec![(1, 1), (2, 1)]);
    router.unsubscribe(1, "a/b");
    assert_eq!(router.matching_qos("a/b"), vec![(1, 0), (2, 1)]);
    assert!(router.subscribe_qos(1, "a/#".into(), 2));
    assert_eq!(router.matching_qos("a/b"), vec![(1, 2), (2, 1)]);
    // Compatibility wrapper also replaces an existing grant with QoS0.
    assert!(router.subscribe(1, "a/#".into()));
    assert_eq!(router.matching_qos("a/b"), vec![(1, 0), (2, 1)]);
}

#[test]
fn qos_validation_is_atomic_and_system_topics_are_isolated() {
    let mut router = Router::default();
    assert!(router.subscribe_qos(1, "#".into(), 2));
    assert!(router.subscribe_qos(1, "$SYS/#".into(), 1));
    assert!(!router.subscribe_qos(1, "$SYS/#".into(), 3));
    assert!(!router.subscribe_qos(2, "invalid/#/x".into(), 2));
    assert!(!router.subscribe_qos(2, "valid".into(), 255));
    assert_eq!(router.subscription_count(1), 2);
    assert_eq!(router.subscription_count(2), 0);
    assert_eq!(router.matching_qos("$SYS/a"), vec![(1, 1)]);
    assert_eq!(router.matching_qos("a"), vec![(1, 2)]);
    assert!(router.matching_qos("bad+").is_empty());
    assert!(router.matching_qos("").is_empty());
}

#[test]
fn qos_cleanup_and_node_reuse_leave_no_stale_grants() {
    let mut router = Router::default();
    assert!(router.subscribe_qos(1, "a/b".into(), 2));
    assert!(router.subscribe_qos(1, "a/#".into(), 2));
    assert!(router.subscribe_qos(2, "a/b".into(), 1));
    router.remove_client(1);
    assert_eq!(router.matching_qos("a/b"), vec![(2, 1)]);
    router.remove_client(2);
    let allocated = router.nodes.len();
    for _ in 0..100 {
        assert!(router.subscribe_qos(3, "x/y".into(), 0));
        assert_eq!(router.matching_qos("x/y"), vec![(3, 0)]);
        assert!(router.matching_qos("a/b").is_empty());
        assert_eq!(router.nodes.len(), allocated);
        router.remove_client(3);
        assert!(router.matching_qos("x/y").is_empty());
        assert!(router.subscriptions.is_empty());
        assert!(router.nodes.iter().all(Node::is_empty));
    }
}

#[test]
fn qos_trie_matches_reference_across_replacements_and_removals() {
    let filters = ["#", "+", "a/#", "a/+", "a/b", "/#", "$SYS/#", "a//+"];
    let topics = ["a", "a/", "a/b", "a//b", "/", "$SYS", "$SYS/a"];
    let mut router = Router::default();
    let mut reference = BTreeMap::new();
    for revision in 0..3 {
        for client in 0..3u64 {
            for (position, filter) in filters.iter().enumerate() {
                let qos = ((position + revision + client as usize) % 3) as u8;
                assert!(router.subscribe_qos(client, (*filter).into(), qos));
                reference.insert((client, *filter), qos);
            }
        }
        for topic in topics {
            let mut expected = BTreeMap::<u64, u8>::new();
            for (&(client, filter), &qos) in &reference {
                if matches(filter, topic) {
                    expected
                        .entry(client)
                        .and_modify(|max| *max = (*max).max(qos))
                        .or_insert(qos);
                }
            }
            assert_eq!(
                router.matching_qos(topic),
                expected.into_iter().collect::<Vec<_>>()
            );
        }
    }
    router.unsubscribe(1, "a/#");
    router.remove_client(2);
    reference.retain(|&(client, filter), _| client != 2 && !(client == 1 && filter == "a/#"));
    for topic in topics {
        let mut expected = BTreeMap::<u64, u8>::new();
        for (&(client, filter), &qos) in &reference {
            if matches(filter, topic) {
                expected
                    .entry(client)
                    .and_modify(|max| *max = (*max).max(qos))
                    .or_insert(qos);
            }
        }
        assert_eq!(
            router.matching_qos(topic),
            expected.into_iter().collect::<Vec<_>>()
        );
    }
}
