use super::*;

#[test]
fn replacement_deletion_and_utf8_accounting_obey_exact_limits() {
    let mut store = RetainedStore::new(2, 14).unwrap();
    store.apply("温度".into(), vec![1; 2], 0).unwrap();
    store.apply("b".into(), vec![2; 3], 1).unwrap();
    assert_eq!(store.used_bytes(), 12);
    store.apply("温度".into(), vec![3; 4], 2).unwrap();
    assert_eq!(store.used_bytes(), 14);
    assert_eq!(store.len(), 2);
    let before = store.entries();
    assert_eq!(
        store.apply("温度".into(), vec![4; 5], 1),
        Err(RetainedError::CapacityExceeded)
    );
    assert_eq!(store.entries(), before);
    assert_eq!(store.used_bytes(), 14);
    store.apply("温度".into(), vec![], 0).unwrap();
    assert_eq!(store.used_bytes(), 4);
    store.apply("温度".into(), vec![], 2).unwrap();
    assert_eq!(store.used_bytes(), 4);
    store.apply("c".into(), vec![5; 9], 2).unwrap();
    assert_eq!(store.used_bytes(), 14);
    assert_eq!(store.len(), 2);
    store.apply("b".into(), vec![], 0).unwrap();
    store.apply("c".into(), vec![], 0).unwrap();
    assert!(store.is_empty());
    assert_eq!(store.used_bytes(), 0);
}

#[test]
fn count_limit_rejects_new_entries_but_allows_replacement() {
    let mut store = RetainedStore::new(1, 32).unwrap();
    store.apply("a".into(), vec![1], 0).unwrap();
    assert_eq!(
        store.apply("b".into(), vec![2], 2),
        Err(RetainedError::CapacityExceeded)
    );
    store.apply("a".into(), vec![3; 4], 2).unwrap();
    assert_eq!(
        store.entries(),
        vec![RetainedMessage {
            topic: "a".into(),
            payload: vec![3; 4],
            qos: 2,
        }]
    );
    assert_eq!(store.used_bytes(), 5);
    store.apply("absent".into(), vec![], 0).unwrap();
    assert_eq!(store.used_bytes(), 5);
}

#[test]
fn invalid_inputs_never_mutate_existing_data_including_deletions() {
    assert!(matches!(
        RetainedStore::new(0, 1),
        Err(RetainedError::InvalidLimits)
    ));
    assert!(matches!(
        RetainedStore::new(1, 0),
        Err(RetainedError::InvalidLimits)
    ));
    let mut store = RetainedStore::new(2, 16).unwrap();
    store.apply("a".into(), vec![0, 255], 1).unwrap();
    let before = store.entries();
    for topic in [
        "".to_owned(),
        "a+".into(),
        "a/#".into(),
        "a\0b".into(),
        "\u{ffff}".into(),
        "a".repeat(65536),
    ] {
        assert_eq!(
            store.apply(topic, vec![], 0),
            Err(RetainedError::InvalidTopic)
        );
        assert_eq!(store.entries(), before);
    }
    for qos in [3, 255] {
        assert_eq!(
            store.apply("a".into(), vec![], qos),
            Err(RetainedError::InvalidQos)
        );
        assert_eq!(store.entries(), before);
    }
    assert_eq!(store.used_bytes(), 3);
}

#[test]
fn matching_reuses_wildcards_system_isolation_and_caps_delivery_qos() {
    let mut store = RetainedStore::new(8, 128).unwrap();
    for (topic, qos) in [
        ("a/b/c", 2),
        ("a/b", 2),
        ("a/", 1),
        ("a", 0),
        ("$SYS/status", 2),
        ("/", 1),
    ] {
        store.apply(topic.into(), vec![qos], qos).unwrap();
    }
    let matches = store.matching("a/#", 1);
    assert_eq!(
        matches
            .iter()
            .map(|message| (message.topic.as_str(), message.qos))
            .collect::<Vec<_>>(),
        vec![("a", 0), ("a/", 1), ("a/b", 1), ("a/b/c", 1)]
    );
    assert_eq!(
        store
            .matching("a/+", 2)
            .iter()
            .map(|message| message.topic.as_str())
            .collect::<Vec<_>>(),
        vec!["a/", "a/b"]
    );
    assert_eq!(store.matching("#", 2).len(), 5);
    assert_eq!(store.matching("+/#", 2).len(), 5);
    assert_eq!(store.matching("$SYS/#", 2)[0].topic, "$SYS/status");
    assert_eq!(store.matching("$SYS/#", 0)[0].qos, 0);
    assert!(store.matching("$share/g/a/#", 2).is_empty());
    assert!(store.matching("a/#/b", 2).is_empty());
    assert!(store.matching("#", 3).is_empty());
    assert_eq!(
        store
            .entries()
            .iter()
            .find(|message| message.topic == "a/b")
            .unwrap()
            .qos,
        2
    );
}

#[test]
fn cloning_and_snapshot_queries_do_not_share_mutable_payloads() {
    let mut store = RetainedStore::new(2, 16).unwrap();
    store.apply("a".into(), vec![1, 2], 2).unwrap();
    let mut proposal = store.clone();
    proposal.apply("a".into(), vec![3], 0).unwrap();
    let mut exported = store.entries();
    exported[0].payload[0] = 99;
    assert_eq!(store.entries()[0].payload, vec![1, 2]);
    assert_eq!(store.entries()[0].qos, 2);
    assert_eq!(proposal.entries()[0].payload, vec![3]);
    assert_eq!(store.used_bytes(), 3);
    assert_eq!(proposal.used_bytes(), 2);
}
