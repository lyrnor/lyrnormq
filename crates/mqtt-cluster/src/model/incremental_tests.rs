use super::*;
fn register() -> Command {
    Command::Register {
        node: 1,
        incarnation: "one".into(),
        address: "http://127.0.0.1:1".into(),
        expires_at: 1000,
        lease_ms: 3000,
        replicas: None,
        memory: false,
        disk_id: None,
    }
}
#[test]
fn normal_tracked_command_does_not_clone_entire_metadata() {
    let mut m = Metadata::default();
    assert!(m.apply(register()).error.is_none());
    METADATA_CLONES.with(|count| count.set(0));
    let (answer, delta) = m.apply_tracked(Command::Claim {
        node: 1,
        incarnation: "one".into(),
        client: "device".into(),
        generation: 1,
    });
    assert!(answer.error.is_none());
    assert!(delta.is_some());
    METADATA_CLONES
        .with(|count| assert_eq!(count.get(), 0, "normal mutation cloned complete directory"));
}
fn at_byte_capacity() -> Metadata {
    let mut m = Metadata::default();
    assert!(m.apply(register()).error.is_none());
    m.revision = 9;
    m.nodes.get_mut(&1).unwrap().applied = 9;
    for n in 0..(incremental::MAX_BYTES / (2 * 64000) - 1) {
        m.clients.insert(
            format!("base-{n}"),
            Client {
                node: 1,
                incarnation: "one".into(),
                generation: 1,
                filters: vec![format!("f{n}/{}", "x".repeat(64000))],
            },
        );
    }
    m.rebuild();
    loop {
        let size = serde_json::to_vec(&m).unwrap().len();
        let room = incremental::MAX_BYTES - size;
        if room < 1024 {
            let key = m.clients.keys().next().unwrap().clone();
            let value = m.clients.remove(&key).unwrap();
            m.clients
                .insert(format!("{key}{}", "z".repeat(room)), value);
            break;
        }
        let n = m.clients.len();
        let len = ((room - 512) / 2).min(65000);
        m.clients.insert(
            format!("padding-{n}"),
            Client {
                node: 1,
                incarnation: "one".into(),
                generation: 1,
                filters: vec![format!("p{n}/{}", "y".repeat(len))],
            },
        );
        m.rebuild();
    }
    assert_eq!(
        serde_json::to_vec(&m).unwrap().len(),
        incremental::MAX_BYTES
    );
    m
}
#[test]
fn revision_growth_cannot_exceed_final_encoded_byte_capacity() {
    let mut m = at_byte_capacity();
    let before = serde_json::to_vec(&m).unwrap();
    let client = m.clients.keys().next().unwrap().clone();
    let answer = m.apply(Command::Claim {
        node: 1,
        incarnation: "one".into(),
        client,
        generation: 1,
    });
    assert!(
        answer.error.is_some(),
        "revision digit growth crossed hard limit"
    );
    assert_eq!(serde_json::to_vec(&m).unwrap(), before);
}
#[test]
fn heartbeat_growth_cannot_bypass_capacity_or_partially_update_node() {
    let mut m = at_byte_capacity();
    let before = serde_json::to_vec(&m).unwrap();
    let answer = m.apply(Command::Heartbeat {
        node: 1,
        incarnation: "one".into(),
        applied: 10,
        expires_at: 1000,
        lease_ms: 3000,
    });
    assert!(answer.error.is_some(), "heartbeat bypassed hard limit");
    assert_eq!(serde_json::to_vec(&m).unwrap(), before);
}
fn legacy_reference(m: &mut Metadata, command: Command) -> Answer {
    let heartbeat = matches!(command, Command::Heartbeat { .. });
    let mut next = m.clone();
    next.derived = incremental::Derived::default();
    next.accounted_bytes();
    let result = next.mutate(command).and_then(|()| {
        next.rebuild();
        if !heartbeat {
            next.revision = next.revision.saturating_add(1);
        }
        if serde_json::to_vec(&next).unwrap().len() > incremental::MAX_BYTES {
            return Err("metadata byte capacity".into());
        }
        Ok(())
    });
    if let Err(error) = result {
        Answer {
            revision: m.revision,
            error: Some(error),
            lease_ms: 0,
            results: Vec::new(),
        }
    } else {
        *m = next;
        Answer {
            revision: m.revision,
            error: None,
            lease_ms: 0,
            results: Vec::new(),
        }
    }
}
#[test]
fn randomized_sparse_commands_match_full_rebuild_and_exact_bytes() {
    let mut actual = Metadata::default();
    let mut expected = Metadata::default();
    let mut observer = Metadata::default();
    let mut seed = 1u64;
    let mut commands = vec![register()];
    for n in 0..600 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let client = format!("device-{}", (seed >> 16) % 32);
        let generation = 1 + ((seed >> 5) % 3);
        commands.push(match n % 6 {
            0 => Command::Claim {
                node: 1,
                incarnation: "one".into(),
                client,
                generation,
            },
            1 => Command::Filters {
                node: 1,
                incarnation: "one".into(),
                client,
                generation,
                filters: vec![
                    format!("sensor/{}/#", seed % 7),
                    format!("sensor/{}/#", seed % 7),
                ],
            },
            2 => Command::Release {
                node: 1,
                incarnation: "one".into(),
                client,
                generation,
            },
            3 => Command::Heartbeat {
                node: 1,
                incarnation: "one".into(),
                applied: n,
                expires_at: n + 1000,
                lease_ms: 3000,
            },
            4 => Command::Claim {
                node: 1,
                incarnation: "wrong".into(),
                client,
                generation,
            },
            _ => Command::Expire {
                node: 1,
                incarnation: "one".into(),
                expires_at: 0,
            },
        });
    }
    let mut successful_filters = 0;
    for command in commands {
        let before = serde_json::to_vec(&actual).unwrap();
        let (a, delta) = actual.apply_tracked(command.clone());
        let e = legacy_reference(&mut expected, command.clone());
        if a.error.is_none() && matches!(command, Command::Filters { .. }) {
            successful_filters += 1;
        }
        assert_eq!(
            (a.revision, &a.error),
            (e.revision, &e.error),
            "{command:?}"
        );
        assert_eq!(
            serde_json::to_vec(&actual).unwrap(),
            serde_json::to_vec(&expected).unwrap(),
            "{command:?}"
        );
        assert_eq!(
            actual.accounted_bytes(),
            serde_json::to_vec(&actual).unwrap().len()
        );
        if a.error.is_some() {
            assert_eq!(serde_json::to_vec(&actual).unwrap(), before);
            assert!(delta.is_none());
        }
        if let Some(delta) = delta {
            delta.merge_in_place(&mut observer).unwrap();
            assert_eq!(
                serde_json::to_vec(&observer).unwrap(),
                serde_json::to_vec(&actual).unwrap()
            );
        }
    }
    assert!(
        successful_filters > 0,
        "generator must cover successful filter edits"
    );
}
#[test]
fn shared_routes_survive_one_client_removal_and_expire_only_own_node() {
    let mut m = Metadata::default();
    m.apply(register());
    m.apply(Command::Register {
        node: 2,
        incarnation: "two".into(),
        address: "http://127.0.0.1:2".into(),
        expires_at: 1000,
        lease_ms: 3000,
        replicas: None,
        memory: false,
        disk_id: None,
    });
    for (client, node, inc) in [("a", 1, "one"), ("b", 1, "one"), ("c", 2, "two")] {
        m.apply(Command::Claim {
            node,
            incarnation: inc.into(),
            client: client.into(),
            generation: 1,
        });
        m.apply(Command::Filters {
            node,
            incarnation: inc.into(),
            client: client.into(),
            generation: 1,
            filters: vec!["devices/+".into()],
        });
    }
    m.apply(Command::Release {
        node: 1,
        incarnation: "one".into(),
        client: "a".into(),
        generation: 1,
    });
    assert_eq!(m.routes["devices/+"], BTreeSet::from([1, 2]));
    m.apply(Command::Expire {
        node: 1,
        incarnation: "one".into(),
        expires_at: 1000,
    });
    assert_eq!(m.routes["devices/+"], BTreeSet::from([2]));
    assert_eq!(m.clients.len(), 1);
    assert_eq!(m.accounted_bytes(), serde_json::to_vec(&m).unwrap().len());
}
#[test]
fn sparse_delta_merge_does_not_clone_metadata_and_gap_is_atomic() {
    let mut m = Metadata::default();
    let (_, delta) = m.apply_tracked(register());
    let mut observer = Metadata::default();
    METADATA_CLONES.with(|n| n.set(0));
    delta.unwrap().merge_in_place(&mut observer).unwrap();
    METADATA_CLONES.with(|n| assert_eq!(n.get(), 0));
    let before = serde_json::to_vec(&observer).unwrap();
    let delta = MetadataUpdate::Delta {
        base: 0,
        revision: 2,
        data_mode: None,
        disk_ids: BTreeMap::new(),
        data_generation: 0,
        excluded: BTreeSet::new(),
        repair: None,
        last_repair: None,
        nodes: BTreeMap::new(),
        clients: BTreeMap::new(),
        durable: BTreeMap::new(),
        routes: BTreeMap::new(),
    };
    assert!(delta.merge_in_place(&mut observer).is_err());
    assert_eq!(serde_json::to_vec(&observer).unwrap(), before);
}
#[test]
fn pending_heartbeat_survives_failed_command_and_next_delta_matches_snapshot() {
    let mut m = Metadata::default();
    let (_, d) = m.apply_tracked(register());
    let mut watcher = Metadata::default();
    d.unwrap().merge_in_place(&mut watcher).unwrap();
    assert!(
        m.apply_tracked(Command::Heartbeat {
            node: 1,
            incarnation: "one".into(),
            applied: 100,
            expires_at: 2000,
            lease_ms: 3000
        })
        .1
        .is_none()
    );
    assert!(
        m.apply_tracked(Command::Claim {
            node: 1,
            incarnation: "bad".into(),
            client: "a".into(),
            generation: 1
        })
        .0
        .error
        .is_some()
    );
    let (_, d) = m.apply_tracked(Command::Claim {
        node: 1,
        incarnation: "one".into(),
        client: "a".into(),
        generation: 1,
    });
    d.unwrap().merge_in_place(&mut watcher).unwrap();
    assert_eq!(watcher.nodes[&1].applied, 100);
    assert_eq!(
        serde_json::to_vec(&watcher).unwrap(),
        serde_json::to_vec(&m).unwrap()
    );
    let mut restored: Metadata = serde_json::from_slice(&serde_json::to_vec(&m).unwrap()).unwrap();
    assert_eq!(
        restored.accounted_bytes(),
        serde_json::to_vec(&restored).unwrap().len()
    );
    restored.apply(Command::Release {
        node: 1,
        incarnation: "one".into(),
        client: "a".into(),
        generation: 1,
    });
    assert!(restored.clients.is_empty());
    assert_eq!(
        restored.accounted_bytes(),
        serde_json::to_vec(&restored).unwrap().len()
    );
}
#[test]
fn encoded_accounting_handles_numeric_map_keys_unicode_and_json_escaping() {
    let mut m = Metadata::default();
    m.apply(register());
    let client = "设备\\\"🌊";
    m.apply(Command::Claim {
        node: 1,
        incarnation: "one".into(),
        client: client.into(),
        generation: 1,
    });
    m.apply(Command::Filters {
        node: 1,
        incarnation: "one".into(),
        client: client.into(),
        generation: 1,
        filters: vec!["温度/\\\"/#".into()],
    });
    assert_eq!(m.accounted_bytes(), serde_json::to_vec(&m).unwrap().len());
    m.apply(Command::Release {
        node: 1,
        incarnation: "one".into(),
        client: client.into(),
        generation: 1,
    });
    assert!(m.routes.is_empty());
    assert_eq!(m.accounted_bytes(), serde_json::to_vec(&m).unwrap().len());
}
#[test]
fn revision_exhaustion_cannot_mutate_without_emitting_a_revision() {
    let mut m = Metadata::default();
    m.apply(register());
    m.revision = u64::MAX;
    m.derived = incremental::Derived::default();
    let before = serde_json::to_vec(&m).unwrap();
    let (answer, delta) = m.apply_tracked(Command::Claim {
        node: 1,
        incarnation: "one".into(),
        client: "a".into(),
        generation: 1,
    });
    assert!(answer.error.is_some());
    assert!(delta.is_none());
    assert_eq!(serde_json::to_vec(&m).unwrap(), before);
}
#[test]
fn durable_owner_transfer_keeps_other_routes_and_fences_old_epoch() {
    let peers = vec![
        (1, "http://127.0.0.1:1".into()),
        (2, "http://127.0.0.1:2".into()),
        (3, "http://127.0.0.1:3".into()),
    ];
    let mut m = Metadata::default();
    for node in 1..=3 {
        assert!(
            m.apply(Command::Register {
                node,
                incarnation: format!("n{node}"),
                address: format!("http://127.0.0.1:{node}"),
                expires_at: 1000,
                lease_ms: 3000,
                replicas: Some(peers.clone()),
                memory: true,
                disk_id: Some(format!("{node:032x}"))
            })
            .error
            .is_none()
        );
    }
    let mut observer = m.clone();
    let mut expected = m.clone();
    for command in [
        Command::Acquire {
            key: "c:a".into(),
            node: 1,
            incarnation: "n1".into(),
            generation: 1,
            expected_epoch: 0,
        },
        Command::Claim {
            node: 1,
            incarnation: "n1".into(),
            client: "legacy".into(),
            generation: 1,
        },
        Command::Filters {
            node: 1,
            incarnation: "n1".into(),
            client: "legacy".into(),
            generation: 1,
            filters: vec!["shared/#".into()],
        },
    ] {
        let (a, d) = m.apply_tracked(command.clone());
        let e = legacy_reference(&mut expected, command);
        assert_eq!(a.error, e.error);
        if let Some(d) = d {
            d.merge_in_place(&mut observer).unwrap();
        }
    }
    let epoch = m.durable["c:a"].epoch;
    for command in [
        Command::DurableFilters {
            key: "c:a".into(),
            node: 1,
            incarnation: "n1".into(),
            epoch,
            filters: vec!["shared/#".into(), "shared/#".into()],
            logical_generation: 1,
        },
        Command::Acquire {
            key: "c:a".into(),
            node: 2,
            incarnation: "n2".into(),
            generation: 2,
            expected_epoch: epoch,
        },
        Command::DurableFilters {
            key: "c:a".into(),
            node: 1,
            incarnation: "n1".into(),
            epoch,
            filters: vec![],
            logical_generation: 1,
        },
    ] {
        let (a, d) = m.apply_tracked(command.clone());
        let e = legacy_reference(&mut expected, command);
        assert_eq!(a.error, e.error);
        if let Some(d) = d {
            d.merge_in_place(&mut observer).unwrap();
        }
        assert_eq!(
            serde_json::to_vec(&m).unwrap(),
            serde_json::to_vec(&expected).unwrap()
        );
    }
    assert_eq!(m.routes["shared/#"], BTreeSet::from([1, 2]));
    assert_eq!(m.accounted_bytes(), serde_json::to_vec(&m).unwrap().len());
    assert_eq!(
        serde_json::to_vec(&observer).unwrap(),
        serde_json::to_vec(&m).unwrap()
    );
}
#[test]
fn over_capacity_route_change_rolls_back_indexes_before_following_commands() {
    let mut m = at_byte_capacity();
    let before = serde_json::to_vec(&m).unwrap();
    let client = m.clients.keys().next().unwrap().clone();
    let mut filters = m.clients[&client].filters.clone();
    filters.push("new/route".into());
    assert!(
        m.apply(Command::Filters {
            node: 1,
            incarnation: "one".into(),
            client: client.clone(),
            generation: 1,
            filters
        })
        .error
        .is_some()
    );
    assert_eq!(serde_json::to_vec(&m).unwrap(), before);
    assert!(
        m.apply(Command::Release {
            node: 1,
            incarnation: "one".into(),
            client,
            generation: 1
        })
        .error
        .is_none()
    );
    m.apply(Command::Claim {
        node: 1,
        incarnation: "one".into(),
        client: "new".into(),
        generation: 1,
    });
    m.apply(Command::Filters {
        node: 1,
        incarnation: "one".into(),
        client: "new".into(),
        generation: 1,
        filters: vec!["new/route".into()],
    });
    m.apply(Command::Release {
        node: 1,
        incarnation: "one".into(),
        client: "new".into(),
        generation: 1,
    });
    assert!(!m.routes.contains_key("new/route"));
    assert_eq!(m.accounted_bytes(), serde_json::to_vec(&m).unwrap().len());
}
#[test]
fn one_hundred_thousand_short_id_owners_fit_authorized_metadata_budget() {
    let peers: Vec<(u64, String)> = vec![
        (101, "http://127.0.0.1:7101".into()),
        (102, "http://127.0.0.1:7102".into()),
        (103, "http://127.0.0.1:7103".into()),
    ];
    let mut m = Metadata::default();
    for (node, address) in &peers {
        assert!(
            m.apply(Command::Register {
                node: *node,
                incarnation: format!("{node:032x}"),
                address: address.clone(),
                expires_at: 1000,
                lease_ms: 3000,
                replicas: Some(peers.clone()),
                memory: true,
                disk_id: Some(format!("{node:032x}"))
            })
            .error
            .is_none()
        );
    }
    for id in 0..100_000u64 {
        let node = 101 + id % 3;
        m.durable.insert(
            format!("c:{id:023}"),
            DurableOwner {
                node,
                incarnation: format!("{node:032x}"),
                generation: id + 1,
                epoch: 1_000_000 + id,
                acquired_from: None,
                filters: Vec::new(),
                logical_generation: 1,
            },
        );
    }
    m.derived = incremental::Derived::default();
    let answer = m.apply(Command::Heartbeat {
        node: 101,
        incarnation: format!("{:032x}", 101),
        applied: 3,
        expires_at: 1001,
        lease_ms: 3000,
    });
    assert!(
        answer.error.is_none(),
        "authorized100k short-owner shape must fit: {:?}",
        answer.error
    );
    assert_eq!(m.durable.len(), 100_000);
    let bytes = serde_json::to_vec(&m).unwrap().len();
    assert_eq!(m.accounted_bytes(), bytes);
    assert!(bytes <= 16 * 1024 * 1024 - 64 * 1024);
    let snapshot = MetadataUpdate::Snapshot(m.clone());
    let wire = serde_json::to_vec(&snapshot).unwrap().len();
    let state = serde_json::to_vec(&State {
        metadata: m,
        ..State::default()
    })
    .unwrap()
    .len();
    assert!(wire + 16 < 16 * 1024 * 1024);
    assert!(state < 16 * 1024 * 1024);
    println!("100k ordinary23-byte IDs: metadata={bytes} snapshot_json={wire} state_json={state}");
}
#[test]
fn maximal_metadata_keeps_state_and_snapshot_envelopes_below_sixteen_mib() {
    let m = at_byte_capacity();
    let bytes = serde_json::to_vec(&m).unwrap().len();
    assert_eq!(incremental::MAX_BYTES, 16_711_680);
    assert_eq!(bytes, incremental::MAX_BYTES);
    let snapshot_bytes = serde_json::to_vec(&MetadataUpdate::Snapshot(m.clone()))
        .unwrap()
        .len();
    // Envelope.data protobuf tag/length plus gRPC message framing need fewer16B.
    assert!(snapshot_bytes + 16 < 16 * 1024 * 1024);
    let nodes = BTreeMap::from([
        (
            1,
            BasicNode::new(format!("http://127.0.0.1:1/{}", "x".repeat(200))),
        ),
        (
            2,
            BasicNode::new(format!("http://127.0.0.1:2/{}", "y".repeat(200))),
        ),
        (
            3,
            BasicNode::new(format!("http://127.0.0.1:3/{}", "z".repeat(200))),
        ),
    ]);
    let membership =
        openraft::Membership::new(vec![BTreeSet::from([1u64, 2, 3])], nodes).expect("membership");
    let log_id = LogId::new(
        crate::model::committed_leader_id(u64::MAX, u64::MAX),
        u64::MAX,
    );
    let state = State {
        last_applied: Some(log_id),
        membership: StoredMembership::new(Some(log_id), membership),
        metadata: m,
    };
    let state_bytes = serde_json::to_vec(&state).unwrap().len();
    assert!(state_bytes < 16 * 1024 * 1024);
    println!(
        "max metadata={bytes} snapshot_json={snapshot_bytes} state_json_with_membership={state_bytes}"
    );
}
#[test]
fn physical_durable_directory_allows_headroom_but_remains_bounded() {
    let peers: Vec<(u64, String)> = vec![
        (101, "http://127.0.0.1:7101".into()),
        (102, "http://127.0.0.1:7102".into()),
        (103, "http://127.0.0.1:7103".into()),
    ];
    let mut m = Metadata::default();
    for (node, address) in &peers {
        assert!(
            m.apply(Command::Register {
                node: *node,
                incarnation: format!("{node:032x}"),
                address: address.clone(),
                expires_at: 1000,
                lease_ms: 3000,
                replicas: Some(peers.clone()),
                memory: true,
                disk_id: Some(format!("{node:032x}"))
            })
            .error
            .is_none()
        );
    }
    for id in 0..101_023 {
        m.durable.insert(
            format!("c:{id:06}"),
            DurableOwner {
                node: 101,
                incarnation: format!("{:032x}", 101),
                generation: 1,
                epoch: 1,
                acquired_from: None,
                filters: Vec::new(),
                logical_generation: 1,
            },
        );
    }
    m.derived = incremental::Derived::default();
    assert!(
        m.apply(Command::Acquire {
            key: "h:final-headroom".into(),
            node: 101,
            incarnation: format!("{:032x}", 101),
            generation: 1,
            expected_epoch: 0
        })
        .error
        .is_none()
    );
    assert_eq!(m.durable.len(), 101_024);
    let before_revision = m.revision;
    let before_bytes = m.accounted_bytes();
    let error = m
        .apply(Command::Acquire {
            key: "c:too-many".into(),
            node: 101,
            incarnation: format!("{:032x}", 101),
            generation: 1,
            expected_epoch: 0,
        })
        .error;
    assert_eq!(error.as_deref(), Some("durable directory capacity"));
    assert_eq!(m.revision, before_revision);
    assert!(!m.durable.contains_key("c:too-many"));
    assert_eq!(m.accounted_bytes(), before_bytes);
    assert_eq!(serde_json::to_vec(&m).unwrap().len(), before_bytes);
}
#[test]
fn repair_seal_accepts_same_physical_record_cap_and_rejects_next_record() {
    let mut seal = RepairSeal {
        records: 101_024,
        bytes: 0,
        digest: "0".repeat(64),
    };
    assert!(seal.valid());
    seal.records = 101_025;
    assert!(!seal.valid());
}

#[test]
fn acquisition_once_duplicate_preserves_epoch_and_survives_serialized_state() {
    let mut metadata = Metadata::default();
    let peers = (1..=3)
        .map(|node| (node, format!("http://127.0.0.1:{node}")))
        .collect();
    let answer = metadata.apply(Command::Register {
        node: 1,
        incarnation: "one".into(),
        address: "http://127.0.0.1:1".into(),
        expires_at: 1000,
        lease_ms: 3000,
        replicas: Some(peers),
        memory: true,
        disk_id: Some(format!("{:032x}", 1)),
    });
    assert!(answer.error.is_none());
    let command:Command=serde_json::from_value(serde_json::json!({"AcquireOnce":{"key":"c:once","node":1,"incarnation":"one","generation":1,"expected_epoch":0}})).expect("new idempotent command must decode");
    let first = metadata.apply(command.clone());
    assert!(first.error.is_none());
    let saved = serde_json::to_vec(&metadata).unwrap();
    let (again, delta) = metadata.apply_tracked(command.clone());
    assert!(again.error.is_none());
    assert_eq!(again.revision, first.revision);
    assert!(delta.is_none());
    assert_eq!(serde_json::to_vec(&metadata).unwrap(), saved);
    let mut reopened: Metadata = serde_json::from_slice(&saved).unwrap();
    let duplicate = reopened.apply(command);
    assert_eq!(duplicate.revision, first.revision);
    assert!(duplicate.error.is_none());
    let conflict = reopened.apply(Command::Acquire {
        key: "c:once".into(),
        node: 1,
        incarnation: "one".into(),
        generation: 2,
        expected_epoch: 0,
    });
    assert!(conflict.error.is_some());
}

#[test]
fn control_batch_matches_sequential_answers_sparse_delta_and_partial_rejections() {
    let mut metadata = Metadata::default();
    assert!(metadata.apply(register()).error.is_none());
    let commands = vec![
        Command::Claim {
            node: 1,
            incarnation: "one".into(),
            client: "a".into(),
            generation: 1,
        },
        Command::Claim {
            node: 1,
            incarnation: "wrong".into(),
            client: "invalid".into(),
            generation: 1,
        },
        Command::Claim {
            node: 1,
            incarnation: "one".into(),
            client: "b".into(),
            generation: 1,
        },
    ];
    let mut sequential = metadata.clone();
    let mut observer = metadata.clone();
    let expected: Vec<_> = commands
        .iter()
        .cloned()
        .map(|command| sequential.apply(command))
        .collect();
    let command: Command =
        serde_json::from_value(serde_json::json!({"Batch":{"commands":commands}}))
            .expect("internal batch command must decode");
    METADATA_CLONES.with(|count| count.set(0));
    let (answer, delta) = metadata.apply_tracked(command);
    METADATA_CLONES.with(|count| assert_eq!(count.get(), 0, "batch cloned complete metadata"));
    let value = serde_json::to_value(&answer).unwrap();
    assert_eq!(value["results"], serde_json::to_value(&expected).unwrap());
    delta.unwrap().merge_in_place(&mut observer).unwrap();
    assert_eq!(
        serde_json::to_vec(&metadata).unwrap(),
        serde_json::to_vec(&sequential).unwrap()
    );
    assert_eq!(
        serde_json::to_vec(&observer).unwrap(),
        serde_json::to_vec(&sequential).unwrap()
    );
}

#[test]
fn malformed_batch_is_rejected_before_any_child_mutates() {
    let mut metadata = Metadata::default();
    metadata.apply(register());
    let before = serde_json::to_vec(&metadata).unwrap();
    let valid = Command::Claim {
        node: 1,
        incarnation: "one".into(),
        client: "valid".into(),
        generation: 1,
    };
    for commands in [
        vec![],
        vec![valid.clone(); 65],
        vec![
            valid.clone(),
            Command::Batch {
                commands: vec![valid.clone()],
            },
        ],
        vec![
            valid.clone(),
            Command::Claim {
                node: 1,
                incarnation: "one".into(),
                client: "x".repeat(512 * 1024),
                generation: 1,
            },
        ],
    ] {
        let (answer, delta) = metadata.apply_tracked(Command::Batch { commands });
        assert!(answer.error.is_some());
        assert!(delta.is_none());
        assert_eq!(serde_json::to_vec(&metadata).unwrap(), before);
    }
}
#[test]
fn batch_trailing_heartbeat_and_invalid_child_preserve_final_sparse_state() {
    let mut metadata = Metadata::default();
    metadata.apply(register());
    let mut observer = metadata.clone();
    let (answer, delta) = metadata.apply_tracked(Command::Batch {
        commands: vec![
            Command::Claim {
                node: 1,
                incarnation: "one".into(),
                client: "".into(),
                generation: 1,
            },
            Command::Claim {
                node: 1,
                incarnation: "one".into(),
                client: "valid".into(),
                generation: 1,
            },
            Command::Heartbeat {
                node: 1,
                incarnation: "one".into(),
                applied: 2,
                expires_at: 5000,
                lease_ms: 3000,
            },
        ],
    });
    assert!(answer.results[0].error.is_some());
    assert!(answer.results[1].error.is_none());
    assert!(answer.results[2].error.is_none());
    delta.unwrap().merge_in_place(&mut observer).unwrap();
    assert_eq!(
        serde_json::to_vec(&observer).unwrap(),
        serde_json::to_vec(&metadata).unwrap()
    );
}

#[test]
fn acquisition_once_batch_epochs_and_takeover_fence_stale_retries() {
    let mut metadata = Metadata::default();
    let peers = (1..=3)
        .map(|node| (node, format!("http://127.0.0.1:{node}")))
        .collect();
    metadata.apply(Command::Register {
        node: 1,
        incarnation: "one".into(),
        address: "http://127.0.0.1:1".into(),
        expires_at: 1000,
        lease_ms: 3000,
        replicas: Some(peers),
        memory: true,
        disk_id: Some(format!("{:032x}", 1)),
    });
    let once = |client: &str, generation, expected_epoch| Command::AcquireOnce {
        key: format!("c:{client}"),
        node: 1,
        incarnation: "one".into(),
        generation,
        expected_epoch,
    };
    let answer = metadata.apply(Command::Batch {
        commands: vec![once("a", 1, 0), once("b", 1, 0), once("a", 1, 0)],
    });
    assert!(answer.results.iter().all(|answer| answer.error.is_none()));
    assert_eq!(
        answer
            .results
            .iter()
            .map(|answer| answer.revision)
            .collect::<Vec<_>>(),
        vec![2, 3, 2]
    );
    assert_eq!(metadata.revision, 3);
    let takeover = metadata.apply(once("a", 2, 2));
    assert!(takeover.error.is_none());
    assert_eq!(takeover.revision, 4);
    let before = serde_json::to_vec(&metadata).unwrap();
    assert!(metadata.apply(once("a", 1, 0)).error.is_some());
    assert_eq!(serde_json::to_vec(&metadata).unwrap(), before);
    assert_eq!(metadata.apply(once("a", 2, 2)).revision, 4);
    metadata.excluded.insert(1);
    assert!(metadata.apply(once("a", 2, 2)).error.is_some());
}
