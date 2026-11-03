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
    for n in 0..64 {
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
        let room = 8 * 1024 * 1024 - size;
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
    assert_eq!(serde_json::to_vec(&m).unwrap().len(), 8 * 1024 * 1024);
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
        if serde_json::to_vec(&next).unwrap().len() > 8 * 1024 * 1024 {
            return Err("metadata byte capacity".into());
        }
        Ok(())
    });
    if let Err(error) = result {
        Answer {
            revision: m.revision,
            error: Some(error),
            lease_ms: 0,
        }
    } else {
        *m = next;
        Answer {
            revision: m.revision,
            error: None,
            lease_ms: 0,
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
        let client = format!("device-{}", seed % 32);
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
    for command in commands {
        let before = serde_json::to_vec(&actual).unwrap();
        let (a, delta) = actual.apply_tracked(command.clone());
        let e = legacy_reference(&mut expected, command.clone());
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
