use super::*;

fn fixture(label: &str) -> io::Result<(Arc<BrokerRuntime>, std::path::PathBuf)> {
    let path = std::env::temp_dir().join(format!(
        "rmqtt-events-{label}-{}-{}",
        std::process::id(),
        controller::now()
    ));
    let config = serde_json::from_value(serde_json::json!({
        "role":"broker", "cluster_id":"events", "node_id":101,
        "listen":"127.0.0.1:7101", "data_dir":path, "token_file":path.join("token"),
        "controllers":[{"id":1,"address":"http://127.0.0.1:7001"},{"id":2,"address":"http://127.0.0.1:7002"},{"id":3,"address":"http://127.0.0.1:7003"}],
        "durable":{"replicas":[{"id":101,"address":"http://127.0.0.1:7101"},{"id":102,"address":"http://127.0.0.1:7102"},{"id":103,"address":"http://127.0.0.1:7103"}]}
    })).map_err(io::Error::other)?;
    let runtime = BrokerRuntime::new(config)?;
    runtime.initialized.store(true, Ordering::Release);
    *runtime.deadline.lock().unwrap() = tokio::time::Instant::now() + Duration::from_secs(60);
    {
        let mut metadata = runtime.metadata.write().unwrap();
        metadata.data_mode = Some(DataMode::Memory(
            runtime
                .config
                .durable
                .as_ref()
                .unwrap()
                .replicas
                .iter()
                .map(|p| (p.id, p.address.clone()))
                .collect(),
        ));
        metadata
            .disk_ids
            .insert(101, runtime.journal.as_ref().unwrap().disk_id().to_owned());
        metadata.nodes.insert(
            101,
            Node {
                id: 101,
                incarnation: runtime.incarnation.clone(),
                address: "http://127.0.0.1:7101".into(),
                expires_at: 0,
                max_lease_ms: 3000,
                applied: 0,
            },
        );
        metadata.durable.insert(
            client_key("idle"),
            DurableOwner {
                acquired_from: None,
                node: 101,
                incarnation: runtime.incarnation.clone(),
                generation: 1,
                epoch: 1,
                logical_generation: 0,
                filters: vec![],
            },
        );
    }
    Ok((runtime, path))
}

#[tokio::test]
async fn idle_directory_does_not_poll_actor() -> io::Result<()> {
    let (runtime, path) = fixture("idle")?;
    let data = runtime.data.as_ref().unwrap().clone();
    let (tx, mut rx) = mpsc::channel(8);
    data.actors.lock().unwrap().insert(client_key("idle"), tx);
    let runner = tokio::spawn(data.run());
    let idle = tokio::time::timeout(Duration::from_millis(250), rx.recv()).await;
    runner.abort();
    let _ = runner.await;
    assert!(
        idle.is_err(),
        "idle directory must not produce background Work operations"
    );
    drop(runtime);
    std::fs::remove_dir_all(path)?;
    Ok(())
}

#[tokio::test]
async fn heartbeat_progress_is_idle_but_owner_loss_and_readiness_wake() -> io::Result<()> {
    let (runtime, path) = fixture("transitions")?;
    let data = runtime.data.as_ref().unwrap();
    let old = runtime.metadata.read().unwrap().clone();
    let mut next = old.clone();
    next.revision += 1;
    next.nodes.get_mut(&101).unwrap().applied += 1;
    next.nodes.get_mut(&101).unwrap().expires_at += 1000;
    data.reconcile(&runtime, Some(&old), &next);
    assert!(data.scheduler.take().is_none());
    next.nodes.remove(&101);
    data.reconcile(&runtime, Some(&old), &next);
    assert_eq!(data.scheduler.take().as_deref(), Some("c:idle"));
    data.scheduler.finish("c:idle".into(), true);
    data.ready_again(&runtime);
    assert_eq!(data.scheduler.take().as_deref(), Some("c:idle"));
    drop(runtime);
    std::fs::remove_dir_all(path)?;
    Ok(())
}

#[tokio::test]
async fn empty_snapshot_commit_is_idle_but_unknown_commit_retains_recovery() -> io::Result<()> {
    let (runtime, path) = fixture("unknown")?;
    let data = runtime.data.as_ref().unwrap();
    let mut cell = Cell {
        data_generation: 0,
        record: Some(Record::empty(1)),
        poisoned: false,
        activate: false,
        activation_token: Arc::new(()),
    };
    data.changed("c:idle", &cell);
    assert!(data.scheduler.take().is_none());
    cell.poisoned = true;
    data.changed("c:idle", &cell);
    assert_eq!(data.scheduler.take().as_deref(), Some("c:idle"));
    drop(runtime);
    std::fs::remove_dir_all(path)?;
    Ok(())
}

#[tokio::test]
async fn event_wakes_immediately_and_drains_fifo_without_timer_gap() -> io::Result<()> {
    let (runtime, path) = fixture("drain")?;
    let data = runtime.data.as_ref().unwrap().clone();
    let (tx, mut rx) = mpsc::channel(8);
    data.actors.lock().unwrap().insert(client_key("idle"), tx);
    let runner = tokio::spawn(data.clone().run());
    tokio::time::sleep(Duration::from_millis(10)).await;
    data.scheduler.schedule("c:idle");
    // A model actor produces two successive intent heads, then the true idle
    // state. All three Work requests must arrive without a polling interval.
    for index in 0..3 {
        let job = tokio::time::timeout(Duration::from_millis(50), rx.recv())
            .await
            .map_err(io::Error::other)?
            .ok_or_else(unavailable)?;
        let Operation::Work { reply } = job.operation else {
            return Err(io::Error::other("expected Work"));
        };
        let intent = (index < 2).then(|| Intent {
            id: DeliveryId {
                source: "c:idle".into(),
                epoch: 1,
                revision: 1,
                index,
                target_generation: 0,
            },
            message: ClusterMessage {
                topic: "events/test".into(),
                payload: vec![],
                qos: 0,
                retain: false,
                properties: Default::default(),
                expires_at: None,
                publisher: None,
                observer_client: String::new(),
                scope: ForwardScope::Ordinary,
            },
            recipients: vec![],
            completed: BTreeSet::new(),
        });
        let _ = reply.send(Ok(Work {
            intent,
            activation: None,
            activation_token: Arc::new(()),
        }));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(150), rx.recv())
            .await
            .is_err()
    );
    runner.abort();
    let _ = runner.await;
    drop(data);
    drop(runtime);
    std::fs::remove_dir_all(path)?;
    Ok(())
}

#[tokio::test]
async fn dropped_mutation_reply_does_not_cancel_committed_work_notification() -> io::Result<()> {
    let (runtime, path) = fixture("cancelled-caller")?;
    let data = runtime.data.as_ref().unwrap();
    let cell = Cell {
        data_generation: 0,
        record: None,
        poisoned: true,
        activate: false,
        activation_token: Arc::new(()),
    };
    let (reply, rx) = oneshot::channel::<io::Result<()>>();
    drop(rx);
    data.respond("c:idle", &cell, reply, Err(unavailable()));
    assert_eq!(data.scheduler.take().as_deref(), Some("c:idle"));
    data.scheduler.finish("c:idle".into(), true);
    let cell = Cell {
        data_generation: 0,
        record: Some(Record::empty(1)),
        poisoned: false,
        activate: true,
        activation_token: Arc::new(()),
    };
    let (reply, rx) = oneshot::channel::<io::Result<()>>();
    drop(rx);
    data.respond("c:idle", &cell, reply, Ok(()));
    assert_eq!(data.scheduler.take().as_deref(), Some("c:idle"));
    let old = runtime.metadata.read().unwrap().clone();
    let mut next = old.clone();
    next.revision += 1;
    next.nodes.get_mut(&101).unwrap().applied += 1;
    assert!(!Durable::metadata_event(
        &MetadataUpdate::between(&old, &next),
        &old
    ));
    next.nodes.remove(&101);
    assert!(Durable::metadata_event(
        &MetadataUpdate::between(&old, &next),
        &old
    ));
    drop(runtime);
    std::fs::remove_dir_all(path)?;
    Ok(())
}

#[test]
fn stale_activation_ack_cannot_clear_new_release_at_same_record_version() {
    let mut cell = Cell {
        data_generation: 0,
        record: Some(Record::empty(1)),
        poisoned: false,
        activate: false,
        activation_token: Arc::new(()),
    };
    cell.request_activation();
    let stale = cell.activation_token.clone();
    cell.request_activation();
    cell.activated(1, 0, &stale);
    assert!(
        cell.activate,
        "old activation acknowledgement lost a newer release wakeup"
    );
    let current = cell.activation_token.clone();
    cell.activated(1, 0, &current);
    assert!(!cell.activate);
}

#[tokio::test]
async fn sixteen_hot_keys_yield_to_seventeenth_waiting_key() -> io::Result<()> {
    let (runtime, path) = fixture("fairness")?;
    let data = runtime.data.as_ref().unwrap().clone();
    let gate = Arc::new(tokio::sync::Barrier::new(17));
    let (started, mut starts) = mpsc::channel(16);
    let mut actors = tokio::task::JoinSet::new();
    for index in 0..16 {
        let key = format!("c:hot-{index}");
        let owner = runtime.metadata.read().unwrap().durable["c:idle"].clone();
        runtime
            .metadata
            .write()
            .unwrap()
            .durable
            .insert(key.clone(), owner);
        let (tx, mut rx) = mpsc::channel::<Job>(8);
        data.actors.lock().unwrap().insert(key.clone(), tx);
        let gate = gate.clone();
        let started = started.clone();
        data.scheduler.schedule(&key);
        actors.spawn(async move {
            for count in 0..32 {
                let job = rx.recv().await.unwrap();
                let Operation::Work { reply } = job.operation else {
                    panic!("expected Work");
                };
                if count == 0 {
                    started.send(()).await.unwrap();
                    gate.wait().await;
                }
                let intent = Intent {
                    id: DeliveryId {
                        source: key.clone(),
                        epoch: 1,
                        revision: 1,
                        index: count,
                        target_generation: 0,
                    },
                    message: ClusterMessage {
                        topic: "hot".into(),
                        payload: vec![],
                        qos: 0,
                        retain: false,
                        properties: Default::default(),
                        expires_at: None,
                        publisher: None,
                        observer_client: String::new(),
                        scope: ForwardScope::Ordinary,
                    },
                    recipients: vec![],
                    completed: BTreeSet::new(),
                };
                let _ = reply.send(Ok(Work {
                    intent: Some(intent),
                    activation: None,
                    activation_token: Arc::new(()),
                }));
            }
            // Keep all 16 actors backlogged after their quantum. Old unbounded
            // drains occupy every worker here and never visit the waiting key.
            let _held = rx.recv().await;
            std::future::pending::<()>().await;
        });
    }
    let (tx, mut cold) = mpsc::channel::<Job>(8);
    data.actors.lock().unwrap().insert("c:idle".into(), tx);
    let runner = tokio::spawn(data.clone().run());
    // Every hot worker reaches its first Work before the cold key is queued.
    // Waiting at the barrier also makes the test independent of scheduler speed.
    for _ in 0..16 {
        tokio::time::timeout(Duration::from_secs(2), starts.recv())
            .await
            .unwrap()
            .unwrap();
    }
    data.scheduler.schedule("c:idle");
    gate.wait().await;
    let progressed = tokio::time::timeout(Duration::from_millis(500), cold.recv()).await;
    runner.abort();
    let _ = runner.await;
    actors.abort_all();
    while actors.join_next().await.is_some() {}
    assert!(
        progressed.is_ok(),
        "16 continuously hot keys starved the 17th waiting key"
    );
    drop(data);
    drop(runtime);
    std::fs::remove_dir_all(path)?;
    Ok(())
}
