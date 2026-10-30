//! Phase4 session-actor lifecycle, capacity and fan-out regressions.
use super::tests::{
    Database, connect, connect_with_will, handle, poll, publication, stop, stored, subscribe, will,
    worker,
};
use super::*;

async fn eventually(mut condition: impl FnMut() -> bool) {
    timeout(Duration::from_secs(2), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn vacant_actor_retires_after_clean_disconnect() {
    let (worker, task) = worker(BrokerConfig::default()).await;
    let broker = Arc::clone(&worker.broker);
    connect(&worker, "transient", 1, true).await;
    assert_eq!(broker.actor_count(), 1);
    worker
        .request(Operation::Disconnect {
            client: "transient".into(),
            generation: 1,
            graceful: true,
        })
        .await
        .unwrap();
    eventually(|| broker.actor_count() == 0).await;
    assert!(!broker.contains("transient"));
    // The identifier is immediately reusable by a fresh actor.
    connect(&worker, "transient", 2, true).await;
    assert_eq!(broker.actor_count(), 1);
    stop(worker, task).await;
}

#[tokio::test]
async fn persistent_actor_survives_disconnect_and_keeps_queue() {
    let db = Database::new();
    let config = db.config();
    let (worker, task) = worker(config.clone()).await;
    let broker = Arc::clone(&worker.broker);
    connect(&worker, "keeper", 1, false).await;
    subscribe(&worker, "keeper", 1).await;
    worker
        .request(Operation::Disconnect {
            client: "keeper".into(),
            generation: 1,
            graceful: true,
        })
        .await
        .unwrap();
    connect(&worker, "pub", 2, true).await;
    handle(&worker, "pub", 2, publication(b"offline", 1))
        .await
        .unwrap();
    assert!(broker.contains("keeper"));
    let (present, _) = connect(&worker, "keeper", 3, false).await;
    assert!(present);
    assert!(
        matches!(poll(&worker, "keeper", 3).await, Some(Packet::Publish { payload, .. }) if payload == b"offline")
    );
    stop(worker, task).await;
    let exported = stored(&db.0, &config).unwrap();
    assert!(std::str::from_utf8(&exported).unwrap().contains("keeper"));
}

#[tokio::test]
async fn active_will_left_by_an_unclean_stop_is_published_on_restart() {
    let db = Database::new();
    let config = db.config();
    let (first, task) = worker(config.clone()).await;
    connect(&first, "target", 1, false).await;
    subscribe(&first, "target", 1).await;
    first
        .request(Operation::Disconnect {
            client: "target".into(),
            generation: 1,
            graceful: true,
        })
        .await
        .unwrap();
    connect_with_will(&first, "owner", 2, Some(will("a", b"crash", false))).await;
    // Stopping actors never publishes Wills, so the rows match a crash.
    stop(first, task).await;
    let (second, task) = worker(config).await;
    connect(&second, "target", 3, false).await;
    let delivered = timeout(Duration::from_secs(2), async {
        loop {
            if let Some(packet) = poll(&second, "target", 3).await {
                break packet;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(delivered, Packet::Publish { payload, .. } if payload == b"crash"));
    stop(second, task).await;
}

#[tokio::test]
async fn max_sessions_bounds_live_actors() {
    let config = BrokerConfig {
        max_sessions: 2,
        ..BrokerConfig::default()
    };
    let (worker, task) = worker(config).await;
    connect(&worker, "one", 1, true).await;
    connect(&worker, "two", 2, true).await;
    let (wake, _) = watch::channel(0);
    let (cancel, _) = watch::channel(false);
    let third = worker
        .request(Operation::Connect {
            client: "three".into(),
            clean: true,
            will: None,
            username: None,
            password: None,
            generation: 3,
            wake,
            cancel,
        })
        .await;
    assert!(!matches!(third, Ok(Reply::Connected(_))));
    assert_eq!(worker.broker.actor_count(), 2);
    stop(worker, task).await;
}

async fn connect_v5(worker: &Worker, client: &str, generation: u64) {
    let (wake, _) = watch::channel(0);
    let (cancel, _) = watch::channel(false);
    let (close_reason, _) = watch::channel(None);
    let reply = worker
        .request(Operation::ConnectFrame {
            frame: Frame::v5(Packet::Connect {
                client_id: client.into(),
                clean_session: true,
                keep_alive: 0,
                will: None,
                username: None,
                password: None,
            }),
            generation,
            wake,
            cancel,
            close_reason,
        })
        .await
        .unwrap();
    assert!(matches!(reply, Reply::Connected(_)));
}
async fn frame(worker: &Worker, client: &str, generation: u64, packet: Packet) -> Vec<Frame> {
    let Reply::Frames(frames) = worker
        .request(Operation::HandleFrameAt {
            client: client.into(),
            generation,
            frame: Frame::v5(packet),
            received_at_millis: SystemClock.now_millis(),
        })
        .await
        .unwrap()
    else {
        panic!("frames expected")
    };
    frames
}
async fn delivered(worker: &Worker, client: &str, generation: u64) -> bool {
    matches!(
        worker
            .request(Operation::PollDelivery {
                client: client.into(),
                generation,
            })
            .await
            .unwrap(),
        Reply::DeliveryPolled(Some(_))
    )
}

#[tokio::test]
async fn shared_group_qos1_delivers_exactly_one_copy_per_publication() {
    let (worker, task) = worker(BrokerConfig::default()).await;
    for (client, generation) in [("m1", 1), ("m2", 2)] {
        connect_v5(&worker, client, generation).await;
        let replies = frame(
            &worker,
            client,
            generation,
            Packet::Subscribe {
                packet_id: 1,
                filters: vec![("$share/g/a".into(), 1)],
            },
        )
        .await;
        assert!(matches!(replies[0].packet, Packet::SubAck { .. }));
    }
    connect_v5(&worker, "pub", 3).await;
    for id in 1..=4u16 {
        let replies = frame(
            &worker,
            "pub",
            3,
            Packet::Publish {
                topic: "a".into(),
                payload: vec![1],
                qos: 1,
                packet_id: Some(id),
                dup: false,
                retain: false,
            },
        )
        .await;
        assert_eq!(replies[0].packet, Packet::PubAck { packet_id: id });
    }
    let mut copies = 0;
    for (client, generation) in [("m1", 1), ("m2", 2)] {
        while delivered(&worker, client, generation).await {
            copies += 1;
        }
    }
    assert_eq!(copies, 4, "each publication reaches exactly one member");
    stop(worker, task).await;
}

#[derive(Debug)]
struct ManualClock(std::sync::atomic::AtomicU64);
impl mqtt_session::durable::Clock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[tokio::test]
async fn expired_retained_entry_frees_capacity_before_the_periodic_prune() {
    let config = BrokerConfig {
        max_retained: 1,
        ..BrokerConfig::default()
    };
    let clock = Arc::new(ManualClock(std::sync::atomic::AtomicU64::new(10_000)));
    let broker = Broker::start(config, clock.clone(), None).await.unwrap();
    let (worker, task) = super::tests::wrap(broker);
    connect_v5(&worker, "source", 1).await;
    let retained = |topic: &str, id: u16, expiry: u32| {
        let mut frame = Frame::v5(Packet::Publish {
            topic: topic.into(),
            payload: vec![1],
            qos: 1,
            packet_id: Some(id),
            dup: false,
            retain: true,
        });
        frame.properties = vec![Property::MessageExpiryInterval(expiry)];
        frame
    };
    let publish = |frame: Frame| {
        let worker = worker.clone();
        async move {
            let Reply::Frames(frames) = worker
                .request(Operation::HandleFrameAt {
                    client: "source".into(),
                    generation: 1,
                    frame,
                    received_at_millis: 10_000,
                })
                .await
                .unwrap()
            else {
                panic!("frames expected")
            };
            frames
        }
    };
    let first = publish(retained("first", 1, 1)).await;
    assert_eq!(first[0].packet, Packet::PubAck { packet_id: 1 });
    clock.0.store(12_000, std::sync::atomic::Ordering::SeqCst);
    let second = publish(retained("second", 2, 60)).await;
    assert_eq!(second[0].packet, Packet::PubAck { packet_id: 2 });
    assert_eq!(second[0].reason_code, ReasonCode::SUCCESS);
    stop(worker, task).await;
}

#[tokio::test]
async fn disconnect_protocol_error_is_reported_but_unknown_client_is_done() {
    let (worker, task) = worker(BrokerConfig::default()).await;
    connect_v5(&worker, "zero", 1).await;
    // Session expiry 0 at CONNECT cannot become positive at DISCONNECT.
    let Err(error) = worker
        .request(Operation::DisconnectOptions {
            client: "zero".into(),
            generation: 1,
            graceful: true,
            expiry: Some(1),
        })
        .await
    else {
        panic!("protocol error expected")
    };
    assert_eq!(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<WorkerFailure>())
            .map(|failure| failure.reason),
        Some(ReasonCode::PROTOCOL_ERROR)
    );
    assert!(matches!(
        worker
            .request(Operation::Disconnect {
                client: "never-connected".into(),
                generation: 9,
                graceful: true,
            })
            .await
            .unwrap(),
        Reply::Done
    ));
    stop(worker, task).await;
}

#[tokio::test]
async fn concurrent_connects_do_not_serialize_behind_one_worker() {
    const CLIENTS: u64 = 2000;
    let config = BrokerConfig {
        max_sessions: CLIENTS as usize,
        max_connections: CLIENTS as usize,
        // This test measures actor parallelism, not admission rejection. Give
        // its explicitly bounded 2000 concurrent login attempts enough slots.
        max_handshakes: CLIENTS as usize,
        max_auth_tasks: CLIENTS as usize,
        auth_rate_per_second: 0,
        ..BrokerConfig::default()
    };
    let (worker, task) = worker(config).await;
    let started = std::time::Instant::now();
    let mut joins = Vec::new();
    for generation in 1..=CLIENTS {
        let worker = worker.clone();
        joins.push(tokio::spawn(async move {
            connect(&worker, &format!("c{generation}"), generation, true).await;
            let replies = handle(
                &worker,
                &format!("c{generation}"),
                generation,
                Packet::PingReq,
            )
            .await
            .unwrap();
            assert_eq!(replies, vec![Packet::PingResp]);
        }));
    }
    for join in joins {
        join.await.unwrap();
    }
    assert_eq!(worker.broker.actor_count(), CLIENTS as usize);
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "2000 connects took {:?}",
        started.elapsed()
    );
    stop(worker, task).await;
}
