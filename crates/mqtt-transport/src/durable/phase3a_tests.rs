//! Notification safety across installed queues, replay, maintenance and network drain.
use super::tests::{Database, stop};
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

fn options(clean: bool, maximum: u16, expiry: u32) -> ConnectionOptions {
    ConnectionOptions {
        version: ProtocolVersion::V5,
        clean_start: clean,
        session_expiry_interval: expiry,
        receive_maximum: maximum,
        maximum_packet_size: u32::MAX,
        will_properties: Vec::new(),
    }
}
fn connect(engine: &mut SessionEngine, name: &str, clean: bool, maximum: u16) {
    engine
        .connect_with_options(
            name,
            None,
            mqtt_auth::Principal::anonymous(),
            options(clean, maximum, 60),
        )
        .unwrap();
}
fn handle(engine: &mut SessionEngine, name: &str, packet: Packet) -> Vec<Frame> {
    engine.handle_frame(name, Frame::v5(packet)).unwrap()
}
fn subscribe(engine: &mut SessionEngine, name: &str, topic: &str, qos: u8) {
    handle(
        engine,
        name,
        Packet::Subscribe {
            packet_id: 7,
            filters: vec![(topic.into(), qos)],
        },
    );
}
fn publication(topic: &str, value: u8, qos: u8) -> Packet {
    Packet::Publish {
        topic: topic.into(),
        payload: vec![value],
        qos,
        packet_id: (qos != 0).then_some(u16::from(value)),
        dup: false,
        retain: false,
    }
}
fn register(online: &mut HashMap<String, Online>, name: &str) -> watch::Receiver<u64> {
    let (wake, observed) = watch::channel(0);
    let (cancel, _) = watch::channel(false);
    online.insert(
        name.into(),
        Online {
            generation: 1,
            wake,
            cancel,
            close_reason: None,
        },
    );
    observed
}

#[test]
fn notify_targets_only_installed_online_queued_or_replay_work() {
    let config = BrokerConfig::default();
    let mut engine = SessionEngine::new(session_config(&config)).unwrap();
    for name in ["publisher", "ready", "idle", "offline"] {
        connect(&mut engine, name, true, 1);
    }
    subscribe(&mut engine, "ready", "a", 0);
    subscribe(&mut engine, "offline", "a", 0);
    engine
        .disconnect_with_options("offline", true, None)
        .unwrap();
    handle(&mut engine, "publisher", publication("a", 1, 0));
    let mut online = HashMap::new();
    let mut ready = register(&mut online, "ready");
    let mut idle = register(&mut online, "idle");
    let offline = register(&mut online, "offline");
    let unknown = register(&mut online, "unknown");
    let before = engine.snapshot().unwrap();
    assert_eq!(notify(&engine, &online), 1);
    assert!(ready.has_changed().unwrap());
    assert!(!idle.has_changed().unwrap());
    assert!(!offline.has_changed().unwrap());
    assert!(!unknown.has_changed().unwrap());
    assert_eq!(
        engine.snapshot().unwrap(),
        before,
        "hint/notify cannot mutate state"
    );
    ready.borrow_and_update();
    idle.borrow_and_update();
    assert!(
        engine
            .poll_delivery_with_changes("ready")
            .unwrap()
            .delivery
            .is_some()
    );
    assert_eq!(notify(&engine, &online), 0);
    assert!(!ready.has_changed().unwrap());
    assert!(!idle.has_changed().unwrap());
}

#[test]
fn released_pubrel_replay_is_not_filtered_when_publish_receive_window_is_full() {
    let mut engine = SessionEngine::new(session_config(&BrokerConfig::default())).unwrap();
    connect(&mut engine, "reader", true, 2);
    connect(&mut engine, "publisher", true, 2);
    subscribe(&mut engine, "reader", "a", 2);
    handle(&mut engine, "publisher", publication("a", 1, 1));
    handle(&mut engine, "publisher", publication("a", 2, 2));
    handle(&mut engine, "publisher", Packet::PubRel { packet_id: 2 });
    let first = engine
        .poll_delivery_with_changes("reader")
        .unwrap()
        .delivery
        .unwrap();
    engine.begin_delivery("reader", first).unwrap();
    let second = engine
        .poll_delivery_with_changes("reader")
        .unwrap()
        .delivery
        .unwrap();
    let Packet::Publish {
        packet_id: Some(released),
        qos: 2,
        ..
    } = second.frame.packet
    else {
        panic!("QoS2 expected")
    };
    engine.begin_delivery("reader", second).unwrap();
    assert_eq!(
        handle(
            &mut engine,
            "reader",
            Packet::PubRec {
                packet_id: released
            }
        )[0]
        .packet,
        Packet::PubRel {
            packet_id: released
        }
    );
    engine
        .disconnect_with_options("reader", true, None)
        .unwrap();
    connect(&mut engine, "reader", false, 1);
    let first = engine
        .poll_delivery_with_changes("reader")
        .unwrap()
        .delivery
        .unwrap();
    assert!(matches!(
        first.frame.packet,
        Packet::Publish {
            qos: 1,
            dup: true,
            ..
        }
    ));
    // The replayed QoS1 PUBLISH consumes the sole ReceiveMaximum slot. PUBREL
    // must still be hinted and polled; it is not another PUBLISH admission.
    let mut online = HashMap::new();
    let observed = register(&mut online, "reader");
    assert_eq!(notify(&engine, &online), 1);
    assert!(observed.has_changed().unwrap());
    assert_eq!(
        engine
            .poll_delivery_with_changes("reader")
            .unwrap()
            .delivery
            .unwrap()
            .frame
            .packet,
        Packet::PubRel {
            packet_id: released
        }
    );
    assert_eq!(notify(&engine, &online), 0);
}

#[derive(Debug)]
struct ManualClock(AtomicU64);
impl Clock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
#[tokio::test]
async fn will_delay_and_session_expiry_tick_notify_newly_ready_recipient_without_input() {
    for (expiry, delay, advance) in [(60, 1, 1500), (2, 10, 2500)] {
        let database = Database::new();
        let config = database.config();
        let clock = Arc::new(ManualClock(AtomicU64::new(1000)));
        let mut engine = SessionEngine::new_with_clock(
            session_config(&config),
            config.access_control.clone(),
            clock.clone(),
        )
        .unwrap();
        connect(&mut engine, "recipient", true, 1);
        connect(&mut engine, "idle", true, 1);
        subscribe(&mut engine, "recipient", "will/topic", 1);
        let mut source = options(true, 1, expiry);
        source
            .will_properties
            .push(Property::WillDelayInterval(delay));
        engine
            .connect_with_options(
                "source",
                Some(Will {
                    topic: "will/topic".into(),
                    payload: vec![42],
                    qos: 1,
                    retain: false,
                }),
                mqtt_auth::Principal::anonymous(),
                source,
            )
            .unwrap();
        engine
            .disconnect_with_options("source", false, None)
            .unwrap();
        let mut store = SqliteStore::open(&database.0, config.max_state_bytes).unwrap();
        store.commit(&engine.snapshot().unwrap()).unwrap();
        let mut online = HashMap::new();
        let mut ready = register(&mut online, "recipient");
        let idle = register(&mut online, "idle");
        let (sender, receiver) = mpsc::channel(config.max_connections);
        let worker = Worker { sender };
        let task = tokio::task::spawn_blocking(move || {
            run_commands(receiver, config, Some(store), engine, online)
        });
        assert!(!ready.has_changed().unwrap());
        clock.0.store(1000 + advance, Ordering::SeqCst);
        worker.request(Operation::Tick).await.unwrap();
        timeout(Duration::from_secs(2), ready.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(!idle.has_changed().unwrap());
        let Reply::DeliveryPolled(Some(delivery)) = worker
            .request(Operation::PollDelivery {
                client: "recipient".into(),
                generation: 1,
            })
            .await
            .unwrap()
        else {
            panic!("Will delivery expected")
        };
        assert!(
            matches!(&delivery.frame.packet, Packet::Publish { topic, payload, qos: 1, .. } if topic == "will/topic" && payload == &[42])
        );
        stop(worker, task).await;
        let store = SqliteStore::open(&database.0, 64 * 1024 * 1024).unwrap();
        let mut restored = SessionEngine::restore_with_clock(
            session_config(&database.config()),
            &store.load().unwrap().unwrap(),
            Arc::new(mqtt_auth::AllowAnonymous),
            clock.clone(),
        )
        .unwrap();
        connect(&mut restored, "recipient", false, 1);
        assert!(
            matches!(restored.poll_delivery_with_changes("recipient").unwrap().delivery.unwrap().frame.packet, Packet::Publish { payload, qos: 1, dup: true, .. } if payload == [42])
        );
    }
}

#[tokio::test]
async fn one_initial_wake_drains_multiple_qos0_frames_through_capacity_one_output() {
    use super::live_tests::{receive, send};
    timeout(Duration::from_secs(3), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (sender, mut commands) = mpsc::channel::<Command>(1);
        let worker = Worker { sender };
        let (shutdown, stopped) = watch::channel(false);
        let config = BrokerConfig {
            outbound_capacity: 1,
            ..BrokerConfig::default()
        };
        let connection = tokio::spawn(super::connection(server, 1, worker, config, stopped));
        let polls = Arc::new(AtomicU64::new(0));
        let observed = polls.clone();
        let scripted = tokio::spawn(async move {
            // This scripted source isolates network self-polling, not model queue
            // admission. The real output channel/writer has exactly one slot.
            let mut sequence = 0u8;
            let mut registration = None;
            while let Some(command) = commands.recv().await {
                let reply = match command.operation {
                    Operation::ConnectFrameAt {
                        wake,
                        cancel,
                        close_reason,
                        ..
                    } => {
                        wake.send_modify(|value| *value += 1); // only wake in the entire trace
                        registration = Some((wake, cancel, close_reason));
                        Reply::Connected(false)
                    }
                    Operation::PollDelivery { .. } => {
                        observed.fetch_add(1, Ordering::SeqCst);
                        if sequence == 4 {
                            Reply::DeliveryPolled(None)
                        } else {
                            let frame = Frame::v311(publication("drain", sequence, 0));
                            sequence += 1;
                            Reply::DeliveryPolled(Some(Box::new(Delivery {
                                frame,
                                expires_at: None,
                                ticket: None,
                                shared_subscription: None,
                            })))
                        }
                    }
                    Operation::BeginDelivery { delivery, .. } => {
                        Reply::FramePolled(Some(Box::new(delivery.frame)))
                    }
                    Operation::CancelAuthentication { .. }
                    | Operation::Disconnect { .. }
                    | Operation::DisconnectOptions { .. } => Reply::Done,
                    _ => panic!("unexpected operation"),
                };
                let _ = command.reply.send(Ok(reply));
            }
            drop(registration);
        });
        let mut client = client;
        send(
            &mut client,
            Packet::Connect {
                client_id: "drain".into(),
                clean_session: true,
                keep_alive: 0,
                will: None,
                username: None,
                password: None,
            },
        )
        .await;
        assert!(matches!(
            receive(&mut client).await,
            Packet::ConnAck { code: 0, .. }
        ));
        for expected in 0u8..4 {
            assert_eq!(
                receive(&mut client).await,
                publication("drain", expected, 0)
            );
        }
        // No PING or further input was used to elicit any of the four frames.
        assert!(polls.load(Ordering::SeqCst) >= 4);
        send(&mut client, Packet::Disconnect).await;
        drop(client);
        shutdown.send(true).unwrap();
        connection.await.unwrap().unwrap();
        scripted.await.unwrap();
    })
    .await
    .unwrap();
}

#[derive(Debug)]
struct ReceivePolicy(Arc<std::sync::atomic::AtomicBool>);
impl mqtt_auth::AccessControl for ReceivePolicy {
    fn authenticate(
        &self,
        _: mqtt_auth::ConnectInfo<'_>,
    ) -> Result<mqtt_auth::Principal, mqtt_auth::AuthError> {
        Ok(mqtt_auth::Principal::anonymous())
    }
    fn authorize(&self, _: &mqtt_auth::Principal, action: mqtt_auth::Action, _: &str) -> bool {
        action != mqtt_auth::Action::Receive || self.0.load(Ordering::SeqCst)
    }
}
#[tokio::test]
async fn external_qos0_lease_is_still_validated_when_filter_selects_no_notifications() {
    for shared in [false, true] {
        let allowed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let config = BrokerConfig {
            access_control: Arc::new(ReceivePolicy(allowed.clone())),
            ..BrokerConfig::default()
        };
        let mut engine =
            SessionEngine::new_with_access(session_config(&config), config.access_control.clone())
                .unwrap();
        connect(&mut engine, "recipient", true, 1);
        connect(&mut engine, "publisher", true, 1);
        let filter = if shared { "$share/g/a" } else { "a" };
        subscribe(&mut engine, "recipient", filter, 0);
        handle(&mut engine, "publisher", publication("a", 1, 0));
        // Explicit barrier: poll has committed the lease out of the queue, but
        // the socket writer has not begun this frame. No kernel buffer assumed.
        let delivery = engine
            .poll_delivery_with_changes("recipient")
            .unwrap()
            .delivery
            .unwrap();
        assert!(!engine.delivery_may_be_ready("recipient"));
        let mut online = HashMap::new();
        let observed = register(&mut online, "recipient");
        let (sender, receiver) = mpsc::channel(8);
        let worker = Worker { sender };
        let task = tokio::task::spawn_blocking(move || {
            run_commands(receiver, config, None, engine, online)
        });
        if shared {
            let Reply::Frames(replies) = worker
                .request(Operation::HandleFrameAt {
                    client: "recipient".into(),
                    generation: 1,
                    frame: Frame::v5(Packet::Unsubscribe {
                        packet_id: 8,
                        filters: vec![filter.into()],
                    }),
                    received_at_millis: SystemClock.now_millis(),
                })
                .await
                .unwrap()
            else {
                panic!("UNSUBACK expected")
            };
            assert!(matches!(
                replies[0].packet,
                Packet::UnsubAck { packet_id: 8 }
            ));
        } else {
            allowed.store(false, Ordering::SeqCst);
        }
        let Reply::FramePolled(frame) = worker
            .request(Operation::BeginDelivery {
                client: "recipient".into(),
                generation: 1,
                delivery,
            })
            .await
            .unwrap()
        else {
            panic!("begin reply expected")
        };
        assert!(
            frame.is_none(),
            "ACL revocation/shared incarnation must prevent frame bytes"
        );
        assert!(
            !observed.has_changed().unwrap(),
            "no ready queues remain to notify"
        );
        stop(worker, task).await;
    }
}
