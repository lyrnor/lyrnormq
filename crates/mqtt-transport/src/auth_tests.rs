//! Independent network/transaction checks for the pluggable policy boundary.
use super::live_tests::{broker, closed, receive, send, stop};
use super::*;
use mqtt_auth::{AccessControl, Action, AuthError, ConnectInfo, Principal, covers_filter};
use std::sync::atomic::{AtomicBool, Ordering};

const SECRET: &[u8] = b"transport-test\0\xff";

#[derive(Debug)]
struct TestPolicy {
    receive: Arc<AtomicBool>,
}
impl AccessControl for TestPolicy {
    fn authenticate(&self, info: ConnectInfo<'_>) -> Result<Principal, AuthError> {
        // Synthetic test backend; real PHC verification is tested by mqtt-auth
        // and independent blackbox clients against StaticPolicy.
        if !matches!(info.username, Some("alice" | "bob")) || info.password != Some(SECRET) {
            return Err(AuthError::BadCredentials);
        }
        if info.client_id.starts_with("blocked") {
            return Err(AuthError::NotAuthorized);
        }
        Ok(Principal {
            name: format!("user:{}", info.username.unwrap()),
        })
    }
    fn authorize(&self, principal: &Principal, action: Action, resource: &str) -> bool {
        if !matches!(principal.name.as_str(), "user:alice" | "user:bob") {
            return false;
        }
        match action {
            Action::Connect => !resource.starts_with("blocked"),
            Action::Publish => mqtt_router::matches("allowed/#", resource),
            Action::Subscribe => covers_filter("allowed/#", resource),
            Action::Receive => {
                self.receive.load(Ordering::SeqCst) && mqtt_router::matches("allowed/#", resource)
            }
        }
    }
}
fn policy() -> (Arc<dyn AccessControl>, Arc<AtomicBool>) {
    let receive = Arc::new(AtomicBool::new(true));
    (
        Arc::new(TestPolicy {
            receive: receive.clone(),
        }),
        receive,
    )
}
fn credentials(client: &str, user: &str, password: &[u8], will: Option<Will>) -> Packet {
    Packet::Connect {
        client_id: client.into(),
        clean_session: true,
        keep_alive: 0,
        will,
        username: Some(user.into()),
        password: Some(Password::new(password.to_vec())),
    }
}
async fn accepted(
    address: std::net::SocketAddr,
    client: &str,
    user: &str,
    will: Option<Will>,
) -> TcpStream {
    let mut stream = TcpStream::connect(address).await.unwrap();
    send(&mut stream, credentials(client, user, SECRET, will)).await;
    assert_eq!(
        receive(&mut stream).await,
        Packet::ConnAck {
            session_present: false,
            code: 0
        }
    );
    stream
}
async fn rejected(address: std::net::SocketAddr, packet: Packet, code: u8) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    send(&mut stream, packet).await;
    assert_eq!(
        receive(&mut stream).await,
        Packet::ConnAck {
            session_present: false,
            code
        }
    );
    closed(&mut stream).await;
}
async fn ping(stream: &mut TcpStream) {
    send(stream, Packet::PingReq).await;
    assert_eq!(receive(stream).await, Packet::PingResp);
}

#[tokio::test]
async fn refused_authentication_flushes_connack_and_discards_pipeline_and_will() {
    let (policy, _) = policy();
    let (address, shutdown, task) = broker(BrokerConfig {
        access_control: policy,
        ..BrokerConfig::default()
    })
    .await;
    let mut observer = accepted(address, "observer", "alice", None).await;
    send(
        &mut observer,
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![("allowed/#".into(), 1)],
        },
    )
    .await;
    assert_eq!(
        receive(&mut observer).await,
        Packet::SubAck {
            packet_id: 1,
            codes: vec![1]
        }
    );
    let mut attacker = TcpStream::connect(address).await.unwrap();
    let mut pipeline = encode(&credentials(
        "attacker",
        "alice",
        b"wrong-password",
        Some(Will {
            topic: "allowed/will".into(),
            payload: b"must-not-fire".to_vec(),
            qos: 1,
            retain: true,
        }),
    ))
    .unwrap();
    pipeline.extend(
        encode(&Packet::Publish {
            topic: "allowed/pipeline".into(),
            payload: b"must-not-publish".to_vec(),
            qos: 1,
            packet_id: Some(9),
            dup: false,
            retain: true,
        })
        .unwrap(),
    );
    attacker.write_all(&pipeline).await.unwrap();
    assert_eq!(
        receive(&mut attacker).await,
        Packet::ConnAck {
            session_present: false,
            code: 4
        }
    );
    closed(&mut attacker).await;
    ping(&mut observer).await;
    assert!(
        timeout(Duration::from_millis(80), observer.read(&mut [0u8; 1]))
            .await
            .is_err()
    );
    rejected(
        address,
        credentials("blocked-client", "alice", SECRET, None),
        5,
    )
    .await;
    rejected(
        address,
        credentials(
            "new-owner",
            "alice",
            SECRET,
            Some(Will {
                topic: "forbidden/will".into(),
                payload: b"no".to_vec(),
                qos: 1,
                retain: true,
            }),
        ),
        5,
    )
    .await;
    stop(shutdown, task).await;
}

#[tokio::test]
async fn wrong_owner_or_denied_replacement_will_cannot_cancel_live_connection() {
    let (policy, _) = policy();
    let (address, shutdown, task) = broker(BrokerConfig {
        access_control: policy,
        ..BrokerConfig::default()
    })
    .await;
    let mut original = accepted(
        address,
        "shared",
        "alice",
        Some(Will {
            topic: "allowed/will".into(),
            payload: b"old-will".to_vec(),
            qos: 1,
            retain: false,
        }),
    )
    .await;
    rejected(address, credentials("shared", "bob", SECRET, None), 5).await;
    ping(&mut original).await;
    rejected(
        address,
        credentials(
            "shared",
            "alice",
            SECRET,
            Some(Will {
                topic: "forbidden/will".into(),
                payload: b"bad".to_vec(),
                qos: 1,
                retain: false,
            }),
        ),
        5,
    )
    .await;
    ping(&mut original).await;
    send(&mut original, Packet::Disconnect).await;
    closed(&mut original).await;
    stop(shutdown, task).await;
}

#[tokio::test]
async fn default_anonymous_policy_refuses_supplied_credentials() {
    let (address, shutdown, task) = broker(BrokerConfig::default()).await;
    rejected(address, credentials("unknown", "alice", SECRET, None), 4).await;
    let mut anonymous = TcpStream::connect(address).await.unwrap();
    send(
        &mut anonymous,
        Packet::Connect {
            client_id: "anonymous".into(),
            clean_session: true,
            keep_alive: 0,
            will: None,
            username: None,
            password: None,
        },
    )
    .await;
    assert_eq!(
        receive(&mut anonymous).await,
        Packet::ConnAck {
            session_present: false,
            code: 0
        }
    );
    ping(&mut anonymous).await;
    stop(shutdown, task).await;
}

#[tokio::test]
async fn empty_poll_commits_revoked_queued_payload_and_notifies() {
    let db = super::tests::Database::new();
    let (policy, permission) = policy();
    let config = BrokerConfig {
        access_control: policy.clone(),
        ..db.config()
    };
    let mut engine = SessionEngine::new_with_access(session_config(&config), policy).unwrap();
    let owner = Principal {
        name: "user:alice".into(),
    };
    engine
        .connect_authenticated("receiver", false, None, owner.clone())
        .unwrap();
    engine
        .handle(
            "receiver",
            Packet::Subscribe {
                packet_id: 1,
                filters: vec![("allowed/#".into(), 1)],
            },
        )
        .unwrap();
    engine
        .connect_authenticated("sender", true, None, owner)
        .unwrap();
    engine
        .handle(
            "sender",
            Packet::Publish {
                topic: "allowed/revoked".into(),
                payload: b"old-payload".to_vec(),
                qos: 1,
                packet_id: Some(2),
                dup: false,
                retain: false,
            },
        )
        .unwrap();
    let before = engine.snapshot().unwrap();
    let mut store = SqliteStore::open(&db.0, config.max_state_bytes).unwrap();
    store.commit(&before).unwrap();
    drop(store);
    let (worker, task) = super::tests::worker(config).await;
    let (wake, mut woke) = watch::channel(0);
    let (cancel, _) = watch::channel(false);
    assert!(matches!(
        worker
            .request(Operation::Connect {
                client: "receiver".into(),
                clean: false,
                will: None,
                username: Some("alice".into()),
                password: Some(Password::new(SECRET.to_vec())),
                generation: 1,
                wake,
                cancel
            })
            .await
            .unwrap(),
        Reply::Connected(true)
    ));
    woke.borrow_and_update();
    permission.store(false, Ordering::SeqCst);
    assert!(matches!(
        worker
            .request(Operation::Poll {
                client: "receiver".into(),
                generation: 1
            })
            .await
            .unwrap(),
        Reply::Polled(None)
    ));
    timeout(Duration::from_secs(1), woke.changed())
        .await
        .unwrap()
        .unwrap();
    super::tests::stop(worker, task).await;
    let store = SqliteStore::open(&db.0, 64 * 1024 * 1024).unwrap();
    let after = store.load().unwrap().unwrap();
    assert_ne!(before, after);
    assert!(
        std::str::from_utf8(&after)
            .unwrap()
            .contains("\"queued\":[]")
    );
}

#[tokio::test]
async fn rejected_owner_and_password_leave_persistent_snapshot_unchanged() {
    let db = super::tests::Database::new();
    let (policy, _) = policy();
    let config = BrokerConfig {
        access_control: policy.clone(),
        ..db.config()
    };
    let mut engine = SessionEngine::new_with_access(session_config(&config), policy).unwrap();
    engine
        .connect_authenticated(
            "shared",
            false,
            None,
            Principal {
                name: "user:alice".into(),
            },
        )
        .unwrap();
    let before = engine.snapshot().unwrap();
    let mut store = SqliteStore::open(&db.0, config.max_state_bytes).unwrap();
    store.commit(&before).unwrap();
    drop(store);
    let (worker, task) = super::tests::worker(config).await;
    let (wake, _) = watch::channel(0);
    let (cancel, original_cancelled) = watch::channel(false);
    assert!(matches!(
        worker
            .request(Operation::Connect {
                client: "shared".into(),
                clean: false,
                will: None,
                username: Some("alice".into()),
                password: Some(Password::new(SECRET.to_vec())),
                generation: 1,
                wake,
                cancel,
            })
            .await
            .unwrap(),
        Reply::Connected(true)
    ));
    for (clean, generation) in [(false, 2), (true, 3)] {
        let (wake, _) = watch::channel(0);
        let (cancel, _) = watch::channel(false);
        assert!(matches!(
            worker
                .request(Operation::Connect {
                    client: "shared".into(),
                    clean,
                    will: None,
                    username: Some("bob".into()),
                    password: Some(Password::new(SECRET.to_vec())),
                    generation,
                    wake,
                    cancel,
                })
                .await
                .unwrap(),
            Reply::Rejected(ConnectRefusal::NotAuthorized)
        ));
    }
    let (wake, _) = watch::channel(0);
    let (cancel, _) = watch::channel(false);
    assert!(matches!(
        worker
            .request(Operation::Connect {
                client: "shared".into(),
                clean: true,
                will: None,
                username: Some("alice".into()),
                password: Some(Password::new(b"wrong".to_vec())),
                generation: 4,
                wake,
                cancel,
            })
            .await
            .unwrap(),
        Reply::Rejected(ConnectRefusal::BadCredentials)
    ));
    assert!(!*original_cancelled.borrow());
    assert!(matches!(worker.request(Operation::Handle {
        client: "shared".into(), generation: 1, packet: Packet::PingReq,
    }).await.unwrap(), Reply::Packets(packets) if packets == vec![Packet::PingResp]));
    super::tests::stop(worker, task).await;
    let store = SqliteStore::open(&db.0, 64 * 1024 * 1024).unwrap();
    let after = store.load().unwrap().unwrap();
    assert_eq!(before, after);
    assert!(
        !after
            .windows(b"transport-test".len())
            .any(|window| window == b"transport-test")
    );
}

#[tokio::test]
async fn security_refusal_precedes_volatile_persistence_capability_without_effects() {
    let (policy, _) = policy();
    let config = BrokerConfig {
        access_control: policy,
        ..BrokerConfig::default()
    };
    let (worker, task) = super::tests::worker(config).await;
    let (wake, _) = watch::channel(0);
    let (cancel, original_cancelled) = watch::channel(false);
    assert!(matches!(
        worker
            .request(Operation::Connect {
                client: "shared".into(),
                clean: true,
                will: Some(Will {
                    topic: "allowed/will".into(),
                    payload: b"original".to_vec(),
                    qos: 1,
                    retain: true
                }),
                username: Some("alice".into()),
                password: Some(Password::new(SECRET.to_vec())),
                generation: 1,
                wake,
                cancel,
            })
            .await
            .unwrap(),
        Reply::Connected(false)
    ));
    let (wake, _) = watch::channel(0);
    let (cancel, _) = watch::channel(false);
    assert!(matches!(
        worker
            .request(Operation::Connect {
                client: "observer".into(),
                clean: true,
                will: None,
                username: Some("alice".into()),
                password: Some(Password::new(SECRET.to_vec())),
                generation: 2,
                wake,
                cancel,
            })
            .await
            .unwrap(),
        Reply::Connected(false)
    ));
    worker
        .request(Operation::Handle {
            client: "observer".into(),
            generation: 2,
            packet: Packet::Subscribe {
                packet_id: 1,
                filters: vec![("allowed/#".into(), 1)],
            },
        })
        .await
        .unwrap();
    for clean in [false, true] {
        for (user, will) in [
            ("bob", None),
            (
                "alice",
                Some(Will {
                    topic: "forbidden/will".into(),
                    payload: b"denied".to_vec(),
                    qos: 1,
                    retain: true,
                }),
            ),
        ] {
            let (wake, _) = watch::channel(0);
            let (cancel, _) = watch::channel(false);
            assert!(matches!(
                worker
                    .request(Operation::Connect {
                        client: "shared".into(),
                        clean,
                        will,
                        username: Some(user.into()),
                        password: Some(Password::new(SECRET.to_vec())),
                        generation: 3,
                        wake,
                        cancel,
                    })
                    .await
                    .unwrap(),
                Reply::Rejected(ConnectRefusal::NotAuthorized)
            ));
        }
    }
    let (wake, _) = watch::channel(0);
    let (cancel, _) = watch::channel(false);
    assert!(
        worker
            .request(Operation::Connect {
                client: "shared".into(),
                clean: false,
                will: None,
                username: Some("alice".into()),
                password: Some(Password::new(SECRET.to_vec())),
                generation: 4,
                wake,
                cancel,
            })
            .await
            .is_err()
    );
    assert!(!*original_cancelled.borrow());
    assert!(matches!(
        worker
            .request(Operation::Poll {
                client: "observer".into(),
                generation: 2
            })
            .await
            .unwrap(),
        Reply::Polled(None)
    ));
    assert!(
        matches!(worker.request(Operation::Handle { client: "shared".into(), generation: 1, packet: Packet::PingReq }).await.unwrap(), Reply::Packets(packets) if packets == vec![Packet::PingResp])
    );
    worker
        .request(Operation::Disconnect {
            client: "shared".into(),
            generation: 1,
            graceful: false,
        })
        .await
        .unwrap();
    assert!(
        matches!(worker.request(Operation::Poll { client: "observer".into(), generation: 2 }).await.unwrap(), Reply::Polled(Some(Packet::Publish { topic, payload, .. })) if topic == "allowed/will" && payload == b"original")
    );
    assert!(matches!(
        worker
            .request(Operation::Poll {
                client: "observer".into(),
                generation: 2
            })
            .await
            .unwrap(),
        Reply::Polled(None)
    ));
    super::tests::stop(worker, task).await;
}
