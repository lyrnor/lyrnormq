use super::*;
use std::sync::{
    Condvar,
    atomic::{AtomicUsize, Ordering},
};
#[derive(Debug)]
struct BlockingPolicy {
    entered: AtomicUsize,
    started: tokio::sync::Notify,
    release: (Mutex<bool>, Condvar),
}
impl BlockingPolicy {
    fn new() -> Self {
        Self {
            entered: AtomicUsize::new(0),
            started: tokio::sync::Notify::new(),
            release: (Mutex::new(false), Condvar::new()),
        }
    }
    fn open(&self) {
        *self.release.0.lock().unwrap() = true;
        self.release.1.notify_all();
    }
}
impl mqtt_auth::AccessControl for BlockingPolicy {
    fn authenticate(&self, _: ConnectInfo<'_>) -> Result<mqtt_auth::Principal, AuthError> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        let mut ready = self.release.0.lock().unwrap();
        while !*ready {
            ready = self.release.1.wait(ready).unwrap();
        }
        Ok(mqtt_auth::Principal::anonymous())
    }
    fn authorize(&self, _: &mqtt_auth::Principal, _: mqtt_auth::Action, _: &str) -> bool {
        true
    }
}
fn connect(client: &str, generation: u64) -> Operation {
    Operation::ConnectFrame {
        frame: Frame::v5(Packet::Connect {
            client_id: client.into(),
            clean_session: true,
            keep_alive: 0,
            will: None,
            username: None,
            password: None,
        }),
        generation,
        wake: watch::channel(0).0,
        cancel: watch::channel(false).0,
        close_reason: watch::channel(None).0,
    }
}
#[tokio::test]
async fn cancelled_blocking_job_retains_global_cpu_slot_and_cleanup_is_nonblocking() {
    let policy = Arc::new(BlockingPolicy::new());
    let broker = start(BrokerConfig {
        access_control: policy.clone(),
        max_auth_tasks: 1,
        auth_rate_per_second: 0,
        ..BrokerConfig::default()
    })
    .await
    .unwrap();
    let first = Worker::new(broker.clone());
    let task = {
        let first = first.clone();
        tokio::spawn(async move { first.request(connect("first", 1)).await })
    };
    policy.started.notified().await;
    task.abort();
    let _ = task.await;
    let cleanup = timeout(
        Duration::from_millis(30),
        first.request(Operation::CancelAuthentication { generation: 1 }),
    )
    .await;
    let second = Worker::new(broker.clone());
    let mut next = tokio::spawn(async move { second.request(connect("second", 2)).await });
    let reply = timeout(Duration::from_millis(30), &mut next).await;
    let calls = policy.entered.load(Ordering::SeqCst);
    policy.open();
    if reply.is_err() {
        next.abort();
        let _ = next.await;
    }
    assert!(
        cleanup.is_ok(),
        "cleanup waited behind the running provider mutex"
    );
    assert_eq!(
        calls, 1,
        "cancelled caller released an outstanding authentication slot"
    );
    assert!(matches!(
        reply,
        Ok(Ok(Ok(Reply::FrameRejected(ReasonCode::SERVER_BUSY))))
    ));
    assert!(
        !broker.contains("first"),
        "cancelled credential job installed a session"
    );
    broker.shutdown().await.unwrap();
}
#[tokio::test]
async fn explicit_cancel_discards_late_provider_success_and_slot_recovers() {
    let policy = Arc::new(BlockingPolicy::new());
    let broker = start(BrokerConfig {
        access_control: policy.clone(),
        max_auth_tasks: 1,
        auth_rate_per_second: 0,
        ..BrokerConfig::default()
    })
    .await
    .unwrap();
    let worker = Worker::new(broker.clone());
    let task = {
        let worker = worker.clone();
        tokio::spawn(async move { worker.request(connect("late", 1)).await })
    };
    policy.started.notified().await;
    timeout(
        Duration::from_millis(30),
        worker.request(Operation::CancelAuthentication { generation: 1 }),
    )
    .await
    .unwrap()
    .unwrap();
    policy.open();
    assert!(task.await.unwrap().is_err());
    assert!(!broker.contains("late"));
    assert!(matches!(
        worker.request(connect("fresh", 2)).await.unwrap(),
        Reply::Connected(false)
    ));
    assert_eq!(policy.entered.load(Ordering::SeqCst), 2);
    broker.shutdown().await.unwrap();
}
#[tokio::test]
async fn rate_rejection_is_global_cancel_is_free_and_311_maps_server_unavailable() {
    let broker = start(BrokerConfig {
        auth_rate_per_second: 1,
        auth_burst: 1,
        ..BrokerConfig::default()
    })
    .await
    .unwrap();
    let first = Worker::new(broker.clone());
    assert!(matches!(
        first.request(connect("one", 1)).await.unwrap(),
        Reply::Connected(false)
    ));
    first
        .request(Operation::CancelAuthentication { generation: 1 })
        .await
        .unwrap();
    let second = Worker::new(broker.clone());
    assert!(matches!(
        second.request(connect("five", 2)).await.unwrap(),
        Reply::FrameRejected(ReasonCode::SERVER_BUSY)
    ));
    let mut request = connect("old", 3);
    if let Operation::ConnectFrame { frame, .. } = &mut request {
        frame.version = ProtocolVersion::V311;
    }
    assert!(matches!(
        second.request(request).await.unwrap(),
        Reply::Rejected(ConnectRefusal::ServerBusy)
    ));
    assert_eq!(ConnectRefusal::ServerBusy.code(), 3);
    assert!(!broker.contains("five"));
    assert!(!broker.contains("old"));
    broker.shutdown().await.unwrap();
}
#[tokio::test]
async fn network_auth_timeout_releases_handshake_but_keeps_running_cpu_job_bounded() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let policy = Arc::new(BlockingPolicy::new());
    let config = BrokerConfig {
        access_control: policy.clone(),
        max_auth_tasks: 1,
        max_handshakes: 1,
        auth_rate_per_second: 0,
        handshake_timeout: Duration::from_millis(50),
        ..BrokerConfig::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = watch::channel(false);
    let mut serving = tokio::spawn(crate::serve(listener, config, stopped));
    let mut client = TcpStream::connect(address).await.unwrap();
    let Operation::ConnectFrame { frame, .. } = connect("blocked", 1) else {
        panic!("connect fixture")
    };
    let bytes = mqtt_protocol::encode_frame(&frame).unwrap();
    client.write_all(&bytes).await.unwrap();
    policy.started.notified().await;
    let mut first = [0; 1];
    let first_result = timeout(Duration::from_millis(250), client.read(&mut first)).await;
    let busy = if matches!(first_result, Ok(Ok(0))) {
        let mut second = TcpStream::connect(address).await.unwrap();
        second.write_all(&bytes).await.unwrap();
        let mut ack = [0; 5];
        matches!(
            timeout(Duration::from_millis(150), second.read_exact(&mut ack)).await,
            Ok(Ok(_))
        ) && ack == [0x20, 3, 0, 0x89, 0]
    } else {
        false
    };
    stop.send(true).unwrap();
    let finished = timeout(Duration::from_millis(150), &mut serving).await;
    let stopped_promptly = finished.is_ok();
    policy.open();
    if finished.is_err() {
        let _ = serving.await;
    }
    assert!(
        matches!(first_result, Ok(Ok(0))),
        "provider wait defeated MQTT handshake timeout"
    );
    assert!(
        busy,
        "timed-out connection held the handshake slot or released the running CPU slot"
    );
    assert_eq!(policy.entered.load(Ordering::SeqCst), 1);
    assert!(stopped_promptly, "shutdown waited behind provider");
}
#[derive(Default)]
struct FlushGate {
    open: std::sync::atomic::AtomicBool,
    started: tokio::sync::Notify,
    waker: Mutex<Option<std::task::Waker>>,
}
struct ProbeWriter(Arc<FlushGate>);
impl tokio::io::AsyncWrite for ProbeWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        self.0.started.notify_one();
        std::task::Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        if self.0.open.load(Ordering::Acquire) {
            std::task::Poll::Ready(Ok(()))
        } else {
            *self.0.waker.lock().unwrap() = Some(cx.waker().clone());
            std::task::Poll::Pending
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}
struct ProbeStream {
    reader: tokio::io::DuplexStream,
    writer: ProbeWriter,
}
impl crate::io_stream::IoStream for ProbeStream {
    const GRACEFUL_CLOSE: bool = false;
    type Reader = tokio::io::DuplexStream;
    type Writer = ProbeWriter;
    fn into_io(self) -> io::Result<(Self::Reader, Self::Writer)> {
        Ok((self.reader, self.writer))
    }
}
#[tokio::test]
async fn handshake_slot_is_held_until_successful_connack_really_flushes() {
    use tokio::io::AsyncWriteExt;
    let config = BrokerConfig {
        max_handshakes: 1,
        auth_rate_per_second: 0,
        ..BrokerConfig::default()
    };
    let broker = start(config.clone()).await.unwrap();
    let mut worker = Worker::new(broker.clone());
    worker.handshake = Some(Arc::new(Mutex::new(Some(
        broker.admission.handshake().unwrap(),
    ))));
    let gate = Arc::new(FlushGate::default());
    let (reader, mut peer) = tokio::io::duplex(4096);
    let stream = ProbeStream {
        reader,
        writer: ProbeWriter(gate.clone()),
    };
    let (stop, stopped) = watch::channel(false);
    let task = {
        let worker = worker.clone();
        tokio::spawn(async move {
            let mut identity = None;
            network::connection_loop(stream, 1, &worker, &config, stopped, &mut identity).await
        })
    };
    let Operation::ConnectFrame { frame, .. } = connect("flush", 1) else {
        panic!("fixture")
    };
    peer.write_all(&mqtt_protocol::encode_frame(&frame).unwrap())
        .await
        .unwrap();
    gate.started.notified().await;
    assert!(
        broker.admission.handshake().is_none(),
        "actor acceptance or buffered CONNACK released handshake capacity"
    );
    gate.open.store(true, Ordering::Release);
    if let Some(waker) = gate.waker.lock().unwrap().take() {
        waker.wake();
    }
    timeout(Duration::from_millis(200), async {
        while worker.handshake.as_ref().unwrap().lock().unwrap().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(broker.admission.handshake().is_some());
    stop.send(true).unwrap();
    task.await.unwrap().unwrap();
    broker.shutdown().await.unwrap();
}
#[tokio::test]
async fn established_enhanced_reauthentication_exhaustion_is_disconnect_quota() {
    let mut config = BrokerConfig {
        access_control: Arc::new(super::enhanced_tests::Policy),
        auth_rate_per_second: 1,
        auth_burst: 1,
        ..BrokerConfig::default()
    };
    config.enhanced_auth = Some(Arc::new(mqtt_auth::PlainAuthenticator::new(
        config.access_control.clone(),
    )));
    let broker = start(config).await.unwrap();
    let worker = Worker::new(broker.clone());
    let mut request = connect("enhanced", 1);
    if let Operation::ConnectFrame { frame, .. } = &mut request {
        frame
            .properties
            .push(Property::AuthenticationMethod("PLAIN".into()));
        frame
            .properties
            .push(Property::AuthenticationData(b"\0alice\0password".to_vec()));
    }
    assert!(matches!(
        worker.request(request).await.unwrap(),
        Reply::Authenticated { .. }
    ));
    let mut frame = Frame::v5(Packet::Auth);
    frame.reason_code = ReasonCode::try_from(0x19).unwrap();
    frame
        .properties
        .push(Property::AuthenticationMethod("PLAIN".into()));
    let error = worker
        .request(Operation::Authenticate {
            generation: 1,
            frame,
            deadline: std::time::Instant::now() + Duration::from_secs(1),
        })
        .await
        .err()
        .unwrap();
    assert_eq!(reason(&error), Some(ReasonCode::QUOTA_EXCEEDED));
    broker.shutdown().await.unwrap();
}
async fn eventually(condition: impl Fn() -> bool) {
    timeout(Duration::from_secs(1), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn persistent_connect(client: &str, generation: u64) -> Operation {
    let mut operation = connect(client, generation);
    if let Operation::ConnectFrame { frame, .. } = &mut operation {
        frame.properties.push(Property::SessionExpiryInterval(60));
        if let Packet::Connect { will, .. } = &mut frame.packet {
            *will = Some(Will {
                topic: "admission/will".into(),
                payload: format!("generation-{generation}").into_bytes(),
                qos: 1,
                retain: false,
            });
        }
    }
    operation
}
#[tokio::test]
async fn cancelled_sqlite_connect_retains_cleanup_slot_and_fences_old_generation() {
    for replacement in [false, true] {
        let db = super::tests::Database::new();
        let config = BrokerConfig {
            storage_path: Some(db.0.clone()),
            max_handshakes: if replacement { 2 } else { 1 },
            auth_rate_per_second: 0,
            ..BrokerConfig::default()
        };
        let broker = start(config).await.unwrap();
        let old = Worker::new(broker.clone());
        assert!(matches!(
            old.request(persistent_connect("target", 1)).await.unwrap(),
            Reply::Connected(false)
        ));
        let connection = rusqlite::Connection::open(&db.0).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        let mut pending = Worker::new(broker.clone());
        pending.handshake = Some(Arc::new(Mutex::new(Some(
            broker.admission.handshake().unwrap(),
        ))));
        let _replacement_slot = if replacement {
            Some(broker.admission.handshake().unwrap())
        } else {
            None
        };
        let caller =
            tokio::spawn(async move { pending.request(persistent_connect("target", 2)).await });
        eventually(|| broker.storage_jobs() > 0).await;
        caller.abort();
        let _ = caller.await;
        let held_through_commit = broker.admission.handshake().is_none();
        let next = if replacement {
            let fresh = Worker::new(broker.clone());
            let next =
                tokio::spawn(async move { fresh.request(persistent_connect("target", 3)).await });
            eventually(|| {
                broker
                    .handle("target")
                    .is_some_and(|h| h.sender.capacity() < h.sender.max_capacity())
            })
            .await;
            Some(next)
        } else {
            None
        };
        connection.execute_batch("COMMIT").unwrap();
        drop(connection);
        if let Some(next) = next {
            assert!(matches!(next.await.unwrap().unwrap(), Reply::Connected(_)));
        }
        eventually(|| broker.storage_jobs() == 0).await;
        let recovered = timeout(Duration::from_millis(200), async {
            loop {
                if let Some(slot) = broker.admission.handshake() {
                    drop(slot);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
        // Give any queued same-generation Disconnect a turn, without relying on
        // its order relative to an already-enqueued newer connection.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        let online = broker.is_current("target", if replacement { 3 } else { 2 });
        broker.shutdown().await.unwrap();
        assert!(
            held_through_commit,
            "cancelled in-flight CONNECT released its cleanup bound before SQLite commit"
        );
        assert!(recovered, "cleanup never released its handshake slot");
        if replacement {
            assert!(
                online,
                "stale cleanup disconnected the replacement generation"
            );
        } else {
            assert!(
                !online,
                "cancelled CONNECT left an online generation after commit"
            );
        }
    }
}
