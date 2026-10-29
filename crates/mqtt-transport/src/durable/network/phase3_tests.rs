//! Deterministic writer pressure at the worker barrier, before socket bytes start.
use super::*;

async fn socket() -> (TcpStream, OwnedWriteHalf) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (server, _) = listener.accept().await.unwrap();
    (client, server.into_split().1)
}
#[tokio::test]
async fn full_output_queue_drain_timeout_preserves_unwritten_credits_and_releases_guard() {
    timeout(Duration::from_secs(3), async {
        let (_nonreader, writer) = socket().await;
        let config = BrokerConfig {
            outbound_capacity: 2,
            max_inflight: 2,
            write_timeout: Duration::from_millis(20),
            ..BrokerConfig::default()
        };
        let (worker, worker_task) = super::super::tests::worker(config.clone()).await;
        let broker = Arc::clone(&worker.broker);
        let release = broker.stall("held").await;
        while broker.queued("held") != 0 {
            tokio::task::yield_now().await;
        }
        let (output, receiver) = mpsc::channel(config.outbound_capacity);
        let (terminal, terminal_rx) = watch::channel(None);
        let (cancel, _) = watch::channel(false);
        let credits = Arc::new(Mutex::new(Credits::new(2)));
        let failed = Arc::new(AtomicBool::new(false));
        let writer = spawn_writer(
            writer,
            receiver,
            terminal_rx,
            credits.clone(),
            WriterLimits {
                config: config.clone(),
                peer_maximum: Arc::new(AtomicUsize::new(usize::MAX)),
                peer_alias_maximum: Arc::new(AtomicUsize::new(0)),
            },
            cancel,
            failed.clone(),
        );
        output
            .send(delivery_output(
                Delivery {
                    frame: Frame::v5(Packet::Publish {
                        topic: "held".into(),
                        payload: vec![1],
                        qos: 1,
                        packet_id: Some(7),
                        dup: false,
                        retain: false,
                    }),
                    expires_at: None,
                    ticket: None,
                    shared_subscription: None,
                },
                "held".into(),
                1,
                worker.clone(),
            ))
            .await
            .unwrap();
        // The stalled actor's inbox is an exact barrier: BeginDelivery is waiting
        // for a reply, rather than a guessed kernel-buffer fill or sleeping producer.
        while broker.queued("held") == 0 {
            tokio::task::yield_now().await;
        }
        for id in 1..=2 {
            locked(&credits).unwrap().register(id, 1).unwrap();
            output
                .try_send(outgoing(Frame::v5(Packet::PubAck { packet_id: id }), &credits).unwrap())
                .unwrap();
        }
        assert_eq!(output.capacity(), 0);
        assert!(matches!(
            output.try_send(outgoing(Frame::v5(Packet::PingResp), &credits).unwrap()),
            Err(mpsc::error::TrySendError::Full(_))
        ));
        assert_eq!(locked(&credits).unwrap().entries.len(), 2);
        // An independent connection's writer progresses despite the blocked output.
        let (mut healthy, healthy_writer) = socket().await;
        let (healthy_tx, healthy_rx) = mpsc::channel(1);
        let (_healthy_terminal, healthy_terminal_rx) = watch::channel(None);
        let (healthy_cancel, _) = watch::channel(false);
        let healthy_task = spawn_writer(
            healthy_writer,
            healthy_rx,
            healthy_terminal_rx,
            Arc::new(Mutex::new(Credits::new(1))),
            WriterLimits {
                config: config.clone(),
                peer_maximum: Arc::new(AtomicUsize::new(usize::MAX)),
                peer_alias_maximum: Arc::new(AtomicUsize::new(0)),
            },
            healthy_cancel,
            Arc::new(AtomicBool::new(false)),
        );
        healthy_tx
            .send(
                outgoing(
                    Frame::v5(Packet::PingResp),
                    &Arc::new(Mutex::new(Credits::new(1))),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let mut bytes = [0; 2];
        healthy.read_exact(&mut bytes).await.unwrap();
        assert_eq!(bytes, [0xd0, 0]);
        drop(healthy_tx);
        healthy_task.await.unwrap().unwrap();
        // Mirror serve's connection-level permit guard around finish_writer.
        // The blackbox admission tests separately exercise the real listener.
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = permits.clone().try_acquire_owned().unwrap();
        terminal.send_replace(Some(Frame::v5(Packet::Disconnect)));
        let connection = tokio::spawn(async move {
            let _permit = permit;
            finish_writer(writer, Ok(()), true, config.write_timeout).await
        });
        assert_eq!(permits.available_permits(), 0);
        assert_eq!(
            connection.await.unwrap().unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(permits.available_permits(), 1);
        output.closed().await;
        assert_eq!(
            locked(&credits).unwrap().entries.len(),
            2,
            "unwritten ACKs never release tickets"
        );
        drop(release);
        assert!(
            !failed.load(Ordering::Acquire),
            "outer drain deadline aborts task,not a successful socket write"
        );
        super::super::tests::stop(worker, worker_task).await;
    })
    .await
    .unwrap();
}
