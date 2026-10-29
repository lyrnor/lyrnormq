//! Cross-layer bounded-admission traces; no socket-buffer or RSS assumptions.
use super::tests::{Database, stop, worker};
use super::*;

async fn connect_v5(
    worker: &Worker,
    client: &str,
    generation: u64,
    clean: bool,
) -> watch::Receiver<bool> {
    let (wake, _) = watch::channel(0);
    let (cancel, receive) = watch::channel(false);
    let (close_reason, _) = watch::channel(None);
    let mut frame = Frame::v5(Packet::Connect {
        client_id: client.into(),
        clean_session: clean,
        keep_alive: 0,
        will: None,
        username: None,
        password: None,
    });
    frame.properties = vec![
        Property::ReceiveMaximum(1),
        Property::SessionExpiryInterval(60),
    ];
    assert!(matches!(
        worker
            .request(Operation::ConnectFrame {
                frame,
                generation,
                wake,
                cancel,
                close_reason,
            })
            .await
            .unwrap(),
        Reply::Connected(_)
    ));
    receive
}
async fn handle(
    worker: &Worker,
    client: &str,
    generation: u64,
    packet: Packet,
) -> io::Result<Vec<Frame>> {
    let Reply::Frames(frames) = worker
        .request(Operation::HandleFrameAt {
            client: client.into(),
            generation,
            frame: Frame::v5(packet),
            received_at_millis: SystemClock.now_millis(),
        })
        .await?
    else {
        panic!("frames expected")
    };
    Ok(frames)
}
async fn poll(worker: &Worker, generation: u64) -> Option<Delivery> {
    let Reply::DeliveryPolled(delivery) = worker
        .request(Operation::PollDelivery {
            client: "slow".into(),
            generation,
        })
        .await
        .unwrap()
    else {
        panic!("delivery expected")
    };
    delivery.map(|value| *value)
}
async fn begin(worker: &Worker, generation: u64, delivery: Delivery) -> Frame {
    let Reply::FramePolled(frame) = worker
        .request(Operation::BeginDelivery {
            client: "slow".into(),
            generation,
            delivery,
        })
        .await
        .unwrap()
    else {
        panic!("frame expected")
    };
    *frame.expect("accepted delivery")
}
fn publication(value: u8) -> Packet {
    Packet::Publish {
        topic: "hot".into(),
        payload: vec![value],
        qos: 1,
        packet_id: Some(u16::from(value) + 1),
        dup: false,
        retain: false,
    }
}
fn id(frame: &Frame, expected: u8, duplicate: bool) -> u16 {
    let Packet::Publish {
        packet_id: Some(id),
        payload,
        dup,
        ..
    } = &frame.packet
    else {
        panic!("PUBLISH expected")
    };
    assert_eq!(payload, &[expected]);
    assert_eq!(*dup, duplicate);
    *id
}
async fn trace(sqlite: bool) {
    let database = Database::new();
    let config = BrokerConfig {
        storage_path: sqlite.then(|| database.0.clone()),
        outbound_capacity: 2,
        max_inflight: 4,
        max_connections: 4,
        ..BrokerConfig::default()
    };
    let (worker, task) = worker(config).await;
    let old_cancel = connect_v5(&worker, "slow", 1, true).await;
    connect_v5(&worker, "publisher", 2, true).await;
    connect_v5(&worker, "healthy", 3, true).await;
    handle(
        &worker,
        "slow",
        1,
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![("hot".into(), 1)],
        },
    )
    .await
    .unwrap();
    handle(&worker, "publisher", 2, publication(0))
        .await
        .unwrap();
    let delivery = poll(&worker, 1).await.unwrap();
    let original = begin(&worker, 1, delivery).await;
    let original_id = id(&original, 0, false);
    for value in 1..=2 {
        handle(&worker, "publisher", 2, publication(value))
            .await
            .unwrap();
    }
    assert!(
        poll(&worker, 1).await.is_none(),
        "peer ReceiveMaximum1 blocks more QoS publications"
    );
    // Phase4 admission is per recipient: the full queue drops only its own copy,
    // the publisher is acknowledged, and independent controls stay healthy.
    for _ in 0..32 {
        let replies = handle(&worker, "publisher", 2, publication(99))
            .await
            .unwrap();
        assert_eq!(replies[0].packet, Packet::PubAck { packet_id: 100 });
        let replies = handle(&worker, "healthy", 3, Packet::PingReq)
            .await
            .unwrap();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].packet, Packet::PingResp);
    }
    // Resume while the queue is full. The old generation cannot release the replacement window.
    connect_v5(&worker, "slow", 4, false).await;
    assert!(*old_cancel.borrow());
    assert!(
        handle(
            &worker,
            "slow",
            1,
            Packet::PubAck {
                packet_id: original_id
            }
        )
        .await
        .is_err()
    );
    let replay = begin(&worker, 4, poll(&worker, 4).await.unwrap()).await;
    assert_eq!(id(&replay, 0, true), original_id);
    assert!(poll(&worker, 4).await.is_none());
    handle(
        &worker,
        "slow",
        4,
        Packet::PubAck {
            packet_id: original_id,
        },
    )
    .await
    .unwrap();
    for value in 1..=2 {
        let frame = begin(&worker, 4, poll(&worker, 4).await.unwrap()).await;
        let packet_id = id(&frame, value, false);
        assert!(poll(&worker, 4).await.is_none());
        handle(&worker, "slow", 4, Packet::PubAck { packet_id })
            .await
            .unwrap();
    }
    assert!(
        poll(&worker, 4).await.is_none(),
        "dropped copies must never be delivered"
    );
    handle(&worker, "publisher", 2, publication(3))
        .await
        .unwrap();
    let frame = begin(&worker, 4, poll(&worker, 4).await.unwrap()).await;
    handle(
        &worker,
        "slow",
        4,
        Packet::PubAck {
            packet_id: id(&frame, 3, false),
        },
    )
    .await
    .unwrap();
    assert!(poll(&worker, 4).await.is_none());
    stop(worker, task).await;
}
#[tokio::test]
async fn full_slow_qos_queue_preserves_control_and_takeover_in_memory() {
    timeout(Duration::from_secs(10), trace(false))
        .await
        .unwrap();
}
#[tokio::test]
async fn full_slow_qos_queue_preserves_control_and_takeover_in_sqlite() {
    timeout(Duration::from_secs(10), trace(true)).await.unwrap();
}
