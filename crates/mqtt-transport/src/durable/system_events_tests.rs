//! Regression coverage for broker-origin lifecycle notifications.
use super::*;
use crate::PublicationObserver;
use mqtt_protocol::{decode_frame, encode_frame};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[derive(Debug)]
struct Observer(mpsc::Sender<(String, Vec<u8>, String, u8, bool)>);
impl PublicationObserver for Observer {
    fn observe(&self, topic: &str, payload: &[u8], client: &str, qos: u8, retain: bool) {
        let _ = self
            .0
            .try_send((topic.into(), payload.into(), client.into(), qos, retain));
    }
}
async fn read(stream: &mut TcpStream, version: ProtocolVersion) -> Frame {
    timeout(Duration::from_secs(3), async {
        let mut bytes = Vec::new();
        loop {
            if let Some((frame, _)) = decode_frame(&bytes, 65536, Some(version)).unwrap() {
                return frame;
            }
            let mut byte = [0];
            stream.read_exact(&mut byte).await.unwrap();
            bytes.push(byte[0]);
        }
    })
    .await
    .unwrap()
}
async fn write(stream: &mut TcpStream, frame: Frame) {
    stream
        .write_all(&encode_frame(&frame).unwrap())
        .await
        .unwrap();
}
fn frame(version: ProtocolVersion, packet: Packet) -> Frame {
    match version {
        ProtocolVersion::V311 => Frame::v311(packet),
        ProtocolVersion::V5 => Frame::v5(packet),
    }
}
async fn connect(
    address: std::net::SocketAddr,
    client: &str,
    version: ProtocolVersion,
    keep_alive: u16,
) -> TcpStream {
    let mut stream = TcpStream::connect(address).await.unwrap();
    write(
        &mut stream,
        frame(
            version,
            Packet::Connect {
                client_id: client.into(),
                clean_session: true,
                keep_alive,
                will: None,
                username: None,
                password: None,
            },
        ),
    )
    .await;
    assert!(matches!(
        read(&mut stream, version).await.packet,
        Packet::ConnAck { code: 0, .. }
    ));
    stream
}
async fn event(
    rx: &mut mpsc::Receiver<(String, Vec<u8>, String, u8, bool)>,
    client: &str,
    name: &str,
) -> Value {
    timeout(Duration::from_secs(4), async {
        loop {
            let (topic, bytes, metadata_client, qos, retain) = rx.recv().await.unwrap();
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            if value["client_id"] != client || value["event"] != name {
                continue;
            }
            assert_eq!(metadata_client, client);
            assert_eq!(
                topic,
                format!(
                    "$SYS/events/client/{}",
                    if name == "client.connected" {
                        "connected"
                    } else {
                        "disconnected"
                    }
                )
            );
            assert_eq!(qos, 1);
            assert!(!retain);
            assert!(value.get("schema_version").is_none());
            return value;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn system_events_cover_both_versions_normal_abnormal_keepalive_takeover_and_shutdown() {
    for version in [ProtocolVersion::V311, ProtocolVersion::V5] {
        let (tx, mut rx) = mpsc::channel(64);
        let (address, shutdown, task) = super::live_tests::broker(BrokerConfig {
            publication_observer: Some(Arc::new(Observer(tx))),
            ..BrokerConfig::default()
        })
        .await;
        let mut client = connect(address, "subject", version, 0).await;
        let online = event(&mut rx, "subject", "client.connected").await;
        assert_eq!(
            online["protocol"],
            if version == ProtocolVersion::V5 {
                "5.0"
            } else {
                "3.1.1"
            }
        );
        assert!(online["username"].is_null());
        write(&mut client, frame(version, Packet::Disconnect)).await;
        let offline = event(&mut rx, "subject", "client.disconnected").await;
        assert_eq!(offline["reason"], "normal");
        assert_eq!(offline["connection_id"], online["connection_id"]);
        assert_eq!(offline["boot_id"], online["boot_id"]);
        assert!(offline["sequence"].as_u64().unwrap() > online["sequence"].as_u64().unwrap());
        assert_eq!(offline["connected_at_ms"], online["timestamp_ms"]);
        let old = connect(address, "subject", version, 0).await;
        let old_online = event(&mut rx, "subject", "client.connected").await;
        let new = connect(address, "subject", version, 0).await;
        // New online and old offline are independently scheduled; retain both.
        let mut taken = false;
        let mut new_online = None;
        while !taken || new_online.is_none() {
            let (_, bytes, _, _, _) = timeout(Duration::from_secs(3), rx.recv())
                .await
                .unwrap()
                .unwrap();
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            if value["event"] == "client.connected" {
                new_online = Some(value);
            } else if value["reason"] == "session_taken_over" {
                assert_eq!(value["connection_id"], old_online["connection_id"]);
                taken = true;
            }
        }
        assert_ne!(
            new_online.unwrap()["connection_id"],
            old_online["connection_id"]
        );
        drop(old);
        drop(new);
        assert_eq!(
            event(&mut rx, "subject", "client.disconnected").await["reason"],
            "network_closed"
        );
        let _timed = connect(address, "timed", version, 1).await;
        event(&mut rx, "timed", "client.connected").await;
        assert_eq!(
            event(&mut rx, "timed", "client.disconnected").await["reason"],
            "keep_alive_timeout"
        );
        let _live = connect(address, "shutdown", version, 0).await;
        event(&mut rx, "shutdown", "client.connected").await;
        shutdown.send(true).unwrap();
        assert_eq!(
            event(&mut rx, "shutdown", "client.disconnected").await["reason"],
            "server_shutdown"
        );
        timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(rx.try_recv().is_err());
    }
}
#[tokio::test]
async fn broker_origin_preserves_no_local_and_explicit_dollar_subscription_rules() {
    let (tx, mut rx) = mpsc::channel(16);
    let broker = Broker::start(
        BrokerConfig {
            publication_observer: Some(Arc::new(Observer(tx))),
            ..BrokerConfig::default()
        },
        Arc::new(SystemClock),
        None,
    )
    .await
    .unwrap();
    let worker = Worker::new(broker.clone());
    let (wake, _) = watch::channel(0);
    let (cancel, _) = watch::channel(false);
    let (close_reason, _) = watch::channel(None);
    assert!(matches!(
        worker
            .request(Operation::ConnectFrame {
                frame: Frame::v5(Packet::Connect {
                    client_id: "subject".into(),
                    clean_session: true,
                    keep_alive: 0,
                    will: None,
                    username: None,
                    password: None
                }),
                generation: 1,
                wake,
                cancel,
                close_reason,
            })
            .await
            .unwrap(),
        Reply::Connected(_)
    ));
    let mut subscription = Frame::v5(Packet::Subscribe {
        packet_id: 1,
        filters: vec![("$SYS/events/client/+".into(), 1)],
    });
    subscription.subscription_options = vec![5];
    let reply = worker
        .request(Operation::HandleFrameAt {
            client: "subject".into(),
            generation: 1,
            received_at_millis: 0,
            frame: subscription,
        })
        .await
        .unwrap();
    assert!(matches!(reply, Reply::Frames(_)));
    let (wake, _) = watch::channel(0);
    let (cancel, _) = watch::channel(false);
    let (close_reason, _) = watch::channel(None);
    worker
        .request(Operation::ConnectFrame {
            frame: Frame::v5(Packet::Connect {
                client_id: "shared".into(),
                clean_session: true,
                keep_alive: 0,
                will: None,
                username: None,
                password: None,
            }),
            generation: 2,
            wake,
            cancel,
            close_reason,
        })
        .await
        .unwrap();
    let reply = worker
        .request(Operation::HandleFrameAt {
            client: "shared".into(),
            generation: 2,
            received_at_millis: 0,
            frame: Frame::v5(Packet::Subscribe {
                packet_id: 1,
                filters: vec![("$share/group/$SYS/events/client/+".into(), 1)],
            }),
        })
        .await
        .unwrap();
    assert!(
        matches!(reply,Reply::Frames(ref frames) if matches!(&frames[0].packet, Packet::SubAck {codes,..} if codes==&vec![1]))
    );
    let connection = broker
        .events
        .connected(
            "subject".into(),
            Some("alice".into()),
            99,
            ProtocolVersion::V5,
            42,
        )
        .unwrap();
    let online = event(&mut rx, "subject", "client.connected").await;
    assert_eq!(online["username"], "alice");
    let delivery = timeout(Duration::from_secs(2), async {
        loop {
            if let Reply::DeliveryPolled(Some(delivery)) = worker
                .request(Operation::PollDelivery {
                    client: "subject".into(),
                    generation: 1,
                })
                .await
                .unwrap()
            {
                break delivery;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        matches!(delivery.frame.packet,Packet::Publish { ref topic,.. } if topic=="$SYS/events/client/connected")
    );

    let shared = timeout(Duration::from_secs(2), async {
        loop {
            if let Reply::DeliveryPolled(Some(delivery)) = worker
                .request(Operation::PollDelivery {
                    client: "shared".into(),
                    generation: 2,
                })
                .await
                .unwrap()
            {
                break delivery.frame.packet;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        matches!(shared,Packet::Publish { ref topic,.. } if topic=="$SYS/events/client/connected")
    );
    broker.events.disconnected(connection, 40, "normal");
    assert_eq!(
        event(&mut rx, "subject", "client.disconnected").await["duration_ms"],
        0
    );
    broker.shutdown().await.unwrap();
}
