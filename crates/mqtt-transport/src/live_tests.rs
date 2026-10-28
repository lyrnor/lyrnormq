//! Existing live transport regression tests.
use super::*;
use crate::server::serve;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::{io::AsyncWriteExt, net::TcpListener, time::timeout};

async fn broker(
    config: BrokerConfig,
) -> (SocketAddr, watch::Sender<bool>, JoinHandle<io::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (tx, rx) = watch::channel(false);
    let handle = tokio::spawn(serve(listener, config, rx));
    (address, tx, handle)
}
async fn send(stream: &mut TcpStream, packet: Packet) {
    stream.write_all(&encode(&packet).unwrap()).await.unwrap();
}
async fn receive(stream: &mut TcpStream) -> Packet {
    timeout(Duration::from_secs(2), async {
        let mut bytes = Vec::new();
        loop {
            if let Some((packet, _)) = decode(&bytes, 4096).unwrap() {
                return packet;
            }
            let mut byte = [0];
            stream.read_exact(&mut byte).await.unwrap();
            bytes.push(byte[0]);
        }
    })
    .await
    .unwrap()
}
async fn connected(address: SocketAddr, name: &str) -> TcpStream {
    let mut stream = TcpStream::connect(address).await.unwrap();
    send(
        &mut stream,
        Packet::Connect {
            client_id: name.into(),
            clean_session: true,
            keep_alive: 0,
        },
    )
    .await;
    assert_eq!(
        receive(&mut stream).await,
        Packet::ConnAck {
            session_present: false,
            code: 0
        }
    );
    stream
}
async fn closed(stream: &mut TcpStream) {
    let mut byte = [0];
    assert!(matches!(
        timeout(Duration::from_secs(2), stream.read(&mut byte)).await,
        Ok(Ok(0)) | Ok(Err(_))
    ));
}
async fn stop(tx: watch::Sender<bool>, handle: JoinHandle<io::Result<()>>) {
    tx.send(true).unwrap();
    timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn zero_limits_rejected() {
    for config in [
        BrokerConfig {
            outbound_capacity: 0,
            ..BrokerConfig::default()
        },
        BrokerConfig {
            max_inflight: 0,
            ..BrokerConfig::default()
        },
        BrokerConfig {
            max_inflight: 65536,
            ..BrokerConfig::default()
        },
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (_tx, rx) = watch::channel(false);
        assert_eq!(
            serve(listener, config, rx).await.unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
#[tokio::test]
async fn handshake_timeout_and_connection_capacity() {
    let config = BrokerConfig {
        max_connections: 1,
        handshake_timeout: Duration::from_millis(100),
        ..BrokerConfig::default()
    };
    let (address, tx, handle) = broker(config).await;
    let mut first = connected(address, "one").await;
    let mut excess = TcpStream::connect(address).await.unwrap();
    closed(&mut excess).await;
    send(&mut first, Packet::Disconnect).await;
    closed(&mut first).await;
    // Completion of EOF precedes the task dropping its permit by a few instructions.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut idle = TcpStream::connect(address).await.unwrap();
    closed(&mut idle).await;
    stop(tx, handle).await;
}
#[tokio::test]
async fn subscription_limit_and_replacement() {
    let (address, tx, handle) = broker(BrokerConfig {
        max_subscriptions: 2,
        ..BrokerConfig::default()
    })
    .await;
    let mut stream = connected(address, "subscriber").await;
    send(
        &mut stream,
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![("a".into(), 2), ("b".into(), 1), ("c".into(), 0)],
        },
    )
    .await;
    assert_eq!(
        receive(&mut stream).await,
        Packet::SubAck {
            packet_id: 1,
            codes: vec![2, 1, 0x80]
        }
    );
    send(
        &mut stream,
        Packet::Subscribe {
            packet_id: 2,
            filters: vec![("a".into(), 0)],
        },
    )
    .await;
    assert_eq!(
        receive(&mut stream).await,
        Packet::SubAck {
            packet_id: 2,
            codes: vec![0]
        }
    );
    send(
        &mut stream,
        Packet::Unsubscribe {
            packet_id: 3,
            filters: vec!["b".into()],
        },
    )
    .await;
    assert_eq!(
        receive(&mut stream).await,
        Packet::UnsubAck { packet_id: 3 }
    );
    send(
        &mut stream,
        Packet::Subscribe {
            packet_id: 4,
            filters: vec![("c".into(), 0)],
        },
    )
    .await;
    assert_eq!(
        receive(&mut stream).await,
        Packet::SubAck {
            packet_id: 4,
            codes: vec![0]
        }
    );
    stop(tx, handle).await;
    closed(&mut stream).await;
}
#[tokio::test]
async fn advertised_oversize_closed_before_payload() {
    let (address, tx, handle) = broker(BrokerConfig {
        max_packet_size: 32,
        ..BrokerConfig::default()
    })
    .await;
    let mut stream = connected(address, "small").await;
    stream.write_all(&[0x30, 100]).await.unwrap();
    closed(&mut stream).await;
    stop(tx, handle).await;
}
#[tokio::test]
async fn takeover_removes_old_subscriptions_and_old_cleanup_is_safe() {
    let (address, tx, handle) = broker(BrokerConfig::default()).await;
    let mut old = connected(address, "same").await;
    send(
        &mut old,
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![("old".into(), 0)],
        },
    )
    .await;
    receive(&mut old).await;
    let mut new = connected(address, "same").await;
    closed(&mut old).await;
    send(
        &mut new,
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![("new".into(), 0)],
        },
    )
    .await;
    receive(&mut new).await;
    send(
        &mut new,
        Packet::Publish {
            topic: "new".into(),
            payload: vec![42],
            qos: 0,
            packet_id: None,
            dup: false,
            retain: false,
        },
    )
    .await;
    assert_eq!(
        receive(&mut new).await,
        Packet::Publish {
            topic: "new".into(),
            payload: vec![42],
            qos: 0,
            packet_id: None,
            dup: false,
            retain: false,
        }
    );
    stop(tx, handle).await;
}
#[tokio::test]
async fn false_shutdown_notification_keeps_server_running() {
    let (address, tx, handle) = broker(BrokerConfig::default()).await;
    tx.send(false).unwrap();
    let mut stream = connected(address, "alive").await;
    send(&mut stream, Packet::PingReq).await;
    assert_eq!(receive(&mut stream).await, Packet::PingResp);
    stop(tx, handle).await;
}
#[tokio::test]
async fn invalid_unsubscribe_filter_closes_connection() {
    let (address, tx, handle) = broker(BrokerConfig::default()).await;
    let mut stream = connected(address, "invalid").await;
    send(
        &mut stream,
        Packet::Unsubscribe {
            packet_id: 1,
            filters: vec!["a#".into()],
        },
    )
    .await;
    closed(&mut stream).await;
    stop(tx, handle).await;
}
fn publication(topic: &str, payload: &[u8], qos: u8, packet_id: Option<u16>) -> Packet {
    Packet::Publish {
        topic: topic.into(),
        payload: payload.to_vec(),
        qos,
        packet_id,
        dup: false,
        retain: false,
    }
}
async fn subscribe(stream: &mut TcpStream, topic: &str, qos: u8) {
    send(
        stream,
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![(topic.into(), qos)],
        },
    )
    .await;
    assert_eq!(
        receive(stream).await,
        Packet::SubAck {
            packet_id: 1,
            codes: vec![qos]
        }
    );
}
async fn no_packet(stream: &mut TcpStream) {
    let mut byte = [0];
    assert!(
        timeout(Duration::from_millis(40), stream.read(&mut byte))
            .await
            .is_err()
    );
}
fn delivered_id(packet: Packet, expected_qos: u8, expected_payload: &[u8]) -> u16 {
    let Packet::Publish {
        qos,
        packet_id,
        payload,
        dup,
        retain,
        ..
    } = packet
    else {
        panic!("expected PUBLISH")
    };
    assert_eq!(qos, expected_qos);
    assert_eq!(payload, expected_payload);
    assert!(!dup);
    assert!(!retain);
    packet_id.unwrap()
}
#[tokio::test]
async fn qos2_full_outgoing_window_keeps_ack_reads_live() {
    let (address, tx, handle) = broker(BrokerConfig {
        max_inflight: 1,
        ..BrokerConfig::default()
    })
    .await;
    let mut sub = connected(address, "qos2-sub").await;
    let mut publisher = connected(address, "qos2-pub").await;
    subscribe(&mut sub, "a", 2).await;
    send(&mut publisher, publication("a", b"first", 2, Some(55))).await;
    assert_eq!(
        receive(&mut publisher).await,
        Packet::PubRec { packet_id: 55 }
    );
    no_packet(&mut sub).await;
    let mut duplicate = publication("a", b"ignored duplicate", 2, Some(55));
    if let Packet::Publish { dup, .. } = &mut duplicate {
        *dup = true;
    }
    send(&mut publisher, duplicate).await;
    assert_eq!(
        receive(&mut publisher).await,
        Packet::PubRec { packet_id: 55 }
    );
    send(&mut publisher, Packet::PubRel { packet_id: 55 }).await;
    assert_eq!(
        receive(&mut publisher).await,
        Packet::PubComp { packet_id: 55 }
    );
    let first = delivered_id(receive(&mut sub).await, 2, b"first");
    assert_ne!(first, 55);
    send(&mut sub, Packet::PubRec { packet_id: first }).await;
    assert_eq!(receive(&mut sub).await, Packet::PubRel { packet_id: first });
    send(&mut sub, Packet::PubRec { packet_id: first }).await;
    assert_eq!(receive(&mut sub).await, Packet::PubRel { packet_id: first });
    // Duplicated incoming PUBREL returns PUBCOMP without another delivery.
    send(&mut publisher, Packet::PubRel { packet_id: 55 }).await;
    assert_eq!(
        receive(&mut publisher).await,
        Packet::PubComp { packet_id: 55 }
    );
    send(&mut publisher, publication("a", b"second", 1, Some(56))).await;
    assert_eq!(
        receive(&mut publisher).await,
        Packet::PubAck { packet_id: 56 }
    );
    no_packet(&mut sub).await;
    send(&mut sub, Packet::PubComp { packet_id: first }).await;
    let second = delivered_id(receive(&mut sub).await, 1, b"second");
    assert_ne!(first, second);
    send(&mut sub, Packet::PubAck { packet_id: second }).await;
    send(&mut sub, Packet::PingReq).await;
    assert_eq!(receive(&mut sub).await, Packet::PingResp);
    stop(tx, handle).await;
}
#[tokio::test]
async fn inbound_outbound_packet_identifiers_are_independent() {
    let (address, tx, handle) = broker(BrokerConfig {
        max_inflight: 1,
        ..BrokerConfig::default()
    })
    .await;
    let mut stream = connected(address, "both-directions").await;
    subscribe(&mut stream, "a", 1).await;
    send(&mut stream, publication("a", b"one", 1, Some(99))).await;
    assert_eq!(receive(&mut stream).await, Packet::PubAck { packet_id: 99 });
    let outgoing = delivered_id(receive(&mut stream).await, 1, b"one");
    send(&mut stream, publication("a", b"two", 2, Some(outgoing))).await;
    assert_eq!(
        receive(&mut stream).await,
        Packet::PubRec {
            packet_id: outgoing
        }
    );
    send(
        &mut stream,
        Packet::PubRel {
            packet_id: outgoing,
        },
    )
    .await;
    assert_eq!(
        receive(&mut stream).await,
        Packet::PubComp {
            packet_id: outgoing
        }
    );
    no_packet(&mut stream).await;
    send(
        &mut stream,
        Packet::PubAck {
            packet_id: outgoing,
        },
    )
    .await;
    let next = delivered_id(receive(&mut stream).await, 1, b"two");
    assert_ne!(next, outgoing);
    send(&mut stream, Packet::PubAck { packet_id: next }).await;
    stop(tx, handle).await;
}
#[tokio::test]
async fn incoming_qos2_window_limit_disconnects_without_acknowledging_excess() {
    let (address, tx, handle) = broker(BrokerConfig {
        max_inflight: 1,
        ..BrokerConfig::default()
    })
    .await;
    let mut stream = connected(address, "incoming-full").await;
    send(&mut stream, publication("a", b"one", 2, Some(1))).await;
    assert_eq!(receive(&mut stream).await, Packet::PubRec { packet_id: 1 });
    send(&mut stream, publication("a", b"two", 2, Some(2))).await;
    closed(&mut stream).await;
    stop(tx, handle).await;
}
#[tokio::test]
async fn wrong_stage_ack_disconnects_and_unknown_pubrel_is_idempotent() {
    let (address, tx, handle) = broker(BrokerConfig::default()).await;
    let mut stream = connected(address, "wrong-stage").await;
    subscribe(&mut stream, "a", 2).await;
    send(&mut stream, Packet::PubRel { packet_id: 321 }).await;
    assert_eq!(
        receive(&mut stream).await,
        Packet::PubComp { packet_id: 321 }
    );
    send(&mut stream, publication("a", b"one", 2, Some(11))).await;
    assert_eq!(receive(&mut stream).await, Packet::PubRec { packet_id: 11 });
    send(&mut stream, Packet::PubRel { packet_id: 11 }).await;
    assert_eq!(
        receive(&mut stream).await,
        Packet::PubComp { packet_id: 11 }
    );
    let outgoing = delivered_id(receive(&mut stream).await, 2, b"one");
    send(
        &mut stream,
        Packet::PubComp {
            packet_id: outgoing,
        },
    )
    .await;
    closed(&mut stream).await;
    stop(tx, handle).await;
}
#[tokio::test]
async fn qos_is_downgraded_to_publication_and_subscription_minimum() {
    let (address, tx, handle) = broker(BrokerConfig::default()).await;
    let mut sub = connected(address, "downgrade-sub").await;
    let mut publisher = connected(address, "downgrade-pub").await;
    subscribe(&mut sub, "a", 2).await;
    send(&mut publisher, publication("a", b"zero", 0, None)).await;
    assert_eq!(receive(&mut sub).await, publication("a", b"zero", 0, None));
    subscribe(&mut sub, "a", 0).await;
    send(&mut publisher, publication("a", b"two", 2, Some(5))).await;
    assert_eq!(
        receive(&mut publisher).await,
        Packet::PubRec { packet_id: 5 }
    );
    send(&mut publisher, Packet::PubRel { packet_id: 5 }).await;
    assert_eq!(
        receive(&mut publisher).await,
        Packet::PubComp { packet_id: 5 }
    );
    assert_eq!(receive(&mut sub).await, publication("a", b"two", 0, None));
    stop(tx, handle).await;
}
#[tokio::test]
async fn continuous_qos0_delivery_does_not_starve_ping_reads() {
    // Drive a connection's real queue without router overflow so it remains
    // continuously ready; PING reads must compete fairly with socket writes.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = Arc::new(Mutex::new(State::default()));
    let connection_state = Arc::clone(&state);
    let (stop_tx, stop_rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        connection(
            stream,
            1,
            &connection_state,
            &BrokerConfig {
                outbound_capacity: 1024,
                ..BrokerConfig::default()
            },
            stop_rx,
        )
        .await
    });
    let mut stream = connected(address, "fairness").await;
    let sender = {
        lock(&state)
            .unwrap()
            .clients
            .get(&1)
            .unwrap()
            .outgoing
            .clone()
    };
    let message = Arc::new(publication("a", b"x", 0, None));
    for _ in 0..1024 {
        sender
            .try_send(Delivery {
                publication: Arc::clone(&message),
                qos: 0,
            })
            .unwrap_or_else(|_| panic!("queue unexpectedly full"));
    }
    let producer = tokio::spawn(async move {
        while sender
            .send(Delivery {
                publication: Arc::clone(&message),
                qos: 0,
            })
            .await
            .is_ok()
        {}
    });
    send(&mut stream, Packet::PingReq).await;
    let response = timeout(Duration::from_millis(500), async {
        loop {
            if receive(&mut stream).await == Packet::PingResp {
                break;
            }
        }
    })
    .await;
    producer.abort();
    stop_tx.send(true).unwrap();
    timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        response.is_ok(),
        "continuous queued delivery starved PINGREQ"
    );
}
#[test]
fn removed_generation_cannot_mutate_routing() {
    let state = State::default();
    assert!(active(&state, 1).is_err());
}
