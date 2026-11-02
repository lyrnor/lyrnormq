use super::*;
#[test]
fn bounded_configuration_and_secret_restoration() {
    let mut c = KafkaConfig {
        brokers: vec!["localhost:9092".into()],
        topic: "topic".into(),
        ..KafkaConfig::default()
    };
    assert!(c.validate().is_ok());
    c.topic = "..".into();
    assert!(c.validate().is_err());
    c.topic = "...".into();
    assert!(c.validate().is_ok());
    c.security = KafkaSecurity {
        protocol: SecurityProtocol::SaslPlaintext,
        sasl_mechanism: Some("PLAIN".into()),
        username: Some("u".into()),
        password: Some("secret".into()),
        ..KafkaSecurity::default()
    };
    assert!(c.validate().is_ok());
    let old = c.clone();
    c.redact();
    assert!(!format!("{c:?}").contains("secret"));
    assert_eq!(c.security.password.as_deref(), Some("[redacted]"));
    assert!(c.validate().is_err());
    c.restore(Some(&old)).unwrap();
    assert!(c.validate().is_ok());
}
#[tokio::test]
async fn native_deadline_and_shutdown_are_bounded() {
    use mqtt_egress_core::Sink;
    let c = KafkaConfig {
        brokers: vec!["127.0.0.1:1".into()],
        topic: "events".into(),
        ..KafkaConfig::default()
    };
    let s = KafkaSink::new("test", c, 1000, 1).unwrap();
    let value=br#"{"event_id":"e","delivery_id":"d","rule_id":"r","client_id":"c","topic":"t","timestamp_ms":1,"data":{"v":1}}"#;
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), s.send(&[value]))
            .await
            .unwrap()
            .is_err()
    );
    s.shutdown().await.unwrap();
    s.shutdown().await.unwrap();
    assert!(s.send(&[value]).await.is_err());
}

#[tokio::test]
async fn producer_constructor_rejects_invalid_concurrency_and_identity() {
    let c = KafkaConfig {
        brokers: vec!["127.0.0.1:1".into()],
        topic: "events".into(),
        ..KafkaConfig::default()
    };
    assert!(KafkaSink::new("valid", c.clone(), 1000, 0).is_err());
    assert!(KafkaSink::new("valid", c.clone(), 1000, 513).is_err());
    assert!(KafkaSink::new("secret/unsafe", c, 1000, 1).is_err());
}

#[tokio::test]
async fn mock_kafka_records_preserve_data_keys_headers_and_same_client_order() {
    use mqtt_egress_core::Sink;
    use rdkafka::{
        ClientConfig, Offset, TopicPartitionList,
        consumer::{Consumer, StreamConsumer},
        message::{Headers, Message},
        mocking::MockCluster,
    };
    let cluster = MockCluster::new(3).unwrap();
    cluster.create_topic("events", 3, 1).unwrap();
    let c = KafkaConfig {
        brokers: cluster
            .bootstrap_servers()
            .split(',')
            .map(str::to_owned)
            .collect(),
        topic: "events".into(),
        ..KafkaConfig::default()
    };
    let sink = KafkaSink::new("mock", c, 5000, 128).unwrap();
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", cluster.bootstrap_servers())
        .set("group.id", "adapter-order-test")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .unwrap();
    let mut partitions = TopicPartitionList::new();
    for p in 0..3 {
        partitions
            .add_partition_offset("events", p, Offset::Beginning)
            .unwrap();
    }
    consumer.assign(&partitions).unwrap();
    let client = "source\"\\é";
    for seq in 0..6 {
        let event=serde_json::to_vec(&serde_json::json!({"event_id":format!("e{seq}"),"delivery_id":format!("d{seq}"),"rule_id":"r","client_id":client,"topic":"devices/t","timestamp_ms":123,"data":{"sequence":seq,"selected":true}})).unwrap();
        sink.send(&[&event]).await.unwrap();
    }
    let mut chosen = None;
    for seq in 0..6 {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), consumer.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(msg.key(), Some(client.as_bytes()));
        assert!(chosen.is_none_or(|p| p == msg.partition()));
        chosen = Some(msg.partition());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(msg.payload().unwrap()).unwrap(),
            serde_json::json!({"sequence":seq,"selected":true})
        );
        let headers = msg.headers().unwrap();
        let hs: std::collections::BTreeMap<_, _> = (0..headers.count())
            .map(|i| {
                let h = headers.get(i);
                (h.key, h.value.unwrap())
            })
            .collect();
        assert_eq!(hs["rmqtt-client-id"], client.as_bytes());
        assert_eq!(hs["rmqtt-delivery-id"], format!("d{seq}").as_bytes());
        assert_eq!(hs["rmqtt-topic"], b"devices/t");
        assert_eq!(hs["rmqtt-rule-id"], b"r");
    }
    sink.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_queue_full_is_retryable_and_never_success() {
    use mqtt_egress_core::{FailureKind, Sink};
    use std::sync::Arc;
    let sink = Arc::new(
        KafkaSink::new(
            "queue",
            KafkaConfig {
                brokers: vec!["127.0.0.1:1".into()],
                topic: "events".into(),
                ..KafkaConfig::default()
            },
            1000,
            128,
        )
        .unwrap(),
    );
    let event=Arc::new(serde_json::to_vec(&serde_json::json!({"event_id":"e","delivery_id":"d","rule_id":"r","client_id":"c","topic":"t","timestamp_ms":1,"data":{"blob":"x".repeat(600000)}})).unwrap());
    let mut jobs = tokio::task::JoinSet::new();
    for _ in 0..20 {
        let sink = Arc::clone(&sink);
        let event = Arc::clone(&event);
        jobs.spawn(async move { sink.send(&[event.as_slice()]).await });
    }
    let mut queue_full = 0;
    while let Some(result) = jobs.join_next().await {
        let error = result.unwrap().unwrap_err();
        assert_eq!(error.kind, FailureKind::Retryable);
        if error.code == "kafka_queue_full" {
            queue_full += 1;
        }
    }
    assert!(queue_full > 0);
    sink.shutdown().await.unwrap();
}

#[test]
fn broker_and_security_boundaries_are_explicit() {
    let mut c = KafkaConfig {
        brokers: vec!["host:9092".into()],
        topic: "events".into(),
        ..KafkaConfig::default()
    };
    for broker in [
        "https://host:9092",
        "host:0",
        "host:+1",
        "h..x:9092",
        "h.:9092",
        "[fe80::1%1]:9092",
        "host:9092/path",
        "u:p@host:9092",
    ] {
        c.brokers = vec![broker.into()];
        assert!(c.validate().is_err(), "{broker}");
    }
    for broker in ["host:9092", "a_b.local:9092", "[::1]:9092"] {
        c.brokers = vec![broker.into()];
        assert!(c.validate().is_ok(), "{broker}");
    }
    c.security.protocol = SecurityProtocol::Ssl;
    c.security.private_key_pem = Some("-----BEGIN PRIVATE KEY-----\nx".into());
    assert!(c.validate().is_err());
    c.security.certificate_pem = Some("-----BEGIN CERTIFICATE-----\nx".into());
    assert!(c.validate().is_ok());
    c.security.password = Some("irrelevant".into());
    assert!(c.validate().is_err());
}

#[tokio::test]
async fn adapter_rejects_bad_keys_batches_and_header_inflated_records_before_enqueue() {
    use mqtt_egress_core::{FailureKind, Sink};
    let config = KafkaConfig {
        brokers: vec!["127.0.0.1:1".into()],
        topic: "events".into(),
        key_field: Some("device".into()),
        ..KafkaConfig::default()
    };
    let sink = KafkaSink::new("boundaries", config, 1000, 1).unwrap();
    let bad=serde_json::to_vec(&serde_json::json!({"event_id":"e","delivery_id":"d","rule_id":"r","client_id":"c","topic":"t","timestamp_ms":1,"data":{"device":7}})).unwrap();
    assert_eq!(sink.send(&[&bad]).await.unwrap_err().code, "kafka_key");
    assert_eq!(
        sink.send(&[&bad, &bad]).await.unwrap_err().code,
        "kafka_batch"
    );
    sink.shutdown().await.unwrap();
    let sink = KafkaSink::new(
        "headers",
        KafkaConfig {
            brokers: vec!["127.0.0.1:1".into()],
            topic: "events".into(),
            ..KafkaConfig::default()
        },
        1000,
        1,
    )
    .unwrap();
    let large=serde_json::to_vec(&serde_json::json!({"event_id":"e","delivery_id":"d","rule_id":"r","client_id":"x".repeat(65535),"topic":"t","timestamp_ms":1,"data":{"blob":"x".repeat(920000)}})).unwrap();
    assert!(large.len() < 1024 * 1024);
    let error = sink.send(&[&large]).await.unwrap_err();
    assert_eq!(error.kind, FailureKind::Permanent);
    assert_eq!(error.code, "request_too_large");
    sink.shutdown().await.unwrap();
}
