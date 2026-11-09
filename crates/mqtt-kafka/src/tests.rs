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
async fn native_queue_full_is_retryable_and_never_success() {
    use mqtt_egress_core::{FailureKind, Sink};
    use std::sync::Arc;
    let broker = krafka::testing::FakeBroker::start().await.unwrap();
    broker.on(krafka::testing::ApiKey::ApiVersions, |_| {
        krafka::testing::Control::Silence
    });
    let sink = Arc::new(
        KafkaSink::new(
            "queue",
            KafkaConfig {
                brokers: vec![broker.bootstrap_servers()],
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_ack_retries_deduplicate_and_shutdown_fences_connections() {
    use krafka::testing::{ApiKey, Control, FakeBroker};
    use mqtt_egress_core::Sink;
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    let sink = KafkaSink::new(
        "dedup",
        KafkaConfig {
            brokers: vec![broker.bootstrap_servers()],
            topic: "events".into(),
            ..KafkaConfig::default()
        },
        5000,
        128,
    )
    .unwrap();
    broker.on_once(ApiKey::Produce, |_| {
        Control::ApplyThen(Box::new(Control::Disconnect))
    });
    let event=br#"{"event_id":"e","delivery_id":"d","rule_id":"r","client_id":"c","topic":"t","timestamp_ms":1,"data":{"v":1}}"#;
    sink.send(&[event]).await.unwrap();
    assert!(broker.request_count(ApiKey::Produce) >= 2);
    assert_eq!(
        broker.with_state(|s| s.partition("events", 0).unwrap().next_offset),
        1
    );
    let (a, b) = tokio::join!(sink.shutdown(), sink.shutdown());
    a.unwrap();
    b.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while broker.open_connections() != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await
        }
    })
    .await
    .unwrap();
    assert!(sink.send(&[event]).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ambiguous_deadline_fences_old_generation_and_can_reconnect() {
    use krafka::testing::{ApiKey, Control, FakeBroker};
    use mqtt_egress_core::Sink;
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    let sink = KafkaSink::new(
        "uncertain",
        KafkaConfig {
            brokers: vec![broker.bootstrap_servers()],
            topic: "events".into(),
            ..KafkaConfig::default()
        },
        1000,
        128,
    )
    .unwrap();
    broker.on(ApiKey::Produce, |_| {
        Control::ApplyThen(Box::new(Control::Delay(std::time::Duration::from_millis(
            1200,
        ))))
    });
    let event=br#"{"event_id":"e","delivery_id":"d","rule_id":"r","client_id":"c","topic":"t","timestamp_ms":1,"data":{"v":1}}"#;
    let start = std::time::Instant::now();
    assert!(sink.send(&[event]).await.is_err());
    assert!(start.elapsed() < std::time::Duration::from_secs(5));
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while broker.open_connections() != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await
        }
    })
    .await
    .unwrap();
    let count = broker.request_count(ApiKey::Produce);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(broker.request_count(ApiKey::Produce), count);
    broker.clear_hooks();
    sink.send(&[event]).await.unwrap();
    sink.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_data_headers_keys_timestamps_and_fifo_are_unchanged() {
    use krafka::testing::FakeBroker;
    use mqtt_egress_core::Sink;
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 3);
    let sink = KafkaSink::new(
        "wire",
        KafkaConfig {
            brokers: vec![broker.bootstrap_servers()],
            topic: "events".into(),
            ..KafkaConfig::default()
        },
        5000,
        128,
    )
    .unwrap();
    let client = "client-\"é";
    for seq in 0..6 {
        let event=serde_json::to_vec(&serde_json::json!({"event_id":format!("e{seq}"),"delivery_id":format!("d{seq}"),"rule_id":"r","client_id":client,"topic":"devices/t","timestamp_ms":123,"data":{"sequence":seq}})).unwrap();
        sink.send(&[&event]).await.unwrap();
    }
    let records = broker.all_records("events").unwrap();
    assert_eq!(records.len(), 6);
    let partition = records[0].partition;
    for (seq, record) in records.iter().enumerate() {
        assert_eq!(record.partition, partition);
        assert_eq!(record.key.as_deref(), Some(client.as_bytes()));
        assert_eq!(record.timestamp, 123);
        let headers: std::collections::BTreeMap<_, _> = record
            .headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_deref().unwrap()))
            .collect();
        assert_eq!(headers["lyrnormq-delivery-id"], format!("d{seq}").as_bytes());
        assert_eq!(headers["lyrnormq-client-id"], client.as_bytes());
        assert_eq!(headers["lyrnormq-topic"], b"devices/t");
        assert_eq!(headers["lyrnormq-rule-id"], b"r");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(record.value.as_deref().unwrap()).unwrap(),
            serde_json::json!({"sequence":seq})
        );
    }
    sink.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicitly_empty_sql_key_keeps_murmur2_partition() {
    use krafka::testing::FakeBroker;
    use mqtt_egress_core::Sink;
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 3);
    let sink = KafkaSink::new(
        "empty",
        KafkaConfig {
            brokers: vec![broker.bootstrap_servers()],
            topic: "events".into(),
            key_field: Some("key".into()),
            ..KafkaConfig::default()
        },
        5000,
        1,
    )
    .unwrap();
    let event=br#"{"event_id":"e","delivery_id":"d","rule_id":"r","client_id":"not-the-key","topic":"t","timestamp_ms":1,"data":{"key":""}}"#;
    sink.send(&[event]).await.unwrap();
    let records = broker.all_records("events").unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].key.as_deref(), Some(b"".as_slice()));
    // Java murmur2(empty)=275646681, positive mask, modulo three.
    assert_eq!(records[0].partition, 0);
    sink.shutdown().await.unwrap();
}
