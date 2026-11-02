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
