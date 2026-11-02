use super::*;
fn configuration(kind: &str) -> Config {
    let endpoint = if kind == "kafka" {
        serde_json::json!({"brokers":["localhost:9092"],"topic":"events"})
    } else {
        serde_json::json!({"url":"http://127.0.0.1:1/unused","headers":{}})
    };
    serde_json::from_value(serde_json::json!({"schema_version":2,"rules":[],"outbounds":[{"id":"send","name":"Send","enabled":false,"type":kind,"config":endpoint,"delivery":{"batch_size":1,"batch_wait_ms":0,"queue_capacity":256,"timeout_ms":30000,"max_attempts":3,"retry_base_ms":200}}]})).unwrap()
}
#[test]
fn kafka_policy_defaults_do_not_change_webhook_limits() {
    let kafka = configuration("kafka");
    assert_eq!(kafka.outbounds[0].concurrency, 128);
    assert!(kafka.validate().is_ok());
    let webhook = configuration("webhook");
    assert_eq!(webhook.outbounds[0].concurrency, 8);
    assert!(webhook.validate().is_ok());
    let mut kafka = configuration("kafka");
    kafka.outbounds[0].concurrency = 512;
    assert!(kafka.validate().is_ok());
    let mut kafka = configuration("kafka");
    kafka.outbounds[0].batch_size = 2;
    assert!(kafka.validate().is_err());
    let mut kafka = configuration("kafka");
    kafka.outbounds[0].timeout_ms = 999;
    assert!(kafka.validate().is_err());
    let mut web = configuration("webhook");
    web.outbounds[0].concurrency = 17;
    assert!(web.validate().is_err());
}
#[test]
fn configuration_roundtrip_redacts_and_restores_all_kafka_secrets() {
    let mut c = configuration("kafka");
    let Endpoint::Kafka(k) = &mut c.outbounds[0].endpoint else {
        panic!()
    };
    k.security = mqtt_kafka::KafkaSecurity {
        protocol: mqtt_kafka::SecurityProtocol::SaslSsl,
        sasl_mechanism: Some("SCRAM-SHA-512".into()),
        username: Some("user".into()),
        password: Some("sasl-secret".into()),
        certificate_pem: Some("-----BEGIN CERTIFICATE-----\nabc".into()),
        private_key_pem: Some("-----BEGIN PRIVATE KEY-----\nprivate-secret".into()),
        private_key_password: Some("key-secret".into()),
        ca_pem: None,
    };
    let mut public = c.public();
    let wire = serde_json::to_string(&public).unwrap();
    for secret in ["sasl-secret", "private-secret", "key-secret"] {
        assert!(!wire.contains(secret));
    }
    public.restore_redacted(&c).unwrap();
    assert_eq!(
        serde_json::to_value(public).unwrap(),
        serde_json::to_value(&c).unwrap()
    );
    let mut copy = c.public();
    copy.outbounds[0].id = "new".into();
    assert!(copy.restore_redacted(&c).is_err());
}

// Frozen pre-Kafka bytes: binding descriptors are durable identities, not JSON
// semantic objects. Reordering DeliveryPolicy keys must not create a new target.
#[test]
fn legacy_webhook_serialization_preserves_exact_durable_descriptor_bytes() {
    let config = configuration("webhook");
    let expected = br#"{"id":"send","name":"Send","enabled":false,"type":"webhook","config":{"headers":{},"url":"http://127.0.0.1:1/unused"},"delivery":{"concurrency":8,"batch_size":1,"batch_wait_ms":0,"queue_capacity":256,"timeout_ms":30000,"max_attempts":3,"retry_base_ms":200}}"#;
    assert_eq!(serde_json::to_vec(&config.outbounds[0]).unwrap(), expected);
}
#[test]
fn legacy_webhook_binding_preserves_exact_durable_descriptor_bytes() {
    let config = configuration("webhook");
    let binding = crate::durable::bindings(&config).unwrap().remove(0);
    let expected_binding = br#"{"id":"send","name":"","enabled":true,"type":"webhook","config":{"headers":{},"url":"http://127.0.0.1:1/unused"},"delivery":{"concurrency":8,"batch_size":1,"batch_wait_ms":0,"queue_capacity":256,"timeout_ms":30000,"max_attempts":3,"retry_base_ms":200}}"#;
    assert_eq!(binding.descriptor, expected_binding);
}
