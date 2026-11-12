use crate::{BackendConfig, Config};

#[test]
fn redis_secrets_round_trip_without_exposing_the_password() {
    let config = Config {
        authentication: BackendConfig {
            kind: "redis".into(),
            enabled: true,
            redis: Some(mqtt_auth_redis::Config {
                password: "private-redis-password".into(),
                tls_mode: mqtt_auth_redis::TlsMode::Disable,
                key_prefix: "tenant:".into(),
                ..mqtt_auth_redis::Config::default()
            }),
            ..Default::default()
        },
        ..Default::default()
    };
    config.validate(false).unwrap();
    let public = config.public();
    let encoded = serde_json::to_string(&public).unwrap();
    assert!(!encoded.contains("private-redis-password"));
    assert!(encoded.contains("tenant:"));
    let mut parsed: Config = serde_json::from_str(&encoded).unwrap();
    parsed
        .authentication
        .restore(&config.authentication)
        .unwrap();
    assert_eq!(
        parsed.authentication.redis.as_ref().unwrap().password,
        "private-redis-password"
    );
    parsed.validate(false).unwrap();
    let recovered = Config::from_stored(&serde_json::to_vec(&config).unwrap()).unwrap();
    assert_eq!(
        recovered.authentication.redis.unwrap().key_prefix,
        "tenant:"
    );
    let mut bad = public.authentication;
    assert!(bad.restore(&BackendConfig::default()).is_err());
}
