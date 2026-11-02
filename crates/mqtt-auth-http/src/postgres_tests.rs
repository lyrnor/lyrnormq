use crate::{BackendConfig, Config};

#[test]
fn postgres_secrets_round_trip_without_changing_query_and_legacy_http_loads() {
    let config = Config {
        authentication: BackendConfig {
            kind: "postgresql".into(),
            enabled: true,
            postgres: Some(mqtt_auth_postgres::Config {
                password: "private-postgres-password".into(),
                query: "SELECT credential AS password_hash FROM accounts WHERE login=$1".into(),
                ..Default::default()
            }),
            ..Default::default()
        },
        ..Default::default()
    };
    config.validate(false).unwrap();
    let public = config.public();
    let encoded = serde_json::to_string(&public).unwrap();
    assert!(!encoded.contains("private-postgres-password"));
    assert!(encoded.contains("credential AS password_hash"));
    let mut parsed: Config = serde_json::from_str(&encoded).unwrap();
    parsed
        .authentication
        .restore(&config.authentication)
        .unwrap();
    assert_eq!(
        parsed.authentication.postgres.as_ref().unwrap().password,
        "private-postgres-password"
    );
    parsed.validate(false).unwrap();
    let bytes = serde_json::to_vec(&config).unwrap();
    let recovered = Config::from_stored(&bytes).unwrap();
    assert_eq!(
        recovered.authentication.postgres.unwrap().password,
        "private-postgres-password"
    );
    assert!(
        Config::from_stored(&serde_json::to_vec(&Config::default()).unwrap())
            .unwrap()
            .authentication
            .postgres
            .is_none()
    );
    let mut bad = public.authentication;
    assert!(bad.restore(&BackendConfig::default()).is_err());
}
