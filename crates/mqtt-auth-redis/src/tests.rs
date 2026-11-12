use crate::{
    Config, Error, Provider, TlsMode,
    keys::{acl_key, auth_key, client_field},
    password,
    rules::{Rule, decide, parse_rules},
};

#[test]
fn configuration_debug_never_contains_redis_password() {
    let config = Config {
        password: "secret-redis-password".into(),
        tls_mode: TlsMode::Disable,
        ..Config::default()
    };
    assert!(!format!("{config:?}").contains("secret-redis-password"));
    assert!(config.validate());
    assert!(
        !Config {
            key_prefix: "tenant*".into(),
            ..config
        }
        .validate()
    );
    let bad_ca = Config {
        ca_pem: Some("not-a-certificate".into()),
        tls_mode: TlsMode::VerifyFull,
        ..Config::default()
    };
    assert!(!bad_ca.validate());
    assert_eq!(
        Provider::build(&bad_ca, 1000).err(),
        Some(Error::InvalidConfig)
    );
}
#[test]
fn keys_keep_username_bytes_and_separate_client_fields() {
    assert_eq!(auth_key("lyrnormq:", "dev:ice"), b"lyrnormq:auth:dev:ice");
    assert_eq!(acl_key("app:", "dev:ice"), b"app:acl:dev:ice");
    assert_eq!(client_field("publish", "a\x1fb"), b"publish\x1fa\x1fb");
}
#[test]
fn rules_follow_postgres_priority_deny_and_subscription_intersection() {
    let mut rules = Vec::new();
    let mut total = 0usize;
    parse_rules(
        br#"[{"effect":"deny","topic_filter":"tenant/private/#","priority":10,"note":"ignored"},{"effect":"allow","topic_filter":"tenant/#","priority":10}]"#,
        &mut rules,
        &mut total,
    )
    .unwrap();
    assert!(!decide(&rules, "tenant/#", true));
    assert!(decide(&rules, "tenant/public/#", true));
    assert!(!decide(&rules, "tenant/private/key", false));
    let higher = vec![Rule {
        effect: true,
        filter: "a/#".into(),
        priority: 20,
    }];
    assert!(decide(&higher, "a/x", false));
    assert!(!decide(&higher, "$SYS/x", false));
    let mut overflow = Vec::new();
    let mut bytes = 0usize;
    let mut body = b"[".to_vec();
    for index in 0..257 {
        if index > 0 {
            body.push(b',');
        }
        body.extend(
            format!(r#"{{"effect":"allow","topic_filter":"t/{index}","priority":1}}"#).into_bytes(),
        );
    }
    body.push(b']');
    assert_eq!(
        parse_rules(&body, &mut overflow, &mut bytes),
        Err(Error::InvalidResult)
    );
    assert_eq!(
        parse_rules(
            br#"[{"effect":"allow","topic_filter":"bad/#/filter","priority":1}]"#,
            &mut Vec::new(),
            &mut 0,
        ),
        Err(Error::InvalidResult)
    );
}
#[test]
fn malformed_password_hashes_are_rejected() {
    assert_eq!(
        password::verify("not-a-phc", b"secret"),
        Err(Error::InvalidResult)
    );
}

#[tokio::test]
#[ignore = "requires Redis at 127.0.0.1:16379; run with --include-ignored"]
async fn real_redis_hash_fields_password_acl_and_failure_bounds() {
    use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
    let password = b"secret\x00\xff";
    let salt = SaltString::encode_b64(b"redis-test-salt-01").unwrap();
    let hash = Argon2::default()
        .hash_password(password, &salt)
        .unwrap()
        .to_string();
    let admin = redis::Client::open("redis://127.0.0.1:16379").unwrap();
    let mut conn = admin.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap();
    let config = Config {
        host: "127.0.0.1".into(),
        port: 16379,
        tls_mode: TlsMode::Disable,
        ..Config::default()
    };
    let provider = Provider::build(&config, 1000).unwrap();
    assert!(provider.probe_connection().await.unwrap());
    assert!(
        !provider
            .authenticate("alice", Some(password))
            .await
            .unwrap()
    );
    let _: () = redis::cmd("HSET")
        .arg("lyrnormq:auth:alice")
        .arg("password_hash")
        .arg(&hash)
        .arg("extra")
        .arg("ignored")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(
        provider
            .authenticate("alice", Some(password))
            .await
            .unwrap()
    );
    assert!(
        !provider
            .authenticate("alice", Some(b"wrong"))
            .await
            .unwrap()
    );
    assert!(!provider.authenticate("alice", None).await.unwrap());
    let _: () = redis::cmd("HSET")
        .arg("lyrnormq:auth:carol")
        .arg("password_hash")
        .arg("a".repeat(513))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        provider.authenticate("carol", Some(password)).await,
        Err(Error::InvalidResult)
    );
    let _: () = redis::cmd("HSET")
        .arg("lyrnormq:auth:empty")
        .arg("password_hash")
        .arg("")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        provider.authenticate("empty", Some(password)).await,
        Err(Error::InvalidResult)
    );
    let _: () = redis::cmd("SET")
        .arg("lyrnormq:auth:bob")
        .arg("string")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        provider.authenticate("bob", Some(password)).await,
        Err(Error::Unavailable)
    );
    let publish = r#"[{"effect":"allow","topic_filter":"tenant/#","priority":10},{"effect":"deny","topic_filter":"tenant/private/#","priority":10}]"#;
    let subscribe = r#"[{"effect":"allow","topic_filter":"tenant/#","priority":10},{"effect":"deny","topic_filter":"tenant/private/#","priority":10}]"#;
    let _: () = redis::cmd("HSET")
        .arg("lyrnormq:acl:alice")
        .arg("publish")
        .arg(publish)
        .arg("subscribe")
        .arg(subscribe)
        .query_async(&mut conn)
        .await
        .unwrap();
    let specific = client_field("publish", "client");
    let _: () = redis::cmd("HSET")
        .arg("lyrnormq:acl:alice")
        .arg(&specific)
        .arg(r#"[{"effect":"deny","topic_filter":"tenant/#","priority":30}]"#)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(
        provider
            .authorize("alice", "other", "publish", "tenant/a")
            .await
            .unwrap()
    );
    assert!(
        !provider
            .authorize("alice", "client", "publish", "tenant/a")
            .await
            .unwrap()
    );
    assert!(
        !provider
            .authorize("alice", "other", "publish", "tenant/private/a")
            .await
            .unwrap()
    );
    assert!(
        !provider
            .authorize("alice", "other", "subscribe", "tenant/#")
            .await
            .unwrap()
    );
    assert!(
        provider
            .authorize("alice", "other", "subscribe", "tenant/public/#")
            .await
            .unwrap()
    );
    assert!(
        !provider
            .authorize("alice", "other", "publish", "$SYS/a")
            .await
            .unwrap()
    );
    assert!(
        !provider
            .authorize("missing", "other", "publish", "tenant/a")
            .await
            .unwrap()
    );
    let mut huge = "[".to_string();
    for index in 0..257 {
        if index > 0 {
            huge.push(',');
        }
        huge.push_str(&format!(
            r#"{{"effect":"allow","topic_filter":"t/{index}","priority":1}}"#
        ));
    }
    huge.push(']');
    let _: () = redis::cmd("HSET")
        .arg("lyrnormq:acl:overflow")
        .arg("publish")
        .arg(huge)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        provider
            .authorize("overflow", "client", "publish", "t/1")
            .await,
        Err(Error::InvalidResult)
    );
    let mut refused = config.clone();
    refused.port = 1;
    assert!(
        Provider::build(&refused, 100)
            .unwrap()
            .probe_connection()
            .await
            .is_err()
    );
    refused = config;
    refused.tls_mode = TlsMode::VerifyFull;
    assert!(
        Provider::build(&refused, 1000)
            .unwrap()
            .probe_connection()
            .await
            .is_err()
    );
}
