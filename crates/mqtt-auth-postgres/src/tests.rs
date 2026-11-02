use crate::{
    Config,
    rules::{Rule, decide},
};

#[test]
fn configuration_debug_never_contains_database_password() {
    let c = Config {
        password: "secret-db-password".into(),
        ..Config::default()
    };
    assert!(!format!("{c:?}").contains("secret-db-password"));
}
#[test]
fn subscription_deny_intersection_cannot_be_bypassed_by_broad_filter() {
    let rules = vec![
        Rule {
            effect: false,
            filter: "tenant/private/#".into(),
            priority: 10,
        },
        Rule {
            effect: true,
            filter: "tenant/#".into(),
            priority: 10,
        },
    ];
    assert!(!decide(&rules, "tenant/#", true));
    assert!(decide(&rules, "tenant/public/#", true));
    assert!(!decide(&rules, "tenant/private/key", false));
}
#[test]
fn priority_and_deny_ties_are_deterministic() {
    let rules = vec![
        Rule {
            effect: true,
            filter: "a/#".into(),
            priority: 20,
        },
        Rule {
            effect: false,
            filter: "a/#".into(),
            priority: 10,
        },
    ];
    assert!(decide(&rules, "a/x", false));
    assert!(!decide(&rules, "b/x", false));
    assert!(!decide(&rules, "$SYS/x", false));
}

#[tokio::test]
#[ignore = "requires PostgreSQL at 127.0.0.1:15432; run with --include-ignored"]
async fn real_postgres_custom_queries_password_acl_and_failure_bounds() {
    use crate::{Error, Provider, TlsMode};
    use argon2::{Argon2, PasswordHasher, password_hash::SaltString};
    let password = b"secret\x00\xff";
    let salt = SaltString::encode_b64(b"postgres-test-salt").unwrap();
    let hash = Argon2::default()
        .hash_password(password, &salt)
        .unwrap()
        .to_string();
    let config = Config {
        host: "127.0.0.1".into(),
        port: 15432,
        database: "postgres".into(),
        username: "postgres".into(),
        tls_mode: TlsMode::Disable,
        query: format!("SELECT '{hash}'::text AS password_hash WHERE $1::text = 'alice'"),
        ..Config::default()
    };
    let provider = Provider::build(&config, 1000).unwrap();
    assert!(provider.probe_connection().await.unwrap());
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
    assert!(
        !provider
            .authenticate("alice' OR true --", Some(password))
            .await
            .unwrap()
    );
    assert!(!provider.authenticate("alice", None).await.unwrap());
    let mut c = config.clone();
    c.query = format!(
        "SELECT '{hash}'::text AS password_hash FROM generate_series(1,2) WHERE $1::text='alice'"
    );
    assert_eq!(
        Provider::build(&c, 1000)
            .unwrap()
            .authenticate("alice", Some(password))
            .await,
        Err(Error::InvalidResult)
    );
    c.query = "SELECT 'invalid'::text AS password_hash WHERE $1::text='alice'".into();
    assert_eq!(
        Provider::build(&c, 1000)
            .unwrap()
            .authenticate("alice", Some(password))
            .await,
        Err(Error::InvalidResult)
    );
    c.query="SELECT * FROM (VALUES ('allow','tenant/#',10),('deny','tenant/private/#',10)) AS rules(effect,topic_filter,priority) WHERE $1::text='alice' AND $2::text='client' AND $3::text IN ('publish','subscribe')".into();
    let acl = Provider::build(&c, 1000).unwrap();
    assert!(
        acl.authorize("alice", "client", "publish", "tenant/a")
            .await
            .unwrap()
    );
    assert!(
        !acl.authorize("alice", "client", "publish", "tenant/private/a")
            .await
            .unwrap()
    );
    assert!(
        !acl.authorize("alice", "client", "subscribe", "tenant/#")
            .await
            .unwrap()
    );
    assert!(
        acl.authorize("alice", "client", "subscribe", "tenant/public/#")
            .await
            .unwrap()
    );
    assert!(
        !acl.authorize("alice", "other", "publish", "tenant/a")
            .await
            .unwrap()
    );
    assert!(
        !acl.authorize("alice", "client", "publish", "$SYS/a")
            .await
            .unwrap()
    );
    c.query="SELECT 'allow'::text AS effect, '#'::text AS topic_filter, 1 AS priority FROM generate_series(1,257) WHERE $1::text='alice' AND $2::text='client' AND $3::text='publish'".into();
    assert_eq!(
        Provider::build(&c, 1000)
            .unwrap()
            .authorize("alice", "client", "publish", "a")
            .await,
        Err(Error::InvalidResult)
    );
    c.query="SELECT 'allow'::text AS effect, 'bad/#/filter'::text AS topic_filter, 1 AS priority WHERE $1::text='alice' AND $2::text='client' AND $3::text='publish'".into();
    assert_eq!(
        Provider::build(&c, 1000)
            .unwrap()
            .authorize("alice", "client", "publish", "a")
            .await,
        Err(Error::InvalidResult)
    );
    c.query = format!(
        "SELECT '{hash}'::text AS password_hash WHERE $1::text='alice' AND current_setting('transaction_read_only')='on'"
    );
    assert!(
        Provider::build(&c, 1000)
            .unwrap()
            .authenticate("alice", Some(password))
            .await
            .unwrap()
    );
    c.query = "SELECT 'x'::text AS password_hash FROM pg_sleep(1) WHERE $1::text='alice'".into();
    let start = std::time::Instant::now();
    assert!(
        Provider::build(&c, 100)
            .unwrap()
            .authenticate("alice", Some(password))
            .await
            .is_err()
    );
    assert!(start.elapsed() < std::time::Duration::from_millis(800));
    c = config.clone();
    c.port = 1;
    assert!(
        Provider::build(&c, 100)
            .unwrap()
            .authenticate("alice", Some(password))
            .await
            .is_err()
    );
    c = config;
    c.tls_mode = TlsMode::VerifyFull;
    assert!(
        Provider::build(&c, 1000)
            .unwrap()
            .probe_connection()
            .await
            .is_err()
    );
}

#[test]
fn unbounded_hash_parameters_and_non_argon2id_are_rejected() {
    for hash in [
        "$argon2id$v=19$m=4294967295,t=1,p=1$c29tZXNhbHQ$YWJj",
        "$argon2i$v=19$m=19456,t=2,p=1$c29tZXNhbHQ$YWJj",
        "not a hash",
    ] {
        assert_eq!(
            crate::password::verify(hash, b"password"),
            Err(crate::Error::InvalidResult)
        );
    }
}
