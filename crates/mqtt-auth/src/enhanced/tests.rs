use super::*;
use crate::{AllowAnonymous, StaticPolicy};
use argon2::{Algorithm, Argon2, Params, PasswordHasher, Version, password_hash::SaltString};

fn policy() -> Arc<dyn AccessControl> {
    let salt = SaltString::encode_b64(b"enhanced-auth-salt").unwrap();
    let hash = Argon2::new(
        Algorithm::Argon2id,
        Version::V0x13,
        Params::new(32, 1, 1, None).unwrap(),
    )
    .hash_password(b"secret", &salt)
    .unwrap()
    .to_string();
    Arc::new(StaticPolicy::from_json(serde_json::to_vec(&serde_json::json!({
        "allow_anonymous":false,
        "users":[{"username":"alice","password_hash":hash,"client_id_prefix":"device-","publish":["allowed/#"],"subscribe":["allowed/#"]}]
    })).unwrap().as_slice()).unwrap())
}

fn info(data: Option<&[u8]>) -> EnhancedConnectInfo<'_> {
    EnhancedConnectInfo {
        client_id: "device-1",
        username: Some("alice"),
        method: "PLAIN",
        data,
    }
}

#[test]
fn direct_and_challenge_verify_real_argon2_policy_and_finish_once() {
    let provider = PlainAuthenticator::new(policy());
    let (mut done, step) = provider.start(info(Some(b"\0alice\0secret"))).unwrap();
    assert!(
        matches!(step, EnhancedAuthStep::Complete{ principal:Principal{name},data:None } if name=="user:alice")
    );
    assert!(matches!(
        done.step(Some(b"\0alice\0secret")),
        Err(EnhancedAuthError::Protocol)
    ));
    let (mut exchange, step) = provider.start(info(None)).unwrap();
    assert!(matches!(step, EnhancedAuthStep::Continue { data: None }));
    assert!(
        matches!(exchange.step(Some(b"alice\0alice\0secret")).unwrap(),EnhancedAuthStep::Complete{principal:Principal{name},data:None} if name=="user:alice")
    );
}

#[test]
fn malformed_credentials_delegation_and_failure_cannot_retry() {
    let provider = PlainAuthenticator::new(policy());
    for data in [
        &b""[..],
        b"alice",
        b"\0alice",
        b"\0\0secret",
        b"\0alice\0",
        b"\0alice\0secret\0extra",
        b"bob\0alice\0secret",
        b"\0bob\0secret",
        b"\0alice\0wrong",
        b"\0alice\0\xff",
    ] {
        let (mut exchange, _) = provider.start(info(None)).unwrap();
        assert!(
            matches!(
                exchange.step(Some(data)),
                Err(EnhancedAuthError::BadCredentials)
            ),
            "{data:?}"
        );
        assert!(matches!(
            exchange.step(Some(b"\0alice\0secret")),
            Err(EnhancedAuthError::Protocol)
        ));
    }
    let (mut exchange, _) = provider.start(info(None)).unwrap();
    assert!(matches!(
        exchange.step(None),
        Err(EnhancedAuthError::BadCredentials)
    ));
    assert!(matches!(
        exchange.step(Some(b"\0alice\0secret")),
        Err(EnhancedAuthError::Protocol)
    ));
}

#[test]
fn unsupported_method_client_acl_and_anonymous_are_not_bypassed() {
    let provider = PlainAuthenticator::new(policy());
    let mut input = info(None);
    input.method = "plain";
    assert!(matches!(
        provider.start(input),
        Err(EnhancedAuthError::UnsupportedMethod)
    ));
    let mut input = info(Some(b"\0alice\0secret"));
    input.client_id = "forbidden";
    assert!(matches!(
        provider.start(input),
        Err(EnhancedAuthError::NotAuthorized)
    ));
    let provider = PlainAuthenticator::new(Arc::new(AllowAnonymous));
    assert!(matches!(
        provider.start(info(Some(b"\0alice\0secret"))),
        Err(EnhancedAuthError::BadCredentials)
    ));
}

#[test]
fn all_debug_and_error_paths_redact_authentication_data() {
    let data = b"\0alice\0secret-unique-value";
    let debug = format!("{:?}", info(Some(data)));
    assert!(!debug.contains("secret-unique-value"));
    for step in [
        EnhancedAuthStep::Continue {
            data: Some(data.to_vec()),
        },
        EnhancedAuthStep::Complete {
            principal: Principal::anonymous(),
            data: Some(data.to_vec()),
        },
    ] {
        assert!(!format!("{step:?}").contains("secret-unique-value"));
    }
    let provider = PlainAuthenticator::new(policy());
    let (exchange, _) = provider.start(info(None)).unwrap();
    assert!(!format!("{provider:?} {exchange:?}").contains("alice"));
}

#[derive(Debug, Default)]
struct Probe {
    calls: std::sync::atomic::AtomicUsize,
}
impl AccessControl for Probe {
    fn authenticate(&self, value: ConnectInfo<'_>) -> Result<Principal, AuthError> {
        self.calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let username = value.username.ok_or(AuthError::BadCredentials)?;
        if value.password.is_none() {
            return Err(AuthError::BadCredentials);
        }
        Ok(Principal {
            name: format!("user:{username}"),
        })
    }
    fn authorize(&self, _: &Principal, _: Action, _: &str) -> bool {
        true
    }
}

#[test]
fn exact_data_limit_unicode_and_byte_counted_username_bounds() {
    let access = Arc::new(Probe::default());
    let provider = PlainAuthenticator::new(access.clone());
    let mut input = info(None);
    input.username = None;
    let mut maximum = b"\0a\0".to_vec();
    maximum.resize(65535, b'x');
    input.data = Some(&maximum);
    assert!(matches!(
        provider.start(input),
        Ok((_, EnhancedAuthStep::Complete { .. }))
    ));
    let mut too_long = maximum.clone();
    too_long.push(b'x');
    let mut input = info(Some(&too_long));
    input.username = None;
    assert!(matches!(
        provider.start(input),
        Err(EnhancedAuthError::BadCredentials)
    ));
    assert_eq!(access.calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    for (username, valid) in [("é".repeat(128), true), ("é".repeat(129), false)] {
        let data = format!("\0{username}\0密码");
        let input = EnhancedConnectInfo {
            client_id: "device-1",
            username: None,
            method: "PLAIN",
            data: Some(data.as_bytes()),
        };
        assert_eq!(provider.start(input).is_ok(), valid);
    }
    assert_eq!(access.calls.load(std::sync::atomic::Ordering::Relaxed), 2);
}

#[test]
fn resolved_context_is_bounded_before_exchange_allocation_or_verification() {
    let access = Arc::new(Probe::default());
    let provider = PlainAuthenticator::new(access.clone());
    let name = "a".repeat(257);
    let client = "x".repeat(65536);
    for input in [
        EnhancedConnectInfo {
            client_id: "",
            username: None,
            method: "PLAIN",
            data: None,
        },
        EnhancedConnectInfo {
            client_id: &client,
            username: None,
            method: "PLAIN",
            data: None,
        },
        EnhancedConnectInfo {
            client_id: "bad\0client",
            username: None,
            method: "PLAIN",
            data: None,
        },
        EnhancedConnectInfo {
            client_id: "device-1",
            username: Some(&name),
            method: "PLAIN",
            data: None,
        },
        EnhancedConnectInfo {
            client_id: "device-1",
            username: Some(""),
            method: "PLAIN",
            data: None,
        },
    ] {
        assert!(matches!(
            provider.start(input),
            Err(EnhancedAuthError::BadCredentials)
        ));
    }
    assert_eq!(access.calls.load(std::sync::atomic::Ordering::Relaxed), 0);
}

#[test]
fn literal_shared_prefix_policy_is_not_recursively_unwrapped() {
    let salt = SaltString::encode_b64(b"literal-policy-salt").unwrap();
    let hash = Argon2::new(
        Algorithm::Argon2id,
        Version::V0x13,
        Params::new(32, 1, 1, None).unwrap(),
    )
    .hash_password(b"secret", &salt)
    .unwrap()
    .to_string();
    let policy = StaticPolicy::from_json(&serde_json::to_vec(&serde_json::json!({
        "allow_anonymous":false,
        "users":[{"username":"alice","password_hash":hash,"client_id_prefix":"device-","publish":["$share/#"],"subscribe":["$share/#"]}]
    })).unwrap()).unwrap();
    let principal = Principal {
        name: "user:alice".into(),
    };
    assert!(policy.authorize(&principal, Action::Subscribe, "$share/group/allowed/#"));
    assert!(policy.authorize(&principal, Action::Receive, "$share/group/allowed/value"));
    assert!(policy.authorize(&principal, Action::Publish, "$share/group/allowed/value"));
    for resource in ["allowed/#", "secret/#"] {
        assert!(!policy.authorize(&principal, Action::Subscribe, resource));
    }
    assert!(!policy.authorize(&principal, Action::Publish, "allowed/value"));
    assert!(!policy.authorize(&principal, Action::Receive, "allowed/value"));
    assert!(!crate::covers_filter("#", "$share/group/allowed/#"));
    assert!(!crate::covers_filter("allowed/#", "$share/group/allowed/#"));
}
