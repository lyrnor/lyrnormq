use super::*;
use crate::AllowAnonymous;
use argon2::{Algorithm, PasswordHasher, Version, password_hash::SaltString};
use serde_json::{Value, json};

const PASSWORD: &[u8] = b"alice-secret";
fn phc(password: &[u8]) -> String {
    let params = Params::new(32, 1, 1, None).unwrap();
    let salt = SaltString::encode_b64(b"mqtt-test-salt-01").unwrap();
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
        .hash_password(password, &salt)
        .unwrap()
        .to_string()
}
fn record() -> Value {
    json!({"allow_anonymous": false, "users": [{
        "username":"alice", "password_hash":phc(PASSWORD),
        "client_id_prefix":"alice-", "publish":["tenant/a/#"],
        "subscribe":["tenant/a/#","$SYS/uptime"]
    }]})
}
fn policy(value: &Value) -> Result<StaticPolicy, PolicyError> {
    StaticPolicy::from_json(&serde_json::to_vec(value).unwrap())
}
fn info<'a>(
    client_id: &'a str,
    username: Option<&'a str>,
    password: Option<&'a [u8]>,
) -> ConnectInfo<'a> {
    ConnectInfo {
        client_id,
        username,
        password,
    }
}
#[test]
fn authenticate_and_authorize_each_resource_kind() {
    let p = policy(&record()).unwrap();
    let alice = p
        .authenticate(info("alice-device", Some("alice"), Some(PASSWORD)))
        .unwrap();
    assert_eq!(alice.name, "user:alice");
    assert!(p.authorize(&alice, Action::Publish, "tenant/a/value"));
    assert!(p.authorize(&alice, Action::Receive, "tenant/a"));
    assert!(p.authorize(&alice, Action::Subscribe, "tenant/a/+/#"));
    assert!(p.authorize(&alice, Action::Subscribe, "$SYS/uptime"));
    for (action, resource) in [
        (Action::Publish, "tenant/b/value"),
        (Action::Publish, "tenant/a/+"),
        (Action::Subscribe, "#"),
        (Action::Subscribe, "tenant/+/#"),
        (Action::Receive, "$SYS/version"),
        (Action::Connect, "alice"),
    ] {
        assert!(!p.authorize(&alice, action, resource));
    }
    assert_eq!(
        p.authenticate(info("other", Some("alice"), Some(PASSWORD))),
        Err(AuthError::NotAuthorized)
    );
    assert!(!p.authorize(
        &Principal {
            name: "alice".into()
        },
        Action::Publish,
        "tenant/a/value"
    ));
    assert!(!p.authorize(
        &Principal {
            name: "user:bob".into()
        },
        Action::Publish,
        "tenant/a/value"
    ));
}
#[test]
fn missing_unknown_wrong_and_partial_credentials_fail_closed() {
    let p = policy(&record()).unwrap();
    assert_eq!(
        p.authenticate(info("alice-a", None, None)),
        Err(AuthError::NotAuthorized)
    );
    for (username, password) in [
        (Some("alice"), Some(b"wrong".as_slice())),
        (Some("bob"), Some(PASSWORD)),
        (Some("alice"), None),
        (None, Some(PASSWORD)),
        (Some(""), Some(PASSWORD)),
    ] {
        assert_eq!(
            p.authenticate(info("alice-a", username, password)),
            Err(AuthError::BadCredentials)
        );
    }
    assert_eq!(
        p.authenticate(info("alice-a", Some("alice"), Some(&vec![0; 65536]))),
        Err(AuthError::BadCredentials)
    );
}
#[test]
fn binary_password_is_not_utf8_or_c_string() {
    let mut value = record();
    value["users"][0]["password_hash"] = phc(b"binary\0\xffsecret").into();
    let p = policy(&value).unwrap();
    assert!(
        p.authenticate(info("alice-a", Some("alice"), Some(b"binary\0\xffsecret")))
            .is_ok()
    );
    assert_eq!(
        p.authenticate(info("alice-a", Some("alice"), Some(b"binary"))),
        Err(AuthError::BadCredentials)
    );
}
#[test]
fn anonymous_default_is_compatible_but_configured_rules_default_deny() {
    let open = AllowAnonymous;
    let anon = open.authenticate(info("id", None, None)).unwrap();
    assert!(open.authorize(&anon, Action::Receive, "$SYS/a"));
    assert!(!open.authorize(
        &Principal {
            name: "user:a".into()
        },
        Action::Receive,
        "a"
    ));
    assert_eq!(
        open.authenticate(info("id", Some(""), None)),
        Err(AuthError::BadCredentials)
    );
    let p = policy(&json!({"allow_anonymous":true,"users":[]})).unwrap();
    assert!(p.authenticate(info("any-id", None, None)).is_ok());
    assert!(!p.authorize(&anon, Action::Publish, "public/a"));
    let p = policy(&json!({"allow_anonymous":true,"users":[],"anonymous":{
        "client_id_prefix":"anon-","publish":["public/#"],"subscribe":["public/#"]
    }}))
    .unwrap();
    assert!(p.authenticate(info("anon-a", None, None)).is_ok());
    assert_eq!(
        p.authenticate(info("private", None, None)),
        Err(AuthError::NotAuthorized)
    );
    assert!(p.authorize(&anon, Action::Publish, "public/a"));
    assert!(!p.authorize(&anon, Action::Subscribe, "#"));
}
#[test]
fn debug_and_errors_never_echo_hash_or_password() {
    let p = policy(&record()).unwrap();
    let display = format!("{p:?} {:?}", info("alice-a", Some("alice"), Some(PASSWORD)));
    assert!(!display.contains("alice-secret"));
    assert!(!display.contains("97, 108, 105, 99, 101, 45, 115"));
    assert!(!display.contains("$argon2"));
    let mut value = record();
    value["users"][0]["password_hash"] = "secret-invalid-hash".into();
    let error = policy(&value).unwrap_err();
    assert!(!format!("{error:?} {error}").contains("secret-invalid-hash"));
}
#[test]
fn unknown_fields_duplicates_missing_fields_and_invalid_rules_are_rejected() {
    let mut values = Vec::new();
    let mut v = record();
    v["unknown"] = true.into();
    values.push(v);
    let mut v = record();
    v["users"][0]["password"] = "plaintext".into();
    values.push(v);
    let mut v = record();
    v["users"][0].as_object_mut().unwrap().remove("publish");
    values.push(v);
    let mut v = record();
    v["anonymous"] = json!({"publish":[],"subscribe":[]});
    values.push(v);
    for value in values {
        assert!(policy(&value).is_err());
    }
    let mut v = record();
    let duplicate = v["users"][0].clone();
    v["users"].as_array_mut().unwrap().push(duplicate);
    assert_eq!(policy(&v).unwrap_err(), PolicyError::DuplicateUser);
    for name in ["", "nul\0name", "\u{ffff}"] {
        let mut v = record();
        v["users"][0]["username"] = name.into();
        assert_eq!(policy(&v).unwrap_err(), PolicyError::InvalidUser);
    }
    for filter in ["", "a+", "a/#/b", "nul\0topic"] {
        let mut v = record();
        v["users"][0]["publish"] = json!([filter]);
        assert_eq!(policy(&v).unwrap_err(), PolicyError::InvalidRules);
    }
    let mut v = record();
    v["users"][0]["client_id_prefix"] = "nul\0prefix".into();
    assert_eq!(policy(&v).unwrap_err(), PolicyError::InvalidRules);
}
#[test]
fn all_hash_parameters_are_checked_before_authentication() {
    let base = phc(PASSWORD);
    for bad in [
        base.replace("argon2id", "argon2i"),
        base.replace("v=19", "v=16"),
        base.replace("m=32", "m=65537"),
        base.replace("t=1", "t=11"),
        base.replace("p=1", "p=5"),
        base.replace("p=1", "p=4294967295"),
        base.replace("m=32", "m=7"),
        base.replace("t=1", "t=0"),
        base.replace("p=1", "p=0"),
        base.replace("m=32,t=1,p=1", "t=1,p=1"),
        base.replace("m=32,t=1,p=1", "m=32,t=1,p=1,data=YWJj"),
        base.replace("bXF0dC10ZXN0LXNhbHQtMDE", "YWJjZA"),
        base.rsplit_once('$').unwrap().0.to_owned(),
    ] {
        let mut v = record();
        v["users"][0]["password_hash"] = bad.into();
        assert!(policy(&v).is_err());
    }
}
#[test]
fn file_user_name_and_rule_limits_are_enforced() {
    assert_eq!(
        StaticPolicy::from_json(&vec![b' '; MAX_POLICY_BYTES + 1]).unwrap_err(),
        PolicyError::TooLarge
    );
    let mut v = record();
    v["users"][0]["username"] = "a".repeat(257).into();
    assert_eq!(policy(&v).unwrap_err(), PolicyError::InvalidUser);
    let mut v = record();
    v["users"][0]["publish"] = json!(vec!["a"; 129]);
    assert_eq!(policy(&v).unwrap_err(), PolicyError::InvalidRules);
    let mut v = record();
    v["users"] = json!(vec![v["users"][0].clone(); 1025]);
    assert_eq!(policy(&v).unwrap_err(), PolicyError::TooManyUsers);
    assert_eq!(
        StaticPolicy::from_json(br#"{"allow_anonymous":false,"allow_anonymous":true,"users":[]}"#)
            .unwrap_err(),
        PolicyError::InvalidJson
    );
}

#[test]
fn synthetic_interop_fixtures() {
    assert_eq!(
        phc(b"alice-test-password"),
        "$argon2id$v=19$m=32,t=1,p=1$bXF0dC10ZXN0LXNhbHQtMDE$9nKCHYN7W3NFFVjg+4h4JqpV8p5TDVUKGWdEeVvnJYg"
    );
    assert_eq!(
        phc(b"binary\0\xff-password"),
        "$argon2id$v=19$m=32,t=1,p=1$bXF0dC10ZXN0LXNhbHQtMDE$3jAQ5tXiH7FScfhnVfPqAXCCpVD9CWtwv2sAzUemTlQ"
    );
}
#[test]
fn recommended_hash_parameters_and_explicit_empty_password_work() {
    let salt = SaltString::encode_b64(b"mqtt-test-salt-01").unwrap();
    let recommended = Argon2::default()
        .hash_password(PASSWORD, &salt)
        .unwrap()
        .to_string();
    let mut value = record();
    value["users"][0]["password_hash"] = recommended.into();
    let p = policy(&value).unwrap();
    assert!(
        p.authenticate(info("alice-1", Some("alice"), Some(PASSWORD)))
            .is_ok()
    );
    value["users"][0]["password_hash"] = phc(b"").into();
    let p = policy(&value).unwrap();
    assert!(
        p.authenticate(info("alice-1", Some("alice"), Some(b"")))
            .is_ok()
    );
    assert_eq!(
        p.authenticate(info("alice-1", Some("alice"), None)),
        Err(AuthError::BadCredentials)
    );
}
