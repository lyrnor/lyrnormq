use super::*;
use mqtt_auth::{Action, AllowAnonymous, ConnectInfo, Principal, RemoteAccess, RemoteAccessError};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU16, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "rmqtt-access-unit-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn path(&self) -> PathBuf {
        self.0.join("access.sqlite")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Server {
    url: String,
    code: Arc<AtomicU16>,
    delay: Arc<AtomicU64>,
    records: Arc<Mutex<Vec<serde_json::Value>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn new(code: u16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mqtt", listener.local_addr().unwrap());
        let code = Arc::new(AtomicU16::new(code));
        let delay = Arc::new(AtomicU64::new(0));
        let records = Arc::new(Mutex::new(Vec::new()));
        let (c, d, r) = (code.clone(), delay.clone(), records.clone());
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {accept=listener.accept()=>{let Ok((mut stream,_))=accept else{break};let(c,d,r)=(c.clone(),d.clone(),r.clone());tasks.spawn(async move{let mut bytes=Vec::new();let mut buffer=[0u8;4096];let end=loop{let Ok(n)=stream.read(&mut buffer).await else{return};if n==0{return}bytes.extend_from_slice(&buffer[..n]);if bytes.len()>1024*1024+16384{return}
                if let Some(p)=bytes.windows(4).position(|w|w==b"\r\n\r\n"){break p+4}};let headers=String::from_utf8_lossy(&bytes[..end]);let length=headers.lines().find_map(|line|line.split_once(':').filter(|(k,_)|k.eq_ignore_ascii_case("content-length")).and_then(|(_,v)|v.trim().parse::<usize>().ok())).unwrap_or(0);while bytes.len()<end+length{let Ok(n)=stream.read(&mut buffer).await else{return};if n==0{return}bytes.extend_from_slice(&buffer[..n]);}let value:serde_json::Value=serde_json::from_slice(&bytes[end..end+length]).unwrap();r.lock().unwrap().push(value);let wait=d.load(Ordering::Relaxed);if wait>0{tokio::time::sleep(Duration::from_millis(wait)).await}let code=c.load(Ordering::Relaxed);let response=if code==204{format!("HTTP/1.1 {code} OK\r\nConnection: close\r\n\r\n")}else{format!("HTTP/1.1 {code} Error\r\nContent-Length: 18\r\nConnection: close\r\n\r\nsecret-error-page!")};let _=stream.write_all(response.as_bytes()).await;});},_ = tasks.join_next(),if !tasks.is_empty()=>{}}
            }
        });
        Self {
            url,
            code,
            delay,
            records,
            task,
        }
    }
    fn count(&self) -> usize {
        self.records.lock().unwrap().len()
    }
    fn backend(&self) -> BackendConfig {
        BackendConfig {
            enabled: true,
            url: self.url.clone(),
            allow_http: true,
            ..BackendConfig::default()
        }
    }
    async fn received(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.count() < count {
                tokio::time::sleep(Duration::from_millis(5)).await
            }
        })
        .await
        .unwrap();
    }
}
async fn runtime() -> (Directory, Arc<Runtime>) {
    let directory = Directory::new();
    let runtime = Runtime::open(directory.path(), Arc::new(AllowAnonymous), false)
        .await
        .unwrap();
    (directory, runtime)
}
fn query(topic: &str) -> AclQuery {
    AclQuery {
        client_id: "client-one".into(),
        username: "device".into(),
        action: AclAction::Publish,
        topic: topic.into(),
    }
}
fn principal() -> Principal {
    Principal {
        name: "user:device".into(),
    }
}
async fn enable(runtime: &Runtime, server: &Server, ttl: u64) {
    let config = Config {
        authentication: server.backend(),
        authorization: AuthorizationConfig {
            backend: server.backend(),
            cache_ttl_seconds: ttl,
        },
        ..Config::default()
    };
    runtime.replace(config).await.unwrap();
}
async fn authorize(runtime: &Runtime, query: AclQuery) -> Result<bool, RemoteAccessError> {
    let attempt = runtime.begin();
    let policy = attempt.policy();
    let action = match query.action {
        AclAction::Publish => Action::Publish,
        AclAction::Subscribe => Action::Subscribe,
    };
    let first = policy.authorize_for_client(
        &Principal {
            name: format!("user:{}", query.username),
        },
        &query.client_id,
        action,
        &query.topic,
    );
    if attempt.pending() {
        attempt.resolve().await?;
    }
    let allowed = policy.authorize_for_client(
        &Principal {
            name: format!("user:{}", query.username),
        },
        &query.client_id,
        action,
        &query.topic,
    );
    assert!(attempt.seal());
    if !first {
        assert!(!allowed)
    }
    Ok(allowed)
}

#[test]
fn configuration_is_flat_strict_and_redacts_every_service_secret() {
    let mut config = Config::default();
    config.authentication.service_auth = ServiceAuth::Bearer {
        token: "secret-token".into(),
    };
    config.authentication.headers = BTreeMap::from([("X-Key".into(), "secret-header".into())]);
    config.authentication.tls.private_key_pem = Some("secret-key".into());
    let public = config.public();
    let json = serde_json::to_string(&public).unwrap();
    for secret in ["secret-token", "secret-header", "secret-key"] {
        assert!(!json.contains(secret));
        assert!(!format!("{config:?}").contains(secret));
    }
    let mut restored = public;
    restored
        .authentication
        .restore(&config.authentication)
        .unwrap();
    assert_eq!(
        serde_json::to_value(restored).unwrap(),
        serde_json::to_value(config).unwrap()
    );
    let wire = serde_json::to_value(Config::default()).unwrap();
    assert!(wire["authorization"].get("backend").is_none());
    assert_eq!(wire["authorization"]["cache_ttl_seconds"], 30);
    let mut bad = wire;
    bad["authorization"]["typo"] = true.into();
    assert!(serde_json::from_value::<Config>(bad).is_err());
}

#[test]
fn configuration_limits_and_composition_are_fail_closed() {
    let mut c = Config::default();
    c.authorization.backend.enabled = true;
    assert_eq!(c.validate(false), Err(Error::InvalidConfig));
    for url in [
        "http://example.com/a",
        "https://user:pass@example.com/a",
        "https://example.com/a?token=secret",
        "https://example.com/#x",
        "file:///etc/passwd",
    ] {
        let b = BackendConfig {
            url: url.into(),
            enabled: true,
            ..BackendConfig::default()
        };
        assert_eq!(b.validate(false), Err(Error::InvalidConfig));
    }
    let mut b = BackendConfig::default();
    b.headers.insert("Host".into(), "example.com".into());
    assert!(b.validate(false).is_err());
    b.headers = BTreeMap::from([
        ("X-Key".into(), "one".into()),
        ("x-key".into(), "two".into()),
    ]);
    assert!(b.validate(false).is_err());
    b.headers = BTreeMap::new();
    b.service_auth = ServiceAuth::Bearer {
        token: "[redacted]".into(),
    };
    assert!(b.restore(&BackendConfig::default()).is_err());
}

#[tokio::test]
async fn actual_http_authentication_uses_status_and_exact_credential_encoding() {
    let (_d, r) = runtime().await;
    let s = Server::new(204).await;
    for (code, expected) in [
        (204, "allow"),
        (401, "deny"),
        (403, "unavailable"),
        (200, "unavailable"),
        (503, "unavailable"),
    ] {
        s.code.store(code, Ordering::Relaxed);
        let result = r
            .probe_authentication(
                s.backend(),
                ConnectInfo {
                    client_id: "one",
                    username: Some("device"),
                    password: Some(b"pass\0word"),
                },
            )
            .await
            .unwrap();
        assert_eq!(result.result, expected);
        assert_eq!(result.http_status, Some(code));
    }
    let body = s.records.lock().unwrap()[0].clone();
    assert_eq!(
        body,
        serde_json::json!({"client_id":"one","username":"device","password":"pass\u{0}word"})
    );
    r.probe_authentication(
        s.backend(),
        ConnectInfo {
            client_id: "one",
            username: Some("device"),
            password: Some(&[255, 0]),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        s.records.lock().unwrap().last().unwrap()["password_base64"],
        "/wA="
    );
    assert!(
        s.records
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .get("password")
            .is_none()
    );
    let status = r.status().await;
    assert_eq!(status.authentication.requests_total, 0);
    assert_eq!(status.cache.entries, 0);
    r.shutdown().await;
}

#[tokio::test]
async fn asynchronous_authentication_binds_username_and_classifies_unavailable() {
    let (_d, r) = runtime().await;
    let s = Server::new(204).await;
    enable(&r, &s, 30).await;
    let attempt = r.begin();
    let p = attempt
        .authenticate(ConnectInfo {
            client_id: "one",
            username: Some("device"),
            password: Some(b"ok"),
        })
        .await
        .unwrap();
    assert_eq!(p.name, "user:device");
    assert!(attempt.seal());
    s.code.store(401, Ordering::Relaxed);
    assert_eq!(
        r.begin()
            .authenticate(ConnectInfo {
                client_id: "one",
                username: Some("device"),
                password: None
            })
            .await,
        Err(RemoteAccessError::BadCredentials)
    );
    s.code.store(403, Ordering::Relaxed);
    assert_eq!(
        r.begin()
            .authenticate(ConnectInfo {
                client_id: "one",
                username: Some("device"),
                password: None
            })
            .await,
        Err(RemoteAccessError::Unavailable)
    );
    let status = r.status().await;
    assert_eq!(
        (
            status.authentication.requests_total,
            status.authentication.allowed_total,
            status.authentication.denied_total,
            status.authentication.errors_total
        ),
        (3, 1, 1, 1)
    );
    r.shutdown().await;
}

#[tokio::test]
async fn point_acl_receipts_are_exact_and_never_global_revocation() {
    let (_d, r) = runtime().await;
    let s = Server::new(204).await;
    enable(&r, &s, 30).await;
    assert!(authorize(&r, query("devices/a")).await.unwrap());
    assert!(authorize(&r, query("devices/a")).await.unwrap());
    assert_eq!(s.count(), 1);
    let mut other = query("devices/a");
    other.client_id = "client-two".into();
    assert!(authorize(&r, other).await.unwrap());
    let mut other = query("devices/a");
    other.action = AclAction::Subscribe;
    assert!(authorize(&r, other).await.unwrap());
    assert!(authorize(&r, query("devices/b")).await.unwrap());
    assert_eq!(s.count(), 4);
    s.code.store(403, Ordering::Relaxed);
    assert!(!authorize(&r, query("devices/denied")).await.unwrap());
    assert!(authorize(&r, query("devices/a")).await.unwrap());
    assert_eq!(s.count(), 5);
    assert!(!r.begin().policy().reconciles_stored_authorization());
    r.shutdown().await;
}

#[tokio::test]
async fn unknown_and_unavailable_never_become_false_denial() {
    let (_d, r) = runtime().await;
    let s = Server::new(503).await;
    enable(&r, &s, 30).await;
    let attempt = r.begin();
    let policy = attempt.policy();
    assert!(policy.authorize_for_client(&principal(), "one", Action::Publish, "devices/a"));
    assert!(attempt.pending());
    assert!(!attempt.seal());
    assert_eq!(attempt.resolve().await, Err(RemoteAccessError::Unavailable));
    assert!(!attempt.seal());
    assert_eq!(r.status().await.cache.entries, 0);
    s.code.store(204, Ordering::Relaxed);
    assert!(authorize(&r, query("devices/a")).await.unwrap());
    let attempt = r.begin();
    assert!(attempt.policy().authorize_for_client(
        &Principal::anonymous(),
        "one",
        Action::Publish,
        "devices/a"
    ));
    assert!(attempt.pending());
    assert_eq!(attempt.resolve().await, Err(RemoteAccessError::Unavailable));
    r.shutdown().await;
}

#[tokio::test]
async fn ttl_zero_pins_only_current_attempt_and_does_not_loop() {
    let (_d, r) = runtime().await;
    let s = Server::new(204).await;
    enable(&r, &s, 0).await;
    for _ in 0..2 {
        let attempt = r.begin();
        let p = attempt.policy();
        assert!(p.authorize_for_client(&principal(), "one", Action::Publish, "devices/a"));
        assert!(attempt.pending());
        attempt.resolve().await.unwrap();
        assert!(p.authorize_for_client(&principal(), "one", Action::Publish, "devices/a"));
        assert!(!attempt.pending());
        assert!(attempt.seal());
    }
    assert_eq!(s.count(), 2);
    assert_eq!(r.status().await.cache.entries, 0);
    r.shutdown().await;
}

#[tokio::test]
async fn cache_clear_and_configuration_change_fence_unsealed_not_admitted_attempts() {
    let (_d, r) = runtime().await;
    let s = Server::new(204).await;
    enable(&r, &s, 30).await;
    let attempt = r.begin();
    let p = attempt.policy();
    p.authorize_for_client(&principal(), "one", Action::Publish, "devices/a");
    attempt.resolve().await.unwrap();
    r.clear_cache().await;
    assert!(!attempt.seal());
    let accepted = r.begin();
    accepted
        .policy()
        .authorize_for_client(&principal(), "one", Action::Publish, "devices/a");
    accepted.resolve().await.unwrap();
    assert!(accepted.seal());
    r.replace(Config::default()).await.unwrap();
    assert!(accepted.seal());
    assert!(accepted.policy().authorize_for_client(
        &principal(),
        "one",
        Action::Publish,
        "devices/a"
    ));
    assert!(!attempt.seal());
    drop(attempt);
    drop(accepted);
    r.shutdown().await;
}

#[tokio::test]
async fn simultaneous_exact_requests_share_one_inflight_call() {
    let (_d, r) = runtime().await;
    let s = Server::new(204).await;
    s.delay.store(100, Ordering::Relaxed);
    enable(&r, &s, 30).await;
    let mut jobs = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let r = r.clone();
        jobs.spawn(async move { authorize(&r, query("devices/a")).await.unwrap() });
    }
    while let Some(result) = jobs.join_next().await {
        assert!(result.unwrap())
    }
    assert_eq!(s.count(), 1);
    assert_eq!(r.status().await.cache.inflight, 0);
    r.shutdown().await;
}

#[tokio::test]
async fn probe_draft_and_redacted_save_have_no_formal_cache_or_live_stat_effect() {
    let (_d, r) = runtime().await;
    let s = Server::new(204).await;
    let mut config = Config {
        authentication: s.backend(),
        ..Config::default()
    };
    config.authentication.service_auth = ServiceAuth::Bearer {
        token: "service-secret".into(),
    };
    config.authorization = AuthorizationConfig {
        backend: s.backend(),
        cache_ttl_seconds: 30,
    };
    r.replace(config).await.unwrap();
    let public = r.configuration().await;
    assert!(
        !serde_json::to_string(&public)
            .unwrap()
            .contains("service-secret")
    );
    let mut probe = public.authorization.clone();
    probe.backend.enabled = false;
    assert_eq!(
        r.probe_authorization(probe, query("devices/a"))
            .await
            .unwrap()
            .result,
        "allow"
    );
    assert_eq!(r.status().await.authorization.requests_total, 0);
    assert_eq!(r.status().await.cache.entries, 0);
    r.replace(public).await.unwrap();
    assert!(
        r.begin()
            .authenticate(ConnectInfo {
                client_id: "one",
                username: Some("device"),
                password: None
            })
            .await
            .is_ok()
    );
    r.shutdown().await;
}

#[tokio::test]
async fn configuration_restart_and_shutdown_preserve_private_durable_settings() {
    let d = Directory::new();
    let s = Server::new(204).await;
    let r = Runtime::open(d.path(), Arc::new(AllowAnonymous), false)
        .await
        .unwrap();
    enable(&r, &s, 0).await;
    r.shutdown().await;
    drop(r);
    let r = Runtime::open(d.path(), Arc::new(AllowAnonymous), false)
        .await
        .unwrap();
    let config = r.configuration().await;
    assert!(config.authentication.enabled);
    assert_eq!(config.authorization.cache_ttl_seconds, 0);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(d.path()).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    r.shutdown().await;
    assert!(matches!(
        r.replace(Config::default()).await,
        Err(Error::Stopped)
    ));
}

#[tokio::test]
async fn disabled_legacy_path_remains_local_and_uses_no_remote_budget() {
    let (_d, r) = runtime().await;
    let attempt = r.begin();
    let p = attempt.policy();
    assert!(!attempt.remote_authentication());
    assert!(p.authorize_for_client(&Principal::anonymous(), "one", Action::Publish, "a"));
    assert!(!p.authorize_for_client(&principal(), "one", Action::Publish, "a"));
    assert!(!attempt.pending());
    r.clear_cache().await;
    assert!(attempt.seal());
    assert!(p.reconciles_stored_authorization());
    assert_eq!(r.shared.attempt_bytes.load(Ordering::Acquire), 0);
    r.shutdown().await;
}

#[tokio::test]
async fn bounded_attempts_requirements_and_generations_fail_busy_without_allowing() {
    let (_d, r) = runtime().await;
    let s = Server::new(204).await;
    enable(&r, &s, 30).await;
    let attempt = r.begin();
    let p = attempt.policy();
    for n in 0..4097 {
        p.authorize_for_client(&principal(), "one", Action::Publish, &format!("topic/{n}"));
    }
    assert!(attempt.pending());
    assert_eq!(attempt.resolve().await, Err(RemoteAccessError::Busy));
    assert!(!attempt.seal());
    drop(p);
    drop(attempt);
    assert_eq!(r.shared.attempt_bytes.load(Ordering::Acquire), 0);
    let a = r.begin();
    enable(&r, &s, 30).await;
    let b = r.begin();
    enable(&r, &s, 30).await;
    let c = r.begin();
    enable(&r, &s, 30).await;
    assert_eq!(r.replace(Config::default()).await.err(), Some(Error::Busy));
    drop((a, b, c));
    r.replace(Config::default()).await.unwrap();
    r.shutdown().await;
}

#[tokio::test]
async fn cancelled_query_releases_flight_and_late_response_cannot_repopulate_clear() {
    let (_d, r) = runtime().await;
    let s = Server::new(204).await;
    s.delay.store(300, Ordering::Relaxed);
    enable(&r, &s, 30).await;
    let rr = r.clone();
    let job = tokio::spawn(async move { authorize(&rr, query("devices/a")).await });
    s.received(1).await;
    r.clear_cache().await;
    let result = job.await.unwrap();
    assert_eq!(result, Err(RemoteAccessError::Unavailable));
    assert_eq!(r.status().await.cache.entries, 0);
    assert_eq!(r.status().await.cache.inflight, 0);
    let rr = r.clone();
    let job = tokio::spawn(async move { authorize(&rr, query("devices/b")).await });
    s.received(2).await;
    job.abort();
    let _ = job.await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while r.status().await.cache.inflight != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await
        }
    })
    .await
    .unwrap();
    r.shutdown().await;
}

#[tokio::test]
async fn both_http_stages_do_not_require_duplicate_static_accounts_but_auth_only_keeps_static_acl()
{
    let d = Directory::new();
    let s = Server::new(204).await;
    let local =
        mqtt_auth::StaticPolicy::from_json(br#"{"allow_anonymous":false,"users":[]}"#).unwrap();
    let r = Runtime::open(d.path(), Arc::new(local), true)
        .await
        .unwrap();
    enable(&r, &s, 30).await;
    let a = r.begin();
    assert!(
        a.policy()
            .authorize_for_client(&principal(), "one", Action::Connect, "one")
    );
    drop(a);
    let mut config = r.configuration().await;
    config.authorization.backend.enabled = false;
    r.replace(config).await.unwrap();
    let a = r.begin();
    assert!(
        !a.policy()
            .authorize_for_client(&principal(), "one", Action::Connect, "one")
    );
    assert!(
        !a.policy()
            .authorize_for_client(&principal(), "one", Action::Publish, "devices/a")
    );
    r.shutdown().await;
}
