use super::*;
use mqtt_auth::{AccessControl, AccessFuture, RemoteAccess};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug)]
struct Grant;
impl AccessControl for Grant {
    fn authenticate(&self, _: ConnectInfo<'_>) -> Result<Principal, AuthError> {
        Err(AuthError::BadCredentials)
    }
    fn authorize(&self, principal: &Principal, _: Action, _: &str) -> bool {
        principal.name.starts_with("user:")
    }
    fn reconciles_stored_authorization(&self) -> bool {
        false
    }
}
#[derive(Debug)]
struct Provider {
    started: AtomicUsize,
    gated: AtomicBool,
    release: Arc<Semaphore>,
    credentials: Mutex<Vec<(String, String, Vec<u8>)>>,
}
impl Default for Provider {
    fn default() -> Self {
        Self {
            started: AtomicUsize::new(0),
            gated: AtomicBool::new(false),
            release: Arc::new(Semaphore::new(0)),
            credentials: Mutex::new(Vec::new()),
        }
    }
}
#[derive(Debug)]
struct TestRemote(Arc<Provider>);
impl RemoteAccess for TestRemote {
    fn begin(&self) -> Arc<dyn AccessAttempt> {
        Arc::new(Attempt(self.0.clone()))
    }
}
#[derive(Debug)]
struct Attempt(Arc<Provider>);
impl AccessAttempt for Attempt {
    fn policy(&self) -> Arc<dyn AccessControl> {
        Arc::new(Grant)
    }
    fn remote_authentication(&self) -> bool {
        true
    }
    fn authenticate<'a>(
        &'a self,
        info: ConnectInfo<'a>,
    ) -> AccessFuture<'a, Result<Principal, RemoteAccessError>> {
        Box::pin(async move {
            self.0.started.fetch_add(1, Ordering::Relaxed);
            self.0.credentials.lock().unwrap().push((
                info.client_id.into(),
                info.username.unwrap_or("").into(),
                info.password.unwrap_or_default().to_vec(),
            ));
            if self.0.gated.load(Ordering::Relaxed) {
                self.0
                    .release
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| RemoteAccessError::Unavailable)?
                    .forget();
            }
            let username = info.username.ok_or(RemoteAccessError::BadCredentials)?;
            if info.password != Some(b"secret".as_slice()) {
                return Err(RemoteAccessError::BadCredentials);
            }
            Ok(Principal {
                name: format!("user:{username}"),
            })
        })
    }
    fn pending(&self) -> bool {
        false
    }
    fn resolve(&self) -> AccessFuture<'_, Result<(), RemoteAccessError>> {
        Box::pin(async { Ok(()) })
    }
    fn seal(&self) -> bool {
        true
    }
}
async fn worker(provider: Arc<Provider>) -> Worker {
    Worker::new(
        Broker::start(
            BrokerConfig {
                access_control: Arc::new(Grant),
                remote_access: Some(Arc::new(TestRemote(provider))),
                remote_plain: true,
                max_auth_tasks: 1,
                auth_rate_per_second: 0,
                ..BrokerConfig::default()
            },
            Arc::new(SystemClock),
            None,
        )
        .await
        .unwrap(),
    )
}
fn connect_request(client: &str, generation: u64, plain: bool, data: Option<&[u8]>) -> Request {
    let (wake, _) = watch::channel(0);
    let (cancel, _) = watch::channel(false);
    // Retain no external subscriber in these internal Worker fixtures.
    let _ = wake.send_replace(0);
    let mut frame = Frame::v5(Packet::Connect {
        client_id: client.into(),
        clean_session: true,
        keep_alive: 30,
        will: None,
        username: if plain { None } else { Some("device".into()) },
        password: if plain {
            None
        } else {
            Some(Password::new(b"secret".to_vec()))
        },
    });
    if plain {
        frame
            .properties
            .push(Property::AuthenticationMethod("PLAIN".into()));
    }
    if let Some(data) = data {
        frame
            .properties
            .push(Property::AuthenticationData(data.to_vec()));
    }
    let (reason, _) = watch::channel(None);
    normalize_connect(
        frame,
        generation,
        wake,
        cancel,
        reason,
        Some(std::time::Instant::now() + Duration::from_secs(5)),
    )
    .unwrap()
}
fn auth(generation: u64, code: u8, data: Option<&[u8]>) -> Request {
    let mut frame = Frame::v5(Packet::Auth);
    frame.reason_code = ReasonCode::try_from(code).unwrap();
    frame
        .properties
        .push(Property::AuthenticationMethod("PLAIN".into()));
    if let Some(data) = data {
        frame
            .properties
            .push(Property::AuthenticationData(data.to_vec()));
    }
    Request::Authenticate {
        generation,
        frame,
        deadline: std::time::Instant::now() + Duration::from_secs(5),
    }
}
#[tokio::test]
async fn http_credentials_are_not_serialized_behind_argon_cpu_slot_or_manager_lock() {
    let provider = Arc::new(Provider::default());
    provider.gated.store(true, Ordering::Relaxed);
    let worker = worker(provider.clone()).await;
    let mut tasks = JoinSet::new();
    for generation in 1..=6 {
        let worker = worker.clone();
        tasks.spawn(async move {
            worker
                .authenticate(connect_request(
                    &format!("client-{generation}"),
                    generation,
                    false,
                    None,
                ))
                .await
        });
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while provider.started.load(Ordering::Relaxed) != 6 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        worker.auth.manager.try_lock().is_ok(),
        "HTTP cannot hold the provider manager lock"
    );
    provider.release.add_permits(6);
    while let Some(reply) = tasks.join_next().await {
        assert!(matches!(
            reply.unwrap().unwrap(),
            Reply::Connected(_) | Reply::Authenticated { .. }
        ));
    }
    assert_eq!(provider.credentials.lock().unwrap().len(), 6);
    worker.broker.shutdown().await.unwrap();
}
#[tokio::test]
async fn plain_challenge_and_reauthentication_use_decoded_credentials_and_reject_owner_change() {
    let provider = Arc::new(Provider::default());
    let worker = worker(provider.clone()).await;
    assert!(matches!(
        worker
            .authenticate(connect_request("plain-client", 11, true, None))
            .await
            .unwrap(),
        Reply::AuthContinue(_)
    ));
    assert_eq!(provider.started.load(Ordering::Relaxed), 0);
    assert!(matches!(
        worker
            .authenticate(auth(11, 0x18, Some(b"\0device\0secret")))
            .await
            .unwrap(),
        Reply::Authenticated { .. }
    ));
    assert!(matches!(
        worker
            .authenticate(auth(11, 0x19, Some(b"\0device\0secret")))
            .await
            .unwrap(),
        Reply::AuthComplete(_)
    ));
    assert_eq!(provider.started.load(Ordering::Relaxed), 2);
    assert_eq!(
        provider.credentials.lock().unwrap()[0],
        ("plain-client".into(), "device".into(), b"secret".to_vec())
    );
    let error = worker
        .authenticate(auth(11, 0x19, Some(b"\0other\0secret")))
        .await
        .err()
        .unwrap();
    assert_eq!(reason(&error), Some(ReasonCode::NOT_AUTHORIZED));
    assert_eq!(provider.started.load(Ordering::Relaxed), 2);
    worker.broker.shutdown().await.unwrap();
}
#[tokio::test]
async fn assigned_identifier_precedes_http_and_bad_credentials_use_connack_reason() {
    let provider = Arc::new(Provider::default());
    let worker = worker(provider.clone()).await;
    let attempt = Arc::new(Attempt(provider.clone())) as Arc<dyn AccessAttempt>;
    let step = worker
        .remote_step(connect_request("", 31, false, None), attempt.clone())
        .await
        .unwrap();
    let Authenticated::Connect { request, .. } = step else {
        panic!("expected authenticated connect")
    };
    assert!(request.assigned);
    assert!(!request.client.is_empty());
    assert_eq!(provider.credentials.lock().unwrap()[0].0, request.client);
    assert!(request.access_attempt.is_some());
    let mut invalid = connect_request("bad", 32, false, None);
    if let Request::Connect(ref mut request) = invalid {
        request.password = Some(Password::new(b"wrong".to_vec()));
    }
    let step = worker.remote_step(invalid, attempt).await.unwrap();
    assert!(
        matches!(step,Authenticated::Reply(reply) if matches!(*reply,Reply::FrameRejected(code) if code==ReasonCode::BAD_USER_NAME_OR_PASSWORD))
    );
    worker.broker.shutdown().await.unwrap();
}
#[test]
fn plain_parser_preserves_existing_sasl_identity_constraints() {
    assert_eq!(
        credentials(b"\0device\0secret", Some("device")).unwrap(),
        ("device", b"secret".as_slice())
    );
    for value in [
        b"\0other\0secret".as_slice(),
        b"spoof\0device\0secret",
        b"\0device\0",
        b"\0device\0a\0b",
        b"\0device\0\xff",
    ] {
        assert!(credentials(value, Some("device")).is_err());
    }
}

#[tokio::test]
async fn cancelled_http_authentication_cannot_create_or_take_over_a_session() {
    let provider = Arc::new(Provider::default());
    provider.gated.store(true, Ordering::Relaxed);
    let worker = worker(provider.clone()).await;
    let task = {
        let worker = worker.clone();
        tokio::spawn(async move {
            worker
                .authenticate(connect_request("cancelled-client", 41, false, None))
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while provider.started.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(matches!(task.await,Err(error) if error.is_cancelled()));
    assert!(!worker.broker.contains("cancelled-client"));
    assert!(worker.auth.manager.try_lock().is_ok());
    provider.gated.store(false, Ordering::Relaxed);
    assert!(matches!(
        worker
            .authenticate(connect_request("cancelled-client", 42, false, None))
            .await
            .unwrap(),
        Reply::Connected(_) | Reply::Authenticated { .. }
    ));
    worker.broker.shutdown().await.unwrap();
}

#[test]
fn unavailable_initial_authentication_uses_server_unavailable_in_both_versions() {
    let Request::Connect(mut request) = connect_request("client", 51, false, None) else {
        panic!("expected CONNECT")
    };
    assert!(
        matches!(refusal(&request,ReasonCode::SERVER_UNAVAILABLE),Reply::FrameRejected(code) if code==ReasonCode::SERVER_UNAVAILABLE)
    );
    request.options.version = ProtocolVersion::V311;
    assert!(matches!(
        refusal(&request, ReasonCode::SERVER_UNAVAILABLE),
        Reply::Rejected(ConnectRefusal::ServerBusy)
    ));
}
