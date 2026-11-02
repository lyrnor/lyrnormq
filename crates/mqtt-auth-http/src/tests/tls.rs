//! HTTPS retains normal trust verification without user-supplied certificates.
use super::*;
use std::{io::Cursor, process::Command};
use tokio_rustls::{TlsAcceptor, rustls};

struct Identity(Directory);
impl Identity {
    fn new() -> Self {
        let directory = Directory::new();
        let output = Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "2",
                "-subj",
                "/CN=localhost",
                "-keyout",
                "server.key",
                "-out",
                "server.pem",
                "-addext",
                "subjectAltName=DNS:localhost",
            ])
            .current_dir(&directory.0)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        Self(directory)
    }
    async fn server(&self) -> TlsServer {
        let certificate = std::fs::read(self.0.0.join("server.pem")).unwrap();
        let key = std::fs::read(self.0.0.join("server.key")).unwrap();
        let certificates = rustls_pemfile::certs(&mut Cursor::new(certificate))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let key = rustls_pemfile::private_key(&mut Cursor::new(key))
            .unwrap()
            .unwrap();
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let requests = Arc::new(AtomicU64::new(0));
        let observed = requests.clone();
        let task = tokio::spawn(async move {
            let permits = Arc::new(tokio::sync::Semaphore::new(8));
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break };
                        let Ok(permit) = permits.clone().try_acquire_owned() else { continue };
                        let acceptor = acceptor.clone();
                        let observed = observed.clone();
                        tasks.spawn(async move {
                            let _permit = permit;
                            let Ok(Ok(mut stream)) = tokio::time::timeout(Duration::from_secs(2), acceptor.accept(stream)).await else { return };
                            let mut data = [0u8; 4096];
                            if let Ok(Ok(n)) = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut data)).await
                                && n > 0 {
                                    observed.fetch_add(1, Ordering::Relaxed);
                                    let _ = stream.write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n").await;
                            }
                        });
                    }
                    _ = tasks.join_next(), if !tasks.is_empty() => {}
                }
            }
        });
        TlsServer {
            url: format!("https://localhost:{port}/mqtt"),
            requests,
            task,
        }
    }
}
struct TlsServer {
    url: String,
    requests: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for TlsServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn https_authentication_and_authorization_reject_untrusted_server_certificate() {
    let identity = tokio::task::spawn_blocking(Identity::new).await.unwrap();
    let server = identity.server().await;
    let (directory, runtime) = runtime().await;
    let backend = BackendConfig {
        enabled: true,
        url: server.url.clone(),
        ..BackendConfig::default()
    };
    let info = || ConnectInfo {
        client_id: "one",
        username: Some("device"),
        password: Some(b"password"),
    };
    let result = runtime
        .probe_authentication(backend.clone(), info())
        .await
        .unwrap();
    assert_eq!(result.result, "unavailable");
    assert_eq!(result.http_status, None);
    assert_eq!(result.error_code, Some("http_error"));
    let authorization = AuthorizationConfig {
        backend: backend.clone(),
        cache_ttl_seconds: 30,
    };
    let result = runtime
        .probe_authorization(authorization.clone(), query("devices/a"))
        .await
        .unwrap();
    assert_eq!(result.result, "unavailable");
    assert_eq!(result.http_status, None);
    runtime
        .replace(Config {
            authentication: backend,
            authorization,
            ..Config::default()
        })
        .await
        .unwrap();
    assert_eq!(
        runtime.begin().authenticate(info()).await,
        Err(RemoteAccessError::Unavailable)
    );
    assert_eq!(server.requests.load(Ordering::Relaxed), 0);
    runtime.shutdown().await;
    let reopened = Runtime::open(directory.path(), Arc::new(AllowAnonymous), false)
        .await
        .unwrap();
    let public = serde_json::to_value(reopened.configuration().await).unwrap();
    assert!(public["authentication"].get("tls").is_none());
    assert!(public["authorization"].get("tls").is_none());
    assert_eq!(
        reopened.begin().authenticate(info()).await,
        Err(RemoteAccessError::Unavailable)
    );
    assert_eq!(server.requests.load(Ordering::Relaxed), 0);
    reopened.shutdown().await;
}
