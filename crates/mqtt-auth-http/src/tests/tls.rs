//! Actual rustls HTTPS/mTLS fixtures with disposable OpenSSL-generated identities.
use super::*;
use std::{io::Cursor, process::Command};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{self, RootCertStore, server::WebPkiClientVerifier},
};
fn openssl(root: &std::path::Path, args: &[&str]) {
    let output = Command::new("openssl")
        .args(args)
        .current_dir(root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "openssl failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
struct Identity {
    directory: Directory,
    ca: String,
    certificate: String,
    key: String,
}
impl Identity {
    fn new() -> Self {
        let directory = Directory::new();
        let p = &directory.0;
        openssl(
            p,
            &[
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "2",
                "-subj",
                "/CN=rmqtt-access-test-ca",
                "-keyout",
                "ca.key",
                "-out",
                "ca.pem",
                "-addext",
                "basicConstraints=critical,CA:TRUE",
                "-addext",
                "keyUsage=critical,keyCertSign,cRLSign",
            ],
        );
        for (name, usage) in [("server", "serverAuth"), ("client", "clientAuth")] {
            let key = format!("{name}.key");
            let csr = format!("{name}.csr");
            let cert = format!("{name}.pem");
            let ext = format!("{name}.ext");
            std::fs::write(p.join(&ext),format!("basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage={usage}\nsubjectAltName=DNS:localhost\n")).unwrap();
            openssl(
                p,
                &[
                    "req",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-subj",
                    "/CN=localhost",
                    "-keyout",
                    &key,
                    "-out",
                    &csr,
                ],
            );
            openssl(
                p,
                &[
                    "x509",
                    "-req",
                    "-days",
                    "2",
                    "-in",
                    &csr,
                    "-CA",
                    "ca.pem",
                    "-CAkey",
                    "ca.key",
                    "-CAcreateserial",
                    "-extfile",
                    &ext,
                    "-out",
                    &cert,
                ],
            );
        }
        let ca = std::fs::read_to_string(p.join("ca.pem")).unwrap();
        let certificate = std::fs::read_to_string(p.join("client.pem")).unwrap();
        let key = std::fs::read_to_string(p.join("client.key")).unwrap();
        Self {
            directory,
            ca,
            certificate,
            key,
        }
    }
    async fn server(&self, mutual: bool) -> TlsServer {
        let p = &self.directory.0;
        let cert = std::fs::read(p.join("server.pem")).unwrap();
        let key = std::fs::read(p.join("server.key")).unwrap();
        let certificates = rustls_pemfile::certs(&mut Cursor::new(cert))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let key = rustls_pemfile::private_key(&mut Cursor::new(key))
            .unwrap()
            .unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap();
        let builder = if mutual {
            let mut roots = RootCertStore::empty();
            for cert in rustls_pemfile::certs(&mut Cursor::new(self.ca.as_bytes())) {
                roots.add(cert.unwrap()).unwrap();
            }
            builder.with_client_cert_verifier(
                WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                    .build()
                    .unwrap(),
            )
        } else {
            builder.with_no_client_auth()
        };
        let config = builder.with_single_cert(certificates, key).unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let requests = Arc::new(AtomicU64::new(0));
        let observed = requests.clone();
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {accepted=listener.accept()=>{let Ok((stream,_))=accepted else{break};let acceptor=acceptor.clone();let observed=observed.clone();tasks.spawn(async move{let Ok(mut stream)=acceptor.accept(stream).await else{return};let mut data=Vec::new();let mut buf=[0u8;4096];let end=loop{let Ok(n)=stream.read(&mut buf).await else{return};if n==0{return}data.extend_from_slice(&buf[..n]);if data.len()>1024*1024+16384{return}
                if let Some(end)=data.windows(4).position(|v|v==b"\r\n\r\n"){break end+4}};let len=String::from_utf8_lossy(&data[..end]).lines().find_map(|line|line.split_once(':').filter(|(k,_)|k.eq_ignore_ascii_case("content-length")).and_then(|(_,v)|v.trim().parse::<usize>().ok())).unwrap_or(0);while data.len()<end+len{let Ok(n)=stream.read(&mut buf).await else{return};if n==0{return}data.extend_from_slice(&buf[..n]);}let body:serde_json::Value=serde_json::from_slice(&data[end..end+len]).unwrap();assert_eq!(body["username"],"device");observed.fetch_add(1,Ordering::Relaxed);let _=stream.write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n").await;});},_=tasks.join_next(),if !tasks.is_empty()=>{}}
            }
        });
        TlsServer {
            url: format!("https://localhost:{port}/mqtt"),
            port,
            requests,
            task,
        }
    }
    fn backend(&self, url: String, client: bool) -> BackendConfig {
        BackendConfig {
            url,
            enabled: true,
            tls: TlsOptions {
                ca_pem: Some(self.ca.clone()),
                certificate_pem: client.then(|| self.certificate.clone()),
                private_key_pem: client.then(|| self.key.clone()),
            },
            ..BackendConfig::default()
        }
    }
}
struct TlsServer {
    url: String,
    port: u16,
    requests: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for TlsServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_https_ca_hostname_and_required_client_certificate_checks() {
    let identity = tokio::task::spawn_blocking(Identity::new).await.unwrap();
    let (_d, r) = runtime().await;
    let server = identity.server(false).await;
    let info = || ConnectInfo {
        client_id: "one",
        username: Some("device"),
        password: Some(b"test-password"),
    };
    assert_eq!(
        r.probe_authentication(identity.backend(server.url.clone(), false), info())
            .await
            .unwrap()
            .result,
        "allow"
    );
    let mut no_ca = identity.backend(server.url.clone(), false);
    no_ca.tls.ca_pem = None;
    assert_eq!(
        r.probe_authentication(no_ca, info()).await.unwrap().result,
        "unavailable"
    );
    let bad_hostname = identity.backend(format!("https://127.0.0.1:{}/mqtt", server.port), false);
    assert_eq!(
        r.probe_authentication(bad_hostname, info())
            .await
            .unwrap()
            .result,
        "unavailable"
    );
    assert_eq!(server.requests.load(Ordering::Relaxed), 1);
    let mtls = identity.server(true).await;
    assert_eq!(
        r.probe_authentication(identity.backend(mtls.url.clone(), true), info())
            .await
            .unwrap()
            .result,
        "allow"
    );
    assert_eq!(
        r.probe_authentication(identity.backend(mtls.url.clone(), false), info())
            .await
            .unwrap()
            .result,
        "unavailable"
    );
    assert_eq!(mtls.requests.load(Ordering::Relaxed), 1);
    let mut config = Config {
        authentication: identity.backend(mtls.url.clone(), true),
        ..Config::default()
    };
    r.replace(config.clone()).await.unwrap();
    let public = r.configuration().await;
    assert_eq!(
        public.authentication.tls.private_key_pem.as_deref(),
        Some("[redacted]")
    );
    assert!(
        !serde_json::to_string(&public)
            .unwrap()
            .contains("BEGIN PRIVATE KEY")
    );
    r.replace(public).await.unwrap();
    assert!(r.begin().authenticate(info()).await.is_ok());
    config.authentication.tls.certificate_pem =
        Some(format!("{}\n{}", identity.certificate, identity.key));
    assert!(r.replace(config).await.is_err());
    r.shutdown().await;
}
