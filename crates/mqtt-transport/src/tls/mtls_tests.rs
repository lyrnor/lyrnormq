use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, duplex},
    time::{Duration, timeout},
};
use tokio_rustls::TlsConnector;

const CERT: &[u8] = include_bytes!("fixtures/cert.pem");
const KEY: &[u8] = include_bytes!("fixtures/key.pem");
const CA: &[u8] = include_bytes!("fixtures/mtls/ca.pem");
const CLIENT: &[u8] = include_bytes!("fixtures/mtls/client.pem");
const CLIENT_KEY: &[u8] = include_bytes!("fixtures/mtls/client-key.pem");

#[test]
fn client_roots_are_nonempty_strict_and_independently_bounded() {
    assert!(TlsConfig::from_pem_with_client_ca(CERT, KEY, CA).is_ok());
    assert!(TlsConfig::from_pem_with_client_ca(CERT, KEY, &[CA, CA].concat()).is_ok());
    for ca in [
        b"".as_slice(),
        b" \n\t",
        b"junk",
        CLIENT_KEY,
        b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
        b"-----BEGIN CERTIFICATE-----\nAAAA",
        b"-----BEGIN ENCRYPTED PRIVATE KEY-----\nAAAA\n-----END ENCRYPTED PRIVATE KEY-----\n",
        &[CA, b"trailing junk"].concat(),
        &[CA, CLIENT_KEY].concat(),
        &vec![b' '; 1024 * 1024 + 1],
    ] {
        assert_eq!(
            TlsConfig::from_pem_with_client_ca(CERT, KEY, ca)
                .err()
                .expect("bad CA accepted")
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    assert!(TlsConfig::from_pem_with_client_ca(b"", KEY, CA).is_err());
    assert!(TlsConfig::from_pem_with_client_ca(CERT, b"", CA).is_err());
    assert!(TlsConfig::from_pem_with_client_ca(&vec![b' '; 1024 * 1024 + 1], KEY, CA).is_err());
    assert!(TlsConfig::from_pem_with_client_ca(CERT, &vec![b' '; 1024 * 1024 + 1], CA).is_err());
    assert!(TlsConfig::from_pem_with_client_ca(CERT, &[KEY, KEY].concat(), CA).is_err());
    assert!(
        TlsConfig::from_pem_with_client_ca(CERT, include_bytes!("fixtures/other-key.pem"), CA)
            .is_err()
    );
}

fn client(
    version: &'static rustls::SupportedProtocolVersion,
    identity: Option<&[u8]>,
) -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in parse_certificates(CERT).unwrap() {
        roots.add(cert).unwrap();
    }
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[version])
    .unwrap()
    .with_root_certificates(roots);
    let mut private_key = CLIENT_KEY;
    let config = match identity {
        Some(cert) => builder
            .with_client_auth_cert(
                parse_certificates(cert).unwrap(),
                rustls_pemfile::private_key(&mut private_key)
                    .unwrap()
                    .unwrap(),
            )
            .unwrap(),
        None => builder.with_no_client_auth(),
    };
    Arc::new(config)
}

/// TLS1.3 may finish connect() before the server rejects client authentication.
/// Require a server-accepted handshake AND actual application exchange, never
/// treat the client's connect() return as acceptance by itself.
async fn handshake(
    server: TlsConfig,
    version: &'static rustls::SupportedProtocolVersion,
    identity: Option<&[u8]>,
) -> (io::Result<()>, bool) {
    handshake_config(server, version, client(version, identity)).await
}

async fn handshake_config(
    server: TlsConfig,
    version: &'static rustls::SupportedProtocolVersion,
    client: Arc<rustls::ClientConfig>,
) -> (io::Result<()>, bool) {
    timeout(Duration::from_secs(2), async {
        let (server_io, client_io) = duplex(16 * 1024);
        let accepting = async {
            let mut stream = server.acceptor.accept(server_io).await?;
            assert_eq!(stream.get_ref().1.protocol_version(), Some(version.version));
            let mut byte = [0];
            stream.read_exact(&mut byte).await?;
            assert_eq!(byte, *b"x");
            stream.write_all(b"y").await?;
            stream.flush().await
        };
        let connecting = async {
            let Ok(mut stream) = TlsConnector::from(client)
                .connect("localhost".try_into().unwrap(), client_io)
                .await
            else {
                return false;
            };
            if stream.write_all(b"x").await.is_err() || stream.flush().await.is_err() {
                return false;
            }
            let mut byte = [0];
            stream.read_exact(&mut byte).await.is_ok() && byte == *b"y"
        };
        tokio::join!(accepting, connecting)
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn mandatory_client_auth_requires_trusted_valid_client_purpose_in_both_tls_versions() {
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        let (server, exchanged) = handshake(
            TlsConfig::from_pem_with_client_ca(CERT, KEY, CA).unwrap(),
            version,
            Some(CLIENT),
        )
        .await;
        assert!(server.is_ok(), "trusted client failed: {server:?}");
        assert!(exchanged);
        for (identity, reason) in [
            (None, "NoCertificatesPresented"),
            (
                Some(include_bytes!("fixtures/mtls/wrong-ca.pem").as_slice()),
                "UnknownIssuer",
            ),
            (
                Some(include_bytes!("fixtures/mtls/expired.pem").as_slice()),
                "Expired",
            ),
            (
                Some(include_bytes!("fixtures/mtls/wrong-eku.pem").as_slice()),
                "InvalidPurpose",
            ),
        ] {
            let (server, exchanged) = handshake(
                TlsConfig::from_pem_with_client_ca(CERT, KEY, CA).unwrap(),
                version,
                identity,
            )
            .await;
            let error = server.expect_err("untrusted/absent client certificate accepted");
            assert!(format!("{error:?}").contains(reason), "{reason}: {error:?}");
            assert!(
                !exchanged,
                "rejected TLS client exchanged application bytes"
            );
        }
    }
}

#[tokio::test]
async fn original_constructor_still_accepts_clients_without_certificates() {
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        let (server, exchanged) =
            handshake(TlsConfig::from_pem(CERT, KEY).unwrap(), version, None).await;
        assert!(server.is_ok(), "legacy TLS behavior changed: {server:?}");
        assert!(exchanged);
    }
}

#[derive(Debug)]
struct WrongPrivateKey(Arc<rustls::sign::CertifiedKey>);
impl rustls::client::ResolvesClientCert for WrongPrivateKey {
    fn resolve(
        &self,
        _: &[&[u8]],
        _: &[rustls::SignatureScheme],
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        Some(self.0.clone())
    }
    fn has_certs(&self) -> bool {
        true
    }
}
#[tokio::test]
async fn trusted_certificate_without_matching_private_key_is_rejected() {
    // Deliberately bypass the normal client's own key-match check to send a
    // trusted certificate with an invalid CertificateVerify proof on the wire.
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        let mut key = KEY;
        let key = rustls_pemfile::private_key(&mut key).unwrap().unwrap();
        let signing = rustls::crypto::ring::sign::any_supported_type(&key).unwrap();
        let certified =
            rustls::sign::CertifiedKey::new(parse_certificates(CLIENT).unwrap(), signing);
        let mut config = (*client(version, Some(CLIENT))).clone();
        config.client_auth_cert_resolver = Arc::new(WrongPrivateKey(Arc::new(certified)));
        let (server, exchanged) = handshake_config(
            TlsConfig::from_pem_with_client_ca(CERT, KEY, CA).unwrap(),
            version,
            Arc::new(config),
        )
        .await;
        assert!(server.is_err(), "invalid private-key proof accepted");
        assert!(!exchanged);
    }
}
