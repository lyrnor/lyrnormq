use super::*;
const CERT: &[u8] = include_bytes!("fixtures/cert.pem");
const KEY: &[u8] = include_bytes!("fixtures/key.pem");
#[test]
fn identity_bounds_and_exact_key_cardinality() {
    assert!(TlsConfig::from_pem(CERT, KEY).is_ok());
    for (cert, key) in [
        (b"".as_slice(), KEY),
        (CERT, b"".as_slice()),
        (b"garbage", KEY),
        (CERT, b"-----BEGIN PRIVATE KEY-----\nAAAA"),
    ] {
        assert!(TlsConfig::from_pem(cert, key).is_err());
    }
    assert!(TlsConfig::from_pem(&vec![b' '; 1024 * 1024 + 1], KEY).is_err());
    assert!(TlsConfig::from_pem(CERT, &vec![b' '; 1024 * 1024 + 1]).is_err());
    assert!(TlsConfig::from_pem(CERT, &[KEY, KEY].concat()).is_err());
    assert!(TlsConfig::from_pem(CERT, include_bytes!("fixtures/other-key.pem")).is_err());
    assert!(TlsConfig::from_pem(CERT, &[KEY, b"junk"].concat()).is_err());
    assert!(
        TlsConfig::from_pem(
            &[
                CERT,
                b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"
            ]
            .concat(),
            KEY
        )
        .is_err()
    );
}

use mqtt_protocol::{Frame, Packet, ProtocolVersion, decode_frame, encode_frame};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    time::{Duration, timeout},
};
fn client_config(version: &'static rustls::SupportedProtocolVersion) -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    let mut pem = CERT;
    for cert in rustls_pemfile::certs(&mut pem) {
        roots.add(cert.unwrap()).unwrap();
    }
    Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[version])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth(),
    )
}
async fn receive<S: tokio::io::AsyncRead + Unpin>(
    stream: &mut S,
    version: ProtocolVersion,
) -> Frame {
    timeout(Duration::from_secs(2), async {
        let mut bytes = Vec::new();
        loop {
            if let Some((frame, _)) = decode_frame(&bytes, 65536, Some(version)).unwrap() {
                return frame;
            }
            let mut chunk = [0; 4096];
            let n = stream.read(&mut chunk).await.unwrap();
            assert_ne!(n, 0);
            bytes.extend_from_slice(&chunk[..n]);
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn verified_tls12_tls13_share_both_mqtt_paths() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = watch::channel(false);
    let server = tokio::spawn(crate::serve_tls(
        listener,
        crate::BrokerConfig::default(),
        TlsConfig::from_pem(CERT, KEY).unwrap(),
        stopped,
    ));
    for tls_version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        for mqtt_version in [ProtocolVersion::V311, ProtocolVersion::V5] {
            let stream = TcpStream::connect(address).await.unwrap();
            let mut stream = tokio_rustls::TlsConnector::from(client_config(tls_version))
                .connect("localhost".try_into().unwrap(), stream)
                .await
                .unwrap();
            assert_eq!(
                stream.get_ref().1.protocol_version(),
                Some(tls_version.version)
            );
            let frame = Frame {
                version: mqtt_version,
                ..Frame::v311(Packet::Connect {
                    client_id: "tls-smoke".into(),
                    clean_session: true,
                    keep_alive: 0,
                    will: None,
                    username: None,
                    password: None,
                })
            };
            stream
                .write_all(&encode_frame(&frame).unwrap())
                .await
                .unwrap();
            stream.flush().await.unwrap();
            assert!(matches!(
                receive(&mut stream, mqtt_version).await.packet,
                Packet::ConnAck { code: 0, .. }
            ));
            let ping = Frame {
                version: mqtt_version,
                ..Frame::v311(Packet::PingReq)
            };
            stream
                .write_all(&encode_frame(&ping).unwrap())
                .await
                .unwrap();
            stream.flush().await.unwrap();
            assert_eq!(
                receive(&mut stream, mqtt_version).await.packet,
                Packet::PingResp
            );
            let disconnect = Frame {
                version: mqtt_version,
                ..Frame::v311(Packet::Disconnect)
            };
            stream
                .write_all(&encode_frame(&disconnect).unwrap())
                .await
                .unwrap();
            stream.flush().await.unwrap();
            stream.shutdown().await.unwrap();
            // Rustls reports an unclean TCP EOF as UnexpectedEof: a clean zero
            // proves the reciprocal TLS close_notify was actually received.
            let mut byte = [0];
            assert_eq!(
                timeout(Duration::from_secs(2), stream.read(&mut byte))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
        }
    }
    stop.send(true).unwrap();
    timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn idle_tls_handshake_holds_permit_times_out_and_shutdown_interrupts() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let config = crate::BrokerConfig {
        max_connections: 1,
        handshake_timeout: Duration::from_millis(200),
        ..crate::BrokerConfig::default()
    };
    let (stop, stopped) = watch::channel(false);
    let server = tokio::spawn(crate::serve_tls(
        listener,
        config,
        TlsConfig::from_pem(CERT, KEY).unwrap(),
        stopped,
    ));
    let mut idle = TcpStream::connect(address).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut excess = TcpStream::connect(address).await.unwrap();
    let mut byte = [0];
    assert_eq!(
        timeout(Duration::from_millis(100), excess.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert_eq!(
        timeout(Duration::from_secs(1), idle.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    let stream = TcpStream::connect(address).await.unwrap();
    let mut tls = tokio_rustls::TlsConnector::from(client_config(&rustls::version::TLS13))
        .connect("localhost".try_into().unwrap(), stream)
        .await
        .unwrap();
    tls.shutdown().await.unwrap();
    drop(tls);
    // A raw TCP peer during a fresh handshake cannot hold broker shutdown.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let _idle = TcpStream::connect(address).await.unwrap();
    stop.send(true).unwrap();
    timeout(Duration::from_millis(150), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
