use super::*;
#[test]
fn endpoint_limits_fail_closed() {
    assert!(WebSocketOptions::default().validate().is_ok());
    for path in ["", "mqtt", "/a b", "/mqtt?x", "/mqtt#x", "/a\n"] {
        assert!(
            WebSocketOptions {
                path: path.into(),
                ..WebSocketOptions::default()
            }
            .validate()
            .is_err()
        );
    }
    for n in [0, 16 * 1024 * 1024 + 1] {
        assert!(
            WebSocketOptions {
                max_message_size: n,
                ..WebSocketOptions::default()
            }
            .validate()
            .is_err()
        );
    }
}

use super::adapter::WebSocketIo;
use std::{io, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex},
    time::timeout,
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::protocol::{Role, WebSocketConfig},
};
fn masked(op: u8, payload: &[u8]) -> Vec<u8> {
    assert!(payload.len() < 126);
    let key = [1, 2, 3, 4];
    let mut frame = vec![op, 0x80 | payload.len() as u8];
    frame.extend_from_slice(&key);
    frame.extend(payload.iter().enumerate().map(|(i, b)| b ^ key[i % 4]));
    frame
}
async fn socket(
    capacity: usize,
    max: usize,
) -> (
    WebSocketIo<super::policy::FramePolicy<DuplexStream>>,
    DuplexStream,
) {
    let (server, client) = duplex(capacity);
    let config = WebSocketConfig::default()
        .read_buffer_size(128)
        .write_buffer_size(0)
        .max_write_buffer_size(max + 1024)
        .max_frame_size(Some(max.max(125)))
        .max_message_size(Some(max));
    let server = WebSocketStream::from_raw_socket(
        super::policy::FramePolicy::new(server, true),
        Role::Server,
        Some(config),
    )
    .await;
    (
        WebSocketIo::new(server, max, Duration::from_millis(30)),
        client,
    )
}
#[tokio::test]
async fn fragments_empty_binary_and_control_flatten_without_eof() {
    let (mut server, mut client) = socket(4096, 128).await;
    let raw = [
        masked(0x82, b""),
        masked(0x02, b"ab"),
        masked(0x89, b"ping"),
        masked(0x80, b"cd"),
        masked(0x82, b"ef"),
        masked(0x8a, b"unsolicited"),
    ]
    .concat();
    client.write_all(&raw).await.unwrap();
    let mut bytes = [0; 6];
    server.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"abcdef");
    let mut pong = [0; 6];
    client.read_exact(&mut pong).await.unwrap();
    assert_eq!(&pong, b"\x8a\x04ping");
    let mut byte = [0];
    assert!(
        timeout(Duration::from_millis(10), client.read(&mut byte))
            .await
            .is_err()
    );
}
#[tokio::test]
async fn framing_errors_flush_typed_close_before_returning_error() {
    for (raw, code, max) in [
        (masked(0x81, b"text"), 1003u16, 128),
        (vec![0x82, 1, b'x'], 1002, 128),
        (masked(0xc2, b"x"), 1002, 128),
        (masked(0x82, b"12345"), 1009, 4),
    ] {
        let (mut server, mut client) = socket(4096, max).await;
        client.write_all(&raw).await.unwrap();
        let mut byte = [0];
        let error = server.read(&mut byte).await.unwrap_err();
        assert!(adapter::is_framing_error(&error));
        let mut close = [0; 4];
        client.read_exact(&mut close).await.unwrap();
        assert_eq!(&close[..2], b"\x88\x02");
        assert_eq!(u16::from_be_bytes([close[2], close[3]]), code);
    }
}
#[tokio::test]
async fn flush_reaches_underlying_socket_and_chunks_obey_message_bound() {
    let (mut server, mut client) = socket(1, 3).await;
    let writer = tokio::spawn(async move {
        server.write_all(b"abcdef").await.unwrap();
        server.flush().await.unwrap();
    });
    tokio::task::yield_now().await;
    assert!(!writer.is_finished());
    let mut bytes = [0; 10];
    timeout(Duration::from_secs(1), client.read_exact(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes, b"\x82\x03abc\x82\x03def");
    writer.await.unwrap();
}
#[tokio::test]
async fn ping_flush_is_bounded_without_mqtt_output() {
    let (mut server, mut client) = socket(1, 128).await;
    let frame = masked(0x89, b"blocked");
    let mut byte = [0];
    let (written, read) = tokio::join!(client.write_all(&frame), server.read(&mut byte));
    written.unwrap();
    assert_eq!(read.unwrap_err().kind(), io::ErrorKind::TimedOut);
}
#[tokio::test]
async fn normal_shutdown_sends_close1000_and_remote_close_is_eof() {
    let (mut server, mut client) = socket(4096, 128).await;
    let closing = tokio::spawn(async move {
        server.shutdown().await.unwrap();
    });
    let mut close = [0; 4];
    client.read_exact(&mut close).await.unwrap();
    assert_eq!(&close, b"\x88\x02\x03\xe8");
    client.write_all(&masked(0x88, b"\x03\xe8")).await.unwrap();
    client.shutdown().await.unwrap();
    closing.await.unwrap();
    let (mut server, mut client) = socket(4096, 128).await;
    client.write_all(&masked(0x88, b"\x03\xe8")).await.unwrap();
    let mut byte = [0];
    assert_eq!(server.read(&mut byte).await.unwrap(), 0);
    client.read_exact(&mut close).await.unwrap();
    assert_eq!(&close, b"\x88\x02\x03\xe8");
}
#[tokio::test]
async fn empty_message_flood_yields_to_timers() {
    let (mut server, mut client) = socket(65536, 128).await;
    let frame = masked(0x82, b"");
    let flood = tokio::spawn(async move {
        loop {
            if client.write_all(&frame).await.is_err() {
                break;
            }
        }
    });
    let mut byte = [0];
    assert!(
        timeout(Duration::from_millis(20), server.read(&mut byte))
            .await
            .is_err()
    );
    flood.abort();
    let _ = flood.await;
}
fn request(path: &str, protocol: &str) -> Vec<u8> {
    format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: {protocol}\r\n\r\n").into_bytes()
}
async fn response(stream: &mut DuplexStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        bytes.push(stream.read_u8().await.unwrap());
    }
    bytes
}
#[tokio::test]
async fn upgrade_preserves_pipeline_and_selects_mqtt_from_tokens() {
    let (server, mut client) = duplex(32768);
    let options = WebSocketOptions::default();
    let task = tokio::spawn(async move {
        let mut server = accept(server, &options, Duration::from_secs(1))
            .await
            .unwrap();
        let mut ping = [0; 2];
        server.read_exact(&mut ping).await.unwrap();
        assert_eq!(&ping, b"\xc0\x00");
    });
    let bytes = [
        request("/mqtt?ignored=1", "other, mqtt"),
        masked(0x82, b"\xc0\0"),
    ]
    .concat();
    client.write_all(&bytes).await.unwrap();
    let headers = String::from_utf8(response(&mut client).await)
        .unwrap()
        .to_ascii_lowercase();
    assert!(headers.starts_with("http/1.1 101"));
    assert!(headers.contains("sec-websocket-protocol: mqtt\r\n"));
    task.await.unwrap();
}
#[tokio::test]
async fn upgrade_rejects_path_subprotocol_key_and_header_overflow() {
    for (bytes, status) in [
        (request("/wrong", "mqtt"), "404"),
        (request("/mqtt", "other"), "400"),
        (
            String::from_utf8(request("/mqtt", "mqtt"))
                .unwrap()
                .replace("dGhlIHNhbXBsZSBub25jZQ==", "YQ==")
                .into_bytes(),
            "400",
        ),
        (vec![b'a'; 16 * 1024], "431"),
    ] {
        let (server, mut client) = duplex(32768);
        let options = WebSocketOptions::default();
        let task = tokio::spawn(async move {
            accept(server, &options, Duration::from_secs(1))
                .await
                .is_err()
        });
        client.write_all(&bytes).await.unwrap();
        let headers = String::from_utf8(response(&mut client).await).unwrap();
        assert!(
            headers.starts_with(&format!("HTTP/1.1 {status}")),
            "{headers}"
        );
        assert!(task.await.unwrap());
    }
}
#[tokio::test]
async fn fragmented_text_and_nonminimal_lengths_fail_before_message_completion() {
    for (raw, code) in [
        (masked(0x01, b"unfinished"), 1003u16),
        (vec![0x82, 0xfe, 0, 2, 1, 2, 3, 4, b'a' ^ 1, b'b' ^ 2], 1002),
        (
            vec![
                0x82,
                0xff,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                2,
                1,
                2,
                3,
                4,
                b'a' ^ 1,
                b'b' ^ 2,
            ],
            1002,
        ),
    ] {
        let (mut server, mut client) = socket(4096, 128).await;
        client.write_all(&raw).await.unwrap();
        let mut byte = [0];
        let result = timeout(Duration::from_millis(100), server.read(&mut byte)).await;
        assert!(
            result.is_ok(),
            "invalid frame waited for the rest of a message"
        );
        assert!(result.unwrap().is_err(), "invalid length was accepted");
        let mut close = [0; 4];
        client.read_exact(&mut close).await.unwrap();
        assert_eq!(u16::from_be_bytes([close[2], close[3]]), code);
    }
}
#[tokio::test]
async fn one_byte_message_budget_still_allows_full_size_ping() {
    let (mut server, mut client) = socket(4096, 1).await;
    let ping = vec![b'p'; 125];
    let raw = [masked(0x82, b"a"), masked(0x89, &ping), masked(0x82, b"b")].concat();
    client.write_all(&raw).await.unwrap();
    let mut data = [0; 2];
    server.read_exact(&mut data).await.unwrap();
    assert_eq!(&data, b"ab");
    let mut pong = vec![0; 127];
    client.read_exact(&mut pong).await.unwrap();
    assert_eq!(&pong[..2], b"\x8a\x7d");
    assert_eq!(&pong[2..], &ping);
    server.write_all(b"cd").await.unwrap();
    server.flush().await.unwrap();
    let mut chunks = [0; 6];
    client.read_exact(&mut chunks).await.unwrap();
    assert_eq!(&chunks, b"\x82\x01c\x82\x01d");
}
#[tokio::test]
async fn canonical_extended_lengths_stream_without_payload_accumulation_in_guard() {
    for size in [126usize, 65536] {
        let (mut server, mut client) = socket(4096, size).await;
        let mut frame = vec![0x82];
        if size == 126 {
            frame.push(0xfe);
            frame.extend_from_slice(&(size as u16).to_be_bytes());
        } else {
            frame.push(0xff);
            frame.extend_from_slice(&(size as u64).to_be_bytes());
        }
        frame.extend_from_slice(&[0; 4]);
        frame.extend(std::iter::repeat_n(b'a', size));
        let task = tokio::spawn(async move {
            client.write_all(&frame).await.unwrap();
        });
        let mut bytes = vec![0; size];
        timeout(Duration::from_secs(1), server.read_exact(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        assert!(bytes.iter().all(|b| *b == b'a'));
        task.await.unwrap();
    }
}
#[tokio::test]
async fn split_reader_control_flush_and_mqtt_writer_make_full_duplex_progress() {
    timeout(Duration::from_secs(1), async {
        let (server, mut client) = socket(1, 128).await;
        let (mut reader, mut writer) = tokio::io::split(server);
        let reading = tokio::spawn(async move {
            let mut bytes = [0; 2];
            reader.read_exact(&mut bytes).await.unwrap();
            bytes
        });
        client.write_all(&masked(0x89, b"p")).await.unwrap();
        let writing = tokio::spawn(async move {
            writer.write_all(b"out").await.unwrap();
            writer.flush().await.unwrap();
        });
        let mut got_pong = false;
        let mut got_binary = false;
        for _ in 0..2 {
            let mut header = [0; 2];
            client.read_exact(&mut header).await.unwrap();
            let mut bytes = vec![0; usize::from(header[1])];
            client.read_exact(&mut bytes).await.unwrap();
            match header[0] {
                0x8a => {
                    assert_eq!(&bytes, b"p");
                    got_pong = true;
                    client.write_all(&masked(0x82, b"in")).await.unwrap();
                }
                0x82 => {
                    assert_eq!(&bytes, b"out");
                    got_binary = true;
                }
                other => panic!("unexpected frame {other}"),
            }
        }
        assert!(got_pong && got_binary);
        assert_eq!(&reading.await.unwrap(), b"in");
        writing.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn incomplete_fragment_flood_yields_to_mqtt_deadlines() {
    let (mut server, mut client) = socket(65536, 128).await;
    client.write_all(&masked(0x02, b"")).await.unwrap();
    let continuation = masked(0x00, b"");
    let flood = tokio::spawn(async move {
        loop {
            if client.write_all(&continuation).await.is_err() {
                break;
            }
        }
    });
    let mut byte = [0];
    assert!(
        timeout(Duration::from_millis(20), server.read(&mut byte))
            .await
            .is_err()
    );
    flood.abort();
    let _ = flood.await;
}
#[tokio::test]
async fn wss_shutdown_waits_for_ws_echo_and_delayed_tls_close_notify() {
    use std::sync::Arc;
    use tokio_rustls::{TlsConnector, rustls};
    timeout(Duration::from_secs(2), async {
        let cert = include_bytes!("../tls/fixtures/cert.pem");
        let key = include_bytes!("../tls/fixtures/key.pem");
        let acceptor = crate::TlsConfig::from_pem(cert, key).unwrap().acceptor;
        let mut roots = rustls::RootCertStore::empty();
        let mut pem = cert.as_slice();
        for cert in rustls_pemfile::certs(&mut pem) {
            roots.add(cert.unwrap()).unwrap();
        }
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        let (raw_server, raw_client) = duplex(4096);
        let connector = TlsConnector::from(Arc::new(config));
        let (server, client) = tokio::join!(
            acceptor.accept(raw_server),
            connector.connect("localhost".try_into().unwrap(), raw_client)
        );
        let mut client = client.unwrap();
        let socket = WebSocketStream::from_raw_socket(
            super::policy::FramePolicy::new(server.unwrap(), true),
            Role::Server,
            None,
        )
        .await;
        let mut server = WebSocketIo::new(socket, 128, Duration::from_millis(250));
        let closing = tokio::spawn(async move {
            server.shutdown().await.unwrap();
        });
        let mut close = [0; 4];
        client.read_exact(&mut close).await.unwrap();
        assert_eq!(&close, b"\x88\x02\x03\xe8");
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !closing.is_finished(),
            "server dropped WSS before receiving peer WS Close"
        );
        client.write_all(&masked(0x88, b"\x03\xe8")).await.unwrap();
        client.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !closing.is_finished(),
            "server dropped WSS before peer TLS close_notify"
        );
        client.shutdown().await.unwrap();
        let mut byte = [0];
        assert_eq!(client.read(&mut byte).await.unwrap(), 0);
        closing.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn shutdown_bounds_missing_ws_ack_and_missing_transport_eof() {
    for ack in [false, true] {
        let (mut server, mut client) = socket(4096, 128).await;
        let closing = tokio::spawn(async move { server.shutdown().await });
        let mut close = [0; 4];
        client.read_exact(&mut close).await.unwrap();
        assert_eq!(&close, b"\x88\x02\x03\xe8");
        if ack {
            client.write_all(&masked(0x88, b"\x03\xe8")).await.unwrap();
            client.flush().await.unwrap();
        }
        // Keep the peer transport open: both a missing WS acknowledgement and
        // a peer that never half-closes must release server resources on time.
        let result = timeout(Duration::from_millis(250), closing)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
    }
}
#[tokio::test]
async fn ungraceful_ws_close_does_not_wait_for_peer_transport_eof() {
    use crate::io_stream::IoStream;
    use std::sync::atomic::Ordering;
    let (mut server, mut client) = socket(4096, 128).await;
    server
        .full_close_policy()
        .unwrap()
        .store(false, Ordering::Release);
    client.write_all(&masked(0x88, b"\x03\xe8")).await.unwrap();
    let mut byte = [0];
    assert_eq!(server.read(&mut byte).await.unwrap(), 0);
    let mut close = [0; 4];
    client.read_exact(&mut close).await.unwrap();
    assert_eq!(&close, b"\x88\x02\x03\xe8");
    // Deliberately keep the peer open, as a Will observer can legitimately wait
    // after receiving its WS close reply without sending TLS close_notify yet.
    timeout(Duration::from_millis(10), server.shutdown())
        .await
        .unwrap()
        .unwrap();
}
