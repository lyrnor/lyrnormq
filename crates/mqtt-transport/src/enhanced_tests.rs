//! Real TCP clients exercise authentication independently of worker internals.
use super::live_tests::{broker, closed, stop};
use super::*;
use mqtt_auth::PlainAuthenticator;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[derive(Debug)]
pub(super) struct Policy;
impl mqtt_auth::AccessControl for Policy {
    fn authenticate(
        &self,
        info: mqtt_auth::ConnectInfo<'_>,
    ) -> Result<mqtt_auth::Principal, mqtt_auth::AuthError> {
        if info.username == Some("alice") && info.password == Some(b"password") {
            Ok(mqtt_auth::Principal::anonymous())
        } else {
            Err(mqtt_auth::AuthError::BadCredentials)
        }
    }
    fn authorize(&self, _: &mqtt_auth::Principal, _: mqtt_auth::Action, _: &str) -> bool {
        true
    }
}
fn configured() -> BrokerConfig {
    let mut c = BrokerConfig {
        access_control: Arc::new(Policy),
        ..BrokerConfig::default()
    };
    c.enhanced_auth = Some(Arc::new(PlainAuthenticator::new(c.access_control.clone())));
    c
}
fn connect(client: &str, data: Option<&[u8]>) -> Frame {
    let mut frame = Frame::v5(Packet::Connect {
        client_id: client.into(),
        clean_session: true,
        keep_alive: 0,
        will: None,
        username: None,
        password: None,
    });
    frame
        .properties
        .push(Property::AuthenticationMethod("PLAIN".into()));
    if let Some(data) = data {
        frame
            .properties
            .push(Property::AuthenticationData(data.to_vec()));
    }
    frame
}
fn auth(code: u8, data: Option<&[u8]>) -> Frame {
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
    frame
}
async fn write(stream: &mut TcpStream, frame: Frame) {
    stream
        .write_all(&mqtt_protocol::encode_frame(&frame).unwrap())
        .await
        .unwrap();
}
async fn read(stream: &mut TcpStream) -> Frame {
    timeout(Duration::from_secs(2), async {
        let mut bytes = Vec::new();
        loop {
            if let Some((frame, _)) =
                mqtt_protocol::decode_frame(&bytes, 4096, Some(ProtocolVersion::V5)).unwrap()
            {
                return frame;
            }
            let mut byte = [0];
            stream.read_exact(&mut byte).await.unwrap();
            bytes.push(byte[0]);
        }
    })
    .await
    .unwrap()
}
fn method(frame: &Frame) {
    assert!(
        frame
            .properties
            .contains(&Property::AuthenticationMethod("PLAIN".into()))
    );
}
#[tokio::test]
async fn plain_challenge_reauth_preserves_control_progress_and_assigned_id() {
    let (address, shutdown, task) = broker(configured()).await;
    let mut stream = TcpStream::connect(address).await.unwrap();
    write(&mut stream, connect("", None)).await;
    let challenge = read(&mut stream).await;
    assert_eq!(challenge.packet, Packet::Auth);
    assert_eq!(challenge.reason_code.value(), 0x18);
    method(&challenge);
    write(&mut stream, auth(0x18, Some(b"\0alice\0password"))).await;
    let accepted = read(&mut stream).await;
    assert!(matches!(accepted.packet, Packet::ConnAck { code: 0, .. }));
    method(&accepted);
    assert!(
        accepted
            .properties
            .iter()
            .any(|p| matches!(p, Property::AssignedClientIdentifier(_)))
    );
    write(&mut stream, auth(0x19, None)).await;
    assert_eq!(read(&mut stream).await.reason_code.value(), 0x18);
    write(&mut stream, Frame::v5(Packet::PingReq)).await;
    assert_eq!(read(&mut stream).await.packet, Packet::PingResp);
    write(&mut stream, auth(0x18, Some(b"\0alice\0password"))).await;
    let success = read(&mut stream).await;
    assert_eq!(success.packet, Packet::Auth);
    assert_eq!(success.reason_code.value(), 0);
    method(&success);
    write(&mut stream, Frame::v5(Packet::Disconnect)).await;
    closed(&mut stream).await;
    stop(shutdown, task).await;
}
#[tokio::test]
async fn incomplete_auth_cannot_takeover_existing_connection_or_accept_publish() {
    let (address, shutdown, task) = broker(configured()).await;
    let mut original = TcpStream::connect(address).await.unwrap();
    write(&mut original, connect("owner", Some(b"\0alice\0password"))).await;
    assert!(matches!(
        read(&mut original).await.packet,
        Packet::ConnAck { code: 0, .. }
    ));
    let mut pending = TcpStream::connect(address).await.unwrap();
    write(&mut pending, connect("owner", None)).await;
    assert_eq!(read(&mut pending).await.packet, Packet::Auth);
    write(&mut original, Frame::v5(Packet::PingReq)).await;
    assert_eq!(read(&mut original).await.packet, Packet::PingResp);
    write(
        &mut pending,
        Frame::v5(Packet::Publish {
            topic: "forbidden".into(),
            payload: b"before-auth".to_vec(),
            qos: 0,
            packet_id: None,
            dup: false,
            retain: true,
        }),
    )
    .await;
    // A negotiated AUTH flow reports protocol failure without creating a session.
    let refused = read(&mut pending).await;
    assert!(matches!(
        refused.packet,
        Packet::ConnAck {
            session_present: false,
            code: 0x82
        }
    ));
    closed(&mut pending).await;
    write(&mut original, Frame::v5(Packet::PingReq)).await;
    assert_eq!(read(&mut original).await.packet, Packet::PingResp);
    write(&mut original, Frame::v5(Packet::Disconnect)).await;
    closed(&mut original).await;
    stop(shutdown, task).await;
}
#[tokio::test]
async fn reauth_timeout_is_absolute_despite_ping_traffic() {
    let mut config = configured();
    config.handshake_timeout = Duration::from_millis(180);
    let (address, shutdown, task) = broker(config).await;
    let mut stream = TcpStream::connect(address).await.unwrap();
    write(&mut stream, connect("timeout", Some(b"\0alice\0password"))).await;
    read(&mut stream).await;
    write(&mut stream, auth(0x19, None)).await;
    read(&mut stream).await;
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(40)).await;
        write(&mut stream, Frame::v5(Packet::PingReq)).await;
        assert_eq!(read(&mut stream).await.packet, Packet::PingResp);
    }
    let failure = read(&mut stream).await;
    assert_eq!(failure.packet, Packet::Disconnect);
    assert_eq!(failure.reason_code, ReasonCode::NOT_AUTHORIZED);
    closed(&mut stream).await;
    stop(shutdown, task).await;
}

#[tokio::test]
async fn wrong_reauthentication_flushes_legal_reason_and_fires_will() {
    let (address, shutdown, task) = broker(configured()).await;
    let mut observer = TcpStream::connect(address).await.unwrap();
    write(
        &mut observer,
        connect("observer", Some(b"\0alice\0password")),
    )
    .await;
    read(&mut observer).await;
    write(
        &mut observer,
        Frame::v5(Packet::Subscribe {
            packet_id: 1,
            filters: vec![("reauth/will".into(), 0)],
        }),
    )
    .await;
    read(&mut observer).await;
    let mut stream = TcpStream::connect(address).await.unwrap();
    let mut request = connect("wrong-reauth", Some(b"\0alice\0password"));
    let Packet::Connect { will, .. } = &mut request.packet else {
        panic!()
    };
    *will = Some(Will {
        topic: "reauth/will".into(),
        payload: b"failed".to_vec(),
        qos: 0,
        retain: false,
    });
    write(&mut stream, request).await;
    read(&mut stream).await;
    write(&mut stream, auth(0x19, Some(b"\0alice\0wrong"))).await;
    let failure = read(&mut stream).await;
    assert_eq!(failure.packet, Packet::Disconnect);
    assert_eq!(failure.reason_code, ReasonCode::NOT_AUTHORIZED);
    closed(&mut stream).await;
    assert!(
        matches!(read(&mut observer).await.packet,Packet::Publish{topic,payload,..} if topic=="reauth/will" && payload==b"failed")
    );
    write(&mut observer, Frame::v5(Packet::Disconnect)).await;
    closed(&mut observer).await;
    stop(shutdown, task).await;
}
#[tokio::test]
async fn initial_enhanced_deadline_reports_authentication_failure() {
    let mut config = configured();
    config.handshake_timeout = Duration::from_millis(60);
    let (address, shutdown, task) = broker(config).await;
    let mut stream = TcpStream::connect(address).await.unwrap();
    write(&mut stream, connect("initial-timeout", None)).await;
    assert_eq!(read(&mut stream).await.packet, Packet::Auth);
    let failure = read(&mut stream).await;
    assert!(matches!(
        failure.packet,
        Packet::ConnAck {
            session_present: false,
            code: 0x87
        }
    ));
    closed(&mut stream).await;
    stop(shutdown, task).await;
}

#[tokio::test]
async fn mqtt_3_14_0_1_initial_auth_failures_use_connack_before_any_disconnect() {
    let mut config = configured();
    config.handshake_timeout = Duration::from_millis(100);
    let (address, shutdown, task) = broker(config).await;
    for scenario in 0..4 {
        let mut stream = TcpStream::connect(address).await.unwrap();
        write(
            &mut stream,
            connect(&format!("initial-failure-{scenario}"), None),
        )
        .await;
        assert_eq!(read(&mut stream).await.packet, Packet::Auth);
        let expected = match scenario {
            0 => {
                write(&mut stream, Frame::v5(Packet::PingReq)).await;
                ReasonCode::PROTOCOL_ERROR
            }
            1 => {
                let mut changed = auth(0x18, None);
                changed.properties = vec![Property::AuthenticationMethod("OTHER".into())];
                write(&mut stream, changed).await;
                ReasonCode::PROTOCOL_ERROR
            }
            2 => {
                // Reserved AUTH fixed-header bit: no successful CONNACK was sent.
                stream.write_all(&[0xf1, 0]).await.unwrap();
                ReasonCode::MALFORMED_PACKET
            }
            _ => ReasonCode::NOT_AUTHORIZED,
        };
        let refusal = read(&mut stream).await;
        assert!(
            matches!(refusal.packet, Packet::ConnAck { code, session_present: false } if code == expected.value()),
            "scenario {scenario}: {refusal:?}"
        );
        closed(&mut stream).await;
    }
    stop(shutdown, task).await;
}

#[tokio::test]
async fn mqtt_3_1_2_28_29_problem_and_response_information_choices() {
    let (address, shutdown, task) = broker(configured()).await;
    for response in [0, 1] {
        let mut stream = TcpStream::connect(address).await.unwrap();
        let mut request = connect(&format!("information-{response}"), None);
        request.properties.extend([
            Property::RequestProblemInformation(0),
            Property::RequestResponseInformation(response),
        ]);
        write(&mut stream, request).await;
        let challenge = read(&mut stream).await;
        assert_eq!(challenge.packet, Packet::Auth);
        assert!(
            challenge
                .properties
                .iter()
                .all(|p| !matches!(p, Property::ReasonString(_) | Property::UserProperty(..)))
        );
        write(&mut stream, auth(0x18, Some(b"\0alice\0password"))).await;
        let accepted = read(&mut stream).await;
        assert!(matches!(accepted.packet, Packet::ConnAck { code: 0, .. }));
        // The optional response facility is deliberately omitted for either value.
        assert!(
            accepted
                .properties
                .iter()
                .all(|p| !matches!(p, Property::ResponseInformation(_)))
        );
        write(
            &mut stream,
            Frame::v5(Packet::Subscribe {
                packet_id: 7,
                filters: vec![("audit/elsewhere".into(), 0)],
            }),
        )
        .await;
        let subscribed = read(&mut stream).await;
        assert!(matches!(
            subscribed.packet,
            Packet::SubAck { packet_id: 7, .. }
        ));
        assert!(subscribed.properties.is_empty());
        write(
            &mut stream,
            Frame::v5(Packet::Publish {
                topic: "audit/value".into(),
                payload: vec![],
                qos: 1,
                packet_id: Some(8),
                dup: false,
                retain: false,
            }),
        )
        .await;
        let acknowledged = read(&mut stream).await;
        assert!(matches!(
            acknowledged.packet,
            Packet::PubAck { packet_id: 8 }
        ));
        assert!(acknowledged.properties.is_empty());
        write(&mut stream, Frame::v5(Packet::Disconnect)).await;
        closed(&mut stream).await;
    }
    stop(shutdown, task).await;
}

#[tokio::test]
async fn mqtt_3_1_2_22_nonzero_keepalive_expires_with_v5_reason() {
    let (address, shutdown, task) = broker(configured()).await;
    let mut stream = TcpStream::connect(address).await.unwrap();
    let mut request = connect("keepalive", Some(b"\0alice\0password"));
    if let Packet::Connect { keep_alive, .. } = &mut request.packet {
        *keep_alive = 1;
    }
    write(&mut stream, request).await;
    assert!(matches!(
        read(&mut stream).await.packet,
        Packet::ConnAck { code: 0, .. }
    ));
    let failure = read(&mut stream).await;
    assert_eq!(failure.packet, Packet::Disconnect);
    assert_eq!(failure.reason_code, ReasonCode::KEEP_ALIVE_TIMEOUT);
    closed(&mut stream).await;
    stop(shutdown, task).await;
}

#[path = "phase2m_tests.rs"]
mod phase2m_tests;

#[path = "phase2n_version_tests.rs"]
mod phase2n_version_tests;
