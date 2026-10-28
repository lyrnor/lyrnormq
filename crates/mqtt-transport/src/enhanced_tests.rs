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
    assert_eq!(refused.packet, Packet::Disconnect);
    assert_eq!(refused.reason_code, ReasonCode::PROTOCOL_ERROR);
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
    assert_eq!(failure.packet, Packet::Disconnect);
    assert_eq!(failure.reason_code, ReasonCode::NOT_AUTHORIZED);
    closed(&mut stream).await;
    stop(shutdown, task).await;
}
