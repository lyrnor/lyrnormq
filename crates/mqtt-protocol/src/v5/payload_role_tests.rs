//! Payloads are Binary Data; a receiver may choose format validation separately.
//! OS3.1.3.2.3/3.3.2.3.2 and5.4.9 distinguish producer format from broker checks.
use super::*;

fn packet(header: u8, body: Vec<u8>) -> Vec<u8> {
    // All independent vectors here have a one-byte Remaining Length.
    assert!(body.len() < 128);
    let mut bytes = vec![header, body.len() as u8];
    bytes.extend(body);
    bytes
}
fn publish(payload: &[u8]) -> (Frame, Vec<u8>) {
    let mut frame = Frame::v5(Packet::Publish {
        topic: "t".into(),
        payload: payload.to_vec(),
        qos: 0,
        packet_id: None,
        dup: false,
        retain: false,
    });
    frame.properties = vec![Property::PayloadFormatIndicator(1)];
    let mut body = b"\x00\x01t\x02\x01\x01".to_vec();
    body.extend(payload);
    (frame, packet(0x30, body))
}
fn will(payload: &[u8]) -> (Frame, Vec<u8>) {
    let mut frame = Frame::v5(Packet::Connect {
        client_id: "c".into(),
        clean_session: true,
        keep_alive: 0,
        will: Some(Will {
            topic: "t".into(),
            payload: payload.to_vec(),
            qos: 0,
            retain: false,
        }),
        username: None,
        password: None,
    });
    frame.will_properties = vec![Property::PayloadFormatIndicator(1)];
    let mut body = b"\x00\x04MQTT\x05\x06\x00\x00\x00\x00\x01c\x02\x01\x01\x00\x01t".to_vec();
    body.extend((payload.len() as u16).to_be_bytes());
    body.extend(payload);
    (frame, packet(0x10, body))
}
fn exact(frame: Frame, bytes: Vec<u8>) {
    assert_eq!(encode_frame(&frame).unwrap(), bytes);
    assert_eq!(
        decode_frame(&bytes, bytes.len(), Some(ProtocolVersion::V5)).unwrap(),
        Some((frame, bytes.len()))
    );
}

#[test]
fn pfi_utf8_payload_uses_rfc3629_not_mqtt_string_codepoint_restrictions() {
    for data in [
        b"".as_slice(),
        b"\x00\x01\x7f".as_slice(),
        "\u{feff}e\u{301}\u{ffff}\u{10ffff}".as_bytes(),
        "é 中文".as_bytes(),
    ] {
        assert!(std::str::from_utf8(data).is_ok());
        for (frame, bytes) in [publish(data), will(data)] {
            exact(frame, bytes);
        }
    }
}

#[test]
fn optional_receiver_validation_does_not_make_codec_a_payload_producer() {
    // These producers would violate the PFI1 data promise. The broker receiver
    // may inspect/reject them, but this framing codec deliberately does not choose
    // that optional policy. OS5.4.9 explicitly anticipates a nonvalidating Server.
    for data in [
        b"\xff\x00".as_slice(),
        b"\xc0\x80".as_slice(),
        b"\xed\xa0\x80".as_slice(),
        b"\xf4\x90\x80\x80".as_slice(),
        b"\xe2\x82".as_slice(),
    ] {
        assert!(std::str::from_utf8(data).is_err());
        for (frame, bytes) in [publish(data), will(data)] {
            exact(frame, bytes);
        }
    }
}

#[test]
fn payload_nul_does_not_relax_the_separate_topic_name_rule() {
    let (mut frame, _) = publish(b"\x00");
    assert!(encode_frame(&frame).is_ok());
    if let Packet::Publish { topic, .. } = &mut frame.packet {
        *topic = "bad\0topic".into();
    }
    assert!(encode_frame(&frame).is_err());
    let (mut frame, _) = will(b"\x00");
    assert!(encode_frame(&frame).is_ok());
    if let Packet::Connect {
        will: Some(will), ..
    } = &mut frame.packet
    {
        will.topic = "bad\0topic".into();
    }
    assert!(encode_frame(&frame).is_err());
}
