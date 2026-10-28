use super::*;
use crate::{Frame, Packet, ProtocolVersion, decode_frame, encode_frame};

fn packet(properties: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut body = b"\x00\x04MQTT\x05\x02\x00\x00".to_vec();
    crate::wire::varint(&mut body, properties.len()).unwrap();
    body.extend_from_slice(properties);
    body.extend_from_slice(payload);
    let mut packet = vec![0x10];
    crate::wire::varint(&mut packet, body.len()).unwrap();
    packet.extend_from_slice(&body);
    packet
}

#[test]
fn valid_properties_are_available_before_client_payload() {
    let packet = packet(&[39, 0, 0, 0, 1], b"\x00\x03abc");
    let property_end = packet.len() - 5;
    for end in 0..property_end {
        assert_eq!(peek_connect_maximum_packet_size(&packet[..end]), None);
    }
    for end in property_end..=packet.len() {
        assert_eq!(peek_connect_maximum_packet_size(&packet[..end]), Some(1));
    }
    assert_eq!(
        peek_connect_maximum_packet_size(&self::packet(&[], &[])),
        Some(u32::MAX)
    );
}

#[test]
fn invalid_property_area_is_untrusted() {
    for properties in [
        vec![39, 0, 0, 0, 0],
        vec![39, 0, 0, 0, 1, 39, 0, 0, 0, 2],
        vec![18, 0, 1, b'x'],
        vec![0x7f],
        vec![33, 0, 0],
        vec![39, 0, 0],
    ] {
        assert_eq!(
            peek_connect_maximum_packet_size(&packet(&properties, &[])),
            None
        );
    }
}

#[test]
fn declared_packet_boundary_cannot_borrow_coalesced_bytes() {
    let full = packet(&[39, 0, 0, 0, 1], &[]);
    let mut short = full.clone();
    short[1] -= 1;
    assert_eq!(peek_connect_maximum_packet_size(&short), None);
    let mut empty = vec![0x10, 0];
    empty.extend_from_slice(&full);
    assert_eq!(peek_connect_maximum_packet_size(&empty), None);
    let mut valid = packet(&[], &[]);
    valid.extend_from_slice(&full);
    assert_eq!(peek_connect_maximum_packet_size(&valid), Some(u32::MAX));
}

#[test]
fn canonical_header_prefix_and_version_required() {
    let valid = packet(&[], &[]);
    for index in [0, 2, 7, 8, 9] {
        let mut malformed = valid.clone();
        malformed[index] = 0xff;
        assert_eq!(
            peek_connect_maximum_packet_size(&malformed),
            None,
            "index {index}"
        );
    }
    let mut v311 = valid.clone();
    v311[8] = 4;
    assert_eq!(peek_connect_maximum_packet_size(&v311), None);
    let mut noncanonical = vec![0x10, valid[1] | 0x80, 0];
    noncanonical.extend_from_slice(&valid[2..]);
    assert_eq!(peek_connect_maximum_packet_size(&noncanonical), None);
    assert_eq!(
        peek_connect_maximum_packet_size(&[0x10, 0xff, 0xff, 0xff, 0xff]),
        None
    );
}

#[test]
fn wide_lengths_reuse_property_validation() {
    let mut properties = vec![39, 0xff, 0xff, 0xff, 0xff, 38, 0, 120];
    properties.extend_from_slice(&[b'x'; 120]);
    properties.extend_from_slice(&[0, 1, b'y']);
    let packet = packet(&properties, &[]);
    assert_ne!(packet[1] & 128, 0);
    assert_eq!(peek_connect_maximum_packet_size(&packet), Some(u32::MAX));
    for end in 0..packet.len() {
        assert_eq!(peek_connect_maximum_packet_size(&packet[..end]), None);
    }
}

#[test]
fn malformed_later_payload_does_not_change_codec_error() {
    let packet = packet(&[39, 0, 0, 0, 1], &[0, 1, 0xff]);
    assert_eq!(peek_connect_maximum_packet_size(&packet), Some(1));
    assert!(decode_frame(&packet, 1024, Some(ProtocolVersion::V5)).is_err());
    let mut will = self::packet(&[39, 0, 0, 0, 1], &[0, 0, 0, 0, 1, 0xff, 0, 0]);
    will[9] = 6;
    assert_eq!(peek_connect_maximum_packet_size(&will), Some(1));
    assert!(decode_frame(&will, 1024, Some(ProtocolVersion::V5)).is_err());
}

#[test]
fn assigned_property_is_nonempty_connack_only() {
    let mut response = Frame::v5(Packet::ConnAck {
        session_present: false,
        code: 0,
    });
    response.properties = vec![Property::AssignedClientIdentifier(String::new())];
    assert!(encode_frame(&response).is_err());
    response.properties = vec![Property::AssignedClientIdentifier("new".into()); 2];
    assert!(encode_frame(&response).is_err());
    assert!(decode_frame(&packet(&[18, 0, 1, b'x'], &[0, 0]), 1024, None).is_err());
}
