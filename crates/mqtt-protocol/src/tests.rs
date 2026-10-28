use super::*;
fn packets() -> Vec<Packet> {
    vec![
        Packet::Connect {
            client_id: "client-1".into(),
            clean_session: true,
            keep_alive: 30,
            will: None,
        },
        Packet::ConnAck {
            session_present: false,
            code: 0,
        },
        Packet::Publish {
            topic: "sensors/温度".into(),
            payload: vec![0, 255, 1],
            qos: 0,
            packet_id: None,
            dup: false,
            retain: false,
        },
        Packet::Publish {
            topic: "qos/one".into(),
            payload: vec![4, 2],
            qos: 1,
            packet_id: Some(42),
            dup: true,
            retain: false,
        },
        Packet::Publish {
            topic: "qos/two".into(),
            payload: vec![],
            qos: 2,
            packet_id: Some(65535),
            dup: false,
            retain: false,
        },
        Packet::PubAck { packet_id: 1 },
        Packet::PubRec { packet_id: 2 },
        Packet::PubRel { packet_id: 3 },
        Packet::PubComp { packet_id: 4 },
        Packet::Subscribe {
            packet_id: 42,
            filters: vec![("a/+".into(), 2), ("#".into(), 1)],
        },
        Packet::SubAck {
            packet_id: 42,
            codes: vec![0, 128],
        },
        Packet::Unsubscribe {
            packet_id: 65535,
            filters: vec!["#".into()],
        },
        Packet::UnsubAck { packet_id: 65535 },
        Packet::PingReq,
        Packet::PingResp,
        Packet::Disconnect,
    ]
}
#[test]
fn all_packets_roundtrip_and_fragment() {
    for p in packets() {
        let bytes = encode(&p).unwrap();
        for n in 0..bytes.len() {
            assert_eq!(decode(&bytes[..n], 65536).unwrap(), None, "{p:?}, {n}");
        }
        assert_eq!(decode(&bytes, 65536).unwrap(), Some((p, bytes.len())));
    }
}
#[test]
fn concatenation_consumes_one_packet() {
    assert_eq!(
        decode(&[0xc0, 0, 0xe0, 0], 2).unwrap(),
        Some((Packet::PingReq, 2))
    );
}
#[test]
fn multibyte_remaining_length() {
    for n in [127, 128, 16383, 16384, 65536] {
        let p = Packet::Publish {
            topic: "a".into(),
            payload: vec![42; n],
            qos: 0,
            packet_id: None,
            dup: false,
            retain: false,
        };
        let bytes = encode(&p).unwrap();
        assert_eq!(decode(&bytes, bytes.len()).unwrap(), Some((p, bytes.len())));
        assert_eq!(
            decode(&bytes, bytes.len() - 1),
            Err(ProtocolError::PacketTooLarge)
        );
    }
}
#[test]
fn reject_malformed_headers_and_lengths() {
    for bytes in [
        vec![0, 0],
        vec![0xf0, 0],
        vec![0xc1, 0],
        vec![0x80, 0],
        vec![0x3e, 0],
        vec![0x38, 0],
        vec![0xc0, 1, 0],
        vec![0xc0, 128, 128, 128, 128],
    ] {
        assert!(
            matches!(decode(&bytes, 65536), Err(ProtocolError::Malformed(_))),
            "{bytes:?}"
        );
    }
    assert_eq!(
        decode(&[0x30, 255, 127], 1024),
        Err(ProtocolError::PacketTooLarge)
    );
    assert_eq!(decode(&[0x30, 128], 2), Err(ProtocolError::PacketTooLarge));
}
#[test]
fn reject_invalid_payloads() {
    for bytes in [
        vec![0x30, 2, 0, 0],
        vec![0x30, 3, 0, 1, b'+'],
        vec![0x30, 3, 0, 1, 0],
        vec![0x30, 3, 0, 1, 255],
        vec![0x82, 2, 0, 1],
        vec![0x82, 6, 0, 0, 0, 1, b'a', 0],
        vec![0x82, 6, 0, 1, 0, 1, b'a', 3],
        vec![0xa2, 2, 0, 1],
        vec![0xb0, 2, 0, 0],
        vec![0x90, 3, 0, 1, 3],
        vec![0x20, 2, 1, 1],
        vec![0x20, 2, 0, 6],
    ] {
        assert!(decode(&bytes, 65536).is_err(), "{bytes:?}");
    }
}
#[test]
fn connect_clean_and_persistent_flags_roundtrip_and_fragment() {
    for clean_session in [false, true] {
        let packet = Packet::Connect {
            client_id: "persistent-client".into(),
            clean_session,
            keep_alive: 45,
            will: None,
        };
        let bytes = encode(&packet).unwrap();
        assert_eq!(bytes[9], if clean_session { 2 } else { 0 });
        for cut in 0..bytes.len() {
            assert_eq!(decode(&bytes[..cut], 1024).unwrap(), None);
        }
        assert_eq!(decode(&bytes, 1024).unwrap(), Some((packet, bytes.len())));
        assert!(
            encode(&Packet::Connect {
                client_id: String::new(),
                clean_session,
                keep_alive: 45,
                will: None,
            })
            .is_err()
        );
        let empty_id = [
            0x10,
            12,
            0,
            4,
            b'M',
            b'Q',
            b'T',
            b'T',
            4,
            if clean_session { 2 } else { 0 },
            0,
            45,
            0,
            0,
        ];
        assert!(decode(&empty_id, 1024).is_err());
    }
}
#[test]
fn connect_rejections_and_version() {
    let p = Packet::Connect {
        client_id: "c".into(),
        clean_session: true,
        keep_alive: 0,
        will: None,
    };
    let base = encode(&p).unwrap();
    let mut b = base.clone();
    b[8] = 5;
    assert_eq!(decode(&b, 1024), Err(ProtocolError::UnsupportedVersion));
    for flags in [1, 0x1a, 0x22, 0x42] {
        let mut b = base.clone();
        b[9] = flags;
        assert!(matches!(decode(&b, 1024), Err(ProtocolError::Malformed(_))));
    }
    for flags in [0x82, 0xc2] {
        let mut b = base.clone();
        b[9] = flags;
        assert!(matches!(
            decode(&b, 1024),
            Err(ProtocolError::UnsupportedFeature(_))
        ));
    }
}
#[test]
fn will_flag_without_payload_is_malformed() {
    let mut bytes = encode(&Packet::Connect {
        client_id: "c".into(),
        clean_session: true,
        keep_alive: 0,
        will: None,
    })
    .unwrap();
    bytes[9] |= 4;
    assert!(matches!(
        decode(&bytes, 1024),
        Err(ProtocolError::Malformed(_))
    ));
}
#[test]
fn encoder_checks_invariants() {
    assert!(
        encode(&Packet::Publish {
            topic: "bad/#".into(),
            payload: vec![],
            qos: 0,
            packet_id: None,
            dup: false,
            retain: false,
        })
        .is_err()
    );
    assert!(
        encode(&Packet::Publish {
            topic: "\u{fdd0}".into(),
            payload: vec![],
            qos: 0,
            packet_id: None,
            dup: false,
            retain: false,
        })
        .is_err()
    );
    assert!(encode(&Packet::UnsubAck { packet_id: 0 }).is_err());
    assert!(
        encode(&Packet::Subscribe {
            packet_id: 1,
            filters: vec![]
        })
        .is_err()
    );
    assert!(
        encode(&Packet::Publish {
            topic: "a".repeat(65536),
            payload: vec![],
            qos: 0,
            packet_id: None,
            dup: false,
            retain: false,
        })
        .is_err()
    );
}
#[test]
fn arbitrary_short_inputs_do_not_panic() {
    for a in 0..=255 {
        for b in 0..=255 {
            let _ = decode(&[a, b], 4096);
        }
    }
}
#[test]
fn qos_publish_wire_fields() {
    for (header, qos, dup) in [
        (0x32, 1, false),
        (0x3a, 1, true),
        (0x34, 2, false),
        (0x3c, 2, true),
        (0x33, 1, false),
        (0x3b, 1, true),
        (0x35, 2, false),
        (0x3d, 2, true),
    ] {
        let bytes = [header, 6, 0, 1, b'a', 0, 42, 255];
        let packet = Packet::Publish {
            topic: "a".into(),
            payload: vec![255],
            qos,
            packet_id: Some(42),
            dup,
            retain: header & 1 != 0,
        };
        assert_eq!(decode(&bytes, 1024), Ok(Some((packet.clone(), 8))));
        assert_eq!(encode(&packet).unwrap(), bytes);
    }
    for header in [0x32, 0x34] {
        for body in [
            vec![0, 1, b'a'],
            vec![0, 1, b'a', 0],
            vec![0, 1, b'a', 0, 0],
        ] {
            let mut bytes = vec![header, body.len() as u8];
            bytes.extend(body);
            assert!(matches!(
                decode(&bytes, 1024),
                Err(ProtocolError::Malformed(_))
            ));
        }
    }
}
#[test]
fn acknowledgement_wire_and_validation() {
    for (header, packet) in [
        (0x40, Packet::PubAck { packet_id: 42 }),
        (0x50, Packet::PubRec { packet_id: 42 }),
        (0x62, Packet::PubRel { packet_id: 42 }),
        (0x70, Packet::PubComp { packet_id: 42 }),
    ] {
        assert_eq!(encode(&packet).unwrap(), [header, 2, 0, 42]);
        assert_eq!(decode(&[header, 2, 0, 42], 1024), Ok(Some((packet, 4))));
        for bytes in [
            vec![header, 2, 0, 0],
            vec![header, 1, 42],
            vec![header, 3, 0, 42, 0],
        ] {
            assert!(matches!(
                decode(&bytes, 1024),
                Err(ProtocolError::Malformed(_))
            ));
        }
    }
    // Retransmitted PUBREL has the same fixed flags, not a PUBLISH-style DUP bit.
    for header in [0x41, 0x51, 0x60, 0x6a, 0x71] {
        assert!(matches!(
            decode(&[header, 2, 0, 42], 1024),
            Err(ProtocolError::Malformed(_))
        ));
    }
    for packet in [
        Packet::PubAck { packet_id: 0 },
        Packet::PubRec { packet_id: 0 },
        Packet::PubRel { packet_id: 0 },
        Packet::PubComp { packet_id: 0 },
    ] {
        assert!(encode(&packet).is_err());
    }
}
#[test]
fn encoder_qos_combinations() {
    for (qos, packet_id, dup) in [
        (0, Some(1), false),
        (0, None, true),
        (1, None, false),
        (2, Some(0), false),
        (3, Some(1), false),
    ] {
        assert!(matches!(
            encode(&Packet::Publish {
                topic: "a".into(),
                payload: vec![],
                qos,
                packet_id,
                dup,
                retain: false
            }),
            Err(ProtocolError::Malformed(_))
        ));
    }
}
#[test]
fn encoder_metadata_validation_without_decoding() {
    let invalid = [
        Packet::Connect {
            client_id: "".into(),
            clean_session: true,
            keep_alive: 0,
            will: None,
        },
        Packet::Connect {
            client_id: "a\0".into(),
            clean_session: true,
            keep_alive: 0,
            will: None,
        },
        Packet::ConnAck {
            session_present: true,
            code: 1,
        },
        Packet::ConnAck {
            session_present: false,
            code: 6,
        },
        Packet::Subscribe {
            packet_id: 0,
            filters: vec![("a".into(), 0)],
        },
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![("a".into(), 3)],
        },
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![("".into(), 0)],
        },
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![("a\0".into(), 0)],
        },
        Packet::SubAck {
            packet_id: 0,
            codes: vec![0],
        },
        Packet::SubAck {
            packet_id: 1,
            codes: vec![],
        },
        Packet::SubAck {
            packet_id: 1,
            codes: vec![3],
        },
        Packet::Unsubscribe {
            packet_id: 0,
            filters: vec!["a".into()],
        },
        Packet::Unsubscribe {
            packet_id: 1,
            filters: vec![],
        },
        Packet::Unsubscribe {
            packet_id: 1,
            filters: vec!["".into()],
        },
        Packet::Unsubscribe {
            packet_id: 1,
            filters: vec!["a\0".into()],
        },
    ];
    for packet in invalid {
        assert!(encode(&packet).is_err(), "{packet:?}");
    }
}

#[test]
fn retained_publish_wire_roundtrip() {
    for qos in 0..=2 {
        let packet = Packet::Publish {
            topic: "retained/state".into(),
            payload: vec![0, 255, 42],
            qos,
            packet_id: if qos == 0 { None } else { Some(7) },
            dup: false,
            retain: true,
        };
        let wire = encode(&packet).unwrap();
        assert_eq!(wire[0] & 1, 1);
        assert_eq!(decode(&wire, 1024).unwrap(), Some((packet, wire.len())));
    }
}

#[test]
fn binary_will_connect_wire_is_supported() {
    let bytes = [
        0x10, 21, 0, 4, b'M', b'Q', b'T', b'T', 4, 0x2e, 0, 30, 0, 1, b'c', 0, 1, b't', 0, 3, 0xff,
        0, b'a',
    ];
    assert!(
        decode(&bytes, 1024).is_ok(),
        "binary Will must not be interpreted as UTF8"
    );
}

#[test]
fn will_roundtrip_flags_binary_payload_and_all_fragments() {
    for clean_session in [false, true] {
        for qos in 0..=2 {
            for retain in [false, true] {
                for payload in [vec![], vec![0, 0xff, 0x80], b"last status".to_vec()] {
                    let packet = Packet::Connect {
                        client_id: "will-client".into(),
                        clean_session,
                        keep_alive: 42,
                        will: Some(Will {
                            topic: "last/温度".into(),
                            payload,
                            qos,
                            retain,
                        }),
                    };
                    let wire = encode(&packet).unwrap();
                    assert_eq!(
                        wire[9],
                        (u8::from(clean_session) * 2) | 4 | (qos << 3) | (u8::from(retain) << 5)
                    );
                    for cut in 0..wire.len() {
                        assert_eq!(decode(&wire[..cut], wire.len()).unwrap(), None);
                    }
                    assert_eq!(
                        decode(&wire, wire.len()).unwrap(),
                        Some((packet, wire.len()))
                    );
                    assert_eq!(
                        decode(&wire, wire.len() - 1),
                        Err(ProtocolError::PacketTooLarge)
                    );
                }
            }
        }
    }
}

#[test]
fn will_wire_fields_and_u16_payload_boundaries() {
    let wire = [
        0x10, 21, 0, 4, b'M', b'Q', b'T', b'T', 4, 0x2e, 0, 30, 0, 1, b'c', 0, 1, b't', 0, 3, 0xff,
        0, b'a',
    ];
    let packet = Packet::Connect {
        client_id: "c".into(),
        clean_session: true,
        keep_alive: 30,
        will: Some(Will {
            topic: "t".into(),
            payload: vec![0xff, 0, b'a'],
            qos: 1,
            retain: true,
        }),
    };
    assert_eq!(
        decode(&wire, 1024).unwrap(),
        Some((packet.clone(), wire.len()))
    );
    assert_eq!(encode(&packet).unwrap(), wire);
    for size in [65535, 65536] {
        let packet = Packet::Connect {
            client_id: "large-will".into(),
            clean_session: false,
            keep_alive: 0,
            will: Some(Will {
                topic: "a".into(),
                payload: vec![255; size],
                qos: 2,
                retain: false,
            }),
        };
        if size == 65535 {
            let wire = encode(&packet).unwrap();
            assert_eq!(
                decode(&wire, wire.len()).unwrap(),
                Some((packet, wire.len()))
            );
        } else {
            assert!(matches!(encode(&packet), Err(ProtocolError::Malformed(_))));
        }
    }
}

#[test]
fn will_rejects_invalid_flags_topics_and_truncated_binary_fields() {
    let base = encode(&Packet::Connect {
        client_id: "c".into(),
        clean_session: true,
        keep_alive: 0,
        will: Some(Will {
            topic: "t".into(),
            payload: vec![0, 255],
            qos: 0,
            retain: false,
        }),
    })
    .unwrap();
    for flags in [0x07, 0x1e, 0x0a, 0x12, 0x22] {
        let mut wire = base.clone();
        wire[9] = flags;
        assert!(
            matches!(decode(&wire, 1024), Err(ProtocolError::Malformed(_))),
            "flags {flags}"
        );
    }
    // The complete frame declares a Will binary field longer than the bytes it contains.
    let mut wire = base.clone();
    wire[19] = 3;
    assert!(matches!(
        decode(&wire, 1024),
        Err(ProtocolError::Malformed(_))
    ));
    // Will topic is UTF8, unlike its payload, and cannot be empty or contain wildcards.
    for topic in ["", "a/+", "a/#", "a\0b", "\u{ffff}"] {
        let packet = Packet::Connect {
            client_id: "c".into(),
            clean_session: true,
            keep_alive: 0,
            will: Some(Will {
                topic: topic.into(),
                payload: vec![],
                qos: 0,
                retain: false,
            }),
        };
        assert!(matches!(encode(&packet), Err(ProtocolError::Malformed(_))));
    }
    let mut wire = base.clone();
    wire[17] = 0xff;
    assert!(matches!(
        decode(&wire, 1024),
        Err(ProtocolError::Malformed(_))
    ));
    let invalid = Packet::Connect {
        client_id: "c".into(),
        clean_session: true,
        keep_alive: 0,
        will: Some(Will {
            topic: "a".into(),
            payload: vec![],
            qos: 3,
            retain: false,
        }),
    };
    assert!(matches!(encode(&invalid), Err(ProtocolError::Malformed(_))));
}
