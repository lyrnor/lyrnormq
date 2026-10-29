//! Bounded fuzz entry points. Assertions are test oracles, not production policy.
use mqtt_protocol::{Frame, Packet, ProtocolVersion, decode_frame, encode_frame};
use mqtt_session::durable::{Clock, SessionConfig, SessionEngine};
use std::sync::Arc;

pub const MAX_INPUT: usize = 4096;
#[derive(Debug)]
struct FixedClock;
impl Clock for FixedClock {
    fn now_millis(&self) -> u64 {
        1000
    }
}
fn config() -> SessionConfig {
    SessionConfig {
        max_sessions: 4,
        max_retained: 8,
        max_subscriptions: 8,
        max_queued: 8,
        max_inflight: 4,
        max_packet_size: 256,
        max_state_bytes: 65536,
    }
}
fn engine() -> SessionEngine {
    SessionEngine::new_with_clock(
        config(),
        Arc::new(mqtt_auth::AllowAnonymous),
        Arc::new(FixedClock),
    )
    .expect("fixed valid fuzz limits")
}
fn restore(bytes: &[u8]) -> Result<SessionEngine, mqtt_session::durable::EngineError> {
    SessionEngine::restore_with_clock(
        config(),
        bytes,
        Arc::new(mqtt_auth::AllowAnonymous),
        Arc::new(FixedClock),
    )
}
/// Exercise parser limits, coalesced input, short reads and canonical re-encoding.
pub fn codec(data: &[u8]) {
    let input = &data[..data.len().min(MAX_INPUT)];
    for version in [None, Some(ProtocolVersion::V311), Some(ProtocolVersion::V5)] {
        for limit in [2, 16, 256, MAX_INPUT] {
            let result = decode_frame(input, limit, version);
            assert_eq!(
                result,
                decode_frame(input, limit, version),
                "deterministic decoder"
            );
            if let Ok(Some((frame, consumed))) = result {
                assert!((2..=input.len()).contains(&consumed));
                assert!(consumed <= limit);
                let canonical = encode_frame(&frame).expect("decoded frame must be encodable");
                let (again, size) = decode_frame(&canonical, MAX_INPUT * 2, Some(frame.version))
                    .expect("canonical decode")
                    .expect("complete canonical packet");
                assert_eq!(size, canonical.len());
                assert_eq!(again, frame, "semantic round trip");
                assert_eq!(encode_frame(&again).expect("canonical encode"), canonical);
                let mut joined = canonical.clone();
                joined.extend_from_slice(&canonical);
                let (first, used) = decode_frame(&joined, MAX_INPUT * 2, Some(frame.version))
                    .expect("coalesced decode")
                    .expect("coalesced first packet");
                assert_eq!(first, frame);
                assert_eq!(used, canonical.len(), "coalesced packet boundary");
                // Prefixes of a validated packet cannot produce an earlier packet.
                for cut in [0, 1, consumed / 2, consumed.saturating_sub(1)] {
                    assert!(!matches!(
                        decode_frame(&input[..cut], limit, version),
                        Ok(Some(_))
                    ));
                }
            }
        }
    }
}
/// Mutate serialized state, or apply a bounded generated trace using clone/commit.
/// Rejected proposals never replace the authoritative engine.
pub fn session(data: &[u8]) {
    let input = &data[..data.len().min(MAX_INPUT)];
    if let Ok(value) = restore(input) {
        let bytes = value.snapshot().expect("restored model bounded");
        assert!(bytes.len() <= config().max_state_bytes);
        assert!(
            restore(&bytes).is_ok(),
            "restored state re-serializes validly"
        );
    }
    let mut model = engine();
    model.connect("source", false).expect("fresh source");
    model.connect("target", false).expect("fresh target");
    for (index, chunk) in input.chunks(3).take(64).enumerate() {
        let b = chunk[0];
        let id = u16::from(chunk.get(1).copied().unwrap_or(1)).saturating_add(1);
        let name = match b & 3 {
            0 => "a",
            1 => "a/b",
            2 => "$SYS/a",
            _ => "/",
        };
        let packet = match (b >> 2) % 12 {
            0 => Packet::Subscribe {
                packet_id: id,
                filters: vec![("#".into(), b % 3)],
            },
            1 => Packet::Unsubscribe {
                packet_id: id,
                filters: vec!["#".into()],
            },
            2..=4 => {
                let qos = (b >> 2) % 3;
                Packet::Publish {
                    topic: name.into(),
                    payload: chunk.to_vec(),
                    qos,
                    packet_id: (qos != 0).then_some(id),
                    dup: b & 128 != 0 && qos != 0,
                    retain: b & 64 != 0,
                }
            }
            5 => Packet::PubRel { packet_id: id },
            6 => Packet::PubAck { packet_id: id },
            7 => Packet::PubRec { packet_id: id },
            8 => Packet::PubComp { packet_id: id },
            _ => Packet::PingReq,
        };
        let client = if b & 1 == 0 { "source" } else { "target" };
        let before = model.snapshot().expect("authoritative bounded");
        let mut proposal = model.clone();
        let operation = match (b >> 2) % 12 {
            9 => proposal.poll(client).map(|_| Vec::new()),
            10 => proposal.disconnect(client).map(|_| Vec::new()),
            _ => proposal.handle(client, packet),
        };
        if operation.is_ok() && proposal.snapshot().is_ok() {
            model = proposal;
        } else {
            assert_eq!(before, model.snapshot().expect("failed proposal discarded"));
        }
        let bytes = model.snapshot().expect("committed bounded");
        assert!(bytes.len() <= config().max_state_bytes);
        let recovered = restore(&bytes).expect("committed state recoverable");
        if index % 7 == 0 {
            // Restore marks sessions offline; reconnect is itself a new proposal.
            let mut next = recovered;
            if next.connect("source", false).is_ok()
                && next.connect("target", false).is_ok()
                && next.snapshot().is_ok()
            {
                model = next;
            }
        }
    }
}
/// Small valid seeds span every packet kind and both wire versions.
pub fn seeds() -> Vec<Vec<u8>> {
    let mut packets = vec![
        Packet::Connect {
            client_id: "fuzz".into(),
            clean_session: true,
            keep_alive: 0,
            will: None,
            username: None,
            password: None,
        },
        Packet::ConnAck {
            session_present: false,
            code: 0,
        },
        Packet::Publish {
            topic: "a".into(),
            payload: vec![0, 255],
            qos: 0,
            packet_id: None,
            dup: false,
            retain: true,
        },
        Packet::PubAck { packet_id: 1 },
        Packet::PubRec { packet_id: 1 },
        Packet::PubRel { packet_id: 1 },
        Packet::PubComp { packet_id: 1 },
        Packet::Subscribe {
            packet_id: 1,
            filters: vec![("a/#".into(), 2)],
        },
        Packet::SubAck {
            packet_id: 1,
            codes: vec![2],
        },
        Packet::Unsubscribe {
            packet_id: 1,
            filters: vec!["a/#".into()],
        },
        Packet::UnsubAck { packet_id: 1 },
        Packet::PingReq,
        Packet::PingResp,
        Packet::Disconnect,
    ];
    for qos in [1, 2] {
        packets.push(Packet::Publish {
            topic: "a/b".into(),
            payload: vec![b'x'; 140],
            qos,
            packet_id: Some(65535),
            dup: true,
            retain: false,
        });
    }
    // Each triple drives an operation/client/id. These are valid Q1 and Q2
    // receive/forward/ACK traces, in addition to individual packet seeds.
    let mut seeds = vec![
        // V5 source subscriber has RAP0 and RAP1; retained incoming Q2 creates
        // two outgoing Q2 exchanges under ReceiveMaximum1, each completed.
        vec![
            2, 0, 2, 2, 1, 137, 69, 6, 1, 21, 6, 0, 36, 0, 0, 28, 0, 0, 32, 0, 0, 36, 0, 0, 28, 1,
            0, 32, 1, 0,
        ],
        vec![1, 0, 0, 16, 6, 0, 37, 0, 0, 25, 0, 0],
        vec![2, 0, 0, 9, 6, 0, 21, 6, 0, 36, 0, 0, 28, 0, 0, 32, 0, 0],
    ];
    for p in packets {
        for mut frame in [Frame::v311(p.clone()), Frame::v5(p)] {
            if frame.version == ProtocolVersion::V5
                && matches!(frame.packet, Packet::UnsubAck { .. })
            {
                frame.reason_codes.push(mqtt_protocol::ReasonCode::SUCCESS);
            }
            seeds.push(encode_frame(&frame).expect("valid per-version packet seed"));
        }
    }
    let mut auth = Frame::v5(Packet::Auth);
    auth.properties
        .push(mqtt_protocol::Property::AuthenticationMethod(
            "PLAIN".into(),
        ));
    seeds.push(encode_frame(&auth).expect("valid AUTH seed"));
    seeds.push(engine().snapshot().expect("valid snapshot seed"));
    let mut model = engine();
    model.connect("source", false).expect("seed source");
    model.connect("target", false).expect("seed target");
    model
        .handle(
            "target",
            Packet::Subscribe {
                packet_id: 1,
                filters: vec![("#".into(), 2)],
            },
        )
        .expect("seed subscription");
    model
        .handle(
            "source",
            Packet::Publish {
                topic: "a".into(),
                payload: vec![b'x'],
                qos: 2,
                packet_id: Some(7),
                dup: false,
                retain: true,
            },
        )
        .expect("seed incoming");
    seeds.push(model.snapshot().expect("incoming receipt seed"));
    model
        .handle("source", Packet::PubRel { packet_id: 7 })
        .expect("seed release");
    seeds.push(model.snapshot().expect("queued seed"));
    let outgoing = model
        .poll("target")
        .expect("seed poll")
        .expect("seed delivery");
    seeds.push(model.snapshot().expect("outgoing seed"));
    if let Packet::Publish {
        packet_id: Some(id),
        ..
    } = outgoing
    {
        model
            .handle("target", Packet::PubRec { packet_id: id })
            .expect("seed outgoing receipt");
        seeds.push(model.snapshot().expect("pubrel seed"));
    }
    seeds
}
