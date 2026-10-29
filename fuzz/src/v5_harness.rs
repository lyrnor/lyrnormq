//! Version5 model trace. Network-only aliases and provider AUTH are separate gates.
use mqtt_protocol::{Frame, Packet, Properties, Property, ProtocolVersion};
use mqtt_session::durable::{Clock, ConnectionOptions, EngineError, SessionConfig, SessionEngine};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Debug)]
struct TraceClock(AtomicU64);
impl Clock for TraceClock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}
fn limits() -> SessionConfig {
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
fn connect(
    engine: &mut SessionEngine,
    client: &str,
    clean: bool,
    receive: u16,
) -> Result<(), EngineError> {
    engine
        .connect_with_options(
            client,
            None,
            mqtt_auth::Principal::anonymous(),
            ConnectionOptions {
                version: ProtocolVersion::V5,
                clean_start: clean,
                session_expiry_interval: 60,
                receive_maximum: receive,
                maximum_packet_size: 256,
                will_properties: Vec::new(),
            },
        )
        .map(|_| ())
}
fn recovered(bytes: &[u8], clock: Arc<TraceClock>) -> Result<SessionEngine, EngineError> {
    SessionEngine::restore_with_clock(limits(), bytes, Arc::new(mqtt_auth::AllowAnonymous), clock)
}
/// Successful model proposals are committed only after bounded serialization.
/// Recovery has an allowed first normalization (active→offline, expiry); afterwards
/// repeated recovery at the same time must be an exact serialized fixed point.
pub fn session_v5(data: &[u8]) -> (usize, usize) {
    let mut committed = 0;
    let mut started_qos2 = 0;
    let clock = Arc::new(TraceClock(AtomicU64::new(1000)));
    let mut model =
        SessionEngine::new_with_clock(limits(), Arc::new(mqtt_auth::AllowAnonymous), clock.clone())
            .expect("fixed V5 limits");
    connect(&mut model, "source", false, 1).expect("V5 source");
    connect(&mut model, "target", false, 1).expect("V5 target");
    for chunk in data[..data.len().min(crate::MAX_INPUT)].chunks(3).take(32) {
        let b = chunk[0];
        let extra = chunk.get(2).copied().unwrap_or(0);
        let id = u16::from(chunk.get(1).copied().unwrap_or(0)) + 1;
        let client = if b & 1 == 0 { "source" } else { "target" };
        let mut proposal = model.clone();
        let mut proposed_qos2 = 0;
        let operation = (b >> 2) % 14;
        let result: Result<(), EngineError> = (|| match operation {
            9 => {
                if let Some(delivery) = proposal.poll_delivery_with_changes(client)?.delivery {
                    let result = proposal.begin_delivery(client, delivery)?;
                    if matches!(
                        result.frame,
                        Some(Frame {
                            packet: Packet::Publish { qos: 2, .. },
                            ..
                        })
                    ) {
                        proposed_qos2 += 1;
                    }
                }
                Ok(())
            }
            10 => {
                clock.0.fetch_add(u64::from(extra) * 100, Ordering::Relaxed);
                proposal.poll_delivery_with_changes(client).map(|_| ())
            }
            11 => connect(
                &mut proposal,
                client,
                extra & 1 != 0,
                u16::from(extra % 4) + 1,
            ),
            _ => {
                let qos = (b >> 2) % 3;
                let packet = match operation {
                    0 => Packet::Subscribe {
                        packet_id: id,
                        filters: vec![(
                            if extra & 128 != 0 { "a/+" } else { "#" }.into(),
                            extra % 3,
                        )],
                    },
                    1 => Packet::Unsubscribe {
                        packet_id: id,
                        filters: vec!["#".into()],
                    },
                    2..=4 => Packet::Publish {
                        topic: "a/b".into(),
                        payload: chunk.to_vec(),
                        qos,
                        packet_id: (qos != 0).then_some(id),
                        dup: b & 128 != 0 && qos != 0,
                        retain: b & 64 != 0,
                    },
                    5 => Packet::PubRel { packet_id: id },
                    6 => Packet::PubAck { packet_id: id },
                    7 => Packet::PubRec { packet_id: id },
                    8 => Packet::PubComp { packet_id: id },
                    12 => Packet::Disconnect,
                    _ => Packet::PingReq,
                };
                let mut frame = Frame::v5(packet);
                if operation == 0 {
                    frame
                        .properties
                        .push(Property::SubscriptionIdentifier(u32::from(id)));
                    frame.subscription_options.push(
                        (extra % 3)
                            | (if extra & 8 != 0 { 8 } else { 0 })
                            | (if extra & 4 != 0 { 4 } else { 0 })
                            | (((extra >> 4) % 3) << 4),
                    );
                }
                if (2..=4).contains(&operation) {
                    frame.properties = Properties::from([
                        Property::MessageExpiryInterval(u32::from(extra % 3)),
                        Property::UserProperty("trace".into(), id.to_string()),
                    ]);
                }
                if operation == 12 {
                    frame
                        .properties
                        .push(Property::SessionExpiryInterval(u32::from(extra % 3)));
                }
                proposal.handle_frame(client, frame).map(|_| ())
            }
        })();
        if result.is_ok() && proposal.snapshot().is_ok() {
            model = proposal;
            committed += 1;
            started_qos2 += proposed_qos2;
        }
        let bytes = model.snapshot().expect("bounded V5 committed snapshot");
        let first = recovered(&bytes, clock.clone()).expect("V5 committed recovery");
        let normalized = first.snapshot().expect("first normalized snapshot");
        let second = recovered(&normalized, clock.clone()).expect("V5 second recovery");
        assert_eq!(
            second.snapshot().expect("second normalized snapshot"),
            normalized,
            "V5 recovery fixed point at a fixed clock"
        );
    }
    (committed, started_qos2)
}
