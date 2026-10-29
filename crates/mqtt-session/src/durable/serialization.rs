//! Borrowed schema7 projections and exact bounded output/counting writers.
//!
//! No session state is shared between proposals. Only the temporary serialization
//! projection borrows the current immutable proposal; owned schema records remain
//! the decode representation and an independent test oracle.
use super::{EngineError, Session, SessionEngine, invalid_record};
use crate::{
    outgoing::OutgoingState,
    profiling::{self, Operation},
};
use mqtt_protocol::Property;
use mqtt_router::retained::RetainedMessage;
use serde::{
    Serialize, Serializer,
    ser::{Error as _, SerializeSeq},
};
use std::{
    collections::BTreeMap,
    io::{self, Write},
};

#[cfg(test)]
mod oracle;
#[cfg(test)]
mod tests;

struct LimitedWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        check_write(self.bytes.len(), bytes.len(), self.limit)?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct CountingWriter {
    count: usize,
    limit: usize,
}
impl Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        check_write(self.count, bytes.len(), self.limit)?;
        self.count += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn check_write(written: usize, next: usize, limit: usize) -> io::Result<()> {
    if next > limit.saturating_sub(written) {
        return Err(io::Error::other("snapshot byte limit exceeded"));
    }
    Ok(())
}
fn json_error(error: serde_json::Error) -> EngineError {
    if error.is_io() {
        EngineError::Capacity
    } else {
        EngineError::Json(error)
    }
}

pub(super) fn encode(
    engine: &SessionEngine,
    persistent_only: bool,
) -> Result<Vec<u8>, EngineError> {
    let operation = if persistent_only {
        Operation::SerializePersistent
    } else {
        Operation::SerializeLive
    };
    let _span = profiling::span(operation);
    let projection = Projection::new(engine, persistent_only)?;
    let mut writer = LimitedWriter {
        bytes: Vec::new(),
        limit: engine.config.max_state_bytes,
    };
    let result = serde_json::to_writer(&mut writer, &projection).map_err(json_error);
    profiling::record_bytes(operation, writer.bytes.len());
    result?;
    Ok(writer.bytes)
}
pub(super) fn check_live_bounds(engine: &SessionEngine) -> Result<(), EngineError> {
    let operation = Operation::SerializeLive;
    let _span = profiling::span(operation);
    let projection = Projection::new(engine, false)?;
    let mut writer = CountingWriter {
        count: 0,
        limit: engine.config.max_state_bytes,
    };
    let result = serde_json::to_writer(&mut writer, &projection).map_err(json_error);
    profiling::record_bytes(operation, writer.count);
    result
}

#[derive(Serialize)]
struct Projection<'a> {
    schema_version: u32,
    clock_floor: u64,
    sessions: Sessions<'a>,
    retained: Retained<'a>,
    wills: Wills<'a>,
}
impl<'a> Projection<'a> {
    fn new(engine: &'a SessionEngine, persistent_only: bool) -> Result<Self, EngineError> {
        let _span = profiling::span(if persistent_only {
            Operation::RecordsPersistent
        } else {
            Operation::RecordsLive
        });
        // The owned implementation checks every selected order reference before
        // writing even its JSON prefix. Retain InvalidRecord precedence over a
        // smaller byte limit or a lazy Serde error.
        for session in engine
            .sessions
            .values()
            .filter(|s| !persistent_only || s.persistent)
        {
            for id in &session.outgoing.order {
                if !session.outgoing.pending.contains_key(id) {
                    return Err(invalid_record("outgoing order references absent exchange"));
                }
            }
        }
        let clock_floor = engine.clock_floor.get();
        let retained = engine.retained.entries();
        Ok(Self {
            schema_version: 7,
            clock_floor,
            sessions: Sessions {
                engine,
                persistent_only,
            },
            retained: Retained {
                engine,
                entries: retained,
            },
            wills: Wills(&engine.wills),
        })
    }
}
struct Sessions<'a> {
    engine: &'a SessionEngine,
    persistent_only: bool,
}
impl Serialize for Sessions<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(None)?;
        for (client, session) in &self.engine.sessions {
            if self.persistent_only && !session.persistent {
                continue;
            }
            seq.serialize_element(&SessionView {
                owner: &session.owner.name,
                client_id: client,
                session_expiry_interval: session.expiry,
                expires_at: session.expires_at,
                subscriptions: Subscriptions(&session.subscriptions),
                incoming: Incoming(session),
                outgoing: Outgoing(session),
                queued: &session.queued,
            })?;
        }
        seq.end()
    }
}
#[derive(Serialize)]
struct SessionView<'a> {
    owner: &'a str,
    client_id: &'a str,
    session_expiry_interval: u32,
    expires_at: Option<u64>,
    subscriptions: Subscriptions<'a>,
    incoming: Incoming<'a>,
    outgoing: Outgoing<'a>,
    queued: &'a std::collections::VecDeque<super::records::Message>,
}
struct Subscriptions<'a>(&'a BTreeMap<String, super::records::SubscriptionRecord>);
impl Serialize for Subscriptions<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for subscription in self.0.values() {
            seq.serialize_element(subscription)?;
        }
        seq.end()
    }
}
struct Incoming<'a>(&'a Session);
impl Serialize for Incoming<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.incoming.pending.len()))?;
        for (id, bytes) in &self.0.incoming.pending {
            let properties = self
                .0
                .incoming_properties
                .get(id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            seq.serialize_element(&(
                id.get(),
                bytes,
                properties,
                self.0.incoming_expiry.get(id).copied(),
            ))?;
        }
        seq.end()
    }
}
struct Outgoing<'a>(&'a Session);
impl Serialize for Outgoing<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.outgoing.order.len()))?;
        for id in &self.0.outgoing.order {
            let stage = match self.0.outgoing.pending.get(id) {
                Some(OutgoingState::AwaitingAck { _message }) => StageView::Ack(_message),
                Some(OutgoingState::AwaitingReceipt { _message }) => StageView::Receipt(_message),
                Some(OutgoingState::Released) => StageView::Comp,
                // Projection::new preflight makes this unreachable for an
                // immutable engine; never unwrap if that invariant changes.
                None => {
                    return Err(S::Error::custom(
                        "outgoing order references absent exchange",
                    ));
                }
            };
            seq.serialize_element(&OutgoingView {
                packet_id: id.get(),
                stage,
                expires_at: self.0.outgoing_expiry.get(id).copied(),
                started: self.0.outgoing_started.contains(id),
                retained_replay: self.0.outgoing_retained_replay.contains(id),
                shared_subscription: self.0.outgoing_shared.get(id).map(String::as_str),
                properties: self
                    .0
                    .outgoing_properties
                    .get(id)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
            })?;
        }
        seq.end()
    }
}
#[derive(Serialize)]
enum StageView<'a> {
    Ack(&'a [u8]),
    Receipt(&'a [u8]),
    Comp,
}
#[derive(Serialize)]
struct OutgoingView<'a> {
    packet_id: u16,
    stage: StageView<'a>,
    expires_at: Option<u64>,
    started: bool,
    retained_replay: bool,
    shared_subscription: Option<&'a str>,
    properties: &'a [Property],
}
struct Retained<'a> {
    engine: &'a SessionEngine,
    entries: Vec<RetainedMessage>,
}
impl Serialize for Retained<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.entries.len()))?;
        for message in &self.entries {
            seq.serialize_element(&RetainedView {
                topic: &message.topic,
                payload: &message.payload,
                qos: message.qos,
                expires_at: self.engine.retained_expiry.get(&message.topic).copied(),
                properties: self
                    .engine
                    .retained_properties
                    .get(&message.topic)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
            })?;
        }
        seq.end()
    }
}
#[derive(Serialize)]
struct RetainedView<'a> {
    topic: &'a str,
    payload: &'a [u8],
    qos: u8,
    expires_at: Option<u64>,
    properties: &'a [Property],
}
struct Wills<'a>(&'a BTreeMap<String, super::records::WillRecord>);
impl Serialize for Wills<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for will in self.0.values() {
            seq.serialize_element(will)?;
        }
        seq.end()
    }
}
