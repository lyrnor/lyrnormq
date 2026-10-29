//! Independent pre-optimization owned record/Vec serializer, test-only.
use super::super::records::{Envelope, OutgoingRecord, RetainedRecord, SessionRecord};
use super::super::*;
use std::io::{self, Write};

struct LimitedWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("snapshot byte limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn records(engine: &SessionEngine, persistent_only: bool) -> Result<Envelope, EngineError> {
    let mut records = Vec::new();
    for (client_id, session) in &engine.sessions {
        if persistent_only && !session.persistent {
            continue;
        }
        let mut outgoing = Vec::new();
        for id in &session.outgoing.order {
            let stage = match session.outgoing.pending.get(id) {
                Some(OutgoingState::AwaitingAck { _message }) => Stage::Ack(_message.clone()),
                Some(OutgoingState::AwaitingReceipt { _message }) => {
                    Stage::Receipt(_message.clone())
                }
                Some(OutgoingState::Released) => Stage::Comp,
                None => {
                    return Err(invalid_record("outgoing order references absent exchange"));
                }
            };
            outgoing.push(OutgoingRecord {
                packet_id: id.get(),
                expires_at: session.outgoing_expiry.get(id).copied(),
                started: session.outgoing_started.contains(id),
                shared_subscription: session.outgoing_shared.get(id).cloned(),
                retained_replay: session.outgoing_retained_replay.contains(id),
                stage,
                properties: session
                    .outgoing_properties
                    .get(id)
                    .cloned()
                    .unwrap_or_default(),
            });
        }
        records.push(SessionRecord {
            client_id: client_id.clone(),
            owner: session.owner.name.clone(),
            session_expiry_interval: session.expiry,
            expires_at: session.expires_at,
            subscriptions: session.subscriptions.values().cloned().collect(),
            incoming: session
                .incoming
                .pending
                .iter()
                .map(|(id, bytes)| {
                    (
                        id.get(),
                        bytes.clone(),
                        session
                            .incoming_properties
                            .get(id)
                            .cloned()
                            .unwrap_or_default(),
                        session.incoming_expiry.get(id).copied(),
                    )
                })
                .collect(),
            outgoing,
            queued: session.queued.iter().cloned().collect(),
        });
    }
    Ok(Envelope {
        schema_version: 7,
        clock_floor: engine.clock_floor.get(),
        sessions: records,
        retained: engine
            .retained
            .entries()
            .into_iter()
            .map(|message| RetainedRecord {
                expires_at: engine.retained_expiry.get(&message.topic).copied(),
                topic: message.topic.clone(),
                payload: message.payload,
                qos: message.qos,
                properties: engine
                    .retained_properties
                    .get(&message.topic)
                    .cloned()
                    .unwrap_or_default(),
            })
            .collect(),
        wills: engine.wills.values().cloned().collect(),
    })
}
pub(super) fn serialize(
    engine: &SessionEngine,
    persistent_only: bool,
) -> Result<Vec<u8>, EngineError> {
    let mut writer = LimitedWriter {
        bytes: Vec::new(),
        limit: engine.config.max_state_bytes,
    };
    let result =
        serde_json::to_writer(&mut writer, &records(engine, persistent_only)?).map_err(|error| {
            if error.is_io() {
                EngineError::Capacity
            } else {
                EngineError::Json(error)
            }
        });
    result?;
    Ok(writer.bytes)
}

pub(super) fn bounds(engine: &SessionEngine) -> Result<(), EngineError> {
    for session in engine.sessions.values() {
        if session
            .incoming_expiry
            .keys()
            .any(|id| !session.incoming.pending.contains_key(id))
            || session
                .outgoing_expiry
                .keys()
                .chain(session.outgoing_started.iter())
                .chain(session.outgoing_retained_replay.iter())
                .chain(session.outgoing_shared.keys())
                .chain(session.delivery_tickets.keys())
                .any(|id| !session.outgoing.pending.contains_key(id))
        {
            return Err(invalid_record("orphan delivery deadline or reservation"));
        }
    }
    serialize(engine, false).map(|_| ())
}
pub(super) fn snapshot(engine: &SessionEngine) -> Result<Vec<u8>, EngineError> {
    bounds(engine)?;
    serialize(engine, true)
}
