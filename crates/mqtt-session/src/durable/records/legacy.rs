//! Explicit schema1/schema2 migration; schema3 binds each session and Will owner.
use super::{EngineError, invalid_record};
use mqtt_auth::Principal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Message {
    pub(super) topic: String,
    pub(super) payload: Vec<u8>,
    pub(super) qos: u8,
    pub(super) retain: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Envelope {
    pub(super) schema_version: u32,
    pub(super) sessions: Vec<SessionRecord>,
    pub(super) retained: Vec<RetainedRecord>,
    pub(super) wills: Vec<WillRecord>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SessionRecord {
    pub(super) owner: String,
    pub(super) client_id: String,
    pub(super) subscriptions: Vec<(String, u8)>,
    pub(super) incoming: Vec<(u16, Vec<u8>)>,
    pub(super) outgoing: Vec<OutgoingRecord>,
    pub(super) queued: Vec<Message>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OutgoingRecord {
    pub(super) packet_id: u16,
    pub(super) stage: Stage,
}
#[derive(Serialize, Deserialize)]
pub(super) enum Stage {
    Ack(Vec<u8>),
    Receipt(Vec<u8>),
    Comp,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetainedRecord {
    pub(super) topic: String,
    pub(super) payload: Vec<u8>,
    pub(super) qos: u8,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WillRecord {
    pub(super) owner: String,
    pub(super) client_id: String,
    pub(super) pending: bool,
    pub(super) will: WillData,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WillData {
    pub(super) topic: String,
    pub(super) payload: Vec<u8>,
    pub(super) qos: u8,
    pub(super) retain: bool,
}
#[derive(Deserialize)]
struct Version {
    schema_version: u32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyEnvelope {
    schema_version: u32,
    sessions: Vec<LegacySession>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacySession {
    client_id: String,
    subscriptions: Vec<(String, u8)>,
    incoming: Vec<(u16, Vec<u8>)>,
    outgoing: Vec<OutgoingRecord>,
    queued: Vec<LegacyMessage>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyMessage {
    topic: String,
    payload: Vec<u8>,
    qos: u8,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UnownedEnvelope {
    schema_version: u32,
    sessions: Vec<UnownedSession>,
    retained: Vec<RetainedRecord>,
    wills: Vec<UnownedWill>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UnownedSession {
    client_id: String,
    subscriptions: Vec<(String, u8)>,
    incoming: Vec<(u16, Vec<u8>)>,
    outgoing: Vec<OutgoingRecord>,
    queued: Vec<Message>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UnownedWill {
    client_id: String,
    pending: bool,
    will: WillData,
}

pub(super) fn decode(bytes: &[u8]) -> Result<(Envelope, bool), EngineError> {
    let version: Version = serde_json::from_slice(bytes).map_err(EngineError::Json)?;
    match version.schema_version {
        1 => {
            let old: LegacyEnvelope = serde_json::from_slice(bytes).map_err(EngineError::Json)?;
            if old.schema_version != 1 {
                return Err(invalid_record("legacy schema version"));
            }
            Ok((
                Envelope {
                    schema_version: 3,
                    sessions: old
                        .sessions
                        .into_iter()
                        .map(|session| SessionRecord {
                            owner: Principal::anonymous().name,
                            client_id: session.client_id,
                            subscriptions: session.subscriptions,
                            incoming: session.incoming,
                            outgoing: session.outgoing,
                            queued: session
                                .queued
                                .into_iter()
                                .map(|message| Message {
                                    topic: message.topic,
                                    payload: message.payload,
                                    qos: message.qos,
                                    retain: false,
                                })
                                .collect(),
                        })
                        .collect(),
                    retained: Vec::new(),
                    wills: Vec::new(),
                },
                true,
            ))
        }
        2 => {
            let old: UnownedEnvelope = serde_json::from_slice(bytes).map_err(EngineError::Json)?;
            if old.schema_version != 2 {
                return Err(invalid_record("unowned schema version"));
            }
            Ok((
                Envelope {
                    schema_version: 3,
                    sessions: old
                        .sessions
                        .into_iter()
                        .map(|session| SessionRecord {
                            owner: Principal::anonymous().name,
                            client_id: session.client_id,
                            subscriptions: session.subscriptions,
                            incoming: session.incoming,
                            outgoing: session.outgoing,
                            queued: session.queued,
                        })
                        .collect(),
                    retained: old.retained,
                    wills: old
                        .wills
                        .into_iter()
                        .map(|will| WillRecord {
                            owner: Principal::anonymous().name,
                            client_id: will.client_id,
                            pending: will.pending,
                            will: will.will,
                        })
                        .collect(),
                },
                false,
            ))
        }
        3 => Ok((
            serde_json::from_slice(bytes).map_err(EngineError::Json)?,
            false,
        )),
        _ => Err(invalid_record("unsupported schema version")),
    }
}
