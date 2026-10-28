//! Strict schema5 message deadlines and delayed Will scheduling.
use super::{EngineError, invalid_record};
use mqtt_protocol::{Properties, Will};
use serde::{Deserialize, Serialize};
mod legacy;
mod v4;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Message {
    pub(super) topic: String,
    pub(super) payload: Vec<u8>,
    pub(super) qos: u8,
    pub(super) retain: bool,
    #[serde(deserialize_with = "required_deadline")]
    pub(super) expires_at: Option<u64>,
    pub(super) properties: Properties,
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
    pub(super) session_expiry_interval: u32,
    #[serde(deserialize_with = "required_deadline")]
    pub(super) expires_at: Option<u64>,
    pub(super) subscriptions: Vec<(String, u8)>,
    pub(super) incoming: Vec<(u16, Vec<u8>, Properties, Option<u64>)>,
    pub(super) outgoing: Vec<OutgoingRecord>,
    pub(super) queued: Vec<Message>,
}
// A missing Option would otherwise silently deserialize to None, which denotes
// active-at-crash and would incorrectly restart a finite offline deadline.
fn required_deadline<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<u64>, D::Error> {
    Option::<u64>::deserialize(deserializer)
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OutgoingRecord {
    pub(super) packet_id: u16,
    pub(super) stage: Stage,
    #[serde(deserialize_with = "required_deadline")]
    pub(super) expires_at: Option<u64>,
    pub(super) started: bool,
    pub(super) properties: Properties,
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
    #[serde(deserialize_with = "required_deadline")]
    pub(super) expires_at: Option<u64>,
    pub(super) properties: Properties,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WillRecord {
    pub(super) owner: String,
    pub(super) client_id: String,
    pub(super) pending: bool,
    #[serde(skip)]
    pub(super) timer_blocked: bool,
    pub(super) delay_interval: u32,
    #[serde(deserialize_with = "required_deadline")]
    pub(super) due_at: Option<u64>,
    pub(super) will: WillData,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WillData {
    pub(super) topic: String,
    pub(super) payload: Vec<u8>,
    pub(super) qos: u8,
    pub(super) retain: bool,
    pub(super) properties: Properties,
}
impl From<Will> for WillData {
    fn from(value: Will) -> Self {
        Self {
            topic: value.topic,
            payload: value.payload,
            qos: value.qos,
            retain: value.retain,
            properties: Vec::new(),
        }
    }
}
impl WillData {
    pub(super) fn to_protocol(&self) -> Will {
        Will {
            topic: self.topic.clone(),
            payload: self.payload.clone(),
            qos: self.qos,
            retain: self.retain,
        }
    }
}
pub(super) fn decode(bytes: &[u8]) -> Result<(Envelope, bool), EngineError> {
    #[derive(Deserialize)]
    struct Version {
        schema_version: u32,
    }
    let version: Version = serde_json::from_slice(bytes).map_err(EngineError::Json)?;
    if version.schema_version == 5 {
        return Ok((
            serde_json::from_slice(bytes).map_err(EngineError::Json)?,
            false,
        ));
    }
    if version.schema_version == 4 {
        return migrate_v4(bytes).map(|envelope| (envelope, false));
    }
    let (old, legacy) = legacy::decode(bytes)?;
    let map_message = |m: legacy::Message| Message {
        topic: m.topic,
        payload: m.payload,
        qos: m.qos,
        retain: m.retain,
        expires_at: None,
        properties: Vec::new(),
    };
    Ok((
        Envelope {
            schema_version: 5,
            sessions: old
                .sessions
                .into_iter()
                .map(|s| SessionRecord {
                    owner: s.owner,
                    client_id: s.client_id,
                    session_expiry_interval: u32::MAX,
                    expires_at: None,
                    subscriptions: s.subscriptions,
                    incoming: s
                        .incoming
                        .into_iter()
                        .map(|(id, bytes)| (id, bytes, Vec::new(), None))
                        .collect(),
                    outgoing: s
                        .outgoing
                        .into_iter()
                        .map(|o| OutgoingRecord {
                            packet_id: o.packet_id,
                            expires_at: None,
                            started: true,
                            properties: Vec::new(),
                            stage: match o.stage {
                                legacy::Stage::Ack(b) => Stage::Ack(b),
                                legacy::Stage::Receipt(b) => Stage::Receipt(b),
                                legacy::Stage::Comp => Stage::Comp,
                            },
                        })
                        .collect(),
                    queued: s.queued.into_iter().map(map_message).collect(),
                })
                .collect(),
            retained: old
                .retained
                .into_iter()
                .map(|r| RetainedRecord {
                    topic: r.topic,
                    payload: r.payload,
                    qos: r.qos,
                    expires_at: None,
                    properties: Vec::new(),
                })
                .collect(),
            wills: old
                .wills
                .into_iter()
                .map(|w| WillRecord {
                    owner: w.owner,
                    client_id: w.client_id,
                    pending: w.pending,
                    timer_blocked: false,
                    delay_interval: 0,
                    due_at: w.pending.then_some(0),
                    will: WillData {
                        topic: w.will.topic,
                        payload: w.will.payload,
                        qos: w.will.qos,
                        retain: w.will.retain,
                        properties: Vec::new(),
                    },
                })
                .collect(),
        },
        legacy,
    ))
}

fn migrate_v4(bytes: &[u8]) -> Result<Envelope, EngineError> {
    let old: v4::Envelope = serde_json::from_slice(bytes).map_err(EngineError::Json)?;
    if old.schema_version != 4 {
        return Err(invalid_record("wrong schema4 version"));
    }
    fn props(properties: Properties) -> Result<Properties, EngineError> {
        if properties.iter().any(|p| {
            matches!(
                p,
                mqtt_protocol::Property::MessageExpiryInterval(_)
                    | mqtt_protocol::Property::WillDelayInterval(_)
            )
        }) {
            return Err(invalid_record("unsupported expiry metadata in schema4"));
        }
        Ok(properties)
    }
    fn message(m: v4::Message) -> Result<Message, EngineError> {
        Ok(Message {
            topic: m.topic,
            payload: m.payload,
            qos: m.qos,
            retain: m.retain,
            properties: props(m.properties)?,
            expires_at: None,
        })
    }
    let mut sessions = Vec::new();
    for s in old.sessions {
        let mut incoming = Vec::new();
        for (id, bytes, p) in s.incoming {
            incoming.push((id, bytes, props(p)?, None));
        }
        let mut outgoing = Vec::new();
        for o in s.outgoing {
            outgoing.push(OutgoingRecord {
                packet_id: o.packet_id,
                expires_at: None,
                started: true,
                properties: props(o.properties)?,
                stage: match o.stage {
                    v4::Stage::Ack(b) => Stage::Ack(b),
                    v4::Stage::Receipt(b) => Stage::Receipt(b),
                    v4::Stage::Comp => Stage::Comp,
                },
            });
        }
        sessions.push(SessionRecord {
            owner: s.owner,
            client_id: s.client_id,
            session_expiry_interval: s.session_expiry_interval,
            expires_at: s.expires_at,
            subscriptions: s.subscriptions,
            incoming,
            outgoing,
            queued: s
                .queued
                .into_iter()
                .map(message)
                .collect::<Result<_, _>>()?,
        });
    }
    let mut retained = Vec::new();
    for r in old.retained {
        retained.push(RetainedRecord {
            topic: r.topic,
            payload: r.payload,
            qos: r.qos,
            properties: props(r.properties)?,
            expires_at: None,
        });
    }
    let mut wills = Vec::new();
    for w in old.wills {
        wills.push(WillRecord {
            owner: w.owner,
            client_id: w.client_id,
            pending: w.pending,
            timer_blocked: false,
            delay_interval: 0,
            due_at: w.pending.then_some(0),
            will: WillData {
                topic: w.will.topic,
                payload: w.will.payload,
                qos: w.will.qos,
                retain: w.will.retain,
                properties: props(w.will.properties)?,
            },
        });
    }
    Ok(Envelope {
        schema_version: 5,
        sessions,
        retained,
        wills,
    })
}

#[cfg(test)]
mod tests;
