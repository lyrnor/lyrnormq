//! Schema4 preserves deadlines and message properties; older schemas migrate explicitly.
use super::{EngineError, invalid_record};
use mqtt_protocol::{Properties, Will};
use serde::{Deserialize, Serialize};
mod legacy;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Message {
    pub(super) topic: String,
    pub(super) payload: Vec<u8>,
    pub(super) qos: u8,
    pub(super) retain: bool,
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
    pub(super) incoming: Vec<(u16, Vec<u8>, Properties)>,
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
    pub(super) properties: Properties,
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
    if version.schema_version == 4 {
        return Ok((
            serde_json::from_slice(bytes).map_err(EngineError::Json)?,
            false,
        ));
    }
    let (old, legacy) = legacy::decode(bytes)?;
    let map_message = |m: legacy::Message| Message {
        topic: m.topic,
        payload: m.payload,
        qos: m.qos,
        retain: m.retain,
        properties: Vec::new(),
    };
    Ok((
        Envelope {
            schema_version: 4,
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
                        .map(|(id, bytes)| (id, bytes, Vec::new()))
                        .collect(),
                    outgoing: s
                        .outgoing
                        .into_iter()
                        .map(|o| OutgoingRecord {
                            packet_id: o.packet_id,
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
