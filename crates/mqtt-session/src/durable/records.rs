//! Strict schema6 message deadlines and delayed Will scheduling.
use super::{EngineError, invalid_record};
use mqtt_protocol::{Properties, Will};
use serde::{Deserialize, Serialize};
mod legacy;
mod v4;
mod v5;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Message {
    pub(super) topic: String,
    pub(super) payload: Vec<u8>,
    pub(super) qos: u8,
    pub(super) retain: bool,
    pub(super) retained_replay: bool,
    #[serde(deserialize_with = "required_deadline")]
    pub(super) expires_at: Option<u64>,
    pub(super) properties: Properties,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Envelope {
    pub(super) schema_version: u32,
    pub(super) clock_floor: u64,
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
    pub(super) subscriptions: Vec<SubscriptionRecord>,
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
    pub(super) retained_replay: bool,
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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct SubscriptionRecord {
    pub(super) filter: String,
    pub(super) qos: u8,
    pub(super) no_local: bool,
    pub(super) retain_as_published: bool,
    pub(super) retain_handling: u8,
    #[serde(deserialize_with = "required_deadline_id")]
    pub(super) identifier: Option<u32>,
}
fn required_deadline_id<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<u32>, D::Error> {
    Option::<u32>::deserialize(d)
}
impl SubscriptionRecord {
    pub(super) fn legacy(filter: String, qos: u8) -> Self {
        Self {
            filter,
            qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            identifier: None,
        }
    }
    pub(super) fn validate(&self) -> Result<(), EngineError> {
        if !mqtt_router::validate_filter(&self.filter)
            || self.qos > 2
            || self.retain_handling > 2
            || self.identifier.is_some_and(|n| n == 0 || n > 268435455)
        {
            return Err(invalid_record("invalid subscription record"));
        }
        Ok(())
    }
}
fn old_properties(properties: &Properties) -> Result<(), EngineError> {
    if properties
        .iter()
        .any(|p| matches!(p, mqtt_protocol::Property::SubscriptionIdentifier(_)))
    {
        return Err(invalid_record("Subscription Identifier in old schema"));
    }
    Ok(())
}
pub(super) fn decode(bytes: &[u8]) -> Result<(Envelope, bool), EngineError> {
    #[derive(Deserialize)]
    struct Version {
        schema_version: u32,
    }
    let version: Version = serde_json::from_slice(bytes).map_err(EngineError::Json)?;
    if version.schema_version == 6 {
        return Ok((
            serde_json::from_slice(bytes).map_err(EngineError::Json)?,
            false,
        ));
    }
    let (old, legacy) = v5::decode(bytes)?;
    let mut sessions = Vec::new();
    for s in old.sessions {
        let mut outgoing = Vec::new();
        for o in s.outgoing {
            old_properties(&o.properties)?;
            let retained_replay = match &o.stage {
                v5::Stage::Ack(b) | v5::Stage::Receipt(b) => b.first().is_some_and(|b| b & 1 != 0),
                v5::Stage::Comp => false,
            };
            outgoing.push(OutgoingRecord {
                packet_id: o.packet_id,
                expires_at: o.expires_at,
                started: o.started,
                retained_replay,
                properties: o.properties,
                stage: match o.stage {
                    v5::Stage::Ack(b) => Stage::Ack(b),
                    v5::Stage::Receipt(b) => Stage::Receipt(b),
                    v5::Stage::Comp => Stage::Comp,
                },
            });
        }
        let mut queued = Vec::new();
        for m in s.queued {
            old_properties(&m.properties)?;
            queued.push(Message {
                topic: m.topic,
                payload: m.payload,
                qos: m.qos,
                retain: m.retain,
                retained_replay: m.retain,
                expires_at: m.expires_at,
                properties: m.properties,
            });
        }
        sessions.push(SessionRecord {
            owner: s.owner,
            client_id: s.client_id,
            session_expiry_interval: s.session_expiry_interval,
            expires_at: s.expires_at,
            subscriptions: s
                .subscriptions
                .into_iter()
                .map(|(f, q)| SubscriptionRecord::legacy(f, q))
                .collect(),
            incoming: s.incoming,
            outgoing,
            queued,
        });
    }
    Ok((
        Envelope {
            schema_version: 6,
            clock_floor: old.clock_floor,
            sessions,
            retained: old
                .retained
                .into_iter()
                .map(|r| RetainedRecord {
                    topic: r.topic,
                    payload: r.payload,
                    qos: r.qos,
                    expires_at: r.expires_at,
                    properties: r.properties,
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
                    delay_interval: w.delay_interval,
                    due_at: w.due_at,
                    will: WillData {
                        topic: w.will.topic,
                        payload: w.will.payload,
                        qos: w.will.qos,
                        retain: w.will.retain,
                        properties: w.will.properties,
                    },
                })
                .collect(),
        },
        legacy,
    ))
}
#[cfg(test)]
mod tests;
