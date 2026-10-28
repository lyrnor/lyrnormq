//! Schema4 preserves deadlines and message properties; older schemas migrate explicitly.
use mqtt_protocol::Properties;
use serde::{Deserialize, Serialize};
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
