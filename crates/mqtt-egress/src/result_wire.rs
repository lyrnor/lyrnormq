//! Shared rule bodies and per-consumer wire envelopes. No adapter-specific IO.
use mqtt_egress_log::{ConsumerMeta, LegacyResult, QueueError, Record};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::borrow::Cow;

#[derive(Clone, Copy)]
pub(crate) enum Codec {
    Rule,
    #[cfg(test)]
    Opaque,
}
impl Codec {
    pub fn wire_len(self, meta: &ConsumerMeta) -> Result<usize, QueueError> {
        match self {
            Self::Rule => meta
                .payload_len
                .checked_add(delivery_extra(&meta.delivery_id)?)
                .ok_or(QueueError::Corrupt),
            #[cfg(test)]
            Self::Opaque => Ok(meta.payload_len),
        }
    }
    pub fn encode(self, payload: &[u8], meta: &ConsumerMeta) -> Result<Vec<u8>, QueueError> {
        if payload.len() != meta.payload_len {
            return Err(QueueError::Corrupt);
        }
        match self {
            Self::Rule => {
                let result: StoredResult<'_> =
                    serde_json::from_slice(payload).map_err(|_| QueueError::Corrupt)?;
                if result.rule_id != meta.stream_id || result.client_id != meta.client_id {
                    return Err(QueueError::Corrupt);
                }
                let wire = WireEvent {
                    event_id: &result.event_id,
                    delivery_id: &meta.delivery_id,
                    rule_id: &result.rule_id,
                    topic: &result.topic,
                    client_id: &result.client_id,
                    qos: result.qos,
                    retain: result.retain,
                    timestamp_ms: result.timestamp_ms,
                    data: result.data,
                };
                let bytes = serde_json::to_vec(&wire).map_err(|_| QueueError::Corrupt)?;
                if bytes.len() != self.wire_len(meta)? {
                    return Err(QueueError::Corrupt);
                }
                Ok(bytes)
            }
            #[cfg(test)]
            Self::Opaque => Ok(payload.to_vec()),
        }
    }
}
pub(crate) fn delivery_extra(id: &str) -> Result<usize, QueueError> {
    let encoded = serde_json::to_vec(id).map_err(|_| QueueError::Corrupt)?;
    Ok(b",\"delivery_id\":".len() + encoded.len())
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredResult<'a> {
    #[serde(borrow)]
    event_id: Cow<'a, str>,
    #[serde(borrow)]
    rule_id: Cow<'a, str>,
    #[serde(borrow)]
    topic: Cow<'a, str>,
    #[serde(borrow)]
    client_id: Cow<'a, str>,
    qos: u8,
    retain: bool,
    timestamp_ms: u64,
    #[serde(borrow)]
    data: &'a RawValue,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyWire<'a> {
    #[serde(borrow)]
    event_id: Cow<'a, str>,
    #[serde(borrow)]
    delivery_id: Cow<'a, str>,
    #[serde(borrow)]
    rule_id: Cow<'a, str>,
    #[serde(borrow)]
    topic: Cow<'a, str>,
    #[serde(borrow)]
    client_id: Cow<'a, str>,
    qos: u8,
    retain: bool,
    timestamp_ms: u64,
    #[serde(borrow)]
    data: &'a RawValue,
}
#[derive(Serialize)]
struct WireEvent<'a> {
    event_id: &'a str,
    delivery_id: &'a str,
    rule_id: &'a str,
    topic: &'a str,
    client_id: &'a str,
    qos: u8,
    retain: bool,
    timestamp_ms: u64,
    data: &'a RawValue,
}
pub(crate) fn resolve_legacy(record: &Record) -> Result<LegacyResult, QueueError> {
    let old: LegacyWire<'_> =
        serde_json::from_slice(&record.payload).map_err(|_| QueueError::Corrupt)?;
    if old.client_id != record.meta.client_id
        || record.meta.payload_len != record.payload.len()
        || old.topic.is_empty()
        || old.topic.len() > 65535
        || old.topic.contains(['\0', '+', '#'])
        || old.client_id.len() > 65535
        || old.event_id.is_empty()
        || old.event_id.len() > 256
        || old.rule_id.is_empty()
        || old.rule_id.len() > 64
        || !old
            .rule_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
        || old.delivery_id.is_empty()
        || old.delivery_id.len() > 256
        || old.qos > 2
    {
        return Err(QueueError::Corrupt);
    }
    let stored = StoredResult {
        event_id: old.event_id.clone(),
        rule_id: old.rule_id.clone(),
        topic: old.topic,
        client_id: old.client_id,
        qos: old.qos,
        retain: old.retain,
        timestamp_ms: old.timestamp_ms,
        data: old.data,
    };
    let payload = serde_json::to_vec(&stored).map_err(|_| QueueError::Corrupt)?;
    Ok(LegacyResult {
        stream_id: old.rule_id.into_owned(),
        result_key: old.event_id.into_owned(),
        payload,
        delivery_id: old.delivery_id.into_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mqtt_egress_log::RecordMeta;
    fn legacy() -> Record {
        let payload = br#"{"event_id":"event-1","delivery_id":"event-1:hot:send","rule_id":"hot","topic":"devices/a","client_id":"client-\"a","qos":1,"retain":false,"timestamp_ms":123,"data":{"b":[0,1],"v":"x"}}"#.to_vec();
        Record {
            meta: RecordMeta {
                id: 1,
                outbound_id: "send".into(),
                revision: 2,
                client_id: "client-\"a".into(),
                failures: 1,
                not_before_ms: 999,
                inflight: true,
                payload_len: payload.len(),
            },
            payload,
        }
    }
    fn consumer(payload_len: usize) -> ConsumerMeta {
        ConsumerMeta {
            id: 1,
            result_id: 9,
            outbound_id: "send".into(),
            revision: 2,
            stream_id: "hot".into(),
            client_id: "client-\"a".into(),
            delivery_id: "event-1:hot:send".into(),
            failures: 1,
            not_before_ms: 999,
            inflight: true,
            payload_len,
        }
    }
    #[test]
    fn migration_and_materialization_preserve_wire_bytes_and_exact_length() {
        let old = legacy();
        let resolved = resolve_legacy(&old).unwrap();
        assert_eq!(resolved.result_key, "event-1");
        assert_eq!(resolved.stream_id, "hot");
        let body: serde_json::Value = serde_json::from_slice(&resolved.payload).unwrap();
        assert!(body.get("delivery_id").is_none());
        let meta = consumer(resolved.payload.len());
        let encoded = Codec::Rule.encode(&resolved.payload, &meta).unwrap();
        assert_eq!(encoded, old.payload);
        assert_eq!(encoded.len(), Codec::Rule.wire_len(&meta).unwrap());
    }
    #[test]
    fn malformed_legacy_or_mismatched_stream_fails_without_identity_fallback() {
        let mut old = legacy();
        old.meta.client_id = "different".into();
        assert!(resolve_legacy(&old).is_err());
        old.payload = vec![255, 0];
        assert!(resolve_legacy(&old).is_err());
        let old = legacy();
        let resolved = resolve_legacy(&old).unwrap();
        let mut meta = consumer(resolved.payload.len());
        meta.stream_id = "other".into();
        assert!(Codec::Rule.encode(&resolved.payload, &meta).is_err());
        assert!(Codec::Rule.encode(&[255, 0], &consumer(2)).is_err());
    }
}
