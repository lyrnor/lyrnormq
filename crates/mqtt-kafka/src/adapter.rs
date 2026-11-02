use crate::{KafkaConfig, owner::Owner};
use krafka::Record;
use mqtt_egress_core::{BatchLimits, FailureKind, SendError, Sink};
use serde::Deserialize;
use serde_json::value::RawValue;
use std::borrow::Cow;
#[derive(Deserialize)]
struct Event<'a> {
    #[serde(borrow)]
    event_id: Cow<'a, str>,
    #[serde(borrow)]
    delivery_id: Cow<'a, str>,
    #[serde(borrow)]
    rule_id: Cow<'a, str>,
    #[serde(borrow)]
    client_id: Cow<'a, str>,
    #[serde(borrow)]
    topic: Cow<'a, str>,
    timestamp_ms: u64,
    #[serde(borrow)]
    data: &'a RawValue,
}
pub struct KafkaSink {
    config: KafkaConfig,
    owner: Owner,
}
impl KafkaSink {
    pub fn new(
        id: &str,
        config: KafkaConfig,
        timeout: u64,
        concurrency: usize,
    ) -> Result<Self, crate::BuildError> {
        if !(1..=512).contains(&concurrency)
            || id.is_empty()
            || id.len() > 64
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
        {
            return Err(crate::BuildError::Invalid);
        }
        let owner = Owner::new(id, &config, timeout)?;
        Ok(Self { config, owner })
    }
    async fn send_one(&self, bytes: &[u8]) -> Result<(), SendError> {
        let event: Event<'_> =
            serde_json::from_slice(bytes).map_err(|_| permanent("kafka_event"))?;
        let key = match &self.config.key_field {
            Some(field) => data_key(event.data.get(), field)?,
            None => event.client_id.to_string(),
        };
        let pairs = [
            ("rmqtt-delivery-id", event.delivery_id.as_ref()),
            ("rmqtt-event-id", event.event_id.as_ref()),
            ("rmqtt-rule-id", event.rule_id.as_ref()),
            ("rmqtt-client-id", event.client_id.as_ref()),
            ("rmqtt-topic", event.topic.as_ref()),
        ];
        let length = event
            .data
            .get()
            .len()
            .saturating_add(key.len())
            .saturating_add(1024)
            .saturating_add(
                pairs
                    .iter()
                    .map(|(k, v)| k.len() + v.len() + 8)
                    .sum::<usize>(),
            );
        if length > 1024 * 1024 {
            return Err(permanent("request_too_large"));
        }
        let timestamp = i64::try_from(event.timestamp_ms).map_err(|_| permanent("kafka_event"))?;
        let mut record = Record::new(
            self.config.topic.clone(),
            event.data.get().as_bytes().to_vec(),
        )
        .key(key.into_bytes())
        .timestamp(timestamp);
        for (key, value) in pairs {
            record = record.header(key, value.as_bytes().to_vec());
        }
        self.owner.send(record).await
    }
}
fn permanent(code: &str) -> SendError {
    SendError {
        kind: FailureKind::Permanent,
        code: code.into(),
    }
}
impl Sink for KafkaSink {
    fn limits(&self) -> BatchLimits {
        BatchLimits {
            max_bytes: 1024 * 1024,
            max_events: 1,
            overhead: 0,
            separator_bytes: 0,
        }
    }
    fn manages_timeout(&self) -> bool {
        true
    }
    fn send<'a>(
        &'a self,
        events: &'a [&'a [u8]],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SendError>> + Send + 'a>>
    {
        Box::pin(async move {
            if events.len() != 1 {
                return Err(permanent("kafka_batch"));
            }
            self.send_one(events[0]).await
        })
    }
    fn shutdown(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SendError>> + Send + '_>>
    {
        Box::pin(self.owner.shutdown())
    }
}

// Select one top-level string without materializing arbitrary SQL JSON into a
// full Value tree. All unrelated arrays/objects are skipped by the deserializer.
fn data_key(data: &str, field: &str) -> Result<String, SendError> {
    struct Pick<'a>(&'a str);
    impl<'de> serde::de::Visitor<'de> for Pick<'_> {
        type Value = String;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an object with a string partition key")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<String, A::Error> {
            let mut selected = None;
            while let Some(key) = map.next_key::<Cow<'de, str>>()? {
                if key == self.0 {
                    if selected.is_some() {
                        return Err(serde::de::Error::custom("duplicate key"));
                    }
                    selected = Some(map.next_value::<String>()?);
                } else {
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
            selected.ok_or_else(|| serde::de::Error::custom("missing key"))
        }
    }
    let mut de = serde_json::Deserializer::from_str(data);
    let value = serde::de::Deserializer::deserialize_map(&mut de, Pick(field))
        .map_err(|_| permanent("kafka_key"))?;
    de.end().map_err(|_| permanent("kafka_key"))?;
    Ok(value)
}

#[cfg(test)]
mod key_tests {
    use super::*;
    #[test]
    fn top_level_key_selection_does_not_use_metadata_or_accept_nonstring() {
        assert_eq!(
            data_key(r#"{"huge":[1,2,{"nested":true}],"id":"device\\\"é"}"#, "id").unwrap(),
            "device\\\"é"
        );
        for value in [
            r#"{"id":2}"#,
            r#"{"id":null}"#,
            r#"{}"#,
            r#"["id"]"#,
            r#"{"id":"a","id":"b"}"#,
        ] {
            assert_eq!(
                data_key(value, "id").unwrap_err().kind,
                FailureKind::Permanent
            );
        }
    }
}
