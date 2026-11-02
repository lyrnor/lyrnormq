use crate::RuntimeError;
use mqtt_kafka::KafkaConfig;
use mqtt_rules::{CompiledRule, Rule};
use mqtt_webhook::WebhookConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
pub const MAX_CONFIG: usize = 64 * 1024;
/// Applied to new policies and legacy records that omit the concurrency field.
pub const DEFAULT_CONCURRENCY: usize = 8;
/// Per-outbound bound; actual attempts also share the Runtime request semaphore.
pub const MAX_CONCURRENCY: usize = 16;
pub(crate) const GLOBAL_CONCURRENCY: usize = 16;
pub(crate) const KAFKA_GLOBAL_CONCURRENCY: usize = 512;

fn default_concurrency() -> usize {
    DEFAULT_CONCURRENCY
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub schema_version: u32,
    pub outbounds: Vec<Outbound>,
    pub rules: Vec<Rule>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            schema_version: 2,
            outbounds: Vec::new(),
            rules: Vec::new(),
        }
    }
}
pub type PublicConfig = Config;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboundType {
    Webhook,
    Kafka,
}
#[derive(Clone, Debug)]
pub enum Endpoint {
    Webhook(WebhookConfig),
    Kafka(Box<KafkaConfig>),
}
impl Endpoint {
    pub fn validate(&self) -> Result<(), RuntimeError> {
        match self {
            Self::Webhook(c) => c.validate().map_err(|_| RuntimeError::InvalidConfig),
            Self::Kafka(c) => c.validate().map_err(|_| RuntimeError::InvalidConfig),
        }
    }
    pub fn redact(&mut self) {
        match self {
            Self::Webhook(c) => c.redact(),
            Self::Kafka(c) => c.redact(),
        }
    }
    pub fn restore(&mut self, old: Option<&Self>) -> Result<(), RuntimeError> {
        match self {
            Self::Webhook(c) => c
                .restore(old.and_then(|o| match o {
                    Self::Webhook(c) => Some(c),
                    _ => None,
                }))
                .map_err(|_| RuntimeError::InvalidConfig),
            Self::Kafka(c) => c
                .restore(old.and_then(|o| match o {
                    Self::Kafka(c) => Some(c.as_ref()),
                    _ => None,
                }))
                .map_err(|_| RuntimeError::InvalidConfig),
        }
    }
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Webhook(_) => "webhook",
            Self::Kafka(_) => "kafka",
        }
    }
    pub fn sink(
        &self,
        id: &str,
        timeout: u64,
        concurrency: usize,
    ) -> Result<std::sync::Arc<dyn mqtt_egress_core::Sink>, RuntimeError> {
        match self {
            Self::Webhook(c) => Ok(std::sync::Arc::new(
                mqtt_webhook::WebhookSink::new(id, c.clone(), timeout, concurrency)
                    .map_err(|_| RuntimeError::InvalidConfig)?,
            )),
            Self::Kafka(c) => Ok(std::sync::Arc::new(
                mqtt_kafka::KafkaSink::new(id, c.as_ref().clone(), timeout, concurrency).map_err(
                    |e| match e {
                        mqtt_kafka::BuildError::Busy => RuntimeError::Busy,
                        _ => RuntimeError::InvalidConfig,
                    },
                )?,
            )),
        }
    }
}
#[derive(Clone, Debug)]
pub struct Outbound {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub endpoint: Endpoint,
    pub delivery: DeliveryPolicy,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireOutbound {
    id: String,
    name: String,
    enabled: bool,
    #[serde(rename = "type")]
    kind: OutboundType,
    config: serde_json::Value,
    delivery: serde_json::Value,
}
impl Serialize for Outbound {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let (kind, config) = match &self.endpoint {
            Endpoint::Webhook(c) => (
                OutboundType::Webhook,
                serde_json::to_value(c).map_err(serde::ser::Error::custom)?,
            ),
            Endpoint::Kafka(c) => (
                OutboundType::Kafka,
                serde_json::to_value(c).map_err(serde::ser::Error::custom)?,
            ),
        };
        WireOutbound {
            id: self.id.clone(),
            name: self.name.clone(),
            enabled: self.enabled,
            kind,
            config,
            delivery: serde_json::to_value(&self.delivery).map_err(serde::ser::Error::custom)?,
        }
        .serialize(s)
    }
}
impl<'de> Deserialize<'de> for Outbound {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let mut w = WireOutbound::deserialize(d)?;
        if matches!(w.kind, OutboundType::Kafka)
            && let Some(delivery) = w.delivery.as_object_mut()
        {
            delivery
                .entry("concurrency")
                .or_insert(serde_json::json!(128));
        }
        let delivery = serde_json::from_value(w.delivery).map_err(serde::de::Error::custom)?;
        Ok(Self {
            id: w.id,
            name: w.name,
            enabled: w.enabled,
            endpoint: match w.kind {
                OutboundType::Webhook => Endpoint::Webhook(
                    serde_json::from_value(w.config).map_err(serde::de::Error::custom)?,
                ),
                OutboundType::Kafka => Endpoint::Kafka(
                    serde_json::from_value(w.config).map_err(serde::de::Error::custom)?,
                ),
            },
            delivery,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryPolicy {
    /// Maximum active outbound attempts; retry backoff does not hold a slot.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    pub batch_size: usize,
    pub batch_wait_ms: u64,
    pub queue_capacity: usize,
    pub timeout_ms: u64,
    pub max_attempts: u32,
    pub retry_base_ms: u64,
}
impl std::ops::DerefMut for Outbound {
    fn deref_mut(&mut self) -> &mut DeliveryPolicy {
        &mut self.delivery
    }
}
impl std::ops::Deref for Outbound {
    type Target = DeliveryPolicy;
    fn deref(&self) -> &DeliveryPolicy {
        &self.delivery
    }
}
pub(crate) struct Validated {
    pub sinks: std::collections::BTreeMap<String, std::sync::Arc<dyn mqtt_egress_core::Sink>>,
    pub config: Config,
    pub rules: Vec<(Rule, CompiledRule)>,
}
fn identity(id: &str, name: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        && !name.is_empty()
        && name.len() <= 128
}
impl Config {
    pub(crate) fn public(&self) -> Self {
        let mut result = self.clone();
        for o in &mut result.outbounds {
            o.endpoint.redact();
        }
        result
    }
    pub(crate) fn restore_redacted(&mut self, previous: &Self) -> Result<(), RuntimeError> {
        for o in &mut self.outbounds {
            let old = previous
                .outbounds
                .iter()
                .find(|old| old.id == o.id && old.endpoint.kind() == o.endpoint.kind());
            o.endpoint.restore(old.map(|old| &old.endpoint))?;
        }
        Ok(())
    }
    pub(crate) fn validate(self) -> Result<Validated, RuntimeError> {
        if self.schema_version != 2
            || self.outbounds.len() > 64
            || self.rules.len() > 128
            || serde_json::to_vec(&self)
                .map_err(|_| RuntimeError::InvalidConfig)?
                .len()
                > MAX_CONFIG
        {
            return Err(RuntimeError::InvalidConfig);
        }
        if self
            .outbounds
            .iter()
            .filter(|o| o.enabled && matches!(o.endpoint, Endpoint::Kafka(_)))
            .count()
            > 8
        {
            return Err(RuntimeError::InvalidConfig);
        }
        let mut ids = BTreeSet::new();
        for o in &self.outbounds {
            if !identity(&o.id, &o.name)
                || !ids.insert(o.id.clone())
                || !(1..=if matches!(o.endpoint, Endpoint::Kafka(_)) {
                    512
                } else {
                    MAX_CONCURRENCY
                })
                    .contains(&o.concurrency)
                || !(1..=500).contains(&o.batch_size)
                || o.batch_wait_ms > 1000
                || !(1..=4096).contains(&o.queue_capacity)
                || !(100..=30000).contains(&o.timeout_ms)
                || !(1..=5).contains(&o.max_attempts)
                || !(10..=10000).contains(&o.retry_base_ms)
            {
                return Err(RuntimeError::InvalidConfig);
            }
            if matches!(o.endpoint, Endpoint::Kafka(_))
                && (o.batch_size != 1 || o.batch_wait_ms != 0 || o.timeout_ms < 1000)
            {
                return Err(RuntimeError::InvalidConfig);
            }
            o.endpoint
                .validate()
                .map_err(|_| RuntimeError::InvalidConfig)?;
        }
        let mut rule_ids = BTreeSet::new();
        let mut rules = Vec::new();
        let mut bindings = 0;
        for rule in &self.rules {
            bindings += rule.outbound_ids.len();
            let unique: BTreeSet<_> = rule.outbound_ids.iter().collect();
            if !identity(&rule.id, &rule.name)
                || !rule_ids.insert(rule.id.clone())
                || unique.len() != rule.outbound_ids.len()
                || rule.outbound_ids.iter().any(|id| !ids.contains(id))
                || bindings > 512
            {
                return Err(RuntimeError::InvalidConfig);
            }
            rules.push((
                rule.clone(),
                CompiledRule::compile(&rule.sql).map_err(|_| RuntimeError::InvalidSql)?,
            ));
        }
        let mut sinks = std::collections::BTreeMap::new();
        for o in self.outbounds.iter().filter(|o| o.enabled) {
            sinks.insert(
                o.id.clone(),
                o.endpoint.sink(&o.id, o.timeout_ms, o.concurrency)?,
            );
        }
        Ok(Validated {
            sinks,
            config: self,
            rules,
        })
    }
}

#[cfg(test)]
#[path = "config_kafka_tests.rs"]
mod kafka_tests;
