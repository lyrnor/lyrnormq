use crate::{Config, RuntimeError, config::MAX_CONFIG};
use mqtt_storage::configuration::ConfigStore;
pub(crate) fn decode(bytes: &[u8]) -> Result<Config, RuntimeError> {
    if bytes.len() > MAX_CONFIG {
        return Err(RuntimeError::InvalidConfig);
    }
    match serde_json::from_slice::<Config>(bytes) {
        Ok(config) => Ok(config),
        Err(_) => migrate(bytes),
    }
}

pub(crate) fn write(store: &mut ConfigStore, config: &Config) -> Result<(), RuntimeError> {
    let bytes = serde_json::to_vec(config).map_err(|_| RuntimeError::InvalidConfig)?;
    if bytes.len() > MAX_CONFIG {
        return Err(RuntimeError::InvalidConfig);
    }
    store.save(&bytes).map_err(|_| RuntimeError::Storage)
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Legacy {
    connectors: Vec<LegacyConnector>,
    actions: Vec<LegacyAction>,
    rules: Vec<LegacyRule>,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyConnector {
    id: String,
    name: String,
    url: String,
    #[serde(default)]
    headers: std::collections::BTreeMap<String, String>,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyAction {
    id: String,
    name: String,
    connector_id: String,
    enabled: bool,
    batch_size: usize,
    batch_wait_ms: u64,
    queue_capacity: usize,
    timeout_ms: u64,
    max_attempts: u32,
    retry_base_ms: u64,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyRule {
    id: String,
    name: String,
    sql: String,
    enabled: bool,
    action_ids: Vec<String>,
}
fn migrate(bytes: &[u8]) -> Result<Config, RuntimeError> {
    use crate::{DeliveryPolicy, Endpoint, Outbound};
    let old: Legacy = serde_json::from_slice(bytes).map_err(|_| RuntimeError::InvalidConfig)?;
    if old.connectors.len() > 32 {
        return Err(RuntimeError::InvalidConfig);
    }
    let mut ids = std::collections::BTreeSet::new();
    for c in &old.connectors {
        if c.name.is_empty()
            || c.name.len() > 128
            || c.id.is_empty()
            || c.id.len() > 64
            || !c
                .id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            || !ids.insert(c.id.clone())
        {
            return Err(RuntimeError::InvalidConfig);
        }
        mqtt_webhook::WebhookConfig {
            url: c.url.clone(),
            headers: c.headers.clone(),
        }
        .validate()
        .map_err(|_| RuntimeError::InvalidConfig)?;
    }
    let mut outbounds = Vec::new();
    for a in old.actions {
        let c = old
            .connectors
            .iter()
            .find(|c| c.id == a.connector_id)
            .ok_or(RuntimeError::InvalidConfig)?;
        outbounds.push(Outbound {
            id: a.id,
            name: a.name,
            enabled: a.enabled,
            endpoint: Endpoint::Webhook(mqtt_webhook::WebhookConfig {
                url: c.url.clone(),
                headers: c.headers.clone(),
            }),
            delivery: DeliveryPolicy {
                concurrency: crate::DEFAULT_CONCURRENCY,
                batch_size: a.batch_size,
                batch_wait_ms: a.batch_wait_ms,
                queue_capacity: a.queue_capacity,
                timeout_ms: a.timeout_ms,
                max_attempts: a.max_attempts,
                retry_base_ms: a.retry_base_ms,
            },
        });
    }
    Ok(Config {
        schema_version: 2,
        outbounds,
        rules: old
            .rules
            .into_iter()
            .map(|r| mqtt_rules::Rule {
                id: r.id,
                name: r.name,
                sql: r.sql,
                enabled: r.enabled,
                outbound_ids: r.action_ids,
            })
            .collect(),
    })
}
