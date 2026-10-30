use crate::RuntimeError;
use mqtt_rules::{CompiledRule, Rule};
use reqwest::{
    Url,
    header::{HeaderName, HeaderValue},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_CONFIG: usize = 64 * 1024;
pub const REDACTED: &str = "[redacted]";
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub connectors: Vec<Connector>,
    pub actions: Vec<Action>,
    pub rules: Vec<Rule>,
}
pub type PublicConfig = Config;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Connector {
    pub id: String,
    pub name: String,
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}
impl std::fmt::Debug for Connector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connector")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Action {
    pub id: String,
    pub name: String,
    pub connector_id: String,
    pub enabled: bool,
    pub batch_size: usize,
    pub batch_wait_ms: u64,
    pub queue_capacity: usize,
    pub timeout_ms: u64,
    pub max_attempts: u32,
    pub retry_base_ms: u64,
}
pub(crate) struct Validated {
    pub config: Config,
    pub rules: Vec<(Rule, CompiledRule)>,
}
fn id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn name(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128
}
impl Config {
    pub(crate) fn public(&self) -> PublicConfig {
        let mut config = self.clone();
        for connector in &mut config.connectors {
            for secret in connector.headers.values_mut() {
                *secret = REDACTED.into();
            }
        }
        config
    }
    pub(crate) fn restore_redacted(&mut self, previous: &Config) -> Result<(), RuntimeError> {
        for connector in &mut self.connectors {
            let old = previous.connectors.iter().find(|c| c.id == connector.id);
            for (key, value) in &mut connector.headers {
                if value == REDACTED {
                    *value = old
                        .and_then(|c| {
                            c.headers
                                .iter()
                                .find(|(k, _)| k.eq_ignore_ascii_case(key))
                                .map(|(_, v)| v.clone())
                        })
                        .ok_or(RuntimeError::InvalidConfig)?;
                }
            }
        }
        Ok(())
    }
    pub(crate) fn validate(self) -> Result<Validated, RuntimeError> {
        if self.connectors.len() > 32
            || self.actions.len() > 64
            || self.rules.len() > 128
            || serde_json::to_vec(&self)
                .map_err(|_| RuntimeError::InvalidConfig)?
                .len()
                > MAX_CONFIG
        {
            return Err(RuntimeError::InvalidConfig);
        }
        let mut connectors = BTreeSet::new();
        for connector in &self.connectors {
            if !id(&connector.id)
                || !name(&connector.name)
                || !connectors.insert(connector.id.clone())
                || connector.headers.len() > 16
            {
                return Err(RuntimeError::InvalidConfig);
            }
            let url = Url::parse(&connector.url).map_err(|_| RuntimeError::InvalidConfig)?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(RuntimeError::InvalidConfig);
            }
            let mut size = 0usize;
            let mut unique = BTreeSet::new();
            for (key, value) in &connector.headers {
                let lower = key.to_ascii_lowercase();
                if !unique.insert(lower.clone())
                    || matches!(
                        lower.as_str(),
                        "host"
                            | "content-length"
                            | "content-type"
                            | "connection"
                            | "transfer-encoding"
                            | "keep-alive"
                            | "proxy-authenticate"
                            | "proxy-authorization"
                            | "te"
                            | "trailer"
                            | "upgrade"
                    )
                    || value == REDACTED
                    || value.contains(['\r', '\n'])
                    || HeaderName::from_bytes(key.as_bytes()).is_err()
                    || HeaderValue::from_str(value).is_err()
                {
                    return Err(RuntimeError::InvalidConfig);
                }
                size = size.saturating_add(key.len()).saturating_add(value.len());
            }
            if size > 8192 {
                return Err(RuntimeError::InvalidConfig);
            }
        }
        let mut actions = BTreeSet::new();
        for action in &self.actions {
            if !id(&action.id)
                || !name(&action.name)
                || !actions.insert(action.id.clone())
                || !connectors.contains(&action.connector_id)
                || !(1..=500).contains(&action.batch_size)
                || action.batch_wait_ms > 1000
                || !(1..=4096).contains(&action.queue_capacity)
                || !(100..=30000).contains(&action.timeout_ms)
                || !(1..=5).contains(&action.max_attempts)
                || !(10..=10000).contains(&action.retry_base_ms)
            {
                return Err(RuntimeError::InvalidConfig);
            }
        }
        let mut ids = BTreeSet::new();
        let mut rules = Vec::new();
        let mut bindings = 0usize;
        for rule in &self.rules {
            bindings = bindings.saturating_add(rule.action_ids.len());
            let unique: BTreeSet<_> = rule.action_ids.iter().collect();
            if !id(&rule.id)
                || !name(&rule.name)
                || !ids.insert(rule.id.clone())
                || rule.action_ids.len() != unique.len()
                || rule.action_ids.iter().any(|id| !actions.contains(id))
                || bindings > 512
            {
                return Err(RuntimeError::InvalidConfig);
            }
            rules.push((
                rule.clone(),
                CompiledRule::compile(&rule.sql).map_err(|_| RuntimeError::InvalidSql)?,
            ));
        }
        Ok(Validated {
            config: self,
            rules,
        })
    }
}
