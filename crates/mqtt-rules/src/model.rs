use crate::RuleError;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub topic: String,
    pub payload: Vec<u8>,
    pub client_id: String,
    pub qos: u8,
    pub retain: bool,
    pub timestamp_ms: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub id: String,
    pub name: String,
    pub sql: String,
    pub enabled: bool,
    pub action_ids: Vec<String>,
}
impl Rule {
    pub fn validate(&self) -> Result<(), RuleError> {
        if !valid_id(&self.id)
            || self.name.is_empty()
            || self.name.len() > 128
            || self.action_ids.len() > 64
            || self.action_ids.iter().any(|id| !valid_id(id))
        {
            return Err(RuleError::InvalidConfiguration);
        }
        Ok(())
    }
}
pub fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
}
