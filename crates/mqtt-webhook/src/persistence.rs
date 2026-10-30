use crate::{Config, RuntimeError, config::MAX_CONFIG};
use mqtt_storage::configuration::ConfigStore;
pub(crate) fn decode(bytes: &[u8]) -> Result<Config, RuntimeError> {
    if bytes.len() > MAX_CONFIG {
        return Err(RuntimeError::InvalidConfig);
    }
    serde_json::from_slice(bytes).map_err(|_| RuntimeError::InvalidConfig)
}

pub(crate) fn write(store: &mut ConfigStore, config: &Config) -> Result<(), RuntimeError> {
    let bytes = serde_json::to_vec(config).map_err(|_| RuntimeError::InvalidConfig)?;
    if bytes.len() > MAX_CONFIG {
        return Err(RuntimeError::InvalidConfig);
    }
    store.save(&bytes).map_err(|_| RuntimeError::Storage)
}
