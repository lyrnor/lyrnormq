use super::assets::{self, Assets};
use crate::cli::Options;
use mqtt_transport::{BrokerConfig, ProtocolMode, management::ManagementHandle};
use serde_json::{Value, json};
use std::{
    fs,
    io::{self, Read},
    path::Path,
    sync::Arc,
};

pub struct State {
    pub handle: ManagementHandle,
    pub token: Vec<u8>,
    pub assets: Assets,
    pub instance: Value,
    pub configuration: Value,
}

pub fn read_token(path: &Path) -> io::Result<Vec<u8>> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "management token must be a regular file containing 32..256 visible ASCII characters",
        )
    };
    let meta = fs::symlink_metadata(path).map_err(|_| invalid())?;
    if !meta.is_file() || meta.len() > 4096 {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    fs::File::open(path)
        .map_err(|_| invalid())?
        .take(4097)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid())?;
    if bytes.len() > 4096 {
        return Err(invalid());
    }
    let token = bytes.trim_ascii();
    if !(32..=256).contains(&token.len()) || !token.iter().all(|b| matches!(b, 33..=126)) {
        return Err(invalid());
    }
    Ok(token.to_vec())
}

/// Called on the blocking pool before binding either listener; errors redact paths.
pub fn prepare(
    options: &Options,
    listen: String,
    handle: ManagementHandle,
) -> io::Result<Arc<State>> {
    let path = options
        .admin_token_file
        .as_deref()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "management token required"))?;
    let token = read_token(path)?;
    let assets = match options.dashboard_dir.as_deref() {
        Some(root) => assets::load(root)?,
        None => assets::embedded()?,
    };
    // Even a hard-linked token with a .js name must never become a public asset.
    if assets
        .values()
        .any(|asset| asset.bytes.trim_ascii() == token.as_slice())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "management token cannot be a dashboard asset",
        ));
    }
    let sensitive = [
        options.admin_token_file.as_deref(),
        options.tls_cert.as_deref(),
        options.tls_key.as_deref(),
        options.tls_client_ca.as_deref(),
        options.auth_policy.as_deref(),
        options.config.storage_path.as_deref(),
    ];
    for secret in sensitive.into_iter().flatten() {
        for asset in assets.values() {
            let Some(source) = &asset.source else {
                continue;
            };
            match same_file::is_same_file(secret, source) {
                Ok(false) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "sensitive configuration cannot be a dashboard asset",
                    ));
                }
            }
        }
    }
    let config = &options.config;
    let tls = options.tls_cert.is_some();
    let instance = json!({"version":env!("CARGO_PKG_VERSION"),"listen":listen,"transport":match(options.websocket,tls){(false,false)=>"TCP",(false,true)=>"TLS",(true,false)=>"WS",(true,true)=>"WSS"},"protocol":match config.protocol_mode{ProtocolMode::Both=>"both",ProtocolMode::V311=>"3.1.1",ProtocolMode::V5=>"5.0"},"storage":if config.storage_path.is_some(){"sqlite"}else{"memory"},"tls":tls,"mtls":options.tls_client_ca.is_some(),"auth_policy":options.auth_policy.is_some(),"enhanced_auth":options.enhanced_auth_plain});
    let configuration = json!({"instance":instance,"limits":limits(config)});
    Ok(Arc::new(State {
        handle,
        token,
        assets,
        instance,
        configuration,
    }))
}
fn limits(c: &BrokerConfig) -> Value {
    json!({"max_connections":c.max_connections,"max_sessions":c.max_sessions,"max_packet_size":c.max_packet_size,"max_subscriptions":c.max_subscriptions,"max_retained":c.max_retained,"max_inflight":c.max_inflight,"outbound_capacity":c.outbound_capacity,"max_handshakes":c.max_handshakes,"max_auth_tasks":c.max_auth_tasks,"auth_rate_per_second":c.auth_rate_per_second,"auth_burst":c.auth_burst,"handshake_timeout_ms":c.handshake_timeout.as_millis(),"write_timeout_ms":c.write_timeout.as_millis(),"topic_alias_maximum":c.topic_alias_maximum})
}
