//! Strict startup-only cluster configuration.
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, io, net::SocketAddr, path::PathBuf};
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Controller,
    Broker,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Controller {
    pub id: u64,
    pub address: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
    pub ca_file: PathBuf,
    pub domain_name: String,
}
/// Fixed replicated-memory group, independent of Controller Raft membership.
/// The historical type/JSON name does not promise persistence across process loss.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DurableConfig {
    pub replicas: Vec<Controller>,
}
fn lease() -> u64 {
    3000
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub role: Role,
    pub cluster_id: String,
    pub node_id: u64,
    pub listen: SocketAddr,
    pub data_dir: PathBuf,
    pub token_file: PathBuf,
    pub controllers: Vec<Controller>,
    #[serde(default)]
    pub bootstrap: bool,
    #[serde(default)]
    pub advertise: Option<String>,
    #[serde(default = "lease")]
    pub lease_ms: u64,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    #[serde(default)]
    pub durable: Option<DurableConfig>,
}
impl Config {
    pub fn validate(&self) -> io::Result<()> {
        let bad = || io::Error::new(io::ErrorKind::InvalidInput, "invalid cluster configuration");
        if self.cluster_id.is_empty()
            || self.cluster_id.len() > 128
            || !self.cluster_id.bytes().all(|b| (33..=126).contains(&b))
            || self.node_id == 0
            || self.controllers.len() != 3
            || !(1000..=30000).contains(&self.lease_ms)
            || self.data_dir.as_os_str().is_empty()
            || self.token_file.as_os_str().is_empty()
        {
            return Err(bad());
        }
        let mut ids = BTreeSet::new();
        let mut addresses = BTreeSet::new();
        for peer in &self.controllers {
            if peer.id == 0
                || !ids.insert(peer.id)
                || !addresses.insert(&peer.address)
                || !endpoint_valid(&peer.address, self.tls.is_some())
            {
                return Err(bad());
            }
        }
        if self.role == Role::Controller && !ids.contains(&self.node_id) {
            return Err(bad());
        }
        if !self.listen.ip().is_loopback() && self.tls.is_none() {
            return Err(bad());
        }
        if let Some(address) = &self.advertise
            && !endpoint_valid(address, self.tls.is_some())
        {
            return Err(bad());
        }
        if self.role == Role::Broker && self.bootstrap {
            return Err(bad());
        }
        if let Some(durable) = &self.durable {
            let mut ids = BTreeSet::new();
            let mut addresses = BTreeSet::new();
            if self.role != Role::Broker || durable.replicas.len() != 3 {
                return Err(bad());
            }
            for peer in &durable.replicas {
                if peer.id == 0
                    || !ids.insert(peer.id)
                    || !addresses.insert(&peer.address)
                    || !endpoint_valid(&peer.address, self.tls.is_some())
                {
                    return Err(bad());
                }
            }
            if !durable
                .replicas
                .iter()
                .any(|p| p.id == self.node_id && p.address == self.address())
            {
                return Err(bad());
            }
        }
        Ok(())
    }
    pub(crate) fn address(&self) -> String {
        self.advertise.clone().unwrap_or_else(|| {
            format!(
                "{}://{}",
                if self.tls.is_some() { "https" } else { "http" },
                self.listen
            )
        })
    }
}
fn endpoint_valid(address: &str, tls: bool) -> bool {
    if address.len() > 2048 {
        return false;
    }
    if let Some(rest) = address.strip_prefix("http://") {
        return !tls
            && rest
                .parse::<SocketAddr>()
                .is_ok_and(|a| a.ip().is_loopback());
    }
    address
        .strip_prefix("https://")
        .is_some_and(|rest| tls && rest.parse::<SocketAddr>().is_ok())
}
pub(crate) async fn token(config: &Config) -> io::Result<String> {
    let meta = tokio::fs::symlink_metadata(&config.token_file).await?;
    if !meta.is_file() || meta.len() > 4096 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid cluster token file",
        ));
    }
    let bytes = read_bounded(&config.token_file, 4096).await?;
    let value = bytes.trim_ascii();
    if !(32..=256).contains(&value.len()) || !value.iter().all(|b| (33..=126).contains(b)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid cluster token file",
        ));
    }
    String::from_utf8(value.to_vec())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid cluster token file"))
}

pub(crate) async fn read_bounded(path: &std::path::Path, limit: usize) -> io::Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let file = tokio::fs::File::open(path).await?;
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut bytes).await?;
    if bytes.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cluster file exceeds limit",
        ));
    }
    Ok(bytes)
}
