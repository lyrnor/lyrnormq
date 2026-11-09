//! Strict startup-only cluster configuration.
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    io,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};
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
/// Fixed replicated data group, independent of Controller Raft membership.
/// The historical type name does not by itself promise persistence across process loss.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DataBackend {
    #[default]
    Memory,
    #[serde(rename = "rocksdb")]
    Rocks,
}
/// Fixed replicated group, independent of Controller Raft membership.
/// `memory` keeps Phase7f volatility. `rocksdb` replays the same records after restart.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DurableConfig {
    pub replicas: Vec<Controller>,
    #[serde(default)]
    pub backend: DataBackend,
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
/// Raft voting size. etcd has no protocol cap and documents a practical maximum
/// of seven members. Only the odd sizes add a failure domain: 3, 5, and 7.
pub fn voting_members(count: usize) -> bool {
    matches!(count, 3 | 5 | 7)
}

impl Config {
    pub fn validate(&self) -> io::Result<()> {
        let bad = || io::Error::new(io::ErrorKind::InvalidInput, "invalid cluster configuration");
        if self.cluster_id.is_empty()
            || self.cluster_id.len() > 128
            || !self.cluster_id.bytes().all(|b| (33..=126).contains(&b))
            || self.node_id == 0
            || !voting_members(self.controllers.len())
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
        if self.tls.is_none() && !plaintext_ip_allowed(self.listen.ip()) {
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
            if self.role != Role::Broker || !voting_members(durable.replicas.len()) {
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
fn plaintext_ip_allowed(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
        IpAddr::V6(ip) => ip.is_loopback(),
    }
}
pub(crate) fn cluster_peer_address(address: &str) -> bool {
    endpoint_valid(address, false) || endpoint_valid(address, true)
}
fn endpoint_valid(address: &str, tls: bool) -> bool {
    if address.len() > 2048 {
        return false;
    }
    if let Some(rest) = address.strip_prefix("http://") {
        return !tls
            && rest
                .parse::<SocketAddr>()
                .is_ok_and(|socket| plaintext_ip_allowed(socket.ip()));
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

#[cfg(test)]
mod tests {
    use super::{Config, Controller, DataBackend, DurableConfig, Role};

    fn controllers(addresses: [&str; 3]) -> Vec<Controller> {
        addresses
            .into_iter()
            .enumerate()
            .map(|(index, address)| Controller {
                id: u64::try_from(index + 1).expect("peer id"),
                address: address.to_owned(),
            })
            .collect()
    }

    fn controller(listen: &str, peers: [&str; 3]) -> Config {
        Config {
            role: Role::Controller,
            cluster_id: "private-net".to_owned(),
            node_id: 1,
            listen: listen.parse().expect("listen"),
            data_dir: "/tmp/lyrnormq-cfg".into(),
            token_file: "/tmp/lyrnormq-cfg/token".into(),
            controllers: controllers(peers),
            bootstrap: false,
            advertise: None,
            lease_ms: 3000,
            tls: None,
            durable: None,
        }
    }

    #[test]
    fn private_network_plaintext_is_accepted() {
        let mut config = controller(
            "172.16.240.180:17001",
            [
                "http://172.16.240.180:17001",
                "http://172.16.240.182:17001",
                "http://172.16.240.183:17001",
            ],
        );
        config.role = Role::Broker;
        config.node_id = 101;
        config.listen = "172.16.240.180:17101".parse().expect("broker listen");
        config.durable = Some(DurableConfig {
            replicas: vec![
                Controller {
                    id: 101,
                    address: "http://172.16.240.180:17101".to_owned(),
                },
                Controller {
                    id: 102,
                    address: "http://172.16.240.182:17101".to_owned(),
                },
                Controller {
                    id: 103,
                    address: "http://172.16.240.183:17101".to_owned(),
                },
            ],
            backend: DataBackend::Memory,
        });
        assert!(config.validate().is_ok());
    }

    #[test]
    fn odd_voting_sizes_through_seven_are_accepted() {
        let base = controller(
            "10.0.0.1:17001",
            [
                "http://10.0.0.1:17001",
                "http://10.0.0.2:17001",
                "http://10.0.0.3:17001",
            ],
        );
        for count in [5usize, 7] {
            let mut config = base.clone();
            config.controllers = (0..count)
                .map(|index| Controller {
                    id: u64::try_from(index + 1).expect("id"),
                    address: format!("http://10.0.0.{}:17001", index + 1),
                })
                .collect();
            assert!(config.validate().is_ok(), "{count}");
        }
        for count in [1usize, 2, 4, 6, 8, 9] {
            let mut config = base.clone();
            config.controllers = (0..count)
                .map(|index| Controller {
                    id: u64::try_from(index + 1).expect("id"),
                    address: format!("http://10.0.0.{}:17001", index + 1),
                })
                .collect();
            assert!(config.validate().is_err(), "{count}");
        }
    }

    #[test]
    fn public_plaintext_is_rejected() {
        let config = controller(
            "47.84.224.186:17001",
            [
                "http://47.84.224.186:17001",
                "http://172.16.240.182:17001",
                "http://172.16.240.183:17001",
            ],
        );
        assert!(config.validate().is_err());
    }

    #[test]
    fn private_register_address_is_a_cluster_peer() {
        assert!(super::cluster_peer_address("http://172.16.240.180:17101"));
        assert!(super::cluster_peer_address("http://127.0.0.1:17101"));
        assert!(super::cluster_peer_address("https://47.84.224.186:17101"));
        assert!(!super::cluster_peer_address("http://47.84.224.186:17101"));
        assert!(!super::cluster_peer_address("http://172.16.240.180/mqtt"));
    }

    #[test]
    fn loopback_plaintext_stays_accepted() {
        let config = controller(
            "127.0.0.1:17001",
            [
                "http://127.0.0.1:17001",
                "http://127.0.0.1:17002",
                "http://127.0.0.1:17003",
            ],
        );
        assert!(config.validate().is_ok());
    }
}
