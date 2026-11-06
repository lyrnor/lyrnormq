use mqtt_transport::{BrokerConfig, WebSocketOptions};
use std::{io, path::PathBuf};

pub const USAGE: &str = "usage: rmqtt [export-state|cluster-status|cluster-repair-learner|cluster-repair|cluster-repair-abort] [--config PATH] [--target-node ID (cluster-repair only)]";

#[derive(Debug)]
pub struct Invocation {
    pub export_state: bool,
    pub cluster_status: bool,
    pub cluster_repair: Option<ClusterRepair>,
    pub config_path: PathBuf,
    pub cluster_repair_target: Option<u64>,
}

#[derive(Debug)]
pub struct Options {
    pub address: String,
    /// Operator configuration file. Retained so dashboard assets cannot alias it.
    pub cluster_config: Option<PathBuf>,
    pub cluster_controller_config: Option<PathBuf>,
    pub cluster_broker: Option<mqtt_cluster::Config>,
    pub cluster_controller: Option<mqtt_cluster::Config>,
    pub cluster_status: bool,
    pub cluster_repair: Option<ClusterRepair>,
    pub cluster_repair_target: Option<u64>,
    pub cluster_sensitive_paths: Vec<PathBuf>,
    pub cluster_data_dir: Option<PathBuf>,
    pub cluster_controller_data_dir: Option<PathBuf>,
    pub admin_listen: Option<std::net::SocketAddr>,
    pub admin_token_file: Option<PathBuf>,
    pub dashboard_dir: Option<PathBuf>,
    pub rules_config: Option<PathBuf>,
    pub access_control_config: Option<PathBuf>,
    pub egress_queue_dir: Option<PathBuf>,
    pub egress_queue_segment_bytes: u64,
    pub egress_queue_max_bytes: u64,
    pub egress_queue_max_records: usize,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub tls_client_ca: Option<PathBuf>,
    pub websocket: bool,
    pub websocket_options: WebSocketOptions,
    pub auth_policy: Option<PathBuf>,
    pub enhanced_auth_plain: bool,
    /// Print the merged stored state of a stopped broker instead of serving.
    pub export_state: bool,
    pub config: BrokerConfig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterRepair {
    Learner,
    Run,
    Abort,
}

impl Options {
    pub(crate) fn blank() -> Self {
        Self {
            address: "127.0.0.1:1883".into(),
            cluster_config: None,
            cluster_controller_config: None,
            cluster_broker: None,
            cluster_controller: None,
            cluster_status: false,
            cluster_repair: None,
            cluster_repair_target: None,
            cluster_sensitive_paths: Vec::new(),
            cluster_data_dir: None,
            cluster_controller_data_dir: None,
            admin_listen: None,
            admin_token_file: None,
            dashboard_dir: None,
            rules_config: None,
            access_control_config: None,
            egress_queue_dir: None,
            egress_queue_segment_bytes: 1024 * 1024,
            egress_queue_max_bytes: 256 * 1024 * 1024,
            egress_queue_max_records: 100_000,
            tls_cert: None,
            tls_key: None,
            tls_client_ca: None,
            websocket: false,
            websocket_options: WebSocketOptions::default(),
            auth_policy: None,
            enhanced_auth_plain: false,
            export_state: false,
            config: BrokerConfig::default(),
        }
    }

    /// The queue directory is independent of the SQLite configuration contents.
    pub fn queue_directory(&self) -> Option<PathBuf> {
        self.egress_queue_dir.clone().or_else(|| {
            self.rules_config.as_ref().map(|path| {
                let mut directory = path.as_os_str().to_os_string();
                directory.push(".outbound-queue");
                PathBuf::from(directory)
            })
        })
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

pub fn parse(args: impl IntoIterator<Item = impl AsRef<str>>) -> io::Result<Option<Invocation>> {
    let args: Vec<String> = args
        .into_iter()
        .map(|arg| arg.as_ref().to_owned())
        .collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        return Ok(None);
    }
    let mut invocation = Invocation {
        export_state: false,
        cluster_status: false,
        cluster_repair: None,
        config_path: PathBuf::from("rmqtt.toml"),
        cluster_repair_target: None,
    };
    let mut index = 0;
    if let Some(first) = args.first() {
        let command = match first.as_str() {
            "export-state" => Some(Command::Export),
            "cluster-status" => Some(Command::Status),
            "cluster-repair-learner" => Some(Command::Repair(ClusterRepair::Learner)),
            "cluster-repair" => Some(Command::Repair(ClusterRepair::Run)),
            "cluster-repair-abort" => Some(Command::Repair(ClusterRepair::Abort)),
            _ => None,
        };
        if let Some(command) = command {
            match command {
                Command::Export => invocation.export_state = true,
                Command::Status => invocation.cluster_status = true,
                Command::Repair(kind) => invocation.cluster_repair = Some(kind),
            }
            index = 1;
        }
    }
    let mut config_set = false;
    while index < args.len() {
        let option = &args[index];
        if option == "--config" {
            let value = args
                .get(index + 1)
                .ok_or_else(|| invalid("--config requires a path"))?;
            if value.is_empty() || config_set {
                return Err(invalid("--config requires one nonempty path"));
            }
            invocation.config_path = PathBuf::from(value);
            config_set = true;
            index += 2;
            continue;
        }
        if option == "--target-node" {
            let value = args
                .get(index + 1)
                .ok_or_else(|| invalid("--target-node requires an ID"))?;
            let id = value
                .parse::<u64>()
                .ok()
                .filter(|id| *id > 0)
                .ok_or_else(|| invalid("--target-node requires a positive node ID"))?;
            if invocation.cluster_repair_target.replace(id).is_some() {
                return Err(invalid("--target-node cannot be repeated"));
            }
            index += 2;
            continue;
        }
        return Err(invalid(format!("unknown option {option}; {USAGE}")));
    }
    if (invocation.cluster_repair == Some(ClusterRepair::Run))
        != invocation.cluster_repair_target.is_some()
    {
        return Err(invalid("--target-node is required only for cluster-repair"));
    }
    Ok(Some(invocation))
}

enum Command {
    Export,
    Status,
    Repair(ClusterRepair),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn help_exits_before_reading_configuration() {
        assert!(parse(["--help"]).unwrap().is_none());
        assert!(parse(["-h"]).unwrap().is_none());
        assert!(parse(["export-state", "--help"]).unwrap().is_none());
    }

    #[test]
    fn config_path_defaults_and_commands_are_exclusive() {
        let serve = parse(Vec::<String>::new()).unwrap().unwrap();
        assert_eq!(serve.config_path, PathBuf::from("rmqtt.toml"));
        assert!(!serve.export_state);
        let exported = parse(["export-state", "--config", "node.toml"])
            .unwrap()
            .unwrap();
        assert!(exported.export_state);
        assert_eq!(exported.config_path, PathBuf::from("node.toml"));
        let repair = parse(["cluster-repair", "--target-node", "2"])
            .unwrap()
            .unwrap();
        assert_eq!(repair.cluster_repair, Some(ClusterRepair::Run));
        assert_eq!(repair.cluster_repair_target, Some(2));
    }

    #[test]
    fn removed_flags_and_incomplete_repair_are_rejected() {
        for input in [
            vec!["--listen", "127.0.0.1:1883"],
            vec!["--cluster-config", "broker.json"],
            vec!["--config"],
            vec!["--config", ""],
            vec!["--config", "a", "--config", "b"],
            vec!["cluster-repair"],
            vec!["cluster-status", "--target-node", "1"],
            vec!["cluster-repair", "--target-node", "0"],
            vec!["--target-node", "1", "--target-node", "2"],
        ] {
            assert!(parse(input.clone()).is_err(), "{input:?}");
        }
    }
}
