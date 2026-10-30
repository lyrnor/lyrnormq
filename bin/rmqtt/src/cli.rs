use mqtt_transport::{BrokerConfig, ProtocolMode, WebSocketOptions};
use std::{io, path::PathBuf, time::Duration};

pub const USAGE: &str = "usage: rmqtt [export-state] [--listen ADDRESS] [--tls-cert PATH --tls-key PATH] [--tls-client-ca PATH] [--websocket] [--ws-path PATH] [--ws-max-message-size BYTES] [--protocol both|3.1.1|5] [--max-packet-size BYTES] [--max-connections N] [--max-handshakes N] [--max-auth-tasks N] [--auth-rate-per-second N] [--auth-burst N] [--outbound-capacity N] [--max-subscriptions N] [--max-inflight 1..65535] [--topic-alias-maximum 0..65535] [--handshake-timeout-ms N] [--write-timeout-ms N] [--storage-path PATH] [--max-sessions N] [--max-state-bytes N] [--max-retained N] [--auth-policy PATH] [--enhanced-auth-plain] [--admin-listen IP:PORT --admin-token-file PATH] [--dashboard-dir PATH]";

pub struct Options {
    pub address: String,
    pub admin_listen: Option<std::net::SocketAddr>,
    pub admin_token_file: Option<PathBuf>,
    pub dashboard_dir: Option<PathBuf>,
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

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn positive(value: &str, option: &str) -> io::Result<usize> {
    let number = value
        .parse::<usize>()
        .map_err(|_| invalid(format!("{option} requires a positive integer")))?;
    if number == 0 {
        return Err(invalid(format!("{option} must be positive")));
    }
    Ok(number)
}

pub fn parse(args: impl IntoIterator<Item = String>) -> io::Result<Option<Options>> {
    let mut args = args.into_iter();
    let mut options = Options {
        address: "127.0.0.1:1883".into(),
        admin_listen: None,
        admin_token_file: None,
        dashboard_dir: None,
        tls_cert: None,
        tls_key: None,
        tls_client_ca: None,
        websocket: false,
        websocket_options: WebSocketOptions::default(),
        auth_policy: None,
        enhanced_auth_plain: false,
        export_state: false,
        config: BrokerConfig::default(),
    };
    let mut first = true;
    let mut ws_path_set = false;
    let mut ws_limit_set = false;
    let mut admission_seen = [false; 4];
    while let Some(option) = args.next() {
        if std::mem::take(&mut first) && option == "export-state" {
            options.export_state = true;
            continue;
        }
        if option == "--help" || option == "-h" {
            return Ok(None);
        }
        if option == "--enhanced-auth-plain" {
            options.enhanced_auth_plain = true;
            continue;
        }
        if option == "--websocket" {
            if options.websocket {
                return Err(invalid("--websocket cannot be repeated"));
            }
            options.websocket = true;
            continue;
        }
        if !matches!(
            option.as_str(),
            "--listen"
                | "--admin-listen"
                | "--admin-token-file"
                | "--dashboard-dir"
                | "--tls-cert"
                | "--tls-key"
                | "--tls-client-ca"
                | "--max-handshakes"
                | "--max-auth-tasks"
                | "--auth-rate-per-second"
                | "--auth-burst"
                | "--ws-path"
                | "--ws-max-message-size"
                | "--protocol"
                | "--auth-policy"
                | "--storage-path"
                | "--max-sessions"
                | "--max-state-bytes"
                | "--max-retained"
                | "--max-packet-size"
                | "--max-connections"
                | "--outbound-capacity"
                | "--max-subscriptions"
                | "--max-inflight"
                | "--topic-alias-maximum"
                | "--handshake-timeout-ms"
                | "--write-timeout-ms"
        ) {
            return Err(invalid(format!("unknown option {option}; {USAGE}")));
        }
        let value = args
            .next()
            .ok_or_else(|| invalid(format!("missing value for {option}")))?;
        match option.as_str() {
            "--admin-listen" => {
                let address: std::net::SocketAddr = value
                    .parse()
                    .map_err(|_| invalid("--admin-listen requires a numeric loopback IP:PORT"))?;
                if !address.ip().is_loopback() || options.admin_listen.replace(address).is_some() {
                    return Err(invalid("--admin-listen requires one loopback IP:PORT"));
                }
                continue;
            }
            "--admin-token-file" | "--dashboard-dir" => {
                let target = if option == "--admin-token-file" {
                    &mut options.admin_token_file
                } else {
                    &mut options.dashboard_dir
                };
                if value.is_empty() || target.replace(PathBuf::from(value)).is_some() {
                    return Err(invalid(
                        "management paths must be nonempty and specified once",
                    ));
                }
                continue;
            }
            _ => {}
        }
        let admission_index = match option.as_str() {
            "--max-handshakes" => Some(0),
            "--max-auth-tasks" => Some(1),
            "--auth-rate-per-second" => Some(2),
            "--auth-burst" => Some(3),
            _ => None,
        };
        if let Some(index) = admission_index {
            if std::mem::replace(&mut admission_seen[index], true) {
                return Err(invalid(format!("{option} cannot be repeated")));
            }
            match index {
                0 | 1 => {
                    let limit = positive(&value, &option)?;
                    if limit > tokio::sync::Semaphore::MAX_PERMITS {
                        return Err(invalid(format!("{option} exceeds semaphore capacity")));
                    }
                    if index == 0 {
                        options.config.max_handshakes = limit;
                    } else {
                        options.config.max_auth_tasks = limit;
                    }
                }
                2 | 3 => {
                    let limit = value
                        .parse::<u32>()
                        .map_err(|_| invalid(format!("{option} requires a u32 integer")))?;
                    if index == 2 {
                        options.config.auth_rate_per_second = limit;
                    } else {
                        if limit == 0 {
                            return Err(invalid("--auth-burst must be positive"));
                        }
                        options.config.auth_burst = limit;
                    }
                }
                _ => return Err(invalid("unrecognized admission option")),
            }
            continue;
        }
        if option == "--listen" {
            options.address = value;
            continue;
        }
        if option == "--ws-path" {
            if ws_path_set
                || !value.starts_with('/')
                || value.len() > 1024
                || value
                    .chars()
                    .any(|ch| ch.is_whitespace() || ch.is_control() || ch == '?' || ch == '#')
            {
                return Err(invalid(
                    "--ws-path requires one absolute path of at most 1024 bytes without whitespace, query or fragment",
                ));
            }
            ws_path_set = true;
            options.websocket_options.path = value;
            continue;
        }
        if option == "--ws-max-message-size" {
            let size = positive(&value, &option)?;
            if ws_limit_set || size > 16 * 1024 * 1024 {
                return Err(invalid(
                    "--ws-max-message-size requires one value in 1..=16777216",
                ));
            }
            ws_limit_set = true;
            options.websocket_options.max_message_size = size;
            continue;
        }
        if matches!(
            option.as_str(),
            "--tls-cert" | "--tls-key" | "--tls-client-ca"
        ) {
            let slot = match option.as_str() {
                "--tls-cert" => &mut options.tls_cert,
                "--tls-key" => &mut options.tls_key,
                _ => &mut options.tls_client_ca,
            };
            if value.is_empty() || slot.is_some() {
                return Err(invalid(format!("{option} requires one nonempty path")));
            }
            *slot = Some(PathBuf::from(value));
            continue;
        }
        if option == "--protocol" {
            options.config.protocol_mode = match value.as_str() {
                "both" => ProtocolMode::Both,
                "3.1.1" => ProtocolMode::V311,
                "5" => ProtocolMode::V5,
                _ => return Err(invalid("--protocol must be both, 3.1.1 or 5")),
            };
            continue;
        }
        if option == "--auth-policy" {
            if value.is_empty() {
                return Err(invalid("--auth-policy cannot be empty"));
            }
            options.auth_policy = Some(PathBuf::from(value));
            continue;
        }
        if option == "--storage-path" {
            if value.is_empty() {
                return Err(invalid("--storage-path cannot be empty"));
            }
            options.config.storage_path = Some(PathBuf::from(value));
            continue;
        }
        if option == "--topic-alias-maximum" {
            options.config.topic_alias_maximum = value
                .parse::<u16>()
                .map_err(|_| invalid("--topic-alias-maximum must be in 0..65535"))?;
            continue;
        }
        let number = positive(&value, &option)?;
        match option.as_str() {
            "--max-packet-size" => {
                if number < 2 {
                    return Err(invalid("--max-packet-size must be at least 2"));
                }
                options.config.max_packet_size = number;
            }
            "--max-retained" => options.config.max_retained = number,
            "--max-sessions" => options.config.max_sessions = number,
            "--max-state-bytes" => options.config.max_state_bytes = number,
            "--max-connections" => options.config.max_connections = number,
            "--outbound-capacity" => options.config.outbound_capacity = number,
            "--max-subscriptions" => options.config.max_subscriptions = number,
            "--max-inflight" => {
                if number > u16::MAX as usize {
                    return Err(invalid("--max-inflight must be in 1..65535"));
                }
                options.config.max_inflight = number;
            }
            "--handshake-timeout-ms" | "--write-timeout-ms" => {
                let millis = u64::try_from(number).map_err(|_| invalid("timeout exceeds u64"))?;
                let duration = Duration::from_millis(millis);
                if option == "--handshake-timeout-ms" {
                    options.config.handshake_timeout = duration;
                } else {
                    options.config.write_timeout = duration;
                }
            }
            _ => return Err(invalid("unrecognized numeric option")),
        }
    }
    if (ws_path_set || ws_limit_set) && !options.websocket {
        return Err(invalid("WebSocket options require --websocket"));
    }
    if options.export_state && options.websocket {
        return Err(invalid(
            "WebSocket options cannot be used with export-state",
        ));
    }
    if options.tls_cert.is_some() != options.tls_key.is_some() {
        return Err(invalid(
            "--tls-cert and --tls-key must be supplied together",
        ));
    }
    if options.tls_client_ca.is_some() && options.tls_cert.is_none() {
        return Err(invalid("--tls-client-ca requires --tls-cert and --tls-key"));
    }
    if options.export_state && options.tls_cert.is_some() {
        return Err(invalid("TLS options cannot be used with export-state"));
    }
    if options.export_state && options.config.storage_path.is_none() {
        return Err(invalid("export-state requires --storage-path"));
    }
    if options.admin_listen.is_some() != options.admin_token_file.is_some()
        || (options.dashboard_dir.is_some() && options.admin_listen.is_none())
        || (options.export_state && options.admin_listen.is_some())
    {
        return Err(invalid(
            "management requires --admin-listen and --admin-token-file, and cannot combine with export-state",
        ));
    }
    Ok(Some(options))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).into()).collect()
    }

    #[test]
    fn management_is_opt_in_loopback_and_requires_complete_options() {
        assert!(parse(args(&[])).unwrap().unwrap().admin_listen.is_none());
        let good = [
            "--admin-listen",
            "127.0.0.1:0",
            "--admin-token-file",
            "token",
        ];
        assert!(
            parse(args(&good))
                .unwrap()
                .unwrap()
                .admin_listen
                .unwrap()
                .ip()
                .is_loopback()
        );
        for values in [
            vec![
                "--admin-listen",
                "0.0.0.0:8080",
                "--admin-token-file",
                "token",
            ],
            vec![
                "--admin-listen",
                "localhost:8080",
                "--admin-token-file",
                "token",
            ],
            vec!["--admin-listen", "127.0.0.1:8080"],
            vec!["--admin-token-file", "token"],
            vec!["--dashboard-dir", "dist"],
            vec!["--admin-listen", "127.0.0.1:0", "--admin-token-file", ""],
            vec![
                "--admin-listen",
                "127.0.0.1:0",
                "--admin-listen",
                "127.0.0.1:1",
                "--admin-token-file",
                "token",
            ],
            vec![
                "--admin-listen",
                "127.0.0.1:0",
                "--admin-token-file",
                "a",
                "--admin-token-file",
                "b",
            ],
            vec![
                "--admin-listen",
                "127.0.0.1:0",
                "--admin-token-file",
                "a",
                "--dashboard-dir",
                "a",
                "--dashboard-dir",
                "b",
            ],
            vec![
                "export-state",
                "--storage-path",
                "s",
                "--admin-listen",
                "127.0.0.1:0",
                "--admin-token-file",
                "token",
            ],
        ] {
            assert!(parse(args(&values)).is_err(), "{values:?}");
        }
    }

    #[test]
    fn admission_limits_have_explicit_defaults_and_rate_can_be_disabled() {
        let default = parse(args(&[])).unwrap().unwrap().config;
        assert_eq!(default.max_handshakes, 128);
        assert_eq!(default.max_auth_tasks, 4);
        assert_eq!(default.auth_rate_per_second, 1000);
        assert_eq!(default.auth_burst, 1000);
        let config = parse(args(&[
            "--max-handshakes",
            "2",
            "--max-auth-tasks",
            "1",
            "--auth-rate-per-second",
            "0",
            "--auth-burst",
            "1",
        ]))
        .unwrap()
        .unwrap()
        .config;
        assert_eq!(config.max_handshakes, 2);
        assert_eq!(config.max_auth_tasks, 1);
        assert_eq!(config.auth_rate_per_second, 0);
        assert_eq!(config.auth_burst, 1);
        for option in [
            "--max-handshakes",
            "--max-auth-tasks",
            "--auth-rate-per-second",
            "--auth-burst",
        ] {
            assert!(parse(args(&[option])).is_err());
            assert!(parse(args(&[option, "1", option, "2"])).is_err());
            assert!(parse(args(&[option, "-1"])).is_err());
            assert!(parse(args(&[option, "184467440737095516160"])).is_err());
        }
        for option in ["--max-handshakes", "--max-auth-tasks", "--auth-burst"] {
            assert!(parse(args(&[option, "0"])).is_err());
        }
        let overflow = (tokio::sync::Semaphore::MAX_PERMITS + 1).to_string();
        assert!(parse(args(&["--max-handshakes", &overflow])).is_err());
        assert!(parse(args(&["--auth-rate-per-second", "4294967296"])).is_err());
    }

    #[test]
    fn client_ca_requires_server_identity_and_rejects_duplicate_or_empty_paths() {
        let options = parse(args(&[
            "--tls-client-ca",
            "ca.pem",
            "--tls-cert",
            "cert.pem",
            "--tls-key",
            "key.pem",
            "--websocket",
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(options.tls_client_ca, Some(PathBuf::from("ca.pem")));
        assert!(options.websocket);
        for values in [
            vec!["--tls-client-ca", "ca.pem"],
            vec!["--tls-client-ca"],
            vec!["--tls-client-ca", "", "--tls-cert", "c", "--tls-key", "k"],
            vec![
                "--tls-client-ca",
                "a",
                "--tls-client-ca",
                "b",
                "--tls-cert",
                "c",
                "--tls-key",
                "k",
            ],
            vec![
                "export-state",
                "--storage-path",
                "state.db",
                "--tls-cert",
                "c",
                "--tls-key",
                "k",
                "--tls-client-ca",
                "a",
            ],
        ] {
            assert!(parse(args(&values)).is_err(), "{values:?}");
        }
    }

    #[test]
    fn websocket_selection_supports_plain_and_tls_without_reading_identity() {
        assert!(!parse(args(&[])).unwrap().unwrap().websocket);
        let ws = parse(args(&["--websocket"])).unwrap().unwrap();
        assert!(ws.websocket);
        assert_eq!(ws.websocket_options.path, "/mqtt");
        assert_eq!(ws.websocket_options.max_message_size, 1024 * 1024);
        assert_eq!(ws.address, "127.0.0.1:1883");
        let wss = parse(args(&[
            "--ws-path",
            "/custom",
            "--websocket",
            "--ws-max-message-size",
            "16777216",
            "--tls-cert",
            "cert.pem",
            "--tls-key",
            "key.pem",
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(wss.websocket_options.path, "/custom");
        assert_eq!(wss.websocket_options.max_message_size, 16 * 1024 * 1024);
        assert!(wss.tls_cert.is_some());
        assert_eq!(
            parse(args(&["--websocket", "--ws-max-message-size", "1"]))
                .unwrap()
                .unwrap()
                .websocket_options
                .max_message_size,
            1
        );
    }

    #[test]
    fn websocket_rejects_incomplete_duplicate_invalid_and_export_configuration() {
        for values in [
            vec!["--websocket", "--websocket"],
            vec!["--ws-path", "/mqtt"],
            vec!["--ws-max-message-size", "1"],
            vec!["--websocket", "--ws-path"],
            vec!["--websocket", "--ws-path", ""],
            vec!["--websocket", "--ws-path", "mqtt"],
            vec!["--websocket", "--ws-path", "/a?b"],
            vec!["--websocket", "--ws-path", "/a#b"],
            vec!["--websocket", "--ws-path", "/a b"],
            vec!["--websocket", "--ws-path", "/a\r\n"],
            vec!["--websocket", "--ws-path", "/a", "--ws-path", "/b"],
            vec!["--websocket", "--ws-max-message-size", "0"],
            vec!["--websocket", "--ws-max-message-size", "-1"],
            vec!["--websocket", "--ws-max-message-size", "16777217"],
            vec![
                "--websocket",
                "--ws-max-message-size",
                "1",
                "--ws-max-message-size",
                "2",
            ],
            vec!["export-state", "--storage-path", "state.db", "--websocket"],
        ] {
            assert!(parse(args(&values)).is_err(), "{values:?}");
        }
        let oversized = format!("/{}", "x".repeat(1024));
        assert!(parse(args(&["--websocket", "--ws-path", &oversized])).is_err());
    }

    #[test]
    fn tls_identity_requires_one_complete_pair_without_reading_files() {
        let plain = parse(args(&[])).unwrap().unwrap();
        assert!(plain.tls_cert.is_none() && plain.tls_key.is_none());
        let tls = parse(args(&["--tls-cert", "cert.pem", "--tls-key", "key.pem"]))
            .unwrap()
            .unwrap();
        assert_eq!(tls.tls_cert, Some(PathBuf::from("cert.pem")));
        assert_eq!(tls.tls_key, Some(PathBuf::from("key.pem")));
        assert_eq!(tls.address, plain.address);
        for values in [
            vec!["--tls-cert", "cert.pem"],
            vec!["--tls-key", "key.pem"],
            vec!["--tls-cert"],
            vec!["--tls-cert", "", "--tls-key", "key.pem"],
            vec!["--tls-cert", "cert.pem", "--tls-key", ""],
            vec!["--tls-cert", "a", "--tls-cert", "b", "--tls-key", "c"],
            vec!["--tls-cert", "a", "--tls-key", "b", "--tls-key", "c"],
            vec![
                "export-state",
                "--storage-path",
                "state.db",
                "--tls-cert",
                "a",
                "--tls-key",
                "b",
            ],
        ] {
            assert!(parse(args(&values)).is_err(), "{values:?}");
        }
    }

    #[test]
    fn rejects_invalid_resource_boundaries_and_missing_values() {
        for values in [
            vec!["--max-inflight", "0"],
            vec!["--max-inflight", "65536"],
            vec!["--max-packet-size", "1"],
            vec!["--write-timeout-ms", "-1"],
            vec!["--max-connections", "184467440737095516160"],
            vec!["--outbound-capacity"],
            vec!["--unknown", "1"],
            vec!["--storage-path", ""],
            vec!["--auth-policy", ""],
            vec!["--auth-policy"],
            vec!["--max-sessions", "0"],
            vec!["--max-state-bytes", "0"],
            vec!["--max-retained", "0"],
            vec!["--topic-alias-maximum", "65536"],
            vec!["--topic-alias-maximum", "-1"],
            vec!["--topic-alias-maximum", "invalid"],
            vec!["--topic-alias-maximum"],
        ] {
            assert!(parse(args(&values)).is_err(), "{values:?}");
        }
    }

    #[test]
    fn topic_alias_budget_defaults_but_can_be_disabled_or_use_full_range() {
        assert_eq!(
            parse(args(&[]))
                .unwrap()
                .unwrap()
                .config
                .topic_alias_maximum,
            32
        );
        for maximum in [0u16, 1, 32, u16::MAX] {
            let value = maximum.to_string();
            let config = parse(args(&["--topic-alias-maximum", &value]))
                .unwrap()
                .unwrap()
                .config;
            assert_eq!(config.topic_alias_maximum, maximum);
        }
    }

    #[test]
    fn persistent_storage_is_opt_in_and_has_explicit_bounds() {
        assert!(
            parse(args(&[]))
                .unwrap()
                .unwrap()
                .config
                .storage_path
                .is_none()
        );
        let config = parse(args(&[
            "--storage-path",
            "/tmp/test.sqlite",
            "--max-sessions",
            "12",
            "--max-state-bytes",
            "65536",
            "--max-retained",
            "128",
        ]))
        .unwrap()
        .unwrap()
        .config;
        assert_eq!(config.storage_path, Some(PathBuf::from("/tmp/test.sqlite")));
        assert_eq!(config.max_sessions, 12);
        assert_eq!(config.max_state_bytes, 65536);
        assert_eq!(config.max_retained, 128);
    }

    #[test]
    fn policy_path_is_opt_in_and_parsed_without_reading_the_file() {
        assert!(parse(args(&[])).unwrap().unwrap().auth_policy.is_none());
        let options = parse(args(&["--auth-policy", "/does/not/exist.json"]))
            .unwrap()
            .unwrap();
        assert_eq!(
            options.auth_policy,
            Some(PathBuf::from("/does/not/exist.json"))
        );
    }

    #[test]
    fn protocol_profile_defaults_to_both_and_requires_an_explicit_valid_value() {
        assert_eq!(
            parse(args(&[])).unwrap().unwrap().config.protocol_mode,
            ProtocolMode::Both
        );
        for (value, expected) in [
            ("both", ProtocolMode::Both),
            ("3.1.1", ProtocolMode::V311),
            ("5", ProtocolMode::V5),
        ] {
            assert_eq!(
                parse(args(&["--protocol", value]))
                    .unwrap()
                    .unwrap()
                    .config
                    .protocol_mode,
                expected
            );
        }
        for values in [
            vec!["--protocol"],
            vec!["--protocol", ""],
            vec!["--protocol", "6"],
            vec!["--protocol", "mqtt5"],
        ] {
            assert!(parse(args(&values)).is_err());
        }
    }

    #[test]
    fn enhanced_plain_is_explicit_and_combines_with_existing_policy() {
        assert!(!parse(args(&[])).unwrap().unwrap().enhanced_auth_plain);
        let options = parse(args(&[
            "--enhanced-auth-plain",
            "--auth-policy",
            "/does/not/exist.json",
        ]))
        .unwrap()
        .unwrap();
        assert!(options.enhanced_auth_plain);
        assert_eq!(
            options.auth_policy,
            Some(PathBuf::from("/does/not/exist.json"))
        );
        assert!(parse(args(&["--enhanced-auth-plain", "unexpected"])).is_err());
    }

    #[test]
    fn supports_limits_without_changing_unset_defaults() {
        let options = parse(args(&[
            "--listen",
            "127.0.0.1:0",
            "--max-inflight",
            "65535",
            "--outbound-capacity",
            "2",
            "--handshake-timeout-ms",
            "250",
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(options.address, "127.0.0.1:0");
        assert_eq!(options.config.max_inflight, 65535);
        assert_eq!(options.config.outbound_capacity, 2);
        assert_eq!(options.config.handshake_timeout, Duration::from_millis(250));
        assert_eq!(
            options.config.max_packet_size,
            BrokerConfig::default().max_packet_size
        );
        assert!(parse(args(&["--help"])).unwrap().is_none());
    }
    #[test]
    fn export_state_is_a_leading_subcommand_requiring_storage() {
        assert!(!parse(args(&[])).unwrap().unwrap().export_state);
        assert!(parse(args(&["export-state"])).is_err());
        assert!(parse(args(&["--storage-path", "x", "export-state"])).is_err());
        let options = parse(args(&["export-state", "--storage-path", "state.db"]))
            .unwrap()
            .unwrap();
        assert!(options.export_state);
        assert_eq!(options.config.storage_path, Some(PathBuf::from("state.db")));
    }
}
