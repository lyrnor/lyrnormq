use mqtt_transport::BrokerConfig;
use std::{io, path::PathBuf, time::Duration};

pub const USAGE: &str = "usage: rmqtt [--listen ADDRESS] [--max-packet-size BYTES] [--max-connections N] [--outbound-capacity N] [--max-subscriptions N] [--max-inflight 1..65535] [--handshake-timeout-ms N] [--write-timeout-ms N] [--storage-path PATH] [--max-sessions N] [--max-state-bytes N] [--max-retained N]";

pub struct Options {
    pub address: String,
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
        config: BrokerConfig::default(),
    };
    while let Some(option) = args.next() {
        if option == "--help" || option == "-h" {
            return Ok(None);
        }
        if !matches!(
            option.as_str(),
            "--listen"
                | "--storage-path"
                | "--max-sessions"
                | "--max-state-bytes"
                | "--max-retained"
                | "--max-packet-size"
                | "--max-connections"
                | "--outbound-capacity"
                | "--max-subscriptions"
                | "--max-inflight"
                | "--handshake-timeout-ms"
                | "--write-timeout-ms"
        ) {
            return Err(invalid(format!("unknown option {option}; {USAGE}")));
        }
        let value = args
            .next()
            .ok_or_else(|| invalid(format!("missing value for {option}")))?;
        if option == "--listen" {
            options.address = value;
            continue;
        }
        if option == "--storage-path" {
            if value.is_empty() {
                return Err(invalid("--storage-path cannot be empty"));
            }
            options.config.storage_path = Some(PathBuf::from(value));
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
    Ok(Some(options))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).into()).collect()
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
            vec!["--max-sessions", "0"],
            vec!["--max-state-bytes", "0"],
            vec!["--max-retained", "0"],
        ] {
            assert!(parse(args(&values)).is_err(), "{values:?}");
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
}
