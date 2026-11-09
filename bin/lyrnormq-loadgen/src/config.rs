use crate::{
    LoadError,
    model::{RunConfig, Version},
};
use std::{collections::BTreeSet, path::PathBuf};
pub const USAGE: &str = "lyrnormq-loadgen [--addr IP:PORT | --addrs IP:PORT,IP:PORT,...] [--protocol 5|3.1.1] [--qos 0|1|2] [--pairs N] [--setup-concurrency 1..1024] [--payload-bytes N] [--rate MESSAGES_PER_SECOND (0=closed-loop)] [--duration-ms N] [--warmup-ms N] [--drain-timeout-ms N] [--connect-timeout-ms N] [--io-timeout-ms N] [--inflight N] [--max-messages N] [--max-packet-bytes N] [--output FILE] [--raw FILE]";
pub struct Options {
    pub config: RunConfig,
    pub output: PathBuf,
    pub raw: Option<PathBuf>,
}
pub fn validate(c: &RunConfig) -> Result<(), LoadError> {
    let bad = |s: &str| LoadError::Config(s.into());
    if c.addresses.len() > 32
        || c.addresses.iter().collect::<BTreeSet<_>>().len() != c.addresses.len()
    {
        return Err(bad("addrs must contain at most32 distinct endpoints"));
    }
    if c.pairs == 0 || c.pairs > 60000 {
        return Err(bad("pairs must be1..60000"));
    }
    if !(1..=1024).contains(&c.setup_concurrency) {
        return Err(bad("setup-concurrency must be1..1024"));
    }
    if c.qos > 2 {
        return Err(bad("qos must be0..2"));
    }
    if c.payload_bytes < 48
        || c.payload_bytes
            .checked_add(512)
            .is_none_or(|v| v > c.max_packet_bytes)
    {
        return Err(bad(
            "payload must be>=48 and leave512 bytes inside packet bound",
        ));
    }
    if c.max_packet_bytes > 16 * 1024 * 1024 {
        return Err(bad("max-packet-bytes exceeds16MiB"));
    }
    if c.duration_ms == 0
        || c.duration_ms > 3_600_000
        || c.warmup_ms > 600_000
        || c.drain_timeout_ms == 0
        || c.drain_timeout_ms > 600_000
        || c.connect_timeout_ms == 0
        || c.io_timeout_ms == 0
    {
        return Err(bad("invalid or unbounded duration/timeout"));
    }
    if c.connect_timeout_ms > 600_000 || c.io_timeout_ms > 600_000 {
        return Err(bad("I/O timeouts cannot exceed600000ms"));
    }
    if c.inflight == 0 || c.inflight > 1024 {
        return Err(bad("inflight must be1..1024"));
    }
    if c.max_messages < c.pairs as u64 || c.max_messages > 20_000_000 {
        return Err(bad("max-messages must cover all pairs and be<=20000000"));
    }
    if c.rate > 0
        && u128::from(c.rate) * u128::from(c.duration_ms) > u128::from(c.max_messages) * 1000
    {
        return Err(bad("rate*duration exceeds max-messages sample budget"));
    }
    Ok(())
}
pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Option<Options>, LoadError> {
    let mut c = RunConfig::default();
    let mut output = PathBuf::from("loadgen-result.json");
    let mut raw = None;
    let mut it = args.into_iter();
    let mut address_seen = false;
    while let Some(k) = it.next() {
        if k == "--help" || k == "-h" {
            return Ok(None);
        }
        let v = it
            .next()
            .ok_or_else(|| LoadError::Config(format!("missing value for {k}")))?;
        let n = || {
            v.parse::<u64>()
                .map_err(|_| LoadError::Config(format!("invalid integer for {k}")))
        };
        let size = || {
            usize::try_from(n()?)
                .map_err(|_| LoadError::Config(format!("{k} exceeds platform size")))
        };
        match k.as_str() {
            "--addr" | "--addrs" => {
                if std::mem::replace(&mut address_seen, true) {
                    return Err(LoadError::Config("specify --addr or --addrs once".into()));
                }
                let parse = |value: &str| {
                    value
                        .parse()
                        .map_err(|_| LoadError::Config("address must be numeric IP:PORT".into()))
                };
                if k == "--addr" {
                    c.address = parse(&v)?;
                } else {
                    if v.is_empty() || v.split(',').count() > 32 {
                        return Err(LoadError::Config("addrs requires1..32 endpoints".into()));
                    }
                    c.addresses = v.split(',').map(parse).collect::<Result<_, _>>()?;
                }
            }
            "--protocol" => {
                c.version = match v.as_str() {
                    "5" => Version::V5,
                    "3.1.1" => Version::V311,
                    _ => return Err(LoadError::Config("protocol must be5 or3.1.1".into())),
                }
            }
            "--qos" => {
                c.qos = u8::try_from(n()?).map_err(|_| LoadError::Config("qos overflow".into()))?
            }
            "--pairs" => c.pairs = size()?,
            "--setup-concurrency" => c.setup_concurrency = size()?,
            "--payload-bytes" => c.payload_bytes = size()?,
            "--rate" => c.rate = n()?,
            "--duration-ms" => c.duration_ms = n()?,
            "--warmup-ms" => c.warmup_ms = n()?,
            "--drain-timeout-ms" => c.drain_timeout_ms = n()?,
            "--connect-timeout-ms" => c.connect_timeout_ms = n()?,
            "--io-timeout-ms" => c.io_timeout_ms = n()?,
            "--inflight" => {
                c.inflight = u16::try_from(n()?)
                    .map_err(|_| LoadError::Config("inflight overflow".into()))?
            }
            "--max-messages" => c.max_messages = n()?,
            "--max-packet-bytes" => c.max_packet_bytes = size()?,
            "--output" => output = v.into(),
            "--raw" => raw = Some(PathBuf::from(v)),
            _ => return Err(LoadError::Config(format!("unknown option {k}"))),
        }
    }
    validate(&c)?;
    if raw.as_ref().is_some_and(|p| *p == output) {
        return Err(LoadError::Config("output and raw paths must differ".into()));
    }
    Ok(Some(Options {
        config: c,
        output,
        raw,
    }))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn multiple_endpoint_cli_is_additive_and_unambiguous() {
        let arguments = ["--addrs", "127.0.0.1:1883,127.0.0.1:1884,127.0.0.1:1885"];
        let options = parse(arguments.map(str::to_owned)).unwrap().unwrap();
        let value = serde_json::to_value(options.config).unwrap();
        assert_eq!(value["addresses"].as_array().unwrap().len(), 3);
        for arguments in [
            vec!["--addrs", ""],
            vec!["--addrs", "127.0.0.1:1883,"],
            vec!["--addrs", "127.0.0.1:1883,127.0.0.1:1883"],
            vec!["--addrs", "localhost:1883"],
            vec!["--addr", "127.0.0.1:1883", "--addrs", "127.0.0.1:1884"],
            vec!["--addrs", "127.0.0.1:1884", "--addr", "127.0.0.1:1883"],
            vec!["--addrs", "127.0.0.1:1884", "--addrs", "127.0.0.1:1885"],
        ] {
            assert!(parse(arguments.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn endpoint_bounds_and_legacy_configuration_roundtrip() {
        let mut c = RunConfig::default();
        let mut old = serde_json::to_value(&c).unwrap();
        old.as_object_mut().unwrap().remove("addresses");
        let restored: RunConfig = serde_json::from_value(old).unwrap();
        assert!(restored.addresses.is_empty());
        assert_eq!(restored.pair_endpoints(7), (c.address, c.address));
        c.addresses = (1..=32)
            .map(|port| format!("127.0.0.1:{port}").parse().unwrap())
            .collect();
        assert!(validate(&c).is_ok());
        c.addresses.push("127.0.0.1:33".parse().unwrap());
        assert!(validate(&c).is_err());
        c.addresses.truncate(2);
        c.addresses[1] = c.addresses[0];
        assert!(validate(&c).is_err());
        let addresses = (1..=33)
            .map(|port| format!("127.0.0.1:{port}"))
            .collect::<Vec<_>>()
            .join(",");
        assert!(parse(["--addrs".to_owned(), addresses]).is_err());
    }

    #[test]
    fn setup_concurrency_is_explicit_bounded_and_defaults_for_legacy_reports() {
        let options = parse(["--setup-concurrency", "1"].map(str::to_owned))
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(options.config).unwrap()["setup_concurrency"],
            1
        );
        for value in ["0", "1025", "18446744073709551615"] {
            assert!(parse(["--setup-concurrency".to_owned(), value.to_owned()]).is_err());
        }
        let mut legacy = serde_json::to_value(RunConfig::default()).unwrap();
        legacy.as_object_mut().unwrap().remove("setup_concurrency");
        let config: RunConfig = serde_json::from_value(legacy).unwrap();
        assert_eq!(
            serde_json::to_value(config).unwrap()["setup_concurrency"],
            32
        );
    }

    #[test]
    fn limits() {
        assert!(validate(&RunConfig::default()).is_ok());
        let mut c = RunConfig {
            pairs: 0,
            ..RunConfig::default()
        };
        assert!(validate(&c).is_err());
        c.pairs = 1;
        c.rate = u64::MAX;
        assert!(validate(&c).is_err());
        assert!(parse(vec!["--qos".into(), "256".into()]).is_err());
        assert!(
            parse(vec![
                "--raw".into(),
                "same".into(),
                "--output".into(),
                "same".into()
            ])
            .is_err()
        );
    }
}
