use std::{io, path::PathBuf};

pub const HELP: &str = "egress-log-bench --queue-dir BASE [--phase io|append|read|heads|lifecycle|maintenance|cycle] [--records N] [--payload-bytes N] [--batch N] [--clients N] [--read-rounds N] [--segment-bytes N] [--max-bytes N] [--max-records N] [--keep]\nio measures append, warm reads and metadata heads; setup for read/heads/lifecycle is outside measured stages. All paths are unique children of BASE, never existing queues.";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Io,
    Append,
    Read,
    Heads,
    Lifecycle,
    Maintenance,
    Cycle,
}
#[derive(Debug)]
pub struct Config {
    pub base: PathBuf,
    pub phase: Phase,
    pub records: usize,
    pub payload: usize,
    pub batch: usize,
    pub clients: usize,
    pub read_rounds: usize,
    pub segment: u64,
    pub max_bytes: u64,
    pub max_records: usize,
    pub keep: bool,
}
pub fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
impl Config {
    pub fn parse(args: impl IntoIterator<Item = String>) -> io::Result<Option<Self>> {
        let mut config = Self {
            base: std::env::temp_dir(),
            phase: Phase::Io,
            records: 8192,
            payload: 128,
            batch: 1,
            clients: 0,
            read_rounds: 1,
            segment: 1024 * 1024,
            max_bytes: 512 * 1024 * 1024,
            max_records: 200000,
            keep: false,
        };
        let mut args = args.into_iter();
        let mut seen = std::collections::BTreeSet::new();
        while let Some(option) = args.next() {
            if matches!(option.as_str(), "--help" | "-h") {
                println!("{HELP}");
                return Ok(None);
            }
            if !seen.insert(option.clone()) {
                return Err(invalid(format!("duplicate option {option}")));
            }
            if option == "--keep" {
                config.keep = true;
                continue;
            }
            let value = args
                .next()
                .ok_or_else(|| invalid(format!("missing value for {option}")))?;
            match option.as_str() {
                "--queue-dir" => {
                    if value.is_empty() {
                        return Err(invalid("empty queue base"));
                    }
                    config.base = value.into()
                }
                "--phase" => {
                    config.phase = match value.as_str() {
                        "io" => Phase::Io,
                        "append" => Phase::Append,
                        "read" => Phase::Read,
                        "heads" => Phase::Heads,
                        "lifecycle" => Phase::Lifecycle,
                        "maintenance" => Phase::Maintenance,
                        "cycle" => Phase::Cycle,
                        _ => return Err(invalid("invalid phase")),
                    }
                }
                _ => {
                    let number = value
                        .parse::<u64>()
                        .map_err(|_| invalid(format!("{option} requires an unsigned integer")))?;
                    let as_usize = || {
                        usize::try_from(number)
                            .map_err(|_| invalid("integer exceeds platform limits"))
                    };
                    match option.as_str() {
                        "--records" => config.records = as_usize()?,
                        "--payload-bytes" => config.payload = as_usize()?,
                        "--batch" => config.batch = as_usize()?,
                        "--clients" => config.clients = as_usize()?,
                        "--read-rounds" => config.read_rounds = as_usize()?,
                        "--segment-bytes" => config.segment = number,
                        "--max-bytes" => config.max_bytes = number,
                        "--max-records" => config.max_records = as_usize()?,
                        _ => return Err(invalid(format!("unknown option {option}"))),
                    }
                }
            }
        }
        if !(1..=1_000_000).contains(&config.records)
            || !(1..=1024 * 1024).contains(&config.payload)
            || !(1..=64).contains(&config.batch)
            || !(1..=100).contains(&config.read_rounds)
            || !(1..=1_000_000).contains(&config.max_records)
            || config.clients > config.records
            || !(4096..=64 * 1024 * 1024).contains(&config.segment)
            || config.max_bytes < config.segment * 4
            || config.max_bytes > 1024 * 1024 * 1024 * 1024
        {
            return Err(invalid("out of range benchmark or queue limit"));
        }
        if config.batch.saturating_mul(config.payload) > 8 * 1024 * 1024 {
            return Err(invalid("one group exceeds the journal 8 MiB payload limit"));
        }
        let batches = config.records.div_ceil(config.batch);
        if config.clients == 0 {
            config.clients = batches
        }
        let read_calls = config
            .clients
            .min(batches)
            .checked_mul(config.read_rounds)
            .ok_or_else(|| invalid("sample count overflow"))?;
        if read_calls > 2_000_000 {
            return Err(invalid("read samples exceed 2000000"));
        }
        let resident = if config.phase == Phase::Cycle {
            config.batch.min(config.records)
        } else {
            config.records
        };
        if resident > config.max_records {
            return Err(invalid("dataset exceeds max-records"));
        }
        let conservative = (resident as u64)
            .checked_mul(config.payload as u64 + 1400)
            .ok_or_else(|| invalid("dataset size overflow"))?;
        if conservative + 8192 > config.max_bytes / 2 {
            return Err(invalid(
                "dataset will exceed the journal live-byte reservation; increase --max-bytes or reduce records",
            ));
        }
        Ok(Some(config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parse(values: &[&str]) -> io::Result<Option<Config>> {
        Config::parse(values.iter().map(|v| v.to_string()))
    }
    #[test]
    fn rejects_oversized_work_and_preserves_parent_topology() {
        assert!(parse(&["--batch", "65"]).is_err());
        assert!(parse(&["--records", "1000000", "--read-rounds", "100"]).is_err());
        assert!(parse(&["--queue-dir", "a", "--queue-dir", "b"]).is_err());
        let c = parse(&[
            "--records",
            "65536",
            "--batch",
            "64",
            "--payload-bytes",
            "1024",
        ])
        .unwrap()
        .unwrap();
        assert_eq!(c.clients, 1024);
        assert_eq!(c.max_bytes, 512 * 1024 * 1024);
    }
}
