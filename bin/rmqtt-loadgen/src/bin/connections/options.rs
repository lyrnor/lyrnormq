//! Additive options for one shard of a synchronized connection ramp.
use super::{default_prefix, failure};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    io,
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, Instant},
};

pub(super) struct Options {
    pub endpoints: Vec<SocketAddr>,
    pub rate: u64,
    pub output: PathBuf,
    pub plateaus: u64,
    pub connections_per_step: u64,
    pub client_prefix: String,
    pub index_offset: u64,
    pub start_unix_ms: Option<u64>,
    pub ping_unix_ms: Option<u64>,
    pub raw: Option<PathBuf>,
}
#[derive(Clone, Copy)]
pub(super) struct SharedSchedule {
    pub start: Instant,
    pub ping: Instant,
}
impl Options {
    pub fn parse(
        args: impl IntoIterator<Item = String>,
        stamp: u64,
        process: u32,
    ) -> io::Result<Self> {
        let mut args = args.into_iter().peekable();
        let endpoints = args
            .next()
            .ok_or_else(|| failure("need endpoints rate output"))?
            .split(',')
            .map(str::parse)
            .collect::<Result<Vec<SocketAddr>, _>>()
            .map_err(|_| failure("invalid endpoint"))?;
        let rate = parse_number(args.next(), "need numeric rate")?;
        let output = PathBuf::from(args.next().ok_or_else(|| failure("need output"))?);
        let plateaus = if args.peek().is_some_and(|s| !s.starts_with("--")) {
            parse_number(args.next(), "invalid plateau count")?
        } else {
            10
        };
        let mut options = Self {
            endpoints,
            rate,
            output,
            plateaus,
            connections_per_step: 10000,
            client_prefix: default_prefix(stamp, process),
            index_offset: 0,
            start_unix_ms: None,
            ping_unix_ms: None,
            raw: None,
        };
        let mut seen = BTreeSet::new();
        while let Some(flag) = args.next() {
            if !seen.insert(flag.clone()) {
                return Err(failure("duplicate option"));
            }
            let value = args
                .next()
                .ok_or_else(|| failure("option requires value"))?;
            match flag.as_str() {
                "--connections-per-step" => {
                    options.connections_per_step =
                        parse_number(Some(value), "invalid connections-per-step")?
                }
                "--client-prefix" => options.client_prefix = value,
                "--index-offset" => {
                    options.index_offset = parse_number(Some(value), "invalid index-offset")?
                }
                "--start-unix-ms" => {
                    options.start_unix_ms =
                        Some(parse_number(Some(value), "invalid start-unix-ms")?)
                }
                "--ping-unix-ms" => {
                    options.ping_unix_ms = Some(parse_number(Some(value), "invalid ping-unix-ms")?)
                }
                "--raw" => options.raw = Some(PathBuf::from(value)),
                _ => return Err(failure("unknown option")),
            }
        }
        options.validate()?;
        Ok(options)
    }
    fn validate(&self) -> io::Result<()> {
        if self.endpoints.len() != 3
            || !(1..=10000).contains(&self.rate)
            || !(1..=10).contains(&self.plateaus)
        {
            return Err(failure("need3 endpoints, rate1..10000, plateaus1..10"));
        }
        if !(1..=100000).contains(&self.connections_per_step)
            || !matches!(self.connections_per_step.checked_mul(self.plateaus),Some(n) if n<=100000)
        {
            return Err(failure("connections-per-step * plateaus must be1..100000"));
        }
        let count = self.connections_per_step * self.plateaus;
        self.index_offset
            .checked_add(count - 1)
            .ok_or_else(|| failure("client index overflow"))?;
        if self.client_prefix.is_empty()
            || self.client_prefix.len() > 48
            || !self
                .client_prefix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(failure(
                "client-prefix must be1..48 ASCII alphanumeric/_/- characters",
            ));
        }
        if self.raw.as_ref().is_some_and(|raw| raw == &self.output) {
            return Err(failure("raw and report must have distinct paths"));
        }
        match (self.start_unix_ms, self.ping_unix_ms) {
            (None, None) => {}
            (Some(start), Some(ping))
                if self.plateaus == 1
                    && matches!(ping.checked_sub(start),Some(ms) if (10000..=600000).contains(&ms)) =>
            {
                let latest = Duration::from_secs_f64(
                    (self.connections_per_step - 1) as f64 / self.rate as f64,
                );
                if latest > Duration::from_millis(ping - start - 10000) {
                    return Err(failure("planned arrivals cannot leave a10s shared hold"));
                }
            }
            _ => {
                return Err(failure(
                    "shared start/PING require1plateau and10000..600000ms separation",
                ));
            }
        }
        Ok(())
    }
    /// Capture the wall/monotonic correspondence once, before opening MQTT traffic.
    pub fn schedule(&self, wall_ms: u64, monotonic: Instant) -> io::Result<Option<SharedSchedule>> {
        match (self.start_unix_ms, self.ping_unix_ms) {
            (Some(start), Some(ping)) => {
                let delay = start
                    .checked_sub(wall_ms)
                    .filter(|v| (1..=60000).contains(v))
                    .ok_or_else(|| failure("shared start is stale or more than60s away"))?;
                let ping_delay = ping
                    .checked_sub(wall_ms)
                    .ok_or_else(|| failure("stale shared PING"))?;
                Ok(Some(SharedSchedule {
                    start: monotonic
                        .checked_add(Duration::from_millis(delay))
                        .ok_or_else(|| failure("start deadline overflow"))?,
                    ping: monotonic
                        .checked_add(Duration::from_millis(ping_delay))
                        .ok_or_else(|| failure("PING deadline overflow"))?,
                }))
            }
            (None, None) => Ok(None),
            _ => Err(failure("shared start/PING must be supplied together")),
        }
    }
    pub fn index(&self, step: u64, index: u64) -> io::Result<u64> {
        step.checked_sub(1)
            .and_then(|v| v.checked_mul(self.connections_per_step))
            .and_then(|v| v.checked_add(index))
            .and_then(|v| v.checked_add(self.index_offset))
            .ok_or_else(|| failure("client index overflow"))
    }
    pub fn endpoint(&self, index: u64) -> SocketAddr {
        self.endpoints[(index % self.endpoints.len() as u64) as usize]
    }
    pub fn client_id(&self, index: u64) -> String {
        format!("{}{index:06}", self.client_prefix)
    }
    pub fn settings(&self) -> Value {
        json!({"endpoints":self.endpoints.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "rate":self.rate,"requested_plateaus":self.plateaus,
            "connections_per_step":self.connections_per_step,"client_prefix":self.client_prefix,
            "index_offset":self.index_offset,"requested_start_unix_ms":self.start_unix_ms,
            "requested_ping_unix_ms":self.ping_unix_ms,"max_concurrent_handshakes":2048,"timeout_seconds":15,
            "wall_timestamp_basis":"shared: requested_start + monotonic elapsed; independent: SystemTime"})
    }
}
fn parse_number(value: Option<String>, message: &str) -> io::Result<u64> {
    value
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| failure(message))
}
