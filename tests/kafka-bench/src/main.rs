//! Finite direct-adapter load. Independent readback is performed by the runner.
use mqtt_egress_core::Sink;
use mqtt_kafka::{Compression, KafkaConfig, KafkaSink};
use serde_json::json;
use std::{collections::BTreeMap, error::Error, io, sync::Arc, time::Instant};
use tokio::{sync::Barrier, task::JoinSet};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
#[derive(Clone)]
struct Options {
    broker: String,
    topic: String,
    run: String,
    clients: usize,
    records: usize,
    payload: usize,
    timeout_ms: u64,
    warmup: usize,
}
impl Options {
    fn parse(args: &[String]) -> Result<Self> {
        if !(7..=8).contains(&args.len()) {
            return Err(invalid(
                "usage: BROKER TOPIC RUN_ID CLIENTS RECORDS_PER_CLIENT PAYLOAD_BYTES TIMEOUT_MS [WARMUP_RECORDS]",
            )
            .into());
        }
        let options = Self {
            broker: args[0].clone(),
            topic: args[1].clone(),
            run: args[2].clone(),
            clients: args[3].parse()?,
            records: args[4].parse()?,
            payload: args[5].parse()?,
            timeout_ms: args[6].parse()?,
            warmup: args.get(7).map(|v| v.parse()).transpose()?.unwrap_or(0),
        };
        if !(1..=512).contains(&options.clients)
            || !(1..=10000).contains(&options.records)
            || options
                .clients
                .saturating_mul(options.records)
                .saturating_add(options.warmup)
                > 10000
            || options.warmup > 512
            || !(128..=65536).contains(&options.payload)
            || (options.clients * options.records + options.warmup) * options.payload
                > 12 * 1024 * 1024
            || !(1000..=30000).contains(&options.timeout_ms)
            || options.run.is_empty()
            || options.run.len() > 64
            || !options
                .run
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(invalid("workload exceeds finite benchmark bounds").into());
        }
        Ok(options)
    }
}
fn payload(client: usize, sequence: usize, size: usize) -> Result<String> {
    let mut value = json!({"client":client,"sequence":sequence,"padding":""});
    let base = serde_json::to_string(&value)?.len();
    let padding = size
        .checked_sub(base)
        .ok_or_else(|| invalid("payload too small"))?;
    value["padding"] = json!("x".repeat(padding));
    let encoded = serde_json::to_string(&value)?;
    if encoded.len() != size {
        return Err(invalid("payload construction mismatch").into());
    }
    Ok(encoded)
}
fn event(options: &Options, client: usize, sequence: usize) -> Result<Vec<u8>> {
    let body = payload(client, sequence, options.payload)?;
    let data: &serde_json::value::RawValue = serde_json::from_str(&body)?;
    Ok(serde_json::to_vec(&json!({
        "event_id":format!("{}-{client}-{sequence}",options.run),
        "delivery_id":format!("{}-{client}-{sequence}",options.run),
        "rule_id":"benchmark-rule", "client_id":format!("bench-client-{client:04}"),
        "topic":"bench/source", "timestamp_ms":123456789,
        "data":data,
    }))?)
}
fn percentile(samples: &[u64], numerator: usize, denominator: usize) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let rank = samples
        .len()
        .saturating_mul(numerator)
        .div_ceil(denominator);
    samples[rank.saturating_sub(1).min(samples.len() - 1)]
}
#[derive(Default)]
struct ClientResult {
    attempted: usize,
    confirmed_ns: Vec<u64>,
    errors: BTreeMap<String, usize>,
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let options = Options::parse(&std::env::args().skip(1).collect::<Vec<_>>())?;
    let sink = Arc::new(KafkaSink::new(
        "adapter-bench",
        KafkaConfig {
            brokers: vec![options.broker.clone()],
            topic: options.topic.clone(),
            compression: Compression::Lz4,
            linger_ms: 1,
            batch_bytes: 65536,
            ..KafkaConfig::default()
        },
        options.timeout_ms,
        options.clients,
    )?);
    let warmup_start = Instant::now();
    let mut warmup_options = options.clone();
    warmup_options.run = format!("warmup-{}", options.run);
    for index in 0..options.warmup {
        let bytes = event(
            &warmup_options,
            index % options.clients,
            index / options.clients,
        )?;
        if let Err(error) = sink.send(&[bytes.as_slice()]).await {
            sink.shutdown()
                .await
                .map_err(|_| invalid("warmup cleanup failed"))?;
            return Err(invalid(&format!("warmup did not confirm: {}", error.code)).into());
        }
    }
    let warmup_ms = warmup_start.elapsed().as_secs_f64() * 1000.;
    let barrier = Arc::new(Barrier::new(options.clients + 1));
    let mut jobs = JoinSet::new();
    for client in 0..options.clients {
        let options = options.clone();
        let sink = Arc::clone(&sink);
        let barrier = Arc::clone(&barrier);
        jobs.spawn(async move {
            let mut result = ClientResult::default();
            barrier.wait().await;
            for sequence in 0..options.records {
                let bytes = event(&options, client, sequence)?;
                result.attempted += 1;
                let start = Instant::now();
                match sink.send(&[bytes.as_slice()]).await {
                    Ok(()) => result
                        .confirmed_ns
                        .push(u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)),
                    Err(error) => *result.errors.entry(error.code).or_default() += 1,
                }
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(result)
        });
    }
    let start = Instant::now();
    barrier.wait().await;
    let mut attempted = 0;
    let mut latencies = Vec::with_capacity(options.clients * options.records);
    let mut errors = BTreeMap::new();
    while let Some(result) = jobs.join_next().await {
        let result = result??;
        attempted += result.attempted;
        latencies.extend(result.confirmed_ns);
        for (code, count) in result.errors {
            *errors.entry(code).or_insert(0usize) += count;
        }
    }
    let seconds = start.elapsed().as_secs_f64();
    let shutdown_start = Instant::now();
    let shutdown = sink.shutdown().await;
    let shutdown_ms = shutdown_start.elapsed().as_secs_f64() * 1000.;
    let shutdown_code = shutdown.err().map(|e| e.code);
    latencies.sort_unstable();
    println!(
        "{}",
        json!({
            "schema_version":1,"run_id":options.run,"clients":options.clients,
            "records_per_client":options.records,"payload_bytes":options.payload,
            "attempted":attempted,"confirmed":latencies.len(),"errors":errors,
        "warmup":{"confirmed":options.warmup,"elapsed_ms":warmup_ms,"included_in_timed_counts":false},
            "elapsed_seconds":seconds,"confirmed_records_per_second":latencies.len() as f64 / seconds,
            "latency_ms":{"p50":percentile(&latencies,50,100) as f64/1e6,"p95":percentile(&latencies,95,100) as f64/1e6,"p99":percentile(&latencies,99,100) as f64/1e6,"max":latencies.last().copied().unwrap_or(0) as f64/1e6},
            "latency_boundary":"KafkaSink::send entry to final confirmed delivery; successful calls only; includes adapter parsing, lazy connection, SDK buffering and retries; excludes event construction and readback",
            "throughput_boundary":"barrier release through all send outcomes; excludes adapter construction, explicit warmup, shutdown and readback",
            "shutdown_ms":shutdown_ms,"shutdown_error":shutdown_code,
            "runtime_workers":4,"compression":"lz4","linger_ms":1,"batch_bytes":65536,
        })
    );
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_payload_and_event_raw_data_are_preserved() {
        let options = Options::parse(
            &[
                "localhost:9092",
                "events",
                "run-1",
                "64",
                "64",
                "128",
                "30000",
            ]
            .map(str::to_owned),
        )
        .unwrap();
        for client in [0, 63] {
            for sequence in [0, 63] {
                let raw = payload(client, sequence, 128).unwrap();
                assert_eq!(raw.len(), 128);
                let wire = event(&options, client, sequence).unwrap();
                let parsed: serde_json::Value = serde_json::from_slice(&wire).unwrap();
                assert_eq!(serde_json::to_string(&parsed["data"]).unwrap(), raw);
                assert_eq!(parsed["data"]["sequence"], sequence);
            }
        }
    }
    #[test]
    fn finite_limits_and_nearest_rank_are_explicit() {
        let mut arguments = [
            "localhost:9092",
            "events",
            "run-1",
            "513",
            "1",
            "128",
            "30000",
        ]
        .map(str::to_owned);
        assert!(Options::parse(&arguments).is_err());
        arguments[3] = "512".into();
        arguments[4] = "10000".into();
        assert!(Options::parse(&arguments).is_err());
        let warm = [
            "localhost:9092",
            "events",
            "run-1",
            "64",
            "64",
            "128",
            "30000",
            "64",
        ]
        .map(str::to_owned);
        assert_eq!(Options::parse(&warm).unwrap().warmup, 64);
        let mut too_many_warmups = warm;
        too_many_warmups[7] = "513".into();
        assert!(Options::parse(&too_many_warmups).is_err());
        assert_eq!(percentile(&[1, 2, 3, 4, 5], 99, 100), 5);
        assert_eq!(percentile(&[1, 2, 3, 4], 50, 100), 2);
        assert_eq!(percentile(&[], 99, 100), 0);
    }
}
