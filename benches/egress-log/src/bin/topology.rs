//! Direct shared-journal topology benchmark; no Broker, SQL evaluation or HTTP.
#[path = "../metrics.rs"]
mod metrics;
use metrics::Samples;
use mqtt_egress_log::{Binding, Journal, NewConsumer, NewResult, QueueOptions};
use serde_json::{Value, json};
use std::{
    error::Error,
    fs, io,
    path::PathBuf,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
type Result<T> = std::result::Result<T, Box<dyn Error>>;
fn invalid(s: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, s)
}
#[derive(Debug)]
struct Config {
    inputs: usize,
    rules: usize,
    fanout: usize,
    batch: usize,
    payload: usize,
    base: PathBuf,
}
impl Config {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut c = Self {
            inputs: 4096,
            rules: 1,
            fanout: 1,
            batch: 64,
            payload: 1024,
            base: std::env::temp_dir(),
        };
        let mut args = args.into_iter();
        while let Some(k) = args.next() {
            let v = args.next().ok_or_else(|| invalid("missing value"))?;
            if k == "--queue-dir" {
                c.base = v.into();
                continue;
            }
            let n = v.parse()?;
            match k.as_str() {
                "--inputs" => c.inputs = n,
                "--rules" => c.rules = n,
                "--fanout" => c.fanout = n,
                "--batch" => c.batch = n,
                "--payload-bytes" => c.payload = n,
                _ => return Err(invalid("unknown option").into()),
            }
        }
        if !(1..=65536).contains(&c.inputs)
            || !(1..=32).contains(&c.rules)
            || !(1..=32).contains(&c.fanout)
            || !(1..=64).contains(&c.batch)
            || !(16..=65536).contains(&c.payload)
        {
            return Err(invalid("out of range").into());
        }
        if c.inputs * c.rules * c.fanout > 200000
            || c.inputs * c.rules * (c.payload + 2200 * c.fanout) > 240 * 1024 * 1024
            || c.payload * c.batch > 8 * 1024 * 1024
        {
            return Err(invalid("dataset exceeds bounded benchmark budget").into());
        }
        Ok(c)
    }
}
struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn rss() -> Option<u64> {
    let o = std::process::Command::new("/bin/ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    if !o.status.success() {
        return None;
    }
    String::from_utf8(o.stdout).ok()?.trim().parse().ok()
}
fn body(rule: usize, input: usize, size: usize) -> Vec<u8> {
    let mut b = vec![b'x'; size];
    b[..8].copy_from_slice(&(rule as u64).to_le_bytes());
    b[8..16].copy_from_slice(&(input as u64).to_le_bytes());
    b
}
fn run(c: Config) -> Result<Value> {
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let d = Directory(
        c.base
            .join(format!("egress-topology-{}-{stamp}", std::process::id())),
    );
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(&d.0)?;
    let mut options = QueueOptions::new(d.0.clone());
    options.max_records = 200000;
    options.max_bytes = 512 * 1024 * 1024;
    options.segment_bytes = 1024 * 1024;
    let mut j = Journal::open(options)?;
    let bindings: Vec<_> = (0..c.fanout)
        .map(|o| Binding {
            outbound_id: format!("out-{o}"),
            descriptor: format!("benchmark-{o}").into_bytes(),
        })
        .collect();
    let revisions = j.bind_all(&bindings)?;
    let initial_rss = rss();
    let mut write = Samples::new("append_results_durable", "rule_results");
    for start in (0..c.inputs).step_by(c.batch) {
        for rule in 0..c.rules {
            let end = (start + c.batch).min(c.inputs);
            let mut group = Vec::with_capacity(end - start);
            for input in start..end {
                let mut consumers = Vec::with_capacity(c.fanout);
                for binding in &bindings {
                    consumers.push(NewConsumer {
                        outbound_id: binding.outbound_id.clone(),
                        revision: *revisions
                            .get(&binding.outbound_id)
                            .ok_or_else(|| invalid("missing revision"))?,
                        delivery_id: format!("{rule}:{input}:{}", binding.outbound_id),
                    });
                }
                group.push(NewResult {
                    stream_id: format!("rule-{rule}"),
                    client_id: format!("client-{}", start / c.batch),
                    payload: body(rule, input, c.payload),
                    consumers,
                });
            }
            let n = group.len();
            let t = Instant::now();
            let ids = j.append_results(group)?;
            write.add(t.elapsed(), n, n * c.payload);
            if ids.len() != n || ids.iter().any(|v| v.consumer_ids.len() != c.fanout) {
                return Err(invalid("append ID mismatch").into());
            }
        }
    }
    write.stop();
    let after_append_rss = rss();
    let stats = j.stats();
    if stats.pending_results != c.inputs * c.rules
        || stats.pending_records != c.inputs * c.rules * c.fanout
        || stats.pending_bytes != (c.inputs * c.rules * c.payload) as u64
    {
        return Err(invalid("shared body/ref accounting mismatch").into());
    }
    let mut read = Samples::new("read_consumers_warm_verified", "consumer_deliveries");
    let mut per_outbound = Vec::new();
    // Bounded head pages, including their lookup cost in the stage wall time.
    for binding in &bindings {
        let mut cursor = None;
        let mut count = 0;
        loop {
            let heads = j.consumer_heads(&binding.outbound_id, cursor, 64);
            if heads.is_empty() {
                break;
            }
            cursor = heads.last().map(|h| h.id);
            for h in heads {
                let t = Instant::now();
                let records = j.read_consumers(h.id, c.batch, c.batch * c.payload)?;
                let elapsed = t.elapsed();
                if records.is_empty() {
                    return Err(invalid("empty read").into());
                }
                for r in &records {
                    let rule = r
                        .meta
                        .stream_id
                        .strip_prefix("rule-")
                        .ok_or_else(|| invalid("bad rule"))?
                        .parse::<usize>()?;
                    let input = u64::from_le_bytes(
                        r.payload
                            .get(8..16)
                            .ok_or_else(|| invalid("short payload"))?
                            .try_into()?,
                    ) as usize;
                    if r.payload != body(rule, input, c.payload)
                        || r.meta.client_id != h.client_id
                        || r.meta.outbound_id != binding.outbound_id
                        || r.meta.stream_id != h.stream_id
                    {
                        return Err(invalid("read body/lane mismatch").into());
                    }
                }
                count += records.len();
                read.add(elapsed, records.len(), records.len() * c.payload);
            }
        }
        if count != c.inputs * c.rules {
            return Err(invalid("read did not cover every result for destination").into());
        }
        per_outbound.push(count);
    }
    read.stop();
    let after_read_rss = rss();
    let value = json!({"schema_version":1,"tool":"egress-topology","debug_assertions":cfg!(debug_assertions),"config":{"inputs":c.inputs,"rules":c.rules,"fanout":c.fanout,"batch":c.batch,"payload_bytes":c.payload,"segment_bytes":1048576},"stats_after_append":stats,"rss_kib":{"initial":initial_rss,"after_append":after_append_rss,"after_read":after_read_rss},"append":write.json(),"read":read.json(),"verified_deliveries_per_outbound":per_outbound,"method":"production sync_all unchanged; single writer; reads warm after append, include CRC; wall includes generation/validation/head lookup; API latency excludes driver; RSS sampled via ps outside timings, peak via external time; no SQL/HTTP/MQTT"});
    drop(j);
    drop(d);
    Ok(value)
}
fn main() -> Result<()> {
    let c = Config::parse(std::env::args().skip(1))?;
    println!("{}", serde_json::to_string_pretty(&run(c)?)?);
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn distinct_rules_shared_fanout() {
        let c = Config::parse(
            [
                "--inputs", "9", "--rules", "3", "--fanout", "4", "--batch", "4",
            ]
            .map(str::to_owned),
        )
        .unwrap();
        let v = run(c).unwrap();
        assert_eq!(v["stats_after_append"]["pending_results"], 27);
        assert_eq!(v["stats_after_append"]["pending_records"], 108);
        assert_eq!(v["read"]["items"], 108);
        assert_eq!(v["stats_after_append"]["pending_bytes"], 27 * 1024);
    }
    #[test]
    fn rejects_unbounded_dataset() {
        assert!(Config::parse(["--rules", "32", "--fanout", "32"].map(str::to_owned)).is_err());
    }
}
