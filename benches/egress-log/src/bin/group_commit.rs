//! Durably confirmed group calls; independent from the unchanged topology control.
#[path = "../metrics.rs"]
mod metrics;
use metrics::Samples;
use mqtt_egress_log::{Binding, Journal, NewConsumer, NewResult, QueueOptions};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    error::Error,
    fs, io,
    path::PathBuf,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
fn invalid(s: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, s)
}
#[derive(Clone, Debug)]
struct Config {
    base: PathBuf,
    inputs: usize,
    rules: usize,
    fanout: usize,
    payload: usize,
    width: usize,
    segment: u64,
    concurrent: bool,
}
impl Config {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut c = Self {
            base: std::env::temp_dir(),
            inputs: 4096,
            rules: 1,
            fanout: 1,
            payload: 1024,
            width: 1,
            segment: 1048576,
            concurrent: false,
        };
        let mut a = args.into_iter();
        while let Some(k) = a.next() {
            let v = a.next().ok_or_else(|| invalid("missing value"))?;
            match k.as_str() {
                "--queue-dir" => c.base = v.into(),
                "--mode" => {
                    c.concurrent = match v.as_str() {
                        "direct" => false,
                        "concurrent" => true,
                        _ => return Err(invalid("invalid mode").into()),
                    }
                }
                _ => {
                    let n = v.parse::<u64>()?;
                    let u = usize::try_from(n)?;
                    match k.as_str() {
                        "--inputs" => c.inputs = u,
                        "--rules" => c.rules = u,
                        "--fanout" => c.fanout = u,
                        "--payload-bytes" => c.payload = u,
                        "--group-width" => c.width = u,
                        "--segment-bytes" => c.segment = n,
                        _ => return Err(invalid("unknown option").into()),
                    }
                }
            }
        }
        if !(1..=65536).contains(&c.inputs)
            || !(1..=16).contains(&c.rules)
            || !(1..=16).contains(&c.fanout)
            || !(16..=65536).contains(&c.payload)
            || ![1, 4].contains(&c.width)
            || ![1048576, 16777216, 67108864].contains(&c.segment)
        {
            return Err(invalid("out of range").into());
        }
        if c.inputs * c.rules * c.fanout > 200000
            || c.inputs * c.rules * (c.payload + 2200 * c.fanout) > 240 * 1024 * 1024
            || c.payload * c.width * 64 > 8 * 1024 * 1024
        {
            return Err(invalid("bounded dataset/group capacity exceeded").into());
        }
        Ok(c)
    }
    fn total(&self) -> usize {
        self.inputs * self.rules
    }
}
struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn directory(c: &Config) -> Result<Directory> {
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let d = Directory(
        c.base
            .join(format!("egress-group-{}-{stamp}", std::process::id())),
    );
    let mut b = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        b.mode(0o700);
    }
    b.create(&d.0)?;
    Ok(d)
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
fn body(rule: usize, input: usize, n: usize) -> Vec<u8> {
    let mut b = vec![b'x'; n];
    b[..8].copy_from_slice(&(rule as u64).to_le_bytes());
    b[8..16].copy_from_slice(&(input as u64).to_le_bytes());
    b
}
fn row(
    c: &Config,
    revisions: &BTreeMap<String, u64>,
    rule: usize,
    input: usize,
) -> Result<NewResult> {
    let mut consumers = Vec::with_capacity(c.fanout);
    for out in 0..c.fanout {
        let id = format!("out-{out}");
        let revision = *revisions
            .get(&id)
            .ok_or_else(|| invalid("missing revision"))?;
        consumers.push(NewConsumer {
            outbound_id: id.clone(),
            revision,
            delivery_id: format!("{rule}:{input}:{id}"),
        });
    }
    Ok(NewResult {
        stream_id: format!("rule-{rule}"),
        client_id: format!("client-{}", input / 64),
        payload: body(rule, input, c.payload),
        consumers,
    })
}
fn commit(j: &mut Journal, frames: &[Vec<NewResult>], c: &Config, s: &mut Samples) -> Result<()> {
    let n: usize = frames.iter().map(Vec::len).sum();
    let t = Instant::now();
    let ids = j.append_result_groups(frames)?;
    s.add(t.elapsed(), n, n * c.payload);
    if ids.len() != frames.len()
        || ids.iter().zip(frames).any(|(ids, f)| {
            ids.len() != f.len() || ids.iter().any(|r| r.consumer_ids.len() != c.fanout)
        })
    {
        return Err(invalid("group identifiers mismatch").into());
    }
    Ok(())
}
fn direct(j: &mut Journal, c: &Config, revisions: &BTreeMap<String, u64>) -> Result<Value> {
    let mut samples = Samples::new("durable_group_call", "rule_results");
    let mut frames = Vec::with_capacity(c.width);
    let mut count = 0;
    for start in (0..c.inputs).step_by(64) {
        for rule in 0..c.rules {
            let mut frame = Vec::with_capacity(64);
            for input in start..(start + 64).min(c.inputs) {
                frame.push(row(c, revisions, rule, input)?)
            }
            frames.push(frame);
            count += 1;
            if frames.len() == c.width {
                commit(j, &frames, c, &mut samples)?;
                frames.clear();
            }
        }
    }
    if !frames.is_empty() {
        commit(j, &frames, c, &mut samples)?
    }
    samples.stop();
    let calls = samples.values.len();
    Ok(
        json!({"mode":"direct","calls":samples.json(),"frames_submitted":count,"logical_group_calls":calls,"mean_frames_per_group":count as f64/calls as f64,"confirmation_latency":null,"latency_definition":"per complete group API call until durable return; NOT per-request enqueue latency; preparation outside API timer but inside wall throughput"}),
    )
}
struct Request {
    record: NewResult,
    enqueued: Instant,
    reply: mpsc::SyncSender<Instant>,
}
fn concurrent(j: &mut Journal, c: &Config, revisions: &BTreeMap<String, u64>) -> Result<Value> {
    const PRODUCERS: usize = 16;
    const WINDOW: usize = 8;
    let (tx, rx) = mpsc::sync_channel::<Request>(128);
    let next = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(PRODUCERS + 1));
    let mut threads = Vec::new();
    for _ in 0..PRODUCERS {
        let tx = tx.clone();
        let next = Arc::clone(&next);
        let b = Arc::clone(&barrier);
        let c = c.clone();
        let revisions = revisions.clone();
        threads.push(std::thread::Builder::new().stack_size(128 * 1024).spawn(
            move || -> Result<Vec<(std::time::Duration, std::time::Duration)>> {
                let (reply, answers) = mpsc::sync_channel(WINDOW);
                let mut latencies = Vec::new();
                b.wait();
                loop {
                    let mut stamps = Vec::with_capacity(WINDOW);
                    for _ in 0..WINDOW {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        if index >= c.total() {
                            break;
                        }
                        let record = row(&c, &revisions, index % c.rules, index / c.rules)?;
                        let enqueued = Instant::now();
                        tx.send(Request {
                            record,
                            enqueued,
                            reply: reply.clone(),
                        })?;
                        stamps.push(enqueued);
                    }
                    if stamps.is_empty() {
                        break;
                    }
                    for enqueued in stamps {
                        let confirmed = answers.recv()?;
                        latencies.push((confirmed.duration_since(enqueued), enqueued.elapsed()));
                    }
                }
                Ok(latencies)
            },
        )?);
    }
    drop(tx);
    let mut api = Samples::new("durable_group_call", "rule_results");
    let mut confirmation = Samples::new("enqueue_to_sync_confirmed", "rule_results");
    let mut observed = Samples::new("enqueue_to_reply_observed", "rule_results");
    let pipeline = Instant::now();
    barrier.wait();
    let mut processed = 0;
    let mut frame_count = 0;
    while processed < c.total() {
        let first = rx.recv()?;
        let mut requests = vec![first];
        while requests.len() < 64 * c.width {
            match rx.try_recv() {
                Ok(v) => requests.push(v),
                Err(_) => break,
            }
        }
        let mut frames: Vec<Vec<NewResult>> = Vec::with_capacity(c.width);
        let mut receipts = Vec::with_capacity(requests.len());
        for request in requests {
            if frames.last().is_none_or(|f| f.len() == 64) {
                frames.push(Vec::with_capacity(64));
            }
            frames
                .last_mut()
                .ok_or_else(|| invalid("missing frame"))?
                .push(request.record);
            receipts.push((request.enqueued, request.reply));
        }
        frame_count += frames.len();
        commit(j, &frames, c, &mut api)?;
        let confirmed = Instant::now();
        processed += receipts.len();
        for (_, reply) in receipts {
            reply.send(confirmed)?;
        }
    }
    for thread in threads {
        for (confirmed, seen) in thread.join().map_err(|_| invalid("producer panicked"))?? {
            confirmation.add(confirmed, 1, 0);
            observed.add(seen, 1, 0);
        }
    }
    let elapsed = pipeline.elapsed();
    api.stop();
    confirmation.stop();
    observed.stop();
    if confirmation.items != c.total() as u64 {
        return Err(invalid("missing confirmation samples").into());
    }
    let calls = api.values.len();
    Ok(
        json!({"mode":"concurrent","calls":api.json(),"confirmation_latency":confirmation.json(),"observed_reply_latency":observed.json(),"pipeline_seconds":elapsed.as_secs_f64(),"pipeline_results_per_second":c.total() as f64/elapsed.as_secs_f64(),"frames_submitted":frame_count,"logical_group_calls":calls,"mean_frames_per_group":frame_count as f64/calls as f64,"producer_count":PRODUCERS,"per_producer_outstanding":WINDOW,"request_channel_capacity":128,"latency_definition":"each request timestamp before bounded send through successful sync-confirmed group return; includes sender backpressure/queueing; observed metric also includes reply scheduling. Owner gathers only queued requests; no artificial wait."}),
    )
}
fn segment_count(d: &Directory) -> Result<usize> {
    let mut n = 0;
    for entry in fs::read_dir(&d.0)? {
        let p = entry?.path();
        if p.is_dir() {
            for file in fs::read_dir(p)? {
                let file = file?;
                if !file.file_type()?.is_file() {
                    return Err(invalid("unexpected segment entry").into());
                }
                n += 1;
            }
        }
    }
    Ok(n)
}
fn verify(j: &Journal, c: &Config) -> Result<usize> {
    let mut total = 0;
    for out in 0..c.fanout {
        let mut cursor = None;
        let mut seen = 0;
        loop {
            let heads = j.consumer_heads(&format!("out-{out}"), cursor, 64);
            if heads.is_empty() {
                break;
            }
            cursor = heads.last().map(|h| h.id);
            for head in heads {
                let rows = j.read_consumers(head.id, 64, 64 * c.payload)?;
                for r in &rows {
                    let rule = r
                        .meta
                        .stream_id
                        .strip_prefix("rule-")
                        .ok_or_else(|| invalid("bad rule"))?
                        .parse::<usize>()?;
                    let input = u64::from_le_bytes(
                        r.payload
                            .get(8..16)
                            .ok_or_else(|| invalid("short body"))?
                            .try_into()?,
                    ) as usize;
                    if rule >= c.rules
                        || input >= c.inputs
                        || r.payload != body(rule, input, c.payload)
                        || r.meta.client_id != head.client_id
                    {
                        return Err(invalid("readback mismatch").into());
                    }
                }
                seen += rows.len();
            }
        }
        if seen != c.total() {
            return Err(invalid("incomplete readback").into());
        }
        total += seen;
    }
    Ok(total)
}
fn run(c: Config) -> Result<Value> {
    let d = directory(&c)?;
    let mut options = QueueOptions::new(d.0.clone());
    options.max_bytes = 512 * 1024 * 1024;
    options.max_records = 200000;
    options.segment_bytes = c.segment;
    let mut j = Journal::open(options.clone())?;
    let bindings: Vec<_> = (0..c.fanout)
        .map(|o| Binding {
            outbound_id: format!("out-{o}"),
            descriptor: format!("benchmark-{o}").into_bytes(),
        })
        .collect();
    let revisions = j.bind_all(&bindings)?;
    let initial_rss = rss();
    let writing = if c.concurrent {
        concurrent(&mut j, &c, &revisions)?
    } else {
        direct(&mut j, &c, &revisions)?
    };
    let after_append_rss = rss();
    let stats = j.stats();
    if stats.pending_results != c.total()
        || stats.pending_records != c.total() * c.fanout
        || stats.pending_bytes != (c.total() * c.payload) as u64
    {
        return Err(invalid("shared accounting mismatch").into());
    }
    let segments = segment_count(&d)?;
    let verified = verify(&j, &c)?;
    let after_read_rss = rss();
    drop(j);
    let reopened = Journal::open(options)?;
    if reopened.stats().pending_results != c.total()
        || reopened.stats().recovered_records != c.total() * c.fanout
    {
        return Err(invalid("reopen counts mismatch").into());
    }
    drop(reopened);
    Ok(
        json!({"tool":"egress-group-commit","schema_version":1,"debug_assertions":cfg!(debug_assertions),"config":{"inputs":c.inputs,"rules":c.rules,"fanout":c.fanout,"payload_bytes":c.payload,"frame_max_results":64,"group_max_frames":c.width,"segment_bytes":c.segment,"mode":if c.concurrent{"concurrent"}else{"direct"}},"writing":writing,"stats_after_append":stats,"segment_files_after_append":segments,"verified_deliveries":verified,"reopen_verified_results":c.total(),"rss_kib":{"initial":initial_rss,"after_append":after_append_rss,"after_read":after_read_rss},"actual_data_sync_calls":null,"sync_count_note":"Public API does not expose physical sync counters. logical_group_calls are durability barriers, not actual sync calls across rotations; private library tests verify sync counts.","method":"Production sync_all, unchanged durable frames, single journal owner. Full readback and reopen after timing. External time includes verification/reopen/cleanup. Groupwidth4 is a different aggregation boundary than width1, not an equal-latency comparison. No SQL/HTTP/MQTT."}),
    )
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
    fn bounded_cli() {
        assert!(Config::parse(["--group-width", "5"].map(str::to_owned)).is_err());
        assert!(Config::parse(["--inputs", "65536", "--fanout", "16"].map(str::to_owned)).is_err());
    }
    #[test]
    fn direct_group_and_concurrent_confirmations_are_real_and_recover() {
        for mode in ["direct", "concurrent"] {
            let c = Config::parse(
                [
                    "--inputs",
                    "129",
                    "--rules",
                    "2",
                    "--fanout",
                    "2",
                    "--group-width",
                    "4",
                    "--mode",
                    mode,
                ]
                .map(str::to_owned),
            )
            .unwrap();
            let v = run(c).unwrap();
            assert_eq!(v["verified_deliveries"], 516);
            assert_eq!(v["reopen_verified_results"], 258);
            if mode == "direct" {
                assert_eq!(v["writing"]["frames_submitted"], 6);
                assert_eq!(v["writing"]["logical_group_calls"], 2);
            } else {
                assert_eq!(v["writing"]["confirmation_latency"]["items"], 258);
                assert!(
                    v["writing"]["confirmation_latency"]["api_call_latency_ns"]["p99"]
                        .as_u64()
                        .unwrap()
                        > 0
                );
            }
        }
    }
}
