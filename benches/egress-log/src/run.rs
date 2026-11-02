use crate::{
    config::{Config, Phase, invalid},
    metrics::Samples,
};
use mqtt_egress_log::{Binding, Completion, Journal, NewRecord, QueueOptions};
use serde_json::{Value, json};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const OUTBOUND: &str = "benchmark";
static NEXT: AtomicU64 = AtomicU64::new(1);
struct Directory {
    path: PathBuf,
    keep: bool,
}
impl Directory {
    fn new(config: &Config) -> Result<Self, Box<dyn std::error::Error>> {
        fs::create_dir_all(&config.base)?;
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = config.base.join(format!(
            "egress-log-bench-{}-{stamp}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path)?;
        #[cfg(unix)]
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
        Ok(Self {
            path,
            keep: config.keep,
        })
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        if !self.keep {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
fn payload(config: &Config) -> Vec<u8> {
    (0..config.payload).map(|i| (i % 251) as u8).collect()
}
fn group(config: &Config, revision: u64, start: usize, template: &[u8]) -> Vec<NewRecord> {
    let count = config.batch.min(config.records - start);
    let client = format!("client-{:08}", (start / config.batch) % config.clients);
    (0..count)
        .map(|_| NewRecord {
            outbound_id: OUTBOUND.into(),
            revision,
            client_id: client.clone(),
            payload: template.to_vec(),
        })
        .collect()
}
fn append(
    journal: &mut Journal,
    config: &Config,
    revision: u64,
    template: &[u8],
) -> Result<Samples, Box<dyn std::error::Error>> {
    let mut result = Samples::new("append_batch_durable", "records");
    let mut previous = 0;
    for start in (0..config.records).step_by(config.batch) {
        let records = group(config, revision, start, template);
        let count = records.len();
        let timer = Instant::now();
        let ids = journal.append_batch(records)?;
        let elapsed = timer.elapsed();
        if ids.len() != count
            || ids.first().is_none_or(|id| *id <= previous)
            || !ids.windows(2).all(|v| v[0] + 1 == v[1])
        {
            return Err(invalid("append returned invalid record identifiers").into());
        }
        previous = ids.last().copied().unwrap_or(previous);
        result.add(elapsed, count, count * config.payload);
    }
    result.stop();
    if journal.stats().pending_records != config.records {
        return Err(invalid("append pending count mismatch").into());
    }
    Ok(result)
}
fn read(
    journal: &mut Journal,
    config: &Config,
    template: &[u8],
) -> Result<Value, Box<dyn std::error::Error>> {
    // Head IDs are snapshotted before timing. With default topology every append
    // batch is a different client, so heads cover the entire dataset exactly once.
    let mut cursor = None;
    let mut heads = Vec::with_capacity(config.clients.min(config.records));
    loop {
        let page = journal.heads(OUTBOUND, cursor, 64);
        if page.is_empty() {
            break;
        }
        cursor = page.last().map(|m| m.id);
        heads.extend(page);
    }
    let mut result = Samples::new("read_batch_warm_checksum_verified", "records");
    let mut unique = 0usize;
    for round in 0..config.read_rounds {
        for head in &heads {
            let timer = Instant::now();
            let records =
                journal.read_batch(head.id, config.batch, config.batch * config.payload)?;
            let elapsed = timer.elapsed();
            if records.is_empty() {
                return Err(invalid("read returned no records").into());
            }
            if !records
                .iter()
                .all(|r| r.payload == template && r.meta.client_id == head.client_id)
            {
                return Err(invalid("read payload/client mismatch").into());
            }
            if round == 0 {
                unique += records.len();
            }
            result.add(elapsed, records.len(), records.len() * config.payload);
        }
    }
    result.stop();
    let mut value = result.json();
    value["unique_records_verified"] = json!(unique);
    value["cache_state"] =
        json!("warm OS page cache after append; no cache purge or cold-device claim");
    value["head_snapshot_excluded_from_timing"] = json!(true);
    Ok(value)
}
fn metadata(journal: &Journal, config: &Config) -> Result<Value, Box<dyn std::error::Error>> {
    let mut result = Samples::new("heads_index_metadata_only", "client_heads");
    let mut expected = None;
    for _ in 0..config.read_rounds {
        let mut cursor = None;
        let mut seen = 0;
        loop {
            let timer = Instant::now();
            let page = journal.heads(OUTBOUND, cursor, 64);
            let elapsed = timer.elapsed();
            if page.is_empty() {
                break;
            }
            if page.windows(2).any(|v| v[0].id >= v[1].id) {
                return Err(invalid("head order mismatch").into());
            }
            seen += page.len();
            cursor = page.last().map(|m| m.id);
            result.add(elapsed, page.len(), 0);
        }
        if expected.is_some_and(|v| v != seen) {
            return Err(invalid("head snapshot changed during metadata benchmark").into());
        }
        expected = Some(seen);
    }
    result.stop();
    Ok(result.json())
}
fn lifecycle(journal: &mut Journal, config: &Config) -> Result<Value, Box<dyn std::error::Error>> {
    let mut begin = Samples::new("begin_attempt_durable", "records");
    let mut complete = Samples::new("complete_durable", "records");
    let mut pair = Samples::new("begin_and_complete_durable_pair", "records");
    while journal.stats().pending_records > 0 {
        let head = journal
            .heads(OUTBOUND, None, 1)
            .into_iter()
            .next()
            .ok_or_else(|| invalid("pending records have no client head"))?;
        let metas = journal.batch_metas(head.id, config.batch, config.batch * config.payload)?;
        let ids: Vec<_> = metas.iter().map(|r| r.id).collect();
        if ids.is_empty() {
            return Err(invalid("empty lifecycle batch").into());
        }
        let pair_start = Instant::now();
        let timer = Instant::now();
        journal.begin_attempt(&ids)?;
        begin.add(timer.elapsed(), ids.len(), 0);
        let timer = Instant::now();
        journal.complete(&ids, Completion::Delivered)?;
        complete.add(timer.elapsed(), ids.len(), 0);
        pair.add(pair_start.elapsed(), ids.len(), 0);
    }
    begin.stop();
    complete.stop();
    pair.stop();
    if pair.items != config.records as u64 {
        return Err(invalid("lifecycle count mismatch").into());
    }
    Ok(
        json!({"stage":"state_pipeline","wall_definition":"each stage wall time spans the whole begin+complete loop; per-API latencies are isolated","begin_attempt":begin.json(),"complete":complete.json(),"pair":pair.json()}),
    )
}
fn maintenance(
    mut journal: Journal,
    options: QueueOptions,
    config: &Config,
    template: &[u8],
) -> Result<(Journal, Vec<Value>), Box<dyn std::error::Error>> {
    let before = journal.stats();
    let timer = Instant::now();
    journal.checkpoint()?;
    let checkpoint = timer.elapsed();
    let after = journal.stats();
    drop(journal);
    let timer = Instant::now();
    let mut journal = Journal::open(options)?;
    let reopen = timer.elapsed();
    let recovered = journal.stats();
    if recovered.pending_records != config.records {
        return Err(invalid("reopen pending count mismatch").into());
    }
    let head = journal
        .heads(OUTBOUND, None, 1)
        .into_iter()
        .next()
        .ok_or_else(|| invalid("reopen lost all heads"))?;
    let records = journal.read_batch(head.id, config.batch, config.batch * config.payload)?;
    if !records.iter().all(|r| r.payload == template) {
        return Err(invalid("reopen payload mismatch").into());
    }
    Ok((
        journal,
        vec![
            json!({"stage":"checkpoint_live","elapsed_ns":checkpoint.as_nanos(),"single_observation":true,"before":before,"after":after}),
            json!({"stage":"reopen_live_stream_crc","elapsed_ns":reopen.as_nanos(),"single_observation":true,"recovered":recovered}),
        ],
    ))
}
fn cycle(
    journal: &mut Journal,
    config: &Config,
    revision: u64,
    template: &[u8],
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut append = Samples::new("cycle_append_api", "records");
    let mut read = Samples::new("cycle_read_api", "records");
    let mut begin = Samples::new("cycle_begin_api", "records");
    let mut complete = Samples::new("cycle_complete_api", "records");
    let mut decreases = 0u64;
    let timer = Instant::now();
    for start in (0..config.records).step_by(config.batch) {
        let records = group(config, revision, start, template);
        let count = records.len();
        let mut disk = journal.stats().disk_bytes;
        let op = Instant::now();
        let ids = journal.append_batch(records)?;
        append.add(op.elapsed(), count, count * config.payload);
        let after = journal.stats().disk_bytes;
        if after < disk {
            decreases += 1
        }
        disk = after;
        let head = ids
            .first()
            .copied()
            .ok_or_else(|| invalid("empty cycle append"))?;
        let op = Instant::now();
        let records = journal.read_batch(head, config.batch, config.batch * config.payload)?;
        read.add(op.elapsed(), records.len(), records.len() * config.payload);
        if records.len() != count || !records.iter().all(|r| r.payload == template) {
            return Err(invalid("cycle read mismatch").into());
        }
        let op = Instant::now();
        journal.begin_attempt(&ids)?;
        begin.add(op.elapsed(), count, 0);
        let after = journal.stats().disk_bytes;
        if after < disk {
            decreases += 1
        }
        disk = after;
        let op = Instant::now();
        journal.complete(&ids, Completion::Delivered)?;
        complete.add(op.elapsed(), count, 0);
        if journal.stats().disk_bytes < disk {
            decreases += 1
        }
    }
    let elapsed = timer.elapsed();
    append.stop();
    read.stop();
    begin.stop();
    complete.stop();
    if journal.stats().pending_records != 0 {
        return Err(invalid("cycle did not drain").into());
    }
    Ok(
        json!({"stage":"bounded_append_read_begin_complete_cycle","wall_seconds":elapsed.as_secs_f64(),"records":config.records,"records_per_second":config.records as f64/elapsed.as_secs_f64(),"observed_disk_byte_decreases":decreases,"wall_definition":"nested API-stage wall times span the entire cycle; per-API latencies are isolated; automatic compaction included","append":append.json(),"read":read.json(),"begin_attempt":begin.json(),"complete":complete.json()}),
    )
}
pub fn run(config: Config) -> Result<Value, Box<dyn std::error::Error>> {
    let directory = Directory::new(&config)?;
    let mut options = QueueOptions::new(directory.path.clone());
    options.segment_bytes = config.segment;
    options.max_bytes = config.max_bytes;
    options.max_records = config.max_records;
    let setup = Instant::now();
    let mut journal = Journal::open(options.clone())?;
    let revisions = journal.bind_all(&[Binding {
        outbound_id: OUTBOUND.into(),
        descriptor: b"benchmark-only-opaque-binding".to_vec(),
    }])?;
    let revision = *revisions
        .get(OUTBOUND)
        .ok_or_else(|| invalid("missing binding revision"))?;
    let template = payload(&config);
    let initialization_seconds = setup.elapsed().as_secs_f64();
    let mut stages = Vec::new();
    let seed_seconds;
    if config.phase == Phase::Cycle {
        seed_seconds = 0.0;
        stages.push(cycle(&mut journal, &config, revision, &template)?);
    } else {
        let seed = Instant::now();
        let appended = append(&mut journal, &config, revision, &template)?;
        seed_seconds = seed.elapsed().as_secs_f64();
        if matches!(config.phase, Phase::Io | Phase::Append) {
            stages.push(appended.json())
        }
        if matches!(config.phase, Phase::Io | Phase::Read) {
            stages.push(read(&mut journal, &config, &template)?)
        }
        if matches!(config.phase, Phase::Io | Phase::Heads) {
            stages.push(metadata(&journal, &config)?)
        }
        if config.phase == Phase::Lifecycle {
            stages.push(lifecycle(&mut journal, &config)?)
        }
        if config.phase == Phase::Maintenance {
            let returned = maintenance(journal, options, &config, &template)?;
            journal = returned.0;
            stages.extend(returned.1);
        }
    }
    let output = json!({"schema_version":1,"tool":"egress-log-bench","profile":"release expected; runner must record build command","build":{"debug_assertions":cfg!(debug_assertions),"arch":std::env::consts::ARCH,"os":std::env::consts::OS},"phase":format!("{:?}",config.phase).to_lowercase(),"config":{"records":config.records,"payload_bytes":config.payload,"batch_max_records":config.batch,"clients":config.clients,"read_rounds":config.read_rounds,"segment_bytes":config.segment,"queue_max_bytes":config.max_bytes,"queue_max_records":config.max_records},"directory":directory.path,"directory_kept":directory.keep,"initialization_seconds":initialization_seconds,"seed_append_seconds":seed_seconds,"stats_after":journal.stats(),"stages":stages,"method":{"append_api_latency":"starts immediately before append_batch and ends on return, including sync; record preparation outside call timing","wall_throughput":"includes driver allocation and validation within that stage, excludes initialization/setup for read/metadata/state stages","payload_bandwidth":"raw delivery bytes only, excludes frame/JSON metadata and filesystem write amplification","durability":"uses production sync_all; not proof of device power-loss behavior","cache":"read is warm after writes, no cold-cache claim","maintenance":"only explicit maintenance phase; append may include automatic compaction if necessary","teardown":"journal drop and owned-directory cleanup occur outside reported stage timings; external /usr/bin/time includes them"}});
    drop(journal);
    drop(directory);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn verified_smoke_covers_reads_and_durable_state() {
        for phase in ["io", "lifecycle", "maintenance", "cycle"] {
            let config = Config::parse(
                ["--records", "9", "--batch", "4", "--phase", phase].map(str::to_owned),
            )
            .unwrap()
            .unwrap();
            let output = run(config).unwrap();
            assert_eq!(output["config"]["records"], 9);
            let directory = output["directory"].as_str().unwrap();
            assert!(!std::path::Path::new(directory).exists());
            if matches!(phase, "lifecycle" | "cycle") {
                assert_eq!(output["stats_after"]["pending_records"], 0)
            }
        }
    }
}
