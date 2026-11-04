//! One Raft WAL append waits for its own `flush(true)`. Disk sync dominates, so a fixed
//! sample count is enough. Run:
//! `cargo test --release -p mqtt-cluster --lib raft_wal_append_latency -- --ignored --nocapture`
use super::log::encode_entry;
use super::wal::Wal;
use crate::model::{Command, Entry, LogId};
use openraft::EntryPayload;
use std::{io, path::PathBuf, time::Instant};

const WARMUP: usize = 16;
const SAMPLES: usize = 100;

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Case {
    label: &'static str,
    commands: usize,
    command_bytes: usize,
    record_bytes: usize,
    samples_us: Vec<u128>,
}

fn scratch() -> io::Result<Scratch> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| io::Error::other(error.to_string()))?
        .as_nanos();
    Ok(Scratch(std::env::temp_dir().join(format!(
        "rmqtt-wal-bench-{}-{nanos}",
        std::process::id()
    ))))
}

fn acquire(index: usize) -> Command {
    Command::AcquireOnce {
        key: format!("c:c1793779302p90068{index:06}"),
        node: 101,
        incarnation: "0123456789abcdef0123456789abcdef".into(),
        generation: 1,
        expected_epoch: 0,
    }
}

fn payload(count: usize) -> Command {
    if count == 1 {
        acquire(0)
    } else {
        Command::Batch {
            commands: (0..count).map(acquire).collect(),
        }
    }
}

fn entry(index: u64, command: &Command) -> Entry {
    Entry {
        log_id: LogId::new(crate::model::committed_leader_id(1, 1), index),
        payload: EntryPayload::Normal(command.clone()),
    }
}

fn percentile(sorted: &[u128], numerator: usize, denominator: usize) -> u128 {
    let rank = sorted.len().saturating_mul(numerator).div_ceil(denominator);
    sorted[rank.saturating_sub(1)]
}

fn measure(label: &'static str, command: Command) -> io::Result<Case> {
    let scratch = scratch()?;
    let mut wal = Wal::open(&scratch.0, "wal-bench", 1)?;
    let record_bytes = encode_entry(&entry(0, &command))?.len();
    let command_bytes = serde_json::to_vec(&command)
        .map_err(|error| io::Error::other(error.to_string()))?
        .len();
    let mut index = 0;
    for _ in 0..WARMUP {
        wal.append(vec![entry(index, &command)])?;
        index += 1;
    }
    let mut samples_us = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        wal.append(vec![entry(index, &command)])?;
        samples_us.push(started.elapsed().as_micros());
        index += 1;
    }
    drop(wal);
    Ok(Case {
        label,
        commands: match &command {
            Command::Batch { commands } => commands.len(),
            _ => 1,
        },
        command_bytes,
        record_bytes,
        samples_us,
    })
}

fn report(case: &Case) {
    let mut sorted = case.samples_us.clone();
    sorted.sort_unstable();
    let total = sorted.iter().sum::<u128>();
    let mean = total / sorted.len() as u128;
    let ops = (SAMPLES as u128).saturating_mul(1_000_000) / total.max(1);
    println!(
        "{}: commands={} command_bytes={} wal_record_bytes={} samples={SAMPLES}",
        case.label, case.commands, case.command_bytes, case.record_bytes,
    );
    println!(
        "  min={}us p50={}us mean={mean}us p99={}us max={}us appends_per_s={ops}",
        sorted[0],
        percentile(&sorted, 1, 2),
        percentile(&sorted, 99, 100),
        sorted[sorted.len() - 1],
    );
}

#[test]
#[ignore = "local disk fsync; cargo test --release -p mqtt-cluster --lib raft_wal_append_latency -- --ignored --nocapture"]
fn raft_wal_append_latency() -> io::Result<()> {
    let single = measure("one AcquireOnce", payload(1))?;
    let batch = measure("batch of 256 AcquireOnce", payload(256))?;
    println!(
        "raft wal append: {WARMUP} warmup + {SAMPLES} timed appends, each waits for flush(true). temp={}",
        std::env::temp_dir().display()
    );
    report(&single);
    report(&batch);
    Ok(())
}
