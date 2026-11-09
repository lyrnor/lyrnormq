#[allow(dead_code)] // The shared oracle also exposes faults exercised by independent_wire.
mod support;
use serde_json::Value;
use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use support::{Fault, Peer, Scenario};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Files(PathBuf);
impl Files {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "lyrnormq-loadgen-cli-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn cli(address: std::net::SocketAddr, files: &Files) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_lyrnormq-loadgen"));
    c.args([
        "--addr",
        &address.to_string(),
        "--pairs",
        "1",
        "--qos",
        "2",
        "--protocol",
        "5",
        "--payload-bytes",
        "128",
        "--duration-ms",
        "100",
        "--warmup-ms",
        "0",
        "--drain-timeout-ms",
        "400",
        "--io-timeout-ms",
        "300",
        "--connect-timeout-ms",
        "300",
        "--max-messages",
        "100000",
        "--output",
    ]);
    c.arg(files.path("report.json"));
    c
}
async fn execute(mut command: Command) -> Output {
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || command.output().unwrap()),
    )
    .await
    .unwrap()
    .unwrap()
}
fn json(files: &Files) -> Value {
    serde_json::from_slice(&fs::read(files.path("report.json")).unwrap())
        .expect("CLI must leave a nonempty valid JSON report")
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_raw_samples_independently_reproduce_all_percentiles() {
    let files = Files::new();
    let peer = Peer::start(Scenario::default()).await;
    let mut c = cli(peer.address, &files);
    c.arg("--raw").arg(files.path("samples.jsonl"));
    let output = execute(c).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(peer.finish().await.is_empty());
    let report = json(&files);
    assert_eq!(report["success"], true);
    let samples: Vec<Value> = fs::read_to_string(files.path("samples.jsonl"))
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    assert!(!samples.is_empty());
    assert_eq!(
        report["accounting"]["samples_retained"].as_u64().unwrap(),
        samples.len() as u64
    );
    for (series, start, end) in [
        ("schedule_to_send", "scheduled_ns", "send_started_ns"),
        (
            "send_to_write_completed",
            "send_started_ns",
            "write_completed_ns",
        ),
        ("send_to_delivery", "send_started_ns", "delivery_ns"),
        ("schedule_to_delivery", "scheduled_ns", "delivery_ns"),
        ("send_to_source_completed", "send_started_ns", "ack_ns"),
        ("send_to_full_completion", "send_started_ns", "completed_ns"),
    ] {
        let mut ns: Vec<u64> = samples
            .iter()
            .filter_map(|s| Some(s[end].as_u64()? - s[start].as_u64()?))
            .collect();
        ns.sort_unstable();
        let d = &report["latencies"][series];
        assert_eq!(d["count"].as_u64(), Some(ns.len() as u64));
        for (p, key) in [(50, "p50_ns"), (95, "p95_ns"), (99, "p99_ns")] {
            let rank = (p * ns.len()).div_ceil(100);
            assert_eq!(d[key].as_u64(), Some(ns[rank - 1]), "{series}/{key}");
        }
        assert_eq!(d["max_ns"].as_u64(), ns.last().copied());
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_measured_failure_has_nonzero_exit_and_failure_json() {
    let files = Files::new();
    let peer = Peer::start(Scenario {
        fault: Fault::DropSource,
        ..Scenario::default()
    })
    .await;
    let output = execute(cli(peer.address, &files)).await;
    let _ = peer.finish().await;
    assert!(!output.status.success());
    let report = json(&files);
    assert_eq!(report["success"], false);
    assert!(report["issues"].as_array().is_some_and(|v| !v.is_empty()));
}
#[tokio::test]
async fn cli_existing_output_is_never_overwritten() {
    let files = Files::new();
    fs::write(files.path("report.json"), b"sentinel").unwrap();
    let output = execute(cli("127.0.0.1:1".parse().unwrap(), &files)).await;
    assert!(!output.status.success());
    assert_eq!(fs::read(files.path("report.json")).unwrap(), b"sentinel");
}
#[tokio::test]
async fn cli_raw_reservation_failure_does_not_leave_blank_report() {
    let files = Files::new();
    let mut c = cli("127.0.0.1:1".parse().unwrap(), &files);
    c.arg("--raw")
        .arg(files.path("missing-directory/samples.jsonl"));
    let output = execute(c).await;
    assert!(!output.status.success());
    if files.path("report.json").exists() {
        let report = json(&files);
        assert_eq!(report["success"], false);
    }
}
