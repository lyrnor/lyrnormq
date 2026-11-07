#![cfg(unix)]
//! Blackbox subscriber initialization, filtering and orderly writer flush.
use std::{
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct Broker {
    child: Child,
    root: PathBuf,
}
impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.root);
    }
}
fn command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rmqtt"));
    command
        .env_remove("RUST_LOG")
        .env_remove("RMQTT_LOG_FORMAT");
    command
}

#[test]
fn help_remains_stdout_and_invalid_logging_settings_fail_without_echoing_values() {
    let output = command().arg("--help").output().unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).starts_with("usage:"));
    assert!(output.stderr.is_empty());
    for (name, value) in [
        ("RUST_LOG", "rmqtt=secret-invalid-level"),
        ("RMQTT_LOG_FORMAT", "secret-invalid-format"),
    ] {
        let output = command().arg("--help").env(name, value).output().unwrap();
        assert!(!output.status.success());
        assert!(!String::from_utf8_lossy(&output.stderr).contains(value));
    }
}

fn lifecycle(filter: &str, json: bool) -> Vec<String> {
    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "rmqtt-logging-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&root).unwrap();
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let log = root.join("stderr.log");
    let config = root.join("rmqtt.toml");
    let data_dir = root
        .display()
        .to_string()
        .replace('\\', "\\\\")
        .replace('"', "\\\"");
    fs::write(
        &config,
        format!("[node]\nid = 1\ndata_dir = \"{data_dir}\"\n[mqtt]\nlisten = \"{address}\"\n"),
    )
    .unwrap();
    let child = command()
        .args(["--config", config.to_str().unwrap()])
        .env("RUST_LOG", filter)
        .env("RMQTT_LOG_FORMAT", if json { "json" } else { "text" })
        .stdout(Stdio::null())
        .stderr(fs::File::create(&log).unwrap())
        .spawn()
        .unwrap();
    let mut broker = Broker { child, root };
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(broker.child.try_wait().unwrap().is_none());
        if let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_millis(100)) {
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            stream
                .write_all(b"\x10\x10\x00\x04MQTT\x04\x02\x00\x1e\x00\x04logt")
                .unwrap();
            let mut connack = [0; 4];
            // Binding precedes initialization; CONNACK proves the broker and
            // its signal handler are ready even when info logs are disabled.
            if stream.read_exact(&mut connack).is_ok() && connack == [0x20, 2, 0, 0] {
                stream.write_all(&[0xe0, 0]).unwrap();
                break;
            }
        }
        assert!(Instant::now() < deadline, "broker did not start");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        Command::new("kill")
            .args(["-INT", &broker.child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    loop {
        if let Some(status) = broker.child.try_wait().unwrap() {
            assert!(status.success(), "{}", fs::read_to_string(&log).unwrap());
            break;
        }
        assert!(Instant::now() < deadline, "broker did not stop");
        std::thread::sleep(Duration::from_millis(20));
    }
    fs::read_to_string(log)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect()
}

#[test]
fn json_lifecycle_is_structured_and_flushed_on_shutdown() {
    let lines = lifecycle("warn,rmqtt=info", true);
    let events: Vec<serde_json::Value> = lines
        .iter()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    // Existing supervisors use the first startup line as their readiness marker.
    assert_eq!(events[0]["fields"]["event"], "mqtt_listening");
    for expected in [
        "rules_started",
        "mqtt_listening",
        "shutdown_requested",
        "broker_stopped",
    ] {
        assert!(
            events
                .iter()
                .any(|event| event["fields"]["event"] == expected),
            "missing {expected}: {lines:?}"
        );
    }
    let listening = events
        .iter()
        .find(|event| event["fields"]["event"] == "mqtt_listening")
        .unwrap();
    assert_eq!(listening["level"], "INFO");
    assert_eq!(listening["fields"]["transport"], "TCP");
    assert!(listening["timestamp"].as_str().is_some());
    assert!(!lines.join("\n").contains("\u{1b}"));
}

#[test]
fn error_filter_suppresses_info_without_changing_broker_lifecycle() {
    assert!(lifecycle("error", false).is_empty());
}
