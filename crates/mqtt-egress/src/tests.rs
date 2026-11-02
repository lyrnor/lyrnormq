use super::*;
use mqtt_rules::Rule;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, watch},
    task::JoinHandle,
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "rmqtt-webhook-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> PathBuf {
        self.0.join("rules.sqlite")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn config(url: String) -> Config {
    Config{schema_version:2,outbounds:vec![Outbound{id:"send".into(),name:"Send".into(),endpoint:Endpoint::Webhook(mqtt_webhook::WebhookConfig{url,headers:BTreeMap::from([("Authorization".into(),"Bearer test-secret".into())])}),enabled:true,delivery:DeliveryPolicy{concurrency:1,batch_size:1,batch_wait_ms:0,queue_capacity:64,timeout_ms:1000,max_attempts:3,retry_base_ms:10}}],rules:vec![Rule{id:"hot".into(),name:"Hot".into(),sql:"SELECT client_id, payload.temperature AS temperature FROM \"devices/+/telemetry\" WHERE payload.temperature >= 80".into(),enabled:true,outbound_ids:vec!["send".into()]}]}
}
async fn server(
    codes: Vec<u16>,
    delay: Duration,
) -> (
    String,
    mpsc::Receiver<Vec<u8>>,
    watch::Sender<bool>,
    JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/hook", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::channel(100);
    let (stop, mut stopped) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut request = 0;
        loop {
            let stream =
                tokio::select! {_ = stopped.changed()=>break,stream=listener.accept()=>stream};
            let Ok((mut stream, _)) = stream else {
                break;
            };
            let mut bytes = Vec::new();
            let mut end = None;
            loop {
                let mut chunk = [0u8; 4096];
                let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut chunk))
                    .await
                    .unwrap()
                    .unwrap();
                if n == 0 {
                    break;
                }
                bytes.extend_from_slice(&chunk[..n]);
                if end.is_none()
                    && let Some(index) = bytes.windows(4).position(|w| w == b"\r\n\r\n")
                {
                    let headers = String::from_utf8_lossy(&bytes[..index]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(str::trim)
                                .and_then(|v| v.parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    end = Some(index + 4 + length);
                }
                if end.is_some_and(|end| bytes.len() >= end) {
                    break;
                }
                assert!(bytes.len() < 2 * 1024 * 1024);
            }
            let _ = tx.send(bytes).await;
            let code = codes.get(request).copied().unwrap_or(200);
            request += 1;
            tokio::time::sleep(delay).await;
            let _ = stream
                .write_all(
                    format!(
                        "HTTP/1.1 {code} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await;
        }
    });
    (url, rx, stop, task)
}
fn body(bytes: &[u8]) -> serde_json::Value {
    let i = bytes.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    serde_json::from_slice(&bytes[i + 4..]).unwrap()
}
async fn receive(rx: &mut mpsc::Receiver<Vec<u8>>) -> Vec<u8> {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn delivered(runtime: &Runtime, n: u64) {
    for _ in 0..200 {
        if runtime.status().await.outbounds[0].delivered == n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("delivery did not complete");
}

#[test]
fn rejects_invalid_and_dangerous_configuration() {
    for url in [
        "file:///etc/passwd",
        "http://user:pass@localhost/",
        "http://localhost/?token=secret",
        "http://localhost/#fragment",
    ] {
        assert!(config(url.into()).validate().is_err());
    }
    let mut c = config("http://localhost/".into());
    webhook_mut(&mut c.outbounds[0].endpoint)
        .headers
        .insert("Host".into(), "evil".into());
    assert!(c.validate().is_err());
    let mut c = config("http://localhost/".into());
    c.rules[0].outbound_ids = vec!["missing".into()];
    assert!(c.validate().is_err());
    let mut c = config("http://localhost/".into());
    c.rules[0].outbound_ids.push("send".into());
    assert!(c.validate().is_err());
    let mut c = config("http://localhost/".into());
    c.outbounds[0].batch_size = 501;
    assert!(c.validate().is_err());
    let mut c = config("http://localhost/".into());
    webhook_mut(&mut c.outbounds[0].endpoint)
        .headers
        .insert("authorization".into(), "duplicate".into());
    assert!(c.validate().is_err());
}
#[tokio::test]
async fn persists_configuration_redacts_and_preserves_headers_on_roundtrip() {
    let f = Fixture::new();
    let runtime = Runtime::open(f.path()).await.unwrap();
    let public = runtime
        .replace(config("http://127.0.0.1:1/".into()))
        .await
        .unwrap();
    assert_eq!(
        webhook(&public.outbounds[0].endpoint).headers["Authorization"],
        "[redacted]"
    );
    runtime.replace(public).await.unwrap();
    assert!(
        std::fs::read(f.path())
            .unwrap()
            .windows(b"Bearer test-secret".len())
            .any(|window| window == b"Bearer test-secret")
    );
    assert!(
        !serde_json::to_string(&runtime.configuration().await)
            .unwrap()
            .contains("test-secret")
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(f.path()).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    runtime.shutdown().await;
    let restored = Runtime::open(f.path()).await.unwrap();
    assert_eq!(restored.configuration().await.rules[0].id, "hot");
    restored.shutdown().await;
}
#[tokio::test]
async fn evaluates_real_sql_and_posts_json_envelope() {
    let f = Fixture::new();
    let (url, mut rx, stop, server) = server(vec![200], Duration::ZERO).await;
    let runtime = Runtime::open(f.path()).await.unwrap();
    runtime.replace(config(url)).await.unwrap();
    let observer = runtime.observer();
    observer.observe(
        "devices/a/telemetry",
        br#"{"temperature":90}"#,
        "device-a",
        1,
        false,
    );
    let raw = receive(&mut rx).await;
    let value = body(&raw);
    assert_eq!(value["outbound_id"], "send");
    assert_eq!(value["events"][0]["rule_id"], "hot");
    assert_eq!(value["events"][0]["data"]["client_id"], "device-a");
    assert_eq!(
        value["events"][0]["data"]["temperature"].as_f64(),
        Some(90.0)
    );
    delivered(&runtime, 1).await;
    observer.observe(
        "devices/a/telemetry",
        br#"{"temperature":10}"#,
        "device-a",
        0,
        false,
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(rx.try_recv().is_err());
    runtime.shutdown().await;
    let _ = stop.send(true);
    server.await.unwrap();
}
#[tokio::test]
async fn retry_reuses_event_identifier_and_counts_per_event() {
    let f = Fixture::new();
    let (url, mut rx, stop, server) = server(vec![500, 200], Duration::ZERO).await;
    let runtime = Runtime::open(f.path()).await.unwrap();
    runtime.replace(config(url)).await.unwrap();
    runtime.observer().observe(
        "devices/a/telemetry",
        br#"{"temperature":91}"#,
        "device-a",
        2,
        false,
    );
    let first = body(&receive(&mut rx).await);
    let second = body(&receive(&mut rx).await);
    assert_eq!(first, second);
    assert!(first["events"][0]["delivery_id"].is_string());
    delivered(&runtime, 1).await;
    assert_eq!(runtime.status().await.outbounds[0].retried, 1);
    runtime.shutdown().await;
    let _ = stop.send(true);
    server.await.unwrap();
}
#[tokio::test]
async fn permanent_http_error_does_not_retry() {
    let f = Fixture::new();
    let (url, mut rx, stop, server) = server(vec![400], Duration::ZERO).await;
    let runtime = Runtime::open(f.path()).await.unwrap();
    runtime.replace(config(url)).await.unwrap();
    runtime.observer().observe(
        "devices/a/telemetry",
        br#"{"temperature":91}"#,
        "device-a",
        0,
        false,
    );
    receive(&mut rx).await;
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(rx.try_recv().is_err());
    let status = runtime.status().await;
    assert_eq!(status.outbounds[0].failed, 1);
    assert_eq!(status.outbounds[0].last_error.as_deref(), Some("http_400"));
    assert_eq!(status.outbounds[0].retried, 0);
    runtime.shutdown().await;
    let _ = stop.send(true);
    server.await.unwrap();
}
#[tokio::test]
async fn batches_only_within_configured_limits() {
    let f = Fixture::new();
    let (url, mut rx, stop, server) = server(vec![200], Duration::ZERO).await;
    let runtime = Runtime::open(f.path()).await.unwrap();
    let mut c = config(url);
    c.outbounds[0].batch_size = 5;
    c.outbounds[0].batch_wait_ms = 100;
    runtime.replace(c).await.unwrap();
    for n in 0..10 {
        runtime.observer().observe(
            "devices/a/telemetry",
            format!("{{\"temperature\":90,\"n\":{n}}}").as_bytes(),
            "device-a",
            0,
            false,
        );
    }
    let first = body(&receive(&mut rx).await);
    let second = body(&receive(&mut rx).await);
    assert_eq!(first["events"].as_array().unwrap().len(), 5);
    assert_eq!(second["events"].as_array().unwrap().len(), 5);
    delivered(&runtime, 10).await;
    runtime.shutdown().await;
    let _ = stop.send(true);
    server.await.unwrap();
}
#[tokio::test]
async fn bounded_overflow_does_not_block_observer_and_shutdown_releases_budget() {
    let f = Fixture::new();
    let (url, _rx, stop, server) = server(vec![200], Duration::from_millis(400)).await;
    let runtime = Runtime::open(f.path()).await.unwrap();
    let mut c = config(url);
    c.outbounds[0].queue_capacity = 1;
    runtime.replace(c).await.unwrap();
    let data = format!(
        "{{\"temperature\":90,\"padding\":\"{}\"}}",
        "x".repeat(200_000)
    )
    .into_bytes();
    let observer = runtime.observer();
    for _ in 0..200 {
        observer.observe("devices/a/telemetry", &data, "same-client", 0, false);
    }
    for _ in 0..100 {
        if runtime.status().await.dropped > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let status = runtime.status().await;
    assert!(status.queued_bytes <= 16 * 1024 * 1024);
    assert!(status.dropped > 0);
    runtime.shutdown().await;
    assert_eq!(runtime.status().await.queued_bytes, 0);
    assert_eq!(runtime.status().await.queued_events, 0);
    let _ = stop.send(true);
    server.await.unwrap();
}
#[tokio::test]
async fn test_mode_has_no_network_side_effect_and_stopped_runtime_rejects_work() {
    let f = Fixture::new();
    let (url, mut rx, stop, server) = server(vec![200], Duration::ZERO).await;
    let runtime = Runtime::open(f.path()).await.unwrap();
    runtime.replace(config(url)).await.unwrap();
    let event = mqtt_rules::Event {
        topic: "devices/a/telemetry".into(),
        payload: br#"{"temperature":90}"#.to_vec(),
        client_id: "a".into(),
        qos: 0,
        retain: false,
        timestamp_ms: 1,
    };
    assert!(
        runtime
            .test("SELECT topic FROM \"devices/#\"", event.clone())
            .await
            .unwrap()
            .is_some()
    );
    assert!(rx.try_recv().is_err());
    runtime.shutdown().await;
    assert!(matches!(
        runtime.test("SELECT * FROM \"#\"", event).await,
        Err(RuntimeError::Stopped)
    ));
    assert!(matches!(
        runtime.replace(Config::default()).await,
        Err(RuntimeError::Stopped)
    ));
    let _ = stop.send(true);
    server.await.unwrap();
}
#[cfg(unix)]
#[tokio::test]
async fn rejects_configuration_symlink_and_non_sqlite_file() {
    let f = Fixture::new();
    std::fs::write(f.path(), "not json").unwrap();
    assert!(Runtime::open(f.path()).await.is_err());
    std::fs::remove_file(f.path()).unwrap();
    std::fs::write(f.0.join("target"), "{}").unwrap();
    std::os::unix::fs::symlink("target", f.path()).unwrap();
    assert!(Runtime::open(f.path()).await.is_err());
}

#[tokio::test]
async fn delivery_ids_distinguish_rule_action_fanout() {
    let f = Fixture::new();
    let (url, mut rx, stop, server) = server(vec![200], Duration::ZERO).await;
    let runtime = Runtime::open(f.path()).await.unwrap();
    let mut c = config(url);
    let mut action = c.outbounds[0].clone();
    action.id = "send-two".into();
    c.outbounds.push(action);
    c.rules[0].outbound_ids.push("send-two".into());
    let mut rule = c.rules[0].clone();
    rule.id = "hot-two".into();
    c.rules.push(rule);
    runtime.replace(c).await.unwrap();
    runtime.observer().observe(
        "devices/a/telemetry",
        br#"{"temperature":90}"#,
        "device-a",
        0,
        false,
    );
    let mut ids = std::collections::BTreeSet::new();
    let mut events = std::collections::BTreeSet::new();
    for _ in 0..4 {
        let value = body(&receive(&mut rx).await);
        let event = &value["events"][0];
        ids.insert(event["delivery_id"].as_str().unwrap().to_owned());
        events.insert(event["event_id"].as_str().unwrap().to_owned());
    }
    assert_eq!(ids.len(), 4);
    assert_eq!(events.len(), 1);
    delivered(&runtime, 2).await;
    runtime.shutdown().await;
    let _ = stop.send(true);
    server.await.unwrap();
}
#[tokio::test]
async fn fixed_shard_preserves_same_publisher_processing_order() {
    let f = Fixture::new();
    let (url, mut rx, stop, server) = server(vec![200], Duration::ZERO).await;
    let runtime = Runtime::open(f.path()).await.unwrap();
    let mut c = config(url);
    c.rules[0].sql = "SELECT payload.n AS n FROM \"devices/#\"".into();
    c.outbounds[0].batch_size = 32;
    c.outbounds[0].batch_wait_ms = 100;
    runtime.replace(c).await.unwrap();
    let observer = runtime.observer();
    for n in 0..32 {
        observer.observe(
            "devices/a/telemetry",
            format!("{{\"n\":{n}}}").as_bytes(),
            "same-client",
            0,
            false,
        );
    }
    let value = body(&receive(&mut rx).await);
    let events = value["events"].as_array().unwrap();
    assert_eq!(events.len(), 32);
    for (n, event) in events.iter().enumerate() {
        assert_eq!(event["data"]["n"].as_f64(), Some(n as f64));
    }
    delivered(&runtime, 32).await;
    runtime.shutdown().await;
    let _ = stop.send(true);
    server.await.unwrap();
}

#[tokio::test]
async fn reconfiguration_pauses_and_retains_committed_pending_deliveries() {
    let f = Fixture::new();
    let (url, mut rx, stop, server) = server(vec![200], Duration::from_millis(300)).await;
    let runtime = Runtime::open(f.path()).await.unwrap();
    runtime.replace(config(url)).await.unwrap();
    runtime.observer().observe(
        "devices/a/telemetry",
        br#"{"temperature":90}"#,
        "same-client",
        0,
        false,
    );
    let original = body(&receive(&mut rx).await);
    assert!(matches!(
        runtime.replace(Config::default()).await,
        Err(RuntimeError::Busy)
    ));
    let mut paused = runtime.configuration().await;
    paused.outbounds[0].enabled = false;
    runtime.replace(paused.clone()).await.unwrap();
    assert_eq!(runtime.status().await.inflight_requests, 0);
    assert_eq!(runtime.status().await.durable_queue.pending_records, 1);
    paused.outbounds[0].enabled = true;
    runtime.replace(paused).await.unwrap();
    let replayed = body(&receive(&mut rx).await);
    assert_eq!(
        original["events"][0]["delivery_id"],
        replayed["events"][0]["delivery_id"]
    );
    delivered(&runtime, 1).await;
    runtime.shutdown().await;
    let _ = stop.send(true);
    server.await.unwrap();
}
#[tokio::test]
async fn uncertain_sqlite_write_failure_stops_runtime_and_restores_actual_commit() {
    fn committed_then_failed(
        store: &mut mqtt_storage::configuration::ConfigStore,
        config: &Config,
    ) -> Result<(), RuntimeError> {
        crate::persistence::write(store, config)?;
        Err(RuntimeError::Storage)
    }
    let f = Fixture::new();
    let runtime = Runtime::open(f.path()).await.unwrap();
    let config = config("http://127.0.0.1:1/hook".into());
    assert!(matches!(
        runtime
            .replace_test_write(config, committed_then_failed)
            .await,
        Err(RuntimeError::Storage)
    ));
    assert!(!runtime.status().await.running);
    // No unconfirmed configuration is published to the stopped process.
    assert!(runtime.configuration().await.rules.is_empty());
    let before = runtime.status().await.received;
    runtime.observer().observe(
        "devices/a/telemetry",
        br#"{"temperature":90}"#,
        "a",
        0,
        false,
    );
    assert_eq!(runtime.status().await.received, before);
    assert!(matches!(
        runtime.replace(Config::default()).await,
        Err(RuntimeError::Stopped)
    ));
    runtime.shutdown().await;
    let restored = Runtime::open(f.path()).await.unwrap();
    assert_eq!(restored.configuration().await.rules[0].id, "hot");
    restored.shutdown().await;
}

#[tokio::test]
async fn rejected_configuration_does_not_commit_or_stop_runtime() {
    let f = Fixture::new();
    let runtime = Runtime::open(f.path()).await.unwrap();
    let mut invalid = config("http://127.0.0.1:1/hook".into());
    invalid.rules[0].sql = "DELETE FROM anything".into();
    assert!(runtime.replace(invalid).await.is_err());
    assert!(runtime.status().await.running);
    assert!(runtime.configuration().await.rules.is_empty());
    runtime.shutdown().await;
    let restored = Runtime::open(f.path()).await.unwrap();
    assert!(restored.configuration().await.rules.is_empty());
    restored.shutdown().await;
}

#[tokio::test]
async fn default_configuration_is_real_sqlite_and_releases_ownership() {
    let f = Fixture::new();
    let runtime = Runtime::open(f.path()).await.unwrap();
    assert!(runtime.configuration().await.rules.is_empty());
    assert_eq!(
        &std::fs::read(f.path()).unwrap()[..16],
        b"SQLite format 3\0"
    );
    assert!(Runtime::open(f.path()).await.is_err());
    runtime.shutdown().await;
    let restored = Runtime::open(f.path()).await.unwrap();
    assert!(restored.status().await.running);
    restored.shutdown().await;
}

#[tokio::test]
async fn failed_sqlite_write_preserves_previous_committed_configuration() {
    fn failed(
        _store: &mut mqtt_storage::configuration::ConfigStore,
        _config: &Config,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Storage)
    }
    let f = Fixture::new();
    let runtime = Runtime::open(f.path()).await.unwrap();
    runtime
        .replace(config("http://127.0.0.1:1/hook".into()))
        .await
        .unwrap();
    assert!(matches!(
        runtime.replace_test_write(Config::default(), failed).await,
        Err(RuntimeError::Storage)
    ));
    assert!(!runtime.status().await.running);
    assert_eq!(runtime.configuration().await.rules[0].id, "hot");
    runtime.shutdown().await;
    let restored = Runtime::open(f.path()).await.unwrap();
    assert_eq!(restored.configuration().await.rules[0].id, "hot");
    restored.shutdown().await;
}

#[tokio::test]
async fn replacing_configuration_releases_old_keepalive_connection_pool() {
    let f = Fixture::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/hook", listener.local_addr().unwrap());
    let (responded, ready) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut end = None;
        loop {
            let mut chunk = [0u8; 4096];
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&chunk[..n]);
            if end.is_none()
                && let Some(index) = bytes.windows(4).position(|w| w == b"\r\n\r\n")
            {
                let headers = String::from_utf8_lossy(&bytes[..index]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(str::trim)
                            .and_then(|v| v.parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                end = Some(index + 4 + length);
            }
            if end.is_some_and(|end| bytes.len() >= end) {
                break;
            }
            assert!(bytes.len() < 2 * 1024 * 1024);
        }
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
            .await
            .unwrap();
        let _ = responded.send(());
        let mut byte = [0u8; 1];
        tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
            .await
            .expect("old generation retained idle connection")
            .unwrap()
    });
    let runtime = Runtime::open(f.path()).await.unwrap();
    runtime.replace(config(url)).await.unwrap();
    runtime.observer().observe(
        "devices/a/telemetry",
        br#"{"temperature":90}"#,
        "device-a",
        0,
        false,
    );
    ready.await.unwrap();
    delivered(&runtime, 1).await;
    runtime.replace(Config::default()).await.unwrap();
    assert_eq!(server.await.unwrap(), 0);
    runtime.shutdown().await;
}

#[tokio::test]
async fn invalid_saved_sqlite_configuration_fails_without_overwriting_payload() {
    use mqtt_storage::configuration::ConfigStore;
    let f = Fixture::new();
    {
        let mut store = ConfigStore::open(&f.path(), 65536).unwrap();
        store.save(b"invalid configuration").unwrap();
    }
    assert!(Runtime::open(f.path()).await.is_err());
    let store = ConfigStore::open(&f.path(), 65536).unwrap();
    assert_eq!(store.load().unwrap().unwrap(), b"invalid configuration");
}

#[tokio::test]
async fn cancelled_update_and_shutdown_callers_do_not_leave_database_locked() {
    use std::sync::{Arc, atomic::AtomicBool};
    static STARTED: AtomicBool = AtomicBool::new(false);
    fn slow_write(
        store: &mut mqtt_storage::configuration::ConfigStore,
        config: &Config,
    ) -> Result<(), RuntimeError> {
        STARTED.store(true, Ordering::Release);
        std::thread::sleep(Duration::from_millis(150));
        crate::persistence::write(store, config)
    }
    let f = Fixture::new();
    let runtime = Runtime::open(f.path()).await.unwrap();
    let update_runtime = Arc::clone(&runtime);
    let update = tokio::spawn(async move {
        update_runtime
            .replace_test_write(config("http://127.0.0.1:1/hook".into()), slow_write)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !STARTED.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    update.abort();
    let shutdown_runtime = Arc::clone(&runtime);
    let shutdown = tokio::spawn(async move { shutdown_runtime.shutdown().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while runtime.status().await.running {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    shutdown.abort();
    let restored = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(restored) = Runtime::open(f.path()).await {
                break restored;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(restored.configuration().await.rules[0].id, "hot");
    restored.shutdown().await;
}

#[tokio::test]
async fn migrates_old_sqlite_blob_transactionally_and_rejects_new_wire_unknowns() {
    let f = Fixture::new();
    let old = serde_json::json!({"connectors":[{"id":"c","name":"C","url":"http://localhost/","headers":{"Authorization":"secret"}}],"actions":[{"id":"a","name":"A","connector_id":"c","enabled":true,"batch_size":1,"batch_wait_ms":0,"queue_capacity":1,"timeout_ms":100,"max_attempts":1,"retry_base_ms":10}],"rules":[{"id":"r","name":"R","sql":"SELECT * FROM \"t\"","enabled":true,"action_ids":["a"]}]});
    {
        let mut store = mqtt_storage::configuration::ConfigStore::open(&f.path(), 65536).unwrap();
        store.save(&serde_json::to_vec(&old).unwrap()).unwrap();
    }
    let runtime = Runtime::open(f.path()).await.unwrap();
    let public = runtime.configuration().await;
    assert_eq!(public.schema_version, 2);
    assert_eq!(public.outbounds[0].concurrency, 8);
    assert_eq!(public.rules[0].outbound_ids, vec!["a"]);
    assert_eq!(
        webhook(&public.outbounds[0].endpoint).headers["Authorization"],
        "[redacted]"
    );
    runtime.shutdown().await;
    let store = mqtt_storage::configuration::ConfigStore::open(&f.path(), 65536).unwrap();
    let saved: serde_json::Value = serde_json::from_slice(&store.load().unwrap().unwrap()).unwrap();
    assert_eq!(saved["schema_version"], 2);
    assert_eq!(
        saved["outbounds"][0]["config"]["headers"]["Authorization"],
        "secret"
    );
    let mut wire = serde_json::to_value(config("http://localhost/".into())).unwrap();
    wire["outbounds"][0]["type"] = "kafka".into();
    assert!(serde_json::from_value::<Config>(wire.clone()).is_err());
    wire["outbounds"][0]["type"] = "webhook".into();
    wire["outbounds"][0]["extra"] = true.into();
    assert!(serde_json::from_value::<Config>(wire).is_err());
}

fn webhook(endpoint: &Endpoint) -> &mqtt_webhook::WebhookConfig {
    match endpoint {
        Endpoint::Webhook(c) => c,
        Endpoint::Kafka(_) => panic!("expected Webhook fixture"),
    }
}
fn webhook_mut(endpoint: &mut Endpoint) -> &mut mqtt_webhook::WebhookConfig {
    match endpoint {
        Endpoint::Webhook(c) => c,
        Endpoint::Kafka(_) => panic!("expected Webhook fixture"),
    }
}

#[tokio::test]
async fn concurrency_defaults_limits_and_sqlite_recovery_preserve_old_policy() {
    let f = Fixture::new();
    let mut old = serde_json::to_value(config("http://127.0.0.1:1/".into())).unwrap();
    old["outbounds"][0]["delivery"]
        .as_object_mut()
        .unwrap()
        .remove("concurrency");
    old["outbounds"][0]["delivery"]["batch_size"] = 100.into();
    old["outbounds"][0]["delivery"]["batch_wait_ms"] = 50.into();
    let decoded: Config = serde_json::from_value(old.clone()).unwrap();
    assert_eq!(decoded.outbounds[0].concurrency, 8);
    for invalid in [0, 17] {
        let mut invalid_config = decoded.clone();
        invalid_config.outbounds[0].concurrency = invalid;
        assert!(invalid_config.validate().is_err());
    }
    {
        let mut store = mqtt_storage::configuration::ConfigStore::open(&f.path(), 65536).unwrap();
        store.save(&serde_json::to_vec(&old).unwrap()).unwrap();
    }
    let runtime = Runtime::open(f.path()).await.unwrap();
    let mut restored = runtime.configuration().await;
    assert_eq!(restored.outbounds[0].concurrency, 8);
    assert_eq!(restored.outbounds[0].batch_size, 100);
    assert_eq!(restored.outbounds[0].batch_wait_ms, 50);
    assert_eq!(runtime.status().await.inflight_requests, 0);
    assert_eq!(runtime.status().await.outbounds[0].inflight_requests, 0);
    restored.outbounds[0].concurrency = 3;
    runtime.replace(restored).await.unwrap();
    runtime.shutdown().await;
    let runtime = Runtime::open(f.path()).await.unwrap();
    let restored = runtime.configuration().await;
    assert_eq!(restored.outbounds[0].concurrency, 3);
    assert_eq!(restored.outbounds[0].batch_size, 100);
    assert_eq!(
        webhook(&restored.outbounds[0].endpoint).headers["Authorization"],
        "[redacted]"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn storage_failure_returns_after_concurrent_attempts_are_cancelled() {
    fn fail_write(
        _: &mut mqtt_storage::configuration::ConfigStore,
        _: &Config,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Storage)
    }
    let f = Fixture::new();
    let (url, mut requests, stop, server) = server(vec![200], Duration::from_secs(1)).await;
    let runtime = Runtime::open(f.path()).await.unwrap();
    let mut policy = config(url);
    policy.outbounds[0].concurrency = 3;
    policy.outbounds[0].timeout_ms = 10000;
    runtime.replace(policy).await.unwrap();
    let observer = runtime.observer();
    for client in 0..3 {
        observer.observe(
            "devices/a/telemetry",
            br#"{"temperature":90}"#,
            &format!("source-{client}"),
            0,
            false,
        );
    }
    let _ = receive(&mut requests).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while runtime.status().await.inflight_requests != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        runtime
            .replace_test_write(runtime.configuration().await, fail_write)
            .await,
        Err(RuntimeError::Storage)
    ));
    let stopped = runtime.status().await;
    assert!(!stopped.running);
    assert_eq!(
        stopped.inflight_requests, 0,
        "storage failure returned before active requests were cancelled"
    );
    assert_eq!(stopped.outbounds[0].inflight_requests, 0);
    assert_eq!(stopped.outbounds[0].queued, 0);
    runtime.shutdown().await;
    assert_eq!(runtime.status().await.queued_bytes, 0);
    let _ = stop.send(true);
    server.await.unwrap();
}

#[tokio::test]
async fn accepted_delivery_replays_after_shutdown_with_stable_identity() {
    let f = Fixture::new();
    let (url, mut requests, stop, server) = server(vec![200], Duration::from_millis(300)).await;
    let runtime = Runtime::open(f.path()).await.unwrap();
    let mut policy = config(url);
    policy.outbounds[0].timeout_ms = 10000;
    runtime.replace(policy).await.unwrap();
    runtime.observer().observe(
        "devices/a/telemetry",
        br#"{"temperature":90}"#,
        "survivor",
        0,
        false,
    );
    let original = body(&receive(&mut requests).await);
    runtime.shutdown().await;
    drop(runtime);
    let resumed = Runtime::open(f.path()).await.unwrap();
    let replayed = body(&receive(&mut requests).await);
    assert_eq!(
        original["events"][0]["delivery_id"],
        replayed["events"][0]["delivery_id"]
    );
    assert_eq!(original["events"][0], replayed["events"][0]);
    delivered(&resumed, 1).await;
    resumed.shutdown().await;
    let _ = stop.send(true);
    server.await.unwrap();
}

#[tokio::test]
async fn one_rule_fanout_stores_its_transformed_body_once() {
    fn occurrences(path: &std::path::Path, marker: &[u8]) -> usize {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    occurrences(&path, marker)
                } else if path.extension().is_some_and(|e| e == "log") {
                    std::fs::read(path)
                        .unwrap()
                        .windows(marker.len())
                        .filter(|w| *w == marker)
                        .count()
                } else {
                    0
                }
            })
            .sum()
    }
    let f = Fixture::new();
    let (url, mut first, stop_first, task_first) = server(vec![200], Duration::from_secs(30)).await;
    let (second_url, mut second, stop_second, task_second) =
        server(vec![200], Duration::from_secs(30)).await;
    let runtime = Runtime::open(f.path()).await.unwrap();
    let mut policy = config(url);
    let mut destination = policy.outbounds[0].clone();
    destination.id = "second".into();
    webhook_mut(&mut destination.endpoint).url = second_url;
    policy.outbounds.push(destination);
    policy.rules[0].outbound_ids.push("second".into());
    policy.rules[0].sql = "SELECT payload.marker AS marker FROM \"devices/#\"".into();
    runtime.replace(policy).await.unwrap();
    runtime.observer().observe(
        "devices/a/telemetry",
        br#"{"marker":"shared-rule-result-body-marker"}"#,
        "device-a",
        0,
        false,
    );
    let _ = receive(&mut first).await;
    let _ = receive(&mut second).await;
    let queue = std::path::PathBuf::from(format!("{}.outbound-queue", f.path().display()));
    let copies = occurrences(&queue, b"shared-rule-result-body-marker");
    runtime.shutdown().await;
    stop_first.send_replace(true);
    stop_second.send_replace(true);
    task_first.abort();
    task_second.abort();
    let _ = task_first.await;
    let _ = task_second.await;
    assert_eq!(
        copies, 1,
        "one transformed result was copied once for every destination"
    );
}
