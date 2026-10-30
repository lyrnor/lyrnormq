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
        self.0.join("rules.json")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn config(url: String) -> Config {
    Config{connectors:vec![Connector{id:"sink".into(),name:"Sink".into(),url,headers:BTreeMap::from([("Authorization".into(),"Bearer test-secret".into())])}],actions:vec![Action{id:"send".into(),name:"Send".into(),connector_id:"sink".into(),enabled:true,batch_size:1,batch_wait_ms:0,queue_capacity:64,timeout_ms:1000,max_attempts:3,retry_base_ms:10}],rules:vec![Rule{id:"hot".into(),name:"Hot".into(),sql:"SELECT client_id, payload.temperature AS temperature FROM \"devices/+/telemetry\" WHERE payload.temperature >= 80".into(),enabled:true,action_ids:vec!["send".into()]}]}
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
        if runtime.status().await.actions[0].delivered == n {
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
    c.connectors[0].headers.insert("Host".into(), "evil".into());
    assert!(c.validate().is_err());
    let mut c = config("http://localhost/".into());
    c.actions[0].connector_id = "missing".into();
    assert!(c.validate().is_err());
    let mut c = config("http://localhost/".into());
    c.rules[0].action_ids.push("send".into());
    assert!(c.validate().is_err());
    let mut c = config("http://localhost/".into());
    c.actions[0].batch_size = 501;
    assert!(c.validate().is_err());
    let mut c = config("http://localhost/".into());
    c.connectors[0]
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
    assert_eq!(public.connectors[0].headers["Authorization"], "[redacted]");
    runtime.replace(public).await.unwrap();
    assert!(
        std::fs::read_to_string(f.path())
            .unwrap()
            .contains("Bearer test-secret")
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
    assert_eq!(value["action_id"], "send");
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
    assert_eq!(runtime.status().await.actions[0].retried, 1);
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
    assert_eq!(status.actions[0].failed, 1);
    assert_eq!(status.actions[0].last_error.as_deref(), Some("http_400"));
    assert_eq!(status.actions[0].retried, 0);
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
    c.actions[0].batch_size = 5;
    c.actions[0].batch_wait_ms = 100;
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
    c.actions[0].queue_capacity = 1;
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
async fn rejects_configuration_symlink_and_invalid_startup_json() {
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
    let mut action = c.actions[0].clone();
    action.id = "send-two".into();
    c.actions.push(action);
    c.rules[0].action_ids.push("send-two".into());
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
    c.actions[0].batch_size = 32;
    c.actions[0].batch_wait_ms = 100;
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
