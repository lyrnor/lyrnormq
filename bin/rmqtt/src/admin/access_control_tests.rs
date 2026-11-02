use super::*;
use mqtt_auth::AllowAnonymous;
use mqtt_transport::management::ManagementHandle;
use serde_json::json;
use std::{path::PathBuf, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn draft(password_fields: &str) -> String {
    format!(
        r#"{{"config":{{"enabled":false,"url":""}},"client_id":"device-1","username":"device-1"{password_fields}}}"#
    )
}

#[test]
fn credential_fields_preserve_binary_absence_empty_and_exclusivity() {
    for (fields, expected) in [
        ("", None),
        (r#", "password":null"#, None),
        (r#", "password":"""#, Some(vec![])),
        (r#", "password":"秘密""#, Some("秘密".as_bytes().to_vec())),
        (r#", "password_base64":"/wA=""#, Some(vec![255, 0])),
    ] {
        let request: AuthenticationTest = serde_json::from_str(&draft(fields)).unwrap();
        assert_eq!(request.password_bytes().unwrap(), expected);
    }
    for fields in [
        r#", "password":null,"password_base64":"/wA=""#,
        r#", "password":"secret","password_base64":null"#,
        r#", "password_base64":"/wA""#,
        r#", "password_base64":"/wA=\n""#,
        r#", "password_base64":"/wB=""#,
    ] {
        let request: AuthenticationTest = serde_json::from_str(&draft(fields)).unwrap();
        assert!(request.password_bytes().is_err());
    }
    assert!(
        serde_json::from_str::<AuthenticationTest>(&draft(r#", "password":"a","password":"b""#))
            .is_err()
    );
}

#[test]
fn public_failures_never_return_raw_provider_or_storage_errors() {
    for (cause, expected) in [
        (Error::InvalidConfig, StatusCode::BAD_REQUEST),
        (Error::Busy, StatusCode::CONFLICT),
        (Error::Storage, StatusCode::SERVICE_UNAVAILABLE),
        (Error::Stopped, StatusCode::SERVICE_UNAVAILABLE),
    ] {
        assert_eq!(failure(cause).status(), expected);
    }
}

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "rmqtt-access-admin-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn state(runtime: Arc<mqtt_auth_http::Runtime>) -> State {
    State {
        handle: ManagementHandle::new(),
        rules: None,
        access_control: Some(runtime),
        token: vec![b'x'; 32],
        assets: super::super::assets::Assets::new(),
        instance: json!({}),
        configuration: json!({}),
    }
}

#[tokio::test]
async fn external_credential_denial_is_json_success_without_live_side_effects() {
    let dir = Directory::new();
    let runtime =
        mqtt_auth_http::Runtime::open(dir.0.join("access.sqlite"), Arc::new(AllowAnonymous), false)
            .await
            .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        loop {
            let mut buffer = [0; 1024];
            let read = stream.read(&mut buffer).await.unwrap();
            assert!(read > 0);
            request.extend_from_slice(&buffer[..read]);
            assert!(request.len() < 8192);
            if request.windows(4).any(|v| v == b"\r\n\r\n") {
                break;
            }
        }
        stream.write_all(b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"devices\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
    });
    let body = json!({
        "config":{"type":"http","enabled":false,"name":"draft","url":format!("http://{address}/auth"),"allow_http":true},
        "client_id":"device-1","username":"device-1","password":"test-only-password"
    });
    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/access-control/test/authentication")
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap();
    let response = dispatch(&state(runtime.clone()), request).await;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
    let result: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(result["result"], "deny");
    assert_eq!(result["http_status"], 401);
    let config = serde_json::to_value(runtime.configuration().await).unwrap();
    let status = serde_json::to_value(runtime.status().await).unwrap();
    assert_eq!(config["authentication"]["enabled"], false);
    assert_eq!(status["authentication"]["requests_total"], 0);
    assert!(!String::from_utf8_lossy(&bytes).contains("test-only-password"));
    service.await.unwrap();
    runtime.shutdown().await;
}
