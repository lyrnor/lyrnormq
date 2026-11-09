use super::*;
use mqtt_transport::management::ManagementHandle;

fn state() -> State {
    State {
        publisher: None,
        handle: ManagementHandle::new(),
        rules: None,
        access_control: None,
        cluster: None,
        token: vec![b'x'; 32],
        assets: super::super::assets::Assets::new(),
        instance: json!({}),
        configuration: json!({}),
    }
}
#[tokio::test]
async fn disabled_rules_are_explicit_without_creating_a_runtime() {
    let request = Request::builder()
        .uri("/api/v1/rules/config")
        .body(axum::body::Body::empty())
        .unwrap();
    let response = dispatch(&state(), request).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["error"]["code"], "rules_disabled");
}
#[test]
fn rule_failures_have_redacted_typed_statuses() {
    for (cause, status) in [
        (RuntimeError::InvalidSql, StatusCode::BAD_REQUEST),
        (RuntimeError::InvalidConfig, StatusCode::BAD_REQUEST),
        (RuntimeError::Busy, StatusCode::CONFLICT),
        (RuntimeError::Storage, StatusCode::SERVICE_UNAVAILABLE),
        (RuntimeError::Stopped, StatusCode::SERVICE_UNAVAILABLE),
    ] {
        assert_eq!(failure(cause).status(), status);
    }
}
