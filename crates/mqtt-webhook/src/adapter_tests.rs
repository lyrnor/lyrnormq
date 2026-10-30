use super::*;
#[test]
fn custom_headers_are_sensitive_and_debug_is_redacted() {
    let config = WebhookConfig {
        url: "https://example.com/".into(),
        headers: BTreeMap::from([("Authorization".into(), "Bearer secret".into())]),
    };
    let headers = sensitive_headers(&config).unwrap();
    assert!(headers["Authorization"].is_sensitive());
    assert!(!format!("{headers:?}").contains("Bearer secret"));
    assert!(!format!("{config:?}").contains("Bearer secret"));
}
