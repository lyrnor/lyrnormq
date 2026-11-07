use super::*;

#[test]
fn authorization_requires_one_exact_bearer_header() {
    let token = b"0123456789abcdef0123456789abcdef";
    let mut headers = HeaderMap::new();
    assert!(!authorized(&headers, token));
    headers.insert(
        header::AUTHORIZATION,
        header::HeaderValue::from_static("Bearer 0123456789abcdef0123456789abcdef"),
    );
    assert!(authorized(&headers, token));
    for candidate in [
        "bearer 0123456789abcdef0123456789abcdef",
        "Bearer 0123456789abcdef0123456789abcdee",
        "Bearer 0123456789abcdef0123456789abcdef0",
        "Basic 0123456789abcdef0123456789abcdef",
    ] {
        headers.insert(
            header::AUTHORIZATION,
            header::HeaderValue::from_str(candidate).unwrap(),
        );
        assert!(!authorized(&headers, token));
    }
    headers.insert(
        header::AUTHORIZATION,
        header::HeaderValue::from_static("Bearer 0123456789abcdef0123456789abcdef"),
    );
    headers.append(
        header::AUTHORIZATION,
        header::HeaderValue::from_static("Bearer 0123456789abcdef0123456789abcdef"),
    );
    assert!(!authorized(&headers, token));
}
#[test]
fn query_validation_enforces_limits_and_rejects_duplicates_and_unknowns() {
    let q = query(None).unwrap();
    assert_eq!((q.offset, q.limit), (0, 25));
    for q in [
        "limit=0",
        "limit=101",
        "limit=-1",
        "offset=1000001",
        "protocol=6",
        "state=bad",
        "unknown=x",
        "limit=1&limit=2",
    ] {
        assert!(query(Some(q)).is_err(), "{q}");
    }
    let q = query(Some(
        "limit=100&offset=1000000&state=offline&protocol=5.0&search=%E4%B8%AD%E6%96%87",
    ))
    .unwrap();
    assert_eq!(q.search, "中文");
}

#[test]
fn management_busy_and_not_ready_have_distinct_statuses() {
    let busy = io::Error::new(io::ErrorKind::WouldBlock, ManagementError::Busy);
    let unavailable = io::Error::new(io::ErrorKind::WouldBlock, ManagementError::Unavailable);
    assert_eq!(model_error(busy).status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        model_error(unavailable).status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn publish_shares_the_management_bearer_gate_without_basic_or_mqtt_identity() {
    let state = Arc::new(State {
        handle: mqtt_transport::management::ManagementHandle::new(),
        rules: None,
        access_control: None,
        publisher: None,
        cluster: None,
        token: b"0123456789abcdef0123456789abcdef".to_vec(),
        assets: Default::default(),
        instance: json!({}),
        configuration: json!({}),
    });
    for credential in [None, Some("Basic YTpi"), Some("Bearer wrong")] {
        let mut builder = Request::builder()
            .method(Method::POST)
            .uri(super::super::publish::PATH);
        if let Some(value) = credential {
            builder = builder.header(header::AUTHORIZATION, value);
        }
        let response = dispatch(
            ExtractState(state.clone()),
            builder.body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(response.headers()[header::WWW_AUTHENTICATE], "Bearer");
    }
    let request = Request::builder()
        .method(Method::POST)
        .uri(super::super::publish::PATH)
        .header(
            header::AUTHORIZATION,
            "Bearer 0123456789abcdef0123456789abcdef",
        )
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"topic":"devices/a","payload":"x"}"#))
        .unwrap();
    // No MQTT authentication runtime is consulted. The only unavailable object
    // is the separately installed privileged broker publisher.
    assert_eq!(
        dispatch(ExtractState(state), request).await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}
