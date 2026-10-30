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
