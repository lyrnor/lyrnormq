use super::settings::State;
use axum::{
    Json, Router,
    body::Body,
    extract::{Request, State as ExtractState},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use mqtt_transport::management::{ManagementError, Query};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    io,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
    task::JoinSet,
};

pub(super) fn error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(json!({"error":{"code":code,"message":message}})),
    )
        .into_response()
}
fn model_error(cause: io::Error) -> Response {
    if cause
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<ManagementError>())
        == Some(&ManagementError::Busy)
    {
        return error(
            StatusCode::TOO_MANY_REQUESTS,
            "busy",
            "Management query capacity exceeded",
        );
    }
    match cause.kind() {
        io::ErrorKind::InvalidInput => error_response_bad_query(),
        io::ErrorKind::WouldBlock => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "Broker is unavailable or management is busy",
        ),
        _ => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "Broker management query failed",
        ),
    }
}
fn error_response_bad_query() -> Response {
    error(
        StatusCode::BAD_REQUEST,
        "invalid_query",
        "Invalid management query",
    )
}
fn authorized(headers: &HeaderMap, token: &[u8]) -> bool {
    if headers.get_all(header::AUTHORIZATION).iter().count() != 1 {
        return false;
    }
    let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.as_bytes().strip_prefix(b"Bearer "))
    else {
        return false;
    };
    if value.len() > 256 {
        return false;
    }
    let mut difference = value.len() ^ token.len();
    for (i, expected) in token.iter().enumerate() {
        difference |= usize::from(expected ^ value.get(i).copied().unwrap_or(0));
    }
    difference == 0
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Parameters {
    offset: usize,
    limit: Option<usize>,
    search: String,
    state: String,
    protocol: String,
    node: Option<u64>,
}
#[derive(Debug)]
struct InvalidQuery;

#[derive(Clone, Debug)]
struct AdminQuery {
    query: Query,
    node: Option<u64>,
}

fn query(raw: Option<&str>) -> Result<AdminQuery, InvalidQuery> {
    let p: Parameters = serde_urlencoded::from_str(raw.unwrap_or("")).map_err(|_| InvalidQuery)?;
    let limit = p.limit.unwrap_or(25);
    if !(1..=100).contains(&limit)
        || p.offset > 1_000_000
        || p.search.len() > 256
        || !matches!(p.state.as_str(), "" | "online" | "offline")
        || !matches!(p.protocol.as_str(), "" | "3.1.1" | "5.0" | "unknown")
        || p.node == Some(0)
    {
        return Err(InvalidQuery);
    }
    Ok(AdminQuery {
        query: Query {
            offset: p.offset,
            limit,
            search: p.search,
            state: p.state,
            protocol: p.protocol,
        },
        node: p.node,
    })
}

fn management_query(admin: &AdminQuery) -> mqtt_cluster::ManagementQuery {
    mqtt_cluster::ManagementQuery {
        offset: admin.query.offset,
        limit: admin.query.limit,
        search: admin.query.search.clone(),
        state: admin.query.state.clone(),
        protocol: admin.query.protocol.clone(),
        node: admin.node,
    }
}

/// Overlay local observer fields (protocol/principal/queues) onto directory rows.
async fn enrich_sessions(
    state: &State,
    mut page: mqtt_cluster::ManagementPage<mqtt_cluster::ManagementSession>,
) -> mqtt_cluster::ManagementPage<mqtt_cluster::ManagementSession> {
    let Ok(local) = state
        .handle
        .sessions(Query {
            offset: 0,
            limit: 100,
            search: String::new(),
            state: String::new(),
            protocol: String::new(),
        })
        .await
    else {
        return page;
    };
    let local: std::collections::BTreeMap<_, _> = local
        .items
        .into_iter()
        .map(|session| (session.client_id.clone(), session))
        .collect();
    for item in &mut page.items {
        let Some(local) = local.get(&item.client_id) else {
            continue;
        };
        item.online = local.online;
        item.protocol = local.protocol.clone();
        item.principal = local.principal.clone();
        item.persistent = local.persistent;
        item.subscriptions = local.subscriptions;
        item.queued = local.queued;
        item.inflight = local.inflight;
        item.receive_maximum = local.receive_maximum;
        item.session_expiry_interval = local.session_expiry_interval;
        item.directory = false;
    }
    page
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Client {
    client_id: String,
}

async fn dispatch(ExtractState(state): ExtractState<Arc<State>>, request: Request) -> Response {
    let path = request.uri().path();
    let api = path == "/api" || path.starts_with("/api/");
    let mut response = if request.uri().to_string().len() > 4096 {
        error(
            StatusCode::URI_TOO_LONG,
            "uri_too_long",
            "Request URI exceeds limit",
        )
    } else if api && !authorized(request.headers(), &state.token) {
        let mut response = error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Management token required",
        );
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            header::HeaderValue::from_static("Bearer"),
        );
        response
    } else if path == super::publish::PATH {
        super::publish::dispatch(&state, request).await
    } else if api && path.starts_with("/api/v1/access-control/") {
        super::access_control::dispatch(&state, request).await
    } else if api && path.starts_with("/api/v1/rules/") {
        super::rules::dispatch(&state, request).await
    } else if request.method() != Method::GET {
        let mut response = error(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "Only GET is supported",
        );
        response
            .headers_mut()
            .insert(header::ALLOW, header::HeaderValue::from_static("GET"));
        response
    } else if request.headers().contains_key(header::TRANSFER_ENCODING)
        || request
            .headers()
            .get(header::CONTENT_LENGTH)
            .is_some_and(|v| v.as_bytes() != b"0")
    {
        error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "Request bodies are not supported",
        )
    } else if api {
        api_dispatch(&state, path, request.uri().query()).await
    } else {
        static_asset(&state, path)
    };
    let headers = response.headers_mut();
    headers.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    headers.insert(header::CONTENT_SECURITY_POLICY,header::HeaderValue::from_static("default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        header::HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        header::X_FRAME_OPTIONS,
        header::HeaderValue::from_static("DENY"),
    );
    response
}
fn json_response<T: serde::Serialize>(result: io::Result<T>) -> Response {
    match result {
        Ok(value) => Json(value).into_response(),
        Err(error) => model_error(error),
    }
}
async fn api_dispatch(state: &State, path: &str, raw: Option<&str>) -> Response {
    match path {
        "/api/v1/overview" => {
            if raw.is_some_and(|q| !q.is_empty()) {
                return error_response_bad_query();
            }
            let overview = match state.handle.overview().await {
                Ok(value) => value,
                Err(error) => return model_error(error),
            };
            let mut value = match serde_json::to_value(overview) {
                Ok(Value::Object(map)) => map,
                _ => {
                    return error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal",
                        "Management encoding failed",
                    );
                }
            };
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis();
            value.insert(
                "sampled_at_millis".into(),
                json!(u64::try_from(now).unwrap_or(u64::MAX)),
            );
            value.insert("instance".into(), state.instance.clone());
            value.insert(
                "resources".into(),
                json!({"cpu_percent":null,"rss_bytes":null}),
            );
            value.insert(
                "capabilities".into(),
                json!({"events":false,"resource_metrics":false,"mutations":false,"cluster":false}),
            );
            if state.rules.is_some()
                && let Some(Value::Object(capabilities)) = value.get_mut("capabilities")
            {
                capabilities.insert("rules".into(), Value::Bool(true));
            }
            let topology = match &state.cluster {
                Some(cluster) => match cluster.topology().await {
                    Ok(view) => match serde_json::to_value(view) {
                        Ok(value) => value,
                        Err(_) => {
                            return error(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "internal",
                                "Management encoding failed",
                            );
                        }
                    },
                    Err(cause) => return model_error(cause),
                },
                None => standalone_topology(&value),
            };
            if let Some(cluster) = &state.cluster {
                match cluster.management_counts().await {
                    Ok(counts) => {
                        value.insert("online_sessions".into(), json!(counts.online_sessions));
                        value.insert("offline_sessions".into(), json!(counts.offline_sessions));
                        value.insert("subscriptions".into(), json!(counts.subscriptions));
                        value.insert("retained_messages".into(), json!(counts.retained_messages));
                    }
                    Err(cause) => return model_error(cause),
                }
                if let Some(Value::Object(capabilities)) = value.get_mut("capabilities") {
                    capabilities.insert("cluster".into(), Value::Bool(true));
                }
                // Traffic counters stay process-local; UI labels them as this node.
                value.insert("traffic_scope".into(), json!("local"));
            } else {
                value.insert("traffic_scope".into(), json!("local"));
            }
            value.insert("topology".into(), topology);
            Json(Value::Object(value)).into_response()
        }
        "/api/v1/configuration" => {
            if raw.is_some_and(|q| !q.is_empty()) {
                return error_response_bad_query();
            }
            Json(state.configuration.clone()).into_response()
        }
        "/api/v1/sessions" | "/api/v1/subscriptions" | "/api/v1/retained" => {
            let q = match query(raw) {
                Ok(q) => q,
                Err(_) => return error_response_bad_query(),
            };
            if let Some(cluster) = &state.cluster {
                let mq = management_query(&q);
                return match path {
                    "/api/v1/sessions" => {
                        let page = match cluster.management_sessions(mq).await {
                            Ok(page) => page,
                            Err(error) => return model_error(error),
                        };
                        json_response(Ok(enrich_sessions(state, page).await))
                    }
                    "/api/v1/subscriptions" => {
                        json_response(cluster.management_subscriptions(mq).await)
                    }
                    _ => json_response(cluster.management_retained(mq).await),
                };
            }
            // Standalone rejects node filters; there is only the local observer.
            if q.node.is_some() {
                return error_response_bad_query();
            }
            match path {
                "/api/v1/sessions" => json_response(state.handle.sessions(q.query).await),
                "/api/v1/subscriptions" => json_response(state.handle.subscriptions(q.query).await),
                _ => json_response(state.handle.retained(q.query).await),
            }
        }
        "/api/v1/session" => {
            let client: Client = match serde_urlencoded::from_str(raw.unwrap_or("")) {
                Ok(client) => client,
                Err(_) => return error_response_bad_query(),
            };
            if client.client_id.is_empty() || client.client_id.len() > 65535 {
                return error_response_bad_query();
            }
            match state.handle.session(&client.client_id).await {
                Ok(Some(value)) => {
                    let mut encoded = match serde_json::to_value(&value) {
                        Ok(Value::Object(map)) => map,
                        _ => return Json(value).into_response(),
                    };
                    if let Some(cluster) = &state.cluster
                        && let Ok(Some(directory)) =
                            cluster.management_session(client.client_id.clone()).await
                    {
                        encoded.insert("node_id".into(), json!(directory.node_id));
                        encoded.insert("directory".into(), json!(false));
                    }
                    Json(Value::Object(encoded)).into_response()
                }
                Ok(None) => {
                    if let Some(cluster) = &state.cluster {
                        match cluster.management_session(client.client_id).await {
                            Ok(Some(value)) => Json(value).into_response(),
                            Ok(None) => {
                                error(StatusCode::NOT_FOUND, "not_found", "Session not found")
                            }
                            Err(error) => model_error(error),
                        }
                    } else {
                        error(StatusCode::NOT_FOUND, "not_found", "Session not found")
                    }
                }
                Err(error) => model_error(error),
            }
        }
        _ => error(
            StatusCode::NOT_FOUND,
            "not_found",
            "Management endpoint not found",
        ),
    }
}
fn standalone_topology(overview: &serde_json::Map<String, Value>) -> Value {
    let listen = overview
        .get("instance")
        .and_then(|value| value.get("listen"))
        .and_then(Value::as_str)
        .unwrap_or("local")
        .to_owned();
    let storage = overview
        .get("instance")
        .and_then(|value| value.get("storage"))
        .and_then(Value::as_str)
        .unwrap_or("memory")
        .to_owned();
    let online = overview
        .get("online_sessions")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let offline = overview
        .get("offline_sessions")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let routes = overview
        .get("subscriptions")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    json!({
        "revision": 0,
        "leader": null,
        "term": 0,
        "last_applied": 0,
        "data_storage": storage,
        "data_generation": 0,
        "clients": online.saturating_add(offline),
        "routes": routes,
        "self_node_id": 0,
        "nodes": [{
            "id": 0,
            "roles": ["broker"],
            "broker_id": 0,
            "address": listen,
            "broker_address": listen,
            "applied": 0,
            "expires_at": 0,
            "max_lease_ms": 0,
            "lease_remaining_ms": 0,
            "clients": online.saturating_add(offline),
            "excluded": false,
            "self": true,
            "leader": true,
            "online": true
        }],
        "excluded": []
    })
}

fn static_asset(state: &State, path: &str) -> Response {
    if path.contains(['%', '\\']) || path.split('/').any(|part| matches!(part, "." | "..")) {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_path",
            "Invalid asset path",
        );
    }
    let path = if path == "/" { "/index.html" } else { path };
    match state.assets.get(path) {
        Some(asset) => (
            [(header::CONTENT_TYPE, asset.content_type)],
            Body::from(asset.bytes.to_vec()),
        )
            .into_response(),
        None => error(
            StatusCode::NOT_FOUND,
            "not_found",
            "Dashboard asset not found",
        ),
    }
}

/// Finite connection set, HTTP/1 parser buffers and total per-connection deadline.
/// Keep-alive is disabled so idle clients cannot occupy slots between polls.
pub async fn serve(
    listener: TcpListener,
    state: Arc<State>,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    let app = Router::new().fallback(dispatch).with_state(state);
    let permits = Arc::new(Semaphore::new(64));
    let mut tasks = JoinSet::new();
    let result = loop {
        if *shutdown.borrow() {
            break Ok(());
        }
        tokio::select! {
            biased;
            _=shutdown.changed()=>break Ok(()),
            Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
            accepted=listener.accept()=>{
                let (stream,_)=match accepted {
                    Ok(value)=>value,
                    Err(error)=>{
                        tracing::error!(error=%error,"dashboard accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let Ok(permit)=Arc::clone(&permits).try_acquire_owned() else {continue;};
                let service=TowerToHyperService::new(app.clone());
                tasks.spawn(async move {
                    let _permit=permit;
                    let mut builder=hyper::server::conn::http1::Builder::new();
                    builder.keep_alive(false).max_buf_size(16*1024).max_headers(64).timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(10));
                    let _=tokio::time::timeout(Duration::from_secs(10),builder.serve_connection(TokioIo::new(stream),service)).await;
                });
            }
        }
    };
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    result
}

#[cfg(test)]
#[path = "http_tests.rs"]
mod tests;
