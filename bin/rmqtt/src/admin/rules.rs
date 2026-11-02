//! Authenticated, bounded rule configuration and side-effect-free SQL testing.
use super::{http::error, settings::State};
use axum::{
    Json,
    body::to_bytes,
    extract::Request,
    http::{Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use mqtt_egress::{Config, RuntimeError};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TestRequest {
    sql: String,
    event: mqtt_rules::Event,
}

fn failure(error_kind: RuntimeError) -> Response {
    match error_kind {
        RuntimeError::InvalidConfig => error(
            StatusCode::BAD_REQUEST,
            "invalid_config",
            "Invalid rule or outbound configuration",
        ),
        RuntimeError::InvalidSql => error(
            StatusCode::BAD_REQUEST,
            "invalid_sql",
            "Invalid or unsupported SQL expression",
        ),
        RuntimeError::Busy => error(
            StatusCode::CONFLICT,
            "rules_busy",
            "Configuration is busy, test capacity is exhausted, or pending deliveries prevent changing the outbound",
        ),
        RuntimeError::Storage => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "rules_storage",
            "Rule configuration or outbound queue storage failed",
        ),
        RuntimeError::Stopped => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "rules_stopped",
            "Rule runtime has stopped",
        ),
    }
}
pub(super) async fn dispatch(state: &State, request: Request) -> Response {
    let Some(runtime) = &state.rules else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "rules_disabled",
            "Rule service is not available",
        );
    };
    if request.uri().query().is_some_and(|q| !q.is_empty()) {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_query",
            "Rule endpoints do not accept query parameters",
        );
    }
    let path = request.uri().path().to_owned();
    let method = request.method().clone();
    if !matches!(
        path.as_str(),
        "/api/v1/rules/config" | "/api/v1/rules/status" | "/api/v1/rules/test"
    ) {
        return error(
            StatusCode::NOT_FOUND,
            "not_found",
            "Rule endpoint not found",
        );
    }
    let allowed = match path.as_str() {
        "/api/v1/rules/config" => method == Method::GET || method == Method::PUT,
        "/api/v1/rules/status" => method == Method::GET,
        _ => method == Method::POST,
    };
    if !allowed {
        return error(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "Unsupported rule endpoint method",
        );
    }
    if method == Method::GET {
        if request.headers().contains_key(header::TRANSFER_ENCODING)
            || request
                .headers()
                .get(header::CONTENT_LENGTH)
                .is_some_and(|v| v.as_bytes() != b"0")
        {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "GET bodies are not supported",
            );
        }
        return if path.ends_with("/config") {
            Json(runtime.configuration().await).into_response()
        } else {
            Json(runtime.status().await).into_response()
        };
    }
    let json_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("application/json"));
    if !json_type {
        return error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "Use application/json",
        );
    }
    let bytes = match to_bytes(request.into_body(), 64 * 1024).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "body_too_large",
                "Rule request body exceeds limit",
            );
        }
    };
    if method == Method::PUT {
        let config: Config = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) => {
                return error(
                    StatusCode::BAD_REQUEST,
                    "invalid_config",
                    "Malformed rule configuration",
                );
            }
        };
        match runtime.replace(config).await {
            Ok(value) => Json(value).into_response(),
            Err(cause) => failure(cause),
        }
    } else {
        let request: TestRequest = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) => {
                return error(
                    StatusCode::BAD_REQUEST,
                    "invalid_test",
                    "Malformed SQL test input",
                );
            }
        };
        if request.sql.len() > 8192 {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_sql",
                "SQL exceeds length limit",
            );
        }
        match runtime.test(&request.sql, request.event).await {
            Ok(value) => Json(json!({"matched":value.is_some(),"output":value})).into_response(),
            Err(cause) => failure(cause),
        }
    }
}

#[cfg(test)]
#[path = "rules_tests.rs"]
mod tests;
