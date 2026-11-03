//! Privileged downlink publication behind the management Bearer gate.
use super::{http::error, settings::State};
use axum::{
    Json,
    body::to_bytes,
    extract::Request,
    http::{Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use mqtt_transport::PublishMessage;
use serde::Deserialize;
use serde_json::json;

pub(super) const PATH: &str = "/api/v1/mqtt/publish";
const MAX_BODY: usize = 1024 * 1024;

fn default_qos() -> u8 {
    1
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Publish {
    topic: String,
    payload: String,
    #[serde(default = "default_qos")]
    qos: u8,
    #[serde(default)]
    retain: bool,
}
impl Publish {
    fn validate(&self) -> Result<(), StatusCode> {
        if self.topic == "$SYS" || self.topic.starts_with("$SYS/") {
            return Err(StatusCode::FORBIDDEN);
        }
        if self.qos > 2
            || self.topic.is_empty()
            || self.topic.len() > 65535
            || self.topic.contains(['\0', '+', '#'])
        {
            return Err(StatusCode::BAD_REQUEST);
        }
        Ok(())
    }
}
pub(super) async fn dispatch(state: &State, request: Request) -> Response {
    if request.method() != Method::POST {
        let mut response = error(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "Only POST is supported",
        );
        response
            .headers_mut()
            .insert(header::ALLOW, header::HeaderValue::from_static("POST"));
        return response;
    }
    if request.uri().query().is_some_and(|q| !q.is_empty()) {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_query",
            "No query parameters supported",
        );
    }
    let json_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"));
    if !json_type {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_content_type",
            "JSON body required",
        );
    }
    let bytes = match to_bytes(request.into_body(), MAX_BODY).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "message_too_large",
                "Request body exceeds limit",
            );
        }
    };
    let publish: Publish = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Invalid publish request",
            );
        }
    };
    if let Err(status) = publish.validate() {
        return error(
            status,
            if status == StatusCode::FORBIDDEN {
                "reserved_topic"
            } else {
                "invalid_request"
            },
            "Invalid or reserved publication topic or QoS",
        );
    }
    let Some(publisher) = &state.publisher else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "Publishing unavailable",
        );
    };
    let message = PublishMessage {
        topic: publish.topic,
        payload: publish.payload.into_bytes(),
        qos: publish.qos,
        retain: publish.retain,
        source: "http".into(),
    };
    match publisher.publish(message).await {
        Ok(()) => (StatusCode::ACCEPTED, Json(json!({"accepted":true}))).into_response(),
        Err(cause) => {
            let (status, code) = match cause.kind() {
                std::io::ErrorKind::InvalidInput => (StatusCode::BAD_REQUEST, "invalid_request"),
                std::io::ErrorKind::PermissionDenied => (StatusCode::FORBIDDEN, "not_authorized"),
                std::io::ErrorKind::InvalidData => {
                    (StatusCode::PAYLOAD_TOO_LARGE, "message_too_large")
                }
                _ => (StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
            };
            error(status, code, "Publication could not be accepted")
        }
    }
}
#[cfg(test)]
#[path = "publish_tests.rs"]
mod tests;
