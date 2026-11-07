//! Protected access settings and draft-only service probes.
use super::{http::error, settings::State};
use axum::{
    Json,
    body::to_bytes,
    extract::Request,
    http::{Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use mqtt_auth::ConnectInfo;
use mqtt_auth_http::{AclAction, AclQuery, AuthorizationConfig, BackendConfig, Config, Error};
use serde::{Deserialize, Deserializer};

#[derive(Default)]
struct PasswordField {
    present: bool,
    value: Option<String>,
}
impl<'de> Deserialize<'de> for PasswordField {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self {
            present: true,
            value: Option::<String>::deserialize(deserializer)?,
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthenticationTest {
    config: BackendConfig,
    client_id: String,
    username: String,
    #[serde(default)]
    password: PasswordField,
    #[serde(default)]
    password_base64: PasswordField,
}
impl AuthenticationTest {
    fn password_bytes(&self) -> Result<Option<Vec<u8>>, ()> {
        if self.password.present && self.password_base64.present {
            return Err(());
        }
        let value = if let Some(encoded) = &self.password_base64.value {
            if encoded.len() > 87380 {
                return Err(());
            }
            let bytes = STANDARD.decode(encoded).map_err(|_| ())?;
            if STANDARD.encode(&bytes) != *encoded {
                return Err(());
            }
            Some(bytes)
        } else {
            self.password.value.as_ref().map(|s| s.as_bytes().to_vec())
        };
        if value.as_ref().is_some_and(|v| v.len() > 65535) {
            return Err(());
        }
        Ok(value)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorizationTest {
    config: AuthorizationConfig,
    client_id: String,
    username: String,
    action: AclAction,
    topic: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectionTest {
    // Also accept the authorization form's optional cache TTL.
    config: AuthorizationConfig,
    stage: String,
}

fn failure(cause: Error) -> Response {
    let (status, code, message) = match cause {
        Error::InvalidConfig => (
            StatusCode::BAD_REQUEST,
            "access_invalid_config",
            "Invalid access configuration or test input",
        ),
        Error::Busy => (
            StatusCode::CONFLICT,
            "access_busy",
            "Access service configuration or test capacity is busy",
        ),
        Error::Storage => (
            StatusCode::SERVICE_UNAVAILABLE,
            "access_storage",
            "Access configuration storage failed",
        ),
        Error::Stopped => (
            StatusCode::SERVICE_UNAVAILABLE,
            "access_stopped",
            "Access service has stopped",
        ),
    };
    error(status, code, message)
}
fn malformed() -> Response {
    error(
        StatusCode::BAD_REQUEST,
        "access_invalid_request",
        "Invalid access service request",
    )
}

pub(super) async fn dispatch(state: &State, request: Request) -> Response {
    let Some(runtime) = &state.access_control else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "access_unavailable",
            "Access service is unavailable",
        );
    };
    if request.uri().query().is_some_and(|query| !query.is_empty()) {
        return malformed();
    }
    let path = request.uri().path().to_owned();
    let method = request.method().clone();
    let allowed = match path.as_str() {
        "/api/v1/access-control/config" => method == Method::GET || method == Method::PUT,
        "/api/v1/access-control/status" => method == Method::GET,
        "/api/v1/access-control/test/authentication"
        | "/api/v1/access-control/test/authorization"
        | "/api/v1/access-control/test/connection"
        | "/api/v1/access-control/cache/clear" => method == Method::POST,
        _ => {
            return error(
                StatusCode::NOT_FOUND,
                "not_found",
                "Access endpoint not found",
            );
        }
    };
    if !allowed {
        return error(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "Unsupported access endpoint method",
        );
    }
    if method == Method::GET {
        if request.headers().contains_key(header::TRANSFER_ENCODING)
            || request
                .headers()
                .get(header::CONTENT_LENGTH)
                .is_some_and(|value| value.as_bytes() != b"0")
        {
            return malformed();
        }
        if path.ends_with("/status") && runtime.is_stopped() {
            return failure(Error::Stopped);
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
                "Access request exceeds limit",
            );
        }
    };
    if method == Method::PUT {
        let config: Config = match serde_json::from_slice(&bytes) {
            Ok(config) => config,
            Err(_) => return malformed(),
        };
        if let Some(cluster) = &state.cluster {
            let document = match runtime.prepare_shared(config).await {
                Ok(document) => document,
                Err(cause) => return failure(cause),
            };
            let expected = runtime.shared_generation();
            return match cluster
                .put_shared_config(mqtt_cluster::SharedConfigKind::Access, expected, document)
                .await
            {
                Ok(_) => Json(runtime.configuration().await).into_response(),
                Err(cause)
                    if cause.kind() == std::io::ErrorKind::InvalidInput
                        || cause.to_string().contains("stale cluster configuration") =>
                {
                    error(
                        StatusCode::CONFLICT,
                        "access_busy",
                        "Configuration generation conflict; reload and retry",
                    )
                }
                Err(_) => failure(mqtt_auth_http::Error::Storage),
            };
        }
        return match runtime.replace(config).await {
            Ok(config) => Json(config).into_response(),
            Err(cause) => failure(cause),
        };
    }
    if path.ends_with("/cache/clear") {
        if serde_json::from_slice::<Empty>(&bytes).is_err() {
            return malformed();
        }
        return Json(runtime.clear_cache().await).into_response();
    }
    if path.ends_with("/test/connection") {
        let test: ConnectionTest = match serde_json::from_slice(&bytes) {
            Ok(test) => test,
            Err(_) => return malformed(),
        };
        if !matches!(test.stage.as_str(), "authentication" | "authorization") {
            return malformed();
        }
        return match runtime
            .probe_connection(test.config.backend, test.stage == "authorization")
            .await
        {
            Ok(result) => Json(result).into_response(),
            Err(cause) => failure(cause),
        };
    }
    if path.ends_with("/test/authentication") {
        let test: AuthenticationTest = match serde_json::from_slice(&bytes) {
            Ok(test) => test,
            Err(_) => return malformed(),
        };
        let password = match test.password_bytes() {
            Ok(password) => password,
            Err(()) => return malformed(),
        };
        return match runtime
            .probe_authentication(
                test.config,
                ConnectInfo {
                    client_id: &test.client_id,
                    username: Some(&test.username),
                    password: password.as_deref(),
                },
            )
            .await
        {
            Ok(result) => Json(result).into_response(),
            Err(cause) => failure(cause),
        };
    }
    let test: AuthorizationTest = match serde_json::from_slice(&bytes) {
        Ok(test) => test,
        Err(_) => return malformed(),
    };
    match runtime
        .probe_authorization(
            test.config,
            AclQuery {
                client_id: test.client_id,
                username: test.username,
                action: test.action,
                topic: test.topic,
            },
        )
        .await
    {
        Ok(result) => Json(result).into_response(),
        Err(cause) => failure(cause),
    }
}

#[cfg(test)]
#[path = "access_control_tests.rs"]
mod tests;
