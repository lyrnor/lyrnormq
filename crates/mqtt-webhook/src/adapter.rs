use mqtt_egress_core::{BatchLimits, FailureKind, SendError, Sink};
use reqwest::{
    Url,
    header::{HeaderName, HeaderValue},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
#[derive(Debug)]
pub struct ConfigError;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookConfig {
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}
impl std::fmt::Debug for WebhookConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookConfig").finish_non_exhaustive()
    }
}
impl WebhookConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.headers.len() > 16 {
            return Err(ConfigError);
        }
        let url = Url::parse(&self.url).map_err(|_| ConfigError)?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(ConfigError);
        }
        let mut size = 0usize;
        let mut unique = BTreeSet::new();
        for (key, value) in &self.headers {
            let lower = key.to_ascii_lowercase();
            if !unique.insert(lower.clone())
                || matches!(
                    lower.as_str(),
                    "host"
                        | "content-length"
                        | "content-type"
                        | "connection"
                        | "transfer-encoding"
                        | "keep-alive"
                        | "proxy-authenticate"
                        | "proxy-authorization"
                        | "te"
                        | "trailer"
                        | "upgrade"
                )
                || value == "[redacted]"
                || value.contains(['\r', '\n'])
                || HeaderName::from_bytes(key.as_bytes()).is_err()
                || HeaderValue::from_str(value).is_err()
            {
                return Err(ConfigError);
            }
            size = size.saturating_add(key.len()).saturating_add(value.len());
        }
        if size > 8192 {
            return Err(ConfigError);
        }
        Ok(())
    }
    pub fn redact(&mut self) {
        for value in self.headers.values_mut() {
            *value = "[redacted]".into();
        }
    }
    pub fn restore(&mut self, old: Option<&Self>) -> Result<(), ConfigError> {
        for (key, value) in &mut self.headers {
            if value == "[redacted]" {
                *value = old
                    .and_then(|c| {
                        c.headers
                            .iter()
                            .find(|(k, _)| k.eq_ignore_ascii_case(key))
                            .map(|(_, v)| v.clone())
                    })
                    .ok_or(ConfigError)?;
            }
        }
        Ok(())
    }
}
pub struct WebhookSink {
    config: WebhookConfig,
    client: reqwest::Client,
    prefix: Vec<u8>,
    timeout: Duration,
}
impl WebhookSink {
    pub fn new(
        id: &str,
        config: WebhookConfig,
        timeout_ms: u64,
        concurrency: usize,
    ) -> Result<Self, ConfigError> {
        config.validate()?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .pool_max_idle_per_host(concurrency)
            .pool_idle_timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| ConfigError)?;
        let prefix =
            format!("{{\"outbound_id\":{},\"events\":[", serde_json::json!(id)).into_bytes();
        Ok(Self {
            config,
            client,
            prefix,
            timeout: Duration::from_millis(timeout_ms),
        })
    }
}
impl Sink for WebhookSink {
    fn limits(&self) -> BatchLimits {
        BatchLimits {
            max_bytes: 1024 * 1024,
            max_events: 500,
            overhead: self.prefix.len() + 2,
            separator_bytes: 1,
        }
    }
    fn send<'a>(
        &'a self,
        events: &'a [&'a [u8]],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SendError>> + Send + 'a>>
    {
        Box::pin(async move {
            let mut body = self.prefix.clone();
            for (i, event) in events.iter().enumerate() {
                if i > 0 {
                    body.push(b',');
                }
                body.extend_from_slice(event);
            }
            body.extend_from_slice(b"]}");
            if body.len() > self.limits().max_bytes {
                return Err(SendError {
                    kind: FailureKind::Permanent,
                    code: "request_too_large".into(),
                });
            }
            let mut request = self
                .client
                .post(&self.config.url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .timeout(self.timeout)
                .body(body);
            request = request.headers(sensitive_headers(&self.config)?);
            match request.send().await {
                Ok(response) => {
                    let code = response.status();
                    if code.is_success() {
                        Ok(())
                    } else {
                        Err(SendError {
                            kind: if code.as_u16() == 408
                                || code.as_u16() == 429
                                || code.is_server_error()
                            {
                                FailureKind::Retryable
                            } else {
                                FailureKind::Permanent
                            },
                            code: format!("http_{}", code.as_u16()),
                        })
                    }
                }
                Err(error) => Err(SendError {
                    kind: FailureKind::Retryable,
                    code: if error.is_timeout() {
                        "timeout"
                    } else {
                        "transport"
                    }
                    .into(),
                }),
            }
        })
    }
}

fn sensitive_headers(config: &WebhookConfig) -> Result<reqwest::header::HeaderMap, SendError> {
    let mut headers = reqwest::header::HeaderMap::new();
    for (k, v) in &config.headers {
        let key = HeaderName::from_bytes(k.as_bytes()).map_err(|_| SendError {
            kind: FailureKind::Permanent,
            code: "invalid_header".into(),
        })?;
        let mut value = HeaderValue::from_str(v).map_err(|_| SendError {
            kind: FailureKind::Permanent,
            code: "invalid_header".into(),
        })?;
        value.set_sensitive(true);
        headers.insert(key, value);
    }
    Ok(headers)
}
#[cfg(test)]
#[path = "adapter_tests.rs"]
mod tests;
