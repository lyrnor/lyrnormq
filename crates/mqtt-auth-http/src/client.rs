use crate::{BackendConfig, Error, ProbeResult, ServiceAuth, status::Counters};
use base64::{Engine, engine::general_purpose::STANDARD};
use mqtt_auth::ConnectInfo;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::sync::Semaphore;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AclAction {
    Publish,
    Subscribe,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AclQuery {
    pub client_id: String,
    pub username: String,
    pub action: AclAction,
    pub topic: String,
}
impl AclQuery {
    pub(crate) fn size(&self) -> usize {
        self.client_id.len() + self.username.len() + self.topic.len() + 128
    }
    pub(crate) fn validate(&self) -> bool {
        valid_username(&self.username)
            && !self.client_id.is_empty()
            && self.client_id.len() <= 65535
            && self.topic.len() <= 65535
            && match self.action {
                AclAction::Publish => {
                    mqtt_router::validate_topic_filter(&self.topic)
                        && !self.topic.contains(['+', '#'])
                }
                AclAction::Subscribe => mqtt_router::validate_topic_filter(&self.topic),
            }
    }
}
pub(crate) fn valid_username(s: &str) -> bool {
    !s.is_empty() && s.len() <= 256 && !s.chars().any(char::is_control)
}
pub(crate) fn username(principal: &mqtt_auth::Principal) -> Option<&str> {
    principal
        .name
        .strip_prefix("user:")
        .filter(|s| valid_username(s))
}
pub(crate) struct Client {
    inner: reqwest::Client,
    config: BackendConfig,
    permits: Arc<Semaphore>,
    pub counters: Counters,
}
impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpClient").finish_non_exhaustive()
    }
}
fn global() -> &'static Arc<Semaphore> {
    static GLOBAL: OnceLock<Arc<Semaphore>> = OnceLock::new();
    GLOBAL.get_or_init(|| Arc::new(Semaphore::new(256)))
}
impl Client {
    pub(crate) fn build(config: &BackendConfig, force: bool) -> Result<Self, Error> {
        config.validate(force)?;
        let mut headers = reqwest::header::HeaderMap::new();
        for (k, v) in &config.headers {
            let name = reqwest::header::HeaderName::from_bytes(k.as_bytes())
                .map_err(|_| Error::InvalidConfig)?;
            let mut value =
                reqwest::header::HeaderValue::from_str(v).map_err(|_| Error::InvalidConfig)?;
            value.set_sensitive(true);
            headers.insert(name, value);
        }
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_millis(config.timeout_ms))
            .connect_timeout(Duration::from_millis(config.timeout_ms.min(1000)))
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(config.max_concurrency.min(16))
            .tcp_nodelay(true)
            .default_headers(headers);
        if let Some(pem) = &config.tls.ca_pem {
            let certs = reqwest::Certificate::from_pem_bundle(pem.as_bytes())
                .map_err(|_| Error::InvalidConfig)?;
            if certs.is_empty() {
                return Err(Error::InvalidConfig);
            }
            for cert in certs {
                builder = builder.add_root_certificate(cert);
            }
        }
        if let (Some(cert), Some(key)) = (&config.tls.certificate_pem, &config.tls.private_key_pem)
        {
            let mut pem = cert.clone();
            pem.push('\n');
            pem.push_str(key);
            builder = builder.identity(
                reqwest::Identity::from_pem(pem.as_bytes()).map_err(|_| Error::InvalidConfig)?,
            );
        }
        let inner = builder.build().map_err(|_| Error::InvalidConfig)?;
        Ok(Self {
            inner,
            config: config.clone(),
            permits: Arc::new(Semaphore::new(config.max_concurrency)),
            counters: Counters::default(),
        })
    }
    async fn send(&self, bytes: Vec<u8>, authn: bool, live: bool) -> ProbeResult {
        let start = tokio::time::Instant::now();
        let result = async {
            let _global = global().clone().try_acquire_owned().map_err(|_| "busy")?;
            let _local = self
                .permits
                .clone()
                .try_acquire_owned()
                .map_err(|_| "busy")?;
            let mut request = self
                .inner
                .post(&self.config.url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .header(reqwest::header::CACHE_CONTROL, "no-store")
                .header(reqwest::header::ACCEPT_ENCODING, "identity")
                .body(bytes);
            match &self.config.service_auth {
                ServiceAuth::Bearer { token } => request = request.bearer_auth(token),
                ServiceAuth::Basic { username, password } => {
                    request = request.basic_auth(username, Some(password))
                }
                _ => {}
            }
            let response = request.send().await.map_err(|e| {
                if e.is_timeout() {
                    "timeout"
                } else {
                    "http_error"
                }
            })?;
            let code = response.status().as_u16();
            drop(response);
            Ok::<_, &'static str>(code)
        };
        let result =
            match tokio::time::timeout(Duration::from_millis(self.config.timeout_ms), result).await
            {
                Ok(Ok(code)) => {
                    let decision = if code == 204 {
                        "allow"
                    } else if code == if authn { 401 } else { 403 } {
                        "deny"
                    } else {
                        "unavailable"
                    };
                    ProbeResult {
                        result: decision,
                        http_status: Some(code),
                        elapsed_ms: 0,
                        error_code: (decision == "unavailable").then_some("http_status"),
                    }
                }
                Ok(Err(code)) => ProbeResult {
                    result: "unavailable",
                    http_status: None,
                    elapsed_ms: 0,
                    error_code: Some(code),
                },
                Err(_) => ProbeResult {
                    result: "unavailable",
                    http_status: None,
                    elapsed_ms: 0,
                    error_code: Some("timeout"),
                },
            };
        let mut result = result;
        result.elapsed_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        if live {
            self.counters.record(&result)
        }
        result
    }
    pub(crate) async fn authenticate(
        &self,
        info: ConnectInfo<'_>,
        live: bool,
    ) -> Result<ProbeResult, Error> {
        let username = info
            .username
            .filter(|s| valid_username(s))
            .ok_or(Error::InvalidConfig)?;
        if info.client_id.is_empty()
            || info.client_id.len() > 65535
            || info.password.is_some_and(|p| p.len() > 65535)
        {
            return Err(Error::InvalidConfig);
        }
        #[derive(Serialize)]
        struct Wire<'a> {
            client_id: &'a str,
            username: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            password: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            password_base64: Option<String>,
        }
        let (password, password_base64) = match info.password {
            None => (None, None),
            Some(bytes) => match std::str::from_utf8(bytes) {
                Ok(s) => (Some(s), None),
                Err(_) => (None, Some(STANDARD.encode(bytes))),
            },
        };
        let bytes = serde_json::to_vec(&Wire {
            client_id: info.client_id,
            username,
            password,
            password_base64,
        })
        .map_err(|_| Error::InvalidConfig)?;
        if bytes.len() > 1024 * 1024 {
            return Err(Error::InvalidConfig);
        }
        Ok(self.send(bytes, true, live).await)
    }
    pub(crate) async fn authorize(
        &self,
        query: &AclQuery,
        live: bool,
    ) -> Result<ProbeResult, Error> {
        if !query.validate() {
            return Err(Error::InvalidConfig);
        }
        let bytes = serde_json::to_vec(query).map_err(|_| Error::InvalidConfig)?;
        if bytes.len() > 1024 * 1024 {
            return Err(Error::InvalidConfig);
        }
        Ok(self.send(bytes, false, live).await)
    }
}
