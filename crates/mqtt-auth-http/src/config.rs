use crate::Error;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};
const REDACTED: &str = "[redacted]";
pub(crate) const MAX_CONFIG: usize = 64 * 1024;
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServiceAuth {
    #[default]
    None,
    Bearer {
        token: String,
    },
    Basic {
        username: String,
        password: String,
    },
    Headers,
}
impl fmt::Debug for ServiceAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceAuth").finish_non_exhaustive()
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackendConfig {
    #[serde(rename = "type")]
    pub kind: String,
    pub enabled: bool,
    pub name: String,
    pub url: String,
    pub service_auth: ServiceAuth,
    pub headers: BTreeMap<String, String>,
    pub timeout_ms: u64,
    pub max_concurrency: usize,
    pub allow_http: bool,
}
impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            kind: "http".into(),
            enabled: false,
            name: "HTTP 认证".into(),
            url: String::new(),
            service_auth: ServiceAuth::None,
            headers: BTreeMap::new(),
            timeout_ms: 1000,
            max_concurrency: 128,
            allow_http: false,
        }
    }
}
impl fmt::Debug for BackendConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackendConfig")
            .field("enabled", &self.enabled)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Serialize)]
pub struct AuthorizationConfig {
    #[serde(flatten)]
    pub backend: BackendConfig,
    pub cache_ttl_seconds: u64,
}
impl Default for AuthorizationConfig {
    fn default() -> Self {
        let backend = BackendConfig {
            name: "HTTP 授权".into(),
            ..BackendConfig::default()
        };
        Self {
            backend,
            cache_ttl_seconds: 30,
        }
    }
}
impl fmt::Debug for AuthorizationConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthorizationConfig")
            .field("backend", &self.backend)
            .field("cache_ttl_seconds", &self.cache_ttl_seconds)
            .finish()
    }
}
// Keep the public JSON flat without combining serde flatten and deny_unknown_fields.
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct AuthorizationWire {
    #[serde(rename = "type")]
    kind: String,
    enabled: bool,
    name: String,
    url: String,
    service_auth: ServiceAuth,
    headers: BTreeMap<String, String>,
    timeout_ms: u64,
    max_concurrency: usize,
    allow_http: bool,
    cache_ttl_seconds: u64,
}
impl Default for AuthorizationWire {
    fn default() -> Self {
        let c = AuthorizationConfig::default();
        let b = c.backend;
        Self {
            kind: b.kind,
            enabled: b.enabled,
            name: b.name,
            url: b.url,
            service_auth: b.service_auth,
            headers: b.headers,
            timeout_ms: b.timeout_ms,
            max_concurrency: b.max_concurrency,
            allow_http: b.allow_http,
            cache_ttl_seconds: c.cache_ttl_seconds,
        }
    }
}
impl<'de> Deserialize<'de> for AuthorizationConfig {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let w = AuthorizationWire::deserialize(d)?;
        Ok(Self {
            backend: BackendConfig {
                kind: w.kind,
                enabled: w.enabled,
                name: w.name,
                url: w.url,
                service_auth: w.service_auth,
                headers: w.headers,
                timeout_ms: w.timeout_ms,
                max_concurrency: w.max_concurrency,
                allow_http: w.allow_http,
            },
            cache_ttl_seconds: w.cache_ttl_seconds,
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub schema_version: u32,
    pub authentication: BackendConfig,
    pub authorization: AuthorizationConfig,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            schema_version: 1,
            authentication: BackendConfig::default(),
            authorization: AuthorizationConfig::default(),
        }
    }
}
fn secret(s: &str, max: usize) -> bool {
    !s.is_empty() && s.len() <= max && !s.contains('\0') && s != REDACTED
}
impl BackendConfig {
    pub(crate) fn validate(&self, force: bool) -> Result<(), Error> {
        if self.kind != "http"
            || self.name.is_empty()
            || self.name.len() > 128
            || self.name.chars().any(char::is_control)
            || !(100..=5000).contains(&self.timeout_ms)
            || !(1..=256).contains(&self.max_concurrency)
        {
            return Err(Error::InvalidConfig);
        }
        if force || self.enabled || !self.url.is_empty() {
            if self.url.len() > 2048
                || self
                    .url
                    .chars()
                    .any(|c| c.is_control() || c.is_whitespace())
            {
                return Err(Error::InvalidConfig);
            }
            let url = reqwest::Url::parse(&self.url).map_err(|_| Error::InvalidConfig)?;
            if !(url.scheme() == "https" || url.scheme() == "http" && self.allow_http)
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(Error::InvalidConfig);
            }
        }
        if self.headers.len() > 32
            || self
                .headers
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
                > 16384
        {
            return Err(Error::InvalidConfig);
        }
        let mut seen = BTreeSet::new();
        for (k, v) in &self.headers {
            let name = reqwest::header::HeaderName::from_bytes(k.as_bytes())
                .map_err(|_| Error::InvalidConfig)?;
            reqwest::header::HeaderValue::from_str(v).map_err(|_| Error::InvalidConfig)?;
            let n = name.as_str();
            if !seen.insert(n.to_owned())
                || matches!(
                    n,
                    "host"
                        | "accept"
                        | "upgrade"
                        | "te"
                        | "trailer"
                        | "proxy-connection"
                        | "content-type"
                        | "content-length"
                        | "transfer-encoding"
                        | "connection"
                        | "expect"
                        | "accept-encoding"
                        | "proxy-authorization"
                        | "cookie"
                )
                || v == REDACTED
                || n == "authorization" && !matches!(self.service_auth, ServiceAuth::Headers)
            {
                return Err(Error::InvalidConfig);
            }
        }
        match &self.service_auth {
            ServiceAuth::Bearer { token } => {
                if !secret(token, 1024) || !token.bytes().all(|b| b.is_ascii_graphic()) {
                    return Err(Error::InvalidConfig);
                }
            }
            ServiceAuth::Basic { username, password } => {
                if !secret(username, 256)
                    || username.contains(':')
                    || username.chars().any(char::is_control)
                    || !secret(password, 1024)
                {
                    return Err(Error::InvalidConfig);
                }
            }
            ServiceAuth::Headers => {
                if self.headers.is_empty() {
                    return Err(Error::InvalidConfig);
                }
            }
            ServiceAuth::None => {}
        }
        Ok(())
    }
    pub(crate) fn restore(&mut self, old: &Self) -> Result<(), Error> {
        fn restore(v: &mut String, old: Option<&String>) -> Result<(), Error> {
            if v == REDACTED {
                *v = old
                    .filter(|s| s.as_str() != REDACTED)
                    .ok_or(Error::InvalidConfig)?
                    .clone()
            }
            Ok(())
        }
        match (&mut self.service_auth, &old.service_auth) {
            (ServiceAuth::Bearer { token }, ServiceAuth::Bearer { token: previous }) => {
                restore(token, Some(previous))?
            }
            (
                ServiceAuth::Basic { password, .. },
                ServiceAuth::Basic {
                    password: previous, ..
                },
            ) => restore(password, Some(previous))?,
            (ServiceAuth::Bearer { token }, _) => restore(token, None)?,
            (ServiceAuth::Basic { password, .. }, _) => restore(password, None)?,
            _ => {}
        }
        for (k, v) in &mut self.headers {
            let old = old
                .headers
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(k))
                .map(|(_, v)| v);
            restore(v, old)?
        }
        Ok(())
    }
    pub(crate) fn redact(&mut self) {
        match &mut self.service_auth {
            ServiceAuth::Bearer { token } => *token = REDACTED.into(),
            ServiceAuth::Basic { password, .. } => *password = REDACTED.into(),
            _ => {}
        }
        for v in self.headers.values_mut() {
            *v = REDACTED.into()
        }
    }
}
impl Config {
    /// Read the former empty TLS placeholder only from private persisted data.
    /// Nonempty or unknown TLS options remain an error; request DTOs stay strict.
    pub(crate) fn from_stored(bytes: &[u8]) -> Result<Self, Error> {
        let mut value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|_| Error::InvalidConfig)?;
        for stage in ["authentication", "authorization"] {
            if let Some(backend) = value
                .get_mut(stage)
                .and_then(serde_json::Value::as_object_mut)
                && let Some(tls) = backend.get("tls")
            {
                let options = tls.as_object().ok_or(Error::InvalidConfig)?;
                if options.iter().any(|(key, value)| {
                    !matches!(
                        key.as_str(),
                        "ca_pem" | "certificate_pem" | "private_key_pem"
                    ) || !value.is_null()
                }) {
                    return Err(Error::InvalidConfig);
                }
                backend.remove("tls");
            }
        }
        serde_json::from_value(value).map_err(|_| Error::InvalidConfig)
    }
    pub(crate) fn validate(&self, local_accounts: bool) -> Result<(), Error> {
        if self.schema_version != 1
            || self.authorization.cache_ttl_seconds > 300
            || self.authorization.backend.enabled && !self.authentication.enabled && !local_accounts
        {
            return Err(Error::InvalidConfig);
        }
        self.authentication.validate(false)?;
        self.authorization.backend.validate(false)?;
        if serde_json::to_vec(self)
            .map_err(|_| Error::InvalidConfig)?
            .len()
            > MAX_CONFIG
        {
            return Err(Error::InvalidConfig);
        }
        Ok(())
    }
    pub(crate) fn public(&self) -> Self {
        let mut c = self.clone();
        c.authentication.redact();
        c.authorization.backend.redact();
        c
    }
}
