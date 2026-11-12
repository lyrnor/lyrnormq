use rustls_pki_types::{CertificateDer, pem::PemObject};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TlsMode {
    #[default]
    VerifyFull,
    Disable,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub tls_mode: TlsMode,
    pub ca_pem: Option<String>,
    pub max_connections: u32,
    pub key_prefix: String,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 6379,
            username: String::new(),
            password: String::new(),
            tls_mode: TlsMode::VerifyFull,
            ca_pem: None,
            max_connections: 8,
            key_prefix: "lyrnormq:".into(),
        }
    }
}
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisConfig").finish_non_exhaustive()
    }
}
fn text(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}
fn optional_text(value: &str, max: usize) -> bool {
    value.len() <= max && !value.chars().any(char::is_control)
}
fn valid_ca(pem: &str) -> bool {
    let mut found = false;
    for item in CertificateDer::pem_slice_iter(pem.as_bytes()) {
        if item.is_err() {
            return false;
        }
        found = true;
    }
    found
}
pub(crate) fn valid_prefix(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value.ends_with(':')
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b':' | b'_' | b'-'))
}
impl Config {
    pub fn validate(&self) -> bool {
        text(&self.host, 253)
            && !self.host.starts_with('/')
            && !self.host.chars().any(char::is_whitespace)
            && self.port != 0
            && optional_text(&self.username, 128)
            && self.password.len() <= 1024
            && !self.password.contains('\0')
            && self.password != "[redacted]"
            && (1..=32).contains(&self.max_connections)
            && valid_prefix(&self.key_prefix)
            && self.ca_pem.as_ref().is_none_or(|pem| {
                !pem.is_empty() && pem.len() <= 16384 && !pem.contains('\0') && valid_ca(pem)
            })
    }
}
