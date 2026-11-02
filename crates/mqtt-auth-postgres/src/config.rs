use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
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
    pub database: String,
    pub username: String,
    pub password: String,
    pub tls_mode: TlsMode,
    pub ca_pem: Option<String>,
    pub max_connections: u32,
    pub query: String,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 5432,
            database: "mqtt".into(),
            username: "mqtt".into(),
            password: String::new(),
            tls_mode: TlsMode::VerifyFull,
            ca_pem: None,
            max_connections: 8,
            query: String::new(),
        }
    }
}
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresConfig").finish_non_exhaustive()
    }
}
impl Config {
    pub fn validate(&self) -> bool {
        let text =
            |s: &str, max| !s.is_empty() && s.len() <= max && !s.chars().any(char::is_control);
        text(&self.host, 253)
            && !self.host.starts_with('/')
            && !self.host.chars().any(char::is_whitespace)
            && self.port != 0
            && text(&self.database, 128)
            && text(&self.username, 128)
            && self.password.len() <= 1024
            && !self.password.contains('\0')
            && self.password != "[redacted]"
            && (1..=32).contains(&self.max_connections)
            && !self.query.trim().is_empty()
            && self.query.len() <= 16384
            && !self.query.contains('\0')
            && self
                .ca_pem
                .as_ref()
                .is_none_or(|s| !s.is_empty() && s.len() <= 16384)
    }
}
