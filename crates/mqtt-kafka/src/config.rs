use serde::{Deserialize, Serialize};
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityProtocol {
    #[default]
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}
impl SecurityProtocol {
    pub(crate) fn tls(self) -> bool {
        matches!(self, Self::Ssl | Self::SaslSsl)
    }
    pub(crate) fn sasl(self) -> bool {
        matches!(self, Self::SaslPlaintext | Self::SaslSsl)
    }
}
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KafkaSecurity {
    pub protocol: SecurityProtocol,
    pub sasl_mechanism: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub ca_pem: Option<String>,
    pub certificate_pem: Option<String>,
    pub private_key_pem: Option<String>,
    pub private_key_password: Option<String>,
}
impl std::fmt::Debug for KafkaSecurity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSecurity").finish_non_exhaustive()
    }
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    None,
    #[default]
    Lz4,
    Zstd,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub topic: String,
    pub key_field: Option<String>,
    pub security: KafkaSecurity,
    pub compression: Compression,
    pub linger_ms: u64,
    pub batch_bytes: usize,
}
impl std::fmt::Debug for KafkaConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaConfig").finish_non_exhaustive()
    }
}
impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: Vec::new(),
            topic: String::new(),
            key_field: None,
            security: KafkaSecurity::default(),
            compression: Compression::Lz4,
            linger_ms: 1,
            batch_bytes: 65536,
        }
    }
}
fn clean(s: &str, max: usize) -> bool {
    !s.is_empty() && s.len() <= max && !s.contains('\0') && s != "[redacted]"
}
fn broker(s: &str) -> bool {
    if s.is_empty()
        || s.len() > 255
        || !s.is_ascii()
        || s.bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
    {
        return false;
    }
    if s.starts_with('[') {
        if s.contains('%') {
            return false;
        }
        return s
            .parse::<std::net::SocketAddr>()
            .is_ok_and(|a| a.is_ipv6() && a.port() != 0);
    }
    let Some((h, p)) = s.rsplit_once(':') else {
        return false;
    };
    !h.is_empty()
        && h.len() <= 253
        && h.split('.')
            .all(|label| !label.is_empty() && label.len() <= 63)
        && p.bytes().all(|b| b.is_ascii_digit())
        && h.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        && p.parse::<u16>().is_ok_and(|p| p != 0)
}
impl KafkaConfig {
    pub fn validate(&self) -> Result<(), crate::BuildError> {
        let s = &self.security;
        let valid = !self.brokers.is_empty()
            && self.brokers.len() <= 16
            && self.brokers.iter().all(|b| broker(b))
            && !self.topic.is_empty()
            && self.topic.len() <= 249
            && !matches!(self.topic.as_str(), "." | "..")
            && self
                .topic
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
            && self
                .key_field
                .as_ref()
                .is_none_or(|f| clean(f, 128) && !f.chars().any(char::is_control))
            && self.linger_ms <= 100
            && (16384..=1048576).contains(&self.batch_bytes);
        if !valid {
            return Err(crate::BuildError::Invalid);
        }
        if s.protocol.sasl() {
            if !matches!(
                s.sasl_mechanism.as_deref(),
                Some("PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512")
            ) || !s.username.as_ref().is_some_and(|v| clean(v, 1024))
                || !s.password.as_ref().is_some_and(|v| clean(v, 1024))
            {
                return Err(crate::BuildError::Invalid);
            }
        } else if s.sasl_mechanism.is_some() || s.username.is_some() || s.password.is_some() {
            return Err(crate::BuildError::Invalid);
        }
        if s.protocol.tls() {
            if s.certificate_pem.is_some() != s.private_key_pem.is_some()
                || s.private_key_password.is_some() && s.private_key_pem.is_none()
            {
                return Err(crate::BuildError::Invalid);
            }
            for value in [&s.ca_pem, &s.certificate_pem, &s.private_key_pem] {
                if value
                    .as_ref()
                    .is_some_and(|v| !clean(v, 16384) || !v.contains("-----BEGIN "))
                {
                    return Err(crate::BuildError::Invalid);
                }
            }
            if s.private_key_password
                .as_ref()
                .is_some_and(|v| !clean(v, 1024))
            {
                return Err(crate::BuildError::Invalid);
            }
        } else if s.ca_pem.is_some()
            || s.certificate_pem.is_some()
            || s.private_key_pem.is_some()
            || s.private_key_password.is_some()
        {
            return Err(crate::BuildError::Invalid);
        }
        Ok(())
    }
    pub fn redact(&mut self) {
        for s in [
            &mut self.security.password,
            &mut self.security.private_key_pem,
            &mut self.security.private_key_password,
        ] {
            if s.is_some() {
                *s = Some("[redacted]".into());
            }
        }
    }
    pub fn restore(&mut self, old: Option<&Self>) -> Result<(), crate::BuildError> {
        for (new, old) in [
            (
                &mut self.security.password,
                old.and_then(|o| o.security.password.as_ref()),
            ),
            (
                &mut self.security.private_key_pem,
                old.and_then(|o| o.security.private_key_pem.as_ref()),
            ),
            (
                &mut self.security.private_key_password,
                old.and_then(|o| o.security.private_key_password.as_ref()),
            ),
        ] {
            if new.as_deref() == Some("[redacted]") {
                *new = Some(old.ok_or(crate::BuildError::Invalid)?.clone());
            }
        }
        Ok(())
    }
}
