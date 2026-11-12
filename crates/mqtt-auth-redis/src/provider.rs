use crate::{
    Config, TlsMode,
    keys::{acl_key, action_field, auth_key, client_field},
    password, pool,
    rules::{MAX_FIELD_BYTES, decide, parse_rules},
};
use std::{fmt, sync::Arc, time::Duration};
use zeroize::Zeroizing;

const MAX_HASH_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidConfig,
    InvalidResult,
    Unavailable,
    Busy,
    Timeout,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidConfig => "redis_invalid_config",
            Self::InvalidResult => "redis_invalid_result",
            Self::Unavailable => "redis_unavailable",
            Self::Busy => "busy",
            Self::Timeout => "timeout",
        })
    }
}
impl std::error::Error for Error {}

pub struct Provider {
    pool: Arc<pool::Pool>,
    prefix: String,
    timeout: Duration,
}
impl fmt::Debug for Provider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisProvider").finish_non_exhaustive()
    }
}
fn install_tls_provider() {
    // rustls 0.23 has no implicit process provider. A second caller finds one already installed.
    let _ = rustls::crypto::ring::default_provider().install_default();
}
fn open_client(config: &Config) -> Result<redis::Client, Error> {
    if config.tls_mode == TlsMode::VerifyFull {
        install_tls_provider();
    }
    let info = redis::ConnectionInfo {
        addr: match config.tls_mode {
            TlsMode::Disable => redis::ConnectionAddr::Tcp(config.host.clone(), config.port),
            TlsMode::VerifyFull => redis::ConnectionAddr::TcpTls {
                host: config.host.clone(),
                port: config.port,
                insecure: false,
                tls_params: None,
            },
        },
        redis: redis::RedisConnectionInfo {
            db: 0,
            username: (!config.username.is_empty()).then(|| config.username.clone()),
            password: (!config.password.is_empty()).then(|| config.password.clone()),
            protocol: redis::ProtocolVersion::RESP2,
        },
    };
    let client = if config.tls_mode == TlsMode::VerifyFull {
        if let Some(ca) = &config.ca_pem {
            redis::Client::build_with_tls(
                info,
                redis::TlsCertificates {
                    client_tls: None,
                    root_cert: Some(ca.as_bytes().to_vec()),
                },
            )
        } else {
            redis::Client::open(info)
        }
    } else {
        redis::Client::open(info)
    };
    client.map_err(|_| Error::InvalidConfig)
}
async fn read_field(
    held: &mut pool::Checkout,
    key: &[u8],
    field: &[u8],
    max: usize,
) -> Result<Option<Vec<u8>>, Error> {
    let mut probe = redis::pipe();
    probe
        .cmd("HEXISTS")
        .arg(key)
        .arg(field)
        .cmd("HSTRLEN")
        .arg(key)
        .arg(field);
    let (exists, len): (i64, i64) = held.query_pipe(&probe).await?;
    if exists == 0 {
        return Ok(None);
    }
    let len = usize::try_from(len).map_err(|_| Error::InvalidResult)?;
    if len == 0 || len > max {
        return Err(Error::InvalidResult);
    }
    let mut cmd = redis::cmd("HGET");
    cmd.arg(key).arg(field);
    match held.query::<Option<Vec<u8>>>(&cmd).await? {
        Some(value) if !value.is_empty() && value.len() <= max => Ok(Some(value)),
        Some(_) => Err(Error::InvalidResult),
        None => Ok(None),
    }
}
impl Provider {
    pub fn build(config: &Config, timeout_ms: u64) -> Result<Self, Error> {
        if !config.validate() || !(100..=5000).contains(&timeout_ms) {
            return Err(Error::InvalidConfig);
        }
        let timeout = Duration::from_millis(timeout_ms);
        Ok(Self {
            pool: Arc::new(pool::Pool::new(
                open_client(config)?,
                config.max_connections,
                timeout,
            )),
            prefix: config.key_prefix.clone(),
            timeout,
        })
    }
    pub fn pool_status(&self) -> (u32, usize) {
        self.pool.status()
    }
    pub async fn probe_connection(&self) -> Result<bool, Error> {
        let operation = async {
            let mut held = self.pool.checkout().await?;
            let cmd = redis::cmd("PING");
            let pong: String = held.query(&cmd).await?;
            if pong == "PONG" {
                Ok(true)
            } else {
                Err(Error::Unavailable)
            }
        };
        tokio::time::timeout(self.timeout, operation)
            .await
            .map_err(|_| Error::Timeout)?
    }
    async fn load_password_hash(&self, username: &str) -> Result<Option<String>, Error> {
        let key = auth_key(&self.prefix, username);
        let mut held = self.pool.checkout().await?;
        let Some(bytes) = read_field(&mut held, &key, b"password_hash", MAX_HASH_BYTES).await?
        else {
            return Ok(None);
        };
        let hash = String::from_utf8(bytes).map_err(|_| Error::InvalidResult)?;
        if hash.len() > MAX_HASH_BYTES {
            return Err(Error::InvalidResult);
        }
        Ok(Some(hash))
    }
    pub async fn authenticate(
        &self,
        username: &str,
        password: Option<&[u8]>,
    ) -> Result<bool, Error> {
        let Some(password) = password else {
            return Ok(false);
        };
        if username.is_empty() || username.len() > 256 || password.len() > 65535 {
            return Err(Error::InvalidResult);
        }
        let operation = async {
            let Some(hash) = self.load_password_hash(username).await? else {
                return Ok(false);
            };
            let permit = mqtt_auth::try_acquire_password_hash_permit().ok_or(Error::Busy)?;
            let password = Zeroizing::new(password.to_vec());
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                password::verify(&hash, &password)
            })
            .await
            .map_err(|_| Error::Unavailable)?
        };
        tokio::time::timeout(self.timeout, operation)
            .await
            .map_err(|_| Error::Timeout)?
    }
    async fn load_rules(
        &self,
        username: &str,
        client_id: &str,
        action: &str,
    ) -> Result<Vec<crate::rules::Rule>, Error> {
        let key = acl_key(&self.prefix, username);
        let fields = [action_field(action), client_field(action, client_id)];
        let mut held = self.pool.checkout().await?;
        let mut loaded = Vec::with_capacity(fields.len());
        for field in &fields {
            loaded.push(read_field(&mut held, &key, field, MAX_FIELD_BYTES).await?);
        }
        drop(held);
        let mut rules = Vec::new();
        let mut total = 0usize;
        for value in loaded.into_iter().flatten() {
            parse_rules(&value, &mut rules, &mut total)?;
        }
        Ok(rules)
    }
    pub async fn authorize(
        &self,
        username: &str,
        client_id: &str,
        action: &str,
        resource: &str,
    ) -> Result<bool, Error> {
        if username.is_empty()
            || username.len() > 256
            || client_id.is_empty()
            || client_id.len() > 65535
            || !matches!(action, "publish" | "subscribe")
        {
            return Err(Error::InvalidResult);
        }
        let operation = async {
            let rules = self.load_rules(username, client_id, action).await?;
            Ok(decide(&rules, resource, action == "subscribe"))
        };
        tokio::time::timeout(self.timeout, operation)
            .await
            .map_err(|_| Error::Timeout)?
    }
}
