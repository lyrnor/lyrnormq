use crate::{
    Config, TlsMode, password,
    rules::{Rule, decide},
};
use futures_util::TryStreamExt;
use sqlx::{
    ConnectOptions, PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
};
use std::{
    fmt,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::sync::Semaphore;
use zeroize::Zeroizing;

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
            Self::InvalidConfig => "postgres_invalid_config",
            Self::InvalidResult => "postgres_invalid_result",
            Self::Unavailable => "postgres_unavailable",
            Self::Busy => "busy",
            Self::Timeout => "timeout",
        })
    }
}
impl std::error::Error for Error {}
pub struct Provider {
    pool: PgPool,
    query: String,
    timeout: Duration,
}
impl fmt::Debug for Provider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresProvider").finish_non_exhaustive()
    }
}
fn hash_slots() -> &'static Arc<Semaphore> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SLOTS.get_or_init(|| Arc::new(Semaphore::new(4)))
}
impl Provider {
    pub fn build(config: &Config, timeout_ms: u64) -> Result<Self, Error> {
        if !config.validate() || !(100..=5000).contains(&timeout_ms) {
            return Err(Error::InvalidConfig);
        }
        let mut options = PgConnectOptions::new()
            .host(&config.host)
            .port(config.port)
            .database(&config.database)
            .username(&config.username)
            .password(&config.password)
            .disable_statement_logging()
            .ssl_mode(match config.tls_mode {
                TlsMode::VerifyFull => PgSslMode::VerifyFull,
                TlsMode::Disable => PgSslMode::Disable,
            });
        if let Some(ca) = &config.ca_pem {
            options = options.ssl_root_cert_from_pem(ca.as_bytes().to_vec());
        }
        let timeout = Duration::from_millis(timeout_ms);
        let statement_timeout = timeout_ms.to_string();
        let pool = PgPoolOptions::new().max_connections(config.max_connections).min_connections(0)
            .acquire_timeout(timeout).idle_timeout(Duration::from_secs(30))
            .after_connect(move |conn, _| {
                let deadline = statement_timeout.clone();
                Box::pin(async move {
                    sqlx::query("SELECT set_config('statement_timeout', $1, false), set_config('lock_timeout', $1, false), set_config('default_transaction_read_only', 'on', false)")
                        .bind(deadline).execute(conn).await?;
                    Ok(())
                })
            }).connect_lazy_with(options);
        Ok(Self {
            pool,
            query: config.query.trim().trim_end_matches(';').trim().into(),
            timeout,
        })
    }
    async fn transaction(&self) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, Error> {
        let mut tx = self.pool.begin().await.map_err(|_| Error::Unavailable)?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(tx)
    }
    pub fn pool_status(&self) -> (u32, usize) {
        (self.pool.size(), self.pool.num_idle())
    }
    pub async fn probe_connection(&self) -> Result<bool, Error> {
        tokio::time::timeout(self.timeout, async {
            let mut connection = self.pool.acquire().await.map_err(|_| Error::Unavailable)?;
            sqlx::query("SELECT 1")
                .execute(&mut *connection)
                .await
                .map_err(|_| Error::Unavailable)?;
            Ok(true)
        })
        .await
        .map_err(|_| Error::Timeout)?
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
            let mut tx = self.transaction().await?;
            let query = format!(
                "SELECT left(password_hash, 513) AS password_hash FROM (\n{}\n) AS mqtt_auth_result LIMIT 2",
                self.query
            );
            let rows = sqlx::query(&query)
                .bind(username)
                .fetch_all(&mut *tx)
                .await
                .map_err(|_| Error::Unavailable)?;
            tx.rollback().await.map_err(|_| Error::Unavailable)?;
            if rows.is_empty() {
                return Ok(false);
            }
            if rows.len() != 1 {
                return Err(Error::InvalidResult);
            }
            let hash: String = rows[0]
                .try_get("password_hash")
                .map_err(|_| Error::InvalidResult)?;
            if hash.len() > 512 {
                return Err(Error::InvalidResult);
            }
            let permit = hash_slots()
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::Busy)?;
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
    pub async fn authorize(
        &self,
        username: &str,
        client_id: &str,
        action: &str,
        resource: &str,
    ) -> Result<bool, Error> {
        let operation = async {
            let mut tx = self.transaction().await?;
            let query = format!(
                "SELECT left(effect, 6) AS effect, left(topic_filter, 65536) AS topic_filter, priority::bigint AS priority FROM (\n{}\n) AS mqtt_acl_result LIMIT 257",
                self.query
            );
            let mut rules = Vec::new();
            let mut bytes = 0usize;
            let mut rows = sqlx::query(&query)
                .bind(username)
                .bind(client_id)
                .bind(action)
                .fetch(&mut *tx);
            while let Some(row) = rows.try_next().await.map_err(|_| Error::Unavailable)? {
                if rules.len() >= 256 {
                    return Err(Error::InvalidResult);
                }
                let effect: String = row.try_get("effect").map_err(|_| Error::InvalidResult)?;
                let filter: String = row
                    .try_get("topic_filter")
                    .map_err(|_| Error::InvalidResult)?;
                let priority: i64 = row.try_get("priority").map_err(|_| Error::InvalidResult)?;
                bytes = bytes.saturating_add(filter.len());
                if filter.len() > 65535
                    || bytes > 1024 * 1024
                    || !mqtt_router::validate_topic_filter(&filter)
                    || !matches!(effect.as_str(), "allow" | "deny")
                {
                    return Err(Error::InvalidResult);
                }
                rules.push(Rule {
                    effect: effect == "allow",
                    filter,
                    priority,
                });
            }
            drop(rows);
            tx.rollback().await.map_err(|_| Error::Unavailable)?;
            Ok(decide(&rules, resource, action == "subscribe"))
        };
        tokio::time::timeout(self.timeout, operation)
            .await
            .map_err(|_| Error::Timeout)?
    }
}
