//! Backend-neutral dispatch; MQTT admission and cache never depend on SQLx.
use crate::{AclQuery, BackendConfig, Error, ProbeResult, status::Counters};
use mqtt_auth::ConnectInfo;
use std::{
    fmt,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::sync::Semaphore;

enum Adapter {
    Http(Box<crate::client::Client>),
    #[cfg(feature = "postgres-auth")]
    Postgres(mqtt_auth_postgres::Provider),
}
pub(crate) struct Client {
    adapter: Adapter,
    permits: Arc<Semaphore>,
    timeout: Duration,
    pub counters: Counters,
}
impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccessBackend").finish_non_exhaustive()
    }
}
fn global() -> &'static Arc<Semaphore> {
    static GLOBAL: OnceLock<Arc<Semaphore>> = OnceLock::new();
    GLOBAL.get_or_init(|| Arc::new(Semaphore::new(256)))
}
impl Client {
    pub(crate) fn build(config: &BackendConfig, force: bool) -> Result<Self, Error> {
        config.validate(force)?;
        let adapter = match config.kind.as_str() {
            "http" => Adapter::Http(Box::new(crate::client::Client::build(config, force)?)),
            #[cfg(feature = "postgres-auth")]
            "postgresql" => Adapter::Postgres(
                mqtt_auth_postgres::Provider::build(
                    config.postgres.as_ref().ok_or(Error::InvalidConfig)?,
                    config.timeout_ms,
                )
                .map_err(|_| Error::InvalidConfig)?,
            ),
            _ => return Err(Error::InvalidConfig),
        };
        Ok(Self {
            adapter,
            permits: Arc::new(Semaphore::new(config.max_concurrency)),
            timeout: Duration::from_millis(config.timeout_ms),
            counters: Counters::default(),
        })
    }
    async fn run(
        &self,
        operation: impl std::future::Future<Output = Result<ProbeResult, Error>>,
        live: bool,
    ) -> Result<ProbeResult, Error> {
        let start = tokio::time::Instant::now();
        let global = global().clone().try_acquire_owned();
        let local = self.permits.clone().try_acquire_owned();
        let mut result = if global.is_err() || local.is_err() {
            ProbeResult {
                result: "unavailable",
                http_status: None,
                elapsed_ms: 0,
                error_code: Some("busy"),
            }
        } else {
            match tokio::time::timeout(self.timeout, operation).await {
                Ok(r) => r?,
                Err(_) => ProbeResult {
                    result: "unavailable",
                    http_status: None,
                    elapsed_ms: 0,
                    error_code: Some("timeout"),
                },
            }
        };
        result.elapsed_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        if live {
            self.counters.record(&result);
        }
        Ok(result)
    }
    pub(crate) fn status(&self) -> crate::StageStatus {
        let status = self.counters.status(true);
        #[cfg(feature = "postgres-auth")]
        if let Adapter::Postgres(p) = &self.adapter {
            let (connections, idle) = p.pool_status();
            return crate::StageStatus {
                pool_connections: Some(connections),
                pool_idle_connections: Some(idle),
                ..status
            };
        }
        status
    }
    pub(crate) async fn probe_connection(&self) -> Result<ProbeResult, Error> {
        #[cfg(feature = "postgres-auth")]
        if let Adapter::Postgres(p) = &self.adapter {
            return self
                .run(async { Ok(pg_result(p.probe_connection().await)) }, false)
                .await;
        }
        Err(Error::InvalidConfig)
    }
    pub(crate) async fn authenticate(
        &self,
        info: ConnectInfo<'_>,
        live: bool,
    ) -> Result<ProbeResult, Error> {
        if info
            .username
            .is_none_or(|s| !crate::client::valid_username(s))
            || info.client_id.is_empty()
            || info.client_id.len() > 65535
            || info.password.is_some_and(|p| p.len() > 65535)
        {
            return Err(Error::InvalidConfig);
        }
        self.run(
            async {
                match &self.adapter {
                    Adapter::Http(c) => c.authenticate(info, false).await,
                    #[cfg(feature = "postgres-auth")]
                    Adapter::Postgres(p) => Ok(pg_result(
                        p.authenticate(info.username.ok_or(Error::InvalidConfig)?, info.password)
                            .await,
                    )),
                }
            },
            live,
        )
        .await
    }
    pub(crate) async fn authorize(
        &self,
        query: &AclQuery,
        live: bool,
    ) -> Result<ProbeResult, Error> {
        if !query.validate() {
            return Err(Error::InvalidConfig);
        }
        self.run(
            async {
                match &self.adapter {
                    Adapter::Http(c) => c.authorize(query, false).await,
                    #[cfg(feature = "postgres-auth")]
                    Adapter::Postgres(p) => Ok(pg_result(
                        p.authorize(
                            &query.username,
                            &query.client_id,
                            match query.action {
                                crate::AclAction::Publish => "publish",
                                crate::AclAction::Subscribe => "subscribe",
                            },
                            &query.topic,
                        )
                        .await,
                    )),
                }
            },
            live,
        )
        .await
    }
}
#[cfg(feature = "postgres-auth")]
fn pg_result(result: Result<bool, mqtt_auth_postgres::Error>) -> ProbeResult {
    use mqtt_auth_postgres::Error;
    let (decision, error) = match result {
        Ok(true) => ("allow", None),
        Ok(false) => ("deny", None),
        Err(Error::Busy) => ("unavailable", Some("busy")),
        Err(Error::Timeout) => ("unavailable", Some("timeout")),
        Err(Error::InvalidConfig | Error::InvalidResult) => {
            ("unavailable", Some("postgres_invalid_result"))
        }
        Err(Error::Unavailable) => ("unavailable", Some("postgres_unavailable")),
    };
    ProbeResult {
        result: decision,
        http_status: None,
        elapsed_ms: 0,
        error_code: error,
    }
}
