use serde::Serialize;
use std::sync::{
    Mutex, PoisonError,
    atomic::{AtomicU64, Ordering},
};
#[derive(Clone, Debug, Default, Serialize)]
pub struct StageStatus {
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_connections: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_idle_connections: Option<usize>,
    pub requests_total: u64,
    pub allowed_total: u64,
    pub denied_total: u64,
    pub errors_total: u64,
    pub last_result: Option<&'static str>,
    pub last_http_status: Option<u16>,
    pub last_latency_ms: Option<u64>,
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct CacheStatus {
    pub entries: usize,
    pub bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub inflight: usize,
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct Status {
    pub postgresql_available: bool,
    pub redis_available: bool,
    pub authentication: StageStatus,
    pub authorization: StageStatus,
    pub cache: CacheStatus,
}
#[derive(Clone, Debug, Serialize)]
pub struct ProbeResult {
    pub result: &'static str,
    pub http_status: Option<u16>,
    pub elapsed_ms: u64,
    pub error_code: Option<&'static str>,
}
pub(crate) struct Counters {
    requests: AtomicU64,
    allowed: AtomicU64,
    denied: AtomicU64,
    errors: AtomicU64,
    last: Mutex<Option<ProbeResult>>,
}
impl Default for Counters {
    fn default() -> Self {
        Self {
            requests: AtomicU64::new(0),
            allowed: AtomicU64::new(0),
            denied: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            last: Mutex::new(None),
        }
    }
}
impl Counters {
    pub fn record(&self, r: &ProbeResult) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        match r.result {
            "allow" => {
                self.allowed.fetch_add(1, Ordering::Relaxed);
            }
            "deny" => {
                self.denied.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        *self.last.lock().unwrap_or_else(PoisonError::into_inner) = Some(r.clone());
    }
    pub fn status(&self, enabled: bool) -> StageStatus {
        let last = self.last.lock().unwrap_or_else(PoisonError::into_inner);
        StageStatus {
            enabled,
            pool_connections: None,
            pool_idle_connections: None,
            requests_total: self.requests.load(Ordering::Relaxed),
            allowed_total: self.allowed.load(Ordering::Relaxed),
            denied_total: self.denied.load(Ordering::Relaxed),
            errors_total: self.errors.load(Ordering::Relaxed),
            last_result: last.as_ref().map(|x| x.result),
            last_http_status: last.as_ref().and_then(|x| x.http_status),
            last_latency_ms: last.as_ref().map(|x| x.elapsed_ms),
        }
    }
}
