//! Opt-in, bounded process-wide elapsed-time counters for session operations.
//!
//! Set `RMQTT_PROFILE=1` before process startup. The setting is read once. Nested
//! spans are inclusive and overlap; their times are not exclusive CPU shares.
//! Reports contain only fixed operation labels, never application/client data.
use serde::Serialize;
use std::{
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

/// Fixed instrumentation labels; no input-dependent label allocation occurs.
#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub enum Operation {
    Clone,
    RecordsLive,
    RecordsPersistent,
    SerializeLive,
    SerializePersistent,
    Bounds,
    Route,
    RouteBuild,
    Authorization,
}
const OPERATIONS: [Operation; 9] = [
    Operation::Clone,
    Operation::RecordsLive,
    Operation::RecordsPersistent,
    Operation::SerializeLive,
    Operation::SerializePersistent,
    Operation::Bounds,
    Operation::Route,
    Operation::RouteBuild,
    Operation::Authorization,
];
impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::Clone => "clone",
            Self::RecordsLive => "records_live",
            Self::RecordsPersistent => "records_persistent",
            Self::SerializeLive => "serialize_live",
            Self::SerializePersistent => "serialize_persistent",
            Self::Bounds => "bounds",
            Self::Route => "route",
            Self::RouteBuild => "route_build",
            Self::Authorization => "authorization",
        }
    }
}
struct Counter {
    count: AtomicU64,
    total_ns: AtomicU64,
    bytes: AtomicU64,
}
impl Counter {
    const fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            total_ns: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }
}
static ENABLED: OnceLock<bool> = OnceLock::new();
static COUNTERS: [Counter; OPERATIONS.len()] = [const { Counter::new() }; OPERATIONS.len()];
pub(crate) fn enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var_os("RMQTT_PROFILE").is_some_and(|value| value == "1"))
}
fn saturating_add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
        Some(old.saturating_add(value))
    });
}
fn as_u64(value: u128) -> u64 {
    value.min(u128::from(u64::MAX)) as u64
}

/// RAII elapsed span. Disabled spans take no clock reading and allocate nothing.
#[must_use = "keep the span guard alive for the measured scope"]
pub struct Span {
    operation: Operation,
    started: Option<Instant>,
}
impl Drop for Span {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            let counter = &COUNTERS[self.operation as usize];
            saturating_add(&counter.total_ns, as_u64(started.elapsed().as_nanos()));
            saturating_add(&counter.count, 1);
        }
    }
}

/// Begin an inclusive elapsed-time span if process profiling is enabled.
pub fn span(operation: Operation) -> Span {
    Span {
        operation,
        started: enabled().then(Instant::now),
    }
}

/// Add produced serialization bytes to the corresponding fixed operation total.
/// Callers use this for completed/partially produced byte buffers, not input data.
pub fn record_bytes(operation: Operation, bytes: usize) {
    if enabled() {
        saturating_add(&COUNTERS[operation as usize].bytes, as_u64(bytes as u128));
    }
}
#[derive(Serialize)]
struct Row {
    name: &'static str,
    count: u64,
    total_ns: u64,
    bytes: u64,
}
#[derive(Serialize)]
struct Report {
    enabled: bool,
    inclusive: bool,
    operations: [Row; OPERATIONS.len()],
}

/// Snapshot bounded atomic counters as JSON. Concurrent readers may see updates
/// between fields; quiescent worker shutdown provides a complete event snapshot.
/// Counters saturate instead of wrapping. Only report construction allocates.
pub fn report_json() -> Result<String, serde_json::Error> {
    let report = Report {
        enabled: enabled(),
        inclusive: true,
        operations: OPERATIONS.map(|operation| {
            let counter = &COUNTERS[operation as usize];
            Row {
                name: operation.name(),
                count: counter.count.load(Ordering::Relaxed),
                total_ns: counter.total_ns.load(Ordering::Relaxed),
                bytes: counter.bytes.load(Ordering::Relaxed),
            }
        }),
    };
    serde_json::to_string(&report)
}

#[cfg(test)]
mod tests;
