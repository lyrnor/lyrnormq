use super::Distribution;
use crate::model::{Counters, RunConfig};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Report {
    pub schema_version: u32,
    pub success: bool,
    pub run_id: u64,
    pub config: RunConfig,
    pub endpoint_mapping: &'static str,
    /// Configured TCP connections, not active/observed traffic or connection success.
    pub configured_endpoint_connections: Vec<EndpointConnections>,
    pub mode: &'static str,
    pub load_semantics: &'static str,
    pub source_completion_definition: &'static str,
    pub delivery_definition: &'static str,
    pub throughput_definition: &'static str,
    pub clock: &'static str,
    pub percentile_method: &'static str,
    pub timestamp_unit: &'static str,
    pub timing: Timing,
    pub accounting: Accounting,
    pub offering_window_events: EventCounts,
    pub after_offering_window_events: EventCounts,
    pub offering_window_tps: Throughput,
    pub measured_cohort_tps_through_drain: Throughput,
    pub latencies: Latencies,
    pub issues: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Timing {
    pub measured_start_ns: u64,
    pub measured_end_ns: u64,
    pub finished_ns: u64,
    pub offering_window_ns: u64,
    /// None when the run ended at/before the measurement origin.
    pub observed_cohort_horizon_ns: Option<u64>,
    pub drain_elapsed_ns: u64,
}

#[derive(Debug, Serialize)]
pub struct Accounting {
    pub counters: Counters,
    pub samples_retained: u64,
    pub not_sent: u64,
    /// A write without its corresponding unique subscriber application delivery.
    pub not_delivered: u64,
    /// Null for QoS0: no MQTT acknowledgement exists at that QoS.
    pub not_source_confirmed: Option<u64>,
    pub incomplete_attempts: u64,
    pub failed_pairs: u64,
    pub configured_pairs: usize,
    pub returned_pairs: usize,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct EventCounts {
    pub attempted: u64,
    pub write_completed: u64,
    pub delivered_unique: u64,
    /// Positive PUBACK for QoS1, final PUBCOMP for QoS2, null for QoS0.
    pub source_completed: Option<u64>,
    pub fully_completed: u64,
}

#[derive(Debug, Serialize)]
pub struct Throughput {
    /// Explicit denominator. None rates when the horizon is zero/unobserved.
    pub denominator_ns: Option<u64>,
    pub attempted_per_second: Option<f64>,
    pub write_completed_per_second: Option<f64>,
    pub delivered_unique_per_second: Option<f64>,
    pub source_completed_per_second: Option<f64>,
    pub fully_completed_per_second: Option<f64>,
}

impl Throughput {
    pub(super) fn new(events: &EventCounts, denominator: Option<u64>) -> Self {
        let rate = |n: u64| denominator.map(|ns| n as f64 * 1_000_000_000.0 / ns as f64);
        Self {
            denominator_ns: denominator,
            attempted_per_second: rate(events.attempted),
            write_completed_per_second: rate(events.write_completed),
            delivered_unique_per_second: rate(events.delivered_unique),
            source_completed_per_second: events.source_completed.and_then(rate),
            fully_completed_per_second: rate(events.fully_completed),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Latencies {
    pub schedule_to_send: Distribution,
    pub send_to_write_completed: Distribution,
    pub send_to_delivery: Distribution,
    pub schedule_to_delivery: Distribution,
    /// None for QoS0, never populated from intermediate QoS2 PUBREC.
    pub send_to_source_completed: Option<Distribution>,
    pub send_to_full_completion: Distribution,
}

#[derive(Debug, Serialize)]
pub struct EndpointConnections {
    pub address: std::net::SocketAddr,
    pub publishers: usize,
    pub subscribers: usize,
}
