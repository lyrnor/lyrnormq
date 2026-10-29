use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Version {
    V311,
    V5,
}
impl Version {
    pub fn protocol(self) -> mqtt_protocol::ProtocolVersion {
        match self {
            Self::V311 => mqtt_protocol::ProtocolVersion::V311,
            Self::V5 => mqtt_protocol::ProtocolVersion::V5,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunConfig {
    pub address: SocketAddr,
    pub version: Version,
    pub qos: u8,
    pub pairs: usize,
    pub payload_bytes: usize,
    /// Zero means closed loop; otherwise aggregate intended messages/second.
    pub rate: u64,
    pub duration_ms: u64,
    pub warmup_ms: u64,
    pub drain_timeout_ms: u64,
    pub connect_timeout_ms: u64,
    pub io_timeout_ms: u64,
    pub inflight: u16,
    pub max_messages: u64,
    pub max_packet_bytes: usize,
}
impl Default for RunConfig {
    fn default() -> Self {
        Self {
            address: SocketAddr::from(([127, 0, 0, 1], 1883)),
            version: Version::V5,
            qos: 0,
            pairs: 2500,
            payload_bytes: 128,
            rate: 0,
            duration_ms: 60000,
            warmup_ms: 10000,
            drain_timeout_ms: 10000,
            connect_timeout_ms: 30000,
            io_timeout_ms: 10000,
            inflight: 1,
            max_messages: 5_000_000,
            max_packet_bytes: 65536,
        }
    }
}
impl RunConfig {
    pub fn quota(&self, pair: u32) -> u64 {
        self.max_messages / self.pairs.max(1) as u64
            + u64::from(u64::from(pair) < self.max_messages % self.pairs.max(1) as u64)
    }
}
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Counters {
    pub scheduled: u64,
    pub attempted: u64,
    pub write_completed: u64,
    pub puback_received: u64,
    pub pubrec_received: u64,
    pub pubcomp_received: u64,
    pub delivered_unique: u64,
    pub duplicates: u64,
    pub unexpected: u64,
    pub invalid_payload: u64,
    pub completed: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    pub pair: u32,
    pub sequence: u64,
    pub scheduled_ns: u64,
    pub send_started_ns: u64,
    pub write_completed_ns: Option<u64>,
    /// Positive PUBACK for Q1, final PUBCOMP for Q2; None for Q0.
    pub ack_ns: Option<u64>,
    pub delivery_ns: Option<u64>,
    pub completed_ns: Option<u64>,
}
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PairData {
    pub pair: u32,
    pub setup_duration_ns: u64,
    pub counters: Counters,
    pub samples: Vec<Sample>,
    pub error: Option<String>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct RunData {
    pub config: RunConfig,
    pub run_id: u64,
    pub measured_start_ns: u64,
    pub measured_end_ns: u64,
    pub finished_ns: u64,
    pub pairs: Vec<PairData>,
    pub failures: Vec<String>,
}
