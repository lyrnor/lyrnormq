//! Embedded Raft metadata control and authenticated Broker replication/forwarding.
mod broker;
mod config;
mod controller;
mod diagnosis;
mod lease;
mod model;
mod repair_state;
mod replica;
mod rpc;
mod storage;
pub use broker::{BrokerRuntime, serve_broker};
pub use config::{Config, Controller, DurableConfig, Role, TlsConfig};
pub use controller::{run_controller, status};
pub use model::ClusterStatus;

#[cfg(test)]
mod tests;

pub use broker::repair::{RepairReport, abort_repair, repair, serve_repair_learner};
