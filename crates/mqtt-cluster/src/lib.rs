//! Embedded Raft metadata control and authenticated Broker replication/forwarding.
mod broker;
mod config;
mod controller;
mod model;
mod rpc;
mod storage;
pub use broker::{BrokerRuntime, serve_broker};
pub use config::{Config, Controller, Role, TlsConfig};
pub use controller::{run_controller, status};
pub use model::ClusterStatus;

#[cfg(test)]
mod tests;
