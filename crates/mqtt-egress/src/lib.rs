//! Bounded asynchronous SQL-to-outbound execution with a generic durable journal.
//! MQTT admission remains best effort; committed outbound deliveries survive restart.
mod config;
mod counters;
mod dispatcher;
mod durable;
mod durable_worker;
mod error;
mod persistence;
mod result_wire;
mod runtime;
mod runtime_init;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod worker;

pub use config::{
    Config, DEFAULT_CONCURRENCY, DeliveryPolicy, Endpoint, MAX_CONCURRENCY, Outbound, OutboundType,
    PublicConfig,
};
pub use counters::{ActionStatus, Status};
pub use error::RuntimeError;
pub use runtime::Runtime;

pub use mqtt_egress_log::QueueOptions;
