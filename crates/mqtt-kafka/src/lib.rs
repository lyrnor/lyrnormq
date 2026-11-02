//! Kafka adapter; generic durable scheduling remains in mqtt-egress.
mod adapter;
mod config;
mod owner;
#[cfg(test)]
mod tests;
mod tls_validation;
pub use adapter::KafkaSink;
pub use config::{Compression, KafkaConfig, KafkaSecurity, SecurityProtocol};
pub use owner::BuildError;
