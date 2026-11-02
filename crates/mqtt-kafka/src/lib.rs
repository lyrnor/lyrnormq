//! Kafka adapter; generic durable scheduling remains in mqtt-egress.
mod adapter;
mod config;
mod owner;
#[cfg(test)]
mod tests;
pub use adapter::KafkaSink;
pub use config::{Compression, KafkaConfig, KafkaSecurity, SecurityProtocol};
pub use owner::BuildError;
