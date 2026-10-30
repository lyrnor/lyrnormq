//! Bounded MQTT load generator with explicit accounting and exact percentiles.
pub mod app;
pub mod config;
pub mod engine;
pub mod error;
pub mod metrics;
pub mod model;
pub mod output;
pub mod payload;
pub use error::LoadError;
pub use model::{RunConfig, RunData};
