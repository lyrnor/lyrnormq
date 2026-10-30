//! Bounded, IO-free MQTT SQL rule compilation and event projection.
mod error;
mod expression;
mod model;
mod plan;
pub use error::RuleError;
pub use model::{Event, Rule};
pub use plan::CompiledRule;
#[cfg(test)]
mod tests;
