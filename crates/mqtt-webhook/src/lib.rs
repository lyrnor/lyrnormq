//! Bounded asynchronous best-effort SQL-to-Webhook actions.
//! Configuration is durable; queued events and execution counters are not.
mod config;
mod counters;
mod dispatcher;
mod error;
mod persistence;
mod runtime;
#[cfg(test)]
mod tests;
mod worker;

pub use config::{Action, Config, Connector, PublicConfig};
pub use counters::{ActionStatus, Status};
pub use error::RuntimeError;
pub use runtime::Runtime;
