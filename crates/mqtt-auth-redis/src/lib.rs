//! Bounded Redis HASH provider for MQTT authentication and authorization.
//!
//! Authentication reads `password_hash`. Authorization reads `effect`,
//! `topic_filter` and `priority`. The broker does not write business keys.

mod config;
mod keys;
mod password;
mod pool;
mod provider;
mod rules;
#[cfg(test)]
mod tests;

pub use config::{Config, TlsMode};
pub use provider::{Error, Provider};
