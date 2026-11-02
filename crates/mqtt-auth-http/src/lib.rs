//! Bounded HTTP/PostgreSQL device authentication and exact-operation authorization.
mod attempt;
mod backend;
mod cache;
mod client;
mod config;
mod error;
mod persistence;
mod policy;
#[cfg(all(test, feature = "postgres-auth"))]
mod postgres_tests;
mod runtime;
mod status;
#[cfg(test)]
mod tests;

pub use client::{AclAction, AclQuery};
pub use config::{AuthorizationConfig, BackendConfig, Config, ServiceAuth};
pub use error::Error;
pub use runtime::Runtime;
pub use status::{CacheStatus, ProbeResult, StageStatus, Status};
