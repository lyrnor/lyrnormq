//! Bounded synchronous storage primitives and a durable SQLite snapshot backend.
//!
//! Values are opaque encoded records; protocol and session owners are responsible
//! for their schema and MQTT semantics. Call SQLite only from a blocking worker.

pub mod durable;
mod error;
mod memory;
mod model;
mod store;

pub use error::StorageError;
pub use memory::MemoryStorage;
pub use model::{Mutation, StorageKey, StorageLimits};
pub use store::Storage;
