//! Generic synchronous segmented durable outbound journal.
//!
//! Execute disk methods only on a dedicated blocking owner. Payloads are opaque;
//! no adapter, network, SQL or MQTT acknowledgement semantics live here.
mod error;
mod format;
mod journal;
mod model;
mod storage;
#[cfg(test)]
mod tests;

pub use error::QueueError;
pub use journal::Journal;
pub use model::{Binding, Completion, NewRecord, QueueOptions, QueueStats, Record, RecordMeta};
