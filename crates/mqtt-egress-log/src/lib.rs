//! Generic synchronous segmented durable outbound journal.
//!
//! Execute disk methods only on a dedicated blocking owner. Payloads are opaque;
//! no adapter, network, SQL or MQTT acknowledgement semantics live here.
mod decode;
mod error;
mod format;
mod grouped;
mod journal;
mod model;
mod read_cache;
mod rewrite;
mod shared;
mod storage;
#[cfg(test)]
mod tests;

pub use error::QueueError;
pub use journal::Journal;
pub use model::{Binding, Completion, NewRecord, QueueOptions, QueueStats, Record, RecordMeta};

pub use model::{ConsumerMeta, ConsumerRecord, LegacyResult, NewConsumer, NewResult, ResultIds};
