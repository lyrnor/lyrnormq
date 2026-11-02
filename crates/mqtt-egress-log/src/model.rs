use crate::QueueError;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub(crate) const MAX_RECORD_PAYLOAD: usize = 1024 * 1024;
pub(crate) const MAX_GROUP: usize = 64;
pub(crate) const MAX_GROUP_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const MAX_META: usize = 128 * 1024 * 1024;

/// Physical active-generation budget. Compaction may temporarily use at most
/// another `max_bytes` before the atomically published generation replaces it.
/// Half the budget is reserved for operation records and reclamation progress.
#[derive(Clone, Debug)]
pub struct QueueOptions {
    pub directory: PathBuf,
    pub segment_bytes: u64,
    pub max_bytes: u64,
    pub max_records: usize,
}
impl QueueOptions {
    pub fn new(directory: PathBuf) -> Self {
        Self {
            directory,
            segment_bytes: 1024 * 1024,
            max_bytes: 256 * 1024 * 1024,
            max_records: 100_000,
        }
    }
    pub(crate) fn validate(&self) -> Result<(), QueueError> {
        if self.directory.as_os_str().is_empty()
            || !(4096..=64 * 1024 * 1024).contains(&self.segment_bytes)
            || self.max_bytes < self.segment_bytes.saturating_mul(4)
            || self.max_bytes > 1024 * 1024 * 1024 * 1024
            || !(1..=1_000_000).contains(&self.max_records)
        {
            return Err(QueueError::InvalidOptions);
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct Binding {
    pub outbound_id: String,
    pub descriptor: Vec<u8>,
}
#[derive(Clone)]
pub struct NewRecord {
    pub outbound_id: String,
    pub revision: u64,
    pub client_id: String,
    pub payload: Vec<u8>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecordMeta {
    pub id: u64,
    pub outbound_id: String,
    pub revision: u64,
    pub client_id: String,
    pub failures: u32,
    pub not_before_ms: u64,
    pub inflight: bool,
    pub payload_len: usize,
}
pub struct Record {
    pub meta: RecordMeta,
    pub payload: Vec<u8>,
}
#[derive(Clone, Copy, Debug, Serialize)]
pub struct QueueStats {
    pub pending_records: usize,
    pub pending_bytes: u64,
    pub disk_bytes: u64,
    pub max_bytes: u64,
    pub recovered_records: usize,
}
#[derive(Clone, Copy, Debug)]
pub enum Completion {
    Delivered,
    Failed,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredBinding {
    pub outbound_id: String,
    pub revision: u64,
    pub descriptor: Vec<u8>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Entry {
    pub meta: RecordMeta,
    pub segment: u64,
    pub offset: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum Transaction {
    Bind {
        bindings: Vec<StoredBinding>,
        next_revision: u64,
    },
    Append {
        records: Vec<RecordMeta>,
        next_id: u64,
    },
    Begin {
        ids: Vec<u64>,
    },
    Retry {
        ids: Vec<u64>,
        not_before_ms: u64,
    },
    Complete {
        ids: Vec<u64>,
    },
    Watermark {
        next_id: u64,
        next_revision: u64,
    },
    Snapshot {
        bindings: Vec<StoredBinding>,
        entries: Vec<Entry>,
        next_id: u64,
        next_revision: u64,
    },
}
