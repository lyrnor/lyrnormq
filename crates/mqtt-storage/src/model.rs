//! Record keys, resource limits and atomic batch operations.

/// Distinct namespaces prevent retained, session and inflight record collisions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum StorageKey {
    Session(String),
    Retained(String),
    Inflight { client_id: String, packet_id: u16 },
}
impl StorageKey {
    pub(super) fn size(&self) -> usize {
        match self {
            Self::Session(key) | Self::Retained(key) => key.len(),
            Self::Inflight { client_id, .. } => client_id.len().saturating_add(2),
        }
    }
    pub(super) fn valid(&self) -> bool {
        match self {
            Self::Session(key) | Self::Retained(key) => !key.is_empty(),
            Self::Inflight {
                client_id,
                packet_id,
            } => !client_id.is_empty() && *packet_id != 0,
        }
    }
}

/// Byte limits count UTF-8 key bytes (plus two inflight identifier bytes) and value
/// bytes. Tree/allocator overhead is separately bounded by `max_entries`.
#[derive(Debug, Clone, Copy)]
pub struct StorageLimits {
    pub max_entries: usize,
    pub max_bytes: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
}
impl Default for StorageLimits {
    fn default() -> Self {
        Self {
            max_entries: 10_000,
            max_bytes: 16 * 1024 * 1024,
            max_key_bytes: 65_535,
            max_value_bytes: 1024 * 1024,
        }
    }
}

/// A single operation in an atomic, ordered [`crate::MemoryStorage::apply_batch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    Put(StorageKey, Vec<u8>),
    Remove(StorageKey),
}
