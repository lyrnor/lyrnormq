//! Synchronous per-record storage interface.

use crate::{StorageError, StorageKey};

/// Operations are synchronous and atomic per record. Failed `put` preserves the
/// previous value. There is no multi-record transaction or durability guarantee.
/// Future blocking backends must be called through `spawn_blocking` by async users.
pub trait Storage {
    fn get(&self, key: &StorageKey) -> Option<&[u8]>;
    fn put(&mut self, key: StorageKey, value: Vec<u8>) -> Result<(), StorageError>;
    fn remove(&mut self, key: &StorageKey) -> Option<Vec<u8>>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
