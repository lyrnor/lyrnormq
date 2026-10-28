//! Bounded synchronous storage primitives for future broker persistence.
//!
//! The broker does not yet use these primitives. Values are opaque encoded records;
//! protocol and session owners are responsible for their schema and MQTT semantics.
use std::{collections::BTreeMap, error::Error, fmt};

/// Distinct namespaces prevent retained, session and inflight record collisions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum StorageKey {
    Session(String),
    Retained(String),
    Inflight { client_id: String, packet_id: u16 },
}
impl StorageKey {
    fn size(&self) -> usize {
        match self {
            Self::Session(key) | Self::Retained(key) => key.len(),
            Self::Inflight { client_id, .. } => client_id.len().saturating_add(2),
        }
    }
    fn valid(&self) -> bool {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageError {
    InvalidLimits,
    InvalidKey,
    KeyTooLarge,
    ValueTooLarge,
    CapacityExceeded,
}
impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidLimits => "storage limits must be positive",
            Self::InvalidKey => "empty key or zero packet identifier",
            Self::KeyTooLarge => "storage key exceeds limit",
            Self::ValueTooLarge => "storage value exceeds limit",
            Self::CapacityExceeded => "storage capacity exceeded",
        })
    }
}
impl Error for StorageError {}

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

/// In-process storage; contents are lost on drop or process termination.
#[derive(Debug)]
pub struct MemoryStorage {
    records: BTreeMap<StorageKey, Vec<u8>>,
    limits: StorageLimits,
    bytes: usize,
}
impl MemoryStorage {
    pub fn new(limits: StorageLimits) -> Result<Self, StorageError> {
        if limits.max_entries == 0
            || limits.max_bytes == 0
            || limits.max_key_bytes == 0
            || limits.max_value_bytes == 0
        {
            return Err(StorageError::InvalidLimits);
        }
        Ok(Self {
            records: BTreeMap::new(),
            limits,
            bytes: 0,
        })
    }
    pub fn used_bytes(&self) -> usize {
        self.bytes
    }
    pub fn limits(&self) -> StorageLimits {
        self.limits
    }
}
impl Storage for MemoryStorage {
    fn get(&self, key: &StorageKey) -> Option<&[u8]> {
        self.records.get(key).map(Vec::as_slice)
    }
    fn put(&mut self, key: StorageKey, value: Vec<u8>) -> Result<(), StorageError> {
        if !key.valid() {
            return Err(StorageError::InvalidKey);
        }
        let key_bytes = key.size();
        if key_bytes > self.limits.max_key_bytes {
            return Err(StorageError::KeyTooLarge);
        }
        if value.len() > self.limits.max_value_bytes {
            return Err(StorageError::ValueTooLarge);
        }
        let old = self.records.get(&key);
        if old.is_none() && self.records.len() >= self.limits.max_entries {
            return Err(StorageError::CapacityExceeded);
        }
        let previous_bytes = old.map_or(0, |v| key_bytes + v.len());
        let next_bytes = self
            .bytes
            .checked_sub(previous_bytes)
            .and_then(|v| v.checked_add(key_bytes))
            .and_then(|v| v.checked_add(value.len()))
            .ok_or(StorageError::CapacityExceeded)?;
        if next_bytes > self.limits.max_bytes {
            return Err(StorageError::CapacityExceeded);
        }
        self.records.insert(key, value);
        self.bytes = next_bytes;
        Ok(())
    }
    fn remove(&mut self, key: &StorageKey) -> Option<Vec<u8>> {
        let value = self.records.remove(key)?;
        self.bytes -= key.size() + value.len();
        Some(value)
    }
    fn len(&self) -> usize {
        self.records.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn store() -> MemoryStorage {
        MemoryStorage::new(StorageLimits {
            max_entries: 2,
            max_bytes: 10,
            max_key_bytes: 3,
            max_value_bytes: 8,
        })
        .unwrap()
    }
    fn key(s: &str) -> StorageKey {
        StorageKey::Session(s.into())
    }
    #[test]
    fn failed_replacement_preserves_old_record_and_accounting() {
        let mut s = store();
        s.put(key("a"), vec![1; 4]).unwrap();
        s.put(key("b"), vec![2; 4]).unwrap();
        assert_eq!(
            s.put(key("a"), vec![3; 5]),
            Err(StorageError::CapacityExceeded)
        );
        assert_eq!(s.get(&key("a")), Some(&[1; 4][..]));
        assert_eq!(s.used_bytes(), 10);
    }
    #[test]
    fn replace_at_entry_limit_and_reclaim_bytes() {
        let mut s = store();
        s.put(key("a"), vec![1; 4]).unwrap();
        s.put(key("b"), vec![2; 4]).unwrap();
        s.put(key("a"), vec![]).unwrap();
        assert_eq!(s.used_bytes(), 6);
        assert_eq!(s.put(key("c"), vec![]), Err(StorageError::CapacityExceeded));
        assert_eq!(s.remove(&key("b")), Some(vec![2; 4]));
        assert_eq!(s.used_bytes(), 1);
        s.put(key("c"), vec![9; 8]).unwrap();
        assert_eq!(s.used_bytes(), 10);
        s.remove(&key("a"));
        s.remove(&key("c"));
        assert!(s.is_empty());
        assert_eq!(s.used_bytes(), 0);
        assert_eq!(s.remove(&key("c")), None);
    }
    #[test]
    fn namespaces_do_not_collide() {
        let mut s = store();
        s.put(key("a"), vec![1]).unwrap();
        let retained = StorageKey::Retained("a".into());
        s.put(retained.clone(), vec![2]).unwrap();
        assert_eq!(s.get(&key("a")), Some(&[1][..]));
        assert_eq!(s.get(&retained), Some(&[2][..]));
    }
    #[test]
    fn validates_limits_keys_and_sizes_without_mutation() {
        let mut s = store();
        assert_eq!(s.put(key(""), vec![]), Err(StorageError::InvalidKey));
        assert_eq!(s.put(key("abcd"), vec![]), Err(StorageError::KeyTooLarge));
        assert_eq!(
            s.put(key("a"), vec![0; 9]),
            Err(StorageError::ValueTooLarge)
        );
        assert_eq!(
            s.put(
                StorageKey::Inflight {
                    client_id: "a".into(),
                    packet_id: 0
                },
                vec![]
            ),
            Err(StorageError::InvalidKey)
        );
        assert!(s.is_empty());
        assert_eq!(s.used_bytes(), 0);
        let limits = StorageLimits {
            max_entries: 0,
            ..StorageLimits::default()
        };
        assert!(matches!(
            MemoryStorage::new(limits),
            Err(StorageError::InvalidLimits)
        ));
    }
    #[test]
    fn inflight_keys_count_packet_identifier_bytes() {
        let mut s = store();
        s.put(
            StorageKey::Inflight {
                client_id: "a".into(),
                packet_id: 1,
            },
            vec![7],
        )
        .unwrap();
        assert_eq!(s.used_bytes(), 4);
    }
}
