//! Bounded in-process record storage and atomic batch application.

use crate::{Mutation, Storage, StorageError, StorageKey, StorageLimits};
use std::collections::BTreeMap;

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
    /// Apply an ordered batch atomically in memory, rolling back on any error.
    ///
    /// Before cloning records, validates every key/value, at most `max_entries`
    /// operations, and at most `max_bytes` total incoming value bytes. Repeated
    /// puts count every supplied value, even if a later operation overwrites it.
    /// All intermediate states must satisfy the same limits as individual puts;
    /// remove first when replacing an entry at full capacity. Missing removals
    /// are idempotent. Empty batches succeed without cloning.
    ///
    /// Staging owns a second bounded copy of existing records. Supplied input is
    /// already allocated by the caller and must also be bounded at ingress. This
    /// operation has no disk durability or concurrency synchronization semantics.
    pub fn apply_batch(&mut self, mutations: Vec<Mutation>) -> Result<(), StorageError> {
        if mutations.len() > self.limits.max_entries {
            return Err(StorageError::CapacityExceeded);
        }
        let mut incoming_bytes = 0usize;
        for mutation in &mutations {
            let key = match mutation {
                Mutation::Put(key, _) | Mutation::Remove(key) => key,
            };
            if !key.valid() {
                return Err(StorageError::InvalidKey);
            }
            if key.size() > self.limits.max_key_bytes {
                return Err(StorageError::KeyTooLarge);
            }
            if let Mutation::Put(_, value) = mutation {
                if value.len() > self.limits.max_value_bytes {
                    return Err(StorageError::ValueTooLarge);
                }
                incoming_bytes = incoming_bytes
                    .checked_add(value.len())
                    .ok_or(StorageError::CapacityExceeded)?;
                if incoming_bytes > self.limits.max_bytes {
                    return Err(StorageError::CapacityExceeded);
                }
            }
        }
        if mutations.is_empty() {
            return Ok(());
        }
        let mut staged = Self {
            records: self.records.clone(),
            limits: self.limits,
            bytes: self.bytes,
        };
        for mutation in mutations {
            match mutation {
                Mutation::Put(key, value) => staged.put(key, value)?,
                Mutation::Remove(key) => {
                    staged.remove(&key);
                }
            }
        }
        *self = staged;
        Ok(())
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
    #[test]
    fn batch_rolls_back_changes_when_later_capacity_check_fails() {
        let mut s = store();
        s.put(key("a"), vec![1; 4]).unwrap();
        s.put(key("b"), vec![2; 4]).unwrap();
        assert_eq!(
            s.apply_batch(vec![
                Mutation::Remove(key("a")),
                Mutation::Put(key("c"), vec![3; 8])
            ]),
            Err(StorageError::CapacityExceeded)
        );
        assert_eq!(s.get(&key("a")), Some(&[1; 4][..]));
        assert_eq!(s.get(&key("b")), Some(&[2; 4][..]));
        assert_eq!(s.get(&key("c")), None);
        assert_eq!(s.len(), 2);
        assert_eq!(s.used_bytes(), 10);
    }
    #[test]
    fn batch_checks_intermediate_capacity_not_only_final_state() {
        let mut s = store();
        s.put(key("a"), vec![1; 4]).unwrap();
        s.put(key("b"), vec![2; 4]).unwrap();
        assert_eq!(
            s.apply_batch(vec![
                Mutation::Put(key("c"), vec![]),
                Mutation::Remove(key("a"))
            ]),
            Err(StorageError::CapacityExceeded)
        );
        assert_eq!(s.used_bytes(), 10);
        s.apply_batch(vec![
            Mutation::Remove(key("a")),
            Mutation::Put(key("c"), vec![3; 4]),
        ])
        .unwrap();
        assert_eq!(s.get(&key("a")), None);
        assert_eq!(s.get(&key("c")), Some(&[3; 4][..]));
        assert_eq!(s.used_bytes(), 10);
    }
    #[test]
    fn batch_preflights_operation_and_input_byte_limits() {
        let mut s = store();
        s.put(key("a"), vec![1]).unwrap();
        assert_eq!(
            s.apply_batch(vec![
                Mutation::Remove(key("a")),
                Mutation::Remove(key("b")),
                Mutation::Remove(key("c"))
            ]),
            Err(StorageError::CapacityExceeded)
        );
        assert_eq!(
            s.apply_batch(vec![
                Mutation::Put(key("a"), vec![2; 6]),
                Mutation::Put(key("a"), vec![3; 6])
            ]),
            Err(StorageError::CapacityExceeded)
        );
        assert_eq!(s.get(&key("a")), Some(&[1][..]));
        assert_eq!(s.used_bytes(), 2);
        s.apply_batch(vec![
            Mutation::Put(key("a"), vec![2; 5]),
            Mutation::Put(key("a"), vec![3; 5]),
        ])
        .unwrap();
        assert_eq!(s.get(&key("a")), Some(&[3; 5][..]));
        assert_eq!(s.used_bytes(), 6);
    }
    #[test]
    fn batch_validates_each_key_and_value_and_empty_batch_is_noop() {
        let mut s = store();
        s.put(key("a"), vec![1]).unwrap();
        for (mutation, error) in [
            (Mutation::Remove(key("")), StorageError::InvalidKey),
            (Mutation::Remove(key("long")), StorageError::KeyTooLarge),
            (
                Mutation::Put(key("a"), vec![0; 9]),
                StorageError::ValueTooLarge,
            ),
            (
                Mutation::Put(
                    StorageKey::Inflight {
                        client_id: "a".into(),
                        packet_id: 0,
                    },
                    vec![],
                ),
                StorageError::InvalidKey,
            ),
        ] {
            assert_eq!(
                s.apply_batch(vec![Mutation::Remove(key("a")), mutation]),
                Err(error)
            );
            assert_eq!(s.get(&key("a")), Some(&[1][..]));
            assert_eq!(s.used_bytes(), 2);
        }
        s.apply_batch(vec![]).unwrap();
        assert_eq!(s.get(&key("a")), Some(&[1][..]));
        assert_eq!(s.used_bytes(), 2);
    }
    #[test]
    fn batch_preserves_namespaces_and_replacement_accounting() {
        let mut s = MemoryStorage::new(StorageLimits {
            max_entries: 4,
            max_bytes: 100,
            max_key_bytes: 10,
            max_value_bytes: 20,
        })
        .unwrap();
        let retained = StorageKey::Retained("a".into());
        let inflight = StorageKey::Inflight {
            client_id: "a".into(),
            packet_id: 7,
        };
        s.apply_batch(vec![
            Mutation::Put(key("a"), vec![1; 4]),
            Mutation::Put(retained.clone(), vec![2; 4]),
            Mutation::Put(inflight.clone(), vec![3; 4]),
        ])
        .unwrap();
        assert_eq!(s.len(), 3);
        assert_eq!(s.used_bytes(), 17);
        s.apply_batch(vec![
            Mutation::Put(key("a"), vec![4; 2]),
            Mutation::Remove(retained.clone()),
            Mutation::Put(inflight.clone(), vec![5]),
        ])
        .unwrap();
        assert_eq!(s.len(), 2);
        assert_eq!(s.used_bytes(), 7);
        assert_eq!(s.get(&key("a")), Some(&[4; 2][..]));
        assert_eq!(s.get(&retained), None);
        assert_eq!(s.get(&inflight), Some(&[5][..]));
    }
}
