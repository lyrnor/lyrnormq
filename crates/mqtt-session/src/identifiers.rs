//! Packet Identifier allocation and release.
use crate::SessionError;
use std::collections::BTreeSet;
use std::num::NonZeroU16;

/// Allocates identifiers from 1..=65535, never allocating an in-use identifier.
/// The caller releases only after the matching protocol exchange completes:
/// PUBACK for outgoing QoS1, PUBCOMP for outgoing QoS2. Storage is
/// proportional to identifiers in use because every session (and every
/// proposal clone of it) carries one allocator.
#[derive(Clone)]
pub struct PacketIdentifiers {
    used: BTreeSet<NonZeroU16>,
    next: u16,
    limit: usize,
}
impl PacketIdentifiers {
    pub fn new(limit: usize) -> Self {
        Self {
            used: BTreeSet::new(),
            next: 1,
            limit: limit.min(65535),
        }
    }
    pub fn allocate(&mut self) -> Result<NonZeroU16, SessionError> {
        if self.used.len() >= self.limit {
            return Err(SessionError::InflightFull);
        }
        loop {
            let candidate = self.next;
            self.next = if candidate == u16::MAX {
                1
            } else {
                candidate + 1
            };
            if let Some(id) = NonZeroU16::new(candidate)
                && self.used.insert(id)
            {
                return Ok(id);
            }
        }
    }
    /// Marks a restored identifier in use without changing its identity.
    /// Returns false if it was already in use.
    pub fn reserve(&mut self, id: NonZeroU16) -> bool {
        self.used.insert(id)
    }
    /// Returns false if this identifier was already free.
    pub fn release(&mut self, id: NonZeroU16) -> bool {
        self.used.remove(&id)
    }
    pub fn is_used(&self, id: NonZeroU16) -> bool {
        self.used.contains(&id)
    }
    pub fn in_use(&self) -> usize {
        self.used.len()
    }
    #[cfg(test)]
    fn heap_bytes(&self) -> usize {
        self.used.len() * std::mem::size_of::<NonZeroU16>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn id(value: u16) -> NonZeroU16 {
        NonZeroU16::new(value).unwrap()
    }
    #[test]
    fn identifiers_exhaust_reuse_and_release() {
        let mut ids = PacketIdentifiers::new(usize::MAX);
        for n in 1..=65535 {
            assert_eq!(ids.allocate(), Ok(id(n)));
        }
        assert_eq!(ids.allocate(), Err(SessionError::InflightFull));
        assert!(ids.release(id(1234)));
        assert!(!ids.release(id(1234)));
        assert_eq!(ids.allocate(), Ok(id(1234)));
        assert_eq!(ids.in_use(), 65535);
        assert_eq!(
            PacketIdentifiers::new(0).allocate(),
            Err(SessionError::InflightFull)
        );
    }

    #[test]
    fn storage_scales_with_identifiers_in_use_not_the_identifier_space() {
        let mut ids = PacketIdentifiers::new(64);
        assert_eq!(ids.heap_bytes(), 0);
        for _ in 0..64 {
            ids.allocate().unwrap();
        }
        assert!(ids.heap_bytes() <= 64 * 64, "{}", ids.heap_bytes());
        assert!(ids.reserve(id(40_000)));
        assert!(!ids.reserve(id(40_000)));
        assert!(ids.is_used(id(40_000)));
        assert_eq!(ids.in_use(), 65);
        assert!(ids.release(id(40_000)));
        assert!(!ids.is_used(id(40_000)));
    }
}
