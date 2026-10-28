//! Packet Identifier allocation and release.
use crate::SessionError;
use std::num::NonZeroU16;

/// Allocates identifiers from 1..=65535, never allocating an in-use identifier.
/// The caller releases only after the matching protocol exchange completes:
/// PUBACK for outgoing QoS1, PUBCOMP for outgoing QoS2.
#[derive(Clone)]
pub struct PacketIdentifiers {
    pub(super) used: Box<[bool; 65536]>,
    next: u16,
    pub(super) count: usize,
    limit: usize,
}
impl PacketIdentifiers {
    pub fn new(limit: usize) -> Self {
        Self {
            used: Box::new([false; 65536]),
            next: 1,
            count: 0,
            limit: limit.min(65535),
        }
    }
    pub fn allocate(&mut self) -> Result<NonZeroU16, SessionError> {
        if self.count >= self.limit {
            return Err(SessionError::InflightFull);
        }
        loop {
            let candidate = self.next;
            self.next = if candidate == u16::MAX {
                1
            } else {
                candidate + 1
            };
            if !self.used[usize::from(candidate)] {
                self.used[usize::from(candidate)] = true;
                self.count += 1;
                // candidate is always in 1..=65535.
                if let Some(id) = NonZeroU16::new(candidate) {
                    return Ok(id);
                }
            }
        }
    }
    /// Returns false if this identifier was already free.
    pub fn release(&mut self, id: NonZeroU16) -> bool {
        let occupied = &mut self.used[usize::from(id.get())];
        if !*occupied {
            return false;
        }
        *occupied = false;
        self.count -= 1;
        true
    }
    pub fn in_use(&self) -> usize {
        self.count
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
}
