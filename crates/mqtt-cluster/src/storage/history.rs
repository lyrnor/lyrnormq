//! Bounded sparse watch history. The caller holds the state lock before this lock.
use super::*;
use std::collections::VecDeque;
const MAX_UPDATES: usize = 256;
const MAX_BYTES: usize = 16 * 1024 * 1024;
#[derive(Default)]
pub(super) struct History {
    updates: VecDeque<(MetadataUpdate, usize)>,
    bytes: usize,
}
impl History {
    #[cfg(test)]
    pub(super) fn updates_len_for_test(&self) -> usize {
        self.updates.len()
    }
    pub(super) fn clear(&mut self) {
        self.updates.clear();
        self.bytes = 0;
    }
    pub(super) fn push(&mut self, update: MetadataUpdate) {
        let MetadataUpdate::Delta { base, .. } = &update else {
            self.clear();
            return;
        };
        let Ok(encoded) = encode(&update) else {
            self.clear();
            return;
        };
        if encoded.len() > MAX_BYTES {
            self.clear();
            return;
        }
        if self.updates.back().is_some_and(|(last, _)| match last {
            MetadataUpdate::Delta { revision, .. } => revision != base,
            _ => true,
        }) {
            self.clear();
        }
        self.bytes += encoded.len();
        self.updates.push_back((update, encoded.len()));
        while self.updates.len() > MAX_UPDATES || self.bytes > MAX_BYTES {
            if let Some((_, bytes)) = self.updates.pop_front() {
                self.bytes -= bytes;
            }
        }
    }
    pub(super) fn since(&self, base: u64) -> Option<Vec<MetadataUpdate>> {
        let start = self.updates.iter().position(
            |(update, _)| matches!(update, MetadataUpdate::Delta { base: b, .. } if *b == base),
        )?;
        let mut bytes = 0;
        let mut result = Vec::new();
        let mut expected = base;
        for (update, size) in self.updates.iter().skip(start).take(64) {
            let MetadataUpdate::Delta { base, revision, .. } = update else {
                return None;
            };
            if *base != expected {
                return None;
            }
            if bytes + size > MAX_BYTES {
                break;
            }
            bytes += size;
            expected = *revision;
            result.push(update.clone());
        }
        (!result.is_empty()).then_some(result)
    }
}
