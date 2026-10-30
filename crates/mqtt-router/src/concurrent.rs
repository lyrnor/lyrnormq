//! Broker-wide retained store shared by per-session actors.
//!
//! Every operation takes a short synchronous lock; callers must not hold
//! returned data as a lock guard and never wait while calling in.
use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use crate::retained::{RetainedError, RetainedMessage, RetainedStore};

/// A [`RetainedStore`] with caller-defined per-topic metadata (properties,
/// expiry) kept in the same critical section.
pub struct ConcurrentRetained<M> {
    state: Mutex<(RetainedStore, HashMap<String, M>)>,
}

impl<M: Clone> ConcurrentRetained<M> {
    pub fn new(max_entries: usize, max_bytes: usize) -> Result<Self, RetainedError> {
        Ok(Self {
            state: Mutex::new((RetainedStore::new(max_entries, max_bytes)?, HashMap::new())),
        })
    }

    /// Replace or delete (empty payload) one topic; failures change nothing.
    pub fn apply(
        &self,
        topic: String,
        payload: Vec<u8>,
        qos: u8,
        metadata: M,
    ) -> Result<(), RetainedError> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (store, metadata_by_topic) = &mut *state;
        let delete = payload.is_empty();
        store.apply(topic.clone(), payload, qos)?;
        if delete {
            metadata_by_topic.remove(&topic);
        } else {
            metadata_by_topic.insert(topic, metadata);
        }
        Ok(())
    }

    pub fn matching(&self, filter: &str, qos: u8) -> Vec<(RetainedMessage, M)> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        with_metadata(state.0.matching(filter, qos), &state.1)
    }

    /// Visit ordered metadata under a synchronous lock, without payload copies.
    /// Callbacks must not block, await, or call back into this store.
    pub fn visit_metadata(&self, mut visitor: impl FnMut(&str, u8, usize, &M)) {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.0.visit_metadata(|topic, qos, bytes| {
            if let Some(metadata) = state.1.get(topic) {
                visitor(topic, qos, bytes, metadata);
            }
        });
    }

    pub fn entries(&self) -> Vec<(RetainedMessage, M)> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        with_metadata(state.0.entries(), &state.1)
    }

    /// Delete every topic whose metadata fails `keep`; returns deleted topics.
    pub fn prune(&self, mut keep: impl FnMut(&M) -> bool) -> Vec<String> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (store, metadata_by_topic) = &mut *state;
        let stale: Vec<String> = metadata_by_topic
            .iter()
            .filter(|(_, metadata)| !keep(metadata))
            .map(|(topic, _)| topic.clone())
            .collect();
        for topic in &stale {
            if store.apply(topic.clone(), Vec::new(), 0).is_ok() {
                metadata_by_topic.remove(topic);
            }
        }
        stale
    }

    pub fn len(&self) -> usize {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn with_metadata<M: Clone>(
    messages: Vec<RetainedMessage>,
    metadata: &HashMap<String, M>,
) -> Vec<(RetainedMessage, M)> {
    messages
        .into_iter()
        .filter_map(|message| {
            let metadata = metadata.get(&message.topic)?.clone();
            Some((message, metadata))
        })
        .collect()
}

#[cfg(test)]
#[path = "concurrent_tests.rs"]
mod tests;
