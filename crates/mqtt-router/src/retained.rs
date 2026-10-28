//! Bounded retained application messages, independent of client sessions.
//!
//! This store does not perform IO or decide when a QoS exchange is complete.
//! Callers apply it as part of their session/outbox transaction. Returned records
//! are owned copies; their allocation is bounded by the store's entry/byte limits.
use crate::filter::{matches, valid_topic, validate_filter};
use std::{collections::BTreeMap, error::Error, fmt};

/// A retained publication. Topic matching returns the effective delivery QoS;
/// [`RetainedStore::entries`] returns the originally stored QoS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
}

/// Invalid inputs and limits leave previously stored records unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainedError {
    InvalidLimits,
    InvalidTopic,
    InvalidQos,
    CapacityExceeded,
}
impl fmt::Display for RetainedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidLimits => "retained store limits must be positive",
            Self::InvalidTopic => "invalid retained topic name",
            Self::InvalidQos => "retained QoS must be in 0..=2",
            Self::CapacityExceeded => "retained store capacity exceeded",
        })
    }
}
impl Error for RetainedError {}

#[derive(Debug, Clone)]
struct StoredMessage {
    payload: Vec<u8>,
    qos: u8,
}

/// Exact-topic retained records, with bounded count and aggregate logical bytes.
///
/// Byte accounting includes UTF-8 topic bytes and payload bytes; allocation/tree
/// overhead is additionally bounded by record count and MQTT's topic length limit.
/// Input buffers are already allocated by the caller and must be bounded at ingress.
#[derive(Debug, Clone)]
pub struct RetainedStore {
    records: BTreeMap<String, StoredMessage>,
    max_entries: usize,
    max_bytes: usize,
    bytes: usize,
}
impl RetainedStore {
    pub fn new(max_entries: usize, max_bytes: usize) -> Result<Self, RetainedError> {
        if max_entries == 0 || max_bytes == 0 {
            return Err(RetainedError::InvalidLimits);
        }
        Ok(Self {
            records: BTreeMap::new(),
            max_entries,
            max_bytes,
            bytes: 0,
        })
    }

    /// Replace an exact topic, or delete it when the payload is empty.
    ///
    /// Empty payloads are never stored. Replacing an existing record does not
    /// require an additional entry slot. Every input/limit check precedes mutation.
    pub fn apply(&mut self, topic: String, payload: Vec<u8>, qos: u8) -> Result<(), RetainedError> {
        if !valid_topic(&topic) {
            return Err(RetainedError::InvalidTopic);
        }
        if qos > 2 {
            return Err(RetainedError::InvalidQos);
        }
        let previous = self.records.get(&topic);
        let previous_bytes = match previous {
            Some(message) => record_bytes(&topic, &message.payload)?,
            None => 0,
        };
        let bytes_without_previous = self
            .bytes
            .checked_sub(previous_bytes)
            .ok_or(RetainedError::CapacityExceeded)?;
        if payload.is_empty() {
            self.records.remove(&topic);
            self.bytes = bytes_without_previous;
            return Ok(());
        }
        if previous.is_none() && self.records.len() >= self.max_entries {
            return Err(RetainedError::CapacityExceeded);
        }
        let next_bytes = bytes_without_previous
            .checked_add(record_bytes(&topic, &payload)?)
            .ok_or(RetainedError::CapacityExceeded)?;
        if next_bytes > self.max_bytes {
            return Err(RetainedError::CapacityExceeded);
        }
        self.records.insert(topic, StoredMessage { payload, qos });
        self.bytes = next_bytes;
        Ok(())
    }

    /// Match one accepted subscription, in topic order, capped to its grant.
    /// Invalid/unsupported filters or QoS values return no records. MQTT system
    /// topic isolation and wildcard validation reuse the ordinary routing matcher.
    pub fn matching(&self, filter: &str, qos: u8) -> Vec<RetainedMessage> {
        if qos > 2 || !validate_filter(filter) {
            return Vec::new();
        }
        self.records
            .iter()
            .filter(|(topic, _)| matches(filter, topic))
            .map(|(topic, message)| RetainedMessage {
                topic: topic.clone(),
                payload: message.payload.clone(),
                qos: message.qos.min(qos),
            })
            .collect()
    }

    /// Export owned records in topic order, preserving their original QoS.
    pub fn entries(&self) -> Vec<RetainedMessage> {
        self.records
            .iter()
            .map(|(topic, message)| RetainedMessage {
                topic: topic.clone(),
                payload: message.payload.clone(),
                qos: message.qos,
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
    /// Aggregate UTF-8 topic plus payload bytes, excluding allocator overhead.
    pub fn used_bytes(&self) -> usize {
        self.bytes
    }
}

fn record_bytes(topic: &str, payload: &[u8]) -> Result<usize, RetainedError> {
    topic
        .len()
        .checked_add(payload.len())
        .ok_or(RetainedError::CapacityExceeded)
}

#[cfg(test)]
mod tests;
