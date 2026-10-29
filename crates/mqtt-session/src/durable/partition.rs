//! One-client partitions for per-session actors (Phase4). A partition never
//! routes to other sessions: publications leave as values, retained state lives
//! outside, and each recipient partition applies its own delivery.
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

/// Last measured live projection size; shared clones observe the same counter
/// only through explicit copies.
#[derive(Debug, Default)]
pub(super) struct LiveBytes(AtomicUsize);

impl LiveBytes {
    pub(super) fn get(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
    pub(super) fn set(&self, bytes: usize) {
        self.0.store(bytes, Ordering::Relaxed);
    }
}

impl Clone for LiveBytes {
    fn clone(&self) -> Self {
        Self(AtomicUsize::new(self.get()))
    }
}

/// A validated PUBLISH or Will leaving its source partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publication {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
    pub properties: Properties,
    pub expires_at: Option<u64>,
    pub publisher: Option<String>,
    /// Expired at publication time: only its retained side effect applies.
    pub expired: bool,
}

impl Publication {
    pub(super) fn from_message(message: Message, publisher: Option<&str>, expired: bool) -> Self {
        Self {
            topic: message.topic,
            payload: message.payload,
            qos: message.qos,
            retain: message.retain,
            properties: message.properties,
            expires_at: message.expires_at,
            publisher: publisher.map(str::to_owned),
            expired,
        }
    }
    fn message(&self) -> Message {
        Message {
            topic: self.topic.clone(),
            payload: self.payload.clone(),
            qos: self.qos,
            retain: self.retain,
            retained_replay: false,
            shared_subscription: None,
            expires_at: self.expires_at,
            properties: self.properties.clone(),
        }
    }
    /// The retained-store update this publication requests, if any. An empty
    /// payload or an expired message clears the topic.
    pub fn retained_update(&self) -> Option<RetainedEntry> {
        self.retain.then(|| {
            let clear = self.payload.is_empty() || self.expired;
            RetainedEntry {
                topic: self.topic.clone(),
                payload: if clear {
                    Vec::new()
                } else {
                    self.payload.clone()
                },
                qos: self.qos,
                properties: if clear {
                    Vec::new()
                } else {
                    self.properties.clone()
                },
                expires_at: if clear { None } else { self.expires_at },
            }
        })
    }
}

/// One retained topic; an empty payload is a deletion when applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetainedEntry {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub properties: Properties,
    pub expires_at: Option<u64>,
}

/// Which local copies one delivery may create. Shared groups are selected by
/// the caller; the partition only checks its own membership and capacity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryScope {
    Ordinary,
    Shared(String),
}

/// A legacy whole-broker engine split into one engine per client identifier.
pub struct Partitions {
    pub clients: Vec<(String, SessionEngine)>,
    pub retained: Vec<RetainedEntry>,
    pub clock_floor: u64,
}

impl SessionEngine {
    pub fn new_partition(
        config: SessionConfig,
        access: Arc<dyn AccessControl>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, EngineError> {
        let mut engine = Self::new_with_clock(config, access, clock)?;
        engine.partition = true;
        Ok(engine)
    }

    /// Restore one partition row; rows holding retained data or several
    /// clients fail closed.
    pub fn restore_partition(
        config: SessionConfig,
        bytes: &[u8],
        access: Arc<dyn AccessControl>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, EngineError> {
        let mut engine = Self::restore_with_clock(config, bytes, access, clock)?;
        let clients: BTreeSet<&String> =
            engine.sessions.keys().chain(engine.wills.keys()).collect();
        if !engine.retained.is_empty() || clients.len() > 1 {
            return Err(invalid_record("partition row must hold exactly one client"));
        }
        engine.partition = true;
        Ok(engine)
    }

    /// Split a legacy engine. Retained state and the clock floor leave the
    /// partitions; every partition observes the shared floor.
    pub fn into_partitions(self) -> Result<Partitions, EngineError> {
        let clock_floor = self.clock_floor.get();
        let retained = self.retained_entries();
        let mut owners: BTreeMap<String, (Option<Box<Session>>, Option<WillRecord>)> =
            BTreeMap::new();
        for (client, session) in self.sessions {
            owners.entry(client).or_default().0 = Some(session);
        }
        for (client, will) in self.wills {
            owners.entry(client).or_default().1 = Some(will);
        }
        let mut clients = Vec::with_capacity(owners.len());
        for (client, (session, will)) in owners {
            let mut engine =
                Self::new_partition(self.config, self.access.clone(), self.clock.clone())?;
            engine.clock_floor.observe(clock_floor);
            if let Some(session) = session {
                engine.sessions.insert(client.clone(), session);
            }
            if let Some(will) = will {
                engine.wills.insert(client.clone(), will);
            }
            for group in engine.active_shared_groups() {
                if let Some(cursor) = self.shared_cursors.get(&group) {
                    engine.shared_cursors.insert(group.clone(), cursor.clone());
                }
                engine.ensure_shared_epoch(&group)?;
            }
            engine.ensure_bounds()?;
            clients.push((client, engine));
        }
        Ok(Partitions {
            clients,
            retained,
            clock_floor,
        })
    }

    /// Merge partitions and retained entries into one legacy engine, the
    /// inverse of [`Self::into_partitions`], for inspection and export.
    pub fn from_partitions(
        config: SessionConfig,
        partitions: Vec<SessionEngine>,
        retained: Vec<RetainedEntry>,
    ) -> Result<Self, EngineError> {
        let mut merged = Self::new(config)?;
        for partition in partitions {
            merged.clock_floor.observe(partition.clock_floor.get());
            for (client, session) in partition.sessions {
                if merged.sessions.insert(client, session).is_some() {
                    return Err(invalid_record("duplicate client across partitions"));
                }
            }
            for (client, will) in partition.wills {
                if merged.wills.insert(client, will).is_some() {
                    return Err(invalid_record("duplicate Will across partitions"));
                }
            }
            for (group, cursor) in partition.shared_cursors {
                merged.shared_cursors.entry(group).or_insert(cursor);
            }
        }
        for group in merged.active_shared_groups() {
            merged.ensure_shared_epoch(&group)?;
        }
        merged.stage_retained(retained)?;
        merged.ensure_bounds()?;
        Ok(merged)
    }

    pub fn is_partition(&self) -> bool {
        self.partition
    }
    pub fn take_publications(&mut self) -> Vec<Publication> {
        std::mem::take(&mut self.publications)
    }
    pub fn discard_publications(&mut self) {
        self.publications.clear();
    }

    /// Apply one routed copy to this partition. Returns whether a copy was
    /// queued; a full queue reports `Capacity` and leaves the engine unchanged
    /// only if the caller discards it.
    pub fn deliver(
        &mut self,
        publication: &Publication,
        scope: &DeliveryScope,
    ) -> Result<bool, EngineError> {
        if publication.expired {
            return Ok(false);
        }
        let message = publication.message();
        self.validate_properties(
            &message.packet((message.qos != 0).then_some(1), false),
            &message.properties,
        )?;
        let admitted = self.route_scoped(message, publication.publisher.as_deref(), Some(scope))?;
        self.ensure_bounds()?;
        Ok(admitted > 0)
    }

    /// Stage global retained entries for one SUBSCRIBE replay. Staged entries
    /// must be cleared before snapshot.
    pub fn stage_retained(&mut self, entries: Vec<RetainedEntry>) -> Result<(), EngineError> {
        for entry in entries {
            if entry.payload.is_empty() {
                self.retained_properties.remove(&entry.topic);
                self.retained_expiry.remove(&entry.topic);
            } else {
                self.retained_properties
                    .insert(entry.topic.clone(), entry.properties);
                match entry.expires_at {
                    Some(deadline) => {
                        self.retained_expiry.insert(entry.topic.clone(), deadline);
                    }
                    None => {
                        self.retained_expiry.remove(&entry.topic);
                    }
                }
            }
            self.retained
                .apply(entry.topic, entry.payload, entry.qos)
                .map_err(EngineError::Retained)?;
        }
        Ok(())
    }
    pub fn clear_staged_retained(&mut self) -> Result<(), EngineError> {
        self.retained = RetainedStore::new(self.config.max_retained, self.config.max_state_bytes)
            .map_err(EngineError::Retained)?;
        self.retained_properties.clear();
        self.retained_expiry.clear();
        self.ensure_bounds()
    }
    pub fn retained_entries(&self) -> Vec<RetainedEntry> {
        self.retained
            .entries()
            .into_iter()
            .map(|entry| RetainedEntry {
                properties: self
                    .retained_properties
                    .get(&entry.topic)
                    .cloned()
                    .unwrap_or_default(),
                expires_at: self.retained_expiry.get(&entry.topic).copied(),
                topic: entry.topic,
                payload: entry.payload,
                qos: entry.qos,
            })
            .collect()
    }

    /// Every subscribed filter, shared keys included.
    pub fn subscription_filters(&self) -> BTreeMap<String, BTreeSet<String>> {
        self.sessions
            .iter()
            .map(|(client, session)| {
                (
                    client.clone(),
                    session.subscriptions.keys().cloned().collect(),
                )
            })
            .collect()
    }
    pub fn is_online(&self, client_id: &str) -> bool {
        self.sessions
            .get(client_id)
            .is_some_and(|session| session.online)
    }
    pub fn is_vacant(&self) -> bool {
        self.sessions.is_empty() && self.wills.is_empty()
    }
    /// Snapshot rows exist only for persistent sessions and pending Wills.
    pub fn has_persistent_state(&self) -> bool {
        self.sessions.values().any(|session| session.persistent) || !self.wills.is_empty()
    }
    pub fn clock_floor(&self) -> u64 {
        self.clock_floor.get()
    }
    pub fn observe_clock_floor(&self, floor: u64) {
        self.clock_floor.observe(floor);
    }
    /// Bytes measured by the last bounds check.
    pub fn live_state_bytes(&self) -> usize {
        self.live_bytes.get()
    }
}

/// Encode one retained topic as a schema7 envelope holding only that record.
pub fn encode_retained_row(
    config: SessionConfig,
    entry: RetainedEntry,
) -> Result<Vec<u8>, EngineError> {
    let mut engine = SessionEngine::new(config)?;
    engine.stage_retained(vec![entry])?;
    engine.snapshot()
}

pub fn decode_retained_row(
    config: SessionConfig,
    bytes: &[u8],
) -> Result<RetainedEntry, EngineError> {
    let engine = SessionEngine::restore(config, bytes)?;
    let mut entries = engine.retained_entries();
    match (entries.pop(), entries.is_empty(), engine.is_vacant()) {
        (Some(entry), true, true) => Ok(entry),
        _ => Err(invalid_record("retained row must hold exactly one record")),
    }
}

#[cfg(test)]
#[path = "partition_tests.rs"]
mod tests;
