//! Atomic generation replacement. Only bounded groups of payloads enter memory.
use crate::{
    Journal, QueueError, format,
    model::*,
    shared::{Shared, valid_consumer},
    storage,
};
use std::{
    collections::BTreeMap,
    fs::File,
    ops::Bound::{Excluded, Unbounded},
    path::PathBuf,
};

struct Writer {
    dir: PathBuf,
    file: File,
    segment: u64,
    len: u64,
    physical: u64,
    max: u64,
    target: u64,
    legacy: BTreeMap<u64, Entry>,
    shared: Shared,
}
impl Writer {
    fn new(j: &Journal) -> Result<Self, QueueError> {
        let generation = j.generation.checked_add(1).ok_or(QueueError::Io)?;
        let dir = storage::generation(&j.root, generation);
        storage::directory(&dir)?;
        let file = storage::private_file(&storage::segment(&dir, 1), true)?;
        let mut w = Self {
            dir,
            file,
            segment: 1,
            len: 0,
            physical: 0,
            max: j.options.max_bytes,
            target: j.options.segment_bytes,
            legacy: BTreeMap::new(),
            shared: Shared::default(),
        };
        w.write(
            &Transaction::Snapshot {
                bindings: j.bindings.values().cloned().collect(),
                entries: Vec::new(),
                next_id: 1,
                next_revision: j.next_revision,
            },
            &[],
        )?;
        Ok(w)
    }
    fn write(&mut self, txn: &Transaction, payloads: &[&[u8]]) -> Result<u64, QueueError> {
        let json = format::json(txn)?;
        let size = format::encoded_len(&json, payloads)?;
        if self.physical.saturating_add(size).saturating_add(20) > self.max {
            return Err(QueueError::Full);
        }
        if self.len > 0 && self.len.saturating_add(size) > self.target {
            self.file.sync_all()?;
            self.segment = self.segment.checked_add(1).ok_or(QueueError::Io)?;
            if self.segment > storage::MAX_SEGMENTS as u64 {
                return Err(QueueError::Full);
            }
            self.file = storage::private_file(&storage::segment(&self.dir, self.segment), true)?;
            self.len = 0;
        }
        let (offset, size) = format::write_frame(&mut self.file, &json, payloads)?;
        self.len += size;
        self.physical += size;
        Ok(offset)
    }
    fn results(
        &mut self,
        records: Vec<StoredResult>,
        payloads: Vec<Vec<u8>>,
        next: u64,
    ) -> Result<(), QueueError> {
        let refs: Vec<_> = payloads.iter().map(Vec::as_slice).collect();
        let mut offset = self.write(
            &Transaction::Results {
                results: records.clone(),
                next_id: next,
            },
            &refs,
        )?;
        for r in records {
            let n = r.meta.payload_len as u64;
            self.shared.insert(r, self.segment, offset)?;
            offset = offset.checked_add(n).ok_or(QueueError::Corrupt)?;
        }
        Ok(())
    }
}
impl Journal {
    fn copy_legacy(&self, w: &mut Writer) -> Result<(), QueueError> {
        let mut cursor = None;
        loop {
            let mut stored = Vec::new();
            let mut payloads = Vec::new();
            let mut bytes = 0usize;
            for (id, e) in self
                .entries
                .range((cursor.map_or(Unbounded, Excluded), Unbounded))
                .take(MAX_GROUP)
            {
                if !stored.is_empty() && bytes.saturating_add(e.meta.payload_len) > MAX_GROUP_BYTES
                {
                    break;
                }
                payloads.push(storage::read_payload(
                    &self.dir,
                    e.segment,
                    e.offset,
                    e.meta.payload_len,
                    e.checksum,
                )?);
                bytes += e.meta.payload_len;
                stored.push((
                    *id,
                    StoredRecord {
                        meta: e.meta.clone(),
                        checksum: e.checksum,
                    },
                ));
            }
            if stored.is_empty() {
                break;
            }
            let last = stored.last().map(|(id, _)| *id).ok_or(QueueError::Io)?;
            let refs: Vec<_> = payloads.iter().map(Vec::as_slice).collect();
            let mut offset = w.write(
                &Transaction::Append {
                    records: stored.iter().map(|(_, r)| r.clone()).collect(),
                    next_id: last.checked_add(1).ok_or(QueueError::Io)?,
                },
                &refs,
            )?;
            for (_, r) in stored {
                let n = r.meta.payload_len as u64;
                w.legacy.insert(
                    r.meta.id,
                    Entry {
                        meta: r.meta,
                        checksum: r.checksum,
                        segment: w.segment,
                        offset,
                    },
                );
                offset += n;
            }
            cursor = Some(last);
        }
        Ok(())
    }
    fn copy_shared(&self, w: &mut Writer) -> Result<(), QueueError> {
        let mut cursor = None;
        loop {
            let mut records = Vec::new();
            let mut payloads = Vec::new();
            let mut bytes = 0usize;
            for (id, b) in self
                .shared
                .bodies
                .range((cursor.map_or(Unbounded, Excluded), Unbounded))
                .take(MAX_GROUP)
            {
                if !records.is_empty() && bytes.saturating_add(b.meta.payload_len) > MAX_GROUP_BYTES
                {
                    break;
                }
                let consumers = b
                    .consumers
                    .iter()
                    .map(|id| {
                        self.shared
                            .consumers
                            .get(id)
                            .map(|(_, c)| c.clone())
                            .ok_or(QueueError::Corrupt)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                payloads.push(storage::read_payload(
                    &self.dir,
                    b.segment,
                    b.offset,
                    b.meta.payload_len,
                    b.checksum,
                )?);
                bytes += b.meta.payload_len;
                records.push(StoredResult {
                    meta: b.meta.clone(),
                    checksum: b.checksum,
                    consumers,
                });
                cursor = Some(*id);
            }
            if records.is_empty() {
                break;
            }
            w.results(records, payloads, self.next_id)?;
        }
        Ok(())
    }
    fn publish_rewrite(&mut self, mut w: Writer, next: u64) -> Result<(), QueueError> {
        w.write(
            &Transaction::Watermark {
                next_id: next,
                next_revision: self.next_revision,
            },
            &[],
        )?;
        w.file.sync_all()?;
        storage::sync_dir(&w.dir)?;
        #[cfg(test)]
        self.inject(Fault::BeforePublish)?;
        let generation = self.generation.checked_add(1).ok_or(QueueError::Io)?;
        storage::publish(&self.root, generation)?;
        #[cfg(test)]
        self.inject(Fault::AfterPublish)?;
        let old_dir = std::mem::replace(&mut self.dir, w.dir);
        let old_file = std::mem::replace(&mut self.active, w.file);
        drop(old_file);
        self.generation = generation;
        self.segment = w.segment;
        self.active_len = w.len;
        self.physical = w.physical;
        self.entries = w.legacy;
        self.shared = w.shared;
        self.reads.get_mut().map_err(|_| QueueError::Io)?.clear();
        self.next_id = next;
        self.lanes.clear();
        self.heads.clear();
        self.counts.clear();
        for (id, e) in &self.entries {
            let lane = self
                .lanes
                .entry((e.meta.outbound_id.clone(), e.meta.client_id.clone()))
                .or_default();
            if lane.is_empty() {
                self.heads
                    .entry(e.meta.outbound_id.clone())
                    .or_default()
                    .insert(*id);
            }
            lane.push_back(*id);
            *self.counts.entry(e.meta.outbound_id.clone()).or_default() += 1;
        }
        for (_, c) in self.shared.consumers.values() {
            *self.counts.entry(c.outbound_id.clone()).or_default() += 1;
        }
        self.pending_bytes = self
            .entries
            .values()
            .map(|e| e.meta.payload_len as u64)
            .sum::<u64>()
            + self
                .shared
                .bodies
                .values()
                .map(|b| b.meta.payload_len as u64)
                .sum::<u64>();
        self.recompute_live()?;
        storage::remove_generation(&old_dir)?;
        storage::sync_dir(&self.root)?;
        Ok(())
    }
    pub(crate) fn rewrite_checkpoint(&mut self) -> Result<(), QueueError> {
        let mut w = Writer::new(self)?;
        self.copy_legacy(&mut w)?;
        self.copy_shared(&mut w)?;
        self.publish_rewrite(w, self.next_id)
    }
    pub fn migrate_legacy<F>(&mut self, mut resolver: F) -> Result<(), QueueError>
    where
        F: FnMut(&Record) -> Result<LegacyResult, QueueError>,
    {
        self.healthy()?;
        if self.entries.is_empty() {
            return Ok(());
        }
        let result = (|| {
            let mut w = Writer::new(self)?;
            self.copy_shared(&mut w)?;
            let mut next = self.next_id;
            let mut keys = BTreeMap::<(String, String, String), u64>::new();
            for e in self.entries.values() {
                let payload = storage::read_payload(
                    &self.dir,
                    e.segment,
                    e.offset,
                    e.meta.payload_len,
                    e.checksum,
                )?;
                let record = Record {
                    meta: e.meta.clone(),
                    payload,
                };
                let resolved = resolver(&record)?;
                if resolved.stream_id.is_empty()
                    || resolved.stream_id.len() > 64
                    || resolved.result_key.is_empty()
                    || resolved.result_key.len() > 256
                    || resolved.payload.len() > MAX_RECORD_PAYLOAD
                {
                    return Err(QueueError::Corrupt);
                }
                let c = StoredConsumer {
                    id: e.meta.id,
                    outbound_id: e.meta.outbound_id.clone(),
                    revision: e.meta.revision,
                    delivery_id: resolved.delivery_id,
                    failures: e.meta.failures,
                    not_before_ms: e.meta.not_before_ms,
                    inflight: e.meta.inflight,
                };
                if !valid_consumer(&c) {
                    return Err(QueueError::Corrupt);
                }
                let key = (
                    resolved.stream_id.clone(),
                    resolved.result_key,
                    e.meta.client_id.clone(),
                );
                if let Some(result) = keys.get(&key).copied() {
                    let b = w.shared.bodies.get(&result).ok_or(QueueError::Corrupt)?;
                    let existing = storage::read_payload(
                        &w.dir,
                        b.segment,
                        b.offset,
                        b.meta.payload_len,
                        b.checksum,
                    )?;
                    if existing != resolved.payload
                        || b.consumers.iter().any(|id| {
                            w.shared
                                .consumers
                                .get(id)
                                .is_some_and(|(_, old)| old.outbound_id == c.outbound_id)
                        })
                    {
                        return Err(QueueError::Corrupt);
                    }
                    w.write(
                        &Transaction::Attach {
                            result_id: result,
                            consumers: vec![c.clone()],
                            next_id: next,
                        },
                        &[],
                    )?;
                    w.shared.attach(result, vec![c])?;
                } else {
                    let id = next;
                    next = next.checked_add(1).ok_or(QueueError::Full)?;
                    let meta = ResultMeta {
                        id,
                        stream_id: resolved.stream_id,
                        client_id: e.meta.client_id.clone(),
                        payload_len: resolved.payload.len(),
                    };
                    let r = StoredResult {
                        meta,
                        checksum: format::checksum(&resolved.payload),
                        consumers: vec![c],
                    };
                    w.results(vec![r], vec![resolved.payload], next)?;
                    keys.insert(key, id);
                }
            }
            // Migrated representations may be slightly larger; never publish an
            // over-quota state that cannot reopen or make terminal progress.
            let live = w
                .shared
                .bodies
                .values()
                .map(|b| crate::shared::body_cost(&b.meta))
                .sum::<u64>()
                + w.shared
                    .consumers
                    .values()
                    .map(|(_, c)| crate::shared::consumer_cost(c))
                    .sum::<u64>()
                + self
                    .bindings
                    .values()
                    .map(|b| {
                        (b.descriptor.len() as u64) * 4 + (b.outbound_id.len() as u64) * 6 + 256
                    })
                    .sum::<u64>()
                + 4096;
            if live > self.options.max_bytes / 2 {
                return Err(QueueError::Full);
            }
            self.publish_rewrite(w, next)
        })();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
}
