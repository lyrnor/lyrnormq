//! Preflighted existing frames sharing a durable synchronization barrier.
use crate::{
    Journal, QueueError, format,
    model::*,
    read_cache::AppendCheck,
    shared::{body_cost, consumer_cost, valid_consumer, valid_stream},
    storage,
};
use std::collections::BTreeSet;
struct Prepared<'a> {
    txn: Transaction,
    json: Vec<u8>,
    payloads: Vec<&'a [u8]>,
    outputs: Vec<ResultIds>,
    len: u64,
    raw: usize,
    added: u64,
    references: usize,
    next_id: u64,
}
impl Journal {
    /// Append at most four independently atomic result frames under one durable
    /// barrier. Success means every frame was synchronized; an error after IO may
    /// recover a complete-frame prefix, never a cross-frame atomic transaction.
    pub fn append_result_groups(
        &mut self,
        groups: &[Vec<NewResult>],
    ) -> Result<Vec<Vec<ResultIds>>, QueueError> {
        self.healthy()?;
        if groups.is_empty() || groups.len() > 4 {
            return Err(QueueError::InvalidOptions);
        }
        let mut frames = Vec::with_capacity(groups.len());
        let mut next = self.next_id;
        let mut raw = 0usize;
        let mut metadata = 0usize;
        let mut added = 0u64;
        let mut references = 0usize;
        let mut bytes = 0u64;
        for group in groups {
            let frame = self.prepare_results(group, next)?;
            next = frame.next_id;
            raw = raw.checked_add(frame.raw).ok_or(QueueError::Full)?;
            metadata = metadata
                .checked_add(frame.json.len())
                .ok_or(QueueError::Full)?;
            added = added.checked_add(frame.added).ok_or(QueueError::Full)?;
            references = references
                .checked_add(frame.references)
                .ok_or(QueueError::Full)?;
            bytes = bytes.checked_add(frame.len).ok_or(QueueError::Full)?;
            if raw > MAX_GROUP_BYTES
                || metadata > MAX_META
                || frame.len > self.options.max_bytes / 2
            {
                return Err(QueueError::Full);
            }
            frames.push(frame);
        }
        if self
            .entries
            .len()
            .saturating_add(self.shared.consumers.len())
            .saturating_add(references)
            > self.options.max_records
            || self.live_bytes.saturating_add(added) > self.options.max_bytes / 2
        {
            return Err(QueueError::Full);
        }
        if self.segment >= storage::MAX_SEGMENTS as u64 || !self.group_fits(&frames, bytes) {
            self.checkpoint()?;
        }
        if !self.group_fits(&frames, bytes) {
            return Err(QueueError::Full);
        }
        let result = self.write_group(frames);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn group_fits(&self, frames: &[Prepared<'_>], bytes: u64) -> bool {
        if self
            .physical
            .checked_add(bytes)
            .is_none_or(|n| n.saturating_add(20) > self.options.max_bytes)
        {
            return false;
        }
        let mut segment = self.segment;
        let mut len = self.active_len;
        for frame in frames {
            if len > 0 && len.saturating_add(frame.len) > self.options.segment_bytes {
                let Some(next) = segment.checked_add(1) else {
                    return false;
                };
                segment = next;
                len = 0;
            }
            if segment > storage::MAX_SEGMENTS as u64 {
                return false;
            }
            len = len.saturating_add(frame.len);
        }
        true
    }
    fn sync_group_segment(&mut self, check: Option<AppendCheck>) -> Result<(), QueueError> {
        self.active.sync_all()?;
        #[cfg(test)]
        {
            self.group_data_syncs += 1;
            self.inject(Fault::AfterSync)?;
        }
        self.reads
            .get_mut()
            .map_err(|_| QueueError::Io)?
            .after_append(&self.dir, check, self.active_len)
    }
    fn write_group(
        &mut self,
        frames: Vec<Prepared<'_>>,
    ) -> Result<Vec<Vec<ResultIds>>, QueueError> {
        let mut locations = Vec::with_capacity(frames.len());
        let mut dirty = false;
        let mut check = None;
        for (index, frame) in frames.iter().enumerate() {
            if self.active_len > 0
                && self.active_len.saturating_add(frame.len) > self.options.segment_bytes
            {
                if dirty {
                    self.sync_group_segment(check.take())?;
                    dirty = false;
                }
                self.segment = self.segment.checked_add(1).ok_or(QueueError::Io)?;
                self.active =
                    storage::private_file(&storage::segment(&self.dir, self.segment), true)?;
                self.active_len = 0;
                storage::sync_dir(&self.dir)?;
            }
            if !dirty {
                check = self
                    .reads
                    .get_mut()
                    .map_err(|_| QueueError::Io)?
                    .before_append(&self.dir, self.segment, self.active_len)?;
            }
            let (offset, written) =
                format::write_frame(&mut self.active, &frame.json, &frame.payloads)?;
            self.active_len += written;
            self.physical += written;
            dirty = true;
            locations.push((self.segment, offset));
            #[cfg(test)]
            {
                self.inject(Fault::AfterWrite)?;
                if index == 1 {
                    self.inject(Fault::AfterGroupSecondWrite)?;
                }
            }
            #[cfg(not(test))]
            let _ = index;
        }
        if dirty {
            self.sync_group_segment(check)?;
        }
        let mut outputs = Vec::with_capacity(frames.len());
        for (frame, (segment, offset)) in frames.into_iter().zip(locations) {
            self.apply(frame.txn, segment, offset, frame.raw as u64)?;
            outputs.push(frame.outputs);
        }
        Ok(outputs)
    }
    fn prepare_results<'a>(
        &self,
        results: &'a [NewResult],
        start: u64,
    ) -> Result<Prepared<'a>, QueueError> {
        if results.is_empty() || results.len() > MAX_GROUP {
            return Err(QueueError::InvalidOptions);
        }
        let mut next = start;
        let mut stored = Vec::with_capacity(results.len());
        let mut outputs = Vec::with_capacity(results.len());
        let mut raw = 0usize;
        let mut added = 0u64;
        let mut references = 0usize;
        for r in results {
            if !valid_stream(&r.stream_id)
                || r.client_id.len() > 65535
                || r.payload.len() > MAX_RECORD_PAYLOAD
                || r.consumers.is_empty()
                || r.consumers.len() > 32
            {
                return Err(QueueError::InvalidOptions);
            }
            raw = raw.checked_add(r.payload.len()).ok_or(QueueError::Full)?;
            if raw > MAX_GROUP_BYTES {
                return Err(QueueError::Full);
            }
            let id = next;
            next = next.checked_add(1).ok_or(QueueError::Full)?;
            let meta = ResultMeta {
                id,
                stream_id: r.stream_id.clone(),
                client_id: r.client_id.clone(),
                payload_len: r.payload.len(),
            };
            let mut consumers = Vec::with_capacity(r.consumers.len());
            let mut destinations = BTreeSet::new();
            for c in &r.consumers {
                let c = StoredConsumer {
                    id: next,
                    outbound_id: c.outbound_id.clone(),
                    revision: c.revision,
                    delivery_id: c.delivery_id.clone(),
                    failures: 0,
                    not_before_ms: 0,
                    inflight: false,
                };
                next = next.checked_add(1).ok_or(QueueError::Full)?;
                if !valid_consumer(&c) || !destinations.insert(c.outbound_id.clone()) {
                    return Err(QueueError::InvalidOptions);
                }
                if self
                    .bindings
                    .get(&c.outbound_id)
                    .is_none_or(|b| b.revision != c.revision)
                {
                    return Err(QueueError::StaleBinding);
                }
                added = added
                    .checked_add(consumer_cost(&c))
                    .ok_or(QueueError::Full)?;
                consumers.push(c);
            }
            references = references
                .checked_add(consumers.len())
                .ok_or(QueueError::Full)?;
            added = added
                .checked_add(body_cost(&meta))
                .ok_or(QueueError::Full)?;
            outputs.push(ResultIds {
                result_id: id,
                consumer_ids: consumers.iter().map(|c| c.id).collect(),
            });
            stored.push(StoredResult {
                meta,
                checksum: format::checksum(&r.payload),
                consumers,
            });
        }
        let txn = Transaction::Results {
            results: stored,
            next_id: next,
        };
        let json = format::json(&txn)?;
        let payloads: Vec<_> = results.iter().map(|r| r.payload.as_slice()).collect();
        let len = format::encoded_len(&json, &payloads)?;
        Ok(Prepared {
            txn,
            json,
            payloads,
            outputs,
            len,
            raw,
            added,
            references,
            next_id: next,
        })
    }
}
