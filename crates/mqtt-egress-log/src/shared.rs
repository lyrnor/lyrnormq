//! Shared immutable bodies and independently acknowledged consumer indexes.
use crate::{Journal, QueueError, format, model::*, storage};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    ops::Bound::{Excluded, Unbounded},
    sync::Arc,
};

type Lane = (Arc<str>, Arc<str>, Arc<str>);
#[derive(Clone)]
pub(crate) struct Body {
    pub meta: ResultMeta,
    pub checksum: u32,
    pub segment: u64,
    pub offset: u64,
    pub consumers: BTreeSet<u64>,
    pub stream: Arc<str>,
    pub client: Arc<str>,
}
#[derive(Clone, Default)]
pub(crate) struct Shared {
    pub bodies: BTreeMap<u64, Body>,
    pub consumers: BTreeMap<u64, (u64, StoredConsumer)>,
    lanes: BTreeMap<Lane, VecDeque<u64>>,
    heads: BTreeMap<String, BTreeSet<u64>>,
}
impl Shared {
    pub fn meta(&self, id: u64) -> Result<ConsumerMeta, QueueError> {
        let (result, c) = self.consumers.get(&id).ok_or(QueueError::InvalidOptions)?;
        let b = self.bodies.get(result).ok_or(QueueError::Corrupt)?;
        Ok(ConsumerMeta {
            id: c.id,
            result_id: *result,
            outbound_id: c.outbound_id.clone(),
            revision: c.revision,
            stream_id: b.meta.stream_id.clone(),
            client_id: b.meta.client_id.clone(),
            delivery_id: c.delivery_id.clone(),
            failures: c.failures,
            not_before_ms: c.not_before_ms,
            inflight: c.inflight,
            payload_len: b.meta.payload_len,
        })
    }
    pub fn insert(
        &mut self,
        stored: StoredResult,
        segment: u64,
        offset: u64,
    ) -> Result<(), QueueError> {
        if self.bodies.contains_key(&stored.meta.id) || self.consumers.contains_key(&stored.meta.id)
        {
            return Err(QueueError::Corrupt);
        }
        let id = stored.meta.id;
        self.bodies.insert(
            id,
            Body {
                stream: Arc::from(stored.meta.stream_id.as_str()),
                client: Arc::from(stored.meta.client_id.as_str()),
                meta: stored.meta,
                checksum: stored.checksum,
                segment,
                offset,
                consumers: BTreeSet::new(),
            },
        );
        self.attach(id, stored.consumers)
    }
    pub fn attach(
        &mut self,
        result: u64,
        consumers: Vec<StoredConsumer>,
    ) -> Result<(), QueueError> {
        for c in &consumers {
            if self.consumers.contains_key(&c.id) || self.bodies.contains_key(&c.id) {
                return Err(QueueError::Corrupt);
            }
        }
        let b = self.bodies.get_mut(&result).ok_or(QueueError::Corrupt)?;
        if b.consumers.len().saturating_add(consumers.len()) > 32 {
            return Err(QueueError::Corrupt);
        }
        let mut destinations: BTreeSet<_> = b
            .consumers
            .iter()
            .filter_map(|id| self.consumers.get(id).map(|(_, c)| c.outbound_id.clone()))
            .collect();
        for c in consumers {
            if c.id == 0
                || self.consumers.contains_key(&c.id)
                || !destinations.insert(c.outbound_id.clone())
            {
                return Err(QueueError::Corrupt);
            }
            let key = (
                Arc::from(c.outbound_id.as_str()),
                Arc::clone(&b.stream),
                Arc::clone(&b.client),
            );
            let lane = self.lanes.entry(key).or_default();
            let old = lane.front().copied();
            if lane.back().is_none_or(|last| *last < c.id) {
                lane.push_back(c.id)
            } else {
                let position = lane
                    .binary_search(&c.id)
                    .unwrap_or_else(|position| position);
                lane.insert(position, c.id);
            }
            if old != lane.front().copied() {
                let heads = self.heads.entry(c.outbound_id.clone()).or_default();
                if let Some(old) = old {
                    heads.remove(&old);
                }
                heads.insert(c.id);
            }
            b.consumers.insert(c.id);
            self.consumers.insert(c.id, (result, c));
        }
        Ok(())
    }
    pub fn remove(&mut self, id: u64) -> Result<(StoredConsumer, Option<ResultMeta>), QueueError> {
        let (result, c) = self.consumers.remove(&id).ok_or(QueueError::Corrupt)?;
        let b = self.bodies.get_mut(&result).ok_or(QueueError::Corrupt)?;
        let key = (
            Arc::from(c.outbound_id.as_str()),
            Arc::clone(&b.stream),
            Arc::clone(&b.client),
        );
        let lane = self.lanes.get_mut(&key).ok_or(QueueError::Corrupt)?;
        let was_head = lane.front() == Some(&id);
        if was_head {
            lane.pop_front();
        } else {
            lane.retain(|n| *n != id);
        }
        if was_head {
            let heads = self
                .heads
                .get_mut(&c.outbound_id)
                .ok_or(QueueError::Corrupt)?;
            heads.remove(&id);
            if let Some(next) = lane.front() {
                heads.insert(*next);
            }
            if heads.is_empty() {
                self.heads.remove(&c.outbound_id);
            }
        }
        if lane.is_empty() {
            self.lanes.remove(&key);
        }
        b.consumers.remove(&id);
        let removed = if b.consumers.is_empty() {
            self.bodies.remove(&result).map(|b| b.meta)
        } else {
            None
        };
        Ok((c, removed))
    }
    pub fn ids_valid(&self, ids: &[u64]) -> Result<(), QueueError> {
        if ids.is_empty() || ids.len() > MAX_GROUP {
            return Err(QueueError::InvalidOptions);
        }
        let mut seen = BTreeSet::new();
        for id in ids {
            if !seen.insert(*id) || !self.consumers.contains_key(id) {
                return Err(QueueError::InvalidOptions);
            }
        }
        Ok(())
    }
}
pub(crate) fn body_cost(meta: &ResultMeta) -> u64 {
    (meta.payload_len as u64)
        .saturating_add((meta.client_id.len() as u64) * 6)
        .saturating_add((meta.stream_id.len() as u64) * 6)
        .saturating_add(1024)
}
pub(crate) fn consumer_cost(c: &StoredConsumer) -> u64 {
    ((c.outbound_id.len() + c.delivery_id.len()) as u64)
        .saturating_mul(6)
        .saturating_add(512)
}
fn valid_stream(s: &str) -> bool {
    !s.is_empty() && s.len() <= 64
}
pub(crate) fn valid_consumer(c: &StoredConsumer) -> bool {
    !c.outbound_id.is_empty()
        && c.outbound_id.len() <= 64
        && c.outbound_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        && !c.delivery_id.is_empty()
        && c.delivery_id.len() <= 256
        && c.id > 0
        && c.revision > 0
}
impl Journal {
    pub fn append_results(
        &mut self,
        results: Vec<NewResult>,
    ) -> Result<Vec<ResultIds>, QueueError> {
        self.healthy()?;
        if results.is_empty() || results.len() > MAX_GROUP {
            return Err(QueueError::InvalidOptions);
        }
        let mut next = self.next_id;
        let mut stored = Vec::with_capacity(results.len());
        let mut outputs = Vec::with_capacity(results.len());
        let mut raw = 0usize;
        let mut added = 0u64;
        let mut references = 0usize;
        for r in &results {
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
        let payloads: Vec<_> = results.iter().map(|r| r.payload.as_slice()).collect();
        self.commit(
            Transaction::Results {
                results: stored,
                next_id: next,
            },
            &payloads,
        )?;
        Ok(outputs)
    }
    pub fn consumer_heads(
        &self,
        outbound: &str,
        after: Option<u64>,
        limit: usize,
    ) -> Vec<ConsumerMeta> {
        self.shared
            .heads
            .get(outbound)
            .into_iter()
            .flat_map(|ids| ids.range((after.map_or(Unbounded, Excluded), Unbounded)))
            .take(limit.min(MAX_GROUP))
            .filter_map(|id| self.shared.meta(*id).ok())
            .collect()
    }
    pub fn consumer_metas(
        &self,
        head: u64,
        limit: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<ConsumerMeta>, QueueError> {
        self.healthy()?;
        if limit == 0 || max_payload_bytes == 0 || max_payload_bytes > MAX_GROUP_BYTES {
            return Err(QueueError::InvalidOptions);
        }
        let first = self.shared.meta(head)?;
        let key = (
            Arc::from(first.outbound_id.as_str()),
            Arc::from(first.stream_id.as_str()),
            Arc::from(first.client_id.as_str()),
        );
        let lane = self.shared.lanes.get(&key).ok_or(QueueError::Corrupt)?;
        if lane.front() != Some(&head) {
            return Err(QueueError::InvalidOptions);
        }
        let mut metas = Vec::new();
        let mut size = 0usize;
        for id in lane.iter().take(limit.min(MAX_GROUP)) {
            let meta = self.shared.meta(*id)?;
            if meta.failures != first.failures
                || meta.not_before_ms != first.not_before_ms
                || meta.inflight != first.inflight
            {
                break;
            }
            let next = size
                .checked_add(meta.payload_len)
                .ok_or(QueueError::Corrupt)?;
            if next > max_payload_bytes {
                break;
            }
            size = next;
            metas.push(meta);
        }
        Ok(metas)
    }
    pub fn read_consumers(
        &self,
        head: u64,
        limit: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<ConsumerRecord>, QueueError> {
        self.consumer_metas(head, limit, max_payload_bytes)?
            .into_iter()
            .map(|meta| {
                let b = self
                    .shared
                    .bodies
                    .get(&meta.result_id)
                    .ok_or(QueueError::Corrupt)?;
                let payload = storage::read_payload(
                    &self.dir,
                    b.segment,
                    b.offset,
                    b.meta.payload_len,
                    b.checksum,
                )?;
                Ok(ConsumerRecord { meta, payload })
            })
            .collect()
    }
    pub fn begin_consumers(&mut self, ids: &[u64]) -> Result<(), QueueError> {
        self.healthy()?;
        self.shared.ids_valid(ids)?;
        self.commit(Transaction::ConsumerBegin { ids: ids.to_vec() }, &[])
    }
    pub fn retry_consumers(&mut self, ids: &[u64], not_before_ms: u64) -> Result<(), QueueError> {
        self.healthy()?;
        self.shared.ids_valid(ids)?;
        if ids.iter().any(|id| {
            self.shared
                .consumers
                .get(id)
                .is_none_or(|(_, c)| c.failures == u32::MAX)
        }) {
            return Err(QueueError::InvalidOptions);
        }
        self.commit(
            Transaction::ConsumerRetry {
                ids: ids.to_vec(),
                not_before_ms,
            },
            &[],
        )
    }
    pub fn complete_consumers(&mut self, ids: &[u64], _: Completion) -> Result<(), QueueError> {
        self.healthy()?;
        self.shared.ids_valid(ids)?;
        self.commit(Transaction::ConsumerComplete { ids: ids.to_vec() }, &[])
    }
    pub(crate) fn apply_results(
        &mut self,
        results: Vec<StoredResult>,
        next_id: u64,
        segment: u64,
        offset: u64,
        raw_len: u64,
    ) -> Result<(), QueueError> {
        if results.is_empty() || results.len() > MAX_GROUP || next_id < self.next_id {
            return Err(QueueError::Corrupt);
        }
        let refs = results.iter().map(|r| r.consumers.len()).sum::<usize>();
        if self
            .entries
            .len()
            .saturating_add(self.shared.consumers.len())
            .saturating_add(refs)
            > self.options.max_records
        {
            return Err(QueueError::Corrupt);
        }
        let mut raw = 0u64;
        let mut location = offset;
        for r in results {
            self.validate_shared_result(&r, next_id)?;
            raw = raw
                .checked_add(r.meta.payload_len as u64)
                .ok_or(QueueError::Corrupt)?;
            self.pending_bytes = self
                .pending_bytes
                .checked_add(r.meta.payload_len as u64)
                .ok_or(QueueError::Corrupt)?;
            self.live_bytes = self
                .live_bytes
                .checked_add(body_cost(&r.meta))
                .ok_or(QueueError::Corrupt)?;
            for c in &r.consumers {
                self.live_bytes = self
                    .live_bytes
                    .checked_add(consumer_cost(c))
                    .ok_or(QueueError::Corrupt)?;
                *self.counts.entry(c.outbound_id.clone()).or_default() += 1;
            }
            let n = r.meta.payload_len as u64;
            self.shared.insert(r, segment, location)?;
            location = location.checked_add(n).ok_or(QueueError::Corrupt)?;
        }
        if raw != raw_len || raw > MAX_GROUP_BYTES as u64 {
            return Err(QueueError::Corrupt);
        }
        self.next_id = next_id;
        Ok(())
    }
    fn validate_shared_result(&self, r: &StoredResult, next: u64) -> Result<(), QueueError> {
        if r.meta.id == 0
            || r.meta.id >= next
            || self.entries.contains_key(&r.meta.id)
            || self.shared.bodies.contains_key(&r.meta.id)
            || self.shared.consumers.contains_key(&r.meta.id)
            || !valid_stream(&r.meta.stream_id)
            || r.meta.client_id.len() > 65535
            || r.meta.payload_len > MAX_RECORD_PAYLOAD
            || r.consumers.is_empty()
            || r.consumers.len() > 32
        {
            return Err(QueueError::Corrupt);
        }
        let mut seen = BTreeSet::new();
        for c in &r.consumers {
            if !valid_consumer(c)
                || c.id >= next
                || self.entries.contains_key(&c.id)
                || !seen.insert(c.id)
                || self
                    .bindings
                    .get(&c.outbound_id)
                    .is_none_or(|b| b.revision != c.revision)
            {
                return Err(QueueError::Corrupt);
            }
        }
        Ok(())
    }
    pub(crate) fn apply_attach(
        &mut self,
        result: u64,
        consumers: Vec<StoredConsumer>,
        next: u64,
    ) -> Result<(), QueueError> {
        if next < self.next_id
            || consumers.is_empty()
            || self
                .entries
                .len()
                .saturating_add(self.shared.consumers.len())
                .saturating_add(consumers.len())
                > self.options.max_records
        {
            return Err(QueueError::Corrupt);
        }
        for c in &consumers {
            if !valid_consumer(c)
                || c.id >= next
                || self.entries.contains_key(&c.id)
                || self
                    .bindings
                    .get(&c.outbound_id)
                    .is_none_or(|b| b.revision != c.revision)
            {
                return Err(QueueError::Corrupt);
            }
            self.live_bytes = self
                .live_bytes
                .checked_add(consumer_cost(c))
                .ok_or(QueueError::Corrupt)?;
            *self.counts.entry(c.outbound_id.clone()).or_default() += 1;
        }
        self.shared.attach(result, consumers)?;
        self.next_id = next;
        Ok(())
    }
    pub(crate) fn remove_consumer(&mut self, id: u64) -> Result<(), QueueError> {
        let (c, removed) = self.shared.remove(id)?;
        let count = self
            .counts
            .get_mut(&c.outbound_id)
            .ok_or(QueueError::Corrupt)?;
        *count = count.checked_sub(1).ok_or(QueueError::Corrupt)?;
        if *count == 0 {
            self.counts.remove(&c.outbound_id);
        }
        self.live_bytes = self
            .live_bytes
            .checked_sub(consumer_cost(&c))
            .ok_or(QueueError::Corrupt)?;
        if let Some(meta) = removed {
            self.pending_bytes = self
                .pending_bytes
                .checked_sub(meta.payload_len as u64)
                .ok_or(QueueError::Corrupt)?;
            self.live_bytes = self
                .live_bytes
                .checked_sub(body_cost(&meta))
                .ok_or(QueueError::Corrupt)?;
        }
        Ok(())
    }
}
