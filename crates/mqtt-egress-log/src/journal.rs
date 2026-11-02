use crate::{
    QueueError,
    format::{self, Scan},
    model::*,
    storage,
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::{self, File},
    ops::Bound::{Excluded, Unbounded},
    path::PathBuf,
};

/// Exclusive synchronous owner. Methods must run on a blocking thread.
/// A failed write poisons this owner; reopen to discover the committed prefix.
pub struct Journal {
    options: QueueOptions,
    root: PathBuf,
    generation: u64,
    dir: PathBuf,
    active: File,
    segment: u64,
    active_len: u64,
    physical: u64,
    _owner: File,
    bindings: BTreeMap<String, StoredBinding>,
    entries: BTreeMap<u64, Entry>,
    lanes: BTreeMap<(String, String), VecDeque<u64>>,
    heads: BTreeMap<String, BTreeSet<u64>>,
    counts: BTreeMap<String, usize>,
    next_id: u64,
    next_revision: u64,
    pending_bytes: u64,
    live_bytes: u64,
    recovered: usize,
    poisoned: bool,
    #[cfg(test)]
    pub(crate) fault: Option<Fault>,
}
impl std::fmt::Debug for Journal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Journal").finish_non_exhaustive()
    }
}
impl Journal {
    pub fn open(options: QueueOptions) -> Result<Self, QueueError> {
        options.validate()?;
        let root = storage::directory(&options.directory)?;
        let owner = storage::owner(&root)?;
        let current = storage::manifest(&root)?;
        let generation = current.unwrap_or(1);
        let dir = storage::generation(&root, generation);
        if current.is_none() {
            // No published manifest means only initial preparation can exist.
            // A missing manifest is not proof that an existing generation is
            // disposable. Never erase a previously accepted queue on damage.
            for entry in fs::read_dir(&root)? {
                if entry?.file_name() != "OWNERSHIP" {
                    return Err(QueueError::Corrupt);
                }
            }
            storage::directory(&dir)?;
            let mut file = storage::private_file(&storage::segment(&dir, 1), true)?;
            let txn = Transaction::Snapshot {
                bindings: Vec::new(),
                entries: Vec::new(),
                next_id: 1,
                next_revision: 1,
            };
            let json = format::json(&txn)?;
            format::write_frame(&mut file, &json, &[])?;
            file.sync_all()?;
            storage::sync_dir(&dir)?;
            storage::publish(&root, 1)?;
        } else {
            let m = fs::symlink_metadata(&dir)?;
            if !m.is_dir() || m.file_type().is_symlink() {
                return Err(QueueError::Io);
            }
        }
        let segments = storage::files(&dir, options.max_bytes)?;
        let last = segments
            .last()
            .map(|(id, _)| *id)
            .ok_or(QueueError::Corrupt)?;
        let physical = segments
            .iter()
            .try_fold(0u64, |n, (_, len)| n.checked_add(*len))
            .ok_or(QueueError::Corrupt)?;
        if physical.saturating_add(20) > options.max_bytes {
            return Err(QueueError::Full);
        }
        let active = storage::private_file(&storage::segment(&dir, last), false)?;
        let active_len = active.metadata()?.len();
        let mut journal = Self {
            options,
            root,
            generation,
            dir,
            active,
            segment: last,
            active_len,
            physical,
            _owner: owner,
            bindings: BTreeMap::new(),
            entries: BTreeMap::new(),
            lanes: BTreeMap::new(),
            heads: BTreeMap::new(),
            counts: BTreeMap::new(),
            next_id: 1,
            next_revision: 1,
            pending_bytes: 0,
            live_bytes: 0,
            recovered: 0,
            poisoned: false,
            #[cfg(test)]
            fault: None,
        };
        let mut saw = false;
        for (segment, _) in segments {
            let mut file = storage::private_file(&storage::segment(&journal.dir, segment), false)?;
            let mut offset = 0;
            loop {
                match format::scan(&mut file, offset, journal.options.max_bytes)? {
                    Scan::End => break,
                    Scan::Partial if segment == last => {
                        file.set_len(offset)?;
                        file.sync_all()?;
                        journal.physical =
                            journal.physical.saturating_sub(journal.active_len - offset);
                        journal.active_len = offset;
                        break;
                    }
                    Scan::Partial => return Err(QueueError::Corrupt),
                    Scan::Frame(frame) => {
                        if !saw && !matches!(frame.txn, Transaction::Snapshot { .. }) {
                            return Err(QueueError::Corrupt);
                        }
                        if saw && matches!(frame.txn, Transaction::Snapshot { .. }) {
                            return Err(QueueError::Corrupt);
                        }
                        journal.apply(frame.txn, segment, frame.raw_offset, frame.raw_len)?;
                        offset = frame.end;
                        saw = true;
                    }
                }
            }
        }
        if !saw {
            return Err(QueueError::Corrupt);
        }
        journal.recovered = journal.entries.len();
        journal.recompute_live()?;
        if journal.entries.len() > journal.options.max_records
            || journal.live_bytes > journal.options.max_bytes / 2
        {
            return Err(QueueError::Full);
        }
        storage::clean_other_generations(&journal.root, journal.generation)?;
        Ok(journal)
    }
    fn healthy(&self) -> Result<(), QueueError> {
        if self.poisoned {
            Err(QueueError::Io)
        } else {
            Ok(())
        }
    }
    pub fn bind_all(&mut self, bindings: &[Binding]) -> Result<BTreeMap<String, u64>, QueueError> {
        self.healthy()?;
        if bindings.len() > 32 {
            return Err(QueueError::InvalidOptions);
        }
        let mut next = BTreeMap::new();
        let mut revision = self.next_revision;
        for binding in bindings {
            if !valid_outbound(&binding.outbound_id)
                || binding.descriptor.len() > 65536
                || next.contains_key(&binding.outbound_id)
            {
                return Err(QueueError::InvalidOptions);
            }
            let old = self.bindings.get(&binding.outbound_id);
            let rev = match old {
                Some(old) if old.descriptor == binding.descriptor => old.revision,
                _ if self.pending_for(&binding.outbound_id) > 0 => {
                    return Err(QueueError::BindingConflict);
                }
                _ => {
                    let n = revision;
                    revision = revision.checked_add(1).ok_or(QueueError::Full)?;
                    n
                }
            };
            next.insert(
                binding.outbound_id.clone(),
                StoredBinding {
                    outbound_id: binding.outbound_id.clone(),
                    revision: rev,
                    descriptor: binding.descriptor.clone(),
                },
            );
        }
        if self
            .counts
            .iter()
            .any(|(id, count)| *count > 0 && !next.contains_key(id))
        {
            return Err(QueueError::BindingConflict);
        }
        let result = next
            .iter()
            .map(|(id, b)| (id.clone(), b.revision))
            .collect();
        let changed = next.len() != self.bindings.len()
            || next.iter().any(|(id, b)| {
                self.bindings
                    .get(id)
                    .is_none_or(|old| old.revision != b.revision)
            });
        if changed {
            let estimated = next
                .values()
                .try_fold(0u64, |n, b| n.checked_add(binding_cost(b)))
                .ok_or(QueueError::Full)?;
            let old = self.bindings.values().map(binding_cost).sum::<u64>();
            if self
                .live_bytes
                .saturating_sub(old)
                .saturating_add(estimated)
                > self.options.max_bytes / 2
            {
                return Err(QueueError::Full);
            }
            self.commit(
                Transaction::Bind {
                    bindings: next.into_values().collect(),
                    next_revision: revision,
                },
                &[],
            )?;
        }
        Ok(result)
    }
    pub fn append_batch(&mut self, records: Vec<NewRecord>) -> Result<Vec<u64>, QueueError> {
        self.healthy()?;
        if records.is_empty() || records.len() > MAX_GROUP {
            return Err(QueueError::InvalidOptions);
        }
        if self
            .entries
            .len()
            .checked_add(records.len())
            .is_none_or(|n| n > self.options.max_records || n > storage::MAX_SEGMENTS - 2)
        {
            return Err(QueueError::Full);
        }
        let mut raw = 0usize;
        let mut added = 0u64;
        let mut next = self.next_id;
        let mut metas = Vec::with_capacity(records.len());
        for record in &records {
            if !valid_outbound(&record.outbound_id)
                || record.client_id.len() > 65535
                || record.payload.len() > MAX_RECORD_PAYLOAD
            {
                return Err(QueueError::InvalidOptions);
            }
            if self
                .bindings
                .get(&record.outbound_id)
                .is_none_or(|b| b.revision != record.revision)
            {
                return Err(QueueError::StaleBinding);
            }
            raw = raw
                .checked_add(record.payload.len())
                .ok_or(QueueError::Full)?;
            if raw > MAX_GROUP_BYTES {
                return Err(QueueError::Full);
            }
            let meta = RecordMeta {
                id: next,
                outbound_id: record.outbound_id.clone(),
                revision: record.revision,
                client_id: record.client_id.clone(),
                failures: 0,
                not_before_ms: 0,
                inflight: false,
                payload_len: record.payload.len(),
            };
            next = next.checked_add(1).ok_or(QueueError::Full)?;
            added = added
                .checked_add(entry_cost(&meta))
                .ok_or(QueueError::Full)?;
            metas.push(StoredRecord {
                meta,
                checksum: format::checksum(&record.payload),
            });
        }
        if self
            .live_bytes
            .checked_add(added)
            .is_none_or(|n| n > self.options.max_bytes / 2)
        {
            return Err(QueueError::Full);
        }
        let ids = metas.iter().map(|m| m.meta.id).collect();
        let payloads: Vec<&[u8]> = records.iter().map(|r| r.payload.as_slice()).collect();
        self.commit(
            Transaction::Append {
                records: metas,
                next_id: next,
            },
            &payloads,
        )?;
        Ok(ids)
    }
    /// Returns indexed client heads; caller wraps its cursor for fair scheduling.
    pub fn heads(&self, outbound_id: &str, after: Option<u64>, limit: usize) -> Vec<RecordMeta> {
        self.heads
            .get(outbound_id)
            .into_iter()
            .flat_map(|ids| ids.range((after.map_or(Unbounded, Excluded), Unbounded)))
            .take(limit.min(MAX_GROUP))
            .filter_map(|id| self.entries.get(id).map(|e| e.meta.clone()))
            .collect()
    }
    pub fn read_batch(
        &mut self,
        head_id: u64,
        limit: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<Record>, QueueError> {
        self.healthy()?;
        let result = self.read_batch_inner(head_id, limit, max_payload_bytes);
        if matches!(result, Err(QueueError::Io | QueueError::Corrupt)) {
            self.poisoned = true;
        }
        result
    }
    /// Bounded metadata preview used to reserve the exact resident byte budget
    /// before reading opaque payloads. Selection is identical to `read_batch`.
    pub fn batch_metas(
        &self,
        head_id: u64,
        limit: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<RecordMeta>, QueueError> {
        self.healthy()?;
        if limit == 0 || max_payload_bytes == 0 || max_payload_bytes > MAX_GROUP_BYTES {
            return Err(QueueError::InvalidOptions);
        }
        let first = self
            .entries
            .get(&head_id)
            .ok_or(QueueError::InvalidOptions)?;
        let key = (first.meta.outbound_id.clone(), first.meta.client_id.clone());
        let lane = self.lanes.get(&key).ok_or(QueueError::Corrupt)?;
        if lane.front() != Some(&head_id) {
            return Err(QueueError::InvalidOptions);
        }
        let mut result = Vec::new();
        let mut size = 0usize;
        for id in lane.iter().take(limit.min(MAX_GROUP)) {
            let entry = self.entries.get(id).ok_or(QueueError::Corrupt)?;
            if entry.meta.failures != first.meta.failures
                || entry.meta.not_before_ms != first.meta.not_before_ms
                || entry.meta.inflight != first.meta.inflight
            {
                break;
            }
            let Some(next) = size.checked_add(entry.meta.payload_len) else {
                return Err(QueueError::Corrupt);
            };
            if next > max_payload_bytes {
                break;
            }
            result.push(entry.meta.clone());
            size = next;
        }
        Ok(result)
    }
    fn read_batch_inner(
        &self,
        head_id: u64,
        limit: usize,
        max_payload_bytes: usize,
    ) -> Result<Vec<Record>, QueueError> {
        let metadata = self.batch_metas(head_id, limit, max_payload_bytes)?;
        metadata
            .into_iter()
            .map(|meta| {
                let entry = self.entries.get(&meta.id).ok_or(QueueError::Corrupt)?;
                let payload = storage::read_payload(
                    &self.dir,
                    entry.segment,
                    entry.offset,
                    entry.meta.payload_len,
                    entry.checksum,
                )?;
                Ok(Record { meta, payload })
            })
            .collect()
    }
    fn validate_ids(&self, ids: &[u64]) -> Result<(), QueueError> {
        self.healthy()?;
        if ids.is_empty() || ids.len() > MAX_GROUP {
            return Err(QueueError::InvalidOptions);
        }
        let mut seen = BTreeSet::new();
        for id in ids {
            if !self.entries.contains_key(id) || !seen.insert(id) {
                return Err(QueueError::InvalidOptions);
            }
        }
        Ok(())
    }
    pub fn begin_attempt(&mut self, ids: &[u64]) -> Result<(), QueueError> {
        self.validate_ids(ids)?;
        self.commit(Transaction::Begin { ids: ids.to_vec() }, &[])
    }
    pub fn retry(&mut self, ids: &[u64], not_before_ms: u64) -> Result<(), QueueError> {
        self.validate_ids(ids)?;
        if ids.iter().any(|id| {
            self.entries
                .get(id)
                .is_none_or(|e| e.meta.failures == u32::MAX)
        }) {
            return Err(QueueError::InvalidOptions);
        }
        self.commit(
            Transaction::Retry {
                ids: ids.to_vec(),
                not_before_ms,
            },
            &[],
        )
    }
    pub fn complete(&mut self, ids: &[u64], _: Completion) -> Result<(), QueueError> {
        self.validate_ids(ids)?;
        self.commit(Transaction::Complete { ids: ids.to_vec() }, &[])
    }
    pub fn pending_for(&self, outbound_id: &str) -> usize {
        self.counts.get(outbound_id).copied().unwrap_or(0)
    }
    pub fn stats(&self) -> QueueStats {
        QueueStats {
            pending_records: self.entries.len(),
            pending_bytes: self.pending_bytes,
            disk_bytes: self.physical + 20,
            max_bytes: self.options.max_bytes,
            recovered_records: self.recovered,
        }
    }
    pub fn checkpoint(&mut self) -> Result<(), QueueError> {
        self.healthy()?;
        let result = self.compact();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn commit(&mut self, txn: Transaction, payloads: &[&[u8]]) -> Result<(), QueueError> {
        let json = format::json(&txn)?;
        let len = format::encoded_len(&json, payloads)?;
        if len > self.options.max_bytes / 2 {
            return Err(QueueError::Full);
        }
        if self.segment >= storage::MAX_SEGMENTS as u64
            || self
                .physical
                .checked_add(len)
                .is_none_or(|n| n.saturating_add(20) > self.options.max_bytes)
        {
            self.checkpoint()?;
        }
        if self
            .physical
            .checked_add(len)
            .is_none_or(|n| n.saturating_add(20) > self.options.max_bytes)
        {
            return Err(QueueError::Full);
        }
        let result = (|| {
            if self.active_len > 0
                && self.active_len.saturating_add(len) > self.options.segment_bytes
            {
                self.segment = self.segment.checked_add(1).ok_or(QueueError::Io)?;
                self.active =
                    storage::private_file(&storage::segment(&self.dir, self.segment), true)?;
                self.active_len = 0;
                storage::sync_dir(&self.dir)?;
            }
            let (offset, written) = format::write_frame(&mut self.active, &json, payloads)?;
            #[cfg(test)]
            self.inject(Fault::AfterWrite)?;
            self.active.sync_all()?;
            #[cfg(test)]
            self.inject(Fault::AfterSync)?;
            self.active_len += written;
            self.physical += written;
            self.apply(
                txn,
                self.segment,
                offset,
                payloads.iter().map(|p| p.len() as u64).sum(),
            )?;
            Ok(())
        })();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn apply(
        &mut self,
        txn: Transaction,
        segment: u64,
        offset: u64,
        raw_len: u64,
    ) -> Result<(), QueueError> {
        match txn {
            Transaction::Snapshot {
                bindings,
                entries,
                next_id,
                next_revision,
            } => {
                if !self.entries.is_empty()
                    || !self.bindings.is_empty()
                    || !entries.is_empty()
                    || next_id == 0
                    || next_revision == 0
                    || raw_len != 0
                {
                    return Err(QueueError::Corrupt);
                }
                self.live_bytes = 4096;
                self.install_bindings(bindings, next_revision)?;
                self.next_id = next_id;
            }
            Transaction::Bind {
                bindings,
                next_revision,
            } => {
                if raw_len != 0 {
                    return Err(QueueError::Corrupt);
                }
                self.install_bindings(bindings, next_revision)?;
            }
            Transaction::Append { records, next_id } => {
                if records.is_empty()
                    || records.len() > MAX_GROUP
                    || self.entries.len().saturating_add(records.len()) > self.options.max_records
                {
                    return Err(QueueError::Corrupt);
                }
                let mut location = offset;
                let mut previous = self.next_id.saturating_sub(1);
                let mut bytes = 0u64;
                for meta in records {
                    let StoredRecord { meta, checksum } = meta;
                    if meta.id <= previous
                        || meta.id >= next_id
                        || meta.payload_len > MAX_RECORD_PAYLOAD
                        || meta.client_id.len() > 65535
                        || !valid_outbound(&meta.outbound_id)
                        || self
                            .bindings
                            .get(&meta.outbound_id)
                            .is_none_or(|b| b.revision != meta.revision)
                    {
                        return Err(QueueError::Corrupt);
                    }
                    previous = meta.id;
                    bytes = bytes
                        .checked_add(meta.payload_len as u64)
                        .ok_or(QueueError::Corrupt)?;
                    let len = meta.payload_len as u64;
                    self.add_entry(Entry {
                        meta,
                        checksum,
                        segment,
                        offset: location,
                    })?;
                    location = location.checked_add(len).ok_or(QueueError::Corrupt)?;
                }
                if bytes != raw_len || bytes > MAX_GROUP_BYTES as u64 || next_id < self.next_id {
                    return Err(QueueError::Corrupt);
                }
                self.next_id = next_id;
            }
            Transaction::Begin { ids } => {
                if raw_len != 0 {
                    return Err(QueueError::Corrupt);
                }
                self.validate_ids(&ids).map_err(|_| QueueError::Corrupt)?;
                for id in ids {
                    self.entries
                        .get_mut(&id)
                        .ok_or(QueueError::Corrupt)?
                        .meta
                        .inflight = true;
                }
            }
            Transaction::Retry { ids, not_before_ms } => {
                if raw_len != 0 {
                    return Err(QueueError::Corrupt);
                }
                self.validate_ids(&ids).map_err(|_| QueueError::Corrupt)?;
                for id in ids {
                    let meta = &mut self.entries.get_mut(&id).ok_or(QueueError::Corrupt)?.meta;
                    meta.failures = meta.failures.checked_add(1).ok_or(QueueError::Corrupt)?;
                    meta.inflight = false;
                    meta.not_before_ms = not_before_ms;
                }
            }
            Transaction::Complete { ids } => {
                if raw_len != 0 {
                    return Err(QueueError::Corrupt);
                }
                self.validate_ids(&ids).map_err(|_| QueueError::Corrupt)?;
                for id in ids {
                    self.remove_entry(id)?;
                }
            }
            Transaction::Watermark {
                next_id,
                next_revision,
            } => {
                if raw_len != 0 || next_id < self.next_id || next_revision < self.next_revision {
                    return Err(QueueError::Corrupt);
                }
                self.next_id = next_id;
                self.next_revision = next_revision;
            }
        }
        Ok(())
    }
    fn install_bindings(
        &mut self,
        bindings: Vec<StoredBinding>,
        next_revision: u64,
    ) -> Result<(), QueueError> {
        if bindings.len() > 32 || next_revision == 0 || next_revision < self.next_revision {
            return Err(QueueError::Corrupt);
        }
        let mut next = BTreeMap::new();
        let mut revs = BTreeSet::new();
        for binding in bindings {
            if !valid_outbound(&binding.outbound_id)
                || binding.descriptor.len() > 65536
                || binding.revision == 0
                || binding.revision >= next_revision
                || !revs.insert(binding.revision)
                || next.contains_key(&binding.outbound_id)
            {
                return Err(QueueError::Corrupt);
            }
            if self.pending_for(&binding.outbound_id) > 0
                && self.bindings.get(&binding.outbound_id).is_none_or(|old| {
                    old.revision != binding.revision || old.descriptor != binding.descriptor
                })
            {
                return Err(QueueError::Corrupt);
            }
            next.insert(binding.outbound_id.clone(), binding);
        }
        if self
            .counts
            .iter()
            .any(|(id, count)| *count > 0 && !next.contains_key(id))
        {
            return Err(QueueError::Corrupt);
        }
        let old = self.bindings.values().map(binding_cost).sum::<u64>();
        let new = next.values().map(binding_cost).sum::<u64>();
        self.live_bytes = self
            .live_bytes
            .checked_sub(old)
            .and_then(|n| n.checked_add(new))
            .ok_or(QueueError::Corrupt)?;
        self.bindings = next;
        self.next_revision = next_revision;
        Ok(())
    }
    fn add_entry(&mut self, entry: Entry) -> Result<(), QueueError> {
        let id = entry.meta.id;
        if self.entries.contains_key(&id) {
            return Err(QueueError::Corrupt);
        }
        let key = (entry.meta.outbound_id.clone(), entry.meta.client_id.clone());
        let lane = self.lanes.entry(key).or_default();
        if lane.is_empty() {
            self.heads
                .entry(entry.meta.outbound_id.clone())
                .or_default()
                .insert(id);
        }
        lane.push_back(id);
        *self
            .counts
            .entry(entry.meta.outbound_id.clone())
            .or_default() += 1;
        self.pending_bytes = self
            .pending_bytes
            .checked_add(entry.meta.payload_len as u64)
            .ok_or(QueueError::Corrupt)?;
        self.live_bytes = self
            .live_bytes
            .checked_add(entry_cost(&entry.meta))
            .ok_or(QueueError::Corrupt)?;
        self.entries.insert(id, entry);
        Ok(())
    }
    fn remove_entry(&mut self, id: u64) -> Result<(), QueueError> {
        let entry = self.entries.remove(&id).ok_or(QueueError::Corrupt)?;
        let key = (entry.meta.outbound_id.clone(), entry.meta.client_id.clone());
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
                .get_mut(&entry.meta.outbound_id)
                .ok_or(QueueError::Corrupt)?;
            heads.remove(&id);
            if let Some(next) = lane.front() {
                heads.insert(*next);
            }
        }
        if lane.is_empty() {
            self.lanes.remove(&key);
        }
        let count = self
            .counts
            .get_mut(&entry.meta.outbound_id)
            .ok_or(QueueError::Corrupt)?;
        *count = count.checked_sub(1).ok_or(QueueError::Corrupt)?;
        self.pending_bytes = self
            .pending_bytes
            .checked_sub(entry.meta.payload_len as u64)
            .ok_or(QueueError::Corrupt)?;
        self.live_bytes = self
            .live_bytes
            .checked_sub(entry_cost(&entry.meta))
            .ok_or(QueueError::Corrupt)?;
        Ok(())
    }
    fn recompute_live(&mut self) -> Result<(), QueueError> {
        let bindings = self
            .bindings
            .values()
            .try_fold(4096u64, |n, b| n.checked_add(binding_cost(b)))
            .ok_or(QueueError::Corrupt)?;
        self.live_bytes = self
            .entries
            .values()
            .try_fold(bindings, |n, e| n.checked_add(entry_cost(&e.meta)))
            .ok_or(QueueError::Corrupt)?;
        Ok(())
    }
    #[cfg(test)]
    fn inject(&mut self, fault: Fault) -> Result<(), QueueError> {
        if self.fault == Some(fault) {
            self.fault = None;
            return Err(QueueError::Io);
        }
        Ok(())
    }
    fn compact(&mut self) -> Result<(), QueueError> {
        let generation = self.generation.checked_add(1).ok_or(QueueError::Io)?;
        let dir = storage::generation(&self.root, generation);
        storage::directory(&dir)?;
        let mut segment = 1u64;
        let mut file = storage::private_file(&storage::segment(&dir, segment), true)?;
        let mut active_len = 0u64;
        let mut physical = 0u64;
        let initial = Transaction::Snapshot {
            bindings: self.bindings.values().cloned().collect(),
            entries: Vec::new(),
            next_id: 1,
            next_revision: self.next_revision,
        };
        let json = format::json(&initial)?;
        let (_, len) = format::write_frame(&mut file, &json, &[])?;
        active_len += len;
        physical += len;
        let mut rewritten = BTreeMap::new();
        let mut cursor = None;
        loop {
            let mut metas = Vec::new();
            let mut payloads = Vec::new();
            let mut bytes = 0usize;
            for (id, entry) in self
                .entries
                .range((cursor.map_or(Unbounded, Excluded), Unbounded))
                .take(MAX_GROUP)
            {
                if !metas.is_empty()
                    && bytes.saturating_add(entry.meta.payload_len) > MAX_GROUP_BYTES
                {
                    break;
                }
                let payload = storage::read_payload(
                    &self.dir,
                    entry.segment,
                    entry.offset,
                    entry.meta.payload_len,
                    entry.checksum,
                )?;
                bytes += payload.len();
                metas.push((
                    *id,
                    StoredRecord {
                        meta: entry.meta.clone(),
                        checksum: entry.checksum,
                    },
                ));
                payloads.push(payload);
            }
            if metas.is_empty() {
                break;
            }
            let last = metas.last().map(|(id, _)| *id).ok_or(QueueError::Io)?;
            let txn = Transaction::Append {
                records: metas.iter().map(|(_, m)| m.clone()).collect(),
                next_id: last.checked_add(1).ok_or(QueueError::Io)?,
            };
            let json = format::json(&txn)?;
            let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
            let len = format::encoded_len(&json, &refs)?;
            if active_len > 0 && active_len.saturating_add(len) > self.options.segment_bytes {
                file.sync_all()?;
                segment = segment.checked_add(1).ok_or(QueueError::Io)?;
                if segment > storage::MAX_SEGMENTS as u64 {
                    return Err(QueueError::Full);
                }
                file = storage::private_file(&storage::segment(&dir, segment), true)?;
                active_len = 0;
            }
            if physical
                .checked_add(len)
                .is_none_or(|n| n.saturating_add(20) > self.options.max_bytes)
            {
                return Err(QueueError::Full);
            }
            let (mut location, written) = format::write_frame(&mut file, &json, &refs)?;
            active_len += written;
            physical += written;
            for (_, stored) in metas {
                let StoredRecord { meta, checksum } = stored;
                let n = meta.payload_len as u64;
                rewritten.insert(
                    meta.id,
                    Entry {
                        meta,
                        checksum,
                        segment,
                        offset: location,
                    },
                );
                location += n;
            }
            cursor = Some(last);
        }
        let json = format::json(&Transaction::Watermark {
            next_id: self.next_id,
            next_revision: self.next_revision,
        })?;
        let (_, len) = format::write_frame(&mut file, &json, &[])?;
        active_len += len;
        physical += len;
        if physical.saturating_add(20) > self.options.max_bytes {
            return Err(QueueError::Full);
        }
        file.sync_all()?;
        storage::sync_dir(&dir)?;
        #[cfg(test)]
        self.inject(Fault::BeforePublish)?;
        storage::publish(&self.root, generation)?;
        #[cfg(test)]
        self.inject(Fault::AfterPublish)?;
        let old_dir = std::mem::replace(&mut self.dir, dir);
        let old_file = std::mem::replace(&mut self.active, file);
        drop(old_file);
        self.generation = generation;
        self.segment = segment;
        self.active_len = active_len;
        self.physical = physical;
        self.entries = rewritten;
        storage::remove_generation(&old_dir)?;
        storage::sync_dir(&self.root)?;
        Ok(())
    }
}
fn valid_outbound(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn binding_cost(binding: &StoredBinding) -> u64 {
    (binding.descriptor.len() as u64)
        .saturating_mul(4)
        .saturating_add((binding.outbound_id.len() as u64) * 6)
        .saturating_add(256)
}
fn entry_cost(meta: &RecordMeta) -> u64 {
    (meta.payload_len as u64)
        .saturating_add((meta.client_id.len() as u64).saturating_mul(6))
        .saturating_add((meta.outbound_id.len() as u64) * 6)
        .saturating_add(1024)
}
