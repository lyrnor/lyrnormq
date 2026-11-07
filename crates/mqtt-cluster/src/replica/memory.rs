//! Typed RAM register. Payloads, receipts, fences and repair staging never reach disk.
use super::{
    model::*,
    store::{ReplicaStore, StoreFuture},
};
use crate::diagnosis::{self, Failure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::File,
    io,
    path::Path,
    sync::{Arc, Mutex, PoisonError},
};

pub(crate) const MAX_RECORD: usize = 8 * 1024 * 1024;
const MAX_BYTES: usize = 256 * 1024 * 1024;
const MAX_REPAIR_BYTES: u64 = 768 * 1024 * 1024;
const MAX_PAGE_BYTES: usize = 12 * 1024 * 1024;
// Physical record count includes bounded internal host/fence records.
const MAX_ENTRIES: usize = 101_024;

fn rejected() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "memory replica unavailable or quota exceeded",
    )
}
fn error(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

#[derive(Clone)]
pub(in crate::replica) struct Value {
    record: Arc<Record>,
    canonical: Arc<[u8]>,
}
impl Value {
    pub(in crate::replica) fn new(
        record: Record,
        canonical: Option<Arc<[u8]>>,
    ) -> io::Result<Self> {
        if record.epoch == 0
            || record.epoch > i64::MAX as u64
            || record
                .snapshot
                .as_ref()
                .is_some_and(|s| s.len() > 2 * 1024 * 1024)
            || record.intents.len() > 512
            || record.receipts.len() > 8192
            || record.filters.len() > 256
        {
            return Err(rejected());
        }
        let canonical = match canonical {
            Some(bytes) => bytes,
            None => serde_json::to_vec(&record).map_err(error)?.into(),
        };
        if canonical.len() > MAX_RECORD {
            return Err(rejected());
        }
        Ok(Self {
            record: Arc::new(record),
            canonical,
        })
    }
}
#[derive(Clone)]
pub(in crate::replica) struct Entry {
    pub(in crate::replica) min_epoch: u64,
    pub(in crate::replica) value: Option<Value>,
}
impl Entry {
    fn charge(&self, key: &str) -> usize {
        key.len() + self.value.as_ref().map_or(0, |v| v.canonical.len())
    }
    fn exported(&self, key: String) -> RepairEntry {
        RepairEntry {
            key,
            min_epoch: self.min_epoch,
            record: self.value.as_ref().map(|v| v.record.as_ref().clone()),
        }
    }
}
#[derive(Clone, Default)]
pub(in crate::replica) struct RepairState {
    generation: u64,
    frozen: Option<RepairToken>,
    last_token: Option<RepairToken>,
    manifest: Option<RepairManifest>,
    next_page: u64,
    last_key: Option<String>,
    stream_bytes: u64,
    records: u64,
    importing: bool,
    installed: bool,
}
#[derive(Clone, Default)]
pub(in crate::replica) struct State {
    pub(in crate::replica) records: BTreeMap<String, Entry>,
    pub(in crate::replica) bytes: usize,
    pub(in crate::replica) staging: BTreeMap<String, Entry>,
    pub(in crate::replica) staging_bytes: usize,
    pub(in crate::replica) pages: BTreeMap<u64, String>,
    pub(in crate::replica) repair: RepairState,
}
impl State {
    pub(in crate::replica) fn readiness(&self) -> (u64, bool) {
        (self.repair.generation, self.repair.frozen.is_some())
    }
}

pub(in crate::replica) enum PersistOp {
    Record { key: String, value: Option<Vec<u8>> },
    Repair(RepairDelta),
}
pub(in crate::replica) struct RepairDelta {
    pub repair_json: Vec<u8>,
    pub records: Vec<(String, Option<Vec<u8>>)>,
    pub staging: Vec<(String, Option<Vec<u8>>)>,
}
type Persist = Arc<dyn Fn(&PersistOp) -> io::Result<()> + Send + Sync>;

pub(super) struct MemoryStore {
    state: Mutex<State>,
    maintenance: Arc<tokio::sync::Semaphore>,
    _lock: File,
    disk_id: String,
    persist: Option<Persist>,
}
impl MemoryStore {
    pub(super) fn open(
        path: &Path,
        identity: (&str, u64),
        members: &[(u64, String)],
    ) -> io::Result<Self> {
        #[cfg(test)]
        let _process_start = crate::replica::TEST_PROCESS_START_LOCK
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let lock = super::store::claim_directory(
            path,
            identity,
            members,
            "memory.identity",
            &[
                (
                    "data.sqlite",
                    "legacy data.sqlite cannot be opened as memory replicas; use a new cluster and separate directories",
                ),
                (
                    "rocks.identity",
                    "rocksdb replica directory cannot be opened as memory replicas; use a new directory",
                ),
            ],
            "memory replica unavailable or quota exceeded",
        )?;
        let mut random = [0u8; 16];
        getrandom::getrandom(&mut random).map_err(error)?;
        let disk_id = random.iter().map(|b| format!("{b:02x}")).collect();
        Ok(Self::from_loaded(lock, disk_id, State::default(), None))
    }
    pub(in crate::replica) fn from_loaded(
        lock: File,
        disk_id: String,
        state: State,
        persist: Option<Persist>,
    ) -> Self {
        Self {
            state: Mutex::new(state),
            maintenance: Arc::new(tokio::sync::Semaphore::new(2)),
            _lock: lock,
            disk_id,
            persist,
        }
    }
    pub(in crate::replica) fn ready_bits(&self) -> (u64, bool) {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        (state.repair.generation, state.repair.frozen.is_some())
    }
    fn persist_record(&self, key: &str, entry: Option<&Entry>) -> io::Result<()> {
        let Some(persist) = &self.persist else {
            return Ok(());
        };
        let value = entry.map(encode_entry).transpose()?;
        persist(&PersistOp::Record {
            key: key.to_owned(),
            value,
        })
    }
    fn persist_repair(&self, before: &State, after: &State) -> io::Result<()> {
        let Some(persist) = &self.persist else {
            return Ok(());
        };
        persist(&PersistOp::Repair(RepairDelta {
            repair_json: encode_repair(after)?,
            records: diff_entries(&before.records, &after.records)?,
            staging: diff_entries(&before.staging, &after.staging)?,
        }))
    }
    fn remember(&self, state: &State) -> Option<State> {
        self.persist.is_some().then(|| state.clone())
    }
    fn publish_repair(&self, state: &mut State, before: Option<State>) -> io::Result<()> {
        let Some(before) = before else {
            return Ok(());
        };
        if let Err(error) = self.persist_repair(&before, state) {
            *state = before;
            return Err(error);
        }
        Ok(())
    }
    fn restore_key(state: &mut State, key: String, previous: Option<Entry>, previous_bytes: usize) {
        match previous {
            Some(entry) => {
                state.records.insert(key, entry);
            }
            None => {
                state.records.remove(&key);
            }
        }
        state.bytes = previous_bytes;
    }
    fn status_of(&self, repair: &RepairState) -> RepairStoreStatus {
        RepairStoreStatus {
            disk_id: self.disk_id.clone(),
            generation: repair.generation,
            frozen: repair.frozen.clone(),
            next_page: repair.next_page,
            manifest: repair.manifest.clone(),
            installed: repair.installed,
        }
    }
    fn execute(&self, generation: u64, request: Request) -> io::Result<Reply> {
        self.execute_with(generation, request, None)
    }
    fn execute_with(
        &self,
        generation: u64,
        request: Request,
        canonical: Option<Arc<[u8]>>,
    ) -> io::Result<Reply> {
        let prepared = prepare(request, canonical)?;
        let read = matches!(prepared, Prepared::Read(_));
        let key = prepared_key(&prepared).to_owned();
        let value = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let previous = state.records.get(&key).cloned();
            let previous_bytes = state.bytes;
            let value = apply(&mut state, generation, prepared)?;
            if !read && let Err(error) = self.persist_record(&key, state.records.get(&key)) {
                Self::restore_key(&mut state, key, previous, previous_bytes);
                return Err(error);
            }
            value
        };
        Ok(reply_of(value))
    }
    fn execute_many(&self, generation: u64, requests: Vec<Request>) -> Vec<io::Result<Reply>> {
        // Validate every request before taking the map lock. One lock then applies them in order,
        // so a later item observes earlier writes in this batch and one rejection does not undo them.
        let prepared: Vec<io::Result<Prepared>> = requests
            .into_iter()
            .map(|request| prepare(request, None))
            .collect();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        prepared
            .into_iter()
            .map(|item| {
                let prepared = item?;
                let read = matches!(prepared, Prepared::Read(_));
                let key = prepared_key(&prepared).to_owned();
                let previous = state.records.get(&key).cloned();
                let previous_bytes = state.bytes;
                let value = apply(&mut state, generation, prepared)?;
                if !read && let Err(error) = self.persist_record(&key, state.records.get(&key)) {
                    Self::restore_key(&mut state, key, previous, previous_bytes);
                    return Err(error);
                }
                Ok(reply_of(value))
            })
            .collect()
    }
    async fn freeze_now(&self, token: RepairToken) -> io::Result<RepairStoreStatus> {
        validate_token(&token)?;
        let permit = self
            .maintenance
            .clone()
            .try_acquire_owned()
            .map_err(|_| rejected())?;
        let snapshot = {
            let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let before = self.remember(&s);
            match &s.repair.frozen {
                Some(old) if old == &token => {}
                Some(old) if old.generation >= token.generation => return Err(rejected()),
                _ => {
                    if token.generation <= s.repair.generation {
                        return Err(rejected());
                    }
                    s.repair.frozen = Some(token.clone());
                    s.repair.manifest = None;
                    s.repair.importing = false;
                    s.repair.installed = false;
                    s.repair.next_page = 0;
                    s.repair.last_key = None;
                    s.staging.clear();
                    s.staging_bytes = 0;
                    s.pages.clear();
                }
            }
            self.publish_repair(&mut s, before)?;
            if s.repair.importing || s.repair.installed || s.repair.manifest.is_some() {
                return Ok(self.status_of(&s.repair));
            }
            // Payloads remain shared Arc values; no normal mutation can pass the freeze.
            s.records.clone()
        };
        let manifest = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            scan_manifest(&snapshot)
        })
        .await
        .map_err(error)??;
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if s.repair.frozen.as_ref() != Some(&token) || s.repair.importing || s.repair.installed {
            return Err(rejected());
        }
        let before = self.remember(&s);
        s.repair.manifest = Some(manifest);
        self.publish_repair(&mut s, before)?;
        Ok(self.status_of(&s.repair))
    }
    pub(in crate::replica) fn freeze_sync(
        &self,
        token: RepairToken,
    ) -> io::Result<RepairStoreStatus> {
        validate_token(&token)?;
        let _permit = self
            .maintenance
            .clone()
            .try_acquire_owned()
            .map_err(|_| rejected())?;
        let snapshot = {
            let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let before = self.remember(&s);
            match &s.repair.frozen {
                Some(old) if old == &token => {}
                Some(old) if old.generation >= token.generation => return Err(rejected()),
                _ => {
                    if token.generation <= s.repair.generation {
                        return Err(rejected());
                    }
                    s.repair.frozen = Some(token.clone());
                    s.repair.manifest = None;
                    s.repair.importing = false;
                    s.repair.installed = false;
                    s.repair.next_page = 0;
                    s.repair.last_key = None;
                    s.staging.clear();
                    s.staging_bytes = 0;
                    s.pages.clear();
                }
            }
            self.publish_repair(&mut s, before)?;
            if s.repair.importing || s.repair.installed || s.repair.manifest.is_some() {
                return Ok(self.status_of(&s.repair));
            }
            s.records.clone()
        };
        let manifest = scan_manifest(&snapshot)?;
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if s.repair.frozen.as_ref() != Some(&token) || s.repair.importing || s.repair.installed {
            return Err(rejected());
        }
        let before = self.remember(&s);
        s.repair.manifest = Some(manifest);
        self.publish_repair(&mut s, before)?;
        Ok(self.status_of(&s.repair))
    }
    fn export_now(
        &self,
        token: RepairToken,
        after: Option<String>,
        limit: usize,
    ) -> io::Result<RepairPage> {
        validate_token(&token)?;
        if limit == 0 || limit > 64 || after.as_ref().is_some_and(|s| s.len() > 65538) {
            return Err(rejected());
        }
        let copied = {
            let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.repair.frozen.as_ref() != Some(&token)
                || state.repair.importing
                || state.repair.installed
            {
                return Err(rejected());
            }
            state
                .records
                .range((
                    after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                    std::ops::Bound::Unbounded,
                ))
                .take(limit + 1)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>()
        };
        let mut entries = Vec::new();
        let mut bytes = 0usize;
        let mut done = true;
        for (key, value) in copied {
            let entry = value.exported(key);
            let frame = canonical_entry(&entry)?;
            if entries.len() == limit || bytes.saturating_add(frame.len()) > MAX_PAGE_BYTES {
                done = false;
                break;
            }
            bytes += frame.len();
            entries.push(entry);
        }
        if entries.is_empty() && !done {
            return Err(rejected());
        }
        let next = if done {
            None
        } else {
            entries.last().map(|e| e.key.clone())
        };
        Ok(RepairPage {
            entries,
            next,
            done,
        })
    }
    fn begin_now(
        &self,
        token: RepairToken,
        manifest: RepairManifest,
    ) -> io::Result<RepairStoreStatus> {
        validate_token(&token)?;
        validate_manifest(&manifest)?;
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if (s.repair.frozen.as_ref() == Some(&token) && (s.repair.importing || s.repair.installed))
            || (s.repair.frozen.is_none()
                && s.repair.last_token.as_ref() == Some(&token)
                && s.repair.installed)
        {
            if s.repair.manifest.as_ref() != Some(&manifest) {
                return Err(rejected());
            }
            return Ok(self.status_of(&s.repair));
        }
        if token.generation <= s.repair.generation
            || s.repair
                .frozen
                .as_ref()
                .is_some_and(|old| old != &token && old.generation >= token.generation)
        {
            return Err(rejected());
        }
        let before = self.remember(&s);
        s.repair = RepairState {
            generation: s.repair.generation,
            last_token: s.repair.last_token.clone(),
            frozen: Some(token),
            manifest: Some(manifest),
            importing: true,
            ..RepairState::default()
        };
        s.staging.clear();
        s.staging_bytes = 0;
        s.pages.clear();
        self.publish_repair(&mut s, before)?;
        Ok(self.status_of(&s.repair))
    }
    fn append_now(
        &self,
        token: RepairToken,
        sequence: u64,
        entries: Vec<RepairEntry>,
    ) -> io::Result<RepairStoreStatus> {
        validate_token(&token)?;
        if entries.is_empty() || entries.len() > 64 || sequence > i64::MAX as u64 {
            return Err(rejected());
        }
        let mut prepared = Vec::with_capacity(entries.len());
        let mut previous: Option<&str> = None;
        let mut frame_bytes = 0u64;
        let mut charge = 0usize;
        let mut hasher = Sha256::new();
        for entry in &entries {
            if previous.is_some_and(|k| k >= entry.key.as_str()) {
                return Err(rejected());
            }
            previous = Some(&entry.key);
            if entry.key.is_empty()
                || entry.key.len() > 65538
                || entry.min_epoch == 0
                || entry.min_epoch > i64::MAX as u64
            {
                return Err(rejected());
            }
            let value = entry
                .record
                .clone()
                .map(|record| Value::new(record, None))
                .transpose()?;
            if value
                .as_ref()
                .is_some_and(|v| v.record.epoch > entry.min_epoch)
            {
                return Err(rejected());
            }
            let e = Entry {
                min_epoch: entry.min_epoch,
                value,
            };
            charge = charge
                .checked_add(e.charge(&entry.key))
                .ok_or_else(rejected)?;
            let frame = canonical_entry(entry)?;
            frame_bytes = frame_bytes
                .checked_add(frame.len() as u64)
                .ok_or_else(rejected)?;
            hasher.update(&frame);
            prepared.push((entry.key.clone(), e));
        }
        if frame_bytes > MAX_PAGE_BYTES as u64 {
            return Err(rejected());
        }
        let digest = hash_string(hasher);
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if s.repair.frozen.as_ref() != Some(&token) {
            return Err(rejected());
        }
        if sequence < s.repair.next_page {
            if s.pages.get(&sequence) != Some(&digest) {
                return Err(rejected());
            }
            return Ok(self.status_of(&s.repair));
        }
        if !s.repair.importing || s.repair.installed || sequence != s.repair.next_page {
            return Err(rejected());
        }
        if s.repair
            .last_key
            .as_ref()
            .is_some_and(|k| k >= &entries[0].key)
        {
            return Err(rejected());
        }
        let expected = s.repair.manifest.as_ref().ok_or_else(rejected)?;
        let records = s
            .repair
            .records
            .checked_add(entries.len() as u64)
            .ok_or_else(rejected)?;
        let bytes = s
            .repair
            .stream_bytes
            .checked_add(frame_bytes)
            .ok_or_else(rejected)?;
        let staged = s.staging_bytes.checked_add(charge).ok_or_else(rejected)?;
        if records > expected.records
            || bytes > expected.bytes
            || staged > MAX_BYTES
            || bytes > MAX_REPAIR_BYTES
        {
            return Err(rejected());
        }
        // Every validation precedes mutation: a malformed page cannot install a prefix.
        let before = self.remember(&s);
        for (key, entry) in prepared {
            s.staging.insert(key, entry);
        }
        s.staging_bytes = staged;
        s.pages.insert(sequence, digest);
        s.repair.next_page += 1;
        s.repair.records = records;
        s.repair.stream_bytes = bytes;
        s.repair.last_key = entries.last().map(|e| e.key.clone());
        self.publish_repair(&mut s, before)?;
        Ok(self.status_of(&s.repair))
    }
    async fn finish_now(&self, token: RepairToken) -> io::Result<RepairStoreStatus> {
        validate_token(&token)?;
        let permit = self
            .maintenance
            .clone()
            .try_acquire_owned()
            .map_err(|_| rejected())?;
        let (snapshot, expected, page) = {
            let s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if s.repair.frozen.as_ref() != Some(&token) {
                return Err(rejected());
            }
            if s.repair.installed {
                return Ok(self.status_of(&s.repair));
            }
            if !s.repair.importing {
                return Err(rejected());
            }
            (
                s.staging.clone(),
                s.repair.manifest.clone(),
                s.repair.next_page,
            )
        };
        let actual = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            scan_manifest(&snapshot)
        })
        .await
        .map_err(error)??;
        let (status, old) = {
            let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if s.repair.frozen.as_ref() != Some(&token)
                || !s.repair.importing
                || s.repair.installed
                || s.repair.next_page != page
                || s.repair.manifest != expected
                || expected.as_ref() != Some(&actual)
            {
                return Err(rejected());
            }
            let before = self.remember(&s);
            let staged = std::mem::take(&mut s.staging);
            let old = std::mem::replace(&mut s.records, staged);
            s.bytes = s.staging_bytes;
            s.staging_bytes = 0;
            s.repair.importing = false;
            s.repair.installed = true;
            if let Err(error) = self.publish_repair(&mut s, before) {
                drop(old);
                return Err(error);
            }
            (self.status_of(&s.repair), old)
        };
        drop(old);
        Ok(status)
    }
    pub(in crate::replica) fn finish_sync(
        &self,
        token: RepairToken,
    ) -> io::Result<RepairStoreStatus> {
        validate_token(&token)?;
        let _permit = self
            .maintenance
            .clone()
            .try_acquire_owned()
            .map_err(|_| rejected())?;
        let (snapshot, expected, page) = {
            let s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if s.repair.frozen.as_ref() != Some(&token) {
                return Err(rejected());
            }
            if s.repair.installed {
                return Ok(self.status_of(&s.repair));
            }
            if !s.repair.importing {
                return Err(rejected());
            }
            (
                s.staging.clone(),
                s.repair.manifest.clone(),
                s.repair.next_page,
            )
        };
        let actual = scan_manifest(&snapshot)?;
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if s.repair.frozen.as_ref() != Some(&token)
            || !s.repair.importing
            || s.repair.installed
            || s.repair.next_page != page
            || s.repair.manifest != expected
            || expected.as_ref() != Some(&actual)
        {
            return Err(rejected());
        }
        let before = self.remember(&s);
        let staged = std::mem::take(&mut s.staging);
        let old = std::mem::replace(&mut s.records, staged);
        s.bytes = s.staging_bytes;
        s.staging_bytes = 0;
        s.repair.importing = false;
        s.repair.installed = true;
        if let Err(error) = self.publish_repair(&mut s, before) {
            drop(old);
            return Err(error);
        }
        drop(old);
        Ok(self.status_of(&s.repair))
    }
    fn resume_now(&self, token: RepairToken, allow: bool) -> io::Result<RepairStoreStatus> {
        validate_token(&token)?;
        let mut s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if s.repair.generation == token.generation
            && s.repair.frozen.is_none()
            && s.repair.last_token.as_ref() == Some(&token)
        {
            return Ok(self.status_of(&s.repair));
        }
        if token.generation <= s.repair.generation {
            return Err(rejected());
        }
        match s.repair.frozen.as_ref() {
            Some(old) if old == &token && !s.repair.importing => {}
            Some(old) if allow && old.generation < token.generation => {}
            None if allow => {}
            _ => return Err(rejected()),
        }
        let before = self.remember(&s);
        s.repair.generation = token.generation;
        s.repair.last_token = Some(token);
        s.repair.frozen = None;
        s.repair.importing = false;
        s.staging.clear();
        s.staging_bytes = 0;
        s.pages.clear();
        self.publish_repair(&mut s, before)?;
        Ok(self.status_of(&s.repair))
    }
    pub(in crate::replica) fn status_now(&self) -> RepairStoreStatus {
        self.status_of(
            &self
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .repair,
        )
    }
    pub(in crate::replica) fn execute_now(
        &self,
        generation: u64,
        request: Request,
        canonical: Option<Arc<[u8]>>,
    ) -> io::Result<Reply> {
        self.execute_with(generation, request, canonical)
    }
    pub(in crate::replica) fn execute_many_now(
        &self,
        generation: u64,
        requests: Vec<Request>,
    ) -> Vec<io::Result<Reply>> {
        self.execute_many(generation, requests)
    }
    pub(in crate::replica) fn export_sync(
        &self,
        token: RepairToken,
        after: Option<String>,
        limit: usize,
    ) -> io::Result<RepairPage> {
        self.export_now(token, after, limit)
    }
    pub(in crate::replica) fn begin_sync(
        &self,
        token: RepairToken,
        manifest: RepairManifest,
    ) -> io::Result<RepairStoreStatus> {
        self.begin_now(token, manifest)
    }
    pub(in crate::replica) fn append_sync(
        &self,
        token: RepairToken,
        sequence: u64,
        entries: Vec<RepairEntry>,
    ) -> io::Result<RepairStoreStatus> {
        self.append_now(token, sequence, entries)
    }
    pub(in crate::replica) fn resume_sync(
        &self,
        token: RepairToken,
        allow: bool,
    ) -> io::Result<RepairStoreStatus> {
        self.resume_now(token, allow)
    }
}
impl ReplicaStore for MemoryStore {
    fn disk_id(&self) -> &str {
        &self.disk_id
    }
    fn voting_ready(&self, generation: u64) -> bool {
        let s = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        s.repair.frozen.is_none() && s.repair.generation == generation
    }
    fn execute_at(&self, generation: u64, request: Request) -> StoreFuture<'_, Reply> {
        Box::pin(async move { self.execute(generation, request) })
    }
    fn execute_prepared(
        &self,
        generation: u64,
        request: Request,
        canonical: Option<Arc<[u8]>>,
    ) -> StoreFuture<'_, Reply> {
        Box::pin(async move { self.execute_with(generation, request, canonical) })
    }
    fn execute_many_at(
        &self,
        generation: u64,
        requests: Vec<Request>,
    ) -> StoreFuture<'_, Vec<io::Result<Reply>>> {
        Box::pin(async move { Ok(self.execute_many(generation, requests)) })
    }
    fn status(&self) -> StoreFuture<'_, RepairStoreStatus> {
        Box::pin(async move {
            Ok(self.status_of(
                &self
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .repair,
            ))
        })
    }
    fn freeze(&self, token: RepairToken) -> StoreFuture<'_, RepairStoreStatus> {
        Box::pin(async move { self.freeze_now(token).await })
    }
    fn export_page(
        &self,
        token: RepairToken,
        after: Option<String>,
        limit: usize,
    ) -> StoreFuture<'_, RepairPage> {
        Box::pin(async move { self.export_now(token, after, limit) })
    }
    fn begin_import(
        &self,
        token: RepairToken,
        manifest: RepairManifest,
    ) -> StoreFuture<'_, RepairStoreStatus> {
        Box::pin(async move { self.begin_now(token, manifest) })
    }
    fn append_page(
        &self,
        token: RepairToken,
        sequence: u64,
        entries: Vec<RepairEntry>,
    ) -> StoreFuture<'_, RepairStoreStatus> {
        Box::pin(async move { self.append_now(token, sequence, entries) })
    }
    fn finish_import(&self, token: RepairToken) -> StoreFuture<'_, RepairStoreStatus> {
        Box::pin(async move { self.finish_now(token).await })
    }
    fn resume(&self, token: RepairToken, allow: bool) -> StoreFuture<'_, RepairStoreStatus> {
        Box::pin(async move { self.resume_now(token, allow) })
    }
}
enum Prepared {
    Read(String),
    Fence(String, u64),
    Write(String, Value),
}
fn prepare(request: Request, canonical: Option<Arc<[u8]>>) -> io::Result<Prepared> {
    // Encoding/validation occurs before locking the map, never under the shared mutex.
    let prepared = match request {
        Request::Read { key } => Prepared::Read(key),
        Request::Fence { key, epoch } => Prepared::Fence(key, epoch),
        Request::Write { key, record } => Prepared::Write(key, Value::new(record, canonical)?),
    };
    let (key, epoch) = match &prepared {
        Prepared::Read(key) => (key.as_str(), 0),
        Prepared::Fence(key, epoch) => (key.as_str(), *epoch),
        Prepared::Write(key, value) => (key.as_str(), value.record.epoch),
    };
    if key.is_empty()
        || key.len() > 65538
        || epoch > i64::MAX as u64
        || (!matches!(prepared, Prepared::Read(_)) && epoch == 0)
    {
        return Err(rejected());
    }
    Ok(prepared)
}
fn prepared_key(prepared: &Prepared) -> &str {
    match prepared {
        Prepared::Read(key) | Prepared::Fence(key, _) | Prepared::Write(key, _) => key,
    }
}
fn apply(state: &mut State, generation: u64, prepared: Prepared) -> io::Result<Option<Value>> {
    let key = match &prepared {
        Prepared::Read(key) | Prepared::Fence(key, _) | Prepared::Write(key, _) => key.as_str(),
    };
    let epoch = match &prepared {
        Prepared::Read(_) => 0,
        Prepared::Fence(_, epoch) => *epoch,
        Prepared::Write(_, value) => value.record.epoch,
    };
    if state.repair.frozen.is_some() {
        return Err(diagnosis::reject(Failure::JournalFrozen));
    }
    if state.repair.generation != generation {
        return Err(diagnosis::reject(Failure::JournalGeneration));
    }
    let min_epoch = state.records.get(key).map_or(0, |entry| entry.min_epoch);
    if !matches!(prepared, Prepared::Read(_)) && epoch < min_epoch {
        return Err(diagnosis::reject(Failure::JournalEpoch));
    }
    match prepared {
        Prepared::Read(key) => Ok(state
            .records
            .get(&key)
            .and_then(|entry| entry.value.clone())),
        Prepared::Fence(key, epoch) => {
            if let Some(entry) = state.records.get_mut(&key) {
                entry.min_epoch = epoch;
                return Ok(entry.value.clone());
            }
            if state.records.len() >= MAX_ENTRIES
                || state.bytes.saturating_add(key.len()) > MAX_BYTES
            {
                return Err(rejected());
            }
            state.bytes += key.len();
            state.records.insert(
                key,
                Entry {
                    min_epoch: epoch,
                    value: None,
                },
            );
            Ok(None)
        }
        Prepared::Write(key, value) => {
            if let Some(old) = state
                .records
                .get(&key)
                .and_then(|entry| entry.value.as_ref())
            {
                if old.record.version() > value.record.version() {
                    return Err(diagnosis::reject(Failure::JournalOlderVersion));
                }
                if old.record.version() == value.record.version()
                    && old.canonical != value.canonical
                {
                    return Err(diagnosis::reject(Failure::JournalConflictingValue));
                }
            }
            let old = state
                .records
                .get(&key)
                .map_or(0, |entry| entry.charge(&key));
            let entry = Entry {
                min_epoch: value.record.epoch,
                value: Some(value),
            };
            let total = state
                .bytes
                .checked_sub(old)
                .and_then(|remaining| remaining.checked_add(entry.charge(&key)))
                .ok_or_else(rejected)?;
            if total > MAX_BYTES
                || (!state.records.contains_key(&key) && state.records.len() >= MAX_ENTRIES)
            {
                return Err(rejected());
            }
            state.records.insert(key, entry);
            state.bytes = total;
            Ok(None)
        }
    }
}
fn reply_of(value: Option<Value>) -> Reply {
    Reply {
        record: value.map(|value| value.record.as_ref().clone()),
    }
}
fn validate_token(token: &RepairToken) -> io::Result<()> {
    if token.generation == 0
        || token.id.is_empty()
        || token.id.len() > 128
        || !token.id.bytes().all(|b| (33..=126).contains(&b))
    {
        Err(rejected())
    } else {
        Ok(())
    }
}
fn validate_manifest(m: &RepairManifest) -> io::Result<()> {
    if m.records > MAX_ENTRIES as u64
        || m.bytes > MAX_REPAIR_BYTES
        || m.digest.len() != 64
        || !m.digest.bytes().all(|b| b.is_ascii_hexdigit())
    {
        Err(rejected())
    } else {
        Ok(())
    }
}
fn canonical_entry(e: &RepairEntry) -> io::Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(e).map_err(error)?;
    bytes.push(b'\n');
    Ok(bytes)
}
fn hash_string(hash: Sha256) -> String {
    hash.finalize().iter().map(|b| format!("{b:02x}")).collect()
}
fn scan_manifest(records: &BTreeMap<String, Entry>) -> io::Result<RepairManifest> {
    let mut hash = Sha256::new();
    let mut bytes = 0u64;
    let mut retained = 0usize;
    for (key, value) in records {
        retained = retained
            .checked_add(value.charge(key))
            .ok_or_else(rejected)?;
        let frame = canonical_entry(&value.exported(key.clone()))?;
        bytes = bytes.checked_add(frame.len() as u64).ok_or_else(rejected)?;
        hash.update(frame);
    }
    if retained > MAX_BYTES || bytes > MAX_REPAIR_BYTES || records.len() > MAX_ENTRIES {
        return Err(rejected());
    }
    Ok(RepairManifest {
        records: records.len() as u64,
        bytes,
        digest: hash_string(hash),
    })
}

fn same_entry(left: &Entry, right: &Entry) -> bool {
    left.min_epoch == right.min_epoch
        && match (&left.value, &right.value) {
            (None, None) => true,
            (Some(left), Some(right)) => left.canonical == right.canonical,
            _ => false,
        }
}
fn diff_entries(
    before: &BTreeMap<String, Entry>,
    after: &BTreeMap<String, Entry>,
) -> io::Result<Vec<(String, Option<Vec<u8>>)>> {
    let mut changed = Vec::new();
    for key in before.keys() {
        if !after.contains_key(key) {
            changed.push((key.clone(), None));
        }
    }
    for (key, entry) in after {
        let stale = before.get(key).is_none_or(|old| !same_entry(old, entry));
        if stale {
            changed.push((key.clone(), Some(encode_entry(entry)?)));
        }
    }
    Ok(changed)
}
pub(in crate::replica) fn encode_entry(entry: &Entry) -> io::Result<Vec<u8>> {
    let canonical = entry
        .value
        .as_ref()
        .map(|value| value.canonical.as_ref())
        .unwrap_or(b"");
    let len = u32::try_from(canonical.len()).map_err(|_| rejected())?;
    let mut bytes = Vec::with_capacity(13 + canonical.len());
    bytes.push(1);
    bytes.extend_from_slice(&entry.min_epoch.to_le_bytes());
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(canonical);
    Ok(bytes)
}
pub(in crate::replica) fn decode_entry(bytes: &[u8]) -> io::Result<Entry> {
    if bytes.len() < 13 || bytes[0] != 1 {
        return Err(rejected());
    }
    let min_epoch = u64::from_le_bytes(bytes[1..9].try_into().map_err(|_| rejected())?);
    let len = u32::from_le_bytes(bytes[9..13].try_into().map_err(|_| rejected())?) as usize;
    if bytes.len() != 13 + len || (len == 0 && min_epoch == 0) {
        return Err(rejected());
    }
    if len == 0 {
        return Ok(Entry {
            min_epoch,
            value: None,
        });
    }
    let canonical: Arc<[u8]> = bytes[13..].into();
    let record: Record = serde_json::from_slice(&canonical).map_err(|_| rejected())?;
    if record.epoch == 0 || record.epoch > min_epoch {
        return Err(rejected());
    }
    Ok(Entry {
        min_epoch,
        value: Some(Value::new(record, Some(canonical))?),
    })
}
#[derive(Serialize, Deserialize)]
struct StoredRepair {
    generation: u64,
    frozen: Option<RepairToken>,
    last_token: Option<RepairToken>,
    manifest: Option<RepairManifest>,
    next_page: u64,
    last_key: Option<String>,
    stream_bytes: u64,
    records: u64,
    importing: bool,
    installed: bool,
    pages: BTreeMap<u64, String>,
}
fn encode_repair(state: &State) -> io::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(&StoredRepair {
        generation: state.repair.generation,
        frozen: state.repair.frozen.clone(),
        last_token: state.repair.last_token.clone(),
        manifest: state.repair.manifest.clone(),
        next_page: state.repair.next_page,
        last_key: state.repair.last_key.clone(),
        stream_bytes: state.repair.stream_bytes,
        records: state.repair.records,
        importing: state.repair.importing,
        installed: state.repair.installed,
        pages: state.pages.clone(),
    })
    .map_err(error)?;
    if bytes.len() > 16 * 1024 * 1024 {
        return Err(rejected());
    }
    Ok(bytes)
}
pub(in crate::replica) fn state_from_loaded(
    records: BTreeMap<String, Entry>,
    staging: BTreeMap<String, Entry>,
    repair_json: Option<&[u8]>,
) -> io::Result<State> {
    let mut state = State::default();
    if let Some(bytes) = repair_json {
        if bytes.len() > 16 * 1024 * 1024 {
            return Err(rejected());
        }
        let stored: StoredRepair = serde_json::from_slice(bytes).map_err(|_| rejected())?;
        state.pages = stored.pages;
        state.repair = RepairState {
            generation: stored.generation,
            frozen: stored.frozen,
            last_token: stored.last_token,
            manifest: stored.manifest,
            next_page: stored.next_page,
            last_key: stored.last_key,
            stream_bytes: stored.stream_bytes,
            records: stored.records,
            importing: stored.importing,
            installed: stored.installed,
        };
    }
    state.bytes = records.iter().map(|(key, entry)| entry.charge(key)).sum();
    state.staging_bytes = staging.iter().map(|(key, entry)| entry.charge(key)).sum();
    state.records = records;
    state.staging = staging;
    if state.bytes > MAX_BYTES
        || state.staging_bytes > MAX_BYTES
        || state.records.len() > MAX_ENTRIES
        || state.staging.len() > MAX_ENTRIES
        || state.pages.len() as u64 != state.repair.next_page
        || state.repair.stream_bytes > MAX_REPAIR_BYTES
        || (state.repair.importing && state.staging.len() as u64 != state.repair.records)
        || (!state.repair.importing && !state.staging.is_empty())
    {
        return Err(rejected());
    }
    Ok(state)
}

#[cfg(test)]
mod tests;
