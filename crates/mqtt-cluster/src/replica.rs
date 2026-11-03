//! Bounded durable data-replica register. Fence and its returned value are atomic.
use mqtt_cluster_core::{ClusterMessage, DeliveryId};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::Path,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::Semaphore;

pub(crate) const MAX_RECORD: usize = 8 * 1024 * 1024;
const MAX_BYTES: i64 = 256 * 1024 * 1024;
const MAX_REPAIR_BYTES: u64 = 768 * 1024 * 1024;
const MAX_PAGE_BYTES: usize = 12 * 1024 * 1024;
const MAX_ENTRIES: u64 = 100_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RepairToken {
    pub id: String,
    pub generation: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RepairManifest {
    pub records: u64,
    pub bytes: u64,
    pub digest: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct RepairEntry {
    pub key: String,
    pub min_epoch: u64,
    pub record: Option<Record>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct RepairPage {
    pub entries: Vec<RepairEntry>,
    pub next: Option<String>,
    pub done: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RepairStoreStatus {
    pub disk_id: String,
    pub generation: u64,
    pub frozen: Option<RepairToken>,
    pub next_page: u64,
    pub manifest: Option<RepairManifest>,
    pub installed: bool,
}
#[derive(Clone, Default, Serialize, Deserialize)]
struct RepairState {
    generation: u64,
    frozen: Option<RepairToken>,
    last_token: Option<RepairToken>,
    manifest: Option<RepairManifest>,
    next_page: u64,
    last_key: Option<String>,
    stream_bytes: u64,
    record_bytes: u64,
    records: u64,
    importing: bool,
    installed: bool,
}
pub(crate) fn rejected() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "replicated data unavailable or quota exceeded",
    )
}
fn error(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

/// FIFO source streams retain a high water mark rather than one receipt per message.
#[derive(Clone, Default, Serialize)]
#[serde(transparent)]
pub(crate) struct Receipts(BTreeMap<String, DeliveryId>);
impl Receipts {
    pub fn contains(&self, id: &DeliveryId) -> bool {
        self.0.get(&id.source).is_some_and(|highest| highest >= id)
    }
    pub fn insert(&mut self, id: DeliveryId) {
        let entry = self
            .0
            .entry(id.source.clone())
            .or_insert_with(|| id.clone());
        if *entry < id {
            *entry = id;
        }
    }
    pub fn clear(&mut self) {
        self.0.clear();
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
}
impl<'de> Deserialize<'de> for Receipts {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Representation {
            Watermarks(BTreeMap<String, DeliveryId>),
            Legacy(Vec<DeliveryId>),
        }
        let mut result = Self::default();
        match Representation::deserialize(deserializer)? {
            Representation::Watermarks(values) => {
                if values.iter().any(|(source, id)| source != &id.source) {
                    return Err(serde::de::Error::custom("receipt stream identity mismatch"));
                }
                result.0 = values;
            }
            Representation::Legacy(values) => {
                for id in values {
                    result.insert(id);
                }
            }
        }
        if result.len() > 8192 {
            return Err(serde::de::Error::custom("receipt stream capacity"));
        }
        Ok(result)
    }
}
#[derive(Clone, Serialize)]
pub(crate) struct Recipient {
    pub client_id: String,
    pub generation: u64,
}
impl<'de> Deserialize<'de> for Recipient {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Value {
            Current {
                client_id: String,
                #[serde(default)]
                generation: u64,
            },
            Legacy(String),
        }
        Ok(match Value::deserialize(deserializer)? {
            Value::Current {
                client_id,
                generation,
            } => Self {
                client_id,
                generation,
            },
            Value::Legacy(client_id) => Self {
                client_id,
                generation: 0,
            },
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Intent {
    pub id: DeliveryId,
    pub message: ClusterMessage,
    pub recipients: Vec<Recipient>,
    pub completed: BTreeSet<String>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Record {
    pub epoch: u64,
    pub physical_revision: u64,
    pub revision: u64,
    #[serde(default)]
    pub logical_generation: u64,
    pub snapshot: Option<Vec<u8>>,
    pub filters: Vec<String>,
    pub intents: Vec<Intent>,
    pub receipts: Receipts,
}
impl Record {
    pub fn received_or_obsolete(&self, id: &DeliveryId) -> bool {
        self.logical_generation != id.target_generation || self.receipts.contains(id)
    }
    pub fn version(&self) -> (u64, u64) {
        (self.epoch, self.physical_revision)
    }
    pub fn empty(epoch: u64) -> Self {
        Self {
            epoch,
            physical_revision: 0,
            revision: 0,
            logical_generation: 0,
            snapshot: None,
            filters: Vec::new(),
            intents: Vec::new(),
            receipts: Receipts::default(),
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum Request {
    Read { key: String },
    Fence { key: String, epoch: u64 },
    Write { key: String, record: Record },
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Reply {
    pub record: Option<Record>,
}
struct Inner {
    db: Mutex<Connection>,
    _lock: std::fs::File,
    disk_id: String,
    slots: Arc<Semaphore>,
    generation: AtomicU64,
    frozen: AtomicBool,
}
#[derive(Clone)]
pub(crate) struct Journal(Arc<Inner>);
impl Journal {
    /// Must be called from a blocking worker, before the Broker begins service.
    pub fn open(path: &Path, identity: (&str, u64), members: &[(u64, String)]) -> io::Result<Self> {
        std::fs::create_dir_all(path)?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.join("data.lock"))?;
        lock.try_lock().map_err(|_| rejected())?;
        let db = Connection::open(path.join("data.sqlite")).map_err(error)?;
        db.pragma_update(None, "journal_mode", "WAL")
            .map_err(error)?;
        db.pragma_update(None, "synchronous", "FULL")
            .map_err(error)?;
        db.pragma_update(None, "wal_autocheckpoint", 32)
            .map_err(error)?;
        // Records and a bounded staged copy coexist during replacement. WAL
        // overhead is additional; this is not a hard directory byte quota.
        db.pragma_update(None, "max_page_count", 196608)
            .map_err(error)?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS identity(id INTEGER PRIMARY KEY CHECK(id=1), data BLOB NOT NULL); CREATE TABLE IF NOT EXISTS records(key TEXT PRIMARY KEY,min_epoch INTEGER NOT NULL,data BLOB);").map_err(error)?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS disk_identity(id INTEGER PRIMARY KEY CHECK(id=1), disk_id TEXT NOT NULL);").map_err(error)?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS repair_state(id INTEGER PRIMARY KEY CHECK(id=1), data BLOB NOT NULL); CREATE TABLE IF NOT EXISTS repair_records(key TEXT PRIMARY KEY,min_epoch INTEGER NOT NULL,data BLOB); CREATE TABLE IF NOT EXISTS repair_pages(sequence INTEGER PRIMARY KEY,digest TEXT NOT NULL);").map_err(error)?;
        let disk_id: Option<String> = db
            .query_row("SELECT disk_id FROM disk_identity WHERE id=1", [], |r| {
                r.get(0)
            })
            .optional()
            .map_err(error)?;
        let disk_id = match disk_id {
            Some(id) if id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()) => id,
            Some(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid replica disk identity",
                ));
            }
            None => {
                let mut random = [0u8; 16];
                getrandom::getrandom(&mut random).map_err(error)?;
                let id = random
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>();
                db.execute("INSERT INTO disk_identity VALUES(1,?1)", [&id])
                    .map_err(error)?;
                id
            }
        };
        let mut members = members.to_vec();
        members.sort();
        let value = serde_json::to_vec(&(identity, members)).map_err(error)?;
        let old: Option<Vec<u8>> = db
            .query_row("SELECT data FROM identity WHERE id=1", [], |r| r.get(0))
            .optional()
            .map_err(error)?;
        if let Some(old) = old {
            if old != value {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "replica identity or membership mismatch",
                ));
            }
        } else {
            db.execute("INSERT INTO identity VALUES(1,?1)", [value])
                .map_err(error)?;
        }
        let repair = read_repair_state(&db)?;
        Ok(Self(Arc::new(Inner {
            db: Mutex::new(db),
            _lock: lock,
            disk_id,
            slots: Arc::new(Semaphore::new(32)),
            generation: AtomicU64::new(repair.generation),
            frozen: AtomicBool::new(repair.frozen.is_some()),
        })))
    }
    pub fn disk_id(&self) -> &str {
        &self.0.disk_id
    }
    pub async fn execute(&self, request: Request) -> io::Result<Reply> {
        self.execute_at(0, request).await
    }
    pub fn voting_ready(&self, generation: u64) -> bool {
        !self.0.frozen.load(Ordering::Acquire)
            && self.0.generation.load(Ordering::Acquire) == generation
    }
    pub async fn execute_at(&self, generation: u64, request: Request) -> io::Result<Reply> {
        self.with_db(move |db| execute_transaction(db, generation, request))
            .await
    }
    async fn with_db<T: Send + 'static>(
        &self,
        call: impl FnOnce(&mut Connection) -> io::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        let permit = self
            .0
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| rejected())?;
        let inner = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut db = inner.db.lock().unwrap_or_else(PoisonError::into_inner);
            let result = call(&mut db);
            // Refresh even after a failed manifest calculation: freeze itself
            // was already committed and must remain fail-closed.
            match read_repair_state(&db) {
                Ok(state) => {
                    inner.frozen.store(true, Ordering::Release);
                    inner.generation.store(state.generation, Ordering::Release);
                    inner
                        .frozen
                        .store(state.frozen.is_some(), Ordering::Release);
                }
                Err(_) => inner.frozen.store(true, Ordering::Release),
            }
            result
        })
        .await
        .map_err(error)?
    }
    pub async fn status(&self) -> io::Result<RepairStoreStatus> {
        let disk_id = self.disk_id().to_owned();
        self.with_db(move |db| Ok(store_status(&disk_id, read_repair_state(db)?)))
            .await
    }
    pub async fn freeze(&self, token: RepairToken) -> io::Result<RepairStoreStatus> {
        let disk_id = self.disk_id().to_owned();
        self.with_db(move |db| {
            validate_token(&token)?;
            let mut state = read_repair_state(db)?;
            match &state.frozen {
                Some(old) if old == &token => {}
                Some(old) if old.generation >= token.generation => return Err(rejected()),
                _ => {
                    if token.generation <= state.generation {
                        return Err(rejected());
                    }
                    state.frozen = Some(token.clone());
                    state.manifest = None;
                    state.importing = false;
                    state.installed = false;
                    state.next_page = 0;
                    state.last_key = None;
                    let tx = db.transaction().map_err(error)?;
                    tx.execute_batch("DELETE FROM repair_records; DELETE FROM repair_pages;")
                        .map_err(error)?;
                    save_repair_state(&tx, &state)?;
                    tx.commit().map_err(error)?;
                }
            }
            if !state.importing && !state.installed && state.manifest.is_none() {
                let manifest = scan_manifest(db, "records")?;
                state.manifest = Some(manifest);
                let tx = db.transaction().map_err(error)?;
                save_repair_state(&tx, &state)?;
                tx.commit().map_err(error)?;
            }
            Ok(store_status(&disk_id, state))
        })
        .await
    }
    pub async fn export_page(
        &self,
        token: RepairToken,
        after: Option<String>,
        limit: usize,
    ) -> io::Result<RepairPage> {
        self.with_db(move |db| {
            validate_token(&token)?;
            if limit == 0 || limit > 64 || after.as_ref().is_some_and(|s| s.len() > 65538) {
                return Err(rejected());
            }
            let state = read_repair_state(db)?;
            if state.frozen.as_ref() != Some(&token) || state.importing || state.installed {
                return Err(rejected());
            }
            let mut statement = db.prepare("SELECT key,min_epoch,data,length(data) FROM records WHERE (?1 IS NULL OR key>?1) ORDER BY key COLLATE BINARY LIMIT ?2").map_err(error)?;
            let mut rows = statement.query(params![after, (limit + 1) as i64]).map_err(error)?;
            let mut entries = Vec::new();
            let mut bytes = 0usize;
            let mut done = true;
            while let Some(row) = rows.next().map_err(error)? {
                let entry = entry_from_row(row)?;
                let encoded = canonical_entry(&entry)?;
                if entries.len() == limit || bytes.saturating_add(encoded.len()) > MAX_PAGE_BYTES {
                    done = false;
                    break;
                }
                bytes = bytes.saturating_add(encoded.len());
                entries.push(entry);
            }
            if entries.is_empty() && !done {
                return Err(rejected());
            }
            let next = if done { None } else { entries.last().map(|e| e.key.clone()) };
            Ok(RepairPage { entries, next, done })
        }).await
    }
    pub async fn begin_import(
        &self,
        token: RepairToken,
        manifest: RepairManifest,
    ) -> io::Result<RepairStoreStatus> {
        let disk_id = self.disk_id().to_owned();
        self.with_db(move |db| {
            validate_token(&token)?;
            validate_manifest(&manifest)?;
            let mut state = read_repair_state(db)?;
            if state.frozen.as_ref() == Some(&token) && (state.importing || state.installed) {
                if state.manifest.as_ref() != Some(&manifest) {
                    return Err(rejected());
                }
                return Ok(store_status(&disk_id, state));
            }
            if state.frozen.is_none()
                && state.last_token.as_ref() == Some(&token)
                && state.installed
            {
                if state.manifest.as_ref() != Some(&manifest) {
                    return Err(rejected());
                }
                return Ok(store_status(&disk_id, state));
            }
            // A newer Controller-authorized repair may replace abandoned staging.
            if token.generation <= state.generation
                || state
                    .frozen
                    .as_ref()
                    .is_some_and(|old| old != &token && old.generation >= token.generation)
            {
                return Err(rejected());
            }
            state.frozen = Some(token);
            state.manifest = Some(manifest);
            state.next_page = 0;
            state.last_key = None;
            state.records = 0;
            state.stream_bytes = 0;
            state.record_bytes = 0;
            state.importing = true;
            state.installed = false;
            let tx = db.transaction().map_err(error)?;
            tx.execute_batch("DELETE FROM repair_records; DELETE FROM repair_pages;")
                .map_err(error)?;
            save_repair_state(&tx, &state)?;
            tx.commit().map_err(error)?;
            Ok(store_status(&disk_id, state))
        })
        .await
    }
    pub async fn append_page(
        &self,
        token: RepairToken,
        sequence: u64,
        entries: Vec<RepairEntry>,
    ) -> io::Result<RepairStoreStatus> {
        let disk_id = self.disk_id().to_owned();
        self.with_db(move |db| {
            validate_token(&token)?;
            if entries.is_empty() || entries.len() > 64 || sequence > i64::MAX as u64 {
                return Err(rejected());
            }
            let mut stream_bytes = 0u64;
            let mut record_bytes = 0u64;
            let mut hasher = Sha256::new();
            let mut previous: Option<&str> = None;
            let mut encoded = Vec::with_capacity(entries.len());
            for entry in &entries {
                if previous.is_some_and(|key| key >= entry.key.as_str()) {
                    return Err(rejected());
                }
                previous = Some(&entry.key);
                let data = validate_entry(entry)?;
                record_bytes = record_bytes
                    .checked_add(data.as_ref().map_or(0, Vec::len) as u64)
                    .ok_or_else(rejected)?;
                let frame = canonical_entry(entry)?;
                stream_bytes = stream_bytes
                    .checked_add(frame.len() as u64)
                    .ok_or_else(rejected)?;
                if stream_bytes > MAX_PAGE_BYTES as u64 {
                    return Err(rejected());
                }
                hasher.update(frame);
                encoded.push(data);
            }
            let digest = hash_string(hasher);
            let tx = db.transaction().map_err(error)?;
            let mut state = read_repair_state(&tx)?;
            if state.frozen.as_ref() != Some(&token) {
                return Err(rejected());
            }
            if sequence < state.next_page {
                let old: Option<String> = tx
                    .query_row(
                        "SELECT digest FROM repair_pages WHERE sequence=?1",
                        [sequence as i64],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(error)?;
                if old.as_ref() != Some(&digest) {
                    return Err(rejected());
                }
                return Ok(store_status(&disk_id, state));
            }
            if !state.importing || state.installed || sequence != state.next_page {
                return Err(rejected());
            }
            let expected = state.manifest.as_ref().ok_or_else(rejected)?;
            if state
                .last_key
                .as_ref()
                .is_some_and(|key| key >= &entries[0].key)
            {
                return Err(rejected());
            }
            state.records = state
                .records
                .checked_add(entries.len() as u64)
                .ok_or_else(rejected)?;
            state.stream_bytes = state
                .stream_bytes
                .checked_add(stream_bytes)
                .ok_or_else(rejected)?;
            state.record_bytes = state
                .record_bytes
                .checked_add(record_bytes)
                .ok_or_else(rejected)?;
            if state.records > expected.records
                || state.stream_bytes > expected.bytes
                || state.record_bytes > MAX_BYTES as u64
            {
                return Err(rejected());
            }
            for (entry, data) in entries.iter().zip(encoded) {
                tx.execute(
                    "INSERT INTO repair_records(key,min_epoch,data) VALUES(?1,?2,?3)",
                    params![entry.key, entry.min_epoch as i64, data],
                )
                .map_err(error)?;
            }
            tx.execute(
                "INSERT INTO repair_pages(sequence,digest) VALUES(?1,?2)",
                params![sequence as i64, digest],
            )
            .map_err(error)?;
            state.last_key = entries.last().map(|entry| entry.key.clone());
            state.next_page = state.next_page.checked_add(1).ok_or_else(rejected)?;
            save_repair_state(&tx, &state)?;
            tx.commit().map_err(error)?;
            Ok(store_status(&disk_id, state))
        })
        .await
    }
    pub async fn finish_import(&self, token: RepairToken) -> io::Result<RepairStoreStatus> {
        let disk_id = self.disk_id().to_owned();
        self.with_db(move |db| {
            validate_token(&token)?;
            let mut state = read_repair_state(db)?;
            if state.frozen.as_ref() != Some(&token) { return Err(rejected()); }
            if state.installed { return Ok(store_status(&disk_id, state)); }
            if !state.importing { return Err(rejected()); }
            let actual = scan_manifest(db, "repair_records")?;
            if state.manifest.as_ref() != Some(&actual) { return Err(rejected()); }
            let tx = db.transaction().map_err(error)?;
            tx.execute_batch("ALTER TABLE records RENAME TO repair_old_records; ALTER TABLE repair_records RENAME TO records; DROP TABLE repair_old_records; CREATE TABLE repair_records(key TEXT PRIMARY KEY,min_epoch INTEGER NOT NULL,data BLOB);").map_err(error)?;
            state.importing = false;
            state.installed = true;
            save_repair_state(&tx, &state)?;
            #[cfg(test)]
            crash_during_install("before_commit");
            tx.commit().map_err(error)?;
            #[cfg(test)]
            crash_during_install("after_commit");
            Ok(store_status(&disk_id, state))
        }).await
    }
    pub async fn resume(
        &self,
        token: RepairToken,
        allow_unfrozen_advance: bool,
    ) -> io::Result<RepairStoreStatus> {
        let disk_id = self.disk_id().to_owned();
        self.with_db(move |db| {
            validate_token(&token)?;
            let mut state = read_repair_state(db)?;
            if state.generation == token.generation
                && state.frozen.is_none()
                && state.last_token.as_ref() == Some(&token)
            {
                return Ok(store_status(&disk_id, state));
            }
            if token.generation <= state.generation {
                return Err(rejected());
            }
            match state.frozen.as_ref() {
                Some(old) if old == &token && !state.importing => {}
                Some(old) if allow_unfrozen_advance && old.generation < token.generation => {}
                None if allow_unfrozen_advance => {}
                _ => return Err(rejected()),
            }
            state.generation = token.generation;
            state.last_token = Some(token);
            state.frozen = None;
            state.importing = false;
            let tx = db.transaction().map_err(error)?;
            tx.execute_batch("DELETE FROM repair_records; DELETE FROM repair_pages;")
                .map_err(error)?;
            save_repair_state(&tx, &state)?;
            tx.commit().map_err(error)?;
            Ok(store_status(&disk_id, state))
        })
        .await
    }
}

fn store_status(disk_id: &str, state: RepairState) -> RepairStoreStatus {
    RepairStoreStatus {
        disk_id: disk_id.to_owned(),
        generation: state.generation,
        frozen: state.frozen,
        next_page: state.next_page,
        manifest: state.manifest,
        installed: state.installed,
    }
}
fn read_repair_state(db: &Connection) -> io::Result<RepairState> {
    let bytes: Option<Vec<u8>> = db
        .query_row("SELECT data FROM repair_state WHERE id=1", [], |row| {
            row.get(0)
        })
        .optional()
        .map_err(error)?;
    match bytes {
        Some(bytes) if bytes.len() <= 65536 => serde_json::from_slice(&bytes).map_err(error),
        Some(_) => Err(rejected()),
        None => Ok(RepairState::default()),
    }
}
fn save_repair_state(db: &Connection, state: &RepairState) -> io::Result<()> {
    db.execute("INSERT INTO repair_state(id,data) VALUES(1,?1) ON CONFLICT(id) DO UPDATE SET data=excluded.data", [serde_json::to_vec(state).map_err(error)?]).map_err(error)?;
    Ok(())
}
fn validate_token(token: &RepairToken) -> io::Result<()> {
    if token.generation == 0
        || token.id.is_empty()
        || token.id.len() > 128
        || !token.id.bytes().all(|b| (33..=126).contains(&b))
    {
        return Err(rejected());
    }
    Ok(())
}
fn validate_manifest(manifest: &RepairManifest) -> io::Result<()> {
    if manifest.records > MAX_ENTRIES
        || manifest.bytes > MAX_REPAIR_BYTES
        || manifest.digest.len() != 64
        || !manifest.digest.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(rejected());
    }
    Ok(())
}
fn validate_entry(entry: &RepairEntry) -> io::Result<Option<Vec<u8>>> {
    if entry.key.is_empty()
        || entry.key.len() > 65538
        || entry.min_epoch == 0
        || entry.min_epoch > i64::MAX as u64
    {
        return Err(rejected());
    }
    match &entry.record {
        None => Ok(None),
        Some(record) => {
            if record.epoch == 0
                || record.epoch > entry.min_epoch
                || record
                    .snapshot
                    .as_ref()
                    .is_some_and(|s| s.len() > 2 * 1024 * 1024)
                || record.intents.len() > 256
                || record.receipts.len() > 8192
                || record.filters.len() > 256
            {
                return Err(rejected());
            }
            let bytes = serde_json::to_vec(record).map_err(error)?;
            if bytes.len() > MAX_RECORD {
                return Err(rejected());
            }
            Ok(Some(bytes))
        }
    }
}
fn canonical_entry(entry: &RepairEntry) -> io::Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(entry).map_err(error)?;
    bytes.push(b'\n');
    Ok(bytes)
}
fn entry_from_row(row: &rusqlite::Row<'_>) -> io::Result<RepairEntry> {
    let length: Option<i64> = row.get(3).map_err(error)?;
    if length.is_some_and(|n| n < 0 || n as usize > MAX_RECORD) {
        return Err(rejected());
    }
    let bytes: Option<Vec<u8>> = row.get(2).map_err(error)?;
    let minimum: i64 = row.get(1).map_err(error)?;
    let entry = RepairEntry {
        key: row.get(0).map_err(error)?,
        min_epoch: u64::try_from(minimum).map_err(error)?,
        record: bytes
            .as_deref()
            .map(serde_json::from_slice)
            .transpose()
            .map_err(error)?,
    };
    validate_entry(&entry)?;
    Ok(entry)
}
fn scan_manifest(db: &Connection, table: &str) -> io::Result<RepairManifest> {
    let query = match table {
        "records" => {
            "SELECT key,min_epoch,data,length(data) FROM records ORDER BY key COLLATE BINARY"
        }
        "repair_records" => {
            "SELECT key,min_epoch,data,length(data) FROM repair_records ORDER BY key COLLATE BINARY"
        }
        _ => return Err(rejected()),
    };
    let mut statement = db.prepare(query).map_err(error)?;
    let mut rows = statement.query([]).map_err(error)?;
    let mut records = 0u64;
    let mut bytes = 0u64;
    let mut record_bytes = 0u64;
    let mut hash = Sha256::new();
    while let Some(row) = rows.next().map_err(error)? {
        let entry = entry_from_row(row)?;
        record_bytes = record_bytes
            .checked_add(validate_entry(&entry)?.as_ref().map_or(0, Vec::len) as u64)
            .ok_or_else(rejected)?;
        let frame = canonical_entry(&entry)?;
        records = records.checked_add(1).ok_or_else(rejected)?;
        bytes = bytes.checked_add(frame.len() as u64).ok_or_else(rejected)?;
        if records > MAX_ENTRIES || bytes > MAX_REPAIR_BYTES || record_bytes > MAX_BYTES as u64 {
            return Err(rejected());
        }
        hash.update(frame);
    }
    Ok(RepairManifest {
        records,
        bytes,
        digest: hash_string(hash),
    })
}
fn hash_string(hash: Sha256) -> String {
    hash.finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Compiled only into the unit-test executable, never into the Broker.
#[cfg(test)]
fn crash_during_install(point: &str) {
    if std::env::var("RMQTT_TEST_REPAIR_INSTALL_CRASH").as_deref() == Ok(point) {
        std::process::exit(if point == "before_commit" { 74 } else { 75 });
    }
}

fn execute_transaction(
    db: &mut Connection,
    generation: u64,
    request: Request,
) -> io::Result<Reply> {
    let tx = db.transaction().map_err(error)?;
    let repair = read_repair_state(&tx)?;
    if repair.frozen.is_some() || repair.generation != generation {
        return Err(rejected());
    }
    let (key, epoch) = match &request {
        Request::Read { key } => (key, 0),
        Request::Fence { key, epoch } => (key, *epoch),
        Request::Write { key, record } => (key, record.epoch),
    };
    if key.is_empty()
        || key.len() > 65538
        || (!matches!(&request, Request::Read { .. }) && epoch == 0)
        || epoch > i64::MAX as u64
    {
        return Err(rejected());
    }
    let old: Option<(i64, Option<Vec<u8>>)> = tx
        .query_row(
            "SELECT min_epoch,data FROM records WHERE key=?1",
            [key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(error)?;
    let (minimum, bytes) = old.unwrap_or((0, None));
    if !matches!(&request, Request::Read { .. }) && (epoch as i64) < minimum {
        return Err(rejected());
    }
    if bytes.as_ref().is_some_and(|b| b.len() > MAX_RECORD) {
        return Err(rejected());
    }
    let record: Option<Record> = bytes
        .as_deref()
        .map(serde_json::from_slice)
        .transpose()
        .map_err(error)?;
    if minimum == 0 && !matches!(&request, Request::Read { .. }) {
        let count: i64 = tx
            .query_row("SELECT count(*) FROM records", [], |r| r.get(0))
            .map_err(error)?;
        if count >= 100_000 {
            return Err(rejected());
        }
    }
    let answer = match &request {
        Request::Read { .. } => record,
        Request::Fence { .. } => {
            tx.execute("INSERT INTO records(key,min_epoch,data) VALUES(?1,?2,NULL) ON CONFLICT(key) DO UPDATE SET min_epoch=excluded.min_epoch",params![key,epoch as i64]).map_err(error)?;
            record
        }
        Request::Write { record: next, .. } => {
            if next
                .snapshot
                .as_ref()
                .is_some_and(|s| s.len() > 2 * 1024 * 1024)
                || next.intents.len() > 256
                || next.receipts.len() > 8192
                || next.filters.len() > 256
            {
                return Err(rejected());
            }
            let next_bytes = serde_json::to_vec(next).map_err(error)?;
            if next_bytes.len() > MAX_RECORD {
                return Err(rejected());
            }
            if let Some(old) = &record
                && (old.version() > next.version()
                    || (old.version() == next.version()
                        && serde_json::to_vec(old).map_err(error)? != next_bytes))
            {
                return Err(rejected());
            }
            let total: i64 = tx
                .query_row(
                    "SELECT coalesce(sum(length(data)),0) FROM records",
                    [],
                    |r| r.get(0),
                )
                .map_err(error)?;
            if total - (bytes.as_ref().map_or(0, Vec::len) as i64) + (next_bytes.len() as i64)
                > MAX_BYTES
            {
                return Err(rejected());
            }
            tx.execute("INSERT INTO records(key,min_epoch,data) VALUES(?1,?2,?3) ON CONFLICT(key) DO UPDATE SET min_epoch=excluded.min_epoch,data=excluded.data",params![key,epoch as i64,next_bytes]).map_err(error)?;
            None
        }
    };
    tx.commit().map_err(error)?;
    Ok(Reply { record: answer })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn repair_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "rmqtt-repair-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ))
    }
    fn test_token(generation: u64) -> RepairToken {
        RepairToken {
            id: format!("repair-{generation}"),
            generation,
        }
    }
    fn test_journal(path: &Path) -> io::Result<Journal> {
        Journal::open(
            path,
            ("repair-test", 1),
            &[(1, "http://127.0.0.1:9001".to_owned())],
        )
    }
    #[tokio::test]
    async fn repair_install_crash_probe() -> io::Result<()> {
        let Some(path) = std::env::var_os("RMQTT_TEST_REPAIR_INSTALL_PATH") else {
            return Ok(());
        };
        let journal = test_journal(Path::new(&path))?;
        journal.finish_import(test_token(1)).await?;
        Err(rejected())
    }
    #[tokio::test]
    async fn repair_install_process_crashes_leave_old_or_complete_new_set_frozen() -> io::Result<()>
    {
        let source_path = repair_path("crash-source");
        let target_path = repair_path("crash-target");
        let source = test_journal(&source_path)?;
        source
            .execute_at(
                0,
                Request::Fence {
                    key: "c:new".into(),
                    epoch: 7,
                },
            )
            .await?;
        let manifest = source
            .freeze(test_token(1))
            .await?
            .manifest
            .ok_or_else(rejected)?;
        let page = source.export_page(test_token(1), None, 64).await?;
        let target = test_journal(&target_path)?;
        target
            .execute_at(
                0,
                Request::Write {
                    key: "c:old".into(),
                    record: Record::empty(1),
                },
            )
            .await?;
        target.begin_import(test_token(1), manifest.clone()).await?;
        target.append_page(test_token(1), 0, page.entries).await?;
        drop(target);
        let binary = std::env::current_exe()?;
        for (point, exit_code, installed) in
            [("before_commit", 74, false), ("after_commit", 75, true)]
        {
            let binary = binary.clone();
            let path = target_path.clone();
            let output = tokio::task::spawn_blocking(move || {
                std::process::Command::new(binary)
                    .args([
                        "--exact",
                        "replica::tests::repair_install_crash_probe",
                        "--nocapture",
                    ])
                    .env("RMQTT_TEST_REPAIR_INSTALL_PATH", path)
                    .env("RMQTT_TEST_REPAIR_INSTALL_CRASH", point)
                    .output()
            })
            .await
            .map_err(error)??;
            assert_eq!(
                output.status.code(),
                Some(exit_code),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let target = test_journal(&target_path)?;
            let status = target.status().await?;
            assert_eq!(status.installed, installed);
            assert_eq!(status.frozen, Some(test_token(1)));
            assert!(!target.voting_ready(0));
            assert!(!target.voting_ready(1));
            let counts = target
                .with_db(|db| {
                    let old = db
                        .query_row("SELECT count(*) FROM records WHERE key='c:old'", [], |r| {
                            r.get::<_, i64>(0)
                        })
                        .map_err(error)?;
                    let new = db
                        .query_row("SELECT count(*) FROM records WHERE key='c:new'", [], |r| {
                            r.get::<_, i64>(0)
                        })
                        .map_err(error)?;
                    Ok((old, new))
                })
                .await?;
            assert_eq!(counts, if installed { (0, 1) } else { (1, 0) });
            drop(target);
        }
        let target = test_journal(&target_path)?;
        assert_eq!(target.status().await?.manifest, Some(manifest));
        target.resume(test_token(1), false).await?;
        assert!(
            target
                .execute_at(
                    1,
                    Request::Fence {
                        key: "c:new".into(),
                        epoch: 6
                    }
                )
                .await
                .is_err()
        );
        drop(source);
        drop(target);
        std::fs::remove_dir_all(source_path)?;
        std::fs::remove_dir_all(target_path)?;
        Ok(())
    }
    #[tokio::test]
    async fn repair_canonical_manifest_and_capacity_rejections_are_exact() -> io::Result<()> {
        let path = repair_path("canonical");
        let journal = test_journal(&path)?;
        journal
            .execute_at(
                0,
                Request::Fence {
                    key: "a".into(),
                    epoch: 1,
                },
            )
            .await?;
        let frozen = journal.freeze(test_token(1)).await?;
        let manifest = frozen.manifest.ok_or_else(rejected)?;
        assert_eq!(manifest.records, 1);
        assert_eq!(
            manifest.bytes,
            b"{\"key\":\"a\",\"min_epoch\":1,\"record\":null}\n".len() as u64
        );
        assert_eq!(
            manifest.digest,
            "f2f0f40c8422ecc465162c049bf6c476cf8e515f9f5fef9c3aa868d185f399ea"
        );
        assert!(journal.export_page(test_token(1), None, 0).await.is_err());
        let target_path = repair_path("capacity");
        let target = test_journal(&target_path)?;
        assert!(
            target
                .begin_import(
                    test_token(1),
                    RepairManifest {
                        records: MAX_ENTRIES + 1,
                        ..manifest.clone()
                    }
                )
                .await
                .is_err()
        );
        assert!(
            target
                .begin_import(
                    test_token(1),
                    RepairManifest {
                        bytes: MAX_REPAIR_BYTES + 1,
                        ..manifest.clone()
                    }
                )
                .await
                .is_err()
        );
        target.begin_import(test_token(1), manifest).await?;
        assert!(
            target
                .append_page(test_token(1), 0, Vec::new())
                .await
                .is_err()
        );
        let invalid = RepairEntry {
            key: "a".into(),
            min_epoch: 1,
            record: Some(Record::empty(2)),
        };
        assert!(
            target
                .append_page(test_token(1), 0, vec![invalid])
                .await
                .is_err()
        );
        assert_eq!(target.status().await?.next_page, 0);
        assert!(!target.voting_ready(0));
        drop(journal);
        drop(target);
        std::fs::remove_dir_all(path)?;
        std::fs::remove_dir_all(target_path)?;
        Ok(())
    }
    #[tokio::test]
    async fn repair_export_accepts_near_eight_mib_record_with_maximum_key() -> io::Result<()> {
        let path = repair_path("large");
        let journal = test_journal(&path)?;
        let key = "k".repeat(65538);
        let mut record = Record::empty(1);
        record.snapshot = Some(vec![255; (MAX_RECORD - 4096) / 4]);
        let length = serde_json::to_vec(&record).map_err(error)?.len();
        assert!(length < MAX_RECORD && length > 7 * 1024 * 1024);
        journal
            .execute_at(
                0,
                Request::Write {
                    key: key.clone(),
                    record,
                },
            )
            .await?;
        journal.freeze(test_token(1)).await?;
        let page = journal.export_page(test_token(1), None, 64).await?;
        assert!(page.done);
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].key, key);
        assert!(canonical_entry(&page.entries[0])?.len() < MAX_PAGE_BYTES);
        drop(journal);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }
    #[tokio::test]
    async fn repair_pages_install_and_restart_preserve_records_fences_and_generation()
    -> io::Result<()> {
        let source_path = repair_path("source");
        let target_path = repair_path("target");
        let source = test_journal(&source_path)?;
        let mut record = Record::empty(1);
        record.snapshot = Some(vec![1, 2, 3]);
        record.logical_generation = 4;
        record.receipts.insert(DeliveryId {
            source: "c:origin".into(),
            epoch: 1,
            revision: 2,
            index: 0,
            target_generation: 4,
        });
        source
            .execute_at(
                0,
                Request::Write {
                    key: "c:data".into(),
                    record,
                },
            )
            .await?;
        source
            .execute_at(
                0,
                Request::Fence {
                    key: "c:data".into(),
                    epoch: 9,
                },
            )
            .await?;
        source
            .execute_at(
                0,
                Request::Fence {
                    key: "c:fence".into(),
                    epoch: 10,
                },
            )
            .await?;
        let frozen = source.freeze(test_token(1)).await?;
        let manifest = frozen.manifest.ok_or_else(rejected)?;
        assert_eq!(manifest.records, 2);
        assert!(!source.voting_ready(0));
        assert!(
            source
                .execute_at(
                    0,
                    Request::Read {
                        key: "c:data".into()
                    }
                )
                .await
                .is_err()
        );
        let first = source.export_page(test_token(1), None, 1).await?;
        assert!(!first.done);
        assert_eq!(first.entries[0].min_epoch, 9);
        let second = source
            .export_page(test_token(1), first.next.clone(), 1)
            .await?;
        assert!(second.done);
        assert_eq!(second.entries[0].key, "c:fence");
        assert_eq!(second.entries[0].min_epoch, 10);
        assert!(second.entries[0].record.is_none());
        let target = test_journal(&target_path)?;
        target.begin_import(test_token(1), manifest.clone()).await?;
        assert!(!target.voting_ready(0));
        let page = target
            .append_page(test_token(1), 0, first.entries.clone())
            .await?;
        assert_eq!(page.next_page, 1);
        assert_eq!(
            target
                .append_page(test_token(1), 0, first.entries.clone())
                .await?
                .next_page,
            1
        );
        let mut changed = first.entries.clone();
        changed[0].min_epoch += 1;
        assert!(target.append_page(test_token(1), 0, changed).await.is_err());
        assert!(
            target
                .append_page(test_token(1), 2, second.entries.clone())
                .await
                .is_err()
        );
        assert!(target.finish_import(test_token(1)).await.is_err());
        drop(target);
        let target = test_journal(&target_path)?;
        assert_eq!(target.status().await?.next_page, 1);
        assert!(!target.voting_ready(0));
        target.append_page(test_token(1), 1, second.entries).await?;
        let installed = target.finish_import(test_token(1)).await?;
        assert!(installed.installed);
        assert_eq!(installed.manifest, Some(manifest));
        assert!(!target.voting_ready(1));
        drop(target);
        let target = test_journal(&target_path)?;
        assert!(target.status().await?.installed);
        assert!(
            target
                .execute_at(
                    1,
                    Request::Read {
                        key: "c:data".into()
                    }
                )
                .await
                .is_err()
        );
        target.resume(test_token(1), false).await?;
        assert!(target.voting_ready(1));
        let data = target
            .execute_at(
                1,
                Request::Read {
                    key: "c:data".into(),
                },
            )
            .await?
            .record
            .ok_or_else(rejected)?;
        assert_eq!(data.snapshot, Some(vec![1, 2, 3]));
        assert_eq!(data.logical_generation, 4);
        assert!(data.receipts.contains(&DeliveryId {
            source: "c:origin".into(),
            epoch: 1,
            revision: 2,
            index: 0,
            target_generation: 4
        }));
        assert!(
            target
                .execute_at(
                    0,
                    Request::Fence {
                        key: "c:data".into(),
                        epoch: 99
                    }
                )
                .await
                .is_err()
        );
        assert!(
            target
                .execute_at(
                    1,
                    Request::Fence {
                        key: "c:data".into(),
                        epoch: 8
                    }
                )
                .await
                .is_err()
        );
        drop(target);
        source.resume(test_token(1), false).await?;
        drop(source);
        std::fs::remove_dir_all(source_path)?;
        std::fs::remove_dir_all(target_path)?;
        Ok(())
    }
    #[tokio::test]
    async fn repair_rejects_bad_tokens_and_digest_without_exposing_staging() -> io::Result<()> {
        let source_path = repair_path("bad-source");
        let target_path = repair_path("bad-target");
        let source = test_journal(&source_path)?;
        source
            .execute_at(
                0,
                Request::Fence {
                    key: "c:new".into(),
                    epoch: 4,
                },
            )
            .await?;
        let manifest = source
            .freeze(test_token(1))
            .await?
            .manifest
            .ok_or_else(rejected)?;
        let page = source.export_page(test_token(1), None, 64).await?;
        let target = test_journal(&target_path)?;
        target
            .execute_at(
                0,
                Request::Write {
                    key: "c:old".into(),
                    record: Record::empty(2),
                },
            )
            .await?;
        let wrong = RepairManifest {
            digest: "0".repeat(64),
            ..manifest.clone()
        };
        target.begin_import(test_token(1), wrong).await?;
        assert!(
            target
                .append_page(test_token(2), 0, page.entries.clone())
                .await
                .is_err()
        );
        target
            .append_page(test_token(1), 0, page.entries.clone())
            .await?;
        assert!(target.finish_import(test_token(1)).await.is_err());
        let old_count = target
            .with_db(|db| {
                db.query_row(
                    "SELECT count(*) FROM records WHERE key='c:old'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(error)
            })
            .await?;
        assert_eq!(old_count, 1);
        assert!(!target.voting_ready(0));
        target.begin_import(test_token(2), manifest).await?;
        target.append_page(test_token(2), 0, page.entries).await?;
        target.finish_import(test_token(2)).await?;
        assert!(target.resume(test_token(1), false).await.is_err());
        target.resume(test_token(2), false).await?;
        assert!(
            target
                .execute_at(
                    2,
                    Request::Read {
                        key: "c:old".into()
                    }
                )
                .await?
                .record
                .is_none()
        );
        assert!(
            target
                .execute_at(
                    2,
                    Request::Fence {
                        key: "c:new".into(),
                        epoch: 3
                    }
                )
                .await
                .is_err()
        );
        drop(source);
        drop(target);
        std::fs::remove_dir_all(source_path)?;
        std::fs::remove_dir_all(target_path)?;
        Ok(())
    }
    #[tokio::test]
    async fn repair_resume_requires_exact_token_or_explicit_unfrozen_abort_authorization()
    -> io::Result<()> {
        let path = repair_path("resume");
        let journal = test_journal(&path)?;
        assert!(journal.resume(test_token(1), false).await.is_err());
        journal.resume(test_token(1), true).await?;
        assert!(journal.voting_ready(1));
        journal.resume(test_token(1), false).await?;
        assert!(
            journal
                .resume(
                    RepairToken {
                        id: "other".into(),
                        generation: 1
                    },
                    true
                )
                .await
                .is_err()
        );
        assert!(journal.freeze(test_token(1)).await.is_err());
        journal.freeze(test_token(2)).await?;
        assert!(journal.resume(test_token(1), true).await.is_err());
        assert!(journal.export_page(test_token(2), None, 65).await.is_err());
        journal.resume(test_token(2), false).await?;
        journal.freeze(test_token(3)).await?;
        journal.freeze(test_token(4)).await?;
        assert!(journal.resume(test_token(3), true).await.is_err());
        journal.resume(test_token(5), true).await?;
        assert!(journal.voting_ready(5));
        assert!(
            journal
                .resume(
                    RepairToken {
                        id: "wrong".into(),
                        generation: 5
                    },
                    true
                )
                .await
                .is_err()
        );
        drop(journal);
        let journal = test_journal(&path)?;
        assert!(journal.voting_ready(5));
        assert!(
            journal
                .execute_at(1, Request::Read { key: "any".into() })
                .await
                .is_err()
        );
        drop(journal);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }
    #[tokio::test]
    async fn fence_returns_prior_write_and_rejects_stale_or_conflicting_records() -> io::Result<()>
    {
        let path = std::env::temp_dir().join(format!("rmqtt-data-replica-{}", std::process::id()));
        let members = vec![(1, "http://127.0.0.1:7001".into())];
        let journal = Journal::open(&path, ("test", 1), &members)?;
        let mut record = Record::empty(1);
        record.physical_revision = 1;
        journal
            .execute(Request::Write {
                key: "c:device".into(),
                record: record.clone(),
            })
            .await?;
        journal
            .execute(Request::Write {
                key: "c:device".into(),
                record: record.clone(),
            })
            .await?;
        let answer = journal
            .execute(Request::Fence {
                key: "c:device".into(),
                epoch: 2,
            })
            .await?;
        assert_eq!(answer.record.map(|r| r.version()), Some((1, 1)));
        assert!(
            journal
                .execute(Request::Write {
                    key: "c:device".into(),
                    record: record.clone()
                })
                .await
                .is_err()
        );
        record.epoch = 2;
        record.physical_revision = 0;
        journal
            .execute(Request::Write {
                key: "c:device".into(),
                record: record.clone(),
            })
            .await?;
        record.revision = 2;
        assert!(
            journal
                .execute(Request::Write {
                    key: "c:device".into(),
                    record
                })
                .await
                .is_err()
        );
        drop(journal);
        let journal = Journal::open(&path, ("test", 1), &members)?;
        assert!(
            journal
                .execute(Request::Fence {
                    key: "c:device".into(),
                    epoch: 1
                })
                .await
                .is_err()
        );
        drop(journal);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }
}

#[cfg(test)]
mod watermark_tests {
    use super::*;
    fn id(source: String, epoch: u64, revision: u64) -> DeliveryId {
        DeliveryId {
            source,
            epoch,
            revision,
            index: 0,
            target_generation: 0,
        }
    }
    #[test]
    fn sequential_messages_and_legacy_receipts_use_bounded_source_watermarks() {
        let mut receipts = Receipts::default();
        for revision in 1..=20_000 {
            receipts.insert(id("c:source".into(), 1, revision));
        }
        assert_eq!(receipts.len(), 1);
        assert!(receipts.contains(&id("c:source".into(), 1, 1)));
        assert!(!receipts.contains(&id("c:source".into(), 1, 20_001)));
        receipts.insert(id("c:source".into(), 2, 1));
        assert!(receipts.contains(&id("c:source".into(), 1, u64::MAX)));
        let old = vec![
            id("c:source".into(), 1, 1),
            id("c:source".into(), 1, 10),
            id("c:other".into(), 1, 1),
        ];
        let restored: Receipts =
            serde_json::from_slice(&serde_json::to_vec(&old).expect("old receipts"))
                .expect("restore");
        assert_eq!(restored.len(), 2);
        assert!(restored.contains(&id("c:source".into(), 1, 9)));
        assert!(!restored.contains(&id("c:source".into(), 1, 11)));
    }
    #[test]
    fn disk_identity_is_stable_and_empty_replacement_gets_a_new_identity() -> io::Result<()> {
        let path = std::env::temp_dir().join(format!("rmqtt-disk-identity-{}", std::process::id()));
        let first = Journal::open(&path, ("disk", 1), &[])?;
        let id = first.disk_id().to_owned();
        drop(first);
        let reopened = Journal::open(&path, ("disk", 1), &[])?;
        assert_eq!(reopened.disk_id(), id);
        drop(reopened);
        std::fs::remove_dir_all(&path)?;
        let empty = Journal::open(&path, ("disk", 1), &[])?;
        assert_ne!(empty.disk_id(), id);
        drop(empty);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }
    #[tokio::test]
    async fn full_source_quota_fails_closed_and_preserves_receipts_after_restart() -> io::Result<()>
    {
        let path = std::env::temp_dir().join(format!("rmqtt-receipt-quota-{}", std::process::id()));
        let journal = Journal::open(&path, ("quota", 1), &[])?;
        let mut record = Record::empty(1);
        record.physical_revision = 1;
        for source in 0..8192 {
            record.receipts.insert(id(format!("c:{source}"), 1, 1));
        }
        journal
            .execute(Request::Write {
                key: "c:recipient".into(),
                record: record.clone(),
            })
            .await?;
        record.physical_revision += 1;
        record.receipts.insert(id("c:overflow".into(), 1, 1));
        assert!(
            journal
                .execute(Request::Write {
                    key: "c:recipient".into(),
                    record
                })
                .await
                .is_err()
        );
        drop(journal);
        let journal = Journal::open(&path, ("quota", 1), &[])?;
        let recovered = journal
            .execute(Request::Read {
                key: "c:recipient".into(),
            })
            .await?
            .record
            .ok_or_else(rejected)?;
        assert_eq!(recovered.physical_revision, 1);
        assert_eq!(recovered.receipts.len(), 8192);
        assert!(recovered.receipts.contains(&id("c:0".into(), 1, 1)));
        assert!(recovered.receipts.contains(&id("c:8191".into(), 1, 1)));
        drop(journal);
        std::fs::remove_dir_all(path)?;
        Ok(())
    }
}
