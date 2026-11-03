//! Bounded durable data-replica register. Fence and its returned value are atomic.
use mqtt_cluster_core::{ClusterMessage, DeliveryId};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::Path,
    sync::{Arc, Mutex, PoisonError},
};
use tokio::sync::Semaphore;

pub(crate) const MAX_RECORD: usize = 8 * 1024 * 1024;
const MAX_BYTES: i64 = 256 * 1024 * 1024;
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
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Intent {
    pub id: DeliveryId,
    pub message: ClusterMessage,
    pub recipients: Vec<String>,
    pub completed: BTreeSet<String>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Record {
    pub epoch: u64,
    pub physical_revision: u64,
    pub revision: u64,
    pub snapshot: Option<Vec<u8>>,
    pub filters: Vec<String>,
    pub intents: Vec<Intent>,
    pub receipts: Receipts,
}
impl Record {
    pub fn version(&self) -> (u64, u64) {
        (self.epoch, self.physical_revision)
    }
    pub fn empty(epoch: u64) -> Self {
        Self {
            epoch,
            physical_revision: 0,
            revision: 0,
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
        db.pragma_update(None, "max_page_count", 65536)
            .map_err(error)?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS identity(id INTEGER PRIMARY KEY CHECK(id=1), data BLOB NOT NULL); CREATE TABLE IF NOT EXISTS records(key TEXT PRIMARY KEY,min_epoch INTEGER NOT NULL,data BLOB);").map_err(error)?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS disk_identity(id INTEGER PRIMARY KEY CHECK(id=1), disk_id TEXT NOT NULL);").map_err(error)?;
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
        Ok(Self(Arc::new(Inner {
            db: Mutex::new(db),
            _lock: lock,
            disk_id,
            slots: Arc::new(Semaphore::new(32)),
        })))
    }
    pub fn disk_id(&self) -> &str {
        &self.0.disk_id
    }
    pub async fn execute(&self, request: Request) -> io::Result<Reply> {
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
            execute_transaction(&mut db, request)
        })
        .await
        .map_err(error)?
    }
}

fn execute_transaction(db: &mut Connection, request: Request) -> io::Result<Reply> {
    let tx = db.transaction().map_err(error)?;
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
