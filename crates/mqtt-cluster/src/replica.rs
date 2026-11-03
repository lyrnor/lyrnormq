//! Bounded durable data-replica register. Fence and its returned value are atomic.
use mqtt_cluster_core::{ClusterMessage, DeliveryId};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
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
    pub receipts: BTreeSet<DeliveryId>,
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
            receipts: BTreeSet::new(),
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
            slots: Arc::new(Semaphore::new(32)),
        })))
    }
    pub async fn execute(&self, request: Request) -> io::Result<Reply> {
        let permit = self
            .0
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| rejected())?;
        let inner = self.0.clone();
        tokio::task::spawn_blocking(move|| {
            let _permit=permit;
            let mut db=inner.db.lock().unwrap_or_else(PoisonError::into_inner);
            let tx=db.transaction().map_err(error)?;
            let (key,epoch)=match &request { Request::Read{key}=>(key,0), Request::Fence{key,epoch}=>(key,*epoch),Request::Write{key,record}=>(key,record.epoch) };
            if key.is_empty()||key.len()>65538||(!matches!(&request,Request::Read{..})&&epoch==0)||epoch>i64::MAX as u64 {return Err(rejected());}
            let old:Option<(i64,Option<Vec<u8>>)>=tx.query_row("SELECT min_epoch,data FROM records WHERE key=?1",[key],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(error)?;
            let (minimum,bytes)=old.unwrap_or((0,None));
            if !matches!(&request,Request::Read{..}) && (epoch as i64)<minimum {return Err(rejected());}
            let record:Option<Record>=bytes.as_deref().map(serde_json::from_slice).transpose().map_err(error)?;
            if minimum==0 && !matches!(&request,Request::Read{..}) {
                let count:i64=tx.query_row("SELECT count(*) FROM records",[],|r|r.get(0)).map_err(error)?;
                if count>=100_000 {return Err(rejected());}
            }
            let answer=match &request {
                Request::Read{..}=>record,
                Request::Fence{..}=>{
                    tx.execute("INSERT INTO records(key,min_epoch,data) VALUES(?1,?2,NULL) ON CONFLICT(key) DO UPDATE SET min_epoch=excluded.min_epoch",params![key,epoch as i64]).map_err(error)?;
                    record
                },
                Request::Write{record:next,..}=>{
                    if next.snapshot.as_ref().is_some_and(|s|s.len()>2*1024*1024)||next.intents.len()>256||next.receipts.len()>8192||next.filters.len()>256 {return Err(rejected());}
                    let next_bytes=serde_json::to_vec(next).map_err(error)?;
                    if next_bytes.len()>MAX_RECORD {return Err(rejected());}
                    if let Some(old)=&record
                        && (old.version()>next.version() || (old.version()==next.version() && bytes.as_ref()!=Some(&next_bytes))) {return Err(rejected());}
                    let total:i64=tx.query_row("SELECT coalesce(sum(length(data)),0) FROM records",[],|r|r.get(0)).map_err(error)?;
                    if total-(bytes.as_ref().map_or(0,Vec::len) as i64)+(next_bytes.len() as i64)>MAX_BYTES {return Err(rejected());}
                    tx.execute("INSERT INTO records(key,min_epoch,data) VALUES(?1,?2,?3) ON CONFLICT(key) DO UPDATE SET min_epoch=excluded.min_epoch,data=excluded.data",params![key,epoch as i64,next_bytes]).map_err(error)?;
                    None
                }
            };
            tx.commit().map_err(error)?;
            Ok(Reply{record:answer})
        }).await.map_err(error)?
    }
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
