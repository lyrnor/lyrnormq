//! SQLite WAL/FULL durable Raft log, vote, commit and state-machine snapshots.
use crate::model::*;
use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine};
use openraft::{
    BasicNode, Entry, LogId, RaftLogReader, RaftSnapshotBuilder, Snapshot, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    io::{self, Cursor},
    ops::{Bound, RangeBounds},
    path::Path,
    sync::{Arc, Mutex, PoisonError},
};
use tokio::sync::{Semaphore, watch};
#[derive(Clone)]
pub struct Store {
    inner: Arc<Inner>,
    pub state: Arc<Mutex<State>>,
    pub changed: watch::Sender<u64>,
}
struct Inner {
    db: Mutex<Connection>,
    _lock: std::fs::File,
    slots: Arc<Semaphore>,
}
fn err(error: impl std::fmt::Display) -> StorageError<u64> {
    StorageIOError::write_state_machine(&io::Error::other(error.to_string())).into()
}
fn db_err(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}
fn encode<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(db_err)
}
fn decode<T: DeserializeOwned>(bytes: &[u8]) -> io::Result<T> {
    serde_json::from_slice(bytes).map_err(db_err)
}
fn get<T: DeserializeOwned>(db: &Connection, key: &str) -> io::Result<Option<T>> {
    let bytes: Option<Vec<u8>> = db
        .query_row("SELECT data FROM meta WHERE key=?1", [key], |r| r.get(0))
        .optional()
        .map_err(db_err)?;
    bytes.as_deref().map(decode).transpose()
}
fn put<T: Serialize>(db: &Connection, key: &str, value: &T) -> io::Result<()> {
    db.execute("INSERT INTO meta(key,data) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET data=excluded.data",params![key,encode(value)?]).map_err(db_err)?;
    Ok(())
}
impl Store {
    pub async fn open(path: &Path, cluster: &str, node: u64) -> io::Result<Self> {
        let path = path.to_path_buf();
        let cluster = cluster.to_owned();
        let (db, lock, state) = tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&path)?;
            let lock = std::fs::OpenOptions::new()
                .create(true).truncate(false).read(true).write(true)
                .open(path.join("controller.lock"))?;
            lock.try_lock().map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "controller data directory is already in use"))?;
            let db = Connection::open(path.join("metadata.sqlite")).map_err(db_err)?;
            db.pragma_update(None, "journal_mode", "WAL").map_err(db_err)?;
            db.pragma_update(None, "synchronous", "FULL").map_err(db_err)?;
            db.execute_batch("CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,data BLOB NOT NULL); CREATE TABLE IF NOT EXISTS logs(idx INTEGER PRIMARY KEY,data BLOB NOT NULL);").map_err(db_err)?;
            let identity = (cluster, node);
            match get::<(String, u64)>(&db, "identity")? {
                Some(old) if old != identity => return Err(io::Error::new(io::ErrorKind::InvalidInput, "controller data identity mismatch")),
                Some(_) => {},
                None => put(&db, "identity", &identity)?,
            }
            let state = get::<State>(&db, "state")?.unwrap_or_default();
            Ok::<_, io::Error>((db, lock, state))
        }).await.map_err(db_err)??;
        let (changed, _) = watch::channel(state.metadata.revision);
        Ok(Self {
            inner: Arc::new(Inner {
                db: Mutex::new(db),
                _lock: lock,
                slots: Arc::new(Semaphore::new(32)),
            }),
            state: Arc::new(Mutex::new(state)),
            changed,
        })
    }
    #[allow(clippy::result_large_err)] // Required by the OpenRaft storage trait.
    async fn run<T: Send + 'static>(
        &self,
        call: impl FnOnce(&mut Connection) -> io::Result<T> + Send + 'static,
    ) -> Result<T, StorageError<u64>> {
        let permit = self
            .inner
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(err)?;
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            call(&mut inner.db.lock().unwrap_or_else(PoisonError::into_inner))
        })
        .await
        .map_err(err)?
        .map_err(err)
    }
    pub fn metadata(&self) -> Metadata {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .metadata
            .clone()
    }
}
impl RaftLogReader<Types> for Store {
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + Send>(
        &mut self,
        range: R,
    ) -> Result<Vec<Entry<Types>>, StorageError<u64>> {
        if matches!(range.end_bound(), Bound::Excluded(0))
            || matches!(range.start_bound(), Bound::Excluded(n) if *n == u64::MAX)
        {
            return Ok(Vec::new());
        }
        let from = match range.start_bound() {
            Bound::Included(n) => *n,
            Bound::Excluded(n) => n.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let to = match range.end_bound() {
            Bound::Included(n) => *n,
            Bound::Excluded(n) => n.saturating_sub(1),
            Bound::Unbounded => i64::MAX as u64,
        };
        self.run(move |db| {
            let mut stmt = db
                .prepare("SELECT data FROM logs WHERE idx>=?1 AND idx<=?2 ORDER BY idx")
                .map_err(db_err)?;
            let rows = stmt
                .query_map(
                    params![
                        from.min(i64::MAX as u64) as i64,
                        to.min(i64::MAX as u64) as i64
                    ],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .map_err(db_err)?;
            let mut result = Vec::new();
            for row in rows {
                result.push(decode(&row.map_err(db_err)?)?);
            }
            Ok(result)
        })
        .await
    }
}
impl RaftLogStorage<Types> for Store {
    type LogReader = Self;
    async fn get_log_state(&mut self) -> Result<LogState<Types>, StorageError<u64>> {
        self.run(|db| {
            let purged = get(db, "purged")?;
            let last: Option<Vec<u8>> = db
                .query_row("SELECT data FROM logs ORDER BY idx DESC LIMIT 1", [], |r| {
                    r.get(0)
                })
                .optional()
                .map_err(db_err)?;
            let last = last
                .as_deref()
                .map(decode::<Entry<Types>>)
                .transpose()?
                .map(|e| e.log_id)
                .or(purged);
            Ok(LogState {
                last_purged_log_id: purged,
                last_log_id: last,
            })
        })
        .await
    }
    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }
    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let vote = *vote;
        self.run(move |db| put(db, "vote", &vote)).await
    }
    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        self.run(|db| get(db, "vote")).await
    }
    async fn save_committed(&mut self, id: Option<LogId<u64>>) -> Result<(), StorageError<u64>> {
        self.run(move |db| put(db, "committed", &id)).await
    }
    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        self.run(|db| Ok(get::<Option<LogId<u64>>>(db, "committed")?.flatten()))
            .await
    }
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<Types>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<Types>> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        self.run(move |db| {
            let tx = db.transaction().map_err(db_err)?;
            for entry in entries {
                let idx = i64::try_from(entry.log_id.index).map_err(db_err)?;
                tx.execute("INSERT INTO logs(idx,data) VALUES(?1,?2) ON CONFLICT(idx) DO UPDATE SET data=excluded.data", params![idx, encode(&entry)?]).map_err(db_err)?;
            }
            tx.commit().map_err(db_err)
        }).await?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }
    async fn truncate(&mut self, id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.run(move |db| {
            db.execute(
                "DELETE FROM logs WHERE idx>=?1",
                [id.index.min(i64::MAX as u64) as i64],
            )
            .map_err(db_err)?;
            Ok(())
        })
        .await
    }
    async fn purge(&mut self, id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.run(move |db| {
            let tx = db.transaction().map_err(db_err)?;
            put(&tx, "purged", &id)?;
            tx.execute(
                "DELETE FROM logs WHERE idx<=?1",
                [id.index.min(i64::MAX as u64) as i64],
            )
            .map_err(db_err)?;
            tx.commit().map_err(db_err)
        })
        .await
    }
}
#[derive(Serialize, serde::Deserialize)]
struct SavedSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}
impl RaftSnapshotBuilder<Types> for Store {
    async fn build_snapshot(&mut self) -> Result<Snapshot<Types>, StorageError<u64>> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let data = encode(&state).map_err(err)?;
        let meta = SnapshotMeta {
            last_log_id: state.last_applied,
            last_membership: state.membership,
            snapshot_id: format!(
                "{}-{}",
                state.last_applied.map_or(0, |id| id.index),
                state.metadata.revision
            ),
        };
        let saved = SavedSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        self.run(move |db| {
            let previous: Option<SavedSnapshot> = get(db, "snapshot")?;
            if previous
                .as_ref()
                .is_none_or(|old| old.meta.last_log_id <= saved.meta.last_log_id)
            {
                put(db, "snapshot", &saved)?;
            }
            Ok(())
        })
        .await?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}
impl RaftStateMachine<Types> for Store {
    type SnapshotBuilder = Self;
    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        Ok((state.last_applied, state.membership.clone()))
    }
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Answer>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<Types>> + Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let state = self.state.clone();
        let changed = self.changed.clone();
        self.run(move |db| {
            let mut next = state.lock().unwrap_or_else(PoisonError::into_inner).clone();
            let old_revision = next.metadata.revision;
            let result = entries.into_iter().map(|entry| next.apply(entry)).collect();
            put(db, "state", &next)?;
            let revision = next.metadata.revision;
            *state.lock().unwrap_or_else(PoisonError::into_inner) = next;
            if revision != old_revision {
                changed.send_replace(revision);
            }
            Ok(result)
        })
        .await
    }
    async fn get_snapshot_builder(&mut self) -> Self {
        self.clone()
    }
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();
        if data.len() > 16 * 1024 * 1024 {
            return Err(err("snapshot exceeds limit"));
        }
        let mut next: State = decode(&data).map_err(err)?;
        next.last_applied = meta.last_log_id;
        next.membership = meta.last_membership.clone();
        let snapshot = SavedSnapshot {
            meta: meta.clone(),
            data,
        };
        let state = self.state.clone();
        let changed = self.changed.clone();
        self.run(move |db| {
            let tx = db.transaction().map_err(db_err)?;
            put(&tx, "snapshot", &snapshot)?;
            put(&tx, "state", &next)?;
            tx.commit().map_err(db_err)?;
            let revision = next.metadata.revision;
            *state.lock().unwrap_or_else(PoisonError::into_inner) = next;
            changed.send_replace(revision);
            Ok(())
        })
        .await
    }
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<Types>>, StorageError<u64>> {
        self.run(|db| {
            let saved: Option<SavedSnapshot> = get(db, "snapshot")?;
            Ok(saved.map(|s| Snapshot {
                meta: s.meta,
                snapshot: Box::new(Cursor::new(s.data)),
            }))
        })
        .await
    }
}
