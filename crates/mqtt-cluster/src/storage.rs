//! Independent durable Raft WAL with transactional bbolt business checkpoints.
use crate::model::*;
use crate::profile::{Metric, Timer as ProfileTimer};
use futures_util::Stream;
use openraft::storage::{EntryResponder, IOFlushed, LogState, RaftLogStorage, RaftStateMachine};
use openraft::type_config::TypeConfigExt;
use openraft::{RaftLogReader, RaftSnapshotBuilder, StorageError};
use openraft_legacy::network_v1::SnapshotReceiverFactory;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    io::{self, Cursor},
    ops::{Bound, RangeBounds},
    path::Path,
    sync::{Arc, Mutex, PoisonError},
};
use tokio::sync::{Semaphore, watch};
use tokio_stream::StreamExt;
mod checkpoint;
mod diagnostics;
mod flush;
mod history;
mod log;
mod recovery;
mod wal;
#[derive(Clone)]
pub struct Store {
    node: u64,
    /// Declared before `inner` so dropping the last store joins the flush thread
    /// before the checkpoint file is closed.
    owner: Arc<flush::Owner>,
    inner: Arc<Inner>,
    pub state: Arc<Mutex<State>>,
    pub changed: watch::Sender<u64>,
    history: Arc<Mutex<history::History>>,
    apply_gate: Arc<Semaphore>,
}
struct Inner {
    db: Mutex<Option<checkpoint::Checkpoint>>,
    wal: Mutex<Option<wal::Wal>>,
    _lock: Option<std::fs::File>,
    slots: Arc<Semaphore>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        let db = self
            .db
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let wal = self
            .wal
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let lock = self._lock.take();
        // Exactly one cleanup per controller store, never per request. Keep the
        // directory lease until the native WAL worker has flushed and joined.
        let close = move || {
            drop(wal);
            drop(db);
            drop(lock);
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                drop(runtime.spawn_blocking(close));
            }
            Err(_) => close(),
        }
    }
}

fn err(error: impl std::fmt::Display) -> StorageError<Types> {
    let io_error = io::Error::other(error.to_string());
    StorageError::write_state_machine(Types::err_from_error(&io_error))
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
impl Store {
    pub async fn open(path: &Path, cluster: &str, node: u64) -> io::Result<Self> {
        let path = path.to_path_buf();
        let cluster = cluster.to_owned();
        let (db, wal, lock, state) = tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            let _process_start = crate::replica::TEST_PROCESS_START_LOCK
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            std::fs::create_dir_all(&path)?;
            let lock = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(path.join("controller.lock"))?;
            let lock_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                match lock.try_lock() {
                    Ok(()) => break,
                    Err(std::fs::TryLockError::WouldBlock)
                        if std::time::Instant::now() < lock_deadline =>
                    {
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                    Err(std::fs::TryLockError::WouldBlock) => {
                        return Err(io::Error::new(
                            io::ErrorKind::WouldBlock,
                            "controller data directory is already in use",
                        ));
                    }
                    Err(std::fs::TryLockError::Error(error)) => return Err(error),
                }
            }
            let (db, wal, state) = recovery::open_stores(&path, &cluster, node)?;
            Ok::<_, io::Error>((db, wal, lock, state))
        })
        .await
        .map_err(db_err)??;
        let (changed, _) = watch::channel(state.metadata.revision);
        let state = Arc::new(Mutex::new(state));
        let inner = Arc::new(Inner {
            db: Mutex::new(Some(db)),
            wal: Mutex::new(Some(wal)),
            _lock: Some(lock),
            slots: Arc::new(Semaphore::new(32)),
        });
        let owner = flush::start(Arc::downgrade(&inner), Arc::clone(&state))?;
        Ok(Self {
            node,
            owner,
            inner,
            state,
            changed,
            history: Arc::new(Mutex::new(history::History::default())),
            apply_gate: Arc::new(Semaphore::new(1)),
        })
    }
    #[allow(clippy::result_large_err)] // Required by the OpenRaft storage trait.
    async fn run<T: Send + 'static>(
        &self,
        call: impl FnOnce(&mut checkpoint::Checkpoint) -> io::Result<T> + Send + 'static,
    ) -> Result<T, StorageError<Types>> {
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
            let mut db = inner.db.lock().unwrap_or_else(PoisonError::into_inner);
            call(
                db.as_mut()
                    .ok_or_else(|| db_err("checkpoint store is closed"))?,
            )
        })
        .await
        .map_err(err)?
        .map_err(err)
    }
    /// Raft operations never acquire the bbolt checkpoint mutex.
    #[allow(clippy::result_large_err)]
    async fn run_wal<T: Send + 'static>(
        &self,
        call: impl FnOnce(&mut wal::Wal) -> io::Result<T> + Send + 'static,
    ) -> Result<T, StorageError<Types>> {
        let slot_wait = ProfileTimer::start(Metric::WalSlotWait);
        let permit = self
            .inner
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(err)?;
        drop(slot_wait);
        let inner = self.inner.clone();
        let spawn_wait = ProfileTimer::start(Metric::WalSpawnWait);
        tokio::task::spawn_blocking(move || {
            drop(spawn_wait);
            let _permit = permit;
            let lock_wait = ProfileTimer::start(Metric::WalMutexWait);
            let mut wal = inner.wal.lock().unwrap_or_else(PoisonError::into_inner);
            drop(lock_wait);
            let _work = ProfileTimer::start(Metric::WalWork);
            call(wal.as_mut().ok_or_else(|| db_err("Raft WAL is closed"))?)
        })
        .await
        .map_err(err)?
        .map_err(err)
    }
    /// Cross-store operations always lock WAL before bbolt, entirely off runtime workers.
    #[allow(clippy::result_large_err)]
    async fn run_checkpoint<T: Send + 'static>(
        &self,
        call: impl FnOnce(&mut wal::Wal, &mut checkpoint::Checkpoint) -> io::Result<T> + Send + 'static,
    ) -> Result<T, StorageError<Types>> {
        let checkpoint_wait = ProfileTimer::start(Metric::CheckpointWait);
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
            let mut wal = inner.wal.lock().unwrap_or_else(PoisonError::into_inner);
            let mut db = inner.db.lock().unwrap_or_else(PoisonError::into_inner);
            drop(checkpoint_wait);
            let _work = ProfileTimer::start(Metric::CheckpointPersist);
            call(
                wal.as_mut().ok_or_else(|| db_err("Raft WAL is closed"))?,
                db.as_mut()
                    .ok_or_else(|| db_err("checkpoint store is closed"))?,
            )
        })
        .await
        .map_err(err)?
        .map_err(err)
    }
    #[allow(clippy::result_large_err)] // Preserve OpenRaft's fixed typed storage error.
    async fn serialized<T, F, Fut>(&self, operation: F) -> Result<T, StorageError<Types>>
    where
        T: Send + 'static,
        F: FnOnce(Self) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, StorageError<Types>>> + Send + 'static,
    {
        // At most one owned continuation exists. Dropping the caller cannot
        // release serialization between durable IO and RAM publication.
        let serial_wait = ProfileTimer::start(Metric::ApplySerialWait);
        let turn = self.apply_gate.clone().acquire_owned().await.map_err(err)?;
        drop(serial_wait);
        let this = self.clone();
        tokio::spawn(async move {
            let _turn = turn;
            operation(this).await
        })
        .await
        .map_err(err)?
    }
    #[cfg(test)]
    pub(crate) fn test_apply_gate(&self) -> Arc<Semaphore> {
        self.apply_gate.clone()
    }
    /// Consistent sparse updates; snapshots are limited to bootstrap/history gaps.
    pub fn watch_updates(&self, base: Option<u64>) -> Vec<MetadataUpdate> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let revision = state.metadata.revision;
        if base == Some(revision) {
            return vec![MetadataUpdate::Progress { revision }];
        }
        if let Some(base) = base
            && let Some(updates) = self
                .history
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .since(base)
        {
            return updates;
        }
        vec![MetadataUpdate::Snapshot(state.metadata.clone())]
    }
    #[allow(clippy::result_large_err)] // Preserve OpenRaft's fixed typed storage error.
    async fn cpu<T: Send + 'static>(
        &self,
        call: impl FnOnce() -> io::Result<T> + Send + 'static,
    ) -> Result<T, StorageError<Types>> {
        let slot_wait = ProfileTimer::start(Metric::ApplySlotWait);
        let permit = self
            .inner
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(err)?;
        drop(slot_wait);
        let spawn_wait = ProfileTimer::start(Metric::ApplySpawnWait);
        tokio::task::spawn_blocking(move || {
            drop(spawn_wait);
            let _permit = permit;
            call()
        })
        .await
        .map_err(err)?
        .map_err(err)
    }
    #[allow(clippy::result_large_err)] // Preserve OpenRaft's fixed typed storage error.
    async fn persist_checkpoint(
        &self,
        state: State,
    ) -> Result<(Snapshot, State), StorageError<Types>> {
        self.owner.state().begin_full();
        let result = async {
            let _total = ProfileTimer::start(Metric::CheckpointTotal);
            let (state, saved) = self
                .cpu(move || {
                    let saved = make_snapshot(&state)?;
                    Ok((state, saved))
                })
                .await?;
            let meta = saved.meta.clone();
            let data = saved.data.clone();
            self.run_checkpoint(move |wal, db| {
                recovery::verify_wal_tail(wal, saved.meta.last_log_id)?;
                let previous = db.read_meta()?;
                if previous.as_ref().is_some_and(|old| {
                    old.last_log_id.map(|id| id.index) > saved.meta.last_log_id.map(|id| id.index)
                        || (old.last_log_id.map(|id| id.index)
                            == saved.meta.last_log_id.map(|id| id.index)
                            && old.last_log_id != saved.meta.last_log_id)
                }) {
                    return Err(db_err("checkpoint cannot regress durable applied state"));
                }
                if previous.as_ref().is_none_or(|old| {
                    old.last_log_id.map(|id| id.index) <= saved.meta.last_log_id.map(|id| id.index)
                }) {
                    db.write_snapshot(&saved)?;
                }
                Ok(())
            })
            .await?;
            Ok((
                Snapshot {
                    meta,
                    snapshot: Cursor::new(data),
                },
                state,
            ))
        }
        .await;
        match &result {
            Ok((_, state)) => self.owner.state().finish(state.last_applied),
            Err(_) => self.owner.state().need_full(),
        }
        result
    }
    #[allow(clippy::result_large_err)] // Preserve OpenRaft's fixed typed storage error.
    async fn apply_entries(&self, entries: Vec<Entry>) -> Result<Vec<Answer>, StorageError<Types>> {
        {
            let mut previous = {
                let lock_wait = ProfileTimer::start(Metric::ApplyMutexWait);
                let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                drop(lock_wait);
                state.last_applied.map(|id| id.index)
            };
            for entry in &entries {
                if previous.is_some_and(|index| entry.log_id.index <= index) {
                    return Err(err("state-machine apply index cannot repeat or regress"));
                }
                previous = Some(entry.log_id.index);
            }
        }
        let (entries, durable) = self
            .run_wal(move |wal| {
                let _check = ProfileTimer::start(Metric::ApplyInputCheck);
                let durable = recovery::applied_input_durable(wal, &entries)?;
                Ok((entries, durable))
            })
            .await?;
        let state = self.state.clone();
        let needs_checkpoint = self
            .cpu({
                let entries = entries.iter().map(|entry| entry.log_id).collect::<Vec<_>>();
                move || {
                    let mut previous = {
                        let lock_wait = ProfileTimer::start(Metric::ApplyMutexWait);
                        let state = state.lock().unwrap_or_else(PoisonError::into_inner);
                        drop(lock_wait);
                        state.last_applied.map(|id| id.index)
                    };
                    Ok(!durable
                        || entries.into_iter().fold(false, |gap, id| {
                            let next_gap =
                                id.index != previous.map_or(0, |idx| idx.saturating_add(1));
                            previous = Some(id.index);
                            gap || next_gap
                        }))
                }
            })
            .await?;
        if needs_checkpoint {
            let state = self.state.clone();
            let (candidate, result, updates) = self
                .cpu(move || {
                    let mut candidate = {
                        let lock_wait = ProfileTimer::start(Metric::ApplyMutexWait);
                        let state = state.lock().unwrap_or_else(PoisonError::into_inner);
                        drop(lock_wait);
                        let _clone = ProfileTimer::start(Metric::CheckpointClone);
                        state.clone()
                    };
                    let mut answers = Vec::with_capacity(entries.len());
                    let mut updates = Vec::new();
                    for entry in entries {
                        let (answer, delta) = {
                            let _mutate = ProfileTimer::start(Metric::ApplyMutate);
                            candidate.apply_tracked(entry)
                        };
                        answers.push(answer);
                        if let Some(delta) = delta {
                            updates.push(delta);
                        }
                    }
                    Ok((candidate, answers, updates))
                })
                .await?;
            let (_, candidate) = self.persist_checkpoint(candidate).await?;
            let state = self.state.clone();
            let history = self.history.clone();
            let changed = self.changed.clone();
            self.cpu(move || {
                let lock_wait = ProfileTimer::start(Metric::ApplyMutexWait);
                let mut state = state.lock().unwrap_or_else(PoisonError::into_inner);
                let mut history = history.lock().unwrap_or_else(PoisonError::into_inner);
                drop(lock_wait);
                *state = candidate;
                for update in updates {
                    let _history = ProfileTimer::start(Metric::ApplyHistory);
                    history.push(update);
                }
                changed.send_replace(state.metadata.revision);
                Ok(())
            })
            .await?;
            Ok(result)
        } else {
            let state = self.state.clone();
            let history = self.history.clone();
            let changed = self.changed.clone();
            let flush = Arc::clone(self.owner.state());
            self.cpu(move || {
                let lock_wait = ProfileTimer::start(Metric::ApplyMutexWait);
                let mut state = state.lock().unwrap_or_else(PoisonError::into_inner);
                let mut history = history.lock().unwrap_or_else(PoisonError::into_inner);
                drop(lock_wait);
                let mut answers = Vec::with_capacity(entries.len());
                for entry in entries {
                    let previous = state.last_applied;
                    let beats = flush::heartbeat_nodes(&entry);
                    let (answer, delta) = {
                        let _mutate = ProfileTimer::start(Metric::ApplyMutate);
                        state.apply_tracked(entry)
                    };
                    let heartbeats = if answer.error.is_none() {
                        beats.as_slice()
                    } else {
                        &[]
                    };
                    flush.observe(&state, previous, heartbeats, delta.as_ref());
                    answers.push(answer);
                    if let Some(delta) = delta {
                        let _history = ProfileTimer::start(Metric::ApplyHistory);
                        history.push(delta);
                    }
                }
                changed.send_replace(state.metadata.revision);
                Ok(answers)
            })
            .await
        }
    }
    #[allow(clippy::result_large_err)] // Preserve OpenRaft's fixed typed storage error.
    async fn install_checkpoint(
        &self,
        meta: SnapshotMeta,
        data: Vec<u8>,
    ) -> Result<(), StorageError<Types>> {
        let applied = meta.last_log_id;
        self.owner.state().begin_full();
        let result = async {
            let (next, saved) = self
                .cpu(move || {
                    let digest = recovery::snapshot_digest(&meta, &data)?;
                    let saved = SavedSnapshot {
                        meta,
                        data,
                        format: 1,
                        digest: Some(digest),
                    };
                    let next = recovery::snapshot_state(&saved)?;
                    Ok((next, saved))
                })
                .await?;
            let checkpoint = next.last_applied;
            self.run_checkpoint(move |wal, db| {
                recovery::verify_wal_tail(wal, checkpoint)?;
                if old_checkpoint_conflicts(db, wal, saved.meta.last_log_id)? {
                    return Err(db_err(
                        "snapshot regresses or conflicts with durable authority",
                    ));
                }
                db.write_snapshot(&saved)?;
                let old = wal.committed()?;
                if checkpoint.map(|id| id.index) > old.map(|id| id.index) {
                    wal.save_committed(checkpoint)?;
                }
                Ok(())
            })
            .await?;
            let state = self.state.clone();
            let history = self.history.clone();
            let changed = self.changed.clone();
            self.cpu(move || {
                let mut state = state.lock().unwrap_or_else(PoisonError::into_inner);
                *state = next;
                history
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .clear();
                changed.send_replace(state.metadata.revision);
                Ok(())
            })
            .await
        }
        .await;
        match &result {
            Ok(()) => self.owner.state().finish(applied),
            Err(_) => self.owner.state().need_full(),
        }
        result
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
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: R,
    ) -> Result<Vec<Entry>, io::Error> {
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
        self.run_wal(move |wal| wal.entries(from, to))
            .await
            .map_err(io::Error::from)
    }
    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        self.run_wal(|wal| wal.vote())
            .await
            .map_err(io::Error::from)
    }
}
impl RaftLogStorage<Types> for Store {
    type LogReader = Self;
    async fn get_log_state(&mut self) -> Result<LogState<Types>, io::Error> {
        self.run_wal(|wal| wal.state())
            .await
            .map_err(io::Error::from)
    }
    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }
    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        let vote = *vote;
        self.run_wal(move |wal| wal.save_vote(vote))
            .await
            .map_err(io::Error::from)
    }
    async fn save_committed(&mut self, id: Option<LogId>) -> Result<(), io::Error> {
        let context = diagnostics::Context {
            node: self.node,
            entries: 0,
            first: None,
            last: id.map(|id| id.index),
        };
        diagnostics::measure(
            diagnostics::Stage::SaveCommitted,
            context,
            self.run_wal(move |wal| {
                diagnostics::measure_sync(diagnostics::Stage::WalCommit, context, || {
                    wal.save_committed(id)
                })
            }),
        )
        .await
        .map_err(io::Error::from)
    }
    async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
        self.run_wal(|wal| wal.committed())
            .await
            .map_err(io::Error::from)
    }
    async fn append<I>(&mut self, entries: I, callback: IOFlushed<Types>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        // The blocking continuation owns WAL serialization even if its caller cancels.
        let inner = self.inner.clone();
        let context = diagnostics::Context::entries(self.node, &entries);
        diagnostics::measure(
            diagnostics::Stage::Append,
            context,
            self.run_wal(move |wal| {
                diagnostics::measure_sync(diagnostics::Stage::WalAppend, context, || {
                    log::append_checked(wal, &inner.db, entries)
                })
            }),
        )
        .await?;
        callback.io_completed(Ok(()));
        diagnostics::flushed(context);
        Ok(())
    }
    async fn truncate_after(&mut self, id: Option<LogId>) -> Result<(), io::Error> {
        self.run_checkpoint(move |wal, db| {
            let committed = wal.committed()?;
            let checkpoint = db.read_meta()?.and_then(|meta| meta.last_log_id);
            let Some(start) = (match id {
                Some(id) => id.index.checked_add(1),
                None => Some(0),
            }) else {
                return Ok(());
            };
            if committed.is_some_and(|last| {
                last.index >= start && checkpoint.is_none_or(|covered| covered.index < last.index)
            }) {
                return Err(db_err(
                    "truncate would remove unsnapshotted committed entries",
                ));
            }
            wal.truncate_from(start)
        })
        .await
        .map_err(io::Error::from)
    }
    async fn purge(&mut self, id: LogId) -> Result<(), io::Error> {
        let needs_checkpoint = self
            .run_checkpoint(move |wal, db| {
                let committed = wal.committed()?;
                let checkpoint = db.read_meta()?.and_then(|meta| meta.last_log_id);
                Ok(committed
                    .map(|log| log.index.min(id.index))
                    .is_some_and(|needed| checkpoint.is_none_or(|log| log.index < needed)))
            })
            .await?;
        if needs_checkpoint {
            self.build_snapshot().await?;
        }
        self.run_checkpoint(move |wal, db| {
            let committed = wal.committed()?;
            let checkpoint = db.read_meta()?.and_then(|meta| meta.last_log_id);
            if committed
                .map(|log| log.index.min(id.index))
                .is_some_and(|needed| checkpoint.is_none_or(|log| log.index < needed))
            {
                return Err(db_err("purge would remove unsnapshotted committed entries"));
            }
            wal.purge(id)
        })
        .await
        .map_err(io::Error::from)
    }
}

fn old_checkpoint_conflicts(
    db: &checkpoint::Checkpoint,
    wal: &mut wal::Wal,
    incoming: Option<LogId>,
) -> io::Result<bool> {
    let checkpoint = db.read_meta()?.and_then(|meta| meta.last_log_id);
    let committed = wal.committed()?;
    Ok(checkpoint.map(|id| id.index) > incoming.map(|id| id.index)
        || checkpoint
            .is_some_and(|old| incoming.is_some_and(|new| old.index == new.index && old != new))
        || committed
            .is_some_and(|old| incoming.is_some_and(|new| old.index == new.index && old != new)))
}
#[derive(Serialize, serde::Deserialize)]
struct SavedSnapshot {
    #[serde(default)]
    format: u64,
    #[serde(default)]
    digest: Option<String>,
    meta: SnapshotMeta,
    data: Vec<u8>,
}
fn make_snapshot(state: &State) -> io::Result<SavedSnapshot> {
    let _encode = ProfileTimer::start(Metric::CheckpointEncode);
    let data = encode(&state)?;
    if data.len() > recovery::MAX_SNAPSHOT {
        return Err(db_err("snapshot exceeds limit"));
    }
    let meta = SnapshotMeta {
        last_log_id: state.last_applied,
        last_membership: state.membership.clone(),
    };
    let digest = recovery::snapshot_digest(&meta, &data)?;
    Ok(SavedSnapshot {
        meta,
        data,
        format: 1,
        digest: Some(digest),
    })
}
impl RaftSnapshotBuilder<Types> for Store {
    type SnapshotData = Cursor<Vec<u8>>;
    async fn build_snapshot(&mut self) -> Result<Snapshot, io::Error> {
        let target = self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .last_applied;
        let flush = Arc::clone(self.owner.state());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let progress = flush.progress.notified();
            if flush.covers(target) {
                break;
            }
            if flush.dirty_reaches(target) {
                flush.kick();
            } else {
                flush.need_full();
            }
            if std::time::Instant::now() >= deadline {
                return Err(err(flush.error()).into());
            }
            let _ = tokio::time::timeout(std::time::Duration::from_millis(20), progress).await;
        }
        let saved = self
            .run(|db| db.read_snapshot())
            .await?
            .ok_or_else(|| err("checkpoint missing after flush"))?;
        Ok(Snapshot {
            meta: saved.meta,
            snapshot: Cursor::new(saved.data),
        })
    }
}
impl Store {
    #[cfg(test)]
    pub async fn apply_log<I>(&self, entries: I) -> Result<Vec<Answer>, StorageError<Types>>
    where
        I: IntoIterator<Item = Entry> + Send,
    {
        self.apply_ready(entries).await
    }
    async fn apply_ready<I>(&self, entries: I) -> Result<Vec<Answer>, StorageError<Types>>
    where
        I: IntoIterator<Item = Entry> + Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let context = diagnostics::Context::entries(self.node, &entries);
        diagnostics::measure(
            diagnostics::Stage::ApplyQueued,
            context,
            self.serialized(move |this| async move {
                diagnostics::measure(
                    diagnostics::Stage::ApplyState,
                    context,
                    this.apply_entries(entries),
                )
                .await
            }),
        )
        .await
    }
}
impl SnapshotReceiverFactory<Types> for Store {
    type SnapshotReceiver = Cursor<Vec<u8>>;
    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }
}
impl RaftStateMachine<Types> for Store {
    type SnapshotData = Cursor<Vec<u8>>;
    type SnapshotBuilder = Self;
    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), io::Error> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        Ok((state.last_applied, state.membership.clone()))
    }
    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<Types>, io::Error>> + Unpin + Send,
    {
        let mut batch = Vec::new();
        let mut responders = Vec::new();
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            batch.push(entry);
            responders.push(responder);
        }
        let answers = self.apply_ready(batch).await.map_err(io::Error::from)?;
        for (answer, responder) in answers.into_iter().zip(responders) {
            if let Some(responder) = responder {
                responder.send(answer);
            }
        }
        Ok(())
    }
    async fn get_snapshot_builder(&mut self) -> Self {
        self.clone()
    }
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let meta = meta.clone();
        let data = snapshot.into_inner();
        self.serialized(move |this| async move { this.install_checkpoint(meta, data).await })
            .await
            .map_err(io::Error::from)
    }
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, io::Error> {
        let saved = self.run(|db| db.read_snapshot()).await?;
        self.cpu(move || {
            saved
                .map(|saved| {
                    recovery::snapshot_state(&saved)?;
                    Ok(Snapshot {
                        meta: saved.meta,
                        snapshot: Cursor::new(saved.data),
                    })
                })
                .transpose()
        })
        .await
        .map_err(io::Error::from)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod wal_integration_tests;

#[cfg(test)]
mod checkpoint_integration_tests;

#[cfg(test)]
mod pipeline_tests;

#[cfg(test)]
mod wal_append_bench;
