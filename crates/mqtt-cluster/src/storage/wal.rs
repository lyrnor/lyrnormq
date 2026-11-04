//! Blocking, durable Raft WAL adapter. Caller supplies bounded blocking workers and a mutex.
use super::{Entry, LogId, LogState, Types, Vote, log};
use crate::profile::{Metric, Timer};
use raft_log::api::raft_log_writer::RaftLogWriter;
use raft_log::{Config, Dump, DumpApi, RaftLog, RaftLogAction, WALRecord};
use serde::{Deserialize, Serialize};
use std::{
    io,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
mod codec;
const MAX_ENTRY: usize = 1024 * 1024;
const CHUNK_BYTES: usize = 8 * 1024 * 1024;
const CHUNK_RECORDS: usize = 4096;
// Rotation checks after an action, then adds a bounded checkpoint record.
const MAX_CHUNK_BYTES: u64 = (CHUNK_BYTES + MAX_ENTRY + 64 * 1024) as u64;
const MAX_CHUNK_RECORDS: u64 = CHUNK_RECORDS as u64 + 4;
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct LogKey(LogId);
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Serialize, Deserialize)]
struct VoteKey(Vote);
#[derive(Debug, Clone, PartialEq, Eq)]
struct Payload(Vec<u8>);
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Metadata {
    format: u8,
    cluster: String,
    node: u64,
    committed: Option<LogKey>,
}
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct WalTypes;
impl raft_log::Types for WalTypes {
    type LogId = LogKey;
    type LogPayload = Payload;
    type Vote = VoteKey;
    type UserData = Metadata;
    type Callback = Completion;
    fn log_index(id: &LogKey) -> u64 {
        id.0.index
    }
    fn payload_size(payload: &Payload) -> u64 {
        payload.0.len() as u64
    }
}
struct Completion {
    sender: std::sync::mpsc::SyncSender<io::Result<()>>,
    #[cfg(test)]
    injected_error: bool,
}
impl raft_log::Callback for Completion {
    fn send(self, result: io::Result<()>) {
        #[cfg(test)]
        let result = if self.injected_error {
            Err(io::Error::other("injected flush callback failure"))
        } else {
            result
        };
        let _ = std::sync::mpsc::SyncSender::send(&self.sender, result);
    }
}
fn invalid(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
fn config(path: &Path) -> io::Result<Arc<Config>> {
    let mut config = Config::new(
        path.to_str()
            .ok_or_else(|| invalid("WAL path must be UTF8"))?,
    );
    config.log_cache_max_items = Some(8192);
    config.log_cache_capacity = Some(16 * 1024 * 1024);
    config.wal.read_buffer_size = Some(64 * 1024);
    config.wal.chunk_max_records = Some(CHUNK_RECORDS);
    config.wal.chunk_max_size = Some(CHUNK_BYTES);
    config.wal.truncate_incomplete_record = Some(false);
    config.wal.flush_batch_wait = Some(Duration::ZERO);
    config.wal.flush_batch_max_items = Some(128);
    config.wal.flush_queue_max_bytes = Some(16 * 1024 * 1024);
    Ok(Arc::new(config))
}
fn identity(meta: &Metadata, cluster: &str, node: u64) -> io::Result<()> {
    if meta.format != 1 || meta.cluster != cluster || meta.node != node {
        return Err(invalid("WAL identity/version mismatch"));
    }
    Ok(())
}
fn decode_payload(id: &LogKey, payload: &Payload) -> io::Result<Entry> {
    let index = i64::try_from(id.0.index).map_err(invalid)?;
    let entry = log::decode_entry(index, &payload.0)?;
    if entry.log_id != id.0 {
        return Err(invalid("WAL outer/inner log identity mismatch"));
    }
    Ok(entry)
}
/// Validate every existing file BEFORE upstream's open can remove incomplete chunks.
fn preflight(config: Arc<Config>, cluster: &str, node: u64) -> io::Result<()> {
    let mut chunks = std::collections::BTreeSet::new();
    for item in std::fs::read_dir(&config.wal.dir)? {
        let item = item?;
        let name = item.file_name();
        let name = name.to_string_lossy();
        if name.ends_with(".wal") {
            let index =
                raft_log::chunked_wal::Config::parse_chunk_file_name(&name).map_err(invalid)?;
            let bytes = item.metadata()?.len();
            if !item.file_type()?.is_file() || bytes == 0 || bytes > MAX_CHUNK_BYTES {
                return Err(invalid("existing WAL has empty/non-file chunk"));
            }
            chunks.insert(index);
        }
    }
    if chunks.is_empty() {
        return Err(invalid("existing WAL has no chunks"));
    }
    let mut seen_chunks = std::collections::BTreeSet::new();
    let mut seen = false;
    let dump = Dump::<WalTypes>::new(config)?;
    dump.write_with(|chunk, record_index, record| {
        if record_index >= MAX_CHUNK_RECORDS {
            return Err(invalid("WAL chunk record count exceeds bound"));
        }
        let (_, record) = record?;
        seen_chunks.insert(*chunk);
        match record {
            WALRecord::Checkpoint(state) => {
                if let Some(meta) = state.user_data.as_ref() {
                    identity(meta, cluster, node)?;
                    seen = true;
                }
            }
            WALRecord::Action(RaftLogAction::Append(id, payload)) => {
                decode_payload(&id, &payload)?;
            }
            _ => {}
        }
        Ok(())
    })?;
    if !seen || chunks != seen_chunks {
        return Err(invalid(
            "existing WAL lacks durable identity or complete chunk",
        ));
    }
    Ok(())
}
pub(super) struct Wal {
    inner: RaftLog<WalTypes>,
    poisoned: AtomicBool,
    #[cfg(test)]
    fail_next_callback: bool,
}
impl Wal {
    pub(super) fn open(path: &Path, cluster: &str, node: u64) -> io::Result<Self> {
        let fresh = match std::fs::symlink_metadata(path) {
            Ok(meta) => {
                if !meta.is_dir() || meta.file_type().is_symlink() {
                    return Err(invalid("WAL path is not a real directory"));
                }
                false
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => true,
            Err(e) => return Err(e),
        };
        let configuration = config(path)?;
        if fresh {
            std::fs::create_dir(path)?;
            if let Some(parent) = path.parent() {
                std::fs::File::open(parent)?.sync_all()?;
            }
        } else {
            preflight(configuration.clone(), cluster, node)?;
        }
        let inner = RaftLog::<WalTypes>::open(configuration)?;
        let mut this = Self {
            inner,
            poisoned: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_callback: false,
        };
        if fresh {
            let meta = Metadata {
                format: 1,
                cluster: cluster.into(),
                node,
                committed: None,
            };
            this.mutate(move |wal| {
                wal.save_user_data(Some(meta))?;
                Ok(())
            })?;
        } else {
            identity(
                this.inner
                    .log_state()
                    .user_data
                    .as_ref()
                    .ok_or_else(|| invalid("WAL identity missing"))?,
                cluster,
                node,
            )?;
        }
        Ok(this)
    }
    fn check(&self) -> io::Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            Err(invalid("WAL instance is poisoned"))
        } else {
            Ok(())
        }
    }
    fn read_result<T>(&self, result: io::Result<T>) -> io::Result<T> {
        if result.is_err() {
            self.poisoned.store(true, Ordering::Release)
        }
        result
    }
    fn mutate(
        &mut self,
        operation: impl FnOnce(&mut RaftLog<WalTypes>) -> io::Result<()>,
    ) -> io::Result<()> {
        self.mutate_with(true, operation)
    }
    /// `sync` false still waits until the write callback. It does not fsync.
    /// The next `flush(true)` calls `sync_data` on this file and persists the
    /// earlier bytes with that append.
    fn mutate_with(
        &mut self,
        sync: bool,
        operation: impl FnOnce(&mut RaftLog<WalTypes>) -> io::Result<()>,
    ) -> io::Result<()> {
        self.check()?;
        let result = (|| {
            {
                let _mutation = Timer::start(Metric::WalMutation);
                operation(&mut self.inner)?;
            }
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            let callback = Completion {
                sender: tx,
                #[cfg(test)]
                injected_error: std::mem::take(&mut self.fail_next_callback),
            };
            {
                let _enqueue = Timer::start(Metric::WalFlushEnqueue);
                self.inner.flush(sync, Some(callback))?;
            }
            let _callback_wait = Timer::start(Metric::WalFlushWait);
            rx.recv_timeout(Duration::from_secs(30))
                .map_err(invalid)??;
            Ok(())
        })();
        if result.is_err() {
            self.poisoned.store(true, Ordering::Release)
        }
        result
    }
    pub(super) fn entries(&self, from: u64, to: u64) -> io::Result<Vec<Entry>> {
        let _read = Timer::start(Metric::WalRead);
        self.check()?;
        if from > to {
            return Ok(Vec::new());
        }
        if from > i64::MAX as u64 {
            return Ok(Vec::new());
        }
        let end = to.min(i64::MAX as u64) + 1;
        self.read_result(
            self.inner
                .read(from, end)
                .map(|entry| {
                    let (id, payload) = entry?;
                    decode_payload(&id, &payload)
                })
                .collect(),
        )
    }
    pub(super) fn vote(&self) -> io::Result<Option<Vote>> {
        self.check()?;
        Ok(self.inner.log_state().vote().map(|vote| vote.0))
    }
    pub(super) fn save_vote(&mut self, vote: Vote) -> io::Result<()> {
        self.check()?;
        if self.vote()? == Some(vote) {
            return Ok(());
        }
        self.mutate(move |wal| {
            wal.save_vote(VoteKey(vote))?;
            Ok(())
        })
    }
    pub(super) fn committed(&self) -> io::Result<Option<LogId>> {
        self.check()?;
        Ok(self
            .inner
            .log_state()
            .user_data
            .as_ref()
            .and_then(|data| data.committed.as_ref())
            .map(|id| id.0))
    }
    pub(super) fn save_committed(&mut self, id: Option<LogId>) -> io::Result<()> {
        self.check()?;
        let old = self.committed()?;
        if old.is_some_and(|old| {
            id.is_none_or(|new| new.index < old.index || (new.index == old.index && new != old))
        }) {
            return Err(invalid("WAL committed marker cannot regress"));
        }
        if id == old {
            return Ok(());
        }
        if id.is_some_and(|id| id.index > i64::MAX as u64) {
            return Err(invalid("WAL committed index exceeds supported bound"));
        }
        let mut meta = self
            .inner
            .log_state()
            .user_data
            .clone()
            .ok_or_else(|| invalid("WAL identity missing"))?;
        meta.committed = id.map(LogKey);
        // Same file as the log. Skip this fsync; the next append's sync_data
        // persists the watermark. A crash before that append leaves the marker
        // one batch behind the already-fsynced log.
        self.mutate_with(false, move |wal| {
            wal.save_user_data(Some(meta))?;
            Ok(())
        })
    }
    pub(super) fn state(&self) -> io::Result<LogState<Types>> {
        self.check()?;
        let state = self.inner.log_state();
        Ok(LogState {
            last_purged_log_id: state.purged().map(|id| id.0),
            last_log_id: state.last().map(|id| id.0),
        })
    }
    pub(super) fn append(&mut self, entries: Vec<Entry>) -> io::Result<()> {
        self.check()?;
        let mut prepared = Vec::with_capacity(entries.len());
        let mut previous = None;
        for entry in entries {
            i64::try_from(entry.log_id.index).map_err(invalid)?;
            if previous.is_some_and(|index: u64| Some(entry.log_id.index) != index.checked_add(1)) {
                return Err(invalid("WAL append batch gap"));
            }
            previous = Some(entry.log_id.index);
            prepared.push((LogKey(entry.log_id), Payload(log::encode_entry(&entry)?)));
        }
        self.mutate(move |wal| {
            wal.append(prepared)?;
            Ok(())
        })
    }
    pub(super) fn truncate(&mut self, id: LogId) -> io::Result<()> {
        self.truncate_from(id.index)
    }
    /// Delete every record at `index` and after it.
    ///
    /// An index past the last stored record is a no-op. raft-log otherwise
    /// looks up the preceding id and fails when that predecessor was never
    /// appended. An index at or before the purged prefix deletes the live
    /// suffix, which is the whole remaining log.
    pub(super) fn truncate_from(&mut self, index: u64) -> io::Result<()> {
        self.mutate(move |wal| {
            let (last, first) = {
                let state = wal.log_state();
                (
                    state.last().map(|id| id.0.index),
                    state
                        .purged()
                        .map(|id| id.0.index.saturating_add(1))
                        .unwrap_or(0),
                )
            };
            if last.is_none_or(|last| index > last) {
                return Ok(());
            }
            wal.truncate(index.max(first))?;
            Ok(())
        })
    }
    pub(super) fn purge(&mut self, id: LogId) -> io::Result<()> {
        self.mutate(move |wal| {
            wal.purge(LogKey(id))?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests;
