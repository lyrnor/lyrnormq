//! Log purge that must not block the Raft core on checkpoint progress.
use super::{Inner, checkpoint, db_err, wal};
use crate::model::LogId;
use std::{
    io,
    sync::{
        Arc, Condvar, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
};
struct Slot {
    pending: Option<LogId>,
    inflight: Option<u64>,
}
struct Shared {
    slot: Mutex<Slot>,
    wake: Condvar,
    stop: AtomicBool,
}
pub(super) struct LogPurge {
    shared: Arc<Shared>,
    thread: Mutex<Option<JoinHandle<()>>>,
}
impl Drop for LogPurge {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Release);
        self.shared.wake.notify_one();
        if let Some(handle) = self
            .thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            let _ = handle.join();
        }
    }
}
impl LogPurge {
    pub(super) fn start(
        inner: Arc<Inner>,
        flush: Arc<super::flush::FlushState>,
    ) -> io::Result<Arc<Self>> {
        let shared = Arc::new(Shared {
            slot: Mutex::new(Slot {
                pending: None,
                inflight: None,
            }),
            wake: Condvar::new(),
            stop: AtomicBool::new(false),
        });
        let worker = Arc::clone(&shared);
        let thread = thread::Builder::new()
            .name("raft-log-purge".to_owned())
            .spawn(move || run(worker, inner, flush))?;
        Ok(Arc::new(Self {
            shared,
            thread: Mutex::new(Some(thread)),
        }))
    }
    /// Keep the newest requested prefix. A lower index is already included.
    pub(super) fn schedule(&self, id: LogId) {
        let mut slot = self
            .shared
            .slot
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if self.shared.stop.load(Ordering::Acquire) {
            return;
        }
        if slot.inflight.is_some_and(|index| index >= id.index) {
            return;
        }
        if slot
            .pending
            .as_ref()
            .is_some_and(|current| current.index >= id.index)
        {
            return;
        }
        slot.pending = Some(id);
        self.shared.wake.notify_one();
    }
}
fn run(shared: Arc<Shared>, inner: Arc<Inner>, flush: Arc<super::flush::FlushState>) {
    loop {
        let id = {
            let mut slot = shared.slot.lock().unwrap_or_else(PoisonError::into_inner);
            loop {
                if let Some(id) = slot.pending.take() {
                    slot.inflight = Some(id.index);
                    break id;
                }
                if shared.stop.load(Ordering::Acquire) {
                    return;
                }
                slot = shared
                    .wake
                    .wait(slot)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        };
        defer(&inner, &flush, id);
        let mut slot = shared.slot.lock().unwrap_or_else(PoisonError::into_inner);
        slot.inflight = None;
    }
}
fn defer(inner: &Inner, flush: &super::flush::FlushState, id: LogId) {
    let needed = match protected_index(inner, id) {
        Ok(index) => index,
        Err(error) => {
            report(id, &error);
            return;
        }
    };
    if let Some(index) = needed
        && !flush.wait_for_incremental(index)
    {
        return;
    }
    if let Err(error) = purge_blocking(inner, id) {
        report(id, &error);
    }
}
pub(super) fn protected_index(inner: &Inner, id: LogId) -> io::Result<Option<u64>> {
    let wal = inner.wal.lock().unwrap_or_else(PoisonError::into_inner);
    let wal = wal.as_ref().ok_or_else(|| db_err("Raft WAL is closed"))?;
    Ok(wal.committed()?.map(|log| log.index.min(id.index)))
}
pub(super) fn purge_if_covered(
    wal: &mut wal::Wal,
    db: &mut checkpoint::Checkpoint,
    id: LogId,
) -> io::Result<()> {
    let committed = wal.committed()?;
    let checkpoint = db.read_meta()?.and_then(|meta| meta.last_log_id);
    if committed
        .map(|log| log.index.min(id.index))
        .is_some_and(|needed| checkpoint.is_none_or(|log| log.index < needed))
    {
        return Err(db_err("purge would remove unsnapshotted committed entries"));
    }
    wal.purge(id)
}
fn purge_blocking(inner: &Inner, id: LogId) -> io::Result<()> {
    let mut wal = inner.wal.lock().unwrap_or_else(PoisonError::into_inner);
    let mut db = inner.db.lock().unwrap_or_else(PoisonError::into_inner);
    purge_if_covered(
        wal.as_mut().ok_or_else(|| db_err("Raft WAL is closed"))?,
        db.as_mut()
            .ok_or_else(|| db_err("checkpoint store is closed"))?,
        id,
    )
}
fn report(id: LogId, error: &io::Error) {
    if quiet(error) {
        return;
    }
    tracing::warn!(
        error = %error,
        index = id.index,
        "deferred raft log purge failed"
    );
}
fn quiet(error: &io::Error) -> bool {
    let message = error.to_string();
    message.contains("purge would remove unsnapshotted committed entries")
        || message.contains("checkpoint store is closed")
        || message.contains("Raft WAL is closed")
}
