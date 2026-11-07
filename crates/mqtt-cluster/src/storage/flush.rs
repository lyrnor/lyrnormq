//! Incremental checkpoint flush. Business rows are written off the Raft WAL lock.
use super::{Inner, State, db_err, encode};
use crate::model::{
    Client, Command, DurableOwner, Entry, LogId, Metadata, MetadataUpdate, Node, SnapshotMeta,
    StoredMembership,
};
use crate::profile::{Metric, Timer as ProfileTimer};
use openraft::EntryPayload;
use std::{
    sync::{
        Arc, Condvar, Mutex, OnceLock, PoisonError, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle, Thread},
    time::{Duration, Instant},
};
/// Fold later updates of the same key into one transaction. Deferred log purge
/// waits until this incremental checkpoint covers the committed prefix, so the
/// WAL keeps that short tail.
const COMMIT_MIN: Duration = Duration::from_millis(200);
const COMMIT_MAX: Duration = Duration::from_secs(1);

const NODE_BUCKET: &[u8] = b"nodes";
const CLIENT_BUCKET: &[u8] = b"clients";
const ROUTE_BUCKET: &[u8] = b"routes";
const DURABLE_BUCKET: &[u8] = b"durable";

struct Mark {
    written: bool,
    applied: Option<LogId>,
}
#[derive(Default)]
struct Dirty {
    since: Option<LogId>,
    through: Option<LogId>,
    membership: StoredMembership,
    revision: u64,
    metadata: Metadata,
    nodes: std::collections::BTreeMap<u64, Option<Node>>,
    clients: std::collections::BTreeMap<String, Option<Client>>,
    durable: std::collections::BTreeMap<String, Option<DurableOwner>>,
    routes: std::collections::BTreeMap<String, Option<std::collections::BTreeSet<u64>>>,
    gap: bool,
}
impl Dirty {
    fn has_work(&self) -> bool {
        self.through.is_some() || self.gap
    }
    fn clear(&mut self) {
        *self = Self::default();
    }
}
pub(super) struct FlushState {
    ram: Arc<Mutex<State>>,
    dirty: Mutex<Dirty>,
    mark: Mutex<Mark>,
    epoch: AtomicU64,
    need_full: AtomicBool,
    /// A dirty transaction has taken its patch and has not finished yet.
    inflight: AtomicBool,
    /// Bootstrap has released the checkpoint mutex. Read by tests before they
    /// take that mutex; production only publishes the flag.
    #[cfg_attr(not(test), allow(dead_code))]
    booted: AtomicBool,
    stop: AtomicBool,
    wake: AtomicBool,
    parker: OnceLock<Thread>,
    pub(super) progress: tokio::sync::Notify,
    progress_mu: Mutex<()>,
    progress_cvar: Condvar,
    last_error: Mutex<Option<String>>,
}
pub(super) struct Owner {
    state: Arc<FlushState>,
    join: Mutex<Option<JoinHandle<()>>>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.state.stop.store(true, Ordering::Release);
        self.state.notify_progress();
        self.state.kick();
        if let Some(handle) = self
            .join
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            let _ = handle.join();
        }
    }
}
impl FlushState {
    pub(super) fn kick(&self) {
        self.wake.store(true, Ordering::Release);
        if let Some(thread) = self.parker.get() {
            thread.unpark();
        }
    }
    pub(super) fn observe(
        &self,
        state: &State,
        previous: Option<LogId>,
        heartbeats: &[u64],
        update: Option<&MetadataUpdate>,
    ) {
        {
            let mut dirty = self.dirty.lock().unwrap_or_else(PoisonError::into_inner);
            if dirty.through.is_none() {
                dirty.since = previous;
            }
            dirty.through = state.last_applied;
            dirty.membership = state.membership.clone();
            dirty.revision = state.metadata.revision;
            dirty.metadata.revision = state.metadata.revision;
            dirty.metadata.data_mode = state.metadata.data_mode.clone();
            dirty.metadata.disk_ids = state.metadata.disk_ids.clone();
            dirty.metadata.data_generation = state.metadata.data_generation;
            dirty.metadata.excluded = state.metadata.excluded.clone();
            dirty.metadata.repair = state.metadata.repair.clone();
            dirty.metadata.last_repair = state.metadata.last_repair.clone();
            dirty.metadata.rules_config = state.metadata.rules_config.clone();
            dirty.metadata.access_config = state.metadata.access_config.clone();
            match update {
                Some(MetadataUpdate::Delta {
                    nodes,
                    clients,
                    durable,
                    routes,
                    ..
                }) => {
                    for (key, value) in nodes {
                        dirty.nodes.insert(*key, value.clone());
                    }
                    for (key, value) in clients {
                        dirty.clients.insert(key.clone(), value.clone());
                    }
                    for (key, value) in durable {
                        dirty.durable.insert(key.clone(), value.clone());
                    }
                    for (key, value) in routes {
                        dirty.routes.insert(key.clone(), value.clone());
                    }
                }
                Some(MetadataUpdate::Snapshot(_)) => dirty.gap = true,
                Some(MetadataUpdate::Progress { .. }) | None => {}
            }
            if dirty.gap {
                self.need_full.store(true, Ordering::Release);
            }
            for node in heartbeats {
                if let Some(value) = state.metadata.nodes.get(node) {
                    dirty.nodes.insert(*node, Some(value.clone()));
                }
            }
        }
        self.kick();
    }
    pub(super) fn begin_full(&self) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
        self.dirty
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
    pub(super) fn finish(&self, applied: Option<LogId>) {
        let mut mark = self.mark.lock().unwrap_or_else(PoisonError::into_inner);
        let advance = !mark.written
            || match (mark.applied, applied) {
                (None, Some(_)) => true,
                (Some(current), Some(next)) => next.index > current.index || next == current,
                _ => false,
            };
        if advance {
            mark.written = true;
            mark.applied = applied;
        }
        drop(mark);
        self.notify_progress();
    }
    pub(super) fn covers_index(&self, index: u64) -> bool {
        let mark = self.mark.lock().unwrap_or_else(PoisonError::into_inner);
        mark.applied.is_some_and(|have| have.index >= index)
    }
    #[cfg(test)]
    pub(super) fn inflight(&self) -> bool {
        self.inflight.load(Ordering::Acquire)
    }
    #[cfg(test)]
    pub(super) fn booted(&self) -> bool {
        self.booted.load(Ordering::Acquire)
    }
    /// Incremental flush can still record `index`. A full snapshot is not a
    /// substitute: purge must not wait for or trigger one.
    pub(super) fn incremental_can_reach(&self, index: u64) -> bool {
        if self.need_full.load(Ordering::Acquire) {
            return false;
        }
        if self.inflight.load(Ordering::Acquire) {
            return true;
        }
        self.dirty_reaches_index(index)
    }
    /// Wait until the incremental checkpoint covers `index`. `false` means this
    /// purge should leave the log in place.
    pub(super) fn wait_for_incremental(&self, index: u64) -> bool {
        loop {
            if self.covers_index(index) {
                return true;
            }
            if self.need_full.load(Ordering::Acquire) {
                return false;
            }
            let busy = self.inflight.load(Ordering::Acquire) || pending(self);
            if self.stop.load(Ordering::Acquire) && !busy {
                return false;
            }
            if !busy && !self.dirty_reaches_index(index) {
                return false;
            }
            self.wait_progress(Duration::from_millis(200));
        }
    }
    fn dirty_reaches_index(&self, index: u64) -> bool {
        let dirty = self.dirty.lock().unwrap_or_else(PoisonError::into_inner);
        !dirty.gap && dirty.through.is_some_and(|have| have.index >= index)
    }
    fn wait_progress(&self, timeout: Duration) {
        let guard = self
            .progress_mu
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let _ = self.progress_cvar.wait_timeout(guard, timeout);
    }
    fn notify_progress(&self) {
        self.progress.notify_waiters();
        self.progress_cvar.notify_all();
    }
    pub(super) fn need_full(&self) {
        self.need_full.store(true, Ordering::Release);
        self.kick();
    }
    pub(super) fn covers(&self, target: Option<LogId>) -> bool {
        let mark = self.mark.lock().unwrap_or_else(PoisonError::into_inner);
        covers(&mark, target)
    }
    pub(super) fn dirty_reaches(&self, target: Option<LogId>) -> bool {
        let dirty = self.dirty.lock().unwrap_or_else(PoisonError::into_inner);
        match target {
            None => false,
            Some(target) => dirty
                .through
                .is_some_and(|have| have == target || have.index > target.index),
        }
    }
    pub(super) fn error(&self) -> String {
        self.last_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .unwrap_or_else(|| "checkpoint flush did not reach applied state".to_owned())
    }
}
fn covers(mark: &Mark, target: Option<LogId>) -> bool {
    match target {
        None => mark.written,
        Some(target) => mark
            .applied
            .is_some_and(|have| have == target || have.index > target.index),
    }
}
pub(super) fn heartbeat_nodes(entry: &Entry) -> Vec<u64> {
    match &entry.payload {
        EntryPayload::Normal(Command::Heartbeat { node, .. }) => vec![*node],
        EntryPayload::Normal(Command::Batch { commands }) => commands
            .iter()
            .filter_map(|command| match command {
                Command::Heartbeat { node, .. } => Some(*node),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}
pub(super) fn start(inner: Weak<Inner>, ram: Arc<Mutex<State>>) -> std::io::Result<Arc<Owner>> {
    let state = Arc::new(FlushState {
        ram,
        dirty: Mutex::new(Dirty::default()),
        mark: Mutex::new(Mark {
            written: false,
            applied: None,
        }),
        epoch: AtomicU64::new(0),
        need_full: AtomicBool::new(false),
        inflight: AtomicBool::new(false),
        booted: AtomicBool::new(false),
        stop: AtomicBool::new(false),
        wake: AtomicBool::new(false),
        parker: OnceLock::new(),
        progress: tokio::sync::Notify::new(),
        progress_mu: Mutex::new(()),
        progress_cvar: Condvar::new(),
        last_error: Mutex::new(None),
    });
    let worker = Arc::clone(&state);
    let handle = thread::Builder::new()
        .name("checkpoint-flush".to_owned())
        .spawn(move || run(inner, worker))?;
    Ok(Arc::new(Owner {
        state,
        join: Mutex::new(Some(handle)),
    }))
}
fn run(inner: Weak<Inner>, state: Arc<FlushState>) {
    let _ = state.parker.set(thread::current());
    if let Some(inner) = inner.upgrade() {
        bootstrap(&inner, &state);
    }
    state.booted.store(true, Ordering::Release);
    loop {
        if state.stop.load(Ordering::Acquire)
            && !pending(&state)
            && !state.need_full.load(Ordering::Acquire)
        {
            break;
        }
        let Some(inner) = inner.upgrade() else {
            break;
        };
        if state.need_full.swap(false, Ordering::AcqRel) {
            if let Err(error) = write_full(&inner, &state) {
                remember(&state, &error);
                if closed(&error) || state.stop.load(Ordering::Acquire) {
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
            continue;
        }
        match write_dirty(&inner, &state) {
            Ok(true) => hold_for_coalesce(&state),
            Ok(false) => {
                if state.stop.load(Ordering::Acquire) {
                    break;
                }
                if !state.wake.swap(false, Ordering::AcqRel) {
                    thread::park_timeout(Duration::from_millis(5));
                }
            }
            Err(error) => {
                remember(&state, &error);
                if closed(&error) || state.stop.load(Ordering::Acquire) {
                    break;
                }
                state.need_full.store(true, Ordering::Release);
            }
        }
    }
}
/// `None` means the next dirty transaction may start. A wait inside the minimum
/// is not extended by new keys; after that, dirty keys hold the next commit
/// until the maximum.
fn next_commit_delay(elapsed: Duration, dirty: bool) -> Option<Duration> {
    if elapsed >= COMMIT_MAX {
        return None;
    }
    if elapsed < COMMIT_MIN {
        return Some(COMMIT_MIN.saturating_sub(elapsed));
    }
    dirty.then(|| COMMIT_MAX.saturating_sub(elapsed))
}
fn hold_for_coalesce(state: &FlushState) {
    let started = Instant::now();
    loop {
        if state.stop.load(Ordering::Acquire) || state.need_full.load(Ordering::Acquire) {
            return;
        }
        let Some(wait) = next_commit_delay(started.elapsed(), pending(state)) else {
            return;
        };
        state.wake.store(false, Ordering::Release);
        thread::park_timeout(wait);
    }
}
fn pending(state: &FlushState) -> bool {
    state
        .dirty
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .has_work()
}
fn remember(state: &FlushState, error: &std::io::Error) {
    *state
        .last_error
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(error.to_string());
}
fn closed(error: &std::io::Error) -> bool {
    error.to_string().contains("checkpoint store is closed")
        || error.to_string().contains("Raft WAL is closed")
}
fn bootstrap(inner: &Inner, state: &FlushState) {
    let header = {
        let db = inner.db.lock().unwrap_or_else(PoisonError::into_inner);
        db.as_ref().and_then(|db| db.read_meta().ok().flatten())
    };
    let ram = state
        .ram
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .last_applied;
    if let Some(meta) = header {
        state.finish(meta.last_log_id);
        let covered = meta
            .last_log_id
            .is_some_and(|have| ram.is_some_and(|ram| have == ram || have.index > ram.index))
            || (meta.last_log_id.is_none() && ram.is_none());
        if ram.is_some() && !covered {
            state.need_full.store(true, Ordering::Release);
        }
    } else if ram.is_some() {
        state.need_full.store(true, Ordering::Release);
    }
}
struct InFlight<'a>(&'a FlushState);
impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.inflight.store(false, Ordering::Release);
        self.0.notify_progress();
    }
}
fn write_dirty(inner: &Inner, state: &FlushState) -> std::io::Result<bool> {
    let epoch = state.epoch.load(Ordering::Acquire);
    let dirty = {
        let mut dirty = state.dirty.lock().unwrap_or_else(PoisonError::into_inner);
        if !dirty.has_work() {
            return Ok(false);
        }
        state.inflight.store(true, Ordering::Release);
        std::mem::take(&mut *dirty)
    };
    let _flight = InFlight(state);
    if state.epoch.load(Ordering::Acquire) != epoch || dirty.gap {
        state.need_full.store(true, Ordering::Release);
        return Ok(true);
    }
    let patch = patch_from(&dirty)?;
    let outcome = {
        let mut db = inner.db.lock().unwrap_or_else(PoisonError::into_inner);
        if state.epoch.load(Ordering::Acquire) != epoch {
            state.need_full.store(true, Ordering::Release);
            return Ok(true);
        }
        let db = db
            .as_mut()
            .ok_or_else(|| db_err("checkpoint store is closed"))?;
        db.write_patch(&patch)?
    };
    match outcome {
        super::checkpoint::PatchResult::Applied => state.finish(dirty.through),
        super::checkpoint::PatchResult::Superseded => {}
        super::checkpoint::PatchResult::Gap => state.need_full.store(true, Ordering::Release),
    }
    Ok(true)
}
fn patch_from(dirty: &Dirty) -> std::io::Result<super::checkpoint::Patch> {
    let mut rows = Vec::new();
    push_rows(&mut rows, NODE_BUCKET, &dirty.nodes, |node| {
        node.to_string()
    })?;
    push_rows(&mut rows, CLIENT_BUCKET, &dirty.clients, |key| key.clone())?;
    push_rows(&mut rows, DURABLE_BUCKET, &dirty.durable, |key| key.clone())?;
    push_rows(&mut rows, ROUTE_BUCKET, &dirty.routes, |key| key.clone())?;
    let mut shell = State {
        last_applied: dirty.through,
        membership: dirty.membership.clone(),
        metadata: dirty.metadata.clone(),
    };
    shell.metadata.nodes.clear();
    shell.metadata.clients.clear();
    shell.metadata.routes.clear();
    shell.metadata.durable.clear();
    let mut scalars = serde_json::to_value(&shell).map_err(db_err)?;
    if let Some(metadata) = scalars
        .get_mut("metadata")
        .and_then(serde_json::Value::as_object_mut)
    {
        metadata.remove("nodes");
        metadata.remove("clients");
        metadata.remove("routes");
        metadata.remove("durable");
    }
    Ok(super::checkpoint::Patch {
        since: dirty.since,
        meta: SnapshotMeta {
            last_log_id: dirty.through,
            last_membership: dirty.membership.clone(),
        },
        scalars,
        rows,
    })
}
fn push_rows<K, V: serde::Serialize>(
    rows: &mut Vec<super::checkpoint::PatchRow>,
    bucket: &'static [u8],
    changes: &std::collections::BTreeMap<K, Option<V>>,
    key: impl Fn(&K) -> String,
) -> std::io::Result<()> {
    for (id, value) in changes {
        rows.push(super::checkpoint::PatchRow {
            bucket,
            key: key(id),
            value: value.as_ref().map(encode).transpose()?,
        });
    }
    Ok(())
}
fn write_full(inner: &Inner, state: &FlushState) -> std::io::Result<()> {
    // A newer full write or install bumps the epoch. This image must not replace it.
    let epoch = state.epoch.fetch_add(1, Ordering::AcqRel) + 1;
    let image = {
        let ram = state.ram.lock().unwrap_or_else(PoisonError::into_inner);
        let image = ram.clone();
        state
            .dirty
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        image
    };
    if state.epoch.load(Ordering::Acquire) != epoch {
        state.need_full.store(true, Ordering::Release);
        return Ok(());
    }
    let _total = ProfileTimer::start(Metric::CheckpointTotal);
    let saved = super::make_snapshot(&image)?;
    {
        let mut wal = inner.wal.lock().unwrap_or_else(PoisonError::into_inner);
        super::recovery::verify_wal_tail(
            wal.as_mut().ok_or_else(|| db_err("Raft WAL is closed"))?,
            saved.meta.last_log_id,
        )?;
    }
    if state.epoch.load(Ordering::Acquire) != epoch {
        state.need_full.store(true, Ordering::Release);
        return Ok(());
    }
    {
        let mut db = inner.db.lock().unwrap_or_else(PoisonError::into_inner);
        if state.epoch.load(Ordering::Acquire) != epoch {
            state.need_full.store(true, Ordering::Release);
            return Ok(());
        }
        let db = db
            .as_mut()
            .ok_or_else(|| db_err("checkpoint store is closed"))?;
        let current = db.read_meta()?.and_then(|meta| meta.last_log_id);
        if let Some(current) = current {
            let keep_existing = saved
                .meta
                .last_log_id
                .is_none_or(|next| current.index >= next.index);
            if keep_existing {
                state.finish(Some(current));
                return Ok(());
            }
        }
        db.write_snapshot(&saved)?;
    }
    state.finish(saved.meta.last_log_id);
    Ok(())
}
impl Owner {
    pub(super) fn state(&self) -> &Arc<FlushState> {
        &self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_interval_waits_at_least_200ms_and_at_most_1s() {
        assert_eq!(next_commit_delay(Duration::ZERO, false), Some(COMMIT_MIN));
        assert_eq!(
            next_commit_delay(Duration::from_millis(199), true),
            Some(Duration::from_millis(1))
        );
        assert_eq!(next_commit_delay(COMMIT_MIN, false), None);
        assert_eq!(
            next_commit_delay(COMMIT_MIN, true),
            Some(Duration::from_millis(800))
        );
        assert_eq!(
            next_commit_delay(Duration::from_millis(500), true),
            Some(Duration::from_millis(500))
        );
        assert_eq!(next_commit_delay(COMMIT_MAX, true), None);
        assert_eq!(
            next_commit_delay(COMMIT_MAX + Duration::from_millis(1), true),
            None
        );
    }
}
