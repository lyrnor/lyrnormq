//! Checkpoint validation and committed-only log replay, executed on blocking IO workers.
use super::*;
use sha2::{Digest, Sha256};
pub(super) const MAX_SNAPSHOT: usize = 16 * 1024 * 1024;

pub(super) fn snapshot_digest(meta: &SnapshotMeta, data: &[u8]) -> io::Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(encode(meta)?);
    hasher.update(data);
    Ok(format!("{:x}", hasher.finalize()))
}
pub(super) fn snapshot_state(saved: &SavedSnapshot) -> io::Result<State> {
    if saved.data.len() > MAX_SNAPSHOT {
        return Err(db_err("snapshot exceeds limit"));
    }
    match (saved.format, saved.digest.as_ref()) {
        (0, None) => {}
        (1, Some(digest)) if *digest == snapshot_digest(&saved.meta, &saved.data)? => {}
        _ => return Err(db_err("snapshot checksum or version mismatch")),
    }
    let state: State = decode(&saved.data)?;
    if state.last_applied != saved.meta.last_log_id
        || state.membership != saved.meta.last_membership
    {
        return Err(db_err("snapshot checkpoint metadata mismatch"));
    }
    Ok(state)
}
pub(super) fn checkpoint(db: &checkpoint::Checkpoint) -> io::Result<State> {
    db.read_snapshot()?
        .as_ref()
        .map(snapshot_state)
        .transpose()
        .map(Option::unwrap_or_default)
}
/// Recover only durable committed input after a verified bbolt authority floor.
pub(super) fn recover_wal(db: &checkpoint::Checkpoint, wal: &mut wal::Wal) -> io::Result<State> {
    let mut state = checkpoint(db)?;
    let floor = state.last_applied;
    let recorded = wal.committed()?;
    if let (Some(a), Some(b)) = (recorded, floor)
        && a.index == b.index
        && a != b
    {
        return Err(db_err("committed/checkpoint identity conflict"));
    }
    visit_tail(wal, floor, |entry| {
        state.apply(entry);
        Ok(())
    })?;
    // A crash after checkpoint commit but before advancing the WAL is recoverable.
    if floor.map(|id| id.index) > recorded.map(|id| id.index) {
        wal.save_committed(floor)?;
    }
    Ok(state)
}
/// Proper Raft calls have flushed logs and committed; direct trait callers must
/// checkpoint instead, never inject entries into the independent log-store API.
pub(super) fn applied_input_durable(wal: &mut wal::Wal, entries: &[Entry]) -> io::Result<bool> {
    let Some(last) = entries.last().map(|entry| entry.log_id) else {
        return Ok(true);
    };
    let committed = wal.committed()?;
    if committed.is_none_or(|id| id.index < last.index) {
        return Ok(false);
    }
    if committed.is_some_and(|id| id.index == last.index && id != last) {
        return Err(db_err("applied committed identity conflict"));
    }
    for entry in entries {
        let saved = wal.entries(entry.log_id.index, entry.log_id.index)?;
        match saved.as_slice() {
            [saved] if encode(saved)? == encode(entry)? => {}
            [] => return Ok(false),
            _ => return Err(db_err("applied input conflicts with durable log")),
        }
    }
    Ok(true)
}
pub(super) fn verify_wal_tail(wal: &mut wal::Wal, floor: Option<LogId>) -> io::Result<()> {
    visit_tail(wal, floor, |_| Ok(()))
}
fn visit_tail(
    wal: &mut wal::Wal,
    floor: Option<LogId>,
    mut visit: impl FnMut(Entry) -> io::Result<()>,
) -> io::Result<()> {
    let Some(committed) = wal.committed()? else {
        return Ok(());
    };
    if let Some(floor) = floor {
        if floor.index == committed.index && floor != committed {
            return Err(db_err("checkpoint committed identity mismatch"));
        }
        if floor.index >= committed.index {
            return Ok(());
        }
    }
    let mut expected = match floor {
        Some(id) => id
            .index
            .checked_add(1)
            .ok_or_else(|| db_err("log index overflow"))?,
        None => 0,
    };
    loop {
        let end = expected.saturating_add(15).min(committed.index);
        let entries = wal.entries(expected, end)?;
        for entry in entries {
            if entry.log_id.index != expected
                || (expected == committed.index && entry.log_id != committed)
            {
                return Err(db_err("committed WAL gap or identity mismatch"));
            }
            visit(entry)?;
            if expected == committed.index {
                return Ok(());
            }
            expected = expected
                .checked_add(1)
                .ok_or_else(|| db_err("log index overflow"))?;
        }
        if expected <= end {
            return Err(db_err("missing committed WAL tail"));
        }
    }
}

const EXCHANGE: &str = "controller-checkpoint.export.json";
const REQUIRED: &str = "controller.bbolt.required";
const STAGING: &str = "controller-checkpoint.staging";
const MAX_EXCHANGE: u64 = 128 * 1024 * 1024;
#[derive(Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Required {
    format: u64,
    identity: (String, u64),
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Exchange {
    format: u64,
    identity: (String, u64),
    #[serde(default)]
    state: Option<State>,
    #[serde(default)]
    snapshot: Option<SavedSnapshot>,
    #[serde(default)]
    snapshot_record_version: Option<u64>,
}
fn bounded_file(path: &Path, limit: u64) -> io::Result<Vec<u8>> {
    use std::io::Read;
    let meta = std::fs::symlink_metadata(path)?;
    if !meta.is_file() || meta.file_type().is_symlink() || meta.len() > limit {
        return Err(db_err("invalid bounded controller record"));
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(db_err("controller record exceeds limit"));
    }
    Ok(bytes)
}
fn present(path: &Path) -> io::Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}
fn required(path: &Path, cluster: &str, node: u64) -> io::Result<bool> {
    let file = path.join(REQUIRED);
    match std::fs::symlink_metadata(&file) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
        Ok(_) => {}
    }
    let marker: Required = decode(&bounded_file(&file, 4096)?)?;
    if marker.format != 1 || marker.identity != (cluster.to_owned(), node) {
        return Err(db_err(
            "checkpoint ownership marker identity/version mismatch",
        ));
    }
    Ok(true)
}
fn publish_required(path: &Path, cluster: &str, node: u64) -> io::Result<()> {
    use std::io::Write;
    let data = encode(&Required {
        format: 1,
        identity: (cluster.to_owned(), node),
    })?;
    if data.len() > 4096 {
        return Err(db_err("checkpoint marker exceeds limit"));
    }
    let temp = path.join("controller.bbolt.required.tmp");
    if temp.exists() {
        std::fs::remove_file(&temp)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)?;
    file.write_all(&data)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(temp, path.join(REQUIRED))?;
    std::fs::File::open(path)?.sync_all()
}
fn exchange_state(path: &Path, cluster: &str, node: u64) -> io::Result<Option<State>> {
    let exchange: Exchange = decode(&bounded_file(path, MAX_EXCHANGE)?)?;
    if exchange.format != 1 || exchange.identity != (cluster.to_owned(), node) {
        return Err(db_err("checkpoint exchange identity/version mismatch"));
    }
    match (&exchange.snapshot, exchange.snapshot_record_version) {
        (None, None) => {}
        (Some(saved), None) if saved.format == 0 && saved.digest.is_none() => {}
        (Some(saved), Some(version)) if saved.format == version && version <= 1 => {}
        _ => return Err(db_err("snapshot exchange format/marker mismatch")),
    }
    let mut state = exchange.state;
    if let Some(saved) = exchange.snapshot {
        let candidate = snapshot_state(&saved)?;
        if let Some(previous) = &state
            && previous.last_applied.map(|id| id.index) == candidate.last_applied.map(|id| id.index)
            && (previous.last_applied != candidate.last_applied
                || encode(previous)? != encode(&candidate)?)
        {
            return Err(db_err(
                "exchange checkpoints conflict at the same applied index",
            ));
        }
        if state.as_ref().is_none_or(|previous| {
            previous.last_applied.map(|id| id.index) <= candidate.last_applied.map(|id| id.index)
        }) {
            state = Some(candidate);
        }
    }
    // Canonical snapshot validation/bounds also apply to unsigned legacy State.
    if let Some(state) = &state {
        make_snapshot(state)?;
    }
    Ok(state)
}
/// Called only under the exclusive controller directory lock on a blocking worker.
pub(super) fn open_stores(
    path: &Path,
    cluster: &str,
    node: u64,
) -> io::Result<(checkpoint::Checkpoint, wal::Wal, State)> {
    let marker = required(path, cluster, node)?;
    let db_path = path.join("controller.bbolt");
    let wal_path = path.join("raft-wal");
    if present(&db_path)? {
        if !wal_path.is_dir() {
            return Err(db_err("existing checkpoint is missing its Raft WAL"));
        }
        let db = checkpoint::Checkpoint::open(path, cluster, node)?;
        let mut wal = wal::Wal::open(&wal_path, cluster, node)?;
        let state = recover_wal(&db, &mut wal)?;
        if !marker {
            publish_required(path, cluster, node)?;
        }
        return Ok((db, wal, state));
    }
    if marker {
        return Err(db_err("required controller checkpoint is missing"));
    }
    let exchange = path.join(EXCHANGE);
    if present(&exchange)? {
        if !wal_path.is_dir() {
            return Err(db_err("checkpoint exchange requires existing Raft WAL"));
        }
        let chosen = exchange_state(&exchange, cluster, node)?;
        let mut wal = wal::Wal::open(&wal_path, cluster, node)?;
        let staging = path.join(STAGING);
        // Validate the exported authority against the live durable tail before
        // replacing any incomplete staging output; retain the legacy source.
        let floor = chosen.as_ref().and_then(|state| state.last_applied);
        let purged = wal.state()?.last_purged_log_id;
        if purged.is_some_and(|purged| {
            floor.is_none_or(|floor| {
                floor.index < purged.index || (floor.index == purged.index && floor != purged)
            })
        }) {
            return Err(db_err("exchange does not cover purged Raft prefix"));
        }
        verify_wal_tail(&mut wal, floor)?;
        if staging.exists() {
            std::fs::remove_dir_all(&staging)?;
        }
        std::fs::create_dir(&staging)?;
        let mut db = checkpoint::Checkpoint::open(&staging, cluster, node)?;
        if let Some(state) = chosen {
            db.write_snapshot(&make_snapshot(&state)?)?;
        }
        let state = recover_wal(&db, &mut wal)?;
        drop(db);
        std::fs::File::open(staging.join("controller.bbolt"))?.sync_all()?;
        std::fs::File::open(&staging)?.sync_all()?;
        std::fs::rename(staging.join("controller.bbolt"), &db_path)?;
        std::fs::File::open(path)?.sync_all()?;
        std::fs::remove_dir(&staging)?;
        let db = checkpoint::Checkpoint::open(path, cluster, node)?;
        publish_required(path, cluster, node)?;
        return Ok((db, wal, state));
    }
    if present(&wal_path)? || present(&path.join("metadata.sqlite"))? {
        return Err(db_err(
            "existing controller state requires an explicit offline checkpoint export",
        ));
    }
    let db = checkpoint::Checkpoint::open(path, cluster, node)?;
    let mut wal = wal::Wal::open(&wal_path, cluster, node)?;
    let state = recover_wal(&db, &mut wal)?;
    publish_required(path, cluster, node)?;
    Ok((db, wal, state))
}
