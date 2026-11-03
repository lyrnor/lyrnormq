//! Checkpoint validation and committed-only log replay, executed on blocking IO workers.
use super::*;
use sha2::{Digest, Sha256};
const FORMAT: u64 = 1;
pub(super) const MAX_SNAPSHOT: usize = 16 * 1024 * 1024;

pub(super) fn snapshot_digest(
    meta: &SnapshotMeta<u64, BasicNode>,
    data: &[u8],
) -> io::Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(encode(meta)?);
    hasher.update(data);
    Ok(format!("{:x}", hasher.finalize()))
}
pub(super) fn put_snapshot(db: &Connection, saved: &SavedSnapshot) -> io::Result<()> {
    put(db, "snapshot", saved)?;
    put(db, "snapshot_record_version", &saved.format)
}
pub(super) fn read_saved_snapshot(db: &Connection) -> io::Result<Option<SavedSnapshot>> {
    let saved: Option<SavedSnapshot> = get(db, "snapshot")?;
    let version: Option<u64> = get(db, "snapshot_record_version")?;
    match (&saved, version) {
        (None, None) => {}
        (Some(saved), None) if saved.format == 0 && saved.digest.is_none() => {}
        (Some(saved), Some(version)) if saved.format == version && version <= 1 => {}
        _ => return Err(db_err("snapshot record format/marker mismatch")),
    }
    Ok(saved)
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
pub(super) fn checkpoint(db: &Connection) -> io::Result<State> {
    let legacy: Option<State> = get(db, "state")?;
    let snapshot = read_saved_snapshot(db)?;
    let mut state = legacy.unwrap_or_default();
    if let Some(snapshot) = snapshot {
        let candidate = snapshot_state(&snapshot)?;
        if candidate.last_applied.map(|id| id.index) >= state.last_applied.map(|id| id.index) {
            if candidate.last_applied.map(|id| id.index) == state.last_applied.map(|id| id.index)
                && candidate.last_applied != state.last_applied
            {
                return Err(db_err("checkpoint identity conflict"));
            }
            if candidate.last_applied == state.last_applied
                && encode(&candidate)? != encode(&state)?
            {
                return Err(db_err("checkpoint state conflict"));
            }
            state = candidate;
        }
    }
    Ok(state)
}
pub(super) fn recover(db: &mut Connection) -> io::Result<State> {
    if get::<u64>(db, "control_log_format")?.is_some_and(|format| format != FORMAT) {
        return Err(db_err("unsupported Controller log format"));
    }
    let mut state = checkpoint(db)?;
    let checkpoint = state.last_applied;
    let recorded = get::<Option<LogId<u64>>>(db, "committed")?.flatten();
    let committed = match (recorded, checkpoint) {
        (Some(a), Some(b)) if a.index == b.index && a != b => {
            return Err(db_err("committed/checkpoint identity conflict"));
        }
        (Some(a), Some(b)) => Some(if a.index >= b.index { a } else { b }),
        (a, b) => a.or(b),
    };
    if let Some(committed) = committed {
        let start = checkpoint.map_or(0, |id| id.index.saturating_add(1));
        if checkpoint.is_none_or(|id| id.index < committed.index) {
            let mut expected = start;
            let mut statement = db
                .prepare("SELECT idx,data FROM logs WHERE idx>=?1 AND idx<=?2 ORDER BY idx")
                .map_err(db_err)?;
            let mut rows = statement
                .query(params![
                    i64::try_from(start).map_err(db_err)?,
                    i64::try_from(committed.index).map_err(db_err)?
                ])
                .map_err(db_err)?;
            while let Some(row) = rows.next().map_err(db_err)? {
                let idx: i64 = row.get(0).map_err(db_err)?;
                let bytes: Vec<u8> = row.get(1).map_err(db_err)?;
                let entry: Entry<Types> = decode(&bytes)?;
                if idx < 0 || idx as u64 != expected || entry.log_id.index != expected {
                    return Err(db_err("committed log gap or index mismatch"));
                }
                if entry.log_id.index == committed.index && entry.log_id != committed {
                    return Err(db_err("committed log identity mismatch"));
                }
                state.apply_tracked(entry);
                expected = expected
                    .checked_add(1)
                    .ok_or_else(|| db_err("log index overflow"))?;
            }
            if expected != committed.index.saturating_add(1) {
                return Err(db_err("missing committed log tail"));
            }
        }
    }
    let tx = db.transaction().map_err(db_err)?;
    put(&tx, "control_log_format", &FORMAT)?;
    if committed != recorded {
        put(&tx, "committed", &committed)?;
    }
    tx.commit().map_err(db_err)?;
    Ok(state)
}
/// Proper Raft calls have already flushed logs and saved the commit watermark.
/// Direct state-machine callers must instead persist a checkpoint; they must not
/// inject state-machine entries into the independent Raft log-store interface.
pub(super) fn applied_input_durable(db: &Connection, entries: &[Entry<Types>]) -> io::Result<bool> {
    let Some(last) = entries.last().map(|entry| entry.log_id) else {
        return Ok(true);
    };
    let committed = get::<Option<LogId<u64>>>(db, "committed")?.flatten();
    if committed.is_none_or(|id| id.index < last.index) {
        return Ok(false);
    }
    if committed.is_some_and(|id| id.index == last.index && id != last) {
        return Err(db_err("applied committed identity conflict"));
    }
    for entry in entries {
        let idx = i64::try_from(entry.log_id.index).map_err(db_err)?;
        let bytes: Option<Vec<u8>> = db
            .query_row("SELECT data FROM logs WHERE idx=?1", [idx], |r| r.get(0))
            .optional()
            .map_err(db_err)?;
        match bytes {
            Some(bytes) if bytes == encode(entry)? => {}
            None => return Ok(false),
            Some(_) => return Err(db_err("applied input conflicts with durable log")),
        }
    }
    Ok(true)
}

/// A checkpoint cannot acknowledge a state whose higher persisted commit marker
/// refers to missing input. This also protects unusual direct trait callers.
pub(super) fn verify_remaining_tail(db: &Connection, floor: Option<LogId<u64>>) -> io::Result<()> {
    let Some(committed) = get::<Option<LogId<u64>>>(db, "committed")?.flatten() else {
        return Ok(());
    };
    if floor.is_some_and(|id| id.index >= committed.index) {
        return Ok(());
    }
    let mut expected = floor.map_or(0, |id| id.index.saturating_add(1));
    let mut statement = db
        .prepare("SELECT idx,data FROM logs WHERE idx>=?1 AND idx<=?2 ORDER BY idx")
        .map_err(db_err)?;
    let mut rows = statement
        .query(params![
            i64::try_from(expected).map_err(db_err)?,
            i64::try_from(committed.index).map_err(db_err)?
        ])
        .map_err(db_err)?;
    while let Some(row) = rows.next().map_err(db_err)? {
        let index: i64 = row.get(0).map_err(db_err)?;
        let bytes: Vec<u8> = row.get(1).map_err(db_err)?;
        let entry: Entry<Types> = decode(&bytes)?;
        if index < 0
            || index as u64 != expected
            || entry.log_id.index != expected
            || (expected == committed.index && entry.log_id != committed)
        {
            return Err(db_err("checkpoint committed tail mismatch"));
        }
        expected = expected
            .checked_add(1)
            .ok_or_else(|| db_err("log index overflow"))?;
    }
    if expected != committed.index.saturating_add(1) {
        return Err(db_err("checkpoint missing committed tail"));
    }
    Ok(())
}
