//! Checkpoint validation and committed-only log replay, executed on blocking IO workers.
use super::*;
use sha2::{Digest, Sha256};
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
pub(super) fn recover_legacy(db: &mut Connection) -> io::Result<State> {
    let tx = db.transaction().map_err(db_err)?;
    log::migrate_and_validate(&tx)?;
    let _: Option<Vote<u64>> = get(&tx, "vote")?;
    let purged: Option<LogId<u64>> = get(&tx, "purged")?;
    let checkpoint = checkpoint(&tx)?.last_applied;
    if purged.is_some_and(|purged| {
        checkpoint.is_none_or(|floor| {
            floor.index < purged.index || (floor.index == purged.index && floor != purged)
        })
    }) {
        return Err(db_err(
            "legacy purged prefix has no matching verified checkpoint",
        ));
    }
    let state = recover_committed(&tx)?;
    tx.commit().map_err(db_err)?;
    Ok(state)
}
fn recover_committed(db: &Connection) -> io::Result<State> {
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
                let bytes = log::row_bytes(row, 1).map_err(db_err)?;
                let entry = log::decode_entry(idx, &bytes)?;
                if idx < 0 || idx as u64 != expected || entry.log_id.index != expected {
                    return Err(db_err("committed log gap or index mismatch"));
                }
                if entry.log_id.index == committed.index && entry.log_id != committed {
                    return Err(db_err("committed log identity mismatch"));
                }
                state.apply(entry);
                expected = expected
                    .checked_add(1)
                    .ok_or_else(|| db_err("log index overflow"))?;
            }
            if expected != committed.index.saturating_add(1) {
                return Err(db_err("missing committed log tail"));
            }
        }
    }
    if committed != recorded {
        put(db, "committed", &committed)?;
    }
    Ok(state)
}
/// Recover only durable committed input after a verified SQLite authority floor.
pub(super) fn recover_wal(db: &Connection, wal: &mut wal::Wal) -> io::Result<State> {
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
pub(super) fn applied_input_durable(
    wal: &mut wal::Wal,
    entries: &[Entry<Types>],
) -> io::Result<bool> {
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
pub(super) fn verify_wal_tail(wal: &mut wal::Wal, floor: Option<LogId<u64>>) -> io::Result<()> {
    visit_tail(wal, floor, |_| Ok(()))
}
fn visit_tail(
    wal: &mut wal::Wal,
    floor: Option<LogId<u64>>,
    mut visit: impl FnMut(Entry<Types>) -> io::Result<()>,
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
