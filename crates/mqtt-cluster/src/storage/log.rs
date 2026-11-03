//! Private on-disk envelopes; public Raft Entry and SQLite table layouts stay unchanged.
use super::*;
use sha2::{Digest, Sha256};
pub(super) const FORMAT: u64 = 2;
const MAX_RECORD: usize = 1024 * 1024;
#[derive(Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredEntry<T> {
    format: u64,
    entry: T,
    digest: String,
}
fn digest(entry: &Entry<Types>) -> io::Result<String> {
    let mut hash = Sha256::new();
    hash.update(b"rmqtt-controller-log-v2\0");
    hash.update(encode(entry)?);
    Ok(format!("{:x}", hash.finalize()))
}
pub(super) fn encode_entry(entry: &Entry<Types>) -> io::Result<Vec<u8>> {
    let data = encode(&StoredEntry {
        format: FORMAT,
        entry,
        digest: digest(entry)?,
    })?;
    if data.len() > MAX_RECORD {
        return Err(db_err("Controller log record exceeds limit"));
    }
    Ok(data)
}
pub(super) fn decode_entry(index: i64, data: &[u8]) -> io::Result<Entry<Types>> {
    if index < 0 || data.len() > MAX_RECORD {
        return Err(db_err("Controller log record bounds"));
    }
    let saved: StoredEntry<Entry<Types>> = decode(data)?;
    if saved.format != FORMAT || saved.digest != digest(&saved.entry)? {
        return Err(db_err("Controller log checksum/version mismatch"));
    }
    if saved.entry.log_id.index != index as u64 {
        return Err(db_err("Controller log row/index mismatch"));
    }
    Ok(saved.entry)
}
pub(super) fn row_bytes(row: &rusqlite::Row<'_>, column: usize) -> rusqlite::Result<Vec<u8>> {
    let raw = row.get_ref(column)?;
    let bytes = raw.as_blob().map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(column, raw.data_type(), Box::new(e))
    })?;
    if bytes.len() > MAX_RECORD {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            column,
            raw.data_type(),
            Box::new(db_err("Controller log record exceeds limit")),
        ));
    }
    Ok(bytes.to_vec())
}
/// Caller owns a SQLite transaction. Check/wrap all rows before publishing format2.
/// Legacy bytes have no authentication; migration validates structure/identity only.
pub(super) fn migrate_and_validate(db: &Connection) -> io::Result<()> {
    let legacy = match get::<u64>(db, "control_log_format")? {
        None | Some(1) => true,
        Some(FORMAT) => false,
        Some(_) => return Err(db_err("unsupported Controller log format")),
    };
    if !legacy_logs_exist(db)? {
        return Ok(());
    }
    let purged = get::<LogId<u64>>(db, "purged")?;
    let mut start = i64::MIN;
    loop {
        let page = {
            let mut statement = db
                .prepare("SELECT idx,data FROM logs WHERE idx>=?1 ORDER BY idx LIMIT 16")
                .map_err(db_err)?;
            statement
                .query_map([start], |row| {
                    Ok((row.get::<_, i64>(0)?, row_bytes(row, 1)?))
                })
                .map_err(db_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(db_err)?
        };
        if page.is_empty() {
            break;
        }
        let mut next = None;
        for (index, bytes) in page {
            if index < 0 || purged.is_some_and(|id| index as u64 <= id.index) {
                return Err(db_err("legacy log contains invalid or purged indices"));
            }
            if legacy {
                let entry: Entry<Types> = decode(&bytes)?;
                if index < 0 || entry.log_id.index != index as u64 {
                    return Err(db_err("legacy Controller log row/index mismatch"));
                }
                db.execute(
                    "UPDATE logs SET data=?1 WHERE idx=?2",
                    params![encode_entry(&entry)?, index],
                )
                .map_err(db_err)?;
            } else {
                decode_entry(index, &bytes)?;
            }
            next = index.checked_add(1);
        }
        let Some(next) = next else {
            break;
        };
        start = next;
    }
    if legacy {
        put(db, "control_log_format", &FORMAT)?;
    }
    Ok(())
}

const WAL_MIGRATION: u64 = 1;
const MIGRATION_KEY: &str = "raft_wal_migration";

/// Import while holding the controller directory lock. A published WAL is never
/// discarded or reconstructed from the inert backup once its marker is durable.
pub(super) fn open_or_migrate(
    db: &mut Connection,
    path: &Path,
    cluster: &str,
    node: u64,
) -> io::Result<wal::Wal> {
    let destination = path.join("raft-wal");
    match get::<u64>(db, MIGRATION_KEY)? {
        Some(WAL_MIGRATION) => {
            if !destination.is_dir() {
                return Err(db_err("migrated Raft WAL is missing"));
            }
            return wal::Wal::open(&destination, cluster, node);
        }
        Some(_) => return Err(db_err("unsupported Raft WAL migration marker")),
        None => {}
    }
    // Validate every legacy row and the entire committed tail before touching
    // any staging output. Transactional raw-envelope upgrades remain compatible.
    recovery::recover_legacy(db)?;
    let mut wal = if destination.exists() {
        // Crash between rename+directory sync and SQLite marker: only an exact
        // completed import may be adopted. Never silently replace this directory.
        let mut wal = wal::Wal::open(&destination, cluster, node)?;
        validate_import(db, &mut wal)?;
        wal
    } else {
        let staging = path.join("raft-wal.staging");
        if staging.exists() {
            std::fs::remove_dir_all(&staging)?;
        }
        let mut staged = wal::Wal::open(&staging, cluster, node)?;
        if let Some(purged) = get::<LogId<u64>>(db, "purged")? {
            staged.purge(purged)?;
        }
        for_each_page(db, |entries| staged.append(entries))?;
        if let Some(vote) = get::<Vote<u64>>(db, "vote")? {
            staged.save_vote(vote)?;
        }
        staged.save_committed(get::<Option<LogId<u64>>>(db, "committed")?.flatten())?;
        validate_import(db, &mut staged)?;
        drop(staged);
        std::fs::File::open(&staging)?.sync_all()?;
        std::fs::rename(&staging, &destination)?;
        std::fs::File::open(path)?.sync_all()?;
        wal::Wal::open(&destination, cluster, node)?
    };
    // Verify opening and committed replay before marking the source inert.
    recovery::recover_wal(db, &mut wal)?;
    put(db, MIGRATION_KEY, &WAL_MIGRATION)?;
    Ok(wal)
}
fn for_each_page(
    db: &Connection,
    mut visit: impl FnMut(Vec<Entry<Types>>) -> io::Result<()>,
) -> io::Result<()> {
    if !legacy_logs_exist(db)? {
        return Ok(());
    }
    let purged = get::<LogId<u64>>(db, "purged")?;
    let mut start = 0i64;
    loop {
        let page = {
            let mut statement = db
                .prepare("SELECT idx,data FROM logs WHERE idx>=?1 ORDER BY idx LIMIT 16")
                .map_err(db_err)?;
            statement
                .query_map([start], |row| {
                    Ok((row.get::<_, i64>(0)?, row_bytes(row, 1)?))
                })
                .map_err(db_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(db_err)?
        };
        if page.is_empty() {
            return Ok(());
        }
        let mut entries = Vec::with_capacity(page.len());
        let mut next = None;
        for (index, bytes) in page {
            if purged.is_some_and(|id| index as u64 <= id.index) {
                return Err(db_err("legacy log contains purged entries"));
            }
            entries.push(decode_entry(index, &bytes)?);
            next = index.checked_add(1);
        }
        visit(entries)?;
        let Some(next) = next else {
            return Ok(());
        };
        start = next;
    }
}
fn validate_import(db: &Connection, wal: &mut wal::Wal) -> io::Result<()> {
    let purged = get::<LogId<u64>>(db, "purged")?;
    if wal.vote()? != get::<Vote<u64>>(db, "vote")?
        || wal.committed()? != get::<Option<LogId<u64>>>(db, "committed")?.flatten()
        || wal.state()?.last_purged_log_id != purged
    {
        return Err(db_err(
            "published Raft WAL differs from legacy import watermarks",
        ));
    }
    let mut last = purged;
    let mut cursor = purged.map_or(0, |id| id.index.saturating_add(1));
    for_each_page(db, |entries| {
        let end = entries
            .last()
            .ok_or_else(|| db_err("empty import page"))?
            .log_id
            .index;
        let saved = wal.entries(cursor, end)?;
        if encode(&saved)? != encode(&entries)? {
            return Err(db_err(
                "published Raft WAL differs from legacy import entries",
            ));
        }
        last = entries.last().map(|entry| entry.log_id);
        cursor = end.saturating_add(1);
        Ok(())
    })?;
    if wal.state()?.last_log_id != last {
        return Err(db_err(
            "published Raft WAL has extra or missing import entries",
        ));
    }
    Ok(())
}

fn legacy_logs_exist(db: &Connection) -> io::Result<bool> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='logs')",
        [],
        |row| row.get(0),
    )
    .map_err(db_err)
}

/// OpenRaft's log-store append permits repeated prefixes and conflict replacement;
/// raft-log itself is deliberately strict append-only. Never remove a committed
/// suffix unless its complete input is already covered by a durable checkpoint.
pub(super) fn append_checked(
    wal: &mut wal::Wal,
    db: &Mutex<Option<Connection>>,
    mut entries: Vec<Entry<Types>>,
) -> io::Result<()> {
    let state = wal.state()?;
    if let Some(purged) = state.last_purged_log_id {
        if entries
            .iter()
            .any(|entry| entry.log_id.index == purged.index && entry.log_id != purged)
        {
            return Err(db_err("append conflicts with purged Raft identity"));
        }
        // Retransmitted compacted prefixes are already represented by the
        // snapshot. Acknowledge without resurrecting them in the log store.
        entries.retain(|entry| entry.log_id.index > purged.index);
    }
    let Some(first) = entries.first() else {
        return Ok(());
    };
    if state
        .last_log_id
        .is_none_or(|last| first.log_id.index > last.index)
    {
        return wal.append(entries);
    }
    let mut previous: Option<LogId<u64>> = None;
    for entry in &entries {
        i64::try_from(entry.log_id.index).map_err(db_err)?;
        encode_entry(entry)?;
        if previous.is_some_and(|old| {
            old.index.checked_add(1) != Some(entry.log_id.index) || entry.log_id < old
        }) {
            return Err(db_err("append batch has reversed or noncontiguous entries"));
        }
        previous = Some(entry.log_id);
    }
    let mut same = 0;
    for entry in &entries {
        let saved = wal.entries(entry.log_id.index, entry.log_id.index)?;
        if matches!(saved.as_slice(), [old] if encode(old)? == encode(entry)?) {
            same += 1;
        } else {
            break;
        }
    }
    if same == entries.len() {
        return Ok(());
    }
    entries = entries.split_off(same);
    let first = entries
        .first()
        .ok_or_else(|| db_err("empty replacement suffix"))?
        .log_id;
    if state
        .last_log_id
        .is_some_and(|last| first.index <= last.index)
    {
        let committed = wal.committed()?;
        let checkpoint = {
            let db = db.lock().unwrap_or_else(PoisonError::into_inner);
            recovery::checkpoint(
                db.as_ref()
                    .ok_or_else(|| db_err("checkpoint store is closed"))?,
            )?
            .last_applied
        };
        if committed.is_some_and(|last| {
            last.index >= first.index && checkpoint.is_none_or(|covered| covered.index < last.index)
        }) {
            return Err(db_err(
                "append would overwrite unsnapshotted committed entries",
            ));
        }
        wal.truncate(first)?;
    }
    wal.append(entries)
}
