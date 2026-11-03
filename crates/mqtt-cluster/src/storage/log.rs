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
