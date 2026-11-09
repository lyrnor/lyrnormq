//! Stable checksummed Raft Entry envelopes and guarded suffix replacement.
use super::*;
use crate::profile::{Metric, Timer};
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
fn digest(entry: &Entry) -> io::Result<String> {
    let mut hash = Sha256::new();
    hash.update(b"lyrnormq-controller-log-v2\0");
    hash.update(encode(entry)?);
    Ok(format!("{:x}", hash.finalize()))
}
pub(super) fn encode_entry(entry: &Entry) -> io::Result<Vec<u8>> {
    let _encode = Timer::start(Metric::WalEncode);
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
pub(super) fn decode_entry(index: i64, data: &[u8]) -> io::Result<Entry> {
    if index < 0 || data.len() > MAX_RECORD {
        return Err(db_err("Controller log record bounds"));
    }
    let saved: StoredEntry<Entry> = decode(data)?;
    if saved.format != FORMAT || saved.digest != digest(&saved.entry)? {
        return Err(db_err("Controller log checksum/version mismatch"));
    }
    if saved.entry.log_id.index != index as u64 {
        return Err(db_err("Controller log row/index mismatch"));
    }
    Ok(saved.entry)
}
/// OpenRaft's log-store append permits repeated prefixes and conflict replacement;
/// raft-log itself is deliberately strict append-only. Never remove a committed
/// suffix unless its complete input is already covered by a durable checkpoint.
pub(super) fn append_checked(
    wal: &mut wal::Wal,
    db: &Mutex<Option<checkpoint::Checkpoint>>,
    mut entries: Vec<Entry>,
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
    let mut previous: Option<LogId> = None;
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
            db.as_ref()
                .ok_or_else(|| db_err("checkpoint store is closed"))?
                .read_meta()?
                .and_then(|meta| meta.last_log_id)
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
