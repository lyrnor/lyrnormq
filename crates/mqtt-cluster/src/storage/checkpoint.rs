//! Bounded, checksummed keyed business-state checkpoints. All operations block.
use super::{SavedSnapshot, State, decode, encode, recovery};
use crate::model::{LogId, SnapshotMeta};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io,
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};
const FILE_LIMIT: u64 = 256 * 1024 * 1024;
const METADATA_LIMIT: usize = 16 * 1024 * 1024 - 64 * 1024;
const RECORD_LIMIT: usize = recovery::MAX_SNAPSHOT + 64 * 1024;
const TRANSACTION_LIMIT: usize = 64 * 1024 * 1024;
const BUCKETS: [&[u8]; 5] = [b"metadata", b"nodes", b"clients", b"routes", b"durable"];
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    format: u64,
    meta: crate::model::SnapshotMeta,
    digest: String,
    scalars: serde_json::Value,
}
pub(super) struct Checkpoint {
    db: bbolt::Db,
    poisoned: AtomicBool,
    #[cfg(test)]
    fail_next_write: bool,
}
fn bad(message: impl std::fmt::Display) -> bbolt::Error {
    bbolt::Error::Corrupt(message.to_string())
}
fn io_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}
fn record(bucket: &[u8], key: &[u8], data: &[u8]) -> bbolt::Result<Vec<u8>> {
    if key.is_empty() || key.len() > 65538 || data.len() > RECORD_LIMIT {
        return Err(bad("checkpoint record bounds"));
    }
    let mut hash = Sha256::new();
    hash.update(b"rmqtt-bbolt-v1\0");
    hash.update((bucket.len() as u64).to_be_bytes());
    hash.update(bucket);
    hash.update((key.len() as u64).to_be_bytes());
    hash.update(key);
    hash.update(data);
    let mut out = Vec::with_capacity(32 + data.len());
    out.extend_from_slice(&hash.finalize());
    out.extend_from_slice(data);
    Ok(out)
}
fn unpack<'a>(bucket: &[u8], key: &[u8], value: &'a [u8]) -> bbolt::Result<&'a [u8]> {
    if value.len() < 32 || value.len() > RECORD_LIMIT + 32 {
        return Err(bad("checkpoint record size"));
    }
    let data = &value[32..];
    if record(bucket, key, data)? != value {
        return Err(bad("checkpoint record checksum"));
    }
    Ok(data)
}
fn business_key(key: &[u8]) -> Vec<u8> {
    Sha256::digest(key).to_vec()
}
fn business_record(bucket: &[u8], key: &[u8], data: &[u8]) -> bbolt::Result<Vec<u8>> {
    let checked = record(bucket, key, data)?;
    let mut value = Vec::with_capacity(4 + key.len() + checked.len());
    value.extend_from_slice(&(key.len() as u32).to_be_bytes());
    value.extend_from_slice(key);
    value.extend_from_slice(&checked);
    Ok(value)
}
fn fold_piece(bucket: &[u8], physical: &[u8], value: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"rmqtt-bbolt-fold-v1\0");
    hash.update((bucket.len() as u64).to_be_bytes());
    hash.update(bucket);
    hash.update((physical.len() as u64).to_be_bytes());
    hash.update(physical);
    hash.update(value);
    hash.finalize().into()
}
fn xor_fold(fold: &mut [u8; 32], piece: &[u8; 32]) {
    for (left, right) in fold.iter_mut().zip(piece) {
        *left ^= *right;
    }
}
fn encode_fold(fold: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in fold {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
fn hex_value(byte: u8) -> bbolt::Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(bad("checkpoint fold")),
    }
}
fn decode_fold(text: &str) -> bbolt::Result<[u8; 32]> {
    let bytes = text.as_bytes();
    if bytes.len() != 64 {
        return Err(bad("checkpoint fold"));
    }
    let mut out = [0u8; 32];
    for (index, slot) in out.iter_mut().enumerate() {
        let high = hex_value(bytes[index * 2])?;
        let low = hex_value(bytes[index * 2 + 1])?;
        *slot = (high << 4) | low;
    }
    Ok(out)
}
fn scan_fold(tx: &bbolt::Tx) -> bbolt::Result<[u8; 32]> {
    let mut fold = [0u8; 32];
    for name in BUCKETS.into_iter().skip(1) {
        required(tx, name)?.for_each(|key, value| {
            let value = value.ok_or_else(|| bad("nested business bucket"))?;
            xor_fold(&mut fold, &fold_piece(name, key, value));
            Ok(())
        })?;
    }
    Ok(fold)
}
fn business_unpack<'a>(
    bucket: &[u8],
    physical: &[u8],
    value: &'a [u8],
) -> bbolt::Result<(&'a [u8], &'a [u8])> {
    if value.len() < 4 || value.len() > RECORD_LIMIT + 65538 + 36 {
        return Err(bad("business record bounds"));
    }
    let size = u32::from_be_bytes(value[..4].try_into().map_err(bad)?) as usize;
    if size == 0 || size > 65538 || size > value.len() - 4 {
        return Err(bad("business key bounds"));
    }
    let key = &value[4..4 + size];
    if physical != business_key(key) {
        return Err(bad("business physical/logical key mismatch"));
    }
    Ok((key, unpack(bucket, key, &value[4 + size..])?))
}
fn insert_row(
    rows: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    key: Vec<u8>,
    value: Vec<u8>,
) -> io::Result<()> {
    if rows.insert(key, value).is_some() {
        return Err(io_error("business physical key collision"));
    }
    Ok(())
}
pub(super) struct PatchRow {
    pub bucket: &'static [u8],
    pub key: String,
    pub value: Option<Vec<u8>>,
}
pub(super) struct Patch {
    pub since: Option<LogId>,
    pub meta: SnapshotMeta,
    pub scalars: serde_json::Value,
    pub rows: Vec<PatchRow>,
}
pub(super) enum PatchResult {
    Applied,
    Superseded,
    Gap,
}
fn patch_decision(
    header: Option<LogId>,
    since: Option<LogId>,
    through: Option<LogId>,
) -> PatchResult {
    if header == since {
        return PatchResult::Applied;
    }
    if let Some(header) = header {
        let ahead = through.is_none_or(|through| header.index > through.index || header == through);
        if ahead {
            return PatchResult::Superseded;
        }
    }
    PatchResult::Gap
}
fn required(tx: &bbolt::Tx, name: &[u8]) -> bbolt::Result<bbolt::Bucket> {
    tx.bucket(name)?
        .ok_or_else(|| bad("missing checkpoint bucket"))
}
impl Checkpoint {
    pub(super) fn open(path: &Path, cluster: &str, node: u64) -> io::Result<Self> {
        let file = path.join("controller.bbolt");
        let fresh = match std::fs::symlink_metadata(&file) {
            Ok(meta) => {
                if !meta.is_file()
                    || meta.file_type().is_symlink()
                    || meta.len() == 0
                    || meta.len() > FILE_LIMIT
                {
                    return Err(io_error("invalid checkpoint file"));
                }
                false
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => true,
            Err(e) => return Err(e),
        };
        let db = bbolt::Db::open(
            &file,
            0o600,
            Some(bbolt::Options {
                no_sync: false,
                no_grow_sync: false,
                max_size: FILE_LIMIT as usize,
                timeout: Some(std::time::Duration::from_secs(2)),
                ..Default::default()
            }),
        )
        .map_err(io_error)?;
        let identity = encode(&(1u64, cluster, node))?;
        if fresh {
            db.update(|tx| {
                for name in BUCKETS {
                    tx.create_bucket(name)?;
                }
                let metadata = required(tx, b"metadata")?;
                metadata.put(b"identity", &record(b"metadata", b"identity", &identity)?)?;
                metadata.put(
                    b"initialized",
                    &record(b"metadata", b"initialized", b"false")?,
                )
            })
            .map_err(io_error)?;
            std::fs::File::open(path)?.sync_all()?;
        } else {
            db.view(|tx| {
                for name in BUCKETS {
                    required(tx, name)?;
                }
                let bytes = required(tx, b"metadata")?
                    .get(b"identity")?
                    .ok_or_else(|| bad("missing checkpoint identity"))?;
                if unpack(b"metadata", b"identity", &bytes)? != identity {
                    return Err(bad("checkpoint identity/version mismatch"));
                }
                Ok(())
            })
            .map_err(io_error)?;
        }
        let this = Self {
            db,
            poisoned: AtomicBool::new(false),
            #[cfg(test)]
            fail_next_write: false,
        };
        this.read_snapshot()?;
        Ok(this)
    }
    fn check(&self) -> io::Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            Err(io_error("checkpoint instance poisoned"))
        } else {
            Ok(())
        }
    }
    /// Header only. Callers that need the applied log id must not scan business rows.
    pub(super) fn read_meta(&self) -> io::Result<Option<SnapshotMeta>> {
        self.check()?;
        self.result(self.db.view(|tx| {
            let metadata = required(tx, b"metadata")?;
            let Some(bytes) = metadata.get(b"checkpoint")? else {
                return Ok(None);
            };
            let header: Header =
                decode(unpack(b"metadata", b"checkpoint", &bytes)?).map_err(bad)?;
            if header.format != 1 && header.format != 2 {
                return Err(bad("checkpoint header version"));
            }
            Ok(Some(header.meta))
        }))
    }
    fn result<T>(&self, result: bbolt::Result<T>) -> io::Result<T> {
        result.map_err(|error| {
            self.poisoned.store(true, Ordering::Release);
            io_error(error)
        })
    }
    pub(super) fn read_snapshot(&self) -> io::Result<Option<SavedSnapshot>> {
        self.check()?;
        self.result(self.db.view(|tx| {
            let metadata = required(tx, b"metadata")?;
            let header = metadata.get(b"checkpoint")?;
            let initialized = metadata
                .get(b"initialized")?
                .ok_or_else(|| bad("missing checkpoint presence marker"))?;
            let initialized: bool =
                decode(unpack(b"metadata", b"initialized", &initialized)?).map_err(bad)?;
            if initialized != header.is_some() {
                return Err(bad("checkpoint presence mismatch"));
            }
            let mut maps = serde_json::Map::new();
            let mut total = 0usize;
            let mut fold = [0u8; 32];
            for name in BUCKETS.into_iter().skip(1) {
                let mut map = serde_json::Map::new();
                required(tx, name)?.for_each(|key, value| {
                    let value = value.ok_or_else(|| bad("nested business bucket"))?;
                    xor_fold(&mut fold, &fold_piece(name, key, value));
                    total = total
                        .checked_add(key.len() + value.len())
                        .ok_or_else(|| bad("checkpoint transaction overflow"))?;
                    if total > TRANSACTION_LIMIT {
                        return Err(bad("checkpoint transaction exceeds limit"));
                    }
                    let (key, bytes) = business_unpack(name, key, value)?;
                    let value = serde_json::from_slice(bytes).map_err(bad)?;
                    let key = std::str::from_utf8(key).map_err(bad)?.to_owned();
                    if map.insert(key, value).is_some() {
                        return Err(bad("duplicate logical business key"));
                    }
                    Ok(())
                })?;
                maps.insert(
                    std::str::from_utf8(name).map_err(bad)?.into(),
                    serde_json::Value::Object(map),
                );
            }
            let Some(header) = header else {
                if total != 0 {
                    return Err(bad("business records without checkpoint"));
                }
                return Ok(None);
            };
            let header: Header =
                decode(unpack(b"metadata", b"checkpoint", &header)?).map_err(bad)?;
            if header.format == 2 {
                if decode_fold(&header.digest)? != fold {
                    return Err(bad("checkpoint fold mismatch"));
                }
            } else if header.format != 1 {
                return Err(bad("checkpoint header version"));
            }
            let mut value = header.scalars;
            let meta = value
                .get_mut("metadata")
                .and_then(serde_json::Value::as_object_mut)
                .ok_or_else(|| bad("checkpoint scalars"))?;
            for (key, map) in maps {
                if meta.insert(key, map).is_some() {
                    return Err(bad("duplicate checkpoint map"));
                }
            }
            let state: State = serde_json::from_value(value).map_err(bad)?;
            if encode(&state.metadata).map_err(bad)?.len() > METADATA_LIMIT {
                return Err(bad("checkpoint metadata exceeds limit"));
            }
            let data = encode(&state).map_err(bad)?;
            let digest = if header.format == 2 {
                recovery::snapshot_digest(&header.meta, &data).map_err(bad)?
            } else {
                header.digest
            };
            let saved = SavedSnapshot {
                format: 1,
                digest: Some(digest),
                meta: header.meta,
                data,
            };
            recovery::snapshot_state(&saved).map_err(bad)?;
            Ok(Some(saved))
        }))
    }
    #[cfg(test)]
    pub(super) fn fail_next_write(&mut self) {
        self.fail_next_write = true;
    }
    pub(super) fn write_snapshot(&mut self, saved: &SavedSnapshot) -> io::Result<()> {
        self.check()?;
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_write) {
            return self.result(Err(bad("injected checkpoint write failure")));
        }
        let state = recovery::snapshot_state(saved)?;
        if encode(&state.metadata)?.len() > METADATA_LIMIT {
            return Err(io_error("checkpoint metadata exceeds limit"));
        }
        let data = encode(&state)?;
        let digest = recovery::snapshot_digest(&saved.meta, &data)?;
        let mut scalars = serde_json::to_value(&state).map_err(io_error)?;
        let metadata = scalars
            .get_mut("metadata")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or_else(|| io_error("checkpoint metadata"))?;
        let mut prepared = BTreeMap::<Vec<u8>, BTreeMap<Vec<u8>, Vec<u8>>>::new();
        let mut total = 0usize;
        for name in BUCKETS.into_iter().skip(1) {
            let key = std::str::from_utf8(name).map_err(io_error)?;
            let map = metadata
                .remove(key)
                .and_then(|v| {
                    if let serde_json::Value::Object(map) = v {
                        Some(map)
                    } else {
                        None
                    }
                })
                .ok_or_else(|| io_error("checkpoint business map"))?;
            let mut rows = BTreeMap::new();
            for (key, value) in map {
                let bytes = encode(&value)?;
                let row = business_record(name, key.as_bytes(), &bytes).map_err(io_error)?;
                total = total
                    .checked_add(key.len() + row.len())
                    .ok_or_else(|| io_error("checkpoint size overflow"))?;
                if total > TRANSACTION_LIMIT {
                    return Err(io_error("checkpoint transaction limit"));
                }
                insert_row(&mut rows, business_key(key.as_bytes()), row)?;
            }
            prepared.insert(name.to_vec(), rows);
        }
        let header = encode(&Header {
            format: 1,
            meta: saved.meta.clone(),
            digest,
            scalars,
        })?;
        let header = record(b"metadata", b"checkpoint", &header).map_err(io_error)?;
        self.result(self.db.update(|tx| {
            for (name, rows) in &prepared {
                let bucket = required(tx, name)?;
                let mut removed = Vec::new();
                bucket.for_each(|key, value| {
                    if value.is_none() {
                        return Err(bad("nested business bucket"));
                    }
                    if !rows.contains_key(key) {
                        removed.push(key.to_vec());
                    }
                    Ok(())
                })?;
                for key in removed {
                    bucket.delete(&key)?;
                }
                for (key, value) in rows {
                    if bucket.get(key)?.as_deref() != Some(value) {
                        bucket.put(key, value)?;
                    }
                }
            }
            let bucket = required(tx, b"metadata")?;
            let initialized = record(b"metadata", b"initialized", b"true")?;
            if bucket.get(b"initialized")?.as_deref() != Some(&initialized) {
                bucket.put(b"initialized", &initialized)?;
            }
            if bucket.get(b"checkpoint")?.as_deref() != Some(&header) {
                bucket.put(b"checkpoint", &header)?;
            }
            Ok(())
        }))
    }
    /// Changed keys only. The header floor and these rows commit in one transaction.
    pub(super) fn write_patch(&mut self, patch: &Patch) -> io::Result<PatchResult> {
        self.check()?;
        if patch
            .rows
            .iter()
            .any(|row| !BUCKETS[1..].contains(&row.bucket))
        {
            return Err(io_error("checkpoint patch bucket"));
        }
        let mut total = 0usize;
        for row in &patch.rows {
            let value = row.value.as_ref().map_or(0, Vec::len);
            total = total
                .checked_add(row.key.len() + value)
                .ok_or_else(|| io_error("checkpoint size overflow"))?;
            if total > TRANSACTION_LIMIT {
                return Err(io_error("checkpoint transaction limit"));
            }
        }
        let current = self.read_meta()?.and_then(|meta| meta.last_log_id);
        // read_meta re-checks poison and takes its own view. The db mutex is held
        // by the caller, so the floor cannot change before the update below.
        match patch_decision(current, patch.since, patch.meta.last_log_id) {
            PatchResult::Applied => {}
            other => return Ok(other),
        }
        let _persist = crate::profile::Timer::start(crate::profile::Metric::CheckpointPersist);
        let meta = patch.meta.clone();
        let scalars = patch.scalars.clone();
        self.result(self.db.update(|tx| {
            let mut fold = match required(tx, b"metadata")?.get(b"checkpoint")? {
                Some(bytes) => {
                    let header: Header =
                        decode(unpack(b"metadata", b"checkpoint", &bytes)?).map_err(bad)?;
                    if header.format == 2 {
                        decode_fold(&header.digest)?
                    } else if header.format == 1 {
                        scan_fold(tx)?
                    } else {
                        return Err(bad("checkpoint header version"));
                    }
                }
                None => [0u8; 32],
            };
            for row in &patch.rows {
                let bucket = required(tx, row.bucket)?;
                let physical = business_key(row.key.as_bytes());
                let old = bucket.get(&physical)?.map(|bytes| {
                    let bytes: &[u8] = bytes.as_ref();
                    bytes.to_vec()
                });
                if let Some(old) = old.as_deref() {
                    xor_fold(&mut fold, &fold_piece(row.bucket, &physical, old));
                }
                if old.is_some() && row.value.is_none() {
                    bucket.delete(&physical)?;
                }
                if let Some(value) = &row.value {
                    let stored = business_record(row.bucket, row.key.as_bytes(), value)?;
                    xor_fold(&mut fold, &fold_piece(row.bucket, &physical, &stored));
                    bucket.put(&physical, &stored)?;
                }
            }
            let header = encode(&Header {
                format: 2,
                meta: meta.clone(),
                digest: encode_fold(&fold),
                scalars: scalars.clone(),
            })
            .map_err(bad)?;
            let header = record(b"metadata", b"checkpoint", &header)?;
            let bucket = required(tx, b"metadata")?;
            let initialized = record(b"metadata", b"initialized", b"true")?;
            if bucket.get(b"initialized")?.as_deref() != Some(&initialized) {
                bucket.put(b"initialized", &initialized)?;
            }
            bucket.put(b"checkpoint", &header)?;
            Ok(())
        }))?;
        Ok(PatchResult::Applied)
    }
}
#[cfg(test)]
mod tests;
