//! Bounded, checksummed keyed business-state checkpoints. All operations block.
use super::{SavedSnapshot, State, decode, encode, recovery};
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
    meta: openraft::SnapshotMeta<u64, openraft::BasicNode>,
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
    if key.is_empty() || key.len() > 65535 || data.len() > RECORD_LIMIT {
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
            for name in BUCKETS.into_iter().skip(1) {
                let mut map = serde_json::Map::new();
                required(tx, name)?.for_each(|key, value| {
                    let value = value.ok_or_else(|| bad("nested business bucket"))?;
                    total = total
                        .checked_add(key.len() + value.len())
                        .ok_or_else(|| bad("checkpoint transaction overflow"))?;
                    if total > TRANSACTION_LIMIT {
                        return Err(bad("checkpoint transaction exceeds limit"));
                    }
                    let bytes = unpack(name, key, value)?;
                    let value = serde_json::from_slice(bytes).map_err(bad)?;
                    let key = std::str::from_utf8(key).map_err(bad)?.to_owned();
                    map.insert(key, value);
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
            if header.format != 1 {
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
            let saved = SavedSnapshot {
                format: 1,
                digest: Some(header.digest),
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
                let row = record(name, key.as_bytes(), &bytes).map_err(io_error)?;
                total = total
                    .checked_add(key.len() + row.len())
                    .ok_or_else(|| io_error("checkpoint size overflow"))?;
                if total > TRANSACTION_LIMIT {
                    return Err(io_error("checkpoint transaction limit"));
                }
                rows.insert(key.into_bytes(), row);
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
}
#[cfg(test)]
mod tests;
