use crate::{QueueError, format};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::{
    fs::{self, File, OpenOptions, TryLockError},
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
};

pub(crate) fn sync_dir(path: &Path) -> Result<(), QueueError> {
    File::open(path)?.sync_all()?;
    Ok(())
}
pub(crate) fn directory(path: &Path) -> Result<PathBuf, QueueError> {
    let full = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    if full
        .components()
        .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(QueueError::InvalidOptions);
    }
    let parent = full.parent().ok_or(QueueError::InvalidOptions)?;
    let parent = match fs::canonicalize(parent) {
        Ok(parent) => parent,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => directory(parent)?,
        Err(_) => return Err(QueueError::Io),
    };
    let name = full.file_name().ok_or(QueueError::InvalidOptions)?;
    let checked = parent.join(name);
    match fs::symlink_metadata(&checked) {
        Ok(meta) if meta.is_dir() && !meta.file_type().is_symlink() => {}
        Ok(_) => return Err(QueueError::Io),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(&checked)?;
            #[cfg(unix)]
            fs::set_permissions(&checked, fs::Permissions::from_mode(0o700))?;
            sync_dir(&parent)?;
        }
        Err(_) => return Err(QueueError::Io),
    }
    #[cfg(unix)]
    if fs::metadata(&checked)?.permissions().mode() & 0o077 != 0 {
        return Err(QueueError::Io);
    }
    Ok(checked)
}
pub(crate) fn private_file(path: &Path, create: bool) -> Result<File, QueueError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    options.mode(0o600);
    let file = if create {
        options.create_new(true).open(path)?
    } else {
        let meta = fs::symlink_metadata(path)?;
        if !meta.is_file() || meta.file_type().is_symlink() {
            return Err(QueueError::Io);
        }
        options.open(path)?
    };
    let meta = file.metadata()?;
    let current = fs::symlink_metadata(path)?;
    if !meta.is_file() || !current.is_file() || current.file_type().is_symlink() {
        return Err(QueueError::Io);
    }
    #[cfg(unix)]
    if meta.nlink() != 1
        || current.nlink() != 1
        || meta.ino() != current.ino()
        || meta.dev() != current.dev()
        || meta.permissions().mode() & 0o077 != 0
    {
        return Err(QueueError::Io);
    }
    Ok(file)
}
pub(crate) fn owner(root: &Path) -> Result<File, QueueError> {
    let path = root.join("OWNERSHIP");
    let file = if path.try_exists()? {
        private_file(&path, false)?
    } else {
        let f = private_file(&path, true)?;
        f.sync_all()?;
        sync_dir(root)?;
        f
    };
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(QueueError::Locked),
        Err(TryLockError::Error(_)) => Err(QueueError::Io),
    }
}
pub(crate) fn generation(root: &Path, id: u64) -> PathBuf {
    root.join(format!("generation-{id:020}"))
}
pub(crate) fn segment(root: &Path, id: u64) -> PathBuf {
    root.join(format!("segment-{id:020}.log"))
}
pub(crate) fn manifest(root: &Path) -> Result<Option<u64>, QueueError> {
    let path = root.join("CURRENT");
    if !path.try_exists()? {
        return Ok(None);
    }
    let mut file = private_file(&path, false)?;
    if file.metadata()?.len() != 20 {
        return Err(QueueError::Corrupt);
    }
    let mut b = [0u8; 20];
    file.read_exact(&mut b)?;
    if &b[..8] != b"RMQCUR01"
        || format::checksum(&b[..16])
            != u32::from_le_bytes(b[16..20].try_into().map_err(|_| QueueError::Corrupt)?)
    {
        return Err(QueueError::Corrupt);
    }
    let n = u64::from_le_bytes(b[8..16].try_into().map_err(|_| QueueError::Corrupt)?);
    if n == 0 {
        return Err(QueueError::Corrupt);
    }
    Ok(Some(n))
}
pub(crate) fn publish(root: &Path, id: u64) -> Result<(), QueueError> {
    let temp = root.join("CURRENT.tmp");
    if temp.try_exists()? {
        drop(private_file(&temp, false)?);
        fs::remove_file(&temp)?;
    }
    let mut file = private_file(&temp, true)?;
    let mut b = [0u8; 20];
    b[..8].copy_from_slice(b"RMQCUR01");
    b[8..16].copy_from_slice(&id.to_le_bytes());
    let crc = format::checksum(&b[..16]);
    b[16..20].copy_from_slice(&crc.to_le_bytes());
    file.write_all(&b)?;
    file.sync_all()?;
    fs::rename(temp, root.join("CURRENT"))?;
    sync_dir(root)
}
#[cfg(not(test))]
pub(crate) const MAX_SEGMENTS: usize = 1_000_002;
#[cfg(test)]
pub(crate) const MAX_SEGMENTS: usize = 32;
pub(crate) fn files(dir: &Path, max_bytes: u64) -> Result<Vec<(u64, u64)>, QueueError> {
    let mut result = Vec::new();
    let mut total = 0u64;
    for e in fs::read_dir(dir)? {
        if result.len() >= MAX_SEGMENTS {
            return Err(QueueError::Corrupt);
        }
        let e = e?;
        let name = e.file_name();
        let name = name.to_str().ok_or(QueueError::Corrupt)?;
        let n = name
            .strip_prefix("segment-")
            .and_then(|s| s.strip_suffix(".log"))
            .filter(|s| s.len() == 20)
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0)
            .ok_or(QueueError::Corrupt)?;
        let file = private_file(&e.path(), false)?;
        let len = file.metadata()?.len();
        total = total.checked_add(len).ok_or(QueueError::Corrupt)?;
        if total.saturating_add(20) > max_bytes {
            return Err(QueueError::Full);
        }
        result.push((n, len));
    }
    result.sort_unstable();
    if result.is_empty() {
        return Err(QueueError::Corrupt);
    }
    for (i, (id, _)) in result.iter().enumerate() {
        if *id != i as u64 + 1 {
            return Err(QueueError::Corrupt);
        }
    }
    Ok(result)
}
pub(crate) fn clean_other_generations(root: &Path, current: u64) -> Result<(), QueueError> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(root)? {
        if entries.len() >= 16 {
            return Err(QueueError::Corrupt);
        }
        entries.push(entry?);
    }
    // Bound and validate the whole root before removing even harmless orphans.
    for entry in &entries {
        let name = entry.file_name();
        let name = name.to_str().ok_or(QueueError::Corrupt)?;
        if matches!(name, "OWNERSHIP" | "CURRENT" | "CURRENT.tmp") {
            drop(private_file(&entry.path(), false)?);
            continue;
        }
        let id = name
            .strip_prefix("generation-")
            .filter(|s| s.len() == 20)
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0)
            .ok_or(QueueError::Corrupt)?;
        let meta = fs::symlink_metadata(entry.path())?;
        if !meta.is_dir() || meta.file_type().is_symlink() {
            return Err(QueueError::Io);
        }
        #[cfg(unix)]
        if meta.permissions().mode() & 0o077 != 0 {
            return Err(QueueError::Io);
        }
        if id != current {
            validate_generation_children(&entry.path())?;
        }
    }
    for entry in entries {
        let name = entry.file_name();
        let name = name.to_str().ok_or(QueueError::Corrupt)?;
        if matches!(name, "OWNERSHIP" | "CURRENT") {
            continue;
        }
        if name == "CURRENT.tmp" {
            fs::remove_file(entry.path())?;
            continue;
        }
        let id = name
            .strip_prefix("generation-")
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or(QueueError::Corrupt)?;
        if id != current {
            remove_generation(&entry.path())?;
        }
    }
    sync_dir(root)
}
fn validate_generation_children(dir: &Path) -> Result<Vec<PathBuf>, QueueError> {
    let mut list = Vec::new();
    for entry in fs::read_dir(dir)? {
        if list.len() >= MAX_SEGMENTS {
            return Err(QueueError::Corrupt);
        }
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_str().ok_or(QueueError::Corrupt)?;
        if name
            .strip_prefix("segment-")
            .and_then(|s| s.strip_suffix(".log"))
            .filter(|s| s.len() == 20)
            .and_then(|s| s.parse::<u64>().ok())
            .is_none_or(|n| n == 0)
        {
            return Err(QueueError::Corrupt);
        }
        drop(private_file(&entry.path(), false)?);
        list.push(entry.path());
    }
    Ok(list)
}
pub(crate) fn remove_generation(dir: &Path) -> Result<(), QueueError> {
    // Never recursively follow paths or allocate before the count bound.
    for path in validate_generation_children(dir)? {
        fs::remove_file(path)?;
    }
    fs::remove_dir(dir)?;
    Ok(())
}
pub(crate) fn read_payload(
    dir: &Path,
    segment_id: u64,
    offset: u64,
    len: usize,
    expected_checksum: u32,
) -> Result<Vec<u8>, QueueError> {
    let mut file = private_file(&segment(dir, segment_id), false)?;
    if offset
        .checked_add(len as u64)
        .is_none_or(|end| end > file.metadata().map(|m| m.len()).unwrap_or(0))
    {
        return Err(QueueError::Corrupt);
    }
    file.seek(SeekFrom::Start(offset))?;
    let mut data = vec![0u8; len];
    file.read_exact(&mut data)?;
    if format::checksum(&data) != expected_checksum {
        return Err(QueueError::Corrupt);
    }
    Ok(data)
}
