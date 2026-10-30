//! Startup-only bounded static loading. No file IO occurs in HTTP handlers.
use super::asset_rules::{MAX_ENTRIES, MAX_FILE_BYTES, MAX_TOTAL_BYTES, content_type};
use include_dir::{Dir, DirEntry, include_dir};
use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
    sync::Arc,
};

pub struct Asset {
    pub source: Option<PathBuf>,
    pub bytes: Arc<[u8]>,
    pub content_type: &'static str,
}
pub type Assets = BTreeMap<String, Asset>;
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "invalid or oversized dashboard assets",
    )
}

pub fn load(root: &Path) -> io::Result<Assets> {
    let metadata = fs::symlink_metadata(root).map_err(|_| invalid())?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(invalid());
    }
    let mut assets = BTreeMap::new();
    let mut visited = 0usize;
    let mut total = 0usize;
    visit(root, root, &mut assets, &mut visited, &mut total)?;
    if !assets.contains_key("/index.html") {
        return Err(invalid());
    }
    Ok(assets)
}
fn visit(
    root: &Path,
    dir: &Path,
    assets: &mut Assets,
    visited: &mut usize,
    total: &mut usize,
) -> io::Result<()> {
    for entry in fs::read_dir(dir).map_err(|_| invalid())? {
        *visited += 1;
        if *visited > MAX_ENTRIES {
            return Err(invalid());
        }
        let entry = entry.map_err(|_| invalid())?;
        let path = entry.path();
        let meta = fs::symlink_metadata(&path).map_err(|_| invalid())?;
        if meta.file_type().is_symlink() {
            return Err(invalid());
        }
        if meta.is_dir() {
            visit(root, &path, assets, visited, total)?;
            continue;
        }
        if !meta.is_file() || meta.len() > MAX_FILE_BYTES as u64 {
            return Err(invalid());
        }
        let mut bytes = Vec::new();
        fs::File::open(&path)
            .map_err(|_| invalid())?
            .take((MAX_FILE_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| invalid())?;
        *total = total.checked_add(bytes.len()).ok_or_else(invalid)?;
        if bytes.len() > MAX_FILE_BYTES || *total > MAX_TOTAL_BYTES {
            return Err(invalid());
        }
        let name = path
            .strip_prefix(root)
            .ok()
            .and_then(|s| s.to_str())
            .ok_or_else(invalid)?;
        if name.contains(['\\', '%']) {
            return Err(invalid());
        }
        let content_type = content_type(&path).ok_or_else(invalid)?;
        assets.insert(
            format!("/{name}"),
            Asset {
                source: Some(path),
                bytes: bytes.into(),
                content_type,
            },
        );
    }
    Ok(())
}

static DASHBOARD: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/../../dashboard/web/dist");

/// Use only resources compiled into this binary; independent of cwd or Node.
pub fn embedded() -> io::Result<Assets> {
    let mut assets = Assets::new();
    embed(&DASHBOARD, &mut assets, &mut 0, &mut 0)?;
    if !assets.contains_key("/index.html") {
        return Err(invalid());
    }
    Ok(assets)
}
fn embed(
    directory: &Dir<'static>,
    assets: &mut Assets,
    entries: &mut usize,
    total: &mut usize,
) -> io::Result<()> {
    for entry in directory.entries() {
        *entries += 1;
        if *entries > MAX_ENTRIES {
            return Err(invalid());
        }
        match entry {
            DirEntry::Dir(dir) => embed(dir, assets, entries, total)?,
            DirEntry::File(file) => {
                let path = file.path();
                let name = path.to_str().ok_or_else(invalid)?;
                if name.contains(['\\', '%']) {
                    return Err(invalid());
                }
                let bytes = file.contents();
                *total = total.checked_add(bytes.len()).ok_or_else(invalid)?;
                if bytes.len() > MAX_FILE_BYTES || *total > MAX_TOTAL_BYTES {
                    return Err(invalid());
                }
                assets.insert(
                    format!("/{name}"),
                    Asset {
                        source: None,
                        bytes: Arc::from(bytes),
                        content_type: content_type(path).ok_or_else(invalid)?,
                    },
                );
            }
        }
    }
    Ok(())
}
