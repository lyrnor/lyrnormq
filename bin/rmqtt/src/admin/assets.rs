//! Startup-only bounded static loading. No file IO occurs in HTTP handlers.
use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
    sync::Arc,
};

pub struct Asset {
    pub source: PathBuf,
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

pub fn load(root: &Path, required: bool) -> io::Result<Assets> {
    let metadata = match fs::symlink_metadata(root) {
        Ok(meta) => meta,
        Err(error) if !required && error.kind() == io::ErrorKind::NotFound => {
            eprintln!(
                "Dashboard assets unavailable; management API only (build dashboard/web first)"
            );
            return Ok(BTreeMap::new());
        }
        Err(_) => return Err(invalid()),
    };
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
        if *visited > 256 {
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
        if !meta.is_file() || meta.len() > 4 * 1024 * 1024 {
            return Err(invalid());
        }
        let mut bytes = Vec::new();
        fs::File::open(&path)
            .map_err(|_| invalid())?
            .take(4 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| invalid())?;
        *total = total.checked_add(bytes.len()).ok_or_else(invalid)?;
        if bytes.len() > 4 * 1024 * 1024 || *total > 16 * 1024 * 1024 {
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
        let content_type = match path.extension().and_then(|s| s.to_str()) {
            Some("html") => "text/html; charset=utf-8",
            Some("js") => "text/javascript; charset=utf-8",
            Some("css") => "text/css; charset=utf-8",
            Some("svg") => "image/svg+xml",
            Some("png") => "image/png",
            Some("jpg" | "jpeg") => "image/jpeg",
            Some("ico") => "image/x-icon",
            Some("woff2") => "font/woff2",
            _ => return Err(invalid()),
        };
        assets.insert(
            format!("/{name}"),
            Asset {
                source: path,
                bytes: bytes.into(),
                content_type,
            },
        );
    }
    Ok(())
}
