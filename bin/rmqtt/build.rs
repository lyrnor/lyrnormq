//! Validate and track the prebuilt UI. Never run Node or network IO from Cargo.
#[path = "src/admin/asset_rules.rs"]
mod asset_rules;
use std::{fs, io, path::Path};

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "Dashboard dist missing or invalid; run `cd dashboard/web && npm ci && npm run build` before Cargo (or scripts/build-rmqtt.sh)",
    )
}
fn validate(root: &Path, directory: &Path, entries: &mut usize, total: &mut u64) -> io::Result<()> {
    let meta = fs::symlink_metadata(directory).map_err(|_| invalid())?;
    if !meta.is_dir() || meta.file_type().is_symlink() {
        return Err(invalid());
    }
    for entry in fs::read_dir(directory).map_err(|_| invalid())? {
        *entries += 1;
        if *entries > asset_rules::MAX_ENTRIES {
            return Err(invalid());
        }
        let path = entry.map_err(|_| invalid())?.path();
        let meta = fs::symlink_metadata(&path).map_err(|_| invalid())?;
        if meta.file_type().is_symlink() {
            return Err(invalid());
        }
        if meta.is_dir() {
            validate(root, &path, entries, total)?;
            continue;
        }
        let name = path
            .strip_prefix(root)
            .ok()
            .and_then(|p| p.to_str())
            .ok_or_else(invalid)?;
        if name.contains(['\\', '%'])
            || !meta.is_file()
            || meta.len() > asset_rules::MAX_FILE_BYTES as u64
            || asset_rules::content_type(&path).is_none()
        {
            return Err(invalid());
        }
        *total = total.checked_add(meta.len()).ok_or_else(invalid)?;
        if *total > asset_rules::MAX_TOTAL_BYTES as u64 {
            return Err(invalid());
        }
    }
    Ok(())
}
fn main() -> io::Result<()> {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR").ok_or_else(invalid)?;
    let root = Path::new(&manifest).join("../../dashboard/web/dist");
    println!("cargo:rerun-if-changed={}", root.display());
    validate(&root, &root, &mut 0, &mut 0)?;
    let index = fs::symlink_metadata(root.join("index.html")).map_err(|_| invalid())?;
    if !index.is_file() || index.len() == 0 {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
#[path = "build_tests.rs"]
mod tests;
