use crate::{Config, RuntimeError, config::MAX_CONFIG};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);

pub(crate) fn read(path: &Path) -> Result<Config, RuntimeError> {
    let meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let config = Config::default();
            write(path, &config)?;
            return Ok(config);
        }
        Err(_) => return Err(RuntimeError::Storage),
    };
    if !meta.is_file() || meta.file_type().is_symlink() || meta.len() > MAX_CONFIG as u64 {
        return Err(RuntimeError::InvalidConfig);
    }
    let mut bytes = Vec::new();
    fs::File::open(path)
        .map_err(|_| RuntimeError::Storage)?
        .take(MAX_CONFIG as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| RuntimeError::Storage)?;
    if bytes.len() > MAX_CONFIG {
        return Err(RuntimeError::InvalidConfig);
    }
    serde_json::from_slice(&bytes).map_err(|_| RuntimeError::InvalidConfig)
}
pub(crate) fn write(path: &Path, config: &Config) -> Result<(), RuntimeError> {
    if let Ok(meta) = fs::symlink_metadata(path)
        && (!meta.is_file() || meta.file_type().is_symlink())
    {
        return Err(RuntimeError::Storage);
    }
    let bytes = serde_json::to_vec(config).map_err(|_| RuntimeError::InvalidConfig)?;
    if bytes.len() > MAX_CONFIG {
        return Err(RuntimeError::InvalidConfig);
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let temp = parent.join(format!(
        ".rmqtt-rules-{}-{}.tmp",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp).map_err(|_| RuntimeError::Storage)?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|_| RuntimeError::Storage)?;
        fs::rename(&temp, path).map_err(|_| RuntimeError::Storage)?;
        fs::File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|_| RuntimeError::Storage)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}
