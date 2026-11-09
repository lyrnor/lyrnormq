//! Bounded, startup-only certificate loading on the blocking pool.
use mqtt_transport::TlsConfig;
use std::{
    fs::File,
    io::{self, Read},
    path::Path,
};

const MAX_IDENTITY_BYTES: usize = 1024 * 1024;

fn read_bounded(path: &Path) -> io::Result<Vec<u8>> {
    // Reject special files before open (opening a FIFO can itself block).
    let metadata = std::fs::metadata(path)?;
    if !metadata.is_file() || metadata.len() > MAX_IDENTITY_BYTES as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "TLS identity must be a regular file of at most 1 MiB",
        ));
    }
    let file = File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > MAX_IDENTITY_BYTES as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "TLS identity must be a regular file of at most 1 MiB",
        ));
    }
    let mut bytes = Vec::new();
    file.take((MAX_IDENTITY_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_IDENTITY_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "TLS identity exceeds 1 MiB",
        ));
    }
    Ok(bytes)
}

pub fn load(cert: &Path, key: &Path) -> io::Result<TlsConfig> {
    TlsConfig::from_pem(&read_bounded(cert)?, &read_bounded(key)?)
}

pub fn load_with_client_ca(cert: &Path, key: &Path, ca: Option<&Path>) -> io::Result<TlsConfig> {
    match ca {
        None => load(cert, key),
        Some(ca) => TlsConfig::from_pem_with_client_ca(
            &read_bounded(cert)?,
            &read_bounded(key)?,
            &read_bounded(ca)?,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn bounded_file_reader_rejects_special_missing_and_oversized_inputs() {
        let path = std::env::temp_dir().join(format!(
            "lyrnormq-tls-loader-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }
        let _cleanup = Cleanup(path.clone());
        assert!(read_bounded(&path).is_err());
        assert!(read_bounded(&std::env::temp_dir()).is_err());
        std::fs::write(&path, vec![b'x'; MAX_IDENTITY_BYTES]).unwrap();
        assert_eq!(read_bounded(&path).unwrap().len(), MAX_IDENTITY_BYTES);
        std::fs::write(&path, vec![b'x'; MAX_IDENTITY_BYTES + 1]).unwrap();
        assert_eq!(
            read_bounded(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        std::fs::write(&path, "not a PEM identity").unwrap();
        assert!(load(&path, &path).is_err());
    }
}
