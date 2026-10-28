//! Bounded startup-only policy IO. Called from a blocking task before binding TCP.
use mqtt_auth::{MAX_POLICY_BYTES, StaticPolicy};
use std::{
    fs::File,
    io::{self, Read},
    path::Path,
};

pub fn load(path: &Path) -> io::Result<StaticPolicy> {
    let metadata = std::fs::metadata(path)?;
    if !metadata.is_file() || metadata.len() > MAX_POLICY_BYTES as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "auth policy must be a regular file of at most 1 MiB",
        ));
    }
    let file = File::open(path)?;
    let mut bytes = Vec::new();
    file.take((MAX_POLICY_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_POLICY_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "auth policy exceeds 1 MiB",
        ));
    }
    StaticPolicy::from_json(&bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid authentication policy"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Fixture(std::path::PathBuf);
    impl Fixture {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "rmqtt-policy-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn file(&self, bytes: &[u8]) -> std::path::PathBuf {
            let path = self.0.join("policy.json");
            std::fs::write(&path, bytes).unwrap();
            path
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    fn reads_valid_policy_and_rejects_invalid_or_missing_file() {
        let fixture = Fixture::new();
        assert!(load(&fixture.file(br#"{"allow_anonymous":true,"users":[]}"#)).is_ok());
        assert!(load(&fixture.0.join("missing")).is_err());
        assert!(load(&fixture.file(b"not-json secret-value")).is_err());
        let error = load(&fixture.file(b"not-json secret-value")).unwrap_err();
        assert!(!error.to_string().contains("secret-value"));
    }
    #[test]
    fn rejects_nonregular_or_oversized_input_before_parsing() {
        let fixture = Fixture::new();
        assert!(load(&fixture.0).is_err());
        assert!(load(&fixture.file(&vec![b' '; MAX_POLICY_BYTES + 1])).is_err());
    }
}
