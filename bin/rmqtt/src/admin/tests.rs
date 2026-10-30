use super::{assets, settings::read_token};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "rmqtt-dashboard-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[test]
fn token_input_is_bounded_and_errors_do_not_disclose_secrets() {
    let dir = Fixture::new();
    let path = dir.0.join("token");
    for bytes in [
        b"short".to_vec(),
        vec![b'a'; 257],
        vec![b'a'; 4097],
        vec![0xff; 32],
        b"0123456789abcdef 0123456789abcdef".to_vec(),
    ] {
        std::fs::write(&path, bytes).unwrap();
        assert!(read_token(&path).is_err());
    }
    let good = "test-only-0123456789abcdef0123456789abcdef";
    std::fs::write(&path, format!(" {good}\n")).unwrap();
    assert_eq!(read_token(&path).unwrap(), good.as_bytes());
    assert!(read_token(&dir.0).is_err());
}
#[test]
fn static_input_has_file_and_total_limits_and_needs_index() {
    let dir = Fixture::new();
    assert!(assets::load(&dir.0, true).is_err());
    std::fs::write(dir.0.join("index.html"), b"<html></html>").unwrap();
    assert_eq!(assets::load(&dir.0, true).unwrap().len(), 1);
    std::fs::write(dir.0.join("large.js"), vec![b'a'; 4 * 1024 * 1024 + 1]).unwrap();
    assert!(assets::load(&dir.0, true).is_err());
    assert!(assets::load(&dir.0.join("missing"), true).is_err());
    assert!(
        assets::load(&dir.0.join("missing"), false)
            .unwrap()
            .is_empty()
    );
}
#[cfg(unix)]
#[test]
fn static_symlinks_and_token_symlinks_are_rejected() {
    let dir = Fixture::new();
    std::fs::write(dir.0.join("index.html"), b"ok").unwrap();
    std::os::unix::fs::symlink("index.html", dir.0.join("alias")).unwrap();
    assert!(assets::load(&dir.0, true).is_err());
    assert!(read_token(&dir.0.join("alias")).is_err());
}
