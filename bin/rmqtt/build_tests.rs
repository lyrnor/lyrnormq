use super::*;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "rmqtt-embed-build-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn validate(&self) -> io::Result<()> {
        validate(&self.0, &self.0, &mut 0, &mut 0)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
#[test]
fn missing_bundle_reports_build_command() {
    let f = Fixture::new();
    let error = validate(&f.0, &f.0.join("missing"), &mut 0, &mut 0).unwrap_err();
    assert!(error.to_string().contains("npm run build"));
}
#[test]
fn validates_static_bundle_and_rejects_unknown_or_oversized_files() {
    let f = Fixture::new();
    fs::write(f.0.join("index.html"), "<html></html>").unwrap();
    fs::write(f.0.join("app.js"), "export default 1").unwrap();
    assert!(f.validate().is_ok());
    fs::write(f.0.join("private.pem"), "secret").unwrap();
    assert!(f.validate().is_err());
    fs::remove_file(f.0.join("private.pem")).unwrap();
    fs::File::create(f.0.join("large.js"))
        .unwrap()
        .set_len(asset_rules::MAX_FILE_BYTES as u64 + 1)
        .unwrap();
    assert!(f.validate().is_err());
}
#[test]
fn enforces_entry_and_total_byte_caps() {
    let f = Fixture::new();
    for n in 0..=asset_rules::MAX_ENTRIES {
        fs::write(f.0.join(format!("{n}.js")), b"").unwrap();
    }
    assert!(f.validate().is_err());
    let total = Fixture::new();
    for n in 0..5 {
        fs::File::create(total.0.join(format!("{n}.js")))
            .unwrap()
            .set_len(asset_rules::MAX_FILE_BYTES as u64)
            .unwrap();
    }
    assert!(total.validate().is_err());
}
#[cfg(unix)]
#[test]
fn rejects_symlinked_root_and_entries() {
    let f = Fixture::new();
    fs::write(f.0.join("index.html"), b"ok").unwrap();
    std::os::unix::fs::symlink("index.html", f.0.join("alias.js")).unwrap();
    assert!(f.validate().is_err());
    let g = Fixture::new();
    std::os::unix::fs::symlink(&f.0, g.0.join("linked")).unwrap();
    assert!(validate(&g.0, &g.0.join("linked"), &mut 0, &mut 0).is_err());
}
