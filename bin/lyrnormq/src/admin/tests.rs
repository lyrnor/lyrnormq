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
            "lyrnormq-dashboard-{}-{}",
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
    assert!(assets::load(&dir.0).is_err());
    std::fs::write(dir.0.join("index.html"), b"<html></html>").unwrap();
    assert_eq!(assets::load(&dir.0).unwrap().len(), 1);
    std::fs::write(dir.0.join("large.js"), vec![b'a'; 4 * 1024 * 1024 + 1]).unwrap();
    assert!(assets::load(&dir.0).is_err());
    assert!(assets::load(&dir.0.join("missing")).is_err());
}
#[cfg(unix)]
#[test]
fn static_symlinks_and_token_symlinks_are_rejected() {
    let dir = Fixture::new();
    std::fs::write(dir.0.join("index.html"), b"ok").unwrap();
    std::os::unix::fs::symlink("index.html", dir.0.join("alias")).unwrap();
    assert!(assets::load(&dir.0).is_err());
    assert!(read_token(&dir.0.join("alias")).is_err());
}

#[test]
fn builtin_bundle_contains_production_entry_and_linked_assets() {
    let bundle = assets::embedded().unwrap();
    let html = std::str::from_utf8(&bundle.get("/index.html").unwrap().bytes).unwrap();
    assert!(html.contains("id=\"root\""));
    assert!(!html.contains("/src/main.tsx"));
    assert!(bundle.values().all(|asset| asset.source.is_none()));
    assert!(bundle.keys().any(|name| name.ends_with(".js")));
    assert!(bundle.keys().any(|name| name.ends_with(".css")));
}

#[test]
fn sqlite_configuration_and_companion_aliases_cannot_be_static_assets() {
    use mqtt_transport::management::ManagementHandle;
    for suffix in ["", "-journal", "-wal", "-shm", ".config-lock"] {
        let dir = Fixture::new();
        let token = dir.0.join("token");
        std::fs::write(&token, b"test-only-0123456789abcdef0123456789abcdef").unwrap();
        let web = dir.0.join("web");
        std::fs::create_dir(&web).unwrap();
        std::fs::write(web.join("index.html"), b"<html></html>").unwrap();
        let sensitive = dir.0.join(format!("rules.sqlite{suffix}"));
        std::fs::write(&sensitive, b"private configuration").unwrap();
        std::fs::hard_link(&sensitive, web.join("private.js")).unwrap();
        let config = dir.0.join("lyrnormq.toml");
        std::fs::write(
            &config,
            format!(
                "[node]\nid = 1\ndata_dir = \"{}\"\n[mqtt]\nlisten = \"127.0.0.1:0\"\n[dashboard]\nlisten = \"127.0.0.1:0\"\ntoken_file = \"{}\"\ndir = \"{}\"\n",
                dir.0.display(),
                token.display(),
                web.display()
            ),
        )
        .unwrap();
        let invocation = crate::cli::parse(["--config", &config.to_string_lossy()])
            .unwrap()
            .unwrap();
        let options = crate::settings::load(&invocation).unwrap();
        assert!(
            super::settings::prepare(
                &options,
                "127.0.0.1:0".into(),
                ManagementHandle::new(),
                None,
            )
            .is_err(),
            "sensitive companion must not be public: {suffix}"
        );
    }
}

#[test]
fn outbound_queue_directory_cannot_be_loaded_as_public_assets() {
    use mqtt_transport::management::ManagementHandle;
    let dir = Fixture::new();
    let token = dir.0.join("token");
    std::fs::write(&token, b"test-only-0123456789abcdef0123456789abcdef").unwrap();
    let web = dir.0.join("web");
    let queue = web.join("egress");
    std::fs::create_dir_all(&queue).unwrap();
    std::fs::write(web.join("index.html"), b"<html></html>").unwrap();
    std::fs::write(queue.join("private.js"), b"private payload or credentials").unwrap();
    let config = dir.0.join("lyrnormq.toml");
    std::fs::write(
        &config,
        format!(
            "[node]\nid = 1\ndata_dir = \"{}\"\n[mqtt]\nlisten = \"127.0.0.1:0\"\n[dashboard]\nlisten = \"127.0.0.1:0\"\ntoken_file = \"{}\"\ndir = \"{}\"\n",
            web.display(),
            token.display(),
            web.display()
        ),
    )
    .unwrap();
    let invocation = crate::cli::parse(["--config", &config.to_string_lossy()])
        .unwrap()
        .unwrap();
    let options = crate::settings::load(&invocation).unwrap();
    assert!(
        super::settings::prepare(
            &options,
            "127.0.0.1:0".into(),
            ManagementHandle::new(),
            None
        )
        .is_err()
    );
}
