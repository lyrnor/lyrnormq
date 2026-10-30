use super::*;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    directory: PathBuf,
    _serial: std::sync::MutexGuard<'static, ()>,
}
impl Fixture {
    fn new() -> Self {
        let serial = crate::test_support::FILE_TESTS.lock().unwrap();
        let path = std::env::temp_dir().join(format!(
            "rmqtt-config-store-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self {
            directory: path,
            _serial: serial,
        }
    }
    fn path(&self) -> PathBuf {
        self.directory.join("rules.sqlite")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[test]
fn sqlite_roundtrip_limits_and_private_permissions() {
    let f = Fixture::new();
    let mut store = ConfigStore::open(&f.path(), 4).unwrap();
    assert_eq!(store.load().unwrap(), None);
    store.save(b"abcd").unwrap();
    assert_eq!(store.load().unwrap(), Some(b"abcd".to_vec()));
    assert!(matches!(
        store.save(b"abcde"),
        Err(ConfigStoreError::TooLarge)
    ));
    drop(store);
    assert_eq!(
        &std::fs::read(f.path()).unwrap()[..16],
        b"SQLite format 3\0"
    );
    let store = ConfigStore::open(&f.path(), 4).unwrap();
    assert_eq!(store.load().unwrap(), Some(b"abcd".to_vec()));
    drop(store);
    assert!(matches!(
        ConfigStore::open(&f.path(), 3),
        Err(ConfigStoreError::TooLarge)
    ));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(f.path()).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(f.path().with_extension("sqlite.config-lock"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[test]
fn exclusive_canonical_owner_releases_on_drop() {
    let f = Fixture::new();
    let store = ConfigStore::open(&f.path(), 64).unwrap();
    assert!(matches!(
        ConfigStore::open(&f.directory.join(".").join("rules.sqlite"), 64),
        Err(ConfigStoreError::Locked)
    ));
    drop(store);
    ConfigStore::open(&f.path(), 64).unwrap();
}

#[test]
fn transaction_failure_preserves_previous_configuration() {
    let f = Fixture::new();
    let mut store = ConfigStore::open(&f.path(), 64).unwrap();
    store.save(b"before").unwrap();
    store.connection.execute_batch("CREATE TRIGGER reject_update BEFORE UPDATE ON rule_configuration BEGIN SELECT RAISE(ABORT, 'injected'); END;").unwrap();
    assert!(store.save(b"after").is_err());
    assert_eq!(store.load().unwrap(), Some(b"before".to_vec()));
    store
        .connection
        .execute_batch("DROP TRIGGER reject_update;")
        .unwrap();
    drop(store);
    assert_eq!(
        ConfigStore::open(&f.path(), 64).unwrap().load().unwrap(),
        Some(b"before".to_vec())
    );
}

#[test]
fn foreign_corrupt_and_unknown_schema_are_rejected_without_rewriting() {
    for kind in 0..4 {
        let f = Fixture::new();
        match kind {
            0 => std::fs::write(f.path(), b"not sqlite and do not overwrite").unwrap(),
            1 => Connection::open(f.path())
                .unwrap()
                .execute_batch("CREATE TABLE arbitrary(data);")
                .unwrap(),
            2 => {
                let store = ConfigStore::open(&f.path(), 64).unwrap();
                store
                    .connection
                    .pragma_update(None, "user_version", 2)
                    .unwrap();
            }
            _ => {
                let store = ConfigStore::open(&f.path(), 64).unwrap();
                store
                    .connection
                    .execute_batch("CREATE TABLE extra(data);")
                    .unwrap();
            }
        }
        let before = std::fs::read(f.path()).unwrap();
        assert!(ConfigStore::open(&f.path(), 64).is_err());
        assert_eq!(std::fs::read(f.path()).unwrap(), before);
    }
}

#[test]
fn rejects_zero_limit_directory_and_invalid_blob_rows() {
    let f = Fixture::new();
    assert!(matches!(
        ConfigStore::open(&f.path(), 0),
        Err(ConfigStoreError::InvalidLimit)
    ));
    assert!(!f.path().exists());
    assert!(ConfigStore::open(&f.directory, 64).is_err());
    {
        let store = ConfigStore::open(&f.path(), 64).unwrap();
        store.connection.execute_batch("PRAGMA ignore_check_constraints=ON; INSERT INTO rule_configuration VALUES(2, 'text');").unwrap();
    }
    assert!(ConfigStore::open(&f.path(), 64).is_err());
}

#[cfg(unix)]
#[test]
fn rejects_database_and_lock_symlinks_without_touching_target() {
    for name in ["rules.sqlite", "rules.sqlite.config-lock"] {
        let f = Fixture::new();
        let target = f.directory.join("target");
        std::os::unix::fs::symlink(&target, f.directory.join(name)).unwrap();
        assert!(ConfigStore::open(&f.path(), 64).is_err());
        assert!(!target.exists());
    }
}

#[test]
fn errors_and_debug_do_not_expose_data_or_paths() {
    for error in [
        ConfigStoreError::InvalidLimit,
        ConfigStoreError::InvalidPath,
        ConfigStoreError::Locked,
        ConfigStoreError::InvalidSchema,
        ConfigStoreError::TooLarge,
        ConfigStoreError::Storage,
    ] {
        assert!(!error.to_string().contains("secret"));
    }
    let f = Fixture::new();
    let mut store = ConfigStore::open(&f.path(), 64).unwrap();
    store.save(b"secret").unwrap();
    let debug = format!("{store:?}");
    assert!(!debug.contains("secret"));
    assert!(!debug.contains(&f.path().display().to_string()));
}

#[test]
fn external_oversize_update_is_rejected_on_load_before_copying() {
    let f = Fixture::new();
    let mut store = ConfigStore::open(&f.path(), 4).unwrap();
    store.save(b"okay").unwrap();
    Connection::open(f.path())
        .unwrap()
        .execute(
            "UPDATE rule_configuration SET data = zeroblob(1048576) WHERE id = 1",
            [],
        )
        .unwrap();
    assert_eq!(store.load(), Err(ConfigStoreError::TooLarge));
}

#[test]
fn crash_worker() {
    let Some(path) = std::env::var_os("RMQTT_CONFIG_CRASH_TEST_PATH") else {
        return;
    };
    use std::io::Write;
    let mut store = ConfigStore::open(Path::new(&path), 64).unwrap();
    store.save(b"committed").unwrap();
    if std::env::var_os("RMQTT_CONFIG_CRASH_TEST_UNCOMMITTED").is_some() {
        store.connection.execute_batch("BEGIN IMMEDIATE; UPDATE rule_configuration SET data = x'756e636f6d6d6974746564' WHERE id = 1;").unwrap();
    }
    println!("crash-worker-ready");
    std::io::stdout().flush().unwrap();
    std::thread::sleep(Duration::from_secs(30));
}

#[test]
fn process_kill_recovers_committed_row_and_rolls_back_uncommitted_row() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
        sync::mpsc,
    };
    for uncommitted in [false, true] {
        let f = Fixture::new();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "configuration::tests::crash_worker",
                "--nocapture",
            ])
            .env("RMQTT_CONFIG_CRASH_TEST_PATH", f.path())
            .stdout(Stdio::piped());
        if uncommitted {
            command.env("RMQTT_CONFIG_CRASH_TEST_UNCOMMITTED", "1");
        }
        let mut child = command.spawn().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::sync_channel(1);
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if line.unwrap().contains("crash-worker-ready") {
                    let _ = tx.send(());
                    break;
                }
            }
        });
        let ready = rx.recv_timeout(Duration::from_secs(10));
        child.kill().unwrap();
        child.wait().unwrap();
        reader.join().unwrap();
        ready.unwrap();
        let store = ConfigStore::open(&f.path(), 64).unwrap();
        assert_eq!(store.load().unwrap(), Some(b"committed".to_vec()));
    }
}

#[test]
fn fixture_serializes_with_durable_crash_helpers() {
    let _fixture = Fixture::new();
    assert!(matches!(
        crate::test_support::FILE_TESTS.try_lock(),
        Err(std::sync::TryLockError::WouldBlock)
    ));
}
