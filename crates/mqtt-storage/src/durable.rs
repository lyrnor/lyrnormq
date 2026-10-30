//! Synchronous, exclusive-owner SQLite storage.
//!
//! Container1 ([`SqliteStore`]) holds one atomic broker snapshot; container2
//! ([`RowStore`]) holds one row per persistent session partition and retained
//! topic and is written by the group-committing [`StorageService`]. Calls perform
//! blocking disk IO and must run on a blocking thread. Row contents belong to the
//! versioned session schema.
use rusqlite::{Connection, OptionalExtension, TransactionBehavior};
use std::{
    error::Error,
    fmt,
    fs::{File, OpenOptions, TryLockError},
    io,
    path::Path,
    time::Duration,
};

mod rows;
mod service;

pub use rows::{Contents, RowChange, RowLimits, RowStore, Rows, read_contents};
pub use service::{Completion, StorageService, SubmitError};

const SCHEMA_VERSION: i64 = 1;
const APPLICATION_ID: i64 = 0x524d5154;
const CREATE_TABLE: &str = "CREATE TABLE broker_snapshot (id INTEGER PRIMARY KEY CHECK (id = 1), schema_version INTEGER NOT NULL CHECK (schema_version = 1), data BLOB NOT NULL)";

/// Failure never authorizes sending a successful MQTT acknowledgement. On IO or
/// SQLite commit errors the caller must treat the result as failed/ambiguous and
/// must not install its proposed in-memory state. Reopen/recovery is authoritative.
#[derive(Debug)]
pub enum DurableError {
    Io(io::Error),
    Sqlite(rusqlite::Error),
    InvalidLimit,
    SnapshotTooLarge,
    Schema(String),
    Locked,
}
impl fmt::Display for DurableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "durable storage IO: {error}"),
            Self::Sqlite(error) => write!(f, "durable storage SQLite: {error}"),
            Self::InvalidLimit => f.write_str("snapshot byte limit must be positive"),
            Self::SnapshotTooLarge => f.write_str("snapshot exceeds configured byte limit"),
            Self::Schema(message) => write!(f, "unsupported/corrupt durable schema: {message}"),
            Self::Locked => f.write_str("database is already owned by another broker"),
        }
    }
}
impl Error for DurableError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Sqlite(error) => Some(error),
            _ => None,
        }
    }
}
impl From<io::Error> for DurableError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<rusqlite::Error> for DurableError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sqlite(value)
    }
}

/// An opaque, size-bounded, versioned snapshot in SQLite WAL/FULL mode.
///
/// A nonblocking exclusive OS lock is held on a canonical-path sidecar for this
/// object's lifetime; process termination releases it. The sidecar is never deleted,
/// and its existence is not used as a lock. Locking the DB inode directly conflicts
/// with SQLite's native locks on some platforms.
/// All cooperating broker writers must use this API. External SQLite tools must
/// not mutate an actively owned database. SQLite's own transaction locks remain
/// in force. Parent directories must already exist. Unknown schemas fail closed.
pub struct SqliteStore {
    // Field order closes SQLite before releasing the separate ownership handle.
    connection: Connection,
    _ownership: File,
    max_snapshot_bytes: usize,
}
impl SqliteStore {
    pub fn open(path: &Path, max_snapshot_bytes: usize) -> Result<Self, DurableError> {
        if max_snapshot_bytes == 0 {
            return Err(DurableError::InvalidLimit);
        }
        let (mut connection, ownership) = acquire(path)?;
        // Validate before changing journal mode or writing any schema metadata.
        let fresh = match header(&connection)? {
            Header::Fresh => true,
            Header::Legacy => {
                validate_legacy(&connection)?;
                false
            }
            Header::Rows | Header::Unknown => {
                return Err(DurableError::Schema("database header or object set".into()));
            }
        };
        configure(&connection)?;
        if fresh {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute(CREATE_TABLE, [])?;
            transaction.pragma_update(None, "application_id", APPLICATION_ID)?;
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            transaction.commit()?;
        }
        validate_legacy_rows(&connection)?;
        let store = Self {
            connection,
            _ownership: ownership,
            max_snapshot_bytes,
        };
        // Validate the configured load bound before returning a usable store.
        store.checked_length()?;
        Ok(store)
    }

    fn checked_length(&self) -> Result<Option<usize>, DurableError> {
        legacy_length(&self.connection, self.max_snapshot_bytes)
    }

    /// Query length before allocating a BLOB. The lifetime ownership lock excludes
    /// cooperating writers, so no other broker can change it between queries.
    pub fn load(&self) -> Result<Option<Vec<u8>>, DurableError> {
        load_legacy(&self.connection, self.max_snapshot_bytes)
    }

    /// Atomically replace the complete persistent session/outbox registry.
    ///
    /// Rejected size checks occur before SQL; transaction errors roll back on
    /// drop. A successful return means SQLite committed under synchronous=FULL.
    /// Durability still depends on filesystem/storage honoring sync requests.
    pub fn commit(&mut self, snapshot: &[u8]) -> Result<(), DurableError> {
        if snapshot.len() > self.max_snapshot_bytes {
            return Err(DurableError::SnapshotTooLarge);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT INTO broker_snapshot (id, schema_version, data) VALUES (1, 1, ?1) ON CONFLICT(id) DO UPDATE SET schema_version = 1, data = excluded.data",
            [snapshot],
        )?;
        transaction.commit()?;
        Ok(())
    }
}

enum Header {
    Fresh,
    Legacy,
    Rows,
    Unknown,
}

/// Classify by header and object count only; table definitions are checked by
/// the matching validator before any write.
fn header(connection: &Connection) -> Result<Header, DurableError> {
    let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let application: i64 = connection.query_row("PRAGMA application_id", [], |row| row.get(0))?;
    let objects: i64 = connection.query_row(
        "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    Ok(match (version, application, objects) {
        (0, 0, 0) => Header::Fresh,
        (SCHEMA_VERSION, APPLICATION_ID, 1) => Header::Legacy,
        (rows::ROW_SCHEMA_VERSION, APPLICATION_ID, 3) => Header::Rows,
        _ => Header::Unknown,
    })
}

fn table_sql(connection: &Connection, name: &str) -> Result<Option<String>, DurableError> {
    let table: Option<(String, String)> = connection
        .query_row(
            "SELECT type, sql FROM sqlite_schema WHERE name = ?1",
            [name],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    Ok(table.and_then(|(kind, sql)| (kind == "table").then_some(sql)))
}

fn validate_legacy(connection: &Connection) -> Result<(), DurableError> {
    if table_sql(connection, "broker_snapshot")?.as_deref() != Some(CREATE_TABLE) {
        return Err(DurableError::Schema("snapshot table definition".into()));
    }
    Ok(())
}

fn validate_legacy_rows(connection: &Connection) -> Result<(), DurableError> {
    quick_check(connection)?;
    let invalid_rows: i64 = connection.query_row(
        "SELECT count(*) FROM broker_snapshot WHERE id != 1 OR schema_version != 1 OR typeof(data) != 'blob'",
        [],
        |row| row.get(0),
    )?;
    if invalid_rows != 0 {
        return Err(DurableError::Schema("invalid snapshot row".into()));
    }
    Ok(())
}

fn quick_check(connection: &Connection) -> Result<(), DurableError> {
    let check: String = connection.query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
    if check != "ok" {
        return Err(DurableError::Schema(check));
    }
    Ok(())
}

fn legacy_length(connection: &Connection, max_bytes: usize) -> Result<Option<usize>, DurableError> {
    let length: Option<i64> = connection
        .query_row(
            "SELECT length(data) FROM broker_snapshot WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    length
        .map(|value| {
            let value = usize::try_from(value)
                .map_err(|_| DurableError::Schema("negative snapshot length".into()))?;
            if value > max_bytes {
                return Err(DurableError::SnapshotTooLarge);
            }
            Ok(value)
        })
        .transpose()
}

fn load_legacy(connection: &Connection, max_bytes: usize) -> Result<Option<Vec<u8>>, DurableError> {
    if legacy_length(connection, max_bytes)?.is_none() {
        return Ok(None);
    }
    Ok(connection
        .query_row("SELECT data FROM broker_snapshot WHERE id = 1", [], |row| {
            row.get(0)
        })
        .optional()?)
}

fn configure(connection: &Connection) -> Result<(), DurableError> {
    let mode: String = connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(DurableError::Schema("WAL mode was not established".into()));
    }
    connection.pragma_update(None, "synchronous", "FULL")?;
    let synchronous: i64 = connection.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
    if synchronous != 2 {
        return Err(DurableError::Schema(
            "FULL synchronization was not established".into(),
        ));
    }
    Ok(())
}

/// Canonicalize, take the nonblocking sidecar ownership lock, then open SQLite.
fn acquire(path: &Path) -> Result<(Connection, File), DurableError> {
    let normalized = match path.canonicalize() {
        Ok(path) => path,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            // A dangling symlink has no canonical target yet. Falling back
            // to its name would give it a different ownership lock from the
            // target after SQLite creates that file. Reject before any IO
            // that could create the target or a sidecar under an alias.
            match path.symlink_metadata() {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "database path must not be a dangling symlink",
                    )
                    .into());
                }
                Ok(_) => return Err(error.into()),
                Err(metadata_error) if metadata_error.kind() == io::ErrorKind::NotFound => {}
                Err(metadata_error) => return Err(metadata_error.into()),
            }
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let name = path.file_name().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "database filename required")
            })?;
            parent.canonicalize()?.join(name)
        }
        Err(error) => return Err(error.into()),
    };
    let mut lock_path = normalized.as_os_str().to_os_string();
    lock_path.push(".broker-lock");
    let ownership = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(Path::new(&lock_path))?;
    match ownership.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => return Err(DurableError::Locked),
        Err(TryLockError::Error(error)) => return Err(DurableError::Io(error)),
    }
    let connection = Connection::open(&normalized)?;
    connection.busy_timeout(Duration::from_secs(2))?;
    Ok((connection, ownership))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{BufRead, Write},
        path::PathBuf,
        process::{Command, Stdio},
        sync::atomic::{AtomicU64, Ordering},
    };
    static NEXT: AtomicU64 = AtomicU64::new(0);
    pub(super) use crate::test_support::FILE_TESTS;
    pub(super) struct TempDb(pub(super) PathBuf);
    impl TempDb {
        pub(super) fn new() -> Self {
            let directory = std::env::temp_dir().join(format!(
                "rmqtt-sqlite-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed),
            ));
            std::fs::create_dir(&directory).unwrap();
            Self(directory.join("state.sqlite"))
        }
    }
    impl Drop for TempDb {
        fn drop(&mut self) {
            if let Some(parent) = self.0.parent() {
                let _ = std::fs::remove_dir_all(parent);
            }
        }
    }
    #[test]
    fn commit_reopen_and_limits_preserve_previous_snapshot() {
        let _serial = FILE_TESTS.lock().unwrap();
        let path = TempDb::new();
        {
            let mut store = SqliteStore::open(&path.0, 8).unwrap();
            assert_eq!(store.load().unwrap(), None);
            store.commit(b"old").unwrap();
            assert!(matches!(
                store.commit(b"too large"),
                Err(DurableError::SnapshotTooLarge)
            ));
            assert_eq!(store.load().unwrap(), Some(b"old".to_vec()));
            store.commit(b"new").unwrap();
        }
        assert!(matches!(
            SqliteStore::open(&path.0, 2),
            Err(DurableError::SnapshotTooLarge)
        ));
        let store = SqliteStore::open(&path.0, 8).unwrap();
        assert_eq!(store.load().unwrap(), Some(b"new".to_vec()));
        assert_eq!(
            store
                .connection
                .query_row("PRAGMA synchronous", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            store
                .connection
                .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "wal"
        );
    }
    #[test]
    fn exclusive_owner_lock_releases_on_drop() {
        let _serial = FILE_TESTS.lock().unwrap();
        let path = TempDb::new();
        let first = SqliteStore::open(&path.0, 8).unwrap();
        assert!(matches!(
            SqliteStore::open(&path.0, 8),
            Err(DurableError::Locked)
        ));
        drop(first);
        assert!(SqliteStore::open(&path.0, 8).is_ok());
    }
    #[cfg(unix)]
    #[test]
    fn rejects_dangling_symlink_but_existing_alias_shares_owner_lock() {
        let _serial = FILE_TESTS.lock().unwrap();
        let path = TempDb::new();
        let alias = path.0.parent().unwrap().join("alias.sqlite");
        std::os::unix::fs::symlink(&path.0, &alias).unwrap();
        assert!(matches!(
            SqliteStore::open(&alias, 8),
            Err(DurableError::Io(error)) if error.kind() == io::ErrorKind::InvalidInput
        ));
        assert!(
            !path.0.exists(),
            "rejected alias must not create its target"
        );
        let _owner = SqliteStore::open(&path.0, 8).unwrap();
        assert!(matches!(
            SqliteStore::open(&alias, 8),
            Err(DurableError::Locked)
        ));
    }
    #[test]
    fn rejects_foreign_unknown_or_corrupt_schemas_without_resetting() {
        let _serial = FILE_TESTS.lock().unwrap();
        let foreign = TempDb::new();
        let connection = Connection::open(&foreign.0).unwrap();
        connection
            .execute("CREATE TABLE important(value TEXT)", [])
            .unwrap();
        connection
            .execute("INSERT INTO important VALUES ('keep')", [])
            .unwrap();
        drop(connection);
        assert!(matches!(
            SqliteStore::open(&foreign.0, 8),
            Err(DurableError::Schema(_))
        ));
        assert_eq!(
            Connection::open(&foreign.0)
                .unwrap()
                .query_row("SELECT value FROM important", [], |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "keep"
        );
        let unknown = TempDb::new();
        drop(SqliteStore::open(&unknown.0, 8).unwrap());
        Connection::open(&unknown.0)
            .unwrap()
            .pragma_update(None, "user_version", 99)
            .unwrap();
        assert!(matches!(
            SqliteStore::open(&unknown.0, 8),
            Err(DurableError::Schema(_))
        ));
        let corrupt = TempDb::new();
        std::fs::write(&corrupt.0, b"not a SQLite database").unwrap();
        assert!(SqliteStore::open(&corrupt.0, 8).is_err());
        assert!(matches!(
            SqliteStore::open(&unknown.0, 0),
            Err(DurableError::InvalidLimit)
        ));
    }
    #[test]
    fn disk_full_sql_error_rolls_back_and_store_can_continue() {
        let _serial = FILE_TESTS.lock().unwrap();
        let path = TempDb::new();
        let mut store = SqliteStore::open(&path.0, 128 * 1024).unwrap();
        store.commit(b"committed").unwrap();
        let pages: i64 = store
            .connection
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .unwrap();
        store
            .connection
            .pragma_update(None, "max_page_count", pages)
            .unwrap();
        assert!(matches!(
            store.commit(&vec![42; 100 * 1024]),
            Err(DurableError::Sqlite(_))
        ));
        assert_eq!(store.load().unwrap(), Some(b"committed".to_vec()));
        store.commit(b"still usable").unwrap();
        drop(store);
        assert_eq!(
            SqliteStore::open(&path.0, 128 * 1024)
                .unwrap()
                .load()
                .unwrap(),
            Some(b"still usable".to_vec())
        );
    }
    #[test]
    fn killed_uncommitted_transaction_keeps_prior_snapshot_and_releases_lock() {
        let _serial = FILE_TESTS.lock().unwrap();
        if let Some(path) = std::env::var_os("RMQTT_SQLITE_CRASH_CHILD") {
            let store = SqliteStore::open(Path::new(&path), 1024).unwrap();
            store.connection.execute_batch("BEGIN IMMEDIATE; UPDATE broker_snapshot SET data = x'6e6f7420636f6d6d6974746564' WHERE id = 1").unwrap();
            println!("RMQTT_UNCOMMITTED");
            std::io::stdout().flush().unwrap();
            loop {
                std::thread::park();
            }
        }
        let path = TempDb::new();
        {
            let mut store = SqliteStore::open(&path.0, 1024).unwrap();
            store.commit(b"prior commit").unwrap();
        }
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "durable::tests::killed_uncommitted_transaction_keeps_prior_snapshot_and_releases_lock", "--nocapture"])
            .env("RMQTT_SQLITE_CRASH_CHILD", &path.0)
            .stdout(Stdio::piped()).spawn().unwrap();
        let output = child.stdout.take().unwrap();
        let mut reader = std::io::BufReader::new(output);
        loop {
            let mut line = String::new();
            assert_ne!(
                reader.read_line(&mut line).unwrap(),
                0,
                "crash child exited before transaction barrier"
            );
            if line.contains("RMQTT_UNCOMMITTED") {
                break;
            }
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let store = SqliteStore::open(&path.0, 1024).unwrap();
        assert_eq!(store.load().unwrap(), Some(b"prior commit".to_vec()));
    }
}
