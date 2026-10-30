//! Exclusive-owner SQLite configuration storage, separate from MQTT session data.
//!
//! All methods perform synchronous disk IO. Call them only on a blocking worker.
//! Stored bytes are opaque; callers must validate their configuration before save.
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use std::{
    error::Error,
    fmt,
    fs::{self, File, OpenOptions, TryLockError},
    path::{Path, PathBuf},
    time::Duration,
};

const APPLICATION_ID: i64 = 0x524d4346;
const SCHEMA_VERSION: i64 = 1;
const CREATE_TABLE: &str = "CREATE TABLE rule_configuration (id INTEGER PRIMARY KEY CHECK (id = 1), data BLOB NOT NULL CHECK (typeof(data) = 'blob'))";

/// Redacted configuration-storage failure. No source paths or SQL/data are exposed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigStoreError {
    InvalidLimit,
    InvalidPath,
    Locked,
    InvalidSchema,
    TooLarge,
    Storage,
}
impl fmt::Display for ConfigStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidLimit => "invalid configuration size limit",
            Self::InvalidPath => "configuration database requires a regular non-symlink path",
            Self::Locked => "configuration database is already in use",
            Self::InvalidSchema => "invalid or unsupported configuration database",
            Self::TooLarge => "configuration exceeds the size limit",
            Self::Storage => "configuration storage failed",
        })
    }
}
impl Error for ConfigStoreError {}
impl From<rusqlite::Error> for ConfigStoreError {
    fn from(_: rusqlite::Error) -> Self {
        Self::Storage
    }
}
impl From<std::io::Error> for ConfigStoreError {
    fn from(_: std::io::Error) -> Self {
        Self::Storage
    }
}

/// One SQLite configuration record, protected by a lifetime ownership lock.
///
/// `open` accepts only fresh databases or this module's exact schema. The lock is
/// released on drop; do not use multiple stores for a single configuration file.
pub struct ConfigStore {
    connection: Connection,
    _ownership: File,
    max_bytes: usize,
}
impl fmt::Debug for ConfigStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfigStore").finish_non_exhaustive()
    }
}
impl ConfigStore {
    /// Open or initialize a private SQLite database with a positive byte limit.
    pub fn open(path: &Path, max_bytes: usize) -> Result<Self, ConfigStoreError> {
        if max_bytes == 0 || max_bytes > i32::MAX as usize - 4096 {
            return Err(ConfigStoreError::InvalidLimit);
        }
        let path = normalized(path)?;
        let mut lock = path.as_os_str().to_os_string();
        lock.push(".config-lock");
        let ownership = private_file(Path::new(&lock))?.0;
        match ownership.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(ConfigStoreError::Locked),
            Err(TryLockError::Error(_)) => return Err(ConfigStoreError::Storage),
        }
        let (file, created) = private_file(&path)?;
        drop(file);
        let mut connection = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?;
        connection.busy_timeout(Duration::from_secs(2))?;
        connection.pragma_update(None, "trusted_schema", false)?;
        validate_header(&connection)?;
        // Do not alter the journal of an unknown/foreign database.
        let journal: String =
            connection.query_row("PRAGMA journal_mode=DELETE", [], |row| row.get(0))?;
        if !journal.eq_ignore_ascii_case("delete") {
            return Err(ConfigStoreError::Storage);
        }
        connection.pragma_update(None, "synchronous", "FULL")?;
        let synchronous: i64 = connection.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
        if synchronous != 2 {
            return Err(ConfigStoreError::Storage);
        }
        let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version == 0 {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute(CREATE_TABLE, [])?;
            transaction.pragma_update(None, "application_id", APPLICATION_ID)?;
            transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            transaction.commit()?;
        }
        let store = Self {
            connection,
            _ownership: ownership,
            max_bytes,
        };
        store.validate_rows()?;
        if created {
            let parent = path.parent().ok_or(ConfigStoreError::InvalidPath)?;
            File::open(parent)?.sync_all()?;
        }
        Ok(store)
    }

    /// Load the singleton record after checking its encoded size before copying.
    pub fn load(&self) -> Result<Option<Vec<u8>>, ConfigStoreError> {
        self.validate_rows()?;
        let mut statement = self
            .connection
            .prepare("SELECT data FROM rule_configuration WHERE id = 1")?;
        let mut rows = statement.query([])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        // Bound the value from this same SQLite row before allocating a Vec.
        // Another connection cannot race a preceding length query to force an
        // unbounded copy here.
        match row.get_ref(0)? {
            rusqlite::types::ValueRef::Blob(bytes) if bytes.len() <= self.max_bytes => {
                Ok(Some(bytes.to_vec()))
            }
            rusqlite::types::ValueRef::Blob(_) => Err(ConfigStoreError::TooLarge),
            _ => Err(ConfigStoreError::InvalidSchema),
        }
    }

    /// Durably replace the complete configuration in one SQLite transaction.
    ///
    /// A failed transaction preserves the prior record. Callers must treat an IO
    /// error at commit as uncertain and stop before publishing an in-memory view.
    pub fn save(&mut self, bytes: &[u8]) -> Result<(), ConfigStoreError> {
        if bytes.len() > self.max_bytes {
            return Err(ConfigStoreError::TooLarge);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute("INSERT INTO rule_configuration(id, data) VALUES(1, ?1) ON CONFLICT(id) DO UPDATE SET data=excluded.data", [bytes])?;
        transaction.commit()?;
        Ok(())
    }

    fn validate_rows(&self) -> Result<(), ConfigStoreError> {
        let invalid: i64 = self.connection.query_row(
            "SELECT count(*) FROM rule_configuration WHERE id != 1 OR typeof(data) != 'blob'",
            [],
            |row| row.get(0),
        )?;
        if invalid != 0 {
            return Err(ConfigStoreError::InvalidSchema);
        }
        let length: Option<i64> = self
            .connection
            .query_row(
                "SELECT length(data) FROM rule_configuration WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(length) = length
            && (length < 0 || length as u64 > self.max_bytes as u64)
        {
            return Err(ConfigStoreError::TooLarge);
        }
        Ok(())
    }
}

fn validate_header(connection: &Connection) -> Result<(), ConfigStoreError> {
    let application: i64 = connection.query_row("PRAGMA application_id", [], |row| row.get(0))?;
    let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let count: i64 = connection.query_row(
        "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    if (application, version, count) == (0, 0, 0) {
        return Ok(());
    }
    if (application, version, count) != (APPLICATION_ID, SCHEMA_VERSION, 1) {
        return Err(ConfigStoreError::InvalidSchema);
    }
    let table: Option<(String, String)> = connection
        .query_row(
            "SELECT type, sql FROM sqlite_schema WHERE name='rule_configuration'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if table
        .as_ref()
        .map(|(kind, sql)| (kind.as_str(), sql.as_str()))
        != Some(("table", CREATE_TABLE))
    {
        return Err(ConfigStoreError::InvalidSchema);
    }
    let check: String = connection.query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
    if check != "ok" {
        return Err(ConfigStoreError::InvalidSchema);
    }
    Ok(())
}

fn normalized(path: &Path) -> Result<PathBuf, ConfigStoreError> {
    match fs::symlink_metadata(path) {
        Ok(meta) if !meta.is_file() || meta.file_type().is_symlink() => {
            return Err(ConfigStoreError::InvalidPath);
        }
        Ok(_) => return Ok(path.canonicalize()?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(ConfigStoreError::Storage),
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = path.file_name().ok_or(ConfigStoreError::InvalidPath)?;
    Ok(parent.canonicalize()?.join(name))
}

fn private_file(path: &Path) -> Result<(File, bool), ConfigStoreError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(file) => Ok((file, true)),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let meta = fs::symlink_metadata(path)?;
            if !meta.is_file() || meta.file_type().is_symlink() {
                return Err(ConfigStoreError::InvalidPath);
            }
            Ok((OpenOptions::new().read(true).write(true).open(path)?, false))
        }
        Err(_) => Err(ConfigStoreError::Storage),
    }
}

#[cfg(test)]
mod tests;
