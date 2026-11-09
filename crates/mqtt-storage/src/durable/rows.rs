//! Container2: one row per persistent session partition and per retained topic.
//!
//! A container1 database is opened in legacy mode; the caller decodes its snapshot,
//! splits it and calls [`RowStore::migrate`], which replaces the container in one
//! transaction. Until then no row write is accepted.
use rusqlite::{Connection, OpenFlags, TransactionBehavior};
use std::{fs::File, path::Path};

use super::{
    APPLICATION_ID, DurableError, Header, acquire, configure, header, load_legacy, quick_check,
    table_sql, validate_legacy, validate_legacy_rows,
};

pub(super) const ROW_SCHEMA_VERSION: i64 = 2;
const CREATE_META: &str = "CREATE TABLE meta (key TEXT PRIMARY KEY NOT NULL, value BLOB NOT NULL)";
const CREATE_SESSIONS: &str =
    "CREATE TABLE sessions (client_id TEXT PRIMARY KEY NOT NULL, data BLOB NOT NULL)";
const CREATE_RETAINED: &str =
    "CREATE TABLE retained (topic TEXT PRIMARY KEY NOT NULL, data BLOB NOT NULL)";
const FORMAT: &[u8] = b"lyrnormq-rows-2";

/// Load and write bounds; every limit must be positive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowLimits {
    pub max_row_bytes: usize,
    pub max_sessions: usize,
    pub max_retained: usize,
    /// Upper bound for a container1 snapshot read during migration.
    pub max_legacy_bytes: usize,
}
impl RowLimits {
    fn validate(self) -> Result<Self, DurableError> {
        if self.max_row_bytes == 0
            || self.max_sessions == 0
            || self.max_retained == 0
            || self.max_legacy_bytes == 0
        {
            return Err(DurableError::InvalidLimit);
        }
        Ok(self)
    }
}

/// Session rows keyed by client identifier and retained rows keyed by topic,
/// each in key order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Rows {
    pub sessions: Vec<(String, Vec<u8>)>,
    pub retained: Vec<(String, Vec<u8>)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Contents {
    /// Container1 awaiting migration, with its snapshot if one was committed.
    Legacy(Option<Vec<u8>>),
    Rows(Rows),
}

/// One row write; later changes to the same key in a batch win.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowChange {
    PutSession(String, Vec<u8>),
    DeleteSession(String),
    PutRetained(String, Vec<u8>),
    DeleteRetained(String),
}
impl RowChange {
    pub(super) fn bytes(&self) -> usize {
        match self {
            Self::PutSession(_, data) | Self::PutRetained(_, data) => data.len(),
            Self::DeleteSession(_) | Self::DeleteRetained(_) => 0,
        }
    }
}

/// Exclusive-owner container2 store with the same lock, WAL/FULL and
/// fail-closed rules as [`super::SqliteStore`].
pub struct RowStore {
    // Field order closes SQLite before releasing the separate ownership handle.
    connection: Connection,
    _ownership: File,
    pub(super) limits: RowLimits,
    legacy: bool,
}

impl RowStore {
    pub fn open(path: &Path, limits: RowLimits) -> Result<Self, DurableError> {
        let limits = limits.validate()?;
        let (mut connection, ownership) = acquire(path)?;
        let kind = header(&connection)?;
        match kind {
            Header::Fresh => {}
            Header::Legacy => validate_legacy(&connection)?,
            Header::Rows => validate_rows_schema(&connection)?,
            Header::Unknown => {
                return Err(DurableError::Schema("database header or object set".into()));
            }
        }
        configure(&connection)?;
        let legacy = match kind {
            Header::Fresh => {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                create_rows_schema(&transaction)?;
                transaction.commit()?;
                false
            }
            Header::Legacy => {
                validate_legacy_rows(&connection)?;
                true
            }
            _ => false,
        };
        if !legacy {
            validate_rows(&connection, limits)?;
        }
        Ok(Self {
            connection,
            _ownership: ownership,
            limits,
            legacy,
        })
    }

    pub fn is_legacy(&self) -> bool {
        self.legacy
    }

    pub fn contents(&self) -> Result<Contents, DurableError> {
        load_contents(&self.connection, self.limits, self.legacy)
    }

    /// Replace a container1 database with container2 rows in one transaction.
    /// Failure leaves the legacy container unchanged.
    pub fn migrate(&mut self, rows: &Rows) -> Result<(), DurableError> {
        if !self.legacy {
            return Err(DurableError::Schema("database is not container1".into()));
        }
        if rows.sessions.len() > self.limits.max_sessions
            || rows.retained.len() > self.limits.max_retained
            || rows
                .sessions
                .iter()
                .chain(&rows.retained)
                .any(|(_, data)| data.len() > self.limits.max_row_bytes)
        {
            return Err(DurableError::SnapshotTooLarge);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute("DROP TABLE broker_snapshot", [])?;
        create_rows_schema(&transaction)?;
        for (client, data) in &rows.sessions {
            transaction.execute(
                "INSERT INTO sessions (client_id, data) VALUES (?1, ?2)",
                (client, data),
            )?;
        }
        for (topic, data) in &rows.retained {
            transaction.execute(
                "INSERT INTO retained (topic, data) VALUES (?1, ?2)",
                (topic, data),
            )?;
        }
        transaction.commit()?;
        self.legacy = false;
        Ok(())
    }

    /// Reject oversize rows before any SQL.
    pub fn check(&self, changes: &[RowChange]) -> Result<(), DurableError> {
        if changes
            .iter()
            .any(|change| change.bytes() > self.limits.max_row_bytes)
        {
            return Err(DurableError::SnapshotTooLarge);
        }
        Ok(())
    }

    /// Apply every change in one IMMEDIATE transaction; failure rolls back all.
    pub fn apply(&mut self, changes: &[RowChange]) -> Result<(), DurableError> {
        if self.legacy {
            return Err(DurableError::Schema(
                "container1 must be migrated first".into(),
            ));
        }
        self.check(changes)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        for change in changes {
            match change {
                RowChange::PutSession(client, data) => transaction.execute(
                    "INSERT INTO sessions (client_id, data) VALUES (?1, ?2) ON CONFLICT(client_id) DO UPDATE SET data = excluded.data",
                    (client, data),
                )?,
                RowChange::DeleteSession(client) => {
                    transaction.execute("DELETE FROM sessions WHERE client_id = ?1", [client])?
                }
                RowChange::PutRetained(topic, data) => transaction.execute(
                    "INSERT INTO retained (topic, data) VALUES (?1, ?2) ON CONFLICT(topic) DO UPDATE SET data = excluded.data",
                    (topic, data),
                )?,
                RowChange::DeleteRetained(topic) => {
                    transaction.execute("DELETE FROM retained WHERE topic = ?1", [topic])?
                }
            };
        }
        transaction.commit()?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn connection(&self) -> &Connection {
        &self.connection
    }
}

/// Read a database without taking ownership, for offline inspection. A missing
/// or empty file reads as no rows; nothing is created or migrated.
pub fn read_contents(path: &Path, limits: RowLimits) -> Result<Contents, DurableError> {
    let limits = limits.validate()?;
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    match header(&connection)? {
        Header::Fresh => Ok(Contents::Rows(Rows::default())),
        Header::Legacy => {
            validate_legacy(&connection)?;
            validate_legacy_rows(&connection)?;
            load_contents(&connection, limits, true)
        }
        Header::Rows => {
            validate_rows_schema(&connection)?;
            validate_rows(&connection, limits)?;
            load_contents(&connection, limits, false)
        }
        Header::Unknown => Err(DurableError::Schema("database header or object set".into())),
    }
}

fn create_rows_schema(connection: &Connection) -> Result<(), DurableError> {
    connection.execute(CREATE_META, [])?;
    connection.execute(CREATE_SESSIONS, [])?;
    connection.execute(CREATE_RETAINED, [])?;
    connection.execute(
        "INSERT INTO meta (key, value) VALUES ('format', ?1)",
        [FORMAT],
    )?;
    connection.pragma_update(None, "application_id", APPLICATION_ID)?;
    connection.pragma_update(None, "user_version", ROW_SCHEMA_VERSION)?;
    Ok(())
}

fn validate_rows_schema(connection: &Connection) -> Result<(), DurableError> {
    for (name, sql) in [
        ("meta", CREATE_META),
        ("sessions", CREATE_SESSIONS),
        ("retained", CREATE_RETAINED),
    ] {
        if table_sql(connection, name)?.as_deref() != Some(sql) {
            return Err(DurableError::Schema(format!("{name} table definition")));
        }
    }
    Ok(())
}

fn validate_rows(connection: &Connection, limits: RowLimits) -> Result<(), DurableError> {
    quick_check(connection)?;
    let format: Vec<u8> =
        connection.query_row("SELECT value FROM meta WHERE key = 'format'", [], |row| {
            row.get(0)
        })?;
    let meta: i64 = connection.query_row("SELECT count(*) FROM meta", [], |row| row.get(0))?;
    if format != FORMAT || meta != 1 {
        return Err(DurableError::Schema("container2 metadata".into()));
    }
    for (table, key, limit) in [
        ("sessions", "client_id", limits.max_sessions),
        ("retained", "topic", limits.max_retained),
    ] {
        let (count, invalid, longest): (i64, i64, Option<i64>) = connection.query_row(
            &format!(
                "SELECT count(*), sum(typeof({key}) != 'text' OR typeof(data) != 'blob'), max(length(data)) FROM {table}"
            ),
            [],
            |row| Ok((row.get(0)?, row.get::<_, Option<i64>>(1)?.unwrap_or(0), row.get(2)?)),
        )?;
        if invalid != 0 {
            return Err(DurableError::Schema(format!("invalid {table} row")));
        }
        let count = usize::try_from(count)
            .map_err(|_| DurableError::Schema(format!("{table} row count")))?;
        let longest = usize::try_from(longest.unwrap_or(0))
            .map_err(|_| DurableError::Schema(format!("{table} row length")))?;
        if count > limit || longest > limits.max_row_bytes {
            return Err(DurableError::SnapshotTooLarge);
        }
    }
    Ok(())
}

fn load_contents(
    connection: &Connection,
    limits: RowLimits,
    legacy: bool,
) -> Result<Contents, DurableError> {
    if legacy {
        return Ok(Contents::Legacy(load_legacy(
            connection,
            limits.max_legacy_bytes,
        )?));
    }
    let mut rows = Rows::default();
    for (table, key, target) in [
        ("sessions", "client_id", &mut rows.sessions),
        ("retained", "topic", &mut rows.retained),
    ] {
        let mut statement =
            connection.prepare(&format!("SELECT {key}, data FROM {table} ORDER BY {key}"))?;
        let mapped = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        for row in mapped {
            target.push(row?);
        }
    }
    Ok(Contents::Rows(rows))
}

#[cfg(test)]
#[path = "rows_tests.rs"]
mod tests;
