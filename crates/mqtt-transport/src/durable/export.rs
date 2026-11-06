//! Offline inspection: merge a stopped broker's rows into one schema7 envelope.
use super::{model_error, session_config};
use crate::BrokerConfig;
use mqtt_session::durable::{Clock, SessionEngine, decode_retained_row};
use mqtt_storage::durable::{Contents, RowLimits, read_contents};
use std::{io, sync::Arc};

/// Evaluates restored state at its persisted clock floor, so repeated exports
/// of unchanged rows are byte-identical.
#[derive(Debug)]
struct StoredClock;
impl Clock for StoredClock {
    fn now_millis(&self) -> u64 {
        0
    }
}

/// Read `config.storage_path` without taking the broker lock and return the
/// merged schema7 envelope. A container1 database is returned as stored (`None`
/// when it holds no snapshot). Blocking; call off the async runtime.
pub fn export_state(config: &BrokerConfig) -> io::Result<Option<Vec<u8>>> {
    let path = config.storage_path.as_ref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "export-state requires a session database",
        )
    })?;
    let limits = RowLimits {
        max_row_bytes: config.max_state_bytes,
        max_sessions: config.max_sessions,
        max_retained: config.max_retained,
        max_legacy_bytes: config.max_state_bytes,
    };
    let rows = match read_contents(path, limits).map_err(io::Error::other)? {
        Contents::Legacy(snapshot) => return Ok(snapshot),
        Contents::Rows(rows) => rows,
    };
    let session = session_config(config);
    let partitions = rows
        .sessions
        .iter()
        .map(|(_, bytes)| {
            SessionEngine::restore_partition(
                session,
                bytes,
                Arc::new(mqtt_auth::AllowAnonymous),
                Arc::new(StoredClock),
            )
            .map_err(model_error)
        })
        .collect::<io::Result<Vec<_>>>()?;
    let retained = rows
        .retained
        .iter()
        .map(|(_, bytes)| decode_retained_row(session, bytes).map_err(model_error))
        .collect::<io::Result<Vec<_>>>()?;
    let merged =
        SessionEngine::from_partitions(session, partitions, retained).map_err(model_error)?;
    merged.snapshot().map(Some).map_err(model_error)
}
