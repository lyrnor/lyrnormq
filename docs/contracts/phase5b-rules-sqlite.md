> Configuration entities and adapter architecture superseded by [Phase5c](phase5c-unified-outbounds.md). SQLite container semantics remain in force.

# Phase 5b: SQLite rule configuration (frozen 2026-10-30)

Scope: configuration persistence only; MQTT durable schema/outbox and best-effort
action delivery stay unchanged. Use a separate configuration SQLite database; never
insert tables into the existing session database (its object set is validated).

## Storage API (storage owner)

Add public `mqtt_storage::configuration::{ConfigStore, ConfigStoreError}`.
Synchronous blocking-only API:
- `ConfigStore::open(path: &Path, max_bytes: usize) -> Result<Self, ConfigStoreError>`
- `load(&self) -> Result<Option<Vec<u8>>, ConfigStoreError>`
- `save(&mut self, bytes: &[u8]) -> Result<(), ConfigStoreError>`
A dedicated application_id/schema version and exact singleton configuration table
validate unknown/corrupt databases before writes. Opaque bytes, positive max <=
caller limit, bounded read (check length before copying), full transaction replace.
Synchronous FULL, rollback journal, finite busy timeout; reject nonregular/symlink
paths and foreign databases. Newly created DB and lock mode0600. Hold a canonical
path process-ownership lock for Store lifetime; concurrent open fails. No async API.
Errors redact credentials/paths. No database schema change to durable session store.

## Runtime API (Webhook owner)

`Runtime::open(PathBuf)` now accepts SQLite, no extension-based JSON behavior.
No JSON file compatibility or import API/CLI. If no saved row exists, validate
and persist the empty default. Existing persisted bytes must deserialize and
validate before starting; invalid data fails closed without overwriting it.
Config JSON remains an internal bounded opaque blob, not a JSON file backend.
HTTP contract unchanged. Build client/validate before commit; publish immutable
snapshot only after commit. All DB work on spawn_blocking with store mutex inside
blocking worker, never across await. Caller cancellation cannot orphan mutation
permit. On any uncertain SQLite save error conservatively stop runtime/old
generation, return redacted Storage; no success claim. SQL validation errors do not
stop runtime or change committed config. Shutdown waits for mutation and releases
Store ownership so same-process reopen works. Config store has no MQTT hot-path IO.
Remove obsolete rename/directory-sync fault injection and replace with rollback/
SQLite failure tests. Keep lib.rs declarations/reexports only.

## CLI/coordinator

Default enabled path `lyrnormq-rules.sqlite` in cwd. `--rules-config PATH` only
overrides the SQLite path; it never accepts/imports JSON files. Export-state
neither reads nor creates configuration. Static asset sensitive alias protection
includes SQLite DB, -journal/-wal/-shm and .config-lock. Error/status API and
Dashboard remain unchanged. Legacy rules.json files are never inspected/imported.
This supersedes all Phase5a JSON-file persistence and directory-sync rules.

## Acceptance

Real SQLite header/schema; atomic rollback and bounded data; rejected foreign,
corrupt/symlink databases; exclusive owner; config secrets redacted in API yet
restored for HTTP; restart and SIGKILL recovery of acknowledged config; JSON input
files rejected without overwrite and no implicit JSON reads. Default
startup API ready without flags, custom overrides, export no side effects.
No throughput/p99 or durable message-delivery claim.
