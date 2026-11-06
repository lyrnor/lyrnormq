"""Phase4 durable-state helpers for black-box suites.

The broker stores one row per session and per retained topic (container2), so
tests read state only through `rmqtt export-state`, which prints the merged
schema7 envelope of a stopped broker. Old-schema fixtures are written as a
container1 `broker_snapshot` database, which the broker migrates on startup;
a rejected migration leaves that container1 database untouched.
"""
import json
import os
import sqlite3
import subprocess
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APPLICATION_ID = 0x524D5154
LEGACY_TABLE = ('CREATE TABLE broker_snapshot (id INTEGER PRIMARY KEY CHECK (id = 1), '
                'schema_version INTEGER NOT NULL CHECK (schema_version = 1), data BLOB NOT NULL)')


def binary():
    return os.environ.get('RMQTT_BIN', str(ROOT / 'target/debug/rmqtt'))


def export_bytes(db, *args):
    """Merged envelope bytes of a stopped broker's database, or None."""
    db = Path(db)
    if db.name != 'sessions.sqlite':
        raise AssertionError(f'session database must be named sessions.sqlite, got {db.name}')
    from rmqttcfg import argv
    result = subprocess.run(argv(binary(), '127.0.0.1:0', db.parent, command='export-state'),
                            capture_output=True, timeout=30, check=False)
    if result.returncode:
        raise AssertionError(f'export-state failed: {result.stderr.decode()}')
    data = result.stdout.strip()
    return data or None


def export_state(db, *args):
    data = export_bytes(db, *args)
    return None if data is None else json.loads(data)


def write_legacy(db, data):
    """Replace `db` by a container1 database holding `data` (bytes or JSON value)."""
    if not isinstance(data, (bytes, bytearray)):
        data = json.dumps(data).encode()
    for suffix in ('', '-wal', '-shm'):
        path = Path(f'{db}{suffix}')
        if path.exists():
            path.unlink()
    with closing(sqlite3.connect(db)) as connection:
        connection.execute(f'PRAGMA application_id={APPLICATION_ID}')
        connection.execute('PRAGMA user_version=1')
        connection.execute(LEGACY_TABLE)
        connection.execute('INSERT INTO broker_snapshot (id, schema_version, data) VALUES (1, 1, ?)',
                           (bytes(data),))
        connection.commit()


def legacy_blob(db):
    """The container1 payload, or None once the database has been migrated."""
    with closing(sqlite3.connect(db)) as connection:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='broker_snapshot'").fetchone()
        if table is None:
            return None
        row = connection.execute('SELECT data FROM broker_snapshot WHERE id=1').fetchone()
    return None if row is None else bytes(row[0])
