#!/usr/bin/env python3
"""Offline exporter unit gates with actual SQLite/WAL files; no Broker needed."""
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

SCRIPT = Path(os.environ.get('LYRNORMQ_CONTROL_EXPORTER',
              str(Path(__file__).resolve().parents[1] / 'scripts/export_control_sqlite.py')))
SPEC = importlib.util.spec_from_file_location('control_sqlite_export', SCRIPT)
exporter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(exporter)


class OfflineExport(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='lyrnormq-bbolt-export-unit-')
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        (self.directory / 'controller.lock').touch()
        (self.directory / 'raft-wal').mkdir()
        # Keep the connection alive so the newest committed data really stays
        # in -wal and a real shared-memory file must remain untouched by export.
        self.db = sqlite3.connect(self.directory / 'metadata.sqlite')
        self.addCleanup(self.db.close)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('CREATE TABLE meta(key TEXT PRIMARY KEY,data BLOB NOT NULL)')
        self.put('identity', ['export-fixture', 7])
        self.put('raft_wal_migration', 1)

    def put(self, key, value):
        self.db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',
                        (key, json.dumps(value).encode()))
        self.db.commit()

    def source(self):
        return {path.name: (path.stat().st_size, hashlib.sha256(path.read_bytes()).hexdigest())
                for path in self.directory.glob('metadata.sqlite*')}

    def test_exact_exchange_schema_and_committed_wal_rows_preserve_source_bytes(self):
        state = {'offline_fixture_row': 'committed in SQLite WAL'}
        self.put('state', state)
        self.assertTrue((self.directory / 'metadata.sqlite-wal').is_file())
        self.assertTrue((self.directory / 'metadata.sqlite-shm').is_file())
        before = self.source()
        output, size = exporter.export(self.directory)
        result = json.loads(output.read_bytes())
        self.assertEqual(result, dict(format=1, identity=['export-fixture', 7], state=state,
                                      snapshot=None, snapshot_record_version=None))
        self.assertEqual(size, output.stat().st_size)
        self.assertEqual(before, self.source())
        self.assertEqual(list(self.directory.glob(exporter.NAME + '.tmp-*')), [])

    def test_active_controller_flock_rejects_export_in_another_process(self):
        before = self.source()
        with (self.directory / 'controller.lock').open('r+b') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = subprocess.run([sys.executable, str(SCRIPT), '--data-dir', str(self.directory)],
                                    capture_output=True, text=True, timeout=10)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn('in use', result.stderr)
        self.assertFalse((self.directory / exporter.NAME).exists())
        self.assertEqual(before, self.source())

    def test_missing_or_nonexact_phase7j_marker_is_rejected(self):
        for marker in (None, False, True, 0, 2, '1'):
            with self.subTest(marker=marker):
                if marker is None:
                    self.db.execute("DELETE FROM meta WHERE key='raft_wal_migration'")
                    self.db.commit()
                else:
                    self.put('raft_wal_migration', marker)
                with self.assertRaisesRegex(ValueError, 'Phase7j'):
                    exporter.export(self.directory)
                self.assertFalse((self.directory / exporter.NAME).exists())

    def test_invalid_identity_is_rejected_without_publishing(self):
        for identity in ([], ['cluster', 0], ['cluster', -1], ['cluster', True],
                         ['cluster', '7'], ['cluster', 2**64], ['', 7], ['x' * 129, 7],
                         ['has\0nul', 7], {'cluster': 7}, ['cluster', 7, 8]):
            with self.subTest(identity=identity):
                self.put('identity', identity)
                with self.assertRaisesRegex(ValueError, 'identity'):
                    exporter.export(self.directory)
                self.assertFalse((self.directory / exporter.NAME).exists())

    def test_existing_exchange_is_never_overwritten(self):
        output = self.directory / exporter.NAME
        original = b'existing authoritative offline exchange'
        output.write_bytes(original)
        before = self.source()
        with self.assertRaisesRegex(ValueError, 'already exists'):
            exporter.export(self.directory)
        self.assertEqual(output.read_bytes(), original)
        self.assertEqual(before, self.source())

    def test_bbolt_store_rejects_stale_sqlite_export(self):
        (self.directory / 'controller.bbolt').write_bytes(b'new storage already owns this directory')
        with self.assertRaisesRegex(ValueError, 'bbolt|authoritative'):
            exporter.export(self.directory)
        self.assertFalse((self.directory / exporter.NAME).exists())

    def test_required_marker_without_db_rejects_stale_export(self):
        (self.directory / 'controller.bbolt.required').write_text(
            json.dumps(dict(format=1, identity=['export-fixture', 7])))
        before = self.source()
        with self.assertRaisesRegex(ValueError, 'bbolt|required|authoritative'):
            exporter.export(self.directory)
        self.assertFalse((self.directory / exporter.NAME).exists())
        self.assertEqual(before, self.source())

    def test_missing_independent_wal_requires_phase7j_upgrade(self):
        (self.directory / 'raft-wal').rmdir()
        with self.assertRaisesRegex(ValueError, 'raft-wal|Phase7j'):
            exporter.export(self.directory)
        self.assertFalse((self.directory / exporter.NAME).exists())

    def test_nonfinite_source_json_is_rejected(self):
        self.db.execute('INSERT INTO meta VALUES(?,?)', ('state', b'{"bad":NaN}'))
        self.db.commit()
        with self.assertRaisesRegex(ValueError, 'non-finite'):
            exporter.export(self.directory)
        self.assertFalse((self.directory / exporter.NAME).exists())

    def test_export_size_limits_fail_without_temporary_output_or_source_changes(self):
        self.put('state', {'bounded': 'x' * 256})
        before = self.source()
        with mock.patch.object(exporter, 'MAX_EXPORT', 128):
            with self.assertRaisesRegex(ValueError, 'bounds'):
                exporter.export(self.directory)
        self.assertEqual(before, self.source())
        self.assertEqual(list(self.directory.glob(exporter.NAME + '*')), [])


if __name__ == '__main__':
    unittest.main()
