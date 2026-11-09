#!/usr/bin/env python3
"""Real bbolt Controller layout, failure, offline import and recovery gates.

LYRNORMQ_BIN=/new/lyrnormq LYRNORMQ_LEGACY_BIN=/old/phase7j/lyrnormq \
  python3 tests/cluster_bbolt_blackbox.py -v -f
LYRNORMQ_CONTROL_EXPORTER optionally overrides scripts/export_control_sqlite.py.
Only private fixture state is damaged. SIGKILL is not physical power-loss proof.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest import mock

from cluster_blackbox import wait_until
from cluster_memory_blackbox import MemoryFixture


def fingerprint(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        while block := source.read(64 * 1024):
            digest.update(block)
    return path.stat().st_size, digest.hexdigest()


class BboltAcceptance(MemoryFixture, unittest.TestCase):
    def test_fresh_controller_has_bbolt_wal_and_no_sqlite(self):
        self.assert_memory_status()
        for node in self.fleet.controllers:
            directory = Path(node.config['data_dir'])
            self.assertTrue((directory / 'controller.bbolt').is_file())
            self.assertTrue(any((directory / 'raft-wal').rglob('*.wal')))
            self.assertEqual(list(directory.glob('*.sqlite*')), [], directory)
        self.data_files()

    def damaged_checkpoint(self, mutate):
        target = self.persistent('bbolt-checkpoint-loss-device', 1, expiry=600)
        self.subscribe(target, 'bbolt/checkpoint-loss', 2)
        for node in self.fleet.controllers:
            node.stop(crash=True)
        node = self.fleet.controllers[0]
        checkpoint = Path(node.config['data_dir']) / 'controller.bbolt'
        self.assertTrue(checkpoint.is_file())
        mutate(checkpoint)
        node.spawn()
        try:
            code = node.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.fail('controller accepted missing or invalid business checkpoint')
        self.assertNotEqual(code, 0, node.logs())
        self.assertTrue(node.logs().strip(), 'checkpoint failure must report an error')
        self.assertFalse((checkpoint.parent / 'metadata.sqlite').exists(),
                         'recovery must not create SQLite fallback state')

    def test_missing_checkpoint_with_wal_refuses_startup(self):
        self.damaged_checkpoint(lambda path: path.unlink())

    def test_missing_checkpoint_with_stale_export_cannot_reimport_old_state(self):
        def remove_with_stale_exchange(path):
            directory = path.parent
            self.assertTrue((directory / 'controller.bbolt.required').is_file())
            node = self.fleet.controllers[0]
            stale = dict(format=1, identity=[node.config['cluster_id'], node.config['node_id']],
                         state=None, snapshot=None, snapshot_record_version=None)
            (directory / 'controller-checkpoint.export.json').write_text(json.dumps(stale))
            path.unlink()
        self.damaged_checkpoint(remove_with_stale_exchange)

    def test_corrupt_existing_checkpoint_refuses_startup(self):
        def corrupt(path):
            size = path.stat().st_size
            self.assertGreater(size, 0)
            with path.open('r+b') as target:
                remaining = size
                while remaining:
                    block = min(remaining, 64 * 1024)
                    target.write(b'\0' * block)
                    remaining -= block
                target.flush()
                os.fsync(target.fileno())
        self.damaged_checkpoint(corrupt)

    def test_empty_existing_checkpoint_refuses_reinitialization(self):
        def truncate(path):
            with path.open('r+b') as target:
                target.truncate(0)
                target.flush()
                os.fsync(target.fileno())
        self.damaged_checkpoint(truncate)


class OfflineMigration(MemoryFixture, unittest.TestCase):
    def setUp(self):
        self.assertIn('LYRNORMQ_LEGACY_BIN', os.environ,
                      'offline upgrade gate requires the actual Phase7j SQLite+WAL binary')
        legacy = Path(os.environ['LYRNORMQ_LEGACY_BIN'])
        self.assertTrue(legacy.is_file(), legacy)
        with mock.patch('cluster_blackbox.BINARY', str(legacy)):
            super().setUp()

    def test_phase7j_export_import_and_second_crash_restart_preserve_sessions(self):
        target = self.persistent('bbolt-migration-device', 1, expiry=600)
        self.subscribe(target, 'bbolt/migration', 2)
        clients = [target] + [self.persistent(f'bbolt-migration-extra-{slot}', 1, expiry=600)
                              for slot in range(1, 5)]
        baseline = self.fleet.status()['last_applied']
        for index in range(1050):
            self.subscribe(clients[index % len(clients)], f'bbolt/migration-extra/{index}', 1)
        wait_until(lambda: (self.fleet.status() or {}).get('last_applied', -1) >= baseline + 1000, 20)
        for node in self.fleet.controllers:
            node.stop(crash=True)
        for client in clients:
            client.close()
        exporter = Path(os.environ.get('LYRNORMQ_CONTROL_EXPORTER',
                        str(Path(__file__).resolve().parents[1] / 'scripts/export_control_sqlite.py')))
        self.assertTrue(exporter.is_file(), exporter)
        backups = []
        for node in self.fleet.controllers:
            directory = Path(node.config['data_dir'])
            self.assertTrue((directory / 'metadata.sqlite').is_file())
            self.assertTrue(any((directory / 'raft-wal').rglob('*.wal')))
            self.assertFalse((directory / 'controller.bbolt').exists())
            before = {path.name: fingerprint(path) for path in directory.glob('metadata.sqlite*')}
            completed = subprocess.run([sys.executable, str(exporter), '--data-dir', str(directory)],
                                       capture_output=True, text=True, timeout=30)
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            exchange = directory / 'controller-checkpoint.export.json'
            self.assertTrue(exchange.is_file())
            exported = json.loads(exchange.read_text())
            self.assertIsNotNone(exported.get('snapshot'), 'legacy fixture must cross a real persisted snapshot')
            self.assertGreaterEqual(exported['snapshot']['meta']['last_log_id']['index'], baseline + 900)
            self.assertEqual(before, {path.name: fingerprint(path)
                                     for path in directory.glob('metadata.sqlite*')},
                             'offline export must preserve the SQLite source')
            backups.append(before)
        for cycle in range(2):
            for node in self.fleet.controllers:
                node.start()
            wait_until(lambda: (self.fleet.status() or {}).get('last_applied', -1) >= baseline + 1000, 25)
            self.healthy(0)
            self.healthy(1)
            restored = self.persistent('bbolt-migration-device', 1, present=True, expiry=600, retry=True)
            # Real MQTT 3.1.1 and MQTT5 publishers target the imported MQTT5 session.
            for version in (4, 5):
                source = self.client(f'bbolt-migration-source-{cycle}-{version}', 0,
                                     version=version, retry=True)
                self.version = version
                payload = f'migrated-{cycle}-{version}'.encode()
                self.publish(source, 'bbolt/migration', payload, 2)
                self.version = 5
                self.complete_message(restored, self.expect_message(restored, 'bbolt/migration', payload, 2))
                source.close()
            for topic in ('bbolt/migration-extra/0', 'bbolt/migration-extra/1045'):
                source = self.client(f'bbolt-migration-route-{cycle}-{topic.rsplit("/", 1)[1]}', 0)
                self.publish(source, topic, b'imported-checkpoint-route', 1)
                self.complete_message(restored, self.expect_message(restored, topic, b'imported-checkpoint-route', 1))
                source.close()
            for node, before in zip(self.fleet.controllers, backups):
                directory = Path(node.config['data_dir'])
                self.assertTrue((directory / 'controller.bbolt').is_file())
                self.assertEqual(before, {path.name: fingerprint(path)
                                         for path in directory.glob('metadata.sqlite*')},
                                 'new Controller must not modify inert SQLite backups')
                node.stop(crash=True)
            restored.close()
        self.data_files()


if __name__ == '__main__':
    unittest.main()
