#!/usr/bin/env python3
"""Real Controller WAL gates; data replicas remain alive and RAM-only.

RMQTT_BIN=/absolute/new/rmqtt python3 tests/cluster_raft_wal_blackbox.py -v
Controller checkpoints use bbolt; legacy exchange migration is tested separately.
Only private temporary fixture directories are corrupted/removed by these tests.
These SIGKILL/reopen checks do not simulate physical power failure or failed fsync.
"""
import os
from pathlib import Path
import shutil
import subprocess
import unittest

import cluster_control_optimization_blackbox as control
from cluster_blackbox import wait_until


class RaftWalAcceptance(control.ControlTail):
    # Inherits the committed-tail/expired-authority/QoS2 recovery gate verbatim.
    def wal_files(self, node):
        directory = Path(node.config['data_dir']) / 'raft-wal'
        self.assertTrue(directory.is_dir(), f'independent Raft WAL missing: {directory}')
        files = sorted(path for path in directory.rglob('*.wal') if path.is_file())
        self.assertTrue(files, f'no raft-log chunks in {directory}')
        self.assertTrue(any(path.stat().st_size > 0 for path in files), files)
        return files

    def test_normal_metadata_updates_use_independent_wal_without_sqlite(self):
        self.assert_memory_status()
        before = [self.checkpoint(node) for node in self.fleet.controllers]
        subscriber = self.persistent('wal-storage-target', 1, expiry=600)
        source = self.persistent('wal-storage-source', 0, expiry=600)
        for index in range(12):
            topic = f'wal/storage/{index}'
            self.subscribe(subscriber, topic, 2)
            self.publish(source, topic, b'wal-backed-control', 2)
            self.complete_message(subscriber, self.expect_message(subscriber, topic, b'wal-backed-control', 2))
        for node, previous in zip(self.fleet.controllers, before):
            self.wal_files(node)
            self.assertEqual(previous, self.checkpoint(node),
                             'normal Raft applies must not rewrite the business checkpoint')
        self.data_files()

    def rejected_restart(self, node):
        node.spawn()
        try:
            code = node.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.fail('controller accepted damaged/missing durable WAL')
        self.assertNotEqual(code, 0, node.logs())
        self.assertTrue(node.logs().strip(), 'failed startup should report its error')

    def damage_wal(self, mutate):
        # Stop every controller to prevent valid peer traffic from obscuring a
        # local fail-closed startup assertion. This is temporary fixture state.
        for node in self.fleet.controllers:
            node.stop(crash=True)
        node = self.fleet.controllers[0]
        chunks = self.wal_files(node)
        # Use a substantial chunk, not a just-created empty rotation successor.
        path = max(chunks, key=lambda item: item.stat().st_size)
        self.assertGreater(path.stat().st_size, 128)
        mutate(path)
        self.rejected_restart(node)

    def test_corrupt_nonempty_wal_refuses_startup(self):
        def corrupt(path):
            with path.open('r+b') as file:
                position = path.stat().st_size // 2
                file.seek(position)
                original = file.read(1)
                file.seek(position)
                file.write(bytes([original[0] ^ 0xff]))
                file.flush()
                os.fsync(file.fileno())
        self.damage_wal(corrupt)

    def test_incomplete_acknowledged_wal_record_refuses_startup(self):
        def tear(path):
            with path.open('r+b') as file:
                file.truncate(path.stat().st_size - 1)
                file.flush()
                os.fsync(file.fileno())
        self.damage_wal(tear)

    def test_missing_wal_never_recreates_durable_authority(self):
        for node in self.fleet.controllers:
            node.stop(crash=True)
        node = self.fleet.controllers[0]
        self.wal_files(node)
        shutil.rmtree(Path(node.config['data_dir']) / 'raft-wal')
        self.rejected_restart(node)

    def test_periodic_snapshot_and_compacted_tail_recover_sessions(self):
        target = self.persistent('wal-snapshot-target', 1, expiry=600)
        self.subscribe(target, 'wal/snapshot', 2)
        targets = [target] + [self.persistent(f'wal-snapshot-extra-device-{slot}', 1, expiry=600)
                              for slot in range(1, 10)]
        baseline = self.fleet.status()['last_applied']
        before = [self.checkpoint(node) for node in self.fleet.controllers]
        # Actual separate SUBSCRIBE operations cross the production snapshot
        # threshold and retained-log window; do not force internal test hooks.
        for index in range(2100):
            self.subscribe(targets[index % len(targets)], f'wal/snapshot-extra/{index}', 1)
        def checkpoint_advanced():
            status = self.fleet.status()
            if not status or status['last_applied'] < baseline + 2000:
                return False
            for node, previous in zip(self.fleet.controllers, before):
                if self.checkpoint(node) == previous:
                    return False
            return True
        wait_until(checkpoint_advanced, 20)
        for node in self.fleet.controllers:
            node.stop(crash=True)
        for subscriber in targets:
            subscriber.close()
        for node in self.fleet.controllers:
            node.start()
        self.healthy(0)
        self.healthy(1)
        restored = self.persistent('wal-snapshot-target', 1, present=True, expiry=600, retry=True)
        source = self.client('wal-snapshot-source', 0, retry=True)
        for topic in ('wal/snapshot', 'wal/snapshot-extra/0', 'wal/snapshot-extra/2090'):
            self.publish(source, topic, b'after-snapshot-crash', 1)
            self.complete_message(restored, self.expect_message(restored, topic, b'after-snapshot-crash', 1))
        last = self.persistent('wal-snapshot-extra-device-9', 1, present=True, expiry=600, retry=True)
        self.publish(source, 'wal/snapshot-extra/2099', b'last-snapshot-route', 1)
        self.complete_message(last, self.expect_message(last, 'wal/snapshot-extra/2099', b'last-snapshot-route', 1))
        self.data_files()


if __name__ == '__main__':
    unittest.main()
