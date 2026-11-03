#!/usr/bin/env python3
"""Small real-process gate for committed control-tail recovery; data stays live RAM."""
import sqlite3
from pathlib import Path
import time
import unittest

from cluster_blackbox import wait_until
from cluster_memory_blackbox import MemoryFixture


class ControlTail(MemoryFixture, unittest.TestCase):
    version = 5

    def checkpoint(self, controller):
        database = Path(controller.config['data_dir']) / 'metadata.sqlite'
        with sqlite3.connect(f'file:{database}?mode=ro', uri=True, timeout=2) as db:
            row = db.execute("SELECT data FROM meta WHERE key='state'").fetchone()
            return row[0] if row else None

    def test_no_per_apply_state_blob_then_committed_tail_survives_controller_crash(self):
        self.assert_memory_status()
        before = [self.checkpoint(node) for node in self.fleet.controllers]
        target = self.persistent('control-tail-target', 1, expiry=600)
        self.subscribe(target, 'control/tail', 2)
        source = self.persistent('control-tail-source', 0, expiry=600)
        self.publish(source, 'control/tail', b'before-control-crash', 2)
        message = self.expect_message(target, 'control/tail', b'before-control-crash', 2)
        self.complete_message(target, message)
        after = [self.checkpoint(node) for node in self.fleet.controllers]
        self.assertEqual(before, after, 'normal committed applies must not rewrite full state blob')
        baseline = wait_until(lambda: self.fleet.status())['last_applied']
        for node in self.fleet.controllers:
            node.stop(crash=True)
        time.sleep(4)  # Prove expired authority is not bypassed by RAM caching.
        self.no_positive_connack(self.fleet.brokers[0])
        source.close()
        target.close()
        for node in self.fleet.controllers:
            node.start()

        def recovered():
            status = self.fleet.status()
            if not status or not status.get('leader'):
                return False
            current = status['last_applied']
            return current >= baseline

        wait_until(recovered, 25)
        self.healthy(0)
        self.healthy(1)
        restored = self.persistent('control-tail-target', 1, present=True, expiry=600, retry=True)
        publisher = self.persistent('control-tail-source', 0, present=True, expiry=600, retry=True)
        self.publish(publisher, 'control/tail', b'after-control-crash', 2)
        message = self.expect_message(restored, 'control/tail', b'after-control-crash', 2)
        self.complete_message(restored, message)
        self.data_files()


if __name__ == '__main__':
    unittest.main()
