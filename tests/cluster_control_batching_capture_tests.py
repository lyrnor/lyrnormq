#!/usr/bin/env python3
"""Pure bounded-log evidence tests; starts no Broker or load generator."""
import io
from pathlib import Path
import tempfile
import unittest

from cluster_control_batching_diagnostic import capture_log, LOG_HALF, LOG_LIMIT
from cluster_blackbox import CLUSTER_TOKEN, TOKEN


class BoundedDiagnosticLogs(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-control-log-test-')
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'node.log'

    def test_complete_small_log_preserves_first_cause_and_redacts_tokens(self):
        source = b'FIRST-CAUSE\n' + CLUSTER_TOKEN.encode() + b'\n' + TOKEN.encode() + b'\nlast\n'
        report = capture_log(io.BytesIO(source), self.path)
        saved = self.path.read_bytes()
        self.assertEqual(saved, b'FIRST-CAUSE\n[redacted]\n[redacted]\nlast\n')
        self.assertFalse(report['truncated'])
        self.assertEqual(report['source_bytes'], len(source))

    def test_exact_limit_is_complete(self):
        source = b'a' * LOG_LIMIT
        report = capture_log(io.BytesIO(source), self.path)
        self.assertFalse(report['truncated'])
        self.assertEqual(self.path.read_bytes(), source)

    def test_oversize_keeps_head_tail_and_explicit_gap(self):
        head = b'FIRST-CAUSE\n' + CLUSTER_TOKEN.encode()
        head += b'a' * (LOG_HALF - len(head))
        tail = TOKEN.encode() + b'\nFINAL-CAUSE\n'
        tail += b'z' * (LOG_HALF - len(tail))
        source = head + b'OMITTED-MIDDLE' + tail
        report = capture_log(io.BytesIO(source), self.path)
        saved = self.path.read_bytes()
        self.assertTrue(report['truncated'])
        self.assertTrue(saved.startswith(b'FIRST-CAUSE\n[redacted]'))
        self.assertIn(b'LOG TRUNCATED:', saved)
        self.assertIn(b'omitted 14 bytes;', saved)
        self.assertIn(b'[redacted]\nFINAL-CAUSE\n', saved)
        self.assertNotIn(b'OMITTED-MIDDLE', saved)
        self.assertNotIn(CLUSTER_TOKEN.encode(), saved)
        self.assertNotIn(TOKEN.encode(), saved)
        self.assertEqual(report['source_bytes'], len(source))
        self.assertLess(len(saved), LOG_LIMIT + 256)

    def test_not_started_node_has_empty_log(self):
        report = capture_log(None, self.path)
        self.assertEqual(self.path.read_bytes(), b'')
        self.assertFalse(report['truncated'])
        self.assertEqual(report['source_bytes'], 0)


if __name__ == '__main__':
    unittest.main()
