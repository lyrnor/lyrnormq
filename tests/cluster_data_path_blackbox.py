#!/usr/bin/env python3
"""Real RF3/W2 steady-data regression, no client retries or quorum weakening.

LYRNORMQ_BIN=/path/lyrnormq LYRNORMQ_LOADGEN_BIN=/path/lyrnormq-loadgen \
  python3 tests/cluster_data_path_blackbox.py -v
Optional LYRNORMQ_DIAG_QOS=0|1|2 selects the high-load QoS (default0).
An unsuccessful warmup is failure, never zero loss. Evidence is preserved under
LYRNORMQ_DIAG_ARTIFACT_DIR (default an independent temporary artifact directory).
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from cluster_blackbox import CLUSTER_TOKEN, TOKEN
from cluster_durable_blackbox import DurableFleet, DurableNode

LOADGEN = os.environ.get('LYRNORMQ_LOADGEN_BIN', str(
    Path(__file__).resolve().parents[1] / 'target/debug/lyrnormq-loadgen'))


class DataPathNode(DurableNode):
    extra_limits = {'connections': 20000, 'sessions': 20000}


class DataPathFleet(DurableFleet):
    node_class = DataPathNode


class DataPathCases(unittest.TestCase):
    def setUp(self):
        self.environment = mock.patch.dict(os.environ,
                                           TOKIO_WORKER_THREADS='2', RUST_LOG='warn')
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.directory = tempfile.TemporaryDirectory(prefix='lyrnormq-data-path-fleet-')
        self.addCleanup(self.directory.cleanup)
        parent = os.environ.get('LYRNORMQ_DIAG_ARTIFACT_DIR')
        if parent:
            Path(parent).mkdir(parents=True, exist_ok=True)
        self.evidence = Path(tempfile.mkdtemp(prefix='lyrnormq-data-path-evidence-', dir=parent))
        self.fleet = DataPathFleet(self.directory.name)
        self.addCleanup(self.fleet.stop)
        self.addCleanup(self.save_logs)
        self.fleet.start()

    def save_logs(self):
        for node in [*self.fleet.controllers, *self.fleet.brokers]:
            logs = node.logs()
            for secret in (CLUSTER_TOKEN, TOKEN):
                logs = logs.replace(secret, '[redacted]')
            (self.evidence / f'{node.config["role"]}-{node.config["node_id"]}.log').write_text(logs)
        print(f'\nPreserved data-path evidence: {self.evidence}', flush=True)

    def load(self, *, pairs, rate, duration_ms, qos):
        report = self.evidence / 'result.json'
        raw = self.evidence / 'samples.jsonl'
        addresses = ','.join(f'127.0.0.1:{node.mqtt}' for node in self.fleet.brokers)
        command = [LOADGEN, '--addrs', addresses, '--protocol', '5', '--qos', str(qos),
                   '--pairs', str(pairs), '--setup-concurrency', '1',
                   '--payload-bytes', '128', '--rate', str(rate), '--inflight', '1',
                   '--warmup-ms', '3000', '--duration-ms', str(duration_ms),
                   '--drain-timeout-ms', '30000', '--connect-timeout-ms', '60000',
                   '--io-timeout-ms', '30000', '--max-messages', '2000000',
                   '--output', str(report), '--raw', str(raw)]
        result = subprocess.run(command, capture_output=True, text=True, timeout=240,
                                env=dict(os.environ, TOKIO_WORKER_THREADS='8'))
        (self.evidence / 'command.json').write_text(json.dumps(command, indent=2) + '\n')
        (self.evidence / 'loadgen.log').write_text(result.stdout + result.stderr)
        self.assertTrue(report.is_file(), f'loadgen emitted no report: {result.stderr}')
        data = json.loads(report.read_text())
        (self.evidence / 'exit.json').write_text(json.dumps({'exit_code': result.returncode}) + '\n')
        self.assertEqual(result.returncode, 0, data)
        self.assertTrue(data.get('success'), data)
        accounting = data['accounting']
        counters = accounting['counters']
        self.assertEqual(accounting['not_sent'], 0, accounting)
        self.assertEqual(accounting['not_delivered'], 0, accounting)
        self.assertEqual(counters['duplicates'], 0, counters)
        self.assertEqual(counters['invalid_payload'], 0, counters)
        self.assertGreater(counters['attempted'], 0, 'warmup failure cannot pass as no loss')
        self.assertEqual(counters['attempted'], counters['delivered_unique'], counters)
        if qos:
            self.assertEqual(accounting['not_source_confirmed'], 0, accounting)

    def test_small_steady_control(self):
        self.load(pairs=6, rate=30, duration_ms=5000, qos=1)

    def test_sixty_connections_at_aggregate_one_hundred_per_second(self):
        qos = int(os.environ.get('LYRNORMQ_DIAG_QOS', '0'))
        self.assertIn(qos, (0, 1, 2))
        self.load(pairs=30, rate=100, duration_ms=30000, qos=qos)


if __name__ == '__main__':
    unittest.main()
