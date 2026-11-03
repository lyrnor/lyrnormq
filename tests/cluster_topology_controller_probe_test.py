#!/usr/bin/env python3
"""Independent controller probe tests: exact censoring and real SQLite lag."""
import importlib.util
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest

SPEC = importlib.util.spec_from_file_location('controller_probe', Path(__file__).resolve().parents[1] / 'scripts/cluster_benchmark_controller_probe.py')
PROBE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROBE)


class ControllerProbeCases(unittest.TestCase):
    def test_exact_observation_delay_censoring_and_follower_ahead(self):
        def row(node, value, end):
            return dict(controller=node, applied_index=value, metadata_revision=value*2,
                        read_start_ns=end-2, read_end_ns=end)
        samples = [row(1, 1, 10), row(2, 1, 20), row(3, 1, 30),
                   row(2, 2, 40), row(1, 2, 50), row(3, 2, 90), row(1, 3, 100)]
        result = PROBE.summarize(samples, 1)['applied_index']
        self.assertEqual(result['leader_advances'], 2)
        first, second = result['followers']
        self.assertEqual(first['matched_advances'], 1)
        self.assertEqual(first['censored_advances'], 1)
        self.assertEqual(first['matches'][0]['observed_delay_ns'], 0)
        self.assertTrue(first['matches'][0]['not_before_leader_bound'])
        self.assertEqual(second['matches'][0]['observed_delay_ns'], 40)
        self.assertEqual(second['observed_delay']['p99_ns'], 40)
        self.assertIsNone(second['matches'][1]['observed_delay_ns'])

    def test_sampling_readonly_sqlite_observes_synthetic_follower_lag(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = [Path(directory) / str(i) for i in range(3)]
            def put(path, value):
                with closing(sqlite3.connect(path / 'metadata.sqlite')) as db, db:
                    db.execute('CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,data BLOB NOT NULL)')
                    db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)', ('state', json.dumps(dict(last_applied=dict(index=value), metadata=dict(revision=value)))))
            for path in paths:
                path.mkdir()
                put(path, 0)
            def update():
                time.sleep(.08)
                for value in range(1, 4):
                    put(paths[0], value)
                    time.sleep(.025)
                    put(paths[1], value)
                    time.sleep(.025)
                    put(paths[2], value)
                    time.sleep(.025)
            thread = threading.Thread(target=update)
            thread.start()
            result = PROBE.sample(paths, 1, .5, 5)
            thread.join()
            self.assertTrue(result['success'], result)
            for follower in result['analysis']['applied_index']['followers']:
                self.assertEqual(follower['matched_advances'], 3)
                self.assertEqual(follower['censored_advances'], 0)
                self.assertGreater(follower['observed_delay']['max_ns'], 0)
            for path in paths:
                with closing(sqlite3.connect(path / 'metadata.sqlite')) as db, db:
                    self.assertEqual(db.execute('SELECT count(*) FROM meta').fetchone()[0], 1)

    def test_read_errors_and_missing_leader_not_success(self):
        with tempfile.TemporaryDirectory() as directory:
            result = PROBE.sample([Path(directory)/str(i) for i in range(3)], 1, .05, 10)
            self.assertFalse(result['success'])
            self.assertGreater(result['read_errors'], 0)
            self.assertFalse(result['leader_observation_available'])
            self.assertEqual(result['analysis']['applied_index']['followers'][0]['observed_delay']['count'], 0)

    def test_finite_budget_and_leader_validation(self):
        for leader, duration, interval in [(0, 20, 10), (1, 61, 10), (1, 60, 1), (1, 20, 0)]:
            with self.subTest(leader=leader, duration=duration, interval=interval):
                with self.assertRaises(ValueError):
                    PROBE.validate(['/tmp/a', '/tmp/b', '/tmp/c'], leader, duration, interval)


if __name__ == '__main__':
    unittest.main()
