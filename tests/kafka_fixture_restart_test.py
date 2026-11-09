#!/usr/bin/env python3
"""Real Kafka fixture resource regression: internal offsets plus topic recovery."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from kafka_fixture import KafkaFixture

HELPER = os.environ.get('KAFKA_CLIENT_BIN', str(Path(__file__).resolve().parents[1] / 'target/release/lyrnormq-kafka-test-client'))


class FixtureRestart(unittest.TestCase):
    def test_eight_topics_and_internal_offsets_survive_restart(self):
        fixture = KafkaFixture()
        self.addCleanup(fixture.close)
        fixture.start()
        with tempfile.TemporaryDirectory(prefix='lyrnormq-kafka-resource-regression-') as directory:
            config = Path(directory) / 'reader.json'
            config.write_text(json.dumps(fixture.client_config('plaintext')))
            config.chmod(0o600)
            for index in range(8):
                result = subprocess.run([HELPER, 'create', str(config), f'fixture-restart-{index}', '3'],
                    capture_output=True, text=True, timeout=20)
                self.assertEqual(result.returncode, 0, result.stderr[-1000:])
            # FindCoordinator auto-creates __consumer_offsets even for a group
            # with no members. Its compacted logs activate the cleaner on restart.
            fixture.cli('kafka-consumer-groups.sh', '--bootstrap-server', fixture.bootstrap,
                        '--group', 'fixture-empty-group', '--describe')
            before = fixture.cli('kafka-topics.sh', '--bootstrap-server', fixture.bootstrap, '--list').splitlines()
            self.assertIn('__consumer_offsets', before)
            self.assertTrue(all(f'fixture-restart-{index}' in before for index in range(8)))
            fixture.stop()
            fixture.start()
            after = fixture.cli('kafka-topics.sh', '--bootstrap-server', fixture.bootstrap, '--list').splitlines()
            self.assertTrue(set(before) <= set(after))
            self.assertNotIn('OutOfMemoryError', fixture.log.read_text())
            self.assertIsNone(fixture.process.poll())


if __name__ == '__main__':
    unittest.main()
