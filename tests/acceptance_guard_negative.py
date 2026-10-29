#!/usr/bin/env python3
"""Reject concrete tampering of real checked-in acceptance metadata.

Each method mutates an isolated source snapshot; the working tree is never edited.
These are metadata-integrity checks, not extra wire/conformance executions.
"""
import csv
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class AcceptanceGuardTampering(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-guard-negative-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        for directory in ('crates', 'bin', 'tests', 'docs', 'fuzz'):
            if (ROOT / directory).exists():
                shutil.copytree(ROOT / directory, self.root / directory,
                    ignore=shutil.ignore_patterns('target', '__pycache__', 'artifacts'))
        self.path = self.root / 'docs/conformance/server-acceptance.json'
        if not self.path.exists():
            self.skipTest('coordinator acceptance metadata not yet integrated')
        self.acceptance = json.loads(self.path.read_text())

    def rejected(self, message):
        self.path.write_text(json.dumps(self.acceptance))
        result = subprocess.run([sys.executable, str(self.root / 'tests/check_conformance_matrix.py'),
                                 '--root', str(self.root)], capture_output=True, text=True, timeout=20)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(message, result.stderr)

    def test_overall_pass_cannot_hide_open_required_criterion(self):
        self.acceptance['overall_status'] = 'passed'
        self.acceptance['criteria'][0]['status'] = 'open'
        self.rejected('overall gate cannot pass with an open criterion')

    def test_passed_criterion_needs_actual_command_evidence(self):
        criterion = self.acceptance['criteria'][0]
        criterion['status'] = 'passed'
        criterion['commands'] = []
        criterion['evidence'] = ['tests/check_conformance_matrix.py:check']
        self.rejected(criterion['id'])

    def test_duplicate_criterion_is_not_an_extra_pass(self):
        self.acceptance['criteria'].append(self.acceptance['criteria'][0].copy())
        self.rejected('AssertionError')

    def test_matrix_verified_u7_cannot_bypass_open_acceptance(self):
        self.acceptance['overall_status'] = 'open'
        self.acceptance['criteria'][0]['status'] = 'open'
        base = self.root / 'docs/conformance'
        for path in (base / 'reviews').glob('*.json'):
            data = json.loads(path.read_text())
            for item in data['clauses']:
                if item['id'] == self.acceptance['gate_id']:
                    item['status'] = 'verified'
                    item['implementation'] = ['tests/check_conformance_matrix.py:check']
                    item['tests'] = ['tests/check_conformance_matrix.py:check']
            path.write_text(json.dumps(data))
        path = base / 'mqtt5-matrix.csv'
        with path.open(newline='') as source:
            reader = csv.DictReader(source)
            columns = reader.fieldnames
            rows = list(reader)
        for row in rows:
            if row['id'] == self.acceptance['gate_id']:
                row.update(status='verified', implementation='tests/check_conformance_matrix.py:check',
                           tests='tests/check_conformance_matrix.py:check', reviewers='tampered')
        with path.open('w', newline='') as target:
            writer = csv.DictWriter(target, fieldnames=columns)
            writer.writeheader();writer.writerows(rows)
        self.rejected('U7 cannot bypass required criteria')


if __name__ == '__main__':
    unittest.main(verbosity=2)
