#!/usr/bin/env python3
"""Deterministic readback verifier regressions; never starts a server."""
import copy
import unittest
from kafka_adapter_benchmark import verify_records


def rows():
    result = []
    for sequence in range(3):
        for client in range(2):
            body = dict(client=client, sequence=sequence, padding='')
            import json
            body['padding'] = 'x' * (128 - len(json.dumps(body, separators=(',', ':'))))
            identity = f'run-{client}-{sequence}'
            result.append(dict(value=body, key=f'bench-client-{client:04}', partition=client,
                offset=sequence, timestamp_ms=123456789,
                headers={'lyrnormq-event-id':identity,'lyrnormq-delivery-id':identity,
                    'lyrnormq-rule-id':'benchmark-rule','lyrnormq-client-id':f'bench-client-{client:04}',
                    'lyrnormq-topic':'bench/source'}))
    return result


class ReadbackTests(unittest.TestCase):
    def verify(self, values):
        return verify_records(values, 'run', 2, 3, 128, 2)

    def test_exact_rows_pass(self):
        self.assertEqual(self.verify(rows())['unique_delivery_ids'], 6)

    def test_warmup_is_verified_separately_including_uneven_client_counts(self):
        warm = rows()[:5]
        for row in warm:
            for key in ('lyrnormq-event-id', 'lyrnormq-delivery-id'):
                row['headers'][key] = 'warmup-' + row['headers'][key]
        pairs={(i % 2, i // 2) for i in range(5)}
        self.assertEqual(verify_records(warm, 'warmup-run', 2, 3, 128, 2,
                         expected_pairs=pairs)['count'],5)
        with self.assertRaises(AssertionError):
            verify_records(warm[:-1], 'warmup-run', 2, 3, 128, 2, expected_pairs=pairs)
        with self.assertRaises(AssertionError):
            self.verify(warm + rows()[:1])
        self.assertEqual(verify_records([], 'warmup-run', 2, 1, 128, 2,
                         expected_pairs=set())['count'],0)

    def test_missing_duplicate_reordered_or_tampered_rows_fail(self):
        original = rows()
        bad = [original[:-1], original + [original[0]]]
        duplicate = copy.deepcopy(original)
        duplicate[-1] = duplicate[1]
        bad.append(duplicate)
        reordered = copy.deepcopy(original)
        reordered[0], reordered[2] = reordered[2], reordered[0]
        bad.append(reordered)
        for field, value in [('timestamp_ms', 1), ('key', 'bad'), ('partition', 33)]:
            changed = copy.deepcopy(original)
            changed[0][field] = value
            bad.append(changed)
        changed = copy.deepcopy(original)
        changed[0]['value']['padding'] += 'x'
        bad.append(changed)
        changed = copy.deepcopy(original)
        changed[0]['headers']['lyrnormq-event-id'] = 'bad'
        bad.append(changed)
        for values in bad:
            with self.subTest(values=values):
                with self.assertRaises(AssertionError):
                    self.verify(values)


if __name__ == '__main__':
    unittest.main()
