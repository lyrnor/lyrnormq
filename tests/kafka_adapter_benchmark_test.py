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
                headers={'rmqtt-event-id':identity,'rmqtt-delivery-id':identity,
                    'rmqtt-rule-id':'benchmark-rule','rmqtt-client-id':f'bench-client-{client:04}',
                    'rmqtt-topic':'bench/source'}))
    return result


class ReadbackTests(unittest.TestCase):
    def verify(self, values):
        return verify_records(values, 'run', 2, 3, 128, 2)

    def test_exact_rows_pass(self):
        self.assertEqual(self.verify(rows())['unique_delivery_ids'], 6)

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
        changed[0]['headers']['rmqtt-event-id'] = 'bad'
        bad.append(changed)
        for values in bad:
            with self.subTest(values=values):
                with self.assertRaises(AssertionError):
                    self.verify(values)


if __name__ == '__main__':
    unittest.main()
