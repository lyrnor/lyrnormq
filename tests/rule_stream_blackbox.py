#!/usr/bin/env python3
"""Independent rule-result sharing, per-consumer progress and v1 migration acceptance."""
import copy
import hashlib
import json
from pathlib import Path
import threading
import time
import unittest

import outbound_concurrency_blackbox as concurrency
from dashboard_blackbox import DashboardBroker, eventually
from rules_webhook_blackbox import CONFIG
import rule_stream_v1_fixture as v1


def events(record):
    return record['body']['events']


def selected(tracker, rule=None, client=None):
    return [event for record in tracker.started for event in events(record)
            if (rule is None or event['rule_id'] == rule)
            and (client is None or event['client_id'] == client)]


class RuleStreams(unittest.TestCase):
    client = concurrency.OutboundConcurrency.client
    publish = concurrency.OutboundConcurrency.publish
    barrier = concurrency.OutboundConcurrency.barrier
    setUp = concurrency.OutboundConcurrency.setUp
    receiver = concurrency.OutboundConcurrency.receiver
    replace = concurrency.OutboundConcurrency.replace
    status = concurrency.OutboundConcurrency.status
    outbound = concurrency.OutboundConcurrency.outbound

    def config(self, first, second=None, *, batch=1, attempts=2):
        config = concurrency.settings(first.url, concurrency=4, batch_size=batch,
                                      batch_wait_ms=50 if batch > 1 else 0,
                                      max_attempts=attempts, retry_base_ms=10)
        config['rules'][0].update(
            sql='SELECT payload.seq AS seq, payload.temperature AS temperature, payload.a AS marker FROM "concurrency/#"')
        if second:
            target = copy.deepcopy(config['outbounds'][0])
            target.update(id='mirror', name='Mirror')
            target['config']['url'] = second.url
            config['outbounds'].append(target)
            config['rules'][0]['outbound_ids'].append('mirror')
        other = copy.deepcopy(config['rules'][0])
        other.update(id='humidity', name='Humidity', outbound_ids=['send'],
                     sql='SELECT payload.seq AS seq, payload.humidity AS humidity, payload.b AS marker FROM "concurrency/#"')
        config['rules'].append(other)
        return config

    def start(self, config=None, extra=()):
        self.broker = DashboardBroker(self.directory.name, extra=['--rules-config', str(self.rules_path), *extra])
        self.addCleanup(self.broker.stop)
        self.broker.start()
        if config is not None:
            self.replace(config)

    def emit(self, source, values):
        for seq in values:
            self.publish(source, 'concurrency/test', json.dumps(dict(
                seq=seq, temperature=30 + seq, humidity=60 + seq,
                a=f'only-rule-temperature-{seq}', b=f'only-rule-humidity-{seq}',
                unused='raw-input-only-must-not-be-journalled')).encode(), qos=0)
        self.barrier(source)

    def pending(self, refs, results=None):
        eventually(lambda: self.status()['durable_queue']['pending_records'] == refs)
        if results is not None:
            self.assertEqual(self.status()['durable_queue'].get('pending_results'), results)

    def drained(self):
        self.pending(0, 0)
        eventually(lambda: self.status()['queued_events'] == 0 and self.status()['queued_bytes'] == 0
                   and self.status()['inflight_requests'] == 0)

    def queue_root(self):
        return self.rules_path.parent / 'egress'

    def bytes(self):
        return b''.join(path.read_bytes() for path in sorted(self.queue_root().glob('generation-*/segment-*.log')))

    def test_webhook_binding_descriptor_retains_pre_kafka_bytes(self):
        receiver = self.receiver()
        config = self.config(receiver)
        self.start(config)
        bindings = [binding for _, transaction, _ in v1.frames(self.queue_root())
                    for binding in transaction.get('Bind', {}).get('bindings', [])]
        self.assertTrue(bindings, 'configuration must durably bind the outbound')
        descriptor = next(binding['descriptor'] for binding in bindings
                          if binding['outbound_id'] == 'send')
        self.assertEqual(bytes(descriptor), v1.descriptor(config['outbounds'][0]),
                         'durable Webhook bindings must retain historical serialization bytes')

    def test_distinct_rule_results_are_stored_once_despite_fanout(self):
        first = self.receiver(gate=threading.Event())
        second = self.receiver(gate=threading.Event())
        self.start(self.config(first, second))
        self.emit(self.client('shared-client'), [0])
        first.tracker.wait(lambda t: t.active >= 1)
        second.tracker.wait(lambda t: t.active == 1)
        self.pending(3)
        data = self.bytes()
        self.assertEqual(data.count(b'only-rule-temperature-0'), 1, 'one rule body must not be copied per outbound')
        self.assertEqual(data.count(b'only-rule-humidity-0'), 1)
        self.assertNotIn(b'raw-input-only-must-not-be-journalled', data)
        self.pending(3, 2)
        first.gate.set()
        second.gate.set()
        first.tracker.wait(lambda t: len(t.completed) == 2)
        second.tracker.wait(lambda t: len(t.completed) == 1)
        self.drained()
        own = selected(first.tracker)
        self.assertEqual({e['rule_id'] for e in own}, {'hot', 'humidity'})
        self.assertEqual(next(e['data'] for e in own if e['rule_id'] == 'hot'),
                         dict(seq=0, temperature=30, marker='only-rule-temperature-0'))
        self.assertEqual(next(e['data'] for e in own if e['rule_id'] == 'humidity'),
                         dict(seq=0, humidity=60, marker='only-rule-humidity-0'))
        mirrored = selected(second.tracker)[0]
        original = next(e for e in own if e['rule_id'] == 'hot')
        self.assertEqual(mirrored['event_id'], original['event_id'])
        self.assertEqual(mirrored['data'], original['data'])
        self.assertNotEqual(mirrored['delivery_id'], original['delivery_id'])

    def test_same_outbound_rules_and_other_outbound_progress_independently(self):
        gate = threading.Event()
        first = self.receiver(hold=lambda r: gate if events(r)[0]['rule_id'] == 'hot' else None)
        second = self.receiver()
        self.start(self.config(first, second))
        self.emit(self.client('same-client'), [0, 1])
        first.tracker.wait(lambda t: sum(events(r)[0]['rule_id'] == 'humidity' for r in t.completed) == 2)
        second.tracker.wait(lambda t: len(t.completed) == 2)
        self.assertEqual([e['data']['seq'] for e in selected(first.tracker, 'hot')], [0])
        self.assertEqual([e['data']['seq'] for e in selected(first.tracker, 'humidity')], [0, 1])
        self.pending(2, 2)
        gate.set()
        first.tracker.wait(lambda t: len(t.completed) == 4)
        self.drained()
        self.assertEqual([e['data']['seq'] for e in selected(first.tracker, 'hot')], [0, 1])
        self.assertLessEqual(first.tracker.peak, 4)

    def test_retry_blocks_only_rule_destination_client_and_keeps_id(self):
        gate = threading.Event()
        first = self.receiver(
            hold=lambda r: gate if events(r)[0]['rule_id'] == 'hot' and events(r)[0]['data']['seq'] == 0 and r['attempt'] == 2 else None,
            status=lambda r: 503 if events(r)[0]['rule_id'] == 'hot' and events(r)[0]['data']['seq'] == 0 and r['attempt'] == 1 else 204)
        second = self.receiver()
        self.start(self.config(first, second))
        self.emit(self.client('retry-client'), [0])
        first.tracker.wait(lambda t: any(events(r)[0]['rule_id'] == 'hot' and r['attempt'] == 2 for r in t.started))
        self.emit(self.client('retry-client'), [1])
        first.tracker.wait(lambda t: sum(events(r)[0]['rule_id'] == 'humidity' for r in t.completed) == 2)
        second.tracker.wait(lambda t: len(t.completed) == 2)
        own = selected(first.tracker, 'hot')
        self.assertEqual([e['data']['seq'] for e in own], [0, 0])
        self.assertEqual(own[0], own[1])
        gate.set()
        first.tracker.wait(lambda t: len(t.completed) == 5)
        self.drained()
        self.assertEqual([e['data']['seq'] for e in selected(first.tracker, 'hot')], [0, 0, 1])

    def test_terminal_failure_keeps_shared_body_for_remaining_consumer(self):
        first = self.receiver(status=lambda r: 400)
        second = self.receiver(gate=threading.Event())
        config = self.config(first, second)
        config['rules'] = config['rules'][:1]
        self.start(config)
        self.emit(self.client('terminal-ref'), [0])
        first.tracker.wait(lambda t: len(t.completed) == 1)
        second.tracker.wait(lambda t: len(t.started) == 1)
        eventually(lambda: self.outbound()['failed'] == 1)
        self.pending(1, 1)
        self.assertEqual(self.outbound('mirror')['durable_queued'], 1)
        self.broker.stop()
        self.broker.start()
        second.tracker.wait(lambda t: len(t.started) == 2)
        self.assertEqual(len(first.tracker.started), 1)
        second.gate.set()
        second.tracker.wait(lambda t: len(t.completed) == 1)
        self.drained()

    def test_batches_never_mix_rules_and_each_rule_retains_client_fifo(self):
        receiver = self.receiver(gate=threading.Event())
        self.start(self.config(receiver, batch=3))
        self.emit(self.client('batch-client'), range(7))
        receiver.tracker.wait(lambda t: t.active == 2)
        self.assertEqual({events(r)[0]['rule_id'] for r in receiver.tracker.started}, {'hot', 'humidity'})
        self.assertTrue(all(len({e['rule_id'] for e in events(r)}) == 1 for r in receiver.tracker.started))
        receiver.gate.set()
        eventually(lambda: self.status()['durable_queue']['pending_records'] == 0)
        self.drained()
        for rule in ('hot', 'humidity'):
            self.assertEqual([e['data']['seq'] for e in selected(receiver.tracker, rule)], list(range(7)))
        self.assertTrue(all(len(events(r)) <= 3 and len({e['rule_id'] for e in events(r)}) == 1
                            for r in receiver.tracker.started))

    def test_sigkill_partial_fanout_does_not_replay_completed_destination(self):
        first = self.receiver(gate=threading.Event())
        second = self.receiver()
        config = self.config(first, second)
        config['rules'] = config['rules'][:1]
        self.start(config)
        self.emit(self.client('partial-crash'), [0, 1])
        first.tracker.wait(lambda t: len(t.started) == 1)
        second.tracker.wait(lambda t: len(t.completed) == 2)
        self.pending(2, 2)
        previous = copy.deepcopy(events(first.tracker.started[0])[0])
        self.broker.stop(crash=True)
        first.tracker.wait(lambda t: t.active == 0)
        self.broker.start()
        first.tracker.wait(lambda t: len(t.started) == 2)
        self.assertEqual(events(first.tracker.started[1])[0], previous)
        self.assertEqual(len(second.tracker.started), 2)
        first.gate.set()
        first.tracker.wait(lambda t: len(t.completed) == 2)
        self.drained()
        self.assertEqual([e['data']['seq'] for e in selected(first.tracker)], [0, 0, 1])

    def test_rule_delete_and_edit_preserve_accepted_result_and_original_recipients(self):
        first = self.receiver(gate=threading.Event())
        second = self.receiver()
        config = self.config(first)
        config['rules'] = config['rules'][:1]
        self.start(config)
        self.emit(self.client('old-rule'), [0])
        first.tracker.wait(lambda t: len(t.started) == 1)
        self.pending(1, 1)
        later = copy.deepcopy(config['outbounds'][0])
        later.update(id='later', name='Later')
        later['config']['url'] = second.url
        config['outbounds'].append(later)
        config['rules'] = []
        previous = copy.deepcopy(events(first.tracker.started[0])[0])
        self.replace(config)
        first.tracker.wait(lambda t: len(t.started) == 2)
        self.assertEqual(events(first.tracker.started[1])[0], previous)
        self.pending(1, 1)
        first.gate.set()
        first.tracker.wait(lambda t: len(t.completed) == 1)
        self.drained()
        self.assertEqual(events(first.tracker.completed[0])[0]['data']['temperature'], 30)
        self.assertEqual(second.tracker.started, [])
        self.emit(self.client('after-delete'), [1])
        self.assertEqual(second.tracker.started, [])

    def test_fanout_capacity_rejection_is_atomic_and_runtime_can_continue(self):
        first = self.receiver()
        second = self.receiver()
        config = self.config(first, second)
        config['rules'] = config['rules'][:1]
        self.start(config, extra=['--egress-queue-max-records', '1'])
        self.emit(self.client('atomic-fanout'), [0])
        eventually(lambda: self.outbound()['dropped'] >= 1 and self.outbound('mirror')['dropped'] >= 1)
        self.pending(0, 0)
        self.assertTrue(self.status()['running'])
        self.assertEqual(first.tracker.started, [])
        self.assertEqual(second.tracker.started, [])
        self.assertNotIn(b'only-rule-temperature-0', self.bytes())
        config['rules'][0]['outbound_ids'] = ['send']
        self.replace(config)
        self.emit(self.client('atomic-fanout'), [1])
        first.tracker.wait(lambda t: len(t.completed) == 1)
        self.drained()
        self.assertEqual(events(first.tracker.completed[0])[0]['data']['seq'], 1)
        self.assertEqual(second.tracker.started, [])

    def fixture_start(self, config, entries):
        self.broker = DashboardBroker(self.directory.name, extra=['--rules-config', str(self.rules_path)])
        self.addCleanup(self.broker.stop)
        root = v1.install(self.rules_path, config, entries)
        self.broker.start()
        return root

    def test_v1_duplicate_bodies_merge_and_unknown_attempt_ids_survive(self):
        first = self.receiver(gate=threading.Event())
        second = self.receiver(gate=threading.Event())
        config = self.config(first, second)
        config['rules'] = config['rules'][:1]
        originals = [v1.wire(outbound=o) for o in ('send', 'mirror')]
        root = self.fixture_start(config, [dict(wire=e, inflight=True) for e in originals])
        first.tracker.wait(lambda t: len(t.started) == 1)
        second.tracker.wait(lambda t: len(t.started) == 1)
        self.assertEqual(events(first.tracker.started[0])[0], originals[0])
        self.assertEqual(events(second.tracker.started[0])[0], originals[1])
        self.pending(2, 1)
        self.assertEqual(self.bytes().count(b'legacy-shared-marker'), 1)
        self.assertEqual({version for version, _, _ in v1.frames(root)}, {2})
        self.broker.stop(crash=True)
        first.tracker.wait(lambda t: t.active == 0)
        second.tracker.wait(lambda t: t.active == 0)
        first.gate.set()
        second.gate.set()
        self.broker.start()
        first.tracker.wait(lambda t: len(t.completed) == 1)
        second.tracker.wait(lambda t: len(t.completed) == 1)
        self.drained()
        self.assertEqual(events(first.tracker.completed[0])[0], originals[0])
        self.assertEqual(events(second.tracker.completed[0])[0], originals[1])

    def test_v1_partial_completion_retry_deadline_and_budget_preserved(self):
        first = self.receiver(status=lambda _r: 503)
        second = self.receiver()
        config = self.config(first, second)
        config['rules'] = config['rules'][:1]
        deadline = int(time.time() * 1000) + 2500
        entries = [dict(wire=v1.wire('send'), complete=True), dict(wire=v1.wire('mirror')),
                   dict(wire=v1.wire('send', seq=1, event_id='legacy-event-1'), failed_once=True, deadline=deadline)]
        self.fixture_start(config, entries)
        second.tracker.wait(lambda t: len(t.completed) == 1)
        self.assertEqual(first.tracker.started, [])
        self.pending(1, 1)
        first.tracker.wait(lambda t: len(t.completed) == 1, timeout=5)
        self.drained()
        self.assertEqual([e['data']['seq'] for e in selected(first.tracker)], [1])
        self.assertEqual([e['data']['seq'] for e in selected(second.tracker)], [0])
        self.assertEqual(events(first.tracker.started[0])[0]['delivery_id'], 'legacy-event-1:hot:send')
        eventually(lambda: self.outbound()['failed'] == 1)
        self.assertEqual(self.outbound()['retried'], 1)

    def malformed_fixture(self, entries):
        receiver = self.receiver()
        config = self.config(receiver)
        config['rules'] = config['rules'][:1]
        self.broker = DashboardBroker(self.directory.name, extra=['--rules-config', str(self.rules_path)])
        self.addCleanup(self.broker.stop)
        root = v1.install(self.rules_path, config, entries)
        before = {p: hashlib.sha256(p.read_bytes()).digest() for p in root.rglob('*') if p.is_file()}
        with self.assertRaises(AssertionError):
            self.broker.start()
        for path, digest in before.items():
            self.assertEqual(hashlib.sha256(path.read_bytes()).digest(), digest, path.name)
        self.assertEqual(receiver.tracker.started, [])

    def test_v1_non_rule_payload_fails_closed_without_erasing_old_files(self):
        self.malformed_fixture([dict(wire=b'opaque-not-a-rule-envelope')])

    def test_v1_conflicting_result_bodies_fail_closed(self):
        first = v1.wire()
        second = v1.wire(event_id='legacy-event-0', marker='conflicting-result')
        # Same rule/event/client but independent destination needed for a conflict.
        receiver = self.receiver()
        config = self.config(receiver)
        other = copy.deepcopy(config['outbounds'][0])
        other.update(id='mirror', name='Other')
        config['outbounds'].append(other)
        config['rules'] = config['rules'][:1]
        second['delivery_id'] = 'legacy-event-0:hot:mirror'
        self.broker = DashboardBroker(self.directory.name, extra=['--rules-config', str(self.rules_path)])
        self.addCleanup(self.broker.stop)
        root = v1.install(self.rules_path, config, [dict(wire=first), dict(wire=second)])
        before = {p: p.read_bytes() for p in root.rglob('*') if p.is_file()}
        with self.assertRaises(AssertionError):
            self.broker.start()
        self.assertTrue(all(path.read_bytes() == data for path, data in before.items()))
        self.assertEqual(receiver.tracker.started, [])

    def test_v1_client_metadata_mismatch_fails_closed(self):
        self.malformed_fixture([dict(wire=v1.wire(), meta_client='different-client')])


if __name__ == '__main__':
    unittest.main()
