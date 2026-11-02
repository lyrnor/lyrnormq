#!/usr/bin/env python3
"""Real MQTT -> HTTP client-lane ordering acceptance (stdlib only).

RMQTT_BIN=/absolute/path/rmqtt python3 tests/outbound_client_order_blackbox.py -v
Barriers hold actual requests, not simulated send calls. All resources are owned
by each test. No production throughput/latency claim is inferred from this suite.
"""
import copy
import threading
import unittest

import outbound_concurrency_blackbox as concurrency
from dashboard_blackbox import eventually


def events(record):
    return record['body']['events']


def seqs(tracker):
    return [event['data']['seq'] for record in tracker.started for event in events(record)]


class ClientOrderedOutbounds(unittest.TestCase):
    # Share setup/protocol helpers, without inheriting/discovering the older suite.
    client = concurrency.OutboundConcurrency.client
    publish = concurrency.OutboundConcurrency.publish
    barrier = concurrency.OutboundConcurrency.barrier
    setUp = concurrency.OutboundConcurrency.setUp
    receiver = concurrency.OutboundConcurrency.receiver
    start = concurrency.OutboundConcurrency.start
    replace = concurrency.OutboundConcurrency.replace
    status = concurrency.OutboundConcurrency.status
    outbound = concurrency.OutboundConcurrency.outbound
    emit = concurrency.OutboundConcurrency.emit
    drained = concurrency.OutboundConcurrency.drained

    def empty(self):
        eventually(lambda: self.status()['queued_events'] == 0 and
                   self.status()['queued_bytes'] == 0 and
                   self.status()['inflight_requests'] == 0)

    def test_same_client_fifo_across_topics_while_other_client_overlaps(self):
        gate = threading.Event()
        receiver = self.receiver(hold=lambda record: gate if concurrency.sequence(record) == 0 else None)
        self.start(concurrency.settings(receiver.url, concurrency=8))
        source = self.client('ordered-source')
        self.emit(source, [0], topic='concurrency/first')
        receiver.tracker.wait(lambda t: len(t.started) == 1)
        self.emit(source, [1], topic='concurrency/second')
        self.emit(source, [2], topic='concurrency/third')
        self.emit(self.client('independent-source'), [90])
        receiver.tracker.wait(lambda t: any(concurrency.sequence(r) == 90 for r in t.completed))
        eventually(lambda: self.status()['matched'] == 4)
        self.assertEqual(sorted(seqs(receiver.tracker)), [0, 90],
                         'later messages from held client must not start')
        self.assertEqual(self.outbound()['inflight_requests'], 1)
        gate.set()
        receiver.tracker.wait(lambda t: len(t.completed) == 4)
        self.drained(4)
        ordered = [event['data']['seq'] for record in receiver.tracker.started
                   for event in events(record) if event['client_id'] == 'ordered-source']
        self.assertEqual(ordered, [0, 1, 2])
        self.assertEqual(receiver.tracker.peak, 2)

    def test_reconnected_source_keeps_same_client_lane(self):
        gate = threading.Event()
        receiver = self.receiver(hold=lambda r: gate if concurrency.sequence(r) == 0 else None)
        self.start(concurrency.settings(receiver.url, concurrency=4))
        source = self.client('reconnected-identity')
        self.emit(source, [0])
        receiver.tracker.wait(lambda t: len(t.started) == 1)
        source.close()
        source = self.client('reconnected-identity')
        self.emit(source, [1])
        self.emit(self.client('different-identity'), [90])
        receiver.tracker.wait(lambda t: any(concurrency.sequence(r) == 90 for r in t.completed))
        eventually(lambda: self.status()['matched'] == 3)
        self.assertEqual(sorted(seqs(receiver.tracker)), [0, 90])
        gate.set()
        receiver.tracker.wait(lambda t: len(t.completed) == 3)
        self.drained(3)
        self.assertEqual([e['data']['seq'] for r in receiver.tracker.started for e in events(r)
                          if e['client_id'] == 'reconnected-identity'], [0, 1])

    def test_retry_blocks_only_own_client_and_keeps_delivery_identity(self):
        first_gate, retry_gate = threading.Event(), threading.Event()
        def hold(record):
            if concurrency.sequence(record) == 0:
                return first_gate if record['attempt'] == 1 else retry_gate
            return None
        receiver = self.receiver(hold=hold, status=lambda r: 500 if concurrency.sequence(r) == 0 and r['attempt'] == 1 else 204)
        self.start(concurrency.settings(receiver.url, concurrency=2, max_attempts=2))
        source = self.client('retry-ordered')
        self.emit(source, [0])
        receiver.tracker.wait(lambda t: len(t.started) == 1)
        self.emit(source, [1, 2])
        self.emit(self.client('retry-independent'), [90])
        receiver.tracker.wait(lambda t: any(concurrency.sequence(r) == 90 for r in t.completed))
        self.assertEqual(sorted(seqs(receiver.tracker)), [0, 90])
        first_gate.set()
        receiver.tracker.wait(lambda t: any(concurrency.sequence(r) == 0 and r['attempt'] == 2 for r in t.started))
        self.emit(self.client('retry-independent-second'), [91])
        receiver.tracker.wait(lambda t: any(concurrency.sequence(r) == 91 for r in t.completed))
        self.assertEqual(sorted(seqs(receiver.tracker)), [0, 0, 90, 91])
        retry_gate.set()
        receiver.tracker.wait(lambda t: len(t.completed) == 6)
        self.drained(5)
        own = [event for record in receiver.tracker.started for event in events(record)
               if event['client_id'] == 'retry-ordered']
        self.assertEqual([event['data']['seq'] for event in own], [0, 0, 1, 2])
        self.assertEqual(own[0]['delivery_id'], own[1]['delivery_id'])
        self.assertEqual(own[0]['event_id'], own[1]['event_id'])
        self.assertEqual(self.outbound()['retried'], 1)
        self.assertLessEqual(receiver.tracker.peak, 2)

    def test_retry_backoff_releases_single_outbound_slot_for_other_client(self):
        receiver = self.receiver(status=lambda r: 500 if concurrency.sequence(r) == 0 else 204)
        config = concurrency.settings(receiver.url, concurrency=1, max_attempts=2,
                                      retry_base_ms=10000)
        self.start(config)
        source = self.client('sleeping-retry')
        self.emit(source, [0, 1])
        receiver.tracker.wait(lambda t: len(t.completed) == 1)
        eventually(lambda: self.status()['inflight_requests'] == 0)
        self.emit(self.client('awake-client'), [90])
        receiver.tracker.wait(lambda t: any(concurrency.sequence(r) == 90 for r in t.completed))
        self.assertEqual(seqs(receiver.tracker), [0, 90])
        eventually(lambda: self.outbound()['delivered'] == 1)
        self.assertEqual(self.outbound()['delivered'], 1)
        self.assertEqual(self.outbound()['retried'], 0)
        self.assertEqual(receiver.tracker.peak, 1)
        config['outbounds'][0]['enabled'] = False
        self.replace(config)
        self.empty()
        self.assertEqual(self.status()['durable_queue']['pending_records'], 2)

    def test_terminal_failure_continues_same_client_after_retry_exhaustion(self):
        gate = threading.Event()
        receiver = self.receiver(hold=lambda r: gate if concurrency.sequence(r) == 0 and r['attempt'] == 1 else None,
                                 status=lambda r: 503 if concurrency.sequence(r) == 0 else 204)
        self.start(concurrency.settings(receiver.url, concurrency=4, max_attempts=2))
        self.emit(self.client('terminal'), [0, 1, 2])
        receiver.tracker.wait(lambda t: len(t.started) >= 1)
        eventually(lambda: self.status()['matched'] == 3)
        self.assertEqual(seqs(receiver.tracker), [0])
        gate.set()
        receiver.tracker.wait(lambda t: len(t.completed) == 4)
        self.drained(2)
        self.assertEqual(seqs(receiver.tracker), [0, 0, 1, 2])
        self.assertEqual(self.outbound()['failed'], 1)
        self.assertEqual(self.outbound()['retried'], 1)

    def test_unrecoverable_failure_continues_without_retry(self):
        gate = threading.Event()
        receiver = self.receiver(hold=lambda r: gate if concurrency.sequence(r) == 0 else None,
                                 status=lambda r: 400 if concurrency.sequence(r) == 0 else 204)
        self.start(concurrency.settings(receiver.url, concurrency=4, max_attempts=3))
        self.emit(self.client('unrecoverable'), [0, 1])
        receiver.tracker.wait(lambda t: len(t.started) >= 1)
        eventually(lambda: self.status()['matched'] == 2)
        self.assertEqual(seqs(receiver.tracker), [0])
        gate.set()
        receiver.tracker.wait(lambda t: len(t.completed) == 2)
        self.drained(1)
        self.assertEqual(seqs(receiver.tracker), [0, 1])
        self.assertEqual(self.outbound()['failed'], 1)
        self.assertEqual(self.outbound()['retried'], 0)

    def test_multiple_rules_have_independent_client_lanes(self):
        gate = threading.Event()
        receiver = self.receiver(hold=lambda r: gate if events(r)[0]['data']['seq'] == 0 and events(r)[0]['rule_id'] == 'hot' else None)
        config = concurrency.settings(receiver.url, concurrency=8)
        second = copy.deepcopy(config['rules'][0])
        second.update(id='second', name='Second matching rule')
        config['rules'].append(second)
        self.start(config)
        source = self.client('many-rules')
        self.emit(source, [0, 1])
        receiver.tracker.wait(lambda t: sum(events(r)[0]['rule_id'] == 'second' for r in t.completed) == 2)
        eventually(lambda: self.status()['matched'] == 4)
        own = [e for r in receiver.tracker.started for e in events(r) if e['client_id'] == 'many-rules']
        self.assertEqual([(e['data']['seq'], e['rule_id']) for e in own if e['rule_id'] == 'hot'], [(0, 'hot')])
        self.assertEqual([e['data']['seq'] for e in own if e['rule_id'] == 'second'], [0, 1])
        self.emit(self.client('other-rule-client'), [90])
        receiver.tracker.wait(lambda t: sum(e['client_id'] == 'other-rule-client'
                                          for r in t.completed for e in events(r)) == 2)
        gate.set()
        receiver.tracker.wait(lambda t: len(t.completed) == 6)
        self.drained(6)
        own = [e for r in receiver.tracker.started for e in events(r) if e['client_id'] == 'many-rules']
        for rule in ['hot', 'second']:
            self.assertEqual([e['data']['seq'] for e in own if e['rule_id'] == rule], [0, 1])
        self.assertEqual(len({e['delivery_id'] for e in own}), 4)

    def test_batches_are_client_homogeneous_and_fifo(self):
        gate = threading.Event()
        receiver = self.receiver(gate=gate)
        self.start(concurrency.settings(receiver.url, concurrency=2, batch_size=3, batch_wait_ms=50))
        first = self.client('batch-first')
        second = self.client('batch-second')
        self.emit(first, range(7))
        self.emit(second, range(100, 107))
        receiver.tracker.wait(lambda t: t.active == 2)
        self.assertEqual({events(r)[0]['client_id'] for r in receiver.tracker.started},
                         {'batch-first', 'batch-second'})
        self.assertTrue(all(len(events(r)) == 3 for r in receiver.tracker.started))
        gate.set()
        receiver.tracker.wait(lambda t: len(t.completed) == 6)
        self.drained(14)
        for identity, expected in [('batch-first', list(range(7))), ('batch-second', list(range(100, 107)))]:
            own = [r for r in receiver.tracker.started if events(r)[0]['client_id'] == identity]
            self.assertEqual([len(events(r)) for r in own], [3, 3, 1])
            self.assertEqual([e['data']['seq'] for r in own for e in events(r)], expected)
        self.assertTrue(all(len({e['client_id'] for e in events(r)}) == 1 for r in receiver.tracker.started))
        self.assertTrue(all(r['size'] <= 1024 * 1024 for r in receiver.tracker.started))
        self.assertEqual(receiver.tracker.peak, 2)

    def test_many_client_lanes_keep_resident_window_bounded_and_pause_preserves_backlog(self):
        receiver = self.receiver(gate=threading.Event())
        config = concurrency.settings(receiver.url, concurrency=2, queue_capacity=4)
        self.start(config)
        sources = [self.client(f'cardinality-{n}') for n in range(40)]
        for n, source in enumerate(sources):
            self.emit(source, [n])
        receiver.tracker.wait(lambda t: t.active == 2)
        eventually(lambda: self.status()['durable_queue']['pending_records'] == 40)
        # Disk backlog can exceed the resident window, but not escape its memory
        # bounds. Existing channel + active batches + carry-over allowance is7.
        self.assertLessEqual(self.outbound()['queued'], 4 + 2 * 1 + 1)
        self.assertEqual(self.outbound()['dropped'], 0)
        self.assertLessEqual(self.status()['queued_bytes'], 16 * 1024 * 1024)
        paused = copy.deepcopy(config)
        paused['outbounds'][0]['enabled'] = False
        self.replace(paused)
        receiver.tracker.wait(lambda t: t.active == 0)
        self.empty()
        self.assertEqual(self.status()['durable_queue']['pending_records'], 40)
        receiver.gate.set()
        self.replace(config)
        receiver.tracker.wait(lambda t: len(t.completed) == 40)
        self.drained(40)
        for n, source in enumerate(sources[:4]):
            self.emit(source, [100 + n])
        receiver.tracker.wait(lambda t: len(t.completed) == 44)
        self.drained(44)
        self.assertEqual(sorted(concurrency.sequence(r) for r in receiver.tracker.completed),
                         list(range(40)) + [100, 101, 102, 103])
        self.assertLessEqual(receiver.tracker.peak, 2)


if __name__ == '__main__':
    unittest.main()
