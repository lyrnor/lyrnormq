#!/usr/bin/env python3
"""Real MQTT -> generic durable outbound queue acceptance (stdlib only).

RMQTT_BIN=/absolute/path/rmqtt python3 tests/outbound_durable_blackbox.py -v
SIGKILL exercises process recovery, not a simulation of physical power loss.
HTTP body acceptance before a withheld response models uncertain remote execution.
"""
import copy
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import threading
import time
import unittest

import outbound_concurrency_blackbox as concurrency
from dashboard_blackbox import DashboardBroker, eventually
from rules_webhook_blackbox import CONFIG


def event(record):
    return record['body']['events'][0]


def client_events(tracker, identity):
    return [value for record in tracker.started for value in record['body']['events']
            if value['client_id'] == identity]


class DurableOutbounds(unittest.TestCase):
    client = concurrency.OutboundConcurrency.client
    publish = concurrency.OutboundConcurrency.publish
    barrier = concurrency.OutboundConcurrency.barrier
    setUp = concurrency.OutboundConcurrency.setUp
    receiver = concurrency.OutboundConcurrency.receiver
    replace = concurrency.OutboundConcurrency.replace
    status = concurrency.OutboundConcurrency.status
    outbound = concurrency.OutboundConcurrency.outbound
    emit = concurrency.OutboundConcurrency.emit

    def start(self, config, extra=()):
        self.broker = DashboardBroker(self.directory.name, extra=[
            '--rules-config', str(self.rules_path), *extra])
        self.addCleanup(self.broker.stop)
        self.broker.start()
        self.replace(config)

    def queue(self):
        value = self.status().get('durable_queue')
        self.assertIsInstance(value, dict, 'status must distinguish durable backlog from RAM buffers')
        self.assertTrue({'pending_records', 'pending_bytes', 'disk_bytes', 'max_bytes',
                         'recovered_records'} <= value.keys())
        return value

    def committed(self, count):
        # The fallback permits the former volatile Release to reach the actual
        # restart assertion in RED, rather than failing only on a missing API key.
        def pending():
            value = self.status()
            return value.get('durable_queue', {}).get('pending_records',
                                                        self.outbound()['queued']) == count
        eventually(pending)

    def drained(self):
        eventually(lambda: self.queue()['pending_records'] == 0 and
                   self.status()['inflight_requests'] == 0 and
                   self.status()['queued_events'] == 0 and self.status()['queued_bytes'] == 0)
        self.assertEqual(self.outbound()['durable_queued'], 0)

    def quiet(self, tracker, count, duration=.2):
        with tracker.condition:
            changed = tracker.condition.wait_for(lambda: len(tracker.started) != count,
                                                  timeout=duration)
        self.assertFalse(changed, 'unexpected request crossed a paused lane or retry deadline')

    def test_graceful_restart_replays_committed_backlog_with_stable_identity(self):
        receiver = self.receiver(gate=threading.Event())
        self.start(concurrency.settings(receiver.url, concurrency=2))
        self.emit(self.client('graceful'), [0, 1, 2])
        receiver.tracker.wait(lambda t: len(t.started) == 1)
        self.committed(3)
        before = copy.deepcopy(event(receiver.tracker.started[0]))
        self.broker.stop()
        receiver.tracker.wait(lambda t: t.active == 0)
        receiver.gate.set()
        self.broker.start()
        receiver.tracker.wait(lambda t: len(t.completed) == 3)
        self.drained()
        own = client_events(receiver.tracker, 'graceful')
        self.assertEqual([e['data']['seq'] for e in own], [0, 0, 1, 2])
        self.assertEqual(own[1], before)
        self.assertGreaterEqual(self.queue()['recovered_records'], 3)

    def test_sigkill_keeps_client_fifo_and_other_client_progress(self):
        gate = threading.Event()
        receiver = self.receiver(hold=lambda r: gate if concurrency.sequence(r) == 0 else None)
        self.start(concurrency.settings(receiver.url, concurrency=2))
        source = self.client('crash-ordered')
        self.emit(source, [0, 1, 2], topic='concurrency/before-crash')
        receiver.tracker.wait(lambda t: any(concurrency.sequence(r) == 0 for r in t.started))
        self.emit(self.client('crash-independent'), [90])
        receiver.tracker.wait(lambda t: any(concurrency.sequence(r) == 90 for r in t.completed))
        self.committed(3)
        before = copy.deepcopy(event(receiver.tracker.started[0]))
        self.broker.stop(crash=True)
        receiver.tracker.wait(lambda t: t.active == 0)
        self.broker.start()
        receiver.tracker.wait(lambda t: sum(concurrency.sequence(r) == 0 for r in t.started) == 2)
        self.emit(self.client('after-crash-independent'), [91])
        receiver.tracker.wait(lambda t: any(concurrency.sequence(r) == 91 for r in t.completed))
        own = client_events(receiver.tracker, 'crash-ordered')
        self.assertEqual([e['data']['seq'] for e in own], [0, 0])
        self.assertEqual(own[1], before)
        gate.set()
        receiver.tracker.wait(lambda t: len(t.completed) == 5)
        self.drained()
        self.assertEqual([e['data']['seq'] for e in client_events(receiver.tracker, 'crash-ordered')],
                         [0, 0, 1, 2])
        self.assertLessEqual(receiver.tracker.peak, 2)

    def test_uncertain_remote_acceptance_repeats_same_delivery_not_a_new_event(self):
        receiver = self.receiver(gate=threading.Event())
        self.start(concurrency.settings(receiver.url, concurrency=1, max_attempts=1))
        self.emit(self.client('accepted-without-response'), [7])
        receiver.tracker.wait(lambda t: len(t.started) == 1)
        self.committed(1)
        accepted = copy.deepcopy(event(receiver.tracker.started[0]))
        # Receiver has parsed the full body (business execution can happen now),
        # but deliberately has not acknowledged it to the Broker.
        self.assertEqual(receiver.tracker.completed, [])
        self.broker.stop(crash=True)
        receiver.tracker.wait(lambda t: t.active == 0)
        receiver.gate.set()
        self.broker.start()
        receiver.tracker.wait(lambda t: len(t.completed) == 1)
        self.drained()
        self.assertEqual(event(receiver.tracker.completed[0]), accepted)
        self.assertEqual(len(receiver.tracker.started), 2)
        self.assertEqual(self.outbound()['failed'], 0,
                         'uncertain attempt must not exhaust the completed-failure budget')

    def test_restart_preserves_retry_deadline_and_finite_failure_budget(self):
        receiver = self.receiver(status=lambda r: 503 if concurrency.sequence(r) == 0 else 204)
        self.start(concurrency.settings(receiver.url, concurrency=1, max_attempts=2,
                                        retry_base_ms=1000))
        self.emit(self.client('backoff-recovery'), [0, 1])
        receiver.tracker.wait(lambda t: len(t.completed) == 1)
        eventually(lambda: self.status()['inflight_requests'] == 0)
        self.committed(2)
        original = copy.deepcopy(event(receiver.tracker.started[0]))
        self.quiet(receiver.tracker, 1, duration=.15)
        failed_at = time.monotonic()
        self.broker.stop(crash=True)
        self.broker.start()
        if time.monotonic() - failed_at < 1.5:
            self.quiet(receiver.tracker, 1, duration=.2)
        receiver.tracker.wait(lambda t: len(t.completed) == 3, timeout=5)
        self.drained()
        self.assertEqual([concurrency.sequence(r) for r in receiver.tracker.started], [0, 0, 1])
        self.assertEqual(event(receiver.tracker.started[1]), original)
        self.assertEqual(self.outbound()['failed'], 1)
        self.assertEqual(self.outbound()['delivered'], 1)
        self.assertEqual(self.outbound()['retried'], 1)

    def test_crash_during_retry_repeats_uncertain_attempt_then_continues_client(self):
        gate = threading.Event()
        receiver = self.receiver(hold=lambda r: gate if concurrency.sequence(r) == 0 and r['attempt'] == 2 else None,
                                 status=lambda r: 503 if concurrency.sequence(r) == 0 and r['attempt'] == 1 else 204)
        self.start(concurrency.settings(receiver.url, concurrency=2, max_attempts=2,
                                        retry_base_ms=10))
        self.emit(self.client('uncertain-retry'), [0, 1])
        receiver.tracker.wait(lambda t: len(t.started) == 2)
        self.committed(2)
        original = copy.deepcopy(event(receiver.tracker.started[0]))
        self.broker.stop(crash=True)
        receiver.tracker.wait(lambda t: t.active == 0)
        gate.set()
        self.broker.start()
        receiver.tracker.wait(lambda t: len(t.completed) == 3)
        self.drained()
        self.assertEqual([concurrency.sequence(r) for r in receiver.tracker.started], [0, 0, 0, 1])
        self.assertTrue(all(event(r) == original for r in receiver.tracker.started[:3]))
        self.assertEqual(self.outbound()['failed'], 0)

    def test_disable_pauses_and_rule_or_name_edit_preserves_pending_records(self):
        receiver = self.receiver(gate=threading.Event())
        config = concurrency.settings(receiver.url, concurrency=2)
        self.start(config)
        self.emit(self.client('paused'), [0, 1, 2, 3])
        receiver.tracker.wait(lambda t: len(t.started) == 1)
        self.committed(4)
        disabled = copy.deepcopy(config)
        disabled['outbounds'][0].update(enabled=False, name='Renamed while pending')
        disabled['rules'][0]['name'] = 'Rule edited without losing old deliveries'
        self.replace(disabled)
        receiver.tracker.wait(lambda t: t.active == 0)
        self.assertEqual(self.queue()['pending_records'], 4)
        count = len(receiver.tracker.started)
        self.quiet(receiver.tracker, count)
        self.broker.stop(crash=True)
        self.broker.start()
        self.assertEqual(self.queue()['pending_records'], 4)
        self.quiet(receiver.tracker, count)
        receiver.gate.set()
        disabled['outbounds'][0]['enabled'] = True
        self.replace(disabled)
        receiver.tracker.wait(lambda t: len(t.completed) == 4)
        self.drained()
        own = client_events(receiver.tracker, 'paused')
        self.assertEqual([e['data']['seq'] for e in own], [0, 0, 1, 2, 3])

    def test_pending_binding_changes_and_removal_return_409_without_sqlite_commit(self):
        receiver = self.receiver(gate=threading.Event())
        fresh = self.receiver()
        config = concurrency.settings(receiver.url)
        self.start(config)
        self.emit(self.client('binding'), [0, 1])
        receiver.tracker.wait(lambda t: len(t.started) == 1)
        self.committed(2)
        original = self.broker.data(CONFIG)
        changes = []
        changed = copy.deepcopy(config)
        changed['outbounds'][0]['config']['url'] = fresh.url
        changes.append(changed)
        changed = copy.deepcopy(config)
        changed['outbounds'][0]['delivery']['concurrency'] = 1
        changes.append(changed)
        changes.append(dict(schema_version=2, outbounds=[], rules=[]))
        for changed in changes:
            code, _, _ = self.broker.request(CONFIG, method='PUT', body=json.dumps(changed),
                                             headers={'Content-Type': 'application/json'})
            self.assertEqual(code, 409)
            self.assertEqual(self.broker.data(CONFIG), original)
            self.assertEqual(self.queue()['pending_records'], 2)
            self.assertTrue(self.status()['running'])
        with closing(sqlite3.connect(self.rules_path)) as db:
            self.assertEqual(json.loads(db.execute('SELECT data FROM rule_configuration').fetchone()[0]), config)
        self.assertEqual(fresh.tracker.started, [])
        receiver.gate.set()
        receiver.tracker.wait(lambda t: len(t.completed) == 2)
        self.drained()
        config['outbounds'][0]['config']['url'] = fresh.url
        self.replace(config)
        self.emit(self.client('new-binding'), [99])
        fresh.tracker.wait(lambda t: len(t.completed) == 1)
        self.drained()
        self.assertEqual(concurrency.sequence(fresh.tracker.completed[0]), 99)

    def test_messages_live_in_private_log_files_not_configuration_sqlite(self):
        receiver = self.receiver(gate=threading.Event())
        self.start(concurrency.settings(receiver.url))
        marker = 'message-must-not-be-a-sqlite-configuration-row'
        self.emit(self.client('log-files'), [0], blob=marker)
        receiver.tracker.wait(lambda t: len(t.started) == 1)
        self.committed(1)
        queue_dir = Path(str(self.rules_path) + '.outbound-queue')
        self.assertTrue(queue_dir.is_dir())
        files = [p for p in queue_dir.rglob('*') if p.is_file()]
        self.assertTrue(files)
        for path in files:
            self.assertFalse(path.is_symlink())
            self.assertEqual(path.stat().st_mode & 0o077, 0)
            with path.open('rb') as stream:
                self.assertNotEqual(stream.read(16), b'SQLite format 3\x00')
        self.assertGreater(self.queue()['disk_bytes'], 0)
        with closing(sqlite3.connect(self.rules_path)) as db:
            self.assertEqual(db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(),
                             [('rule_configuration',)])
            self.assertNotIn(marker.encode(), db.execute('SELECT data FROM rule_configuration').fetchone()[0])
        receiver.gate.set()
        receiver.tracker.wait(lambda t: len(t.completed) == 1)
        self.drained()

    def test_disk_backlog_exceeds_resident_window_without_blocking_another_client(self):
        gate = threading.Event()
        receiver = self.receiver(hold=lambda r: gate if event(r)['client_id'] == 'disk-hot' else None)
        self.start(concurrency.settings(receiver.url, concurrency=2, queue_capacity=2))
        source = self.client('disk-hot')
        for start in range(0, 40, 5):
            self.emit(source, range(start, start + 5), blob='q' * 32000)
        self.committed(40)
        self.emit(self.client('disk-independent'), [90])
        receiver.tracker.wait(lambda t: any(concurrency.sequence(r) == 90 for r in t.completed))
        self.assertEqual(self.queue()['pending_records'], 40)
        self.assertGreater(self.queue()['pending_bytes'], 1_000_000)
        self.assertLess(self.status()['queued_bytes'], self.queue()['pending_bytes'])
        self.assertLessEqual(self.status()['queued_bytes'], 16 * 1024 * 1024)
        self.assertEqual(len(client_events(receiver.tracker, 'disk-hot')), 1)
        gate.set()
        receiver.tracker.wait(lambda t: len(t.completed) == 41)
        self.drained()
        self.assertEqual([e['data']['seq'] for e in client_events(receiver.tracker, 'disk-hot')], list(range(40)))

    def test_full_record_limit_retains_old_messages_and_recovers_capacity(self):
        receiver = self.receiver(gate=threading.Event())
        queue_dir = self.root / 'explicit-queue'
        self.start(concurrency.settings(receiver.url, concurrency=2), extra=[
            '--egress-queue-dir', str(queue_dir), '--egress-queue-max-records', '8'])
        source = self.client('capacity')
        self.emit(source, range(8))
        self.committed(8)
        self.emit(source, range(8, 20))
        receiver.tracker.wait(lambda t: len(t.started) == 1)
        eventually(lambda: self.queue()['pending_records'] == 8 and self.status()['dropped'] >= 12)
        self.broker.stop(crash=True)
        receiver.tracker.wait(lambda t: t.active == 0)
        receiver.gate.set()
        self.broker.start()
        receiver.tracker.wait(lambda t: len(t.completed) == 8)
        self.drained()
        self.assertEqual([e['data']['seq'] for r in receiver.tracker.completed for e in r['body']['events']], list(range(8)))
        self.emit(self.client('capacity-next'), [99])
        receiver.tracker.wait(lambda t: len(t.completed) == 9)
        self.drained()
        self.assertTrue(queue_dir.is_dir())

    def test_small_disk_limit_can_complete_reclaim_and_accept_again(self):
        receiver = self.receiver(gate=threading.Event())
        self.start(concurrency.settings(receiver.url, concurrency=2), extra=[
            '--egress-queue-segment-bytes', '4096', '--egress-queue-max-bytes', '65536'])
        source = self.client('disk-full')
        self.emit(source, [0], blob='p' * 6000)
        self.committed(1)
        self.emit(source, range(1, 24), blob='p' * 6000)
        receiver.tracker.wait(lambda t: len(t.started) == 1)
        eventually(lambda: self.status()['matched'] == 24 and self.status()['dropped'] > 0)
        accepted = self.queue()['pending_records']
        self.assertGreater(accepted, 0)
        self.assertLess(accepted, 24)
        self.assertEqual(self.queue()['max_bytes'], 65536)
        receiver.gate.set()
        receiver.tracker.wait(lambda t: len(t.completed) == accepted)
        self.drained()
        self.emit(self.client('after-reclaim'), [99], blob='r' * 6000)
        receiver.tracker.wait(lambda t: len(t.completed) == accepted + 1)
        self.drained()
        self.assertTrue(self.status()['running'])
        self.assertLessEqual(self.queue()['disk_bytes'], 2 * self.queue()['max_bytes'])


if __name__ == '__main__':
    unittest.main()
