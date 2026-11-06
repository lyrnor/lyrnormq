#!/usr/bin/env python3
"""Independent bounded outbound concurrency acceptance (stdlib only).

RMQTT_BIN=/absolute/path/rmqtt python3 tests/outbound_concurrency_blackbox.py -v
Finite request barriers measure actual receiver concurrency, not inferred TPS.
All brokers, sockets, files and HTTP servers belong to the individual test.
"""
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import select
import socket
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path

import dashboard_blackbox as dashboard
from dashboard_blackbox import DashboardBroker, eventually
from rules_webhook_blackbox import CONFIG, STATUS, config_for


class Tracker:
    def __init__(self):
        self.condition = threading.Condition()
        self.started = []
        self.completed = []
        self.active = 0
        self.peak = 0
        self.per_outbound = {}
        self.peaks = {}
        self.connections = 0
        self.closed = 0
        self.attempts = {}

    def wait(self, predicate, timeout=5):
        with self.condition:
            if not self.condition.wait_for(lambda: predicate(self), timeout):
                raise AssertionError(f'HTTP barrier timed out: active={self.active}, '
                                     f'started={len(self.started)}, completed={len(self.completed)}, '
                                     f'peak={self.peak}, closed={self.closed}')

    def begin(self, body, size, connection):
        identity = body['events'][0]['delivery_id']
        outbound = body['outbound_id']
        with self.condition:
            if len(self.started) >= 1024:
                raise AssertionError('test receiver record bound exceeded')
            attempt = self.attempts.get(identity, 0) + 1
            self.attempts[identity] = attempt
            record = dict(body=body, size=size, connection=connection, attempt=attempt)
            self.started.append(record)
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.per_outbound[outbound] = self.per_outbound.get(outbound, 0) + 1
            self.peaks[outbound] = max(self.peaks.get(outbound, 0), self.per_outbound[outbound])
            self.condition.notify_all()
        return record

    def end_active(self, record):
        with self.condition:
            self.active -= 1
            self.per_outbound[record['body']['outbound_id']] -= 1
            self.condition.notify_all()


class HttpReceiver:
    """HTTP/1.1 keepalive receiver with cancellation-aware finite request holds."""
    def __init__(self, tracker=None, gate=None, hold=None, status=None):
        self.tracker = tracker or Tracker()
        self.gate = gate
        self.hold = hold
        self.status = status or (lambda _record: 204)
        self.stop = threading.Event()
        self.sockets = set()
        self.lock = threading.Lock()
        owner = self

        class Server(ThreadingHTTPServer):
            daemon_threads = True
            request_queue_size = 128

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def setup(self):
                super().setup()
                with owner.lock:
                    owner.sockets.add(self.connection)
                with owner.tracker.condition:
                    owner.tracker.connections += 1
                    self.number = owner.tracker.connections
                    owner.tracker.condition.notify_all()

            def finish(self):
                try:
                    super().finish()
                except OSError:
                    pass
                finally:
                    with owner.lock:
                        owner.sockets.discard(self.connection)
                    with owner.tracker.condition:
                        owner.tracker.closed += 1
                        owner.tracker.condition.notify_all()

            def handle(self):
                try:
                    super().handle()
                except (ConnectionResetError, BrokenPipeError):
                    self.close_connection = True

            def do_POST(self):
                size = int(self.headers.get('Content-Length', '0'))
                if not 0 < size <= 1024 * 1024:
                    self.send_error(413)
                    return
                record = owner.tracker.begin(json.loads(self.rfile.read(size)), size, self.number)
                active = True
                try:
                    gate = owner.hold(record) if owner.hold else owner.gate
                    deadline = time.monotonic() + 12
                    while gate is not None and not gate.is_set():
                        if owner.stop.is_set() or time.monotonic() >= deadline:
                            self.close_connection = True
                            return
                        readable, _, _ = select.select([self.connection], [], [], .01)
                        if readable and self.connection.recv(1, socket.MSG_PEEK) == b'':
                            self.close_connection = True
                            return
                    # Business request work ends before writing headers. Counting
                    # the trailing bookkeeping as active could invent overlap.
                    owner.tracker.end_active(record)
                    active = False
                    self.send_response(owner.status(record))
                    self.send_header('Content-Length', '0')
                    self.end_headers()
                    with owner.tracker.condition:
                        owner.tracker.completed.append(record)
                        owner.tracker.condition.notify_all()
                except (OSError, ValueError):
                    self.close_connection = True
                finally:
                    if active:
                        owner.tracker.end_active(record)

            def log_message(self, *_args):
                pass

        self.server = Server(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=.05), daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}/events'

    def close(self):
        self.stop.set()
        if self.gate:
            self.gate.set()
        self.server.shutdown()
        with self.lock:
            sockets = list(self.sockets)
        for connection in sockets:
            try:
                connection.shutdown(socket.SHUT_RDWR)
                connection.close()
            except OSError:
                pass
        self.server.server_close()
        self.thread.join(timeout=2)


def sequence(record):
    return record['body']['events'][0]['data']['seq']


def settings(url, concurrency=8, **policy):
    options = dict(batch_wait_ms=0, timeout_ms=10000, max_attempts=1, concurrency=concurrency)
    options.update(policy)
    value = config_for(url, sql='SELECT payload.seq AS seq, payload.blob AS blob FROM "concurrency/#"',
                       **options)
    return value


class OutboundConcurrency(unittest.TestCase):
    client = dashboard.DashboardBlackbox.client
    publish = dashboard.DashboardBlackbox.publish
    barrier = dashboard.DashboardBlackbox.barrier

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-outbound-concurrency-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.rules_path = self.root / 'rules.sqlite'
        self.broker = None

    def receiver(self, **kwargs):
        value = HttpReceiver(**kwargs)
        self.addCleanup(value.close)
        return value

    def start(self, config=None):
        self.broker = DashboardBroker(self.directory.name, extra=['--rules-config', str(self.rules_path)])
        self.addCleanup(self.broker.stop)
        self.broker.start()
        if config is not None:
            self.replace(config)

    def replace(self, value):
        code, _, data = self.broker.request(CONFIG, method='PUT', body=json.dumps(value),
                                           headers={'Content-Type': 'application/json'})
        self.assertEqual(code, 200, data)
        return data

    def status(self):
        return self.broker.data(STATUS)

    def outbound(self, identity='send'):
        return next(item for item in self.status()['outbounds'] if item['id'] == identity)

    def emit(self, source, values, topic='concurrency/test', blob=None):
        for value in values:
            payload = dict(seq=value)
            if blob is not None:
                payload['blob'] = blob
            self.publish(source, topic, json.dumps(payload).encode(), qos=0)
        self.barrier(source)

    def drained(self, delivered):
        eventually(lambda: self.outbound()['delivered'] == delivered)
        eventually(lambda: self.status()['queued_events'] == 0 and
                   self.status()['queued_bytes'] == 0 and self.status()['inflight_requests'] == 0)
        self.assertEqual(self.outbound()['inflight_requests'], 0)
        if 'durable_queue' in self.status():
            eventually(lambda: self.status()['durable_queue']['pending_records'] == 0)

    def test_omitted_old_sqlite_defaults_to_eight_validates_and_recovers(self):
        receiver = self.receiver()
        old = settings(receiver.url)
        old['outbounds'][0]['delivery'].pop('concurrency')
        with sqlite3.connect(self.rules_path) as database:
            database.execute("CREATE TABLE rule_configuration (id INTEGER PRIMARY KEY CHECK (id = 1), data BLOB NOT NULL CHECK (typeof(data) = 'blob'))")
            database.execute('PRAGMA application_id=1380795206')
            database.execute('PRAGMA user_version=1')
            database.execute('INSERT INTO rule_configuration VALUES(1, ?)', (json.dumps(old).encode(),))
        database.close()
        self.start()
        public = self.broker.data(CONFIG)
        self.assertEqual(public['outbounds'][0]['delivery'].get('concurrency'), 8)
        self.assertEqual(self.status()['inflight_requests'], 0)
        self.assertEqual(self.outbound()['inflight_requests'], 0)
        for invalid in [0, 17, -1, 1.5, '8', None]:
            bad = copy.deepcopy(public)
            bad['outbounds'][0]['delivery']['concurrency'] = invalid
            code, _, _ = self.broker.request(CONFIG, method='PUT', body=json.dumps(bad),
                                             headers={'Content-Type': 'application/json'})
            self.assertEqual(code, 400, invalid)
            self.assertEqual(self.broker.data(CONFIG), public)
        public['outbounds'][0]['delivery']['concurrency'] = 3
        public = self.replace(public)
        self.broker.stop(crash=True)
        self.broker.start()
        self.assertEqual(self.broker.data(CONFIG), public)
        self.emit(self.client('recovered'), [0])
        receiver.tracker.wait(lambda t: len(t.completed) == 1)
        self.drained(1)

    def test_same_outbound_reaches_three_but_never_exceeds_and_mqtt_stays_live(self):
        gate = threading.Event()
        receiver = self.receiver(gate=gate)
        self.start(settings(receiver.url, concurrency=3))
        sources = [self.client(f'parallel-{n}') for n in range(3)]
        for n, source in enumerate(sources):
            self.emit(source, range(n, 10, 3))
        source = sources[0]
        receiver.tracker.wait(lambda t: t.active == 3)
        self.assertEqual(self.status()['inflight_requests'], 3)
        self.assertEqual(self.outbound()['inflight_requests'], 3)
        self.publish(source, 'concurrency/test', b'{"seq":10}', qos=1)
        self.barrier(source)
        self.assertLessEqual(self.status()['queued_bytes'], 16 * 1024 * 1024)
        gate.set()
        receiver.tracker.wait(lambda t: len(t.completed) == 11)
        self.drained(11)
        self.assertEqual(receiver.tracker.peak, 3)
        self.assertEqual(sorted(sequence(r) for r in receiver.tracker.completed), list(range(11)))
        print('measured same-outbound peak=3, completed=11')

    def test_default_eight_reuses_keepalive_pool_across_two_bursts(self):
        receiver = self.receiver(gate=threading.Event())
        config = settings(receiver.url)
        config['outbounds'][0]['delivery'].pop('concurrency')
        self.start(config)
        sources = [self.client(f'pool-{n}') for n in range(8)]
        for n, source in enumerate(sources):
            self.emit(source, [n])
        receiver.tracker.wait(lambda t: t.active == 8)
        self.assertEqual(self.status()['inflight_requests'], 8)
        first_connections = receiver.tracker.connections
        self.assertEqual(first_connections, 8)
        receiver.gate.set()
        receiver.tracker.wait(lambda t: len(t.completed) == 8)
        self.drained(8)
        receiver.gate = threading.Event()
        for n, source in enumerate(sources):
            self.emit(source, [n + 8])
        receiver.tracker.wait(lambda t: t.active == 8)
        self.assertEqual(receiver.tracker.connections, first_connections)
        receiver.gate.set()
        receiver.tracker.wait(lambda t: len(t.completed) == 16)
        self.drained(16)
        self.assertEqual(receiver.tracker.peak, 8)
        print('measured default peak=8, requests=16, accepted TCP connections=8')

    def test_global_sixteen_across_same_and_different_hosts(self):
        tracker, gate = Tracker(), threading.Event()
        first = self.receiver(tracker=tracker, gate=gate)
        second = self.receiver(tracker=tracker, gate=gate)
        config = settings(first.url, concurrency=16)
        for identity, url in [('same-host', first.url.replace('/events', '/other')),
                              ('other-host', second.url)]:
            extra = copy.deepcopy(config['outbounds'][0])
            extra.update(id=identity, name=identity, config=dict(url=url, headers={}))
            config['outbounds'].append(extra)
            config['rules'][0]['outbound_ids'].append(identity)
        self.start(config)
        for n in range(8):
            self.emit(self.client(f'global-{n}'), [n])
        tracker.wait(lambda t: t.active == 16)
        self.assertEqual(self.status()['inflight_requests'], 16)
        self.assertTrue(all(item['inflight_requests'] <= 16 for item in self.status()['outbounds']))
        gate.set()
        tracker.wait(lambda t: len(t.completed) == 24)
        eventually(lambda: sum(item['delivered'] for item in self.status()['outbounds']) == 24)
        eventually(lambda: self.status()['inflight_requests'] == 0 and self.status()['queued_bytes'] == 0)
        self.assertEqual(tracker.peak, 16)
        self.assertTrue(all(self.outbound(identity)['delivered'] == 8
                            for identity in ['send', 'same-host', 'other-host']))
        print(f'measured global peak={tracker.peak}, completed={len(tracker.completed)}, per-outbound peaks={tracker.peaks}')

    def test_blocked_request_does_not_stall_other_and_retry_keeps_identity(self):
        first_gate, retry_gate = threading.Event(), threading.Event()
        def hold(record):
            if sequence(record) == 0:
                return first_gate if record['attempt'] == 1 else retry_gate
            return None
        receiver = self.receiver(hold=hold, status=lambda r: 500 if sequence(r) == 0 and r['attempt'] == 1 else 204)
        self.start(settings(receiver.url, concurrency=2, max_attempts=2))
        self.emit(self.client('retry-slow'), [0])
        self.emit(self.client('retry-fast'), [1])
        receiver.tracker.wait(lambda t: any(sequence(r) == 1 for r in t.completed))
        self.assertFalse(first_gate.is_set())
        eventually(lambda: self.outbound()['delivered'] == 1)
        self.assertEqual(self.outbound()['delivered'], 1)
        first_gate.set()
        receiver.tracker.wait(lambda t: any(sequence(r) == 0 and r['attempt'] == 2 for r in t.started))
        self.assertEqual(self.outbound()['inflight_requests'], 1)
        retry_gate.set()
        receiver.tracker.wait(lambda t: len(t.completed) == 3)
        self.drained(2)
        repeated = [r['body']['events'][0] for r in receiver.tracker.started if sequence(r) == 0]
        self.assertEqual(len(repeated), 2)
        self.assertEqual(repeated[0]['event_id'], repeated[1]['event_id'])
        self.assertEqual(repeated[0]['delivery_id'], repeated[1]['delivery_id'])
        self.assertEqual(self.outbound()['retried'], 1)
        self.assertLessEqual(receiver.tracker.peak, 2)

    def test_single_mode_preserves_order_including_retry(self):
        receiver = self.receiver(status=lambda r: 500 if sequence(r) == 0 and r['attempt'] == 1 else 204)
        self.start(settings(receiver.url, concurrency=1, max_attempts=2))
        self.emit(self.client('ordered'), range(8))
        receiver.tracker.wait(lambda t: len(t.completed) == 9)
        self.drained(8)
        self.assertEqual([(sequence(r), r['attempt']) for r in receiver.tracker.started],
                         [(0, 1), (0, 2)] + [(n, 1) for n in range(1, 8)])
        self.assertEqual(receiver.tracker.peak, 1)
        self.assertEqual(self.outbound()['retried'], 1)

    def test_retry_backoff_releases_global_permits_for_other_outbound(self):
        receiver = self.receiver(status=lambda r: 500 if r['body']['outbound_id'] == 'send' else 204)
        config = settings(receiver.url, concurrency=16, max_attempts=2, retry_base_ms=10000)
        config['rules'][0]['sql'] = 'SELECT payload.seq AS seq FROM "concurrency/slow"'
        fast = copy.deepcopy(config['outbounds'][0])
        fast.update(id='fast', name='Fast', config=dict(url=receiver.url, headers={}))
        fast['delivery'].update(concurrency=1, max_attempts=1)
        config['outbounds'].append(fast)
        config['rules'].append(dict(id='fast-rule', name='Fast rule', enabled=True,
                                   sql='SELECT payload.seq AS seq FROM "concurrency/fast"', outbound_ids=['fast']))
        self.start(config)
        for n in range(16):
            self.emit(self.client(f'backoff-{n}'), [n], topic='concurrency/slow')
        source = self.client('backoff-fast')
        receiver.tracker.wait(lambda t: len(t.completed) == 16)
        eventually(lambda: self.status()['inflight_requests'] == 0 and self.outbound()['durable_queued'] == 16)
        self.assertEqual(len(receiver.tracker.started), 16)
        self.emit(source, [99], topic='concurrency/fast')
        receiver.tracker.wait(lambda t: any(r['body']['outbound_id'] == 'fast' for r in t.completed))
        self.assertFalse(any(r['attempt'] > 1 for r in receiver.tracker.started))
        eventually(lambda: self.outbound('fast')['delivered'] == 1)
        self.assertEqual(self.outbound('fast')['delivered'], 1)
        for item in config['outbounds']:
            item['enabled'] = False
        self.replace(config)
        eventually(lambda: self.status()['queued_bytes'] == 0 and self.status()['inflight_requests'] == 0)
        self.assertEqual(self.status()['durable_queue']['pending_records'], 16)

    def test_replacement_preserves_blocked_jobs_and_rejects_pending_rerouting(self):
        old = self.receiver(gate=threading.Event())
        fresh = self.receiver()
        config = settings(old.url, concurrency=4)
        self.start(config)
        sources = [self.client(f'replace-{n}') for n in range(4)]
        for n, source in enumerate(sources):
            self.emit(source, range(n, 12, 4))
        old.tracker.wait(lambda t: t.active == 4)
        eventually(lambda: self.status()['durable_queue']['pending_records'] == 12)
        rerouted = copy.deepcopy(config)
        rerouted['outbounds'][0]['config']['url'] = fresh.url
        code, _, _ = self.broker.request(CONFIG, method='PUT', body=json.dumps(rerouted),
                                         headers={'Content-Type': 'application/json'})
        self.assertEqual(code, 409)
        self.assertEqual(fresh.tracker.started, [])
        paused = copy.deepcopy(config)
        paused['outbounds'][0]['enabled'] = False
        self.replace(paused)
        old.tracker.wait(lambda t: t.active == 0 and t.closed == 4)
        eventually(lambda: self.status()['inflight_requests'] == 0 and self.status()['queued_events'] == 0 and self.status()['queued_bytes'] == 0)
        self.assertEqual(self.status()['durable_queue']['pending_records'], 12)
        self.assertEqual(self.status()['dropped'], 0)
        old.gate.set()
        self.replace(config)
        old.tracker.wait(lambda t: len(t.completed) == 12)
        self.drained(12)
        self.replace(rerouted)
        self.emit(sources[0], [99])
        fresh.tracker.wait(lambda t: len(t.completed) == 1)
        self.drained(1)
        self.assertEqual(sequence(fresh.tracker.completed[0]), 99)

    def test_shutdown_cancels_blocked_jobs_and_replays_durable_queue(self):
        receiver = self.receiver(gate=threading.Event())
        self.start(settings(receiver.url, concurrency=4))
        for n in range(4):
            self.emit(self.client(f'shutdown-{n}'), range(n, 12, 4))
        receiver.tracker.wait(lambda t: t.active == 4)
        eventually(lambda: self.status()['durable_queue']['pending_records'] == 12)
        before = {r['body']['events'][0]['delivery_id'] for r in receiver.tracker.started}
        self.broker.stop()
        receiver.tracker.wait(lambda t: t.active == 0 and t.closed == 4)
        receiver.gate.set()
        self.broker.start()
        receiver.tracker.wait(lambda t: len(t.completed) == 12)
        self.drained(12)
        self.assertEqual(self.broker.data(CONFIG)['outbounds'][0]['delivery']['concurrency'], 4)
        self.assertEqual(sorted(sequence(r) for r in receiver.tracker.completed), list(range(12)))
        self.assertTrue(before <= {r['body']['events'][0]['delivery_id'] for r in receiver.tracker.completed})
        self.emit(self.client('after-shutdown'), [99])
        receiver.tracker.wait(lambda t: len(t.completed) == 13)
        self.drained(13)
        self.assertEqual(len(receiver.tracker.started), 17)

    def test_parallel_batches_keep_count_and_one_mib_byte_limits(self):
        for large in [False, True]:
            with self.subTest(large=large):
                # Each subcase owns its Broker root and therefore cannot overwrite
                # another live process's SQLite ownership or listener files.
                gate = threading.Event()
                receiver = self.receiver(gate=gate)
                if self.broker is not None:
                    self.broker.stop()
                    self.broker = None
                    self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-outbound-batch-')
                    self.addCleanup(self.directory.cleanup)
                    self.root = Path(self.directory.name)
                    self.rules_path = self.root / 'rules.sqlite'
                self.start(settings(receiver.url, concurrency=2, batch_size=3, batch_wait_ms=50))
                count = 3 if large else 7
                for n, values in enumerate(([0, 1], [2]) if large else (range(4), range(4, 7))):
                    self.emit(self.client(f'batch-{large}-{n}'), values,
                              blob='z' * 600000 if large else None)
                receiver.tracker.wait(lambda t: t.active == 2)
                self.assertTrue(all(len(r['body']['events']) == (1 if large else 3)
                                    for r in receiver.tracker.started))
                gate.set()
                receiver.tracker.wait(lambda t: len(t.completed) == 3)
                self.drained(count)
                events = [event for record in receiver.tracker.completed for event in record['body']['events']]
                self.assertEqual(sorted(event['data']['seq'] for event in events), list(range(count)))
                self.assertTrue(all(record['size'] <= 1024 * 1024 for record in receiver.tracker.completed))
                self.assertLessEqual(receiver.tracker.peak, 2)


if __name__ == '__main__':
    unittest.main()
