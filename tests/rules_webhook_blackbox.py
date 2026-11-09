#!/usr/bin/env python3
"""Independent Phase5a real MQTT → SQL → Webhook acceptance (stdlib only).
Run after building: LYRNORMQ_BIN=/absolute/path/lyrnormq python3 tests/rules_webhook_blackbox.py -v
Admission before durable append remains best effort; shared queue recovery is tested separately.
"""
from collections import deque
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import queue
import socket
import sqlite3
import struct
import subprocess
import tempfile
import threading
import time
import unittest

import dashboard_blackbox as dashboard
from dashboard_blackbox import DashboardBroker, TOKEN, BINARY, eventually, free_port
from blackbox import frame, receive, string

CONFIG = '/api/v1/rules/config'
STATUS = '/api/v1/rules/status'
TEST = '/api/v1/rules/test'
SECRET = 'webhook-secret-for-negative-redaction-check'


class Webhook:
    def __init__(self, statuses=(204,), delay=0):
        self.records = queue.Queue(maxsize=10000)
        self.statuses = deque(statuses)
        self.delay = delay
        self.lock = threading.Lock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get('Content-Length', 0))
                if length > 1024 * 1024:
                    self.send_error(413)
                    return
                raw = self.rfile.read(length)
                owner.records.put_nowait((self.path, {key.lower(): value for key, value in self.headers.items()}, json.loads(raw), length))
                with owner.lock:
                    code = owner.statuses[0]
                    if len(owner.statuses) > 1:
                        owner.statuses.popleft()
                if owner.delay:
                    time.sleep(owner.delay)
                try:
                    self.send_response(code)
                    self.send_header('Content-Length', '0')
                    self.end_headers()
                except OSError:
                    pass

            def log_message(self, *_):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}/events'

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def get(self, timeout=5):
        return self.records.get(timeout=timeout)


def config_for(url, sql=None, batch_size=1, queue_capacity=64, **settings):
    delivery = dict(batch_size=batch_size, batch_wait_ms=30, queue_capacity=queue_capacity,
                    timeout_ms=1000, max_attempts=3, retry_base_ms=10, concurrency=1)
    delivery.update(settings)
    return dict(schema_version=2,
                outbounds=[dict(id='send', name='Send alarms', enabled=True, type='webhook',
                                config=dict(url=url, headers={'Authorization': f'Bearer {SECRET}'}),
                                delivery=delivery)],
                rules=[dict(id='hot', name='High temperature', enabled=True,
                            sql=sql or 'SELECT client_id, payload.temperature AS temperature FROM "devices/+/telemetry" WHERE payload.temperature >= 80',
                            outbound_ids=['send'])])



class RulesWebhook(unittest.TestCase):
    client = dashboard.DashboardBlackbox.client
    subscribe = dashboard.DashboardBlackbox.subscribe
    publish = dashboard.DashboardBlackbox.publish
    delivery = dashboard.DashboardBlackbox.delivery
    barrier = dashboard.DashboardBlackbox.barrier

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='lyrnormq-rules-quality-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.rules_path = self.root / 'rules.sqlite'
        self.broker = None

    def start(self, config=None, default=False, queue_options=()):
        self.rules_path = self.root / 'rules.sqlite'
        self.broker = DashboardBroker(self.directory.name, extra=[*queue_options])
        self.addCleanup(self.broker.stop)
        broker = self.broker.start()
        if config is not None:
            self.replace(config)
        return broker

    def stored_config(self):
        self.assertEqual(self.rules_path.read_bytes()[:16], b'SQLite format 3\0')
        with sqlite3.connect(self.rules_path) as database:
            tables = database.execute("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()
            self.assertEqual(len(tables), 1)
            name = tables[0][0].replace('"', '""')
            records = database.execute(f'SELECT * FROM "{name}"').fetchall()
            self.assertEqual(len(records), 1)
            for field in records[0]:
                if isinstance(field, bytes):
                    return json.loads(field)
        self.fail('configuration must be stored in a bounded SQLite blob')

    def webhook(self, *args, **kwargs):
        webhook = Webhook(*args, **kwargs)
        self.addCleanup(webhook.close)
        return webhook

    def request(self, path, method='GET', value=None, token=TOKEN, **kwargs):
        body = json.dumps(value) if value is not None else None
        headers = {'Content-Type': 'application/json'} if body is not None else None
        return self.broker.request(path, method=method, body=body, token=token, headers=headers, **kwargs)

    def replace(self, config):
        code, _, data = self.request(CONFIG, 'PUT', config)
        self.assertEqual(code, 200, data)
        return data

    def status(self):
        return self.broker.data(STATUS)

    def outbound_status(self, name='send'):
        item = next(outbound for outbound in self.status()['outbounds'] if outbound['id'] == name)
        self.assertEqual(item['type'], 'webhook')
        return item

    def sample(self, payload=None):
        return dict(topic='devices/a/telemetry', payload=list(payload or b'{"temperature":81}'),
                    client_id='test-source', qos=1, retain=False, timestamp_ms=123)

    def test_authenticated_mutations_sql_preview_and_no_network_side_effect(self):
        webhook = self.webhook()
        self.start(config_for(webhook.url))
        for endpoint, method, value in [(CONFIG, 'GET', None), (CONFIG, 'PUT', {}), (STATUS, 'GET', None),
                                        (TEST, 'POST', {'sql': 'bad', 'event': self.sample()})]:
            for token in [None, 'wrong']:
                code, headers, data = self.request(endpoint, method, value, token)
                self.assertEqual(code, 401, data)
                self.assertIn('no-store', headers.get('cache-control', ''))
        public = self.broker.data(CONFIG)
        self.assertNotIn(SECRET, json.dumps(public))
        self.assertEqual(public['outbounds'][0]['config']['headers']['Authorization'], '[redacted]')
        self.replace(public)
        self.assertIn(SECRET, json.dumps(self.stored_config()))
        body = {'sql': public['rules'][0]['sql'], 'event': self.sample()}
        code, _, result = self.request(TEST, 'POST', body)
        self.assertEqual(code, 200, result)
        self.assertEqual(result, {'matched': True, 'output': {'client_id': 'test-source', 'temperature': 81}})
        self.assertTrue(webhook.records.empty(), 'SQL preview must not perform outbound requests')
        self.assertNotIn(SECRET, self.broker.logs())
        self.assertEqual(self.status()['mode'], 'best_effort')

    def test_sql_errors_null_binary_and_request_body_bounds(self):
        self.start()
        for sql in ['DELETE FROM "a"', 'SELECT topic FROM "a" JOIN "b" ON true',
                    'SELECT sleep(10) FROM "a"', 'SELECT 9007199254740992 FROM "a"',
                    'SELECT topic, client_id AS topic FROM "a"', 'SELECT 1 / 0 FROM "devices/#"']:
            code, _, value = self.request(TEST, 'POST', {'sql': sql, 'event': self.sample()})
            self.assertEqual(code, 400, (sql, value))
        code, _, value = self.request(TEST, 'POST', {
            'sql': 'SELECT topic, payload.x AS x FROM "devices/#" WHERE payload.x IS NULL',
            'event': self.sample(b'\xff\x00binary')})
        self.assertEqual(code, 200, value)
        self.assertEqual(value['output'], {'topic': 'devices/a/telemetry', 'x': None})
        code, _, _ = self.broker.request(TEST, method='POST', body='{}')
        self.assertEqual(code, 415)
        code, _, _ = self.broker.request(TEST, method='POST', body='{' * 65537,
                                       headers={'Content-Type': 'application/json'})
        self.assertEqual(code, 413)
        code, _, _ = self.request(TEST, 'POST', {'sql': 'SELECT topic FROM "a"', 'event': self.sample(), 'extra': 1})
        self.assertEqual(code, 400)
        self.assertEqual(self.request(CONFIG, 'DELETE')[0], 405)
        self.assertEqual(self.request(STATUS + '?x=1')[0], 400)

    def test_dual_version_qos_sql_to_webhook_and_mqtt_delivery(self):
        webhook = self.webhook()
        self.start(config_for(webhook.url))
        for version in [4, 5]:
            source, target = self.client(f'source-{version}', version), self.client(f'target-{version}', version)
            self.subscribe(target, 'devices/+/telemetry', qos=2, version=version)
            for qos in [0, 1, 2]:
                with self.subTest(version=version, qos=qos):
                    payload = json.dumps({'temperature': 80 + qos, 'secret': 'not-selected'}).encode()
                    self.publish(source, 'devices/a/telemetry', payload, qos, version)
                    self.delivery(target, 'devices/a/telemetry', payload, qos, version)
                    path, headers, body, size = webhook.get()
                    self.assertEqual(path, '/events')
                    self.assertEqual(headers['authorization'], f'Bearer {SECRET}')
                    self.assertEqual(headers['content-type'], 'application/json')
                    self.assertEqual(body['outbound_id'], 'send')
                    self.assertLessEqual(size, 1024 * 1024)
                    self.assertEqual(len(body['events']), 1)
                    event = body['events'][0]
                    self.assertEqual(event['rule_id'], 'hot')
                    self.assertEqual(event['qos'], qos)
                    self.assertEqual(event['client_id'], f'source-{version}')
                    self.assertEqual(event['data'], {'client_id': f'source-{version}', 'temperature': 80 + qos})
                    self.assertIsInstance(event['event_id'], str)
                    self.assertNotIn('not-selected', json.dumps(body))
        eventually(lambda: self.outbound_status()['delivered'] == 6)
        self.assertEqual(self.outbound_status()['failed'], 0)

    def test_nonmatching_rules_binary_metadata_and_disabled_outbounds(self):
        webhook = self.webhook()
        config = config_for(webhook.url)
        self.start(config)
        source = self.client('source')
        for topic, payload in [('other/topic', b'{"temperature":90}'), ('devices/a/telemetry', b'{"temperature":70}'),
                               ('devices/a/telemetry', b'opaque')]:
            self.publish(source, topic, payload)
        self.barrier(source)
        eventually(lambda: self.status()['queued_events'] == 0)
        self.assertTrue(webhook.records.empty())
        config['rules'][0]['sql'] = 'SELECT topic FROM "devices/#"'
        self.replace(config)
        self.publish(source, 'devices/a/telemetry', b'opaque')
        self.assertEqual(webhook.get()[2]['events'][0]['data'], {'topic': 'devices/a/telemetry'})
        config['outbounds'][0]['enabled'] = False
        self.replace(config)
        self.publish(source, 'devices/a/telemetry', b'opaque')
        self.barrier(source)
        eventually(lambda: self.status()['queued_events'] == 0)
        self.assertTrue(webhook.records.empty())

    def test_retry_ids_remain_stable_and_permanent_error_does_not_retry(self):
        webhook = self.webhook(statuses=(500, 429, 204))
        self.start(config_for(webhook.url))
        source = self.client('source')
        self.publish(source, 'devices/a/telemetry', b'{"temperature":90}', qos=1)
        records = [webhook.get()[2] for _ in range(3)]
        self.assertIsInstance(records[0]['events'][0]['delivery_id'], str)
        self.assertEqual(records[0]['events'][0]['delivery_id'], records[2]['events'][0]['delivery_id'])
        self.assertEqual(records[0], records[1])
        self.assertEqual(records[1], records[2])
        eventually(lambda: self.outbound_status()['delivered'] == 1)
        self.assertEqual(self.outbound_status()['retried'], 2)
        with webhook.lock:
            webhook.statuses = deque([400])
        self.publish(source, 'devices/a/telemetry', b'{"temperature":91}', qos=1)
        webhook.get()
        eventually(lambda: self.outbound_status()['failed'] == 1)
        self.assertEqual(self.outbound_status()['retried'], 2)
        self.assertTrue(webhook.records.empty())
        self.assertNotIn(SECRET, json.dumps(self.status()))

    def test_batching_preserves_all_events_and_body_limit(self):
        webhook = self.webhook()
        self.start(config_for(webhook.url, batch_size=4, batch_wait_ms=500))
        source = self.client('batch-source')
        for n in range(4):
            self.publish(source, 'devices/a/telemetry', json.dumps({'temperature': 80 + n}).encode())
        seen = []
        batch_lengths = []
        while len(seen) < 4:
            _, _, body, size = webhook.get()
            self.assertLessEqual(size, 1024 * 1024)
            self.assertLessEqual(len(body['events']), 4)
            batch_lengths.append(len(body['events']))
            seen += body['events']
        self.assertGreater(max(batch_lengths), 1, "configured batching never grouped events")
        self.assertEqual(len(seen), 4)
        self.assertEqual(sorted(event['data']['temperature'] for event in seen), [80, 81, 82, 83])
        self.assertEqual(len({event['event_id'] for event in seen}), 4)
        eventually(lambda: self.outbound_status()['delivered'] == 4)

    def test_batch_splits_on_request_byte_limit(self):
        webhook = self.webhook()
        config = config_for(webhook.url, sql='SELECT payload.blob AS blob FROM "devices/#"',
                            batch_size=4, batch_wait_ms=500)
        self.start(config)
        source = self.client('large-source')
        payload = json.dumps({'blob': 'x' * 600000}).encode()
        for _ in range(2):
            self.publish(source, 'devices/a/telemetry', payload, qos=1)
        records = [webhook.get(timeout=8), webhook.get(timeout=8)]
        self.assertEqual(sum(len(record[2]['events']) for record in records), 2)
        for _, _, body, size in records:
            self.assertLessEqual(size, 1024 * 1024)
            self.assertEqual(len(body['events']), 1)
            self.assertEqual(len(body['events'][0]['data']['blob']), 600000)
        eventually(lambda: self.outbound_status()['delivered'] == 2)
        self.assertEqual(self.outbound_status()['failed'], 0)

    def test_outbound_fanout_has_distinct_delivery_ids_even_at_same_endpoint(self):
        webhook = self.webhook()
        config = config_for(webhook.url)
        config['outbounds'].append(dict(config['outbounds'][0], id='second', name='Second action'))
        config['rules'][0]['outbound_ids'].append('second')
        self.start(config)
        source = self.client('fanout-source')
        self.publish(source, 'devices/a/telemetry', b'{"temperature":90}', qos=1)
        requests = [webhook.get()[2] for _ in range(2)]
        self.assertEqual({request['outbound_id'] for request in requests}, {'send', 'second'})
        events = [request['events'][0] for request in requests]
        self.assertEqual(events[0]['event_id'], events[1]['event_id'])
        self.assertNotEqual(events[0]['delivery_id'], events[1]['delivery_id'])

    def test_slow_target_and_overflow_do_not_block_other_outbound_or_mqtt(self):
        slow, fast = self.webhook(delay=.5), self.webhook()
        config = config_for(slow.url, queue_capacity=1, timeout_ms=100, max_attempts=5, retry_base_ms=1000)
        fast_outbound = copy.deepcopy(config['outbounds'][0])
        fast_outbound.update(id='fast', name='Fast outbound', config=dict(url=fast.url, headers={}))
        fast_outbound['delivery'].update(queue_capacity=256, timeout_ms=1000)
        config['outbounds'].append(fast_outbound)
        config['rules'][0]['outbound_ids'].append('fast')
        self.start(config, queue_options=['--egress-queue-max-records', '16'])
        source = self.client('overflow-source')
        for n in range(60):
            self.publish(source, 'devices/a/telemetry', b'{"temperature":90}', qos=0)
        self.barrier(source)
        self.assertEqual(fast.get()[2]['outbound_id'], 'fast')
        eventually(lambda: self.outbound_status()['dropped'] > 0)
        eventually(lambda: self.outbound_status('fast')['delivered'] > 0)
        self.assertEqual(self.outbound_status('fast')['failed'], 0)
        self.assertLessEqual(self.status()['queued_bytes'], 16 * 1024 * 1024)

    def test_config_atomic_validation_secret_roundtrip_and_restart(self):
        webhook = self.webhook()
        config = config_for(webhook.url)
        self.start(config)
        public = self.broker.data(CONFIG)
        self.replace(public)
        original = self.stored_config()
        bad = copy.deepcopy(public)
        bad['rules'][0]['outbound_ids'] = ['absent']
        self.assertEqual(self.request(CONFIG, 'PUT', bad)[0], 400)
        self.assertEqual(self.stored_config(), original)
        self.assertEqual(self.broker.data(CONFIG), public)
        for header in ['Host', 'Content-Length', 'Content-Type', 'Connection']:
            bad = copy.deepcopy(config)
            bad['outbounds'][0]['config']['headers'][header] = 'forbidden'
            self.assertEqual(self.request(CONFIG, 'PUT', bad)[0], 400)
        bad = copy.deepcopy(config)
        bad['outbounds'][0]['config']['url'] += '?secret=do-not-accept'
        self.assertEqual(self.request(CONFIG, 'PUT', bad)[0], 400)
        self.broker.stop()
        self.broker.start()
        self.assertEqual(self.broker.data(CONFIG), public)
        self.assertNotIn(SECRET, json.dumps(self.broker.data(CONFIG)))
        source = self.client('restart-source')
        self.publish(source, 'devices/a/telemetry', b'{"temperature":90}', qos=1)
        self.assertEqual(webhook.get()[1]['authorization'], f'Bearer {SECRET}')
        if os.name == 'posix':
            self.assertEqual(self.rules_path.stat().st_mode & 0o777, 0o600)

    def test_default_rules_enabled_without_loading_legacy_json(self):
        legacy = self.root / 'rules.json'
        legacy.write_text('{invalid-legacy-json-do-not-read')
        self.start(default=True)
        self.assertTrue(self.status()['running'])
        self.assertEqual(self.broker.data(CONFIG), {'schema_version': 2, 'outbounds': [], 'rules': []})
        self.assertEqual(self.request(CONFIG, token=None)[0], 401)
        self.assertEqual(self.stored_config(), {'schema_version': 2, 'outbounds': [], 'rules': []})
        self.assertEqual(legacy.read_text(), '{invalid-legacy-json-do-not-read')
        self.barrier(self.client('default-rules'))

    def test_acknowledged_config_survives_sigkill_and_delivers_after_recovery(self):
        webhook = self.webhook()
        self.start()
        config = config_for(webhook.url)
        config['rules'][0]['name'] = 'Committed before process crash'
        public = self.replace(config)
        self.broker.stop(crash=True)
        self.broker.start()
        self.assertEqual(self.broker.data(CONFIG), public)
        self.assertEqual(self.stored_config()['rules'][0]['name'], 'Committed before process crash')
        self.assertTrue(self.status()['running'])
        source = self.client('after-crash')
        self.publish(source, 'devices/a/telemetry', b'{"temperature":90}', qos=1)
        self.assertEqual(webhook.get()[1]['authorization'], f'Bearer {SECRET}')
        self.assertNotIn(SECRET, json.dumps(self.broker.data(CONFIG)))

    def test_sqlite_write_failure_keeps_committed_config_and_stops_rules(self):
        self.start(config_for('http://127.0.0.1:1/'))
        original = self.stored_config()
        changed = copy.deepcopy(original)
        changed['rules'][0]['name'] = 'Must not be committed'
        with sqlite3.connect(self.rules_path, timeout=1) as database:
            database.execute('BEGIN IMMEDIATE')
            self.assertEqual(self.request(CONFIG, 'PUT', changed)[0], 503)
            self.assertFalse(self.status()['running'])
            self.assertNotEqual(self.broker.data(CONFIG)['rules'][0]['name'], 'Must not be committed')
            database.rollback()
        self.assertEqual(self.stored_config(), original)
        self.barrier(self.client('mqtt-after-config-failure'))
        self.broker.stop()
        self.broker.start()
        self.assertEqual(self.stored_config(), original)
        self.assertTrue(self.status()['running'])

    def test_unknown_types_versions_fields_and_references_are_atomic(self):
        self.start(config_for('http://127.0.0.1:1/'))
        original = self.stored_config()
        public = self.broker.data(CONFIG)
        invalid = []
        for kind in ['kafka', 'postgresql', 'redis', 'unknown']:
            bad = copy.deepcopy(public)
            bad['outbounds'][0]['type'] = kind
            invalid.append(bad)
        for version in [0, 1, 3, '2', None]:
            bad = copy.deepcopy(public)
            bad['schema_version'] = version
            invalid.append(bad)
        for target in ['root', 'outbound', 'config', 'delivery', 'rule']:
            bad = copy.deepcopy(public)
            item = {'root': bad, 'outbound': bad['outbounds'][0],
                    'config': bad['outbounds'][0]['config'],
                    'delivery': bad['outbounds'][0]['delivery'], 'rule': bad['rules'][0]}[target]
            item['unknown_field'] = 'reject'
            invalid.append(bad)
        bad = copy.deepcopy(public)
        bad['outbounds'] = []
        invalid.append(bad)
        invalid.append(dict(connectors=[], actions=[], rules=[]))
        for bad in invalid:
            with self.subTest(config=bad):
                self.assertEqual(self.request(CONFIG, 'PUT', bad)[0], 400)
                self.assertEqual(self.stored_config(), original)
                self.assertEqual(self.broker.data(CONFIG), public)
                self.assertTrue(self.status()['running'])
        self.assertNotIn('actions', self.status())
        self.assertNotIn('connectors', public)

    def legacy_config(self, url):
        modern = config_for(url)
        outbound = copy.deepcopy(modern['outbounds'][0])
        outbound['delivery'].pop('concurrency')
        rule = copy.deepcopy(modern['rules'][0])
        rule['action_ids'] = rule.pop('outbound_ids')
        return dict(connectors=[dict(id='alarm', name='Alarm endpoint', **outbound['config'])],
                    actions=[dict(id=outbound['id'], name=outbound['name'], enabled=True,
                                  connector_id='alarm', **outbound['delivery'])], rules=[rule])

    def seed_legacy_sqlite(self, value):
        with sqlite3.connect(self.rules_path) as database:
            database.execute("CREATE TABLE rule_configuration (id INTEGER PRIMARY KEY CHECK (id = 1), data BLOB NOT NULL CHECK (typeof(data) = 'blob'))")
            database.execute('PRAGMA application_id=1380795206')
            database.execute('PRAGMA user_version=1')
            database.execute('INSERT INTO rule_configuration VALUES(1, ?)',
                             (json.dumps(value).encode(),))

    def test_old_sqlite_migrates_once_preserving_ids_secrets_and_delivery(self):
        webhook = self.webhook()
        self.seed_legacy_sqlite(self.legacy_config(webhook.url))
        self.start()
        modern = self.stored_config()
        self.assertEqual(modern, config_for(webhook.url, concurrency=8))
        self.assertEqual(self.broker.data(CONFIG)['outbounds'][0]['config']['headers']['Authorization'], '[redacted]')
        self.assertEqual(self.broker.data(CONFIG)['rules'][0]['outbound_ids'], ['send'])
        self.broker.stop(crash=True)
        self.broker.start()
        self.assertEqual(self.stored_config(), modern)
        self.publish(self.client('migrated'), 'devices/a/telemetry', b'{"temperature":90}', qos=1)
        _, headers, body, _ = webhook.get()
        self.assertEqual(headers['authorization'], f'Bearer {SECRET}')
        self.assertEqual(body['outbound_id'], 'send')
        self.assertEqual(body['events'][0]['rule_id'], 'hot')
        self.assertNotIn('action_id', body)

    def test_invalid_old_sqlite_is_not_overwritten_during_migration(self):
        invalid = []
        missing = self.legacy_config('http://127.0.0.1:1/')
        missing['actions'][0]['connector_id'] = 'missing'
        invalid.append(missing)
        for identity in ['invalid/id', 'x' * 65]:
            legacy = self.legacy_config('http://127.0.0.1:1/')
            legacy['connectors'].append(dict(legacy['connectors'][0], id=identity))
            invalid.append(legacy)
        excessive = self.legacy_config('http://127.0.0.1:1/')
        excessive['connectors'] += [dict(excessive['connectors'][0], id=f'unused-{n}') for n in range(32)]
        invalid.append(excessive)
        for legacy in invalid:
            with self.subTest(legacy=legacy):
                if self.rules_path.exists():
                    self.rules_path.unlink()
                self.seed_legacy_sqlite(legacy)
                before = self.rules_path.read_bytes()
                from lyrnormqcfg import argv
                process = subprocess.run(argv(BINARY, '127.0.0.1:0', self.root),
                                         cwd=self.root, capture_output=True, timeout=8)
                self.assertNotEqual(process.returncode, 0)
                self.assertEqual(self.rules_path.read_bytes(), before)
                self.assertEqual(self.stored_config(), legacy)

    def test_json_foreign_and_corrupt_databases_are_rejected_without_overwrite(self):
        from lyrnormqcfg import argv
        args = argv(BINARY, '127.0.0.1:0', self.root)
        for content in [b'{"connectors":[],"actions":[],"rules":[]}', b'not a database']:
            self.rules_path.write_bytes(content)
            process = subprocess.run(args, cwd=self.root, capture_output=True, timeout=8)
            self.assertNotEqual(process.returncode, 0)
            self.assertEqual(self.rules_path.read_bytes(), content)
        self.rules_path.unlink()
        with sqlite3.connect(self.rules_path) as database:
            database.execute('CREATE TABLE foreign_records (value TEXT)')
            database.execute('INSERT INTO foreign_records VALUES (?)', ('do-not-overwrite',))
        before = self.rules_path.read_bytes()
        process = subprocess.run(args, cwd=self.root, capture_output=True, timeout=8)
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(self.rules_path.read_bytes(), before)

    def test_cli_invalid_config_and_sensitive_static_alias_fail_closed(self):
        token = self.root / 'token'
        token.write_text(TOKEN)
        args = [BINARY, '--listen', '127.0.0.1:0']
        for tail in [['--rules-config'], ['--rules-config', ''], ['--rules-config', 'a', '--rules-config', 'b'],
                     ['export-state', '--storage-path', 'a', '--rules-config', 'b']]:
            command = [BINARY] + tail if tail[0] == 'export-state' else args + tail
            process = subprocess.run(command, cwd=self.root, capture_output=True, timeout=5)
            self.assertNotEqual(process.returncode, 0)
        self.rules_path.write_text('{invalid')
        from lyrnormqcfg import argv
        process = subprocess.run(argv(BINARY, '127.0.0.1:0', self.root), capture_output=True, timeout=5, cwd=self.root)
        self.assertNotEqual(process.returncode, 0)
        self.rules_path.unlink()
        self.start()
        self.broker.stop()
        self.assertEqual(self.stored_config(), {'schema_version': 2, 'outbounds': [], 'rules': []})
        assets = self.root / 'assets'
        assets.mkdir()
        (assets / 'index.html').write_text('<div>test</div>')
        os.link(self.rules_path, assets / 'secret.js')
        command = argv(BINARY, '127.0.0.1:0', self.root, [
            '--admin-listen', f'127.0.0.1:{free_port()}',
            '--admin-token-file', str(token),
            '--dashboard-dir', str(assets),
        ])
        process = subprocess.run(command, cwd=self.root, capture_output=True, timeout=5)
        self.assertNotEqual(process.returncode, 0)
        self.assertNotIn(SECRET.encode(), process.stdout + process.stderr)


if __name__ == '__main__':
    unittest.main()
