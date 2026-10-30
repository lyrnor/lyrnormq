#!/usr/bin/env python3
"""Independent Phase5a real MQTT → SQL → Webhook acceptance (stdlib only).
Run after building: RMQTT_BIN=/absolute/path/rmqtt python3 tests/rules_webhook_blackbox.py -v
All queues/outbound tasks are explicitly best-effort; no crash-durable action claim.
"""
from collections import deque
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import queue
import socket
import struct
import subprocess
import tempfile
import threading
import time
import unittest

from dashboard_blackbox import DashboardBroker, DashboardBlackbox, TOKEN, BINARY, eventually, free_port
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
                owner.records.put_nowait((self.path, dict(self.headers), json.loads(raw), length))
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
    action = dict(id='send', name='Send alarms', connector_id='alarm', enabled=True,
                  batch_size=batch_size, batch_wait_ms=30, queue_capacity=queue_capacity,
                  timeout_ms=1000, max_attempts=3, retry_base_ms=10)
    action.update(settings)
    return dict(connectors=[dict(id='alarm', name='Alarm endpoint', url=url,
                                 headers={'Authorization': f'Bearer {SECRET}'})],
                actions=[action], rules=[dict(id='hot', name='High temperature', enabled=True,
                                             sql=sql or 'SELECT client_id, payload.temperature AS temperature FROM "devices/+/telemetry" WHERE payload.temperature >= 80',
                                             action_ids=['send'])])


class RulesWebhook(unittest.TestCase):
    client = DashboardBlackbox.client
    subscribe = DashboardBlackbox.subscribe
    publish = DashboardBlackbox.publish
    delivery = DashboardBlackbox.delivery
    barrier = DashboardBlackbox.barrier

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-rules-quality-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.rules_path = self.root / 'private-rules.json'
        self.broker = None

    def start(self, config=None, enabled=True):
        if config is not None:
            self.rules_path.write_text(json.dumps(config))
        extra = ['--rules-config', str(self.rules_path)] if enabled else []
        self.broker = DashboardBroker(self.directory.name, extra=extra)
        self.addCleanup(self.broker.stop)
        return self.broker.start()

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

    def action_status(self, name='send'):
        return next(action for action in self.status()['actions'] if action['id'] == name)

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
        self.assertEqual(public['connectors'][0]['headers']['Authorization'], '[redacted]')
        self.replace(public)
        self.assertIn(SECRET, self.rules_path.read_text())
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
                    self.assertEqual(headers['Authorization'], f'Bearer {SECRET}')
                    self.assertEqual(headers['Content-Type'], 'application/json')
                    self.assertEqual(body['action_id'], 'send')
                    self.assertLessEqual(size, 1024 * 1024)
                    self.assertEqual(len(body['events']), 1)
                    event = body['events'][0]
                    self.assertEqual(event['rule_id'], 'hot')
                    self.assertEqual(event['qos'], qos)
                    self.assertEqual(event['client_id'], f'source-{version}')
                    self.assertEqual(event['data'], {'client_id': f'source-{version}', 'temperature': 80 + qos})
                    self.assertIsInstance(event['event_id'], str)
                    self.assertNotIn('not-selected', json.dumps(body))
        eventually(lambda: self.action_status()['delivered'] == 6)
        self.assertEqual(self.action_status()['failed'], 0)

    def test_nonmatching_rules_binary_metadata_and_disabled_actions(self):
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
        config['actions'][0]['enabled'] = False
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
        self.assertEqual(records[0], records[1])
        self.assertEqual(records[1], records[2])
        eventually(lambda: self.action_status()['delivered'] == 1)
        self.assertEqual(self.action_status()['retried'], 2)
        with webhook.lock:
            webhook.statuses = deque([400])
        self.publish(source, 'devices/a/telemetry', b'{"temperature":91}', qos=1)
        webhook.get()
        eventually(lambda: self.action_status()['failed'] == 1)
        self.assertEqual(self.action_status()['retried'], 2)
        self.assertTrue(webhook.records.empty())
        self.assertNotIn(SECRET, json.dumps(self.status()))

    def test_batching_preserves_all_events_and_body_limit(self):
        webhook = self.webhook()
        self.start(config_for(webhook.url, batch_size=4, batch_wait_ms=500))
        source = self.client('batch-source')
        for n in range(4):
            self.publish(source, 'devices/a/telemetry', json.dumps({'temperature': 80 + n}).encode())
        seen = []
        while len(seen) < 4:
            _, _, body, size = webhook.get()
            self.assertLessEqual(size, 1024 * 1024)
            self.assertLessEqual(len(body['events']), 4)
            seen += body['events']
        self.assertEqual(len(seen), 4)
        self.assertEqual(sorted(event['data']['temperature'] for event in seen), [80, 81, 82, 83])
        self.assertEqual(len({event['event_id'] for event in seen}), 4)
        eventually(lambda: self.action_status()['delivered'] == 4)

    def test_slow_target_and_overflow_do_not_block_other_action_or_mqtt(self):
        slow, fast = self.webhook(delay=.5), self.webhook()
        config = config_for(slow.url, queue_capacity=1, timeout_ms=100, max_attempts=1)
        config['connectors'].append(dict(id='fast', name='Fast endpoint', url=fast.url, headers={}))
        action = dict(config['actions'][0], id='fast', name='Fast action', connector_id='fast', queue_capacity=256, timeout_ms=1000)
        config['actions'].append(action)
        config['rules'][0]['action_ids'].append('fast')
        self.start(config)
        source = self.client('overflow-source')
        for n in range(60):
            self.publish(source, 'devices/a/telemetry', b'{"temperature":90}', qos=0)
        self.barrier(source)
        self.assertEqual(fast.get()[2]['action_id'], 'fast')
        eventually(lambda: self.action_status()['dropped'] > 0)
        eventually(lambda: self.action_status('fast')['delivered'] > 0)
        self.assertEqual(self.action_status('fast')['failed'], 0)
        self.assertLessEqual(self.status()['queued_bytes'], 16 * 1024 * 1024)

    def test_config_atomic_validation_secret_roundtrip_and_restart(self):
        webhook = self.webhook()
        config = config_for(webhook.url)
        self.start(config)
        public = self.broker.data(CONFIG)
        self.replace(public)
        original = self.rules_path.read_bytes()
        bad = copy.deepcopy(public)
        bad['rules'][0]['action_ids'] = ['absent']
        self.assertEqual(self.request(CONFIG, 'PUT', bad)[0], 400)
        self.assertEqual(self.rules_path.read_bytes(), original)
        self.assertEqual(self.broker.data(CONFIG), public)
        for header in ['Host', 'Content-Length', 'Content-Type', 'Connection']:
            bad = copy.deepcopy(config)
            bad['connectors'][0]['headers'][header] = 'forbidden'
            self.assertEqual(self.request(CONFIG, 'PUT', bad)[0], 400)
        bad = copy.deepcopy(config)
        bad['connectors'][0]['url'] += '?secret=do-not-accept'
        self.assertEqual(self.request(CONFIG, 'PUT', bad)[0], 400)
        self.broker.stop()
        self.broker.start()
        self.assertEqual(self.broker.data(CONFIG), public)
        self.assertNotIn(SECRET, json.dumps(self.broker.data(CONFIG)))
        source = self.client('restart-source')
        self.publish(source, 'devices/a/telemetry', b'{"temperature":90}', qos=1)
        self.assertEqual(webhook.get()[1]['Authorization'], f'Bearer {SECRET}')
        if os.name == 'posix':
            self.assertEqual(self.rules_path.stat().st_mode & 0o777, 0o600)

    def test_disabled_rules_fail_explicitly_and_existing_mqtt_stays_live(self):
        self.start(enabled=False)
        self.assertEqual(self.request(CONFIG)[0], 503)
        self.assertEqual(self.request(STATUS)[0], 503)
        self.assertEqual(self.request(TEST, 'POST', {'sql': 'bad', 'event': self.sample()})[0], 503)
        self.assertEqual(self.request(CONFIG, token=None)[0], 401)
        self.barrier(self.client('no-rules'))

    def test_cli_invalid_config_and_sensitive_static_alias_fail_closed(self):
        token = self.root / 'token'
        token.write_text(TOKEN)
        args = [BINARY, '--listen', '127.0.0.1:0']
        for tail in [['--rules-config'], ['--rules-config', ''], ['--rules-config', 'a', '--rules-config', 'b'],
                     ['export-state', '--storage-path', 'a', '--rules-config', 'b']]:
            command = [BINARY] + tail if tail[0] == 'export-state' else args + tail
            process = subprocess.run(command, capture_output=True, timeout=5)
            self.assertNotEqual(process.returncode, 0)
        self.rules_path.write_text('{invalid')
        process = subprocess.run(args + ['--rules-config', str(self.rules_path)], capture_output=True, timeout=5)
        self.assertNotEqual(process.returncode, 0)
        self.rules_path.write_text(json.dumps({'connectors': [], 'actions': [], 'rules': []}))
        assets = self.root / 'assets'
        assets.mkdir()
        (assets / 'index.html').write_text('<div>test</div>')
        os.link(self.rules_path, assets / 'secret.js')
        command = args + ['--rules-config', str(self.rules_path), '--admin-listen', f'127.0.0.1:{free_port()}',
                          '--admin-token-file', str(token), '--dashboard-dir', str(assets)]
        process = subprocess.run(command, capture_output=True, timeout=5)
        self.assertNotEqual(process.returncode, 0)
        self.assertNotIn(SECRET.encode(), process.stdout + process.stderr)


if __name__ == '__main__':
    unittest.main()
