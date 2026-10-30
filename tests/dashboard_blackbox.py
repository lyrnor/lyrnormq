#!/usr/bin/env python3
"""Independent Phase4f HTTP + MQTT wire acceptance, without mocked Broker data.

Run: RMQTT_BIN=/absolute/path/rmqtt python3 tests/dashboard_blackbox.py -v
Each test owns ports/files/processes. Recovery uses SIGKILL and SQLite, not a
claim of physical power-loss durability. No external Python packages required.
"""
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import tempfile
import time
import unittest
from urllib.parse import urlencode

from blackbox import frame, receive, string
from mqtt5_blackbox import (ack5, connect5, parse5, parse_ack5, props,
                            publication5, readprops)
from qos_blackbox import ack, parse_publish


ROOT = Path(__file__).resolve().parents[1]
BINARY = os.environ.get('RMQTT_BIN', str(ROOT / 'target/debug/rmqtt'))
TOKEN = 'dashboard-test-token-7b2ac829ee6a4573'


def free_port():
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        return probe.getsockname()[1]


def eventually(predicate, timeout=8):
    """Wait for a committed/live predicate; report its actual last value."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(.025)
    raise AssertionError(f'condition did not become true within {timeout}s; last={last!r}')


class DashboardBroker:
    def __init__(self, directory, sqlite=False, extra=()):
        self.root = Path(directory)
        self.port, self.admin = free_port(), free_port()
        while self.admin == self.port:
            self.admin = free_port()
        self.token_path = self.root / 'administrator-secret.token'
        self.token_path.write_text(TOKEN + '\n')
        self.assets = self.root / 'web'
        self.assets.mkdir()
        (self.assets / 'index.html').write_text(
            '<!doctype html><html><body><div id="root">fixture</div>'
            '<script src="/assets/app.js"></script></body></html>')
        (self.assets / 'assets').mkdir()
        (self.assets / 'assets/app.js').write_text('document.title="local fixture";')
        self.db = self.root / 'private-state-path.sqlite'
        self.sqlite = sqlite
        self.extra = list(extra)
        self.process = None
        self.stderr = None

    def command(self):
        command = [BINARY, '--listen', f'127.0.0.1:{self.port}',
                   '--admin-listen', f'127.0.0.1:{self.admin}',
                   '--admin-token-file', str(self.token_path),
                   '--dashboard-dir', str(self.assets)]
        if self.sqlite:
            command += ['--storage-path', str(self.db)]
        return command + self.extra

    def start(self):
        self.stderr = tempfile.TemporaryFile()
        self.process = subprocess.Popen(self.command(), stdout=subprocess.DEVNULL,
                                        stderr=self.stderr)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(f'broker exited during startup: {self.logs()}')
            try:
                code, _, data = self.request('/api/v1/overview')
                if code == 200 and data.get('ready'):
                    return self
            except (OSError, http.client.HTTPException):
                pass
            time.sleep(.025)
        raise AssertionError(f'admin API not ready: {self.logs()}')

    def logs(self):
        self.stderr.seek(0)
        return self.stderr.read().decode(errors='replace')

    def stop(self, crash=False):
        if self.process is not None and self.process.poll() is None:
            self.process.send_signal(signal.SIGKILL if crash else signal.SIGINT)
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                raise AssertionError('broker did not terminate within 8 seconds')
        if self.stderr is not None:
            self.stderr.close()
            self.stderr = None

    def request(self, path, token=TOKEN, method='GET', headers=None, body=None):
        connection = http.client.HTTPConnection('127.0.0.1', self.admin, timeout=5)
        supplied = dict(headers or {})
        if token is not None:
            supplied['Authorization'] = f'Bearer {token}'
        try:
            connection.request(method, path, body=body, headers=supplied)
            response = connection.getresponse()
            payload = response.read()
            response_headers = {key.lower(): value for key, value in response.getheaders()}
            data = json.loads(payload) if 'application/json' in response_headers.get('content-type', '') else payload
            return response.status, response_headers, data
        finally:
            connection.close()

    def data(self, path):
        code, _, data = self.request(path)
        if code != 200:
            raise AssertionError((path, code, data))
        return data


class DashboardBlackbox(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-dashboard-quality-')
        self.addCleanup(self.directory.cleanup)
        self.broker = None

    def start(self, sqlite=False, extra=()):
        self.broker = DashboardBroker(self.directory.name, sqlite, extra)
        self.addCleanup(self.broker.stop)
        return self.broker.start()

    def client(self, name, version=5, persistent=False, present=False):
        sock = socket.create_connection(('127.0.0.1', self.broker.port), timeout=5)
        sock.settimeout(5)
        self.addCleanup(sock.close)
        if version == 5:
            packet = connect5(name, clean=not persistent,
                              properties=[(0x11, 3600)] if persistent else [])
        else:
            packet = frame(0x10, string('MQTT') + bytes([4, 0 if persistent else 2])
                           + b'\x00\x1e' + string(name))
        sock.sendall(packet)
        header, data = receive(sock)
        self.assertEqual(header, 0x20)
        self.assertEqual(data[:2], bytes([int(present), 0]))
        if version == 5:
            _, end = readprops(data, 2)
            self.assertEqual(end, len(data))
        return sock

    def subscribe(self, sock, topic, qos=2, version=5, identifier=None):
        properties = props([(0x0b, identifier)] if identifier is not None else []) if version == 5 else b''
        sock.sendall(frame(0x82, b'\x00\x01' + properties + string(topic) + bytes([qos])))
        header, body = receive(sock)
        self.assertEqual(header, 0x90)
        self.assertEqual(body[:2], b'\x00\x01')
        if version == 5:
            _, offset = readprops(body, 2)
        else:
            offset = 2
        self.assertEqual(body[offset:], bytes([qos]))

    def publish(self, sock, topic, payload, qos=0, version=5, retain=False, packet_id=42, properties=()):
        if version == 5:
            packet = publication5(topic, payload, qos, packet_id, retain=retain, properties=properties)
        else:
            body = string(topic) + (struct.pack('!H', packet_id) if qos else b'') + payload
            packet = frame(0x30 | qos << 1 | int(retain), body)
        sock.sendall(packet)
        if qos:
            response = receive(sock)
            if version == 5:
                parse_ack5(response, 0x40 if qos == 1 else 0x50, packet_id)
            else:
                self.assertEqual(response, (0x40 if qos == 1 else 0x50, struct.pack('!H', packet_id)))
        if qos == 2:
            sock.sendall(ack5(0x62, packet_id) if version == 5 else ack(0x62, packet_id))
            response = receive(sock)
            if version == 5:
                parse_ack5(response, 0x70, packet_id)
            else:
                self.assertEqual(response, (0x70, struct.pack('!H', packet_id)))
        return len(packet)

    def delivery(self, sock, topic, payload, qos, version=5, dup=None, acknowledge=True):
        packet = receive(sock)
        parsed = parse5(packet) if version == 5 else parse_publish(packet)
        self.assertEqual(parsed[:3], (topic, payload, qos))
        if dup is not None:
            self.assertEqual(parsed[4], dup)
        identifier = parsed[3]
        if qos and acknowledge:
            sock.sendall(ack(0x40 if qos == 1 else 0x50, identifier))
            if qos == 2:
                response = receive(sock)
                if version == 5:
                    parse_ack5(response, 0x62, identifier)
                else:
                    self.assertEqual(response, (0x62, struct.pack('!H', identifier)))
                sock.sendall(ack(0x70, identifier))
        return parsed

    def barrier(self, sock):
        sock.sendall(frame(0xc0))
        self.assertEqual(receive(sock), (0xd0, b''))

    def session(self, client):
        return self.broker.data('/api/v1/session?' + urlencode({'client_id': client}))

    def page(self, endpoint, **query):
        return self.broker.data(f'/api/v1/{endpoint}?' + urlencode(query))

    def test_api_authentication_errors_and_secret_redaction(self):
        broker = self.start(sqlite=True)
        for path in ['/api/v1/overview', '/api/v1/configuration', '/api/v1/sessions',
                     '/api/v1/session?client_id=absent', '/api/v1/subscriptions',
                     '/api/v1/retained', '/api/v1/not-a-route']:
            for token in [None, 'incorrect-token', TOKEN[:-1]]:
                with self.subTest(path=path, token_kind='missing' if token is None else 'wrong'):
                    code, headers, data = broker.request(path, token=token)
                    self.assertEqual(code, 401, data)
                    self.assertIn('error', data)
                    self.assertIn('no-store', headers.get('cache-control', ''))
                    self.assertNotIn(TOKEN, json.dumps(data))
                    self.assertNotIn('access-control-allow-origin', headers)
        code, _, _ = broker.request('/api/v1/overview?token=' + TOKEN, token=None,
                                    headers={'Cookie': f'token={TOKEN}'})
        self.assertEqual(code, 401)
        for path in ['/api/v1/overview', '/api/v1/configuration', '/api/v1/sessions']:
            code, headers, data = broker.request(path)
            self.assertEqual(code, 200, data)
            self.assertIn('no-store', headers.get('cache-control', ''))
            encoded = json.dumps(data)
            for secret in [TOKEN, str(broker.token_path), str(broker.db), '$argon2']:
                self.assertNotIn(secret, encoded)
        configuration = broker.data('/api/v1/configuration')
        self.assertEqual(configuration['instance']['listen'], f'127.0.0.1:{broker.port}')
        self.assertEqual(configuration['limits']['max_handshakes'], 128)
        overview = broker.data('/api/v1/overview')
        self.assertEqual(overview['resources'], {'cpu_percent': None, 'rss_bytes': None})
        self.assertEqual(overview['capabilities'], {'events': False, 'resource_metrics': False, 'mutations': False})
        self.assertEqual(overview['online_sessions'], 0)
        self.assertNotIn(TOKEN, broker.logs())

    def test_bounded_queries_methods_unknown_paths_and_static_files(self):
        broker = self.start()
        for endpoint in ['sessions', 'subscriptions', 'retained']:
            for query in [{'limit': '0'}, {'limit': '101'}, {'offset': '-1'},
                          {'offset': '1000001'}, {'limit': 'x'}, {'search': 'x' * 257},
                          {'state': 'other'}, {'protocol': '6'}]:
                with self.subTest(endpoint=endpoint, query=query):
                    code, _, data = broker.request('/api/v1/' + endpoint + '?' + urlencode(query))
                    self.assertEqual(code, 400, data)
                    self.assertIsInstance(data['error']['message'], str)
            page = self.page(endpoint, limit=100, offset=1_000_000)
            self.assertEqual(page['items'], [])
            self.assertEqual(page['limit'], 100)
        for method in ['POST', 'PUT', 'DELETE', 'PATCH']:
            code, _, _ = broker.request('/api/v1/sessions', method=method, body='{}')
            self.assertEqual(code, 405)
        for path in ['/api/v1/unknown', '/api/v1/session?client_id=missing']:
            code, _, data = broker.request(path)
            self.assertEqual(code, 404)
            self.assertIn('error', data)
        code, headers, data = broker.request('/', token=None)
        self.assertEqual(code, 200)
        self.assertIn(b'fixture', data)
        self.assertIn("script-src 'self'", headers.get('content-security-policy', ''))
        self.assertIn("frame-ancestors 'none'", headers.get('content-security-policy', ''))
        self.assertEqual(headers.get('x-content-type-options'), 'nosniff')
        self.assertEqual(headers.get('referrer-policy'), 'no-referrer')
        code, _, data = broker.request('/assets/app.js', token=None)
        self.assertEqual(code, 200)
        self.assertIn(b'local fixture', data)
        for path in ['/assets/missing.js', '/../administrator-secret.token',
                     '/%2e%2e/administrator-secret.token', '/assets/%2e%2e/%2e%2e/administrator-secret.token']:
            code, _, data = broker.request(path, token=None)
            self.assertIn(code, [400, 404])
            self.assertNotIn(TOKEN.encode(), data if isinstance(data, bytes) else json.dumps(data).encode())

    def test_dual_version_qos_wire_activity_observed_without_mock_data(self):
        self.start()
        for version in [4, 5]:
            with self.subTest(version=version):
                source_id, target_id = f'source-{version}', f'target-{version}'
                source, target = self.client(source_id, version), self.client(target_id, version)
                self.subscribe(target, f'live/{version}/#', version=version,
                               identifier=7 if version == 5 else None)
                for qos in [0, 1, 2]:
                    before = self.broker.data('/api/v1/overview')
                    topic, payload = f'live/{version}/q{qos}', bytes([qos]) * 128
                    sent_bytes = self.publish(source, topic, payload, qos, version)
                    self.delivery(target, topic, payload, qos, version)
                    self.barrier(source)
                    self.barrier(target)
                    after = eventually(lambda: (value if value['messages_in_total'] == before['messages_in_total'] + 1
                                                  and value['messages_out_total'] == before['messages_out_total'] + 1 else False)
                                       if (value := self.broker.data('/api/v1/overview')) else False)
                    self.assertGreaterEqual(after['bytes_in_total'] - before['bytes_in_total'], sent_bytes)
                    self.assertGreater(after['bytes_out_total'], before['bytes_out_total'])
                session = self.session(target_id)
                self.assertEqual(session['protocol'], '5.0' if version == 5 else '3.1.1')
                self.assertTrue(session['online'])
                self.assertEqual(session['subscriptions'], 1)
                subscriptions = self.page('subscriptions', search=f'live/{version}/')['items']
                self.assertEqual(len(subscriptions), 1)
                row = subscriptions[0]
                self.assertEqual((row['client_id'], row['filter'], row['qos']), (target_id, f'live/{version}/#', 2))
                self.assertEqual(row['identifier'], 7 if version == 5 else None)
                self.assertFalse(row['no_local'])
                self.assertFalse(row['retain_as_published'])

    def test_session_pagination_filters_unicode_and_untrusted_strings(self):
        self.start()
        names = ['z-last', 'a-first', '<script>alert("x")</script>', '设备/三']
        sockets = []
        for i, name in enumerate(names):
            sockets.append(self.client(name, version=4 if i == 0 else 5, persistent=True))
        ordered = sorted(names)
        first = self.page('sessions', limit=2)
        second = self.page('sessions', limit=2, offset=2)
        self.assertEqual(first['total'], 4)
        self.assertEqual([row['client_id'] for row in first['items'] + second['items']], ordered)
        self.assertEqual(self.page('sessions', protocol='3.1.1')['total'], 1)
        self.assertEqual(self.page('sessions', search='设备')['items'][0]['client_id'], '设备/三')
        self.assertEqual(self.session(names[2])['client_id'], names[2])
        sockets[0].sendall(frame(0xe0))
        sockets[0].close()
        eventually(lambda: self.page('sessions', state='offline')['total'] == 1)
        self.assertEqual(self.page('sessions', state='online')['total'], 3)
        self.assertEqual(self.page('sessions', state='offline')['items'][0]['client_id'], 'z-last')

    def test_retained_update_delete_and_expiry_metadata(self):
        self.start()
        source = self.client('retained-source')
        topic = 'retain/<img src=x onerror=alert(1)>'
        self.publish(source, topic, b'first secret payload', 1, retain=True)
        row = eventually(lambda: (items[0] if items else False)
                         if (items := self.page('retained', search='retain/')['items']) is not None else False)
        self.assertEqual(row['topic'], topic)
        self.assertEqual(row['payload_bytes'], len(b'first secret payload'))
        self.assertNotIn('payload', row)
        self.assertNotIn('first secret payload', json.dumps(row))
        self.publish(source, topic, b'new', 1, retain=True)
        eventually(lambda: self.page('retained', search='retain/')['items'][0]['payload_bytes'] == 3)
        self.publish(source, topic, b'', 1, retain=True)
        eventually(lambda: self.page('retained', search='retain/')['total'] == 0)
        self.publish(source, 'expire/short', b'expiring', 1, retain=True, properties=[(2, 1)])
        row = self.page('retained', search='expire/')['items'][0]
        self.assertIsInstance(row['expires_at_millis'], int)
        eventually(lambda: self.page('retained', search='expire/')['total'] == 0, timeout=5)

    def test_graceful_abnormal_disconnect_and_clean_session_removal(self):
        self.start()
        graceful = self.client('graceful', persistent=True)
        abnormal = self.client('abnormal', version=4, persistent=True)
        clean = self.client('clean')
        self.subscribe(graceful, 'offline/graceful')
        self.subscribe(abnormal, 'offline/abnormal', version=4)
        graceful.sendall(frame(0xe0))
        graceful.close()
        abnormal.close()
        clean.close()
        eventually(lambda: self.page('sessions', state='offline')['total'] == 2
                   and self.page('sessions', state='online')['total'] == 0)
        self.assertEqual({item['client_id'] for item in self.page('sessions')['items']}, {'graceful', 'abnormal'})
        code, _, _ = self.broker.request('/api/v1/session?client_id=clean')
        self.assertEqual(code, 404)
        source = self.client('source')
        self.publish(source, 'offline/graceful', b'queued', 1)
        eventually(lambda: self.session('graceful')['queued'] == 1)
        resumed = self.client('graceful', persistent=True, present=True)
        self.delivery(resumed, 'offline/graceful', b'queued', 1)
        self.barrier(resumed)
        eventually(lambda: self.session('graceful')['online'] and self.session('graceful')['inflight'] == 0)

    def test_sqlite_crash_restores_offline_queue_and_inflight_packet_identifier(self):
        broker = self.start(sqlite=True)
        source = self.client('source')
        target = self.client('durable-target', persistent=True)
        self.subscribe(target, 'recover/#', qos=2, identifier=9)
        self.publish(source, 'recover/inflight', b'committed-qos1', 1, retain=True)
        first = self.delivery(target, 'recover/inflight', b'committed-qos1', 1, acknowledge=False)
        packet_id = first[3]
        target.close()
        eventually(lambda: not self.session('durable-target')['online'])
        self.publish(source, 'recover/queued', b'committed-qos2', 2)
        eventually(lambda: self.session('durable-target')['queued'] == 1)
        old_total = broker.data('/api/v1/overview')['messages_in_total']
        self.assertGreaterEqual(old_total, 2)
        broker.stop(crash=True)
        broker.start()
        recovered = self.session('durable-target')
        self.assertFalse(recovered['online'])
        self.assertTrue(recovered['persistent'])
        self.assertEqual(recovered['queued'], 1)
        self.assertEqual(recovered['inflight'], 1)
        self.assertEqual(broker.data('/api/v1/overview')['messages_in_total'], 0)
        self.assertEqual(self.page('retained')['items'][0]['payload_bytes'], len(b'committed-qos1'))
        self.assertEqual(self.page('subscriptions')['items'][0]['identifier'], 9)
        resumed = self.client('durable-target', persistent=True, present=True)
        replay = self.delivery(resumed, 'recover/inflight', b'committed-qos1', 1, dup=True)
        self.assertEqual(replay[3], packet_id)
        self.delivery(resumed, 'recover/queued', b'committed-qos2', 2)
        self.barrier(resumed)
        eventually(lambda: self.session('durable-target')['queued'] == 0
                   and self.session('durable-target')['inflight'] == 0)

    def test_sqlite_mqtt311_qos2_awaitcomp_replays_pubrel_after_crash(self):
        broker = self.start(sqlite=True)
        # Cross-version routing: MQTT5 publisher to MQTT3.1.1 durable recipient.
        source = self.client('source-mqtt5')
        target = self.client('qos2-mqtt311', version=4, persistent=True)
        self.subscribe(target, 'recover/qos2', qos=2, version=4)
        self.publish(source, 'recover/qos2', b'awaitcomp', qos=2)
        publication = self.delivery(target, 'recover/qos2', b'awaitcomp', 2,
                                    version=4, acknowledge=False)
        packet_id = publication[3]
        target.sendall(ack(0x50, packet_id))
        self.assertEqual(receive(target), (0x62, struct.pack('!H', packet_id)))
        eventually(lambda: self.session('qos2-mqtt311')['inflight'] == 1)
        broker.stop(crash=True)
        broker.start()
        restored = self.session('qos2-mqtt311')
        self.assertEqual(restored['protocol'], '3.1.1')
        self.assertFalse(restored['online'])
        self.assertEqual(restored['inflight'], 1)
        resumed = self.client('qos2-mqtt311', version=4, persistent=True, present=True)
        # PUBREL is replayed with the original identifier, never a second PUBLISH.
        self.assertEqual(receive(resumed), (0x62, struct.pack('!H', packet_id)))
        resumed.sendall(ack(0x70, packet_id))
        self.barrier(resumed)
        eventually(lambda: self.session('qos2-mqtt311')['inflight'] == 0)
        self.assertEqual(broker.data('/api/v1/overview')['messages_out_total'], 0)


class DashboardStartupValidation(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-dashboard-startup-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.token = self.root / 'secret.token'
        self.token.write_text(TOKEN)
        self.admin_address = f'127.0.0.1:{free_port()}'

    def rejected(self, args):
        process = subprocess.Popen([BINARY, '--listen', '127.0.0.1:0'] + args,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            stdout, stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            self.fail(f'invalid configuration accepted: {args}')
        self.assertNotEqual(process.returncode, 0, args)
        self.assertNotIn(TOKEN.encode(), stdout + stderr)

    def admin(self):
        return ['--admin-listen', self.admin_address, '--admin-token-file', str(self.token)]

    def test_rejects_incomplete_duplicate_nonloopback_and_export_combinations(self):
        for args in [
            ['--admin-listen', self.admin_address], ['--admin-token-file', str(self.token)],
            ['--admin-listen'], ['--admin-token-file'], ['--dashboard-dir', str(self.root)],
            self.admin() + ['--admin-listen', self.admin_address],
            self.admin() + ['--admin-token-file', str(self.token)],
            self.admin() + ['--dashboard-dir', '', '--dashboard-dir', str(self.root)],
            ['export-state', '--storage-path', str(self.root / 'state.sqlite')] + self.admin(),
        ]:
            with self.subTest(args=args):
                # export-state must be first, rather than following test --listen.
                if args[0] == 'export-state':
                    process = subprocess.run([BINARY] + args, capture_output=True, timeout=5)
                    self.assertNotEqual(process.returncode, 0)
                else:
                    self.rejected(args)
        for address in ['0.0.0.0:8080', '10.0.0.1:8080', '[::]:8080', 'localhost:8080', '', 'invalid']:
            with self.subTest(address=address):
                self.rejected(['--admin-listen', address, '--admin-token-file', str(self.token)])

    def test_rejects_invalid_token_files_and_explicit_invalid_assets(self):
        for value in [b'', b'short', b'x' * 257, b'x' * 4097, b'x' * 32 + b'\ty', b'\xff' * 32]:
            with self.subTest(token_length=len(value)):
                self.token.write_bytes(value)
                self.rejected(self.admin())
        self.token.write_text(TOKEN)
        for path in [self.root / 'missing', self.root]:
            self.rejected(['--admin-listen', self.admin_address, '--admin-token-file', str(path)])
        self.rejected(self.admin() + ['--dashboard-dir', str(self.root / 'missing')])
        assets = self.root / 'web'
        assets.mkdir()
        (assets / 'index.html').write_text('<!doctype html><div>test</div>')
        (assets / 'exposed.token').symlink_to(self.token)
        self.rejected(self.admin() + ['--dashboard-dir', str(assets)])
        (assets / 'exposed.token').unlink()
        with (assets / 'oversize.js').open('wb') as output:
            output.truncate(4 * 1024 * 1024 + 1)
        self.rejected(self.admin() + ['--dashboard-dir', str(assets)])
        (assets / 'oversize.js').unlink()
        for index in range(256):
            (assets / f'{index}.js').write_text('')
        self.rejected(self.admin() + ['--dashboard-dir', str(assets)])
        for path in assets.glob('*.js'):
            path.unlink()
        for index in range(4):
            with (assets / f'{index}.js').open('wb') as output:
                output.truncate(4 * 1024 * 1024)
        self.rejected(self.admin() + ['--dashboard-dir', str(assets)])

    def test_admin_bind_failure_fails_process_without_leaking_token(self):
        with socket.socket() as occupied:
            occupied.bind(('127.0.0.1', 0))
            occupied.listen(1)
            self.admin_address = f'127.0.0.1:{occupied.getsockname()[1]}'
            self.rejected(self.admin())


if __name__ == '__main__':
    unittest.main()
