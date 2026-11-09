#!/usr/bin/env python3
"""HISTORICAL Phase6a acceptance; not an active Phase6b gate.

Receive quarantine/shared remote preflight assertions below describe the superseded
scope. Kept independently for review; do not run against the Phase6b implementation.

Run after building an immutable Broker:
 RUST_LOG=warn,lyrnormq=info LYRNORMQ_LOG_FORMAT=text LYRNORMQ_BIN=/abs/lyrnormq python3 tests/http_access_blackbox.py -v
Only disposable loopback services/files/credentials. SIGKILL is not physical power loss.
"""
import base64
import copy
import json
import os
from pathlib import Path
import select
import socket
import struct
import tempfile
import threading
import time
import unittest
from urllib.parse import quote

os.environ['RUST_LOG'] = 'warn,lyrnormq=info'
os.environ['LYRNORMQ_LOG_FORMAT'] = 'text'

from admission_helpers import MutualIdentities
from auth_acl_blackbox import connect_packet as connect311
from blackbox import frame, receive, string
from dashboard_blackbox import DashboardBlackbox, DashboardBroker, eventually
from enhanced_auth_blackbox import authentication
from http_access_fixture import AccessService, PASSWORD
from mqtt5_blackbox import ack5, connect5, parse_ack5, props, publication5, readprops

ROOT = '/api/v1/access-control'


class HttpAccessBlackbox(unittest.TestCase):
    publish = DashboardBlackbox.publish
    delivery = DashboardBlackbox.delivery
    barrier = DashboardBlackbox.barrier

    @classmethod
    def setUpClass(cls):
        cls.identity_directory = tempfile.TemporaryDirectory(prefix='lyrnormq-access-identities-')
        cls.addClassCleanup(cls.identity_directory.cleanup)
        cls.identities = MutualIdentities(cls.identity_directory.name)

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='lyrnormq-http-access-')
        self.addCleanup(self.directory.cleanup)
        self.service = AccessService()
        self.addCleanup(self.service.close)
        self.broker = None

    def start(self, ttl=30, authz=True, extra=(), service=None):
        self.broker = DashboardBroker(self.directory.name, sqlite=True,
            extra=['--rules-config', str(Path(self.directory.name) / 'rules.sqlite'),
                   '--auth-rate-per-second', '0', *extra])
        self.addCleanup(self.broker.stop)
        self.broker.start()
        self.replace((service or self.service).configuration(ttl, authz))
        return self.broker

    def request(self, suffix, value=None, method=None, token=True):
        kwargs = dict(method=method or ('POST' if value is not None else 'GET'))
        if value is not None:
            kwargs.update(body=json.dumps(value), headers={'Content-Type': 'application/json'})
        if not token:
            kwargs['token'] = None
        return self.broker.request(ROOT + suffix, **kwargs)

    def replace(self, config):
        status, _, data = self.request('/config', config, 'PUT')
        self.assertEqual(status, 200, data)
        return data

    def clear(self):
        status, _, data = self.request('/cache/clear', {})
        self.assertEqual(status, 200, data)
        self.assertEqual(data['entries'], 0)

    def raw(self):
        sock = socket.create_connection(('127.0.0.1', self.broker.port), timeout=5)
        sock.settimeout(5)
        self.addCleanup(sock.close)
        return sock

    def begin(self, name, username=None, password=PASSWORD, version=5, persistent=False,
              will=None, properties=()):
        username = name if username is None else username
        sock = self.raw()
        if version == 5:
            values = list(properties) + ([(0x11, 3600)] if persistent else [])
            packet = connect5(name, clean=not persistent, username=username, password=password,
                              properties=values, will=will)
        else:
            packet = connect311(name, username=username, password=password, clean=not persistent, will=will)
        sock.sendall(packet)
        return sock

    def connack(self, sock, version=5, code=0, present=False):
        header, body = receive(sock)
        self.assertEqual(header, 0x20)
        self.assertEqual(body[:2], bytes([int(present), code]))
        if version == 5:
            values, end = readprops(body, 2)
            self.assertEqual(end, len(body))
            return values
        self.assertEqual(len(body), 2)
        return []

    def client(self, name, username=None, password=PASSWORD, version=5, persistent=False,
               present=False, will=None, properties=()):
        sock = self.begin(name, username, password, version, persistent, will, properties)
        self.connack(sock, version, present=present)
        return sock

    def subscribe(self, sock, topic, qos=2, version=5, code=None, packet_id=1):
        packet = struct.pack('!H', packet_id) + (props() if version == 5 else b'') + string(topic) + bytes([qos])
        sock.sendall(frame(0x82, packet))
        header, body = receive(sock)
        self.assertEqual(header, 0x90)
        self.assertEqual(body[:2], struct.pack('!H', packet_id))
        offset = readprops(body, 2)[1] if version == 5 else 2
        self.assertEqual(body[offset:], bytes([qos if code is None else code]))

    def quiet(self, sock, duration=.2):
        ready, _, _ = select.select([sock], [], [], duration)
        self.assertFalse(ready, 'unexpected wire activity')

    def disconnect(self, sock):
        sock.sendall(frame(0xe0))
        self.assertEqual(sock.recv(1), b'')
        sock.close()

    def session(self, name):
        return self.broker.data('/api/v1/session?client_id=' + quote(name, safe=''))

    def pending(self, name):
        row = self.session(name)
        return row['queued'] + row['inflight']

    def assert_service_shape(self):
        self.assertEqual(self.service.failures, [])
        for item in self.service.observations('/acl'):
            self.assertEqual(set(item['body']), {'client_id', 'username', 'action', 'topic'})

    def test_management_probes_drafts_redaction_and_external401_do_not_logout(self):
        self.start()
        for suffix, method, body in [('/config', 'GET', None), ('/status', 'GET', None),
                                      ('/config', 'PUT', {}), ('/cache/clear', 'POST', {})]:
            self.assertEqual(self.request(suffix, body, method, token=False)[0], 401)
        initial = self.request('/config')[2]
        before = self.request('/status')[2]
        draft = self.service.endpoint('/authenticate', enabled=False)
        draft['service_auth'] = dict(type='bearer', token='fake-service-token-only')
        draft['headers'] = {'X-Test-Secret': 'fake-header-secret-only'}
        code, _, result = self.request('/test/authentication', dict(config=draft,
            client_id='probe-client', username='probe-device', password='wrong-password'))
        self.assertEqual(code, 200)
        self.assertEqual((result['result'], result['http_status']), ('deny', 401))
        self.assertEqual(self.request('/config')[2], initial)
        self.assertEqual(self.request('/status')[2], before)
        self.assertEqual(self.broker.data('/api/v1/sessions')['total'], 0)
        config = copy.deepcopy(initial)
        config['authentication'] = draft
        config['authentication']['enabled'] = True
        saved = self.replace(config)
        self.assertEqual(saved['authentication']['service_auth']['token'], '[redacted]')
        self.assertEqual(saved['authentication']['headers']['X-Test-Secret'], '[redacted]')
        self.replace(saved)
        code, _, result = self.request('/test/authentication', dict(
            config=saved['authentication'], client_id='saved-probe',
            username='real-device', password=PASSWORD.decode()))
        self.assertEqual((code, result['result']), (200, 'allow'))
        call = self.service.observations('/authenticate', client_id='saved-probe')[-1]
        self.assertEqual(call['headers']['authorization'], 'Bearer fake-service-token-only')
        self.assertEqual(call['headers']['x-test-secret'], 'fake-header-secret-only')
        self.assertNotIn('wrong-password', self.broker.logs())
        self.assertNotIn('fake-service-token-only', self.broker.logs())
        self.assert_service_shape()

    def test_invalid_config_and_ambiguous_probe_credentials_are_atomic(self):
        self.start()
        current = self.request('/config')[2]
        for mutate in [
            lambda c: c['authentication'].update(url='http://user:secret@127.0.0.1/auth'),
            lambda c: c['authentication'].update(type='postgresql'),
            lambda c: c['authentication'].update(enabled=False),
            lambda c: c['authentication'].update(headers={'Content-Length': '1'}),
        ]:
            invalid = copy.deepcopy(current)
            mutate(invalid)
            code, _, result = self.request('/config', invalid, 'PUT')
            self.assertEqual(code, 400, result)
            self.assertEqual(self.request('/config')[2], current)
        calls = len(self.service.observations())
        code, _, _ = self.request('/test/authentication', dict(
            config=current['authentication'], client_id='probe', username='device',
            password=None, password_base64='/wA='))
        self.assertEqual(code, 400)
        self.assertEqual(len(self.service.observations()), calls)

    def test_dual_versions_qos0_1_2_use_authenticated_username_and_point_acl(self):
        self.start()
        for version in (4, 5):
            sub = self.client(f'subscriber-{version}', username=f'sub-device-{version}', version=version)
            pub = self.client(f'publisher-{version}', username=f'pub-device-{version}', version=version)
            self.subscribe(sub, f'matrix/{version}/#', version=version)
            for qos in range(3):
                topic = f'matrix/{version}/{qos}'
                payload = bytes([version, qos]) + b'point-acl'
                self.publish(pub, topic, payload, qos, version=version)
                self.delivery(sub, topic, payload, qos, version=version)
            eventually(lambda: len(self.service.observations('/acl', client_id=f'subscriber-{version}', topic=f'matrix/{version}/2')) >= 1)
            self.assertTrue(self.service.observations('/acl', username=f'pub-device-{version}', action='publish'))
            self.assertTrue(self.service.observations('/acl', username=f'sub-device-{version}', action='subscribe'))
        self.assert_service_shape()

    def test_authentication401_bad_credentials_and403_unavailable_are_distinct(self):
        self.start(authz=False)
        for version in (4, 5):
            wrong = self.begin(f'wrong-{version}', password=b'wrong', version=version)
            self.connack(wrong, version, code=4 if version == 4 else 0x86)
            self.service.auth_status = 403
            unavailable = self.begin(f'unavailable-{version}', version=version)
            self.connack(unavailable, version, code=3 if version == 4 else 0x88)
            self.service.auth_status = None
        status = self.request('/status')[2]['authentication']
        self.assertGreaterEqual(status['denied_total'], 2)
        self.assertGreaterEqual(status['errors_total'], 2)
        self.client('works-after-failures')

    def test_authentication_timeout_does_not_admit_or_block_established_client(self):
        self.start(authz=False)
        healthy = self.client('healthy-before-slow-auth')
        config = self.request('/config')[2]
        config['authentication']['timeout_ms'] = 100
        self.replace(config)
        entered = threading.Event()
        release = threading.Event()
        def policy(path, body):
            if path == '/authenticate' and body['client_id'] == 'slow-auth':
                entered.set()
                release.wait(3)
            return None
        self.service.policy = policy
        slow = self.begin('slow-auth')
        try:
            self.assertTrue(entered.wait(2))
            self.barrier(healthy)
            self.connack(slow, code=0x88)
            self.assertFalse(release.is_set())
            self.assertEqual(self.broker.data('/api/v1/sessions')['total'], 1)
        finally:
            release.set()
        self.barrier(healthy)

    def test_binary_password_is_base64_without_extra_identity_or_context(self):
        expected = b'non-utf8\x00\xff-secret'
        self.service.policy = lambda path, body: (204 if body.get('password_base64') == base64.b64encode(expected).decode() else 401) if path == '/authenticate' else None
        self.start(authz=False)
        for version in (4, 5):
            self.client(f'binary-{version}', username='binary-device', password=expected, version=version)
        calls = self.service.observations('/authenticate')
        self.assertEqual(len(calls), 2)
        for call in calls:
            self.assertEqual(set(call['body']), {'client_id', 'username', 'password_base64'})
            self.assertEqual(base64.b64decode(call['body']['password_base64']), expected)
        self.assertNotIn(base64.b64encode(expected).decode(), self.broker.logs())

    def test_failed_same_client_login_cannot_take_over_authenticated_connection(self):
        self.start(authz=False)
        original = self.client('same-client', username='unique-device', persistent=True)
        failed = self.begin('same-client', username='unique-device', password=b'bad')
        self.connack(failed, code=0x86)
        self.barrier(original)
        replacement = self.client('same-client', username='unique-device', persistent=True, present=True)
        header, body = receive(original)
        self.assertEqual((header, body[0]), (0xe0, 0x8e))
        self.barrier(replacement)

    def test_acl_cache_exact_keys_clear_and_no_cross_filter_inference(self):
        self.start(ttl=30)
        a = self.client('cache-a', username='same-device')
        b = self.client('cache-b', username='same-device')
        target = self.client('cache-target')
        self.subscribe(target, 'cache/#', qos=0)
        for n in range(2):
            self.publish(a, 'cache/exact', bytes([n]), 1)
            self.delivery(target, 'cache/exact', bytes([n]), 0)
        self.assertEqual(len(self.service.observations('/acl', client_id='cache-a', action='publish', topic='cache/exact')), 1)
        self.assertEqual(len(self.service.observations('/acl', client_id='cache-target', action='subscribe', topic='cache/exact')), 1)
        self.publish(b, 'cache/exact', b'other-client', 1)
        self.delivery(target, 'cache/exact', b'other-client', 0)
        self.assertEqual(len(self.service.observations('/acl', client_id='cache-b', action='publish', topic='cache/exact')), 1)
        self.clear()
        self.publish(a, 'cache/exact', b'after-clear', 1)
        self.delivery(target, 'cache/exact', b'after-clear', 0)
        self.assertEqual(len(self.service.observations('/acl', client_id='cache-a', action='publish', topic='cache/exact')), 2)

    def test_acl_cache0_requeries_and_expiry_does_not_renew_on_hit(self):
        self.start(ttl=0)
        pub = self.client('ttl-source')
        for _ in range(2):
            self.publish(pub, 'ttl/zero', b'no-global-cache', 1)
        self.assertEqual(len(self.service.observations('/acl', action='publish', topic='ttl/zero')), 2)
        config = self.request('/config')[2]
        config['authorization']['cache_ttl_seconds'] = 1
        self.replace(config)
        self.publish(pub, 'ttl/one', b'first', 1)
        time.sleep(.55)
        self.publish(pub, 'ttl/one', b'hit', 1)
        time.sleep(.6)
        self.publish(pub, 'ttl/one', b'expired', 1)
        self.assertEqual(len(self.service.observations('/acl', action='publish', topic='ttl/one')), 2)

    def test_dormant_connections_do_not_pin_retired_configuration_generations(self):
        self.start()
        clients = []
        for generation in range(8):
            config = self.request('/config')[2]
            config['authentication']['headers'] = {'X-Generation': str(generation)}
            self.replace(config)
            clients.append(self.client(f'generation-{generation}'))
        for client in clients:
            self.barrier(client)
        config = self.request('/config')[2]
        self.assertEqual(config['authentication']['headers']['X-Generation'], '[redacted]')

    def test_probe_unknown_status_and_acl401_are_unavailable_not_business_denials(self):
        self.start()
        for status in (200, 202, 302, 401, 429, 503):
            self.service.policy = lambda path, body, status=status: status
            draft = self.service.configuration()['authorization']
            code, _, result = self.request('/test/authorization', dict(config=draft,
                client_id='probe', username='device', action='publish', topic='probe/topic'))
            self.assertEqual(code, 200)
            self.assertEqual((result['result'], result['http_status']), ('unavailable', status))
        self.assertEqual(self.request('/status')[2]['authorization']['requests_total'], 0)
        self.assertEqual(self.request('/status')[2]['cache']['entries'], 0)

    def test_subscribe_denial_is_per_filter_and_does_not_destroy_other_subscription(self):
        self.start(ttl=0)
        self.service.policy = lambda path, body: 403 if path == '/acl' and body['action'] == 'subscribe' and body['topic'] == 'private/#' else None
        sub = self.client('denied-filter')
        pub = self.client('allowed-publisher')
        self.subscribe(sub, 'public/#', qos=1)
        self.subscribe(sub, 'private/#', qos=1, code=0x87, packet_id=2)
        self.publish(pub, 'public/value', b'preserved', 1)
        self.delivery(sub, 'public/value', b'preserved', 1)
        self.barrier(sub)

    def test_custom_tls_configuration_is_rejected_and_not_exposed(self):
        self.start(authz=False)
        original = self.request('/config')[2]
        for stage in ('authentication', 'authorization'):
            self.assertNotIn('tls', original[stage])
            changed = copy.deepcopy(original)
            changed[stage]['tls'] = {'ca_pem': 'obsolete-certificate'}
            self.assertEqual(self.request('/config', changed, 'PUT')[0], 400)
            self.assertEqual(self.request('/config')[2], original)
        for stage in ('authentication', 'authorization'):
            config = copy.deepcopy(original[stage])
            config['tls'] = {}
            payload = dict(config=config, client_id='probe', username='device')
            payload.update(dict(password=PASSWORD.decode()) if stage == 'authentication'
                           else dict(action='publish', topic='test/topic'))
            self.assertEqual(self.request('/test/' + stage, payload)[0], 400)

    def test_https_untrusted_certificate_remains_unavailable(self):
        service = AccessService(self.identities)
        self.addCleanup(service.close)
        self.start(authz=False)
        for stage, path in (('authentication', '/authenticate'), ('authorization', '/acl')):
            config = service.endpoint(path)
            if stage == 'authorization':
                config['cache_ttl_seconds'] = 30
            payload = dict(config=config, client_id='untrusted-probe', username='device')
            payload.update(dict(password=PASSWORD.decode()) if stage == 'authentication'
                           else dict(action='publish', topic='test/topic'))
            code, _, result = self.request('/test/' + stage, payload)
            self.assertEqual(code, 200)
            self.assertEqual(result['result'], 'unavailable')
            self.assertIsNone(result['http_status'])
        self.assertEqual(service.observations(), [])
        self.assertEqual(self.request('/status')[2]['authentication']['requests_total'], 0)

    def test_retained_and_will_use_original_device_and_actual_topic(self):
        self.start(ttl=0)
        watcher = self.client('will-watcher')
        self.subscribe(watcher, 'will/#', qos=1)
        owner = self.client('will-owner', username='will-device', will=('will/device', b'offline', 1, False))
        self.assertTrue(self.service.observations('/acl', username='will-device', action='publish', topic='will/device'))
        owner.close()
        self.delivery(watcher, 'will/device', b'offline', 1)
        pub = self.client('retained-owner')
        self.publish(pub, 'retained/value', b'stored', 1, retain=True)
        fresh = self.client('retained-reader')
        self.subscribe(fresh, 'retained/#', qos=1)
        self.delivery(fresh, 'retained/value', b'stored', 1)
        self.assertTrue(self.service.observations('/acl', username='retained-reader', action='subscribe', topic='retained/value'))

    def test_receive_unavailable_quarantine_survives_sigkill_then_reconnect(self):
        self.start(ttl=0)
        sub = self.client('quarantine-target', persistent=True)
        self.subscribe(sub, 'quarantine/#', qos=2)
        self.disconnect(sub)
        self.service.policy = lambda path, body: 503 if path == '/acl' and body['client_id'] == 'quarantine-target' and body['topic'] == 'quarantine/value' else None
        pub = self.client('quarantine-source')
        self.publish(pub, 'quarantine/value', b'durable-before-receive-authorization', 2)
        eventually(lambda: self.pending('quarantine-target') == 1)
        resumed = self.client('quarantine-target', persistent=True, present=True)
        eventually(lambda: bool(self.service.observations('/acl', client_id='quarantine-target', topic='quarantine/value')))
        self.quiet(resumed)
        self.assertEqual(self.pending('quarantine-target'), 1)
        self.broker.stop(crash=True)
        self.broker.start()
        self.assertEqual(self.pending('quarantine-target'), 1)
        self.service.policy = None
        final = self.client('quarantine-target', persistent=True, present=True)
        self.delivery(final, 'quarantine/value', b'durable-before-receive-authorization', 2)
        self.barrier(final)
        eventually(lambda: self.session('quarantine-target')['queued'] == 0 and self.session('quarantine-target')['inflight'] == 0)

    def test_shared_denied_member_is_skipped_for_allowed_member(self):
        self.start(ttl=0)
        self.service.policy = lambda path, body: 403 if path == '/acl' and body['client_id'] == 'a-denied' and body['action'] == 'subscribe' and body['topic'] == 'shared/value' else None
        denied = self.client('a-denied')
        allowed = self.client('z-allowed')
        self.subscribe(denied, '$share/workers/shared/#', qos=2)
        self.subscribe(allowed, '$share/workers/shared/#', qos=2)
        pub = self.client('shared-source')
        for qos in range(3):
            self.publish(pub, 'shared/value', bytes([qos]), qos)
            self.delivery(allowed, 'shared/value', bytes([qos]), qos)
            self.quiet(denied)

    def test_plain_authentication_and_same_owner_reauthentication(self):
        self.start(authz=True, extra=['--enhanced-auth-plain'])
        sock = self.raw()
        plain = b'\0plain-device\0' + PASSWORD
        sock.sendall(connect5('plain-client', properties=[(0x15, 'PLAIN'), (0x16, plain)]))
        values = self.connack(sock)
        self.assertEqual(dict(values)[0x15], 'PLAIN')
        sock.sendall(authentication(0x19, plain))
        header, body = receive(sock)
        self.assertEqual((header, body[0]), (0xf0, 0))
        self.barrier(sock)
        sock.sendall(authentication(0x19, b'\0different-device\0' + PASSWORD))
        header, body = receive(sock)
        self.assertEqual((header, body[0]), (0xe0, 0x87))

    def test_http_authentication_concurrency_is_independent_of_local_hash_cpu_limit(self):
        self.start(authz=False, extra=['--max-auth-tasks', '1'])
        config = self.request('/config')[2]
        config['authentication']['max_concurrency'] = 8
        config['authentication']['timeout_ms'] = 3000
        self.replace(config)
        entered = threading.Event()
        release = threading.Event()
        def policy(path, body):
            if path == '/authenticate' and body['client_id'].startswith('parallel-'):
                with self.service.lock:
                    if self.service.active >= 8:
                        entered.set()
                release.wait(3)
            return None
        self.service.policy = policy
        clients = [self.begin(f'parallel-{index}') for index in range(8)]
        try:
            self.assertTrue(entered.wait(2), 'HTTP authentication serialized behind local CPU admission')
        finally:
            release.set()
        for client in clients:
            self.connack(client)
        self.assertGreaterEqual(self.service.peak, 8)

    def test_pending_authentication_cannot_admit_after_configuration_generation_changes(self):
        self.start(authz=False)
        entered = threading.Event()
        release = threading.Event()
        def policy(path, body):
            if path == '/authenticate' and body['client_id'] == 'late-client':
                entered.set()
                release.wait(3)
            return None
        self.service.policy = policy
        sock = self.begin('late-client')
        self.assertTrue(entered.wait(2))
        config = self.request('/config')[2]
        config['authentication']['headers'] = {'X-Generation': 'changed'}
        self.replace(config)
        release.set()
        header, body = receive(sock)
        self.assertEqual(header, 0x20)
        self.assertNotEqual(body[1], 0, 'stale configuration admitted CONNECT')
        self.assertEqual(self.broker.data('/api/v1/sessions')['total'], 0)

    def test_shared_unavailable_preflight_preserves_qos2_without_partial_fanout_before_crash(self):
        self.start(ttl=0)
        ordinary = self.client('ordinary-target', persistent=True)
        shared = self.client('shared-target', persistent=True)
        self.subscribe(ordinary, 'partial/#', qos=2)
        self.subscribe(shared, '$share/group/partial/#', qos=2)
        self.service.policy = lambda path, body: 503 if path == '/acl' and body['client_id'] == 'shared-target' and body['topic'] == 'partial/value' else None
        source = self.client('partial-source', persistent=True)
        source.sendall(publication5('partial/value', b'once-per-recipient', 2, 77))
        parse_ack5(receive(source), 0x50, 77)
        source.sendall(ack5(0x62, 77))
        eventually(lambda: bool(self.service.observations('/acl', client_id='shared-target', topic='partial/value')))
        self.quiet(ordinary, .25)
        self.quiet(shared, .25)
        self.quiet(source, .25)
        self.broker.stop(crash=True)
        self.service.policy = None
        self.broker.start()
        ordinary2 = self.client('ordinary-target', persistent=True, present=True)
        shared2 = self.client('shared-target', persistent=True, present=True)
        source2 = self.client('partial-source', persistent=True, present=True)
        source2.sendall(ack5(0x62, 77))
        parse_ack5(receive(source2), 0x70, 77)
        self.delivery(ordinary2, 'partial/value', b'once-per-recipient', 2)
        self.delivery(shared2, 'partial/value', b'once-per-recipient', 2)
        self.quiet(ordinary2, .3)
        self.quiet(shared2, .3)
        self.barrier(source2)


if __name__ == '__main__':
    unittest.main()
