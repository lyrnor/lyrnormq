#!/usr/bin/env python3
"""Independent Phase6b HTTP operation admission and MQTT state acceptance.

Run after building an immutable Broker:
 RUST_LOG=warn,rmqtt=info RMQTT_LOG_FORMAT=text RMQTT_BIN=/abs/rmqtt python3 tests/http_access_blackbox.py -v
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

os.environ['RUST_LOG'] = 'warn,rmqtt=info'
os.environ['RMQTT_LOG_FORMAT'] = 'text'

from admission_helpers import MutualIdentities
import auth_acl_blackbox
from auth_acl_blackbox import connect_packet as connect311
from blackbox import frame, receive, string
import dashboard_blackbox
from dashboard_blackbox import DashboardBroker, eventually
from enhanced_auth_blackbox import authentication
from http_access_fixture import AccessService, PASSWORD
from mqtt5_blackbox import ack5, connect5, parse_ack5, props, publication5, readprops

ROOT = '/api/v1/access-control'


class HttpAccessBlackbox(unittest.TestCase):
    publish = dashboard_blackbox.DashboardBlackbox.publish
    delivery = dashboard_blackbox.DashboardBlackbox.delivery
    barrier = dashboard_blackbox.DashboardBlackbox.barrier

    @classmethod
    def setUpClass(cls):
        cls.identity_directory = tempfile.TemporaryDirectory(prefix='rmqtt-access-identities-')
        cls.addClassCleanup(cls.identity_directory.cleanup)
        cls.identities = MutualIdentities(cls.identity_directory.name)

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-http-access-')
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
            self.barrier(sub)
            self.assertEqual(len(self.service.observations('/acl', client_id=f'subscriber-{version}')), 1)
            self.assertEqual(len(self.service.observations('/acl', client_id=f'publisher-{version}')), 3)
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
        for version in (4, 5):
            name = f'same-client-{version}'
            original = self.client(name, username='unique-device', persistent=True, version=version)
            for unavailable in (False, True):
                self.service.auth_status = 503 if unavailable else None
                failed = self.begin(name, username='unique-device', password=b'bad', version=version)
                self.connack(failed, version, code=(3 if unavailable else 4) if version == 4 else (0x88 if unavailable else 0x86))
                self.barrier(original)
                self.assertTrue(self.session(name)['online'])
            self.service.auth_status = None
            replacement = self.client(name, username='unique-device', persistent=True, present=True, version=version)
            if version == 5:
                header, body = receive(original)
                self.assertEqual((header, body[0]), (0xe0, 0x8e))
            else:
                self.assertEqual(original.recv(1), b'')
            self.barrier(replacement)

    def test_valid_different_username_cannot_resume_another_owners_session(self):
        self.start(authz=False)
        for version in (4, 5):
            name = f'owned-{version}'
            original = self.client(name, username='owner-device', persistent=True, version=version)
            other = self.begin(name, username='different-device', persistent=True, version=version)
            self.connack(other, version, code=5 if version == 4 else 0x87)
            self.barrier(original)
            self.assertTrue(self.session(name)['online'])

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
        self.assertEqual(len(self.service.observations('/acl', client_id='cache-target', action='subscribe', topic='cache/#')), 1)
        self.assertEqual(self.service.observations('/acl', client_id='cache-target', topic='cache/exact'), [])
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

    def test_mixed_subscribe_filters_admit_individually_without_denied_retained_replay(self):
        self.start(ttl=0)
        pub = self.client('mixed-publisher')
        self.publish(pub, 'private/retained', b'not-for-denied-filter', 1, retain=True)
        for version in (4, 5):
            sub = self.client(f'mixed-{version}', version=version)
            self.subscribe(sub, 'existing/#', qos=1, version=version)
            self.service.policy = lambda path, body: 403 if path == '/acl' and body['action'] == 'subscribe' and body['topic'] == 'private/#' else None
            body = struct.pack('!H', 9) + (props() if version == 5 else b'')
            for topic in ('public/#', 'private/#'):
                body += string(topic) + bytes([1])
            sub.sendall(frame(0x82, body))
            header, response = receive(sub)
            self.assertEqual((header, response[:2]), (0x90, b'\x00\x09'))
            offset = readprops(response, 2)[1] if version == 5 else 2
            self.assertEqual(response[offset:], bytes([1, 0x80 if version == 4 else 0x87]))
            self.quiet(sub)
            for prefix in ('public', 'existing'):
                self.publish(pub, prefix + '/value', b'still-admitted', 1)
                self.delivery(sub, prefix + '/value', b'still-admitted', 1, version=version)
            self.assertEqual(len(self.service.observations('/acl', client_id=f'mixed-{version}')), 3)
            self.disconnect(sub)

    def test_publish_denied_or_unavailable_all_qos_preserves_retained_and_fanout(self):
        self.start(ttl=0)
        admin = self.client('denial-admin')
        for version in (4, 5):
            for status in (403, 503):
                for qos in (0, 1, 2):
                    with self.subTest(version=version, status=status, qos=qos):
                        topic = f'protected/{version}/{status}/{qos}'
                        self.service.policy = None
                        self.publish(admin, topic, b'preserved', 1, retain=True)
                        observer = self.client(f'observer-{version}-{status}-{qos}')
                        self.subscribe(observer, topic, qos=0)
                        self.delivery(observer, topic, b'preserved', 0)
                        attacker = self.client(f'attacker-{version}-{status}-{qos}', version=version)
                        self.service.policy = lambda path, body, status=status: status if path == '/acl' and body['action'] == 'publish' else None
                        if version == 5:
                            packet = publication5(topic, b'', qos, 17, retain=True)
                        else:
                            packet = frame(0x31 | qos << 1, string(topic) + (struct.pack('!H', 17) if qos else b''))
                        attacker.sendall(packet)
                        if version == 5:
                            header, body = receive(attacker)
                            self.assertEqual(header, 0xe0)
                            self.assertEqual(body[0], 0x87 if status == 403 else 0x89)
                        self.assertEqual(attacker.recv(1), b'')
                        self.quiet(observer)
                        self.assertEqual(len(self.service.observations('/acl', client_id=f'attacker-{version}-{status}-{qos}')), 1)
                        replay = self.client(f'replay-{version}-{status}-{qos}')
                        self.subscribe(replay, topic, qos=0)
                        self.delivery(replay, topic, b'preserved', 0)
                        self.disconnect(observer)
                        self.disconnect(replay)

    def test_resolved_topic_alias_is_authorized_and_unknown_alias_has_no_rpc(self):
        self.start(ttl=0)
        pub = self.client('alias-source')
        self.publish(pub, 'alias/resolved', b'bind', 1, properties=[(0x23, 1)])
        self.publish(pub, '', b'use', 1, properties=[(0x23, 1)])
        calls = self.service.observations('/acl', client_id='alias-source')
        self.assertEqual([c['body']['topic'] for c in calls], ['alias/resolved', 'alias/resolved'])
        self.service.policy = lambda path, body: 403 if path == '/acl' else None
        pub.sendall(publication5('', b'denied', 1, 20, properties=[(0x23, 1)]))
        header, body = receive(pub)
        self.assertEqual((header, body[0]), (0xe0, 0x87))
        self.assertEqual(self.service.observations('/acl', client_id='alias-source')[-1]['body']['topic'], 'alias/resolved')
        unknown = self.client('alias-unknown')
        unknown.sendall(publication5('', b'invalid', 1, 21, properties=[(0x23, 2)]))
        header, body = receive(unknown)
        self.assertEqual((header, body[0]), (0xe0, 0x94))
        self.assertEqual(self.service.observations('/acl', client_id='alias-unknown'), [])

    def test_subscribe_unavailable_keeps_existing_subscription_and_queue(self):
        self.start(ttl=0)
        pub = self.client('unavailable-source')
        for version in (4, 5):
            self.service.policy = None
            name = f'unavailable-sub-{version}'
            sub = self.client(name, version=version, persistent=True)
            self.subscribe(sub, f'unavailable/{version}/#', qos=1, version=version)
            self.service.policy = lambda path, body: 503 if path == '/acl' and body['action'] == 'subscribe' else None
            body = struct.pack('!H', 2) + (props() if version == 5 else b'') + string('new-unavailable/#') + bytes([1])
            sub.sendall(frame(0x82, body))
            # Unavailability fails the entire current operation. 0x89 is a
            # DISCONNECT reason, never a legal per-filter SUBACK reason.
            if version == 5:
                header, body = receive(sub)
                self.assertEqual((header, body[0]), (0xe0, 0x89))
            self.assertEqual(sub.recv(1), b'')
            self.publish(pub, f'unavailable/{version}/value', b'preserved-queue', 1)
            eventually(lambda: self.pending(name) == 1)
            resumed = self.client(name, version=version, persistent=True, present=True)
            self.delivery(resumed, f'unavailable/{version}/value', b'preserved-queue', 1, version=version)
            self.barrier(resumed)
            self.assertEqual(len(self.service.observations('/acl', client_id=name)), 2)
            self.disconnect(resumed)

    def test_pending_publish_generation_change_cannot_install_late_retained_or_fanout(self):
        self.start(ttl=0)
        watcher = self.client('late-watcher')
        self.subscribe(watcher, 'late/#', qos=1)
        source = self.client('late-publisher')
        entered = threading.Event()
        release = threading.Event()
        def policy(path, body):
            if path == '/acl' and body['client_id'] == 'late-publisher':
                entered.set()
                release.wait(3)
            return None
        self.service.policy = policy
        source.sendall(publication5('late/value', b'never-install', 1, 99, retain=True))
        try:
            self.assertTrue(entered.wait(2))
            self.barrier(watcher)
            self.quiet(watcher)
            config = self.request('/config')[2]
            config['authorization']['headers'] = {'X-Generation': 'changed'}
            self.replace(config)
        finally:
            release.set()
        header, body = receive(source)
        self.assertEqual((header, body[0]), (0xe0, 0x89))
        self.quiet(watcher)
        self.assertEqual(self.broker.data('/api/v1/retained?search=late%2F')['total'], 0)
        self.assertEqual(len(self.service.observations('/acl', client_id='late-publisher')), 1)

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

    def test_retained_replay_and_will_do_not_issue_remote_checks(self):
        # Will publish admission belongs to CONNECT; later expiry/delivery must
        # reuse that grant, without the superseded Phase6a receive-time checks.
        self.start(ttl=0)
        watcher = self.client('will-watcher')
        self.subscribe(watcher, 'will/#', qos=1)
        owner = self.client('will-owner', username='will-device', will=('will/device', b'offline', 1, False))
        will_calls = self.service.observations('/acl', client_id='will-owner')
        self.assertEqual(len(will_calls), 1)
        self.assertEqual(will_calls[0]['body'], dict(client_id='will-owner',
            username='will-device', action='publish', topic='will/device'))
        calls = len(self.service.observations('/acl'))
        self.service.policy = lambda path, body: 503 if path == '/acl' else None
        owner.close()
        self.delivery(watcher, 'will/device', b'offline', 1)
        self.barrier(watcher)
        self.assertEqual(len(self.service.observations('/acl')), calls)
        self.service.policy = None
        pub = self.client('retained-owner')
        self.publish(pub, 'retained/value', b'stored', 1, retain=True)
        fresh = self.client('retained-reader')
        self.subscribe(fresh, 'retained/#', qos=1)
        self.delivery(fresh, 'retained/value', b'stored', 1)
        self.barrier(fresh)
        self.assertEqual(len(self.service.observations('/acl', client_id='retained-reader')), 1)
        self.assertEqual(self.service.observations('/acl', topic='retained/value', action='subscribe'), [])

    def test_acl_only_local_accounts_reject_fresh_anonymous_and_keep_connect_rules(self):
        local = auth_acl_blackbox.policy()
        local['allow_anonymous'] = True
        policy_path = Path(self.directory.name) / 'local-policy.json'
        policy_path.write_text(json.dumps(local))
        self.start(ttl=0, extra=['--auth-policy', str(policy_path)])
        config = self.request('/config')[2]
        config['authentication']['enabled'] = False
        self.replace(config)
        for version in (4, 5):
            anonymous = self.raw()
            packet = (connect5 if version == 5 else connect311)(
                f'fresh-anonymous-{version}', username=None, password=None)
            anonymous.sendall(packet)
            self.connack(anonymous, version, code=5 if version == 4 else 0x87)
            self.assertEqual(anonymous.recv(1), b'')
            disallowed_id = self.begin(f'wrong-prefix-{version}', username='prefixed',
                password=auth_acl_blackbox.PASSWORD, version=version)
            self.connack(disallowed_id, version, code=5 if version == 4 else 0x87)
            self.assertEqual(disallowed_id.recv(1), b'')
            named = self.client(f'owned-local-{version}', username='prefixed',
                password=auth_acl_blackbox.PASSWORD, version=version)
            self.publish(named, f'remote-granted/{version}', b'named-only', 1, version=version)
            self.assertEqual(len(self.service.observations('/acl', client_id=f'owned-local-{version}')), 1)
        self.assertEqual(self.service.observations('/authenticate'), [])
        self.assertEqual(self.service.observations('/acl', client_id='fresh-anonymous-4'), [])
        self.assertEqual(self.service.observations('/acl', client_id='fresh-anonymous-5'), [])

    def test_will_admission_rejects_before_connect_without_takeover_or_publish(self):
        self.start(ttl=0)
        watcher = self.client('protected-will-watcher')
        self.subscribe(watcher, 'protected-will/#', qos=1)
        for version in (4, 5):
            name = f'protected-will-owner-{version}'
            topic = f'protected-will/{version}'
            original = self.client(name, username='will-device', persistent=True, version=version)
            for status in (401, 403, 503):
                with self.subTest(version=version, status=status):
                    self.service.policy = lambda path, body, status=status: status if path == '/acl' and body['action'] == 'publish' else None
                    before = len(self.service.observations('/acl', client_id=name))
                    failed = self.begin(name, username='will-device',
                        password=b'wrong' if status == 401 else PASSWORD,
                        version=version, persistent=True,
                        will=(topic, b'never-publish', 1, True))
                    code = ({401: 4, 403: 5, 503: 3} if version == 4 else
                            {401: 0x86, 403: 0x87, 503: 0x88})[status]
                    self.connack(failed, version, code=code)
                    self.assertEqual(failed.recv(1), b'')
                    self.barrier(original)
                    self.assertTrue(self.session(name)['online'])
                    self.quiet(watcher)
                    will_calls = self.service.observations('/acl', client_id=name)
                    self.assertEqual(len(will_calls), before + (status != 401))
                    if status != 401:
                        self.assertEqual(will_calls[-1]['body'], dict(client_id=name,
                            username='will-device', action='publish', topic=topic))
                    self.assertEqual(self.broker.data('/api/v1/retained?search=protected-will%2F')['total'], 0)
            self.service.policy = None
            self.disconnect(original)

    def test_existing_queue_survives_unavailable_service_and_restart_without_remote_recovery(self):
        # Old Phase6a quarantined delivery. Accepted subscriptions now retain grants.
        self.start(ttl=0)
        sub = self.client('queue-target', persistent=True)
        self.subscribe(sub, 'queue/#', qos=2)
        self.disconnect(sub)
        pub = self.client('queue-source')
        self.publish(pub, 'queue/value', b'durable-accepted', 2)
        eventually(lambda: self.pending('queue-target') == 1)
        calls = len(self.service.observations('/acl'))
        self.service.policy = lambda path, body: 503 if path == '/acl' else None
        all_calls = len(self.service.observations())
        self.broker.stop(crash=True)
        self.broker.start()
        self.assertEqual(len(self.service.observations()), all_calls)
        self.assertEqual(self.pending('queue-target'), 1)
        self.assertEqual(len(self.service.observations('/acl')), calls)
        final = self.client('queue-target', persistent=True, present=True)
        self.delivery(final, 'queue/value', b'durable-accepted', 2)
        self.barrier(final)
        eventually(lambda: self.pending('queue-target') == 0)
        self.assertEqual(len(self.service.observations('/acl')), calls)

    def test_shared_filter_denial_excludes_only_unadmitted_member(self):
        self.start(ttl=0)
        self.service.policy = lambda path, body: 403 if path == '/acl' and body['client_id'] == 'a-denied' and body['action'] == 'subscribe' else None
        denied = self.client('a-denied')
        allowed = self.client('z-allowed')
        self.subscribe(denied, '$share/workers/shared/#', qos=2, code=0x87)
        self.subscribe(allowed, '$share/workers/shared/#', qos=2)
        pub = self.client('shared-source')
        for qos in range(3):
            self.publish(pub, 'shared/value', bytes([qos]), qos)
            self.delivery(allowed, 'shared/value', bytes([qos]), qos)
            self.quiet(denied)
        for client in ('z-allowed', 'a-denied'):
            calls = self.service.observations('/acl', client_id=client)
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]['body'], dict(client_id=client,
                username=client, action='subscribe', topic='shared/#'))

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

    def test_accepted_qos2_duplicates_and_shared_fanout_ignore_later_http_failure(self):
        # The old Phase6a shared preflight delayed PUBCOMP. Phase6b grants the
        # original PUBLISH once; PUBREL, DUP and recipient ACKs reuse acceptance.
        self.start(ttl=0)
        ordinary = self.client('ordinary-target', persistent=True)
        shared = self.client('shared-target', persistent=True)
        self.subscribe(ordinary, 'partial/#', qos=2)
        self.subscribe(shared, '$share/group/partial/#', qos=2)
        source = self.client('partial-source', persistent=True)
        source.sendall(publication5('partial/value', b'once-per-recipient', 2, 77))
        parse_ack5(receive(source), 0x50, 77)
        calls = len(self.service.observations('/acl'))
        self.service.policy = lambda path, body: 503 if path == '/acl' else None
        source.sendall(publication5('partial/value', b'once-per-recipient', 2, 77, dup=True))
        parse_ack5(receive(source), 0x50, 77)
        source.sendall(ack5(0x62, 77))
        parse_ack5(receive(source), 0x70, 77)
        self.delivery(ordinary, 'partial/value', b'once-per-recipient', 2)
        self.delivery(shared, 'partial/value', b'once-per-recipient', 2)
        source.sendall(ack5(0x62, 77))
        parse_ack5(receive(source), 0x70, 77, reason=0x92)
        self.barrier(source)
        self.barrier(ordinary)
        self.barrier(shared)
        self.quiet(ordinary)
        self.quiet(shared)
        self.assertEqual(len(self.service.observations('/acl')), calls)
        self.assertEqual(len(self.service.observations('/acl', client_id='partial-source')), 1)


if __name__ == '__main__':
    unittest.main()
