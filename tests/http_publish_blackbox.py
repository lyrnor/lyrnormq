#!/usr/bin/env python3
"""Real Basic HTTP->MQTT311/5 publishing and auth-cache acceptance.

RMQTT_BIN=/absolute/path/rmqtt python3 tests/http_publish_blackbox.py -v
Disposable loopback processes/configs only. Raw MQTT ACKs and committed HTTP
responses establish barriers; bounded silence checks unwanted effects only.
"""
import base64
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
from pathlib import Path
import select
import socket
import struct
import tempfile
import time
import unittest

from blackbox import frame, receive, string
import dashboard_blackbox as dashboard
from dashboard_blackbox import DashboardBroker, TOKEN
from http_access_fixture import AccessService, PASSWORD
from mqtt5_blackbox import connect5, props, readprops
from auth_acl_blackbox import connect_packet as connect311
from rules_webhook_blackbox import CONFIG as RULES, Webhook, config_for

PUBLISH = '/api/v1/mqtt/publish'
ACCESS = '/api/v1/access-control'


def basic(username='backend', password=PASSWORD, scheme='Basic'):
    return scheme + ' ' + base64.b64encode(username.encode() + b':' + password).decode()


class HTTPPublishFixture:
    version = 5
    delivery = dashboard.DashboardBlackbox.delivery

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-http-publish-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.service = AccessService()
        self.addCleanup(self.service.close)
        self.broker = None

    def start(self, extra=(), configured=True, auth_ttl=None, acl_ttl=30):
        args = ['--rules-config', str(self.root / 'rules.sqlite'),
                '--access-control-config', str(self.root / 'access.sqlite'),
                '--auth-rate-per-second', '0', *extra]
        if auth_ttl is not None:
            args += ['--http-publish-auth-cache-ttl-seconds', str(auth_ttl)]
        self.broker = DashboardBroker(self.directory.name, sqlite=True, extra=args)
        self.addCleanup(self.broker.stop)
        self.broker.start()
        if configured:
            self.replace(self.service.configuration(ttl=acl_ttl))

    def replace(self, value):
        code, _, data = self.broker.request(ACCESS + '/config', method='PUT',
            headers={'Content-Type': 'application/json'}, body=json.dumps(value))
        self.assertEqual(code, 200, data)
        return data

    def clear(self):
        code, _, data = self.broker.request(ACCESS + '/cache/clear', method='POST',
            headers={'Content-Type': 'application/json'}, body='{}')
        self.assertEqual(code, 200, data)

    def request(self, value=None, authorization=None, method='POST', raw=None):
        headers = {'Content-Type': 'application/json'}
        if authorization is not None:
            headers['Authorization'] = authorization
        return self.broker.request(PUBLISH, token=None, method=method, headers=headers,
            body=raw if raw is not None else json.dumps(value or {'topic': 'commands/device', 'payload': 'command'}))

    def accepted(self, value=None, authorization=None):
        response = self.request(value, basic() if authorization is None else authorization)
        self.assertEqual(response[0], 202, response[2])
        self.assertEqual(response[2], {'accepted': True})
        return response

    def client(self, name, persistent=False, present=False):
        sock = socket.create_connection(('127.0.0.1', self.broker.port), timeout=5)
        sock.settimeout(5)
        self.addCleanup(sock.close)
        packet = (connect5(name, clean=not persistent, username='device', password=PASSWORD,
                  properties=[(0x11, 3600)] if persistent else []) if self.version == 5
                  else connect311(name, clean=not persistent, username='device', password=PASSWORD))
        sock.sendall(packet)
        header, body = receive(sock)
        self.assertEqual(header, 0x20)
        self.assertEqual(body[:2], bytes([int(present), 0]))
        if self.version == 5:
            self.assertEqual(readprops(body, 2)[1], len(body))
        return sock

    def subscribe(self, sock, topic, qos=2, no_local=False):
        data = b'\x00\x01' + (props() if self.version == 5 else b'')
        sock.sendall(frame(0x82, data + string(topic) + bytes([qos | (4 if no_local else 0)])))
        header, data = receive(sock)
        self.assertEqual(header, 0x90)
        offset = readprops(data, 2)[1] if self.version == 5 else 2
        self.assertEqual(data[offset:], bytes([qos]))

    def close(self, sock):
        sock.sendall(frame(0xe0))
        self.assertEqual(sock.recv(1), b'')
        sock.close()

    def quiet(self, sock, timeout=.15):
        self.assertFalse(select.select([sock], [], [], timeout)[0], 'unexpected MQTT data')

    def http_auth_calls(self):
        return self.service.observations('/authenticate', client_id='', username='backend')

    def http_acl_calls(self):
        return self.service.observations('/acl', client_id='', username='backend', action='publish')


class PublishWireCases(HTTPPublishFixture):
    def test_qos_zero_one_two_and_default_utf8(self):
        self.start()
        device = self.client('device')
        self.subscribe(device, 'commands/#')
        for qos in (0, 1, 2):
            with self.subTest(qos=qos):
                payload = f'指令-{qos}'
                self.accepted({'topic': f'commands/{qos}', 'payload': payload, 'qos': qos})
                self.delivery(device, f'commands/{qos}', payload.encode(), qos, self.version)
        self.accepted({'topic': 'commands/default', 'payload': 'default'})
        self.delivery(device, 'commands/default', b'default', 1, self.version)
        self.assertEqual(len(self.http_auth_calls()), 1)
        self.assertEqual(len(self.http_acl_calls()), 4)
        self.assertEqual(self.service.failures, [])

    def test_retained_update_delete_and_replay(self):
        self.start()
        self.accepted({'topic': 'state/device', 'payload': 'first', 'qos': 2, 'retain': True})
        self.accepted({'topic': 'state/device', 'payload': 'latest', 'retain': True})
        device = self.client('device')
        self.subscribe(device, 'state/device')
        value = self.delivery(device, 'state/device', b'latest', 1, self.version)
        self.assertTrue(value[5])
        self.accepted({'topic': 'state/device', 'payload': '', 'retain': True})
        self.delivery(device, 'state/device', b'', 1, self.version)
        later = self.client('later')
        self.subscribe(later, 'state/device')
        self.quiet(later)
        self.assertEqual(self.broker.data('/api/v1/retained')['total'], 0)

    def test_offline_qos2_survives_broker_crash_and_resume(self):
        self.start()
        device = self.client('durable-device', persistent=True)
        self.subscribe(device, 'commands/durable')
        self.close(device)
        self.accepted({'topic': 'commands/durable', 'payload': 'durable', 'qos': 2})
        self.broker.stop(crash=True)
        self.broker.start()
        resumed = self.client('durable-device', persistent=True, present=True)
        self.delivery(resumed, 'commands/durable', b'durable', 2, self.version)
        self.close(resumed)

    def test_shared_subscription_and_no_local_use_broker_origin(self):
        self.start()
        # Collide with observer source metadata: it must not become MQTT origin.
        device = self.client('http:backend')
        self.subscribe(device, 'commands/local', no_local=self.version == 5)
        self.accepted({'topic': 'commands/local', 'payload': 'broker'})
        self.delivery(device, 'commands/local', b'broker', 1, self.version)
        first, second = self.client('shared-1'), self.client('shared-2')
        self.subscribe(first, '$share/workers/commands/shared')
        self.subscribe(second, '$share/workers/commands/shared')
        self.accepted({'topic': 'commands/shared', 'payload': 'one-copy'})
        ready = select.select([first, second], [], [], 3)[0]
        self.assertEqual(len(ready), 1)
        self.delivery(ready[0], 'commands/shared', b'one-copy', 1, self.version)
        self.quiet(second if ready[0] is first else first)

    def test_no_synthetic_mqtt_session_or_lifecycle_event(self):
        self.start()
        # No MQTT client exists, hence any session would have been synthesized by HTTP.
        self.assertEqual(self.broker.data('/api/v1/sessions')['total'], 0)
        self.accepted()
        self.assertEqual(self.broker.data('/api/v1/sessions')['total'], 0)
        self.assertEqual(self.http_auth_calls()[0]['body']['client_id'], '')
        self.assertEqual(self.http_acl_calls()[0]['body']['client_id'], '')
        observer = self.client('observer')
        self.subscribe(observer, '$SYS/events/client/disconnected', qos=0)
        self.accepted()
        self.quiet(observer)
        self.assertEqual(self.broker.data('/api/v1/sessions')['total'], 1)

    def test_topic_acl_denial_and_reserved_namespace_have_no_effect(self):
        self.start()
        device = self.client('device')
        self.subscribe(device, 'commands/#')
        self.service.policy = lambda path, body: 403 if path == '/acl' and body['topic'] == 'commands/denied' else None
        denied = self.request({'topic': 'commands/denied', 'payload': 'forbidden', 'retain': True}, basic())
        self.assertEqual(denied[0], 403, denied)
        for topic in ('$SYS', '$SYS/events/client/connected', '$SYS/private'):
            response = self.request({'topic': topic, 'payload': 'forged', 'retain': True}, basic())
            self.assertEqual(response[0], 403, response)
        self.assertEqual(self.broker.data('/api/v1/retained')['total'], 0)
        self.quiet(device)
        self.accepted({'topic': '$SYSfoo/allowed', 'payload': 'ordinary-dollar-topic'})

    def test_sql_webhook_reuses_service_identity(self):
        self.start()
        webhook = Webhook()
        self.addCleanup(webhook.close)
        config = config_for(webhook.url, sql='SELECT client_id, payload.action AS action FROM "commands/#"', batch_wait_ms=0)
        code, _, result = self.broker.request(RULES, method='PUT', headers={'Content-Type': 'application/json'}, body=json.dumps(config))
        self.assertEqual(code, 200, result)
        self.accepted({'topic': 'commands/device', 'payload': '{"action":"restart"}'})
        _, _, body, _ = webhook.get()
        self.assertEqual(body['events'][0]['client_id'], 'http:backend')
        self.assertEqual(body['events'][0]['data'], {'client_id': 'http:backend', 'action': 'restart'})


class HTTPPublish311(PublishWireCases, unittest.TestCase):
    version = 4


class HTTPPublish5(PublishWireCases, unittest.TestCase):
    version = 5


class HTTPPublishSecurityCache(HTTPPublishFixture, unittest.TestCase):
    def test_basic_is_required_and_management_bearer_is_independent(self):
        self.start()
        for header in (None, 'Bearer ' + TOKEN, 'Basic !!!', 'Basic ' + base64.b64encode(b'backend').decode(),
                       basic('', PASSWORD), basic('backend', b'wrong')):
            with self.subTest(header_type=None if header is None else header.split(' ')[0]):
                response = self.request(authorization=header)
                self.assertEqual(response[0], 401, response)
                self.assertTrue(response[1].get('www-authenticate', '').startswith('Basic'))
        self.accepted(authorization=basic(scheme='bAsIc'))
        code, _, _ = self.broker.request('/api/v1/overview', token=None, headers={'Authorization': basic()})
        self.assertEqual(code, 401)
        self.assertEqual(self.broker.request('/api/v1/overview')[0], 200)
        self.assertNotIn(PASSWORD.decode(), self.broker.logs())

    def test_duplicate_basic_header_rejected(self):
        self.start()
        conn = http.client.HTTPConnection('127.0.0.1', self.broker.admin, timeout=5)
        try:
            body = json.dumps({'topic': 'commands/device', 'payload': 'ignored'}).encode()
            conn.putrequest('POST', PUBLISH)
            conn.putheader('Content-Type', 'application/json')
            conn.putheader('Authorization', basic())
            conn.putheader('Authorization', basic())
            conn.putheader('Content-Length', str(len(body)))
            conn.endheaders(body)
            response = conn.getresponse()
            self.assertEqual(response.status, 401, response.read())
        finally:
            conn.close()
        self.assertEqual(self.http_auth_calls(), [])

    def test_strict_body_topic_and_size_validation(self):
        self.start()
        invalid = [{}, {'topic': '', 'payload': 'x'}, {'topic': 'a/+', 'payload': 'x'},
                   {'topic': 'a/#', 'payload': 'x'}, {'topic': 'a\0b', 'payload': 'x'},
                   {'topic': 'a', 'payload': {}}, {'topic': 'a', 'payload': 'x', 'qos': 3},
                   {'topic': 'a', 'payload': 'x', 'qos': True},
                   {'topic': 'a', 'payload': 'x', 'retain': 1},
                   {'topic': 'a', 'payload': 'x', 'username': 'device'}]
        for value in invalid:
            response = self.request(authorization=basic(), raw=json.dumps(value))
            self.assertEqual(response[0], 400, (value, response))
        self.assertEqual(self.request(authorization=basic(), raw='not JSON')[0], 400)
        self.assertEqual(self.request(authorization=basic(), method='GET')[0], 405)
        self.assertEqual(self.request(authorization=basic(), raw=json.dumps({'topic': 'a', 'payload': 'x' * (1024 * 1024)}))[0], 413)

    def test_actual_mqtt_packet_limit(self):
        self.start(extra=['--max-packet-size', '128'])
        self.assertEqual(self.request({'topic': 'commands/device', 'payload': 'x' * 200}, basic())[0], 413)
        self.accepted({'topic': 'commands/device', 'payload': 'small'})

    def test_unconfigured_anonymous_policy_cannot_authenticate_basic(self):
        self.start(configured=False)
        self.assertEqual(self.request(authorization=basic())[0], 401)
        self.assertEqual(self.broker.data('/api/v1/sessions')['total'], 0)

    def test_success_cache_reuses_auth_and_acl_but_wrong_password_never_hits(self):
        self.start()
        for _ in range(4):
            self.accepted()
        self.assertEqual(len(self.http_auth_calls()), 1)
        self.assertEqual(len(self.http_acl_calls()), 1)
        for _ in range(2):
            self.assertEqual(self.request(authorization=basic(password=b'incorrect'))[0], 401)
        self.assertEqual(len(self.http_auth_calls()), 3)
        self.accepted()
        self.assertEqual(len(self.http_auth_calls()), 3)

    def test_manual_clear_and_configuration_save_invalidate_auth_cache(self):
        self.start()
        self.accepted()
        self.clear()
        self.accepted()
        self.assertEqual(len(self.http_auth_calls()), 2)
        self.service.auth_status = 401
        self.replace(self.service.configuration())
        self.assertEqual(self.request(authorization=basic())[0], 401)
        self.assertEqual(len(self.http_auth_calls()), 3)
        self.service.auth_status = None
        self.accepted()
        self.assertEqual(len(self.http_auth_calls()), 4)

    def test_ttl_zero_disables_completed_auth_cache_but_reuses_acl(self):
        self.start(auth_ttl=0)
        for _ in range(3):
            self.accepted()
        self.assertEqual(len(self.http_auth_calls()), 3)
        self.assertEqual(len(self.http_acl_calls()), 1)

    def test_expired_auth_cache_does_not_fail_open(self):
        self.start(auth_ttl=1)
        self.accepted()
        self.service.auth_status = 401
        time.sleep(1.1)
        self.assertEqual(self.request(authorization=basic())[0], 401)
        self.assertEqual(len(self.http_auth_calls()), 2)

    def test_backend_auth_and_acl_unavailability_never_cache_denial(self):
        self.start()
        self.service.auth_status = 503
        self.assertEqual(self.request(authorization=basic())[0], 503)
        self.service.auth_status = None
        self.service.policy = lambda path, body: 503 if path == '/acl' else None
        self.assertEqual(self.request(authorization=basic())[0], 503)
        self.service.policy = None
        self.accepted()
        self.assertEqual(len(self.http_acl_calls()), 2)

    def test_same_credentials_concurrent_requests_share_authentication(self):
        self.start()
        self.service.policy = lambda path, body: (204, .15) if path == '/authenticate' else None
        with ThreadPoolExecutor(max_workers=12) as workers:
            results = list(workers.map(lambda _: self.request(authorization=basic())[0], range(12)))
        self.assertEqual(results, [202] * 12)
        self.assertEqual(len(self.http_auth_calls()), 1)
        self.assertEqual(len(self.http_acl_calls()), 1)

    def test_binary_and_colon_passwords_are_exact_basic_bytes(self):
        self.start()
        expected = b'first:second\x00\xff'
        encoded = base64.b64encode(expected).decode()
        self.service.policy = lambda path, body: (204 if body.get('password_base64') == encoded else 401) if path == '/authenticate' else None
        self.accepted(authorization=basic(password=expected))
        self.assertEqual(self.http_auth_calls()[0]['body']['password_base64'], encoded)
        self.assertNotIn(encoded, self.broker.logs())


if __name__ == '__main__':
    unittest.main()
