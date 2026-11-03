#!/usr/bin/env python3
"""Real MQTT311/5 client lifecycle topics and SQL→Webhook acceptance.

Run with RMQTT_BIN=/absolute/path/rmqtt python3 tests/system_client_events_blackbox.py -v.
Every test owns its process, loopback ports and SQLite configuration. Publication
receipt/API replies are barriers; bounded silence only checks unwanted outputs.
Event admission is best effort, not a crash-complete audit log or ACK transaction.
"""
import json
from pathlib import Path
import queue
import socket
import struct
import tempfile
import time
import unittest

from blackbox import frame, receive, string
from auth_acl_blackbox import PASSWORD, connect_packet as auth311, policy
from dashboard_blackbox import DashboardBroker
from mqtt5_blackbox import connect5, parse5, props, publication5, readprops
from qos_blackbox import parse_publish
from retained_will_blackbox import connect_packet as connect311, publication
from rules_webhook_blackbox import CONFIG, Webhook, config_for

CONNECTED = '$SYS/events/client/connected'
DISCONNECTED = '$SYS/events/client/disconnected'
SYSTEM = '$SYS/events/client/+'


class LifecycleCases:
    version = 5

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-lifecycle-wire-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.broker = None

    def start(self, authenticated=False):
        args = ['--rules-config', str(self.root / 'rules.sqlite'),
                '--access-control-config', str(self.root / 'access.sqlite')]
        if authenticated:
            policy_path = self.root / 'policy.json'
            policy_path.write_text(json.dumps(policy()))
            args += ['--auth-policy', str(policy_path)]
        self.broker = DashboardBroker(self.directory.name, sqlite=True, extra=args)
        self.addCleanup(self.broker.stop)
        self.broker.start()

    def raw(self):
        sock = socket.create_connection(('127.0.0.1', self.broker.port), timeout=5)
        self.addCleanup(sock.close)
        return sock

    def packet(self, name, keepalive=30, user=None, password=None, will=None, clean=True):
        if self.version == 5:
            return connect5(name, keepalive=keepalive, username=user, clean=clean,
                            properties=[] if clean else [(0x11, 3600)],
                            password=password, will=will)
        if user is not None:
            return auth311(name, username=user, password=password, will=will, clean=clean)
        return connect311(name, keepalive=keepalive, will=will, clean=clean)

    def client(self, name, present=False, **kwargs):
        sock = self.raw()
        sock.sendall(self.packet(name, **kwargs))
        header, body = receive(sock)
        self.assertEqual(header, 0x20)
        self.assertEqual(body[:2], bytes([int(present), 0]))
        if self.version == 5:
            _, end = readprops(body, 2)
            self.assertEqual(end, len(body))
        return sock

    def subscribe(self, sock, topic=SYSTEM, qos=1, identifier=1, no_local=False):
        body = struct.pack('!H', identifier)
        if self.version == 5:
            body += props()
        sock.sendall(frame(0x82, body + string(topic) + bytes([qos | (4 if no_local else 0)])))
        # Own connected event can be dispatched after SUBSCRIBE is processed.
        while True:
            header, body = receive(sock)
            if header >> 4 == 3:
                self.decode_event(sock, (header, body))
                continue
            self.assertEqual(header, 0x90)
            self.assertEqual(body[:2], struct.pack('!H', identifier))
            offset = readprops(body, 2)[1] if self.version == 5 else 2
            self.assertEqual(body[offset:], bytes([qos]))
            return

    def observer(self, **kwargs):
        sock = self.client('observer', **kwargs)
        self.subscribe(sock)
        return sock

    def decode_event(self, sock, packet):
        parsed = parse5(packet) if self.version == 5 else parse_publish(packet)
        topic, payload, qos, identifier, dup, retain = parsed[:6]
        self.assertIn(topic, (CONNECTED, DISCONNECTED))
        self.assertEqual(qos, 1)
        self.assertFalse(retain)
        if qos:
            sock.sendall(frame(0x40, struct.pack('!H', identifier)))
        event = json.loads(payload)
        for field in ('event_id', 'event', 'timestamp_ms', 'boot_id', 'sequence',
                      'username', 'client_id', 'connection_id', 'protocol'):
            self.assertIn(field, event)
        self.assertNotIn('schema_version', event)
        self.assertNotIn('password', event)
        self.assertNotIn(PASSWORD.decode(), payload.decode())
        self.assertEqual(event['event'], 'client.' + topic.rsplit('/', 1)[1])
        self.assertEqual(event['protocol'], '5.0' if self.version == 5 else '3.1.1')
        self.assertIsInstance(event['timestamp_ms'], int)
        self.assertIsInstance(event['sequence'], int)
        self.assertGreater(event['sequence'], 0)
        for key in ('event_id', 'boot_id', 'connection_id'):
            self.assertIsInstance(event[key], str)
            self.assertTrue(event[key])
        return event

    def event(self, sock, client, kind, timeout=5):
        deadline = time.monotonic() + timeout
        try:
            while True:
                sock.settimeout(max(.01, deadline - time.monotonic()))
                event = self.decode_event(sock, receive(sock))
                if event['client_id'] == client and event['event'] == 'client.' + kind:
                    return event
                self.assertLess(time.monotonic(), deadline, 'expected lifecycle event missing')
        finally:
            sock.settimeout(5)

    def quiet_for(self, sock, client, timeout=.2):
        deadline = time.monotonic() + timeout
        try:
            while time.monotonic() < deadline:
                sock.settimeout(max(.01, deadline - time.monotonic()))
                try:
                    value = self.decode_event(sock, receive(sock))
                except socket.timeout:
                    return
                self.assertNotEqual(value['client_id'], client, value)
        finally:
            sock.settimeout(5)

    def assert_pair(self, connected, disconnected, reason):
        self.assertEqual(disconnected['reason'], reason)
        for field in ('connection_id', 'boot_id', 'client_id', 'username', 'protocol'):
            self.assertEqual(connected[field], disconnected[field])
        self.assertNotEqual(connected['event_id'], disconnected['event_id'])
        self.assertGreater(disconnected['sequence'], connected['sequence'])
        self.assertEqual(disconnected['connected_at_ms'], connected['timestamp_ms'])
        self.assertGreaterEqual(disconnected['duration_ms'], 0)

    def test_connected_normal_disconnected_and_no_retained_replay(self):
        self.start()
        observer = self.observer()
        subject = self.client('subject')
        connected = self.event(observer, 'subject', 'connected')
        self.assertIsNone(connected['username'])
        subject.sendall(frame(0xe0))
        disconnected = self.event(observer, 'subject', 'disconnected')
        self.assert_pair(connected, disconnected, 'normal')
        self.quiet_for(observer, 'subject')
        late = self.client('late')
        self.subscribe(late, DISCONNECTED)
        self.quiet_for(late, 'subject')

    def test_abnormal_close_is_one_network_closed_event(self):
        self.start()
        observer = self.observer()
        subject = self.client('subject')
        connected = self.event(observer, 'subject', 'connected')
        subject.shutdown(socket.SHUT_RDWR)
        subject.close()
        disconnected = self.event(observer, 'subject', 'disconnected')
        self.assert_pair(connected, disconnected, 'network_closed')
        self.quiet_for(observer, 'subject')

    def test_keepalive_timeout(self):
        self.start()
        observer = self.observer()
        self.client('subject', keepalive=1)
        connected = self.event(observer, 'subject', 'connected')
        disconnected = self.event(observer, 'subject', 'disconnected')
        self.assert_pair(connected, disconnected, 'keep_alive_timeout')

    def test_takeover_has_distinct_connections_and_keeps_new_connection_alive(self):
        self.start()
        observer = self.observer()
        old = self.client('subject')
        first = self.event(observer, 'subject', 'connected')
        new = self.client('subject')
        found = {}
        for _ in range(2):
            item = None
            # Cross-connection scheduling may publish the two transition events in either order.
            while item is None:
                candidate = self.decode_event(observer, receive(observer))
                if candidate['client_id'] == 'subject':
                    item = candidate
            found[item['event']] = item
        self.assertEqual(set(found), {'client.connected', 'client.disconnected'})
        self.assert_pair(first, found['client.disconnected'], 'session_taken_over')
        self.assertNotEqual(first['connection_id'], found['client.connected']['connection_id'])
        old.close()
        new.sendall(frame(0xc0))
        self.assertEqual(receive(new), (0xd0, b''))
        new.sendall(frame(0xe0))
        disconnected = self.event(observer, 'subject', 'disconnected')
        self.assert_pair(found['client.connected'], disconnected, 'normal')
        self.quiet_for(observer, 'subject')

    def test_persistent_subscriber_recovers_offline_system_delivery(self):
        self.start()
        observer = self.client('observer', clean=False)
        self.subscribe(observer)
        observer.sendall(frame(0xe0))
        observer.close()
        subject = self.client('subject')
        resumed = self.client('observer', clean=False, present=True)
        connected = self.event(resumed, 'subject', 'connected')
        subject.sendall(frame(0xe0))
        self.assert_pair(connected, self.event(resumed, 'subject', 'disconnected'), 'normal')

    def test_restart_uses_new_boot_without_synthetic_offline_replay(self):
        self.start()
        observer = self.observer()
        subject = self.client('subject')
        connected = self.event(observer, 'subject', 'connected')
        self.broker.stop(crash=True)
        subject.close()
        observer.close()
        self.broker.start()
        observer = self.observer()
        self.quiet_for(observer, 'subject')
        subject = self.client('subject')
        restored = self.event(observer, 'subject', 'connected')
        self.assertNotEqual(restored['boot_id'], connected['boot_id'])
        self.assertNotEqual(restored['connection_id'], connected['connection_id'])
        subject.sendall(frame(0xe0))
        self.assert_pair(restored, self.event(observer, 'subject', 'disconnected'), 'normal')

    def test_root_wildcard_excludes_system_events(self):
        self.start()
        root = self.client('root-observer')
        self.subscribe(root, '#')
        observer = self.observer()
        subject = self.client('subject')
        self.event(observer, 'subject', 'connected')
        subject.sendall(frame(0xe0))
        self.event(observer, 'subject', 'disconnected')
        root.settimeout(.2)
        with self.assertRaises(socket.timeout):
            receive(root)

    def test_authenticated_username_and_refused_login_no_event(self):
        self.start(authenticated=True)
        observer = self.observer(user='admin', password=PASSWORD)
        subject = self.client('subject', user='admin', password=PASSWORD)
        connected = self.event(observer, 'subject', 'connected')
        self.assertEqual(connected['username'], 'admin')
        bad = self.raw()
        bad.sendall(self.packet('refused', user='admin', password=b'wrong-password'))
        header, body = receive(bad)
        self.assertEqual(header, 0x20)
        self.assertNotEqual(body[1], 0)
        self.quiet_for(observer, 'refused')
        subject.sendall(frame(0xe0))
        self.assert_pair(connected, self.event(observer, 'subject', 'disconnected'), 'normal')

    def test_client_cannot_forge_system_publication_or_will(self):
        self.start(authenticated=True)
        observer = self.observer(user='admin', password=PASSWORD)
        for index, topic in enumerate(('$SYS', CONNECTED, '$SYS/unrelated')):
            with self.subTest(topic=topic):
                name = f'forger-{index}'
                subject = self.client(name, user='admin', password=PASSWORD)
                self.event(observer, name, 'connected')
                payload = b'{"forged":true}'
                packet = (publication5(topic, payload, 1, 77, retain=True)
                          if self.version == 5 else publication(topic, payload, 1, 77, True))
                subject.sendall(packet)
                try:
                    header, body = receive(subject)
                    self.assertEqual(self.version, 5)
                    if header == 0x40:
                        self.assertEqual(body[:2], b'\x00M')
                        self.assertEqual(body[2], 0x87)
                    else:
                        self.assertEqual(header, 0xe0)
                        self.assertEqual(body[0], 0x87)
                except (EOFError, ConnectionError):
                    pass
                willer = self.raw()
                willer.sendall(self.packet(f'bad-will-{index}', user='admin', password=PASSWORD,
                                           will=(topic, payload, 1, True)))
                header, body = receive(willer)
                self.assertEqual(header, 0x20)
                self.assertEqual(body[1], 0x87 if self.version == 5 else 5)
                self.quiet_for(observer, f'bad-will-{index}')

    def configure_webhook(self, statuses=(204,), **delivery):
        webhook = Webhook(statuses=statuses)
        self.addCleanup(webhook.close)
        sql = ('SELECT client_id, payload.event AS event, payload.username AS username, '
               'payload.connection_id AS connection_id, payload.reason AS reason '
               'FROM "$SYS/events/client/+" WHERE payload.client_id = \'subject\'')
        value = config_for(webhook.url, sql=sql, batch_wait_ms=0, **delivery)
        code, _, result = self.broker.request(CONFIG, method='PUT',
                                             body=json.dumps(value),
                                             headers={'Content-Type': 'application/json'})
        self.assertEqual(code, 200, result)
        return webhook

    def webhook_event(self, webhook, kind):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            _, _, body, _ = webhook.get(timeout=max(.01, deadline - time.monotonic()))
            for event in body['events']:
                if event['data']['event'] == 'client.' + kind:
                    self.assertEqual(event['client_id'], 'subject')
                    self.assertEqual(event['data']['client_id'], 'subject')
                    return event
        self.fail('expected Webhook lifecycle event missing')

    def test_same_real_publication_reaches_sql_webhook_and_mqtt(self):
        self.start()
        webhook = self.configure_webhook()
        observer = self.observer()
        subject = self.client('subject')
        connected = self.event(observer, 'subject', 'connected')
        outbound = self.webhook_event(webhook, 'connected')
        self.assertEqual(outbound['data']['connection_id'], connected['connection_id'])
        subject.sendall(frame(0xe0))
        disconnected = self.event(observer, 'subject', 'disconnected')
        outbound = self.webhook_event(webhook, 'disconnected')
        self.assertEqual(outbound['data']['connection_id'], disconnected['connection_id'])
        self.assertEqual(outbound['data']['reason'], 'normal')

    def test_unavailable_webhook_shutdown_is_bounded_and_replays_durable_events(self):
        self.start()
        webhook = self.configure_webhook(statuses=(503,), max_attempts=5, retry_base_ms=1000)
        self.client('subject')
        connected = self.webhook_event(webhook, 'connected')
        started = time.monotonic()
        self.broker.stop()
        self.assertLess(time.monotonic() - started, 8, 'shutdown must not wait indefinitely for HTTP')
        # Clear old failed-attempt observations, then recover the same durable
        # queue/configuration against an available destination.
        while True:
            try:
                webhook.records.get_nowait()
            except queue.Empty:
                break
        with webhook.lock:
            webhook.statuses.clear()
            webhook.statuses.append(204)
        self.broker.start()
        replay = self.webhook_event(webhook, 'connected')
        self.assertEqual(replay['event_id'], connected['event_id'])
        disconnected = self.webhook_event(webhook, 'disconnected')
        self.assertEqual(disconnected['data']['reason'], 'server_shutdown')
        self.assertEqual(disconnected['data']['connection_id'], connected['data']['connection_id'])

    def test_graceful_shutdown_feeds_existing_webhook_queue(self):
        self.start()
        webhook = self.configure_webhook()
        self.client('subject')
        connected = self.webhook_event(webhook, 'connected')
        self.broker.stop()
        disconnected = self.webhook_event(webhook, 'disconnected')
        self.assertEqual(disconnected['data']['reason'], 'server_shutdown')
        self.assertEqual(connected['data']['connection_id'], disconnected['data']['connection_id'])


class SystemClientEvents311(LifecycleCases, unittest.TestCase):
    version = 4


class SystemClientEvents5(LifecycleCases, unittest.TestCase):
    version = 5

    def test_no_local_keeps_broker_originated_events_for_affected_client(self):
        self.start()
        observer = self.client('subject', clean=False)
        self.subscribe(observer, CONNECTED, no_local=True)
        observer.sendall(frame(0xe0))
        observer.close()
        resumed = self.client('subject', clean=False, present=True)
        # Rule metadata identifies the affected client; MQTT publication origin
        # is still the broker, so No Local must not suppress this notification.
        connected = self.event(resumed, 'subject', 'connected')
        self.assertEqual(connected['client_id'], 'subject')
        resumed.sendall(frame(0xe0))


if __name__ == '__main__':
    unittest.main()
