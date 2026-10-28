#!/usr/bin/env python3
"""Independent MQTT3.1.1 authentication/ACL TCP and process-recovery checks.

Each test owns its policy and DB. RMQTT_TEST_STORAGE_PATH selects SQLite mode;
its value is not shared between tests. Fixed low-cost hashes below are synthetic
fixtures, never recommended production parameters. ACKs/observed committed SQL
state are barriers; bounded silence checks only observe unwanted deliveries.
"""
import json
import os
from pathlib import Path
import socket
import sqlite3
import struct
import tempfile
import time
import unittest

from blackbox import frame, receive, string
from schema_compat import schema3_fixture
from qos_blackbox import ack, parse_publish
from retained_will_blackbox import FeatureBroker, publication

PASSWORD = b'alice-test-password'
PASSWORD_HASH = ('$argon2id$v=19$m=32,t=1,p=1$bXF0dC10ZXN0LXNhbHQtMDE$'
                 '9nKCHYN7W3NFFVjg+4h4JqpV8p5TDVUKGWdEeVvnJYg')
BINARY_PASSWORD = b'binary\x00\xff-password'
BINARY_HASH = ('$argon2id$v=19$m=32,t=1,p=1$bXF0dC10ZXN0LXNhbHQtMDE$'
               '3jAQ5tXiH7FScfhnVfPqAXCCpVD9CWtwv2sAzUemTlQ')


def policy():
    def user(name, publish, subscribe, prefix='', password_hash=PASSWORD_HASH):
        return dict(username=name, password_hash=password_hash, client_id_prefix=prefix,
                    publish=publish, subscribe=subscribe)
    return dict(allow_anonymous=False, users=[
        user('admin', ['#', '$SYS/#'], ['#', '$SYS/#']),
        user('alice', ['allowed/#'], ['allowed/#']),
        user('bob', ['other/#'], ['other/#']),
        user('single', ['allowed/#'], ['allowed/+']),
        user('normal', ['#'], ['#']),
        user('prefixed', ['allowed/#'], ['allowed/#'], prefix='owned-'),
        user('binary', ['allowed/#'], ['allowed/#'], password_hash=BINARY_HASH),
    ])


def connect_packet(client, username='alice', password=PASSWORD, clean=True, will=None):
    flags = 2 if clean else 0
    payload = string(client)
    if will is not None:
        topic, data, qos, retain = will
        flags |= 4 | qos << 3 | (32 if retain else 0)
        payload += string(topic) + struct.pack('!H', len(data)) + data
    if username is not None:
        flags |= 128
        payload += string(username)
    if password is not None:
        flags |= 64
        payload += struct.pack('!H', len(password)) + password
    return frame(0x10, string('MQTT') + bytes([4, flags]) + b'\x00\x1e' + payload)


class AuthBroker(FeatureBroker):
    def __init__(self, directory, sqlite, value=None, extra_args=()):
        super().__init__(directory, sqlite, extra_args)
        self.policy_path = Path(directory) / 'policy.json'
        self.use_policy = True
        self.write_policy(policy() if value is None else value)

    def write_policy(self, value):
        self.policy_path.write_text(json.dumps(value))

    def command(self):
        command = super().command()
        if self.use_policy:
            command += ['--auth-policy', str(self.policy_path)]
        return command

    def snapshot(self):
        with sqlite3.connect(self.db) as db:
            return json.loads(db.execute('SELECT data FROM broker_snapshot WHERE id=1').fetchone()[0])


class AuthAclBlackbox(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-auth-acl-')
        self.addCleanup(self.directory.cleanup)
        self.sqlite = bool(os.environ.get('RMQTT_TEST_STORAGE_PATH'))
        self.broker = AuthBroker(self.directory.name, self.sqlite)
        self.addCleanup(self.broker.stop)

    def start(self, *args):
        self.broker.extra_args = list(args)
        self.broker.start()

    def raw(self):
        sock = self.broker.socket()
        self.addCleanup(sock.close)
        return sock

    def client(self, name, user='alice', password=PASSWORD, clean=True, will=None, present=False):
        sock = self.raw()
        sock.sendall(connect_packet(name, user, password, clean, will))
        self.assertEqual(receive(sock), (0x20, bytes([int(present), 0])))
        return sock

    def closed(self, sock):
        try:
            self.assertEqual(sock.recv(1), b'', 'unexpected packet/ACK before close')
        except (ConnectionResetError, BrokenPipeError):
            pass

    def refused(self, name, code, user='alice', password=PASSWORD, clean=True, will=None,
                pipeline=b''):
        sock = self.raw()
        sock.sendall(connect_packet(name, user, password, clean, will) + pipeline)
        self.assertEqual(receive(sock), (0x20, bytes([0, code])))
        self.closed(sock)
        return sock

    def subscribe(self, sock, filters, codes=None, packet_id=1):
        sock.sendall(frame(0x82, struct.pack('!H', packet_id)
                           + b''.join(string(topic) + bytes([qos]) for topic, qos in filters)))
        if codes is None:
            codes = [qos for _, qos in filters]
        self.assertEqual(receive(sock), (0x90, struct.pack('!H', packet_id) + bytes(codes)))

    def barrier(self, sock):
        sock.sendall(frame(0xc0))
        self.assertEqual(receive(sock), (0xd0, b''))

    def quiet(self, sock):
        sock.settimeout(.15)
        try:
            with self.assertRaises(socket.timeout):
                sock.recv(1)
        finally:
            sock.settimeout(5)

    def disconnect(self, sock):
        sock.sendall(frame(0xe0))
        self.closed(sock)
        sock.close()

    def publish(self, sock, topic, payload, qos=1, packet_id=7, retain=False):
        sock.sendall(publication(topic, payload, qos, packet_id, retain))
        if qos:
            self.assertEqual(receive(sock), (0x40 if qos == 1 else 0x50,
                                            struct.pack('!H', packet_id)))
            if qos == 2:
                sock.sendall(ack(0x62, packet_id))
                self.assertEqual(receive(sock), (0x70, struct.pack('!H', packet_id)))
        else:
            self.barrier(sock)

    def complete(self, sock, message):
        qos, packet_id = message[2:4]
        if qos == 1:
            sock.sendall(ack(0x40, packet_id))
        elif qos == 2:
            sock.sendall(ack(0x50, packet_id))
            self.assertEqual(receive(sock), (0x62, struct.pack('!H', packet_id)))
            sock.sendall(ack(0x70, packet_id))
        self.barrier(sock)

    def delivery(self, sock, topic, payload, qos=0, retain=False, dup=False):
        message = parse_publish(receive(sock))
        self.assertEqual((message[0], message[1], message[2], message[4], message[5]),
                         (topic, payload, qos, dup, retain))
        self.complete(sock, message)
        return message

    def require_sqlite(self):
        if not self.sqlite:
            self.skipTest('SQLite persistent state/recovery only')

    def session(self, client):
        return next(s for s in self.broker.snapshot()['sessions'] if s['client_id'] == client)

    def test_valid_fragmented_and_binary_password_credentials(self):
        self.start()
        fragmented = self.raw()
        for byte in connect_packet('fragmented'):
            fragmented.sendall(bytes([byte]))
        self.assertEqual(receive(fragmented), (0x20, b'\0\0'))
        self.barrier(fragmented)
        binary = self.client('binary-client', user='binary', password=BINARY_PASSWORD)
        self.subscribe(binary, [('allowed/binary', 1)])
        self.publish(fragmented, 'allowed/binary', b'opaque password accepted')
        self.delivery(binary, 'allowed/binary', b'opaque password accepted', 1)

    def test_bad_credentials_disabled_anonymous_and_prefix_refusal_codes(self):
        self.start()
        for name, user, password, code in [
            ('wrong', 'alice', b'wrong password', 4),
            ('unknown', 'nobody', PASSWORD, 4),
            ('missing-password', 'alice', None, 4),
            ('empty-password', 'alice', b'', 4),
            ('empty-username', '', PASSWORD, 4),
            ('anonymous', None, None, 5),
            ('wrong-prefix', 'prefixed', PASSWORD, 5),
        ]:
            with self.subTest(name=name):
                self.refused(name, code, user, password)
        accepted = self.client('owned-correct', user='prefixed')
        self.barrier(accepted)

    def test_default_without_policy_preserves_anonymous_and_refuses_unknown_credentials(self):
        self.broker.use_policy = False
        self.start()
        anonymous = self.client('default', user=None, password=None)
        self.publish(anonymous, '$SYS/open-default', b'previous default behavior', qos=0)
        self.refused('unexpected-credentials', 4)
        self.barrier(anonymous)

    def test_anonymous_policy_uses_separate_prefix_and_acl(self):
        value = policy()
        value.update(allow_anonymous=True, anonymous=dict(client_id_prefix='anon-',
                     publish=['public/#'], subscribe=['public/#']))
        self.broker.write_policy(value)
        self.start()
        anonymous = self.client('anon-allowed', user=None, password=None)
        self.refused('not-anon-prefix', 5, None, None)
        self.subscribe(anonymous, [('public/#', 1), ('#', 0)], [1, 128])
        self.publish(anonymous, 'public/state', b'anonymous scope')
        self.delivery(anonymous, 'public/state', b'anonymous scope', 1)
        anonymous.sendall(publication('allowed/escape', b'denied', 1))
        self.closed(anonymous)

    def test_malformed_credential_flags_and_username_utf8_close_without_connack(self):
        self.start()
        # Password flag without username, invalid UTF8 username, truncated password.
        packets = [connect_packet('no-user', None, PASSWORD),
                   frame(0x10, string('MQTT') + b'\x04\x82\0\x1e' + string('bad-utf8')
                         + b'\0\1\xff'),
                   frame(0x10, string('MQTT') + b'\x04\xc2\0\x1e' + string('truncated')
                         + string('alice') + b'\0\x04ab')]
        packets.append(frame(0x10, string('MQTT') + b'\x04\x82\0\x1e'
                             + string('nul-username') + string('bad\0name')))
        for packet in packets:
            with self.subTest(packet=packet):
                sock = self.raw()
                sock.sendall(packet)
                self.closed(sock)

    def test_rejected_connect_does_not_process_pipeline_or_register_will(self):
        self.start()
        observer = self.client('pipeline-observer', 'admin')
        self.subscribe(observer, [('allowed/#', 0), ('other/#', 0)])
        self.refused('bad-pipeline', 4, password=b'wrong',
                     will=('allowed/rejected-will', b'not accepted', 1, True),
                     pipeline=publication('allowed/escaped', b'not processed', 0, retain=True))
        self.refused('denied-will', 5,
                     will=('other/rejected-will', b'not authorized', 1, True),
                     pipeline=publication('allowed/escaped-2', b'not processed', 0, retain=True))
        self.quiet(observer)
        replay = self.client('pipeline-replay', 'admin')
        self.subscribe(replay, [('#', 0)])
        self.quiet(replay)

    def test_denied_takeover_preserves_live_owner_and_old_will(self):
        self.start()
        observer = self.client('takeover-observer', 'admin')
        self.subscribe(observer, [('allowed/status', 0)])
        owner = self.client('shared-id', will=('allowed/status', b'original Will', 1, False))
        self.refused('shared-id', 4, password=b'wrong')
        self.refused('shared-id', 5, user='bob', clean=True)
        self.refused('shared-id', 5, will=('other/denied-will', b'bad', 1, False))
        self.barrier(owner)
        self.quiet(observer)
        replacement = self.client('shared-id')
        self.closed(owner)
        self.delivery(observer, 'allowed/status', b'original Will')
        self.barrier(replacement)
        self.quiet(observer)

    def test_security_refusal_precedes_unsupported_persistent_mode(self):
        self.start()
        observer = self.client('precedence-observer', 'admin')
        self.subscribe(observer, [('allowed/#', 0), ('other/#', 0)])
        owner = self.client('precedence-shared-id',
                            will=('allowed/precedence-status', b'original active Will', 1, True))
        with self.subTest(rejection='cross-owner nonclean CONNECT'):
            try:
                self.refused('precedence-shared-id', 5, user='bob', clean=False,
                             pipeline=publication('other/precedence-pipeline', b'not processed',
                                                  0, retain=True))
            finally:
                self.barrier(owner)
                self.quiet(observer)
        with self.subTest(rejection='forbidden-Will nonclean CONNECT'):
            try:
                self.refused('precedence-forbidden-will', 5, clean=False,
                             will=('other/precedence-forbidden', b'not registered', 1, True),
                             pipeline=publication('allowed/precedence-pipeline', b'not processed',
                                                  0, retain=True))
            finally:
                self.barrier(owner)
                self.quiet(observer)
        replay = self.client('precedence-replay', 'admin')
        self.subscribe(replay, [('other/#', 0), ('allowed/#', 0)])
        self.quiet(replay)
        # An otherwise authorized persistent CONNECT retains existing mode policy.
        if self.sqlite:
            valid = self.client('precedence-authorized-persistent', clean=False)
            self.disconnect(valid)
        else:
            unsupported = self.raw()
            unsupported.sendall(connect_packet('precedence-authorized-persistent', clean=False))
            self.closed(unsupported)
        self.disconnect(owner)
        self.quiet(observer)

    def test_denied_publish_all_qos_cannot_delete_retained_or_route(self):
        self.start()
        admin = self.client('denial-admin', 'admin')
        for qos in (0, 1, 2):
            with self.subTest(qos=qos):
                topic = f'other/protected-{qos}'
                self.publish(admin, topic, b'preserved retained value', retain=True)
                observer = self.client(f'denial-observer-{qos}', 'admin')
                self.subscribe(observer, [(topic, 0)])
                self.delivery(observer, topic, b'preserved retained value', retain=True)
                attacker = self.client(f'denied-publisher-{qos}')
                attacker.sendall(publication(topic, b'', qos, 17, True))
                self.closed(attacker)
                self.quiet(observer)
                replay = self.client(f'denial-replay-{qos}', 'admin')
                self.subscribe(replay, [(topic, 0)])
                self.delivery(replay, topic, b'preserved retained value', retain=True)

    def test_mixed_subscribe_grants_and_retained_replay_respect_acl(self):
        self.start()
        admin = self.client('mixed-admin', 'admin')
        for topic in ('allowed/state', 'other/state', '$SYS/info'):
            self.publish(admin, topic, topic.encode(), qos=2, retain=True)
        alice = self.client('mixed-alice')
        filters = [('allowed/#', 1), ('#', 2), ('other/#', 0),
                   ('allowed/+/value', 2), ('$SYS/#', 0)]
        self.subscribe(alice, filters, [1, 128, 128, 2, 128])
        self.delivery(alice, 'allowed/state', b'allowed/state', 1, retain=True)
        self.quiet(alice)
        for topic in ('other/live', '$SYS/live'):
            self.publish(admin, topic, b'forbidden live delivery')
        self.publish(admin, 'allowed/live', b'authorized live delivery')
        self.delivery(alice, 'allowed/live', b'authorized live delivery', 1)
        self.quiet(alice)

    def test_filter_coverage_preserves_empty_levels_depth_and_system_isolation(self):
        self.start()
        single = self.client('single-filter', 'single')
        filters = [('allowed/+', 1), ('allowed/#', 0), ('allowed/+/more', 0),
                   ('allowed/', 0), ('allowed', 0), ('+/+', 0)]
        self.subscribe(single, filters, [1, 128, 128, 0, 128, 128])
        normal = self.client('normal-filter', 'normal')
        self.subscribe(normal, [('#', 0), ('$SYS/#', 0)], [0, 128])
        normal.sendall(publication('$SYS/forbidden', b'root wildcard cannot publish system', 1))
        self.closed(normal)
        explicit = self.client('explicit-system', 'admin')
        self.subscribe(explicit, [('$SYS/#', 1)])
        self.publish(explicit, '$SYS/authorized', b'explicit permission')
        self.delivery(explicit, '$SYS/authorized', b'explicit permission', 1)

    def test_unsubscribe_and_control_packets_do_not_require_publish_permission(self):
        self.start()
        alice = self.client('control-client')
        alice.sendall(frame(0xa2, b'\0\x03' + string('other/never-authorized')))
        self.assertEqual(receive(alice), (0xb0, b'\0\x03'))
        # Unknown PUBREL completes without publishing anything.
        alice.sendall(ack(0x62, 390))
        self.assertEqual(receive(alice), (0x70, struct.pack('!H', 390)))
        self.barrier(alice)
        self.disconnect(alice)

    def test_invalid_policy_is_rejected_before_listening_without_secret_output(self):
        invalid = [dict(allow_anonymous=False, users=[], typo=True),
                   dict(allow_anonymous=False, users=[dict(username='alice',
                        password='do-not-log-policy-secret')]),
                   dict(allow_anonymous=False, users=[dict(username='alice',
                        password_hash='do-not-log-policy-secret', client_id_prefix='',
                        publish=['#'], subscribe=['#'])])]
        for value in invalid:
            with self.subTest(value=value):
                self.broker.write_policy(value)
                error = self.broker.assert_start_fails()
                self.assertNotIn('do-not-log-policy-secret', error)
        self.broker.policy_path.write_bytes(b' ' * (1024 * 1024 + 1))
        self.broker.assert_start_fails()

    def test_persistent_owner_cannot_be_stolen_or_clean_reset_and_credentials_not_stored(self):
        self.require_sqlite()
        self.start()
        alice = self.client('durable-owned', clean=False)
        self.subscribe(alice, [('allowed/#', 2)])
        self.disconnect(alice)
        publisher = self.client('durable-writer', 'admin')
        self.publish(publisher, 'allowed/offline-1', b'offline one', qos=1)
        self.publish(publisher, 'allowed/offline-2', b'offline two', qos=2)
        for clean in (False, True):
            self.refused('durable-owned', 5, user='bob', clean=clean)
        snapshot = self.broker.snapshot()
        self.assertEqual(snapshot['schema_version'], 6)
        self.assertEqual(self.session('durable-owned')['owner'], 'user:alice')
        self.assertEqual(len(self.session('durable-owned')['queued']), 2)
        encoded = json.dumps(snapshot).encode()
        for forbidden in (PASSWORD, PASSWORD_HASH.encode(), BINARY_PASSWORD):
            self.assertNotIn(forbidden, encoded)
            for path in (self.broker.db, Path(str(self.broker.db) + '-wal')):
                if path.exists():
                    self.assertNotIn(forbidden, path.read_bytes())
        resumed = self.client('durable-owned', clean=False, present=True)
        # Both PUBLISH frames may be queued before any ACK/PING reaches the worker.
        first, second = [parse_publish(receive(resumed)) for _ in range(2)]
        self.assertEqual((first[0], first[1], first[2], first[4], first[5]),
                         ('allowed/offline-1', b'offline one', 1, False, False))
        self.assertEqual((second[0], second[1], second[2], second[4], second[5]),
                         ('allowed/offline-2', b'offline two', 2, False, False))
        resumed.sendall(ack(0x40, first[3]))
        resumed.sendall(ack(0x50, second[3]))
        self.assertEqual(receive(resumed), (0x62, struct.pack('!H', second[3])))
        resumed.sendall(ack(0x70, second[3]))
        self.barrier(resumed)
        self.disconnect(resumed)
        self.broker.restart_after_crash()
        self.refused('durable-owned', 5, user='bob', clean=True)
        restored = self.client('durable-owned', clean=False, present=True)
        self.barrier(restored)
        self.quiet(restored)

    def test_restart_reconciles_revoked_subscriptions_queued_and_unacked_payloads(self):
        self.require_sqlite()
        self.start('--max-inflight', '2')
        alice = self.client('revoked-reader', clean=False)
        self.subscribe(alice, [('allowed/#', 2)])
        publisher = self.client('revocation-writer', 'admin')
        for qos in (1, 2):
            self.publish(publisher, f'allowed/inflight-{qos}', b'must not replay', qos=qos)
            message = parse_publish(receive(alice))
            self.assertEqual(message[2], qos)  # Leave ACK and PUBREC outstanding.
        self.publish(publisher, 'allowed/queued', b'must not dequeue')
        self.disconnect(alice)
        self.assertEqual(len(self.session('revoked-reader')['outgoing']), 2)
        self.assertEqual(len(self.session('revoked-reader')['queued']), 1)
        self.broker.stop(kill=True)
        value = policy()
        next(user for user in value['users'] if user['username'] == 'alice')['subscribe'] = []
        self.broker.write_policy(value)
        self.broker.start()
        state = self.session('revoked-reader')
        self.assertEqual((state['subscriptions'], state['queued'], state['outgoing']), ([], [], []))
        restored = self.client('revoked-reader', clean=False, present=True)
        self.barrier(restored)
        self.quiet(restored)
        self.subscribe(restored, [('allowed/#', 1)], [128])

    def test_restart_suppresses_revoked_clean_source_will_and_preserves_retained(self):
        self.require_sqlite()
        self.start()
        admin = self.client('will-admin', 'admin')
        self.publish(admin, 'allowed/status', b'old retained', retain=True)
        self.client('will-clean-source', will=('allowed/status', b'revoked Will', 1, True))
        before = self.broker.snapshot()
        record = next(w for w in before['wills'] if w['client_id'] == 'will-clean-source')
        self.assertEqual(record['owner'], 'user:alice')
        self.assertFalse(any(s['client_id'] == 'will-clean-source' for s in before['sessions']))
        self.broker.stop(kill=True)
        value = policy()
        next(user for user in value['users'] if user['username'] == 'alice')['publish'] = []
        self.broker.write_policy(value)
        self.broker.start()
        self.assertEqual(self.broker.snapshot()['wills'], [])
        observer = self.client('will-after-revoke', 'admin')
        self.subscribe(observer, [('allowed/status', 1)])
        self.delivery(observer, 'allowed/status', b'old retained', 1, retain=True)
        self.quiet(observer)

    def test_revoked_incoming_qos2_pubrel_is_denied_without_losing_pending_state(self):
        self.require_sqlite()
        self.start()
        admin = self.client('incoming-admin', 'admin')
        self.publish(admin, 'allowed/staged', b'old retained', retain=True)
        source = self.client('incoming-source', clean=False)
        source.sendall(publication('allowed/staged', b'pending incoming', 2, 37, True))
        self.assertEqual(receive(source), (0x50, struct.pack('!H', 37)))
        self.broker.stop(kill=True)
        value = policy()
        next(user for user in value['users'] if user['username'] == 'alice')['publish'] = []
        self.broker.write_policy(value)
        self.broker.start()
        restored = self.client('incoming-source', clean=False, present=True)
        restored.sendall(ack(0x62, 37))
        self.closed(restored)  # No PUBCOMP for this denied known transaction.
        self.assertEqual(len(self.session('incoming-source')['incoming']), 1)
        observer = self.client('incoming-before-regrant', 'admin')
        self.subscribe(observer, [('allowed/staged', 0)])
        self.delivery(observer, 'allowed/staged', b'old retained', retain=True)
        self.broker.stop(kill=True)
        self.broker.write_policy(policy())
        self.broker.start()
        source = self.client('incoming-source', clean=False, present=True)
        source.sendall(ack(0x62, 37))
        self.assertEqual(receive(source), (0x70, struct.pack('!H', 37)))
        observer = self.client('incoming-after-regrant', 'admin')
        self.subscribe(observer, [('allowed/staged', 0)])
        self.delivery(observer, 'allowed/staged', b'pending incoming', retain=True)
        self.assertEqual(self.session('incoming-source')['incoming'], [])

    def test_awaitcomp_completes_after_read_revocation_without_publish_replay(self):
        self.require_sqlite()
        self.start()
        alice = self.client('awaitcomp-reader', clean=False)
        self.subscribe(alice, [('allowed/#', 2)])
        publisher = self.client('awaitcomp-writer', 'admin')
        self.publish(publisher, 'allowed/complete', b'already received payload', qos=2)
        message = parse_publish(receive(alice))
        packet_id = message[3]
        alice.sendall(ack(0x50, packet_id))
        self.assertEqual(receive(alice), (0x62, struct.pack('!H', packet_id)))
        self.broker.stop(kill=True)
        value = policy()
        next(user for user in value['users'] if user['username'] == 'alice')['subscribe'] = []
        self.broker.write_policy(value)
        self.broker.start()
        restored = self.client('awaitcomp-reader', clean=False, present=True)
        self.assertEqual(receive(restored), (0x62, struct.pack('!H', packet_id)))
        restored.sendall(ack(0x70, packet_id))
        self.barrier(restored)
        self.assertEqual(self.session('awaitcomp-reader')['outgoing'], [])
        self.quiet(restored)

    def test_pending_will_without_session_keeps_owner_binding(self):
        self.require_sqlite()
        self.start('--outbound-capacity', '1', '--max-inflight', '1')
        observer = self.client('pending-recipient', 'admin')
        self.subscribe(observer, [('allowed/pending', 1)])
        publisher = self.client('pending-writer', 'admin')
        self.publish(publisher, 'allowed/pending', b'inflight', packet_id=1)
        first = parse_publish(receive(observer))
        self.publish(publisher, 'allowed/pending', b'queued', packet_id=2)
        source = self.client('pending-source',
                             will=('allowed/pending', b'owned pending Will', 1, False))
        source.close()
        deadline = time.monotonic() + 5
        while True:
            state = self.broker.snapshot()
            pending = next((w for w in state['wills'] if w['client_id'] == 'pending-source'), None)
            if pending and pending['pending']:
                break
            if time.monotonic() >= deadline:
                self.fail('did not observe committed pending Will')
            time.sleep(.01)
        self.assertEqual(pending['owner'], 'user:alice')
        self.assertFalse(any(s['client_id'] == 'pending-source' for s in state['sessions']))
        self.refused('pending-source', 5, user='bob')
        observer.sendall(ack(0x40, first[3]))
        second = parse_publish(receive(observer))
        self.assertEqual(second[:3], ('allowed/pending', b'queued', 1))
        observer.sendall(ack(0x40, second[3]))
        self.delivery(observer, 'allowed/pending', b'owned pending Will', 1)
        self.quiet(observer)

    def test_schema2_migration_binds_anonymous_and_never_first_authenticated_claimant(self):
        self.require_sqlite()
        self.broker.use_policy = False
        self.start()
        legacy = self.client('legacy-owned', user=None, password=None, clean=False)
        self.subscribe(legacy, [('allowed/#', 1)])
        self.disconnect(legacy)
        publisher = self.client('legacy-writer', user=None, password=None)
        self.publish(publisher, 'allowed/legacy', b'legacy retained and offline', retain=True)
        self.disconnect(publisher)
        self.broker.stop()
        with sqlite3.connect(self.broker.db) as db:
            value = schema3_fixture(json.loads(db.execute('SELECT data FROM broker_snapshot WHERE id=1').fetchone()[0]))
            self.assertEqual(value['schema_version'], 3)
            value['schema_version'] = 2
            for record in value['sessions'] + value['wills']:
                self.assertEqual(record.pop('owner'), 'anonymous')
            db.execute('UPDATE broker_snapshot SET data=? WHERE id=1', (json.dumps(value).encode(),))
        self.broker.start()
        legacy = self.client('legacy-owned', user=None, password=None, clean=False, present=True)
        self.delivery(legacy, 'allowed/legacy', b'legacy retained and offline', 1)
        self.disconnect(legacy)
        self.assertEqual(self.broker.snapshot()['schema_version'], 6)
        self.assertEqual(self.session('legacy-owned')['owner'], 'anonymous')
        self.broker.stop()
        self.broker.use_policy = True
        self.broker.start()
        for clean in (False, True):
            self.refused('legacy-owned', 5, clean=clean)
        observer = self.client('legacy-retained-reader', 'admin')
        self.subscribe(observer, [('allowed/legacy', 0)])
        self.delivery(observer, 'allowed/legacy', b'legacy retained and offline', retain=True)
        self.assertEqual(self.session('legacy-owned')['owner'], 'anonymous')


if __name__ == '__main__':
    unittest.main(verbosity=2)
