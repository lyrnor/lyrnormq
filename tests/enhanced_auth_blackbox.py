#!/usr/bin/env python3
"""Real TCP SASL PLAIN MQTT5 client: no mocked mechanism, broker or SDK AUTH adapter.

The synthetic existing Argon2 fixture enforces credentials. Paho2.1 has no native
AUTH callback, so these tests frame AUTH directly. PLAIN does not encrypt secrets.
"""
import json
import socket
import struct
import time
import unittest

import mqtt5_blackbox as wire
from blackbox import frame, receive, string
from qos_blackbox import ack
from auth_acl_blackbox import AuthBroker, PASSWORD, connect_packet as connect311


PLAIN = b'\0alice\0' + PASSWORD


def authentication(reason=0x18, data=None, method='PLAIN', extra=()):
    properties = [] if method is None else [(0x15, method)]
    if data is not None:properties += [(0x16, data)]
    return frame(0xf0, bytes([reason]) + wire.props(properties + list(extra)))


class EnhancedAuthBlackbox(unittest.TestCase):
    def setUp(self):
        wire.Mqtt5Blackbox.setUp(self)
        self.broker = AuthBroker(self.directory.name, self.sqlite)
        self.addCleanup(self.broker.stop)

    def start(self, *args, enabled=True):
        self.broker.extra_args = (['--enhanced-auth-plain'] if enabled else []) + list(args)
        self.broker.start()

    def begin(self, name='enhanced', data=PLAIN, properties=(), **kwargs):
        sock = self.raw();values = [(0x15, 'PLAIN')] + list(properties)
        if data is not None:values += [(0x16, data)]
        sock.sendall(wire.connect5(name, properties=values, **kwargs))
        return sock

    def accepted(self, sock, present=False):
        values = self.connack(sock, present=present)
        self.assertEqual([v for k, v in values if k == 0x15], ['PLAIN'])
        self.assertNotIn(0x16, dict(values))
        return values

    def auth_reply(self, sock, reason=0x18):
        header, data = receive(sock);self.assertEqual(header, 0xf0)
        self.assertEqual(data[0], reason)
        values, end = wire.readprops(data, 1);self.assertEqual(end, len(data))
        self.assertEqual([v for k, v in values if k == 0x15], ['PLAIN'])
        self.assertNotIn(0x16, dict(values))

    def login(self, name='enhanced', present=False, **kwargs):
        sock = self.begin(name, **kwargs);self.accepted(sock, present);return sock

    def basic(self, name='basic', **kwargs):
        return self.client(name, username='alice', password=PASSWORD, **kwargs)

    def test_direct_credentials_real_acl_and_basic_interoperation(self):
        self.start();enhanced = self.login();basic = self.basic();self.subscribe(basic, [('allowed/#', 2)])
        for qos in range(3):
            self.publish(enhanced, 'allowed/direct', str(qos).encode(), qos)
            self.message(basic, 'allowed/direct', str(qos).encode(), qos)
        self.subscribe(enhanced, [('other/#', 1)], codes=[0x87])
        enhanced.sendall(wire.publication5('other/denied', b'no', 1, 11));self.terminal(enhanced, 0x87)

    def test_enabled_enhanced_and_native311_clients_bridge_all_qos(self):
        self.start();old = self.raw();old.sendall(connect311('native311'))
        self.assertEqual(receive(old), (0x20, b'\0\0'));enhanced = self.login('enhanced5')
        self.subscribe(old, [('allowed/from-enhanced', 2)], version=4)
        self.subscribe(enhanced, [('allowed/from-legacy', 2)])
        for qos in range(3):
            payload = bytes([qos]);self.publish(enhanced, 'allowed/from-enhanced', payload, qos)
            self.message(old, 'allowed/from-enhanced', payload, qos, version=4)
            self.publish(old, 'allowed/from-legacy', payload, qos, version=4)
            self.message(enhanced, 'allowed/from-legacy', payload, qos)

    def test_challenge_then_fragmented_auth_and_pipeline_ping(self):
        self.start();sock = self.begin(data=None);self.auth_reply(sock)
        for byte in authentication(data=PLAIN):sock.sendall(bytes([byte]))
        self.accepted(sock);self.barrier(sock)

    def test_successful_reauth_direct_and_challenge_preserves_subscriptions(self):
        self.start();sock = self.login();pub = self.basic();self.subscribe(sock, [('allowed/#', 1)])
        sock.sendall(authentication(0x19, PLAIN));self.auth_reply(sock, 0)
        sock.sendall(authentication(0x19));self.auth_reply(sock)
        self.barrier(sock);self.publish(pub, 'allowed/reauth', b'old-owner');self.message(sock, 'allowed/reauth', b'old-owner')
        sock.sendall(authentication(data=PLAIN));self.auth_reply(sock, 0)
        self.publish(pub, 'allowed/reauth', b'after');self.message(sock, 'allowed/reauth', b'after')

    def test_disabled_plain_and_unknown_method_return8c(self):
        self.start(enabled=False);sock = self.begin();self.connack(sock, code=0x8c);self.closed(sock)
        self.broker.stop();self.start();sock = self.raw();sock.sendall(wire.connect5('unknown', properties=[(0x15, 'SCRAM-SHA-256')]))
        self.connack(sock, code=0x8c);self.closed(sock)

    def test_password_field_rejected_and_basic_success_has_no_method(self):
        self.start();sock = self.begin(password=PASSWORD);self.connack(sock, code=0x82);self.closed(sock)
        basic = self.raw();basic.sendall(wire.connect5('basic', username='alice', password=PASSWORD))
        self.assertNotIn(0x15, dict(self.connack(basic)));self.barrier(basic)

    def test_bad_credentials_and_malformed_plain_rejected(self):
        self.start()
        cases = [b'', b'alice', b'\0alice', b'\0alice\0wrong', b'other\0alice\0' + PASSWORD,
                 b'\0\0password', b'\0alice\0', b'\0alice\0x\0y', b'\0alice\0\xff']
        for index, data in enumerate(cases):
            with self.subTest(index=index):
                sock = self.begin('bad-' + str(index), data=data);self.connack(sock, code=0x86);self.closed(sock)
        self.login('valid-after-failures')

    def test_connect_username_must_match_and_authzid_equal_allowed(self):
        self.start();sock = self.begin(username='bob');self.connack(sock, code=0x86);self.closed(sock)
        sock = self.begin('explicit', data=b'alice\0alice\0' + PASSWORD, username='alice');self.accepted(sock)

    def test_initial_wrong_reason_missing_changed_method_fail(self):
        self.start()
        for index, packet in enumerate([authentication(0, PLAIN), authentication(0x19, PLAIN),
                                         authentication(data=PLAIN, method=None), authentication(data=PLAIN, method='OTHER')]):
            sock = self.begin('initial-' + str(index), data=None);self.auth_reply(sock);sock.sendall(packet)
            self.terminal(sock, 0x82)

    def test_basic_unsolicited_auth_and_nested_reauth_fail82(self):
        self.start();basic = self.basic();basic.sendall(authentication(0x19, PLAIN));self.terminal(basic, 0x82)
        for index, packet in enumerate([authentication(data=PLAIN), authentication(0, PLAIN),
                                         authentication(0x19, PLAIN, method=None), authentication(0x19, PLAIN, method='OTHER')]):
            sock = self.login('unsolicited-' + str(index));sock.sendall(packet);self.terminal(sock, 0x82)
        sock = self.login('nested');sock.sendall(authentication(0x19));self.auth_reply(sock)
        sock.sendall(authentication(0x19, PLAIN));self.terminal(sock, 0x82)

    def test_preauth_publish_subscribe_ping_and_disconnect_do_not_create_session(self):
        self.start();reader = self.basic('reader');self.subscribe(reader, [('allowed/#', 1)])
        for index, packet in enumerate([wire.publication5('allowed/preauth', b'no', 1, retain=True),
                                        frame(0x82, b'\0\1' + wire.props() + string('allowed/#') + b'\1'), frame(0xc0)]):
            sock = self.begin('preauth-' + str(index), data=None, will=('allowed/will', b'no', 1, False));self.auth_reply(sock)
            sock.sendall(packet);self.terminal(sock, 0x82)
        cancelled = self.begin('cancelled', data=None, will=('allowed/will', b'no', 1, False));self.auth_reply(cancelled)
        cancelled.sendall(frame(0xe0));self.closed(cancelled);self.quiet(reader)
        self.subscribe(reader, [('allowed/preauth', 1)], packet_id=2);self.quiet(reader)

    def test_abandoned_or_bad_auth_cannot_takeover_existing_client(self):
        self.start();old = self.basic('same', expiry=60);self.subscribe(old, [('allowed/#', 1)])
        pending = self.begin('same', data=None);self.auth_reply(pending);pending.close();self.barrier(old)
        bad = self.begin('same', data=b'\0alice\0wrong');self.connack(bad, code=0x86);self.closed(bad);self.barrier(old)
        self.publish(old, 'allowed/still', b'owner');self.message(old, 'allowed/still', b'owner')

    def test_successful_auth_takeover_happens_after_challenge_completion(self):
        self.start();old = self.basic('same', expiry=60)
        new = self.begin('same', data=None, clean=False, properties=[(0x11, 60)]);self.auth_reply(new);self.barrier(old)
        new.sendall(authentication(data=PLAIN));self.accepted(new, True);self.terminal(old, 0x8e);self.barrier(new)

    def test_reauth_failure_triggers_abnormal_will(self):
        self.start();reader = self.basic('reader');self.subscribe(reader, [('allowed/will', 1)])
        owner = self.login('will-owner', will=('allowed/will', b'failed', 1, False))
        owner.sendall(authentication(0x19, b'\0alice\0wrong'));self.terminal(owner, 0x87)
        self.message(reader, 'allowed/will', b'failed')

    def test_normal_disconnect_suppresses_will(self):
        self.start();reader = self.basic('reader');self.subscribe(reader, [('allowed/will', 1)])
        owner = self.login('will-owner', will=('allowed/will', b'no', 1, False));self.disconnect(owner);self.quiet(reader)

    def test_assigned_identifier_and_exact_mandatory_packet_boundary(self):
        self.start()
        for cap, expected in [(27, False), (28, True)]:
            sock = self.begin('size-' + str(cap), properties=[(0x27, cap)])
            if expected:self.accepted(sock);self.barrier(sock)
            else:self.connack(sock, code=0x95);self.closed(sock)
        sock = self.begin('', properties=[(0x27, 54), (0x11, 60)])
        values = dict(self.accepted(sock));identifier = values[0x12];self.assertTrue(identifier);self.disconnect(sock)
        self.login(identifier, present=True, clean=False, properties=[(0x11, 60)])

    def test_tiny_peer_preflight_prevents_takeover_and_session_reset(self):
        self.start();old = self.basic('same', expiry=60);self.subscribe(old, [('allowed/#', 1)])
        tiny = self.begin('same', data=None, properties=[(0x27, 27)]);self.connack(tiny, code=0x95);self.closed(tiny);self.barrier(old)
        self.publish(old, 'allowed/still', b'yes');self.message(old, 'allowed/still', b'yes')

    def test_initial_absolute_deadline_and_pending_cleanup(self):
        self.start('--handshake-timeout-ms', '200', '--max-connections', '4')
        sock = self.begin('timeout', data=None, will=('allowed/will', b'no', 1, False));self.auth_reply(sock)
        time.sleep(.3);self.terminal(sock, 0x87)
        for index in range(6):
            pending = self.begin('cancel-' + str(index), data=None);self.auth_reply(pending);pending.sendall(frame(0xe0));self.closed(pending)
        self.login('after-cleanup')

    def test_reauth_deadline_not_extended_by_ping(self):
        self.start('--handshake-timeout-ms', '250');sock = self.login();sock.sendall(authentication(0x19));self.auth_reply(sock)
        for _ in range(3):self.barrier(sock);time.sleep(.05)
        # An independent auth timer must close this otherwise live connection.
        header, body = receive(sock);self.assertEqual(header, 0xe0);self.assertIn(body[0], (0x87, 0x8d));self.closed(sock)

    def test_alias_qos_and_outbox_survive_reauth_without_reset(self):
        self.start();reader = self.basic('reader');self.subscribe(reader, [('allowed/#', 2)])
        sock = self.login();self.publish(sock, 'allowed/alias', b'bind', 1, properties=[(0x23, 1)])
        self.message(reader, 'allowed/alias', b'bind');sock.sendall(authentication(0x19, PLAIN));self.auth_reply(sock, 0)
        self.publish(sock, '', b'reference', 2, properties=[(0x23, 1)]);self.message(reader, 'allowed/alias', b'reference', 2)

    def test_reauth_cannot_change_principal_and_connect_acl_still_applies(self):
        self.start();sock = self.login('owner')
        sock.sendall(authentication(0x19, b'\0bob\0' + PASSWORD));self.terminal(sock, 0x87)
        denied = self.begin('wrong-prefix', data=b'\0prefixed\0' + PASSWORD);self.connack(denied, code=0x87);self.closed(denied)
        allowed = self.begin('owned-prefix', data=b'\0prefixed\0' + PASSWORD);self.accepted(allowed);self.barrier(allowed)

    def test_coalesced_connect_auth_and_publish_preserves_response_order(self):
        self.start();reader = self.basic('reader');self.subscribe(reader, [('allowed/#', 1)])
        sock = self.raw();sock.sendall(wire.connect5('pipeline', properties=[(0x15, 'PLAIN')])
                                     + authentication(data=PLAIN) + wire.publication5('allowed/pipeline', b'after-auth', 1, 17))
        self.auth_reply(sock);self.accepted(sock);wire.parse_ack5(receive(sock), 0x40, 17)
        self.message(reader, 'allowed/pipeline', b'after-auth');self.barrier(sock)

    def test_qos2_pubrel_and_receive_window_progress_during_reauth(self):
        self.start();pub = self.basic('pub');sub = self.login('window', properties=[(0x21, 1)])
        self.subscribe(sub, [('allowed/#', 2)]);self.publish(pub, 'allowed/first', b'first', 2)
        old = self.message(sub, 'allowed/first', b'first', 2, complete=False)
        self.publish(pub, 'allowed/next', b'next', 2)
        sub.sendall(authentication(0x19));self.auth_reply(sub)
        sub.sendall(ack(0x50, old[3]));wire.parse_ack5(receive(sub), 0x62, old[3]);sub.sendall(ack(0x70, old[3]))
        self.message(sub, 'allowed/next', b'next', 2)
        sub.sendall(authentication(data=PLAIN));self.auth_reply(sub, 0);self.barrier(sub)

    def test_sqlite_session_outbox_restart_reauthenticate_and_secret_not_persisted(self):
        self.require_sqlite();self.start();pub = self.basic('pub');sub = self.login('durable', properties=[(0x11, 60)])
        self.subscribe(sub, [('allowed/#', 2)]);self.disconnect(sub);self.publish(pub, 'allowed/offline', b'durable', 2)
        snapshot = json.dumps(self.snapshot());self.assertNotIn('AuthenticationData', snapshot);self.assertNotIn(PASSWORD.decode(), snapshot);self.assertNotIn('PLAIN', snapshot)
        self.broker.restart_after_crash();sub = self.login('durable', present=True, clean=False, properties=[(0x11, 60)])
        self.message(sub, 'allowed/offline', b'durable', 2);sub.sendall(authentication(0x19, PLAIN));self.auth_reply(sub, 0);self.barrier(sub)

    def test_sqlite_cancelled_preauth_has_no_session_will_or_snapshot_secret(self):
        self.require_sqlite();self.start();self.basic('existing', expiry=60)
        sock = self.begin('cancelled', data=None, properties=[(0x11, 60)], will=('allowed/will', b'no', 1, False));self.auth_reply(sock);sock.sendall(frame(0xe0));self.closed(sock)
        snapshot = self.snapshot();self.assertNotIn('cancelled', [s['client_id'] for s in snapshot['sessions']]);self.assertNotIn('cancelled', json.dumps(snapshot['wills']))
        self.assertNotIn('AuthenticationData', json.dumps(snapshot));self.broker.restart_after_crash();self.login('cancelled')


for _name in ('raw', 'client', 'connack', 'closed', 'terminal', 'barrier', 'quiet', 'disconnect',
              'subscribe', 'publish', 'complete', 'message', 'require_sqlite', 'snapshot', 'wait_snapshot'):
    setattr(EnhancedAuthBlackbox, _name, getattr(wire.Mqtt5Blackbox, _name))

if __name__ == '__main__':unittest.main(verbosity=2)
