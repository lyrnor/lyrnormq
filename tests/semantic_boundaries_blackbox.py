#!/usr/bin/env python3
"""Finite real-wire acceptance for remaining MQTT5 semantic boundaries.

Mixed-RAP now uses per-filter copies under the Phase2n contract; PFI remains a producer assertion with optional broker validation. Unknown
versions retain the multi-version listener's MQTT311 refusal policy; they are not
claimed as a negotiated MQTT5 exchange. SIGKILL covers process recovery only.
"""
import unittest

import enhanced_auth_blackbox as enhanced
import mqtt5_blackbox as wire
from auth_acl_blackbox import PASSWORD, connect_packet as connect311
from blackbox import frame, receive
from qos_blackbox import ack


class SemanticBoundariesBlackbox(unittest.TestCase):
    setUp = enhanced.EnhancedAuthBlackbox.setUp

    def test_wrong_protocol_name_with_level5_silently_closes(self):
        self.start()
        for name in (b'NOPE', b'mqtt', b'MQtt'):
            with self.subTest(protocol_name=name):
                sock = self.raw()
                packet = bytearray(wire.connect5('wrong-name', username='alice', password=PASSWORD))
                packet[4:8] = name
                sock.sendall(packet)
                self.closed(sock)
        self.barrier(self.basic('valid-after-name-errors'))

    def test_supported_versions_and_unknown_version_legacy_refusal_policy(self):
        self.start()
        old = self.raw();old.sendall(connect311('legacy'))
        self.assertEqual(receive(old), (0x20, b'\0\0'));self.barrier(old)
        self.barrier(self.basic('mqtt5'))
        for level in (0, 3, 6, 255):
            with self.subTest(unnegotiated_level=level):
                sock = self.raw()
                packet = bytearray(wire.connect5('unknown-level', username='alice', password=PASSWORD))
                packet[8] = level
                sock.sendall(packet)
                self.assertEqual(receive(sock), (0x20, b'\0\1'))
                self.closed(sock)

    def test_runtime_connack_failure_reasons_are_legal_zero_present_and_close(self):
        self.start()
        cases = [
            (wire.connect5('bad-password', username='alice', password=b'wrong'), 0x86),
            (wire.connect5('not-owned-prefix', username='prefixed', password=PASSWORD), 0x87),
            (wire.connect5('unknown-method', properties=[(0x15, 'UNKNOWN')]), 0x8c),
            (wire.connect5('tiny-peer', properties=[(0x27, 19)], username='alice', password=PASSWORD), 0x95),
        ]
        for packet, reason in cases:
            with self.subTest(reason=reason):
                sock = self.raw();sock.sendall(packet)
                self.connack(sock, code=reason, present=False);self.closed(sock)
        self.barrier(self.basic('legal-success-after-failures'))

    def test_auth_reason_string_cannot_change_initial_acceptance(self):
        self.start()
        for index, text in enumerate(('reject', '0x87 Not authorized', 'success\tignored', '诊断 only')):
            sock = self.begin(f'diagnostic-initial-{index}', data=None)
            self.auth_reply(sock)
            sock.sendall(enhanced.authentication(data=enhanced.PLAIN, extra=[(0x1f, text)]))
            self.accepted(sock);self.barrier(sock)

    def test_auth_reason_string_cannot_change_reauth_or_session_owner(self):
        self.start();sock = self.login('diagnostic-owner', properties=[(0x11, 60)])
        pub = self.basic('publisher');self.subscribe(sock, [('allowed/diagnostic', 1)])
        sock.sendall(enhanced.authentication(0x19, extra=[(0x1f, 'cancel session')]))
        self.auth_reply(sock);self.barrier(sock)
        self.publish(pub, 'allowed/diagnostic', b'during')
        self.message(sock, 'allowed/diagnostic', b'during')
        sock.sendall(enhanced.authentication(data=enhanced.PLAIN, extra=[(0x1f, '0x86 invalid credentials')]))
        self.auth_reply(sock, 0);self.barrier(sock)
        self.disconnect(sock)
        resumed = self.login('diagnostic-owner', clean=False, properties=[(0x11, 60)], present=True)
        self.publish(pub, 'allowed/diagnostic', b'after')
        self.message(resumed, 'allowed/diagnostic', b'after')

    def test_auth_reason_string_does_not_rescue_wrong_credentials(self):
        self.start();sock = self.begin(data=None);self.auth_reply(sock)
        sock.sendall(enhanced.authentication(data=b'\0alice\0wrong', extra=[(0x1f, 'Success')]))
        self.connack(sock, code=0x86);self.closed(sock)

    def test_ordered_normal_topics_preserve_qos1_source_identifier_wrap(self):
        self.start();pub = self.basic('source');sub = self.basic('ordered', expiry=60)
        self.subscribe(sub, [('allowed/order', 1)])
        for index, identifier in enumerate((65535, 1, 32768)):
            self.publish(pub, 'allowed/order', bytes([index]), 1, packet_id=identifier)
        for index in range(3):self.message(sub, 'allowed/order', bytes([index]), 1)
        self.barrier(sub)
        self.disconnect(sub);sub = self.basic('ordered', clean=False, expiry=60, present=True)
        for index, identifier in enumerate((2, 65535, 1), 3):
            self.publish(pub, 'allowed/order', bytes([index]), 1, packet_id=identifier)
        for index in range(3, 6):self.message(sub, 'allowed/order', bytes([index]), 1)

    def test_ordered_qos2_pending_pubrel_recovery_precedes_later_messages(self):
        self.start();pub = self.basic('source');sub = self.basic('ordered-q2', expiry=60, maximum=1)
        self.subscribe(sub, [('allowed/order', 2)])
        for index, identifier in enumerate((65535, 1, 32768)):
            self.publish(pub, 'allowed/order', bytes([index]), 2, packet_id=identifier)
        first = self.message(sub, 'allowed/order', b'\0', 2, complete=False)
        sub.sendall(ack(0x50, first[3]));wire.parse_ack5(receive(sub), 0x62, first[3])
        self.disconnect(sub)
        sub = self.basic('ordered-q2', clean=False, expiry=60, maximum=1, present=True)
        wire.parse_ack5(receive(sub), 0x62, first[3]);sub.sendall(ack(0x70, first[3]))
        for index in (1, 2):self.message(sub, 'allowed/order', bytes([index]), 2)

    def test_mixed_rap_overlap_policy_and_retained_replay_are_distinct(self):
        self.start();pub = self.basic('publisher');sub = self.basic('overlap')
        self.subscribe(sub, [('allowed/#', 1), ('allowed/value', 9)], properties=[(0x0b, 12)])
        self.publish(pub, 'allowed/value', b'live', retain=True)
        choices = []
        for _ in range(2):
            value = wire.parse5(receive(sub));self.assertEqual(value[:3], ('allowed/value', b'live', 1))
            choices.append((value[5], [v for k, v in value[6] if k == 11]));self.complete(sub, value)
        self.assertCountEqual(choices, [(False, [12]), (True, [12])]);self.quiet(sub)
        replay = self.basic('retained-replay')
        self.subscribe(replay, [('allowed/value', 1)])
        self.message(replay, 'allowed/value', b'live', retain=True)

    def test_shared_rap_entries_keep_separate_captured_choices(self):
        self.start();pub = self.basic('publisher');sub = self.basic('shared')
        self.subscribe(sub, [('$share/a/allowed/#', 1)], properties=[(0x0b, 21)])
        self.subscribe(sub, [('$share/b/allowed/#', 9)], properties=[(0x0b, 22)])
        self.publish(pub, 'allowed/value', b'live', retain=True)
        observed = []
        for _ in range(2):
            value = wire.parse5(receive(sub));observed.append((value[5], [v for k, v in value[6] if k == 11]));self.complete(sub, value)
        self.assertCountEqual(observed, [(False, [21]), (True, [22])]);self.quiet(sub)

    def test_pfi1_no_validation_policy_preserves_publish_and_retained_bytes(self):
        self.start();pub = self.basic('publisher');sub = self.basic('recipient')
        self.subscribe(sub, [('allowed/pfi', 1)])
        payload = b'\xff\xc0\xaf\x00';metadata = [(1, 1)]
        self.publish(pub, 'allowed/pfi', payload, properties=metadata, retain=True)
        self.message(sub, 'allowed/pfi', payload, properties=metadata)
        replay = self.basic('replay');self.subscribe(replay, [('allowed/pfi', 1)])
        self.message(replay, 'allowed/pfi', payload, properties=metadata, retain=True)

    def test_pfi1_no_validation_policy_preserves_abnormal_will_bytes(self):
        self.start();sub = self.basic('recipient');self.subscribe(sub, [('allowed/will', 1)])
        owner = self.basic('will-owner', will=('allowed/will', b'\xff\x80', 1, False), will_properties=[(1, 1)])
        owner.close();self.message(sub, 'allowed/will', b'\xff\x80', properties=[(1, 1)])

    def test_sqlite_ordered_offline_queue_survives_process_restart(self):
        self.require_sqlite();self.start();pub = self.basic('publisher')
        sub = self.basic('offline', expiry=60);self.subscribe(sub, [('allowed/order', 2)]);self.disconnect(sub)
        for index, qos in enumerate((1, 2, 1)):
            self.publish(pub, 'allowed/order', bytes([index]), qos, packet_id=65535 - index)
        self.broker.restart_after_crash()
        sub = self.basic('offline', clean=False, expiry=60, maximum=1, present=True)
        for index, qos in enumerate((1, 2, 1)):self.message(sub, 'allowed/order', bytes([index]), qos)


for _name in ('start', 'begin', 'accepted', 'auth_reply', 'login', 'basic', 'raw', 'client',
              'connack', 'closed', 'terminal', 'barrier', 'quiet', 'disconnect', 'subscribe',
              'publish', 'complete', 'message', 'require_sqlite', 'snapshot', 'wait_snapshot'):
    setattr(SemanticBoundariesBlackbox, _name, getattr(enhanced.EnhancedAuthBlackbox, _name))

if __name__ == '__main__':unittest.main(verbosity=2)
