#!/usr/bin/env python3
"""Independent clause-driven MQTT5 TCP checks; actual broker, no codec reuse.

Each test owns a real process and optional SQLite DB. Subcases are not independent
execution counts. SIGKILL tests cover process failure, not physical power loss.
"""
import struct
import unittest

import enhanced_auth_blackbox as enhanced
import mqtt5_blackbox as wire
from blackbox import frame, receive, string
from qos_blackbox import ack


class ConformanceBlackbox(unittest.TestCase):
    setUp = enhanced.EnhancedAuthBlackbox.setUp

    def unsubscribe(self, sock, filters, codes, packet_id=71):
        sock.sendall(frame(0xa2, struct.pack('!H', packet_id) + wire.props()
                           + b''.join(string(value) for value in filters)))
        header, body = receive(sock)
        self.assertEqual(header, 0xb0)
        self.assertEqual(body[:2], struct.pack('!H', packet_id))
        properties, end = wire.readprops(body, 2)
        self.assertEqual(properties, [])
        self.assertEqual(list(body[end:]), codes)
        self.barrier(sock)  # Exactly one UNSUBACK, no second response.

    def refused_initial(self, sock, reason=0x82):
        self.connack(sock, code=reason, present=False)
        self.closed(sock)

    def test_mqtt_3_14_0_1_preauth_errors_use_connack_never_disconnect(self):
        self.start()
        packets = [frame(0xc0), wire.publication5('allowed/no', b'no', 1),
                   frame(0x82, b'\0\1' + wire.props() + string('allowed/#') + b'\1'),
                   enhanced.authentication(data=enhanced.PLAIN, method='OTHER')]
        for index, packet in enumerate(packets):
            with self.subTest(packet=index):
                sock = self.begin('preauth-' + str(index), data=None)
                self.auth_reply(sock)
                sock.sendall(packet)
                self.refused_initial(sock)

    def test_mqtt_4_12_0_5_auth_method_required_in_raw_short_forms(self):
        self.start()
        for index, packet in enumerate([b'\xf0\0', b'\xf0\x02\x18\0',
                                        enhanced.authentication(data=enhanced.PLAIN, method=None)]):
            with self.subTest(form=index):
                sock = self.begin('missing-' + str(index), data=None)
                self.auth_reply(sock)
                sock.sendall(packet)
                self.refused_initial(sock)
        valid = self.login('method-only')
        valid.sendall(enhanced.authentication(0x19))
        self.auth_reply(valid)
        valid.sendall(enhanced.authentication(data=enhanced.PLAIN))
        self.auth_reply(valid, 0)

    def test_mqtt_3_14_0_1_initial_auth_deadline_is_failure_connack(self):
        self.start('--handshake-timeout-ms', '150')
        sock = self.begin('deadline', data=None)
        self.auth_reply(sock)
        self.refused_initial(sock, 0x87)
        self.login('after-timeout')

    def test_mqtt_3_2_0_1_rejected_auth_cannot_takeover_or_create_will(self):
        self.start()
        observer = self.basic('observer')
        self.subscribe(observer, [('allowed/will', 1)])
        old = self.basic('same', expiry=60)
        self.subscribe(old, [('allowed/still', 1)])
        pending = self.begin('same', data=None, properties=[(0x11, 60)],
                             will=('allowed/will', b'forbidden', 1, False))
        self.auth_reply(pending)
        self.barrier(old)
        pending.sendall(frame(0xc0))
        self.refused_initial(pending)
        self.publish(old, 'allowed/still', b'old-owner')
        self.message(old, 'allowed/still', b'old-owner')
        self.quiet(observer)
        if self.sqlite:
            value = self.snapshot()
            self.assertEqual([s['client_id'] for s in value['sessions']].count('same'), 1)
            self.assertNotIn('forbidden', str(value['wills']))

    def test_mqtt_3_14_0_1_established_auth_errors_remain_disconnect(self):
        self.start()
        sock = self.login()
        sock.sendall(enhanced.authentication(0x19, b'\0alice\0wrong'))
        self.terminal(sock, 0x87)
        sock = self.login('missing-after-success')
        sock.sendall(b'\xf0\0')
        self.terminal(sock, 0x82)

    def test_mqtt_3_1_2_11_4_preauth_terminal_obeys_peer_size(self):
        self.start()
        sock = self.begin('bounded', data=None, properties=[(0x27, 28)])
        self.auth_reply(sock)
        sock.sendall(frame(0xc0))
        header, body = receive(sock)
        self.assertEqual(header, 0x20)
        self.assertEqual(body[:2], b'\0\x82')
        self.assertLessEqual(len(frame(header, body)), 28)
        self.closed(sock)

    def test_mqtt_3_10_4_6_duplicate_ordinary_filters_observe_each_removal(self):
        self.start()
        sock = self.basic()
        self.subscribe(sock, [('allowed/exact', 1)])
        self.unsubscribe(sock, ['allowed/exact', 'allowed/exact', 'allowed/absent'], [0, 17, 17])
        self.publish(sock, 'allowed/exact', b'no-new-delivery')
        self.quiet(sock)

    def test_mqtt_3_10_4_6_duplicate_shared_filters_observe_each_removal(self):
        self.start()
        sock = self.basic()
        self.subscribe(sock, [('$share/g/allowed/#', 2)])
        self.unsubscribe(sock, ['$share/g/allowed/#', '$share/g/allowed/#', 'allowed/absent'], [0, 17, 17])
        self.publish(sock, 'allowed/exact', b'no-group-copy', 2)
        self.quiet(sock)

    def test_mqtt_3_11_3_1_unsuback_preserves_mixed_filter_order(self):
        self.start()
        sock = self.basic()
        self.subscribe(sock, [('allowed/#', 1), ('$share/g/allowed/#', 1)])
        self.unsubscribe(sock, ['allowed/missing', '$share/g/allowed/#', 'allowed/#',
                                '$share/g/allowed/#', 'allowed/#'], [17, 0, 0, 17, 17], 65535)

    def test_mqtt_3_10_4_1_malformed_later_filter_preserves_subscriptions(self):
        self.start()
        sock = self.basic('persistent', expiry=60)
        self.subscribe(sock, [('allowed/exact', 1)])
        sock.sendall(frame(0xa2, b'\0\x47' + wire.props() + string('allowed/exact') + string('bad/#/tail')))
        self.terminal(sock, 0x82)
        restored = self.basic('persistent', clean=False, expiry=60, present=True)
        self.publish(restored, 'allowed/exact', b'not-partially-removed')
        self.message(restored, 'allowed/exact', b'not-partially-removed')

    def test_mqtt_3_10_4_3_started_qos1_and_qos2_complete_after_unsubscribe(self):
        self.start()
        pub = self.basic('publisher')
        for qos in (1, 2):
            sub = self.basic('subscriber-' + str(qos))
            topic = 'allowed/started-' + str(qos)
            self.subscribe(sub, [(topic, qos)])
            self.publish(pub, topic, b'started', qos)
            message = self.message(sub, topic, b'started', qos, complete=False)
            self.unsubscribe(sub, [topic, topic], [0, 17])
            self.complete(sub, message, barrier=True)
            self.publish(pub, topic, b'after-unsubscribe', qos)
            self.quiet(sub)

    def test_mqtt_2_1_3_1_invalid_reserved_flags_close_connection(self):
        self.start()
        for index, packet in enumerate([frame(0xc1), frame(0x80), frame(0xa0), frame(0xf1)]):
            with self.subTest(packet=index):
                sock = self.basic('reserved-' + str(index))
                sock.sendall(packet)
                self.terminal(sock, 0x81)

    def test_mqtt_4_13_1_1_server_only_packet_types_from_client_close(self):
        self.start()
        for index, packet in enumerate([frame(0x20, b'\0\0\0'), frame(0x90, b'\0\1\0\0'), frame(0xd0)]):
            with self.subTest(packet=index):
                sock = self.basic('direction-' + str(index))
                sock.sendall(packet)
                self.terminal(sock, 0x82)

    def test_mqtt_4_3_2_5_reused_qos1_id_is_new_independent_of_dup(self):
        self.start()
        pub = self.basic('publisher')
        sub = self.basic('subscriber')
        self.subscribe(sub, [('allowed/reused', 1)])
        for dup in (False, True, False):
            pub.sendall(wire.publication5('allowed/reused', bytes([dup]), 1, 39, dup=dup))
            wire.parse_ack5(receive(pub), 0x40, 39)
            delivered = self.message(sub, 'allowed/reused', bytes([dup]), 1)
            self.assertFalse(delivered[4])
            self.barrier(sub)

    def test_mqtt_4_4_0_1_live_pending_delivery_is_not_unsolicited_retransmission(self):
        self.start()
        pub = self.basic('publisher')
        sub = self.basic('subscriber', expiry=60)
        self.subscribe(sub, [('allowed/pending', 1)])
        self.publish(pub, 'allowed/pending', b'pending')
        pending = self.message(sub, 'allowed/pending', b'pending', complete=False)
        for _ in range(3):
            self.barrier(sub)
            self.quiet(sub)
        self.disconnect(sub)
        restored = self.basic('subscriber', clean=False, expiry=60, present=True)
        replay = self.message(restored, 'allowed/pending', b'pending', dup=True)
        self.assertEqual(replay[3], pending[3])

    def test_mqtt_3_1_3_5_alphanumeric_client_ids_one_through_23_bytes(self):
        self.start()
        for length in range(1, 24):
            with self.subTest(encoded_bytes=length):
                sock = self.basic(('a0Z' * 8)[:length])
                self.barrier(sock)
                self.disconnect(sock)

    def test_mqtt_3_1_0_2_second_connect_and_3_2_2_session_present(self):
        self.start()
        sock = self.basic('stateful', expiry=60)
        self.disconnect(sock)
        resumed = self.basic('stateful', clean=False, expiry=60, present=True)
        resumed.sendall(wire.connect5('stateful', username='alice', password=enhanced.PASSWORD))
        self.terminal(resumed, 0x82)
        reset = self.basic('stateful', clean=True, expiry=60, present=False)
        self.barrier(reset)

    def test_mqtt_3_10_4_6_sqlite_unsubscribe_reasons_survive_restart(self):
        self.require_sqlite()
        self.start()
        sub = self.basic('durable', expiry=60)
        self.subscribe(sub, [('allowed/#', 1), ('$share/g/allowed/#', 1)])
        self.unsubscribe(sub, ['allowed/#', 'allowed/#'], [0, 17])
        self.disconnect(sub)
        self.broker.restart_after_crash()
        restored = self.basic('durable', clean=False, expiry=60, present=True)
        self.unsubscribe(restored, ['allowed/#', '$share/g/allowed/#', '$share/g/allowed/#'], [17, 0, 17])


for _name in ('start', 'begin', 'accepted', 'auth_reply', 'login', 'basic', 'raw', 'client',
              'connack', 'closed', 'terminal', 'barrier', 'quiet', 'disconnect', 'subscribe',
              'publish', 'complete', 'message', 'require_sqlite', 'snapshot', 'wait_snapshot'):
    setattr(ConformanceBlackbox, _name, getattr(enhanced.EnhancedAuthBlackbox, _name))

if __name__ == '__main__':
    unittest.main(verbosity=2)
