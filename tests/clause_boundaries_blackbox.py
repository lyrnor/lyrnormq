#!/usr/bin/env python3
"""Independent finite MQTT5 clause boundaries on real TCP, memory and SQLite.

Subcases are bounded input enumerations, not separate reported test executions.
Client obligations and optional refusal formats are never inferred from broker tests.
"""
import socket
import struct
import unittest

import conformance_blackbox as conform
import enhanced_auth_blackbox as enhanced
import mqtt5_blackbox as wire
from blackbox import frame, receive, string
from qos_blackbox import ack


class ClauseBoundariesBlackbox(unittest.TestCase):
    setUp = enhanced.EnhancedAuthBlackbox.setUp
    unsubscribe = conform.ConformanceBlackbox.unsubscribe

    def test_mqtt_3_reserved_fixed_flags_every_invalid_combination_closes(self):
        self.start()
        # Expected values independently specified by the wire fixed-header layouts.
        for kind, valid, body in [(6, 2, b'\0\1'), (8, 2, b'\0\1\0' + string('allowed/#') + b'\1'),
                                  (10, 2, b'\0\1\0' + string('allowed/#')),
                                  (14, 0, b''), (15, 0, b'\0' + wire.props([(0x15, 'PLAIN')]))]:
            for flags in range(16):
                if flags == valid:
                    continue
                with self.subTest(kind=kind, flags=flags):
                    client = self.basic(f'flags-{kind}-{flags}')
                    client.sendall(frame(kind << 4 | flags, body))
                    self.terminal(client, 0x81)

    def test_mqtt_2_2_1_5_publish_responses_echo_edge_identifiers(self):
        self.start()
        client = self.basic('publish-ids')
        for identifier in (1, 32768, 65535):
            with self.subTest(identifier=identifier):
                client.sendall(wire.publication5('allowed/no-subscriber', b'one', 1, identifier))
                wire.parse_ack5(receive(client), 0x40, identifier)
                client.sendall(wire.publication5('allowed/no-subscriber', b'two', 2, identifier))
                wire.parse_ack5(receive(client), 0x50, identifier)
                client.sendall(ack(0x62, identifier))
                wire.parse_ack5(receive(client), 0x70, identifier)
        self.barrier(client)

    def test_mqtt_2_2_1_6_suback_unsuback_echo_edge_identifiers(self):
        self.start()
        client = self.basic('subscription-ids')
        for identifier in (1, 32768, 65535):
            with self.subTest(identifier=identifier):
                self.subscribe(client, [('allowed/exact', 1), ('other/denied', 2)],
                               codes=[1, 0x87], packet_id=identifier)
                self.unsubscribe(client, ['allowed/exact', 'allowed/absent'], [0, 17], identifier)

    def test_mqtt_2_2_1_4_two_outstanding_server_ids_are_unique(self):
        self.start()
        client = self.basic('recipient', maximum=4)
        pub = self.basic('publisher')
        self.subscribe(client, [('allowed/out', 2)])
        self.publish(pub, 'allowed/out', b'one', 1)
        one = self.message(client, 'allowed/out', b'one', 1, complete=False)
        self.publish(pub, 'allowed/out', b'two', 2)
        two = self.message(client, 'allowed/out', b'two', 2, complete=False)
        self.assertNotEqual(one[3], two[3])
        self.assertNotEqual(one[3], 0);self.assertNotEqual(two[3], 0)
        self.complete(client, one);self.complete(client, two, barrier=True)

    def test_mqtt_2_2_1_independent_direction_same_id_cannot_retire_outgoing(self):
        self.start()
        client = self.basic('bidirectional', expiry=60)
        pub = self.basic('publisher')
        self.subscribe(client, [('allowed/out', 1)])
        self.publish(pub, 'allowed/out', b'pending', 1)
        pending = self.message(client, 'allowed/out', b'pending', complete=False)
        identifier = pending[3]
        client.sendall(wire.publication5('allowed/in', b'input', 2, identifier))
        wire.parse_ack5(receive(client), 0x50, identifier)
        client.sendall(ack(0x62, identifier));wire.parse_ack5(receive(client), 0x70, identifier)
        self.disconnect(client)
        client = self.basic('bidirectional', clean=False, expiry=60, present=True)
        pending = self.message(client, 'allowed/out', b'pending', dup=True, complete=False)
        self.assertEqual(pending[3], identifier, 'incoming completion removed an unrelated outgoing record')
        self.publish(pub, 'allowed/out', b'next', 1)
        other = self.message(client, 'allowed/out', b'next', complete=False)
        self.assertNotEqual(other[3], identifier, 'incoming completion retired an unrelated outgoing ID')
        self.complete(client, pending);self.complete(client, other, barrier=True)

    def test_mqtt_2_2_1_5_qos2_pubrel_id_and_window_held_through_pubcomp(self):
        self.start()
        client = self.basic('qos2', maximum=1)
        pub = self.basic('publisher')
        self.subscribe(client, [('allowed/qos2', 2)])
        self.publish(pub, 'allowed/qos2', b'first', 2)
        first = self.message(client, 'allowed/qos2', b'first', 2, complete=False)
        client.sendall(ack(0x50, first[3]));wire.parse_ack5(receive(client), 0x62, first[3])
        self.publish(pub, 'allowed/qos2', b'second', 2)
        self.barrier(client);self.quiet(client)
        client.sendall(ack(0x70, first[3]))
        self.message(client, 'allowed/qos2', b'second', 2)

    def test_mqtt_2_2_1_5_wrong_stage_ack_does_not_free_persistent_id(self):
        self.start()
        client = self.basic('wrong-stage', expiry=60)
        pub = self.basic('publisher')
        self.subscribe(client, [('allowed/retry', 2)])
        self.publish(pub, 'allowed/retry', b'original', 2)
        first = self.message(client, 'allowed/retry', b'original', 2, complete=False)
        client.sendall(ack(0x40, first[3]));self.terminal(client, 0x82)
        resumed = self.basic('wrong-stage', clean=False, expiry=60, present=True)
        replay = self.message(resumed, 'allowed/retry', b'original', 2, dup=True, complete=False)
        self.assertEqual(first[3], replay[3]);self.complete(resumed, replay, barrier=True)

    def test_mqtt_2_2_1_5_qos1_and_pubrel_only_ids_survive_sqlite_crash(self):
        self.require_sqlite();self.start()
        pub = self.basic('publisher')
        q1 = self.basic('persistent-q1', expiry=60)
        q2 = self.basic('persistent-q2', expiry=60)
        self.subscribe(q1, [('allowed/recovery', 1)]);self.subscribe(q2, [('allowed/recovery', 2)])
        self.publish(pub, 'allowed/recovery', b'original', 2)
        first = self.message(q1, 'allowed/recovery', b'original', 1, complete=False)
        second = self.message(q2, 'allowed/recovery', b'original', 2, complete=False)
        q2.sendall(ack(0x50, second[3]));wire.parse_ack5(receive(q2), 0x62, second[3])
        self.broker.restart_after_crash()
        q1 = self.basic('persistent-q1', clean=False, expiry=60, present=True)
        replay = self.message(q1, 'allowed/recovery', b'original', 1, dup=True, complete=False)
        self.assertEqual(replay[3], first[3]);self.complete(q1, replay, barrier=True)
        q2 = self.basic('persistent-q2', clean=False, expiry=60, present=True)
        wire.parse_ack5(receive(q2), 0x62, second[3]);q2.sendall(ack(0x70, second[3]));self.barrier(q2)

    def test_mqtt_3_1_4_3_takeover_has_8e_and_eof(self):
        self.start()
        old = self.basic('takeover', expiry=60)
        self.subscribe(old, [('allowed/live', 1)])
        new = self.basic('takeover', clean=False, expiry=60, present=True)
        self.terminal(old, 0x8e)
        self.publish(new, 'allowed/live', b'resumed')
        self.message(new, 'allowed/live', b'resumed')

    def test_mqtt_4_7_3_4_normal_unicode_and_spaces_are_not_normalized(self):
        self.start()
        pub = self.basic('publisher');sub = self.basic('subscriber')
        names = ['allowed/\u00e9', 'allowed/e\u0301', 'allowed/ value ', 'allowed/value']
        self.subscribe(sub, [(name, 1) for name in names])
        for number, name in enumerate(names):
            self.publish(pub, name, bytes([number]))
            self.message(sub, name, bytes([number]));self.barrier(sub);self.quiet(sub)

    def test_mqtt_4_7_3_4_shared_unicode_filters_keep_exact_matching(self):
        self.start()
        pub = self.basic('publisher');one = self.basic('composed');two = self.basic('decomposed')
        self.subscribe(one, [('$share/g/allowed/\u00e9', 1)])
        self.subscribe(two, [('$share/g/allowed/e\u0301', 1)])
        self.publish(pub, 'allowed/\u00e9', b'one');self.message(one, 'allowed/\u00e9', b'one');self.quiet(two)
        self.publish(pub, 'allowed/e\u0301', b'two');self.message(two, 'allowed/e\u0301', b'two');self.quiet(one)

    def test_mqtt_4_7_3_4_retained_unicode_values_are_distinct(self):
        self.start()
        pub = self.basic('publisher')
        for name, payload in [('allowed/\u00e9', b'one'), ('allowed/e\u0301', b'two')]:
            self.publish(pub, name, payload, retain=True)
        for index, (name, payload) in enumerate([('allowed/\u00e9', b'one'), ('allowed/e\u0301', b'two')]):
            sub = self.basic('retained-' + str(index));self.subscribe(sub, [(name, 1)])
            self.message(sub, name, payload, retain=True);self.barrier(sub);self.quiet(sub)

    def test_mqtt_4_12_0_1_unknown_auth_method_refuses_8c_then_eof(self):
        self.start();client = self.raw()
        client.sendall(wire.connect5('unknown', properties=[(0x15, 'UNKNOWN-MECHANISM')]))
        self.connack(client, code=0x8c);self.closed(client)
        self.login('healthy-after-refusal')

    def test_mqtt_3_1_2_unknown_protocol_prefix_is_refused_and_closed(self):
        self.start()
        # Only close/refusal is asserted: packet-version choice remains separately
        # reviewed because this listener also negotiates MQTT311.
        for index, (name, version) in enumerate([('NOPE', 5), ('MQTT', 6)]):
            client = self.raw()
            client.sendall(frame(0x10, string(name) + bytes([version, 2]) + b'\0\x1e\0' + string('bad-' + str(index))))
            try:
                present = client.recv(1, socket.MSG_PEEK)
            except ConnectionResetError:
                present = b''
            if present:
                header, body = receive(client)
                self.assertEqual(header, 0x20);self.assertEqual(body[0], 0)
                self.assertNotEqual(body[1], 0)
                self.closed(client)


for _name in ('start', 'begin', 'accepted', 'auth_reply', 'login', 'basic', 'raw', 'client',
              'connack', 'closed', 'terminal', 'barrier', 'quiet', 'disconnect', 'subscribe',
              'publish', 'complete', 'message', 'require_sqlite', 'snapshot', 'wait_snapshot'):
    setattr(ClauseBoundariesBlackbox, _name, getattr(enhanced.EnhancedAuthBlackbox, _name))

if __name__ == '__main__':
    unittest.main(verbosity=2)
