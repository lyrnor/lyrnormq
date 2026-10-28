#!/usr/bin/env python3
"""Independent MQTT5 packet ceilings/assigned IDs over real TCP and SQLite.

Encoded frame sizes include both variable integers. ACKs and committed snapshots
are barriers; SIGKILL checks process recovery, not physical power failure.
"""
import concurrent.futures
import json
import socket
import struct
import unittest

import mqtt5_blackbox as wire
from blackbox import frame, receive, string
from qos_blackbox import ack
from auth_acl_blackbox import AuthBroker, PASSWORD


class NegotiationBlackbox(unittest.TestCase):
    def connect(self, name, limit=None, clean=True, expiry=60, present=False,
                receive_maximum=None, **kwargs):
        properties = [(0x11, expiry)]
        if limit is not None:
            properties += [(0x27, limit)]
        if receive_maximum is not None:
            properties += [(0x21, receive_maximum)]
        sock = self.raw()
        sock.sendall(wire.connect5(name, clean, properties, **kwargs))
        packet = receive(sock)
        if limit is not None:
            self.assertLessEqual(len(frame(*packet)), limit)
        self.assertEqual(packet[0], 0x20)
        self.assertEqual(packet[1][:2], bytes([int(present), 0]))
        values, end = wire.readprops(packet[1], 2)
        self.assertEqual(end, len(packet[1]))
        assigned = [v for k, v in values if k == 0x12]
        self.assertEqual(len(assigned), int(not name))
        identifier = assigned[0] if assigned else name
        self.assertTrue(identifier)
        # Actual capability omissions must not enable unsupported/default features.
        caps = dict(values)
        self.assertEqual(caps[0x22], 32)
        self.assertEqual(caps[0x21], 32)
        self.assertEqual(caps[0x27], 1048576)
        self.assertEqual(caps[0x2a], 0)
        self.assertEqual(caps[0x29], 1)
        return sock, identifier

    def limited(self, sock, maximum):
        packet = receive(sock)
        self.assertLessEqual(len(frame(*packet)), maximum)
        return packet

    def test_positive_maximum_and_absent_or_uint_maximum(self):
        self.start()
        for cap in (None, 64, 0xffffffff):
            sock, _ = self.connect('cap-' + str(cap), cap)
            self.barrier(sock)

    def test_tiny_limits_never_emit_oversized_refusal(self):
        self.start()
        for cap in range(1, 20):
            with self.subTest(maximum=cap):
                sock = self.raw()
                sock.sendall(wire.connect5('tiny-' + str(cap), properties=[(0x27, cap)]))
                if cap < 5:
                    self.closed(sock)
                else:
                    header, body = self.limited(sock, cap)
                    self.assertEqual((header, body[:2]), (0x20, b'\0\x95'))
                    self.closed(sock)

    def test_connack_exact_encoded_limit_and_one_below(self):
        self.start()
        reference = self.raw();reference.sendall(wire.connect5('reference'))
        size = len(frame(*receive(reference)))
        self.connect('exact', size)
        other = self.raw();other.sendall(wire.connect5('below', properties=[(0x27, size - 1)]))
        header, body = self.limited(other, size - 1)
        self.assertEqual(header, 0x20)
        # Nonzero alias capacity and unavailable capabilities are mandatory.
        self.assertEqual(body[:2], b'\0\x95')
        self.closed(other)

    def test_zero_duplicate_and_wrong_context_remain_protocol_errors(self):
        self.start()
        for values in ([(0x27, 0)], [(0x27, 64), (0x27, 64)], [(0x12, 'client-supplied')]):
            sock = self.raw();sock.sendall(wire.connect5('invalid', properties=values))
            header, body = receive(sock)
            self.assertEqual(header, 0x20)
            self.assertIn(body[1], (0x81, 0x82))
            self.closed(sock)
        sub, _ = self.connect('invalid-context', 64)
        sub.sendall(wire.publication5('t', b'x', properties=[(0x27, 64)]))
        header, body = self.limited(sub, 64)
        self.assertEqual(header, 0xe0);self.assertIn(body[0], (0x81, 0x82))
        self.closed(sub)

    def test_valid_tiny_maximum_then_malformed_later_connect_payload(self):
        self.start()
        suffixes = [
            (2, b'\x00'),  # truncated client identifier length
            (2, string('bad\x00id')),  # invalid MQTT UTF8 after valid properties
            (6, string('id') + wire.props() + string('bad/+') + b'\x00\x00'),
            (6, string('id') + wire.props() + b'\x00\x05a'),
            (6, string('id') + wire.props() + string('will') + b'\x00\x05x'),
        ]
        for maximum in (1, 2, 3, 4, 5, 64):
            for flags, suffix in suffixes:
                with self.subTest(maximum=maximum, suffix=suffix):
                    sock = self.raw()
                    body = (string('MQTT') + bytes([5, flags]) + b'\x00\x1e'
                            + wire.props([(0x27, maximum)]) + suffix)
                    sock.sendall(frame(0x10, body))
                    try:
                        first = sock.recv(1, socket.MSG_PEEK)
                    except ConnectionResetError:
                        continue
                    if first:
                        header, data = self.limited(sock, maximum)
                        self.assertEqual(header, 0x20)
                        self.assertNotEqual(data[1], 0)
                        self.closed(sock)

    def test_complete_publish_boundaries_qos_matrix_and_other_recipient(self):
        self.start()
        pub = self.client('source')
        witness = self.client('unlimited');self.subscribe(witness, [('b/#', 2)])
        for qos in range(3):
            for cap in (64, 127, 128, 129, 130, 131, 16383, 16384, 16385, 16386, 16387, 16388):
                with self.subTest(qos=qos, cap=cap):
                    topic = f'b/{qos}/{cap}'
                    sub, _ = self.connect('target-' + str(qos) + '-' + str(cap), cap)
                    self.subscribe(sub, [(topic, 2)])
                    payload = max((b'x' * count for count in range(max(0, cap - 30), cap)
                                   if len(wire.publication5(topic, b'x' * count, qos)) <= cap), key=len)
                    self.publish(pub, topic, payload, qos=qos)
                    packet = self.limited(sub, cap);self.assertEqual(len(frame(*packet)), len(wire.publication5(topic, payload, qos)))
                    message = wire.parse5(packet);self.assertEqual(message[1], payload)
                    self.complete(sub, message)
                    self.message(witness, topic, payload, qos=qos)
                    self.publish(pub, topic, payload + b'!', qos=qos)
                    self.message(witness, topic, payload + b'!', qos=qos)
                    self.publish(pub, topic, b'fits', qos=qos)
                    self.message(sub, topic, b'fits', qos=qos)
                    self.message(witness, topic, b'fits', qos=qos)
                    self.quiet(sub)
                    self.disconnect(sub)

    def test_property_length_varint_and_metadata_not_truncated(self):
        self.start();pub = self.client('metadata-source')
        for value_length in (119, 120, 121, 122, 123, 124):
            metadata = [(0x26, ('k', 'v' * value_length))]
            topic = 'metadata/' + str(value_length)
            payload = b'data'
            cap = len(wire.publication5(topic, payload, 1, properties=metadata))
            sub, _ = self.connect('metadata-' + str(value_length), cap)
            self.subscribe(sub, [(topic, 1)])
            self.publish(pub, topic, payload, properties=metadata)
            packet = self.limited(sub, cap);self.assertEqual(len(frame(*packet)), cap)
            parsed = wire.parse5(packet);self.assertEqual(parsed[6], metadata);self.complete(sub, parsed)
            self.publish(pub, topic, payload + b'!', properties=metadata)
            self.publish(pub, topic, b'ok')
            self.message(sub, topic, b'ok');self.quiet(sub)

    def test_small_peer_does_not_reject_global_retained_and_later_replay(self):
        self.start();pub = self.client('retained-source')
        sub, _ = self.connect('small-retained', 64);self.subscribe(sub, [('retained/#', 2)])
        self.publish(pub, 'retained/large', b'x' * 256, qos=2, retain=True)
        self.quiet(sub)
        self.subscribe(sub, [('retained/#', 2)], packet_id=2);self.quiet(sub)
        replay = self.client('large-retained');self.subscribe(replay, [('retained/#', 2)])
        self.message(replay, 'retained/large', b'x' * 256, qos=2, retain=True)
        self.publish(pub, 'retained/large', b'', retain=True)
        self.message(sub, 'retained/large', b'');self.message(replay, 'retained/large', b'')

    def test_oversized_retained_matches_do_not_consume_small_peer_queue(self):
        self.start('--outbound-capacity', '1')
        pub = self.client('capacity-retained-source')
        for topic in ('capacity/a', 'capacity/b'):
            self.publish(pub, topic, b'x' * 256, retain=True)
        self.publish(pub, 'capacity/z', b'fits', retain=True)
        small, _ = self.connect('capacity-small', 64)
        # Retained replay is one atomic SUBSCRIBE proposal: no network-poll race
        # can rescue an implementation that charges oversized copies to this queue.
        self.subscribe(small, [('capacity/#', 1)])
        self.message(small, 'capacity/z', b'fits', retain=True)
        self.quiet(small)
        uncapped = self.client('capacity-uncapped')
        for index, topic in enumerate(('capacity/a', 'capacity/b')):
            self.subscribe(uncapped, [(topic, 1)], packet_id=index + 1)
            self.message(uncapped, topic, b'x' * 256, retain=True)
            self.barrier(uncapped)

    def test_peer_limit_is_connection_only_offline_and_reconnect(self):
        self.start();sub, _ = self.connect('offline', 64);self.subscribe(sub, [('offline/#', 2)])
        self.disconnect(sub);pub = self.client('offline-source')
        for qos in (1, 2):self.publish(pub, 'offline/' + str(qos), b'x' * 256, qos=qos)
        sub, _ = self.connect('offline', None, clean=False, present=True)
        for qos in (1, 2):self.message(sub, 'offline/' + str(qos), b'x' * 256, qos=qos)
        self.disconnect(sub)
        self.publish(pub, 'offline/big', b'x' * 256)
        self.publish(pub, 'offline/small', b'yes')
        sub, _ = self.connect('offline', 64, clean=False, present=True, receive_maximum=1)
        self.message(sub, 'offline/small', b'yes');self.barrier(sub);self.quiet(sub)

    def test_started_qos_replay_discard_releases_credit_and_stays_retired(self):
        self.start();pub = self.client('replay-source')
        for qos in (1, 2):
            name = 'started-' + str(qos)
            sub, _ = self.connect(name);self.subscribe(sub, [(name + '/#', qos)])
            self.publish(pub, name + '/big', b'x' * 256, qos=qos)
            old = self.message(sub, name + '/big', b'x' * 256, qos=qos, complete=False)
            self.disconnect(sub)
            self.publish(pub, name + '/small', b'yes', qos=qos)
            sub, _ = self.connect(name, 64, clean=False, present=True, receive_maximum=1)
            self.message(sub, name + '/small', b'yes', qos=qos);self.quiet(sub)
            self.disconnect(sub)
            sub, _ = self.connect(name, None, clean=False, present=True)
            self.quiet(sub);self.barrier(sub)
            if self.sqlite:
                state = next(s for s in self.snapshot()['sessions'] if s['client_id'] == name)
                self.assertEqual(state['outgoing'], [])

    def test_awaitcomp_is_pubrel_only_under_smaller_limit(self):
        self.start();pub = self.client('comp-source')
        sub, _ = self.connect('comp');self.subscribe(sub, [('comp', 2)])
        self.publish(pub, 'comp', b'x' * 256, qos=2)
        message = self.message(sub, 'comp', b'x' * 256, qos=2, complete=False)
        sub.sendall(ack(0x50, message[3]));wire.parse_ack5(receive(sub), 0x62, message[3])
        self.disconnect(sub)
        sub, _ = self.connect('comp', 64, clean=False, present=True, receive_maximum=1)
        wire.parse_ack5(self.limited(sub, 64), 0x62, message[3]);sub.sendall(ack(0x70, message[3]))
        self.publish(pub, 'comp', b'fits', qos=2);self.message(sub, 'comp', b'fits', qos=2)

    def test_suback_too_large_closes_without_partial_control(self):
        self.start();sub, _ = self.connect('suback', 32)
        filters = [(f't/{i}', 0) for i in range(40)]
        sub.sendall(frame(0x82, b'\0\1' + wire.props() + b''.join(string(t) + bytes([q]) for t, q in filters)))
        self.closed(sub)

    def test_unsuback_too_large_closes_without_partial_control(self):
        self.start();sub, _ = self.connect('unsuback', 32)
        sub.sendall(frame(0xa2, b'\0\1' + wire.props() + b''.join(string(f't/{i}') for i in range(40))))
        self.closed(sub)

    def test_protocol_error_takeover_keepalive_terminal_obey_limit(self):
        self.start()
        old, _ = self.connect('taken', 32)
        self.connect('taken', 64, clean=False, present=True)
        packet = self.limited(old, 32);self.assertEqual(packet[0], 0xe0);self.assertEqual(packet[1][0], 0x8e);self.closed(old)
        malformed, _ = self.connect('malformed', 32);malformed.sendall(frame(0xc1))
        packet = self.limited(malformed, 32);self.assertEqual(packet[0], 0xe0);self.closed(malformed)
        timeout, _ = self.connect('timeout', 32, keepalive=1)
        packet = self.limited(timeout, 32);self.assertEqual(packet[0], 0xe0);self.closed(timeout)

    def test_tiny_reconnect_preserves_old_connection_subscription_and_will(self):
        self.start();observer = self.client('will-observer');self.subscribe(observer, [('will', 1)])
        old, _ = self.connect('old', will=('will', b'intact', 1, False))
        self.subscribe(old, [('old', 1)])
        before = self.snapshot() if self.sqlite else None
        failed = self.raw();failed.sendall(wire.connect5('old', True, [(0x27, 5)]))
        self.connack(failed, 0x95);self.closed(failed)
        if self.sqlite:
            after = self.snapshot()
            self.assertEqual(after['sessions'], before['sessions'])
            self.assertEqual(after['wills'], before['wills'])
        self.barrier(old);self.quiet(observer)
        pub = self.client('still-source');self.publish(pub, 'old', b'alive');self.message(old, 'old', b'alive')
        old.shutdown(socket.SHUT_RDWR);old.close();self.message(observer, 'will', b'intact')

    def test_assigned_identifier_both_clean_values_and_explicit_resume(self):
        self.start()
        for clean in (False, True):
            sock, assigned = self.connect('', 128, clean=clean)
            self.assertLessEqual(len(assigned.encode()), 23);self.assertTrue(assigned.isalnum())
            self.subscribe(sock, [(assigned + '/#', 2)])
            self.disconnect(sock)
            pub = self.client('assigned-source-' + str(clean))
            self.publish(pub, assigned + '/message', b'offline', qos=2)
            resumed, name = self.connect(assigned, 128, clean=False, present=True)
            self.assertEqual(name, assigned);self.message(resumed, assigned + '/message', b'offline', qos=2)

    def test_assigned_id_does_not_enlarge_original_wire_connect_will_validation(self):
        self.start('--max-packet-size', '64')
        observer = self.raw();observer.sendall(wire.connect5('observer'))
        values = self.connack(observer)
        self.assertEqual(dict(values)[0x27], 64)
        self.subscribe(observer, [('w', 1)])
        payload = next(b'x' * length for length in range(64)
                       if len(wire.connect5('', properties=[(0x11, 60), (0x27, 64)],
                                            will=('w', b'x' * length, 1, False))) == 64)
        packet = wire.connect5('', properties=[(0x11, 60), (0x27, 64)],
                               will=('w', payload, 1, False))
        self.assertEqual(len(packet), 64)
        owner = self.raw();owner.sendall(packet)
        header, body = self.limited(owner, 64)
        self.assertEqual((header, body[:2]), (0x20, b'\x00\x00'))
        properties, end = wire.readprops(body, 2);self.assertEqual(end, len(body))
        identifiers = [value for key, value in properties if key == 0x12]
        self.assertEqual(len(identifiers), 1);self.assertTrue(identifiers[0])
        self.assertGreater(len(wire.connect5(identifiers[0],
                                            properties=[(0x11, 60), (0x27, 64)],
                                            will=('w', payload, 1, False))), 64)
        self.barrier(owner);owner.shutdown(socket.SHUT_RDWR);owner.close()
        self.message(observer, 'w', payload)

    def test_concurrent_assigned_identifiers_are_unique(self):
        self.start()
        def allocate(index):
            sock = self.broker.socket();sock.sendall(wire.connect5('', properties=[(0x11, 60)]))
            header, body = receive(sock);assert header == 0x20 and body[:2] == b'\0\0'
            values, _ = wire.readprops(body, 2);identifier = [v for k, v in values if k == 0x12]
            assert len(identifier) == 1
            return sock, identifier[0]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            allocations = list(pool.map(allocate, range(24)))
        for sock, _ in allocations:self.addCleanup(sock.close)
        self.assertEqual(len({identifier for _, identifier in allocations}), 24)
        for sock, _ in allocations:self.barrier(sock)

    def test_assignment_tiny_limit_cannot_create_durable_or_will_state(self):
        self.start();observer = self.client('observer');self.subscribe(observer, [('will', 1)])
        failed = self.raw();failed.sendall(wire.connect5('', properties=[(0x11, 60), (0x27, 5)],
                                                       will=('will', b'never', 1, True)))
        self.connack(failed, 0x95);self.closed(failed);self.quiet(observer)
        if self.sqlite:
            data = self.snapshot();self.assertEqual(data['wills'], [])
            self.assertEqual([s['client_id'] for s in data['sessions']], [])

    def test_generated_key_authorization_and_authenticated_ownership(self):
        self.broker = AuthBroker(self.directory.name, self.sqlite);self.addCleanup(self.broker.stop);self.start()
        denied = self.raw();denied.sendall(wire.connect5('', properties=[(0x11, 60)], username='prefixed', password=PASSWORD))
        self.connack(denied, 0x87);self.closed(denied)
        bad = self.raw();bad.sendall(wire.connect5('', properties=[(0x11, 60), (0x27, 64)], username='alice', password=b'wrong'))
        header, body = self.limited(bad, 64);self.assertEqual((header, body[:2]), (0x20, b'\0\x86'));self.closed(bad)
        owner, assigned = self.connect('', username='alice', password=PASSWORD)
        self.subscribe(owner, [('allowed/#', 1)]);self.disconnect(owner)
        stolen = self.raw();stolen.sendall(wire.connect5(assigned, False, [(0x11, 60)], username='bob', password=PASSWORD))
        self.connack(stolen, 0x87);self.closed(stolen)
        owner, _ = self.connect(assigned, clean=False, present=True, username='alice', password=PASSWORD)
        self.barrier(owner)
        if self.sqlite:
            states = self.snapshot()['sessions'];self.assertEqual(len(states), 1)
            self.assertEqual((states[0]['client_id'], states[0]['owner']), (assigned, 'user:alice'))

    def test_mixed_versions_bridge_under_peer_limit_and_explicit_assigned_id(self):
        self.start();pub = self.client('legacy-source', version=4)
        sub, assigned = self.connect('', 64);self.subscribe(sub, [('mixed/#', 2)])
        for qos in range(3):
            self.publish(pub, 'mixed/big', b'x' * 256, qos=qos, version=4)
            self.publish(pub, 'mixed/small', b'yes', qos=qos, version=4)
            self.message(sub, 'mixed/small', b'yes', qos=qos)
        self.disconnect(sub)
        if self.sqlite:
            legacy = self.client(assigned, version=4, clean=False, present=True)
            source = self.client('v5-source');self.publish(source, 'mixed/legacy', b'x' * 256, qos=2)
            self.message(legacy, 'mixed/legacy', b'x' * 256, qos=2, version=4)

    def test_packet_size_and_expiry_metadata_coexist(self):
        self.start();pub = self.client('ttl-source');sub, _ = self.connect('ttl-peer', 64)
        self.subscribe(sub, [('ttl/#', 2)])
        self.publish(pub, 'ttl/big', b'x' * 256, properties=[(2, 60)], qos=2)
        self.publish(pub, 'ttl/expired', b'zero', properties=[(2, 0)], qos=2)
        self.publish(pub, 'ttl/small', b'yes', properties=[(2, 60)], qos=2)
        packet = self.limited(sub, 64);value = wire.parse5(packet)
        self.assertEqual(value[:3], ('ttl/small', b'yes', 2));self.assertEqual([k for k, _ in value[6]], [2]);self.assertTrue(0 < value[6][0][1] <= 60)
        self.complete(sub, value);self.quiet(sub)

    def test_sqlite_assigned_offline_inflight_recovery_and_no_persisted_limit(self):
        self.require_sqlite();self.start();sub, assigned = self.connect('', 64)
        self.subscribe(sub, [('recover/#', 2)])
        pub = self.client('recover-source');self.publish(pub, 'recover/first', b'one', qos=1)
        old = self.message(sub, 'recover/first', b'one', complete=False)
        self.disconnect(sub)
        self.publish(pub, 'recover/big', b'x' * 256, qos=2)
        self.disconnect(pub);self.broker.restart_after_crash()
        sub, _ = self.connect(assigned, clean=False, present=True)
        replay = self.message(sub, 'recover/first', b'one', dup=True)
        self.assertEqual(replay[3], old[3]);self.message(sub, 'recover/big', b'x' * 256, qos=2)
        data = self.snapshot();self.assertEqual(data['schema_version'], 6)
        self.assertNotIn('maximum_packet_size', json.dumps(data))
        later, fresh = self.connect('');self.assertNotEqual(assigned, fresh);self.barrier(later)

    def test_sqlite_smaller_resume_discards_old_started_stage_durably(self):
        self.require_sqlite();self.start();pub = self.client('crash-source')
        sub, _ = self.connect('crash');self.subscribe(sub, [('crash/#', 2)])
        for qos in (1, 2):
            self.publish(pub, f'crash/big{qos}', b'x' * 256, qos=qos)
            self.message(sub, f'crash/big{qos}', b'x' * 256, qos=qos, complete=False)
        self.disconnect(sub);self.publish(pub, 'crash/small', b'yes', qos=2);self.disconnect(pub)
        self.broker.restart_after_crash()
        sub, _ = self.connect('crash', 64, clean=False, present=True, receive_maximum=1)
        self.message(sub, 'crash/small', b'yes', qos=2);self.barrier(sub);self.disconnect(sub)
        self.broker.restart_after_crash();sub, _ = self.connect('crash', None, clean=False, present=True)
        self.barrier(sub);self.quiet(sub)
        state = next(s for s in self.snapshot()['sessions'] if s['client_id'] == 'crash')
        self.assertEqual(state['outgoing'], []);self.assertEqual(state['queued'], [])

    def test_sqlite_assigned_crash_will_uses_resolved_identity(self):
        self.require_sqlite();self.start();observer = self.client('will-reader', expiry=60)
        self.subscribe(observer, [('will/recovered', 1)]);self.disconnect(observer)
        owner, assigned = self.connect('', will=('will/recovered', b'assigned will', 1, True))
        self.barrier(owner);self.broker.restart_after_crash()
        observer = self.client('will-reader', clean=False, expiry=60, present=True)
        self.message(observer, 'will/recovered', b'assigned will')
        resumed, _ = self.connect(assigned, clean=False, present=True);self.barrier(resumed)


for _name in ('setUp', 'start', 'raw', 'client', 'connack', 'closed', 'terminal', 'barrier',
              'quiet', 'disconnect', 'subscribe', 'publish', 'complete', 'message',
              'require_sqlite', 'snapshot', 'wait_snapshot'):
    setattr(NegotiationBlackbox, _name, getattr(wire.Mqtt5Blackbox, _name))

if __name__ == '__main__':
    unittest.main(verbosity=2)
