#!/usr/bin/env python3
"""Independent MQTT311/5 wire, metadata, flow-control and recovery checks.

RMQTT_TEST_STORAGE_PATH selects SQLite; every method owns a fresh DB. Barriers are
ACKs, confirmed output, or committed snapshots. Bounded silence is only an unwanted
output observation. SIGKILL tests do not simulate physical power failure.
"""
from contextlib import closing
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
from qos_blackbox import ack, parse_publish
from retained_will_blackbox import FeatureBroker
from auth_acl_blackbox import AuthBroker, PASSWORD, connect_packet as auth311
from schema_compat import schema3_fixture, schema4_fixture


def varint(value):
    assert 0 <= value <= 268435455
    result = bytearray()
    while True:
        digit = value % 128
        value //= 128
        result.append(digit | (128 if value else 0))
        if not value:
            return bytes(result)


def readvar(data, offset=0):
    result = 0
    for count in range(4):
        digit = data[offset]
        offset += 1
        result |= (digit & 127) << (7 * count)
        if not digit & 128:
            assert count == 0 or digit != 0, 'noncanonical variable integer'
            return result, offset
    raise AssertionError('oversized variable integer')


BYTE_PROPERTIES = {1, 0x17, 0x19, 0x24, 0x25, 0x28, 0x29, 0x2a}
SHORT_PROPERTIES = {0x13, 0x21, 0x22, 0x23}
INT_PROPERTIES = {2, 0x11, 0x18, 0x27}
STRING_PROPERTIES = {3, 8, 0x12, 0x15, 0x1a, 0x1c, 0x1f}
BINARY_PROPERTIES = {9, 0x16}


def prop(identifier, value):
    prefix = varint(identifier)
    if identifier in BYTE_PROPERTIES:
        return prefix + bytes([value])
    if identifier in SHORT_PROPERTIES:
        return prefix + struct.pack('!H', value)
    if identifier in INT_PROPERTIES:
        return prefix + struct.pack('!I', value)
    if identifier in STRING_PROPERTIES:
        return prefix + string(value)
    if identifier in BINARY_PROPERTIES:
        return prefix + struct.pack('!H', len(value)) + value
    if identifier == 0x0b:
        return prefix + varint(value)
    if identifier == 0x26:
        return prefix + string(value[0]) + string(value[1])
    raise AssertionError(('unknown property', identifier))


def props(values=()):
    encoded = b''.join(prop(key, value) for key, value in values)
    return varint(len(encoded)) + encoded


def readprops(data, offset=0):
    length, offset = readvar(data, offset)
    end = offset + length
    assert end <= len(data), 'truncated properties'
    values = []

    def binary(at):
        size = struct.unpack('!H', data[at:at + 2])[0]
        stop = at + 2 + size
        assert stop <= end
        return data[at + 2:stop], stop

    while offset < end:
        identifier, offset = readvar(data, offset)
        if identifier in BYTE_PROPERTIES:
            value = data[offset]
            offset += 1
        elif identifier in SHORT_PROPERTIES:
            value = struct.unpack('!H', data[offset:offset + 2])[0]
            offset += 2
        elif identifier in INT_PROPERTIES:
            value = struct.unpack('!I', data[offset:offset + 4])[0]
            offset += 4
        elif identifier in STRING_PROPERTIES | BINARY_PROPERTIES:
            value, offset = binary(offset)
            if identifier in STRING_PROPERTIES:
                value = value.decode('utf8')
        elif identifier == 0x0b:
            value, offset = readvar(data, offset)
        elif identifier == 0x26:
            key, offset = binary(offset)
            value, offset = binary(offset)
            value = (key.decode('utf8'), value.decode('utf8'))
        else:
            raise AssertionError(('unknown response property', identifier))
        values.append((identifier, value))
    assert offset == end
    return values, offset


def connect5(name, clean=True, properties=(), will=None, will_properties=(),
             username=None, password=None, keepalive=30):
    flags = 2 if clean else 0
    payload = string(name)
    if will is not None:
        topic, data, qos, retain = will
        flags |= 4 | (qos << 3) | (32 if retain else 0)
        payload += props(will_properties) + string(topic) + struct.pack('!H', len(data)) + data
    if username is not None:
        flags |= 128
        payload += string(username)
    if password is not None:
        flags |= 64
        payload += struct.pack('!H', len(password)) + password
    return frame(0x10, string('MQTT') + bytes([5, flags])
                 + struct.pack('!H', keepalive) + props(properties) + payload)


def publication5(topic, payload, qos=0, packet_id=1, properties=(), retain=False, dup=False):
    body = string(topic)
    if qos:
        body += struct.pack('!H', packet_id)
    return frame(0x30 | (qos << 1) | int(retain) | (8 if dup else 0),
                 body + props(properties) + payload)


def parse5(packet):
    header, body = packet
    assert header >> 4 == 3, packet
    size = struct.unpack('!H', body[:2])[0]
    topic = body[2:2 + size].decode()
    offset = 2 + size
    qos = (header >> 1) & 3
    identifier = None
    if qos:
        identifier = struct.unpack('!H', body[offset:offset + 2])[0]
        assert identifier
        offset += 2
    properties, offset = readprops(body, offset)
    return topic, body[offset:], qos, identifier, bool(header & 8), bool(header & 1), properties


def ack5(header, packet_id, reason=0, properties=(), form='full'):
    body = struct.pack('!H', packet_id)
    if form != 'short':
        body += bytes([reason])
    if form == 'full':
        body += props(properties)
    return frame(header, body)


def parse_ack5(packet, kind, packet_id=None, reason=0):
    header, body = packet
    assert header == kind, packet
    actual = struct.unpack('!H', body[:2])[0]
    if packet_id is not None:
        assert actual == packet_id, (actual, packet_id)
    value = body[2] if len(body) >= 3 else 0
    allowed = (0, 0x10) if reason == 0 and kind in (0x40, 0x50) else (reason,)
    assert value in allowed, (value, allowed)
    if len(body) >= 4:
        _, end = readprops(body, 3)
        assert end == len(body)
    return actual


METADATA = [(0x26, ('duplicate', 'one')), (3, 'application/x-test'),
            (0x26, ('duplicate', 'two')), (1, 1), (8, 'response/topic'),
            (9, b'correlation\x00\xff')]


class Mqtt5Blackbox(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-mqtt5-')
        self.addCleanup(self.directory.cleanup)
        self.sqlite = bool(os.environ.get('RMQTT_TEST_STORAGE_PATH'))
        self.broker = FeatureBroker(self.directory.name, self.sqlite)
        self.addCleanup(self.broker.stop)

    def start(self, *args):
        self.broker.extra_args = list(args)
        self.broker.start()

    def raw(self):
        result = self.broker.socket()
        self.addCleanup(result.close)
        return result

    def client(self, name, version=5, clean=True, expiry=None, maximum=None,
               properties=(), present=False, will=None, will_properties=(),
               username=None, password=None, keepalive=30):
        result = self.raw()
        if version == 5:
            values = list(properties)
            if expiry is not None:
                values += [(0x11, expiry)]
            if maximum is not None:
                values += [(0x21, maximum)]
            result.sendall(connect5(name, clean, values, will, will_properties,
                                    username, password, keepalive))
            self.connack(result, present=present)
        else:
            from retained_will_blackbox import connect_packet
            result.sendall(connect_packet(name, clean, keepalive, will))
            self.assertEqual(receive(result), (0x20, bytes([int(present), 0])))
        return result

    def connack(self, sock, code=0, present=False):
        header, data = receive(sock)
        self.assertEqual(header, 0x20)
        self.assertEqual(data[:2], bytes([int(present), code]))
        values, end = readprops(data, 2)
        self.assertEqual(end, len(data))
        return values

    def closed(self, sock):
        try:
            self.assertEqual(sock.recv(1), b'')
        except (ConnectionResetError, BrokenPipeError):
            pass

    def terminal(self, sock, reason):
        header, body = receive(sock)
        self.assertEqual(header, 0xe0)
        self.assertEqual(body[0] if body else 0, reason)
        if len(body) > 1:
            _, end = readprops(body, 1)
            self.assertEqual(end, len(body))
        self.closed(sock)

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

    def disconnect(self, sock, version=5, expiry=None, reason=0):
        properties = [] if expiry is None else [(0x11, expiry)]
        sock.sendall(frame(0xe0, bytes([reason]) + props(properties)) if version == 5 else frame(0xe0))
        self.closed(sock)
        sock.close()

    def subscribe(self, sock, filters, version=5, codes=None, properties=(), packet_id=1):
        body = struct.pack('!H', packet_id)
        if version == 5:
            body += props(properties)
        body += b''.join(string(topic) + bytes([options]) for topic, options in filters)
        sock.sendall(frame(0x82, body))
        header, response = receive(sock)
        self.assertEqual(header, 0x90)
        self.assertEqual(response[:2], struct.pack('!H', packet_id))
        offset = 2
        if version == 5:
            _, offset = readprops(response, offset)
        self.assertEqual(list(response[offset:]), [option & 3 for _, option in filters] if codes is None else codes)

    def publish(self, sock, topic, payload, qos=1, packet_id=9, properties=(), retain=False, version=5):
        if version == 5:
            wire = publication5(topic, payload, qos, packet_id, properties, retain)
        else:
            from retained_will_blackbox import publication
            wire = publication(topic, payload, qos, packet_id, retain)
        sock.sendall(wire)
        if qos:
            if version == 5:
                parse_ack5(receive(sock), 0x40 if qos == 1 else 0x50, packet_id)
            else:
                self.assertEqual(receive(sock), (0x40 if qos == 1 else 0x50, struct.pack('!H', packet_id)))
            if qos == 2:
                sock.sendall(ack(0x62, packet_id))
                if version == 5:
                    parse_ack5(receive(sock), 0x70, packet_id)
                else:
                    self.assertEqual(receive(sock), (0x70, struct.pack('!H', packet_id)))
        else:
            self.barrier(sock)

    def complete(self, sock, message, version=5, barrier=False):
        qos, packet_id = message[2:4]
        if qos == 1:
            sock.sendall(ack(0x40, packet_id))
        elif qos == 2:
            sock.sendall(ack(0x50, packet_id))
            if version == 5:
                parse_ack5(receive(sock), 0x62, packet_id)
            else:
                self.assertEqual(receive(sock), (0x62, struct.pack('!H', packet_id)))
            sock.sendall(ack(0x70, packet_id))
        if barrier:
            self.barrier(sock)

    def message(self, sock, topic, payload, qos=1, properties=(), version=5,
                retain=False, dup=False, complete=True):
        value = parse5(receive(sock)) if version == 5 else parse_publish(receive(sock)) + ([],)
        self.assertEqual(value[:3], (topic, payload, qos))
        self.assertEqual(value[4:7], (dup, retain, list(properties)))
        if complete:
            self.complete(sock, value, version)
        return value

    def require_sqlite(self):
        if not self.sqlite:
            self.skipTest('SQLite process persistence only')

    def snapshot(self):
        with closing(sqlite3.connect(self.broker.db)) as db:
            return json.loads(db.execute('SELECT data FROM broker_snapshot WHERE id=1').fetchone()[0])

    def wait_snapshot(self, predicate, timeout=3):
        # TCP connect readiness does not imply that the startup worker has committed.
        # Observe SQL only: no MQTT request can lazily expire/reconcile state for us.
        deadline = time.monotonic() + timeout
        value = self.snapshot()
        while not predicate(value) and time.monotonic() < deadline:
            time.sleep(.02)
            value = self.snapshot()
        self.assertTrue(predicate(value), 'committed snapshot condition not reached')
        return value

    def test_connack_capabilities_fragmentation_and_default_expiry(self):
        self.start('--max-inflight', '3', '--max-packet-size', '65536')
        sock = self.raw()
        packet = connect5('fragmented', properties=[(0x17, 0), (0x19, 1), (0x22, 8), (0x26, ('k', 'v'))])
        for value in packet:
            sock.sendall(bytes([value]))
        capabilities = dict(self.connack(sock))
        self.assertEqual(capabilities[0x21], 3)
        self.assertEqual(capabilities[0x27], 65536)
        self.assertEqual(capabilities.get(0x22, 0), 0)
        self.assertEqual(capabilities[0x2a], 0)
        self.assertEqual(capabilities[0x29], 0)
        self.assertNotIn(0x1f, capabilities)
        self.disconnect(sock)
        self.client('fragmented', clean=False, present=False)

    def test_mixed_versions_qos_matrix_and_metadata_bridge(self):
        self.start()
        for source_version in (4, 5):
            for target_version in (4, 5):
                for source_qos in range(3):
                    for grant in range(3):
                        name = f'v{source_version}-{target_version}-{source_qos}-{grant}'
                        with self.subTest(name=name):
                            sub = self.client('sub-' + name, version=target_version)
                            source = self.client('src-' + name, version=source_version)
                            topic = 'matrix/' + name
                            self.subscribe(sub, [(topic, grant)], target_version)
                            metadata = METADATA if source_version == 5 else []
                            self.publish(source, topic, name.encode(), source_qos, properties=metadata, version=source_version)
                            self.message(sub, topic, name.encode(), min(source_qos, grant),
                                         metadata if target_version == 5 else [], target_version)
                            self.disconnect(source, source_version)
                            self.disconnect(sub, target_version)

    def test_incoming_qos2_duplicate_retains_original_metadata_and_retained_replay(self):
        self.start()
        sub = self.client('metadata-live')
        old = self.client('metadata-v311', version=4)
        source = self.client('metadata-publisher')
        for sock, version in ((sub, 5), (old, 4)):
            self.subscribe(sock, [('metadata/value', 1)], version)
        source.sendall(publication5('metadata/value', b'original', 2, 71, METADATA, True))
        parse_ack5(receive(source), 0x50, 71)
        source.sendall(publication5('metadata/value', b'poison', 2, 71, [(3, 'poison')], True, True))
        parse_ack5(receive(source), 0x50, 71)
        self.quiet(sub)
        source.sendall(ack(0x62, 71))
        parse_ack5(receive(source), 0x70, 71)
        self.message(sub, 'metadata/value', b'original', properties=METADATA)
        self.message(old, 'metadata/value', b'original', version=4)
        source.sendall(ack(0x62, 71))
        parse_ack5(receive(source), 0x70, 71, 0x92)
        replay = self.client('metadata-replay')
        self.subscribe(replay, [('metadata/#', 2)])
        self.message(replay, 'metadata/value', b'original', 2, METADATA, retain=True)
        self.quiet(sub)

    def test_property_length_127_128_unicode_and_coalesced_control(self):
        self.start()
        sub = self.client('property-length-target')
        self.subscribe(sub, [('properties/boundary', 1)])
        source = self.client('property-length-source')
        values = [[(0x26, ('k', 'v' * count))] for count in (121, 122)]
        self.assertEqual(len(b''.join(prop(k, v) for k, v in values[0])), 127)
        self.assertEqual(len(b''.join(prop(k, v) for k, v in values[1])), 128)
        values.append([(0x26, ('键', '温度' * 70)), (0x26, ('键', '第二个值'))])
        for index, metadata in enumerate(values):
            with self.subTest(index=index):
                source.sendall(publication5('properties/boundary', b'value', 1, 29, metadata) + frame(0xc0))
                parse_ack5(receive(source), 0x40, 29)
                self.assertEqual(receive(source), (0xd0, b''))
                self.message(sub, 'properties/boundary', b'value', properties=metadata)

    def test_will_metadata_normal_disconnect_and_disconnect_with_will(self):
        self.start()
        sub = self.client('will-observer')
        self.subscribe(sub, [('will/#', 1)])
        normal = self.client('will-normal', will=('will/normal', b'no', 1, True), will_properties=METADATA)
        self.disconnect(normal)
        self.quiet(sub)
        source = self.client('will-special', will=('will/special', b'yes', 1, True),
                             will_properties=METADATA + [(0x18, 0)])
        self.disconnect(source, reason=4)
        self.message(sub, 'will/special', b'yes', properties=METADATA)
        replay = self.client('will-replay')
        self.subscribe(replay, [('will/#', 1)])
        self.message(replay, 'will/special', b'yes', properties=METADATA, retain=True)

    def test_clean_start_independent_from_expiry_and_zero_resume(self):
        self.start()
        source = self.client('expiry-source')
        first = self.client('expiry-owned', clean=True, expiry=30)
        self.subscribe(first, [('expiry/offline', 1)])
        self.disconnect(first)
        self.publish(source, 'expiry/offline', b'queued')
        resumed = self.client('expiry-owned', clean=False, expiry=0, present=True)
        self.message(resumed, 'expiry/offline', b'queued')
        self.disconnect(resumed)
        empty = self.client('expiry-owned', clean=False, expiry=30, present=False)
        self.subscribe(empty, [('expiry/offline', 1)])
        self.disconnect(empty)
        reset = self.client('expiry-owned', clean=True, expiry=30, present=False)
        self.publish(source, 'expiry/offline', b'not subscribed')
        self.quiet(reset)
        self.disconnect(reset)
        self.client('expiry-owned', clean=False, expiry=30, present=True)

    def test_finite_expiry_removes_subscription_outbox_and_frees_client_id(self):
        self.start()
        source = self.client('finite-source')
        target = self.client('finite-target', expiry=1)
        self.subscribe(target, [('finite/queued', 1)])
        self.disconnect(target)
        self.publish(source, 'finite/queued', b'must expire')
        time.sleep(1.2)
        restarted = self.client('finite-target', clean=False, expiry=30, present=False)
        self.publish(source, 'finite/queued', b'old subscription gone')
        self.quiet(restarted)

    def test_online_finite_session_does_not_expire_and_takeover_has_reason(self):
        self.start()
        owner = self.client('online-owner', expiry=1)
        self.subscribe(owner, [('online/#', 1)])
        time.sleep(1.2)
        self.barrier(owner)
        replacement = self.client('online-owner', clean=False, expiry=1, present=True)
        self.terminal(owner, 0x8e)
        source = self.client('online-source')
        self.publish(source, 'online/value', b'still subscribed')
        self.message(replacement, 'online/value', b'still subscribed')

    def test_disconnect_expiry_override_and_zero_to_positive_is_protocol_error(self):
        self.start()
        source = self.client('override-source')
        target = self.client('override-target', expiry=30)
        self.subscribe(target, [('override/#', 1)])
        self.disconnect(target, expiry=0)
        self.client('override-target', clean=False, expiry=30, present=False)
        illegal = self.client('override-zero', expiry=0)
        illegal.sendall(frame(0xe0, b'\0' + props([(0x11, 1)])))
        self.terminal(illegal, 0x82)
        finite = self.client('override-short', expiry=30)
        self.subscribe(finite, [('override/#', 1)])
        self.disconnect(finite, expiry=1)
        self.publish(source, 'override/value', b'expired')
        time.sleep(1.2)
        self.client('override-short', clean=False, expiry=30, present=False)

    def test_receive_maximum_qos1_negative_puback_releases_and_controls_progress(self):
        self.start()
        sub = self.client('receive-one', maximum=1)
        self.subscribe(sub, [('receive/#', 1)])
        source = self.client('receive-source')
        for index in range(2):
            self.publish(source, 'receive/value', str(index).encode())
        first = self.message(sub, 'receive/value', b'0', complete=False)
        self.barrier(sub)
        self.quiet(sub)
        sub.sendall(ack5(0x40, first[3], 0x80, form='reason'))
        second = self.message(sub, 'receive/value', b'1')
        self.assertNotEqual(first[3], second[3])
        self.barrier(sub)

    def test_receive_maximum_qos2_held_through_pubrec_until_pubcomp(self):
        self.start()
        sub = self.client('receive-qos2', maximum=1)
        self.subscribe(sub, [('receive/qos2', 2)])
        source = self.client('receive-qos2-source')
        for value in (b'first', b'second'):
            self.publish(source, 'receive/qos2', value, 2)
        first = self.message(sub, 'receive/qos2', b'first', 2, complete=False)
        sub.sendall(ack5(0x50, first[3], form='short'))
        parse_ack5(receive(sub), 0x62, first[3])
        self.barrier(sub)
        self.quiet(sub)
        sub.sendall(ack5(0x70, first[3], form='reason'))
        self.message(sub, 'receive/qos2', b'second', 2)

    def test_negative_pubrec_releases_without_pubrel_or_retransmission(self):
        self.start()
        sub = self.client('negative-rec', expiry=30, maximum=1)
        self.subscribe(sub, [('negative/#', 2)])
        source = self.client('negative-source')
        self.publish(source, 'negative/value', b'rejected', 2)
        first = self.message(sub, 'negative/value', b'rejected', 2, complete=False)
        self.publish(source, 'negative/value', b'next', 2)
        sub.sendall(ack5(0x50, first[3], 0x87))
        self.message(sub, 'negative/value', b'next', 2)
        self.barrier(sub)
        self.disconnect(sub)
        resumed = self.client('negative-rec', clean=False, expiry=30, present=True)
        self.barrier(resumed)
        self.quiet(resumed)

    def test_reconnect_lower_receive_maximum_gates_existing_pending_replay(self):
        self.start()
        sub = self.client('replay-lower', expiry=30, maximum=2)
        self.subscribe(sub, [('replay/lower', 1)])
        source = self.client('replay-lower-source')
        for index in range(3):
            self.publish(source, 'replay/lower', str(index).encode())
        first = self.message(sub, 'replay/lower', b'0', complete=False)
        second = self.message(sub, 'replay/lower', b'1', complete=False)
        self.disconnect(sub)
        resumed = self.client('replay-lower', clean=False, expiry=30, maximum=1, present=True)
        replay1 = self.message(resumed, 'replay/lower', b'0', dup=True, complete=False)
        self.assertEqual(replay1[3], first[3])
        self.barrier(resumed)
        self.quiet(resumed)
        resumed.sendall(ack(0x40, replay1[3]))
        replay2 = self.message(resumed, 'replay/lower', b'1', dup=True, complete=False)
        self.assertEqual(replay2[3], second[3])
        resumed.sendall(ack(0x40, replay2[3]))
        self.message(resumed, 'replay/lower', b'2')

    def test_awaitcomp_control_bypasses_blocked_publish_without_creating_credit(self):
        self.start()
        sub = self.client('replay-control', expiry=30, maximum=3)
        self.subscribe(sub, [('replay/control', 2)])
        source = self.client('replay-control-source')
        for index in range(3):
            self.publish(source, 'replay/control', str(index).encode(), 2)
        messages = [self.message(sub, 'replay/control', str(index).encode(), 2, complete=False) for index in range(3)]
        sub.sendall(ack(0x50, messages[2][3]))
        parse_ack5(receive(sub), 0x62, messages[2][3])
        self.disconnect(sub)
        resumed = self.client('replay-control', clean=False, expiry=30, maximum=1, present=True)
        first = self.message(resumed, 'replay/control', b'0', 2, dup=True, complete=False)
        parse_ack5(receive(resumed), 0x62, messages[2][3])
        resumed.sendall(ack(0x70, messages[2][3]))
        self.barrier(resumed)
        self.quiet(resumed)
        self.complete(resumed, first)
        self.message(resumed, 'replay/control', b'1', 2, dup=True)

    def test_inbound_receive_maximum_qos2_duplicate_and_limit_reason(self):
        self.start('--max-inflight', '1')
        source = self.client('inbound-limit')
        source.sendall(publication5('inbound/value', b'one', 2, 1))
        parse_ack5(receive(source), 0x50, 1)
        source.sendall(publication5('inbound/value', b'one', 2, 1, dup=True))
        parse_ack5(receive(source), 0x50, 1)
        source.sendall(publication5('inbound/value', b'two', 2, 2))
        self.terminal(source, 0x93)
        healthy = self.client('inbound-healthy')
        for identifier in (1, 2):
            healthy.sendall(publication5('inbound/value', b'ok', 2, identifier))
            parse_ack5(receive(healthy), 0x50, identifier)
            healthy.sendall(ack(0x62, identifier))
            parse_ack5(receive(healthy), 0x70, identifier)

    def test_negative_wrong_stage_ack_does_not_complete_or_free_original_exchange(self):
        self.start()
        target = self.client('wrong-stage-five', expiry=30, maximum=1)
        self.subscribe(target, [('wrong-stage/value', 2)])
        source = self.client('wrong-stage-source')
        self.publish(source, 'wrong-stage/value', b'pending receipt', 2, properties=METADATA)
        first = self.message(target, 'wrong-stage/value', b'pending receipt', 2, METADATA, complete=False)
        target.sendall(ack5(0x40, first[3], 0x87))
        self.terminal(target, 0x82)
        resumed = self.client('wrong-stage-five', clean=False, expiry=30, maximum=1, present=True)
        retry = self.message(resumed, 'wrong-stage/value', b'pending receipt', 2, METADATA,
                             dup=True, complete=False)
        self.assertEqual(retry[3], first[3])
        self.complete(resumed, retry, barrier=True)

    def test_reserved_subscription_bits_malformed_and_invalid_values_protocol_error(self):
        self.start()
        cases = [(0x41, 'options/topic', 0x81), (0x81, 'options/topic', 0x81),
                 (3, 'options/topic', 0x82), (0x31, 'options/topic', 0x82),
                 (5, '$share/group/options', 0x82)]
        for index, (option, topic, reason) in enumerate(cases):
            with self.subTest(option=option, topic=topic):
                source = self.client('bad-options-' + str(index))
                source.sendall(frame(0x82, b'\0\1' + props() + string(topic) + bytes([option])))
                self.terminal(source, reason)

    def test_unknown_packet_identifier_ack_reasons_and_short_forms(self):
        self.start()
        source = self.client('unknown-identifier')
        source.sendall(ack5(0x50, 501, form='short'))
        parse_ack5(receive(source), 0x62, 501, 0x92)
        source.sendall(ack5(0x62, 502, form='reason'))
        parse_ack5(receive(source), 0x70, 502, 0x92)
        self.barrier(source)
        source.sendall(frame(0xf0, b'\x19'))
        self.terminal(source, 0x82)

    def test_unsupported_subscribe_features_are_per_filter_reason_codes(self):
        self.start()
        sub = self.client('subscription-options')
        self.subscribe(sub, [('options/good', 1), ('options/no-local', 5),
                             ('options/rap', 9), ('options/retain-handling', 17)],
                       codes=[1, 0x83, 0x83, 0x83])
        source = self.client('subscription-options-source')
        for topic in ('options/no-local', 'options/rap', 'options/retain-handling', 'options/identifier'):
            self.publish(source, topic, b'not subscribed')
        self.quiet(sub)
        self.publish(source, 'options/good', b'yes')
        self.message(sub, 'options/good', b'yes')
        sub.sendall(frame(0xa2, b'\0\x09' + props() + string('options/good') + string('options/missing')))
        header, body = receive(sub)
        self.assertEqual(header, 0xb0)
        self.assertEqual(body[:2], b'\0\x09')
        _, offset = readprops(body, 2)
        self.assertEqual(body[offset:], b'\0\x11')

    def test_unavailable_shared_and_subscription_identifier_disconnect_without_partial_grants(self):
        self.start()
        source = self.client('unavailable-source')
        for name, properties, filters, reason in [
            ('sub-id', [(0x0b, 1)], [('feature/unaccepted', 1)], 0xa1),
            ('shared', [], [('feature/unaccepted', 1), ('$share/group/feature/#', 1)], 0x9e),
        ]:
            with self.subTest(feature=name):
                target = self.client('feature-' + name, expiry=30)
                topic = 'feature/existing/' + name
                self.subscribe(target, [(topic, 1)])
                target.sendall(frame(0x82, b'\0\2' + props(properties)
                                     + b''.join(string(f) + bytes([q]) for f, q in filters)))
                self.terminal(target, reason)
                resumed = self.client('feature-' + name, clean=False, expiry=30, present=True)
                self.publish(source, 'feature/unaccepted', b'not granted')
                self.quiet(resumed)
                self.publish(source, topic, b'prior grant remains')
                self.message(resumed, topic, b'prior grant remains')
                if self.sqlite:
                    record = next(s for s in self.snapshot()['sessions'] if s['client_id'] == 'feature-' + name)
                    self.assertEqual(record['subscriptions'], [[topic, 1]])

    def test_duplicate_zero_wrong_context_and_unknown_properties_fail(self):
        self.start()
        bad_connect = [
            [(0x21, 0)], [(0x21, 1), (0x21, 2)], [(0x11, 1), (0x11, 2)],
            [(0x27, 0)], [(0x25, 1)], [(0x17, 2)],
        ]
        for index, values in enumerate(bad_connect):
            with self.subTest(connect=values):
                sock = self.raw()
                sock.sendall(connect5('invalid-' + str(index), properties=values))
                packet = receive(sock) if self._has_packet(sock) else None
                if packet is not None:
                    self.assertEqual(packet[0], 0x20)
                    self.assertGreaterEqual(packet[1][1], 0x80)
                self.closed(sock)
        cases = [([(0x21, 1)], 0x81), ([(0x23, 0)], 0x94),
                 ([(0x23, 1)], 0x94), ([(0x0b, 1)], 0x82),
                 ([(3, 'a'), (3, 'b')], 0x82),
                 ([(8, 'bad/+')], 0x82)]
        for index, (values, reason) in enumerate(cases):
            with self.subTest(publish=values):
                sock = self.client('invalid-publish-' + str(index))
                sock.sendall(publication5('invalid/value', b'bad', properties=values))
                self.terminal(sock, reason)
        sock = self.client('unknown-property')
        sock.sendall(frame(0x30, string('invalid/value') + b'\1\x7f' + b'bad'))
        self.terminal(sock, 0x81)
        for alias in (0, 1):
            sock = self.client('empty-topic-alias-' + str(alias))
            sock.sendall(publication5('', b'bad', properties=[(0x23, alias)]))
            self.terminal(sock, 0x94)

    def _has_packet(self, sock):
        try:
            return bool(sock.recv(1, socket.MSG_PEEK))
        except ConnectionResetError:
            return False

    def test_noncanonical_truncated_and_invalid_utf8_property_fields_disconnect(self):
        self.start()
        bad_blocks = [b'\x80\0', b'\3\1', b'\2\3\xff',
                      b'\4\3\0\1\0', b'\3\x81\0\1']
        for index, block in enumerate(bad_blocks):
            with self.subTest(block=block):
                sock = self.client('malformed-properties-' + str(index))
                sock.sendall(frame(0x30, string('malformed/value') + block))
                self.terminal(sock, 0x81)
        healthy = self.client('malformed-survivor')
        self.barrier(healthy)

    def test_oversize_packet_disconnect_reason_and_will(self):
        self.start('--max-packet-size', '128')
        observer = self.client('size-observer')
        self.subscribe(observer, [('size/will', 0)])
        owner = self.client('size-owner', will=('size/will', b'closed', 0, False))
        owner.sendall(publication5('size/too-large', b'x' * 200))
        self.terminal(owner, 0x95)
        self.message(observer, 'size/will', b'closed', 0)

    def test_unsupported_connect_properties_refuse_before_will_or_pipeline(self):
        self.start()
        observer = self.client('unsupported-observer')
        self.subscribe(observer, [('unsupported/#', 1)])
        cases = [([(0x15, 'unsupported')], [], 0x8c),
                 ]
        for index, (values, will_values, code) in enumerate(cases):
            sock = self.raw()
            sock.sendall(connect5('unsupported-' + str(index), properties=values,
                                   will=('unsupported/will', b'bad', 1, True), will_properties=will_values)
                         + publication5('unsupported/pipeline', b'bad', retain=True))
            self.connack(sock, code)
            self.closed(sock)
        self.quiet(observer)
        replay = self.client('unsupported-replay')
        self.subscribe(replay, [('unsupported/#', 1)])
        self.quiet(replay)

    def size_case(self, maximum):
        self.broker.stop()
        directory = Path(self.directory.name) / str(maximum)
        directory.mkdir()
        self.broker = FeatureBroker(directory, self.sqlite)
        self.addCleanup(self.broker.stop)
        self.start('--max-packet-size', str(maximum))

    def test_oversize_legacy_queue_refuses_v5_resume_but_explicit_reset_is_allowed(self):
        self.require_sqlite()
        from retained_will_blackbox import publication
        for maximum, payload_length in ((64, 57), (129, 122)):
            with self.subTest(maximum=maximum):
                self.size_case(maximum)
                payload = b'x' * payload_length
                self.assertEqual(len(publication('s', payload, 1, 9)), maximum)
                self.assertGreater(len(publication5('s', payload, 1, 9)), maximum)
                target = self.client('queue-size', version=4, clean=False)
                self.subscribe(target, [('s', 1)], version=4)
                self.disconnect(target, version=4)
                source = self.client('queue-source', version=4)
                self.publish(source, 's', payload, version=4)
                rejected = self.raw()
                rejected.sendall(connect5('queue-size', clean=False, properties=[(0x11, 30)]))
                self.connack(rejected, 0x83)
                self.closed(rejected)
                if self.sqlite:
                    record = next(s for s in self.snapshot()['sessions'] if s['client_id'] == 'queue-size')
                    self.assertEqual(len(record['queued']), 1)
                reset = self.client('queue-size', clean=True, expiry=30, present=False)
                self.barrier(reset)
                self.quiet(reset)

    def test_offline_last_v5_recipient_is_preflighted_before_legacy_source_ack(self):
        from retained_will_blackbox import publication
        self.start('--max-packet-size', '64')
        target = self.client('offline-v5-size', expiry=60)
        self.subscribe(target, [('a', 1)])
        self.disconnect(target)
        source = self.client('offline-old-source', version=4)
        wire = publication('a', b'v' * 57, 1, 9)
        self.assertEqual(len(wire), 64)
        source.sendall(wire)
        self.closed(source)
        if self.sqlite:
            record = next(s for s in self.snapshot()['sessions'] if s['client_id'] == 'offline-v5-size')
            self.assertEqual(record['queued'], [])
            self.assertEqual(record['outgoing'], [])
            self.assertEqual(record['subscriptions'], [['a', 1]])
        resumed = self.client('offline-v5-size', clean=False, expiry=60, present=True)
        self.barrier(resumed)
        self.quiet(resumed)
        valid = self.client('offline-valid-source')
        self.publish(valid, 'a', b'after failure')
        self.message(resumed, 'a', b'after failure')

    def test_identity_denial_precedes_oversize_resume_capability(self):
        self.require_sqlite()
        from retained_will_blackbox import publication
        self.broker = AuthBroker(self.directory.name, self.sqlite)
        self.addCleanup(self.broker.stop)
        self.start('--max-packet-size', '64')
        owner = self.raw()
        owner.sendall(auth311('cap-owned', 'alice', clean=False))
        self.assertEqual(receive(owner), (0x20, b'\0\0'))
        self.subscribe(owner, [('allowed/s', 1)], version=4)
        self.disconnect(owner, version=4)
        source = self.raw()
        source.sendall(auth311('cap-source', 'alice'))
        self.assertEqual(receive(source), (0x20, b'\0\0'))
        wire = publication('allowed/s', b'x' * 49, 1, 9)
        self.assertEqual(len(wire), 64)
        source.sendall(wire)
        self.assertEqual(receive(source), (0x40, b'\0\x09'))
        for user, code in (('bob', 0x87), ('alice', 0x83)):
            rejected = self.raw()
            rejected.sendall(connect5('cap-owned', clean=False, properties=[(0x11, 30)],
                                      username=user, password=PASSWORD))
            self.connack(rejected, code)
            self.closed(rejected)
        if self.sqlite:
            record = next(s for s in self.snapshot()['sessions'] if s['client_id'] == 'cap-owned')
            self.assertEqual(record['owner'], 'user:alice')
            self.assertEqual(len(record['queued']), 1)

    def test_oversize_legacy_replay_refusal_preserves_ids_and_v311_drain(self):
        self.require_sqlite()
        for maximum, payload_length in ((64, 57), (129, 122)):
            with self.subTest(maximum=maximum):
                self.size_case(maximum)
                payload = b'y' * payload_length
                target = self.client('replay-size', version=4, clean=False)
                self.subscribe(target, [('s', 1)], version=4)
                source = self.client('replay-source', version=4)
                self.publish(source, 's', payload, version=4)
                first = self.message(target, 's', payload, version=4, complete=False)
                self.disconnect(target, version=4)
                rejected = self.raw()
                rejected.sendall(connect5('replay-size', clean=False, properties=[(0x11, 30)]))
                self.connack(rejected, 0x83)
                self.closed(rejected)
                record = next(s for s in self.snapshot()['sessions'] if s['client_id'] == 'replay-size')
                self.assertEqual(record['session_expiry_interval'], 0xffffffff)
                self.assertEqual(record['outgoing'][0]['packet_id'], first[3])
                resumed = self.client('replay-size', version=4, clean=False, present=True)
                replay = self.message(resumed, 's', payload, version=4, dup=True)
                self.assertEqual(replay[3], first[3])
                self.barrier(resumed)
                self.disconnect(resumed, version=4)
                self.client('replay-size', clean=False, expiry=30, present=True)

    def test_oversize_v5_delivery_and_retained_replay_abort_without_partial_effects(self):
        from retained_will_blackbox import publication
        self.start('--max-packet-size', '64')
        legacy = self.client('atomic-old', version=4)
        modern = self.client('atomic-five')
        self.subscribe(legacy, [('s', 1)], version=4)
        self.subscribe(modern, [('s', 1)])
        source = self.client('atomic-source', version=4)
        source.sendall(publication('s', b'z' * 57, 1, 9, True))
        self.closed(source)
        self.quiet(legacy)
        self.quiet(modern)
        replay = self.client('atomic-replay')
        self.subscribe(replay, [('s', 1)])
        self.quiet(replay)
        for sock, version in ((legacy, 4), (modern, 5), (replay, 5)):
            self.disconnect(sock, version)
        store = self.client('store-source', version=4)
        self.publish(store, 's', b'z' * 57, retain=True, version=4)
        reader = self.client('too-large-retained', expiry=30)
        reader.sendall(frame(0x82, b'\0\1' + props() + string('s') + b'\1'))
        self.terminal(reader, 0x97)
        if self.sqlite:
            record = next(s for s in self.snapshot()['sessions'] if s['client_id'] == 'too-large-retained')
            self.assertEqual(record['subscriptions'], [])
            self.assertEqual(record['queued'], [])
        old_reader = self.client('legacy-retained', version=4)
        self.subscribe(old_reader, [('s', 1)], version=4)
        self.message(old_reader, 's', b'z' * 57, version=4, retain=True)

    def test_refusal_respects_supplied_maximum_packet_size_and_empty_client_id(self):
        self.start()
        for maximum in (1, 4):
            sock = self.raw()
            sock.sendall(connect5('tiny-' + str(maximum), properties=[(0x27, maximum)]))
            self.closed(sock)
        boundary = self.raw()
        boundary.sendall(connect5('tiny-five', properties=[(0x27, 5)]))
        self.connack(boundary, 0x95)
        self.closed(boundary)
        empty = self.raw()
        empty.sendall(connect5(''))
        assigned = [v for k, v in self.connack(empty) if k == 0x12]
        self.assertEqual(len(assigned), 1)
        self.assertTrue(assigned[0])
        self.barrier(empty)

    def test_expired_authenticated_owner_releases_id_without_old_payload(self):
        self.broker = AuthBroker(self.directory.name, self.sqlite)
        self.addCleanup(self.broker.stop)
        self.start()
        alice = self.client('expired-identity', expiry=1, username='alice', password=PASSWORD)
        self.subscribe(alice, [('allowed/#', 1)])
        self.disconnect(alice)
        source = self.client('expired-identity-source', username='alice', password=PASSWORD)
        self.publish(source, 'allowed/private', b'must not leak')
        time.sleep(1.2)
        bob = self.client('expired-identity', clean=False, expiry=30, username='bob', password=PASSWORD)
        self.barrier(bob)
        self.quiet(bob)
        self.subscribe(bob, [('other/#', 1)])
        self.publish(bob, 'other/own', b'new owner')
        self.message(bob, 'other/own', b'new owner')

    def test_auth_acl_owner_boundaries_cross_version_and_reason_codes(self):
        self.broker = AuthBroker(self.directory.name, self.sqlite)
        self.addCleanup(self.broker.stop)
        self.start()
        wrong = self.raw()
        wrong.sendall(connect5('wrong', username='alice', password=b'wrong'))
        self.connack(wrong, 0x86)
        self.closed(wrong)
        owner = self.client('owned-shared', expiry=30, username='alice', password=PASSWORD)
        self.subscribe(owner, [('allowed/#', 1)])
        for version in (4, 5):
            other = self.raw()
            other.sendall(auth311('owned-shared', 'bob') if version == 4 else
                          connect5('owned-shared', username='bob', password=PASSWORD))
            if version == 4:
                self.assertEqual(receive(other), (0x20, b'\0\5'))
            else:
                self.connack(other, 0x87)
            self.closed(other)
        self.barrier(owner)
        forbidden = self.client('forbidden-publisher', username='alice', password=PASSWORD)
        forbidden.sendall(publication5('other/no', b'denied', 1, 9, METADATA, True))
        self.terminal(forbidden, 0x87)
        self.subscribe(owner, [('other/#', 1)], codes=[0x87])

    def test_keepalive_timeout_has_v5_reason_and_will(self):
        self.start()
        observer = self.client('timeout-observer')
        self.subscribe(observer, [('timeout/will', 1)])
        owner = self.client('timeout-owner', keepalive=1,
                            will=('timeout/will', b'timed out', 1, False), will_properties=METADATA)
        self.terminal(owner, 0x8d)
        self.message(observer, 'timeout/will', b'timed out', properties=METADATA)

    def test_offline_metadata_and_cross_version_resume_survive_process_crash(self):
        self.require_sqlite()
        self.start()
        sub = self.client('durable-five', expiry=0xffffffff)
        self.subscribe(sub, [('durable/#', 2)])
        source = self.client('durable-source')
        self.publish(source, 'durable/value', b'first', 2, properties=METADATA)
        first = self.message(sub, 'durable/value', b'first', 2, METADATA, complete=False)
        self.disconnect(sub)
        self.publish(source, 'durable/value', b'offline', 1, properties=METADATA)
        self.broker.restart_after_crash()
        resumed = self.client('durable-five', version=4, clean=False, present=True)
        retry = self.message(resumed, 'durable/value', b'first', 2, version=4, dup=True, complete=False)
        self.assertEqual(retry[3], first[3])
        offline = self.message(resumed, 'durable/value', b'offline', version=4, complete=False)
        self.complete(resumed, retry, 4)
        self.complete(resumed, offline, 4, barrier=True)
        self.disconnect(resumed, 4)
        self.client('durable-five', clean=False, expiry=30, present=True)

    def test_v311_delivery_and_input_retry_strip_only_wire_metadata_across_restart(self):
        self.require_sqlite()
        self.start()
        target = self.client('bridge-persistent-target', version=4, clean=False)
        self.subscribe(target, [('bridge/persistent', 2)], version=4)
        source = self.client('bridge-persistent-source', expiry=30)
        self.publish(source, 'bridge/persistent', b'old-v311-output', properties=METADATA)
        first = self.message(target, 'bridge/persistent', b'old-v311-output', version=4, complete=False)
        source.sendall(publication5('bridge/persistent', b'original-input', 2, 89, METADATA))
        parse_ack5(receive(source), 0x50, 89)
        self.disconnect(target, 4)
        self.disconnect(source)
        source = self.client('bridge-persistent-source', version=4, clean=False, present=True)
        from retained_will_blackbox import publication
        source.sendall(publication('bridge/persistent', b'duplicate-must-not-replace', 2, 89, dup=True))
        self.assertEqual(receive(source), (0x50, struct.pack('!H', 89)))
        source.sendall(ack(0x62, 89))
        self.assertEqual(receive(source), (0x70, struct.pack('!H', 89)))
        self.broker.restart_after_crash()
        target = self.client('bridge-persistent-target', clean=False, expiry=30, present=True)
        retry = self.message(target, 'bridge/persistent', b'old-v311-output', properties=METADATA, dup=True)
        self.assertEqual(retry[3], first[3])
        self.message(target, 'bridge/persistent', b'original-input', 2, METADATA)

    def test_offline_expiry_deadline_includes_downtime_and_timer_removes_state(self):
        self.require_sqlite()
        self.start()
        sub = self.client('durable-expiry', expiry=1)
        self.subscribe(sub, [('durable/expiry', 1)])
        self.disconnect(sub)
        source = self.client('expiry-publisher')
        self.publish(source, 'durable/expiry', b'queued')
        self.broker.stop(kill=True)
        time.sleep(1.2)
        self.broker.start()
        self.wait_snapshot(lambda value: 'durable-expiry' not in [s['client_id'] for s in value['sessions']])
        self.client('durable-expiry', clean=False, expiry=30, present=False)
        target = self.client('timer-expiry', expiry=1)
        self.subscribe(target, [('timer/#', 1)])
        self.disconnect(target)
        self.wait_snapshot(lambda value: 'timer-expiry' not in [s['client_id'] for s in value['sessions']])

    def test_active_crash_deadline_is_started_once_and_not_extended_by_restart(self):
        self.require_sqlite()
        self.start()
        self.client('active-crash', expiry=2)
        self.broker.restart_after_crash()
        ready = self.wait_snapshot(lambda value: any(s['client_id'] == 'active-crash' and s['expires_at'] is not None
                                                     for s in value['sessions']))
        first = next(s for s in ready['sessions'] if s['client_id'] == 'active-crash')
        deadline = first['expires_at']
        self.assertIsNotNone(deadline)
        self.broker.restart_after_crash()
        second = next(s for s in self.snapshot()['sessions'] if s['client_id'] == 'active-crash')
        self.assertEqual(second['expires_at'], deadline)
        time.sleep(max(0, deadline / 1000 - time.time()) + .2)
        self.client('active-crash', clean=False, expiry=2, present=False)

    def test_incoming_qos2_metadata_and_active_will_recover_after_crash(self):
        self.require_sqlite()
        self.start()
        sub = self.client('incoming-target', expiry=30)
        self.subscribe(sub, [('incoming/#', 2)])
        self.disconnect(sub)
        source = self.client('incoming-source', expiry=30,
                             will=('incoming/will', b'crash Will', 1, True), will_properties=METADATA)
        source.sendall(publication5('incoming/value', b'pending', 2, 99, METADATA, True))
        parse_ack5(receive(source), 0x50, 99)
        self.broker.restart_after_crash()
        resumed = self.client('incoming-target', clean=False, expiry=30, present=True)
        self.message(resumed, 'incoming/will', b'crash Will', 1, METADATA)
        source = self.client('incoming-source', clean=False, expiry=30, present=True)
        source.sendall(ack(0x62, 99))
        parse_ack5(receive(source), 0x70, 99)
        self.message(resumed, 'incoming/value', b'pending', 2, METADATA)
        replay = self.client('incoming-replay')
        self.subscribe(replay, [('incoming/#', 1)])
        seen = {}
        for _ in range(2):
            message = parse5(receive(replay))
            self.assertTrue(message[5])
            self.assertEqual(message[6], METADATA)
            seen[message[0]] = message[1]
            self.complete(replay, message)
        self.assertEqual(seen, {'incoming/value': b'pending', 'incoming/will': b'crash Will'})

    def test_negative_ack_completion_is_durable_and_never_replayed(self):
        self.require_sqlite()
        self.start()
        target = self.client('negative-durable', expiry=30, maximum=1)
        self.subscribe(target, [('negative/durable', 2)])
        source = self.client('negative-durable-source')
        self.publish(source, 'negative/durable', b'negative-comp', 2, properties=METADATA)
        first = self.message(target, 'negative/durable', b'negative-comp', 2, METADATA, complete=False)
        target.sendall(ack(0x50, first[3]))
        parse_ack5(receive(target), 0x62, first[3])
        target.sendall(ack5(0x70, first[3], 0x92))
        self.barrier(target)
        self.publish(source, 'negative/durable', b'negative-ack', properties=METADATA)
        second = self.message(target, 'negative/durable', b'negative-ack', properties=METADATA, complete=False)
        target.sendall(ack5(0x40, second[3], 0x87))
        self.barrier(target)
        self.disconnect(target)
        self.publish(source, 'negative/durable', b'only-new', properties=METADATA)
        self.broker.restart_after_crash()
        resumed = self.client('negative-durable', clean=False, expiry=30, maximum=1, present=True)
        self.message(resumed, 'negative/durable', b'only-new', properties=METADATA)
        self.barrier(resumed)
        self.quiet(resumed)

    def test_schema3_legacy_session_ids_and_pending_input_migrate_to_mqtt5(self):
        self.require_sqlite()
        self.start()
        target = self.client('legacy-target', version=4, clean=False)
        self.subscribe(target, [('legacy/#', 2)], version=4)
        source = self.client('legacy-source', version=4, clean=False)
        self.publish(source, 'legacy/retained', b'old inflight', 1, retain=True, version=4)
        original = self.message(target, 'legacy/retained', b'old inflight', version=4, complete=False)
        self.disconnect(target, 4)
        self.publish(source, 'legacy/offline', b'old queued', version=4)
        from retained_will_blackbox import publication
        source.sendall(publication('legacy/pending', b'old pending', 2, 72))
        self.assertEqual(receive(source), (0x50, struct.pack('!H', 72)))
        self.broker.stop(kill=True)
        with sqlite3.connect(self.broker.db) as db:
            current = json.loads(db.execute('SELECT data FROM broker_snapshot WHERE id=1').fetchone()[0])
            legacy = schema3_fixture(current)
            db.execute('UPDATE broker_snapshot SET data=? WHERE id=1', (json.dumps(legacy).encode(),))
        self.broker.start()
        target = self.client('legacy-target', clean=False, expiry=30, present=True)
        replay = self.message(target, 'legacy/retained', b'old inflight', dup=True)
        self.assertEqual(replay[3], original[3])
        self.message(target, 'legacy/offline', b'old queued')
        source = self.client('legacy-source', clean=False, expiry=30, present=True)
        source.sendall(ack(0x62, 72))
        parse_ack5(receive(source), 0x70, 72)
        self.message(target, 'legacy/pending', b'old pending', 2)
        self.assertEqual(self.snapshot()['schema_version'], 5)
        self.assertTrue(all(s['owner'] == 'anonymous' for s in self.snapshot()['sessions']))

    def test_schema4_missing_required_expiry_deadline_rejects_startup_without_reset(self):
        self.require_sqlite()
        self.start()
        target = self.client('required-deadline', expiry=30)
        self.subscribe(target, [('required/#', 1)])
        self.disconnect(target)
        self.broker.stop()
        with sqlite3.connect(self.broker.db) as db:
            original = db.execute('SELECT data FROM broker_snapshot WHERE id=1').fetchone()[0]
            current = schema4_fixture(json.loads(original))
            record = next(s for s in current['sessions'] if s['client_id'] == 'required-deadline')
            self.assertIsNotNone(record.pop('expires_at'))
            corrupt = json.dumps(current).encode()
            db.execute('UPDATE broker_snapshot SET data=? WHERE id=1', (corrupt,))
        self.broker.assert_start_fails()
        with sqlite3.connect(self.broker.db) as db:
            self.assertEqual(db.execute('SELECT data FROM broker_snapshot WHERE id=1').fetchone()[0], corrupt)
            db.execute('UPDATE broker_snapshot SET data=? WHERE id=1', (original,))
        self.broker.start()
        self.client('required-deadline', clean=False, expiry=30, present=True)

    def test_memory_positive_expiry_survives_disconnect_but_not_process_death(self):
        if self.sqlite:
            self.skipTest('volatile process-death behavior only')
        self.start()
        sub = self.client('memory-owned', expiry=30)
        self.subscribe(sub, [('memory/#', 1)])
        self.disconnect(sub)
        self.broker.restart_after_crash()
        self.client('memory-owned', clean=False, expiry=30, present=False)


if __name__ == '__main__':
    unittest.main(verbosity=2)
