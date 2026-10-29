#!/usr/bin/env python3
"""Finite passive-reader regressions for the conservative delivery wake hint.

After SUBACK a passive QoS0 recipient sends no packets before its expected data.
Publisher QoS acknowledgements establish accepted work; no recipient PING is used
as a drain barrier. Quiet checks are bounded observations, not commit proofs.
Memory and SQLite are separate executions; only legacy persistent reconnect needs SQL.
"""
import os
import socket
import struct
import tempfile
import time
import unittest

import mqtt5_blackbox as wire
from blackbox import frame, receive, string
from qos_blackbox import ack, parse_publish
from retained_will_blackbox import FeatureBroker


class WakeupBlackbox(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-phase3a-wakeup-')
        self.addCleanup(self.directory.cleanup)
        self.sqlite = bool(os.environ.get('RMQTT_TEST_STORAGE_PATH'))
        self.broker = FeatureBroker(self.directory.name, self.sqlite)
        self.addCleanup(self.broker.stop)

    def start(self, *args):
        self.broker.extra_args = ['--outbound-capacity', '4', '--max-inflight', '2',
                                 '--max-packet-size', '4096', '--max-sessions', '32',
                                 '--max-connections', '16', '--max-subscriptions', '8',
                                 '--write-timeout-ms', '2000'] + list(args)
        self.broker.start()

    def raw(self):
        result = self.broker.socket()
        result.settimeout(5)
        self.addCleanup(result.close)
        return result

    def passive_message(self, sock, topic, payload, qos=0, version=5, dup=False):
        message = wire.parse5(receive(sock)) if version == 5 else parse_publish(receive(sock))
        self.assertEqual(message[:3], (topic, payload, qos))
        self.assertEqual(message[4], dup)
        return message

    def finish_qos2(self, sock, message, version=5):
        sock.sendall(ack(0x50, message[3]))
        if version == 5:
            wire.parse_ack5(receive(sock), 0x62, message[3])
        else:
            self.assertEqual(receive(sock), (0x62, struct.pack('!H', message[3])))
        sock.sendall(ack(0x70, message[3]))

    def abort(self, sock):
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()

    def test_single_publication_drains_all_mixed_rap_qos0_copies_without_recipient_ping(self):
        self.start()
        target = self.client('mixed-rap-target', keepalive=0)
        self.subscribe(target, [('wake/#', 0), ('wake/+', 8),
                                ('wake/value', 0), ('+/value', 8)])
        idle = [self.client(f'idle-{v}', version=v, keepalive=0) for v in (4, 5)]
        # Bounded passive observation lets prior control traffic settle; it is not
        # claimed to prove every internal watch receiver has consumed its version.
        self.quiet(target)
        source = self.client('single-publisher', keepalive=0)
        self.publish(source, 'wake/value', b'one-publication-four-copies', qos=1, retain=True)
        messages = [self.passive_message(target, 'wake/value', b'one-publication-four-copies')
                    for _ in range(4)]
        self.assertEqual(sorted(message[5] for message in messages), [False, False, True, True])
        self.quiet(target)
        for sock in idle:
            self.quiet(sock)

    def test_paced_mixed_version_fanout_progresses_without_recipient_ping(self):
        self.start('--outbound-capacity', '1')
        targets = []
        for version in (4, 5):
            for index in range(2):
                target = self.client(f'active-{version}-{index}', version=version, keepalive=0)
                self.subscribe(target, [('paced/value', 0)], version=version)
                targets.append((target, version))
        idle = self.client('paced-idle', keepalive=0)
        source = self.client('paced-source', keepalive=0)
        for sequence in range(8):
            payload = b'ordered-' + sequence.to_bytes(2, 'big')
            self.publish(source, 'paced/value', payload, qos=1, packet_id=sequence + 1)
            # Drain each accepted publication before the next; a cap1 rejection
            # caused by offered load would not diagnose a missing wake.
            for target, version in targets:
                self.passive_message(target, 'paced/value', payload, version=version)
        for target, _ in targets:
            self.quiet(target)
        self.quiet(idle)

    def test_direct_idle_control_replies_and_unsubscribed_isolation(self):
        self.start()
        for version in (4, 5):
            with self.subTest(version=version):
                idle = self.client(f'empty-{version}', version=version, keepalive=0)
                self.barrier(idle)  # Deliberately verify direct reply when hintfalse.
                self.subscribe(idle, [('removed/value', 0)], version=version)
                body = b'\x00\x02' + (b'\x00' if version == 5 else b'') + string('removed/value')
                idle.sendall(frame(0xa2, body))
                if version == 5:
                    self.assertEqual(receive(idle), (0xb0, b'\x00\x02\x00\x00'))
                else:
                    self.assertEqual(receive(idle), (0xb0, b'\x00\x02'))
                source = self.client(f'isolated-source-{version}', version=version, keepalive=0)
                self.publish(source, 'removed/value', b'not-for-idle', version=version)
                self.quiet(idle)
                self.barrier(idle)

    def test_will_delay_tick_wakes_initially_empty_recipient_without_ping(self):
        self.start()
        target = self.client('will-delay-reader', keepalive=0)
        self.subscribe(target, [('tick/will', 0)])
        source = self.client('will-delay-source', expiry=60, keepalive=0,
                             will=('tick/will', b'delay-fired', 1, False),
                             will_properties=[(0x18, 1)])
        before = time.monotonic()
        self.abort(source)
        self.passive_message(target, 'tick/will', b'delay-fired')
        self.assertGreaterEqual(time.monotonic() - before, .9)
        self.quiet(target)

    def test_session_expiry_tick_caps_will_delay_and_wakes_legacy_reader(self):
        self.start()
        target = self.client('expiry-legacy-reader', version=4, keepalive=0)
        self.subscribe(target, [('tick/expiry', 0)], version=4)
        source = self.client('short-session-source', expiry=1, keepalive=0,
                             will=('tick/expiry', b'session-ended', 1, False),
                             will_properties=[(0x18, 60)])
        before = time.monotonic()
        self.abort(source)
        self.passive_message(target, 'tick/expiry', b'session-ended', version=4)
        self.assertGreaterEqual(time.monotonic() - before, .9)
        self.quiet(target)
        self.client('short-session-source', clean=False, expiry=60, present=False, keepalive=0)

    def test_pubcomp_releases_full_receive_window_without_ping(self):
        self.start('--max-inflight', '1')
        target = self.client('window-reader', expiry=60, maximum=1, keepalive=0)
        self.subscribe(target, [('window/value', 2)])
        source = self.client('window-source', keepalive=0)
        self.publish(source, 'window/value', b'first', qos=2, packet_id=10)
        first = self.passive_message(target, 'window/value', b'first', qos=2)
        self.publish(source, 'window/value', b'second', qos=2, packet_id=11)
        self.quiet(target)  # ReceiveMaximum1 must still block second PUBLISH.
        self.finish_qos2(target, first)
        second = self.passive_message(target, 'window/value', b'second', qos=2)
        self.finish_qos2(target, second)
        self.quiet(target)

    def reconnect_qos2(self, version):
        target = self.client(f'reconnect-{version}', version=version, clean=False,
                             expiry=60, maximum=1, keepalive=0)
        self.subscribe(target, [('resume/value', 2)], version=version)
        source = self.client(f'reconnect-source-{version}', version=version, keepalive=0)
        self.publish(source, 'resume/value', b'original', qos=2, packet_id=20, version=version)
        original = self.passive_message(target, 'resume/value', b'original', qos=2, version=version)
        self.publish(source, 'resume/value', b'queued', qos=2, packet_id=21, version=version)
        self.abort(target)
        target = self.client(f'reconnect-{version}', version=version, clean=False,
                             expiry=60, maximum=1, present=True, keepalive=0)
        replay = self.passive_message(target, 'resume/value', b'original', qos=2,
                                      version=version, dup=True)
        self.assertEqual(replay[3], original[3])
        self.finish_qos2(target, replay, version)
        queued = self.passive_message(target, 'resume/value', b'queued', qos=2, version=version)
        self.finish_qos2(target, queued, version)
        self.quiet(target)

    def test_v5_reconnect_replays_qos2_and_drains_queue_without_ping(self):
        self.start('--max-inflight', '1')
        self.reconnect_qos2(5)

    def test_v311_sql_reconnect_replays_qos2_and_drains_queue_without_ping(self):
        self.require_sqlite()
        self.start('--max-inflight', '1')
        self.reconnect_qos2(4)

    def test_released_pubrel_replay_then_queued_qos2_drain_without_ping(self):
        self.start('--max-inflight', '1')
        target = self.client('released-reader', expiry=60, maximum=1, keepalive=0)
        self.subscribe(target, [('released/value', 2)])
        source = self.client('released-source', keepalive=0)
        self.publish(source, 'released/value', b'already-received', qos=2, packet_id=30)
        original = self.passive_message(target, 'released/value', b'already-received', qos=2)
        target.sendall(ack(0x50, original[3]))
        wire.parse_ack5(receive(target), 0x62, original[3])
        self.publish(source, 'released/value', b'queued-next', qos=2, packet_id=31)
        self.abort(target)
        target = self.client('released-reader', clean=False, expiry=60, maximum=1,
                             present=True, keepalive=0)
        wire.parse_ack5(receive(target), 0x62, original[3])
        target.sendall(ack(0x70, original[3]))
        queued = self.passive_message(target, 'released/value', b'queued-next', qos=2)
        self.finish_qos2(target, queued)
        self.quiet(target)


# Reuse helpers without importing/inheriting the old test suite's methods.
for _name in ('client', 'connack', 'closed', 'terminal', 'barrier', 'quiet',
              'disconnect', 'subscribe', 'publish', 'require_sqlite'):
    setattr(WakeupBlackbox, _name, getattr(wire.Mqtt5Blackbox, _name))

if __name__ == '__main__':
    unittest.main(verbosity=2)
