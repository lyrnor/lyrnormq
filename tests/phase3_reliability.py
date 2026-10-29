#!/usr/bin/env python3
"""Bounded real-TCP slow-consumer and admission/recovery checks.

Run with RMQTT_BIN pointing to a built executable. RMQTT_TEST_STORAGE_PATH selects
SQLite; each test owns a fresh DB. Nonreading tests use ReceiveMaximum=1 so the
application queue reaches an exact bound independently of kernel socket buffers.
They do not claim to saturate the OS send buffer. Kernel-independent blocked-write
and writer-timeout invariants belong to transport's deterministic tests.
"""
import os
import select
import socket
import tempfile
import time
import unittest

from blackbox import frame, receive
import mqtt5_blackbox as wire
from retained_will_blackbox import FeatureBroker


class Phase3Reliability(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-phase3-reliability-')
        self.addCleanup(self.directory.cleanup)
        self.sqlite = bool(os.environ.get('RMQTT_TEST_STORAGE_PATH'))
        self.broker = FeatureBroker(self.directory.name, self.sqlite)
        self.addCleanup(self.broker.stop)

    def start(self, *args):
        self.broker.extra_args = ['--protocol', '5', '--outbound-capacity', '1',
                                 '--max-inflight', '1', '--max-packet-size', '4096',
                                 '--max-subscriptions', '4', '--max-sessions', '16',
                                 '--max-connections', '8', '--write-timeout-ms', '500'] + list(args)
        self.broker.start()

    def raw(self):
        sock = self.broker.socket()
        sock.settimeout(3)
        self.addCleanup(sock.close)
        return sock

    def resume_after_capacity_release(self, name):
        # Socket EOF precedes worker Disconnect completion and permit release.
        # Retry only pre-CONNACK admission EOF/reset, within one fixed deadline.
        deadline = time.monotonic() + 3
        packet = wire.connect5(name, clean=False, properties=[(0x11, 600), (0x21, 1)])
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            sock = socket.create_connection(('127.0.0.1', self.broker.port),
                                            timeout=remaining)
            self.addCleanup(sock.close)
            try:
                sock.sendall(packet)
                prefix = sock.recv(1, socket.MSG_PEEK)
            except (ConnectionResetError, BrokenPipeError):
                prefix = b''
            if prefix:
                # Once any response starts, validate normally. Invalid/truncated
                # CONNACK, refusal codes and timeouts are failures, not retries.
                sock.settimeout(max(.001, deadline - time.monotonic()))
                self.connack(sock, present=True)
                sock.settimeout(3)
                return sock
            sock.close()
            time.sleep(min(.01, max(0, deadline - time.monotonic())))
        self.fail('connection permit was not released within 3 seconds')

    def dropped_publication(self, name, payload):
        # Phase4 per-recipient admission: the full reader loses only its copy.
        source = self.client(name)
        source.sendall(wire.publication5('pressure/value', payload, qos=1, packet_id=7))
        wire.parse_ack5(receive(source), 0x40, 7)
        source.close()

    def healthy_pair(self):
        observer = self.client('healthy-observer')
        self.subscribe(observer, [('healthy/value', 0)])
        return self.client('healthy-source'), observer

    def healthy_progress(self, source, observer, index):
        payload = f'healthy-{index}'.encode()
        before = time.monotonic()
        self.publish(source, 'healthy/value', payload)
        self.message(observer, 'healthy/value', payload, qos=0)
        self.barrier(source)
        self.barrier(observer)
        self.assertLess(time.monotonic() - before, 3,
                        'independent healthy traffic/control did not progress within socket deadline')

    def assert_window_bound(self, name):
        if self.sqlite:
            record = next(s for s in self.snapshot()['sessions'] if s['client_id'] == name)
            self.assertEqual(len(record['outgoing']), 1)
            self.assertEqual(len(record['queued']), 1)
            self.assertLessEqual(len(record['incoming']), 1)

    def pressure_pair(self, name='slow-reader', read_first=False):
        slow = self.client(name, expiry=600, maximum=1)
        self.subscribe(slow, [('pressure/value', 1)])
        source = self.client('pressure-source')
        self.publish(source, 'pressure/value', b'accepted-first')
        if read_first:
            first = self.message(slow, 'pressure/value', b'accepted-first', complete=False)
        else:
            # Observe a PUBLISH prefix without consuming any application data.
            # This proves the outgoing exchange started before filling its queue.
            self.assertTrue(select.select([slow], [], [], 3)[0], 'no first delivery became readable')
            self.assertEqual(slow.recv(1, socket.MSG_PEEK), b'\x32')
            first = None
        self.publish(source, 'pressure/value', b'accepted-second', packet_id=8)
        self.assert_window_bound(name)
        return slow, source, first

    def resume_two(self, name, first_id=None):
        resumed = self.client(name, clean=False, expiry=600, maximum=1, present=True)
        first = self.message(resumed, 'pressure/value', b'accepted-first', dup=True)
        if first_id is not None:
            self.assertEqual(first[3], first_id)
        # The first ID is free after PUBACK and may legally be reused, including
        # immediately after restoration; payload order distinguishes the exchange.
        self.message(resumed, 'pressure/value', b'accepted-second')
        self.barrier(resumed)
        self.quiet(resumed)
        if self.sqlite:
            record = next(s for s in self.snapshot()['sessions'] if s['client_id'] == name)
            self.assertEqual((record['outgoing'], record['queued']), ([], []))
        return resumed

    def test_nonreading_peer_bounds_admission_and_reconnect_replays_only_accepted_work(self):
        self.start()
        slow, _, _ = self.pressure_pair()
        healthy_source, observer = self.healthy_pair()
        for index in range(4):
            self.dropped_publication(f'overflow-{index}', f'dropped-{index}'.encode())
            self.assert_window_bound('slow-reader')
            self.healthy_progress(healthy_source, observer, index)
        # Reconnect/takeover while the old peer has never read either PUBLISH.
        # Observe old buffered data only after its replacement is established.
        resumed = self.resume_two('slow-reader')
        slow.close()
        self.publish(healthy_source, 'pressure/value', b'after-drain')
        self.message(resumed, 'pressure/value', b'after-drain')
        self.barrier(resumed)

    def test_full_slow_window_drops_its_copy_while_controls_and_other_recipients_progress(self):
        self.start()
        slow = self.client('slow-window', expiry=600, maximum=1)
        self.subscribe(slow, [('pressure/value', 1)])
        observer = self.client('fanout-observer')
        self.subscribe(observer, [('pressure/value', 0), ('healthy/value', 0)])
        source = self.client('pressure-source')
        for value in (b'accepted-first', b'accepted-second'):
            self.publish(source, 'pressure/value', value)
            self.message(observer, 'pressure/value', value, qos=0)
            if value == b'accepted-first':
                first = self.message(slow, 'pressure/value', value, complete=False)
        self.assert_window_bound('slow-window')
        for index in range(4):
            self.dropped_publication(f'atomic-overflow-{index}', f'dropped-{index}'.encode())
            # The writable recipient still gets its copy; the full one does not.
            self.message(observer, 'pressure/value', f'dropped-{index}'.encode(), qos=0)
            self.barrier(observer)
            self.barrier(slow)
            self.healthy_progress(source, observer, index)
            self.assert_window_bound('slow-window')
        self.complete(slow, first)
        self.message(slow, 'pressure/value', b'accepted-second')
        self.barrier(slow)
        self.publish(source, 'pressure/value', b'admission-resumed')
        self.message(observer, 'pressure/value', b'admission-resumed', qos=0)
        self.message(slow, 'pressure/value', b'admission-resumed')
        self.barrier(slow)

    def test_connection_cap_refuses_extra_peer_and_releases_slot_after_disconnect(self):
        self.start('--max-connections', '3')
        slow = self.client('capacity-slow', expiry=600, maximum=1)
        self.subscribe(slow, [('pressure/value', 1)])
        source = self.client('capacity-source')
        control = self.client('capacity-control')
        self.publish(source, 'pressure/value', b'pending')
        self.assertTrue(select.select([slow], [], [], 3)[0])
        self.assertEqual(slow.recv(1, socket.MSG_PEEK), b'\x32')
        extra = self.raw()
        # Listener admission drops an over-capacity TCP stream before CONNECT.
        self.closed(extra)
        self.barrier(control)
        slow.sendall(frame(0xe0))
        # The slow socket has one buffered PUBLISH before its normal EOF.
        self.message(slow, 'pressure/value', b'pending', complete=False)
        self.closed(slow)
        slow.close()
        resumed = self.resume_after_capacity_release('capacity-slow')
        self.message(resumed, 'pressure/value', b'pending', dup=True)
        self.barrier(resumed)
        self.barrier(control)

    def test_sqlite_crash_with_full_slow_window_restores_exact_pending_id_and_queue(self):
        self.require_sqlite()
        self.start()
        slow, _, first = self.pressure_pair('crash-slow', read_first=True)
        self.dropped_publication('crash-overflow', b'must-not-recover')
        self.assert_window_bound('crash-slow')
        self.broker.restart_after_crash()
        slow.close()
        self.resume_two('crash-slow', first_id=first[3])
        source, observer = self.healthy_pair()
        self.healthy_progress(source, observer, 0)


# Composition reuses the mature wire helpers without collecting their test cases.
for _name in ('client', 'connack', 'closed', 'terminal', 'barrier', 'quiet',
              'disconnect', 'subscribe', 'publish', 'complete', 'message',
              'require_sqlite', 'snapshot'):
    setattr(Phase3Reliability, _name, getattr(wire.Mqtt5Blackbox, _name))

if __name__ == '__main__':
    unittest.main(verbosity=2)
