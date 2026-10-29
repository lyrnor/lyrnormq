#!/usr/bin/env python3
"""Real SQLite writer contention and process-crash ACK barriers.

BEGIN IMMEDIATE establishes an actual external write lock. A bounded socket silence
observation is combined with the unchanged committed exported state, then either
release or SIGKILL. This tests transaction/process recovery, not physical power loss.
"""
import json
import os
import socket
import sqlite3
import struct
import tempfile
import unittest

from blackbox import frame, receive, string
import mqtt5_blackbox as wire
from retained_will_blackbox import FeatureBroker
from state_store import export_bytes


class FaultRecoveryBlackbox(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-fault-recovery-')
        self.addCleanup(self.directory.cleanup)
        self.sqlite = bool(os.environ.get('RMQTT_TEST_STORAGE_PATH'))
        self.broker = FeatureBroker(self.directory.name, self.sqlite)
        self.addCleanup(self.broker.stop)

    def start(self):
        self.broker.start()

    def snapshot_bytes(self):
        return export_bytes(self.broker.db)

    def write_lock(self):
        connection = sqlite3.connect(self.broker.db, timeout=1)
        connection.execute('BEGIN IMMEDIATE')
        self.addCleanup(connection.close)
        return connection

    def no_ack_before_commit(self, sock, before):
        sock.settimeout(.2)
        try:
            with self.assertRaises(socket.timeout):
                sock.recv(1)
        finally:
            sock.settimeout(5)
        self.assertEqual(self.snapshot_bytes(), before)

    def crash_locked(self, connection):
        self.broker.stop(kill=True)
        connection.rollback()
        connection.close()
        self.broker.start()

    def pair(self, qos=2):
        self.require_sqlite()
        self.start()
        target = self.client('target', clean=False, expiry=3600)
        source = self.client('source', clean=False, expiry=3600)
        self.subscribe(target, [('fault/#', qos)])
        return source, target

    def test_fragmented_healthy_control_remains_usable_after_incomplete_peer(self):
        self.start()
        partial = self.raw()
        partial.sendall(b'\x10\x7f\x00')
        healthy = self.client('healthy')
        for _ in range(3):
            self.barrier(healthy)
        partial.close()
        self.barrier(healthy)

    def test_external_write_lock_blocks_puback_and_retained_outbox_until_release(self):
        source, target = self.pair(1)
        before = self.snapshot_bytes()
        lock = self.write_lock()
        source.sendall(wire.publication5('fault/committed', b'value', 1, 91, retain=True))
        self.no_ack_before_commit(source, before)
        self.quiet(target)
        lock.rollback();lock.close()
        wire.parse_ack5(receive(source), 0x40, 91)
        self.message(target, 'fault/committed', b'value')
        self.barrier(target)
        self.broker.restart_after_crash()
        resumed = self.client('target', clean=False, expiry=3600, present=True)
        self.subscribe(resumed, [('fault/committed', 1)])
        self.message(resumed, 'fault/committed', b'value', retain=True)

    def test_crash_before_pubrec_commit_discards_proposal_and_duplicate_retries_once(self):
        source, target = self.pair()
        before = self.snapshot_bytes()
        lock = self.write_lock()
        source.sendall(wire.publication5('fault/q2', b'original', 2, 92, retain=True))
        self.no_ack_before_commit(source, before)
        self.crash_locked(lock)
        self.assertEqual(json.loads(before)['retained'], json.loads(self.snapshot_bytes())['retained'])
        resumed = self.client('source', clean=False, expiry=3600, present=True)
        receiver = self.client('target', clean=False, expiry=3600, present=True)
        resumed.sendall(wire.publication5('fault/q2', b'original', 2, 92, retain=True, dup=True))
        wire.parse_ack5(receive(resumed), 0x50, 92)
        self.quiet(receiver)
        resumed.sendall(wire.ack5(0x62, 92))
        wire.parse_ack5(receive(resumed), 0x70, 92)
        self.message(receiver, 'fault/q2', b'original', 2)
        self.barrier(receiver);self.quiet(receiver)

    def test_crash_before_suback_commit_does_not_install_new_subscription(self):
        source, target = self.pair()
        # Replace broad initial filter so the new subscription has observable effect.
        target.sendall(frame(0xa2, struct.pack('!H', 3) + wire.props() + string('fault/#')))
        wire.parse_ack5(receive(target), 0xb0, 3)
        self.subscribe(target, [('base/#', 1)])
        before = self.snapshot_bytes()
        lock = self.write_lock()
        target.sendall(frame(0x82, struct.pack('!H', 93) + wire.props() + string('fault/new') + b'\x01'))
        self.no_ack_before_commit(target, before)
        self.crash_locked(lock)
        resumed = self.client('target', clean=False, expiry=3600, present=True)
        pub = self.client('source', clean=False, expiry=3600, present=True)
        self.publish(pub, 'fault/new', b'not subscribed')
        self.quiet(resumed)
        self.publish(pub, 'base/existing', b'preserved')
        self.message(resumed, 'base/existing', b'preserved')

    def test_crash_before_pubcomp_commit_retains_incoming_qos2_original(self):
        source, target = self.pair()
        source.sendall(wire.publication5('fault/release', b'pending', 2, 94, retain=True))
        wire.parse_ack5(receive(source), 0x50, 94)
        before = self.snapshot_bytes()
        lock = self.write_lock()
        source.sendall(wire.ack5(0x62, 94))
        self.no_ack_before_commit(source, before)
        self.quiet(target)
        self.crash_locked(lock)
        resumed = self.client('source', clean=False, expiry=3600, present=True)
        receiver = self.client('target', clean=False, expiry=3600, present=True)
        resumed.sendall(wire.ack5(0x62, 94))
        wire.parse_ack5(receive(resumed), 0x70, 94)
        self.message(receiver, 'fault/release', b'pending', 2)
        resumed.sendall(wire.ack5(0x62, 94))
        wire.parse_ack5(receive(resumed), 0x70, 94, reason=0x92)
        self.barrier(receiver);self.quiet(receiver)

    def test_crash_before_outgoing_pubrel_commit_replays_original_publish(self):
        source, target = self.pair()
        self.publish(source, 'fault/outgoing', b'original', 2)
        original = self.message(target, 'fault/outgoing', b'original', 2, complete=False)
        before = self.snapshot_bytes()
        lock = self.write_lock()
        target.sendall(wire.ack5(0x50, original[3]))
        self.no_ack_before_commit(target, before)
        self.crash_locked(lock)
        resumed = self.client('target', clean=False, expiry=3600, present=True)
        replay = self.message(resumed, 'fault/outgoing', b'original', 2, dup=True)
        self.assertEqual(replay[3], original[3])
        self.barrier(resumed);self.quiet(resumed)

    def test_after_committed_pubrel_crash_resumes_pubrel_without_publish(self):
        source, target = self.pair()
        self.publish(source, 'fault/pubrel', b'payload retired', 2)
        original = self.message(target, 'fault/pubrel', b'payload retired', 2, complete=False)
        target.sendall(wire.ack5(0x50, original[3]))
        wire.parse_ack5(receive(target), 0x62, original[3])
        self.broker.restart_after_crash()
        resumed = self.client('target', clean=False, expiry=3600, present=True)
        wire.parse_ack5(receive(resumed), 0x62, original[3])
        resumed.sendall(wire.ack5(0x70, original[3]))
        self.barrier(resumed);self.quiet(resumed)

    def test_sqlite_busy_failure_never_falls_back_to_acknowledged_memory(self):
        source, target = self.pair(1)
        before = self.snapshot_bytes()
        lock = self.write_lock()
        source.sendall(wire.publication5('fault/busy', b'must not commit', 1, 96, retain=True))
        # The real store busy timeout is two seconds. No release is made: this
        # exercises a returned commit error, which stops the broker (fail closed).
        source.settimeout(6)
        try:
            header, body = receive(source)
            self.assertEqual(header, 0xe0, 'storage failure must not send PUBACK/PUBREC/PUBCOMP')
            self.assertGreaterEqual(body[0] if body else 0, 0x80)
            self.closed(source)
        except (EOFError, ConnectionResetError):
            pass
        self.assertEqual(self.snapshot_bytes(), before)
        self.crash_locked(lock)
        resumed = self.client('target', clean=False, expiry=3600, present=True)
        self.quiet(resumed)
        self.assertFalse(json.loads(self.snapshot_bytes())['retained'])
        pub = self.client('source', clean=False, expiry=3600, present=True)
        self.publish(pub, 'fault/busy', b'retry after recovery', 1, packet_id=96)
        self.message(resumed, 'fault/busy', b'retry after recovery')

    def test_after_ack_crash_preserves_queued_retained_batch_and_packet_ids(self):
        source, target = self.pair()
        self.disconnect(target)
        self.publish(source, 'fault/batch', b'durable', 2, packet_id=95, retain=True)
        committed = json.loads(self.snapshot_bytes())
        self.assertEqual(len(committed['retained']), 1)
        self.assertEqual(len(next(s for s in committed['sessions'] if s['client_id']=='target')['queued']), 1)
        self.broker.restart_after_crash()
        resumed = self.client('target', clean=False, expiry=3600, present=True)
        delivered = self.message(resumed, 'fault/batch', b'durable', 2, complete=False)
        self.broker.restart_after_crash()
        again = self.client('target', clean=False, expiry=3600, present=True)
        retry = self.message(again, 'fault/batch', b'durable', 2, dup=True)
        self.assertEqual(delivered[3], retry[3])
        self.barrier(again);self.quiet(again)


# Composition avoids importing/discovering the entire base TestCase twice.
for _name in ('client', 'raw', 'connack', 'closed', 'terminal', 'barrier', 'quiet',
              'disconnect', 'subscribe', 'publish', 'complete', 'message', 'require_sqlite'):
    setattr(FaultRecoveryBlackbox, _name, getattr(wire.Mqtt5Blackbox, _name))

if __name__ == '__main__':
    unittest.main(verbosity=2)
