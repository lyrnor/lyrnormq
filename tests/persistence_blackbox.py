#!/usr/bin/env python3
"""Independent MQTT3.1.1 durability tests using real TCP and process SIGKILL.

Protocol acknowledgements, not sleeps, establish committed-state barriers.
These tests cover process crashes; they do not simulate physical power loss.
"""
import os
from pathlib import Path
import signal
import socket
import sqlite3
import struct
import subprocess
import sys
import tempfile
import time
import unittest

from blackbox import frame, receive, string
from qos_blackbox import ack, parse_publish, publish

ROOT = Path(__file__).resolve().parents[1]


def connect_packet(client, clean=False):
    return frame(0x10, string('MQTT') + bytes([4, 2 if clean else 0])
                 + b'\x00\x1e' + string(client))


def free_port():
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        return probe.getsockname()[1]


class PersistentBroker:
    def __init__(self, directory, extra_args=()):
        self.db = Path(directory) / 'sessions.sqlite'
        self.extra_args = list(extra_args)
        self.process = None

    def command(self):
        executable = os.environ.get('RMQTT_BIN', str(ROOT / 'target/debug/rmqtt'))
        return [executable, '--listen', f'127.0.0.1:{self.port}',
                '--storage-path', str(self.db)] + self.extra_args

    def start(self):
        self.port = free_port()
        self.process = subprocess.Popen(self.command(), stdout=subprocess.DEVNULL,
                                        stderr=subprocess.PIPE)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                error = self.process.stderr.read().decode()
                self.process.stderr.close()
                raise RuntimeError(error)
            try:
                with socket.create_connection(('127.0.0.1', self.port), timeout=.1):
                    return self
            except OSError:
                time.sleep(.02)  # Listener readiness only, never a commit barrier.
        self.stop(kill=True)
        raise TimeoutError('persistent broker did not start')

    def stop(self, kill=False):
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGKILL if kill else signal.SIGINT)
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                raise AssertionError('persistent broker failed graceful shutdown')
        self.process.stderr.close()
        self.process = None

    def restart_after_crash(self):
        self.stop(kill=True)
        self.start()

    def socket(self):
        sock = socket.create_connection(('127.0.0.1', self.port), timeout=5)
        sock.settimeout(5)
        return sock

    def assert_start_fails(self):
        self.port = free_port()
        result = subprocess.run(self.command(), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=8)
        if result.returncode == 0:
            raise AssertionError('invalid/locked persistent database was accepted')
        return result.stderr.decode()


@unittest.skipUnless(hasattr(signal, 'SIGKILL'), 'requires process SIGKILL')
class PersistenceBlackbox(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-persistence-')
        self.addCleanup(self.directory.cleanup)
        self.broker = PersistentBroker(self.directory.name)
        self.addCleanup(self.broker.stop)

    def client(self, name, clean=False, present=False):
        sock = self.broker.socket()
        self.addCleanup(sock.close)
        sock.sendall(connect_packet(name, clean))
        self.assertEqual(receive(sock), (0x20, bytes([int(present), 0])))
        return sock

    def subscribe(self, sock, topic, qos=2):
        sock.sendall(frame(0x82, b'\x00\x01' + string(topic) + bytes([qos])))
        self.assertEqual(receive(sock), (0x90, b'\x00\x01' + bytes([qos])))

    def disconnect(self, sock):
        sock.sendall(frame(0xe0))
        self.closed(sock)
        sock.close()

    def closed(self, sock):
        try:
            self.assertEqual(sock.recv(1), b'')
        except (ConnectionResetError, BrokenPipeError):
            pass

    def quiet(self, sock):
        sock.settimeout(.15)
        try:
            with self.assertRaises(socket.timeout):
                sock.recv(1)
        finally:
            sock.settimeout(5)

    def barrier(self, sock):
        sock.sendall(frame(0xc0))
        self.assertEqual(receive(sock), (0xd0, b''))

    def incoming(self, sock, topic, payload, qos=1, packet_id=50):
        sock.sendall(publish(topic, payload, qos, packet_id))
        self.assertEqual(receive(sock), (0x40 if qos == 1 else 0x50,
                                         struct.pack('!H', packet_id)))
        if qos == 2:
            sock.sendall(ack(0x62, packet_id))
            self.assertEqual(receive(sock), (0x70, struct.pack('!H', packet_id)))

    def complete(self, sock, parsed):
        qos, packet_id = parsed[2:4]
        if qos == 1:
            sock.sendall(ack(0x40, packet_id))
        elif qos == 2:
            sock.sendall(ack(0x50, packet_id))
            self.assertEqual(receive(sock), (0x62, struct.pack('!H', packet_id)))
            sock.sendall(ack(0x70, packet_id))
        self.barrier(sock)

    def test_persistent_subscription_and_offline_qos_survive_restart(self):
        self.broker.start()
        for qos in (1, 2):
            with self.subTest(qos=qos):
                name, topic = f'offline-{qos}', f'offline/{qos}'
                sub = self.client(name)
                self.subscribe(sub, topic)
                self.disconnect(sub)
                publisher = self.client(f'publisher-{qos}', clean=True)
                self.incoming(publisher, topic, b'durable offline', qos)
                self.broker.restart_after_crash()
                sub = self.client(name, present=True)
                message = parse_publish(receive(sub))
                self.assertEqual(message[:3], (topic, b'durable offline', qos))
                self.assertFalse(message[4])  # Previously unsent queued message.
                self.complete(sub, message)
                self.disconnect(sub)

    def test_clean_reset_discards_old_subscription_and_outbox(self):
        self.broker.start()
        sub = self.client('reset')
        self.subscribe(sub, 'reset/topic')
        self.disconnect(sub)
        publisher = self.client('reset-publisher', clean=True)
        self.incoming(publisher, 'reset/topic', b'discard old outbox')
        clean = self.client('reset', clean=True)
        self.quiet(clean)
        self.disconnect(clean)
        self.broker.restart_after_crash()
        fresh = self.client('reset', present=False)
        self.quiet(fresh)
        publisher = self.client('reset-publisher-new', clean=True)
        self.incoming(publisher, 'reset/topic', b'no subscription remains')
        self.quiet(fresh)

    def test_unacknowledged_outgoing_publish_replays_same_id_and_dup(self):
        self.broker.start()
        for qos in (1, 2):
            with self.subTest(qos=qos):
                name, topic = f'replay-{qos}', f'replay/{qos}'
                sub = self.client(name)
                self.subscribe(sub, topic)
                publisher = self.client(f'replay-pub-{qos}', clean=True)
                self.incoming(publisher, topic, b'unacknowledged', qos)
                initial = parse_publish(receive(sub))
                self.assertEqual(initial[:3], (topic, b'unacknowledged', qos))
                self.assertFalse(initial[4])
                self.broker.restart_after_crash()
                resumed = self.client(name, present=True)
                replayed = parse_publish(receive(resumed))
                self.assertEqual(replayed[:4], initial[:4])
                self.assertTrue(replayed[4])
                self.complete(resumed, replayed)
                self.disconnect(resumed)

    def test_plain_tcp_disconnect_replays_each_outgoing_ack_stage(self):
        self.broker.start()
        for stage, qos in [('awaitack', 1), ('awaitrec', 2), ('awaitcomp', 2)]:
            with self.subTest(stage=stage):
                name, topic = f'tcp-{stage}', f'tcp/{stage}'
                sub = self.client(name)
                self.subscribe(sub, topic)
                publisher = self.client(f'tcp-source-{stage}', clean=True)
                self.incoming(publisher, topic, b'normal TCP disconnect', qos)
                initial = parse_publish(receive(sub))
                self.assertEqual(initial[:3], (topic, b'normal TCP disconnect', qos))
                if stage == 'awaitcomp':
                    sub.sendall(ack(0x50, initial[3]))
                    self.assertEqual(receive(sub), (0x62, struct.pack('!H', initial[3])))
                # Receiving PUBLISH/PUBREL is the commit barrier. Reconnect
                # immediately, covering late disconnect cleanup from the old
                # generation without restarting the broker or waiting on sleeps.
                sub.shutdown(socket.SHUT_RDWR)
                sub.close()
                resumed = self.client(name, present=True)
                if stage == 'awaitcomp':
                    self.assertEqual(receive(resumed), (0x62, struct.pack('!H', initial[3])))
                    self.quiet(resumed)
                    resumed.sendall(ack(0x70, initial[3]))
                    self.barrier(resumed)
                else:
                    replay = parse_publish(receive(resumed))
                    self.assertEqual(replay[:4], initial[:4])
                    self.assertTrue(replay[4])
                    self.complete(resumed, replay)
                self.disconnect(resumed)

    def test_qos2_awaiting_pubcomp_replays_pubrel_only(self):
        self.broker.start()
        sub = self.client('awaitcomp')
        self.subscribe(sub, 'awaitcomp')
        publisher = self.client('awaitcomp-pub', clean=True)
        self.incoming(publisher, 'awaitcomp', b'publish must not repeat', 2)
        initial = parse_publish(receive(sub))
        packet_id = initial[3]
        sub.sendall(ack(0x50, packet_id))
        self.assertEqual(receive(sub), (0x62, struct.pack('!H', packet_id)))
        self.broker.restart_after_crash()
        resumed = self.client('awaitcomp', present=True)
        self.assertEqual(receive(resumed), (0x62, struct.pack('!H', packet_id)))
        self.quiet(resumed)
        resumed.sendall(ack(0x70, packet_id))
        self.barrier(resumed)
        self.broker.restart_after_crash()
        finished = self.client('awaitcomp', present=True)
        self.quiet(finished)

    def test_incoming_qos2_pending_survives_and_pubrel_routes_once(self):
        self.broker.start()
        sub = self.client('incoming-target')
        self.subscribe(sub, 'incoming')
        self.disconnect(sub)
        publisher = self.client('incoming-source')
        publisher.sendall(publish('incoming', b'stored before pubrec', 2, 23))
        self.assertEqual(receive(publisher), (0x50, b'\x00\x17'))
        self.broker.restart_after_crash()
        publisher = self.client('incoming-source', present=True)
        publisher.sendall(ack(0x62, 23))
        self.assertEqual(receive(publisher), (0x70, b'\x00\x17'))
        sub = self.client('incoming-target', present=True)
        delivered = parse_publish(receive(sub))
        self.assertEqual(delivered[:3], ('incoming', b'stored before pubrec', 2))
        self.complete(sub, delivered)
        publisher.sendall(ack(0x62, 23))
        self.assertEqual(receive(publisher), (0x70, b'\x00\x17'))
        self.quiet(sub)

    def test_committed_pubrel_with_unprocessed_pubcomp_is_not_redelivered(self):
        self.broker.start()
        sub = self.client('commit-target')
        self.subscribe(sub, 'commit')
        publisher = self.client('commit-source')
        publisher.sendall(publish('commit', b'atomic source and outbox', 2, 9))
        self.assertEqual(receive(publisher), (0x50, b'\x00\x09'))
        publisher.sendall(ack(0x62, 9))
        # Never read PUBCOMP at the source. Successful onward delivery and its
        # final ACK barrier prove the PUBREL+outbox transaction committed.
        message = parse_publish(receive(sub))
        self.assertEqual(message[:3], ('commit', b'atomic source and outbox', 2))
        self.complete(sub, message)
        self.broker.restart_after_crash()
        sub = self.client('commit-target', present=True)
        self.quiet(sub)
        publisher = self.client('commit-source', present=True)
        publisher.sendall(ack(0x62, 9))
        self.assertEqual(receive(publisher), (0x70, b'\x00\x09'))
        self.quiet(sub)

    def test_replayed_publish_order_is_original_send_order(self):
        self.broker.start()
        sub = self.client('ordered-target')
        self.subscribe(sub, 'ordered', qos=1)
        publisher = self.client('ordered-source', clean=True)
        initial = []
        for packet_id, payload in enumerate((b'first', b'second', b'third'), 1):
            self.incoming(publisher, 'ordered', payload, packet_id=packet_id)
            initial.append(parse_publish(receive(sub)))
        self.assertEqual(len({packet[3] for packet in initial}), 3)
        self.broker.restart_after_crash()
        resumed = self.client('ordered-target', present=True)
        replayed = [parse_publish(receive(resumed)) for _ in initial]
        self.assertEqual([packet[:4] for packet in replayed], [packet[:4] for packet in initial])
        self.assertTrue(all(packet[4] for packet in replayed))
        for packet in replayed:
            resumed.sendall(ack(0x40, packet[3]))
        self.barrier(resumed)

    def test_pubrel_fanout_drops_only_the_full_recipient_copy(self):
        # Phase4 per-recipient admission: PUBCOMP follows once every recipient
        # has decided; a full queue loses only its own copy.
        self.broker.extra_args = ['--outbound-capacity', '1']
        self.broker.start()
        first = self.client('a-atomic-target')
        self.subscribe(first, 'transaction/#')
        self.disconnect(first)
        full = self.client('z-full-target')
        self.subscribe(full, 'transaction/#')
        self.subscribe(full, 'fill')
        self.disconnect(full)
        filler = self.client('filler', clean=True)
        self.incoming(filler, 'fill', b'occupies the queue')
        source = self.client('atomic-source')
        source.sendall(publish('transaction/item', b'per-recipient fanout', 2, 7))
        self.assertEqual(receive(source), (0x50, b'\x00\x07'))
        source.sendall(ack(0x62, 7))
        self.assertEqual(receive(source), (0x70, b'\x00\x07'))
        self.broker.restart_after_crash()
        first = self.client('a-atomic-target', present=True)
        delivered = parse_publish(receive(first))
        self.assertEqual(delivered[:3], ('transaction/item', b'per-recipient fanout', 2))
        self.complete(first, delivered)
        full = self.client('z-full-target', present=True)
        filled = parse_publish(receive(full))
        self.assertEqual(filled[:3], ('fill', b'occupies the queue', 1))
        self.complete(full, filled)
        self.quiet(full)
        source = self.client('atomic-source', present=True)
        source.sendall(ack(0x62, 7))
        self.assertEqual(receive(source), (0x70, b'\x00\x07'))

    def test_offline_queue_overflow_drops_only_the_excess_copy(self):
        self.broker.extra_args = ['--outbound-capacity', '2']
        self.broker.start()
        sub = self.client('full-queue')
        self.subscribe(sub, 'full')
        self.disconnect(sub)
        publisher = self.client('full-publisher', clean=True)
        self.incoming(publisher, 'full', b'one', packet_id=1)
        self.incoming(publisher, 'full', b'two', packet_id=2)
        self.incoming(publisher, 'full', b'excess', packet_id=3)
        self.broker.restart_after_crash()
        resumed = self.client('full-queue', present=True)
        messages = [parse_publish(receive(resumed)), parse_publish(receive(resumed))]
        self.assertEqual([message[1] for message in messages], [b'one', b'two'])
        for message in messages:
            resumed.sendall(ack(0x40, message[3]))
        self.barrier(resumed)
        self.quiet(resumed)

    def test_reconnect_takeover_keeps_persistent_subscription(self):
        self.broker.start()
        old = self.client('takeover')
        self.subscribe(old, 'takeover')
        replacement = self.client('takeover', present=True)
        self.closed(old)
        publisher = self.client('takeover-publisher', clean=True)
        self.incoming(publisher, 'takeover', b'new generation')
        message = parse_publish(receive(replacement))
        self.assertEqual(message[:3], ('takeover', b'new generation', 1))
        self.complete(replacement, message)
        self.broker.restart_after_crash()
        resumed = self.client('takeover', present=True)
        self.quiet(resumed)

    def test_second_writer_and_corrupt_or_foreign_database_fail_startup(self):
        self.broker.start()
        self.client('writer-lock')
        self.broker.assert_start_fails()
        self.broker.stop()
        self.broker.db.write_bytes(b'not a sqlite database')
        self.broker.assert_start_fails()
        self.broker.db.unlink()
        with sqlite3.connect(self.broker.db) as db:
            db.execute('CREATE TABLE foreign_records (value BLOB)')
            db.execute('PRAGMA user_version=999')
        self.broker.assert_start_fails()

    def snapshot_blob(self):
        """The single per-session row BLOB (container2) used for corruption tests."""
        with sqlite3.connect(self.broker.db) as db:
            rows = db.execute('SELECT rowid, data FROM sessions').fetchall()
        self.assertEqual(len(rows), 1, 'expected one persistent session row')
        return '"sessions"', '"data"', rows[0][0], bytes(rows[0][1])

    def test_corrupt_snapshot_is_rejected_instead_of_reset(self):
        self.broker.start()
        self.client('corruption-session')
        self.broker.stop()
        table, column, rowid, _ = self.snapshot_blob()
        with sqlite3.connect(self.broker.db) as db:
            db.execute(f'UPDATE {table} SET {column}=? WHERE rowid=?', (b'not a session snapshot', rowid))
        self.broker.assert_start_fails()

    def test_killed_uncommitted_sql_transaction_preserves_previous_snapshot(self):
        self.broker.start()
        sock = self.client('sql-rollback')
        self.subscribe(sock, 'rollback')
        self.broker.stop()
        table, column, rowid, prior = self.snapshot_blob()
        child_code = '''import signal, sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
conn.execute('BEGIN IMMEDIATE')
conn.execute('UPDATE ' + sys.argv[2] + ' SET ' + sys.argv[3] + '=? WHERE rowid=?', (b'uncommitted corruption', int(sys.argv[4])))
print('UPDATED', flush=True)
signal.pause()
'''
        child = subprocess.Popen([sys.executable, '-c', child_code, str(self.broker.db), table, column, str(rowid)],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertEqual(child.stdout.readline(), 'UPDATED\n')
            child.kill()
            child.wait(timeout=5)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            child.stdout.close()
            child.stderr.close()
        self.assertEqual(self.snapshot_blob()[3], prior)
        self.broker.start()
        resumed = self.client('sql-rollback', present=True)
        publisher = self.client('rollback-publisher', clean=True)
        self.incoming(publisher, 'rollback', b'previous committed state')
        message = parse_publish(receive(resumed))
        self.assertEqual(message[:3], ('rollback', b'previous committed state', 1))
        self.complete(resumed, message)


if __name__ == '__main__':
    unittest.main(verbosity=2)
