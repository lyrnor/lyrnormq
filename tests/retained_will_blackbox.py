#!/usr/bin/env python3
"""Independent MQTT3.1.1 retained/Will wire checks, with optional SQLite mode.

LYRNORMQ_TEST_STORAGE_PATH selects SQLite; each test owns a separate temporary DB.
ACKs/confirmed outputs establish ordering. Silence checks are bounded observations,
not evidence of a durable commit. SIGKILL cases do not simulate physical power loss.
"""
import json
import os
from pathlib import Path
import socket
import struct
import tempfile
import unittest

from blackbox import frame, receive, string
from schema_compat import schema3_fixture
from state_store import export_state, legacy_blob, write_legacy
from persistence_blackbox import PersistentBroker
from qos_blackbox import ack, parse_publish


def connect_packet(client, clean=True, keepalive=30, will=None):
    flags = 2 if clean else 0
    body = string(client)
    if will is not None:
        topic, payload, qos, retain = will
        flags |= 4 | (qos << 3) | (32 if retain else 0)
        body += string(topic) + struct.pack('!H', len(payload)) + payload
    return frame(0x10, string('MQTT') + bytes([4, flags])
                 + struct.pack('!H', keepalive) + body)


def publication(topic, payload, qos=0, packet_id=1, retain=False, dup=False):
    body = string(topic)
    if qos:
        body += struct.pack('!H', packet_id)
    return frame(0x30 | (qos << 1) | int(retain) | (8 if dup else 0), body + payload)


class FeatureBroker(PersistentBroker):
    def __init__(self, directory, sqlite, extra_args=()):
        super().__init__(directory, extra_args)
        self.sqlite = sqlite

    def command(self):
        executable = os.environ.get('LYRNORMQ_BIN', str(Path(__file__).resolve().parents[1]
                                                     / 'target/debug/lyrnormq'))
        from lyrnormqcfg import argv
        return argv(executable, f'127.0.0.1:{self.port}', self.db.parent, self.extra_args)


class RetainedWillBlackbox(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='lyrnormq-retained-will-')
        self.addCleanup(self.directory.cleanup)
        # Standalone TOML always uses data_dir/sessions.sqlite.
        self.sqlite = True
        self.broker = FeatureBroker(self.directory.name, self.sqlite)
        self.addCleanup(self.broker.stop)

    def start(self, *args):
        self.broker.extra_args = list(args)
        self.broker.start()

    def client(self, name, clean=True, keepalive=30, will=None, present=False):
        sock = self.broker.socket()
        self.addCleanup(sock.close)
        sock.sendall(connect_packet(name, clean, keepalive, will))
        self.assertEqual(receive(sock), (0x20, bytes([int(present), 0])))
        return sock

    def subscribe(self, sock, filters, packet_id=1):
        sock.sendall(frame(0x82, struct.pack('!H', packet_id)
                           + b''.join(string(topic) + bytes([qos]) for topic, qos in filters)))
        self.assertEqual(receive(sock), (0x90, struct.pack('!H', packet_id)
                                        + bytes(qos for _, qos in filters)))

    def closed(self, sock):
        try:
            self.assertEqual(sock.recv(1), b'')
        except (ConnectionResetError, BrokenPipeError):
            pass

    def disconnect(self, sock):
        sock.sendall(frame(0xe0))
        self.closed(sock)
        sock.close()

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

    def publish(self, sock, topic, payload, qos=1, packet_id=7, retain=False):
        sock.sendall(publication(topic, payload, qos, packet_id, retain))
        if qos:
            self.assertEqual(receive(sock), (0x40 if qos == 1 else 0x50,
                                            struct.pack('!H', packet_id)))
            if qos == 2:
                sock.sendall(ack(0x62, packet_id))
                self.assertEqual(receive(sock), (0x70, struct.pack('!H', packet_id)))
        else:
            self.barrier(sock)

    def complete(self, sock, message):
        qos, packet_id = message[2:4]
        if qos == 1:
            sock.sendall(ack(0x40, packet_id))
        elif qos == 2:
            sock.sendall(ack(0x50, packet_id))
            self.assertEqual(receive(sock), (0x62, struct.pack('!H', packet_id)))
            sock.sendall(ack(0x70, packet_id))
        self.barrier(sock)

    def delivery(self, sock, topic, payload, qos, retain=False):
        message = parse_publish(receive(sock))
        self.assertEqual((message[0], message[1], message[2], message[4], message[5]),
                         (topic, payload, qos, False, retain))
        self.complete(sock, message)
        return message

    def test_retained_replace_delete_and_live_retain_flags(self):
        self.start()
        observer = self.client('live')
        self.subscribe(observer, [('retained/state', 0)])
        publisher = self.client('writer')
        for payload in (b'first', b'second\x00\xff'):
            self.publish(publisher, 'retained/state', payload, retain=True)
            self.delivery(observer, 'retained/state', payload, 0)
            new = self.client('replay-' + str(len(payload)))
            self.subscribe(new, [('retained/state', 2)])
            self.delivery(new, 'retained/state', payload, 1, True)
            self.disconnect(new)
        self.publish(publisher, 'retained/state', b'', retain=True)
        self.delivery(observer, 'retained/state', b'', 0)
        new = self.client('after-delete')
        self.subscribe(new, [('retained/state', 2)])
        self.quiet(new)

    def test_nonretained_publish_does_not_replace_or_delete_retained(self):
        self.start()
        publisher = self.client('nonretained-writer')
        self.publish(publisher, 'preserved', b'retained', retain=True)
        for payload in (b'ordinary', b''):
            self.publish(publisher, 'preserved', payload)
            sub = self.client('nonretained-sub-' + str(len(payload)))
            self.subscribe(sub, [('preserved', 0)])
            self.delivery(sub, 'preserved', b'retained', 0, True)
            self.disconnect(sub)

    def test_retained_wildcards_system_topics_and_resubscribe(self):
        self.start()
        publisher = self.client('wildcard-writer')
        for topic in ('normal/a', 'normal/b', '$OTHER/info'):
            self.publish(publisher, topic, topic.encode(), qos=2, retain=True)
        sub = self.client('wildcard-sub')
        self.subscribe(sub, [('#', 0)])
        messages = [parse_publish(receive(sub)) for _ in range(2)]
        self.assertEqual([(m[0], m[2], m[5]) for m in messages],
                         [('normal/a', 0, True), ('normal/b', 0, True)])
        self.barrier(sub)
        self.subscribe(sub, [('$OTHER/#', 1)])
        self.delivery(sub, '$OTHER/info', b'$OTHER/info', 1, True)
        self.subscribe(sub, [('normal/+', 0)])
        for topic in ('normal/a', 'normal/b'):
            message = parse_publish(receive(sub))
            self.assertEqual((message[0], message[2], message[5]), (topic, 0, True))
        self.barrier(sub)
        # Replacing an identical filter must send retained data again.
        self.subscribe(sub, [('$OTHER/#', 0)])
        self.delivery(sub, '$OTHER/info', b'$OTHER/info', 0, True)

    def test_multiple_filters_replay_as_sequential_subscriptions(self):
        self.start()
        publisher = self.client('multi-writer')
        self.publish(publisher, 'multi/item', b'one retained record', retain=True)
        sub = self.client('multi-sub')
        self.subscribe(sub, [('multi/+', 0), ('multi/#', 0)])
        messages = [parse_publish(receive(sub)) for _ in range(2)]
        self.assertTrue(all((m[0], m[1], m[2], m[5])
                            == ('multi/item', b'one retained record', 0, True) for m in messages))
        self.barrier(sub)
        self.publish(publisher, 'multi/item', b'one live publication')
        self.delivery(sub, 'multi/item', b'one live publication', 0)
        self.quiet(sub)

    def test_retained_qos2_changes_only_on_pubrel_and_duplicate_is_idempotent(self):
        self.start()
        publisher = self.client('qos2-writer')
        self.publish(publisher, 'qos2/state', b'old', retain=True)
        publisher.sendall(publication('qos2/state', b'new', 2, 23, True))
        self.assertEqual(receive(publisher), (0x50, b'\x00\x17'))
        publisher.sendall(publication('qos2/state', b'ignored duplicate', 2, 23, True, True))
        self.assertEqual(receive(publisher), (0x50, b'\x00\x17'))
        before = self.client('qos2-before')
        self.subscribe(before, [('qos2/state', 0)])
        self.delivery(before, 'qos2/state', b'old', 0, True)
        publisher.sendall(ack(0x62, 23))
        self.assertEqual(receive(publisher), (0x70, b'\x00\x17'))
        self.delivery(before, 'qos2/state', b'new', 0)
        publisher.sendall(ack(0x62, 23))
        self.assertEqual(receive(publisher), (0x70, b'\x00\x17'))
        self.quiet(before)
        after = self.client('qos2-after')
        self.subscribe(after, [('qos2/state', 2)])
        self.delivery(after, 'qos2/state', b'new', 2, True)

    def test_normal_disconnect_suppresses_will_and_does_not_update_retained(self):
        self.start()
        writer = self.client('normal-status-writer')
        self.publish(writer, 'will/normal', b'old retained status', retain=True)
        observer = self.client('normal-observer')
        self.subscribe(observer, [('will/normal', 0)])
        self.delivery(observer, 'will/normal', b'old retained status', 0, True)
        owner = self.client('normal-owner', will=('will/normal', b'must not publish', 2, True))
        self.disconnect(owner)
        self.barrier(observer)
        self.quiet(observer)
        again = self.client('normal-new-sub')
        self.subscribe(again, [('will/normal', 0)])
        self.delivery(again, 'will/normal', b'old retained status', 0, True)

    def test_eof_will_qos_matrix_and_binary_empty_payload(self):
        self.start()
        observer = self.client('eof-observer')
        self.subscribe(observer, [('will/eof/+', 2)])
        for qos, payload in ((0, b'\x00\xffbinary'), (1, b''), (2, b'\xfe\x00qos2')):
            topic = f'will/eof/{qos}'
            owner = self.client(f'eof-owner-{qos}', will=(topic, payload, qos, False))
            owner.close()
            self.delivery(observer, topic, payload, qos)
            self.quiet(observer)

    def test_reset_and_protocol_error_publish_will(self):
        self.start()
        observer = self.client('error-observer')
        self.subscribe(observer, [('will/error/+', 1)])
        reset = self.client('reset-owner', will=('will/error/reset', b'rst', 1, False))
        reset.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        reset.close()
        self.delivery(observer, 'will/error/reset', b'rst', 1)
        invalid = self.client('invalid-owner', will=('will/error/protocol', b'protocol', 1, False))
        invalid.sendall(frame(0xc1))
        self.closed(invalid)
        self.delivery(observer, 'will/error/protocol', b'protocol', 1)
        self.quiet(observer)

    def test_keepalive_timeout_publishes_will(self):
        self.start()
        observer = self.client('keepalive-observer')
        self.subscribe(observer, [('will/keepalive', 1)])
        owner = self.client('keepalive-owner', keepalive=1,
                            will=('will/keepalive', b'timed out', 1, False))
        self.delivery(observer, 'will/keepalive', b'timed out', 1)
        self.closed(owner)
        self.quiet(observer)

    def test_takeover_publishes_old_will_once_and_replacement_can_clear_its_will(self):
        self.start()
        observer = self.client('takeover-observer')
        self.subscribe(observer, [('will/takeover', 2)])
        old = self.client('same-id', will=('will/takeover', b'old', 1, False))
        replacement = self.client('same-id', will=('will/takeover', b'new', 2, False))
        self.closed(old)
        self.delivery(observer, 'will/takeover', b'old', 1)
        self.quiet(observer)
        self.disconnect(replacement)
        self.quiet(observer)
        third = self.client('same-id', will=('will/takeover', b'third', 0, False))
        third.close()
        self.delivery(observer, 'will/takeover', b'third', 0)
        self.quiet(observer)

    def test_retained_will_updates_and_empty_retained_will_deletes(self):
        self.start()
        observer = self.client('retained-will-observer')
        self.subscribe(observer, [('will/retained', 0)])
        for index, payload in enumerate((b'last status\x00\xff', b'')):
            owner = self.client(f'retained-will-owner-{index}',
                                will=('will/retained', payload, 1, True))
            owner.close()
            self.delivery(observer, 'will/retained', payload, 0)
            sub = self.client(f'retained-will-replay-{index}')
            self.subscribe(sub, [('will/retained', 2)])
            if payload:
                self.delivery(sub, 'will/retained', payload, 1, True)
            else:
                self.quiet(sub)
            self.disconnect(sub)

    def test_rejected_connect_does_not_publish_will(self):
        self.start()
        observer = self.client('reject-observer')
        self.subscribe(observer, [('will/rejected', 0)])
        for data in (connect_packet('bad-qos', will=('will/rejected', b'bad', 3, False)),
                     connect_packet('bad-topic', will=('+', b'bad', 0, False)), frame(0xc0)):
            sock = self.broker.socket()
            self.addCleanup(sock.close)
            sock.sendall(data)
            self.closed(sock)
        self.barrier(observer)
        self.quiet(observer)

    def test_will_to_full_outbox_drops_only_that_copy(self):
        # Phase4: Wills fan out after commit; a full recipient loses its copy
        # and the Will is not kept pending, so the client id is free again.
        self.start('--outbound-capacity', '1', '--max-inflight', '1')
        observer = self.client('pending-observer')
        self.subscribe(observer, [('will/pending', 1)])
        roomy = self.client('pending-roomy')
        self.subscribe(roomy, [('will/pending', 1)])
        publisher = self.client('pending-publisher')
        self.publish(publisher, 'will/pending', b'inflight', packet_id=1)
        first = parse_publish(receive(observer))
        self.complete(roomy, parse_publish(receive(roomy)))
        self.publish(publisher, 'will/pending', b'queued', packet_id=2)
        self.complete(roomy, parse_publish(receive(roomy)))
        owner = self.client('pending-owner', will=('will/pending', b'dropped Will', 1, False))
        owner.close()
        self.delivery(roomy, 'will/pending', b'dropped Will', 1)
        replacement = self.client('pending-owner')
        self.disconnect(replacement)
        self.quiet(observer)
        observer.sendall(ack(0x40, first[3]))
        second = parse_publish(receive(observer))
        self.assertEqual(second[:3], ('will/pending', b'queued', 1))
        observer.sendall(ack(0x40, second[3]))
        self.barrier(observer)
        self.quiet(observer)

    def test_self_will_to_own_full_outbox_is_dropped_and_not_resurrected(self):
        if not self.sqlite:
            self.skipTest('SQLite persistent session only')
        self.start('--outbound-capacity', '1', '--max-inflight', '1')
        topic = 'will/persistent-self'
        owner = self.client('self-owner', clean=False,
                            will=(topic, b'original Will', 1, False))
        self.subscribe(owner, [(topic, 1)])
        publisher = self.client('self-publisher')
        self.publish(publisher, topic, b'inflight before disconnect', packet_id=1)
        first = parse_publish(receive(owner))
        self.assertEqual((first[0], first[1], first[2], first[4], first[5]),
                         (topic, b'inflight before disconnect', 1, False, False))
        self.publish(publisher, topic, b'queued before disconnect', packet_id=2)
        owner.close()

        # The full persistent outbox drops this client's own Will copy (Phase4
        # per-recipient admission); resuming replays only the accepted work.
        resumed = self.client('self-owner', clean=False, present=True)
        replay = parse_publish(receive(resumed))
        self.assertEqual(replay[:4], first[:4])
        self.assertEqual((replay[4], replay[5]), (True, False))
        resumed.sendall(ack(0x40, replay[3]))
        queued = parse_publish(receive(resumed))
        self.assertEqual((queued[0], queued[1], queued[2], queued[4], queued[5]),
                         (topic, b'queued before disconnect', 1, False, False))
        resumed.sendall(ack(0x40, queued[3]))
        self.barrier(resumed)
        self.quiet(resumed)
        self.disconnect(resumed)

        # Normal close and restart must preserve the subscription but neither
        # replay the acknowledged Will nor resurrect its already-cleared intent.
        self.broker.restart_after_crash()
        restored = self.client('self-owner', clean=False, present=True)
        self.barrier(restored)
        self.quiet(restored)
        publisher = self.client('self-publisher-after-restart')
        self.publish(publisher, topic, b'subscription survives', packet_id=3)
        self.delivery(restored, topic, b'subscription survives', 1)
        self.quiet(restored)
        self.disconnect(restored)

    def test_retained_survives_clean_reset_and_restart_then_delete_is_durable(self):
        if not self.sqlite:
            self.skipTest('SQLite persistence only')
        self.start()
        writer = self.client('retained-clean')
        self.publish(writer, 'durable/retained', b'global retained', qos=2, retain=True)
        self.disconnect(writer)
        self.broker.restart_after_crash()
        reset = self.client('retained-clean')
        self.subscribe(reset, [('durable/retained', 1)])
        self.delivery(reset, 'durable/retained', b'global retained', 1, True)
        self.disconnect(reset)
        writer = self.client('retained-delete')
        self.publish(writer, 'durable/retained', b'', retain=True)
        self.disconnect(writer)
        self.broker.restart_after_crash()
        sub = self.client('retained-deleted-check')
        self.subscribe(sub, [('durable/retained', 0)])
        self.quiet(sub)

    def test_active_clean_source_will_recovers_after_sigkill_once(self):
        if not self.sqlite:
            self.skipTest('SQLite process recovery only')
        self.start()
        target = self.client('crash-target', clean=False)
        self.subscribe(target, [('will/crash', 2)])
        self.disconnect(target)
        self.client('crash-clean-owner', will=('will/crash', b'crash Will', 1, True))
        self.broker.restart_after_crash()
        target = self.client('crash-target', clean=False, present=True)
        self.delivery(target, 'will/crash', b'crash Will', 1)
        replay = self.client('crash-retained-observer')
        self.subscribe(replay, [('will/crash', 0)])
        self.delivery(replay, 'will/crash', b'crash Will', 0, True)
        self.broker.restart_after_crash()
        target = self.client('crash-target', clean=False, present=True)
        self.quiet(target)

    def test_retained_count_limit_preserves_old_value_and_delete_frees_capacity(self):
        self.start('--max-retained', '1')
        writer = self.client('count-writer')
        self.publish(writer, 'capacity/a', b'old value', retain=True)
        writer.sendall(publication('capacity/b', b'rejected value', 1, 8, True))
        self.closed(writer)  # No successful PUBACK for the rejected record.
        sub = self.client('count-observer')
        self.subscribe(sub, [('capacity/#', 0)])
        self.delivery(sub, 'capacity/a', b'old value', 0, True)
        self.quiet(sub)
        self.disconnect(sub)
        writer = self.client('count-replacement')
        self.publish(writer, 'capacity/a', b'replaced', retain=True)
        self.publish(writer, 'capacity/a', b'', retain=True)
        self.publish(writer, 'capacity/b', b'accepted after delete', retain=True)
        sub = self.client('count-after-delete')
        self.subscribe(sub, [('capacity/#', 0)])
        self.delivery(sub, 'capacity/b', b'accepted after delete', 0, True)
        self.quiet(sub)

    def test_retained_replay_queue_failure_does_not_install_partial_subscription(self):
        if not self.sqlite:
            self.skipTest('persistent subscription rollback check')
        self.start('--outbound-capacity', '1')
        writer = self.client('replay-limit-writer')
        for topic in ('replay-limit/a', 'replay-limit/b'):
            self.publish(writer, topic, b'retained', retain=True)
        sub = self.client('replay-limit-sub', clean=False)
        self.subscribe(sub, [('original-only', 0)])
        sub.sendall(frame(0x82, b'\x00\x02' + string('replay-limit/#') + b'\x00'))
        self.closed(sub)
        resumed = self.client('replay-limit-sub', clean=False, present=True)
        self.quiet(resumed)
        self.publish(writer, 'replay-limit/new', b'must not match failed filter')
        self.quiet(resumed)
        self.publish(writer, 'original-only', b'original subscription kept')
        self.delivery(resumed, 'original-only', b'original subscription kept', 0)

    def test_unacknowledged_retained_replay_preserves_retain_id_and_dup_after_restart(self):
        if not self.sqlite:
            self.skipTest('SQLite retained replay recovery only')
        self.start()
        writer = self.client('retained-retry-writer')
        self.publish(writer, 'retained/retry', b'unacknowledged replay', retain=True)
        sub = self.client('retained-retry-sub', clean=False)
        self.subscribe(sub, [('retained/retry', 1)])
        initial = parse_publish(receive(sub))
        self.assertEqual((initial[0], initial[1], initial[2], initial[4], initial[5]),
                         ('retained/retry', b'unacknowledged replay', 1, False, True))
        self.broker.restart_after_crash()
        resumed = self.client('retained-retry-sub', clean=False, present=True)
        replayed = parse_publish(receive(resumed))
        self.assertEqual(replayed[:4], initial[:4])
        self.assertEqual(replayed[4:], (True, True))
        self.complete(resumed, replayed)
        self.quiet(resumed)

    def test_schema1_migrates_without_losing_offline_message_or_subscription(self):
        if not self.sqlite:
            self.skipTest('SQLite schema migration only')
        self.start()
        target = self.client('migration-target', clean=False)
        self.subscribe(target, [('migration', 1)])
        self.disconnect(target)
        publisher = self.client('migration-publisher')
        self.publish(publisher, 'migration', b'legacy queue')
        self.disconnect(publisher)
        self.broker.stop()
        current = schema3_fixture(export_state(self.broker.db))
        self.assertEqual(current['schema_version'], 3)
        self.assertEqual(current.pop('retained'), [])
        self.assertEqual(current.pop('wills'), [])
        current['schema_version'] = 1
        for session in current['sessions']:
            self.assertEqual(session.pop('owner'), 'anonymous')
            for message in session['queued']:
                self.assertFalse(message.pop('retain'))
        write_legacy(self.broker.db, current)
        self.broker.start()
        target = self.client('migration-target', clean=False, present=True)
        self.delivery(target, 'migration', b'legacy queue', 1)
        self.assertIsNone(legacy_blob(self.broker.db))
        migrated = export_state(self.broker.db)
        self.assertEqual(migrated['schema_version'], 7)
        self.assertTrue(all(s['owner'] == 'anonymous' for s in migrated['sessions']))


if __name__ == '__main__':
    unittest.main(verbosity=2)
