#!/usr/bin/env python3
"""Independent MQTT5 Message Expiry/Will Delay TCP and crash checks.

ACKs and committed SQL observations are barriers. Real time is used only for elapsed
lifetime assertions. Each case owns a broker; SQLite-only tests explicitly skip RAM.
"""
import copy
import hashlib
import json
import os
import socket
import sqlite3
import struct
import time
import unittest

import mqtt5_blackbox as wire
from blackbox import frame, receive
from qos_blackbox import ack
from auth_acl_blackbox import AuthBroker, PASSWORD
from schema_compat import schema4_fixture


class ExpiryWillBlackbox(unittest.TestCase):
    def ttl_message(self, sock, topic, payload, qos=1, maximum=1, retain=False,
                    dup=False, complete=True, metadata=()):
        message = wire.parse5(receive(sock))
        self.assertEqual(message[:3], (topic, payload, qos))
        self.assertEqual(message[4:6], (dup, retain))
        values = [value for key, value in message[6] if key == 2]
        self.assertEqual(len(values), 1)
        self.assertGreaterEqual(values[0], 0)
        self.assertLessEqual(values[0], maximum)
        self.assertEqual([(key, value) for key, value in message[6] if key != 2], list(metadata))
        if complete:
            self.complete(sock, message)
        return message

    def abort(self, sock):
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()

    def observer(self, topic='will/#', version=5):
        result = self.client('observer', version=version)
        self.subscribe(result, [(topic, 1)], version=version)
        return result

    def will_source(self, name='owner', delay=1, expiry=60, properties=(), retain=False,
                    clean=True):
        return self.client(name, clean=clean, expiry=expiry,
                           will=('will/event', name.encode(), 1, retain),
                           will_properties=[(0x18, delay)] + list(properties))

    def test_zero_expiry_qos_matrix_acknowledges_without_delivery(self):
        self.start()
        sub = self.observer('zero/#')
        pub = self.client('zero-source')
        for qos in range(3):
            self.publish(pub, f'zero/{qos}', b'expired immediately', qos=qos,
                         packet_id=20 + qos, properties=[(2, 0)])
        self.quiet(sub)
        self.publish(pub, 'zero/live', b'alive')
        self.message(sub, 'zero/live', b'alive')

    def test_zero_expiry_retained_replacement_never_revives_old_value(self):
        self.start()
        pub = self.client('zero-retained-source')
        for qos in range(3):
            topic = f'zero-retained/{qos}'
            self.publish(pub, topic, b'old', retain=True)
            self.publish(pub, topic, b'expired replacement', qos=qos, packet_id=40 + qos,
                         retain=True, properties=[(2, 0)])
        sub = self.client('zero-retained-reader')
        self.subscribe(sub, [('zero-retained/#', 1)])
        self.quiet(sub)

    def test_retained_timer_expiry_and_capacity_reclamation(self):
        self.start('--max-retained', '1')
        pub = self.client('retained-timer-source')
        self.publish(pub, 'retained/first', b'first', retain=True, properties=[(2, 1)])
        time.sleep(1.2)
        self.publish(pub, 'retained/second', b'second', retain=True, properties=[(2, 60)])
        sub = self.client('retained-timer-reader')
        self.subscribe(sub, [('retained/#', 1)])
        self.ttl_message(sub, 'retained/second', b'second', maximum=60, retain=True)
        self.quiet(sub)

    def test_retained_replay_decreases_lifetime_and_resubscribe_does_not_reset(self):
        self.start()
        pub = self.client('remaining-source')
        self.publish(pub, 'remaining/value', b'value', retain=True,
                     properties=[(0x26, ('k', 'one')), (2, 3), (0x26, ('k', 'two'))])
        time.sleep(1.15)
        sub = self.client('remaining-reader')
        self.subscribe(sub, [('remaining/#', 1)])
        self.ttl_message(sub, 'remaining/value', b'value', maximum=2, retain=True,
                         metadata=[(0x26, ('k', 'one')), (0x26, ('k', 'two'))])
        time.sleep(2)
        self.subscribe(sub, [('remaining/#', 1)], packet_id=2)
        self.quiet(sub)

    def test_delete_retained_clears_expiry_metadata(self):
        self.start()
        pub = self.client('retained-delete-source')
        self.publish(pub, 'delete/value', b'expiring', retain=True, properties=[(2, 1)])
        self.publish(pub, 'delete/value', b'', retain=True)
        self.publish(pub, 'delete/value', b'no expiry', retain=True)
        time.sleep(1.15)
        sub = self.client('retained-delete-reader')
        self.subscribe(sub, [('delete/#', 1)])
        self.message(sub, 'delete/value', b'no expiry', retain=True)

    def test_offline_messages_expire_without_reconnect_and_free_queue(self):
        self.start('--outbound-capacity', '2')
        target = self.client('offline-target', expiry=60)
        self.subscribe(target, [('offline/#', 2)])
        self.disconnect(target)
        pub = self.client('offline-source')
        for qos in (1, 2):
            self.publish(pub, f'offline/{qos}', b'expiring', qos=qos,
                         packet_id=50 + qos, properties=[(2, 1)])
        time.sleep(1.2)
        self.publish(pub, 'offline/live', b'after expiry', qos=2, packet_id=60)
        target = self.client('offline-target', clean=False, expiry=60, present=True)
        self.message(target, 'offline/live', b'after expiry', qos=2)
        self.quiet(target)

    def test_offline_remaining_interval_and_mixed_version_strip(self):
        self.start()
        target = self.client('offline-remaining', expiry=60)
        self.subscribe(target, [('offline-remaining', 1)])
        self.disconnect(target)
        pub = self.client('offline-remaining-source')
        self.publish(pub, 'offline-remaining', b'waiting', properties=[(2, 3)])
        time.sleep(1.15)
        target = self.client('offline-remaining', clean=False, expiry=60, present=True)
        self.ttl_message(target, 'offline-remaining', b'waiting', maximum=2)
        legacy = self.client('legacy-reader', version=4)
        self.subscribe(legacy, [('mixed-expiry', 1)], version=4)
        self.publish(pub, 'mixed-expiry', b'legacy live', properties=[(2, 60)])
        self.message(legacy, 'mixed-expiry', b'legacy live', version=4)
        self.publish(pub, 'mixed-expiry', b'legacy expired', properties=[(2, 0)])
        self.quiet(legacy)

    def test_absent_and_uint_max_expiry_are_distinct_and_supported(self):
        self.start()
        sub = self.observer('forever/#')
        pub = self.client('forever-source')
        self.publish(pub, 'forever/absent', b'absent', retain=True)
        self.message(sub, 'forever/absent', b'absent')
        self.publish(pub, 'forever/finite-max', b'finite', retain=True, properties=[(2, 0xffffffff)])
        self.ttl_message(sub, 'forever/finite-max', b'finite', maximum=0xffffffff)
        if self.sqlite:
            records = {x['topic']: x for x in self.snapshot()['retained']}
            self.assertIsNone(records['forever/absent']['expires_at'])
            deadline = records['forever/finite-max']['expires_at']
            self.assertIsNotNone(deadline)
            remaining = deadline - time.time() * 1000
            self.assertGreater(remaining, 0xffffffff * 1000 - 5000)
            self.assertLessEqual(remaining, 0xffffffff * 1000 + 1000)

    def test_expired_incoming_qos2_completes_and_duplicate_cannot_reset_deadline(self):
        self.start()
        sub = self.observer('incoming-expiry')
        pub = self.client('incoming-expiry-source', expiry=60)
        pub.sendall(wire.publication5('incoming-expiry', b'original', 2, 71, [(2, 1)]))
        wire.parse_ack5(receive(pub), 0x50, 71)
        time.sleep(.65)
        pub.sendall(wire.publication5('incoming-expiry', b'changed', 2, 71,
                                      [(2, 60)], dup=True))
        wire.parse_ack5(receive(pub), 0x50, 71)
        time.sleep(.65)
        pub.sendall(ack(0x62, 71))
        wire.parse_ack5(receive(pub), 0x70, 71)
        self.quiet(sub)
        self.publish(pub, 'incoming-expiry', b'reused', qos=2, packet_id=71)
        self.message(sub, 'incoming-expiry', b'reused')

    def test_expired_retained_incoming_qos2_replaces_old_on_pubrel(self):
        self.start()
        pub = self.client('expired-retained-input')
        self.publish(pub, 'expired-input-retain', b'old', retain=True)
        pub.sendall(wire.publication5('expired-input-retain', b'expired new', 2, 72,
                                      [(2, 1)], retain=True))
        wire.parse_ack5(receive(pub), 0x50, 72)
        time.sleep(1.15)
        pub.sendall(ack(0x62, 72))
        wire.parse_ack5(receive(pub), 0x70, 72)
        sub = self.client('expired-retained-reader')
        self.subscribe(sub, [('expired-input-retain', 1)])
        self.quiet(sub)

    def test_started_qos1_and_qos2_replay_after_expiry_keep_id_and_handshake(self):
        self.start()
        pub = self.client('started-source')
        for qos in (1, 2):
            name = f'started-{qos}'
            target = self.client(name, expiry=60)
            self.subscribe(target, [(name, qos)])
            self.publish(pub, name, b'started', qos=qos, packet_id=80 + qos,
                         properties=[(2, 1)])
            old = self.ttl_message(target, name, b'started', qos=qos, complete=False)
            self.abort(target)
            time.sleep(1.2)
            target = self.client(name, clean=False, expiry=60, present=True)
            replay = self.ttl_message(target, name, b'started', qos=qos,
                                      maximum=0, dup=True, complete=False)
            self.assertEqual(replay[3], old[3])
            self.complete(target, replay, barrier=True)

    def test_started_awaitcomp_survives_expiry_without_publish_replay(self):
        self.start()
        pub = self.client('awaitcomp-source')
        target = self.client('awaitcomp-target', expiry=60)
        self.subscribe(target, [('awaitcomp-expiry', 2)])
        self.publish(pub, 'awaitcomp-expiry', b'owned by client', qos=2, properties=[(2, 1)])
        old = self.ttl_message(target, 'awaitcomp-expiry', b'owned by client', qos=2, complete=False)
        target.sendall(ack(0x50, old[3]))
        wire.parse_ack5(receive(target), 0x62, old[3])
        self.abort(target)
        time.sleep(1.2)
        target = self.client('awaitcomp-target', clean=False, expiry=60, present=True)
        wire.parse_ack5(receive(target), 0x62, old[3])
        target.sendall(ack(0x70, old[3]))
        self.barrier(target)

    def test_slow_writer_drops_expired_first_delivery_and_stays_connected(self):
        self.start('--max-packet-size', str(8 * 1024 * 1024),
                   '--max-state-bytes', str(128 * 1024 * 1024), '--write-timeout-ms', '120000')
        # Set the receive window before TCP negotiates window scaling. Changing
        # it after CONNECT leaves a previously advertised large window in flight.
        target = socket.socket()
        self.addCleanup(target.close)
        target.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024)
        target.settimeout(125)
        target.connect(('127.0.0.1', self.broker.port))
        target.sendall(wire.connect5('slow-reader', properties=[(0x21, 4), (0x11, 60)], keepalive=0))
        self.connack(target)
        self.subscribe(target, [('slow/#', 1)])
        pub = self.client('slow-source', keepalive=0)
        pub.settimeout(125)
        payload = b'x' * (6 * 1024 * 1024)
        # QoS0 keeps the large TCP blocker out of repeated durable snapshots.
        self.publish(pub, 'slow/blocker', payload, qos=0)
        for qos in (0, 1, 2):
            self.publish(pub, f'slow/expired-{qos}', b'must not start', qos=qos,
                         packet_id=110 + qos, properties=[(2, 5)])
        if self.sqlite:
            # Prove at least one timed QoS exchange is reserved but not started
            # behind the blocker; kernel-buffered bytes count as already written.
            self.wait_snapshot(lambda state: any(
                item['expires_at'] is not None and not item['started']
                for session in state['sessions'] if session['client_id'] == 'slow-reader'
                for item in session['outgoing']))
        time.sleep(5.3)
        target.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        value = wire.parse5(receive(target))
        self.assertEqual(value[0], 'slow/blocker')
        self.assertEqual(len(value[1]), len(payload))
        self.assertEqual(hashlib.sha256(value[1]).digest(), hashlib.sha256(payload).digest())
        self.complete(target, value)
        self.barrier(target)  # A leaked expired PUBLISH would precede PINGRESP.

    def test_delayed_will_fires_after_deadline_and_only_once(self):
        self.start()
        sub = self.observer()
        source = self.will_source(delay=1)
        self.abort(source)
        self.quiet(sub)
        self.message(sub, 'will/event', b'owner')
        self.quiet(sub)

    def test_reconnect_cancels_delayed_will_and_installs_replacement(self):
        self.start()
        sub = self.observer()
        source = self.will_source('reconnect-owner', delay=2)
        self.abort(source)
        source = self.client('reconnect-owner', clean=False, expiry=60, present=True,
                             will=('will/event', b'replacement', 1, False),
                             will_properties=[(0x18, 1)])
        time.sleep(2.2)
        self.quiet(sub)
        self.abort(source)
        self.message(sub, 'will/event', b'replacement')
        self.quiet(sub)

    def test_reconnect_without_new_will_cancels_waiting_intent(self):
        self.start()
        sub = self.observer()
        source = self.will_source('no-new-will', delay=1)
        self.abort(source)
        source = self.client('no-new-will', clean=False, expiry=60, present=True)
        time.sleep(1.2)
        self.quiet(sub)
        self.disconnect(source)
        self.quiet(sub)

    def test_normal_disconnect_cancels_will_but_nonzero_reasons_delay_it(self):
        self.start()
        sub = self.observer()
        normal = self.will_source('normal', delay=1)
        self.disconnect(normal)
        time.sleep(1.15)
        self.quiet(sub)
        for reason in (0x04, 0x80):
            source = self.will_source(f'reason-{reason}', delay=1)
            self.disconnect(source, reason=reason)
            self.quiet(sub)
            self.message(sub, 'will/event', f'reason-{reason}'.encode())

    def test_session_expiry_zero_forces_immediate_will_despite_delay(self):
        self.start()
        sub = self.observer()
        source = self.will_source('zero-session', delay=60, expiry=0)
        self.abort(source)
        self.message(sub, 'will/event', b'zero-session')

    def test_shorter_session_expiry_caps_delay_and_removes_old_session(self):
        self.start()
        sub = self.observer()
        source = self.will_source('short-session', delay=60, expiry=1)
        self.abort(source)
        self.quiet(sub)
        self.message(sub, 'will/event', b'short-session')
        source = self.client('short-session', clean=False, expiry=60, present=False)
        self.disconnect(source)

    def test_infinite_session_preserves_delay_and_remains_resumable(self):
        self.start()
        sub = self.observer()
        source = self.will_source('infinite-session', delay=1, expiry=0xffffffff)
        self.abort(source)
        self.quiet(sub)
        self.message(sub, 'will/event', b'infinite-session')
        source = self.client('infinite-session', clean=False, expiry=60, present=True)
        self.disconnect(source)

    def test_disconnect04_expiry_override_zero_forces_session_end_will(self):
        self.start()
        sub = self.observer()
        source = self.will_source('override-zero', delay=60, expiry=60)
        self.disconnect(source, reason=0x04, expiry=0)
        self.message(sub, 'will/event', b'override-zero')
        self.client('override-zero', clean=False, expiry=60, present=False)

    def test_clean_start_true_ends_old_session_and_forces_will_now(self):
        self.start()
        sub = self.observer()
        source = self.will_source('clean-reset', delay=60)
        self.abort(source)
        replacement = self.client('clean-reset', clean=True, expiry=60)
        self.message(sub, 'will/event', b'clean-reset')
        self.disconnect(replacement)

    def test_clean_start_discards_full_old_self_queue_before_forced_will(self):
        self.start('--outbound-capacity', '1')
        source = self.client('self-capacity', expiry=60,
                             will=('self-capacity/topic', b'old Will', 1, False),
                             will_properties=[(0x18, 60)])
        self.subscribe(source, [('self-capacity/topic', 1)])
        self.abort(source)
        pub = self.client('self-fill')
        self.publish(pub, 'self-capacity/topic', b'old queued')
        observer = self.observer('self-capacity/topic')
        replacement = self.client('self-capacity', clean=True, expiry=60,
                                  will=('self-capacity/topic', b'new Will', 1, False))
        self.message(observer, 'self-capacity/topic', b'old Will')
        self.publish(pub, 'self-capacity/topic', b'fresh session probe')
        self.message(observer, 'self-capacity/topic', b'fresh session probe')
        self.quiet(replacement)  # The clean session must not inherit its old subscription.
        self.abort(replacement)
        self.message(observer, 'self-capacity/topic', b'new Will')
        self.quiet(observer)

    def test_active_takeover_resume_cancels_but_clean_reset_publishes(self):
        self.start()
        sub = self.observer()
        old = self.will_source('takeover-delay', delay=1)
        replacement = self.client('takeover-delay', clean=False, expiry=60, present=True)
        self.terminal(old, 0x8e)
        time.sleep(1.2)
        self.quiet(sub)
        self.disconnect(replacement)
        old = self.will_source('takeover-reset', delay=60)
        replacement = self.client('takeover-reset', clean=True, expiry=60)
        self.terminal(old, 0x8e)
        self.message(sub, 'will/event', b'takeover-reset')
        self.disconnect(replacement)

    def test_active_takeover_expiry_zero_and_delay_zero_publish_old_will(self):
        self.start()
        sub = self.observer()
        for name, expiry, delay in [('old-zero', 0, 60), ('delay-zero', 60, 0)]:
            old = self.will_source(name, delay=delay, expiry=expiry)
            replacement = self.client(name, clean=False, expiry=60, present=expiry != 0)
            self.terminal(old, 0x8e)
            self.message(sub, 'will/event', name.encode())
            self.disconnect(replacement)

    def test_will_message_lifetime_starts_after_delay_not_connect(self):
        self.start()
        sub = self.observer()
        source = self.will_source('delayed-ttl', delay=2, retain=True, properties=[(2, 1)])
        time.sleep(1.2)  # More than TTL has elapsed before the connection closes.
        self.abort(source)
        self.quiet(sub)
        self.ttl_message(sub, 'will/event', b'delayed-ttl', maximum=1)
        time.sleep(1.2)
        replay = self.client('will-expired-reader')
        self.subscribe(replay, [('will/#', 1)])
        self.quiet(replay)

    def test_capacity_blocked_will_does_not_consume_its_message_lifetime(self):
        self.start('--max-retained', '1')
        pub = self.client('capacity-fill-source')
        self.publish(pub, 'capacity/occupied', b'occupied', retain=True)
        sub = self.observer()
        source = self.will_source('capacity-will', delay=1, retain=True, properties=[(2, 1)])
        self.abort(source)
        time.sleep(2.3)
        self.quiet(sub)
        self.publish(pub, 'capacity/occupied', b'', retain=True)
        self.ttl_message(sub, 'will/event', b'capacity-will', maximum=1)
        self.quiet(sub)

    def test_rejected_other_owner_connect_cannot_cancel_delayed_will(self):
        self.broker = AuthBroker(self.directory.name, self.sqlite)
        self.addCleanup(self.broker.stop)
        self.start()
        observer = self.client('auth-observer', username='admin', password=PASSWORD)
        self.subscribe(observer, [('allowed/#', 1)])
        source = self.client('auth-will-owner', expiry=60, username='alice', password=PASSWORD,
                             will=('allowed/will', b'protected', 1, False),
                             will_properties=[(0x18, 1)])
        self.abort(source)
        rejected = self.raw()
        rejected.sendall(wire.connect5('auth-will-owner', clean=False,
                                      properties=[(0x11, 60)], username='bob', password=PASSWORD))
        self.connack(rejected, code=0x87)
        self.closed(rejected)
        self.message(observer, 'allowed/will', b'protected')

    def test_timer_only_prunes_offline_and_retained_without_mqtt_activity(self):
        self.require_sqlite()
        self.start()
        target = self.client('idle-timer-target', expiry=60)
        self.subscribe(target, [('idle-timer', 1)])
        self.disconnect(target)
        pub = self.client('idle-timer-source')
        self.publish(pub, 'idle-timer', b'timed', retain=True, properties=[(2, 1)])
        self.disconnect(pub)
        value = self.wait_snapshot(lambda s: not s['retained'] and
                                   not next(x for x in s['sessions']
                                            if x['client_id'] == 'idle-timer-target')['queued'])
        session = next(x for x in value['sessions'] if x['client_id'] == 'idle-timer-target')
        self.assertEqual(session['subscriptions'], [['idle-timer', 1]])

    def test_retained_and_offline_deadlines_survive_downtime(self):
        self.require_sqlite()
        self.start()
        target = self.client('crash-offline', expiry=60)
        self.subscribe(target, [('crash-expiry/#', 1)])
        self.disconnect(target)
        pub = self.client('crash-expiry-source')
        self.publish(pub, 'crash-expiry/value', b'expiring', retain=True, properties=[(2, 1)])
        self.broker.stop(kill=True)
        time.sleep(1.2)
        self.broker.start()
        target = self.client('crash-offline', clean=False, expiry=60, present=True)
        self.quiet(target)
        check = self.client('crash-retained-reader')
        self.subscribe(check, [('crash-expiry/#', 1)])
        self.quiet(check)

    def test_started_outgoing_and_expired_incoming_qos2_survive_crash(self):
        self.require_sqlite()
        self.start()
        target = self.client('crash-started', expiry=60)
        self.subscribe(target, [('crash-started', 2)])
        pub = self.client('crash-started-source', expiry=60)
        self.publish(pub, 'crash-started', b'first', qos=2, properties=[(2, 1)])
        first = self.ttl_message(target, 'crash-started', b'first', qos=2, complete=False)
        pub.sendall(wire.publication5('crash-started', b'pending expired', 2, 99, [(2, 1)]))
        wire.parse_ack5(receive(pub), 0x50, 99)
        self.broker.stop(kill=True)
        time.sleep(1.2)
        self.broker.start()
        target = self.client('crash-started', clean=False, expiry=60, present=True)
        replay = self.ttl_message(target, 'crash-started', b'first', qos=2, maximum=0,
                                  dup=True, complete=False)
        self.assertEqual(first[3], replay[3])
        self.complete(target, replay, barrier=True)
        pub = self.client('crash-started-source', clean=False, expiry=60, present=True)
        pub.sendall(ack(0x62, 99))
        wire.parse_ack5(receive(pub), 0x70, 99)
        self.quiet(target)

    def test_known_will_schedule_survives_restarts_without_extension(self):
        self.require_sqlite()
        self.start()
        source = self.will_source('known-schedule', delay=3, retain=True)
        self.abort(source)
        first = self.wait_snapshot(lambda s: any(w['pending'] for w in s['wills']))
        due = first['wills'][0]['due_at']
        self.broker.stop(kill=True)
        time.sleep(.4)
        self.broker.start()
        value = self.wait_snapshot(lambda s: any(w['pending'] for w in s['wills']))
        self.assertEqual(value['wills'][0]['due_at'], due)
        self.broker.stop(kill=True)
        time.sleep(max(0, (due - time.time() * 1000) / 1000) + .2)
        self.broker.start()
        sub = self.client('overdue-will-reader')
        self.subscribe(sub, [('will/#', 1)])
        self.message(sub, 'will/event', b'known-schedule', retain=True)
        self.assertEqual(self.snapshot()['wills'], [])

    def test_active_crash_schedules_once_and_resume_cancels(self):
        self.require_sqlite()
        self.start()
        self.will_source('active-crash', delay=3, retain=True)
        self.broker.stop(kill=True)
        self.broker.start()
        value = self.wait_snapshot(lambda s: any(w['pending'] for w in s['wills']))
        due = value['wills'][0]['due_at']
        source = self.client('active-crash', clean=False, expiry=60, present=True)
        self.assertEqual(self.snapshot()['wills'], [])
        time.sleep(max(0, (due - time.time() * 1000) / 1000) + .2)
        observer = self.client('active-crash-reader')
        self.subscribe(observer, [('will/#', 1)])
        self.quiet(observer)
        self.disconnect(source)

    def test_schema4_migration_preserves_old_data_and_does_not_invent_ttl(self):
        self.require_sqlite()
        self.start()
        target = self.client('legacy4-target', version=4, clean=False)
        self.subscribe(target, [('legacy4', 1)], version=4)
        self.disconnect(target, version=4)
        pub = self.client('legacy4-source')
        self.publish(pub, 'legacy4', b'unlimited')
        self.disconnect(pub)
        self.broker.stop()
        with sqlite3.connect(self.broker.db) as db:
            value = schema4_fixture(json.loads(db.execute('SELECT data FROM broker_snapshot').fetchone()[0]))
            db.execute('UPDATE broker_snapshot SET data=? WHERE id=1', (json.dumps(value).encode(),))
        self.broker.start()
        target = self.client('legacy4-target', clean=False, expiry=60, present=True)
        self.message(target, 'legacy4', b'unlimited')
        self.assertEqual(self.snapshot()['schema_version'], 5)

    def test_legacy_schema4_cannot_smuggle_previously_unsupported_ttl(self):
        self.require_sqlite()
        self.start()
        pub = self.client('legacy4-corrupt-source')
        self.publish(pub, 'legacy4-corrupt', b'old unlimited', retain=True)
        self.disconnect(pub)
        self.broker.stop()
        with sqlite3.connect(self.broker.db) as db:
            value = schema4_fixture(json.loads(db.execute('SELECT data FROM broker_snapshot').fetchone()[0]))
            value['retained'][0]['properties'] = [{'MessageExpiryInterval': 60}]
            damaged = json.dumps(value).encode()
            db.execute('UPDATE broker_snapshot SET data=? WHERE id=1', (damaged,))
        self.broker.assert_start_fails()
        with sqlite3.connect(self.broker.db) as db:
            self.assertEqual(bytes(db.execute('SELECT data FROM broker_snapshot').fetchone()[0]), damaged)

    def test_legacy_schema4_will_cannot_smuggle_new_relative_message_ttl(self):
        self.require_sqlite()
        self.start()
        self.will_source('legacy4-will', delay=0, retain=True)
        self.broker.stop(kill=True)
        with sqlite3.connect(self.broker.db) as db:
            value = schema4_fixture(json.loads(db.execute('SELECT data FROM broker_snapshot').fetchone()[0]))
            self.assertEqual(len(value['wills']), 1)
            value['wills'][0]['will']['properties'] = [{'MessageExpiryInterval': 60}]
            damaged = json.dumps(value).encode()
            db.execute('UPDATE broker_snapshot SET data=? WHERE id=1', (damaged,))
        self.broker.assert_start_fails()
        with sqlite3.connect(self.broker.db) as db:
            self.assertEqual(bytes(db.execute('SELECT data FROM broker_snapshot').fetchone()[0]), damaged)

    def test_missing_schema5_clock_floor_rejects_startup_without_reset(self):
        self.require_sqlite()
        self.start()
        pub = self.client('clock-floor-source')
        self.publish(pub, 'clock-floor/value', b'persist', retain=True)
        self.disconnect(pub)
        self.broker.stop()
        with sqlite3.connect(self.broker.db) as db:
            value = json.loads(db.execute('SELECT data FROM broker_snapshot').fetchone()[0])
        self.assertEqual(value['schema_version'], 5)
        self.assertIn('clock_floor', value)
        value.pop('clock_floor')
        damaged = json.dumps(value).encode()
        with sqlite3.connect(self.broker.db) as db:
            db.execute('UPDATE broker_snapshot SET data=? WHERE id=1', (damaged,))
        self.broker.assert_start_fails()
        with sqlite3.connect(self.broker.db) as db:
            self.assertEqual(bytes(db.execute('SELECT data FROM broker_snapshot').fetchone()[0]), damaged)

    def test_missing_new_nullable_deadlines_reject_startup_without_reset(self):
        self.require_sqlite()
        self.start()
        pub = self.client('strict-source')
        self.publish(pub, 'strict-retain', b'timed', retain=True, properties=[(2, 60)])
        self.disconnect(pub)
        self.broker.stop()
        with sqlite3.connect(self.broker.db) as db:
            original = bytes(db.execute('SELECT data FROM broker_snapshot').fetchone()[0])
        value = json.loads(original)
        value['retained'][0].pop('expires_at')
        damaged = json.dumps(value).encode()
        with sqlite3.connect(self.broker.db) as db:
            db.execute('UPDATE broker_snapshot SET data=? WHERE id=1', (damaged,))
        self.broker.assert_start_fails()
        with sqlite3.connect(self.broker.db) as db:
            self.assertEqual(bytes(db.execute('SELECT data FROM broker_snapshot').fetchone()[0]), damaged)


# Reuse helpers without importing/inheriting the previous 42 test methods.
for _name in ('setUp', 'start', 'raw', 'client', 'connack', 'closed', 'terminal', 'barrier',
              'quiet', 'disconnect', 'subscribe', 'publish', 'complete', 'message',
              'require_sqlite', 'snapshot', 'wait_snapshot'):
    setattr(ExpiryWillBlackbox, _name, getattr(wire.Mqtt5Blackbox, _name))

if __name__ == '__main__':
    unittest.main(verbosity=2)
