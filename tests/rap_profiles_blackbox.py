#!/usr/bin/env python3
"""Independent TCP acceptance of frozen mixed-RAP policy and endpoint profiles.

Actual producer-format assertions remain producer responsibilities. Crash tests are
process recovery, not power-loss certification. Every method owns its broker/DB.
"""
import os
import struct
import unittest

import mqtt5_blackbox as wire
import subscription_blackbox as subs
from auth_acl_blackbox import AuthBroker, PASSWORD
from blackbox import frame, receive, string
from qos_blackbox import ack
from retained_will_blackbox import connect_packet as connect311


class RapProfilesBlackbox(unittest.TestCase):
    setUp = wire.Mqtt5Blackbox.setUp
    sub = subs.SubscriptionBlackbox.sub
    msg = subs.SubscriptionBlackbox.msg

    def mixed(self, sock, topic='mixed/value', qos0=1, qos1=1, same_id=False):
        self.sub(sock, [(topic.rsplit('/', 1)[0] + '/#', qos0 | 0x20)], 11)
        self.sub(sock, [(topic, qos1 | 0x28)], 11 if same_id else 22, packet_id=2)

    def copies(self, sock, topic, payload, expected, app=()):
        found = [];receipts = set()
        while len(found) < len(expected) or receipts:
            packet = receive(sock)
            if packet[0] >> 4 == 6:
                identifier = wire.parse_ack5(packet, 0x62)
                self.assertIn(identifier, receipts);receipts.remove(identifier)
                sock.sendall(ack(0x70, identifier));continue
            value = wire.parse5(packet)
            self.assertEqual(value[:2], (topic, payload))
            self.assertEqual([(k, v) for k, v in value[6] if k != 11], list(app))
            found.append((value[2], value[5], tuple(subs.identifiers(value[6]))))
            if value[2] == 1:sock.sendall(ack(0x40, value[3]))
            elif value[2] == 2:
                receipts.add(value[3]);sock.sendall(ack(0x50, value[3]))
        self.assertCountEqual(found, expected)
        return found

    def test_mixed_retained_publication_has_per_filter_qos_rap_and_identifier(self):
        self.start();pub = self.client('source');target = self.client('target')
        self.mixed(target, qos0=0, qos1=2)
        self.sub(target, [('mixed/+', 0x21)], 33, packet_id=3)
        self.publish(pub, 'mixed/value', b'three', 2, properties=wire.METADATA, retain=True)
        self.copies(target, 'mixed/value', b'three', [(0, False, (11,)), (2, True, (22,)), (1, False, (33,))], wire.METADATA)
        self.quiet(target)

    def test_repeated_identifiers_stay_one_per_selected_copy(self):
        self.start();pub = self.client('source');target = self.client('target')
        self.mixed(target, same_id=True)
        self.publish(pub, 'mixed/value', b'equal', retain=True)
        self.copies(target, 'mixed/value', b'equal', [(1, False, (11,)), (1, True, (11,))]);self.quiet(target)

    def test_nonretained_and_homogeneous_rap_still_collapse(self):
        self.start();pub = self.client('source');target = self.client('target')
        self.mixed(target, qos0=0, qos1=2)
        self.publish(pub, 'mixed/value', b'plain', 2)
        self.msg(target, 'mixed/value', b'plain', 2, [11, 22]);self.quiet(target)
        self.sub(target, [('mixed/#', 0x2a)], 11, packet_id=3)
        self.publish(pub, 'mixed/value', b'homogeneous', 2, retain=True)
        self.msg(target, 'mixed/value', b'homogeneous', 2, [11, 22], True);self.quiet(target)

    def test_no_local_filter_is_excluded_before_mixedness(self):
        self.start();target = self.client('same')
        self.sub(target, [('mixed/#', 0x26)], 11)
        self.sub(target, [('mixed/value', 0x29)], 22, packet_id=2)
        self.publish(target, 'mixed/value', b'own', 2, retain=True)
        self.msg(target, 'mixed/value', b'own', 1, [22], True);self.quiet(target)

    def test_denied_filter_cannot_change_rap_qos_or_identifiers(self):
        self.broker = AuthBroker(self.directory.name, self.sqlite);self.addCleanup(self.broker.stop);self.start()
        pub = self.client('source', username='alice', password=PASSWORD)
        target = self.client('target', username='single', password=PASSWORD)
        self.sub(target, [('allowed/value', 0x21)], 11)
        self.sub(target, [('allowed/#', 0x2a)], 22, packet_id=2, codes=[0x87])
        self.publish(pub, 'allowed/value', b'eligible', 2, retain=True)
        self.msg(target, 'allowed/value', b'eligible', 1, [11], False);self.quiet(target)

    def test_shared_copy_stays_separate_from_both_ordinary_copies(self):
        self.start();pub = self.client('source');target = self.client('target')
        self.mixed(target)
        self.sub(target, [('$share/g/mixed/#', 0x29)], 33, packet_id=3)
        self.publish(pub, 'mixed/value', b'three', retain=True)
        self.copies(target, 'mixed/value', b'three', [(1, False, (11,)), (1, True, (22,)), (1, True, (33,))]);self.quiet(target)

    def test_recipient_copies_are_admitted_per_partition_against_capacity(self):
        # Phase4: a recipient's ordinary copies are one atomic delivery; the
        # shared copy is a separate delivery. Overflow drops only those copies,
        # the publisher is acknowledged and the retained value is stored.
        # Admission limits queued records, not records already moved to outgoing.
        # Keep this persistent recipient offline until source acknowledgement so
        # ordinary delivery cannot race Poll/BeginDelivery and free queue space
        # before the separate shared copy is offered.
        for shared in (False, True):
            with self.subTest(shared=shared):
                self.start('--outbound-capacity', '2' if shared else '1')
                pub = self.client('source');target = self.client('target', expiry=60);self.mixed(target)
                if shared:self.sub(target, [('$share/g/mixed/#', 0x21)], 33, packet_id=3)
                self.disconnect(target)
                pub.sendall(wire.publication5('mixed/value', b'stored', 1, 91, retain=True))
                wire.parse_ack5(receive(pub), 0x40, 91)
                target = self.client('target', clean=False, expiry=60, present=True, maximum=1)
                if shared:
                    self.copies(target, 'mixed/value', b'stored', [(1, False, (11,)), (1, True, (22,))])
                self.quiet(target)
                replay = self.client('replay');self.sub(replay, [('mixed/#', 1)])
                self.msg(replay, 'mixed/value', b'stored', retain=True);self.quiet(replay)
                self.broker.stop()

    def test_preexisting_queue_drops_only_the_overflowing_shared_copy(self):
        self.start('--outbound-capacity', '3');pub = self.client('source')
        witness = self.client('a-witness');self.sub(witness, [('mixed/#', 1)], 44)
        blocked = self.client('z-blocked', expiry=60, maximum=1);self.mixed(blocked)
        self.sub(blocked, [('$share/g/mixed/#', 0x21)], 33, packet_id=3)
        self.publish(pub, 'mixed/value', b'old', 1)
        self.msg(witness, 'mixed/value', b'old', 1, [44])
        first = self.msg(blocked, 'mixed/value', b'old', 1, [11, 22], complete=False)
        self.quiet(blocked)
        pub.sendall(wire.publication5('mixed/value', b'new', 1, 92, retain=True))
        wire.parse_ack5(receive(pub), 0x40, 92)
        self.msg(witness, 'mixed/value', b'new', 1, [44]);self.quiet(witness)
        self.complete(blocked, first)
        self.msg(blocked, 'mixed/value', b'old', 1, [33])
        self.msg(blocked, 'mixed/value', b'new', 1, [11])
        self.msg(blocked, 'mixed/value', b'new', 1, [22], retain=True);self.quiet(blocked)
        replay = self.client('replay');self.sub(replay, [('mixed/#', 1)])
        self.msg(replay, 'mixed/value', b'new', retain=True);self.quiet(replay)

    def test_receive_maximum_and_captured_metadata_survive_unsubscribe(self):
        self.start();pub = self.client('source');target = self.client('target', expiry=60, maximum=1)
        self.mixed(target)
        self.publish(pub, 'mixed/value', b'captured', retain=True)
        first = wire.parse5(receive(target));self.assertEqual(first[2], 1);self.quiet(target)
        target.sendall(frame(0xa2, b'\0\7' + wire.props() + string('mixed/#') + string('mixed/value')))
        header, body = receive(target);self.assertEqual(header, 0xb0);self.assertEqual(body[:2], b'\0\7')
        self.complete(target, first)
        second = wire.parse5(receive(target));self.complete(target, second)
        self.assertCountEqual([(v[5], tuple(subs.identifiers(v[6]))) for v in (first, second)], [(False, (11,)), (True, (22,))])
        self.publish(pub, 'mixed/value', b'gone', retain=True);self.quiet(target)

    def test_qos2_repeated_pubrel_does_not_refanout_mixed_copies(self):
        self.start();pub = self.client('source');target = self.client('target');self.mixed(target, qos0=2, qos1=2)
        pub.sendall(wire.publication5('mixed/value', b'once', 2, 65535, retain=True));wire.parse_ack5(receive(pub), 0x50, 65535)
        pub.sendall(ack(0x62, 65535));wire.parse_ack5(receive(pub), 0x70, 65535)
        self.copies(target, 'mixed/value', b'once', [(2, False, (11,)), (2, True, (22,))])
        pub.sendall(ack(0x62, 65535));wire.parse_ack5(receive(pub), 0x70, 65535, reason=0x92);self.quiet(target)

    def test_peer_cap_is_applied_to_each_split_copy_not_combined_identifiers(self):
        self.start();pub = self.client('source');target = self.client('target', properties=[(0x27, 20)])
        self.mixed(target, topic='m/v')
        self.publish(pub, 'm/v', b'12345678', retain=True)
        choices = []
        for _ in range(2):
            packet = receive(target);self.assertEqual(len(packet[1]) + 2, 20)
            value = wire.parse5(packet);self.assertEqual(value[:3], ('m/v', b'12345678', 1))
            choices.append((value[5], tuple(subs.identifiers(value[6]))));self.complete(target, value)
        self.assertCountEqual(choices, [(False, (11,)), (True, (22,))]);self.quiet(target)

    def test_outgoing_alias_resolution_keeps_split_copy_metadata(self):
        self.start();pub = self.client('source');target = self.client('target', properties=[(0x22, 1)])
        self.mixed(target)
        aliases = {};choices = []
        for payload in (b'first', b'next'):
            self.publish(pub, 'mixed/value', payload, retain=True)
            for _ in range(2):
                value = wire.parse5(receive(target));topic = value[0]
                alias = dict(value[6]).get(0x23)
                if alias is not None:
                    self.assertEqual(alias, 1)
                    if topic:aliases[alias] = topic
                    else:topic = aliases[alias]
                self.assertEqual((topic, value[1], value[2]), ('mixed/value', payload, 1))
                choices.append((payload, value[5], tuple(subs.identifiers(value[6]))));self.complete(target, value)
        self.assertCountEqual(choices, [(p, r, (i,)) for p in (b'first', b'next') for r, i in [(False, 11), (True, 22)]])

    def test_retained_replay_stays_retain_one_with_triggering_identifier(self):
        self.start();pub = self.client('source');self.publish(pub, 'mixed/value', b'stored', retain=True)
        target = self.client('target');self.mixed(target);self.quiet(target)
        self.sub(target, [('mixed/+', 1)], 33, packet_id=3)
        self.msg(target, 'mixed/value', b'stored', 1, [33], True)

    def test_profiles_accept_only_their_versions_and_refuse_unknown_levels(self):
        for mode, accepted, response in [('both', (4, 5), (0x20, b'\0\1')), ('3.1.1', (4,), (0x20, b'\0\1')), ('5', (5,), (0x20, b'\0\x84\0'))]:
            with self.subTest(profile=mode):
                self.start('--protocol', mode)
                for level in (0, 3, 4, 5, 6, 255):
                    sock = self.raw()
                    if level == 4:packet = connect311('known4')
                    else:
                        packet = bytearray(wire.connect5('known5'));packet[8] = level
                    sock.sendall(packet)
                    if level in accepted:
                        if level == 5:self.connack(sock)
                        else:self.assertEqual(receive(sock), (0x20, b'\0\0'))
                        self.barrier(sock)
                    else:self.assertEqual(receive(sock), response);self.closed(sock)
                wrong = self.raw();packet = bytearray(wire.connect5('wrong'));packet[4:8] = b'NOPE';wrong.sendall(packet);self.closed(wrong)
                self.broker.stop()

    def test_profile_refusal_cannot_takeover_register_will_or_publish_pipeline(self):
        self.start('--protocol', '5');owner = self.client('owned');observer = self.client('observer')
        self.sub(observer, [('forbidden/#', 1)])
        for level in (0, 3, 4, 6, 255):
            sock = self.raw()
            packet = bytearray(connect311('owned', will=('forbidden/will', b'no', 1, True)) if level == 4
                               else wire.connect5('owned', will=('forbidden/will', b'no', 1, True)))
            packet[8] = level
            sock.sendall(packet + wire.publication5('forbidden/data', b'no', 1, 9, retain=True))
            self.assertEqual(receive(sock), (0x20, b'\0\x84\0'));self.closed(sock);self.barrier(owner);self.quiet(observer)
        self.sub(observer, [('forbidden/#', 1)], packet_id=2);self.quiet(observer)

    def test_v5_profile_rejects_known_v311_level_before_malformed_payload(self):
        self.start('--protocol', '5');sock = self.raw()
        packet = bytearray(connect311('x'));packet[-3:-1] = b'\xff\xff'
        sock.sendall(packet + wire.publication5('forbidden/data', b'no', 1, 9, retain=True))
        self.assertEqual(receive(sock), (0x20, b'\0\x84\0'));self.closed(sock)
        self.barrier(self.client('valid-after-refusal'))

    def test_known_v5_peer_cap_limits_v311_profile_refusal(self):
        self.start('--protocol', '3.1.1')
        for maximum in (1, 3, 4, 5):
            sock = self.raw();sock.sendall(wire.connect5('peer', properties=[(0x27, maximum)]))
            if maximum >= 4:self.assertEqual(receive(sock), (0x20, b'\0\1'))
            self.closed(sock)

    def test_sqlite_mixed_copies_restore_with_independent_qos_and_metadata(self):
        self.require_sqlite();self.start('--protocol', '5');pub = self.client('source');target = self.client('durable', expiry=60)
        self.mixed(target, qos0=1, qos1=2);self.disconnect(target)
        self.publish(pub, 'mixed/value', b'offline', 2, retain=True);self.disconnect(pub);self.broker.restart_after_crash()
        target = self.client('durable', clean=False, expiry=60, maximum=1, present=True)
        self.copies(target, 'mixed/value', b'offline', [(1, False, (11,)), (2, True, (22,))]);self.quiet(target)

    def test_sqlite_profile_switch_resumes_session_without_retagging_copies(self):
        self.require_sqlite();self.start('--protocol', '5');pub = self.client('source');target = self.client('durable', expiry=60)
        self.mixed(target);self.disconnect(target);self.publish(pub, 'mixed/value', b'offline', retain=True);self.disconnect(pub)
        self.broker.stop();self.start('--protocol', '3.1.1')
        target = self.client('durable', version=4, clean=False, present=True)
        for _ in range(2):self.message(target, 'mixed/value', b'offline', 1, version=4, retain=False)
        self.disconnect(target, version=4);self.broker.stop();self.start('--protocol', '5')
        target = self.client('durable', clean=False, expiry=60, present=True);pub = self.client('new-source')
        self.publish(pub, 'mixed/value', b'new', retain=True)
        self.copies(target, 'mixed/value', b'new', [(1, False, (11,)), (1, True, (22,))])

    def test_sqlite_old_artifact_captured_collapse_survives_upgrade(self):
        self.require_sqlite()
        old = '/tmp/rmqtt-p2n-baseline'
        if not os.path.isfile(old):self.skipTest('optional actual previous-artifact fixture unavailable')
        final = os.environ.get('RMQTT_BIN')
        try:
            os.environ['RMQTT_BIN'] = old;self.start()
        finally:
            if final is None:os.environ.pop('RMQTT_BIN', None)
            else:os.environ['RMQTT_BIN'] = final
        pub = self.client('source');target = self.client('durable', expiry=60, maximum=1);self.mixed(target, qos0=2, qos1=2)
        self.publish(pub, 'mixed/value', b'old-inflight', 2, retain=True)
        first = self.msg(target, 'mixed/value', b'old-inflight', 2, [11, 22], True, complete=False)
        self.publish(pub, 'mixed/value', b'old-queued', 1, packet_id=10, retain=True)
        self.broker.stop(kill=True);pub.close();target.close();self.start('--protocol', '5')
        target = self.client('durable', clean=False, expiry=60, maximum=1, present=True)
        replay = self.msg(target, 'mixed/value', b'old-inflight', 2, [11, 22], True, complete=False, dup=True)
        self.assertEqual(replay[3], first[3]);self.complete(target, replay)
        self.msg(target, 'mixed/value', b'old-queued', 1, [11, 22], True)
        pub = self.client('new-source');self.publish(pub, 'mixed/value', b'new', 2, retain=True)
        self.copies(target, 'mixed/value', b'new', [(2, False, (11,)), (2, True, (22,))])


for _name in ('start', 'raw', 'client', 'connack', 'closed', 'terminal', 'barrier', 'quiet', 'disconnect', 'subscribe', 'publish', 'complete', 'message', 'require_sqlite', 'snapshot', 'wait_snapshot'):
    setattr(RapProfilesBlackbox, _name, getattr(wire.Mqtt5Blackbox, _name))

if __name__ == '__main__':unittest.main(verbosity=2)
