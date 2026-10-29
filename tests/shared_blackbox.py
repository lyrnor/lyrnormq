#!/usr/bin/env python3
"""Independent MQTT5 shared-group TCP/outbox tests; SIGKILL is process recovery only."""
import copy
import json
import select
import socket
import struct
import time
import unittest

import mqtt5_blackbox as wire
from blackbox import frame, receive, string
from qos_blackbox import ack
from auth_acl_blackbox import AuthBroker, PASSWORD, policy
import subscription_blackbox as subscription
from schema_compat import schema6_fixture
from state_store import export_bytes, export_state, legacy_blob, write_legacy


class SharedBlackbox(unittest.TestCase):
    def group(self, sock, pattern='work/#', group='workers', qos=1, identifier=None, options=None, packet_id=1):
        full = '$share/' + group + '/' + pattern
        self.sub(sock, [(full, qos if options is None else options)], identifier, packet_id)
        return full

    def chosen(self, sockets):
        ready, _, _ = select.select(sockets, [], [], 5)
        self.assertEqual(len(ready), 1, 'one recipient per group per publication')
        return ready[0]

    def test_capability_and_round_robin_one_copy_all_qos(self):
        self.start();pub = self.client('source');a = self.client('a');b = self.client('b')
        cap = self.raw();cap.sendall(wire.connect5('cap'));self.assertEqual(dict(self.connack(cap))[0x2a], 1)
        self.group(a, qos=2, identifier=11);self.group(b, qos=2, identifier=22)
        selected = []
        for n, qos in enumerate([0, 1, 2, 0, 1, 2]):
            self.publish(pub, 'work/a', str(n).encode(), qos)
            target = self.chosen([a, b]);selected.append(target)
            self.msg(target, 'work/a', str(n).encode(), qos, [11 if target is a else 22]);self.barrier(target)
        self.assertEqual(selected, [a, b, a, b, a, b]);self.quiet(a);self.quiet(b)

    def test_group_keys_include_full_filter_and_ordinary_is_separate(self):
        self.start();pub = self.client('source');sub = self.client('single')
        self.group(sub, 'work/#', 'same', identifier=11)
        self.group(sub, 'work/+', 'same', identifier=12, packet_id=2)
        self.group(sub, 'work/#', 'different', identifier=13, packet_id=3)
        self.sub(sub, [('work/a', 1)], 14, packet_id=4)
        self.publish(pub, 'work/a', b'four')
        ids = []
        for _ in range(4):
            value = wire.parse5(receive(sub));self.assertEqual(value[:3], ('work/a', b'four', 1))
            ids.append([v for k, v in value[6] if k == 11]);self.complete(sub, value)
        self.assertCountEqual(ids, [[11], [12], [13], [14]]);self.barrier(sub);self.quiet(sub)

    def test_no_retained_replay_for_all_retain_handling_replacement(self):
        self.start();pub = self.client('source');self.publish(pub, 'work/a', b'old', retain=True)
        members = []
        for rh in range(3):
            sub = self.client('rh' + str(rh));self.group(sub, group='rh' + str(rh), options=1 | rh << 4, identifier=30 + rh)
            self.quiet(sub);self.group(sub, group='rh' + str(rh), options=1 | rh << 4, identifier=40 + rh, packet_id=2);self.quiet(sub);members.append(sub)
        self.publish(pub, 'work/a', b'live', retain=True)
        for rh, sub in enumerate(members):self.msg(sub, 'work/a', b'live', ids=[40 + rh]);self.quiet(sub)

    def test_rap_and_selected_identifier_snapshot(self):
        self.start();pub = self.client('source');sub = self.client('sub')
        self.group(sub, options=9, identifier=51);self.publish(pub, 'work/a', b'live', retain=True)
        old = self.msg(sub, 'work/a', b'live', ids=[51], retain=True, complete=False)
        self.group(sub, options=1, identifier=52, packet_id=2)
        self.complete(sub, old);self.barrier(sub)
        self.publish(pub, 'work/a', b'next', retain=True);self.msg(sub, 'work/a', b'next', ids=[52])

    def test_wildcards_system_topics_and_literal_nested_share_namespace(self):
        self.start();pub = self.client('source');normal = self.client('normal');system = self.client('system');literal = self.client('literal')
        self.group(normal, '#');self.group(system, '$SYS/+', identifier=61)
        self.group(literal, '$share/literal/+', identifier=62)
        self.publish(pub, '$SYS/load', b'system');self.msg(system, '$SYS/load', b'system', ids=[61]);self.quiet(normal)
        self.publish(pub, '$share/literal/a', b'literal');self.msg(literal, '$share/literal/a', b'literal', ids=[62]);self.quiet(normal)
        self.publish(pub, 'empty//tail', b'normal');self.msg(normal, 'empty//tail', b'normal')

    def test_malformed_filters_rejected_per_filter_and_nolocal_is_protocol_error(self):
        self.start();sub = self.client('invalid')
        filters = ['$share//x', '$share/g/', '$share/g+/x', '$share/g#/x', '$share/g/a/#/b', '$share/g/a+']
        self.subscribe(sub, [(f, 1) for f in filters] + [('ordinary', 1)], codes=[0x8f] * len(filters) + [1])
        sub.sendall(frame(0x82, b'\0\2' + wire.props() + string('$share/g/x') + b'\5'))
        self.terminal(sub, 0x82)

    def test_v311_creation_denied_without_disconnecting_normal_subscription(self):
        self.start();sub = self.client('legacy', version=4)
        self.subscribe(sub, [('$share/g/x', 1), ('plain', 1)], version=4, codes=[0x80, 1])
        pub = self.client('source');self.publish(pub, 'plain', b'yes');self.message(sub, 'plain', b'yes', version=4)

    def test_acl_checks_underlying_filter_and_literal_grants(self):
        value = policy();value['users'][1]['publish'] += ['$share/literal/#'];value['users'][1]['subscribe'] += ['$share/literal/#']
        self.broker = AuthBroker(self.directory.name, self.sqlite, value);self.addCleanup(self.broker.stop);self.start()
        pub = self.client('source', username='alice', password=PASSWORD);sub = self.client('sub', username='alice', password=PASSWORD)
        self.subscribe(sub, [('$share/g/allowed/#', 1), ('$share/g/other/#', 1), ('$share/g/$share/literal/#', 1)], codes=[1, 0x87, 1])
        self.publish(pub, 'allowed/a', b'allowed');self.msg(sub, 'allowed/a', b'allowed')
        self.publish(pub, '$share/literal/a', b'literal');self.msg(sub, '$share/literal/a', b'literal')

    def test_peer_oversize_member_skipped_and_online_preferred(self):
        self.start();pub = self.client('source');small = self.client('a-small', properties=[(0x27, 64)]);large = self.client('z-large')
        offline = self.client('0-offline', expiry=60);self.group(offline);self.disconnect(offline)
        self.group(small);self.group(large)
        self.publish(pub, 'work/a', b'x' * 128);self.msg(large, 'work/a', b'x' * 128);self.quiet(small)
        resumed = self.client('0-offline', clean=False, expiry=60, present=True);self.quiet(resumed)

    def test_qos0_never_queued_offline(self):
        self.start();pub = self.client('source');sub = self.client('offline', expiry=60);self.group(sub);self.disconnect(sub)
        self.publish(pub, 'work/a', b'zero', qos=0)
        resumed = self.client('offline', clean=False, expiry=60, present=True);self.quiet(resumed)
        self.publish(pub, 'work/a', b'online');self.msg(resumed, 'work/a', b'online')

    def test_last_unsubscribe_drops_unstarted_offline_group_copy(self):
        self.start();pub = self.client('source');sub = self.client('member', expiry=60, maximum=1);full = self.group(sub)
        self.publish(pub, 'work/a', b'first');old = self.msg(sub, 'work/a', b'first', complete=False)
        self.publish(pub, 'work/a', b'queued');self.unsubscribe(sub, [full])
        self.complete(sub, old);self.barrier(sub);self.quiet(sub)
        self.group(sub, packet_id=3);self.quiet(sub)
        self.publish(pub, 'work/a', b'new');self.msg(sub, 'work/a', b'new')

    def test_offline_qos1_and_qos2_recover_to_same_owner(self):
        self.start();pub = self.client('source');sub = self.client('owner', expiry=60);self.group(sub, qos=2, identifier=71);self.disconnect(sub)
        for qos in (1, 2):self.publish(pub, 'work/a', bytes([qos]), qos)
        sub = self.client('owner', clean=False, expiry=60, present=True)
        for qos in (1, 2):self.msg(sub, 'work/a', bytes([qos]), qos, [71])

    def test_qos2_pubrel_only_resume_no_other_member_reassignment(self):
        self.start();pub = self.client('source');a = self.client('a-owner', expiry=60);b = self.client('b-other', expiry=60)
        self.group(a, qos=2, identifier=81);self.group(b, qos=2, identifier=82)
        self.publish(pub, 'work/a', b'exactly', 2);old = self.msg(a, 'work/a', b'exactly', 2, [81], complete=False)
        a.sendall(ack(0x50, old[3]));wire.parse_ack5(receive(a), 0x62, old[3]);self.disconnect(a);self.quiet(b)
        a = self.client('a-owner', clean=False, expiry=60, present=True)
        wire.parse_ack5(receive(a), 0x62, old[3]);a.sendall(ack(0x70, old[3]));self.barrier(a);self.quiet(a);self.quiet(b)

    def test_negative_puback_does_not_reassign(self):
        self.start();pub = self.client('source');a = self.client('a');b = self.client('b')
        self.group(a);self.group(b);self.publish(pub, 'work/a', b'rejected')
        old = self.msg(a, 'work/a', b'rejected', complete=False);a.sendall(wire.ack5(0x40, old[3], 0x80));self.barrier(a);self.quiet(b)
        self.publish(pub, 'work/a', b'next');self.msg(b, 'work/a', b'next')

    def test_qos1_owned_copy_waits_for_original_recipient(self):
        self.start();pub = self.client('source');a = self.client('a-owner', expiry=60);b = self.client('b-other', expiry=60)
        self.group(a, identifier=85);self.group(b, identifier=86)
        self.publish(pub, 'work/a', b'owned');old = self.msg(a, 'work/a', b'owned', ids=[85], complete=False);self.disconnect(a);self.quiet(b)
        self.publish(pub, 'work/a', b'new');self.msg(b, 'work/a', b'new', ids=[86])
        a = self.client('a-owner', clean=False, expiry=60, present=True)
        resumed = self.msg(a, 'work/a', b'owned', ids=[85], dup=True);self.assertEqual(resumed[3], old[3]);self.quiet(b)

    def test_full_member_queue_skipped_without_losing_other_group_output(self):
        self.start('--outbound-capacity', '1');pub = self.client('source');a = self.client('a', maximum=1);b = self.client('b')
        self.group(a);self.group(b)
        self.publish(pub, 'work/a', b'first');old = self.msg(a, 'work/a', b'first', complete=False)
        self.publish(pub, 'work/a', b'second');self.msg(b, 'work/a', b'second');self.barrier(b)
        self.publish(pub, 'work/a', b'queued')
        self.publish(pub, 'work/a', b'fourth');self.msg(b, 'work/a', b'fourth');self.barrier(b)
        self.publish(pub, 'work/a', b'skip-full');self.msg(b, 'work/a', b'skip-full');self.barrier(b)
        self.complete(a, old);self.msg(a, 'work/a', b'queued');self.quiet(a)

    def test_shared_offline_message_expiry_does_not_reappear(self):
        self.start();pub = self.client('source');sub = self.client('owner', expiry=60);self.group(sub);self.disconnect(sub)
        self.publish(pub, 'work/a', b'expired', properties=[(2, 1)]);time.sleep(1.2)
        sub = self.client('owner', clean=False, expiry=60, present=True);self.quiet(sub)
        self.publish(pub, 'work/a', b'live');self.msg(sub, 'work/a', b'live')

    def test_sqlite_schema7_membership_and_offline_metadata_survive_sigkill(self):
        self.require_sqlite();self.start();pub = self.client('source');sub = self.client('owner', expiry=60);full = self.group(sub, qos=2, identifier=91, options=10);self.disconnect(sub)
        self.publish(pub, 'work/a', b'durable', 2, retain=True, properties=wire.METADATA);self.disconnect(pub)
        value = self.snapshot();self.assertEqual(value['schema_version'], 7)
        owner = next(s for s in value['sessions'] if s['client_id'] == 'owner')
        self.assertEqual(owner['queued'][0]['shared_subscription'], full)
        self.broker.restart_after_crash();sub = self.client('owner', clean=False, expiry=60, present=True)
        self.msg(sub, 'work/a', b'durable', 2, [91], True, wire.METADATA);self.quiet(sub)

    def test_sqlite_v5_membership_resumes311_unsubscribe_then_v5(self):
        self.require_sqlite();self.start();pub = self.client('source');sub = self.client('bridge', expiry=60);full = self.group(sub);self.disconnect(sub)
        self.publish(pub, 'work/a', b'offline');self.broker.restart_after_crash()
        old = self.client('bridge', version=4, clean=False, present=True);self.message(old, 'work/a', b'offline', version=4)
        old.sendall(frame(0xa2, b'\0\5' + string(full)));self.assertEqual(receive(old), (0xb0, b'\0\5'));self.disconnect(old, 4)
        sub = self.client('bridge', clean=False, expiry=60, present=True);pub = self.client('new-source');self.publish(pub, 'work/a', b'gone');self.quiet(sub)

    def test_sqlite_started_qos2_survives_crash_and_last_member_unsubscribe(self):
        self.require_sqlite();self.start();pub = self.client('source');sub = self.client('owner', expiry=60);full = self.group(sub, qos=2, identifier=101)
        self.publish(pub, 'work/a', b'owned', 2);old = self.msg(sub, 'work/a', b'owned', 2, [101], complete=False)
        sub.sendall(ack(0x50, old[3]));wire.parse_ack5(receive(sub), 0x62, old[3]);self.unsubscribe(sub, [full]);self.broker.restart_after_crash()
        sub = self.client('owner', clean=False, expiry=60, present=True);wire.parse_ack5(receive(sub), 0x62, old[3]);sub.sendall(ack(0x70, old[3]));self.barrier(sub);self.quiet(sub)

    def test_sqlite_ordinary_schema6_migration_preserves_inflight_and_options(self):
        self.require_sqlite();self.start();pub = self.client('source');sub = self.client('legacy6', expiry=60)
        self.sub(sub, [('legacy/#', 10)], 111);self.publish(pub, 'legacy/a', b'old', retain=True)
        old = self.msg(sub, 'legacy/a', b'old', ids=[111], retain=True, complete=False);self.disconnect(sub);self.disconnect(pub);self.broker.stop(kill=True)
        write_legacy(self.broker.db, schema6_fixture(export_state(self.broker.db)))
        self.broker.start();sub = self.client('legacy6', clean=False, expiry=60, present=True)
        value = self.msg(sub, 'legacy/a', b'old', ids=[111], retain=True, dup=True);self.assertEqual(value[3], old[3])
        self.barrier(sub);self.assertEqual(self.snapshot()['schema_version'], 7)

    def test_sqlite_schema7_recipient_markers_are_required_and_validated(self):
        self.require_sqlite();self.start();pub = self.client('source');sub = self.client('strict7', expiry=60);self.group(sub);self.disconnect(sub)
        self.publish(pub, 'work/a', b'offline');self.disconnect(pub);self.broker.stop(kill=True)
        original = export_bytes(self.broker.db)
        base = json.loads(original);target = next(i for i, s in enumerate(base['sessions']) if s['client_id'] == 'strict7')
        cases = []
        value = copy.deepcopy(base);del value['sessions'][target]['queued'][0]['shared_subscription'];cases.append(value)
        for marker in ('ordinary', '$share//work/#', '$share/g/invalid/#/more', 7):
            value = copy.deepcopy(base);value['sessions'][target]['queued'][0]['shared_subscription'] = marker;cases.append(value)
        value = copy.deepcopy(base);value['sessions'][target]['subscriptions'][0]['no_local'] = True;cases.append(value)
        for value in cases:
            damaged = json.dumps(value).encode()
            write_legacy(self.broker.db, damaged)
            self.broker.assert_start_fails()
            self.assertEqual(legacy_blob(self.broker.db), damaged)
        write_legacy(self.broker.db, original)
        self.broker.start();sub = self.client('strict7', clean=False, expiry=60, present=True);self.msg(sub, 'work/a', b'offline')


for _name in ('setUp', 'start', 'raw', 'client', 'connack', 'closed', 'terminal', 'barrier', 'quiet',
              'disconnect', 'subscribe', 'publish', 'complete', 'message', 'require_sqlite', 'snapshot', 'wait_snapshot'):
    setattr(SharedBlackbox, _name, getattr(wire.Mqtt5Blackbox, _name))
for _name in ('sub', 'msg', 'unsubscribe'):
    setattr(SharedBlackbox, _name, getattr(subscription.SubscriptionBlackbox, _name))

if __name__ == '__main__':unittest.main(verbosity=2)
