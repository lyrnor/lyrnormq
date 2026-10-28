#!/usr/bin/env python3
"""Independent MQTT5 subscription options/identifiers with real TCP and recovery."""
from contextlib import closing
import copy
import json
import socket
import sqlite3
import struct
import unittest

import mqtt5_blackbox as wire
from blackbox import frame, receive, string
from qos_blackbox import ack
from auth_acl_blackbox import AuthBroker, PASSWORD
from schema_compat import schema5_fixture, schema6_fixture


def identifiers(properties):
    return [value for key, value in properties if key == 11]


class SubscriptionBlackbox(unittest.TestCase):
    def sub(self, sock, filters, identifier=None, packet_id=1, codes=None):
        self.subscribe(sock, filters, properties=[] if identifier is None else [(11, identifier)],
                       packet_id=packet_id, codes=codes)

    def msg(self, sock, topic, payload, qos=1, ids=(), retain=False, app=(), complete=True, dup=False):
        value = wire.parse5(receive(sock))
        self.assertEqual(value[:3], (topic, payload, qos))
        self.assertEqual(value[4:6], (dup, retain))
        self.assertCountEqual(identifiers(value[6]), ids)
        self.assertEqual([(key, v) for key, v in value[6] if key != 11], list(app))
        if complete:self.complete(sock, value)
        return value

    def unsubscribe(self, sock, filters, packet_id=8):
        sock.sendall(frame(0xa2, struct.pack('!H', packet_id) + wire.props()
                           + b''.join(string(topic) for topic in filters)))
        header, body = receive(sock);self.assertEqual(header, 0xb0)
        self.assertEqual(body[:2], struct.pack('!H', packet_id))
        _, offset = wire.readprops(body, 2)
        self.assertEqual(body[offset:], bytes([0] * len(filters)))

    def test_advertised_identifier_capability_and_default_subscription(self):
        self.start();sock = self.raw();sock.sendall(wire.connect5('capability'))
        values = self.connack(sock);self.assertEqual(dict(values).get(0x29, 1), 1)
        self.sub(sock, [('cap/default', 1)])
        pub = self.client('default-source');self.publish(pub, 'cap/default', b'ok')
        self.msg(sock, 'cap/default', b'ok');self.quiet(sock)

    def test_no_local_all_qos_and_other_receiver(self):
        self.start();local = self.client('local');other = self.client('other')
        self.sub(local, [('nl/#', 6)], 11);self.sub(other, [('nl/#', 2)], 12)
        for qos in range(3):
            self.publish(local, 'nl/value', str(qos).encode(), qos)
            self.msg(other, 'nl/value', str(qos).encode(), qos, [12]);self.quiet(local)

    def test_no_local_excluded_matches_cannot_raise_qos_or_leak_identifiers(self):
        self.start();local = self.client('mixed-local')
        self.sub(local, [('mixed/#', 6)], 91)
        self.sub(local, [('mixed/+', 0)], 92, packet_id=2)
        self.publish(local, 'mixed/value', b'one-copy', 2)
        self.msg(local, 'mixed/value', b'one-copy', 0, [92]);self.quiet(local)

    def test_rap_live_all_qos_retained_nonretained_and_delete(self):
        self.start();pub = self.client('rap-source');plain = self.client('rap-plain');kept = self.client('rap-kept')
        self.sub(plain, [('rap/#', 2)], 21);self.sub(kept, [('rap/#', 10)], 22)
        for qos in range(3):
            for retain in (False, True):
                data = bytes([48 + qos, int(retain)])
                self.publish(pub, 'rap/value', data, qos, retain=retain)
                self.msg(plain, 'rap/value', data, qos, [21], False)
                self.msg(kept, 'rap/value', data, qos, [22], retain)
        self.publish(pub, 'rap/value', b'', retain=True)
        self.msg(plain, 'rap/value', b'', ids=[21]);self.msg(kept, 'rap/value', b'', ids=[22], retain=True)
        fresh = self.client('rap-fresh');self.sub(fresh, [('rap/#', 1)]);self.quiet(fresh)

    def test_overlap_deduplicates_with_max_qos_repeated_ids_and_any_rap(self):
        self.start();pub = self.client('overlap-source');sub = self.client('overlap-target')
        self.sub(sub, [('overlap/#', 0), ('overlap/+', 10)], 17)
        self.sub(sub, [('overlap/value', 1)], 18, packet_id=2)
        self.publish(pub, 'overlap/value', b'one', 2, properties=wire.METADATA, retain=True)
        self.msg(sub, 'overlap/value', b'one', 2, [17, 17, 18], True, wire.METADATA);self.quiet(sub)

    def test_identifier_replacement_absence_and_unsubscribe(self):
        self.start();pub = self.client('replace-source');sub = self.client('replace-target')
        for index, identifier in enumerate((1, 268435455, None)):
            self.sub(sub, [('replace', 1)], identifier, packet_id=index + 1)
            self.publish(pub, 'replace', str(index).encode())
            self.msg(sub, 'replace', str(index).encode(), ids=[] if identifier is None else [identifier])
        self.unsubscribe(sub, ['replace']);self.publish(pub, 'replace', b'gone');self.quiet(sub)

    def test_identifier_applies_to_all_accepted_filters(self):
        self.start();pub = self.client('all-source');sub = self.client('all-target')
        self.sub(sub, [('all/a', 0), ('all/b', 1), ('all/c', 2)], 128)
        for index, qos in enumerate(range(3)):
            topic = 'all/' + chr(97 + index);self.publish(pub, topic, b'v', 2)
            self.msg(sub, topic, b'v', qos, [128])

    def test_retain_handling_new_existing_and_replacement(self):
        self.start();pub = self.client('rh-source');self.publish(pub, 'rh/value', b'stored', retain=True)
        for rh in range(3):
            sub = self.client('rh-' + str(rh));option = 1 | (rh << 4)
            self.sub(sub, [('rh/#', option)], 30 + rh)
            if rh < 2:self.msg(sub, 'rh/value', b'stored', ids=[30 + rh], retain=True)
            else:self.quiet(sub)
            self.sub(sub, [('rh/#', option)], 40 + rh, packet_id=2)
            if rh == 0:self.msg(sub, 'rh/value', b'stored', ids=[40 + rh], retain=True)
            else:self.quiet(sub)
            self.sub(sub, [('rh/#', 1)], 50 + rh, packet_id=3)
            self.msg(sub, 'rh/value', b'stored', ids=[50 + rh], retain=True)

    def test_rh1_duplicate_filters_are_processed_sequentially(self):
        self.start();pub = self.client('seq-source');self.publish(pub, 'seq/value', b'stored', retain=True)
        sub = self.client('seq-target');self.sub(sub, [('seq/#', 17), ('seq/#', 17)], 55)
        self.msg(sub, 'seq/value', b'stored', ids=[55], retain=True);self.quiet(sub)
        self.publish(pub, 'seq/value', b'live');self.msg(sub, 'seq/value', b'live', ids=[55]);self.quiet(sub)

    def test_replay_uses_only_triggering_id_and_retain_is_always_one(self):
        self.start();pub = self.client('replay-source');self.publish(pub, 'replay/value', b'stored', retain=True)
        sub = self.client('replay-target');self.sub(sub, [('replay/#', 33)], 101)
        self.sub(sub, [('replay/+', 1)], 102, packet_id=2)
        self.msg(sub, 'replay/value', b'stored', ids=[102], retain=True)
        self.publish(pub, 'replay/value', b'live', retain=True)
        self.msg(sub, 'replay/value', b'live', ids=[101, 102]);self.quiet(sub)

    def test_no_local_does_not_suppress_subscription_triggered_retained_replay(self):
        self.start();same = self.client('retained-author');self.publish(same, 'self/stored', b'own', retain=True)
        self.sub(same, [('self/#', 5)], 71)
        self.msg(same, 'self/stored', b'own', ids=[71], retain=True)
        self.publish(same, 'self/stored', b'new', retain=True);self.quiet(same)

    def test_denied_filter_does_not_contribute_identifier(self):
        self.broker = AuthBroker(self.directory.name, self.sqlite);self.addCleanup(self.broker.stop);self.start()
        pub = self.client('acl-source', username='alice', password=PASSWORD)
        sub = self.client('acl-target', username='alice', password=PASSWORD)
        self.sub(sub, [('allowed/#', 1), ('other/#', 2)], 73, codes=[1, 0x87])
        self.sub(sub, [('allowed/value', 1)], 74, packet_id=2)
        self.publish(pub, 'allowed/value', b'allowed')
        self.msg(sub, 'allowed/value', b'allowed', ids=[73, 74]);self.quiet(sub)

    def test_no_local_self_will_and_other_reader(self):
        self.start();reader = self.client('will-reader');self.sub(reader, [('will/nl', 1)], 80)
        owner = self.client('will-owner', expiry=60, will=('will/nl', b'will', 1, True))
        self.sub(owner, [('will/#', 5)], 81);owner.close()
        self.msg(reader, 'will/nl', b'will', ids=[80])
        resumed = self.client('will-owner', clean=False, expiry=60, present=True);self.quiet(resumed)

    def test_rap_will_live_and_retained_replay(self):
        self.start();reader = self.client('will-rap-reader');self.sub(reader, [('will/rap', 9)], 82)
        owner = self.client('will-rap-owner', will=('will/rap', b'will', 1, True));owner.close()
        self.msg(reader, 'will/rap', b'will', ids=[82], retain=True)
        self.sub(reader, [('will/rap', 1)], 83, packet_id=2)
        self.msg(reader, 'will/rap', b'will', ids=[83], retain=True)

    def test_identifier_zero_duplicate_and_invalid_options_are_errors(self):
        self.start()
        for properties, option, reason in (([(11, 0)], 1, 0x82), ([(11, 1), (11, 2)], 1, 0x82),
                                           ([(11, 1)], 3, 0x82), ([(11, 1)], 49, 0x82),
                                           ([(11, 1)], 65, 0x81)):
            sub = self.client('invalid-' + str(option) + '-' + str(properties))
            sub.sendall(frame(0x82, b'\0\1' + wire.props(properties) + string('invalid') + bytes([option])))
            self.terminal(sub, reason)
        shared = self.client('shared-supported');self.sub(shared, [('ordinary', 1)])
        self.sub(shared, [('$share/g/x', 1)], 5, packet_id=2);self.barrier(shared)

    def test_client_publish_and_will_cannot_supply_server_identifiers(self):
        self.start();pub = self.client('bad-publish')
        pub.sendall(wire.publication5('bad', b'bad', 1, 9, [(11, 9)]));self.terminal(pub, 0x82)
        owner = self.raw();owner.sendall(wire.connect5('bad-will', will=('bad', b'bad', 1, False), will_properties=[(11, 9)]))
        header, body = receive(owner);self.assertEqual(header, 0x20);self.assertIn(body[1], (0x81, 0x82));self.closed(owner)

    def test_identifier_varint_widths_and_user_property_order(self):
        self.start();pub = self.client('width-source');sub = self.client('width-target')
        for index, identifier in enumerate((1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455)):
            self.sub(sub, [('width', 1)], identifier, packet_id=index + 1)
            self.publish(pub, 'width', b'v', properties=wire.METADATA)
            self.msg(sub, 'width', b'v', ids=[identifier], app=wire.METADATA)

    def test_peer_maximum_counts_appended_identifier_and_preserves_other_peer(self):
        self.start();pub = self.client('limit-source');big = self.client('limit-large');small = self.client('limit-small', properties=[(0x27, 64)])
        self.sub(big, [('size', 1)])
        self.sub(small, [('size', 1)], 268435455)
        payload = b'x' * (64 - len(wire.publication5('size', b'', 1)))
        self.publish(pub, 'size', payload);self.msg(big, 'size', payload)
        self.publish(pub, 'size', b'ok');self.msg(small, 'size', b'ok', ids=[268435455]);self.msg(big, 'size', b'ok');self.quiet(small)

    def test_maxqueue_one_retained_expansion_skips_before_capacity(self):
        self.start('--outbound-capacity', '1');pub = self.client('retained-limit-source')
        for topic, payload in [('size/a', b'x' * 256), ('size/b', b'x' * 256), ('size/c', b'fits')]:self.publish(pub, topic, payload, retain=True)
        small = self.client('retained-small', properties=[(0x27, 64)]);self.sub(small, [('size/#', 1)], 268435455)
        self.msg(small, 'size/c', b'fits', ids=[268435455], retain=True);self.quiet(small)
        big = self.client('retained-big')
        for index, topic in enumerate(('size/a', 'size/b')):
            self.sub(big, [(topic, 1)], 10 + index, packet_id=index + 1)
            self.msg(big, topic, b'x' * 256, ids=[10 + index], retain=True)

    def test_snapshot_properties_survive_replacement_and_qos2_pubrel_only(self):
        self.start();pub = self.client('snapshot-source');sub = self.client('snapshot', expiry=60)
        self.sub(sub, [('snapshot', 10)], 111)
        self.publish(pub, 'snapshot', b'old', 2, retain=True)
        old = self.msg(sub, 'snapshot', b'old', 2, [111], True, complete=False)
        sub.sendall(ack(0x50, old[3]));wire.parse_ack5(receive(sub), 0x62, old[3])
        self.sub(sub, [('snapshot', 33)], 112, packet_id=2)
        self.disconnect(sub);sub = self.client('snapshot', clean=False, expiry=60, present=True)
        wire.parse_ack5(receive(sub), 0x62, old[3]);sub.sendall(ack(0x70, old[3]));self.barrier(sub)
        self.publish(pub, 'snapshot', b'new');self.msg(sub, 'snapshot', b'new', ids=[112])

    def test_sqlite_options_ids_and_offline_metadata_survive_crash(self):
        self.require_sqlite();self.start();pub = self.client('durable-source');sub = self.client('durable', expiry=60)
        self.sub(sub, [('durable/#', 26)], 201);self.disconnect(sub)
        self.publish(pub, 'durable/value', b'offline', 2, retain=True, properties=wire.METADATA)
        self.disconnect(pub);self.broker.restart_after_crash()
        sub = self.client('durable', clean=False, expiry=60, present=True)
        self.msg(sub, 'durable/value', b'offline', 2, [201], True, wire.METADATA)
        self.sub(sub, [('durable/#', 26)], 202, packet_id=2);self.quiet(sub)
        data = self.snapshot();self.assertEqual(data['schema_version'], 7)
        item = next(s for s in data['sessions'] if s['client_id'] == 'durable')['subscriptions'][0]
        self.assertEqual(item, {'filter': 'durable/#', 'qos': 2, 'no_local': False, 'retain_as_published': True, 'retain_handling': 1, 'identifier': 202})

    def test_sqlite_no_local_pending_qos2_origin_survives_crash(self):
        self.require_sqlite();self.start();source = self.client('pending-source', expiry=60)
        self.sub(source, [('pending/#', 6)], 211)
        reader = self.client('pending-reader', expiry=60);self.sub(reader, [('pending/#', 2)], 212);self.disconnect(reader)
        source.sendall(wire.publication5('pending/value', b'pending', 2, 41));wire.parse_ack5(receive(source), 0x50, 41)
        self.broker.restart_after_crash();source = self.client('pending-source', clean=False, expiry=60, present=True)
        source.sendall(ack(0x62, 41));wire.parse_ack5(receive(source), 0x70, 41);self.quiet(source)
        reader = self.client('pending-reader', clean=False, expiry=60, present=True);self.msg(reader, 'pending/value', b'pending', 2, [212])

    def test_sqlite_v5_to311_to5_live_rap_marker_and_identifier(self):
        self.require_sqlite();self.start();pub = self.client('bridge-source');sub = self.client('bridge', expiry=60)
        self.sub(sub, [('bridge/#', 10)], 221);self.disconnect(sub)
        self.publish(pub, 'bridge/value', b'live', retain=True)
        self.disconnect(pub);self.broker.restart_after_crash()
        legacy = self.client('bridge', version=4, clean=False, present=True)
        old = self.message(legacy, 'bridge/value', b'live', version=4, retain=False, complete=False)
        self.disconnect(legacy, 4)
        sub = self.client('bridge', clean=False, expiry=60, present=True)
        again = self.msg(sub, 'bridge/value', b'live', ids=[221], retain=True, dup=True)
        self.assertEqual(old[3], again[3]);self.quiet(sub)

    def test_sqlite_retained_replay_marker_keeps_retain_one_on311(self):
        self.require_sqlite();self.start();pub = self.client('marker-source');self.publish(pub, 'marker', b'stored', retain=True)
        sub = self.client('marker', expiry=60);self.sub(sub, [('marker', 1)], 231)
        old = self.msg(sub, 'marker', b'stored', ids=[231], retain=True, complete=False);self.disconnect(sub)
        self.broker.restart_after_crash();legacy = self.client('marker', version=4, clean=False, present=True)
        value = self.message(legacy, 'marker', b'stored', version=4, retain=True, dup=True)
        self.assertEqual(old[3], value[3])

    def test_sqlite_schema5_defaults_migrate_without_losing_qos_metadata(self):
        self.require_sqlite();self.start();pub = self.client('legacy-source');sub = self.client('legacy', expiry=60)
        self.sub(sub, [('legacy/#', 2)]);self.publish(pub, 'legacy/live', b'old', 1, properties=wire.METADATA)
        old = self.msg(sub, 'legacy/live', b'old', app=wire.METADATA, complete=False);self.disconnect(sub);self.disconnect(pub)
        self.broker.stop(kill=True)
        with closing(sqlite3.connect(self.broker.db)) as db:
            current = json.loads(db.execute('SELECT data FROM broker_snapshot').fetchone()[0]);old5 = schema5_fixture(current)
            db.execute('UPDATE broker_snapshot SET data=?', (json.dumps(old5).encode(),));db.commit()
        self.broker.start();sub = self.client('legacy', clean=False, expiry=60, present=True)
        value = self.msg(sub, 'legacy/live', b'old', app=wire.METADATA, dup=True);self.assertEqual(old[3], value[3])
        data = self.snapshot();self.assertEqual(data['schema_version'], 7)
        record = next(s for s in data['sessions'] if s['client_id'] == 'legacy')['subscriptions'][0]
        self.assertEqual(record, {'filter': 'legacy/#', 'qos': 2, 'no_local': False, 'retain_as_published': False, 'retain_handling': 0, 'identifier': None})

    def test_sqlite_schema6_missing_or_invalid_subscription_state_fails_closed(self):
        self.require_sqlite();self.start();sub = self.client('strict', expiry=60);self.sub(sub, [('strict', 1)], 241)
        self.disconnect(sub);self.broker.stop()
        with closing(sqlite3.connect(self.broker.db)) as db:original = db.execute('SELECT data FROM broker_snapshot').fetchone()[0]
        base = schema6_fixture(json.loads(original))
        cases = []
        for field in ('identifier', 'no_local', 'retain_as_published', 'retain_handling'):
            value = copy.deepcopy(base);del value['sessions'][0]['subscriptions'][0][field];cases.append(value)
        for field, invalid in [('identifier', 0), ('identifier', 268435456), ('retain_handling', 3), ('no_local', 1)]:
            value = copy.deepcopy(base);value['sessions'][0]['subscriptions'][0][field] = invalid;cases.append(value)
        for value in cases:
            damaged = json.dumps(value).encode()
            with closing(sqlite3.connect(self.broker.db)) as db:db.execute('UPDATE broker_snapshot SET data=?', (damaged,));db.commit()
            self.broker.assert_start_fails()
            with closing(sqlite3.connect(self.broker.db)) as db:self.assertEqual(db.execute('SELECT data FROM broker_snapshot').fetchone()[0], damaged)
        with closing(sqlite3.connect(self.broker.db)) as db:db.execute('UPDATE broker_snapshot SET data=?', (original,));db.commit()
        self.broker.start();sub = self.client('strict', clean=False, expiry=60, present=True);self.barrier(sub)


for _name in ('setUp', 'start', 'raw', 'client', 'connack', 'closed', 'terminal', 'barrier', 'quiet',
              'disconnect', 'subscribe', 'publish', 'complete', 'message', 'require_sqlite', 'snapshot', 'wait_snapshot'):
    setattr(SubscriptionBlackbox, _name, getattr(wire.Mqtt5Blackbox, _name))

if __name__ == '__main__':unittest.main(verbosity=2)
