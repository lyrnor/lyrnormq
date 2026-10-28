#!/usr/bin/env python3
"""Independent Topic Alias wire/state boundaries, memory and SQLite."""
import json
import struct
import unittest

import mqtt5_blackbox as wire
from blackbox import frame, receive, string
from qos_blackbox import ack
from auth_acl_blackbox import AuthBroker, PASSWORD


class AliasBlackbox(unittest.TestCase):
    def connection(self, name, aliases=None, maximum=None, **kwargs):
        values = []
        if aliases is not None:values.append((0x22, aliases))
        if maximum is not None:values.append((0x27, maximum))
        return self.client(name, properties=values, **kwargs)

    def get(self, sock):
        raw = wire.parse5(receive(sock));topic = raw[0]
        aliases = [value for key, value in raw[6] if key == 0x23]
        self.assertLessEqual(len(aliases), 1)
        mappings = self.maps.setdefault(sock, {})
        if aliases:
            identifier = aliases[0];self.assertGreater(identifier, 0)
            if topic:mappings[identifier] = topic
            else:self.assertIn(identifier, mappings);topic = mappings[identifier]
        else:self.assertTrue(topic, 'empty topic without alias')
        props = [(key, value) for key, value in raw[6] if key != 0x23]
        return (topic,) + raw[1:6] + (props,), raw

    def expected(self, sock, topic, payload, qos=1, properties=(), retain=False, dup=False, complete=True):
        value, raw = self.get(sock)
        self.assertEqual(value[:3], (topic, payload, qos))
        self.assertEqual(value[4:], (dup, retain, list(properties)))
        if complete:self.complete(sock, value)
        return value, raw

    def alias_publish(self, sock, topic, payload, alias=1, **kwargs):
        properties = list(kwargs.pop('properties', ()))
        if alias is not None:properties.append((0x23, alias))
        self.publish(sock, topic, payload, properties=properties, **kwargs)

    def test_default_capability_and_bind_reference_rebind_all_qos(self):
        self.start();raw = self.raw();raw.sendall(wire.connect5('capability'))
        self.assertEqual(dict(self.connack(raw))[0x22], 32)
        sub = self.client('reader');self.subscribe(sub, [('alias/#', 2)])
        for qos in range(3):
            pub = self.client('source-' + str(qos))
            for topic, actual, payload in [('alias/one', 'alias/one', b'bind'), ('', 'alias/one', b'ref'),
                                            ('alias/two', 'alias/two', b'rebind'), ('', 'alias/two', b'ref2')]:
                self.alias_publish(pub, topic, payload, qos=qos, properties=wire.METADATA)
                self.message(sub, actual, payload, qos, wire.METADATA)

    def test_unicode_coalesced_bind_reference_and_control(self):
        self.start();pub = self.client('unicode-source');sub = self.client('unicode-reader')
        topic = 'unicode/温度/🌡';self.subscribe(sub, [('unicode/#', 1)])
        pub.sendall(wire.publication5(topic, b'first', properties=wire.METADATA + [(0x23, 1)])
                    + wire.publication5('', b'second', properties=wire.METADATA + [(0x23, 1)])
                    + frame(0xc0))
        self.assertEqual(receive(pub), (0xd0, b''))
        self.message(sub, topic, b'first', 0, wire.METADATA);self.message(sub, topic, b'second', 0, wire.METADATA)

    def test_configured_zero_disables_both_maps(self):
        self.start('--topic-alias-maximum', '0');raw = self.raw();raw.sendall(wire.connect5('zero'))
        self.assertEqual(dict(self.connack(raw))[0x22], 0)
        sub = self.connection('zero-reader', aliases=65535);self.subscribe(sub, [('zero/long/topic', 1)])
        pub = self.client('zero-source')
        for _ in range(2):
            self.publish(pub, 'zero/long/topic', b'plain');self.message(sub, 'zero/long/topic', b'plain')
        self.alias_publish_bad(raw, 'zero/long/topic', 1, 0x94)

    def alias_publish_bad(self, sock, topic, alias, reason):
        sock.sendall(wire.publication5(topic, b'bad', properties=[(0x23, alias)]));self.terminal(sock, reason)

    def test_configured_boundaries_and_sparse_largest_identifier(self):
        for maximum in (1, 32, 65535):
            with self.subTest(maximum=maximum):
                self.broker.stop();self.start('--topic-alias-maximum', str(maximum))
                pub = self.client('source-' + str(maximum));sub = self.client('target-' + str(maximum));self.subscribe(sub, [('sparse/#', 1)])
                self.alias_publish(pub, 'sparse/value', b'bind', alias=maximum);self.message(sub, 'sparse/value', b'bind')
                self.alias_publish(pub, '', b'ref', alias=maximum);self.message(sub, 'sparse/value', b'ref')
                if maximum < 65535:self.alias_publish_bad(pub, 'sparse/value', maximum + 1, 0x94)

    def test_zero_over_limit_unbound_and_empty_without_alias(self):
        self.start()
        for index, (topic, alias) in enumerate([('invalid', 0), ('invalid', 33), ('', 1)]):
            self.alias_publish_bad(self.client('bad-' + str(index)), topic, alias, 0x94)
        pub = self.client('empty-no-alias');pub.sendall(wire.publication5('', b'bad'));self.terminal(pub, 0x82)

    def test_duplicate_and_wrong_context_preserve_reason_validation(self):
        self.start();pub = self.client('duplicate')
        pub.sendall(wire.publication5('valid', b'bad', properties=[(0x23, 1), (0x23, 2)]));self.terminal(pub, 0x82)
        owner = self.raw();owner.sendall(wire.connect5('will-alias', will=('will', b'bad', 1, False), will_properties=[(0x23, 1)]))
        packet = receive(owner);self.assertEqual(packet[0], 0x20);self.assertIn(packet[1][1], (0x81, 0x82));self.closed(owner)
        sub = self.client('subscribe-alias');sub.sendall(frame(0x82, b'\0\1' + wire.props([(0x23, 1)]) + string('x') + b'\1'));self.terminal(sub, 0x81)

    def test_separate_clients_cannot_borrow_inbound_mapping(self):
        self.start();one = self.client('one');self.alias_publish(one, 'private/one', b'bind')
        self.alias_publish_bad(self.client('two'), '', 1, 0x94)
        self.alias_publish(one, '', b'still-one');self.barrier(one)

    def test_no_outgoing_alias_without_peer_advertisement(self):
        self.start();sub = self.client('plain');self.subscribe(sub, [('plain/long/topic', 2)])
        pub = self.client('plain-source')
        for qos in range(3):
            for _ in range(2):self.publish(pub, 'plain/long/topic', b'data', qos);self.message(sub, 'plain/long/topic', b'data', qos)

    def test_outgoing_binding_precedes_references_each_qos(self):
        self.start();pub = self.client('out-source')
        for qos in range(3):
            topic = 'out/long/topic/' + str(qos);sub = self.connection('out-' + str(qos), aliases=2);self.subscribe(sub, [(topic, 2)])
            self.publish(pub, topic, b'first', qos, properties=wire.METADATA)
            _, first = self.expected(sub, topic, b'first', qos, wire.METADATA);self.assertEqual(first[0], topic);self.assertTrue([v for k, v in first[6] if k == 0x23])
            self.publish(pub, topic, b'second', qos, properties=wire.METADATA)
            _, second = self.expected(sub, topic, b'second', qos, wire.METADATA);self.assertEqual(second[0], '')

    def test_outgoing_bounded_replacement_and_references(self):
        self.start();pub = self.client('replace-source');sub = self.connection('replace-target', aliases=1);self.subscribe(sub, [('replace/#', 1)])
        for topic in ('replace/one/long', 'replace/two/long', 'replace/one/long'):
            self.publish(pub, topic, b'bind');_, raw = self.expected(sub, topic, b'bind');self.assertEqual(raw[0], topic);self.assertEqual([v for k, v in raw[6] if k == 0x23], [1])
            self.publish(pub, topic, b'ref');_, raw = self.expected(sub, topic, b'ref');self.assertEqual(raw[0], '')

    def test_local_outbound_budget_caps_peer_maximum(self):
        self.start('--topic-alias-maximum', '2');pub = self.client('budget-source');sub = self.connection('budget-target', aliases=65535);self.subscribe(sub, [('budget/#', 1)])
        for i in range(6):
            topic = 'budget/long/' + str(i);self.publish(pub, topic, b'v');_, raw = self.expected(sub, topic, b'v');self.assertTrue(all(1 <= value <= 2 for key, value in raw[6] if key == 0x23))

    def test_map_directions_are_independent(self):
        self.start();same = self.connection('directions', aliases=1);self.subscribe(same, [('directions/outgoing', 1)])
        self.alias_publish(same, 'directions/incoming', b'bind', alias=1)
        pub = self.client('directions-source');self.publish(pub, 'directions/outgoing', b'out')
        _, raw = self.expected(same, 'directions/outgoing', b'out');self.assertEqual([v for k, v in raw[6] if k == 0x23], [1])
        reader = self.client('direction-witness');self.subscribe(reader, [('directions/incoming', 1)])
        self.alias_publish(same, '', b'input-ref', alias=1);self.message(reader, 'directions/incoming', b'input-ref')

    def test_binding_fallback_does_not_create_phantom_reference(self):
        self.start();topic = 'fallback/long/topic';cap = 64;sub = self.connection('fallback', aliases=1, maximum=cap);self.subscribe(sub, [(topic, 1)]);pub = self.client('fallback-source')
        payload = b'x' * (cap - len(wire.publication5(topic, b'', 1)))
        for _ in range(2):
            self.publish(pub, topic, payload);_, raw = self.expected(sub, topic, payload);self.assertEqual(raw[0], topic);self.assertFalse([v for k, v in raw[6] if k == 0x23]);self.assertEqual(len(wire.publication5(raw[0], raw[1], 1, raw[3], raw[6])), cap)
        self.publish(pub, topic, b'small');_, raw = self.expected(sub, topic, b'small');self.assertEqual(raw[0], topic);self.assertTrue([v for k, v in raw[6] if k == 0x23])
        self.publish(pub, topic, b'ref');_, raw = self.expected(sub, topic, b'ref');self.assertEqual(raw[0], '')

    def test_encoded_varint_boundaries_keep_metadata(self):
        self.start();pub = self.client('width-source')
        for cap in (127, 128, 129, 130, 131, 16383, 16384, 16385, 16386, 16387):
            topic = 'width/long/' + str(cap);app = [(0x26, ('key', 'v' * (120 if cap < 200 else 128)))] if cap > 200 else []
            sub = self.connection('width-' + str(cap), aliases=1, maximum=cap);self.subscribe(sub, [(topic, 1)])
            for data in (b'a', b'b'):
                self.publish(pub, topic, data, properties=app);_, raw = self.expected(sub, topic, data, properties=app);self.assertLessEqual(len(wire.publication5(raw[0], raw[1], 1, raw[3], raw[6])), cap)

    def test_canonical_peer_limit_policy_is_not_salvaged_by_alias(self):
        self.start();pub = self.client('policy-source');sub = self.connection('policy', aliases=1, maximum=64);self.subscribe(sub, [('policy/very/long/topic', 1)])
        for data in (b'bind', b'ref'):self.publish(pub, 'policy/very/long/topic', data);self.expected(sub, 'policy/very/long/topic', data)
        self.publish(pub, 'policy/very/long/topic', b'x' * 64);self.publish(pub, 'policy/very/long/topic', b'fits');self.expected(sub, 'policy/very/long/topic', b'fits');self.quiet(sub)

    def test_expanded_canonical_resource_limit_is_quota_not_wire_error(self):
        self.start('--max-packet-size', '128');pub = self.client('expand-source')
        topic = 'expand/' + 't' * 80;self.alias_publish(pub, topic, b'bind')
        encoded = wire.publication5('', b'x' * 80, properties=[(0x23, 1)])
        self.assertLessEqual(len(encoded), 128);pub.sendall(encoded);self.terminal(pub, 0x97)

    def test_resolved_topics_obey_nl_rap_subid_and_metadata(self):
        self.start();local = self.client('options-source');reader = self.connection('options-reader', aliases=1)
        self.subscribe(local, [('options/#', 6)], properties=[(11, 41)])
        self.subscribe(reader, [('options/#', 10)], properties=[(11, 42)])
        for topic, payload in [('options/value/long', b'bind'), ('', b'ref')]:
            self.alias_publish(local, topic, payload, qos=2, retain=True, properties=wire.METADATA)
            self.expected(reader, 'options/value/long', payload, 2, wire.METADATA + [(11, 42)], True);self.quiet(local)

    def test_acl_checks_resolved_topic(self):
        self.broker = AuthBroker(self.directory.name, self.sqlite);self.addCleanup(self.broker.stop);self.start()
        pub = self.client('acl-alias', username='alice', password=PASSWORD);reader = self.client('acl-reader', username='alice', password=PASSWORD);self.subscribe(reader, [('allowed/#', 1)])
        self.alias_publish(pub, 'allowed/topic', b'bind');self.message(reader, 'allowed/topic', b'bind')
        self.alias_publish(pub, '', b'ref');self.message(reader, 'allowed/topic', b'ref')
        pub.sendall(wire.publication5('other/denied', b'bad', 1, 12, [(0x23, 1)]));self.terminal(pub, 0x87);self.quiet(reader)

    def test_ttl_and_retained_replay_use_full_canonical_topic(self):
        self.start();pub = self.client('ttl-alias')
        self.alias_publish(pub, 'ttl/value/long', b'bind', properties=[(2, 60)], retain=True)
        self.alias_publish(pub, '', b'new', properties=[(2, 60)], retain=True)
        sub = self.connection('ttl-reader', aliases=1);self.subscribe(sub, [('ttl/#', 1)])
        value, raw = self.get(sub);self.assertEqual(value[:3], ('ttl/value/long', b'new', 1));self.assertTrue(value[5]);self.assertTrue(0 < dict(value[6])[2] <= 60);self.assertEqual(raw[0], 'ttl/value/long');self.complete(sub, value)
        self.alias_publish(pub, '', b'zero', properties=[(2, 0)], retain=True)
        self.subscribe(sub, [('ttl/#', 1)], packet_id=2);self.quiet(sub)

    def test_qos2_duplicate_rebind_cannot_replace_original_publication(self):
        self.start();pub = self.client('q2-source');sub = self.client('q2-reader');self.subscribe(sub, [('q2/#', 2)])
        pub.sendall(wire.publication5('q2/original', b'original', 2, 42, wire.METADATA + [(0x23, 1)], True));wire.parse_ack5(receive(pub), 0x50, 42)
        pub.sendall(wire.publication5('q2/rebound', b'wrong', 2, 42, [(0x23, 1)], False, True));wire.parse_ack5(receive(pub), 0x50, 42)
        self.alias_publish(pub, '', b'new-binding');self.message(sub, 'q2/rebound', b'new-binding')
        pub.sendall(ack(0x62, 42));wire.parse_ack5(receive(pub), 0x70, 42);self.message(sub, 'q2/original', b'original', 2, wire.METADATA)

    def test_reconnect_and_takeover_reset_inbound_map(self):
        self.start();old = self.client('reset', expiry=60);self.alias_publish(old, 'reset/topic', b'old');self.disconnect(old)
        resumed = self.client('reset', clean=False, expiry=60, present=True);self.alias_publish_bad(resumed, '', 1, 0x94)
        owner = self.client('takeover', expiry=60);self.alias_publish(owner, 'takeover/topic', b'old')
        fresh = self.client('takeover', clean=False, expiry=60, present=True);self.terminal(owner, 0x8e);self.alias_publish_bad(fresh, '', 1, 0x94)

    def test_outgoing_replay_rebinds_after_reconnect_and_pubrel_stays_control(self):
        self.start();pub = self.client('replay-source')
        for qos in (1, 2):
            topic = 'replay/long/topic/' + str(qos);name = 'replay-' + str(qos)
            sub = self.connection(name, aliases=1, expiry=60);self.subscribe(sub, [(topic, qos)])
            self.publish(pub, topic, b'old', qos);old, _ = self.expected(sub, topic, b'old', qos, complete=False);self.disconnect(sub)
            sub = self.connection(name, aliases=1, expiry=60, clean=False, present=True)
            replay, raw = self.expected(sub, topic, b'old', qos, dup=True, complete=False);self.assertEqual(replay[3], old[3]);self.assertEqual(raw[0], topic)
            if qos == 2:
                sub.sendall(ack(0x50, replay[3]));wire.parse_ack5(receive(sub), 0x62, replay[3]);self.disconnect(sub)
                sub = self.connection(name, aliases=1, expiry=60, clean=False, present=True);wire.parse_ack5(receive(sub), 0x62, replay[3]);sub.sendall(ack(0x70, replay[3]))
            else:self.complete(sub, replay)
            self.barrier(sub)

    def test_v311_and_v5_bridge_aliases_are_connection_local(self):
        self.start();legacy = self.client('legacy', version=4);sub = self.connection('v5-target', aliases=1);self.subscribe(sub, [('bridge/long/topic', 2)])
        for qos in range(3):self.publish(legacy, 'bridge/long/topic', b'legacy', qos, version=4);self.expected(sub, 'bridge/long/topic', b'legacy', qos)
        legacy_sub = self.client('legacy-target', version=4);self.subscribe(legacy_sub, [('bridge/long/topic', 2)], version=4)
        pub = self.client('bridge-alias');self.alias_publish(pub, 'bridge/long/topic', b'bind', qos=2);self.message(legacy_sub, 'bridge/long/topic', b'bind', 2, version=4)
        self.expected(sub, 'bridge/long/topic', b'bind', 2)
        self.alias_publish(pub, '', b'ref', qos=2);self.message(legacy_sub, 'bridge/long/topic', b'ref', 2, version=4);self.expected(sub, 'bridge/long/topic', b'ref', 2)

    def test_sqlite_qos2_pending_and_retained_snapshot_never_store_aliases(self):
        self.require_sqlite();self.start();pub = self.client('durable-source', expiry=60)
        self.alias_publish(pub, 'durable/retained', b'stored', retain=True)
        pub.sendall(wire.publication5('durable/pending', b'pending', 2, 42, [(0x23, 2)]));wire.parse_ack5(receive(pub), 0x50, 42)
        data = self.snapshot();self.assertEqual(data['schema_version'], 7);self.assertNotIn('TopicAlias', json.dumps(data))
        incoming = next(s for s in data['sessions'] if s['client_id'] == 'durable-source')['incoming'];self.assertEqual(len(incoming), 1)
        self.disconnect(pub);self.broker.restart_after_crash()
        sub = self.connection('durable-reader', aliases=1);self.subscribe(sub, [('durable/#', 2)]);self.expected(sub, 'durable/retained', b'stored', retain=True)
        pub = self.client('durable-source', clean=False, expiry=60, present=True);pub.sendall(ack(0x62, 42));wire.parse_ack5(receive(pub), 0x70, 42);self.expected(sub, 'durable/pending', b'pending', 2)
        self.alias_publish_bad(pub, '', 2, 0x94)

    def test_sqlite_outgoing_retry_has_no_restored_alias_mapping(self):
        self.require_sqlite();self.start();pub = self.client('crash-source');sub = self.connection('crash-target', aliases=1, expiry=60);self.subscribe(sub, [('crash/long/topic', 1)])
        self.alias_publish(pub, 'crash/long/topic', b'old');old, _ = self.expected(sub, 'crash/long/topic', b'old', complete=False);self.disconnect(sub);self.disconnect(pub)
        data = self.snapshot();self.assertNotIn('TopicAlias', json.dumps(data));self.broker.restart_after_crash()
        sub = self.connection('crash-target', aliases=1, expiry=60, clean=False, present=True);replay, raw = self.expected(sub, 'crash/long/topic', b'old', dup=True);self.assertEqual(replay[3], old[3]);self.assertEqual(raw[0], 'crash/long/topic')


for _name in ('setUp', 'start', 'raw', 'client', 'connack', 'closed', 'terminal', 'barrier', 'quiet',
              'disconnect', 'subscribe', 'publish', 'complete', 'message', 'require_sqlite', 'snapshot', 'wait_snapshot'):
    setattr(AliasBlackbox, _name, getattr(wire.Mqtt5Blackbox, _name))
_original_setup = AliasBlackbox.setUp
def _setup(self):
    _original_setup(self);self.maps = {}
AliasBlackbox.setUp = _setup

if __name__ == '__main__':unittest.main(verbosity=2)
