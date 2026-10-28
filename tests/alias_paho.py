#!/usr/bin/env python3
"""Real Paho2.1 IO/QoS with explicit receive-alias adapter.

Paho2.1's _handle_publish does not resolve TopicAlias. This test-only subclass
resolves at packet receipt BEFORE Paho caches QoS2; all sockets, parsing, callbacks,
ACKs and handshakes otherwise remain Paho. Raw wire tests independently verify the
broker's actual topic/alias encoding. No outbound alias behavior is patched.
"""
import os
import struct
import tempfile
import unittest

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from mqtt5_paho import Client, publication_properties, metadata
from retained_will_blackbox import FeatureBroker
from auth_acl_blackbox import AuthBroker, PASSWORD


class AliasMqttClient(mqtt.Client):
    def __init__(self, *args, receive_alias_maximum=0, **kwargs):
        self.receive_alias_maximum = receive_alias_maximum
        self.receive_aliases = {}
        self.alias_packets = []
        super().__init__(*args, **kwargs)

    def _handle_connack(self):
        self.receive_aliases.clear()
        return super()._handle_connack()

    def _handle_publish(self):
        if self._protocol == mqtt.MQTTv5:
            packet = self._in_packet['packet']
            length = struct.unpack('!H', packet[:2])[0]
            topic = bytes(packet[2:2 + length])
            offset = 2 + length
            if (self._in_packet['command'] >> 1) & 3:offset += 2
            properties = Properties(PacketTypes.PUBLISH);properties.unpack(packet[offset:])
            alias = getattr(properties, 'TopicAlias', None)
            if alias is not None:
                assert 1 <= alias <= self.receive_alias_maximum, ('invalid incoming alias', alias)
                if topic:self.receive_aliases[alias] = topic
                else:
                    assert alias in self.receive_aliases, ('unbound incoming alias', alias)
                    resolved = self.receive_aliases[alias]
                    self._in_packet['packet'] = struct.pack('!H', len(resolved)) + resolved + bytes(packet[2:])
            else:assert topic, 'empty topic without incoming alias'
            self.alias_packets.append((topic, alias))
        return super()._handle_publish()


class AliasPaho(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-alias-paho-');self.addCleanup(self.directory.cleanup)
        self.sqlite = bool(os.environ.get('RMQTT_TEST_STORAGE_PATH'))
        self.broker = FeatureBroker(self.directory.name, self.sqlite);self.broker.start();self.addCleanup(self.broker.stop)

    def client(self, name, aliases=0, **kwargs):
        maximum = aliases if kwargs.get('version', 5) == 5 else 0
        factory = lambda *args, **values: AliasMqttClient(*args, receive_alias_maximum=maximum, **values)
        result = Client(self.broker, name, topic_alias_maximum=aliases, client_factory=factory, **kwargs)
        self.addCleanup(result.disconnect)
        if result.version == 5:self.assertEqual(result.connect_properties.TopicAliasMaximum, 32)
        return result

    def send(self, client, topic, payload, alias=None, qos=1, retain=False, app=False):
        properties = publication_properties() if app else Properties(PacketTypes.PUBLISH)
        if alias is not None:properties.TopicAlias = alias
        value = client.value.publish(topic, payload, qos=qos, retain=retain, properties=properties)
        value.wait_for_publish(timeout=5);self.assertTrue(value.is_published())

    def test_actual_bind_rebind_reference_and_qos_matrix(self):
        pub = self.client('paho-alias-source');sub = self.client('paho-alias-target', aliases=2)
        sub.subscribe([('paho/alias/#', 2)])
        for qos in range(3):
            topic = 'paho/alias/long/' + str(qos)
            self.send(pub, topic, b'bind', alias=1, qos=qos, app=True);sub.expect(topic, b'bind', qos)
            self.send(pub, '', b'ref', alias=1, qos=qos, app=True);sub.expect(topic, b'ref', qos)
        self.assertTrue(any(not topic and alias for topic, alias in sub.value.alias_packets))

    def test_actual_default_and_zero_peer_alias_limit_use_full_topics(self):
        pub = self.client('paho-noalias-source');sub = self.client('paho-noalias-target')
        sub.subscribe([('paho/noalias/long/topic', 1)])
        for data in (b'one', b'two'):
            self.send(pub, 'paho/noalias/long/topic', data);sub.expect('paho/noalias/long/topic', data, 1, with_metadata=False)
        self.assertEqual(sub.value.alias_packets, [(b'paho/noalias/long/topic', None)] * 2)

    def test_actual_retained_metadata_and_outbound_replacement(self):
        pub = self.client('paho-retained-alias');sub = self.client('paho-retained-reader', aliases=1)
        for topic in ('paho/retained/one/long', 'paho/retained/two/long'):
            self.send(pub, topic, b'stored', alias=1, retain=True, app=True)
        sub.subscribe([('paho/retained/#', 1)])
        sub.expect('paho/retained/one/long', b'stored', 1, True);sub.expect('paho/retained/two/long', b'stored', 1, True)
        self.send(pub, '', b'new', alias=1, app=True);sub.expect('paho/retained/two/long', b'new', 1)

    def test_actual_bidirectional_map_independence_and_legacy_bridge(self):
        same = self.client('paho-directional', aliases=1);legacy = self.client('paho-legacy', version=4)
        same.subscribe([('paho/direction/out/long', 2)]);legacy.subscribe([('paho/direction/in/long', 2)])
        self.send(same, 'paho/direction/in/long', b'input', alias=1, qos=2);legacy.expect('paho/direction/in/long', b'input', 2, with_metadata=False)
        legacy.publish('paho/direction/out/long', b'output', 2, with_metadata=False);same.expect('paho/direction/out/long', b'output', 2, with_metadata=False)
        self.send(same, '', b'input-ref', alias=1, qos=2);legacy.expect('paho/direction/in/long', b'input-ref', 2, with_metadata=False)

    def test_actual_authenticated_resolved_topic_and_reconnect_reset(self):
        self.broker.stop();self.broker = AuthBroker(self.directory.name, self.sqlite);self.broker.start();self.addCleanup(self.broker.stop)
        pub = self.client('paho-auth-alias', username='alice', password=PASSWORD, expiry=60)
        sub = self.client('paho-auth-reader', aliases=1, username='alice', password=PASSWORD);sub.subscribe([('allowed/#', 1)])
        self.send(pub, 'allowed/long/topic', b'bind', alias=1, app=True);sub.expect('allowed/long/topic', b'bind', 1)
        pub.disconnect();pub = self.client('paho-auth-alias', username='alice', password=PASSWORD, expiry=60, clean=False, present=True)
        self.send(pub, 'allowed/long/topic', b'new-binding', alias=1, app=True);sub.expect('allowed/long/topic', b'new-binding', 1)
        self.send(pub, '', b'ref', alias=1, app=True);sub.expect('allowed/long/topic', b'ref', 1)

    def test_actual_sqlite_offline_recovery_is_canonical_and_rebinds_output(self):
        if not self.sqlite:self.skipTest('SQLite process recovery only')
        sub = self.client('paho-durable-alias', aliases=1, expiry=60);sub.subscribe([('paho/durable/long/topic', 2)]);sub.disconnect()
        pub = self.client('paho-durable-source');self.send(pub, 'paho/durable/long/topic', b'queued', alias=1, qos=2, app=True);pub.disconnect()
        self.broker.restart_after_crash()
        sub = self.client('paho-durable-alias', aliases=1, expiry=60, clean=False, present=True);sub.expect('paho/durable/long/topic', b'queued', 2)
        self.assertEqual(sub.value.alias_packets[0][0], b'paho/durable/long/topic')


if __name__ == '__main__':unittest.main(verbosity=2)
