#!/usr/bin/env python3
"""Real Paho2.1 MQTT5 subscription options/identifier interoperability."""
import os
import tempfile
import unittest

from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.subscribeoptions import SubscribeOptions
import paho.mqtt.client as mqtt
from mqtt5_paho import Client as BasicClient, metadata, EXPECTED_METADATA
from retained_will_blackbox import FeatureBroker


class Client(BasicClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        def on_message(client, userdata, message):
            properties = getattr(message, 'properties', None)
            ids = list(getattr(properties, 'SubscriptionIdentifier', []))
            self.messages.put((message.topic, message.payload, message.qos, message.retain,
                               metadata(properties), ids))
        self.value.on_message = on_message

    def options(self, filters, identifier=None):
        properties = Properties(PacketTypes.SUBSCRIBE)
        if identifier is not None:properties.SubscriptionIdentifier = identifier
        values = [(topic, SubscribeOptions(qos=qos, noLocal=nl, retainAsPublished=rap, retainHandling=rh))
                  for topic, qos, nl, rap, rh in filters]
        status, _ = self.value.subscribe(values, properties=properties)
        assert status == mqtt.MQTT_ERR_SUCCESS
        assert self.subscriptions.get(timeout=5) == [qos for _, qos, _, _, _ in filters]

    def expected(self, topic, payload, qos=1, ids=(), retain=False, app=False):
        actual = self.messages.get(timeout=5)
        assert actual[:5] == (topic, payload, qos, retain, EXPECTED_METADATA if app else {}), actual
        assert sorted(actual[5]) == sorted(ids), (actual[5], ids)


class SubscriptionPaho(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-subscription-paho-')
        self.addCleanup(self.directory.cleanup)
        self.sqlite = bool(os.environ.get('RMQTT_TEST_STORAGE_PATH'))
        self.broker = FeatureBroker(self.directory.name, self.sqlite)
        self.broker.start();self.addCleanup(self.broker.stop)

    def client(self, name, **kwargs):
        value = Client(self.broker, name, **kwargs);self.addCleanup(value.disconnect)
        return value

    def test_actual_no_local_all_qos_and_metadata_identifier(self):
        source = self.client('paho-local');other = self.client('paho-other')
        self.assertEqual(source.connect_properties.SubscriptionIdentifierAvailable, 1)
        source.options([('paho/local', 2, True, False, 0)], 11)
        other.options([('paho/local', 2, False, False, 0)], 12)
        for qos in range(3):
            source.publish('paho/local', str(qos).encode(), qos)
            other.expected('paho/local', str(qos).encode(), qos, [12], app=True)
        source.quiet()

    def test_actual_rap_live_and_rh_replacement(self):
        pub = self.client('paho-rap-source');sub = self.client('paho-rap-target')
        pub.publish('paho/rap', b'initial', retain=True, with_metadata=False)
        sub.options([('paho/rap', 1, False, True, 1)], 21)
        sub.expected('paho/rap', b'initial', ids=[21], retain=True)
        sub.options([('paho/rap', 1, False, True, 1)], 22);sub.quiet()
        pub.publish('paho/rap', b'live', retain=True, with_metadata=False)
        sub.expected('paho/rap', b'live', ids=[22], retain=True)
        sub.options([('paho/rap', 1, False, False, 0)], 23)
        sub.expected('paho/rap', b'live', ids=[23], retain=True)
        pub.publish('paho/rap', b'changed', retain=True, with_metadata=False)
        sub.expected('paho/rap', b'changed', ids=[23], retain=False)

    def test_actual_overlap_repeated_ids_highest_qos_and_nl_filtering(self):
        sub = self.client('paho-overlap');pub = self.client('paho-overlap-source')
        sub.options([('paho/overlap/#', 0, False, False, 0), ('paho/overlap/+', 2, False, True, 0)], 31)
        sub.options([('paho/overlap/value', 1, False, False, 0)], 32)
        pub.publish('paho/overlap/value', b'one', 2, True)
        values = [sub.messages.get(timeout=5) for _ in range(3)]
        self.assertTrue(all(value[:2] == ('paho/overlap/value', b'one') and value[4] == EXPECTED_METADATA for value in values))
        self.assertCountEqual([(v[2], v[3], v[5]) for v in values], [(0, False, [31]), (2, True, [31]), (1, False, [32])]);sub.quiet()
        own = self.client('paho-filtered')
        own.options([('paho/self/#', 2, True, True, 0)], 33)
        own.options([('paho/self/+', 0, False, False, 0)], 34)
        own.publish('paho/self/value', b'own', 2, False, with_metadata=False)
        own.expected('paho/self/value', b'own', 0, [34]);own.quiet()

    def test_actual_rh2_identifier_clear_and_tiny_peer(self):
        pub = self.client('paho-tiny-source');sub = self.client('paho-tiny', packet_maximum=64)
        pub.publish('paho/tiny', b'x' * 256, retain=True, with_metadata=False)
        sub.options([('paho/tiny', 1, False, False, 2)], 268435455);sub.quiet()
        pub.publish('paho/tiny', b'x' * 256, with_metadata=False)
        pub.publish('paho/tiny', b'fits', with_metadata=False)
        sub.expected('paho/tiny', b'fits', ids=[268435455]);sub.quiet()
        sub.options([('paho/tiny', 1, False, False, 2)])
        pub.publish('paho/tiny', b'clear', with_metadata=False)
        sub.expected('paho/tiny', b'clear');sub.quiet()

    def test_actual_will_rap_and_no_local_reconnect(self):
        observer = self.client('paho-will-reader');observer.options([('paho/will', 1, False, True, 0)], 41)
        owner = self.client('paho-will-owner', expiry=60, will=('paho/will', b'will', 1, True))
        owner.options([('paho/will', 1, True, True, 0)], 42);owner.abort()
        observer.expected('paho/will', b'will', ids=[41], retain=True, app=True)
        resumed = self.client('paho-will-owner', clean=False, expiry=60, present=True);resumed.quiet()

    def test_actual_sqlite_options_identifiers_offline_and_311_replacement(self):
        if not self.sqlite:self.skipTest('SQLite persistent311/process recovery only')
        pub = self.client('paho-durable-source');sub = self.client('paho-durable', expiry=60)
        sub.options([('paho/durable/#', 2, False, True, 1)], 51);sub.disconnect()
        pub.publish('paho/durable/value', b'offline', 2, True, with_metadata=False);pub.disconnect()
        self.broker.restart_after_crash()
        sub = self.client('paho-durable', clean=False, expiry=60, present=True)
        sub.expected('paho/durable/value', b'offline', 2, [51], True)
        sub.options([('paho/durable/#', 2, False, True, 1)], 52);sub.quiet();sub.disconnect()
        legacy = self.client('paho-durable', version=4, clean=False, present=True)
        legacy.subscribe([('paho/durable/#', 1)])
        legacy.expected('paho/durable/value', b'offline', 1, retain=True);legacy.disconnect()
        sub = self.client('paho-durable', clean=False, expiry=60, present=True)
        pub = self.client('paho-durable-source-new');pub.publish('paho/durable/value', b'new', retain=True, with_metadata=False)
        sub.expected('paho/durable/value', b'new', 1, retain=False);sub.quiet()


if __name__ == '__main__':unittest.main(verbosity=2)
