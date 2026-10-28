#!/usr/bin/env python3
"""Actual Paho2.1 dual-version QoS/property/expiry/persistence interoperability."""
import os
import queue
import socket
import tempfile
import threading
import time
import unittest

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from retained_will_blackbox import FeatureBroker
from auth_acl_blackbox import AuthBroker, PASSWORD


def publication_properties(kind=PacketTypes.PUBLISH):
    result = Properties(kind)
    result.UserProperty = [('duplicate', 'one'), ('duplicate', 'two')]
    result.ContentType = 'application/x-test'
    result.PayloadFormatIndicator = 1
    result.ResponseTopic = 'response/topic'
    result.CorrelationData = b'correlation\x00\xff'
    return result


def metadata(value):
    if value is None:
        return {}
    result = {}
    for name in ('UserProperty', 'ContentType', 'PayloadFormatIndicator', 'ResponseTopic', 'CorrelationData'):
        if hasattr(value, name):
            result[name] = getattr(value, name)
    return result


EXPECTED_METADATA = metadata(publication_properties())


class Client:
    def __init__(self, broker, name, version=5, clean=True, expiry=None, maximum=None,
                 will=None, username=None, password=None, accepted=True, present=False, packet_maximum=None, topic_alias_maximum=None, client_factory=mqtt.Client):
        self.version = version
        self.messages = queue.Queue()
        self.subscriptions = queue.Queue()
        self.connected = threading.Event()
        self.disconnected = threading.Event()
        self.reason = None
        self.present = None
        self.connect_properties = None
        self.closed = False
        options = dict(client_id=name, protocol=mqtt.MQTTv5 if version == 5 else mqtt.MQTTv311,
                       reconnect_on_failure=False)
        if version != 5:
            options['clean_session'] = clean
        self.value = client_factory(mqtt.CallbackAPIVersion.VERSION2, **options)
        if username is not None:
            self.value.username_pw_set(username, password)
        if will is not None:
            topic, payload, qos, retain = will
            self.value.will_set(topic, payload, qos, retain,
                                properties=publication_properties(PacketTypes.WILLMESSAGE) if version == 5 else None)

        def on_connect(client, userdata, flags, reason, properties):
            self.reason = reason
            self.present = flags.session_present
            self.connect_properties = properties
            self.connected.set()

        def on_disconnect(client, userdata, flags, reason, properties):
            self.disconnected.set()

        def on_subscribe(client, userdata, mid, reasons, properties):
            self.subscriptions.put([reason.value for reason in reasons])

        def on_message(client, userdata, message):
            self.messages.put((message.topic, message.payload, message.qos, message.retain,
                               metadata(getattr(message, 'properties', None))))

        self.value.on_connect = on_connect
        self.value.on_disconnect = on_disconnect
        self.value.on_subscribe = on_subscribe
        self.value.on_message = on_message
        connect_options = {}
        if version == 5:
            properties = Properties(PacketTypes.CONNECT)
            if expiry is not None:
                properties.SessionExpiryInterval = expiry
            if maximum is not None:
                properties.ReceiveMaximum = maximum
            if packet_maximum is not None:
                properties.MaximumPacketSize = packet_maximum
            if topic_alias_maximum is not None:
                properties.TopicAliasMaximum = topic_alias_maximum
            connect_options = dict(clean_start=clean, properties=properties)
        self.value.connect('127.0.0.1', broker.port, keepalive=5, **connect_options)
        self.value.loop_start()
        try:
            assert self.connected.wait(5), 'missing Paho CONNACK callback'
            assert self.reason.is_failure == (not accepted), ('CONNACK', self.reason)
            if accepted:
                assert self.present == present, ('session_present', self.present, present)
            else:
                assert self.disconnected.wait(5), 'rejected connection did not close'
        except BaseException:
            self.disconnect()
            raise

    def subscribe(self, filters, codes=None):
        status, _ = self.value.subscribe(filters)
        assert status == mqtt.MQTT_ERR_SUCCESS
        expected = [qos for _, qos in filters] if codes is None else codes
        assert self.subscriptions.get(timeout=5) == expected

    def publish(self, topic, payload, qos=1, retain=False, with_metadata=True):
        properties = publication_properties() if self.version == 5 and with_metadata else None
        result = self.value.publish(topic, payload, qos, retain, properties=properties)
        result.wait_for_publish(timeout=5)
        assert result.is_published(), 'Paho publication not acknowledged'

    def expect(self, topic, payload, qos, retain=False, with_metadata=True):
        expected = EXPECTED_METADATA if self.version == 5 and with_metadata else {}
        actual = self.messages.get(timeout=5)
        assert actual == (topic, payload, qos, retain, expected), (actual, expected)

    def quiet(self):
        try:
            unexpected = self.messages.get(timeout=.2)
        except queue.Empty:
            return
        raise AssertionError(('unexpected Paho delivery', unexpected))

    def disconnect(self, expiry=None):
        if self.closed:
            return
        properties = None
        if expiry is not None:
            assert self.version == 5
            properties = Properties(PacketTypes.DISCONNECT)
            properties.SessionExpiryInterval = expiry
        # Paho2.1 closes its local descriptor after sending DISCONNECT; that
        # callback/loop_stop does not prove the broker committed session removal.
        # Its _sock_close uses close(), not shutdown(), so retain an independent
        # descriptor and wait for the remote FIN after the Paho reader stops.
        stream = self.value.socket()
        remote = stream.dup() if stream is not None else None
        try:
            self.value.disconnect(properties=properties)
            self.value.loop_stop()
            self.closed = True
            if remote is not None:
                remote.settimeout(5)
                assert remote.recv(1) == b'', 'unread server output before graceful remote EOF'
        finally:
            if remote is not None:
                remote.close()

    def abort(self):
        if self.closed:
            return
        self.value.loop_stop()
        stream = self.value.socket()
        if stream is not None:
            stream.shutdown(socket.SHUT_RDWR)
            stream.close()
        self.closed = True


class Mqtt5Paho(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-mqtt5-paho-')
        self.addCleanup(self.directory.cleanup)
        self.sqlite = bool(os.environ.get('RMQTT_TEST_STORAGE_PATH'))
        self.broker = FeatureBroker(self.directory.name, self.sqlite)
        self.broker.start()
        self.addCleanup(self.broker.stop)

    def client(self, name, **kwargs):
        result = Client(self.broker, name, **kwargs)
        self.addCleanup(result.disconnect)
        return result

    def test_actual_dual_version_3x3_qos_and_metadata_matrix(self):
        for source_version in (4, 5):
            for target_version in (4, 5):
                name = f'{source_version}-{target_version}'
                source = self.client('paho-source-' + name, version=source_version)
                sub = self.client('paho-target-' + name, version=target_version)
                filters = [(f'paho/matrix/{name}/{grant}', grant) for grant in range(3)]
                sub.subscribe(filters)
                if target_version == 5:
                    self.assertEqual(sub.connect_properties.ReceiveMaximum, 32)
                    self.assertEqual(sub.connect_properties.SharedSubscriptionAvailable, 1)
                    self.assertEqual(sub.connect_properties.SubscriptionIdentifierAvailable, 1)
                for source_qos in range(3):
                    for grant in range(3):
                        with self.subTest(source=source_version, target=target_version,
                                          publication=source_qos, subscription=grant):
                            topic = f'paho/matrix/{name}/{grant}'
                            payload = f'{source_qos}-{grant}'.encode()
                            source.publish(topic, payload, source_qos)
                            sub.expect(topic, payload, min(source_qos, grant),
                                       with_metadata=source_version == 5)
                source.disconnect()
                sub.disconnect()

    def test_actual_retained_metadata_replay_delete_and_abnormal_will(self):
        source = self.client('paho-retained-source')
        source.publish('paho/retained', b'stored', 2, True)
        sub = self.client('paho-retained-target')
        sub.subscribe([('paho/retained', 1), ('paho/will', 1)])
        sub.expect('paho/retained', b'stored', 1, True)
        source.publish('paho/retained', b'', 1, True)
        sub.expect('paho/retained', b'', 1)
        owner = self.client('paho-will-owner', will=('paho/will', b'Will metadata', 1, True))
        owner.abort()
        sub.expect('paho/will', b'Will metadata', 1)
        replay = self.client('paho-will-replay')
        replay.subscribe([('paho/#', 1)])
        replay.expect('paho/will', b'Will metadata', 1, True)
        replay.quiet()

    def test_actual_clean_start_expiry_offline_metadata_and_zero_override(self):
        source = self.client('paho-offline-source')
        target = self.client('paho-offline-target', clean=True, expiry=30, maximum=1)
        target.subscribe([('paho/offline/#', 2)])
        target.disconnect()
        source.publish('paho/offline/one', b'queued-qos1', 1)
        source.publish('paho/offline/two', b'queued-qos2', 2)
        resumed = self.client('paho-offline-target', clean=False, expiry=30, maximum=1, present=True)
        resumed.expect('paho/offline/one', b'queued-qos1', 1)
        resumed.expect('paho/offline/two', b'queued-qos2', 2)
        resumed.disconnect(expiry=0)
        empty = self.client('paho-offline-target', clean=False, expiry=1, present=False)
        empty.subscribe([('paho/offline/#', 1)])
        empty.disconnect()
        time.sleep(1.2)
        self.client('paho-offline-target', clean=False, expiry=30, present=False)

    def test_actual_sqlite_crash_resume_preserves_properties_and_subscriptions(self):
        if not self.sqlite:
            self.skipTest('SQLite process recovery only')
        target = self.client('paho-durable', expiry=30)
        target.subscribe([('paho/durable/#', 2)])
        target.disconnect()
        source = self.client('paho-durable-source')
        source.publish('paho/durable/one', b'one', 1)
        source.publish('paho/durable/two', b'two', 2)
        source.disconnect()
        self.broker.restart_after_crash()
        resumed = self.client('paho-durable', clean=False, expiry=30, maximum=1, present=True)
        resumed.expect('paho/durable/one', b'one', 1)
        resumed.expect('paho/durable/two', b'two', 2)

    def test_actual_v5_credentials_acl_denial_and_owner_refusal(self):
        self.broker.stop()
        self.broker = AuthBroker(self.directory.name, self.sqlite)
        self.broker.start()
        self.addCleanup(self.broker.stop)
        bad = self.client('paho-v5-bad', username='alice', password=b'wrong', accepted=False)
        self.assertEqual(bad.reason.value, 0x86)
        owner = self.client('paho-v5-owned', username='alice', password=PASSWORD, expiry=30)
        owner.subscribe([('allowed/#', 1), ('other/#', 1)], codes=[1, 0x87])
        other = self.client('paho-v5-owned', username='bob', password=PASSWORD, accepted=False)
        self.assertEqual(other.reason.value, 0x87)
        source = self.client('paho-v5-source', username='alice', password=PASSWORD)
        source.publish('allowed/value', b'authorized', 2)
        owner.expect('allowed/value', b'authorized', 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
