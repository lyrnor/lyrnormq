#!/usr/bin/env python3
"""Paho2.1 real-client Message Expiry and Will Delay interoperability."""
import os
import queue
import socket
import tempfile
import threading
import time
import unittest

import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
from retained_will_blackbox import FeatureBroker


class Client:
    def __init__(self, broker, name, expiry=60, clean=True, will=None, delay=0,
                 will_expiry=None, present=False, version=5):
        self.messages = queue.Queue()
        self.subscriptions = queue.Queue()
        self.connected = threading.Event()
        self.closed = False
        self.version = version
        options = dict(client_id=name, protocol=mqtt.MQTTv5 if version == 5 else mqtt.MQTTv311,
                       reconnect_on_failure=False)
        if version != 5:
            options['clean_session'] = clean
        self.value = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, **options)
        if will is not None:
            properties = Properties(PacketTypes.WILLMESSAGE)
            properties.WillDelayInterval = delay
            if will_expiry is not None:
                properties.MessageExpiryInterval = will_expiry
            self.value.will_set(will[0], will[1], qos=1, retain=True, properties=properties)

        def on_connect(client, userdata, flags, reason, properties):
            self.reason = reason
            self.present = flags.session_present
            self.connected.set()

        self.value.on_connect = on_connect
        self.value.on_subscribe = lambda c, u, m, reasons, p: self.subscriptions.put([r.value for r in reasons])
        self.value.on_message = lambda c, u, message: self.messages.put(message)
        properties = Properties(PacketTypes.CONNECT)
        properties.SessionExpiryInterval = expiry
        self.value.connect('127.0.0.1', broker.port, keepalive=10,
                           **(dict(clean_start=clean, properties=properties) if version == 5 else {}))
        self.value.loop_start()
        assert self.connected.wait(5), 'missing actual Paho CONNACK'
        assert not self.reason.is_failure, self.reason
        assert self.present == present, (self.present, present)

    def subscribe(self, topic, qos=1):
        status, _ = self.value.subscribe(topic, qos)
        assert status == mqtt.MQTT_ERR_SUCCESS
        assert self.subscriptions.get(timeout=5) == [qos]

    def publish(self, topic, payload, expiry=None, retain=False, qos=1):
        properties = None
        if expiry is not None:
            properties = Properties(PacketTypes.PUBLISH)
            properties.MessageExpiryInterval = expiry
            properties.UserProperty = [('k', 'one'), ('k', 'two')]
        result = self.value.publish(topic, payload, qos, retain, properties=properties)
        result.wait_for_publish(timeout=5)
        assert result.is_published(), 'Paho publication not completed'

    def message(self, topic, payload, ttl=None, retain=False):
        result = self.messages.get(timeout=5)
        assert (result.topic, result.payload, result.retain) == (topic, payload, retain)
        if ttl is not None:
            assert 0 <= result.properties.MessageExpiryInterval <= ttl
        return result

    def quiet(self):
        try:
            unexpected = self.messages.get(timeout=.2)
        except queue.Empty:
            return
        raise AssertionError(('unexpected Paho message', unexpected.topic, unexpected.payload))

    def disconnect(self):
        if not self.closed:
            self.value.disconnect()
            self.value.loop_stop()
            self.closed = True

    def abort(self):
        if not self.closed:
            self.value.loop_stop()
            sock = self.value.socket()
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sock.close()
            self.closed = True


class ExpiryWillPaho(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='lyrnormq-expiry-paho-')
        self.addCleanup(self.directory.cleanup)
        self.sqlite = bool(os.environ.get('LYRNORMQ_TEST_STORAGE_PATH'))
        self.broker = FeatureBroker(self.directory.name, self.sqlite)
        self.broker.start()
        self.addCleanup(self.broker.stop)

    def client(self, name, **kwargs):
        result = Client(self.broker, name, **kwargs)
        self.addCleanup(result.disconnect)
        return result

    def test_actual_retained_remaining_and_timer_expiry(self):
        source = self.client('paho-retained-source')
        source.publish('paho/retained', b'value', expiry=3, retain=True)
        time.sleep(1.2)
        target = self.client('paho-retained-reader')
        target.subscribe('paho/retained')
        value = target.message('paho/retained', b'value', ttl=2, retain=True)
        self.assertEqual(value.properties.UserProperty, [('k', 'one'), ('k', 'two')])
        time.sleep(2)
        target.subscribe('paho/retained')
        target.quiet()

    def test_actual_offline_qos2_expiry_and_zero_expiry(self):
        target = self.client('paho-offline')
        target.subscribe('paho/offline', qos=2)
        target.disconnect()
        source = self.client('paho-offline-source')
        source.publish('paho/offline', b'expiring', qos=2, expiry=1)
        time.sleep(1.2)
        target = self.client('paho-offline', clean=False, present=True)
        target.quiet()
        source.publish('paho/offline', b'zero', expiry=0)
        target.quiet()
        source.publish('paho/offline', b'fresh')
        target.message('paho/offline', b'fresh')

    def test_actual_reconnect_cancels_delayed_will(self):
        target = self.client('paho-will-reader')
        target.subscribe('paho/will')
        source = self.client('paho-will-owner', will=('paho/will', b'canceled'), delay=2)
        source.abort()
        replacement = self.client('paho-will-owner', clean=False, present=True)
        time.sleep(2.2)
        target.quiet()
        replacement.disconnect()

    def test_actual_delayed_will_message_ttl_begins_at_publication(self):
        target = self.client('paho-delay-reader')
        target.subscribe('paho/delay')
        source = self.client('paho-delay-owner', will=('paho/delay', b'delayed'), delay=2,
                             will_expiry=1)
        time.sleep(1.2)
        source.abort()
        target.quiet()
        target.message('paho/delay', b'delayed', ttl=1)
        time.sleep(1.2)
        late = self.client('paho-delay-late')
        late.subscribe('paho/delay')
        late.quiet()

    def test_actual_mixed_version_expired_publication_is_not_delivered(self):
        target = self.client('paho-legacy-reader', version=4)
        target.subscribe('paho/mixed')
        source = self.client('paho-mixed-source')
        source.publish('paho/mixed', b'zero', expiry=0)
        target.quiet()
        source.publish('paho/mixed', b'live', expiry=60)
        value = target.message('paho/mixed', b'live')
        self.assertIsNone(getattr(value, 'properties', None))

    def test_actual_offline_expiry_survives_sigkill(self):
        if not self.sqlite:
            self.skipTest('SQLite process persistence only')
        target = self.client('paho-crash-offline')
        target.subscribe('paho/crash')
        target.disconnect()
        source = self.client('paho-crash-source')
        source.publish('paho/crash', b'expired', expiry=1)
        self.broker.stop(kill=True)
        source.abort()
        time.sleep(1.2)
        self.broker.start()
        target = self.client('paho-crash-offline', clean=False, present=True)
        target.quiet()


if __name__ == '__main__':
    unittest.main(verbosity=2)
