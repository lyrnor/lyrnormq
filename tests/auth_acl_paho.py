#!/usr/bin/env python3
"""Real Paho2.1 credential/ACL/retained/Will and persistent-owner checks."""
import os
import queue
import socket
import tempfile
import threading
import unittest

import paho.mqtt.client as mqtt
from auth_acl_blackbox import AuthBroker, PASSWORD, BINARY_PASSWORD


class Client:
    def __init__(self, broker, name, user='alice', password=PASSWORD, clean=True,
                 will=None, accepted=True, present=False):
        self.messages = queue.Queue()
        self.subscriptions = queue.Queue()
        self.connected = threading.Event()
        self.disconnected = threading.Event()
        self.reason = None
        self.closed = False
        self.present = None
        self.value = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=name,
                                 clean_session=clean, protocol=mqtt.MQTTv311,
                                 reconnect_on_failure=False)
        if user is not None:
            self.value.username_pw_set(user, password)
        if will is not None:
            self.value.will_set(*will)

        def on_connect(client, userdata, flags, reason, properties):
            self.reason = reason
            self.present = flags.session_present
            self.connected.set()

        def on_disconnect(client, userdata, flags, reason, properties):
            self.disconnected.set()

        def on_subscribe(client, userdata, mid, reasons, properties):
            self.subscriptions.put([reason.value for reason in reasons])

        def on_message(client, userdata, message):
            self.messages.put((message.topic, message.payload, message.qos, message.retain))

        self.value.on_connect = on_connect
        self.value.on_disconnect = on_disconnect
        self.value.on_subscribe = on_subscribe
        self.value.on_message = on_message
        self.value.connect('127.0.0.1', broker.port, keepalive=5)
        self.value.loop_start()
        try:
            assert self.connected.wait(5), 'missing Paho CONNACK callback'
            assert self.reason.is_failure == (not accepted), ('CONNECT', self.reason)
            if accepted:
                assert self.present == present, ('session_present', self.present, present)
            else:
                assert self.disconnected.wait(5), 'server did not close rejected CONNECT'
        except BaseException:
            self.disconnect()
            raise

    def subscribe(self, filters, codes):
        result, _ = self.value.subscribe(filters)
        assert result == mqtt.MQTT_ERR_SUCCESS
        assert self.subscriptions.get(timeout=5) == codes

    def publish(self, topic, payload, qos=1, retain=False):
        result = self.value.publish(topic, payload, qos=qos, retain=retain)
        result.wait_for_publish(timeout=5)
        assert result.is_published(), 'Paho publication ACK missing'

    def expect(self, topic, payload, qos, retain=False):
        assert self.messages.get(timeout=5) == (topic, payload, qos, retain)

    def quiet(self):
        try:
            message = self.messages.get(timeout=.2)
        except queue.Empty:
            return
        raise AssertionError(('unexpected message', message))

    def disconnect(self):
        if not self.closed:
            self.value.disconnect()
            self.value.loop_stop()
            self.closed = True

    def abort(self):
        self.value.loop_stop()
        stream = self.value.socket()
        assert stream is not None
        stream.shutdown(socket.SHUT_RDWR)
        stream.close()
        self.closed = True


class AuthAclPaho(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='lyrnormq-auth-paho-')
        self.addCleanup(self.directory.cleanup)
        self.sqlite = bool(os.environ.get('LYRNORMQ_TEST_STORAGE_PATH'))
        self.broker = AuthBroker(self.directory.name, self.sqlite)
        self.broker.start()
        self.addCleanup(self.broker.stop)

    def client(self, name, **kwargs):
        value = Client(self.broker, name, **kwargs)
        self.addCleanup(value.disconnect)
        return value

    def test_actual_credentials_rejection_and_binary_password(self):
        self.client('paho-wrong-password', password=b'wrong', accepted=False)
        self.client('paho-missing-credentials', user=None, accepted=False)
        self.client('paho-prefix-denied', user='prefixed', accepted=False)
        binary = self.client('paho-binary', user='binary', password=BINARY_PASSWORD)
        binary.subscribe([('allowed/binary', 2)], [2])
        publisher = self.client('paho-valid-publisher')
        publisher.publish('allowed/binary', b'actual binary password accepted', qos=2)
        binary.expect('allowed/binary', b'actual binary password accepted', 2)

    def test_actual_acl_retained_and_will_enforcement(self):
        admin = self.client('paho-admin', user='admin')
        admin.publish('other/secret', b'not visible to Alice', retain=True)
        alice = self.client('paho-alice')
        alice.subscribe([('allowed/#', 1), ('other/#', 1), ('#', 0)], [1, 128, 128])
        alice.quiet()
        admin.publish('allowed/state', b'authorized retained', qos=2, retain=True)
        alice.expect('allowed/state', b'authorized retained', 1)
        replay = self.client('paho-replay')
        replay.subscribe([('allowed/state', 2)], [2])
        replay.expect('allowed/state', b'authorized retained', 2, True)
        admin.subscribe([('allowed/will', 1), ('other/#', 1)], [1, 1])
        admin.expect('other/secret', b'not visible to Alice', 1, True)
        self.client('paho-denied-will', will=('other/rejected-will', b'never accepted', 1, True),
                    accepted=False)
        admin.quiet()
        owner = self.client('paho-allowed-will',
                            will=('allowed/will', b'authorized Will', 1, False))
        owner.abort()
        admin.expect('allowed/will', b'authorized Will', 1)
        alice.expect('allowed/will', b'authorized Will', 1)
        denied = alice.value.publish('other/secret', b'', qos=1, retain=True)
        self.assertTrue(alice.disconnected.wait(5), 'denied publication kept connection open')
        self.assertFalse(denied.is_published(), 'denied publication received PUBACK')
        admin.quiet()
        secret = self.client('paho-secret-after-denial', user='admin')
        secret.subscribe([('other/secret', 0)], [0])
        secret.expect('other/secret', b'not visible to Alice', 0, True)

    def test_actual_persistent_owner_and_offline_qos2(self):
        if not self.sqlite:
            self.skipTest('SQLite persistent session only')
        alice = self.client('paho-durable-owner', clean=False)
        alice.subscribe([('allowed/offline', 2)], [2])
        alice.disconnect()
        admin = self.client('paho-offline-admin', user='admin')
        admin.publish('allowed/offline', b'owned offline QoS2', qos=2)
        self.client('paho-durable-owner', user='bob', clean=True, accepted=False)
        resumed = self.client('paho-durable-owner', clean=False, present=True)
        resumed.expect('allowed/offline', b'owned offline QoS2', 2)
        resumed.quiet()


if __name__ == '__main__':
    unittest.main(verbosity=2)
