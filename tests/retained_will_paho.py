#!/usr/bin/env python3
"""Real Paho2.1 retained/Will interoperability, optional SQLite via test env."""
import queue
import socket
import threading

import paho.mqtt.client as mqtt
from blackbox import Broker


class Client:
    def __init__(self, broker, name, will=None):
        self.messages = queue.Queue()
        self.subscriptions = queue.Queue()
        connected = threading.Event()
        errors = []
        self.value = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=name,
                                 clean_session=True, protocol=mqtt.MQTTv311)
        if will:
            self.value.will_set(*will)

        def on_connect(client, userdata, flags, reason, properties):
            if reason != 0:
                errors.append(reason)
            connected.set()

        def on_subscribe(client, userdata, mid, reasons, properties):
            self.subscriptions.put([reason.value for reason in reasons])

        def on_message(client, userdata, message):
            self.messages.put((message.topic, message.payload, message.qos, message.retain))

        self.value.on_connect = on_connect
        self.value.on_subscribe = on_subscribe
        self.value.on_message = on_message
        self.value.connect('127.0.0.1', broker.port, keepalive=5)
        self.value.loop_start()
        assert connected.wait(5) and not errors, ('CONNECT', errors)

    def subscribe(self, topic, qos):
        result, _ = self.value.subscribe(topic, qos)
        assert result == mqtt.MQTT_ERR_SUCCESS
        assert self.subscriptions.get(timeout=5) == [qos]

    def publish(self, topic, payload, qos=1, retain=False):
        result = self.value.publish(topic, payload, qos=qos, retain=retain)
        result.wait_for_publish(timeout=5)
        assert result.is_published(), 'Paho publication ACK missing'

    def expect(self, topic, payload, qos, retain):
        assert self.messages.get(timeout=5) == (topic, payload, qos, retain)

    def quiet(self):
        try:
            message = self.messages.get(timeout=.2)
        except queue.Empty:
            return
        raise AssertionError(('unexpected message', message))

    def disconnect(self):
        self.value.disconnect()
        self.value.loop_stop()

    def abort(self):
        # Stop automatic reconnect first, then induce EOF without DISCONNECT.
        self.value.loop_stop()
        stream = self.value.socket()
        assert stream is not None, 'Will owner disconnected before fault injection'
        stream.shutdown(socket.SHUT_RDWR)
        stream.close()


def main():
    clients = []
    with Broker() as broker:
        def create(name, will=None):
            client = Client(broker, name, will)
            clients.append(client)
            return client

        try:
            publisher = create('paho-retained-writer')
            publisher.publish('paho/retained', b'original\x00', qos=2, retain=True)
            observer = create('paho-retained-observer')
            observer.subscribe('paho/retained', 1)
            observer.expect('paho/retained', b'original\x00', 1, True)
            publisher.publish('paho/retained', b'updated\xff', retain=True)
            observer.expect('paho/retained', b'updated\xff', 1, False)
            observer.subscribe('paho/retained', 2)
            observer.expect('paho/retained', b'updated\xff', 1, True)
            publisher.publish('paho/retained', b'', retain=True)
            observer.expect('paho/retained', b'', 1, False)
            deleted = create('paho-retained-deleted')
            deleted.subscribe('paho/retained', 2)
            deleted.quiet()

            observer.subscribe('paho/will', 2)
            payload = b'Will binary\x00\xff'
            graceful = create('paho-will-graceful', ('paho/will', payload, 2, True))
            graceful.disconnect()
            clients.remove(graceful)
            observer.quiet()
            owner = create('paho-will-abnormal', ('paho/will', payload, 2, True))
            owner.abort()
            clients.remove(owner)
            observer.expect('paho/will', payload, 2, False)
            replay = create('paho-will-replay')
            replay.subscribe('paho/will', 0)
            replay.expect('paho/will', payload, 0, True)

            delete_owner = create('paho-will-delete', ('paho/will', b'', 1, True))
            delete_owner.abort()
            clients.remove(delete_owner)
            observer.expect('paho/will', b'', 1, False)
            replay.expect('paho/will', b'', 0, False)
            after = create('paho-will-deleted')
            after.subscribe('paho/will', 0)
            after.quiet()
            print('PASS: Paho retained update/delete/resubscribe and graceful/abnormal retained Will')
        finally:
            for client in reversed(clients):
                client.disconnect()


if __name__ == '__main__':
    main()
