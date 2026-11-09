#!/usr/bin/env python3
"""Real Paho persistent-session offline QoS1/2 delivery after SIGKILL/restart."""
import tempfile
import threading
import paho.mqtt.client as mqtt
from persistence_blackbox import PersistentBroker


def client(broker, client_id, clean, expected_present):
    connected = threading.Event()
    subscribed = threading.Event()
    delivered = threading.Event()
    received = []
    errors = []
    value = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id,
                        clean_session=clean, protocol=mqtt.MQTTv311)

    def on_connect(client, userdata, flags, reason, properties):
        if reason != 0 or flags.session_present != expected_present:
            errors.append((reason, flags.session_present, expected_present))
        connected.set()

    def on_subscribe(client, userdata, mid, reasons, properties):
        if [reason.value for reason in reasons] != [2]:
            errors.append(reasons)
        subscribed.set()

    def on_message(client, userdata, message):
        received.append((message.topic, message.payload, message.qos))
        delivered.set()

    value.on_connect = on_connect
    value.on_subscribe = on_subscribe
    value.on_message = on_message
    value.connect('127.0.0.1', broker.port, keepalive=5)
    value.loop_start()
    assert connected.wait(5), 'persistent CONNECT did not complete'
    assert not errors, errors
    return value, subscribed, delivered, received, errors


def disconnect(value):
    value.disconnect()
    value.loop_stop()


def main():
    for qos in (1, 2):
        with tempfile.TemporaryDirectory(prefix='lyrnormq-paho-persistent-') as directory:
            broker = PersistentBroker(directory).start()
            clients = []
            try:
                sub, subscribed, _, _, errors = client(broker, 'persistent-paho', False, False)
                clients.append(sub)
                result, _ = sub.subscribe('persistent/+', qos=2)
                assert result == mqtt.MQTT_ERR_SUCCESS
                assert subscribed.wait(5) and not errors, errors
                disconnect(sub)
                clients.remove(sub)
                # A same-ID temporary takeover is an observable barrier that the
                # old clean disconnect cannot remove the stored subscription.
                sub, _, _, _, errors = client(broker, 'persistent-paho', False, True)
                clients.append(sub)
                disconnect(sub)
                clients.remove(sub)
                pub, _, _, _, _ = client(broker, 'persistent-publisher', True, False)
                clients.append(pub)
                result = pub.publish('persistent/offline', b'Paho durable offline', qos=qos)
                result.wait_for_publish(timeout=5)
                assert result.is_published(), 'durable publisher ACK missing'
                disconnect(pub)
                clients.remove(pub)
                broker.restart_after_crash()
                sub, _, delivered, messages, errors = client(broker, 'persistent-paho', False, True)
                clients.append(sub)
                # Intentionally do not subscribe after recovery.
                assert delivered.wait(5), 'offline message or durable subscription lost'
                disconnect(sub)
                clients.remove(sub)
                assert not errors, errors
                assert messages == [('persistent/offline', b'Paho durable offline', qos)], messages
            finally:
                for value in clients:
                    disconnect(value)
                broker.stop()
    print('PASS: Paho persistent reconnect, offline QoS1/2, SIGKILL/restart without resubscribe')


if __name__ == '__main__':
    main()
