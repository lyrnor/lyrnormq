#!/usr/bin/env python3
"""Real Paho 2.1 MQTT3.1.1 clients: complete 3x3 live clean-session QoS matrix."""
import threading
import paho.mqtt.client as mqtt
from blackbox import Broker


def cell(broker, publication_qos, subscription_qos):
    subscriber_ready = threading.Event()
    publisher_ready = threading.Event()
    subscribed = threading.Event()
    delivered = threading.Event()
    messages = []
    grants = []
    topic = f'interop/{publication_qos}/{subscription_qos}'
    payload = b'hello from paho\x00'
    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f'paho-sub-{publication_qos}-{subscription_qos}',
                      clean_session=True, protocol=mqtt.MQTTv311)
    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f'paho-pub-{publication_qos}-{subscription_qos}',
                      clean_session=True, protocol=mqtt.MQTTv311)

    def on_connect_sub(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            subscriber_ready.set()

    def on_connect_pub(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            publisher_ready.set()

    def on_subscribe(client, userdata, mid, reason_codes, properties):
        grants.extend(reason.value for reason in reason_codes)
        subscribed.set()

    def on_message(client, userdata, message):
        messages.append((message.topic, message.payload, message.qos, message.retain))
        delivered.set()

    sub.on_connect = on_connect_sub
    pub.on_connect = on_connect_pub
    sub.on_subscribe = on_subscribe
    sub.on_message = on_message
    try:
        sub.connect('127.0.0.1', broker.port, keepalive=2)
        sub.loop_start()
        assert subscriber_ready.wait(5), 'subscriber CONNECT failed'
        result, _ = sub.subscribe(topic, qos=subscription_qos)
        assert result == mqtt.MQTT_ERR_SUCCESS
        assert subscribed.wait(5), 'SUBACK missing'
        assert grants == [subscription_qos], grants
        pub.connect('127.0.0.1', broker.port, keepalive=2)
        pub.loop_start()
        assert publisher_ready.wait(5), 'publisher CONNECT failed'
        info = pub.publish(topic, payload, qos=publication_qos)
        info.wait_for_publish(timeout=5)
        assert info.is_published(), 'publication handshake did not complete'
        assert delivered.wait(5), 'routed message missing'
    finally:
        for client in (pub, sub):
            client.disconnect()
            client.loop_stop()
    assert messages == [(topic, payload, min(publication_qos, subscription_qos), False)], messages


def main():
    with Broker() as broker:
        for publication_qos in range(3):
            for subscription_qos in range(3):
                cell(broker, publication_qos, subscription_qos)
    print('PASS: Paho MQTT3.1.1 clean-session 3x3 publish/subscribe QoS matrix and disconnect')


if __name__ == '__main__':
    main()
