#!/usr/bin/env python3
"""Real Paho MQTT client interoperability; pip install paho-mqtt==2.1.0."""
import threading
import paho.mqtt.client as mqtt
from blackbox import Broker


def main():
    with Broker() as broker:
        connected = threading.Event()
        subscribed = threading.Event()
        delivered = threading.Event()
        messages = []
        sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id='paho-sub',
                          clean_session=True, protocol=mqtt.MQTTv311)
        pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id='paho-pub',
                          clean_session=True, protocol=mqtt.MQTTv311)

        def on_connect(client, userdata, flags, reason_code, properties):
            if reason_code == 0:
                connected.set()

        def on_subscribe(client, userdata, mid, reason_codes, properties):
            if all(reason == 0 for reason in reason_codes):
                subscribed.set()

        def on_message(client, userdata, message):
            messages.append((message.topic, message.payload, message.qos, message.retain))
            delivered.set()

        sub.on_connect = on_connect
        sub.on_subscribe = on_subscribe
        sub.on_message = on_message
        try:
            sub.connect('127.0.0.1', broker.port, keepalive=2)
            sub.loop_start()
            assert connected.wait(5), 'Paho subscriber CONNECT failed'
            result, _ = sub.subscribe('interop/+', qos=0)
            assert result == mqtt.MQTT_ERR_SUCCESS
            assert subscribed.wait(5), 'Paho SUBACK failed'
            pub.connect('127.0.0.1', broker.port, keepalive=2)
            pub.loop_start()
            info = pub.publish('interop/real-client', b'hello from paho\x00', qos=0)
            info.wait_for_publish(timeout=5)
            assert info.is_published(), 'Paho publish not flushed'
            assert delivered.wait(5), 'Paho did not receive routed publish'
            assert messages == [('interop/real-client', b'hello from paho\x00', 0, False)], messages
            print('PASS: Paho MQTT 3.1.1 CONNECT/SUBSCRIBE/QoS0 PUBLISH/delivery/DISCONNECT')
        finally:
            for client in (pub, sub):
                client.disconnect()
                client.loop_stop()


if __name__ == '__main__':
    main()
