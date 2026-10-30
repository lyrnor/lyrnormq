#!/usr/bin/env python3
"""Standard Paho 2.1.0 WS/WSS interoperability, separate from the raw oracle.

Missing SDK is an explicit skip, never evidence of interoperability. CI installs
paho-mqtt==2.1.0 before this script. Uses verified temporary CA identities.
"""
import queue
import tempfile
import threading
import unittest

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None

from tls_blackbox import Identities
from ws_wire import WebSocketBroker


@unittest.skipUnless(mqtt is not None, 'requires paho-mqtt==2.1.0 (not an interoperability pass)')
class WebSocketPaho(unittest.TestCase):
    def test_standard_client_ws_wss_both_versions_all_qos(self):
        with tempfile.TemporaryDirectory(prefix='rmqtt-ws-paho-') as directory:
            identities=Identities(directory)
            for secure in (False,True):
                broker=WebSocketBroker(identities,secure).start()
                self.addCleanup(broker.stop)
                for version in (mqtt.MQTTv311,mqtt.MQTTv5):
                    for qos in (0,1,2):
                        with self.subTest(wss=secure,mqtt=version,qos=qos):
                            received=queue.Queue();subscribed=threading.Event()
                            clients=[]
                            try:
                                for role in ('s','p'):
                                    connected=threading.Event();reasons=[]
                                    client=mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                                       client_id=f'paho-ws-{secure}-{version}-{qos}-{role}',
                                                       protocol=version,transport='websockets')
                                    clients.append(client);client.ws_set_options(path='/mqtt')
                                    if secure:client.tls_set(ca_certs=str(identities.path/'ca.pem'))
                                    def on_connect(c,u,f,r,p, event=connected,values=reasons):
                                        values.append(r);event.set()
                                    client.on_connect=on_connect
                                    if role=='s':
                                        client.on_message=lambda c,u,m:received.put((m.topic,m.payload,m.qos))
                                        client.on_subscribe=lambda c,u,i,r,p:subscribed.set()
                                    self.assertEqual(client.connect('localhost',broker.port,keepalive=10),mqtt.MQTT_ERR_SUCCESS)
                                    client.loop_start();self.assertTrue(connected.wait(3),'Paho CONNACK timeout')
                                    self.assertFalse(reasons[0].is_failure,str(reasons[0]))
                                sub,pub=clients;topic=f'paho/ws/{secure}/{version}/{qos}'
                                result,_=sub.subscribe(topic,qos);self.assertEqual(result,mqtt.MQTT_ERR_SUCCESS)
                                self.assertTrue(subscribed.wait(3),'Paho SUBACK timeout')
                                info=pub.publish(topic,bytes(range(128)),qos=qos)
                                info.wait_for_publish(timeout=3);self.assertTrue(info.is_published(),'Paho final publish confirmation timeout')
                                self.assertEqual(received.get(timeout=3),(topic,bytes(range(128)),qos))
                                with self.assertRaises(queue.Empty):received.get(timeout=.1)
                            finally:
                                for client in clients:
                                    client.disconnect();client.loop_stop()
                broker.stop()


if __name__=='__main__':
    unittest.main(verbosity=2)
