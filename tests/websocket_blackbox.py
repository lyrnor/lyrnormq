#!/usr/bin/env python3
"""Independent raw RFC6455 + MQTT WS/WSS checks. No websocket SDK oracle.

OpenSSL identities and MQTT helpers come from the independently verified TLS suite.
RMQTT_BINARY selects the broker. SIGKILL tests cover process recovery, not power loss.
"""
import io
import socket
import ssl
import struct
import subprocess
import tempfile
import time
import unittest

from blackbox import frame, receive
from mqtt5_blackbox import connect5, parse5, parse_ack5, publication5
from retained_will_blackbox import connect_packet
import tls_blackbox as tls
from tls_blackbox import Identities
from ws_wire import WebSocket, WebSocketBroker, accept_key, masked, request, response


class BytesSocket:
    def __init__(self, data):
        self.data = io.BytesIO(data)
    def recv(self, count):
        return self.data.read(count)


class ResetAfterCloseSocket(BytesSocket):
    def sendall(self, data):
        raise BrokenPipeError('fixture: peer already closed after its close frame')


class WebSocketOracle(unittest.TestCase):
    def test_rfc6455_known_accept_and_mask(self):
        self.assertEqual(accept_key('dGhlIHNhbXBsZSBub25jZQ=='), 's3pPLMBiTxaQ9kYGzzhZRbK+xOo=')
        self.assertEqual(masked(b'Hello', 1, mask=bytes.fromhex('37fa213d')),
                         bytes.fromhex('818537fa213d7f9f4d5158'))
        self.assertEqual(masked(b'a'*126)[:4], b'\x82\xfe\x00\x7e')
        self.assertEqual(masked(b'a'*65536)[:10], b'\x82\xff'+struct.pack('!Q', 65536))

    def test_server_frame_decoder_known_bytes_and_http_pipeline(self):
        ws = WebSocket(BytesSocket(b'\x82\x03abc\x82\x00\x8a\x01p'))
        self.assertEqual(ws.read_frame(), (True, 2, b'abc'))
        self.assertEqual(ws.read_frame(), (True, 2, b''))
        self.assertEqual(ws.read_frame(), (True, 10, b'p'))
        fragmented = WebSocket(BytesSocket(b'\x02\x01a\x80\x01b'))
        self.assertEqual(fragmented.recv(1), b'a')
        self.assertEqual(fragmented.recv(1), b'b')
        self.assertEqual(fragmented.message_sizes, [2])
        status, headers, pending = response(BytesSocket(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n\x82\x01a'))
        self.assertEqual((status, headers, pending), (101, {'upgrade':['websocket']}, b'\x82\x01a'))

    def test_received_error_close_remains_observable_after_echo_reset(self):
        error = WebSocket(ResetAfterCloseSocket(b'\x88\x02\x03\xea'))
        self.assertEqual(error.recv(1), b'')
        self.assertEqual(error.close_code, 1002)
        normal = WebSocket(ResetAfterCloseSocket(b'\x88\x02\x03\xe8'))
        with self.assertRaises(BrokenPipeError):
            normal.recv(1)


class WebSocketBlackbox(unittest.TestCase):
    client = tls.TLSWire.client
    subscribe = tls.TLSWire.subscribe
    source_ack = tls.TLSWire.source_ack
    publish = tls.TLSWire.publish
    delivery = tls.TLSWire.delivery
    barrier = tls.TLSWire.barrier
    quiet = tls.TLSWire.quiet

    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='rmqtt-ws-')
        cls.addClassCleanup(cls.temp.cleanup)
        cls.identities = Identities(cls.temp.name)
        cls.brokers = {}
        for secure in (False, True):
            broker = WebSocketBroker(cls.identities, secure)
            cls.addClassCleanup(broker.stop)
            cls.brokers[secure] = broker.start()

    def disconnect(self, sock):
        sock.sendall(frame(0xe0))
        self.assertEqual(sock.recv(1), b'')
        self.assertEqual(sock.close_code, 1000)
        if isinstance(sock.raw, ssl.SSLSocket):
            # WSS normal MQTT close must include both WS Close and TLS alert.
            plain = sock.raw.unwrap()
            plain.close()
        sock.close()

    def test_ws_wss_dual_mqtt_versions_all_qos(self):
        for secure, broker in self.brokers.items():
            for version in (4, 5):
                for qos in (0, 1, 2):
                    with self.subTest(wss=secure, mqtt=version, qos=qos):
                        name = f'ws-{secure}-{version}-{qos}'
                        sub = self.client(broker, name+'-s', version)
                        pub = self.client(broker, name+'-p', version)
                        self.subscribe(sub, name, qos, version)
                        self.publish(pub, name, bytes(range(128)), qos, version)
                        self.delivery(sub, name, bytes(range(128)), qos, version)
                        self.disconnect(pub);self.disconnect(sub)

    def test_binary_split_coalesced_fragmented_and_upgrade_pipeline(self):
        for secure, broker in self.brokers.items():
            for version in (4, 5):
                for mode in ('split','fragment','pipeline'):
                    with self.subTest(wss=secure, mqtt=version, mode=mode):
                        name = f'framing-{secure}-{version}-{mode}'
                        packet = connect5(name) if version == 5 else connect_packet(name)
                        if mode == 'pipeline':
                            sock = broker.socket(pipeline=masked(packet+frame(0xc0)))
                        else:
                            sock = broker.socket()
                            sock.send_frame(b'')
                            if mode == 'split':
                                sock.send_frame(packet[:5]);sock.send_frame(packet[5:])
                                sock.send_frame(frame(0xc0))
                            else:
                                sock.send_frame(packet[:5], fin=False)
                                sock.send_frame(b'interleaved-control', 9)
                                sock.send_frame(packet[5:], 0)
                                sock.send_frame(frame(0xc0))
                        self.addCleanup(sock.close)
                        header, body = receive(sock)
                        self.assertEqual((header,body[:2]), (0x20,b'\0\0'))
                        self.assertEqual(receive(sock), (0xd0,b''))
                        if mode == 'fragment':self.assertIn(b'interleaved-control',sock.pongs)
                        sock.send_frame(frame(0xc0)+frame(0xc0))
                        self.assertEqual(receive(sock),(0xd0,b''));self.assertEqual(receive(sock),(0xd0,b''))
                        self.disconnect(sock)

    def test_empty_binary_ping_and_unsolicited_pong(self):
        for secure, broker in self.brokers.items():
            sock = self.client(broker, f'controls-{secure}')
            for _ in range(8):sock.send_frame(b'')
            sock.send_frame(b'unprompted',10)
            sock.send_frame(b'ping-bytes\0',9)
            self.barrier(sock)
            self.assertEqual(sock.pongs,[b'ping-bytes\0'])
            self.disconnect(sock)

    def test_path_subprotocol_and_invalid_upgrade(self):
        for secure, broker in self.brokers.items():
            for path, protocols, expected in [('/wrong','mqtt',404),('/mqtt',None,None),('/mqtt','other',None)]:
                with self.subTest(wss=secure,path=path,protocols=protocols):
                    with broker.transport_socket() as raw:
                        head,_ = request(path,protocols);raw.sendall(head)
                        status,_,_ = response(raw)
                        self.assertNotEqual(status,101)
                        if expected:self.assertEqual(status,expected)
            for head in [request()[0].replace(b'GET ',b'POST ',1),
                         request()[0].replace(b'Sec-WebSocket-Version: 13',b'Sec-WebSocket-Version: 12'),
                         request(key='not-base64')[0]]:
                with broker.transport_socket() as raw:
                    raw.sendall(head)
                    try:self.assertNotEqual(response(raw)[0],101)
                    except EOFError:pass
            # Exact path routing ignores the query; token negotiation returns mqtt only.
            with broker.socket(path='/mqtt?trace=1', protocols='other, mqtt, extra') as sock:
                sock.sendall(connect5(f'query-{secure}'))
                self.assertEqual(receive(sock)[0],0x20)
                self.disconnect(sock)

    def test_protocol_errors_have_bounded_close_codes(self):
        for secure, broker in self.brokers.items():
            cases = [(masked(b'text',1),1003), (masked(b'plain',mask=None),1002),
                     (masked(b'bad-rsv',rsv=0x40),1002), (masked(b'orphan',0),1002),
                     (masked(b'bad-control',9,False),1002), (masked(b'x'*126,9),1002),
                     (masked(b'bad-opcode',3),1002)]
            for index,(data,code) in enumerate(cases):
                with self.subTest(wss=secure,index=index):
                    sock = self.client(broker, f'bad-frame-{secure}-{index}')
                    sock.raw.sendall(data)
                    self.assertEqual(sock.recv(1),b'')
                    self.assertEqual(sock.close_code,code)
            healthy = self.client(broker, f'after-frame-errors-{secure}')
            self.barrier(healthy);self.disconnect(healthy)

    def test_initial_text_fragment_and_noncanonical_lengths_fail_closed(self):
        key = b'\x12\x34\x56\x78'
        ping = bytes(v ^ key[i % 4] for i, v in enumerate(frame(0xc0)))
        cases = [
            # MQTT requires binary: reject the first text fragment immediately,
            # without waiting forever for a final continuation fragment.
            (masked(b'no-final-fragment', 1, False), 1003),
            # Strict project input policy rejects non-shortest lengths. RFC6455
            # requires the sender's minimal encoding; this is not presented as
            # a separate receiver MUST inferred from that sender requirement.
            (b'\x82\xfe\x00\x02' + key + ping, 1002),
            (b'\x82\xff' + struct.pack('!Q', 2) + key + ping, 1002),
        ]
        for secure, broker in self.brokers.items():
            for index, (data, code) in enumerate(cases):
                with self.subTest(wss=secure, malformed=index):
                    sock = broker.socket()
                    self.addCleanup(sock.close)
                    sock.sendall(connect5(f'early-fail-{secure}-{index}', keepalive=0))
                    header, body = receive(sock)
                    self.assertEqual((header, body[:2]), (0x20, b'\0\0'))
                    sock.settimeout(2)
                    sock.raw.sendall(data)
                    self.assertEqual(sock.recv(1), b'')
                    self.assertEqual(sock.close_code, code)

    def test_aggregate_message_bound_and_outgoing_chunks(self):
        for secure in (False, True):
            broker = WebSocketBroker(self.identities, secure, extra=['--ws-max-message-size','64']).start()
            self.addCleanup(broker.stop)
            for fragmented in (False,True):
                sock = self.client(broker, f'size-{int(fragmented)}')
                if fragmented:
                    sock.send_frame(b'x'*40,fin=False);sock.send_frame(b'x'*40,0)
                else:sock.send_frame(b'x'*65)
                self.assertEqual(sock.recv(1),b'');self.assertEqual(sock.close_code,1009)
            sub = self.client(broker,'chunk-sub');self.subscribe(sub,'chunks',0,5)
            pub = self.client(broker,'chunk-pub')
            data = publication5('chunks',b'a'*200,0)
            for at in range(0,len(data),60):pub.send_frame(data[at:at+60])
            self.barrier(pub);self.delivery(sub,'chunks',b'a'*200,0,5)
            self.assertTrue(sub.frame_sizes);self.assertLessEqual(max(sub.frame_sizes),64)
            self.assertTrue(sub.message_sizes);self.assertLessEqual(max(sub.message_sizes),64)
            self.disconnect(pub);self.disconnect(sub);broker.stop()

    def test_wss_requires_trusted_ca_and_correct_hostname(self):
        broker = self.brokers[True]
        for kwargs in ({'trusted':False},{'hostname':'wrong.invalid'}):
            with self.assertRaises(ssl.SSLCertVerificationError):broker.socket(**kwargs)
        healthy = self.client(broker,'wss-after-rejected-identity');self.barrier(healthy);self.disconnect(healthy)

    def test_one_byte_data_budget_preserves_full_size_control_ping(self):
        broker = WebSocketBroker(self.identities, extra=['--ws-max-message-size', '1'],
                                 client_chunk_size=1).start()
        self.addCleanup(broker.stop)
        sock = broker.socket()
        self.addCleanup(sock.close)
        # Send each CONNECT byte as a separate complete binary message.
        for byte in connect5('single-byte-ws'):
            sock.send_frame(bytes([byte]))
        header, body = receive(sock)
        self.assertEqual((header, body[:2]), (0x20, b'\0\0'))
        self.assertTrue(sock.message_sizes)
        self.assertTrue(all(size <= 1 for size in sock.message_sizes))
        ping = bytes(range(125))
        sock.send_frame(ping, 9)
        self.barrier(sock)  # Byte-chunked MQTT PINGREQ; consume WS Pong on the way.
        self.assertEqual(sock.pongs, [ping])
        self.assertTrue(all(size <= 1 for size in sock.message_sizes))
        self.disconnect(sock)

    def test_upgrade_header_cap_before_incomplete_request_finishes(self):
        broker = WebSocketBroker(self.identities,extra=['--handshake-timeout-ms','10000']).start()
        self.addCleanup(broker.stop)
        with broker.transport_socket() as raw:
            raw.settimeout(2)
            raw.sendall(b'GET /mqtt HTTP/1.1\r\nHost: localhost\r\nX-Large: '+b'x'*17000)
            try:
                data = raw.recv(4096)
                self.assertFalse(data.startswith(b'HTTP/1.1 101'))
                self.assertTrue(not data or data.startswith(b'HTTP/1.1 4'))
            except ConnectionResetError:pass
        healthy = self.client(broker,'after-header-cap');self.barrier(healthy);self.disconnect(healthy)

    def test_handshake_timeout_permit_recovery_and_shutdown(self):
        for secure in (False,True):
            broker = WebSocketBroker(self.identities,secure,extra=['--max-connections','1','--handshake-timeout-ms','400']).start()
            self.addCleanup(broker.stop)
            deadline = time.monotonic()+2
            while True:
                try:
                    idle = broker.transport_socket();idle.sendall(b'GET /mqtt HTTP/1.1\r\n')
                    idle.settimeout(.05)
                    try:
                        data = idle.recv(1)
                        self.assertEqual(data,b'');idle.close()
                    except socket.timeout:break
                except (OSError,EOFError):pass
                self.assertLess(time.monotonic(),deadline)
            self.addCleanup(idle.close);idle.settimeout(2)
            try:self.assertEqual(idle.recv(1),b'')
            except ConnectionResetError:pass
            deadline = time.monotonic()+2
            while True:
                try:healthy=self.client(broker,'after-upgrade-timeout');break
                except (OSError,EOFError):
                    if time.monotonic()>deadline:raise
            self.barrier(healthy);self.disconnect(healthy)
            deadline = time.monotonic()+2
            while True:
                try:mqtt_idle=broker.socket();break
                except (OSError,EOFError):
                    if time.monotonic()>deadline:raise
            self.addCleanup(mqtt_idle.close);mqtt_idle.settimeout(2)
            began = time.monotonic()
            try:
                self.assertEqual(mqtt_idle.recv(1),b'')
            except (EOFError, ConnectionResetError):
                pass  # No MQTT CONNECT/session: require bounded closure, not normal DISCONNECT semantics.
            self.assertLess(time.monotonic()-began,1.5)
            deadline = time.monotonic()+2
            while True:
                try:healthy=self.client(broker,'after-mqtt-handshake-timeout');break
                except (OSError,EOFError):
                    if time.monotonic()>deadline:raise
            self.barrier(healthy);self.disconnect(healthy)
            broker.stop()
            pending = WebSocketBroker(self.identities,secure,extra=['--handshake-timeout-ms','10000']).start()
            self.addCleanup(pending.stop)
            raw = pending.transport_socket();self.addCleanup(raw.close);raw.sendall(b'GET /mqtt HTTP/1.1\r\n')
            healthy=self.client(pending,'healthy-during-upgrade');self.barrier(healthy);self.disconnect(healthy)
            pending.stop(timeout=3)

    def test_ws_control_ping_does_not_extend_mqtt_keepalive(self):
        sock=self.brokers[False].socket();self.addCleanup(sock.close)
        sock.sendall(connect5('ws-keepalive',keepalive=1));self.assertEqual(receive(sock)[0],0x20)
        sock.settimeout(.2);deadline=time.monotonic()+3
        while time.monotonic()<deadline:
            sock.send_frame(b'ws-not-mqtt-activity',9)
            try:
                packet=receive(sock)
                self.assertEqual(packet[0],0xe0);self.assertEqual(packet[1][0],0x8d)
                return
            except EOFError:return
            except socket.timeout:pass
        self.fail('WS controls incorrectly refreshed MQTT Keep Alive')

    def test_retained_and_will_normal_close_vs_ws_close_or_drop(self):
        for secure,broker in self.brokers.items():
            for version in (4,5):
                for ending in ('mqtt','ws','drop'):
                    with self.subTest(wss=secure,mqtt=version,ending=ending):
                        topic=f'ws-will-{secure}-{version}-{ending}'
                        sub=self.client(broker,topic+'-s',version);self.subscribe(sub,topic,0,version)
                        owner=self.client(broker,topic+'-p',version,will=(topic,b'will',1,False))
                        if ending=='mqtt':self.disconnect(owner);self.quiet(sub)
                        elif ending=='ws':
                            owner.send_frame(struct.pack('!H',1000),8);self.assertEqual(owner.recv(1),b'')
                            self.delivery(sub,topic,b'will',0,version)
                        else:owner.close();self.delivery(sub,topic,b'will',0,version)
                        self.disconnect(sub)
            pub=self.client(broker,f'retained-{secure}')
            pub.sendall(publication5(f'ws-retained-{secure}',b'retained',1,7,retain=True));parse_ack5(receive(pub),0x40,7)
            sub=self.client(broker,f'replay-{secure}');self.subscribe(sub,f'ws-retained-{secure}',0,5)
            message=parse5(receive(sub));self.assertEqual((message[1],message[5]),(b'retained',True))
            self.disconnect(sub);self.disconnect(pub)

    def test_sqlite_offline_and_inflight_resume_across_ws_wss(self):
        from qos_blackbox import parse_publish, ack
        for secure in (False,True):
            for version in (4,5):
                for qos in (1,2):
                    with self.subTest(initial_wss=secure,mqtt=version,qos=qos), tempfile.TemporaryDirectory(prefix='rmqtt-ws-db-') as directory:
                        broker=WebSocketBroker(self.identities,secure,directory).start();self.addCleanup(broker.stop)
                        sub=self.client(broker,'durable-ws-s',version,clean=False)
                        self.subscribe(sub,'ws/durable',qos,version);self.disconnect(sub)
                        pub=self.client(broker,'durable-ws-p',version)
                        self.publish(pub,'ws/durable',b'persisted',qos,version);self.disconnect(pub)
                        broker.stop(kill=True);broker.secure=not broker.secure;broker.start()
                        sub=self.client(broker,'durable-ws-s',version,clean=False,present=True)
                        original=parse5(receive(sub)) if version==5 else parse_publish(receive(sub))
                        self.assertEqual(original[:3],('ws/durable',b'persisted',qos));self.assertFalse(original[4])
                        # AwaitAck (Q1) / AwaitRec (Q2): restart on the other transport.
                        broker.stop(kill=True);sub.close();broker.secure=not broker.secure;broker.start()
                        sub=self.client(broker,'durable-ws-s',version,clean=False,present=True)
                        replay=parse5(receive(sub)) if version==5 else parse_publish(receive(sub))
                        self.assertEqual(replay[:4],original[:4]);self.assertTrue(replay[4])
                        if qos==1:
                            sub.sendall(ack(0x40,replay[3]))
                        else:
                            sub.sendall(ack(0x50,replay[3]));self.source_ack(sub,0x62,replay[3],version)
                            # AwaitComp: recovery sends only PUBREL with the same ID.
                            broker.stop(kill=True);sub.close();broker.secure=not broker.secure;broker.start()
                            sub=self.client(broker,'durable-ws-s',version,clean=False,present=True)
                            self.source_ack(sub,0x62,replay[3],version)
                            sub.sendall(ack(0x70,replay[3]))
                        self.barrier(sub);self.quiet(sub)
                        self.disconnect(sub);broker.stop()


if __name__=='__main__':
    unittest.main(verbosity=2)
