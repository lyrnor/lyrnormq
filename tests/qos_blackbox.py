#!/usr/bin/env python3
"""Independent MQTT 3.1.1 live clean-session QoS wire tests."""
import socket
import struct
import unittest
from blackbox import Broker, connect_packet, frame, receive, string


def publish(topic, payload, qos=0, packet_id=None, dup=False):
    body = string(topic)
    if qos:
        body += struct.pack('!H', packet_id)
    return frame(0x30 | (qos << 1) | (8 if dup else 0), body + payload)


def ack(header, packet_id):
    return frame(header, struct.pack('!H', packet_id))


def parse_publish(packet):
    header, body = packet
    assert header >> 4 == 3, packet
    length = struct.unpack('!H', body[:2])[0]
    topic = body[2:2 + length].decode()
    offset = 2 + length
    qos = (header >> 1) & 3
    packet_id = None
    if qos:
        packet_id = struct.unpack('!H', body[offset:offset + 2])[0]
        assert packet_id != 0
        offset += 2
    return topic, body[offset:], qos, packet_id, bool(header & 8), bool(header & 1)


class QoSBlackbox(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.broker = Broker().__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.broker.__exit__(None, None, None)

    def client(self, name):
        sock = self.broker.socket()
        self.addCleanup(sock.close)
        sock.sendall(connect_packet(name))
        self.assertEqual(receive(sock), (0x20, b'\0\0'))
        return sock

    def subscribe(self, sock, filters):
        sock.sendall(frame(0x82, b'\0\x01' + b''.join(string(f) + bytes([q]) for f, q in filters)))
        self.assertEqual(receive(sock), (0x90, b'\0\x01' + bytes(q for _, q in filters)))

    def quiet(self, sock):
        sock.settimeout(.15)
        try:
            with self.assertRaises(socket.timeout):
                sock.recv(1)
        finally:
            sock.settimeout(3)

    def closed(self, sock):
        try:
            self.assertEqual(sock.recv(1), b'')
        except ConnectionResetError:
            pass

    def incoming(self, sock, topic, payload, qos, packet_id):
        sock.sendall(publish(topic, payload, qos, packet_id))
        if qos == 1:
            self.assertEqual(receive(sock), (0x40, struct.pack('!H', packet_id)))
        elif qos == 2:
            self.assertEqual(receive(sock), (0x50, struct.pack('!H', packet_id)))
            sock.sendall(ack(0x62, packet_id))
            self.assertEqual(receive(sock), (0x70, struct.pack('!H', packet_id)))

    def complete_outgoing(self, sock, packet_id, qos):
        if qos == 1:
            sock.sendall(ack(0x40, packet_id))
        elif qos == 2:
            sock.sendall(ack(0x50, packet_id))
            self.assertEqual(receive(sock), (0x62, struct.pack('!H', packet_id)))
            sock.sendall(ack(0x70, packet_id))

    def test_qos_matrix_max_overlap_and_minimum_grant(self):
        for incoming_qos in range(3):
            for granted in range(3):
                with self.subTest(incoming=incoming_qos, granted=granted):
                    topic = f'matrix/{incoming_qos}/{granted}'
                    sub = self.client(f'matrix-sub-{incoming_qos}-{granted}')
                    pub = self.client(f'matrix-pub-{incoming_qos}-{granted}')
                    self.subscribe(sub, [(topic, granted), (topic + '/#', 0)])
                    self.incoming(pub, topic, b'matrix', incoming_qos, 77)
                    name, payload, qos, packet_id, dup, retain = parse_publish(receive(sub))
                    self.assertEqual((name, payload, qos, dup, retain),
                                     (topic, b'matrix', min(incoming_qos, granted), False, False))
                    self.complete_outgoing(sub, packet_id, qos)
                    self.quiet(sub)

    def test_incoming_qos2_duplicates_are_not_forwarded_before_pubrel(self):
        sub = self.client('incoming-qos2-sub')
        pub = self.client('incoming-qos2-pub')
        self.subscribe(sub, [('incoming-qos2', 0)])
        pub.sendall(publish('incoming-qos2', b'original', 2, 23))
        self.assertEqual(receive(pub), (0x50, b'\0\x17'))
        pub.sendall(publish('incoming-qos2', b'changed', 2, 23, dup=True))
        self.assertEqual(receive(pub), (0x50, b'\0\x17'))
        self.quiet(sub)
        pub.sendall(ack(0x62, 23))
        self.assertEqual(receive(pub), (0x70, b'\0\x17'))
        self.assertEqual(parse_publish(receive(sub))[:3], ('incoming-qos2', b'original', 0))
        pub.sendall(ack(0x62, 23))
        self.assertEqual(receive(pub), (0x70, b'\0\x17'))
        pub.sendall(ack(0x62, 65432))
        self.assertEqual(receive(pub), (0x70, struct.pack('!H', 65432)))
        self.quiet(sub)
        # A completed identifier can name a fresh exchange even when DUP is set.
        pub.sendall(publish('incoming-qos2', b'new exchange', 2, 23, dup=True))
        self.assertEqual(receive(pub), (0x50, b'\0\x17'))
        pub.sendall(ack(0x62, 23))
        self.assertEqual(receive(pub), (0x70, b'\0\x17'))
        self.assertEqual(parse_publish(receive(sub))[:3], ('incoming-qos2', b'new exchange', 0))

    def test_outgoing_qos2_duplicate_pubrec_repeats_only_pubrel(self):
        sub = self.client('outgoing-qos2-sub')
        pub = self.client('outgoing-qos2-pub')
        self.subscribe(sub, [('outgoing-qos2', 2)])
        self.incoming(pub, 'outgoing-qos2', b'outgoing', 2, 9)
        packet = parse_publish(receive(sub))
        self.assertEqual(packet[:3], ('outgoing-qos2', b'outgoing', 2))
        packet_id = packet[3]
        sub.sendall(ack(0x50, packet_id))
        self.assertEqual(receive(sub), (0x62, struct.pack('!H', packet_id)))
        sub.sendall(ack(0x50, packet_id))
        self.assertEqual(receive(sub), (0x62, struct.pack('!H', packet_id)))
        sub.sendall(ack(0x70, packet_id))
        self.quiet(sub)

    def test_incoming_and_outgoing_identifier_namespaces_are_independent(self):
        sock = self.client('independent-ids')
        self.subscribe(sock, [('self-qos2', 2)])
        # The first outgoing allocator identifier may equal the incoming ID.
        sock.sendall(publish('self-qos2', b'self', 2, 1))
        self.assertEqual(receive(sock), (0x50, b'\0\x01'))
        sock.sendall(ack(0x62, 1))
        packets = [receive(sock), receive(sock)]
        self.assertIn((0x70, b'\0\x01'), packets)
        message = next(packet for packet in packets if packet[0] >> 4 == 3)
        parsed = parse_publish(message)
        self.assertEqual(parsed[:3], ('self-qos2', b'self', 2))
        self.complete_outgoing(sock, parsed[3], 2)
        sock.sendall(frame(0xc0))
        self.assertEqual(receive(sock), (0xd0, b''))

    def test_wrong_stage_outgoing_ack_disconnects(self):
        for qos, wrong_header in [(1, 0x50), (2, 0x70)]:
            with self.subTest(qos=qos):
                topic = f'wrong-ack/{qos}'
                sub = self.client(f'wrong-ack-sub-{qos}')
                pub = self.client(f'wrong-ack-pub-{qos}')
                self.subscribe(sub, [(topic, qos)])
                self.incoming(pub, topic, b'bad ack', qos, 4)
                packet_id = parse_publish(receive(sub))[3]
                sub.sendall(ack(wrong_header, packet_id))
                self.closed(sub)

    def test_qos1_duplicate_is_valid_at_least_once(self):
        sub = self.client('qos1-dup-sub')
        pub = self.client('qos1-dup-pub')
        self.subscribe(sub, [('qos1-dup', 0)])
        for dup in (False, True):
            pub.sendall(publish('qos1-dup', b'valid duplicate', 1, 45, dup))
            self.assertEqual(receive(pub), (0x40, b'\0\x2d'))
            self.assertEqual(parse_publish(receive(sub))[:3], ('qos1-dup', b'valid duplicate', 0))

    def test_incoming_window_disconnects_new_id_without_acknowledgement(self):
        pub = self.client('incoming-window-full')
        for packet_id in range(1, 33):
            pub.sendall(publish('incoming-window', b'pending', 2, packet_id))
            self.assertEqual(receive(pub), (0x50, struct.pack('!H', packet_id)))
        # A retransmission does not consume a second slot, even at full capacity.
        pub.sendall(publish('incoming-window', b'ignored duplicate', 2, 1, dup=True))
        self.assertEqual(receive(pub), (0x50, b'\0\x01'))
        pub.sendall(publish('incoming-window', b'rejected', 2, 33))
        # Reading even one byte would expose an erroneous PUBREC before disconnect.
        self.closed(pub)

    def test_outgoing_window_waits_for_final_ack_and_preserves_ids(self):
        sub = self.client('window-sub')
        pub = self.client('window-pub')
        self.subscribe(sub, [('window', 2)])
        # BrokerConfig's documented default window is 32; use 33 small messages.
        ids = []
        for n in range(32):
            self.incoming(pub, 'window', str(n).encode(), 2, n + 1)
            packet_id = parse_publish(receive(sub))[3]
            self.assertNotIn(packet_id, ids)
            ids.append(packet_id)
        self.incoming(pub, 'window', b'queued', 2, 100)
        self.quiet(sub)
        sub.sendall(ack(0x50, ids[0]))
        self.assertEqual(receive(sub), (0x62, struct.pack('!H', ids[0])))
        self.quiet(sub)  # PUBREC must not free the identifier/window slot.
        sub.sendall(ack(0x70, ids[0]))
        queued = parse_publish(receive(sub))
        self.assertEqual(queued[:3], ('window', b'queued', 2))
        self.assertNotIn(queued[3], ids[1:])

    def test_subscription_replacement_changes_granted_qos(self):
        sub = self.client('replace-qos-sub')
        pub = self.client('replace-qos-pub')
        self.subscribe(sub, [('replace-qos', 2)])
        self.subscribe(sub, [('replace-qos', 1)])
        self.incoming(pub, 'replace-qos', b'replaced', 2, 11)
        packet = parse_publish(receive(sub))
        self.assertEqual(packet[:3], ('replace-qos', b'replaced', 1))
        self.complete_outgoing(sub, packet[3], 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
