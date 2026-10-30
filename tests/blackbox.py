#!/usr/bin/env python3
"""Independent MQTT 3.1.1 wire tests. Run after cargo build --workspace."""
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]


def string(value):
    data = value.encode()
    return struct.pack('!H', len(data)) + data


def frame(header, body=b''):
    length = len(body)
    prefix = bytearray([header])
    while True:
        digit = length % 128
        length //= 128
        prefix.append(digit | (128 if length else 0))
        if not length:
            return bytes(prefix) + body


def connect_packet(client, keepalive=30, version=4):
    return frame(0x10, string('MQTT') + bytes([version, 2])
                 + struct.pack('!H', keepalive) + string(client))


def exact(sock, count):
    data = bytearray()
    while len(data) < count:
        chunk = sock.recv(count - len(data))
        if not chunk:
            raise EOFError('connection closed')
        data.extend(chunk)
    return bytes(data)


def receive(sock):
    header = exact(sock, 1)[0]
    length = 0
    for shift in range(0, 28, 7):
        digit = exact(sock, 1)[0]
        length |= (digit & 127) << shift
        if not digit & 128:
            return header, exact(sock, length)
    raise AssertionError('invalid server remaining length')


class Broker:
    def __enter__(self):
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            self.port = probe.getsockname()[1]
        executable = os.environ.get('RMQTT_BIN', str(ROOT / 'target/debug/rmqtt'))
        self.rules_directory = tempfile.TemporaryDirectory(prefix='rmqtt-wire-rules-')
        command = [executable, '--listen', f'127.0.0.1:{self.port}',
                   '--rules-config', str(Path(self.rules_directory.name) / 'rules.sqlite')]
        storage_path = os.environ.get('RMQTT_TEST_STORAGE_PATH')
        if storage_path:
            command.extend(['--storage-path', storage_path])
        self.process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(self.process.stderr.read().decode())
            try:
                with socket.create_connection(('127.0.0.1', self.port), timeout=.1):
                    return self
            except OSError:
                time.sleep(.02)
        self.__exit__(None, None, None)
        raise TimeoutError('broker did not start')

    def __exit__(self, *_):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                raise AssertionError('broker failed graceful shutdown')
        self.process.stderr.close()
        self.rules_directory.cleanup()

    def socket(self):
        sock = socket.create_connection(('127.0.0.1', self.port), timeout=3)
        sock.settimeout(3)
        return sock


class MQTTBlackbox(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.broker = Broker().__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.broker.__exit__(None, None, None)

    def client(self, name, keepalive=30):
        sock = self.broker.socket()
        self.addCleanup(sock.close)
        sock.sendall(connect_packet(name, keepalive))
        self.assertEqual(receive(sock), (0x20, b'\x00\x00'))
        return sock

    def closed(self, sock):
        try:
            self.assertEqual(sock.recv(1), b'')
        except (ConnectionResetError, BrokenPipeError):
            pass

    def subscribe(self, sock, filters, packet_id=1):
        sock.sendall(frame(0x82, struct.pack('!H', packet_id)
                           + b''.join(string(f) + bytes([q]) for f, q in filters)))
        return receive(sock)

    def test_wildcards_dedup_qos_grant_and_unsubscribe(self):
        sub = self.client('routing-sub')
        pub = self.client('routing-pub')
        self.assertEqual(self.subscribe(sub, [('sensor/+', 2), ('sensor/#', 1)]),
                         (0x90, b'\x00\x01\x02\x01'))
        pub.sendall(frame(0x30, string('sensor/room') + b'payload\x00bytes'))
        self.assertEqual(receive(sub), (0x30, string('sensor/room') + b'payload\x00bytes'))
        # PINGRESP must be next: detects overlapping-subscription duplicate deliveries.
        sub.sendall(frame(0xc0))
        self.assertEqual(receive(sub), (0xd0, b''))
        sub.sendall(frame(0xa2, b'\x00\x02' + string('sensor/+') + string('sensor/#')))
        self.assertEqual(receive(sub), (0xb0, b'\x00\x02'))
        pub.sendall(frame(0x30, string('sensor/room') + b'not delivered'))
        self.assertEqual(self.subscribe(pub, [('barrier', 0)]), (0x90, b'\x00\x01\x00'))
        sub.settimeout(.2)
        with self.assertRaises(socket.timeout):
            sub.recv(1)

    def test_system_topic_isolation(self):
        sub = self.client('system-sub')
        pub = self.client('system-pub')
        self.assertEqual(self.subscribe(sub, [('#', 0)]), (0x90, b'\x00\x01\x00'))
        pub.sendall(frame(0x30, string('$SYS/status') + b'private'))
        self.assertEqual(self.subscribe(pub, [('barrier', 0)]), (0x90, b'\x00\x01\x00'))
        sub.settimeout(.2)
        with self.assertRaises(socket.timeout):
            sub.recv(1)
        sub.settimeout(3)
        self.assertEqual(self.subscribe(sub, [('$SYS/#', 0)]), (0x90, b'\x00\x01\x00'))
        pub.sendall(frame(0x30, string('$SYS/status') + b'visible'))
        self.assertEqual(receive(sub), (0x30, string('$SYS/status') + b'visible'))

    def test_fragmented_connect_and_coalesced_packets(self):
        sock = self.broker.socket()
        self.addCleanup(sock.close)
        for byte in connect_packet('fragmented'):
            sock.sendall(bytes([byte]))
        self.assertEqual(receive(sock), (0x20, b'\x00\x00'))
        sock.sendall(frame(0xc0) + frame(0xc0))
        self.assertEqual(receive(sock), (0xd0, b''))
        self.assertEqual(receive(sock), (0xd0, b''))
        sock.sendall(frame(0xe0))
        self.closed(sock)

    def test_invalid_filters_are_rejected(self):
        sock = self.client('bad-filters')
        self.assertEqual(self.subscribe(sock, [('a/#/b', 0), ('$share/g/a', 0), ('valid', 0)]),
                         (0x90, b'\x00\x01\x80\x80\x00'))

    def test_second_connect_disconnects(self):
        sock = self.client('second-connect')
        sock.sendall(connect_packet('second-connect'))
        self.closed(sock)

    def test_server_direction_packet_disconnects(self):
        sock = self.client('server-direction')
        sock.sendall(frame(0xd0))
        self.closed(sock)

    def test_connect_must_be_first(self):
        sock = self.broker.socket()
        self.addCleanup(sock.close)
        sock.sendall(frame(0xc0))
        self.closed(sock)

    def test_malformed_headers_and_utf8(self):
        for index, data in enumerate([b'\xc1\x00', b'\xc0\x80\x80\x80\x80',
                                      frame(0x30, b'\x00\x01\xff'),
                                      frame(0x82, b'\x00\x00' + string('a') + b'\x00')]):
            with self.subTest(index=index):
                sock = self.client(f'malformed-{index}')
                sock.sendall(data)
                self.closed(sock)

    def test_malformed_publish_flags_disconnect(self):
        for header in (0x36, 0x38, 0x3e):
            with self.subTest(header=header):
                sock = self.client(f'invalid-publish-{header}')
                sock.sendall(frame(header, string('a') + b'\x00\x01payload'))
                self.closed(sock)

    def test_unsupported_protocol_level_rejected_with_connack(self):
        sock = self.broker.socket()
        self.addCleanup(sock.close)
        sock.sendall(connect_packet('unsupported-v6', version=6))
        self.assertEqual(receive(sock), (0x20, b'\x00\x01'))
        self.closed(sock)

    def test_invalid_unsubscribe_filter_disconnects(self):
        sock = self.client('invalid-unsubscribe')
        sock.sendall(frame(0xa2, b'\x00\x01' + string('a/#/b')))
        self.closed(sock)

    def test_partial_packet_does_not_refresh_keepalive(self):
        sock = self.client('partial-keepalive', keepalive=1)
        # The declared body is 4,096 bytes, much larger than this test sends.
        # Trickle bytes continuously; only a complete control packet may reset
        # the server's keepalive timer. A traffic-based timer would never fire.
        sock.sendall(b'\x30\x80\x20')
        deadline = time.monotonic() + 3
        sock.settimeout(.3)
        while time.monotonic() < deadline:
            try:
                if sock.recv(1) == b'':
                    return
            except socket.timeout:
                pass
            except (ConnectionResetError, BrokenPipeError):
                return
            try:
                sock.sendall(b'x')
            except (ConnectionResetError, BrokenPipeError):
                return
        self.fail('partial packet traffic incorrectly refreshed keepalive')

    def test_keepalive_timeout(self):
        sock = self.client('keepalive', keepalive=1)
        sock.settimeout(4)
        self.closed(sock)

    def test_same_id_takeover_and_cleanup(self):
        old = self.client('takeover')
        self.subscribe(old, [('takeover/old', 0)])
        new = self.client('takeover')
        self.closed(old)
        self.assertEqual(self.subscribe(new, [('takeover/new', 0)]), (0x90, b'\x00\x01\x00'))
        pub = self.client('takeover-pub')
        pub.sendall(frame(0x30, string('takeover/new') + b'alive'))
        self.assertEqual(receive(new), (0x30, string('takeover/new') + b'alive'))


if __name__ == '__main__':
    unittest.main(verbosity=2)
