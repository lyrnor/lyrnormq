#!/usr/bin/env python3
"""RF3/W2 process acceptance with a gated third-replica HTTP/2 transport.

LYRNORMQ_BIN=/absolute/lyrnormq python3 tests/cluster_quorum_cpu_blackbox.py -v
The proxy reads only HTTP/2 frame metadata; no HPACK, credentials or bodies are
recorded. Existing W2/QoS cases also run on the pre-optimization binary. The tail
reset regression intentionally fails there; it is not a throughput measurement.
"""
import concurrent.futures
import socket
import struct
import threading
import time
import unittest

from blackbox import frame, receive, string
from cluster_blackbox import wait_until
from cluster_memory_blackbox import MemoryFleet, MemoryFixture
from mqtt5_blackbox import parse5, props, publication5, readprops
from qos_blackbox import parse_publish
from retained_will_blackbox import publication


class GatedReplica:
    """Transparent finite proxy; a response gate leaves requests in flight."""

    PREFACE = b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n'
    MAX_CONNECTIONS = 32
    MAX_FRAME = 1024 * 1024

    def __init__(self, listen, backend, host='127.0.0.1', backend_host=None):
        self.backend = backend
        self.backend_host = backend_host or host
        self.condition = threading.Condition()
        self.held = False
        self.closed = False
        self.blocked = 0
        self.forwarded = 0
        self.resets = 0
        self.errors = []
        self.sockets = []
        self.threads = []
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind((host, listen))
        self.listener.listen(self.MAX_CONNECTIONS)
        self.listener.settimeout(.1)
        self.acceptor = threading.Thread(target=self.accept, daemon=True)
        self.acceptor.start()

    def accept(self):
        while not self.closed:
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                server = socket.create_connection((self.backend_host, self.backend), timeout=2)
                with self.condition:
                    if self.closed or len(self.sockets) >= self.MAX_CONNECTIONS * 2:
                        client.close()
                        server.close()
                        continue
                    self.sockets.extend((client, server))
                client.settimeout(.1)
                server.settimeout(.1)
                for source, target, response in ((client, server, False), (server, client, True)):
                    thread = threading.Thread(target=self.forward,
                                              args=(source, target, response), daemon=True)
                    self.threads.append(thread)
                    thread.start()
            except OSError:
                client.close()

    def forward(self, source, target, response):
        buffer = bytearray()
        preface = response
        try:
            while not self.closed:
                try:
                    data = source.recv(65536)
                except socket.timeout:
                    continue
                if not data:
                    return
                buffer.extend(data)
                if not preface:
                    if len(buffer) < len(self.PREFACE):
                        continue
                    if bytes(buffer[:len(self.PREFACE)]) != self.PREFACE:
                        raise ValueError('invalid HTTP/2 client preface')
                    target.sendall(buffer[:len(self.PREFACE)])
                    del buffer[:len(self.PREFACE)]
                    preface = True
                while len(buffer) >= 9:
                    length = int.from_bytes(buffer[:3], 'big')
                    if length > self.MAX_FRAME:
                        raise ValueError('test proxy frame limit exceeded')
                    if len(buffer) < 9 + length:
                        break
                    packet = bytes(buffer[:9 + length])
                    del buffer[:9 + length]
                    kind, stream = packet[3], int.from_bytes(packet[5:9], 'big') & 0x7fffffff
                    was_blocked = False
                    with self.condition:
                        if response and stream and self.held:
                            self.blocked += 1
                            was_blocked = True
                            self.condition.notify_all()
                            self.condition.wait_for(lambda: not self.held or self.closed)
                        if not response and kind == 3:
                            self.resets += 1
                            self.condition.notify_all()
                        if self.closed:
                            return
                    target.sendall(packet)
                    if was_blocked:
                        with self.condition:
                            self.forwarded += 1
                            self.condition.notify_all()
        except ValueError as error:
            with self.condition:
                self.errors.append(str(error))
                self.condition.notify_all()
        except OSError:
            # Reset/EOF is expected when a quorum caller or fixture closes.
            pass
        finally:
            for sock in (source, target):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def hold(self):
        with self.condition:
            self.held = True
            return self.resets

    def release(self):
        with self.condition:
            self.held = False
            self.condition.notify_all()

    def close(self):
        with self.condition:
            self.closed = True
            self.held = False
            self.condition.notify_all()
        self.listener.close()
        for sock in self.sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        deadline = time.monotonic() + 3
        self.acceptor.join(timeout=max(0, deadline - time.monotonic()))
        for thread in self.threads:
            thread.join(timeout=max(0, deadline - time.monotonic()))
        if self.acceptor.is_alive() or any(thread.is_alive() for thread in self.threads):
            raise AssertionError('third-replica proxy did not stop')
        if self.errors:
            raise AssertionError(self.errors)


class GatedFleet(MemoryFleet):
    def __init__(self, directory):
        super().__init__(directory)
        # TOML peers dial http://{member.host}:{broker_port}. Put the gate on
        # that advertise address and move the real broker RPC behind an override.
        replica = self.brokers[2]
        backend = self.port()
        front = self.broker_port
        host = replica.host
        replica.broker_listen_override = f'{host}:{backend}'
        self.proxy = GatedReplica(front, backend, host=host, backend_host=host)

    def stop(self):
        self.proxy.release()
        try:
            super().stop()
        finally:
            self.proxy.close()


class QuorumCpuCases(MemoryFixture, unittest.TestCase):
    fleet_class = GatedFleet

    def subscribe_version(self, sock, topic, version):
        sock.sendall(frame(0x82, b'\x00\x01' + (props() if version == 5 else b'')
                           + string(topic) + b'\x02'))
        header, body = receive(sock)
        self.assertEqual(header, 0x90, body)
        self.assertEqual(body[:2], b'\x00\x01')
        offset = readprops(body, 2)[1] if version == 5 else 2
        self.assertEqual(body[offset:], b'\x02')

    def publish_version(self, sock, topic, payload, qos, packet_id, version):
        packet = (publication5(topic, payload, qos, packet_id) if version == 5
                  else publication(topic, payload, qos, packet_id))
        sock.sendall(packet)
        if qos:
            self.expect_ack(sock, 0x40 if qos == 1 else 0x50, packet_id)
            if qos == 2:
                sock.sendall(frame(0x62, struct.pack('!H', packet_id)))
                self.expect_ack(sock, 0x70, packet_id)

    def message_version(self, sock, version):
        return parse5(receive(sock)) if version == 5 else parse_publish(receive(sock))

    def test_both_versions_ordered_qos_with_third_replica_response_held(self):
        def conversation(version):
            subscriber = self.client(f'quorum-sub-{version}', 1, version=version, expiry=0)
            publisher = self.client(f'quorum-pub-{version}', 0, version=version, expiry=0)
            topic = f'quorum/ordered/{version}'
            self.subscribe_version(subscriber, topic, version)
            for qos in (0, 1, 2):
                for serial in range(2):
                    payload = f'{version}:{qos}:{serial}'.encode()
                    self.publish_version(publisher, topic, payload, qos, 10 + serial, version)
                    message = self.message_version(subscriber, version)
                    self.assertEqual(message[:3], (topic, payload, qos))
                    self.complete_message(subscriber, message)
            self.ping(publisher)
            self.quiet(subscriber, .025)
            self.disconnect(publisher)
            self.disconnect(subscriber)

        proxy = self.fleet.proxy
        proxy.hold()
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                tasks = [pool.submit(conversation, version) for version in (4, 5)]
                for task in tasks:
                    task.result(timeout=20)
            self.assertTrue(proxy.held, 'the third response gate must remain closed through ACKs')
            self.assertGreater(proxy.blocked, 0, 'no third-replica response reached the gate')
        finally:
            proxy.release()
        wait_until(lambda: proxy.forwarded > 0, 3)

    def test_pending_qos2_replays_on_new_owner_after_acknowledged_publication(self):
        pending = []
        for version in (4, 5):
            identity = f'quorum-recovery-{version}'
            topic = f'quorum/recovery/{version}'
            subscriber = self.persistent(identity, 0, version=version, expiry=3600)
            source = self.client(f'quorum-recovery-source-{version}', 1,
                                 version=version, expiry=0)
            self.subscribe_version(subscriber, topic, version)
            payload = f'confirmed-publication-{version}'.encode()
            self.publish_version(source, topic, payload, 2, 71 + version, version)
            first = self.message_version(subscriber, version)
            self.assertEqual(first[:3], (topic, payload, 2))
            pending.append((version, identity, topic, payload, first))
        self.lose_owner(0)
        for version, identity, topic, payload, first in pending:
            resumed = self.persistent(identity, 1, version=version, present=True,
                                      expiry=3600, retry=True)
            replay = self.message_version(resumed, version)
            self.assertEqual(replay[:4], first[:4])
            self.assertEqual(replay[:3], (topic, payload, 2))
            self.assertTrue(replay[4], 'recovered PUBLISH must carry DUP')
            self.complete_message(resumed, replay)
            self.quiet(resumed, .025)
            self.disconnect(resumed)

    def test_successful_quorum_does_not_reset_pending_third_response(self):
        proxy = self.fleet.proxy
        before = proxy.hold()
        start = time.monotonic()
        try:
            client = self.persistent('quorum-tail-lifecycle', 0, expiry=3600)
            with proxy.condition:
                self.assertTrue(proxy.condition.wait_for(lambda: proxy.blocked > 0, timeout=.5),
                                'third response did not reach the gate')
                reset = proxy.condition.wait_for(lambda: proxy.resets != before, timeout=.25)
                self.assertFalse(reset, 'successful W2 canceled an already-issued third RPC')
            self.assertLess(time.monotonic() - start, 1.5,
                            'tail test exceeded the unchanged request deadline margin')
            self.ping(client)
        finally:
            proxy.release()
        wait_until(lambda: proxy.forwarded > 0, 3)
        self.disconnect(client)


if __name__ == '__main__':
    unittest.main()
