#!/usr/bin/env python3
"""Event forwarding acceptance over real MQTT and bounded peer-RPC faults.

RMQTT_BIN=/absolute/path/rmqtt python3 tests/cluster_event_blackbox.py -v
Timing samples include a fixed200ms quiet/barrier check; they are observations,
not sub-100ms conformance claims. Polling removal
and no-idle-scans have deterministic scheduler unit oracles in the runtime crate.
"""
import os
import signal
import socket
import struct
import threading
import time
import unittest

from blackbox import receive
from cluster_blackbox import wait_until
from mqtt5_blackbox import parse5
from qos_blackbox import ack, parse_publish
from cluster_memory_blackbox import MemoryFleet, MemoryFixture
from cluster_repair_proxy import exact


class DeliveryFault:
    """Bounded h2 relay affecting only one named RecipientDelivery.

    Every connection has one reader in each direction, a 16MiB frame bound, and
    bounded socket waits. Replica/metadata requests pass without mutation. Closing
    a selected stream's connection models an RPC transport failure; in response
    mode it happens only AFTER the recipient's committed delivery reply exists.
    """
    def __init__(self, port, backend, recipient, host='127.0.0.1', backend_host='127.0.0.1'):
        self.backend = backend
        self.backend_host = backend_host
        self.marker = ('{"client_id":"' + recipient + '","id":').encode()
        self.enabled = threading.Event()
        self.response_loss = False
        self.response_dropped = threading.Event()
        self.stopped = threading.Event()
        self.lock = threading.Lock()
        self.sockets = set()
        self.threads = []
        self.attempts = 0
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind((host, port))
        self.listener.listen(32)
        self.listener.settimeout(.1)
        self.acceptor = threading.Thread(target=self.accept, daemon=True)
        self.acceptor.start()

    def block(self, response=False):
        self.response_loss = response
        self.enabled.set()

    def unblock(self):
        self.enabled.clear()

    def count(self):
        with self.lock:
            return self.attempts

    @staticmethod
    def close_socket(sock):
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()

    def accept(self):
        while not self.stopped.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with self.lock:
                # A test cannot create unlimited sockets or background threads.
                if len(self.sockets) >= 128:
                    client.close()
                    continue
                self.sockets.add(client)
                self.threads = [worker for worker in self.threads if worker.is_alive()]
                worker = threading.Thread(target=self.connection, args=(client,), daemon=True)
                self.threads.append(worker)
            worker.start()

    def connection(self, client):
        server = None
        response = None
        try:
            server = socket.create_connection((self.backend_host, self.backend), timeout=3)
            client.settimeout(12)
            server.settimeout(12)
            with self.lock:
                self.sockets.add(server)
            streams = set()
            stream_lock = threading.Lock()
            response = threading.Thread(target=self.responses,
                                        args=(server, client, streams, stream_lock), daemon=True)
            response.start()
            preface = exact(client, 24)
            if preface != b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n':
                raise ValueError('unexpected private RPC transport')
            server.sendall(preface)
            while not self.stopped.is_set():
                header, payload = self.read_frame(client)
                if header[3] == 0 and self.marker in payload:
                    with self.lock:
                        self.attempts += 1
                    if self.enabled.is_set():
                        if not self.response_loss or self.response_dropped.is_set():
                            return
                        with stream_lock:
                            streams.add(header[5:9])
                server.sendall(header + payload)
        except (OSError, EOFError, ValueError):
            pass
        finally:
            for sock in (client, server):
                if sock is not None:
                    self.close_socket(sock)
                    with self.lock:
                        self.sockets.discard(sock)
            if response is not None:
                response.join(timeout=1)

    @staticmethod
    def read_frame(sock):
        header = exact(sock, 9)
        length = int.from_bytes(header[:3], 'big')
        if length > 16 * 1024 * 1024:
            raise ValueError('unbounded private RPC frame')
        return header, exact(sock, length)

    def responses(self, server, client, streams, lock):
        try:
            while not self.stopped.is_set():
                header, payload = self.read_frame(server)
                with lock:
                    selected = header[5:9] in streams
                if selected and header[3] == 0 and self.enabled.is_set():
                    self.response_dropped.set()
                    return
                client.sendall(header + payload)
        except (OSError, EOFError, ValueError):
            pass
        finally:
            self.close_socket(client)
            self.close_socket(server)

    def close(self):
        self.stopped.set()
        self.listener.close()
        with self.lock:
            sockets = list(self.sockets)
        for sock in sockets:
            self.close_socket(sock)
        self.acceptor.join(timeout=2)
        for worker in self.threads:
            worker.join(timeout=2)


class EventFleet(MemoryFleet):
    recipient = 'event-target'

    def __init__(self, directory):
        super().__init__(directory)
        target = self.brokers[1]
        backend = self.port()
        target.broker_listen_override = f'{target.host}:{backend}'
        target.config['listen'] = target.broker_listen_override
        self.fault = DeliveryFault(
            self.broker_port, backend, self.recipient,
            host=target.host, backend_host=target.host)

    def stop(self):
        try:
            super().stop()
        finally:
            self.fault.close()


class EventCases(MemoryFixture):
    fleet_class = EventFleet

    def pair(self, qos=2):
        self.assert_memory_status()
        target = self.persistent('event-target', 1, expiry=3600)
        self.subscribe(target, 'event/messages', qos)
        source = self.persistent('event-source', 0, expiry=3600)
        return source, target

    def drain(self, target, bodies, qos, topic='event/messages'):
        pending = set()
        delivered = []
        while len(delivered) < len(bodies) or pending:
            packet = receive(target)
            if packet[0] >> 4 == 3:
                message = parse5(packet) if self.version == 5 else parse_publish(packet)
                self.assertLess(len(delivered), len(bodies), 'unexpected extra PUBLISH')
                self.assertEqual(message[:3], (topic, bodies[len(delivered)], qos))
                delivered.append(message[1])
                if qos == 1:
                    target.sendall(ack(0x40, message[3]))
                elif qos == 2:
                    self.assertNotIn(message[3], pending)
                    pending.add(message[3])
                    target.sendall(ack(0x50, message[3]))
            else:
                self.assertEqual(packet[0], 0x62, packet)
                self.assertGreaterEqual(len(packet[1]), 2)
                identifier = struct.unpack('!H', packet[1][:2])[0]
                self.assertIn(identifier, pending)
                pending.remove(identifier)
                target.sendall(ack(0x70, identifier))
        # Queued PUBLISH/PUBREL may interleave; only now is PING a valid barrier.
        self.ping(target)
        self.quiet(target, .2)

    def drain_qos0(self, target, topic, bodies):
        remaining = set(bodies)
        while remaining:
            packet = receive(target)
            self.assertEqual(packet[0] >> 4, 3, packet)
            message = parse5(packet) if self.version == 5 else parse_publish(packet)
            self.assertEqual((message[0], message[2]), (topic, 0), message)
            self.assertIn(message[1], remaining, message)
            remaining.remove(message[1])
        self.ping(target)
        self.quiet(target, .2)

    def observation(self, label, began):
        print(f'\nOBSERVATION MQTT{self.version} {label}_ms='
              f'{(time.monotonic() - began) * 1000:.3f}', flush=True)

    def test_idle_publish_and_many_pending_intents_preserve_fifo(self):
        source, target = self.pair()
        for qos in (0, 1, 2):
            with self.subTest(qos=qos):
                topic = f'event/fifo/{qos}'
                self.subscribe(target, topic, qos)
                self.fleet.fault.block()
                before = self.fleet.fault.count()
                bodies = [f'q{qos}-sequence-{i:03}'.encode() for i in range(12)]
                for i, body in enumerate(bodies):
                    self.publish(source, topic, body, qos, packet_id=i + 1)
                self.ping(source)
                if qos == 0:
                    # QoS 0 uses batched Forward RPCs, not RecipientDelivery, and
                    # MQTT does not require in-order delivery at QoS 0.
                    self.fleet.fault.unblock()
                    self.drain_qos0(target, topic, bodies)
                else:
                    wait_until(lambda: self.fleet.fault.count() > before, 10)
                    began = time.monotonic()
                    self.fleet.fault.unblock()
                    self.drain(target, bodies, qos, topic)
                    self.observation(f'fifo_qos{qos}_12_total', began)
                time.sleep(.25)
                began = time.monotonic()
                self.publish(source, topic, b'after-idle', qos, packet_id=100)
                if qos == 0:
                    self.drain_qos0(target, topic, [b'after-idle'])
                else:
                    self.drain(target, [b'after-idle'], qos, topic)
                self.observation(f'after_idle_qos{qos}', began)

    def test_failed_rpc_retries_without_any_new_publish(self):
        source, target = self.pair(1)
        self.fleet.fault.block()
        self.publish(source, 'event/messages', b'retry-existing-work', 1)
        wait_until(lambda: self.fleet.fault.count() >= 2, 12)
        # No source publish/CONNECT/PING after fault recovery may wake this work.
        self.fleet.fault.unblock()
        self.drain(target, [b'retry-existing-work'], 1)
        self.data_files()

    def test_lost_committed_reply_retries_without_duplicate_delivery(self):
        source, target = self.pair(2)
        self.fleet.fault.block(response=True)
        self.publish(source, 'event/messages', b'committed-but-reply-lost', 2)
        self.assertTrue(self.fleet.fault.response_dropped.wait(12))
        self.drain(target, [b'committed-but-reply-lost'], 2)
        wait_until(lambda: self.fleet.fault.count() >= 2, 12)
        self.fleet.fault.unblock()
        self.quiet(target, 1.2)
        # A second intent verifies completion of the first did not strand FIFO.
        self.publish(source, 'event/messages', b'following-completion', 1, packet_id=18)
        self.drain(target, [b'following-completion'], 1)

    def test_lost_source_owner_resumes_pending_work_without_new_publish(self):
        source, target = self.pair(1)
        self.fleet.fault.block()
        bodies = [b'owner-loss-first', b'owner-loss-second']
        for index, body in enumerate(bodies):
            self.publish(source, 'event/messages', body, 1, packet_id=index + 1)
        wait_until(lambda: self.fleet.fault.count() >= 1, 10)
        self.lose_owner(0)
        wait_until(lambda: (self.fleet.status() or {}).get('nodes') == 2, 20)
        self.fleet.fault.unblock()
        self.drain(target, bodies, 1)
        self.data_files()

    def test_control_readiness_recovery_retains_committed_work(self):
        source, target = self.pair(1)
        self.fleet.fault.block()
        self.publish(source, 'event/messages', b'before-control-gap', 1)
        wait_until(lambda: self.fleet.fault.count() >= 1, 10)
        state = self.fleet.status()
        self.assertIsNotNone(state)
        paused = sorted(self.fleet.controllers,
                        key=lambda node: node.config['node_id'] != state['leader'])[:2]
        for node in paused:
            os.kill(node.process.pid, signal.SIGSTOP)
        try:
            # Dashboard process readiness does not mean cluster admission. Wait
            # beyond the configured3s lease, then assert the actual MQTT gate.
            time.sleep(4)
            self.no_positive_connack(self.fleet.brokers[0])
            self.fleet.fault.unblock()
        finally:
            for node in paused:
                os.kill(node.process.pid, signal.SIGCONT)
        self.healthy(0)
        self.healthy(1)
        target.close()
        restored = self.persistent('event-target', 1, present=True, expiry=3600, retry=True)
        self.drain(restored, [b'before-control-gap'], 1)
        self.data_files()


class Event311(EventCases, unittest.TestCase):
    version = 4


class Event5(EventCases, unittest.TestCase):
    version = 5


if __name__ == '__main__':
    unittest.main()
