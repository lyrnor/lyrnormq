#!/usr/bin/env python3
"""Phase7b real three-Controller/three-disk Broker recovery acceptance.

RMQTT_BIN=/absolute/path/rmqtt python3 tests/cluster_durable_blackbox.py -v
No failed Broker directory is mounted or copied into its replacement. Protocol
ACKs are durability barriers; process crashes do not certify physical power loss.
"""
import os
import copy
import json
import signal
import socket
import struct
import tempfile
import time
import unittest

from blackbox import frame, receive
from cluster_blackbox import ClusterFixture, Fleet, Node, wait_until
from auth_acl_blackbox import PASSWORD, PASSWORD_HASH
from mqtt5_blackbox import connect5, parse5, publication5, readprops
from qos_blackbox import ack, parse_publish
from retained_will_blackbox import connect_packet, publication


class DurableNode(Node):
    def command(self):
        command = super().command()
        policy = self.root / 'policy.json'
        if policy.is_file():
            command += ['--auth-policy', str(policy)]
        return command


class DurableFleet(Fleet):
    def new_broker(self, node_id, **changes):
        root = self.root / f'broker-{node_id}'
        port = self.port()
        config = dict(role='broker', cluster_id='wire-test-cluster', node_id=node_id,
                      listen=f'127.0.0.1:{port}', advertise=f'http://127.0.0.1:{port}',
                      data_dir=str(root / 'cluster-data'), token_file=str(self.token),
                      controllers=copy.deepcopy(self.controllers[0].config['controllers']),
                      lease_ms=3000)
        config.update(changes)
        return DurableNode(root, config, self.port(), self.port())

    def __init__(self, directory):
        super().__init__(directory)
        self.brokers.append(self.new_broker(103))
        replicas = [{'id': node.config['node_id'], 'address': node.config['advertise']}
                    for node in self.brokers]
        for node in self.brokers:
            node.config['durable'] = {'replicas': replicas}


class DurableFixture(ClusterFixture):
    policy = None

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-durable-cluster-')
        self.addCleanup(self.directory.cleanup)
        self.fleet = DurableFleet(self.directory.name)
        self.addCleanup(self.fleet.stop)
        if self.policy is not None:
            for node in self.fleet.brokers:
                (node.root / 'policy.json').write_text(json.dumps(self.policy))
        self.fleet.start()

    def packet(self, client, clean=True, expiry=120, version=None, username=None):
        version = self.version if version is None else version
        if version == 5:
            return connect5(client, clean=clean, properties=[(0x11, expiry)], username=username,
                            password=PASSWORD if username is not None else None)
        self.assertIsNone(username, 'use the MQTT5 ownership case for credentials')
        return connect_packet(client, clean=clean)

    def client(self, client, node=0, clean=True, present=False, expiry=120,
               version=None, retry=False, username=None):
        version = self.version if version is None else version
        deadline = time.monotonic() + (35 if retry else 0)
        last = None
        while True:
            sock = None
            try:
                sock = socket.create_connection(('127.0.0.1', self.fleet.brokers[node].mqtt), timeout=3)
                sock.settimeout(8)
                sock.sendall(self.packet(client, clean, expiry, version, username))
                header, data = receive(sock)
                self.assertEqual(header, 0x20, (header, data))
                if data[1] == 0:
                    self.assertEqual(data[0], int(present), data)
                    if version == 5:
                        self.assertEqual(readprops(data, 2)[1], len(data))
                    self.addCleanup(sock.close)
                    return sock
                last = data
                self.assertIn(data[1], (0x88, 0x89) if version == 5 else (3,), data)
            except (OSError, EOFError) as error:
                last = repr(error)
            if sock is not None:
                sock.close()
            if not retry or time.monotonic() >= deadline:
                self.fail(f'no successful MQTT CONNECT on Broker {node}: {last!r}')
            time.sleep(.05)

    def persistent(self, client, node=0, present=False, **kwargs):
        return self.client(client, node, clean=False, present=present, **kwargs)

    def disconnect(self, sock):
        sock.sendall(frame(0xe0))
        # A normal DISCONNECT is committed by the MQTT actor before the server
        # closes its socket. Do not kill the owner immediately after sendall and
        # assume that an unprocessed DISCONNECT established an expiry deadline.
        try:
            self.assertEqual(sock.recv(1), b'')
        except ConnectionResetError:
            pass
        sock.close()

    def message(self, sock):
        packet = receive(sock)
        return parse5(packet) if self.version == 5 else parse_publish(packet)

    def complete_message(self, sock, message, barrier=True):
        qos, packet_id = message[2:4]
        if qos == 1:
            sock.sendall(ack(0x40, packet_id))
        elif qos == 2:
            sock.sendall(ack(0x50, packet_id))
            self.assertEqual(receive(sock), (0x62, struct.pack('!H', packet_id)))
            sock.sendall(ack(0x70, packet_id))
        if barrier:
            self.ping(sock)

    def send_publish(self, sock, topic, payload, qos, packet_id=17, dup=False):
        packet = (publication5(topic, payload, qos, packet_id, dup=dup)
                  if self.version == 5 else publication(topic, payload, qos, packet_id, dup=dup))
        sock.sendall(packet)

    def expect_ack(self, sock, header, packet_id, completed=False):
        actual, data = receive(sock)
        self.assertEqual(actual, header, data)
        self.assertEqual(data[:2], struct.pack('!H', packet_id))
        reason = data[2] if len(data) > 2 else 0
        if completed and self.version == 5:
            self.assertEqual(header, 0x70, 'only completed PUBREL retries use this exception')
            # OASIS MQTT5 §3.7.2.1 Table3-7 permits PUBCOMP Packet Identifier not
            # found during recovery. First pending exchanges still require 0.
            # https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html
            self.assertIn(reason, (0, 0x92), data)
        else:
            self.assertEqual(reason, 0, data)

    def expect_message(self, sock, topic, payload, qos):
        message = self.message(sock)
        self.assertEqual(message[:3], (topic, payload, qos))
        return message

    def lose_owner(self, node=0):
        self.fleet.brokers[node].stop(crash=True)

    def healthy(self, node):
        probe = self.client(f'healthy-{node}', node, expiry=0, retry=True)
        self.ping(probe)
        self.disconnect(probe)


class DurableSessionCases(DurableFixture):
    def test_offline_subscriptions_and_qos_survive_permanent_owner_loss(self):
        target = self.persistent('offline-device')
        self.subscribe(target, 'durable/offline', 2)
        self.disconnect(target)
        self.lose_owner()
        source = self.client('offline-source', 2)
        for qos in (1, 2):
            self.publish(source, 'durable/offline', f'offline-{qos}'.encode(), qos, packet_id=qos)
        resumed = self.persistent('offline-device', 1, present=True, retry=True)
        for qos in (1, 2):
            message = self.expect_message(resumed, 'durable/offline', f'offline-{qos}'.encode(), qos)
            # Both messages may already be inflight after CONNACK. A PING barrier
            # before reading the second would mistake its PUBLISH for PINGRESP.
            self.complete_message(resumed, message, barrier=qos == 2)
        self.quiet(resumed)

    def test_outgoing_awaitack_recovers_same_packet_id_with_dup_on_other_disk(self):
        self.outgoing_recovery('ack', 1)

    def test_outgoing_awaitrec_recovers_same_packet_id_with_dup_on_other_disk(self):
        self.outgoing_recovery('rec', 2)

    def test_outgoing_awaitcomp_recovers_pubrel_only_on_other_disk(self):
        self.outgoing_recovery('comp', 2)

    def outgoing_recovery(self, stage, qos):
        target = self.persistent('outgoing-' + stage)
        self.subscribe(target, 'durable/' + stage, qos)
        source = self.client('source-' + stage, 2)
        self.publish(source, 'durable/' + stage, b'committed before wire', qos)
        first = self.expect_message(target, 'durable/' + stage, b'committed before wire', qos)
        if stage == 'comp':
            target.sendall(ack(0x50, first[3]))
            self.assertEqual(receive(target), (0x62, struct.pack('!H', first[3])))
        self.lose_owner()
        resumed = self.persistent('outgoing-' + stage, 1, present=True, retry=True)
        if stage == 'comp':
            self.assertEqual(receive(resumed), (0x62, struct.pack('!H', first[3])))
            self.quiet(resumed)
            resumed.sendall(ack(0x70, first[3]))
            self.ping(resumed)
        else:
            replay = self.expect_message(resumed, 'durable/' + stage, b'committed before wire', qos)
            self.assertEqual(replay[:4], first[:4])
            self.assertTrue(replay[4], 'unacknowledged replay must retain ID and set DUP')
            self.complete_message(resumed, replay)
        self.quiet(resumed)

    def test_incoming_pubrec_resumes_elsewhere_and_duplicate_pubrel_routes_once(self):
        target = self.persistent('incoming-target', 2)
        self.subscribe(target, 'durable/incoming', 2)
        source = self.persistent('incoming-source')
        self.send_publish(source, 'durable/incoming', b'pending qos2', 2, 23)
        self.expect_ack(source, 0x50, 23)
        self.lose_owner()
        resumed = self.persistent('incoming-source', 1, present=True, retry=True)
        # MQTT senders can retransmit their original PUBLISH while AwaitPubrec.
        self.send_publish(resumed, 'durable/incoming', b'pending qos2', 2, 23, dup=True)
        self.expect_ack(resumed, 0x50, 23)
        resumed.sendall(ack(0x62, 23))
        self.expect_ack(resumed, 0x70, 23)
        received = self.expect_message(target, 'durable/incoming', b'pending qos2', 2)
        self.complete_message(target, received)
        resumed.sendall(ack(0x62, 23))
        self.expect_ack(resumed, 0x70, 23, completed=True)
        self.quiet(target)

    def test_lost_pubcomp_then_source_failover_does_not_duplicate_delivery(self):
        target = self.persistent('comp-target', 2)
        self.subscribe(target, 'durable/comp-once', 2)
        source = self.persistent('comp-source')
        self.send_publish(source, 'durable/comp-once', b'exact exchange', 2, 41)
        self.expect_ack(source, 0x50, 41)
        source.sendall(ack(0x62, 41))
        # Deliberately never read source PUBCOMP. Subscriber delivery + ACK is a
        # recipient durable barrier, not proof that the source terminal was read.
        delivered = self.expect_message(target, 'durable/comp-once', b'exact exchange', 2)
        self.complete_message(target, delivered)
        self.lose_owner()
        resumed = self.persistent('comp-source', 1, present=True, retry=True)
        resumed.sendall(ack(0x62, 41))
        self.expect_ack(resumed, 0x70, 41, completed=True)
        self.quiet(target, .4)

    def test_clean_start_discards_old_subscription_on_another_owner(self):
        target = self.persistent('clean-device')
        self.subscribe(target, 'durable/clean', 1)
        self.disconnect(target)
        self.lose_owner()
        reset = self.client('clean-device', 1, clean=True, present=False, retry=True)
        source = self.client('clean-source', 2)
        self.publish(source, 'durable/clean', b'old subscription must be gone', 1)
        self.quiet(reset)
        self.disconnect(reset)
        # MQTT5 clean start and positive expiry create a new persistent session;
        # MQTT311 clean session must be absent after its disconnect.
        resumed = self.persistent('clean-device', 1, present=self.version == 5)
        self.quiet(resumed)


class Durable311(DurableSessionCases, unittest.TestCase):
    version = 4


class Durable5(DurableSessionCases, unittest.TestCase):
    version = 5


class DurableFaults(DurableFixture, unittest.TestCase):
    version = 5

    def test_fixed_group_refuses_legacy_and_different_replica_membership(self):
        for node_id, durable in [(201, None), (202, True)]:
            node = self.fleet.new_broker(node_id)
            self.addCleanup(node.close)
            if durable:
                node.config['durable'] = {'replicas': [
                    {'id': peer.config['node_id'], 'address': peer.config['advertise']}
                    for peer in [*self.fleet.brokers[:2], node]]}
            node.spawn()
            wait_until(lambda: node.process.poll() is not None, 30)
            self.assertNotEqual(node.process.returncode, 0,
                                'a mismatched member must not become an active Broker')
        # Refused membership cannot rewrite the accepted group's metadata or make
        # its data-plane unusable.
        self.healthy(0)
        self.cross_roundtrip('membership-still-valid')

    def test_source_backlog_recovers_without_source_client_reconnect(self):
        target = self.persistent('backlog-target')
        self.subscribe(target, 'durable/backlog', 2)
        self.disconnect(target)
        stopped = self.fleet.brokers[0]
        os.kill(stopped.process.pid, signal.SIGSTOP)
        try:
            source = self.persistent('backlog-source', 2)
            # W2 acceptance while the destination is paused must not wait for its
            # socket delivery. The remaining two independent disks own the intent.
            self.publish(source, 'durable/backlog', b'accepted durable responsibility', 2)
            self.lose_owner(2)
        finally:
            os.kill(stopped.process.pid, signal.SIGCONT)
        resumed = self.persistent('backlog-target', 1, present=True, retry=True)
        message = self.expect_message(resumed, 'durable/backlog', b'accepted durable responsibility', 2)
        self.complete_message(resumed, message)
        self.quiet(resumed, .4)

    def test_host_backlog_survives_source_death_and_restarted_host_accepts_new_work(self):
        target = self.persistent('host-backlog-target')
        self.subscribe(target, 'durable/host-backlog', 1)
        self.disconnect(target)
        stopped = self.fleet.brokers[0]
        os.kill(stopped.process.pid, signal.SIGSTOP)
        try:
            status, value = self.fleet.brokers[2].request('/api/v1/mqtt/publish', method='POST',
                body={'topic': 'durable/host-backlog', 'payload': 'accepted host backlog', 'qos': 1})
            self.assertEqual((status, value), (202, {'accepted': True}))
            self.lose_owner(2)
        finally:
            os.kill(stopped.process.pid, signal.SIGCONT)
        resumed = self.persistent('host-backlog-target', 1, present=True, retry=True)
        message = self.expect_message(resumed, 'durable/host-backlog', b'accepted host backlog', 1)
        self.complete_message(resumed, message)
        self.fleet.brokers[2].start()
        self.healthy(2)
        status, value = self.fleet.brokers[2].request('/api/v1/mqtt/publish', method='POST',
            body={'topic': 'durable/host-backlog', 'payload': 'new host incarnation', 'qos': 1})
        self.assertEqual((status, value), (202, {'accepted': True}))
        message = self.expect_message(resumed, 'durable/host-backlog', b'new host incarnation', 1)
        self.complete_message(resumed, message)
        self.quiet(resumed, .4)

    def test_recipient_crash_preserves_receipt_and_unacknowledged_delivery(self):
        target = self.persistent('receipt-target')
        self.subscribe(target, 'durable/receipt', 2)
        source = self.persistent('receipt-source', 2)
        self.send_publish(source, 'durable/receipt', b'one recipient exchange', 2, 55)
        self.expect_ack(source, 0x50, 55)
        source.sendall(ack(0x62, 55))
        initial = self.expect_message(target, 'durable/receipt', b'one recipient exchange', 2)
        self.lose_owner()
        resumed = self.persistent('receipt-target', 1, present=True, retry=True)
        replay = self.expect_message(resumed, 'durable/receipt', b'one recipient exchange', 2)
        self.assertEqual(replay[:4], initial[:4])
        self.assertTrue(replay[4])
        self.complete_message(resumed, replay)
        # Drain the original completion, then retransmit PUBREL. This must not
        # create another recipient exchange after its receipt has been migrated.
        self.expect_ack(source, 0x70, 55)
        source.sendall(ack(0x62, 55))
        self.expect_ack(source, 0x70, 55, completed=True)
        self.quiet(resumed, .4)

    def test_rf1_refuses_new_durable_connack_and_publication_success(self):
        source = self.persistent('quorum-source')
        target = self.persistent('quorum-target')
        self.subscribe(target, 'durable/quorum', 1)
        self.lose_owner(1)
        self.lose_owner(2)
        self.send_publish(source, 'durable/quorum', b'must not acknowledge', 1, 91)
        try:
            header, data = receive(source)
        except socket.timeout:
            self.fail('RF1 mutation must fail within the finite socket deadline')
        except (EOFError, OSError):
            pass
        else:
            self.assertNotEqual(header, 0x40, 'RF1 must not PUBACK a durable mutation')
            self.assertEqual(header, 0xe0, data)
            self.assertGreaterEqual(data[0], 0x80)
        sock = self.raw()
        sock.sendall(self.packet('rf1-new', clean=False))
        try:
            header, data = receive(sock)
        except socket.timeout:
            self.fail('RF1 CONNECT must fail within the finite socket deadline')
        except (EOFError, OSError):
            pass
        else:
            self.assertEqual(header, 0x20)
            self.assertGreaterEqual(data[1], 0x80, 'RF1 must not acknowledge durable CONNECT')

    def test_stale_returning_owner_does_not_overwrite_migrated_session(self):
        target = self.persistent('epoch-device')
        self.subscribe(target, 'durable/old', 1)
        self.lose_owner()
        moved = self.persistent('epoch-device', 1, present=True, retry=True)
        self.subscribe(moved, 'durable/new', 1)
        self.fleet.brokers[0].start()
        self.healthy(0)
        source = self.client('epoch-publisher', 2)
        self.publish(source, 'durable/new', b'new epoch subscription', 1)
        message = self.expect_message(moved, 'durable/new', b'new epoch subscription', 1)
        self.complete_message(moved, message)
        self.disconnect(moved)
        self.lose_owner(1)
        again = self.persistent('epoch-device', 0, present=True, retry=True)
        self.publish(source, 'durable/new', b'new epoch survived old owner', 1, packet_id=18)
        message = self.expect_message(again, 'durable/new', b'new epoch survived old owner', 1)
        self.complete_message(again, message)

    def test_whole_fleet_crash_recovers_replicated_offline_queue(self):
        target = self.persistent('fleet-device')
        self.subscribe(target, 'durable/fleet', 2)
        self.disconnect(target)
        source = self.persistent('fleet-source', 2)
        self.publish(source, 'durable/fleet', b'quorum durable before crash', 2)
        for node in [*self.fleet.brokers, *self.fleet.controllers]:
            node.stop(crash=True)
        for node in self.fleet.controllers:
            node.start()
        wait_until(lambda: (self.fleet.status() or {}).get('leader'), 30)
        for node in self.fleet.brokers:
            node.start()
        resumed = self.persistent('fleet-device', 1, present=True, retry=True)
        message = self.expect_message(resumed, 'durable/fleet', b'quorum durable before crash', 2)
        self.complete_message(resumed, message)
        self.quiet(resumed, .4)

    def test_cross_version_recovery_preserves_outgoing_packet_identifier(self):
        target = self.persistent('version-device', version=4)
        # Explicit legacy SUBSCRIBE because this fixture normally emits MQTT5.
        from blackbox import string
        target.sendall(frame(0x82, b'\x00\x01' + string('durable/version') + b'\x01'))
        self.assertEqual(receive(target), (0x90, b'\x00\x01\x01'))
        source = self.client('version-source', 2)
        self.publish(source, 'durable/version', b'version independent session', 1)
        original = parse_publish(receive(target))
        self.lose_owner()
        resumed = self.persistent('version-device', 1, version=5, present=True, retry=True)
        replay = self.expect_message(resumed, 'durable/version', b'version independent session', 1)
        self.assertEqual(replay[:4], original[:4])
        self.assertTrue(replay[4])
        self.complete_message(resumed, replay)

    def test_session_expiry_is_not_reset_by_owner_failover(self):
        target = self.persistent('expiry-device', expiry=2)
        self.subscribe(target, 'durable/expiry', 1)
        self.disconnect(target)
        self.lose_owner()
        # Wall time is the expiry input; this sleep is not used as commit proof.
        time.sleep(3)
        resumed = self.persistent('expiry-device', 1, expiry=60, present=False, retry=True)
        source = self.client('expiry-source', 2)
        self.publish(source, 'durable/expiry', b'expired interest must be gone', 1)
        self.quiet(resumed)


class DurableIdentity(DurableFixture, unittest.TestCase):
    version = 5
    policy = dict(allow_anonymous=True, anonymous=dict(client_id_prefix='', publish=['#'], subscribe=['#']),
                  users=[dict(username=name, password_hash=PASSWORD_HASH,
                              client_id_prefix='', publish=['#'], subscribe=['#'])
                         for name in ['alice', 'bob']])

    def test_valid_other_username_cannot_resume_or_reset_replicated_owner(self):
        target = self.persistent('identity-device', username='alice')
        self.subscribe(target, 'durable/identity', 1)
        def denied(clean=False):
            with socket.create_connection(('127.0.0.1', self.fleet.brokers[1].mqtt), timeout=3) as sock:
                sock.settimeout(8)
                sock.sendall(self.packet('identity-device', clean=clean, username='bob'))
                header, data = receive(sock)
                self.assertEqual(header, 0x20)
                if data[1] in (0x88, 0x89):
                    return False
                self.assertEqual(data[1], 0x87, 'valid other principal must not acquire stored session')
                return True
        self.assertTrue(denied())
        self.ping(target)
        self.assertTrue(denied(clean=True))
        self.ping(target)
        self.lose_owner()
        wait_until(denied, 35)
        resumed = self.persistent('identity-device', 1, present=True, username='alice', retry=True)
        source = self.client('identity-source', 2)
        self.publish(source, 'durable/identity', b'original owner retained', 1)
        self.complete_message(resumed, self.expect_message(resumed, 'durable/identity',
                                                         b'original owner retained', 1))


if __name__ == '__main__':
    unittest.main()
