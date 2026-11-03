#!/usr/bin/env python3
"""Real embedded Raft Controller/Broker process acceptance, stdlib only.

RMQTT_BIN=/absolute/path/rmqtt python3 tests/cluster_blackbox.py -v
Every fixture owns three Controller processes, two Broker processes, ports,
metadata journals, private tokens and session/rule SQLite databases. No mocked
consensus, physical power-loss or cluster message replication claim is made.
"""
import copy
import http.client
import json
import os
from pathlib import Path
import select
import signal
import socket
import struct
import subprocess
import tempfile
import time
import unittest

from blackbox import frame, receive, string
from dashboard_blackbox import TOKEN, free_port
from mqtt5_blackbox import connect5, props, publication5, readprops
from retained_will_blackbox import connect_packet as connect311, publication
from rules_webhook_blackbox import CONFIG as RULES_CONFIG, Webhook, config_for
import dashboard_blackbox as dashboard

BINARY = os.environ.get('RMQTT_BIN', str(Path(__file__).resolve().parents[1] / 'target/debug/rmqtt'))
CLUSTER_TOKEN = 'cluster-test-secret-709def7a902445eb'


def wait_until(predicate, timeout=20):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (OSError, EOFError, http.client.HTTPException, ValueError):
            pass
        time.sleep(.04)
    raise AssertionError(f'condition not established in {timeout}s; last={last!r}')


class Node:
    def __init__(self, root, config, mqtt=None, admin=None):
        self.root = Path(root)
        self.root.mkdir()
        self.config = config
        self.path = self.root / 'cluster.json'
        self.path.write_text(json.dumps(config))
        self.mqtt, self.admin = mqtt, admin
        self.process = None
        self.output = None
        if admin is not None:
            (self.root / 'admin.token').write_text(TOKEN + '\n')

    def command(self):
        command = [BINARY, '--cluster-config', str(self.path)]
        if self.mqtt is not None:
            command += ['--listen', f'127.0.0.1:{self.mqtt}',
                        '--admin-listen', f'127.0.0.1:{self.admin}',
                        '--admin-token-file', str(self.root / 'admin.token'),
                        '--storage-path', str(self.root / 'sessions.sqlite'),
                        '--rules-config', str(self.root / 'rules.sqlite'),
                        '--access-control-config', str(self.root / 'access.sqlite'),
                        '--auth-rate-per-second', '0']
        return command

    def spawn(self):
        if self.process is not None and self.process.poll() is None:
            raise AssertionError('attempted to double-start fixture node')
        self.path.write_text(json.dumps(self.config))
        if self.output is not None:
            self.output.close()
        self.output = tempfile.TemporaryFile()
        self.process = subprocess.Popen(self.command(), cwd=self.root,
                                        stdout=self.output, stderr=subprocess.STDOUT)

    def logs(self):
        if self.output is None:
            return ''
        self.output.seek(0)
        return self.output.read().decode(errors='replace')[-12000:]

    def alive(self):
        if self.process.poll() is not None:
            raise AssertionError(f'{self.config["role"]} exited with {self.process.returncode}: {self.logs()}')

    def request(self, path, method='GET', body=None, token=TOKEN):
        connection = http.client.HTTPConnection('127.0.0.1', self.admin, timeout=6)
        headers = {}
        if token is not None:
            headers['Authorization'] = 'Bearer ' + token
        if body is not None:
            headers['Content-Type'] = 'application/json'
            body = json.dumps(body)
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            raw = response.read()
            return response.status, json.loads(raw) if raw else None
        finally:
            connection.close()

    def ready(self):
        self.alive()
        try:
            status, body = self.request('/api/v1/overview')
            return status == 200 and body.get('ready')
        except (OSError, http.client.HTTPException):
            return False

    def start(self):
        self.spawn()
        if self.admin is not None:
            try:
                wait_until(self.ready, 30)
            except Exception:
                raise AssertionError(f'Broker did not become ready: {self.logs()}') from None

    def stop(self, crash=False):
        if self.process is not None and self.process.poll() is None:
            self.process.send_signal(signal.SIGKILL if crash else signal.SIGINT)
            try:
                self.process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                raise AssertionError(f'node shutdown exceeded12s: {self.logs()}')

    def close(self):
        self.stop()
        if self.output is not None:
            self.output.close()
            self.output = None


class Fleet:
    def __init__(self, directory):
        self.root = Path(directory)
        self.used_ports = set()
        self.token = self.root / 'cluster.token'
        self.token.write_text(CLUSTER_TOKEN + '\n')
        ports = [self.port() for _ in range(3)]
        self.controllers = []
        members = [{'id': index + 1, 'address': f'http://127.0.0.1:{port}'}
                   for index, port in enumerate(ports)]
        for index, port in enumerate(ports):
            root = self.root / f'controller-{index + 1}'
            config = dict(role='controller', cluster_id='wire-test-cluster',
                          node_id=index + 1, listen=f'127.0.0.1:{port}',
                          data_dir=str(root / 'metadata'), token_file=str(self.token),
                          controllers=members, bootstrap=index == 0, lease_ms=3000)
            self.controllers.append(Node(root, config))
        self.brokers = [self.new_broker(index + 101) for index in range(2)]

    def port(self):
        while True:
            port = free_port()
            if port not in self.used_ports:
                self.used_ports.add(port)
                return port

    def new_broker(self, node_id, **changes):
        root = self.root / f'broker-{node_id}'
        port = self.port()
        config = dict(role='broker', cluster_id='wire-test-cluster', node_id=node_id,
                      listen=f'127.0.0.1:{port}', advertise=f'http://127.0.0.1:{port}',
                      data_dir=str(root / 'cluster-data'), token_file=str(self.token),
                      controllers=copy.deepcopy(self.controllers[0].config['controllers']),
                      lease_ms=3000)
        config.update(changes)
        return Node(root, config, self.port(), self.port())

    def status(self):
        result = subprocess.run([BINARY, 'cluster-status', '--cluster-config',
                                 str(self.controllers[0].path)], cwd=self.root,
                                capture_output=True, timeout=12)
        if result.returncode != 0:
            return None
        try:
            return json.loads(result.stdout)
        except ValueError:
            raise AssertionError(f'cluster-status returned invalid JSON: {result.stdout!r}, {result.stderr!r}')

    def start(self):
        for node in self.controllers:
            node.start()
        def leader_ready():
            for node in self.controllers:
                node.alive()
            return (self.status() or {}).get('leader')
        wait_until(leader_ready, 30)
        for node in self.brokers:
            node.start()

    def stop(self):
        failures = []
        for node in [*reversed(self.brokers), *reversed(self.controllers)]:
            try:
                node.close()
            except Exception as error:
                failures.append(str(error))
        if failures:
            raise AssertionError('\n'.join(failures))


class ClusterFixture:
    version = 5
    delivery = dashboard.DashboardBlackbox.delivery

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-cluster-wire-')
        self.addCleanup(self.directory.cleanup)
        self.fleet = Fleet(self.directory.name)
        self.addCleanup(self.fleet.stop)
        self.fleet.start()

    def tearDown(self):
        result = self._outcome.result
        if any(test is self for test, _ in [*result.failures, *result.errors]):
            # Temp directories are removed by cleanup. Preserve finite diagnostics
            # before teardown, while never emitting the fixture credentials.
            for node in [*self.fleet.controllers, *self.fleet.brokers]:
                logs = node.logs()
                for secret in (CLUSTER_TOKEN, TOKEN, 'incorrect-private-cluster-token-abc123'):
                    logs = logs.replace(secret, '[redacted]')
                print(f'\n--- {node.config["role"]} {node.config["node_id"]} '
                      f'exit={node.process.poll()} ---\n{logs}', flush=True)

    def raw(self, node=0):
        sock = socket.create_connection(('127.0.0.1', self.fleet.brokers[node].mqtt), timeout=6)
        sock.settimeout(8)
        self.addCleanup(sock.close)
        return sock

    def packet(self, client, **kwargs):
        return connect5(client, **kwargs) if self.version == 5 else connect311(client, **kwargs)

    def client(self, client, node=0):
        sock = self.raw(node)
        sock.sendall(self.packet(client))
        header, data = receive(sock)
        self.assertEqual(header, 0x20)
        self.assertEqual(data[:2], b'\x00\x00', data)
        if self.version == 5:
            self.assertEqual(readprops(data, 2)[1], len(data))
        return sock

    def subscribe(self, sock, topic, qos=2, expected=None):
        data = b'\x00\x01' + (props() if self.version == 5 else b'')
        sock.sendall(frame(0x82, data + string(topic) + bytes([qos])))
        header, data = receive(sock)
        self.assertEqual(header, 0x90)
        offset = readprops(data, 2)[1] if self.version == 5 else 2
        self.assertEqual(data[offset:], bytes([qos if expected is None else expected]), data)

    def publish(self, sock, topic, payload=b'body', qos=1, packet_id=7, retain=False):
        packet = (publication5(topic, payload, qos, packet_id, retain=retain)
                  if self.version == 5 else publication(topic, payload, qos, packet_id, retain))
        sock.sendall(packet)
        if qos:
            header, data = receive(sock)
            self.assertEqual(header, 0x40 if qos == 1 else 0x50, data)
            self.assertEqual(data[:2], struct.pack('!H', packet_id))
            if self.version == 5 and len(data) > 2:
                self.assertEqual(data[2], 0)
            if qos == 2:
                sock.sendall(frame(0x62, struct.pack('!H', packet_id)))
                header, data = receive(sock)
                self.assertEqual(header, 0x70, data)
                self.assertEqual(data[:2], struct.pack('!H', packet_id))

    def ping(self, sock):
        sock.sendall(frame(0xc0))
        self.assertEqual(receive(sock), (0xd0, b''))

    def quiet(self, sock, timeout=.2):
        self.assertFalse(select.select([sock], [], [], timeout)[0], 'unexpected MQTT output')

    def wait_broker_recovery(self):
        # A new Raft leader is observable before watches and node leases converge.
        # Verify actual data-plane admission with a bounded recovery allowance.
        def probe(index):
            try:
                with socket.create_connection(('127.0.0.1', self.fleet.brokers[index].mqtt), timeout=3) as sock:
                    sock.settimeout(8)
                    sock.sendall(self.packet(f'recovery-probe-{index}'))
                    header, data = receive(sock)
                    if header != 0x20 or data[:2] != b'\x00\x00':
                        return False
                    sock.sendall(frame(0xc0))
                    if receive(sock) != (0xd0, b''):
                        return False
                    sock.sendall(frame(0xe0))
                    return True
            except (OSError, EOFError):
                return False
        for index in range(len(self.fleet.brokers)):
            wait_until(lambda index=index: probe(index), 30)

    def cross_roundtrip(self, suffix='one'):
        subscriber = self.client('subscriber-' + suffix, 1)
        self.subscribe(subscriber, 'cluster/' + suffix)
        publisher = self.client('publisher-' + suffix, 0)
        self.publish(publisher, 'cluster/' + suffix, suffix.encode())
        self.delivery(subscriber, 'cluster/' + suffix, suffix.encode(), 1, self.version)


class ClusterWireCases(ClusterFixture):
    def test_cross_node_qos_zero_one_two_and_order(self):
        subscriber = self.client('subscriber', 1)
        self.subscribe(subscriber, 'cluster/ordered')
        publisher = self.client('publisher', 0)
        for qos in (0, 1, 2):
            for serial in range(3):
                payload = f'{qos}:{serial}'.encode()
                self.publish(publisher, 'cluster/ordered', payload, qos, 10 + serial)
                self.delivery(subscriber, 'cluster/ordered', payload, qos, self.version)

    def test_suback_is_route_visibility_barrier_and_unsubscribe_removes_interest(self):
        publisher = self.client('publisher', 0)
        subscriber = self.client('subscriber', 1)
        self.subscribe(subscriber, 'cluster/new')
        self.publish(publisher, 'cluster/new', b'immediate')
        self.delivery(subscriber, 'cluster/new', b'immediate', 1, self.version)
        body = b'\x00\x02' + (props() if self.version == 5 else b'') + string('cluster/new')
        subscriber.sendall(frame(0xa2, body))
        header, _ = receive(subscriber)
        self.assertEqual(header, 0xb0)
        self.publish(publisher, 'cluster/new', b'after-unsubscribe')
        self.ping(subscriber)
        self.quiet(subscriber)

    def test_global_takeover_and_stale_cleanup_preserve_replacement(self):
        old = self.client('global-client', 0)
        replacement = self.client('global-client', 1)
        if self.version == 5:
            header, data = receive(old)
            self.assertEqual(header, 0xe0)
            self.assertEqual(data[0], 0x8e)
        else:
            self.assertEqual(old.recv(1), b'')
        old.close()
        self.ping(replacement)
        self.subscribe(replacement, 'cluster/takeover')
        source = self.client('source', 0)
        self.publish(source, 'cluster/takeover', b'new-owner')
        self.delivery(replacement, 'cluster/takeover', b'new-owner', 1, self.version)
        self.ping(replacement)

    def test_http_downlink_routes_to_remote_subscriber(self):
        subscriber = self.client('device', 1)
        self.subscribe(subscriber, 'cluster/http')
        status, value = self.fleet.brokers[0].request('/api/v1/mqtt/publish', method='POST',
            body={'topic': 'cluster/http', 'payload': 'server-command', 'qos': 2})
        self.assertEqual((status, value), (202, {'accepted': True}))
        self.delivery(subscriber, 'cluster/http', b'server-command', 2, self.version)

    def test_forwarding_does_not_reflect_or_repeat_source_rules(self):
        webhook = Webhook()
        self.addCleanup(webhook.close)
        value = config_for(webhook.url, sql='SELECT payload AS payload FROM "cluster/rules"', batch_wait_ms=0)
        for node in self.fleet.brokers:
            status, result = node.request(RULES_CONFIG, method='PUT', body=value)
            self.assertEqual(status, 200, result)
        subscriber = self.client('subscriber', 1)
        self.subscribe(subscriber, 'cluster/rules')
        publisher = self.client('publisher', 0)
        self.publish(publisher, 'cluster/rules', b'{"number":1}')
        self.delivery(subscriber, 'cluster/rules', b'{"number":1}', 1, self.version)
        _, _, body, _ = webhook.get()
        self.assertEqual(len(body['events']), 1)
        for index, node in enumerate(self.fleet.brokers):
            status, counters = node.request('/api/v1/rules/status')
            self.assertEqual(status, 200)
            self.assertEqual(counters['received'], 1 if index == 0 else 0)
        self.quiet(subscriber)

    def test_broker_restart_rebuilds_route_snapshot(self):
        subscriber = self.client('subscriber', 1)
        self.subscribe(subscriber, 'cluster/after-restart')
        self.fleet.brokers[0].stop(crash=True)
        self.fleet.brokers[0].start()
        publisher = self.client('publisher-after-restart', 0)
        self.publish(publisher, 'cluster/after-restart', b'caught-up')
        self.delivery(subscriber, 'cluster/after-restart', b'caught-up', 1, self.version)

    def test_initial_cluster_feature_limits_are_explicit(self):
        persistent = {'clean': False}
        if self.version == 5:
            persistent['properties'] = [(0x11, 60)]
        for name, options in [('with-will', {'will': ('cluster/will', b'will', 1, False)}),
                              ('persistent', persistent)]:
            with self.subTest(feature=name):
                sock = self.raw()
                sock.sendall(self.packet(name, **options))
                header, data = receive(sock)
                self.assertEqual(header, 0x20)
                self.assertEqual(data[1], 0x83 if self.version == 5 else 3)
        if self.version == 5:
            # CleanStart=false is independent of persistent expiry. With expiry0
            # this remains an ordinary transient session and must not be refused.
            transient = self.raw()
            transient.sendall(self.packet('transient-resume', clean=False))
            header, data = receive(transient)
            self.assertEqual((header, data[:2]), (0x20, b'\x00\x00'))
            self.ping(transient)
        subscriber = self.client('subscriber', 1)
        self.subscribe(subscriber, '$share/group/cluster/shared', expected=0x9e if self.version == 5 else 0x80)
        publisher = self.client('retained-publisher', 0)
        packet = (publication5('cluster/retained', b'forbidden', 1, 9, retain=True)
                  if self.version == 5 else publication('cluster/retained', b'forbidden', 1, 9, True))
        publisher.sendall(packet)
        if self.version == 5:
            header, data = receive(publisher)
            self.assertEqual(header, 0xe0)
            self.assertEqual(data[0], 0x9a)
        else:
            self.assertEqual(publisher.recv(1), b'')


class Cluster311(ClusterWireCases, unittest.TestCase):
    version = 4


class Cluster5(ClusterWireCases, unittest.TestCase):
    version = 5


class ClusterFaults(ClusterFixture, unittest.TestCase):
    def test_actual_raft_leader_failure_elects_replacement_and_routes(self):
        self.cross_roundtrip('before-election')
        before = wait_until(lambda: self.fleet.status())
        leader = before['leader']
        node = next(node for node in self.fleet.controllers if node.config['node_id'] == leader)
        node.stop(crash=True)
        wait_until(lambda: (state if (state := self.fleet.status()) and state.get('leader') not in (None, leader) else None), 20)
        self.wait_broker_recovery()
        self.cross_roundtrip('after-election')
        node.start()
        self.cross_roundtrip('after-return')

    def test_unresponsive_leader_does_not_strand_metadata_watch(self):
        self.cross_roundtrip('before-paused-leader')
        state = wait_until(lambda: self.fleet.status())
        leader = state['leader']
        node = next(node for node in self.fleet.controllers if node.config['node_id'] == leader)
        # Unlike SIGKILL, SIGSTOP preserves the old leader's TCP watch streams.
        # Healthy peers must still elect a leader and Brokers must refresh routes.
        os.kill(node.process.pid, signal.SIGSTOP)
        try:
            wait_until(lambda: (current if (current := self.fleet.status()) and
                                current.get('leader') not in (None, leader) else None), 25)
            self.wait_broker_recovery()
            self.cross_roundtrip('while-old-leader-unresponsive')
        finally:
            os.kill(node.process.pid, signal.SIGCONT)
        self.cross_roundtrip('after-old-leader-resumed')

    def test_loss_of_majority_expires_broker_service_then_recovers(self):
        self.cross_roundtrip('before-majority-loss')
        state = wait_until(lambda: self.fleet.status())
        leader = state['leader']
        selected = sorted(self.fleet.controllers, key=lambda n: n.config['node_id'] != leader)[:2]
        for node in selected:
            node.stop(crash=True)
        # The configured3s node lease plus transport scheduling allowance. This is
        # a fault-injection deadline, not a measured failover SLO.
        time.sleep(4)
        status, _ = self.fleet.brokers[0].request('/api/v1/mqtt/publish', method='POST',
            body={'topic': 'cluster/unavailable', 'payload': 'must-not-succeed'})
        self.assertEqual(status, 503)
        sock = self.raw()
        sock.sendall(connect5('not-admitted'))
        header, data = receive(sock)
        self.assertEqual(header, 0x20)
        self.assertEqual(data[1], 0x88)
        for node in selected:
            node.start()
        wait_until(lambda: (self.fleet.status() or {}).get('leader'), 20)
        self.wait_broker_recovery()
        self.cross_roundtrip('after-majority-return')

    def test_controller_journals_recover_after_total_process_crash(self):
        self.cross_roundtrip('before-controller-crash')
        before = wait_until(lambda: self.fleet.status())
        baseline = before['last_applied']
        if isinstance(baseline, dict):
            baseline = baseline['index']
        for node in self.fleet.controllers:
            node.stop(crash=True)
        time.sleep(4)
        for node in self.fleet.controllers:
            node.start()
        def restored():
            state = self.fleet.status()
            if not state or not state.get('leader'):
                return None
            applied = state['last_applied']
            if isinstance(applied, dict):
                applied = applied['index']
            return state if applied >= baseline else None
        wait_until(restored, 25)
        self.wait_broker_recovery()
        self.cross_roundtrip('after-controller-crash')

    def test_expired_broker_rejoins_without_process_restart(self):
        self.cross_roundtrip('before-broker-lease-expiry')
        old = self.client('lease-expired-owner', 0)
        node = self.fleet.brokers[0]
        os.kill(node.process.pid, signal.SIGSTOP)
        try:
            wait_until(lambda: (state if (state := self.fleet.status()) and
                                state.get('nodes') == 1 else None), 20)
        finally:
            os.kill(node.process.pid, signal.SIGCONT)
        self.wait_broker_recovery()
        try:
            header, data = receive(old)
        except EOFError:
            pass
        else:
            self.assertEqual(header, 0xe0)
            # Revocation can race the transport's service shutdown path. Both
            # reasons explicitly end service; accepting an open socket is invalid.
            self.assertIn(data[0], (0x88, 0x8b))
        self.cross_roundtrip('after-broker-lease-rejoin')

    def test_wrong_cluster_identity_and_private_token_cannot_register(self):
        wrong_token = self.fleet.root / 'wrong.token'
        wrong_token.write_text('incorrect-private-cluster-token-abc123\n')
        for index, changes in enumerate([{'cluster_id': 'other-cluster'}, {'token_file': str(wrong_token)}]):
            node = self.fleet.new_broker(200 + index, **changes)
            self.addCleanup(node.close)
            node.spawn()
            wait_until(lambda: node.process.poll() is not None, 30)
            self.assertNotEqual(node.process.returncode, 0)
            self.assertNotIn(CLUSTER_TOKEN, node.logs())
            self.assertNotIn('incorrect-private-cluster-token-abc123', node.logs())


if __name__ == '__main__':
    unittest.main()
