#!/usr/bin/env python3
"""Real process tests for volatile RF3/W2 replicas, not disk crash recovery.

RMQTT_BIN=/path/rmqtt python3 tests/cluster_memory_blackbox.py -v
Controllers persist metadata; data replicas must keep session/message state only
in RAM. Empty restarted replicas cannot vote until in-process repair completes.
"""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import unittest
from unittest import mock
import uuid

from blackbox import frame, receive
from cluster_blackbox import BINARY, CLUSTER_TOKEN, TOKEN, wait_until
from cluster_durable_blackbox import DurableFixture, DurableFleet, DurableNode, DurableSessionCases
from cluster_repair_proxy import Driver, ImportProxy


class MemoryNode(DurableNode):
    """Replicated mode already omits a session SQLite mirror."""


class MemoryFleet(DurableFleet):
    node_class = MemoryNode


class CombinedMemoryNode(MemoryNode):
    def __init__(self, broker, controller):
        self.__dict__.update(broker.__dict__)
        self.controller = controller
        self.role = 'controller,broker'
        self.config = dict(self.config)
        self.config['role'] = 'controller,broker'
        self.write_config()


class CombinedMemoryFleet(MemoryFleet):
    def __init__(self, directory):
        super().__init__(directory)
        self.brokers = [CombinedMemoryNode(broker, control)
                        for broker, control in zip(self.brokers, self.controllers)]

    def start(self):
        for node in self.brokers:
            node.spawn()
        wait_until(lambda: (self.status() or {}).get('nodes') == 3 and
                   all(node.ready() for node in self.brokers), 35)

    def stop(self):
        for node in reversed(self.brokers):
            node.close()


class MemoryFixture(DurableFixture):
    fleet_class = MemoryFleet
    version = 5

    def setUp(self):
        self.environment = mock.patch.dict(os.environ,
                                           TOKIO_WORKER_THREADS='2', RUST_LOG='warn')
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-memory-wire-')
        self.addCleanup(self.directory.cleanup)
        self.fleet = self.fleet_class(self.directory.name)
        self.addCleanup(self.fleet.stop)
        self.fleet.start()

    def tearDown(self):
        result = self._outcome.result
        if any(test is self for test, _ in [*result.failures, *result.errors]):
            for node in [*self.fleet.controllers, *self.fleet.brokers]:
                logs = node.logs()
                for secret in (CLUSTER_TOKEN, TOKEN):
                    logs = logs.replace(secret, '[redacted]')
                code = node.process.poll() if node.process is not None else None
                print(f'{node.config["role"]} {node.config["node_id"]} exit={code}:\n{logs}',
                      flush=True)

    def assert_memory_status(self):
        state = self.fleet.status()
        self.assertIsNotNone(state)
        self.assertEqual(state.get('data_storage'), 'memory', state)
        self.assertEqual(state.get('nodes'), 3, state)
        return state

    def data_files(self):
        result = {}
        for node in self.fleet.brokers:
            directory = Path(node.config['data_dir'])
            replica = directory / 'broker'
            if replica.is_dir():
                directory = replica
            self.assertFalse((directory / 'data.sqlite').exists(), directory)
            for path in directory.rglob('*'):
                self.assertFalse(path.is_symlink(), 'replica identity files must not be aliases')
                if path.is_file():
                    self.assertIn(path.relative_to(directory).as_posix(),
                                  ('data.lock', 'memory.identity'),
                                  f'payload-bearing data file is forbidden: {path}')
                    self.assertLessEqual(path.stat().st_size, 65536,
                                         f'only small identity/lock files allowed: {path}')
                    result[str(path)] = (path.read_bytes(), path.stat().st_mtime_ns)
        return result

    def ring(self):
        self.assert_memory_status()
        before = self.data_files()
        marker = ('RAM-ONLY-' + uuid.uuid4().hex).encode()
        for version in (5, 4):
            self.version = version
            for source in range(3):
                target = (source + 1) % 3
                sub = self.client(f'ram-s-{version}-{source}', target, expiry=0)
                pub = self.client(f'ram-p-{version}-{source}', source, expiry=0)
                for qos in (0, 1, 2):
                    topic = f'ram/{version}/{source}/{qos}'
                    payload = marker + f':{version}:{source}:{qos}'.encode()
                    self.subscribe(sub, topic, qos)
                    self.publish(pub, topic, payload, qos, packet_id=20 + qos)
                    message = self.expect_message(sub, topic, payload, qos)
                    self.complete_message(sub, message)
                    self.quiet(sub, .025)
                self.disconnect(pub)
                self.disconnect(sub)
        after = self.data_files()
        self.assertEqual(before, after, 'data replica files changed during message handling')
        self.assertTrue(all(marker not in body for body, _ in after.values()))
        # This is specifically data-plane RAM; persisted Raft metadata is allowed.
        directories = []
        for node in self.fleet.brokers:
            combined = Path(node.root) / 'controller'
            if (combined / 'controller.bbolt').is_file():
                directories.append(combined)
        if not directories:
            directories = [Path(node.config['data_dir']) for node in self.fleet.controllers]
        for directory in directories:
            self.assertTrue((directory / 'controller.bbolt').is_file(), directory)
            self.assertFalse((directory / 'metadata.sqlite').exists())

    def peer_listening(self, node):
        host, port = node.config['listen'].rsplit(':', 1)
        def listening():
            node.alive()
            try:
                with socket.create_connection((host, int(port)), timeout=.15):
                    return True
            except OSError:
                return False
        wait_until(listening, 12)

    def no_positive_connack(self, node, timeout=.35):
        try:
            sock = socket.create_connection(('127.0.0.1', node.mqtt), timeout=timeout)
        except (ConnectionError, socket.timeout):
            return
        with sock:
            sock.settimeout(timeout)
            sock.sendall(self.packet('cannot-use-blank-ram', clean=False))
            try:
                header, data = receive(sock)
            except (EOFError, ConnectionError, socket.timeout):
                return
            self.assertIn(header, (0x20, 0xe0), (header, data))
            if header == 0x20:
                self.assertGreaterEqual(len(data), 2, data)
                self.assertNotEqual(data[1], 0,
                                    'an erased RAM replica produced a successful CONNACK')

    def repair(self, target=101, abort=False, expected=None):
        command = [BINARY, 'cluster-repair-abort' if abort else 'cluster-repair',
                   '--config', str(self.fleet.controllers[0].path)]
        if not abort:
            command += ['--target-node', str(target)]
        completed = subprocess.run(command, cwd=self.fleet.root, capture_output=True,
                                   text=True, timeout=90)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        reports = []
        for line in completed.stdout.splitlines():
            try:
                reports.append(json.loads(line))
            except ValueError:
                pass
        self.assertTrue(reports, completed.stdout)
        report = reports[-1]
        self.assertEqual(report['state'], 'aborted' if abort else 'committed', report)
        if expected is not None:
            self.assertEqual(report['id'], expected, report)
        return report

    def witness(self):
        target = self.persistent('ram-witness', 1, expiry=3600)
        self.subscribe(target, 'ram/witness', 1)
        self.disconnect(target)
        source = self.client('ram-witness-source', 2, expiry=0)
        self.publish(source, 'ram/witness', b'accepted in live memory replicas', 1)
        self.disconnect(source)

    def replacement(self, proxy_mode=None):
        target = self.fleet.brokers[0]
        target.stop(crash=True)
        wait_until(lambda: (self.fleet.status() or {}).get('nodes') == 2, 20)
        proxy = None
        if proxy_mode is not None:
            backend = self.fleet.port()
            target.broker_listen_override = f'{target.host}:{backend}'
            target.config['listen'] = target.broker_listen_override
            proxy = ImportProxy(
                self.fleet.broker_port, backend, proxy_mode,
                host=target.host, backend_host=target.host)
            self.addCleanup(proxy.close)
        # Same data_dir, new process, new RAM incarnation. Never stop a loaded
        # learner and assume the following Broker process can reuse its memory.
        target.spawn()
        self.peer_listening(target)
        self.assertFalse(target.ready())
        self.no_positive_connack(target)
        return target, proxy


class MemoryProtocol(MemoryFixture, unittest.TestCase):
    def test_both_versions_all_qos_cross_nodes_without_data_files(self):
        self.ring()


class MemoryCombined(MemoryFixture, unittest.TestCase):
    fleet_class = CombinedMemoryFleet

    def test_three_combined_processes_both_versions_all_qos(self):
        self.assertEqual(len({node.process.pid for node in self.fleet.brokers}), 3)
        self.ring()


class MemoryLiveSessionCases(MemoryFixture):
    outgoing_recovery = DurableSessionCases.outgoing_recovery

    def test_offline_queue_survives_one_owner_loss_with_two_live_replicas(self):
        DurableSessionCases.test_offline_subscriptions_and_qos_survive_permanent_owner_loss(self)

    def test_qos1_inflight_packet_id_restores_from_surviving_memory(self):
        self.outgoing_recovery('ack', 1)

    def test_qos2_inflight_pubrel_restores_from_surviving_memory(self):
        self.outgoing_recovery('comp', 2)

    def test_qos2_incoming_exchange_resumes_once_from_live_majority(self):
        DurableSessionCases.test_incoming_pubrec_resumes_elsewhere_and_duplicate_pubrel_routes_once(self)


class Memory311(MemoryLiveSessionCases, unittest.TestCase):
    version = 4


class Memory5(MemoryLiveSessionCases, unittest.TestCase):
    version = 5


class MemoryRecovery(MemoryFixture, unittest.TestCase):
    def test_empty_restarted_replica_cannot_vote_then_repairs_in_same_process(self):
        self.witness()
        target, _ = self.replacement()
        pid = target.process.pid
        donor = self.fleet.brokers[2]
        os.kill(donor.process.pid, signal.SIGSTOP)
        try:
            # The blank target plus the sole responding original must not form W2.
            self.no_positive_connack(self.fleet.brokers[1], timeout=7)
        finally:
            os.kill(donor.process.pid, signal.SIGCONT)
        self.healthy(2)
        self.repair()
        wait_until(target.ready, 35)
        self.assertEqual(target.process.pid, pid, 'promotion must retain the imported RAM')
        self.assert_memory_status()
        donor.stop(crash=True)
        resumed = self.persistent('ram-witness', 0, present=True, expiry=3600, retry=True)
        self.complete_message(resumed, self.expect_message(
            resumed, 'ram/witness', b'accepted in live memory replicas', 1))
        self.quiet(resumed, .2)
        self.data_files()

    def test_pending_replacement_survives_startup_timeout_and_sigint_exits(self):
        target, _ = self.replacement()
        # The old transport open deadline was 5s. A replacement must keep serving
        # private repair RPC while waiting, without accidentally opening MQTT.
        time.sleep(5.5)
        target.alive()
        self.assertFalse(target.ready())
        self.no_positive_connack(target)
        target.process.send_signal(signal.SIGINT)
        try:
            code = target.process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            self.fail('SIGINT did not cancel pending memory initialization within 8s')
        self.assertEqual(code, 0, target.logs())

    def test_whole_group_restart_cannot_fabricate_recovery_from_control_metadata(self):
        self.witness()
        for node in self.fleet.brokers:
            node.stop(crash=True)
        wait_until(lambda: (self.fleet.status() or {}).get('nodes') == 0, 20)
        for node in self.fleet.brokers:
            node.spawn()
        for node in self.fleet.brokers:
            self.peer_listening(node)
        # All old leases expired before restart; this is not only a startup race.
        time.sleep(.5)
        self.assertEqual((self.fleet.status() or {}).get('nodes'), 0)
        for node in self.fleet.brokers:
            node.alive()
            self.assertFalse(node.ready())
            self.no_positive_connack(node)
        failed = subprocess.run([BINARY, 'cluster-repair', '--config',
                                 str(self.fleet.controllers[0].path), '--target-node', '101'],
                                cwd=self.fleet.root, capture_output=True, timeout=30)
        self.assertNotEqual(failed.returncode, 0,
                            'empty RAM replicas fabricated two original donor proofs')
        self.data_files()

    def test_target_restart_loses_staging_and_requires_new_repair(self):
        self.witness()
        target, proxy = self.replacement('installed')
        command = [BINARY, 'cluster-repair', '--config',
                   str(self.fleet.controllers[0].path), '--target-node', '101']
        driver = Driver(command, self.fleet.root)
        self.addCleanup(driver.close)
        self.assertTrue(proxy.blocked.wait(25), driver.logs())
        state = self.fleet.status() or {}
        active = state.get('repair') or {}
        self.assertTrue(active.get('sealed'), state)
        old_id = active['id']
        target.stop(crash=True)
        target.spawn()
        self.peer_listening(target)
        proxy.release.set()
        code, _ = driver.wait(timeout=80)
        self.assertNotEqual(code, 0, 'old proof committed after learner RAM vanished')
        self.assertFalse(target.ready())
        self.assertEqual(((self.fleet.status() or {}).get('repair') or {}).get('id'), old_id)
        self.repair(abort=True, expected=old_id)
        report = self.repair()
        self.assertNotEqual(report['id'], old_id)
        wait_until(target.ready, 35)
        self.assert_memory_status()


class MemoryStartup(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-memory-startup-')
        self.addCleanup(self.directory.cleanup)
        self.fleet = MemoryFleet(self.directory.name)
        self.addCleanup(self.fleet.stop)
        for node in [*self.fleet.controllers, *self.fleet.brokers]:
            node.write_config()

    def invoke(self, node, extra=(), command=None):
        args = node.command() + list(extra) if command is None else command
        return subprocess.run(args, cwd=node.root, capture_output=True, timeout=12)

    def test_legacy_data_sqlite_refused_without_deletion_or_import(self):
        node = self.fleet.brokers[0]
        directory = Path(node.config['data_dir'])
        directory.mkdir()
        old = directory / 'data.sqlite'
        sentinel = b'legacy SQLite must stay untouched'
        old.write_bytes(sentinel)
        result = self.invoke(node)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(old.read_bytes(), sentinel)
        self.assertIn(b'data.sqlite', result.stderr)

    def test_cluster_session_sqlite_mirror_is_refused(self):
        node = self.fleet.brokers[0]
        mirror = node.root / 'must-not-create.sqlite'
        result = self.invoke(node, ['--storage-path', str(mirror)])
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(mirror.exists())

    def test_pure_learner_command_cannot_promote_into_another_process(self):
        node = self.fleet.brokers[0]
        result = self.invoke(node, command=[BINARY, 'cluster-repair-learner',
                                           '--config', str(node.path)])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'memory', result.stderr.lower())


if __name__ == '__main__':
    unittest.main()
