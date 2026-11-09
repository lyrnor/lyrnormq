#!/usr/bin/env python3
"""Phase7c/7f repair maintenance on live memory replicas, real processes.

LYRNORMQ_BIN=/absolute/path/lyrnormq python3 tests/cluster_repair_blackbox.py -v
Memory replacements keep a normal Broker process waiting for operator
cluster-repair. Pure cluster-repair-learner is refused; disk-learner then
Broker reuse is not a memory path. A transparent HTTP/2 DATA-frame proxy can
hold import RPCs while the test kills a process or corrupts a digest.
"""
import concurrent.futures
import json
import os
import signal
import socket
import struct
import subprocess
import time
import unittest

from blackbox import frame, receive
from cluster_blackbox import BINARY, wait_until
from cluster_memory_blackbox import MemoryFixture
from cluster_repair_proxy import Driver, ImportProxy, exact
from qos_blackbox import ack


class RepairFixture(MemoryFixture):
    """Crash → waiting normal Broker → cluster-repair against that live process."""

    version = 5

    def setUp(self):
        super().setUp()
        self.target = self.fleet.brokers[0]
        self.proxy = None
        self.drivers = []

    def command(self, abort=False):
        command = [BINARY, 'cluster-repair-abort' if abort else 'cluster-repair',
                   '--config', str(self.fleet.controllers[0].path)]
        if not abort:
            command += ['--target-node', str(self.target.config['node_id'])]
        return command

    def driver(self, abort=False):
        result = Driver(self.command(abort), self.fleet.root)
        self.drivers.append(result)
        self.addCleanup(result.close)
        return result

    def repair_state(self):
        return self.fleet.status() or {}

    def run_repair(self, expected=None, abort=False, retry=False):
        deadline = time.monotonic() + (60 if retry else 0)
        while True:
            driver = self.driver(abort)
            code, output = driver.wait()
            if code == 0:
                for line in reversed(output.splitlines()):
                    try:
                        report = json.loads(line)
                    except ValueError:
                        continue
                    if isinstance(report, dict):
                        break
                else:
                    self.fail(f'CLI succeeded without JSON report: {output}')
                self.assertEqual(report['state'], 'aborted' if abort else 'committed', report)
                self.assertEqual(report['target'], self.target.config['node_id'])
                if expected is not None:
                    self.assertEqual(report['id'], expected, 'resume silently created a different repair')
                return report
            if not retry or time.monotonic() >= deadline:
                self.fail(f'repair CLI exited{code}: {output}')
            time.sleep(.1)

    def waiting_target(self, proxy_mode=None, wrong_token=False):
        """Replace a crashed Broker with a live process waiting for repair."""
        self.target.stop(crash=True)
        wait_until(lambda: (self.fleet.status() or {}).get('nodes') == 2, 25)
        self.proxy = None
        if proxy_mode is not None:
            backend = self.fleet.port()
            self.target.broker_listen_override = f'{self.target.host}:{backend}'
            self.target.config['listen'] = self.target.broker_listen_override
            self.proxy = ImportProxy(
                self.fleet.broker_port, backend, proxy_mode,
                host=self.target.host, backend_host=self.target.host)
            self.addCleanup(self.proxy.close)
        if wrong_token:
            wrong = self.target.root / 'wrong-repair.token'
            wrong.write_text('incorrect-private-repair-test-token-abcdef\n')
            self.target.config['token_file'] = str(wrong)
        # Same data_dir path, new RAM incarnation. Never promote via learner CLI.
        self.target.spawn()
        self.peer_listening(self.target)
        self.assertFalse(self.target.ready())
        self.no_positive_connack(self.target)
        return self.target.process.pid

    def promote_target(self):
        wait_until(self.target.ready, 35)
        self.healthy(0)

    def blocked(self, driver):
        self.assertIsNotNone(self.proxy)
        if not self.proxy.blocked.wait(25):
            self.fail(f'repair import frame was not observed; CLI={driver.process.poll()}: {driver.logs()}')
        state = wait_until(lambda: (s if ((s := self.repair_state()).get('repair') or {}).get('sealed')
                                   else None), 10)
        self.assertIn(self.target.config['node_id'], state['excluded'])
        return state['repair']['id']

    def no_success(self, source=None):
        if source is not None:
            try:
                self.send_publish(source, 'repair/unavailable', b'must not ACK', 1, 61)
                header, body = receive(source)
            except (EOFError, ConnectionError, BrokenPipeError):
                pass
            except socket.timeout:
                self.fail('durable operation hung instead of bounded refusal')
            else:
                self.assertEqual(header, 0xe0, (header, body))
                self.assertIn(body[0], (0x88, 0x89, 0x8b))
        with socket.create_connection(('127.0.0.1', self.fleet.brokers[1].mqtt), timeout=3) as sock:
            sock.settimeout(8)
            sock.sendall(self.packet('must-not-use-waiting-replica', clean=False))
            try:
                header, body = receive(sock)
            except (EOFError, ConnectionError):
                return
            except socket.timeout:
                self.fail('CONNACK hung instead of bounded refusal')
            self.assertEqual(header, 0x20)
            self.assertIn(body[1], (0x88, 0x89), body)

    def witness(self):
        # Match memory blackbox: rely on W2 ACK, not management queue counters.
        target = self.persistent('repair-witness', expiry=3600)
        self.subscribe(target, 'repair/witness', 1)
        self.disconnect(target)
        source = self.client('repair-witness-source', 2, expiry=0)
        self.publish(source, 'repair/witness', b'acknowledged before replacement', 1)
        self.disconnect(source)

    def verify_witness(self, stop_donor=1):
        self.promote_target()
        self.fleet.brokers[stop_donor].stop(crash=True)
        resumed = self.persistent('repair-witness', 0, present=True, expiry=3600, retry=True)
        self.complete_message(resumed, self.expect_message(
            resumed, 'repair/witness', b'acknowledged before replacement', 1))
        self.quiet(resumed)

    def seed_pages(self):
        # Pure data preparation, not an admission-load benchmark. Clean Start
        # prevents Session Present ambiguity after an uncertain setup response.
        # More than64 durable records guarantees a second import page.
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            sockets = list(pool.map(lambda i: self.client(f'repair-page-{i}', 1, clean=True,
                                                        expiry=3600, retry=True), range(70)))
        for sock in sockets:
            self.disconnect(sock)


class RepairQoSCases(RepairFixture):
    def test_waiting_broker_and_one_original_resume_qos_states(self):
        original = {}
        source = self.client('qos-repair-source', 2)
        for name, qos in [('ack', 1), ('rec', 2), ('comp', 2)]:
            target = self.persistent('repair-' + name, expiry=3600)
            self.subscribe(target, 'repair/' + name, qos)
            self.publish(source, 'repair/' + name, name.encode(), qos)
            message = self.expect_message(target, 'repair/' + name, name.encode(), qos)
            original[name] = message
            if name == 'comp':
                target.sendall(ack(0x50, message[3]))
                self.assertEqual(receive(target), (0x62, struct.pack('!H', message[3])))
        pid = self.waiting_target()
        report = self.run_repair()
        self.assertGreaterEqual(report['records'], 3)
        self.promote_target()
        self.assertEqual(self.target.process.pid, pid, 'promotion must keep the waiting process')
        self.fleet.brokers[1].stop(crash=True)
        for name, qos in [('ack', 1), ('rec', 2), ('comp', 2)]:
            target = self.persistent('repair-' + name, 0, present=True, expiry=3600, retry=True)
            if name == 'comp':
                self.assertEqual(receive(target), (0x62, struct.pack('!H', original[name][3])))
                target.sendall(ack(0x70, original[name][3]))
                self.ping(target)
            else:
                message = self.expect_message(target, 'repair/' + name, name.encode(), qos)
                self.assertEqual(message[:4], original[name][:4])
                self.assertTrue(message[4])
                self.complete_message(target, message)
            self.quiet(target)


class Repair311(RepairQoSCases, unittest.TestCase):
    version = 4


class Repair5(RepairQoSCases, unittest.TestCase):
    version = 5


class RepairFaults(RepairFixture, unittest.TestCase):
    def test_unready_target_cannot_form_w2_abort_retains_exclusion_and_stale_import_fails(self):
        self.witness()
        source = self.client('repair-refusal-source', 1)
        self.ping(source)
        self.waiting_target('begin')
        driver = self.driver()
        repair_id = self.blocked(driver)
        donor = self.fleet.brokers[2]
        # Freeze the third replica without erasing its RAM (stop+start would).
        os.kill(donor.process.pid, signal.SIGSTOP)
        try:
            self.no_success(source)
        finally:
            os.kill(donor.process.pid, signal.SIGCONT)
        report = self.run_repair(expected=repair_id, abort=True, retry=True)
        self.assertEqual(report['state'], 'aborted')
        state = self.repair_state()
        self.assertIsNone(state['repair'])
        self.assertIn(101, state['excluded'])
        self.proxy.release.set()  # Old BeginImport must now be rejected by token.
        code, _ = driver.wait()
        self.assertNotEqual(code, 0)
        self.healthy(1)
        self.healthy(2)
        # Active maintenance alone would reject every mutation. Exercise the
        # stronger post-Abort case with global maintenance lifted: the waiting
        # replica must still not contribute a second data vote alongside one donor.
        os.kill(donor.process.pid, signal.SIGSTOP)
        try:
            self.no_success()
        finally:
            os.kill(donor.process.pid, signal.SIGCONT)
        self.healthy(1)
        self.healthy(2)
        completed = self.run_repair()
        self.assertNotEqual(completed['id'], repair_id)
        self.verify_witness(stop_donor=2)

    def test_driver_crash_after_first_import_page_resumes_exact_repair(self):
        self.witness()
        self.seed_pages()
        self.waiting_target('second')
        driver = self.driver()
        repair_id = self.blocked(driver)
        driver.process.kill()
        driver.process.wait(timeout=5)
        self.proxy.release.set()
        report = self.run_repair(expected=repair_id)
        self.assertGreater(report['records'], 64)
        self.verify_witness()

    def test_target_crash_mid_import_loses_staging_and_requires_abort(self):
        self.witness()
        self.seed_pages()
        self.waiting_target('second')
        driver = self.driver()
        repair_id = self.blocked(driver)
        self.target.stop(crash=True)
        driver.process.kill()
        driver.process.wait(timeout=5)
        self.proxy.release.set()
        # New process = new RAM identity; the sealed repair cannot finish safely.
        self.target.spawn()
        self.peer_listening(self.target)
        self.assertFalse(self.target.ready())
        state = self.repair_state()
        self.assertEqual(state['repair']['id'], repair_id)
        self.assertIn(101, state['excluded'])
        self.no_success()
        code, _ = self.driver().wait()
        self.assertNotEqual(code, 0, 'old sealed repair must not commit onto a new boot')
        self.run_repair(expected=repair_id, abort=True)
        self.run_repair()
        self.verify_witness(stop_donor=2)

    def test_all_controllers_crash_during_sealed_repair_then_resume(self):
        self.witness()
        self.waiting_target('begin')
        driver = self.driver()
        repair_id = self.blocked(driver)
        driver.process.kill()
        driver.process.wait(timeout=5)
        for node in self.fleet.controllers:
            node.stop(crash=True)
        self.no_success()
        for node in self.fleet.controllers:
            node.start()
        wait_until(lambda: (self.fleet.status() or {}).get('leader'), 25)
        state = wait_until(lambda: (s if (s := self.repair_state()).get('repair') else None), 15)
        self.assertEqual(state['repair']['id'], repair_id)
        self.proxy.release.set()
        self.run_repair(expected=repair_id, retry=True)
        self.verify_witness()

    def test_installed_target_crash_before_control_commit_requires_new_repair(self):
        self.witness()
        self.waiting_target('installed')
        driver = self.driver()
        # Staging installed in RAM, but installed=true never reaches the driver.
        repair_id = self.blocked(driver)
        driver.process.kill()
        driver.process.wait(timeout=5)
        self.target.stop(crash=True)
        self.proxy.release.set()
        self.target.spawn()
        self.peer_listening(self.target)
        state = self.repair_state()
        self.assertEqual(state['repair']['id'], repair_id)
        self.assertIn(101, state['excluded'])
        self.no_success()
        code, _ = self.driver().wait()
        self.assertNotEqual(code, 0)
        self.run_repair(expected=repair_id, abort=True)
        self.run_repair()
        self.verify_witness(stop_donor=2)

    def test_commit_then_target_crash_needs_fresh_repair_for_new_boot(self):
        self.witness()
        self.waiting_target('resume')
        driver = self.driver()
        self.assertTrue(self.proxy.blocked.wait(25), f'target Resume request absent: {driver.logs()}')
        state = wait_until(lambda: (s if ((s := self.repair_state()).get('last_repair') or {}).get('state')
                                   == 'committed' and s.get('repair') is None else None), 10)
        repair_id = state['last_repair']['id']
        self.assertNotIn(101, state['excluded'])
        driver.process.kill()
        driver.process.wait(timeout=5)
        # Commit landed, but RAM vanishes with the process; a new boot is empty.
        self.target.stop(crash=True)
        self.proxy.release.set()
        wait_until(lambda: (self.fleet.status() or {}).get('nodes') == 2, 25)
        self.target.spawn()
        self.peer_listening(self.target)
        self.assertFalse(self.target.ready())
        self.no_positive_connack(self.target)
        # The committed boot identity is gone; operator starts a fresh repair.
        self.run_repair(retry=True)
        self.verify_witness()
        self.assertNotEqual(self.repair_state()['last_repair']['id'], repair_id)

    def test_corrupt_manifest_rejected_without_voting_then_abort_and_retry(self):
        self.witness()
        self.waiting_target('corrupt')
        code, _ = self.driver().wait()
        self.assertTrue(self.proxy.mutated.is_set(), 'test did not corrupt the manifest on wire')
        self.assertNotEqual(code, 0)
        state = self.repair_state()
        self.assertIsNotNone(state['repair'])
        self.assertTrue(state['repair']['sealed'])
        self.assertIn(101, state['excluded'])
        repair_id = state['repair']['id']
        self.no_success()
        self.run_repair(expected=repair_id, abort=True)
        self.run_repair()
        self.verify_witness()

    def test_wrong_peer_token_cannot_start_a_repair(self):
        self.witness()
        self.waiting_target(wrong_token=True)
        code, output = self.driver().wait()
        self.assertNotEqual(code, 0)
        self.assertNotIn('incorrect-private-repair-test-token-abcdef', output)
        self.assertIsNone(self.repair_state()['repair'])
        self.target.stop()
        self.target.config['token_file'] = str(self.fleet.token)
        self.target.spawn()
        self.peer_listening(self.target)
        self.no_positive_connack(self.target)
        self.run_repair()
        self.verify_witness()

    def test_source_outbox_and_fence_only_donor_recover_without_source_reconnect(self):
        # Place donor C behind a write-excluding proxy before its first boot so a
        # memory restart is not required (stop+start would wait for repair).
        sink_node = self.fleet.brokers[2]
        sink_node.stop(crash=True)
        wait_until(lambda: (self.fleet.status() or {}).get('nodes') == 2, 25)
        backend = self.fleet.port()
        sink_node.broker_listen_override = f'{sink_node.host}:{backend}'
        sink_node.config['listen'] = sink_node.broker_listen_override
        proxy = ImportProxy(
            self.fleet.broker_port, backend, 'outbox',
            host=sink_node.host, backend_host=sink_node.host)
        self.addCleanup(proxy.close)
        sink_node.spawn()
        self.peer_listening(sink_node)
        self.assertFalse(sink_node.ready())
        # Catch C up first; later Write exclusion still drops source replication.
        completed = subprocess.run(
            [BINARY, 'cluster-repair', '--config', str(self.fleet.controllers[0].path),
             '--target-node', str(sink_node.config['node_id'])],
            cwd=self.fleet.root, capture_output=True, text=True, timeout=90)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        wait_until(sink_node.ready, 35)
        sink = self.persistent('repair-outbox-target', 2, expiry=3600)
        self.subscribe(sink, 'repair/outbox', 2)
        self.disconnect(sink)
        source = self.persistent('repair-outbox-source', 0, expiry=3600)
        try:
            self.publish(source, 'repair/outbox', b'accepted but never delivered', 2)
            self.assertTrue(proxy.blocked.wait(15), 'pending delivery was not held on wire')
            self.assertGreater(proxy.rejected_writes, 0, 'test did not exclude donor C source data')
            pid = self.waiting_target()
            self.run_repair()
            # Stop B before A accepts ordinary post-repair writes; no source reconnect.
            self.fleet.brokers[1].stop(crash=True)
            self.promote_target()
            self.assertEqual(self.target.process.pid, pid)
            proxy.reject_source_writes = False
            proxy.release.set()
            resumed = self.persistent('repair-outbox-target', 0, present=True,
                                      expiry=3600, retry=True)
            self.complete_message(resumed, self.expect_message(
                resumed, 'repair/outbox', b'accepted but never delivered', 2))
            self.quiet(resumed, .5)
        finally:
            proxy.reject_source_writes = False
            proxy.release.set()

    def test_completed_repair_survives_controller_restart_not_whole_group_ram_loss(self):
        self.witness()
        pid = self.waiting_target()
        report = self.run_repair()
        self.promote_target()
        self.assertEqual(self.target.process.pid, pid)
        # Controllers persist metadata; data bodies stay in the three live RAMs.
        for node in self.fleet.controllers:
            node.stop(crash=True)
        for node in self.fleet.controllers:
            node.start()
        wait_until(lambda: (self.fleet.status() or {}).get('leader'), 25)
        self.assertEqual(self.repair_state()['last_repair']['id'], report['id'])
        resumed = self.persistent('repair-witness', 0, present=True, expiry=3600, retry=True)
        self.complete_message(resumed, self.expect_message(
            resumed, 'repair/witness', b'acknowledged before replacement', 1))
        # Pure learner CLI remains refused for memory replacements.
        refused = subprocess.run([BINARY, 'cluster-repair-learner', '--config',
                                  str(self.target.path)], cwd=self.fleet.root,
                                 capture_output=True, timeout=12)
        self.assertNotEqual(refused.returncode, 0)
        self.assertIn(b'memory', refused.stderr.lower())


if __name__ == '__main__':
    unittest.main()
