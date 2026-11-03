#!/usr/bin/env python3
"""Phase7c repair maintenance and non-voter safety, real isolated processes.

RMQTT_BIN=/absolute/path/rmqtt python3 tests/cluster_repair_blackbox.py -v
No production fault switch: a transparent HTTP2 DATA-frame proxy deterministically
holds learner import requests while the test kills a process or corrupts a digest.
"""
import concurrent.futures
import json
import os
from pathlib import Path
import re
import signal
import socket
import struct
import subprocess
import tempfile
import threading
import time
import unittest

from blackbox import frame, receive
from cluster_blackbox import BINARY, wait_until
from cluster_durable_blackbox import DurableFixture
from qos_blackbox import ack


def exact(sock, count):
    result = bytearray()
    while len(result) < count:
        part = sock.recv(count - len(result))
        if not part:
            raise EOFError('proxy connection closed')
        result.extend(part)
    return bytes(result)


class ImportProxy:
    """Bounded transparent h2 relay; headers and normal bytes remain unchanged."""
    def __init__(self, port, backend, mode):
        self.port, self.backend, self.mode = port, backend, mode
        self.blocked = threading.Event()
        self.release = threading.Event()
        self.stopped = threading.Event()
        self.mutated = threading.Event()
        self.reject_source_writes = mode == 'outbox'
        self.rejected_writes = 0
        self.sockets = []
        self.lock = threading.Lock()
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(('127.0.0.1', port))
        self.listener.listen(16)
        self.listener.settimeout(.1)
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    def accept(self):
        while not self.stopped.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self.connection, args=(client,), daemon=True).start()

    def connection(self, client):
        server = None
        try:
            server = socket.create_connection(('127.0.0.1', self.backend), timeout=5)
            server.settimeout(65)
            client.settimeout(65)
            with self.lock:
                self.sockets.extend([client, server])
            threading.Thread(target=self.responses, args=(server, client), daemon=True).start()
            # tonic uses h2 prior knowledge over the explicitly loopback HTTP URI.
            preface = exact(client, 24)
            if preface != b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n':
                raise ValueError('unexpected learner transport preface')
            server.sendall(preface)
            while not self.stopped.is_set():
                header = exact(client, 9)
                length = int.from_bytes(header[:3], 'big')
                if length > 16 * 1024 * 1024:
                    raise ValueError('unbounded HTTP2 frame')
                payload = exact(client, length)
                if header[3] == 0:  # DATA; gRPC Envelope contains compact JSON.
                    if (self.reject_source_writes and b'"Write"' in payload and
                            b'"key":"c:repair-outbox-source"' in payload):
                        # Source A's replication to donor C fails, while A/B
                        # can still confirm W2. C might later recover a candidate
                        # from B, so this is a network/intent recovery oracle,
                        # not a claim that C remains permanently body-free.
                        self.rejected_writes += 1
                        raise ConnectionAbortedError('test replica network failure')
                    begin = b'"BeginImport"' in payload and b'"manifest"' in payload
                    second = (b'"AppendPage"' in payload and
                              re.search(br'"sequence":1(?:,|})', payload) is not None)
                    should_hold = ((self.mode == 'begin' and begin) or
                                   (self.mode == 'second' and second) or
                                   (self.mode == 'resume' and b'"Resume"' in payload) or
                                   (self.mode == 'outbox' and
                                    b'"client_id":"repair-outbox-target","id":' in payload))
                    if should_hold and (self.mode == 'outbox' or not self.blocked.is_set()):
                        self.blocked.set()
                        while not self.release.wait(.05):
                            if self.stopped.is_set():
                                return
                    if self.mode == 'corrupt' and begin and not self.mutated.is_set():
                        match = re.search(br'"digest":"([0-9a-f]{64})"', payload)
                        if match is None:
                            raise ValueError('manifest digest not present in first DATA frame')
                        index = match.start(1)
                        other = b'a' if payload[index:index + 1] != b'a' else b'b'
                        payload = payload[:index] + other + payload[index + 1:]
                        self.mutated.set()
                server.sendall(header + payload)
        except (OSError, EOFError, ValueError):
            pass
        finally:
            for sock in (client, server):
                if sock is not None:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    sock.close()

    def responses(self, server, client):
        try:
            while True:
                header = exact(server, 9)
                length = int.from_bytes(header[:3], 'big')
                if length > 16 * 1024 * 1024:
                    raise ValueError('unbounded HTTP2 response frame')
                payload = exact(server, length)
                if (header[3] == 0 and self.mode == 'installed' and
                        b'"installed":true' in payload and not self.blocked.is_set()):
                    self.blocked.set()
                    while not self.release.wait(.05):
                        if self.stopped.is_set():
                            return
                client.sendall(header + payload)
        except (OSError, EOFError, ValueError):
            pass
        finally:
            try:
                client.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def close(self):
        self.stopped.set()
        self.release.set()
        self.listener.close()
        with self.lock:
            for sock in self.sockets:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sock.close()
        self.thread.join(timeout=2)


class Driver:
    def __init__(self, command, root):
        self.output = tempfile.TemporaryFile()
        self.process = subprocess.Popen(command, cwd=root, stdout=self.output,
                                        stderr=subprocess.STDOUT)

    def logs(self):
        self.output.seek(0)
        return self.output.read().decode(errors='replace')[-16000:]

    def wait(self, timeout=120):
        try:
            code = self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            raise AssertionError(f'repair CLI exceeded{timeout}s: {self.logs()}') from None
        return code, self.logs()

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=5)
        self.output.close()


class RepairFixture(DurableFixture):
    version = 5

    def setUp(self):
        super().setUp()
        self.target = self.fleet.brokers[0]
        self.normal_command = self.target.command
        self.old_data = Path(self.target.config['data_dir'])
        self.drivers = []

    def command(self, abort=False):
        command = [BINARY, 'cluster-repair-abort' if abort else 'cluster-repair',
                   '--cluster-config', str(self.fleet.brokers[1].path)]
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

    def learner(self, proxy_mode=None, wrong_token=False):
        self.target.stop(crash=True)
        wait_until(lambda: (self.fleet.status() or {}).get('nodes') == 2, 25)
        self.target.config['data_dir'] = str(self.target.root / 'replacement-data')
        self.proxy = None
        if proxy_mode is not None:
            original = self.target.config['listen'].rsplit(':', 1)[1]
            backend = self.fleet.port()
            self.target.config['listen'] = f'127.0.0.1:{backend}'
            self.proxy = ImportProxy(int(original), backend, proxy_mode)
            self.addCleanup(self.proxy.close)
        if wrong_token:
            wrong = self.target.root / 'wrong-repair.token'
            wrong.write_text('incorrect-private-repair-test-token-abcdef\n')
            self.target.config['token_file'] = str(wrong)
        self.target.command = lambda: [BINARY, 'cluster-repair-learner', '--cluster-config',
                                       str(self.target.path)]
        self.target.spawn()
        self.wait_learner()

    def wait_learner(self):
        port = int(self.target.config['listen'].rsplit(':', 1)[1])
        def listening():
            self.target.alive()
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=.2):
                    return True
            except OSError:
                return False
        wait_until(listening, 10)

    def normal_target(self):
        self.target.stop()
        self.target.command = self.normal_command
        self.target.start()
        self.healthy(0)

    def blocked(self, driver):
        self.assertIsNotNone(self.proxy)
        if not self.proxy.blocked.wait(25):
            self.fail(f'learner import frame was not observed; CLI={driver.process.poll()}: {driver.logs()}')
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
            sock.sendall(self.packet('must-not-use-learner', clean=False))
            try:
                header, body = receive(sock)
            except (EOFError, ConnectionError):
                return
            except socket.timeout:
                self.fail('CONNACK hung instead of bounded refusal')
            self.assertEqual(header, 0x20)
            self.assertIn(body[1], (0x88, 0x89), body)

    def witness(self):
        target = self.persistent('repair-witness', expiry=3600)
        self.subscribe(target, 'repair/witness', 1)
        self.disconnect(target)
        source = self.client('repair-witness-source', 2)
        self.publish(source, 'repair/witness', b'acknowledged before replacement', 1)
        def queued():
            code, body = self.target.request('/api/v1/session?client_id=repair-witness')
            return code == 200 and body.get('queued') == 1
        wait_until(queued, 15)

    def verify_witness(self, stop_donor=1):
        self.normal_target()
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
    def test_new_disk_and_one_original_resume_qos_states_without_old_owner_files(self):
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
        self.learner()
        report = self.run_repair()
        self.assertGreaterEqual(report['records'], 3)
        self.normal_target()
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
        self.learner('begin')
        driver = self.driver()
        repair_id = self.blocked(driver)
        self.fleet.brokers[2].stop(crash=True)
        self.no_success(source)
        self.fleet.brokers[2].start()
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
        # stronger post-Abort case with global maintenance lifted: the learner
        # must still not contribute a second data vote alongside the sole donor.
        self.fleet.brokers[2].stop(crash=True)
        self.no_success()
        self.fleet.brokers[2].start()
        self.healthy(1)
        self.healthy(2)
        completed = self.run_repair()
        self.assertNotEqual(completed['id'], repair_id)
        self.verify_witness(stop_donor=2)

    def test_driver_crash_after_first_import_page_resumes_exact_repair(self):
        self.witness()
        self.seed_pages()
        self.learner('second')
        driver = self.driver()
        repair_id = self.blocked(driver)
        driver.process.kill()
        driver.process.wait(timeout=5)
        self.proxy.release.set()
        report = self.run_repair(expected=repair_id)
        self.assertGreater(report['records'], 64)
        self.verify_witness()

    def test_learner_crash_after_first_import_page_keeps_staging_nonvoting(self):
        self.witness()
        self.seed_pages()
        self.learner('second')
        driver = self.driver()
        repair_id = self.blocked(driver)
        self.target.stop(crash=True)
        driver.process.kill()
        driver.process.wait(timeout=5)
        self.proxy.release.set()
        self.target.spawn()
        self.wait_learner()
        state = self.repair_state()
        self.assertEqual(state['repair']['id'], repair_id)
        self.assertIn(101, state['excluded'])
        self.no_success()
        self.run_repair(expected=repair_id)
        self.verify_witness(stop_donor=2)

    def test_all_controllers_crash_during_sealed_repair_then_resume(self):
        self.witness()
        self.learner('begin')
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

    def test_installed_target_crash_before_control_commit_remains_nonvoting(self):
        self.witness()
        self.learner('installed')
        driver = self.driver()
        # The target has durably installed the entire staging set, but its
        # installed=true response cannot reach the driver to authorize Commit.
        repair_id = self.blocked(driver)
        driver.process.kill()
        driver.process.wait(timeout=5)
        self.target.stop(crash=True)
        self.proxy.release.set()
        self.target.spawn()
        self.wait_learner()
        state = self.repair_state()
        self.assertEqual(state['repair']['id'], repair_id)
        self.assertIn(101, state['excluded'])
        self.no_success()
        self.run_repair(expected=repair_id)
        self.verify_witness(stop_donor=2)

    def test_commit_survives_driver_and_target_crash_before_target_resume(self):
        self.witness()
        self.learner('resume')
        driver = self.driver()
        self.assertTrue(self.proxy.blocked.wait(25), f'target Resume request absent: {driver.logs()}')
        state = wait_until(lambda: (s if ((s := self.repair_state()).get('last_repair') or {}).get('state')
                                   == 'committed' and s.get('repair') is None else None), 10)
        repair_id = state['last_repair']['id']
        self.assertNotIn(101, state['excluded'])
        driver.process.kill()
        driver.process.wait(timeout=5)
        self.target.stop(crash=True)
        self.proxy.release.set()
        # No repair CLI is rerun: normal startup must reconcile the persisted
        # terminal Commit token, safely thawing the installed new disk itself.
        self.verify_witness()
        self.assertEqual(self.repair_state()['last_repair']['id'], repair_id)

    def test_corrupt_manifest_rejected_without_voting_then_abort_and_retry(self):
        self.witness()
        self.learner('corrupt')
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

    def test_wrong_learner_token_cannot_start_a_repair(self):
        self.witness()
        self.learner(wrong_token=True)
        code, output = self.driver().wait()
        self.assertNotEqual(code, 0)
        self.assertNotIn('incorrect-private-repair-test-token-abcdef', output)
        self.assertIsNone(self.repair_state()['repair'])
        self.target.stop()
        self.target.config['token_file'] = str(self.fleet.token)
        self.target.spawn()
        self.wait_learner()
        self.run_repair()
        self.verify_witness()

    def test_source_outbox_and_fence_only_donor_recover_without_source_reconnect(self):
        sink_node = self.fleet.brokers[2]
        sink_node.stop()
        wait_until(lambda: (self.fleet.status() or {}).get('nodes') == 2, 25)
        port = int(sink_node.config['listen'].rsplit(':', 1)[1])
        backend = self.fleet.port()
        proxy = ImportProxy(port, backend, 'outbox')
        self.addCleanup(proxy.close)
        sink_node.config['listen'] = f'127.0.0.1:{backend}'
        sink_node.start()
        sink = self.persistent('repair-outbox-target', 2, expiry=3600)
        self.subscribe(sink, 'repair/outbox', 2)
        self.disconnect(sink)
        source = self.persistent('repair-outbox-source', 0, expiry=3600)
        try:
            self.publish(source, 'repair/outbox', b'accepted but never delivered', 2)
            self.assertTrue(proxy.blocked.wait(15), 'pending delivery was not held on wire')
            self.assertGreater(proxy.rejected_writes, 0, 'test did not exclude donor C source data')
            self.learner()
            self.run_repair()
            # A is still a non-voting learner here. Stop B before A can accept
            # ordinary post-repair writes from it; no source MQTT reconnect occurs.
            self.fleet.brokers[1].stop(crash=True)
            self.normal_target()
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

    def test_completed_repair_survives_fleet_crash_and_old_disk_stays_rejected(self):
        self.witness()
        self.learner()
        report = self.run_repair()
        self.normal_target()
        for node in self.fleet.brokers:
            node.stop(crash=True)
        for node in self.fleet.controllers:
            node.stop(crash=True)
        self.fleet.start()
        self.assertEqual(self.repair_state()['last_repair']['id'], report['id'])
        resumed = self.persistent('repair-witness', 0, present=True, expiry=3600, retry=True)
        self.complete_message(resumed, self.expect_message(
            resumed, 'repair/witness', b'acknowledged before replacement', 1))
        self.target.stop(crash=True)
        replacement = self.target.config['data_dir']
        self.target.config['data_dir'] = str(self.old_data)
        self.target.spawn()
        wait_until(lambda: self.target.process.poll() is not None, 25)
        self.assertNotEqual(self.target.process.returncode, 0)
        self.target.config['data_dir'] = replacement
        self.healthy(1)
        self.healthy(2)


if __name__ == '__main__':
    unittest.main()
