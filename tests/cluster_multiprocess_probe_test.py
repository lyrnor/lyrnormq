#!/usr/bin/env python3
"""Independent raw-wire tests of the connection probe, not Broker conformance.

RMQTT_CONNECTIONS_BIN=/absolute/connections python3 tests/cluster_multiprocess_probe_test.py -v
Four native processes connect to three tiny MQTT stubs. No Broker is started.
"""
from collections import Counter
import json
import importlib.util
import math
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import unittest


BINARY = os.environ.get('RMQTT_CONNECTIONS_BIN')


def exact(sock, count):
    data = bytearray()
    while len(data) < count:
        chunk = sock.recv(count - len(data))
        if not chunk:
            raise EOFError('stub peer closed before DISCONNECT')
        data.extend(chunk)
    return bytes(data)


def packet(sock):
    header = exact(sock, 1)[0]
    length = 0
    for index in range(4):
        digit = exact(sock, 1)[0]
        length |= (digit & 127) << (index * 7)
        if not digit & 128:
            if length > 65536:
                raise ValueError('stub packet bound exceeded')
            return header, exact(sock, length)
    raise ValueError('invalid remaining length')


def nearest_rank(values):
    ordered = sorted(values)
    return {'count': len(ordered), 'mean_us': sum(ordered) / len(ordered),
            'p99_us': ordered[math.ceil(.99 * len(ordered)) - 1], 'max_us': ordered[-1]}


class Stubs:
    def __init__(self, reject=None):
        self.reject = reject
        self.lock = threading.Lock()
        self.stopped = threading.Event()
        self.listeners = []
        self.acceptors = []
        self.workers = []
        self.sockets = set()
        self.records = []
        self.errors = []
        try:
            for endpoint in range(3):
                listener = socket.socket()
                self.listeners.append(listener)
                listener.bind(('127.0.0.1', 0))
                listener.listen(16)
                listener.settimeout(.1)
                acceptor = threading.Thread(target=self.accept, args=(endpoint, listener), daemon=True)
                self.acceptors.append(acceptor)
                acceptor.start()
        except Exception:
            self.close()
            raise

    @property
    def endpoints(self):
        return [f'127.0.0.1:{listener.getsockname()[1]}' for listener in self.listeners]

    def accept(self, endpoint, listener):
        while not self.stopped.is_set():
            try:
                sock, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with self.lock:
                if len(self.workers) >= 16:
                    self.errors.append('stub bounded connection count exceeded')
                    sock.close()
                    continue
                self.sockets.add(sock)
                worker = threading.Thread(target=self.connection, args=(endpoint, sock), daemon=True)
                self.workers.append(worker)
            worker.start()

    def connection(self, endpoint, sock):
        try:
            sock.settimeout(20)
            header, body = packet(sock)
            if header != 0x10 or body[:11] != b'\x00\x04MQTT\x05\x02\x00\x00\x00':
                raise ValueError('expected MQTT5 clean CONNECT with keepalive0 and no properties')
            size = int.from_bytes(body[11:13], 'big')
            client = body[13:].decode('ascii')
            if len(body) != 13 + size or len(client) != size:
                raise ValueError('bad client identifier')
            record = {'client': client, 'endpoint': endpoint, 'connect_ns': time.monotonic_ns()}
            with self.lock:
                self.records.append(record)
            if client == self.reject:
                sock.sendall(b'\x20\x03\x00\x88\x00')
                record['rejected'] = True
                return
            # Unequal shards make an average-of-shard-p99 oracle visibly wrong.
            shard = int(client[3]) if client.startswith('shd') else 0
            time.sleep((.002, .020, .060, .120)[shard])
            sock.sendall(b'\x20\x03\x00\x00\x00')
            record['connack_ns'] = time.monotonic_ns()
            if packet(sock) != (0xc0, b''):
                raise ValueError('expected PINGREQ')
            record['ping_ns'] = time.monotonic_ns()
            sock.sendall(b'\xd0\x00')
            if packet(sock) != (0xe0, b''):
                raise ValueError('expected final DISCONNECT')
            record['disconnected'] = True
        except (OSError, EOFError, ValueError) as error:
            if not self.stopped.is_set():
                with self.lock:
                    self.errors.append(repr(error))
        finally:
            sock.close()
            with self.lock:
                self.sockets.discard(sock)

    def settle(self):
        for worker in list(self.workers):
            worker.join(timeout=2)
        if any(worker.is_alive() for worker in self.workers):
            raise AssertionError('stub connection worker did not finish')

    def close(self):
        self.stopped.set()
        for listener in self.listeners:
            listener.close()
        with self.lock:
            sockets = list(self.sockets)
        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        for acceptor in self.acceptors:
            acceptor.join(timeout=2)
        for worker in list(self.workers):
            worker.join(timeout=2)
        if any(thread.is_alive() for thread in [*self.acceptors, *self.workers]):
            raise AssertionError('stub threads survived cleanup')


@unittest.skipUnless(BINARY, 'set RMQTT_CONNECTIONS_BIN to the native connection probe')
class MultiprocessProbe(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-multiprocess-wire-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.processes = []
        self.addCleanup(self.stop_processes)

    def stop_processes(self):
        for process, output in self.processes:
            if process.poll() is None:
                process.terminate()
        for process, output in self.processes:
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
            output.close()

    def launch(self, endpoints, prefix, offset, start):
        report = self.root / f'{prefix}.json'
        raw = self.root / f'{prefix}.raw.json'
        command = [BINARY, ','.join(endpoints), '2', str(report), '1',
                   '--connections-per-step', '3', '--client-prefix', prefix,
                   '--index-offset', str(offset), '--start-unix-ms', str(start),
                   '--ping-unix-ms', str(start + 12000), '--raw', str(raw)]
        output = (self.root / f'{prefix}.log').open('wb')
        try:
            process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                                       env=dict(os.environ, TOKIO_WORKER_THREADS='2'))
        except Exception:
            output.close()
            raise
        self.processes.append((process, output))
        return process, report, raw

    def finish(self, launched, start):
        process, report, raw = launched
        remaining = max(1, (start + 20000 - time.time_ns() // 1000000) / 1000)
        process.wait(timeout=remaining)
        self.assertEqual(process.returncode, 0, (self.root / f'{report.stem}.log').read_text())
        return json.loads(report.read_text()), json.loads(raw.read_text())

    def check_stats(self, values, reported):
        expected = nearest_rank(values)
        for field in ('count', 'p99_us', 'max_us'):
            self.assertEqual(reported[field], expected[field])
        self.assertAlmostEqual(reported['mean_us'], expected['mean_us'], places=6)
        return expected

    def test_four_processes_share_hold_unique_ids_and_exact_pooled_raw(self):
        stubs = Stubs()
        self.addCleanup(stubs.close)
        start = time.time_ns() // 1000000 + 3000
        launched = [self.launch(stubs.endpoints, f'shd{shard}', shard * 3, start)
                    for shard in range(4)]
        latencies, lags, shard_p99 = [], [], []
        for shard, worker in enumerate(launched):
            report, raw = self.finish(worker, start)
            self.assertTrue(report['plateau_passed'], report)
            self.assertEqual(report['client_prefix'], f'shd{shard}')
            self.assertEqual(report['index_offset'], shard * 3)
            self.assertEqual(report['endpoints'], stubs.endpoints)
            self.assertEqual(report['requested_start_unix_ms'], start)
            self.assertEqual(report['requested_ping_unix_ms'], start + 12000)
            for field in ('client_prefix', 'index_offset', 'endpoints', 'rate',
                          'connections_per_step', 'requested_start_unix_ms', 'requested_ping_unix_ms'):
                self.assertEqual(raw['settings'][field], report[field])
            step, samples = report['steps'][0], raw['steps'][0]
            for field in ('new_scheduled', 'new_attempted', 'new_connack_success', 'live_after_hold_ping'):
                self.assertEqual(step[field], 3)
            self.assertEqual(step['not_started'], 0)
            self.assertFalse(step['errors'])
            self.assertFalse(step['ping_errors'])
            self.assertTrue(step['shared_hold_at_least_10s'])
            self.assertGreaterEqual(step['observed_hold_ms'], 10000)
            self.assertLessEqual(step['ramp_completed_unix_ms'], start + 2000)
            self.assertGreaterEqual(step['ping_started_unix_ms'], start + 12000)
            self.assertEqual(samples['latency_count'], len(samples['latency_us']))
            self.assertEqual(samples['schedule_lag_count'], len(samples['schedule_lag_us']))
            self.assertEqual(samples['latency_count'], 3)
            self.assertEqual(samples['schedule_lag_count'], 3)
            self.check_stats(samples['latency_us'], step['connect_latency'])
            self.check_stats(samples['schedule_lag_us'], step['schedule_lag'])
            shard_p99.append(step['connect_latency']['p99_us'])
            latencies.extend(samples['latency_us'])
            lags.extend(samples['schedule_lag_us'])
        stubs.settle()
        self.assertFalse(stubs.errors, stubs.errors)
        expected_ids = {f'shd{shard}{index:06}' for shard in range(4)
                        for index in range(shard * 3, shard * 3 + 3)}
        self.assertEqual({row['client'] for row in stubs.records}, expected_ids)
        self.assertEqual(len(stubs.records), 12)
        self.assertEqual(Counter(row['endpoint'] for row in stubs.records), {0: 4, 1: 4, 2: 4})
        self.assertTrue(all(row.get('disconnected') for row in stubs.records))
        self.assertGreaterEqual(min(row['ping_ns'] for row in stubs.records)
                                - max(row['connack_ns'] for row in stubs.records), 10_000_000_000)
        merged = nearest_rank(latencies)
        path = Path(os.environ.get('RMQTT_MULTIPROCESS_AGGREGATE',
                    str(Path(__file__).resolve().parents[1] / 'docs/performance/'
                        'cluster-multiprocess-load-20261103/aggregate.py')))
        specification = importlib.util.spec_from_file_location('multiprocess_aggregate', path)
        aggregator = importlib.util.module_from_spec(specification)
        specification.loader.exec_module(aggregator)
        self.assertEqual(aggregator.distribution(latencies), merged)
        self.assertEqual(aggregator.distribution(lags), nearest_rank(lags))
        self.assertEqual(merged['count'], 12)
        self.assertEqual(len(lags), 12)
        self.assertEqual(merged['p99_us'], max(latencies))
        self.assertNotAlmostEqual(merged['p99_us'], sum(shard_p99) / 4, delta=1000)
        print('POOLED_RAW_ORACLE', json.dumps({'latency': merged, 'schedule_lag': nearest_rank(lags)}))

    def test_rejected_connect_has_attempt_lag_but_no_success_latency(self):
        stubs = Stubs(reject='reject000002')
        self.addCleanup(stubs.close)
        start = time.time_ns() // 1000000 + 3000
        launched = self.launch(stubs.endpoints, 'reject', 1, start)
        report, raw = self.finish(launched, start)
        stubs.settle()
        self.assertFalse(stubs.errors, stubs.errors)
        self.assertFalse(report['plateau_passed'])
        step, samples = report['steps'][0], raw['steps'][0]
        self.assertEqual(step['new_attempted'], 3)
        self.assertEqual(step['new_connack_success'], 2)
        self.assertEqual(step['live_after_hold_ping'], 2)
        self.assertEqual(step['not_started'], 0)
        self.assertEqual(sum(step['errors'].values()), 1)
        self.assertEqual(samples['latency_count'], 2)
        self.assertEqual(samples['schedule_lag_count'], 3)
        self.assertEqual(len(samples['latency_us']), 2)
        self.assertEqual(len(samples['schedule_lag_us']), 3)
        self.check_stats(samples['latency_us'], step['connect_latency'])
        self.check_stats(samples['schedule_lag_us'], step['schedule_lag'])
        self.assertEqual({row['client']: row['endpoint'] for row in stubs.records},
                         {'reject000001': 1, 'reject000002': 2, 'reject000003': 0})


if __name__ == '__main__':
    unittest.main()
