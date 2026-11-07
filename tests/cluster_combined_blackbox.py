#!/usr/bin/env python3
"""Three cohosted controller,broker processes on unique loopback addresses.

The operator TOML file requires unique member hosts, so local processes use
127.0.0.1/2/3 with shared controller and broker ports. This is the current
combined-role layout, not the removed --cluster-config JSON launcher.

RMQTT_BIN=/absolute/path/rmqtt python3 tests/cluster_combined_blackbox.py -v
"""
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import tempfile
import time
import unittest

from blackbox import connect_packet, frame, receive, string
from dashboard_blackbox import free_port
import rmqttcfg

BINARY = os.environ.get('RMQTT_BIN', str(Path(__file__).resolve().parents[1] / 'target/debug/rmqtt'))
LOADGEN = os.environ.get(
    'RMQTT_LOADGEN_BIN',
    str(Path(__file__).resolve().parents[1] / 'target/debug/rmqtt-loadgen'),
)
CLUSTER_TOKEN = 'cluster-test-secret-709def7a902445eb'
HOSTS = ('127.0.0.1', '127.0.0.2', '127.0.0.3')


def wait_until(predicate, timeout=40):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (OSError, ValueError, subprocess.SubprocessError):
            pass
        time.sleep(0.05)
    raise AssertionError(f'condition not established in {timeout}s; last={last!r}')


def mqtt_connect(host, port, client_id):
    sock = socket.create_connection((host, port), timeout=6)
    sock.sendall(connect_packet(client_id))
    header, body = receive(sock)
    if header != 0x20 or body != b'\x00\x00':
        sock.close()
        raise AssertionError(f'CONNACK {header:#x} {body!r}')
    return sock


class CombinedCluster(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-combined-cluster-')
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        token = root / 'peer.token'
        token.write_text(CLUSTER_TOKEN + '\n')
        controller_port = free_port()
        broker_port = free_port()
        while broker_port == controller_port:
            broker_port = free_port()
        members = [{'id': index + 1, 'host': host} for index, host in enumerate(HOSTS)]
        cluster = {
            'id': 'combined-wire',
            'token_file': str(token),
            'controller_port': controller_port,
            'broker_port': broker_port,
            'members': members,
        }
        self.nodes = []
        self.processes = []
        for index, host in enumerate(HOSTS):
            node_id = index + 1
            data_dir = root / f'n{node_id}'
            mqtt_port = free_port()
            listen = f'{host}:{mqtt_port}'
            path = rmqttcfg.write(
                data_dir,
                listen,
                node_id=node_id,
                role='controller,broker',
                cluster=cluster,
                limits={'connections': 1024, 'sessions': 1024, 'state_bytes': 8 * 1024 * 1024},
            )
            log = tempfile.TemporaryFile()
            process = subprocess.Popen(
                [BINARY, '--config', str(path)],
                cwd=data_dir,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            self.nodes.append({'id': node_id, 'host': host, 'mqtt': mqtt_port, 'path': path, 'log': log, 'dir': data_dir})
            self.processes.append(process)
        self.addCleanup(self._stop)

        def leader():
            for process, node in zip(self.processes, self.nodes):
                code = process.poll()
                if code is not None:
                    node['log'].seek(0)
                    raise AssertionError(
                        f'node {node["id"]} exited {code}: {node["log"].read().decode(errors="replace")[-4000:]}'
                    )
            result = subprocess.run(
                [BINARY, 'cluster-status', '--config', str(self.nodes[0]['path'])],
                capture_output=True,
                timeout=8,
            )
            if result.returncode != 0:
                return None
            status = json.loads(result.stdout)
            return status.get('leader') if status.get('nodes') == 3 else None

        self.leader = wait_until(leader)

    def _stop(self):
        for process in self.processes:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        for node in self.nodes:
            node['log'].close()

    def test_cross_node_exact_and_wildcard_delivery(self):
        publisher = mqtt_connect(self.nodes[0]['host'], self.nodes[0]['mqtt'], 'pub-1')
        subscriber = mqtt_connect(self.nodes[1]['host'], self.nodes[1]['mqtt'], 'sub-1')
        self.addCleanup(publisher.close)
        self.addCleanup(subscriber.close)
        subscriber.sendall(frame(0x82, b'\x00\x01' + string('sensors/+/temp') + b'\x00'))
        header, body = receive(subscriber)
        self.assertEqual(header, 0x90)
        self.assertEqual(body[:2], b'\x00\x01')
        publisher.sendall(frame(0x30, string('sensors/room/temp') + b'22c'))
        header, body = receive(subscriber)
        self.assertEqual(header & 0xf0, 0x30)
        topic_len = struct.unpack('!H', body[:2])[0]
        self.assertEqual(body[2:2 + topic_len], b'sensors/room/temp')
        self.assertEqual(body[2 + topic_len:], b'22c')

        isolated = mqtt_connect(self.nodes[2]['host'], self.nodes[2]['mqtt'], 'sys-1')
        self.addCleanup(isolated.close)
        isolated.sendall(frame(0x82, b'\x00\x02' + string('#') + b'\x00'))
        header, body = receive(isolated)
        self.assertEqual(header, 0x90)
        publisher.sendall(frame(0x30, string('$SYS/should-not-match-root-hash') + b'x'))
        isolated.settimeout(0.4)
        with self.assertRaises(socket.timeout):
            receive(isolated)

    def test_cross_node_loadgen_qos0_and_qos1(self):
        addresses = ','.join(f"{node['host']}:{node['mqtt']}" for node in self.nodes)
        for qos in (0, 1):
            with self.subTest(qos=qos):
                output = Path(self.directory.name) / f'loadgen-qos{qos}.json'
                result = subprocess.run(
                    [
                        LOADGEN, '--addrs', addresses, '--protocol', '5', '--qos', str(qos),
                        '--pairs', '8', '--setup-concurrency', '4', '--payload-bytes', '64',
                        '--rate', '80', '--inflight', '1', '--warmup-ms', '500',
                        '--duration-ms', '3000', '--drain-timeout-ms', '10000',
                        '--connect-timeout-ms', '15000', '--io-timeout-ms', '15000',
                        '--output', str(output),
                    ],
                    capture_output=True, text=True, timeout=40,
                )
                self.assertTrue(output.is_file(), result.stderr)
                data = json.loads(output.read_text())
                self.assertEqual(result.returncode, 0, data)
                self.assertTrue(data.get('success'), data)
                accounting = data['accounting']
                counters = accounting['counters']
                self.assertEqual(accounting['not_sent'], 0, accounting)
                self.assertEqual(accounting['not_delivered'], 0, accounting)
                self.assertEqual(counters['duplicates'], 0, counters)
                self.assertGreater(counters['attempted'], 0, counters)
                self.assertEqual(counters['attempted'], counters['delivered_unique'], counters)
                if qos:
                    self.assertEqual(accounting['not_source_confirmed'], 0, accounting)


if __name__ == '__main__':
    unittest.main()
