#!/usr/bin/env python3
"""Independent three-process dual-role acceptance; no performance claims."""
import socket
import subprocess
import tempfile
import unittest

from cluster_blackbox import BINARY, wait_until
from cluster_durable_blackbox import DurableFixture, DurableFleet, DurableNode


class CombinedNode(DurableNode):
    def __init__(self, broker, controller):
        # Reuse existing isolated fixture directories without creating them twice.
        self.__dict__.update(broker.__dict__)
        self.controller = controller
        self.role = 'controller,broker'
        self.config = dict(self.config)
        self.config['role'] = 'controller,broker'
        self.write_config()


class CombinedFleet(DurableFleet):
    def __init__(self, directory):
        super().__init__(directory)
        self.brokers = [CombinedNode(b, c) for b, c in zip(self.brokers, self.controllers)]

    def start(self):
        # All three processes must exist before waiting for either role's quorum.
        for node in self.brokers:
            node.spawn()
        def ready():
            for node in self.brokers:
                node.alive()
            return (self.status() or {}).get('nodes') == 3 and all(n.ready() for n in self.brokers)
        wait_until(ready, 35)

    def stop(self):
        failures = []
        for node in reversed(self.brokers):
            try:
                node.close()
            except Exception as error:
                failures.append(str(error))
        if failures:
            raise AssertionError('\n'.join(failures))


class CombinedFixture(DurableFixture):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-combined-wire-')
        self.addCleanup(self.directory.cleanup)
        self.fleet = CombinedFleet(self.directory.name)
        self.addCleanup(self.fleet.stop)
        self.fleet.start()

    def tearDown(self):
        result = self._outcome.result
        if any(test is self for test, _ in [*result.failures, *result.errors]):
            for node in self.fleet.brokers:
                # Logging never prints configuration files or token contents.
                print(f'combined broker {node.config["node_id"]}: {node.logs()}', flush=True)


class CombinedMQTTCases(CombinedFixture, unittest.TestCase):
    def test_ring_cross_node_qos_0_1_2_both_protocol_versions(self):
        self.assertEqual(len({n.process.pid for n in self.fleet.brokers}), 3)
        for version in (5, 311):
            self.version = version
            for source in range(3):
                target = (source + 1) % 3
                subscriber = self.client(f'topo-sub-{version}-{source}', target, expiry=0, retry=True)
                publisher = self.client(f'topo-pub-{version}-{source}', source, expiry=0, retry=True)
                for qos in (0, 1, 2):
                    topic = f'topology/{version}/{source}/{qos}'
                    payload = f'exact-body-{version}-{source}-{qos}'.encode()
                    self.subscribe(subscriber, topic, qos)
                    self.publish(publisher, topic, payload, qos, packet_id=20 + qos)
                    message = self.expect_message(subscriber, topic, payload, qos)
                    self.complete_message(subscriber, message)
                    self.quiet(subscriber, .03)
                self.disconnect(publisher)
                self.disconnect(subscriber)

    def test_sigint_closes_controller_and_broker_ports_and_restarts_same_disks(self):
        node = self.fleet.brokers[2]
        node.stop()
        # Peer Controllers still listen on the same port of *their* loopback host.
        # Probe this dual-role member's host and its private MQTT/admin ports.
        endpoints = [
            (node.host, int(node.controller.config['listen'].rsplit(':', 1)[1])),
            (node.host, int(node.config['listen'].rsplit(':', 1)[1])),
            ('127.0.0.1', node.mqtt),
            ('127.0.0.1', node.admin),
        ]
        for host, port in endpoints:
            with socket.socket() as probe:
                self.assertNotEqual(probe.connect_ex((host, port)), 0, (host, port))
        node.spawn()
        # A restarted memory replica is a new identity and waits for repair.
        wait_until(lambda: 'memory_replica_waiting_for_repair' in node.logs(), 15)
        self.healthy(0)
        self.assertEqual((self.fleet.status() or {}).get('nodes'), 2)


class CombinedStartupCases(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-combined-startup-')
        self.addCleanup(self.directory.cleanup)
        self.fleet = CombinedFleet(self.directory.name)
        self.addCleanup(self.fleet.stop)

    def invoke(self, node, extra=()):
        node.write_config()
        return subprocess.run(node.command() + list(extra), cwd=node.root,
                              capture_output=True, timeout=12)

    def test_controller_bind_failure_terminates_entire_process(self):
        node = self.fleet.brokers[0]
        port = int(node.controller.config['listen'].rsplit(':', 1)[1])
        with socket.socket() as blocker:
            blocker.bind((node.host, port))
            blocker.listen()
            result = self.invoke(node)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        # The controller cannot fail while leaving an orphaned MQTT endpoint.
        with socket.socket() as probe:
            self.assertNotEqual(probe.connect_ex(('127.0.0.1', node.mqtt)), 0)

    def test_invalid_cluster_membership_rejected_before_listeners(self):
        node = self.fleet.brokers[0]
        original_cluster = dict(node.cluster)
        node.cluster = dict(original_cluster)
        # Membership must be 3/5/7; reject before MQTT binds.
        node.cluster['members'] = list(original_cluster['members'])[:2]
        result = self.invoke(node)
        self.assertNotEqual(result.returncode, 0, result.stderr[-500:])
        with socket.socket() as probe:
            self.assertNotEqual(probe.connect_ex(('127.0.0.1', node.mqtt)), 0)
        node.cluster = original_cluster
        node.write_config()

    def test_inspection_commands_reject_combined_start_option(self):
        node = self.fleet.brokers[0]
        for operation in ('cluster-status', 'cluster-repair-abort', 'export-state'):
            with self.subTest(operation=operation):
                # Removed dual --cluster-config flags; unknown options must still fail.
                result = subprocess.run([BINARY, operation, '--cluster-config', str(node.path),
                                         '--cluster-controller-config', str(node.controller.path)],
                                        cwd=node.root, capture_output=True, timeout=5)
                self.assertNotEqual(result.returncode, 0)


if __name__ == '__main__':
    unittest.main()
