#!/usr/bin/env python3
"""Real-process functional acceptance for control batching and owner recovery.

RMQTT_BIN=/absolute/rmqtt python3 tests/cluster_control_batching_blackbox.py -v
The separate cluster_control_batching_diagnostic.py runs the finite 900-client
performance reproduction; default functional tests have no throughput assertion.
"""
from contextlib import ExitStack, contextmanager
import json
import socket
import subprocess
import unittest

from cluster_blackbox import BINARY, wait_until
from cluster_memory_blackbox import CombinedMemoryFleet, MemoryFixture
import rmqttcfg


@contextmanager
def controller_only_status(fleet, node):
    """Query one Controller via a single-member TOML seed list.

    The normal CLI may fall back across seeds. Omitting peer members forces the
    probe onto the restarted Controller without contacting healthy peers. Held
    ephemeral binds keep those ports from being reused during the probe window.
    """
    host, port_text = node.config['listen'].rsplit(':', 1)
    port = int(port_text)
    # Keep a legal 3-member list, but point every peer host at an unused
    # loopback so the CLI cannot fall back to a healthy Controller.
    decoys = ['127.0.0.4', '127.0.0.5', '127.0.0.6']
    members = [{'id': node.config['node_id'], 'host': host}]
    for peer in node.config.get('controllers') or getattr(fleet, 'members', []):
        peer_id = peer['id'] if isinstance(peer, dict) else peer
        if peer_id == node.config['node_id']:
            continue
        members.append({'id': peer_id, 'host': decoys[len(members) - 1]})
    while len(members) < 3:
        members.append({'id': 10 + len(members), 'host': decoys[len(members) - 1]})
    with ExitStack() as resources:
        for _ in range(2):
            reserved = resources.enter_context(socket.socket())
            reserved.bind(('127.0.0.1', 0))
        path = rmqttcfg.write(
            node.root / 'status-only',
            None,
            node_id=node.config['node_id'],
            role='controller',
            cluster={
                'id': node.config['cluster_id'],
                'token_file': node.config['token_file'],
                'controller_port': port,
                'broker_port': getattr(fleet, 'broker_port', port + 1),
                'members': members,
            },
        )

        def query():
            try:
                result = subprocess.run([BINARY, 'cluster-status', '--config', str(path)],
                                        cwd=fleet.root, capture_output=True, timeout=4)
            except subprocess.TimeoutExpired:
                return None
            return json.loads(result.stdout) if result.returncode == 0 else None

        yield query


class CleanupCombinedMemoryFleet(CombinedMemoryFleet):
    def stop(self):
        failures = []
        for node in reversed(self.brokers):
            try:
                node.close()
            except Exception as error:
                failures.append(str(error))
        if failures:
            raise AssertionError('\n'.join(failures))


class CombinedControlBatching(MemoryFixture, unittest.TestCase):
    fleet_class = CleanupCombinedMemoryFleet

    def test_both_versions_cross_node_qos_and_client_order(self):
        for version in (4, 5):
            with self.subTest(version=version):
                self.version = version
                subscriber = self.client(f'batch-sub-{version}', 1, expiry=0)
                publisher = self.client(f'batch-pub-{version}', 0, expiry=0)
                topic = f'batch/ordered/{version}'
                self.subscribe(subscriber, topic, 2)
                for qos in (0, 1, 2):
                    for serial in range(4):
                        payload = f'{version}:{qos}:{serial}'.encode()
                        self.publish(publisher, topic, payload, qos, packet_id=10 + serial)
                        message = self.expect_message(subscriber, topic, payload, qos)
                        self.complete_message(subscriber, message)
                self.ping(publisher)
                self.ping(subscriber)
                self.quiet(subscriber, .1)
                self.disconnect(publisher)
                self.disconnect(subscriber)


class ControlBatchRecovery(MemoryFixture, unittest.TestCase):
    """Controllers restart independently so all three data RAM copies survive."""

    def test_leader_crash_preserves_metadata_and_old_owner_offline_delivery(self):
        for version in (4, 5):
            with self.subTest(version=version):
                self.version = version
                client = f'batch-recovery-{version}'
                topic = f'batch/recovery/{version}'
                subscriber = self.persistent(client, 0, expiry=3600)
                self.subscribe(subscriber, topic, 2)
                self.disconnect(subscriber)
                before = self.fleet.status()
                self.assertIsNotNone(before)
                leader = next(node for node in self.fleet.controllers
                              if node.config['node_id'] == before['leader'])
                leader.stop(crash=True)
                wait_until(lambda: (self.fleet.status() or {}).get('leader') not in
                           (None, before['leader']), 20)
                # Readiness/leases converge after leader election. Admission is
                # probed only in recovery; the measured diagnostic never retries.
                self.wait_broker_recovery()
                with controller_only_status(self.fleet, leader) as restarted_status:
                    # The other two Controllers are alive. A response here would
                    # prove the probe can silently fall back, invalidating it.
                    self.assertIsNone(restarted_status())
                    leader.spawn()

                    def caught_up():
                        leader.alive()
                        state = restarted_status()
                        return state if state and all(
                            state[field] >= before[field]
                            for field in ('last_applied', 'routes', 'clients')
                        ) else None

                    restored = wait_until(caught_up, 20)
                    self.assertEqual(restored['nodes'], 3, restored)
                publisher = self.client(f'batch-recovery-source-{version}', 2, expiry=0)
                for qos in (1, 2):
                    self.publish(publisher, topic, f'after-restart-{qos}'.encode(),
                                 qos, packet_id=qos)
                resumed = self.persistent(client, 1, present=True, expiry=3600, retry=True)
                for qos in (1, 2):
                    message = self.expect_message(resumed, topic,
                                                  f'after-restart-{qos}'.encode(), qos)
                    # A later queued PUBLISH may precede a PINGRESP; consume
                    # the ordered backlog before issuing the final barrier.
                    self.complete_message(resumed, message, barrier=False)
                self.ping(resumed)
                self.quiet(resumed, .1)
                state = self.fleet.status()
                self.assertEqual(state['nodes'], 3, state)
                self.assertGreaterEqual(state['last_applied'], before['last_applied'])
                self.assertGreaterEqual(state['routes'], before['routes'])
                self.assertGreaterEqual(state['clients'], before['clients'])
                self.disconnect(publisher)
                self.disconnect(resumed)


if __name__ == '__main__':
    unittest.main()
