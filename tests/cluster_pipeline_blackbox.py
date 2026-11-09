#!/usr/bin/env python3
"""Concurrent acknowledged metadata survives Controller leader loss.

LYRNORMQ_BIN=/absolute/lyrnormq python3 tests/cluster_pipeline_blackbox.py -v
This is a finite correctness test, not a throughput or in-flight-count assertion.
Data replicas remain alive; unknown control outcomes are allowed to have committed.
"""
import concurrent.futures
import socket
import struct
import threading
import time
import unittest

from blackbox import frame, receive, string
from cluster_blackbox import wait_until
from cluster_control_batching_blackbox import controller_only_status
from cluster_memory_blackbox import MemoryFixture
from mqtt5_blackbox import props, readprops


class ControlPipelineRecovery(MemoryFixture, unittest.TestCase):
    version = 5

    def concurrent_subscriptions(self, phase, fault=None):
        count = 16
        start = threading.Barrier(count + 1)
        sent = [threading.Event() for _ in range(count)]

        def attempt(index):
            identity = f'pipeline-{phase}-{index}'
            topic = f'pipeline/isolated/{phase}/{index}'
            with socket.create_connection(
                    ('127.0.0.1', self.fleet.brokers[index % 3].mqtt), timeout=3) as sock:
                sock.settimeout(8)
                start.wait(timeout=8)
                try:
                    sock.sendall(self.packet(identity, clean=False, expiry=3600))
                    sent[index].set()
                    header, body = receive(sock)
                    self.assertEqual(header, 0x20, (identity, header, body))
                    self.assertGreaterEqual(len(body), 3)
                    self.assertEqual(readprops(body, 2)[1], len(body))
                    if body[1] != 0:
                        self.assertIn(body[1], (0x88, 0x89), (identity, body))
                        return identity, topic, False
                    self.assertEqual(body[0], 0, (identity, body))
                    packet_id = index + 1
                    sock.sendall(frame(0x82, struct.pack('!H', packet_id) + props()
                                       + string(topic) + b'\x01'))
                    header, body = receive(sock)
                    if header == 0xe0:
                        self.assertTrue(body and body[0] >= 0x80, (identity, body))
                        return identity, topic, False
                    self.assertEqual(header, 0x90, (identity, header, body))
                    self.assertEqual(body[:2], struct.pack('!H', packet_id))
                    reasons = body[readprops(body, 2)[1]:]
                    self.assertEqual(len(reasons), 1)
                    if reasons[0] != 1:
                        self.assertGreaterEqual(reasons[0], 0x80, (identity, reasons))
                        return identity, topic, False
                    # Once SUBACK is observed this identity is an obligation,
                    # even if disconnect/cleanup subsequently encounters the fault.
                    try:
                        sock.sendall(frame(0xe0))
                    except OSError:
                        pass
                    return identity, topic, True
                except (OSError, EOFError):
                    return identity, topic, False

        with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
            futures = [pool.submit(attempt, index) for index in range(count)]
            start.wait(timeout=8)
            if fault is not None:
                deadline = time.monotonic() + 5
                for event in sent:
                    self.assertTrue(event.wait(max(0, deadline - time.monotonic())),
                                    'CONNECT was not sent before the leader fault')
                fault()
            return [future.result(timeout=15) for future in futures]

    def test_concurrent_subacks_remain_owned_after_leader_crash_and_restart(self):
        seeded = self.concurrent_subscriptions('confirmed')
        self.assertTrue(all(acknowledged for _, _, acknowledged in seeded), seeded)
        before = self.fleet.status()
        self.assertIsNotNone(before)
        leader = next(node for node in self.fleet.controllers
                      if node.config['node_id'] == before['leader'])
        raced = self.concurrent_subscriptions('racing', lambda: leader.stop(crash=True))
        acknowledged = [(identity, topic) for identity, topic, success in seeded + raced
                        if success]
        self.assertGreaterEqual(len(acknowledged), 16)
        # Do not require failed/unknown attempts to be absent: their durable
        # command may have committed before the socket observed a reply.
        wait_until(lambda: (self.fleet.status() or {}).get('leader') not in
                   (None, before['leader']), 20)
        self.wait_broker_recovery()
        with controller_only_status(self.fleet, leader) as local_status:
            self.assertIsNone(local_status(), 'status probe must not fall back to a healthy peer')
            leader.spawn()

            def caught_up():
                leader.alive()
                state = local_status()
                return state if state and state['last_applied'] >= before['last_applied'] else None

            restored = wait_until(caught_up, 20)
            self.assertEqual(restored['nodes'], 3, restored)

        source = self.client('pipeline-delivery-source', 2, expiry=0)
        targets = []
        for identity, topic in acknowledged:
            index = int(identity.rsplit('-', 1)[1])
            target = self.persistent(identity, (index + 1) % 3,
                                     present=True, expiry=3600, retry=True)
            targets.append((identity, topic, target))
        for index, (identity, topic, _) in enumerate(targets):
            self.publish(source, topic, identity.encode(), 1, packet_id=index + 1)
        for identity, topic, target in targets:
            message = self.expect_message(target, topic, identity.encode(), 1)
            self.complete_message(target, message)
            self.quiet(target, .025)
            self.disconnect(target)
        self.disconnect(source)


if __name__ == '__main__':
    unittest.main()
