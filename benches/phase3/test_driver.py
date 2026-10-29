#!/usr/bin/env python3
"""Accounting tests plus synthetic streams; real TCP smoke is a separate command."""
import asyncio
import struct
import unittest

from metrics import cpu_seconds, distribution, summarize
from run import Workload, payload, phase
from wire import frame, string


class MetricsTests(unittest.TestCase):
    def test_nearest_rank_uses_all_samples(self):
        result = distribution([4_000_000, 1_000_000, 3_000_000, 2_000_000])
        self.assertEqual(result, dict(samples=4, p50_ms=2, p95_ms=4, p99_ms=4, max_ms=4))
        with self.assertRaises(AssertionError):
            distribution([])
        with self.assertRaises(AssertionError):
            distribution([-1])

    def test_cpu_formats(self):
        self.assertEqual(cpu_seconds('03:04.50'), 184.5)
        self.assertEqual(cpu_seconds('1-02:03:04.50'), 93784.5)

    def test_repetitions_are_not_pooled(self):
        runs = []
        for number in (2, 9, 4):
            runs.append(dict(case='hot', storage='memory', ingress_messages_per_second=number,
                             egress_deliveries_per_second=number * 32,
                             delivery_latency=dict(p50_ms=number, p95_ms=number, p99_ms=number, max_ms=number),
                             ack_latency=dict(p50_ms=number, p95_ms=number, p99_ms=number, max_ms=number),
                             memory=dict(sampled_peak_rss_bytes=number * 1024)))
        result = summarize(runs)[0]
        self.assertEqual(result['repetitions'], 3)
        self.assertEqual(result['ingress_messages_per_second'], dict(median=4, min=2, max=9))

    def test_formal_topologies(self):
        concurrent = Workload('concurrent', False).description()
        self.assertEqual(concurrent['simultaneous_connected_clients'], 128)
        self.assertEqual(Workload('hot', False).subscribers, 32)
        many = Workload('many', False)
        self.assertEqual(many.description()['subscriptions'], 4096)
        self.assertEqual(len({many.topic(sequence) for sequence in range(4096)}), 4096)


class SyntheticWriter:
    def __init__(self, reader, workload, subscriber_readers=None, duplicate=False):
        self.reader, self.workload = reader, workload
        self.subscriber_readers = subscriber_readers
        self.duplicate = duplicate

    def write(self, packet):
        if packet == b'\xc0\x00':
            self.reader.feed_data(b'\xd0\x00')
            return
        assert packet[0] == 0x32
        at = 1
        while packet[at] & 128:
            at += 1
        at += 1
        body = packet[at:]
        topic_size = int.from_bytes(body[:2], 'big')
        topic = body[2:2 + topic_size].decode()
        at = topic_size + 2
        packet_id = body[at:at + 2]
        data = body[at + 3:]
        identifier = int.from_bytes(data[:8], 'big')
        sequence = identifier - 2_000_000
        self.reader.feed_data(frame(0x40, packet_id))
        outbound = frame(0x30, string(topic) + b'\x00' + data)
        for index in self.workload.recipients(sequence):
            self.subscriber_readers[index].feed_data(outbound)
            if self.duplicate:
                self.subscriber_readers[index].feed_data(outbound)

    async def drain(self):
        await asyncio.sleep(0)


class AccountingTests(unittest.IsolatedAsyncioTestCase):
    def setup_streams(self, case, duplicate=False):
        workload = Workload(case, True)
        readers = [asyncio.StreamReader() for _ in range(workload.subscribers)]
        subscribers = [(reader, SyntheticWriter(reader, workload)) for reader in readers]
        publishers = []
        for _ in range(workload.publishers):
            reader = asyncio.StreamReader()
            publishers.append((reader, SyntheticWriter(reader, workload, readers, duplicate)))
        return workload, subscribers, publishers

    async def test_fanout_counts_every_delivery_and_ack_sample(self):
        workload, subscribers, publishers = self.setup_streams('hot')
        result = await asyncio.wait_for(phase(workload, subscribers, publishers, 17, 256, 2), 2)
        self.assertEqual(result['acknowledgements'], 17)
        self.assertEqual(result['deliveries'], 68)
        self.assertEqual(result['delivery_latency']['samples'], 68)
        self.assertEqual(result['ack_latency']['samples'], 17)
        self.assertAlmostEqual(result['egress_deliveries_per_second'], result['ingress_messages_per_second'] * 4)

    async def test_many_subscription_routing_counts_exact_recipients(self):
        workload, subscribers, publishers = self.setup_streams('many')
        result = await asyncio.wait_for(phase(workload, subscribers, publishers, 33, 256, 2), 2)
        self.assertEqual(result['deliveries'], 33)
        self.assertEqual(result['delivery_latency']['samples'], 33)

    async def test_duplicate_is_failure_not_silently_dropped_sample(self):
        workload, subscribers, publishers = self.setup_streams('hot', duplicate=True)
        with self.assertRaises(ExceptionGroup):
            await asyncio.wait_for(phase(workload, subscribers, publishers, 17, 256, 2), 2)


if __name__ == '__main__':
    unittest.main(verbosity=2)
