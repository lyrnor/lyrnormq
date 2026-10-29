#!/usr/bin/env python3
"""Accounting tests plus synthetic streams; real TCP smoke is a separate command."""
import asyncio
import io
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
from unittest import mock
import unittest

from metrics import cpu_seconds, distribution, summarize, process_sample
from run import Workload, campaign, environment, phase
from wire import frame, string, validate_ack


class MetricsTests(unittest.TestCase):
    def test_linux_cpu_metadata_uses_model_name_or_explicit_unavailability(self):
        model = 'Intel(R) Xeon(R) Fixture CPU @ 3.00GHz'
        fixtures = (
            ('processor\t: 0\nmodel name\t: ' + model + '\n', model),
            ('processor\t: 0\nCPU architecture\t: 8\n', None),
        )
        with tempfile.TemporaryDirectory(prefix='rmqtt-bench-cpu-metadata-') as directory:
            binary = Path(directory) / 'broker'
            binary.write_bytes(b'fixture; no process launch')
            arguments = SimpleNamespace(artifact_source_revision='fixture',
                                        build_profile='release', sample_interval=.05)
            actual_open = Path.open
            for cpuinfo, expected in fixtures:
                def open_for_metadata(path, *args, **kwargs):
                    if path == Path('/proc/cpuinfo'):
                        return io.StringIO(cpuinfo)
                    return actual_open(path, *args, **kwargs)
                with self.subTest(cpuinfo=cpuinfo), \
                        mock.patch('run.sys.platform', 'linux'), \
                        mock.patch('run.os.sysconf', return_value=4096), \
                        mock.patch('run.Path.open', autospec=True, side_effect=open_for_metadata), \
                        mock.patch('run.captured', side_effect=lambda command:
                                   'x86_64' if command == ['uname', '-m'] else 'fixture'):
                    value = environment(binary, arguments)['cpu_model']
                    if expected is not None:
                        self.assertEqual(value, expected)
                    else:
                        self.assertIsInstance(value, dict)
                        self.assertTrue(value.get('unavailable'))
                    self.assertNotEqual(value, 'x86_64', 'architecture is not a CPU model')

    def test_nearest_rank_uses_all_samples(self):
        result = distribution([4_000_000, 1_000_000, 3_000_000, 2_000_000])
        self.assertEqual(result, dict(samples=4, p50_ms=2, p95_ms=4, p99_ms=4, max_ms=4))
        with self.assertRaises(AssertionError):
            distribution([])
        with self.assertRaises(AssertionError):
            distribution([-1])

    def test_ack_failures_preserve_actual_packet_diagnostics(self):
        for header, body in ((0xe0, bytes.fromhex('9700')),
                             (0x40, bytes.fromhex('00019700')),
                             (0x40, bytes.fromhex('0002')), (0x40, b'')):
            with self.subTest(header=header, body=body):
                with self.assertRaises(AssertionError) as raised:
                    validate_ack(header, body, 1)
                message = str(raised.exception)
                self.assertIn(f'header=0x{header:02x}', message)
                self.assertIn(f'body={body.hex()}', message)
                self.assertIn('id=1', message)
                if 0x97 in body:
                    self.assertIn('reason=0x97', message)

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
    def __init__(self, reader, workload, subscriber_readers=None, duplicate=False, reorder=False):
        self.reader, self.workload = reader, workload
        self.subscriber_readers = subscriber_readers
        self.duplicate = duplicate
        self.reorder, self.held = reorder, None
        self.deferred = None
        self.publication_writes = 0

    def write(self, packet):
        if packet == b'\xc0\x00':
            self.reader.feed_data(b'\xd0\x00')
            return
        assert packet[0] == 0x32
        self.publication_writes += 1
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
        if self.deferred is not None:
            self.deferred.append((outbound, tuple(self.workload.recipients(sequence))))
            return
        if self.reorder:
            if self.held is None:
                self.held = outbound
                return
            for index in self.workload.recipients(sequence):
                self.subscriber_readers[index].feed_data(outbound + self.held)
            self.held = None
            return
        for index in self.workload.recipients(sequence):
            self.subscriber_readers[index].feed_data(outbound)
            if self.duplicate:
                self.subscriber_readers[index].feed_data(outbound)

    async def drain(self):
        await asyncio.sleep(0)


class AccountingTests(unittest.IsolatedAsyncioTestCase):
    def setup_streams(self, case, duplicate=False, reorder=False):
        workload = Workload(case, True)
        readers = [asyncio.StreamReader() for _ in range(workload.subscribers)]
        subscribers = [(reader, SyntheticWriter(reader, workload)) for reader in readers]
        publishers = []
        for _ in range(workload.publishers):
            reader = asyncio.StreamReader()
            publishers.append((reader, SyntheticWriter(reader, workload, readers, duplicate, reorder)))
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

    async def test_delivery_gate_waits_for_ack_and_every_expected_recipient(self):
        workload, subscribers, publishers = self.setup_streams('hot')
        for _, writer in publishers:
            writer.deferred = []
        task = asyncio.create_task(phase(workload, subscribers, publishers, 6, 256, 2,
                                         flow_control='delivery'))
        async def until(predicate):
            while not predicate():
                await asyncio.sleep(0)
        try:
            await asyncio.wait_for(until(lambda: all(writer.deferred for _, writer in publishers)), 1)
            for _ in range(10):
                await asyncio.sleep(0)
            self.assertEqual([writer.publication_writes for _, writer in publishers], [1, 1])
            writer = publishers[0][1]
            packet, recipients = writer.deferred.pop(0)
            for recipient in recipients[:-1]:
                subscribers[recipient][0].feed_data(packet)
            for _ in range(10):
                await asyncio.sleep(0)
            self.assertEqual(writer.publication_writes, 1, 'one missing subscriber must keep gate closed')
            subscribers[recipients[-1]][0].feed_data(packet)
            await asyncio.wait_for(until(lambda: writer.publication_writes == 2), 1)
            self.assertEqual(publishers[1][1].publication_writes, 1)
            for _, writer in publishers:
                held = writer.deferred
                writer.deferred = None
                for packet, recipients in held:
                    for recipient in recipients:
                        subscribers[recipient][0].feed_data(packet)
            result = await asyncio.wait_for(task, 1)
            self.assertEqual(result['flow_control'], 'delivery')
            self.assertEqual(result['publications'], 6)
            self.assertEqual(result['deliveries'], 24)
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_same_topic_publisher_reordering_fails(self):
        workload, subscribers, publishers = self.setup_streams('hot', reorder=True)
        with self.assertRaises(ExceptionGroup) as raised:
            await asyncio.wait_for(phase(workload, subscribers, publishers, 20, 256, 2), 2)
        self.assertIn('order', str(raised.exception.exceptions))

    async def test_duplicate_is_failure_not_silently_dropped_sample(self):
        workload, subscribers, publishers = self.setup_streams('hot', duplicate=True)
        with self.assertRaises(ExceptionGroup):
            await asyncio.wait_for(phase(workload, subscribers, publishers, 17, 256, 2), 2)


class CleanupAndStatusTests(unittest.IsolatedAsyncioTestCase):
    async def test_first_case_failure_overwrites_stale_success(self):
        with tempfile.TemporaryDirectory(prefix='rmqtt-bench-status-') as directory:
            binary = Path(directory) / 'broker'
            binary.write_bytes(b'fake binary, no launch')
            output = Path(directory) / 'result.json'
            output.write_text(json.dumps({'campaign_status': 'passed', 'runs': ['stale']}))
            arguments = SimpleNamespace(binary=str(binary), output=str(output), storage='memory',
                                        cases='concurrent', repetitions=1, timeout=1)
            with mock.patch('run.environment', return_value={}), mock.patch('run.run_one',
                    new=mock.AsyncMock(side_effect=RuntimeError('injected failure'))):
                with self.assertRaisesRegex(RuntimeError, 'injected failure'):
                    await campaign(arguments)
            saved = json.loads(output.read_text())
            self.assertEqual(saved['campaign_status'], 'running')
            self.assertEqual(saved['runs'], [])

    async def test_ps_timeout_kills_and_reaps_child(self):
        process = mock.Mock(returncode=None)
        process.communicate = mock.AsyncMock()
        process.wait = mock.AsyncMock()
        async def timed_out(awaitable, timeout):
            awaitable.close()
            raise TimeoutError('injected timeout')
        with mock.patch('metrics.asyncio.create_subprocess_exec', new=mock.AsyncMock(return_value=process)), \
                mock.patch('metrics.asyncio.wait_for', side_effect=timed_out):
            with self.assertRaises(TimeoutError):
                await process_sample(123, 'active')
        process.kill.assert_called_once()
        process.wait.assert_awaited_once()

    async def test_ps_cancellation_kills_and_reaps_child(self):
        started = asyncio.Event()
        process = mock.Mock(returncode=None)
        async def blocked():
            started.set()
            await asyncio.Event().wait()
        process.communicate = blocked
        process.wait = mock.AsyncMock()
        with mock.patch('metrics.asyncio.create_subprocess_exec', new=mock.AsyncMock(return_value=process)):
            task = asyncio.create_task(process_sample(123, 'active'))
            await asyncio.wait_for(started.wait(), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        process.kill.assert_called_once()
        process.wait.assert_awaited_once()


if __name__ == '__main__':
    unittest.main(verbosity=2)
