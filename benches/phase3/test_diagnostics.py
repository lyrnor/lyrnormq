#!/usr/bin/env python3
"""Failure evidence regressions; no production process is required."""
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
from unittest import mock
import unittest

import run
import test_driver
from wire import frame
from diagnostics import drain_stderr, profile_environment, read_profile, stop_broker


class FailureEvidenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_actual_ack_packet_sender_sequence_and_partial_counts_survive(self):
        workload, subscribers, publishers = test_driver.AccountingTests().setup_streams('hot')
        original = publishers[0][1].write
        def rejected(packet):
            original(packet)
            publishers[0][0]._buffer.clear()
            publishers[0][0].feed_data(frame(0xe0, b'\x97'))
        publishers[0][1].write = rejected
        evidence = run.Diagnostics()
        with self.assertRaises(ExceptionGroup):
            await run.phase(workload, subscribers, publishers, 8, 256, 2, diagnostics=evidence)
        record = evidence.snapshot()
        self.assertEqual(record['stage'], 'measured')
        self.assertGreaterEqual(record['phases']['measured']['issued'], 1)
        packet = record['errors'][0]
        self.assertEqual((packet['publisher'], packet['sequence'], packet['packet_id']), (0, 0, 1))
        self.assertEqual((packet['header'], packet['body_hex'], packet['reason']), (224, '97', 151))
        self.assertEqual(record['phases']['measured']['expected_deliveries'], 32)
        counters=record['phases']['measured']
        self.assertEqual(counters['expected_receipts_for_issued'],4*counters['issued'])
        self.assertEqual(counters['expected_receipts_for_acked'],4*counters['acknowledged'])

    async def test_timeout_writes_failed_status_context_without_successful_tps(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory)/'broker'; binary.write_bytes(b'fake')
            output = Path(directory)/'result.json'
            arguments = SimpleNamespace(binary=str(binary), output=str(output), storage='memory',
                                        cases='hot', repetitions=1, timeout=.01)
            async def blocked(*args, diagnostics=None, **kwargs):
                diagnostics.context.update(broker_command=['fake'], binary_sha256='fixture')
                diagnostics.stage = 'warmup'
                diagnostics.phases['warmup'] = {'issued': 3, 'acknowledged': 2, 'received': 4}
                await asyncio.Event().wait()
            with mock.patch('run.environment', return_value={}), mock.patch('run.run_one', side_effect=blocked):
                with self.assertRaises(TimeoutError): await run.campaign(arguments)
            result = json.loads(output.read_text())
            self.assertEqual(result['campaign_status'], 'failed')
            self.assertEqual(result['runs'], [])
            self.assertEqual(result['summary'], [])
            failure = result['failures'][0]
            self.assertEqual(failure['failure']['kind'], 'timeout')
            self.assertEqual(failure['stage'], 'warmup')
            self.assertEqual(failure['phases']['warmup']['acknowledged'], 2)
            self.assertNotIn('ingress_messages_per_second', failure)

    async def test_external_cancellation_is_preserved_and_written(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory)/'broker';binary.write_bytes(b'fake')
            output = Path(directory)/'result.json'
            arguments = SimpleNamespace(binary=str(binary),output=str(output),storage='memory',
                                        cases='hot',repetitions=1,timeout=1)
            started=asyncio.Event()
            async def blocked(*args,diagnostics=None,**kwargs):
                started.set();diagnostics.stage='measured';await asyncio.Event().wait()
            with mock.patch('run.environment',return_value={}),mock.patch('run.run_one',side_effect=blocked):
                task=asyncio.create_task(run.campaign(arguments));await asyncio.wait_for(started.wait(),1);task.cancel()
                with self.assertRaises(asyncio.CancelledError):await task
            saved=json.loads(output.read_text())
            self.assertEqual(saved['campaign_status'],'cancelled')
            self.assertEqual(saved['failures'][0]['failure']['kind'],'cancelled')

    async def test_stderr_tail_is_bounded_and_complete_tail_is_preserved(self):
        reader = asyncio.StreamReader()
        content = b'A' * 70000 + b'last-error'
        reader.feed_data(content); reader.feed_eof()
        evidence = run.Diagnostics()
        await drain_stderr(reader, evidence)
        self.assertEqual(len(evidence.stderr['text'].encode()), 65536)
        self.assertEqual(evidence.stderr['total_bytes'], len(content))
        self.assertTrue(evidence.stderr['truncated'])
        self.assertTrue(evidence.stderr['text'].endswith('last-error'))

    async def test_sigint_flush_precedes_hard_kill_fallback(self):
        process = mock.Mock(returncode=None)
        process.wait = mock.AsyncMock(side_effect=[TimeoutError('still running'), 0])
        await stop_broker(process)
        import signal
        process.send_signal.assert_called_once_with(signal.SIGINT)
        process.kill.assert_called_once()
        self.assertEqual(process.wait.await_count, 2)

    def test_profile_is_explicitly_disabled_and_rows_are_validated(self):
        with mock.patch.dict('os.environ', {'LYRNORMQ_PROFILE': '1', 'LYRNORMQ_PROFILE_PATH': 'stale'}):
            env = profile_environment(False, 'unused')
            self.assertEqual(env['LYRNORMQ_PROFILE'], '0')
            self.assertNotIn('LYRNORMQ_PROFILE_PATH', env)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'profile.tsv'
            self.assertEqual(read_profile(path, True)['status'], 'unavailable')
            path.write_text('LYRNORMQ_PROFILE\t1\nSTATUS\tfinal\nREQUEST\tpublish\t2\t20\t1\t1\t0\n'
                            'SPAN\tproposal_clone\t2\t9\t0\nCOUNTER\tempty_poll_delivery\t3\n'
                            'SESSION_JSON\t{"enabled":true}\n')
            profile = read_profile(path, True)
            self.assertEqual(profile['status'], 'captured')
            self.assertEqual(profile['requests']['publish']['count'], 2)
            self.assertEqual(profile['spans']['proposal_clone']['total_ns'], 9)
            self.assertEqual(profile['counters']['empty_poll_delivery'], 3)
            path.write_text('LYRNORMQ_PROFILE\t1\nCOUNTER\tempty_poll_delivery\t-1\nSESSION_JSON\t{}\n')
            self.assertEqual(read_profile(path, True)['status'], 'invalid')

    def test_pilot_keeps_full_topology_and_formal_rules_remain(self):
        with mock.patch('sys.argv',['run','--binary','fixture','--output','/tmp/fixture','--pilot',
                                    '--messages','8','--repetitions','1']):
            arguments=run.parse_arguments()
        self.assertTrue(arguments.pilot)
        self.assertFalse(arguments.smoke)
        self.assertEqual(run.Workload('hot',arguments.smoke).subscribers,32)
        with mock.patch('sys.argv',['run','--binary','fixture','--output','/tmp/fixture',
                                    '--messages','8','--repetitions','1']):
            with self.assertRaises(AssertionError):run.parse_arguments()


if __name__=='__main__':unittest.main(verbosity=2)
