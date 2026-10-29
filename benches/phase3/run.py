#!/usr/bin/env python3
"""Bounded, exact-count MQTT5 loopback baseline; see README before interpreting rates."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import socket
import struct
import subprocess
import sys
import tempfile
import time

from diagnostics import (Diagnostics, drain_stderr, error_summary, profile_environment, read_profile, stop_broker)
from metrics import Sampler, distribution, summarize
from wire import connect, frame, parse_publication, publication, receive, subscribe, validate_ack

if not __debug__:
    raise RuntimeError('benchmark correctness requires Python assertions enabled')

ROOT = Path(__file__).resolve().parents[2]


def captured(command):
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=5, check=True)
        return result.stdout.strip()
    except (OSError, subprocess.SubprocessError) as error:
        return {'unavailable': (getattr(error, 'stderr', None) or str(error)).strip()}


def cpu_model():
    if sys.platform == 'darwin':
        return captured(['sysctl', '-n', 'machdep.cpu.brand_string'])
    if sys.platform.startswith('linux'):
        try:
            with Path('/proc/cpuinfo').open(encoding='utf-8', errors='replace') as cpuinfo:
                text = cpuinfo.read(65536)
        except OSError as error:
            return {'unavailable': f'cannot read /proc/cpuinfo: {error}'}
        for line in text.splitlines():
            key, _, value = line.partition(':')
            if key.strip().casefold() == 'model name' and value.strip():
                return value.strip()
        return {'unavailable': 'no model name in first65536 characters of /proc/cpuinfo; architecture is recorded separately'}
    return {'unavailable': f'CPU model lookup unsupported on {sys.platform}; architecture is recorded separately'}


def environment(binary, arguments):
    return {'os': platform.platform(), 'uname': list(platform.uname()), 'python': sys.version,
            'rustc': captured(['rustc', '-Vv']), 'cargo': captured(['cargo', '-V']),
            'cpu_logical': os.cpu_count(), 'cpu_model': cpu_model(),
            'ram_bytes': captured(['sysctl', '-n', 'hw.memsize']) if sys.platform == 'darwin'
            else os.sysconf('SC_PHYS_PAGES') * os.sysconf('SC_PAGE_SIZE'),
            'ram_note': 'bytes; unavailable result retains the command failure reason',
            'fd_soft_hard': list(resource.getrlimit(resource.RLIMIT_NOFILE)),
            'binary': str(binary), 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
            'driver_source_revision': captured(['git', '-C', str(ROOT), 'rev-parse', 'HEAD']),
            'artifact_source_revision': arguments.artifact_source_revision or 'not provided; never inferred from driver revision',
            'build_profile': arguments.build_profile, 'command': [sys.executable, *sys.argv],
            'clock': 'time.monotonic_ns, one host for send and receive',
            'percentiles': 'nearest rank ceil(p*N), no dropped samples; milliseconds',
            'rss_method': 'broker PID only: ps rss KiB *1024, sampled peak only; client RSS excluded',
            'cpu_method': 'ps process cumulative time; end minus pre-measure sample, not utilization',
            'sample_interval_seconds': arguments.sample_interval,
            'sampling_note': 'nominal wait after each ps call; raw timestamps show actual spacing',
            'storage': 'memory volatile or fresh SQLite WAL/synchronous FULL via broker; no power-loss claim'}


class Workload:
    def __init__(self, case, smoke):
        self.case = case
        self.subscribers = (4 if smoke else 64) if case != 'hot' else (4 if smoke else 32)
        self.publishers = (4 if smoke else 64) if case == 'concurrent' else (2 if smoke else 8)
        self.filters_per_subscriber = (4 if smoke else 64) if case == 'many' else 1

    def topic(self, sequence):
        if self.case == 'hot':
            return 'bench/hot'
        subscriber = sequence % self.subscribers
        suffix = (sequence // self.subscribers) % self.filters_per_subscriber
        return f'bench/{self.case}/{subscriber}/{suffix}'

    def filters(self, subscriber):
        if self.case == 'hot':
            return ['bench/hot']
        return [f'bench/{self.case}/{subscriber}/{index}' for index in range(self.filters_per_subscriber)]

    def recipients(self, sequence):
        return range(self.subscribers) if self.case == 'hot' else (sequence % self.subscribers,)

    def description(self):
        return {'publishers': self.publishers, 'subscribers': self.subscribers,
                'simultaneous_connected_clients': self.publishers + self.subscribers,
                'subscriptions': self.subscribers * self.filters_per_subscriber,
                'filters_per_subscriber': self.filters_per_subscriber,
                'distribution': 'single shared hot topic' if self.case == 'hot' else
                'round-robin subscriber; filter index=floor(sequence/subscribers) mod filters_per_subscriber',
                'ingress_qos': 1, 'subscription_qos': 0, 'outstanding_per_publisher': 1}


def payload(identifier, size):
    return struct.pack('!Q', identifier) + b'P' * (size - 8)


async def barrier(subscribers, diagnostics=None):
    async def one(connection):
        reader, writer = connection
        writer.write(frame(0xc0)); await writer.drain()
        header, body = await receive(reader)
        if (header, body) != (0xd0, b'') and diagnostics is not None:
            diagnostics.packet_error('barrier', header, body)
        assert (header, body) == (0xd0, b''), f'extra frame before PING barrier: {header:#x}/{body.hex()}'
    async with asyncio.TaskGroup() as group:
        for connection in subscribers:
            group.create_task(one(connection))


async def phase(workload, subscribers, publishers, messages, payload_size, phase_index, flow_control='ack', diagnostics=None):
    assert flow_control in ('ack', 'delivery')
    expected = [set() for _ in subscribers]
    for sequence in range(messages):
        for subscriber in workload.recipients(sequence):
            expected[subscriber].add(sequence)
    send_times = [None] * messages
    delivery_latencies, ack_latencies = [], []
    phase_name = 'warmup' if phase_index == 1 else 'measured'
    diagnostics = diagnostics or Diagnostics()
    diagnostics.begin_phase(phase_name, messages, sum(map(len, expected)))
    counters = diagnostics.phases[phase_name]
    offset = phase_index * 1_000_000
    receipts_remaining = [len(tuple(workload.recipients(sequence))) for sequence in range(messages)]
    delivery_complete = [asyncio.Event() for _ in range(messages)] if flow_control == 'delivery' else None

    async def consume(index, reader):
        remaining = set(expected[index])
        last_by_topic_publisher = {}
        while remaining:
            header, body = await receive(reader)
            arrival = time.monotonic_ns()
            try:
                topic, data = parse_publication(header, body)
                assert len(data) == payload_size
                identifier = struct.unpack('!Q', data[:8])[0]
                sequence = identifier - offset
                assert sequence in remaining, f'unexpected/duplicate delivery {index}:{identifier}'
                assert topic == workload.topic(sequence) and data == payload(identifier, payload_size)
                key = (topic, sequence % len(publishers))
                assert sequence > last_by_topic_publisher.get(key, -1), f'publisher/topic order violation {index}:{key}:{sequence}'
                last_by_topic_publisher[key] = sequence
                sent = send_times[sequence]
                assert sent is not None
            except Exception:
                diagnostics.packet_error('delivery', header, body, subscriber=index)
                raise
            delivery_latencies.append(arrival - sent)
            counters['received'] += 1
            remaining.remove(sequence)
            receipts_remaining[sequence] -= 1
            assert receipts_remaining[sequence] >= 0
            if delivery_complete is not None and receipts_remaining[sequence] == 0:
                delivery_complete[sequence].set()

    async def produce(index, connection):
        reader, writer = connection
        for sequence in range(index, messages, len(publishers)):
            identifier = sequence + 1
            packet = publication(workload.topic(sequence), identifier,
                                 payload(offset + sequence, payload_size))
            sent = time.monotonic_ns()
            send_times[sequence] = sent
            counters['issued'] += 1
            counters['expected_receipts_for_issued'] += len(tuple(workload.recipients(sequence)))
            diagnostics.publishers[str(index)] = dict(publisher=index, sequence=sequence, packet_id=identifier, state='await_ack', sent_monotonic_ns=sent)
            writer.write(packet)
            await writer.drain()
            header, body = await receive(reader)
            arrived = time.monotonic_ns()
            try:
                validate_ack(header, body, identifier)
            except Exception:
                diagnostics.packet_error('ack', header, body, publisher=index, sequence=sequence, packet_id=identifier)
                raise
            counters['acknowledged'] += 1
            counters['expected_receipts_for_acked'] += len(tuple(workload.recipients(sequence)))
            ack_latencies.append(arrived - sent)
            if delivery_complete is not None:
                diagnostics.publishers[str(index)]['state'] = 'await_delivery'
                await delivery_complete[sequence].wait()
            diagnostics.publishers[str(index)]['state'] = 'complete'

    start = time.monotonic_ns()
    async with asyncio.TaskGroup() as group:
        for index, (reader, _) in enumerate(subscribers):
            group.create_task(consume(index, reader))
        for index, connection in enumerate(publishers):
            group.create_task(produce(index, connection))
    end = time.monotonic_ns()
    deliveries = sum(map(len, expected))
    assert len(ack_latencies) == messages and len(delivery_latencies) == deliveries
    assert all(timestamp is not None for timestamp in send_times)
    assert receipts_remaining == [0] * messages
    diagnostics.stage = phase_name + '_barrier'
    await barrier(subscribers, diagnostics)
    return {'flow_control': flow_control, 'publisher_gate': 'PUBACK and all expected subscriber receipts'
            if flow_control == 'delivery' else 'PUBACK only; recipient backlog may reach broker admission limits',
            'publications': messages, 'deliveries': deliveries, 'acknowledgements': len(ack_latencies),
            'duration_seconds': (end - start) / 1e9,
            'ingress_messages_per_second': messages * 1e9 / (end - start),
            'egress_deliveries_per_second': deliveries * 1e9 / (end - start),
            'delivery_latency': distribution(delivery_latencies), 'ack_latency': distribution(ack_latencies),
            'measurement_start_monotonic_ns': start, 'measurement_end_monotonic_ns': end,
            'correctness': 'every expected subscriber/sequence/payload/topic checked once and publisher/topic ordered; all PUBACKs validated; postphase PING barriers reject extra preceding frames'}


async def ready(process, port):
    for _ in range(250):
        if process.returncode is not None:
            raise RuntimeError(f'broker exited {process.returncode}')
        try:
            _, writer = await asyncio.open_connection('127.0.0.1', port)
            writer.close(); await writer.wait_closed()
            return
        except OSError:
            await asyncio.sleep(.02)
    raise TimeoutError('broker readiness timeout')


async def run_one(arguments, binary, case, storage, repetition, diagnostics=None):
    diagnostics = diagnostics or Diagnostics()
    diagnostics.context.update(case=case, storage=storage, repetition=repetition, binary=str(binary),
                               binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                               flow_control=arguments.flow_control, pilot=getattr(arguments, 'pilot', False))
    workload = Workload(case, arguments.smoke)
    with tempfile.TemporaryDirectory(prefix='rmqtt-p3-bench-') as directory:
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0)); port = probe.getsockname()[1]
        command = [str(binary), '--listen', f'127.0.0.1:{port}', '--protocol', '5',
                   '--max-connections', '256', '--max-sessions', '256', '--max-subscriptions', '128',
                   '--max-inflight', '64', '--outbound-capacity', str(arguments.outbound_capacity), '--max-packet-size', '8192',
                   '--max-state-bytes', '67108864', '--max-retained', '16', '--topic-alias-maximum', '0',
                   '--handshake-timeout-ms', '10000', '--write-timeout-ms', str(getattr(arguments, 'broker_write_timeout_ms', 10000))]
        if storage == 'sqlite':
            command += ['--storage-path', str(Path(directory) / 'sessions.sqlite')]
        diagnostics.context.update(broker_command=command, topology=workload.description())
        profile_enabled = getattr(arguments, 'profile', False)
        profile_path = Path(directory)/'worker-profile.tsv'
        connections, process, sampling_task, stderr_task = [], None, None, None
        result = None
        primary_error = None
        sampler = None
        try:
            process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE, env=profile_environment(profile_enabled, profile_path))
            stderr_task = asyncio.create_task(drain_stderr(process.stderr, diagnostics))
            await ready(process, port)
            sampler = Sampler(process.pid, arguments.sample_interval)
            diagnostics.rss_samples = sampler.samples
            await sampler.once('idle')
            for index in range(workload.subscribers):
                connection = await connect(port, f'subscriber-{index}', arguments.session_expiry_seconds)
                connections.append(connection)
                await subscribe(*connection, workload.filters(index))
            subscribers = list(connections)
            for index in range(workload.publishers):
                connections.append(await connect(port, f'publisher-{index}', arguments.session_expiry_seconds))
            publishers = connections[len(subscribers):]
            diagnostics.stage = 'setup_barrier'
            await barrier(subscribers, diagnostics)
            await sampler.once('setup')
            await phase(workload, subscribers, publishers, arguments.warmup, arguments.payload_size, 1, arguments.flow_control, diagnostics)
            before = await sampler.once('pre_measure')
            sampling_task = asyncio.create_task(sampler.run())
            result = await phase(workload, subscribers, publishers, arguments.messages, arguments.payload_size, 2, arguments.flow_control, diagnostics)
            sampler.stop.set(); await sampling_task
            after = await sampler.once('end')
            result.update(case=case, storage=storage, repetition=repetition, smoke=arguments.smoke,
                          topology=workload.description(), broker_command=command, payload_bytes=arguments.payload_size,
                          pilot=getattr(arguments, 'pilot', False),
                          warmup_publications=arguments.warmup, session_expiry_seconds=arguments.session_expiry_seconds,
                          session_note='CleanStart1; zero expiry is clean routing, positive expiry retains sessions; QoS0 live delivery is not offline durability throughput', memory={'samples': sampler.samples,
                          'sampled_peak_rss_bytes': max(sample['rss_bytes'] for sample in sampler.samples),
                          'note': 'peak over idle/setup/premeasure/periodic/end ps samples, not true process high-water mark'},
                          cpu_seconds_observed_window=after['cpu_seconds'] - before['cpu_seconds'])
            assert process.returncode is None, 'broker died during run'
            diagnostics.stage = 'completed'
        except BaseException as error:
            primary_error = error
            diagnostics.context['error_leaves'] = error_summary(error)
            raise
        finally:
            if sampler:
                sampler.stop.set()
            if sampling_task and not sampling_task.done():
                sampling_task.cancel()
                await asyncio.gather(sampling_task, return_exceptions=True)
            cleanup_errors = []
            for _, writer in connections:
                try:
                    writer.close()
                except Exception as error:
                    cleanup_errors.append(error)
            try:
                await asyncio.wait_for(asyncio.gather(*(writer.wait_closed() for _, writer in connections),
                                                      return_exceptions=True), 5)
            except Exception as error:
                cleanup_errors.append(error)
            if process:
                try:
                    await stop_broker(process)
                except Exception as error:
                    cleanup_errors.append(error)
            if stderr_task:
                try:
                    await asyncio.wait_for(stderr_task, 5)
                except Exception as error:
                    cleanup_errors.append(error)
                    stderr_task.cancel()
                    await asyncio.gather(stderr_task, return_exceptions=True)
            if cleanup_errors:
                diagnostics.context['cleanup_errors'] = [leaf for error in cleanup_errors
                                                         for leaf in error_summary(error)][:16]
                if primary_error is None:
                    raise cleanup_errors[0]
            diagnostics.profile = read_profile(profile_path, profile_enabled)
            if result is not None:
                result['profiling'] = diagnostics.profile
                result['stderr'] = diagnostics.stderr
                if profile_enabled and (diagnostics.profile['status'] != 'captured' or not diagnostics.profile.get('complete')):
                    raise RuntimeError('requested profiling report unavailable or invalid: ' + str(diagnostics.profile))
        return result


async def campaign(arguments):
    binary = Path(arguments.binary).resolve(strict=True)
    result = {'schema_version': 1, 'scope': 'bounded loopback baseline; not saturation/WAN/production SLA',
              'environment': environment(binary, arguments), 'parameters': vars(arguments), 'runs': [], 'summary': [],
              'campaign_status': 'running', 'failures': [],
              'pilot': getattr(arguments, 'pilot', False)}
    output = Path(arguments.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + '\n')
    for storage in arguments.storage.split(','):
        for case in arguments.cases.split(','):
            for repetition in range(1, arguments.repetitions + 1):
                diagnostics = Diagnostics()
                diagnostics.context.update(case=case, storage=storage, repetition=repetition)
                try:
                    run = await asyncio.wait_for(run_one(arguments, binary, case, storage, repetition,
                                                         diagnostics=diagnostics), arguments.timeout)
                except BaseException as error:
                    cancelled = isinstance(error, asyncio.CancelledError)
                    failure = diagnostics.snapshot()
                    failure['failure'] = dict(kind='cancelled' if cancelled else 'timeout' if isinstance(error, TimeoutError) else 'error',
                                              leaves=error_summary(error))
                    result['failures'].append(failure)
                    result['campaign_status'] = 'cancelled' if cancelled else 'failed'
                    output.write_text(json.dumps(result, indent=2) + '\n')
                    raise
                result['runs'].append(run)
                result['summary'] = summarize(result['runs'])
                output.write_text(json.dumps(result, indent=2) + '\n')
                print(json.dumps({key: run[key] for key in ('case', 'storage', 'repetition', 'publications',
                                  'deliveries', 'duration_seconds', 'ingress_messages_per_second')}, sort_keys=True), flush=True)
    result['campaign_status'] = 'passed'
    output.write_text(json.dumps(result, indent=2) + '\n')
    return result


def parse_arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--artifact-source-revision')
    parser.add_argument('--build-profile', default='release')
    parser.add_argument('--cases', default='concurrent,hot,many')
    parser.add_argument('--storage', default='memory,sqlite')
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--messages', type=int, default=4096)
    parser.add_argument('--warmup', type=int, default=128)
    parser.add_argument('--payload-size', type=int, default=256)
    parser.add_argument('--session-expiry-seconds', type=int, default=0)
    parser.add_argument('--flow-control', choices=('ack', 'delivery'), default='ack')
    parser.add_argument('--outbound-capacity', type=int, default=256)
    parser.add_argument('--sample-interval', type=float, default=.05)
    parser.add_argument('--timeout', type=float, default=180)
    parser.add_argument('--profile', action='store_true', help='opt-in worker/session counters; not formal timing')
    parser.add_argument('--pilot', action='store_true', help='small campaign with full topology; not formal acceptance')
    parser.add_argument('--broker-write-timeout-ms', type=int, default=10000)
    parser.add_argument('--smoke', action='store_true', help='small topology; never a formal acceptance run')
    arguments = parser.parse_args()
    assert set(arguments.cases.split(',')) <= {'concurrent', 'hot', 'many'}
    assert set(arguments.storage.split(',')) <= {'memory', 'sqlite'}
    assert 1 <= arguments.repetitions <= 5 and 1 <= arguments.messages <= 10000
    assert 0 <= arguments.session_expiry_seconds <= 86400
    assert 1 <= arguments.outbound_capacity <= 4096
    assert 1 <= arguments.warmup <= 1000 and 8 <= arguments.payload_size <= 4096
    assert .01 <= arguments.sample_interval <= 1 and 1 <= arguments.timeout <= 600
    assert not (arguments.smoke and arguments.pilot)
    assert 1 <= arguments.broker_write_timeout_ms <= 600000
    if not arguments.smoke and not arguments.pilot:
        assert arguments.messages >= 2000 and arguments.repetitions >= 3
    return arguments


if __name__ == '__main__':
    asyncio.run(campaign(parse_arguments()))
