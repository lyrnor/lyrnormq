#!/usr/bin/env python3
"""Connection-scale benchmark: N MQTT5 clients connect at a bounded rate, hold
with keepalive PINGs, then disconnect. Every failure is counted by kind; no
throughput or latency is reported for a failed phase."""
import argparse
import asyncio
import hashlib
import json
import platform
import random
import resource
import socket
import struct
import sys
import tempfile
import time
from pathlib import Path

from diagnostics import Diagnostics, drain_stderr, error_summary, stop_broker
from metrics import Sampler, distribution
from run import ready
from wire import MAX_FRAME, frame, receive, string

FD_HEADROOM = 256


def connect_packet(client, keepalive, session_expiry):
    properties = b'\x05\x11' + struct.pack('!I', session_expiry) if session_expiry else b'\x00'
    flags = 0x02 if not session_expiry else 0x00
    return frame(0x10, string('MQTT') + bytes([5, flags]) + struct.pack('!H', keepalive)
                 + properties + string(client))


def source_address(index, sources):
    return (f'127.0.0.{1 + index % sources}', 0) if sources > 1 else None


def raise_fd_limit(needed):
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft == resource.RLIM_INFINITY or soft >= needed:
        return soft
    target = needed if hard == resource.RLIM_INFINITY else min(hard, needed)
    resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
    if target < needed:
        raise RuntimeError(f'file descriptor limit {target} below required {needed}; raise the hard limit')
    return target


class Client:
    def __init__(self, index):
        self.index = index
        self.reader = self.writer = None

    async def connect(self, arguments, port, failures, latencies):
        started = time.monotonic_ns()
        try:
            self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection(
                '127.0.0.1', port, limit=MAX_FRAME * 2,
                local_addr=source_address(self.index, arguments.source_addresses)), arguments.connect_timeout)
            self.writer.write(connect_packet(f'conn-{self.index}', arguments.keepalive, arguments.session_expiry))
            await self.writer.drain()
            header, body = await asyncio.wait_for(receive(self.reader), arguments.connect_timeout)
        except TimeoutError:
            failures['connect_timeout'] = failures.get('connect_timeout', 0) + 1
            return False
        except (OSError, asyncio.IncompleteReadError) as error:
            kind = 'connect_' + type(error).__name__
            failures[kind] = failures.get(kind, 0) + 1
            return False
        if header != 0x20 or len(body) < 3 or body[1] != 0:
            kind = f'connack_0x{header:02x}_reason_0x{body[1] if len(body) > 1 else 0:02x}'
            failures[kind] = failures.get(kind, 0) + 1
            return False
        latencies.append(time.monotonic_ns() - started)
        return True

    async def hold(self, arguments, until, failures, latencies):
        interval = arguments.keepalive / 2
        await asyncio.sleep(random.uniform(0, interval))
        while time.monotonic() < until:
            started = time.monotonic_ns()
            try:
                self.writer.write(frame(0xc0))
                await self.writer.drain()
                header, _ = await asyncio.wait_for(receive(self.reader), arguments.keepalive)
            except TimeoutError:
                failures['ping_timeout'] = failures.get('ping_timeout', 0) + 1
                return
            except (OSError, asyncio.IncompleteReadError) as error:
                kind = 'ping_' + type(error).__name__
                failures[kind] = failures.get(kind, 0) + 1
                return
            if header != 0xd0:
                failures[f'ping_unexpected_0x{header:02x}'] = failures.get(f'ping_unexpected_0x{header:02x}', 0) + 1
                return
            latencies.append(time.monotonic_ns() - started)
            await asyncio.sleep(min(interval, max(0, until - time.monotonic())))

    async def close(self):
        if self.writer is None:
            return
        try:
            self.writer.write(frame(0xe0, b'\x00'))
            await asyncio.wait_for(self.writer.drain(), 5)
        except (OSError, TimeoutError):
            pass
        writer, self.writer = self.writer, None
        writer.close()
        try:
            await asyncio.wait_for(writer.wait_closed(), 5)
        except (OSError, TimeoutError):
            pass


async def ramp(arguments, port):
    clients = [Client(index) for index in range(arguments.connections)]
    failures, latencies = {}, []
    limit = asyncio.Semaphore(arguments.concurrency)
    started = time.monotonic()

    async def one(client):
        if arguments.rate:
            await asyncio.sleep(max(0, started + client.index / arguments.rate - time.monotonic()))
        async with limit:
            return await client.connect(arguments, port, failures, latencies)

    results = await asyncio.gather(*(one(client) for client in clients))
    elapsed = time.monotonic() - started
    connected = [client for client, ok in zip(clients, results) if ok]
    return clients, connected, failures, latencies, elapsed


async def one_wave(arguments, port, sampler, process, result, wave):
    index = wave['index']
    result['phase'] = f'connect-{index}'
    clients, connected, failures, latencies, elapsed = await asyncio.wait_for(
        ramp(arguments, port), arguments.timeout)
    wave['connect'] = {'requested': arguments.connections, 'connected': len(connected),
                       'failures': failures, 'elapsed_seconds': elapsed,
                       'connections_per_second': len(connected) / elapsed if elapsed else None,
                       'latency': distribution(latencies) if latencies else None}
    wave['connected'] = await sampler.once(f'connected-{index}')
    if failures:
        raise RuntimeError(f'wave {index} connect failures: {failures}')
    result['phase'] = f'hold-{index}'
    ping_failures, ping_latencies = {}, []
    until = time.monotonic() + arguments.hold
    await asyncio.wait_for(asyncio.gather(*(client.hold(arguments, until, ping_failures, ping_latencies)
                                            for client in connected)), arguments.hold + arguments.timeout)
    wave['hold'] = {'seconds': arguments.hold, 'keepalive_seconds': arguments.keepalive,
                    'failures': ping_failures, 'pings': len(ping_latencies),
                    'latency': distribution(ping_latencies) if ping_latencies else None}
    wave['held'] = await sampler.once(f'held-{index}')
    if process.returncode is not None:
        raise RuntimeError(f'broker exited {process.returncode} during wave {index}')
    if ping_failures:
        raise RuntimeError(f'wave {index} keepalive failures: {ping_failures}')
    grown = wave['held']['rss_bytes'] - result['idle']['rss_bytes']
    wave['rss_bytes_per_connection'] = grown / len(connected)
    return clients


async def measure(arguments, binary, directory, result):
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    capacity = arguments.connections + 16
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'tests'))
    from lyrnormqcfg import argv
    command = argv(binary, f'127.0.0.1:{port}', directory, [
        '--protocol', '5',
        '--max-connections', str(capacity),
        '--max-sessions', str(capacity),
        '--max-handshakes', str(capacity),
        '--max-auth-tasks', str(capacity),
        '--max-state-bytes', str(max(67108864, capacity * 16384)),
    ])
    evidence = Diagnostics()
    result.update(broker_command=command, phase='startup')
    process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.DEVNULL,
                                                   stderr=asyncio.subprocess.PIPE)
    stderr_task = asyncio.create_task(drain_stderr(process.stderr, evidence))
    clients = []
    try:
        await ready(process, port)
        sampler = Sampler(process.pid, 1)
        result['idle'] = await sampler.once('idle')
        result['waves'] = []
        for index in range(arguments.waves):
            wave = {'index': index}
            result['waves'].append(wave)
            clients = await one_wave(arguments, port, sampler, process, result, wave)
            await asyncio.gather(*(client.close() for client in clients), return_exceptions=True)
            await asyncio.sleep(arguments.settle)
            wave['closed'] = await sampler.once(f'closed-{index}')
        first = result['waves'][0]
        result.update(connect=first['connect'], connected=first['connected'], hold=first['hold'],
                      held=first['held'], closed=first['closed'],
                      rss_bytes_per_connection=first['rss_bytes_per_connection'])
        result['phase'] = 'completed'
    finally:
        await asyncio.gather(*(client.close() for client in clients), return_exceptions=True)
        await stop_broker(process)
        await stderr_task
        result['broker_returncode'] = process.returncode
        result['stderr'] = evidence.stderr


async def main(arguments):
    binary = Path(arguments.binary).resolve()
    descriptors = raise_fd_limit(arguments.connections + FD_HEADROOM)
    record = {'benchmark': 'phase4-connections', 'smoke': arguments.smoke,
              'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
              'platform': platform.platform(), 'python': sys.version.split()[0],
              'fd_limit': descriptors, 'arguments': {key: value for key, value in vars(arguments).items()},
              'campaign_status': 'failed'}
    output = Path(arguments.output)
    try:
        with tempfile.TemporaryDirectory(prefix='lyrnormq-connections-') as directory:
            record['result'] = {}
            await measure(arguments, binary, directory, record['result'])
        record['campaign_status'] = 'completed'
    except BaseException as error:
        record['error'] = error_summary(error)
        raise
    finally:
        output.write_text(json.dumps(record, indent=2) + '\n')
    return record


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--connections', type=int, default=10000)
    parser.add_argument('--rate', type=float, default=100, help='connects per second; 0 means unpaced')
    parser.add_argument('--concurrency', type=int, default=512, help='outstanding handshakes')
    parser.add_argument('--keepalive', type=int, default=60)
    parser.add_argument('--hold', type=float, default=120, help='seconds held after the ramp')
    parser.add_argument('--session-expiry', type=int, default=0)
    parser.add_argument('--storage', choices=('memory', 'sqlite'), default='memory')
    parser.add_argument('--source-addresses', type=int, default=1,
                        help='spread clients over 127.0.0.1..N (needs loopback aliases on macOS)')
    parser.add_argument('--connect-timeout', type=float, default=10)
    parser.add_argument('--timeout', type=float, default=1800, help='bound for the ramp phase')
    parser.add_argument('--waves', type=int, default=1,
                        help='connect/hold/disconnect rounds against one broker; RSS is sampled after each')
    parser.add_argument('--settle', type=float, default=5, help='seconds after a wave disconnects before sampling')
    parser.add_argument('--smoke', action='store_true', help='500 unpaced clients, 2s keepalive, 3s hold')
    arguments = parser.parse_args(argv)
    if arguments.smoke:
        arguments.connections, arguments.rate, arguments.keepalive, arguments.hold = 500, 0, 2, 3
    if not 1 <= arguments.connections <= 1_000_000:
        parser.error('--connections must be 1..1000000')
    if arguments.rate < 0 or not 1 <= arguments.concurrency <= 65536:
        parser.error('--rate must be >= 0 and --concurrency 1..65536')
    if not 1 <= arguments.keepalive <= 65535 or arguments.hold < 0:
        parser.error('--keepalive must be 1..65535 and --hold >= 0')
    if not 1 <= arguments.waves <= 20 or arguments.settle < 0:
        parser.error('--waves must be 1..20 and --settle >= 0')
    if not 1 <= arguments.source_addresses <= 254:
        parser.error('--source-addresses must be 1..254')
    if arguments.connections > 16000 * arguments.source_addresses:
        parser.error('each loopback source address supplies about 16000 ephemeral ports; '
                     'raise --source-addresses')
    return arguments


if __name__ == '__main__':
    asyncio.run(main(parse_arguments()))
