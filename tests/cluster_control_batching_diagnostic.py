#!/usr/bin/env python3
"""Finite, opt-in 900 CONNECT reproduction; no client retry or default test gate.

LYRNORMQ_BIN=/absolute/lyrnormq python3 tests/cluster_control_batching_diagnostic.py \
    --output /absolute/fresh-directory
Same absolute 1000/s schedule and 384-worker cap as the prior local diagnostic.
Worker congestion delays actual starts; report it rather than claiming target rate.
All accepted clients are held 1s after ramp drain, then checked with MQTT PING.
Three cohosted processes are always stopped, including failures during bootstrap.
"""
import argparse
import concurrent.futures
import json
import hashlib
import math
import os
from pathlib import Path
import platform
import socket
import time

from blackbox import frame, receive
from cluster_blackbox import BINARY, CLUSTER_TOKEN, TOKEN
from cluster_control_batching_blackbox import CleanupCombinedMemoryFleet
from mqtt5_blackbox import connect5, readprops

COUNT = 900
RATE = 1000
WORKERS = 384
LOG_LIMIT = 2 * 1024 * 1024
LOG_HALF = LOG_LIMIT // 2


def percentile(values, quantile):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * quantile) - 1)] if ordered else None


def capture_log(output, destination):
    """Keep complete small logs; large logs retain their first/last 1MiB.

    The source-byte capture bound is 2MiB; the explicit gap marker adds only a
    small fixed envelope. Never call Node.logs(), which drops the first cause.
    """
    size = 0
    truncated = False
    content = b''
    if output is not None:
        output.seek(0, os.SEEK_END)
        size = output.tell()
        output.seek(0)
        if size <= LOG_LIMIT:
            content = output.read(size)
        else:
            truncated = True
            head = output.read(LOG_HALF)
            output.seek(size - LOG_HALF)
            tail = output.read(LOG_HALF)
            marker = (f'\n\n--- LOG TRUNCATED: first {LOG_HALF} bytes above; '
                      f'omitted {size - LOG_LIMIT} bytes; '
                      f'last {LOG_HALF} bytes below ---\n\n').encode()
            content = head + marker + tail
    for secret in (CLUSTER_TOKEN, TOKEN):
        content = content.replace(secret.encode(), b'[redacted]')
    destination.write_bytes(content)
    return {'file': destination.name, 'source_bytes': size,
            'capture_limit_bytes': LOG_LIMIT, 'truncated': truncated,
            'saved_bytes': len(content)}


def run(root):
    fleet = CleanupCombinedMemoryFleet(root)
    sockets = [None] * COUNT
    results = []
    pings = []
    with open(BINARY, 'rb') as binary:
        digest = hashlib.file_digest(binary, 'sha256').hexdigest()
    summary = {'binary': str(Path(BINARY).resolve()), 'binary_sha256': digest,
               'environment': {'platform': platform.platform(), 'cpu_count': os.cpu_count(),
                               'python': platform.python_version()}, 'planned': COUNT,
               'planned_rate_per_second': RATE, 'worker_cap': WORKERS,
               'hold_seconds': 1, 'tokio_workers_per_process': os.environ['TOKIO_WORKER_THREADS'],
               'completed': False}

    def connect(index, start):
        due = start + index / RATE
        time.sleep(max(0, due - time.monotonic()))
        began = time.monotonic()
        sock = None
        result = {'index': index, 'node': index % 3,
                  'started_seconds': began - start, 'start_lag_ms': (began - due) * 1000}
        try:
            sock = socket.create_connection(('127.0.0.1', fleet.brokers[index % 3].mqtt), timeout=3)
            sock.settimeout(10)
            sock.sendall(connect5(f'diag-{index}', clean=True, properties=[(0x11, 0)]))
            header, body = receive(sock)
            ok = header == 0x20 and len(body) >= 3 and body[:2] == b'\x00\x00'
            if ok:
                ok = readprops(body, 2)[1] == len(body)
            result.update(ok=ok, header=header, reply=body.hex())
            if ok:
                sockets[index] = sock
                sock = None
        except (OSError, EOFError, AssertionError, ValueError, IndexError) as error:
            result.update(ok=False, error=repr(error))
        finally:
            if sock is not None:
                sock.close()
            result['connect_ms'] = (time.monotonic() - began) * 1000
        return result

    def ping(index):
        sock = sockets[index]
        try:
            sock.sendall(frame(0xc0))
            header, body = receive(sock)
            return {'index': index, 'ok': (header, body) == (0xd0, b''),
                    'header': header, 'reply': body.hex()}
        except (OSError, EOFError, AssertionError) as error:
            return {'index': index, 'ok': False, 'error': repr(error)}

    try:
        fleet.start()
        start = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
            results = list(executor.map(lambda index: connect(index, start), range(COUNT)))
        summary['ramp_and_drain_seconds'] = time.monotonic() - start
        time.sleep(1)
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
            pings = list(executor.map(ping, [i for i, sock in enumerate(sockets) if sock is not None]))
        accepted = [item['connect_ms'] for item in results if item['ok']]
        summary.update(completed=True, accepted=len(accepted),
                       alive_after_hold_and_ping=sum(item['ok'] for item in pings),
                       connack_success_mean_ms=sum(accepted) / len(accepted) if accepted else None,
                       connack_success_p99_ms=percentile(accepted, .99),
                       actual_start_lag_p99_ms=percentile([r['start_lag_ms'] for r in results], .99),
                       status=fleet.status())
        summary['passed'] = summary['accepted'] == COUNT and summary['alive_after_hold_and_ping'] == COUNT
    finally:
        for sock in sockets:
            if sock is not None:
                sock.close()
        try:
            # Capture bounded fixture logs before close() disposes temporary outputs.
            summary['logs'] = []
            for index, node in enumerate(fleet.brokers):
                summary['logs'].append(capture_log(node.output, root / f'node{index + 1}.log'))
            (root / 'results.json').write_text(json.dumps(results, indent=2))
            (root / 'pings.json').write_text(json.dumps(pings, indent=2))
        finally:
            try:
                fleet.stop()
            finally:
                summary['all_processes_stopped'] = all(node.process is None or node.process.poll() is not None
                                                       for node in fleet.brokers)
                (root / 'summary.json').write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary), flush=True)
    return 0 if summary.get('passed') and summary['all_processes_stopped'] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    arguments = parser.parse_args()
    # Refuse reuse so failed-run evidence cannot be silently replaced.
    arguments.output.mkdir(parents=True, exist_ok=False)
    os.environ.setdefault('TOKIO_WORKER_THREADS', '4')
    os.environ.setdefault('RUST_LOG', 'warn')
    return run(arguments.output)


if __name__ == '__main__':
    raise SystemExit(main())
