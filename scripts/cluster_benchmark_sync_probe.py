#!/usr/bin/env python3
"""Bounded remote routing visibility probe; all clocks are local monotonic.

Measures SUBACK observation to first matching cross-node application PUBLISH.
This includes route propagation, publisher writes, network and scheduling; it is
NOT a pure Raft replication latency. Repeated QoS0 probes may be missed before a
route becomes visible. This is not a message-loss throughput test.
"""
import argparse
import contextlib
import json
import math
from pathlib import Path
import select
import socket
import struct
import subprocess
import time
import uuid


def varint(value):
    data = bytearray()
    while True:
        byte, value = value % 128, value // 128
        data.append(byte | (128 if value else 0))
        if not value:
            return bytes(data)


def readint(data, offset):
    value = 0
    for index in range(4):
        byte = data[offset + index]
        value += (byte & 127) * 128 ** index
        if byte < 128:
            return value, offset + index + 1
    raise ValueError('invalid MQTT variable integer')


def string(value):
    data = value.encode()
    return struct.pack('!H', len(data)) + data


def frame(header, body=b''):
    return bytes([header]) + varint(len(body)) + body


def exact(sock, size):
    result = bytearray()
    while len(result) < size:
        data = sock.recv(size - len(result))
        if not data:
            raise EOFError('broker closed connection')
        result.extend(data)
    return bytes(result)


def receive(sock):
    header = exact(sock, 1)[0]
    size = 0
    for index in range(4):
        byte = exact(sock, 1)[0]
        size += (byte & 127) * 128 ** index
        if byte < 128:
            if size > 65536:
                raise ValueError('probe frame exceeds 64KiB')
            return header, exact(sock, size)
    raise ValueError('invalid MQTT frame length')


def connect(address, client_id, version, timeout):
    host, port = address.rsplit(':', 1)
    sock = socket.create_connection((host, int(port)), timeout=timeout)
    sock.settimeout(timeout)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    body = string('MQTT') + bytes([version, 2, 0, 0])
    sock.sendall(frame(0x10, body + (b'\x00' if version == 5 else b'') + string(client_id)))
    header, data = receive(sock)
    if header != 0x20 or len(data) < 2 or data[0:2] != b'\x00\x00':
        sock.close()
        raise ValueError(f'CONNECT rejected: header={header}, response={data.hex()}')
    return sock


def publication(header, data, version):
    if header != 0x30 or len(data) < 2:
        raise ValueError(f'unexpected probe packet {header:#x}')
    length = struct.unpack('!H', data[:2])[0]
    topic = data[2:2 + length].decode()
    offset = 2 + length
    if version == 5:
        size, offset = readint(data, offset)
        offset += size
    return topic, data[offset:]


def distribution(values):
    values = sorted(values)
    if not values:
        return dict(count=0, p50_ns=None, p95_ns=None, p99_ns=None, max_ns=None)
    return dict(count=len(values), p50_ns=values[math.ceil(.5 * len(values))-1],
                p95_ns=values[math.ceil(.95 * len(values))-1],
                p99_ns=values[math.ceil(.99 * len(values))-1], max_ns=values[-1])


def probe(args):
    if not 1 <= args.trials <= 10000 or not .001 <= args.interval <= 1 or not .1 <= args.timeout <= 60:
        raise ValueError('finite bounds: trials1..10000 interval.001..1 timeout.1..60')
    addresses = args.addrs.split(',')
    if len(addresses) != 3 or len(set(addresses)) != 3:
        raise ValueError('probe requires exactly three distinct endpoints')
    version = 5 if args.protocol == '5' else 4
    run = uuid.uuid4().hex[:12]
    samples, failures = [], []
    with contextlib.ExitStack() as stack:
        subscribers, publishers = [], []
        for index, address in enumerate(addresses):
            subscribers.append(stack.enter_context(connect(address, f'sp{run}s{index}', version, args.timeout)))
            publishers.append(stack.enter_context(connect(address, f'sp{run}p{index}', version, args.timeout)))
        for trial in range(args.trials):
            target, source = trial % 3, (trial + 1) % 3
            subscriber, publisher = subscribers[target], publishers[source]
            topic = f'cluster-sync-probe/{run}/{trial}'
            payload = f'{run}:{trial}'.encode()
            packet_id = trial % 65535 + 1
            props = b'\x00' if version == 5 else b''
            begin = time.monotonic_ns()
            subscriber.sendall(frame(0x82, struct.pack('!H', packet_id) + props + string(topic) + b'\x00'))
            ignored = 0
            while True:
                header, body = receive(subscriber)
                if header >> 4 == 3:
                    publication(header, body, version)
                    ignored += 1
                    continue
                offset = readint(body, 2)[1] if version == 5 else 2
                if header != 0x90 or body[:2] != struct.pack('!H', packet_id) or body[offset:] != b'\x00':
                    raise ValueError('SUBACK rejected or invalid')
                break
            suback = time.monotonic_ns()
            deadline = time.monotonic() + args.timeout
            attempts, delivered = 0, None
            next_send = time.monotonic()
            while time.monotonic() < deadline:
                now = time.monotonic()
                if now >= next_send:
                    publisher.sendall(frame(0x30, string(topic) + props + payload))
                    attempts += 1
                    next_send = now + args.interval
                readable, _, _ = select.select([subscriber], [], [], max(0, min(next_send, deadline) - time.monotonic()))
                if readable:
                    header, body = receive(subscriber)
                    actual_topic, actual_payload = publication(header, body, version)
                    if actual_topic != topic:
                        ignored += 1
                        continue
                    if actual_payload != payload:
                        raise ValueError('probe payload mismatch')
                    delivered = time.monotonic_ns()
                    break
            samples.append(dict(trial=trial, source_endpoint=addresses[source], target_endpoint=addresses[target],
                                subscribe_to_suback_ns=suback-begin,
                                suback_to_delivery_ns=None if delivered is None else delivered-suback,
                                publish_attempts=attempts, late_prior_probe_observations=ignored))
            if delivered is None:
                failures.append(f'trial{trial}: no matching application message within {args.timeout}s')
            subscriber.sendall(frame(0xa2, struct.pack('!H', packet_id) + props + string(topic)))
            while True:
                header, body = receive(subscriber)
                if header >> 4 == 3:
                    publication(header, body, version)
                    continue
                if header != 0xb0 or body[:2] != struct.pack('!H', packet_id):
                    raise ValueError('invalid UNSUBACK')
                break
        for sock in [*subscribers, *publishers]:
            sock.sendall(frame(0xe0))
    report = dict(protocol=args.protocol, endpoints=addresses, trials=args.trials, success=not failures,
                  limitations='Observation includes route synchronization, network, write and scheduling. Repeated QoS0 visibility probes are not loss measurements or pure Raft latency.',
                  subscribe_to_suback=distribution([x['subscribe_to_suback_ns'] for x in samples]),
                  suback_to_first_cross_node_delivery=distribution([x['suback_to_delivery_ns'] for x in samples if x['suback_to_delivery_ns'] is not None]),
                  samples=samples, failures=failures)
    if args.status_config:
        result = subprocess.run([args.binary, 'cluster-status', '--cluster-config', args.status_config],
                                capture_output=True, timeout=15)
        report['control_status'] = json.loads(result.stdout) if result.returncode == 0 else {'error': 'status unavailable'}
        report['control_status_semantics'] = 'seed-selected controller status; not independent evidence from all Raft members'
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--addrs', required=True)
    parser.add_argument('--protocol', choices=['5', '3.1.1'], default='5')
    parser.add_argument('--trials', type=int, default=30)
    parser.add_argument('--interval', type=float, default=.01)
    parser.add_argument('--timeout', type=float, default=5)
    parser.add_argument('--status-config')
    parser.add_argument('--binary', default='./rmqtt')
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    try:
        result = probe(args)
    except (OSError, EOFError, ValueError, subprocess.TimeoutExpired) as error:
        result = dict(success=False, error=str(error))
    with open(Path(args.output), 'x') as output:
        json.dump(result, output, indent=2)
        output.write('\n')
    raise SystemExit(0 if result['success'] else 2)


if __name__ == '__main__':
    main()
