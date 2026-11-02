#!/usr/bin/env python3
"""Finite real-Kafka direct-adapter benchmark with independent rdkafka readback.

Run the same benchmark source/bounds against before/after SDK binaries. This is
an adapter measurement, not an end-to-end MQTT throughput or power-loss claim.
"""
import argparse
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import threading
import time
import uuid

from kafka_fixture import KafkaFixture


def checked_json(command, timeout, env=None):
    result = subprocess.run(command, capture_output=True, text=True, timeout=timeout, env=env)
    if result.returncode:
        raise AssertionError(f'command failed ({result.returncode}): {result.stderr[-2000:]}')
    return json.loads(result.stdout)


def verify_records(records, run_id, clients, per_client, payload_bytes, partitions, expected_pairs=None):
    """Check independent actual Kafka values, identity headers, keys and lane order."""
    pairs = set(expected_pairs) if expected_pairs is not None else {(client, sequence) for client in range(clients) for sequence in range(per_client)}
    expected = len(pairs)
    if len(records) != expected:
        raise AssertionError(f'readback count {len(records)} != {expected} (duplicate or missing)')
    sequences = [[] for _ in range(clients)]
    key_partitions = {}
    ids = set()
    last_offsets = {}
    for row in records:
        body = row['value']
        if set(body) != {'client', 'sequence', 'padding'}:
            raise AssertionError('SQL-projected body shape changed')
        client, sequence = body['client'], body['sequence']
        if type(client) is not int or not 0 <= client < clients:
            raise AssertionError('invalid client identity in readback')
        if type(sequence) is not int or not 0 <= sequence < per_client:
            raise AssertionError('invalid sequence in readback')
        encoded = json.dumps(body, separators=(',', ':'), ensure_ascii=False).encode()
        if len(encoded) != payload_bytes or set(body['padding']) - {'x'}:
            raise AssertionError('SQL payload content or exact size changed')
        identity = f'{run_id}-{client}-{sequence}'
        if identity in ids:
            raise AssertionError('duplicate delivery ID in no-fault workload')
        ids.add(identity)
        headers = row['headers']
        expected_headers = {'rmqtt-event-id':identity, 'rmqtt-delivery-id':identity,
            'rmqtt-rule-id':'benchmark-rule','rmqtt-client-id':f'bench-client-{client:04}',
            'rmqtt-topic':'bench/source'}
        if headers != expected_headers or row['key'] != f'bench-client-{client:04}':
            raise AssertionError('key or trusted identity headers changed')
        if row['timestamp_ms'] != 123456789:
            raise AssertionError('original source timestamp changed')
        partition = row['partition']
        if type(partition) is not int or not 0 <= partition < partitions:
            raise AssertionError('invalid partition')
        if client in key_partitions and key_partitions[client] != partition:
            raise AssertionError('same client changed partition')
        key_partitions[client] = partition
        if row['offset'] <= last_offsets.get(partition, -1):
            raise AssertionError('partition offsets are not strictly increasing')
        last_offsets[partition] = row['offset']
        sequences[client].append(sequence)
    if any(sequence != sorted(seq for c, seq in pairs if c == client) for client, sequence in enumerate(sequences)):
        raise AssertionError('per-client FIFO or completeness failed')
    return dict(count=len(records), unique_delivery_ids=len(ids), exact_payload_bytes=payload_bytes,
                per_client_fifo=True, key_partition_consistent=True, headers_verified=True,
                observed_partitions=sorted(set(key_partitions.values())))


def run_writer(command, timeout):
    """Sample only the writer PID's resident memory, excluding Java and readback."""
    with tempfile.TemporaryFile(mode='w+') as stdout, tempfile.TemporaryFile(mode='w+') as stderr:
        process = subprocess.Popen(command, stdout=stdout, stderr=stderr,
                                   env=dict(os.environ, RUST_LOG='warn'))
        done = threading.Event()
        samples, failures = [], []

        def sample():
            while not done.is_set():
                try:
                    response = subprocess.run(['ps', '-o', 'rss=', '-p', str(process.pid)],
                        capture_output=True, text=True, timeout=2)
                    value = response.stdout.strip()
                    if response.returncode == 0 and value.isdigit():
                        samples.append(int(value) * 1024)
                    elif process.poll() is None:
                        failures.append('rss sample unavailable')
                except (OSError, subprocess.TimeoutExpired):
                    failures.append('rss sampler failed')
                done.wait(.05)

        thread = threading.Thread(target=sample, daemon=True)
        thread.start()
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            raise
        finally:
            done.set()
            thread.join(timeout=3)
        stdout.seek(0)
        stderr.seek(0)
        if process.returncode:
            raise AssertionError(f'writer failed ({process.returncode}): {stderr.read()[-2000:]}')
        result = json.load(stdout)
        result['writer_rss'] = dict(peak_bytes=max(samples) if samples else None,
            samples=len(samples), failures=len(failures),
            method='ps rss KiB of writer PID sampled every 50ms; sampled peak, not exact high-water mark; excludes Kafka and reader')
        if not samples:
            raise AssertionError('no successful writer RSS samples; run with local process-inspection permission')
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--writer', required=True, type=Path)
    parser.add_argument('--reader', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--label', required=True)
    parser.add_argument('--clients', type=int, default=64)
    parser.add_argument('--records-per-client', type=int, default=64)
    parser.add_argument('--payloads', type=int, nargs='+', default=[128, 1024])
    parser.add_argument('--partitions', type=int, nargs='+', default=[1, 12])
    parser.add_argument('--timeout-seconds', type=int, default=180)
    parser.add_argument('--warmup', type=int, default=0, help='confirmed same-sink records before timing; default0 is cold, use64 for a warm session')
    args = parser.parse_args()
    if not (1 <= args.clients <= 512 and 1 <= args.records_per_client
            and 0 <= args.warmup <= 512
            and args.clients * args.records_per_client + args.warmup <= 9999
            and all(128 <= n <= 65536 for n in args.payloads)
            and all(1 <= n <= 32 for n in args.partitions)
            and len(args.payloads) <= 4 and len(args.partitions) <= 4
            and 10 <= args.timeout_seconds <= 900):
        parser.error('workload outside finite bounds')
    for n in args.payloads:
        if n * (args.clients * args.records_per_client + args.warmup) > 12 * 1024 * 1024:
            parser.error('aggregate SQL payload exceeds 12MiB bound')
    writer, reader = str(args.writer.resolve()), str(args.reader.resolve())
    report = dict(schema_version=1, label=args.label, platform=platform.platform(),
        machine=platform.machine(), cpu_count=os.cpu_count(),
        fixture='real Apache Kafka 4.2.2; one local broker/controller; RF=1; min ISR=1; forced per-message log flush; Java heap128..256MiB; no production throughput claim',
        workload='direct KafkaSink; one outstanding send per client; explicit optional same-sink warmup; fixture/readback outside timing',
        warmup_records=args.warmup,
        latency_percentile='nearest rank, successful confirmed calls only; no omission of failed counts',
        scenarios=[])
    with tempfile.TemporaryDirectory(prefix='rmqtt-kafka-adapter-bench-') as directory:
        fixture = KafkaFixture()
        try:
            fixture.start()
            config = Path(directory) / 'reader.json'
            config.write_text(json.dumps(fixture.client_config('plaintext')))
            config.chmod(0o600)
            for partitions in args.partitions:
                for payload in args.payloads:
                    run_id = 'bench-' + uuid.uuid4().hex
                    topic = 'rmqtt-' + run_id
                    checked_json([reader, 'create', str(config), topic, str(partitions)], 25)
                    result = run_writer([writer, f'127.0.0.1:{fixture.port}', topic, run_id,
                        str(args.clients), str(args.records_per_client), str(payload), '30000', str(args.warmup)],
                        args.timeout_seconds)
                    expected = args.clients * args.records_per_client
                    if (result['attempted'] != expected or result['confirmed'] != expected
                            or result['errors'] or result['shutdown_error'] is not None
                            or result['warmup']['confirmed'] != args.warmup):
                        raise AssertionError(f'writer did not confirm every expected record: {result}')
                    if not math.isfinite(result['confirmed_records_per_second']):
                        raise AssertionError('non-finite throughput')
                    # Ask for one extra record: a bounded two-second drain catches
                    # no-fault duplicate delivery rather than hiding it at COUNT.
                    readback = checked_json([reader, 'consume', str(config), topic,
                        str(expected + args.warmup + 1), '2000'], 25)
                    warm_id = 'warmup-' + run_id
                    warm_rows = [row for row in readback['records'] if row['headers'].get('rmqtt-delivery-id', '').startswith(warm_id + '-')]
                    measured_rows = [row for row in readback['records'] if not row['headers'].get('rmqtt-delivery-id', '').startswith(warm_id + '-')]
                    result['warmup']['readback'] = verify_records(warm_rows, warm_id, args.clients,
                        max(1, math.ceil(args.warmup / args.clients)), payload, partitions,
                        expected_pairs={(i % args.clients, i // args.clients) for i in range(args.warmup)})
                    result['readback'] = verify_records(measured_rows, run_id, args.clients,
                        args.records_per_client, payload, partitions)
                    result['partitions'] = partitions
                    report['scenarios'].append(result)
                    args.output.parent.mkdir(parents=True, exist_ok=True)
                    args.output.write_text(json.dumps(report, indent=2) + '\n')
                    print(json.dumps(dict(label=args.label, partitions=partitions,
                        payload_bytes=payload, confirmed=result['confirmed'],
                        tps=result['confirmed_records_per_second'], p99_ms=result['latency_ms']['p99'],
                        sampled_peak_rss_bytes=result['writer_rss']['peak_bytes'])), flush=True)
        finally:
            fixture.close()


if __name__ == '__main__':
    main()
