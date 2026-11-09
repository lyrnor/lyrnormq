#!/usr/bin/env python3
"""Sequential local Journal measurements; never opens an existing queue."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import plistlib
import random
import re
import shutil
import signal
import subprocess
import tempfile
import time


def output_text(command):
    return subprocess.check_output(command, text=True).strip()


def volume_info(base):
    device = output_text(['df', '-P', str(base)]).splitlines()[1].split()[0]
    info = plistlib.loads(subprocess.check_output(['diskutil', 'info', '-plist', device]))
    keys = ('DeviceProtocol', 'BusProtocol', 'FilesystemName', 'FilesystemType', 'SolidState',
            'Internal', 'FileVault', 'MountPoint', 'TotalSize', 'VolumeFreeSpace')
    return {key: info[key] for key in keys if key in info}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--internal-base', type=Path, default=Path('/private/tmp'))
    parser.add_argument('--external-base', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if os.uname().sysname != 'Darwin':
        parser.error('this resource wrapper uses macOS time -l/diskutil; the Rust tool is standalone')
    binary = args.binary.resolve(strict=True)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    (output / 'raw').mkdir(exist_ok=True)
    cases = []
    for repeat in range(1, 4):
        group = [(fs, 'io', size, batch, 1024 * batch, repeat)
                 for fs in ('internal', 'external')
                 for size in (128, 1024) for batch in (1, 8, 64)]
        random.Random(20261102 + repeat).shuffle(group)
        cases.extend(group)
    for repeat in range(1, 4):
        for fs in ('internal', 'external'):
            for batch in (1, 64):
                cases.append((fs, 'cycle', 128, batch, 256 * batch, repeat))
    for fs in ('internal', 'external'):
        cases.append((fs, 'maintenance', 1024, 64, 65536, 1))
    started = time.monotonic()
    environment = {
        'platform': os.uname().sysname + ' ' + os.uname().machine,
        'cpu': output_text(['sysctl', '-n', 'machdep.cpu.brand_string']),
        'physical_cores': int(output_text(['sysctl', '-n', 'hw.physicalcpu'])),
        'logical_cores': int(output_text(['sysctl', '-n', 'hw.logicalcpu'])),
        'ram_bytes': int(output_text(['sysctl', '-n', 'hw.memsize'])),
        'os': output_text(['sw_vers']),
        'rustc': output_text(['rustc', '--version']),
        'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
        'host_load_before': os.getloadavg(),
        'swap_before': output_text(['sysctl', 'vm.swapusage']),
        'internal': volume_info(args.internal_base),
        'external': volume_info(args.external_base),
        'method': 'single process at a time, 3 independent runs; 1024 append calls '
                  'per IO case, 256 complete cycles per cycle case; nearest-rank '
                  'per-API percentiles; read follows append, no cache purge',
    }
    (output / 'environment.json').write_text(json.dumps(environment, indent=2) + '\n')
    rows = []
    bases = {'internal': args.internal_base, 'external': args.external_base}
    for index, (fs, phase, size, batch, records, repeat) in enumerate(cases, 1):
        if time.monotonic() - started > 900:
            raise RuntimeError('overall 900-second benchmark budget exceeded')
        name = f'{fs}-{phase}-{size}b-batch{batch}-run{repeat}'
        result_path = output / 'raw' / (name + '.json')
        resource_path = output / 'raw' / (name + '.time')
        error_path = output / 'raw' / (name + '.stderr')
        free = shutil.disk_usage(bases[fs]).free
        if free < 2 * 1024**3:
            raise RuntimeError(f'insufficient free space for temporary test: {fs}')
        with tempfile.TemporaryDirectory(prefix='lyrnormq-journal-perf-', dir=bases[fs]) as directory:
            command = ['/usr/bin/time', '-l', '-o', str(resource_path), str(binary),
                       '--queue-dir', directory, '--phase', phase,
                       '--records', str(records), '--batch', str(batch),
                       '--payload-bytes', str(size)]
            wall_start = time.monotonic()
            with result_path.open('w') as stdout, error_path.open('w') as stderr:
                process = subprocess.Popen(command, stdout=stdout, stderr=stderr,
                                           start_new_session=True)
                try:
                    code = process.wait(timeout=90)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                    raise
                if code:
                    raise subprocess.CalledProcessError(code, command)
            elapsed = time.monotonic() - wall_start
        result = json.loads(result_path.read_text())
        assert result['build']['debug_assertions'] is False
        if phase == 'io':
            append, read, _heads = result['stages']
            assert append['calls'] == 1024 and append['items'] == records
            assert read['unique_records_verified'] == records
            assert read['items'] == records and read['calls'] == 1024
        if phase == 'cycle':
            assert result['stats_after']['pending_records'] == 0
            assert result['stages'][0]['records'] == records
        resource = resource_path.read_text()
        rss = int(re.search(r'(\d+)\s+maximum resident set size', resource).group(1))
        swaps = int(re.search(r'(\d+)\s+swaps', resource).group(1))
        cpu = re.search(r'([\d.]+) real\s+([\d.]+) user\s+([\d.]+) sys', resource)
        row = {'filesystem': fs, 'phase': phase, 'payload_bytes': size,
               'batch': batch, 'records': records, 'repeat': repeat,
               'peak_rss_bytes': rss, 'process_swaps': swaps,
               'process_wall_seconds': elapsed,
               'process_user_seconds': float(cpu.group(2)),
               'process_sys_seconds': float(cpu.group(3)),
               'free_bytes_before': free, 'command': command,
               'result': result}
        rows.append(row)
        (output / 'results.json').write_text(json.dumps(rows, indent=2) + '\n')
        if phase == 'io':
            print(f'{index}/{len(cases)} {name}: append={append["wall_items_per_second"]:.0f}/s '
                  f'p99={append["api_call_latency_ns"]["p99"]/1e6:.3f}ms '
                  f'read={read["wall_items_per_second"]:.0f}/s rss={rss/1024**2:.1f}MiB', flush=True)
        else:
            print(f'{index}/{len(cases)} {name}: {elapsed:.2f}s rss={rss/1024**2:.1f}MiB', flush=True)
    environment['host_load_after'] = os.getloadavg()
    environment['total_wall_seconds'] = time.monotonic() - started
    environment['runs_completed'] = len(rows)
    (output / 'environment.json').write_text(json.dumps(environment, indent=2) + '\n')


if __name__ == '__main__':
    main()
