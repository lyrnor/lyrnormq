#!/usr/bin/env python3
"""Run the unchanged 39-case topology matrix without overwriting baseline data.

Example from repository root (build first):
  python3 benches/egress-log/run-topology-matrix.py --output /private/tmp/io-results
Output MUST NOT exist. Runs are sequential to avoid disk workload interference.
"""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--binary', type=Path, default=ROOT / 'target/release/topology')
    parser.add_argument('--queue-dir', type=Path, default=Path('/private/tmp'))
    parser.add_argument('--suite', choices=['all', 'grouped', 'single', 'small'], default='all')
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    output = args.output.resolve()
    # Refuse even empty existing output: protects historical reports/manifests.
    output.mkdir(parents=True, exist_ok=False)
    suites = ['grouped', 'single', 'small'] if args.suite == 'all' else [args.suite]
    common = {
        'platform': platform.platform(), 'binary': str(binary),
        'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
        'engine_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'benchmark_source_sha256': hashlib.sha256((ROOT / 'benches/egress-log/src/bin/topology.rs').read_bytes()).hexdigest(),
        'queue_dir': str(args.queue_dir.resolve()),
        'method': 'Exact 2026-11-02 baseline workloads, sequential execution, three repeats/configuration; production sync unchanged.',
    }
    for suite in suites:
        cases = [(1, 1), (4, 1), (16, 1), (1, 4), (1, 16)]
        batch, inputs, payload = 64, 4096, 1024
        if suite == 'single':
            cases = [(1, 1), (4, 1), (1, 4)]
            batch, inputs = 1, 2048
        elif suite == 'small':
            payload = 128
        manifest = {**common, 'suite': suite, 'started': time.strftime('%Y-%m-%dT%H:%M:%S%z'), 'runs': []}
        for repeat in range(1, 4):
            for rules, fanout in cases:
                name = f'{suite}-r{rules}-f{fanout}-{repeat}'
                command = ['/usr/bin/time', '-l', str(binary), '--inputs', str(inputs),
                           '--rules', str(rules), '--fanout', str(fanout), '--batch', str(batch),
                           '--payload-bytes', str(payload), '--queue-dir', str(args.queue_dir)]
                with (output / f'{name}.json').open('x') as out, (output / f'{name}.time').open('x') as err:
                    completed = subprocess.run(command, stdout=out, stderr=err, cwd=ROOT, timeout=180)
                entry = {'name': name, 'command': command, 'returncode': completed.returncode}
                manifest['runs'].append(entry)
                (output / f'{suite}-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
                completed.check_returncode()
                result = json.loads((output / f'{name}.json').read_text())
                assert result['debug_assertions'] is False
                assert result['config'] == {'inputs': inputs, 'rules': rules, 'fanout': fanout,
                                            'batch': batch, 'payload_bytes': payload, 'segment_bytes': 1048576}
                assert result['append']['items'] == inputs * rules
                assert result['read']['items'] == inputs * rules * fanout
                assert result['verified_deliveries_per_outbound'] == [inputs * rules] * fanout
                assert result['stats_after_append']['pending_bytes'] == inputs * rules * payload
                assert result['stats_after_append']['pending_results'] == inputs * rules
                assert result['stats_after_append']['pending_records'] == inputs * rules * fanout
                assert all(result['rss_kib'].values()), 'RSS collection unavailable; do not report zero'
                resources = (output / f'{name}.time').read_text()
                assert 'maximum resident set size' in resources, 'peak RSS unavailable'
                print(f"{name}: write {result['append']['wall_items_per_second']:.0f} results/s; read {result['read']['wall_items_per_second']:.0f} deliveries/s", flush=True)


if __name__ == '__main__':
    main()
