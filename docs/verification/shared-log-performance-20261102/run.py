#!/usr/bin/env python3
"""Sequential Release measurements, bounded datasets, no existing queue touched."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import time

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--suite', choices=['grouped', 'single', 'small'], default='grouped')
    parser.add_argument('--binary', type=Path, default=ROOT / 'target/release/topology')
    args = parser.parse_args()
    binary = args.binary.resolve()
    cases = [(1, 1), (4, 1), (16, 1), (1, 4), (1, 16)]
    batch, inputs, payload = 64, 4096, 1024
    if args.suite == 'single':
        cases = [(1, 1), (4, 1), (1, 4)]
        batch, inputs = 1, 2048
    elif args.suite == 'small':
        payload = 128
    manifest = {'platform': platform.platform(), 'binary': str(binary),
                'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                'engine_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
                'benchmark_source_sha256': hashlib.sha256((ROOT / 'benches/egress-log/src/bin/topology.rs').read_bytes()).hexdigest(),
                'suite': args.suite, 'started': time.strftime('%Y-%m-%dT%H:%M:%S%z'),
                'runs': []}
    for repeat in range(1, 4):
        for rules, fanout in cases:
            name = f'{args.suite}-r{rules}-f{fanout}-{repeat}'
            command = ['/usr/bin/time', '-l', str(binary), '--inputs', str(inputs),
                       '--rules', str(rules), '--fanout', str(fanout), '--batch', str(batch),
                       '--payload-bytes', str(payload), '--queue-dir', '/private/tmp']
            with (OUT / f'{name}.json').open('w') as out, (OUT / f'{name}.time').open('w') as err:
                subprocess.run(command, stdout=out, stderr=err, check=True, cwd=ROOT)
            result = json.loads((OUT / f'{name}.json').read_text())
            assert result['debug_assertions'] is False
            assert result['read']['items'] == inputs * rules * fanout
            manifest['runs'].append({'name': name, 'command': command})
            (OUT / f'{args.suite}-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
            print(f"{name}: write {result['append']['wall_items_per_second']:.0f} results/s; read {result['read']['wall_items_per_second']:.0f} deliveries/s", flush=True)

if __name__ == '__main__':
    main()
