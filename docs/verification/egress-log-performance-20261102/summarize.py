#!/usr/bin/env python3
"""Aggregate independent runs; never average batch p99 into per-record p99."""
import csv
import json
from pathlib import Path
from statistics import median


def main():
    root = Path(__file__).resolve().parent
    data = json.loads((root / 'results.json').read_text())
    assert len(data) == 50
    summaries = []
    for fs in ('internal', 'external'):
        for size in (128, 1024):
            for batch in (1, 8, 64):
                runs = [r for r in data if (r['filesystem'], r['phase'],
                        r['payload_bytes'], r['batch']) == (fs, 'io', size, batch)]
                assert len(runs) == 3
                append = [r['result']['stages'][0] for r in runs]
                read = [r['result']['stages'][1] for r in runs]
                row = {'filesystem': fs, 'payload_bytes': size, 'batch': batch,
                       'records_per_run': runs[0]['records'], 'repeats': 3,
                       'append_calls_per_run': 1024,
                       'append_records_s': median(s['wall_items_per_second'] for s in append),
                       'append_records_s_min': min(s['wall_items_per_second'] for s in append),
                       'append_records_s_max': max(s['wall_items_per_second'] for s in append),
                       'append_payload_mib_s': median(s['wall_payload_mib_per_second'] for s in append),
                       'read_records_s': median(s['wall_items_per_second'] for s in read),
                       'read_records_s_min': min(s['wall_items_per_second'] for s in read),
                       'read_records_s_max': max(s['wall_items_per_second'] for s in read),
                       'read_payload_mib_s': median(s['wall_payload_mib_per_second'] for s in read),
                       'peak_process_rss_mib': max(r['peak_rss_bytes'] for r in runs) / 1024**2,
                       'process_cpu_one_core_percent': median(
                           100 * (r['process_user_seconds'] + r['process_sys_seconds']) /
                           r['process_wall_seconds'] for r in runs),
                       'disk_file_bytes_per_payload_byte': median(
                           r['result']['stats_after']['disk_bytes'] /
                           (size * r['records']) for r in runs),
                       'process_swaps': sum(r['process_swaps'] for r in runs)}
                for name, stages in [('append', append), ('read', read)]:
                    for p in ('p50', 'p95', 'p99', 'max'):
                        values = [s['api_call_latency_ns'][p] / 1e6 for s in stages]
                        row[f'{name}_api_{p}_ms'] = max(values) if p == 'max' else median(values)
                summaries.append(row)
    with (root / 'summary.csv').open('w') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    cycles = []
    for fs in ('internal', 'external'):
        for batch in (1, 64):
            runs = [r for r in data if (r['filesystem'], r['phase'], r['batch']) ==
                    (fs, 'cycle', batch)]
            assert len(runs) == 3
            cycles.append({'filesystem': fs, 'batch': batch, 'payload_bytes': 128,
                           'cycles_per_run': 256, 'repeats': 3,
                           'durable_cycle_records_s': median(
                               r['result']['stages'][0]['records_per_second'] for r in runs),
                           'peak_process_rss_mib': max(r['peak_rss_bytes'] for r in runs) / 1024**2})
    maintenance = []
    for r in data:
        if r['phase'] == 'maintenance':
            checkpoint, reopen = r['result']['stages']
            maintenance.append({'filesystem': r['filesystem'], 'records': r['records'],
                                'payload_bytes_per_record': 1024,
                                'checkpoint_seconds': checkpoint['elapsed_ns'] / 1e9,
                                'reopen_seconds': reopen['elapsed_ns'] / 1e9,
                                'recovered_records': reopen['recovered']['pending_records'],
                                'peak_process_rss_mib': r['peak_rss_bytes'] / 1024**2})
    assert all(not Path(r['result']['directory']).exists() for r in data)
    result = {'io': summaries, 'cycle': cycles, 'maintenance': maintenance,
              'method': 'rates and per-run API percentiles are medians of 3 runs; '
                        'not a pooled percentile. Max latency is maximum across runs. '
                        'Maintenance is one observation. '
                        'RSS is maximum per-process peak across runs, excluding OS page cache.'}
    (root / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
    print('fs size batch append/s p99ms read/s p99ms RSSMiB')
    for r in summaries:
        print(r['filesystem'], r['payload_bytes'], r['batch'],
              round(r['append_records_s']), round(r['append_api_p99_ms'], 3),
              round(r['read_records_s']), round(r['read_api_p99_ms'], 3),
              round(r['peak_process_rss_mib'], 1))
    print(json.dumps({'cycles': cycles, 'maintenance': maintenance}, indent=2))


if __name__ == '__main__':
    main()
