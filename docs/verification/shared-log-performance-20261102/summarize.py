#!/usr/bin/env python3
"""Summarize raw runs; macOS time peak RSS is bytes, ps RSS is KiB."""
import hashlib
import json
from pathlib import Path
import re
import statistics as st

P = Path(__file__).resolve().parent
rows = []
for suite in ('grouped', 'single', 'small'):
    paths = list(P.glob(f'{suite}-r*-f*-*.json'))
    keys = sorted({(json.loads(p.read_text())['config']['rules'], json.loads(p.read_text())['config']['fanout']) for p in paths})
    for rules, fanout in keys:
        files = sorted(P.glob(f'{suite}-r{rules}-f{fanout}-*.json'))
        assert len(files) == 3, (suite, rules, fanout, len(files))
        data = [json.loads(p.read_text()) for p in files]
        peaks = [int(re.search(r'(\d+)\s+maximum resident set size', p.with_suffix('.time').read_text())[1])/1048576 for p in files]
        median = lambda f: st.median(f(v) for v in data)
        row = {'suite': suite, 'rules': rules, 'fanout': fanout, 'config': data[0]['config'],
               'results': data[0]['stats_after_append']['pending_results'], 'references': data[0]['stats_after_append']['pending_records'],
               'write_results_s': median(lambda v: v['append']['wall_items_per_second']),
               'write_results_s_range': [min(v['append']['wall_items_per_second'] for v in data), max(v['append']['wall_items_per_second'] for v in data)],
               'read_deliveries_s': median(lambda v: v['read']['wall_items_per_second']),
               'append_p99_ms': median(lambda v: v['append']['api_call_latency_ns']['p99']/1e6),
               'read_p99_ms': median(lambda v: v['read']['api_call_latency_ns']['p99']/1e6),
               'rss_after_append_mib': median(lambda v: v['rss_kib']['after_append']/1024),
               'rss_after_read_mib': median(lambda v: v['rss_kib']['after_read']/1024),
               'peak_rss_mib': st.median(peaks), 'peak_rss_mib_range': [min(peaks), max(peaks)],
               'disk_mib': median(lambda v: v['stats_after_append']['disk_bytes']/1048576)}
        row['write_references_s'] = row['write_results_s']*fanout
        row['source_input_equivalent_s'] = row['write_results_s']/rules
        rows.append(row)
(P/'summary.json').write_text(json.dumps(rows, indent=2)+'\n')
for suite in ('grouped','single','small'):
    print('\n'+suite+'\n|规则×出站|结果/引用数|写入结果/s|新增投递引用/s|读取投递/s|写完 RSS MiB|峰值 RSS MiB|日志 MiB|\n|---|---:|---:|---:|---:|---:|---:|---:|')
    for r in rows:
        if r['suite']!=suite: continue
        print(f"|{r['rules']}×{r['fanout']}|{r['results']:,}/{r['references']:,}|{r['write_results_s']:,.0f}|{r['write_references_s']:,.0f}|{r['read_deliveries_s']:,.0f}|{r['rss_after_append_mib']:.2f}|{r['peak_rss_mib']:.2f}|{r['disk_mib']:.2f}|")
# Add source hash to earlier manifests created before hash reporting was added.
for p in P.glob('*-manifest.json'):
    v=json.loads(p.read_text())
    v['benchmark_source_sha256']=hashlib.sha256((P.parents[2]/'benches/egress-log/src/bin/topology.rs').read_bytes()).hexdigest()
    p.write_text(json.dumps(v,indent=2)+'\n')
