#!/usr/bin/env python3
"""Compare identical three-repeat matrices, preserving both raw datasets."""
import json
from pathlib import Path
import re
import statistics as st

HERE=Path(__file__).resolve().parent
OLD=HERE.parent/'shared-log-performance-20261102'
NEW=HERE/'matrix'
rows=[]
for suite in ('grouped','single','small'):
    keys=[(1,1),(4,1),(16,1),(1,4),(1,16)] if suite!='single' else [(1,1),(4,1),(1,4)]
    for rules,fanout in keys:
        sides=[]
        for base in (OLD,NEW):
            data=[];peaks=[]
            for repeat in range(1,4):
                name=f'{suite}-r{rules}-f{fanout}-{repeat}'
                v=json.loads((base/f'{name}.json').read_text());data.append(v)
                peaks.append(int(re.search(r'(\d+)\s+maximum resident set size',(base/f'{name}.time').read_text())[1])/1048576)
                if base==NEW:
                    old=json.loads((OLD/f'{name}.json').read_text())
                    assert v['config']==old['config']
                    assert v['verified_deliveries_per_outbound']==old['verified_deliveries_per_outbound']
                    assert v['stats_after_append']==old['stats_after_append'], 'Disk layout or accounting changed'
            median=lambda f: st.median(f(v) for v in data)
            sides.append({'write_results_s':median(lambda v:v['append']['wall_items_per_second']),
                          'write_results_s_range':[min(v['append']['wall_items_per_second'] for v in data),max(v['append']['wall_items_per_second'] for v in data)],
                          'read_deliveries_s':median(lambda v:v['read']['wall_items_per_second']),
                          'read_deliveries_s_range':[min(v['read']['wall_items_per_second'] for v in data),max(v['read']['wall_items_per_second'] for v in data)],
                          'append_p99_ms':median(lambda v:v['append']['api_call_latency_ns']['p99']/1e6),
                          'read_p99_ms':median(lambda v:v['read']['api_call_latency_ns']['p99']/1e6),
                          'rss_after_append_mib':median(lambda v:v['rss_kib']['after_append']/1024),
                          'rss_after_read_mib':median(lambda v:v['rss_kib']['after_read']/1024),
                          'peak_rss_mib':st.median(peaks), 'peak_rss_mib_range':[min(peaks),max(peaks)],
                          'disk_mib':median(lambda v:v['stats_after_append']['disk_bytes']/1048576)})
        before,after=sides
        rows.append({'suite':suite,'rules':rules,'fanout':fanout,'before':before,'after':after,
                     'write_ratio':after['write_results_s']/before['write_results_s'],
                     'read_ratio':after['read_deliveries_s']/before['read_deliveries_s']})
(HERE/'comparison.json').write_text(json.dumps(rows,indent=2)+'\n')
for suite in ('grouped','single','small'):
    print(f'\n{suite}\n\n|规则×出站|写入结果/s 前→后|写入倍数|读取投递/s 前→后|读取倍数|峰值RSS MiB 前→后|\n|---|---:|---:|---:|---:|---:|')
    for r in rows:
        if r['suite']!=suite:continue
        b,a=r['before'],r['after']
        print(f"|{r['rules']}×{r['fanout']}|{b['write_results_s']:,.0f} → {a['write_results_s']:,.0f}|{r['write_ratio']:.2f}×|{b['read_deliveries_s']:,.0f} → {a['read_deliveries_s']:,.0f}|{r['read_ratio']:.2f}×|{b['peak_rss_mib']:.2f} → {a['peak_rss_mib']:.2f}|")
