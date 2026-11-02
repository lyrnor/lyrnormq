#!/usr/bin/env python3
"""Aggregate three repeats; keep group-call and per-request latency distinct."""
from pathlib import Path
import json,re,statistics as st
P=Path(__file__).resolve().parent

def peak(path):
 return int(re.search(r'(\d+)\s+maximum resident set size',path.read_text())[1])/1048576

def rows_for(folder,mode):
 rows=[]
 for case in ['single','rules','fanout']:
  for seg in [1,16,64]:
   for width in [1,4]:
    files=[folder/f'{mode}-{case}-s{seg}-g{width}-{r}.json' for r in [1,2,3]]
    if not any(f.exists() for f in files):continue
    assert all(f.exists() for f in files)
    vs=[json.loads(f.read_text()) for f in files]; med=lambda f:st.median(f(v) for v in vs)
    throughput=lambda v:v['writing'].get('pipeline_results_per_second',v['writing']['calls']['wall_items_per_second'])
    r={'case':case,'segment_mib':seg,'width':width,'mode':mode,'inputs':vs[0]['config']['inputs'],'rules':vs[0]['config']['rules'],'fanout':vs[0]['config']['fanout'],
       'write_results_s':med(throughput),'write_results_s_range':[min(map(throughput,vs)),max(map(throughput,vs))],
       'group_call_p99_ms':med(lambda v:v['writing']['calls']['api_call_latency_ns']['p99']/1e6),
       'group_call_samples':med(lambda v:v['writing']['calls']['calls']),
       'actual_append_data_sync_calls':med(lambda v:v['actual_append_data_sync_calls']),
       'frames_per_append_data_sync':med(lambda v:v['frames_per_append_data_sync']),
       'mean_frames_per_group':med(lambda v:v['writing']['mean_frames_per_group']),
       'segment_files':med(lambda v:v['segment_files_after_append']),
       'rss_after_append_mib':med(lambda v:v['rss_kib']['after_append']/1024),
       'peak_rss_mib':st.median(peak(f.with_suffix('.time')) for f in files),
       'disk_mib':med(lambda v:v['stats_after_append']['disk_bytes']/1048576)}
    r['write_references_s']=r['write_results_s']*r['fanout']
    if mode=='concurrent':
     r['confirmation_p99_ms']=med(lambda v:v['writing']['confirmation_latency']['api_call_latency_ns']['p99']/1e6)
     r['observed_reply_p99_ms']=med(lambda v:v['writing']['observed_reply_latency']['api_call_latency_ns']['p99']/1e6)
     r['confirmation_samples']=med(lambda v:v['writing']['confirmation_latency']['items'])
    rows.append(r)
 return rows

direct=rows_for(P/'groups','direct');concurrent=rows_for(P/'confirmations','concurrent')
assert len(direct)==18 and len(concurrent)==2
control=[];baseline=P.parent/'shared-log-io-optimization-20261102/matrix'
for suite in ['grouped','single','small']:
 for rules,fanout in ([(1,1),(4,1),(1,4)] if suite=='single' else [(1,1),(4,1),(16,1),(1,4),(1,16)]):
  sides=[]
  for folder in [baseline,P/'control']:
   vs=[]
   for repeat in [1,2,3]:
    name=f'{suite}-r{rules}-f{fanout}-{repeat}'
    v=json.loads((folder/f'{name}.json').read_text());vs.append(v)
    if folder!=baseline:
     old=json.loads((baseline/f'{name}.json').read_text());assert v['config']==old['config'] and v['stats_after_append']==old['stats_after_append']
   sides.append({'write_results_s':st.median(v['append']['wall_items_per_second'] for v in vs),'read_deliveries_s':st.median(v['read']['wall_items_per_second'] for v in vs),'peak_rss_mib':st.median(peak(folder/f'{suite}-r{rules}-f{fanout}-{r}.time') for r in [1,2,3])})
  control.append({'suite':suite,'rules':rules,'fanout':fanout,'before':sides[0],'after':sides[1]})
for folder,count in [('groups',54),('confirmations',6)]:
 m=json.loads((P/folder/'manifest.json').read_text());assert len(m['runs'])==count and all(r['returncode']==0 for r in m['runs'])
for suite,count in [('grouped',15),('single',9),('small',15)]:
 m=json.loads((P/'control'/f'{suite}-manifest.json').read_text());assert len(m['runs'])==count and all(r['returncode']==0 for r in m['runs']);assert m['benchmark_source_sha256']==json.loads((baseline/f'{suite}-manifest.json').read_text())['benchmark_source_sha256']
(P/'summary.json').write_text(json.dumps({'direct':direct,'confirmation':concurrent,'control':control},indent=2)+'\n')
for case in ['single','rules','fanout']:
 print(f'\n{case}\n\n|段 MiB|组宽|结果/s|组调用p99 ms|追加data sync次数|帧/追加data sync|写完RSS MiB|峰值RSS MiB|\n|---|---:|---:|---:|---:|---:|---:|---:|')
 for r in direct:
  if r['case']!=case:continue
  print(f"|{r['segment_mib']}|{r['width']}|{r['write_results_s']:,.0f}|{r['group_call_p99_ms']:.2f}|{r['actual_append_data_sync_calls']:.0f}|{r['frames_per_append_data_sync']:.2f}|{r['rss_after_append_mib']:.2f}|{r['peak_rss_mib']:.2f}|")
print('\n逐请求确认（16MiB，1规则16出站）\n\n|组宽|结果/s|确认p99 ms|观察回复p99 ms|确认样本|平均帧/组|峰值RSS MiB|\n|---|---:|---:|---:|---:|---:|---:|')
for r in concurrent:print(f"|{r['width']}|{r['write_results_s']:,.0f}|{r['confirmation_p99_ms']:.2f}|{r['observed_reply_p99_ms']:.2f}|{r['confirmation_samples']:.0f}|{r['mean_frames_per_group']:.2f}|{r['peak_rss_mib']:.2f}|")
print('\n原39场景对照（单帧同步）\n\n|套件|规则×出站|结果/s 前→后|读取投递/s 前→后|峰值RSS MiB 前→后|\n|---|---|---:|---:|---:|')
for r in control:
 b,a=r['before'],r['after'];print(f"|{r['suite']}|{r['rules']}×{r['fanout']}|{b['write_results_s']:,.0f}→{a['write_results_s']:,.0f}|{b['read_deliveries_s']:,.0f}→{a['read_deliveries_s']:,.0f}|{b['peak_rss_mib']:.2f}→{a['peak_rss_mib']:.2f}|")
