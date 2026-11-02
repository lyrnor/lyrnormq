#!/usr/bin/env python3
"""Investigate the single-request historical-control slowdown, not replace data."""
from pathlib import Path
import subprocess,json,hashlib,statistics as st
P=Path(__file__).resolve().parent;ROOT=P.parents[2];D=P/'paired-control';D.mkdir(exist_ok=False)
bins={'before':Path('/private/tmp/rmqtt-topology-before-groupcommit'),'after':ROOT/'target/release/topology'}
m={'purpose':'Old/new alternation on historical single-r1-f1 control slowdown; same2048inputs/1KiB/batch1; no replacement of39controls','binary_sha256':{k:hashlib.sha256(v.read_bytes()).hexdigest() for k,v in bins.items()},'runs':[]}
for r in [1,2,3]:
 for side in (['before','after'] if r%2 else ['after','before']):
  name=f'{side}-{r}';cmd=['/usr/bin/time','-l',str(bins[side]),'--inputs','2048','--rules','1','--fanout','1','--batch','1','--payload-bytes','1024','--queue-dir','/private/tmp']
  with (D/f'{name}.json').open('x') as out,(D/f'{name}.time').open('x') as err:subprocess.run(cmd,stdout=out,stderr=err,check=True,timeout=180)
  m['runs'].append({'name':name,'command':cmd});(D/'manifest.json').write_text(json.dumps(m,indent=2)+'\n')
  v=json.loads((D/f'{name}.json').read_text());print(f"{name}: {v['append']['wall_items_per_second']:.0f}results/s",flush=True)
s={}
for side in bins:
 vs=[json.loads((D/f'{side}-{r}.json').read_text()) for r in [1,2,3]]
 rates=[v['append']['wall_items_per_second'] for v in vs]
 s[side]={'write_results_s':st.median(rates),'range':[min(rates),max(rates)],'append_p99_ms':st.median(v['append']['api_call_latency_ns']['p99']/1e6 for v in vs)}
s['ratio']=s['after']['write_results_s']/s['before']['write_results_s'];(D/'summary.json').write_text(json.dumps(s,indent=2)+'\n')
