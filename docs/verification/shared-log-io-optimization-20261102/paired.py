#!/usr/bin/env python3
"""Targeted old/new alternation for regressions in historical matrix; no replacement."""
import json
from pathlib import Path
import subprocess
import hashlib
import statistics as st
P=Path(__file__).resolve().parent
ROOT=P.parents[2]
D=P/'paired'
D.mkdir(exist_ok=False)
binaries={'before':Path('/private/tmp/lyrnormq-topology-before-io'),'after':ROOT/'target/release/topology'}
manifest={'purpose':'Investigate observed write regressions, same workload parameters; does not replace original39-case matrix','binary_sha256':{k:hashlib.sha256(v.read_bytes()).hexdigest() for k,v in binaries.items()},'runs':[]}
for case,inputs,payload,batch,fanout in [('small16',4096,128,64,16),('single4',2048,1024,1,4),('grouped4',4096,1024,64,4)]:
 for repeat in range(1,4):
  order=['before','after'] if repeat%2 else ['after','before']
  for side in order:
   name=f'{case}-{side}-{repeat}'
   cmd=['/usr/bin/time','-l',str(binaries[side]),'--inputs',str(inputs),'--rules','1','--fanout',str(fanout),'--batch',str(batch),'--payload-bytes',str(payload),'--queue-dir','/private/tmp']
   with (D/f'{name}.json').open('x') as out,(D/f'{name}.time').open('x') as err:subprocess.run(cmd,stdout=out,stderr=err,check=True,timeout=180)
   v=json.loads((D/f'{name}.json').read_text());assert v['read']['items']==inputs*fanout
   manifest['runs'].append({'name':name,'command':cmd})
   (D/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
   print(f"{name}: write {v['append']['wall_items_per_second']:.0f}; read {v['read']['wall_items_per_second']:.0f}",flush=True)
summary=[]
for case in ['small16','single4','grouped4']:
 row={'case':case}
 for side in ['before','after']:
  vs=[json.loads((D/f'{case}-{side}-{r}.json').read_text()) for r in range(1,4)]
  row[side]={'write_results_s':st.median(v['append']['wall_items_per_second'] for v in vs),'range':[min(v['append']['wall_items_per_second'] for v in vs),max(v['append']['wall_items_per_second'] for v in vs)]}
 row['ratio']=row['after']['write_results_s']/row['before']['write_results_s'];summary.append(row)
(D/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
