#!/usr/bin/env python3
import json,gzip,pathlib,math,sys
ROOT=pathlib.Path(sys.argv[1]);results=[];setup_failures=[]
for f in sorted((ROOT/'client').glob('*/*.json')):
    if f.name.endswith('client-resources.json'):continue
    r=json.loads(f.read_text())
    if 'config' not in r or 'accounting' not in r:
        if r.get('success') is False: setup_failures.append({'topology':f.parent.name,'case':f.stem,'phase':r.get('phase'),'error':r.get('error'),'success':False})
        continue
    a=r['accounting'];c=a['counters'];raw=f.with_suffix('.jsonl.gz')
    vals=[];pair_counts={};ack_count=0;count=0;complete=0;writes=0;keys=set()
    with gzip.open(raw,'rt') as stream:
        for line in stream:
            x=json.loads(line);count+=1
            assert (x['pair'],x['sequence']) not in keys
            keys.add((x['pair'],x['sequence']))
            pair_counts[x['pair']]=pair_counts.get(x['pair'],0)+1
            if x['delivery_ns'] is not None:vals.append(x['delivery_ns']-x['send_started_ns'])
            ack_count+=x['ack_ns'] is not None
            complete+=x['completed_ns'] is not None
            writes+=x['write_completed_ns'] is not None
    assert count==a['samples_retained']==c['attempted'],f
    assert len(vals)==c['delivered_unique'] and complete==c['completed'] and writes==c['write_completed'],f
    q=r['config']['qos']
    assert ack_count==(c['puback_received'] if q==1 else c['pubcomp_received'] if q==2 else 0),f
    vals.sort();p99=vals[(99*len(vals)+99)//100-1] if vals else None
    assert p99==r['latencies']['send_to_delivery']['p99_ns'],f
    cfg=r['config'];assert len(cfg['addresses'])==3
    expected=cfg['pairs']//3
    assert all(x['publishers']==expected and x['subscribers']==expected for x in r['configured_endpoint_connections']),f
    results.append({'topology':f.parent.name,'case':f.stem,'success':r['success'],'protocol':cfg['version'],'qos':q,'connections':cfg['pairs']*2,'rate':cfg['rate'],'inflight':cfg['inflight'],'duration_ms':cfg['duration_ms'],'attempted':c['attempted'],'written':c['write_completed'],'delivered':c['delivered_unique'],'confirmed':c['puback_received'] if q==1 else c['pubcomp_received'] if q==2 else None,'not_delivered':a['not_delivered'],'not_sent':a['not_sent'],'not_source_confirmed':a['not_source_confirmed'],'duplicates':c['duplicates'],'invalid_payload':c['invalid_payload'],'delivery_tps':r['offering_window_tps']['delivered_unique_per_second'],'cohort_delivery_tps':r['measured_cohort_tps_through_drain']['delivered_unique_per_second'],'send_delivery_p50_ms':r['latencies']['send_to_delivery']['p50_ns']/1e6 if vals else None,'send_delivery_p99_ms':p99/1e6 if vals else None,'issues':r['issues'][:2],'raw_verified':True,'measured_cohort_available':c['attempted']>0,'loss_status':'no measured cohort; not a zero-loss result' if c['attempted']==0 else 'observed full delivery' if a['not_delivered']==0 else 'observed missing after finite drain'})
resources=[]
resource_files=list((ROOT/'server').glob('*/resources.jsonl'))+list((ROOT/'server').glob('*/resources.jsonl.gz'))
for f in sorted(resource_files):
    text=gzip.open(f,'rt').read() if f.suffix=='.gz' else f.read_text()
    rows=[json.loads(x) for x in text.splitlines()]
    if not rows:continue
    peakrss=max(sum(p['rss_bytes'] for p in r['processes']) for r in rows)
    cpu=[];per={};skew=[];balanced={}
    for a,b in zip(rows,rows[1:]):
        old={p['pid']:p for p in a['processes']};dt=(b['monotonic_ns']-a['monotonic_ns'])/1e9
        total=0
        for p in b['processes']:
            if p['pid'] not in old:continue
            v=(p['ticks']-old[p['pid']]['ticks'])/b['cpu_hz']/dt*100
            total+=v;per.setdefault(p['name'],[]).append(v)
        cpu.append(total)
    perrss={}
    for r in rows:
        for p in r['processes']:perrss[p['name']]=max(perrss.get(p['name'],0),p['rss_bytes'])
        arr=[x['applied'] for x in r['controllers'] if x.get('applied') is not None]
        if len(arr)==3:skew.append(max(arr)-min(arr))
        counts=list(r['mqtt_established'].values())
        if min(counts)>0 and len(set(counts))==1:balanced[str(counts[0])]=balanced.get(str(counts[0]),0)+1
    resources.append({'topology':f.parent.name,'samples':len(rows),'aggregate_peak_rss_mib':peakrss/2**20,'aggregate_mean_cpu_percent':sum(cpu)/len(cpu) if cpu else None,'aggregate_peak_cpu_percent':max(cpu) if cpu else None,'per_process_peak_rss_mib':{k:v/2**20 for k,v in perrss.items()},'per_process_mean_cpu_percent':{k:sum(v)/len(v) for k,v in per.items()},'max_observed_apply_index_skew':max(skew) if skew else None,'equal_active_connections_per_node_sample_counts':balanced,'scope':'Entire monitored topology phase including setup/warmup/failures/drain/idle. CPU100%=one logical CPU; RSS sums may double-count shared pages.'})
output={'performance_gate_passed':False,'raw_verification_scope':'Reports with measured accounting; setup failures separately inventoried. Matching counters is not performance acceptance.','setup_failures':setup_failures,'results':results,'resources':resources,'all_raw_checks_passed':True}
(ROOT/'summary.json').write_text(json.dumps(output,indent=2)+'\n')
for x in results:print(x['topology'],x['case'],x['success'],x['attempted'],x['delivered'],x['delivery_tps'],x['send_delivery_p99_ms'])
print(json.dumps(resources,indent=2))
