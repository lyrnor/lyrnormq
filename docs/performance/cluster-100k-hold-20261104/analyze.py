"""Recompute failed100k case from original counters; never invent partial client ACKs."""
import json,re,sys
from pathlib import Path


def main(root):
    settings=json.loads((root/'settings.json').read_text());start=settings['start_unix_ms']*1000000
    rows=[json.loads(s)for s in (root/'server/resources.jsonl').read_text().splitlines()]
    peak=max(rows,key=lambda r:sum(r['mqtt_established'].values()))
    peak_total=sum(peak['mqtt_established'].values())
    scans={r['tcp_end_wall_ns']:r for r in rows};scans=sorted(scans.values(),key=lambda r:r['tcp_end_wall_ns'])
    drops=[r for r in scans if r['tcp_end_wall_ns']>peak['tcp_end_wall_ns'] and sum(r['mqtt_established'].values())==0]
    intervals=[]
    for a,b in zip(rows,rows[1:]):
        aa={p['pid']:p['ticks']for p in a['processes']};bb={p['pid']:p['ticks']for p in b['processes']}
        if aa.keys()!=bb.keys() or not aa:continue
        dt=(b['monotonic_ns']-a['monotonic_ns'])/1e9;ticks=sum(bb[k]-v for k,v in aa.items());assert dt>0 and ticks>=0
        intervals.append(dict(first_wall_ns=a['wall_ns'],last_wall_ns=b['wall_ns'],cpu_seconds=ticks/a['cpu_hz'],seconds=dt,cores=ticks/a['cpu_hz']/dt))
    before=[r for r in intervals if start<=r['first_wall_ns'] and r['last_wall_ns']<=peak['wall_ns']]
    admissions={};checkpoints={};first_failures={}
    for p in sorted((root/'server').glob('node*/process.log')):
        profiles=[];admission=[];fails=[]
        for line in p.read_text().splitlines():
            if 'event="handshake_profile"'in line:admission.append({k:int(v)for k,v in re.findall(r'(acquired|rejected|active|observed_peak|limit)=(\d+)',line)})
            if 'event="raft_profile"'in line:
                profiles.append((line.split()[0],json.loads(line.split('data=',1)[1])))
            if ('outcome=Failure'in line or ' ERROR 'in line) and len(fails)<12:fails.append(line)
        admissions[p.parent.name]=admission[-1] if admission else None
        previous={};values=[]
        for wall,metrics in profiles:
            for m in metrics:
                if not m['metric'].startswith('Checkpoint'):continue
                old=previous.get(m['metric'],dict(count=0,sum=0))
                count=m['count']-old['count'];total=m['sum']-old['sum']
                if count>0:
                    values.append(dict(observed_utc=wall,metric=m['metric'],through_ordinal=m['count'],delta_count=count,delta_sum_us=total,individual_us=total if count==1 else None,consistent=sum(m['buckets'])==m['count']))
                previous[m['metric']]=m
        checkpoints[p.parent.name]=values;first_failures[p.parent.name]=fails
    gates={}
    for name in ('startup','full','hold-end','final'):
        path=root/'server'/('raft-'+name+'.json');gates[name]=json.loads(path.read_text())if path.exists()else None
    membership=json.loads((root/'server/raft-membership.json').read_text())
    result=dict(passed=False,requested=dict(rate=2000,total=100000,hold_seconds=300),client_metrics=None,client_metrics_note='Coordinator aborted after observed mass disconnection; all four final report/raw files are empty. ACK successes/mean/p99/300s hold are unverified, not zero.',tcp_sample_peak=dict(total=peak_total,by_node=peak['mqtt_established'],scan_begin_wall_ns=peak['tcp_begin_wall_ns'],scan_end_wall_ns=peak['tcp_end_wall_ns'],row_wall_ns=peak['wall_ns']),first_zero_scan_after_peak=dict(begin_wall_ns=drops[0]['tcp_begin_wall_ns'],end_wall_ns=drops[0]['tcp_end_wall_ns'])if drops else None,admission=admissions,admission_totals={key:sum(v[key]for v in admissions.values())for key in ('acquired','rejected')},resources=dict(simultaneous_rss_peak_mib=max(sum(p['rss_bytes']for p in r['processes'])for r in rows)/1048576,simultaneous_fd_peak=max(sum(p['fd']for p in r['processes'])for r in rows),cpu_interval_peak_cores=max(i['cores']for i in intervals),before_peak_cpu_cores=sum(i['cpu_seconds']for i in before)/sum(i['seconds']for i in before),before_peak_sampled_seconds=sum(i['seconds']for i in before),mem_available_min_mib=min(r['mem_kib']['MemAvailable']for r in rows)/1024),raft_gates={name:value['passed']if value else None for name,value in gates.items()},raft_final=gates['final'],membership=membership,checkpoints=checkpoints,first_failures=first_failures,source_ports=json.loads((root/'client/source-ports.json').read_text()),post_stop=json.loads((root/'post-stop.json').read_text()),notes=['TCP fields can be reused for5s; use scan timestamps, not every row as a fresh TCP count.','Admission acquired is permission, not CONNACK; owner count is metadata, not online or ACK success.','Resource peaks span a failed case and cleanup, not a stable100k plateau.','CheckpointPersist measures work while WAL+checkpoint mutexes are held, not isolated fsync or bbolt-library CPU.'])
    (root/'result-summary.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items()if k not in ('membership','raft_final','checkpoints','first_failures','post_stop')},indent=2))


if __name__=='__main__':main(Path(sys.argv[1]))
