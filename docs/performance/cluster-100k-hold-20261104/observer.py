"""Finite remote observer; fresh per-controller gates and initial durable membership."""
import argparse,hashlib,json,os,pathlib,subprocess,time
BASE=pathlib.Path(os.environ['RMQTT_100K_BASE'])
ROOT=BASE/'case'/'combined'
BIN=pathlib.Path('/opt/rmqtt-quorum-cpu/20261104/rmqtt')

def statuses():
    result=[]
    for i in (1,2,3):
        start=time.time_ns()
        try:
            p=subprocess.run([str(BIN),'cluster-status','--cluster-config',str(ROOT/f'probe{i}.json')],capture_output=True,text=True,timeout=5,check=True,env=dict(os.environ,TOKIO_WORKER_THREADS='1',RUST_LOG='error',RMQTT_RAFT_PROFILE='0'))
            result.append(dict(controller=i,status=json.loads(p.stdout),read_start_wall_ns=start,read_end_wall_ns=time.time_ns()))
        except (OSError,ValueError,subprocess.SubprocessError) as e:
            result.append(dict(controller=i,error=type(e).__name__,read_start_wall_ns=start,read_end_wall_ns=time.time_ns()))
    return result

def gate(label):
    initial=statuses(); valid=[r['status'] for r in initial if 'status' in r]
    target=max((r['last_applied'] for r in valid),default=0)
    rounds=[initial];deadline=time.monotonic()+8
    passed=False
    while True:
        rows=rounds[-1]; values=[r['status'] for r in rows if 'status'in r]
        passed=(len(values)==3 and len({(s['term'],s['leader']) for s in values})==1 and values[0]['leader'] in (1,2,3) and all(s['last_applied']>=target and s['nodes']==3 and s['data_storage']=='memory' and not s['excluded'] and s['repair'] is None for s in values))
        if passed or time.monotonic()>=deadline:break
        time.sleep(.15);rounds.append(statuses())
    result=dict(label=label,wall_ns=time.time_ns(),target_revision=target,passed=passed,rounds=rounds)
    (ROOT/f'raft-{label}.json').write_text(json.dumps(result,indent=2));return result

def membership():
    result=[];decoder=json.JSONDecoder()
    for i in (1,2,3):
        records=[]
        for path in sorted((ROOT/f'controller-data{i}'/'raft-wal').glob('*.wal')):
            with path.open('rb') as f:data=f.read(16*1024*1024)
            pos=0
            while True:
                pos=data.find(b'{"format":2,"entry":',pos)
                if pos<0:break
                try:
                    value,_=decoder.raw_decode(data[pos:].decode('utf-8',errors='replace'));entry=value['entry'];payload=entry['payload']
                    if isinstance(payload,dict) and 'Membership' in payload:
                        content=json.dumps(entry,ensure_ascii=False,separators=(',',':')).encode()
                        checksum=hashlib.sha256(b'rmqtt-controller-log-v2\0'+content).hexdigest()
                        records.append(dict(file=path.name,entry=entry,checksum_valid=checksum==value['digest']))
                except (ValueError,KeyError,TypeError):pass
                pos+=1
        result.append(dict(controller=i,records=records))
    (ROOT/'raft-membership.json').write_text(json.dumps(result,indent=2));return result

def proc(p):
    text=pathlib.Path(f"/proc/{p['pid']}/stat").read_text().rsplit(')',1)[1].split()
    if text[0]=='Z' or text[19]!=p['start_ticks']:raise ProcessLookupError('process identity/liveness changed')
    return dict(name=p['name'],pid=p['pid'],ticks=int(text[11])+int(text[12]),rss_bytes=int(text[21])*os.sysconf('SC_PAGE_SIZE'),threads=int(text[17]),fd=len(list(pathlib.Path(f"/proc/{p['pid']}/fd").iterdir())))

def monitor():
    processes=json.loads((ROOT/'processes.json').read_text()); began=time.monotonic();last_status=last_tcp=-100.;states=[];tcp={};gated=False;full=False
    with (ROOT/'resources.jsonl').open('x') as output:
        while time.monotonic()-began<480 and not(ROOT/'monitor.stop').exists():
            now=time.monotonic();fresh=now-last_status>=10
            if fresh:states=statuses();last_status=now
            if now-last_tcp>=5:
                tcp={str(p):0 for p in (18831,18832,18833)};tb=time.time_ns()
                for line in pathlib.Path('/proc/net/tcp').read_text().splitlines()[1:]:
                    parts=line.split();port=str(int(parts[1].split(':')[1],16))
                    if parts[3]=='01' and port in tcp:tcp[port]+=1
                te=time.time_ns();last_tcp=now
            mem={line.split(':',1)[0]:int(line.split()[1]) for line in pathlib.Path('/proc/meminfo').read_text().splitlines() if line.startswith(('MemAvailable:','MemFree:','SwapFree:'))}
            row=dict(wall_ns=time.time_ns(),monotonic_ns=time.monotonic_ns(),cpu_hz=os.sysconf('SC_CLK_TCK'),processes=[],controllers=states,status_refreshed=fresh,mqtt_established=tcp,tcp_begin_wall_ns=tb,tcp_end_wall_ns=te,mem_kib=mem)
            for p in processes:
                try:row['processes'].append(proc(p))
                except (OSError,ProcessLookupError):pass
            reasons=[]
            if len(row['processes'])!=3:reasons.append('test Broker process exited')
            if mem['MemAvailable']<512*1024:reasons.append('MemAvailable below512MiB')
            row['abort_reasons']=reasons
            output.write(json.dumps(row)+'\n');output.flush()
            temp=ROOT/'health.tmp';temp.write_text(json.dumps(row));temp.replace(ROOT/'health.json')
            if reasons:(ROOT/'abort.json').write_text(json.dumps(row));return
            if not full and sum(tcp.values())==100000 and all(r.get('status',{}).get('clients')==100000 for r in states):gate('full');full=True
            settings=BASE/'load-settings.json'
            if not gated and settings.exists() and time.time_ns()//1000000>=json.loads(settings.read_text())['ping_unix_ms']-5000:
                gate('hold-end');gated=True
            time.sleep(1)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['start','monitor','final']);a=p.parse_args()
    if a.mode=='start':print(json.dumps(dict(membership=membership(),gate=gate('startup'))))
    elif a.mode=='final':print(json.dumps(gate('final')))
    else:monitor()
