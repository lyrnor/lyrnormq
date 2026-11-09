#!/usr/bin/env python3
import argparse,subprocess,pathlib,json,time,os,threading,hashlib,platform,socket,gzip,shutil
ROOT=pathlib.Path('/opt/lyrnormq-cluster-load/20261103')
ADDRS='172.16.240.180:18831,172.16.240.180:18832,172.16.240.180:18833'

def statproc(pid):
    t=pathlib.Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()
    return {'ticks':int(t[11])+int(t[12]),'rss_bytes':int(t[21])*os.sysconf('SC_PAGE_SIZE'),'threads':int(t[17])}

def run(topology,name,qos,protocol,pairs,rate,duration,inflight):
    root=ROOT/topology;root.mkdir(exist_ok=True)
    report=root/f'{name}.json';raw=root/f'{name}.jsonl'
    cmd=[str(ROOT/'lyrnormq-loadgen'),'--addrs',ADDRS,'--protocol',protocol,'--qos',str(qos),'--setup-concurrency','1','--pairs',str(pairs),'--payload-bytes','128','--rate',str(rate),'--inflight',str(inflight),'--warmup-ms','3000','--duration-ms',str(duration),'--drain-timeout-ms','30000','--connect-timeout-ms','60000','--io-timeout-ms','30000','--max-messages','2000000','--output',str(report),'--raw',str(raw)]
    before=time.time_ns();samples=[]
    with (root/f'{name}.log').open('xb') as log:
        p=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT,env=dict(os.environ,TOKIO_WORKER_THREADS='8'))
        deadline=time.monotonic()+240
        while p.poll() is None:
            try:samples.append(dict(wall_ns=time.time_ns(),**statproc(p.pid)))
            except (FileNotFoundError,ProcessLookupError):pass
            if time.monotonic()>deadline:
                p.kill();p.wait();break
            time.sleep(.5)
    after=time.time_ns()
    record={'name':name,'topology':topology,'command':cmd,'pid':p.pid,'exit_code':p.returncode,'wall_start_ns':before,'wall_end_ns':after,'cpu_hz':os.sysconf('SC_CLK_TCK'),'samples':samples}
    (root/f'{name}.client-resources.json').write_text(json.dumps(record,indent=2))
    if raw.exists():
        with raw.open('rb') as src,gzip.open(str(raw)+'.gz','wb') as dst:shutil.copyfileobj(src,dst)
        raw.unlink()
    r=json.loads(report.read_text()) if report.exists() and report.stat().st_size else {}
    print(json.dumps({'case':name,'exit':p.returncode,'success':r.get('success'),'phase':r.get('phase'),'error':r.get('error'),'accounting':r.get('accounting'),'tps':r.get('offering_window_tps'),'latency':r.get('latencies',{}).get('send_to_delivery'),'issues':r.get('issues',[])[:3]}),flush=True)
    return r

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('topology',choices=['combined','separate']);p.add_argument('--mode',choices=['pilot','main','smoke','scale','baseline'],default='main');a=p.parse_args()
    ROOT.mkdir(parents=True,exist_ok=True)
    env={'private':'172.16.240.181','hostname':socket.gethostname(),'cpus':os.cpu_count(),'platform':platform.platform(),'meminfo':pathlib.Path('/proc/meminfo').read_text(),'worker_threads':8,'binary_sha256':hashlib.sha256((ROOT/'lyrnormq-loadgen').read_bytes()).hexdigest()}
    (ROOT/'environment.json').write_text(json.dumps(env,indent=2))
    if a.mode=='pilot':
        for q in range(3):run(a.topology,f'pilot-q{q}',q,'5',30,0,5000,1)
    elif a.mode=='scale':print('Optional connection-scale run skipped; requested comparison focuses on message forwarding and synchronization',flush=True)
    elif a.mode=='smoke':
        for q in range(3):run(a.topology,f'v311-q{q}',q,'3.1.1',6,30,5000,1)
    elif a.mode=='baseline':
        for q in range(3):
            run(a.topology,f'baseline-q{q}',q,'5',6,30,30000,1)
            run(a.topology,f'baseline-pressure-q{q}',q,'5',6,0,10000,1)
    else:
        for q in range(3):
            run(a.topology,f'fixed-q{q}',q,'5',30,100,30000,1)
            time.sleep(2)
            run(a.topology,f'pressure-q{q}',q,'5',30,0,30000,4)
            time.sleep(2)
