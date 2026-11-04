"""One requested100k connection hold; no broker compilation or tuning."""
import concurrent.futures,hashlib,importlib.util,json,pathlib,shlex,subprocess,time,tarfile
HERE=pathlib.Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('ssh_prior',HERE.parent/'cluster-multiprocess-load-20261103/run.py');prior=importlib.util.module_from_spec(spec);spec.loader.exec_module(prior)
spec=importlib.util.spec_from_file_location('hold_aggregate',HERE/'aggregate.py');agg=importlib.util.module_from_spec(spec);spec.loader.exec_module(agg)
BASE='/opt/rmqtt-100k-hold/20261104-validated-probe'
OUT=pathlib.Path('/Volumes/data/rmqtt/target/cluster-100k-hold-validated-results-20261104')
BROKER='/opt/rmqtt-quorum-cpu/20261104/rmqtt'
PROBE=BASE+'/connections'
LOCAL_PROBE=pathlib.Path('/Volumes/data/rmqtt/target/x86_64-unknown-linux-musl/release/connections')
EXPECTED_BROKER='eb6f4be285dc7b8af33e820de4f8b4bed64772c9c6961a581b3423c72f4b55b4'
EXPECTED_PROBE='dac46888dfb38b0ee1fc4ec38e67074b00eddb37728271bc392767256bc1b528'


def server(action):
    return prior.remote(prior.SERVER,f'ulimit -n 250000; RMQTT_RAFT_PROFILE=1 RMQTT_BENCH_ROOT={BASE}/case RMQTT_BENCH_BIN={BROKER} python3 {prior.SERVER_SCRIPT} {action} combined',timeout=120)


def observe(action):
    return prior.remote(prior.SERVER,f'RMQTT_100K_BASE={BASE} python3 {BASE}/observer.py {action}',timeout=120)


def prepare():
    OUT.mkdir(exist_ok=False)
    code=f'''from pathlib import Path
import json,os
live=[]
for p in Path('/proc').iterdir():
 if p.name.isdigit():
  try:
   n=(p/'comm').read_text().strip();state=(p/'stat').read_text().rsplit(')',1)[1].split()[0]
   if state!='Z' and (n.startswith('rmqtt') or n in ('connections','perf')):live.append(p.name)
  except OSError:pass
assert not live,live
Path({BASE!r}).mkdir(mode=0o700,parents=True,exist_ok=False)
print(json.dumps(dict(active=live,source_ports=Path('/proc/sys/net/ipv4/ip_local_port_range').read_text().strip())))
'''
    for host in (prior.SERVER,prior.CLIENT):(OUT/f'{host}-preflight.json').write_text(prior.remote(host,'python3 -c '+shlex.quote(code)))
    actual=prior.remote(prior.SERVER,f'sha256sum {BROKER} {prior.SERVER_SCRIPT}').splitlines()
    assert actual[0].split()[0]==EXPECTED_BROKER
    assert actual[1].split()[0]=='a74f6d32f91716c21c9e898ed30d9dcbfb0a66a75465b3708f3154cf5bed61b3'
    subprocess.run(['scp',*prior.SSH[1:],str(LOCAL_PROBE),f'root@{prior.CLIENT}:{PROBE}'],check=True,timeout=120)
    prior.remote(prior.CLIENT,f'chmod 755 {PROBE}')
    assert prior.remote(prior.CLIENT,f'sha256sum {PROBE}').split()[0]==EXPECTED_PROBE
    code='''import pathlib,subprocess,time,json,os
base=pathlib.Path('''+repr(BASE)+''');marker=base/'preflight-existing-output';marker.write_text('reserved')
start=time.time_ns()//1000000+8000
args=['''+repr(PROBE)+''','172.16.240.180:18831,172.16.240.180:18832,172.16.240.180:18833','500',str(marker),'1','--connections-per-step','25000','--client-prefix','hold-preflight','--index-offset','75000','--start-unix-ms',str(start),'--ping-unix-ms',str(start+370000)]
p=subprocess.run(args,capture_output=True,text=True,timeout=10,env=dict(os.environ,TOKIO_WORKER_THREADS='2'))
assert p.returncode!=0 and 'file exists' in p.stderr.lower(),p.stderr
print(json.dumps(dict(parameter_validation_passed=True,stopped_before_network_on_existing_output=True,stderr=p.stderr)))
'''
    (OUT/'probe-argument-preflight.json').write_text(prior.remote(prior.CLIENT,'python3 -c '+shlex.quote(code)))
    hashes=dict(broker=EXPECTED_BROKER,probe=EXPECTED_PROBE,server_helper=actual[1].split()[0])
    for host,name in ((prior.SERVER,'observer.py'),(prior.CLIENT,'client.py')):
        subprocess.run(['scp',*prior.SSH[1:],str(HERE/name),f'root@{host}:{BASE}/{name}'],check=True,timeout=120)
        expected=hashlib.sha256((HERE/name).read_bytes()).hexdigest();assert prior.remote(host,f'sha256sum {BASE}/{name}').split()[0]==expected;hashes[name]=expected
    (OUT/'deployment.json').write_text(json.dumps(hashes,indent=2))
    (OUT/'environment.json').write_text(prior.remote(prior.SERVER,f'RMQTT_BENCH_ROOT={BASE} RMQTT_BENCH_BIN={BROKER} python3 {prior.SERVER_SCRIPT} environment'))


def start_monitor():
    code=f'''import subprocess,pathlib,os,json
base=pathlib.Path({BASE!r})
with (base/'observer.log').open('xb') as log:
 child=subprocess.Popen(['python3',str(base/'observer.py'),'monitor'],env=dict(os.environ,RMQTT_100K_BASE=str(base)),stdout=log,stderr=log,stdin=subprocess.DEVNULL,start_new_session=True)
(base/'observer.json').write_text(json.dumps(dict(pid=child.pid,start_ticks=pathlib.Path('/proc/'+str(child.pid)+'/stat').read_text().rsplit(')',1)[1].split()[19])))
'''
    prior.remote(prior.SERVER,'python3 -c '+shlex.quote(code))


def collect():
    for host,root,label,names in [(prior.SERVER,BASE+'/case/combined','server',['resources.jsonl','processes.json','limits.json','raft-membership.json','raft-startup.json','raft-full.json','raft-hold-end.json','raft-final.json','health.json','abort.json']+[f'node{i}/process.log' for i in (1,2,3)]+[f'{kind}{i}.json' for kind in ('controller','probe') for i in (1,2,3)]), (prior.CLIENT,BASE,'client',['source-ports.json']+['client/'+n for n in ('settings.json','commands.json','client-resources.json','reports.json')]+[f'client/{i}/{n}' for i in range(4) for n in ('report.json','raw.json','process.log')])]:
        code='import pathlib,tarfile,sys;root=pathlib.Path('+repr(root)+');a=tarfile.open(fileobj=sys.stdout.buffer,mode="w|gz");[a.add(root/n,arcname=n,recursive=False) for n in '+repr(names)+' if (root/n).is_file()];a.close()'
        with (OUT/f'{label}.tar.gz').open('xb') as f:p=subprocess.run(prior.SSH+['root@'+host,'python3 -c '+shlex.quote(code)],stdout=f,stderr=subprocess.PIPE,timeout=240)
        assert p.returncode==0,p.stderr.decode()[-500:]
        dest=OUT/label;dest.mkdir()
        with tarfile.open(OUT/f'{label}.tar.gz') as a:a.extractall(dest,filter='data')


def main():
    prepare();started=False;child=None;aborted=None
    try:
        started=True;(OUT/'start.json').write_text(server('start'))
        startup=json.loads(observe('start'));(OUT/'raft-startup.json').write_text(json.dumps(startup,indent=2))
        assert startup['gate']['passed'], 'three-node startup status gate failed'
        for row in startup['membership']:
            memberships=[r for r in row['records'] if r['checksum_valid'] and r['entry']['payload']['Membership']['configs']==[[1,2,3]]]
            assert memberships,'no checksummed three-voter membership in Controller'+str(row['controller'])
            assert startup['gate']['target_revision']>=max(r['entry']['log_id']['index'] for r in memberships)
        start_monitor();print('Three applied Raft memberships {1,2,3} verified; starting 2000/s100k probes',flush=True)
        with (OUT/'client-output.json').open('w') as out,(OUT/'client-ssh.err').open('w') as err:
            command=f'RMQTT_MULTI_ROOT={BASE}/client RMQTT_MULTI_BIN={PROBE} RMQTT_MULTI_RATE=2000 python3 {BASE}/client.py'
            child=subprocess.Popen(prior.SSH+['root@'+prior.CLIENT,command],stdout=out,stderr=err)
            deadline=time.monotonic()+510;settings_sent=False;health={}
            while child.poll() is None:
                if not settings_sent:
                    raw=prior.remote(prior.CLIENT,f'test ! -f {BASE}/client/settings.json || cat {BASE}/client/settings.json')
                    if raw:
                        settings=json.loads(raw);(OUT/'settings.json').write_text(raw)
                        prior.remote(prior.SERVER,'python3 -c '+shlex.quote('from pathlib import Path;Path('+repr(BASE+'/load-settings.json')+').write_text('+repr(raw)+')'));settings_sent=True
                raw=prior.remote(prior.SERVER,f'test ! -f {BASE}/case/combined/health.json || cat {BASE}/case/combined/health.json')
                if raw:
                    health=json.loads(raw);print(json.dumps(dict(wall_ns=health['wall_ns'],tcp=health['mqtt_established'],mem_available_mib=health['mem_kib']['MemAvailable']/1024,raft=[dict(node=r['controller'],leader=r.get('status',{}).get('leader'),term=r.get('status',{}).get('term'),applied=r.get('status',{}).get('last_applied'))for r in health['controllers']])),flush=True)
                    if health['abort_reasons']:aborted=health['abort_reasons']
                if time.monotonic()>deadline:aborted=['client supervisor timeout']
                if aborted:
                    (OUT/'abort.json').write_text(json.dumps(dict(reasons=aborted,health=health),indent=2))
                    prior.remote(prior.CLIENT,f'touch {BASE}/client/abort');break
                time.sleep(10)
            child.wait(timeout=20)
        (OUT/'raft-final.json').write_text(observe('final'))
    finally:
        if child is not None and child.poll() is None:
            prior.remote(prior.CLIENT,f'touch {BASE}/client/abort');child.wait(timeout=20)
        if started:
            try:
                (OUT/'stop.log').write_text(server('stop'))
            except Exception as error:
                (OUT/'stop-error.txt').write_text(str(error))
                code='''import pathlib,json,os,signal,time
root=pathlib.Path('''+repr(BASE+'/case/combined')+''')
items=json.loads((root/'processes.json').read_text())
def alive(p):
 try:
  q=pathlib.Path('/proc/'+str(p['pid']));v=(q/'stat').read_text().rsplit(')',1)[1].split()
  if v[0]=='Z':return False
  assert v[19]==p['start_ticks'] and (q/'exe').resolve()==pathlib.Path('''+repr(BROKER)+''').resolve(),'PID identity changed'
  return True
 except FileNotFoundError:return False
for sig in (signal.SIGTERM,signal.SIGKILL):
 for p in items:
  if alive(p):os.kill(p['pid'],sig)
 time.sleep(3)
assert not any(alive(p) for p in items)
print('test processes stopped after graceful timeout')
'''
                (OUT/'stop-forced.log').write_text(prior.remote(prior.SERVER,'python3 -c '+shlex.quote(code)))
    collect();result=agg.aggregate(OUT/'client/client')
    result['totals_are_partial'] = len(result['workers']) != 4
    if aborted:result['passed']=False;result['issues'].append('aborted: '+','.join(aborted))
    result['raft_gates']={}
    for phase in ('startup','full','hold-end','final'):
        path=OUT/'server'/('raft-'+phase+'.json')
        passed=path.exists() and json.loads(path.read_text()).get('passed') is True
        result['raft_gates'][phase]=passed
        if not passed:result['passed']=False;result['issues'].append('Raft gate failed or missing: '+phase)
    (OUT/'aggregate.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)


if __name__=='__main__':main()
