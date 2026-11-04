"""One requested100k connection hold; no broker compilation or tuning."""
import concurrent.futures,hashlib,importlib.util,json,pathlib,shlex,subprocess,time,tarfile
HERE=pathlib.Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('ssh_prior',HERE.parent/'cluster-multiprocess-load-20261103/run.py');prior=importlib.util.module_from_spec(spec);spec.loader.exec_module(prior)
spec=importlib.util.spec_from_file_location('hold_aggregate',HERE/'aggregate.py');agg=importlib.util.module_from_spec(spec);spec.loader.exec_module(agg)
BASE='/opt/rmqtt-100k-hold/20261104'
OUT=pathlib.Path('/Volumes/data/rmqtt/target/cluster-100k-hold-results-20261104')
BROKER='/opt/rmqtt-quorum-cpu/20261104/rmqtt'
PROBE='/opt/rmqtt-quorum-cpu/20261104/connections'
EXPECTED_BROKER='eb6f4be285dc7b8af33e820de4f8b4bed64772c9c6961a581b3423c72f4b55b4'
EXPECTED_PROBE='556b3d560b3c79a4c77e9f06edc93052a876f6fb8a4be86eeaf163f70784b4ad'


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
    assert prior.remote(prior.CLIENT,f'sha256sum {PROBE}').split()[0]==EXPECTED_PROBE
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
    (OUT/'aggregate.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)


if __name__=='__main__':main()
