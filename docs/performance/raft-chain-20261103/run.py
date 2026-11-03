"""One diagnostic cohort, same load/limits; perf is bounded and never starts new load."""
import importlib.util
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import sys
import time

PRIOR = Path(__file__).resolve().parent.parent / 'cluster-multiprocess-load-20261103'
sys.path.insert(0, str(PRIOR))
spec = importlib.util.spec_from_file_location('prior_run', PRIOR / 'run.py')
prior = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prior)
BASE = '/opt/rmqtt-raft-chain/20261103'
OUT = Path('/Volumes/data/rmqtt/target/raft-chain-results-20261103')
LOCAL_BROKER = Path('/Volumes/data/rmqtt/target/x86_64-unknown-linux-musl/release/rmqtt')
prior.BASE, prior.OUT, prior.BROKER = BASE, OUT, BASE + '/rmqtt'


def server(action):
    return prior.remote(prior.SERVER, f'ulimit -n 250000; RMQTT_RAFT_PROFILE=1 '
        f'RMQTT_BENCH_ROOT={BASE}/c2000 RMQTT_BENCH_BIN={BASE}/rmqtt '
        f'python3 {prior.SERVER_SCRIPT} {action} combined')


def prepare():
    OUT.mkdir(exist_ok=False)
    for host in (prior.SERVER, prior.CLIENT):
        code = f'''from pathlib import Path
import json,os
live=[]
for p in Path('/proc').iterdir():
 if p.name.isdigit():
  try:
   n=(p/'comm').read_text().strip()
   if n.startswith('rmqtt') or n=='connections':live.append(p.name)
  except OSError:pass
print(json.dumps({{'live':live,'cpus':os.cpu_count()}}))
assert not live,'existing workload must not be interrupted'
Path({BASE!r}).mkdir(mode=0o700,parents=True,exist_ok=False)
'''
        (OUT / f'{host}-preflight.json').write_text(prior.remote(host, 'python3 -c ' + shlex.quote(code)))
    hashes = {}
    for host, source, name in [(prior.SERVER, LOCAL_BROKER, 'rmqtt'),
            (prior.CLIENT, prior.LOCAL_PROBE, 'connections'),
            (prior.CLIENT, PRIOR / 'client.py', 'client.py')]:
        subprocess.run(['scp', *prior.SSH[1:], str(source), f'root@{host}:{BASE}/{name}'], check=True, timeout=180)
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        assert prior.remote(host, f'sha256sum {BASE}/{name}').split()[0] == digest
        hashes[name] = digest
    prior.remote(prior.SERVER, f'chmod 755 {BASE}/rmqtt')
    prior.remote(prior.CLIENT, f'chmod 755 {BASE}/connections')
    (OUT / 'deployment.json').write_text(json.dumps(hashes, indent=2))
    (OUT / 'environment.json').write_text(prior.remote(prior.SERVER,
        f'RMQTT_BENCH_ROOT={BASE} RMQTT_BENCH_BIN={BASE}/rmqtt python3 {prior.SERVER_SCRIPT} environment'))


def start_observers():
    code = f'''import subprocess,os,pathlib,json,time
base=pathlib.Path({BASE!r});root=base/'c2000'/'combined'
pids=','.join(str(p['pid']) for p in json.loads((root/'processes.json').read_text()))
commands=[
 ['python3',{prior.SERVER_SCRIPT!r},'monitor','combined','--seconds','180'],
 ['timeout','--signal=INT','50','perf','record','-e','cpu-clock','-F','49','--call-graph','dwarf,8192','-p',pids,'-o',str(base/'perf.data'),'--','sleep','45'],
 ['timeout','--signal=INT','50','perf','stat','-e','task-clock,context-switches,cpu-migrations,page-faults','-I','1000','-p',pids,'-o',str(base/'perf-stat.txt'),'--','sleep','45']]
items=[]
for i,command in enumerate(commands):
 log=(base/f'observer{{i}}.log').open('xb')
 child=subprocess.Popen(command,stdout=log,stderr=log,stdin=subprocess.DEVNULL,start_new_session=True,
    env=dict(os.environ,RMQTT_BENCH_ROOT=str(base/'c2000'),RMQTT_BENCH_BIN=str(base/'rmqtt')))
 items.append(dict(pid=child.pid,command=command,start_ticks=pathlib.Path(f'/proc/{{child.pid}}/stat').read_text().rsplit(')',1)[1].split()[19]))
(base/'observers.json').write_text(json.dumps(items,indent=2))
print(json.dumps(dict(observers=items,wall_ns=time.time_ns(),monotonic_ns=time.monotonic_ns())))
'''
    (OUT / 'observers.json').write_text(prior.remote(prior.SERVER, 'python3 -c ' + shlex.quote(code)))


def stop_observers():
    # PID+start-time validation protects against unrelated processes; no broad pkill.
    code = f'''import pathlib,json,os,signal,time
base=pathlib.Path({BASE!r});(base/'c2000'/'combined'/'monitor.stop').touch()
for item in json.loads((base/'observers.json').read_text()):
 try:
  stat=pathlib.Path('/proc/'+str(item['pid'])+'/stat').read_text().rsplit(')',1)[1].split()
  if stat[0]!='Z' and stat[19]==item['start_ticks']:os.killpg(item['pid'],signal.SIGINT)
 except ProcessLookupError:pass
 except FileNotFoundError:pass
print('observer signals sent')
'''
    return prior.remote(prior.SERVER, 'python3 -c ' + shlex.quote(code))


def collect_perf():
    # perf may emit warnings on this VM; preserve stdout/stderr and status explicitly.
    code = f'''import pathlib,subprocess,json,tarfile,sys
base=pathlib.Path({BASE!r});results=[]
for command,name in [(['perf','script','--ns','-i',str(base/'perf.data')],'perf-script'),
 (['perf','report','--stdio','--no-children','-i',str(base/'perf.data')],'perf-report')]:
 try:
  with (base/(name+'.txt')).open('wb') as out,(base/(name+'.err')).open('wb') as err:
   p=subprocess.run(command,stdout=out,stderr=err,timeout=90)
   results.append(dict(command=command,returncode=p.returncode))
 except subprocess.TimeoutExpired:results.append(dict(command=command,timeout=True))
(base/'perf-export.json').write_text(json.dumps(results,indent=2))
files=['perf.data','perf-script.txt','perf-script.err','perf-report.txt','perf-report.err','perf-stat.txt','perf-export.json','observers.json','observer0.log','observer1.log','observer2.log']
with tarfile.open(fileobj=sys.stdout.buffer,mode='w|gz') as archive:
 for name in files:
  if (base/name).is_file():archive.add(base/name,arcname=name,recursive=False)
'''
    with (OUT / 'perf.tar.gz').open('xb') as dest:
        subprocess.run(prior.SSH + ['root@' + prior.SERVER, 'python3 -c ' + shlex.quote(code)], stdout=dest, check=True, timeout=210)


def main():
    prepare()
    active = True
    observers = False
    try:
        (OUT / 'start.log').write_text(server('start'))
        start_observers(); observers = True
        print('Started profile-enabled cluster and finite perf capture', flush=True)
        result = prior.remote(prior.CLIENT, f'RMQTT_MULTI_ROOT={BASE}/client RMQTT_MULTI_BIN={BASE}/connections '
            f'RMQTT_MULTI_RATE=2000 python3 {BASE}/client.py', timeout=240)
        (OUT / 'client-output.json').write_text(result)
        (OUT / 'state.json').write_text(server('state'))
    finally:
        if observers:
            (OUT / 'observer-stop.log').write_text(stop_observers())
        if active:
            (OUT / 'stop.log').write_text(server('stop'))
    prior.collect()
    result = prior.aggregate(OUT / 'client')
    (OUT / 'aggregate.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result), flush=True)
    collect_perf()


if __name__ == '__main__':
    main()
