import subprocess,pathlib,time,json,shlex,sys
S='47.84.224.186';C='47.84.241.200';BASE='/opt/rmqtt-quota-free/20261103';CB='/opt/rmqtt-quota-free-load/20261103';BIN=BASE+'/rmqtt';OUT=pathlib.Path('/private/tmp/quota-free-results');OUT.mkdir(exist_ok=True)
sys.path.insert(0,'/Volumes/data/rmqtt/scripts')
from cluster_scale_limits import case_passed
SSH=['ssh','-i','/private/tmp/rmqtt-sg-test-key.pem','-o','ConnectTimeout=15','-o','IdentitiesOnly=yes','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile=/private/tmp/rmqtt-sg-known-hosts','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3']
def remote(host,cmd,timeout=120):
 p=subprocess.run(SSH+['root@'+host,cmd],capture_output=True,text=True,timeout=timeout)
 if p.returncode:raise RuntimeError(f'{host} exit{p.returncode}: {p.stdout[-1500:]} {p.stderr[-1500:]}')
 return p.stdout

def server(name,action):
 return remote(S,f'ulimit -n 250000; RMQTT_BENCH_ROOT={BASE}/{name} RMQTT_BENCH_BIN={BIN} python3 {BASE}/cluster_scale_server.py {action} combined')
active=None
try:
 for name in ['c1000']:
  print(json.dumps({'event':'begin','case':name,'time_ns':time.time_ns()}),flush=True)
  active=name
  (OUT/(name+'-start.log')).write_text(server(name,'start'))
  code=f"import subprocess,os,pathlib;r=pathlib.Path('{BASE}/{name}');f=(r/'monitor.log').open('wb');subprocess.Popen(['python3','{BASE}/cluster_scale_server.py','monitor','combined','--seconds','1200'],env=dict(os.environ,RMQTT_BENCH_ROOT=str(r),RMQTT_BENCH_BIN='{BIN}'),stdout=f,stderr=f,start_new_session=True,stdin=subprocess.DEVNULL)"
  remote(S,'python3 -c '+shlex.quote(code))
  result=remote(C,f'RMQTT_SCALE_LOAD_ROOT={CB} python3 {CB}/cluster_scale_client.py {name}',timeout=1100)
  (OUT/(name+'-client.json')).write_text(result)
  r=json.loads(result);report=r.get('report') or {};passed=bool(r.get('exit')==0 and not r.get('timeout') and report.get('plateau_passed') and len(report.get('steps',[]))==1 and report['steps'][0].get('live_after_hold_ping')==10000)
  print(json.dumps({'event':'end','case':name,'exit':r['exit'],'timeout':r['timeout'],'capacity_passed':passed,'success':report.get('success'),'phase':report.get('phase'),'error':report.get('error'),'plateau_passed':report.get('plateau_passed'),'steps':report.get('steps'),'accounting':report.get('accounting'),'latencies':report.get('latencies',{}).get('send_to_delivery')}),flush=True)
  remote(S,f'touch {BASE}/{name}/combined/monitor.stop')
  (OUT/(name+'-state.json')).write_text(server(name,'state'))
  (OUT/(name+'-stop.log')).write_text(server(name,'stop'))
  active=None
  if not passed:
   print(json.dumps({'event':'matrix_stopped_on_first_failure','case':name}),flush=True)
   break
finally:
 if active:
  try:(OUT/(active+'-exception-stop.log')).write_text(server(active,'stop'))
  except Exception as e:print('STOP_FAILED',repr(e),flush=True)
