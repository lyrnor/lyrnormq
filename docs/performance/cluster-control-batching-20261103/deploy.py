import subprocess,pathlib,shlex,hashlib,json
SSH=['ssh','-i','/private/tmp/rmqtt-sg-test-key.pem','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile=/private/tmp/rmqtt-sg-known-hosts','-o','ConnectTimeout=15']
SCP=['scp',*SSH[1:]]
ROOT=pathlib.Path('/Volumes/data/rmqtt')
OUT=pathlib.Path('/private/tmp/rmqtt-7o-server-results');OUT.mkdir(exist_ok=True)
base='/opt/rmqtt-control-batch/20261103';load='/opt/rmqtt-control-batch-load/20261103'
def remote(host,command):
 return subprocess.check_output(SSH+['root@'+host,command],text=True,timeout=120)
def copy(host,source,dest):
 subprocess.run(SCP+[str(source),'root@'+host+':'+dest],check=True,timeout=180)
for host,directory,files in [('47.84.224.186',base,[('/private/tmp/rmqtt-7o-linux','rmqtt'),(ROOT/'scripts/cluster_scale_server.py','cluster_scale_server.py'),(ROOT/'scripts/cluster_scale_limits.py','cluster_scale_limits.py')]),('47.84.241.200',load,[('/private/tmp/rmqtt-7l-connections-linux','connections'),(ROOT/'docs/performance/cluster-quota-free-20261103/single-cohort-client.py','cluster_scale_client.py')])]:
 code='from pathlib import Path;Path('+repr(directory)+').mkdir(mode=0o700,parents=True,exist_ok=False)'
 remote(host,'python3 -c '+shlex.quote(code))
 for source,name in files:
  copy(host,source,directory+'/'+name)
  expected=hashlib.sha256(pathlib.Path(source).read_bytes()).hexdigest()
  actual=remote(host,'sha256sum '+shlex.quote(directory+'/'+name)).split()[0]
  if expected!=actual:raise RuntimeError('uploaded hash mismatch')
 print(json.dumps({'uploaded':host,'directory':directory,'files':len(files)}),flush=True)
 remote(host,'chmod 755 '+shlex.quote(directory+('/rmqtt' if host=='47.84.224.186' else '/connections')))
(OUT/'environment.log').write_text(remote('47.84.224.186','RMQTT_BENCH_ROOT='+base+' RMQTT_BENCH_BIN='+base+'/rmqtt python3 '+base+'/cluster_scale_server.py environment'))
print('Deployment verified; no cluster or pressure process started.',flush=True)
