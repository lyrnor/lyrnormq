import subprocess,pathlib,json
SSH=['ssh','-i','/private/tmp/rmqtt-sg-test-key.pem','-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile=/private/tmp/rmqtt-sg-known-hosts','-o','ConnectTimeout=15']
SCP=['scp',*SSH[1:]]
OUT=pathlib.Path('/private/tmp/rmqtt-7o-server-results')
base='/opt/rmqtt-control-batch/20261103';load='/opt/rmqtt-control-batch-load/20261103'
files=[('47.84.224.186',base+'/environment.json','server-environment.json'),('47.84.224.186',base+'/c1000/combined/resources.jsonl','server-resources.jsonl'),('47.84.224.186',base+'/c1000/combined/processes.json','server-processes.json'),('47.84.224.186',base+'/c1000/combined/limits.json','limits.json')]
files += [('47.84.224.186',base+'/c1000/combined/node'+str(i)+'/process.log','node'+str(i)+'.log') for i in range(1,4)]
files += [('47.84.241.200',load+'/c1000/'+name,name) for name in ['report.json','client-resources.json','process.log']]
for host,remote,local in files:
 subprocess.run(SCP+['root@'+host+':'+remote,str(OUT/local)],check=True,timeout=120)
print('Copied only logs/results/configuration bounds; no token or data files.',flush=True)
