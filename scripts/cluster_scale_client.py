import pathlib,subprocess,time,os,json,resource,sys,gzip,shutil,hashlib
ROOT=pathlib.Path(os.environ.get('LYRNORMQ_SCALE_LOAD_ROOT','/opt/lyrnormq-control-optimized-load/20261103'))
name=sys.argv[1];r=ROOT/name;r.mkdir()
resource.setrlimit(resource.RLIMIT_NOFILE,(250000,250000))
addrs='172.16.240.180:18831,172.16.240.180:18832,172.16.240.180:18833'
if name.startswith('c'):
 cmd=[str(ROOT/'connections'),addrs,name[1:],str(r/'report.json')];deadline=900
else:
 cmd=['/opt/lyrnormq-cluster-load/20261103/lyrnormq-loadgen','--addrs',addrs,'--protocol','5','--qos',name[1:],'--pairs','5000','--setup-concurrency','8','--payload-bytes','128','--rate','10000','--inflight','1','--warmup-ms','3000','--duration-ms','60000','--drain-timeout-ms','30000','--connect-timeout-ms','30000','--io-timeout-ms','10000','--max-messages','2000000','--output',str(r/'report.json'),'--raw',str(r/'raw.jsonl')];deadline=420
start=time.time_ns();samples=[];timeout=False
with (r/'process.log').open('xb') as log:
 p=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT,env=dict(os.environ,TOKIO_WORKER_THREADS='8'))
 end=time.monotonic()+deadline
 while p.poll() is None:
  try:
   s=pathlib.Path(f'/proc/{p.pid}/stat').read_text().rsplit(')',1)[1].split();samples.append({'wall_ns':time.time_ns(),'ticks':int(s[11])+int(s[12]),'rss_bytes':int(s[21])*os.sysconf('SC_PAGE_SIZE')})
  except FileNotFoundError:pass
  if time.monotonic()>end:p.kill();p.wait();timeout=True;break
  time.sleep(1)
record={'command':cmd,'exit':p.returncode,'timeout':timeout,'started_ns':start,'ended_ns':time.time_ns(),'cpu_hz':os.sysconf('SC_CLK_TCK'),'samples':samples}
(r/'client-resources.json').write_text(json.dumps(record,indent=2))
raw=r/'raw.jsonl'
if raw.exists():
 with raw.open('rb') as src,gzip.open(str(raw)+'.gz','wb') as dst:shutil.copyfileobj(src,dst)
 raw.unlink()
print(json.dumps({'case':name,'exit':p.returncode,'timeout':timeout,'report':json.loads((r/'report.json').read_text()) if (r/'report.json').exists() and (r/'report.json').stat().st_size else None}),flush=True)
