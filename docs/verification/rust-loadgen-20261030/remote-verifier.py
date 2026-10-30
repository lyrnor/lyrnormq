from pathlib import Path
import subprocess,json,hashlib,time
out=Path('/var/log/rmqtt-loadgen/acceptance-20261030');out.mkdir(parents=True,exist_ok=True);binary=Path('/opt/rmqtt-loadgen/rmqtt-loadgen');checks=[];error=None
try:
 for qos in (0,1,2):
  report=out/f'q{qos}.json';raw=out/f'q{qos}.jsonl';cmd=[str(binary),'--addr','172.30.139.129:1883','--qos',str(qos),'--pairs','2500','--inflight','1','--payload-bytes','128','--warmup-ms','1000','--duration-ms','5000','--drain-timeout-ms','5000','--connect-timeout-ms','10000','--max-messages','5000000','--output',str(report),'--raw',str(raw)]
  p=subprocess.run(cmd,capture_output=True,text=True,timeout=90);(out/f'q{qos}.stdout').write_text(p.stdout);(out/f'q{qos}.stderr').write_text(p.stderr);assert p.returncode==0,(p.returncode,p.stderr,report.read_text() if report.exists() else 'no report');r=json.loads(report.read_text());assert r['success'];records=[json.loads(line) for line in raw.open()]
  a=r['accounting'];c=a['counters'];assert c['attempted']==c['write_completed']==c['delivered_unique']==c['completed']==len(records);assert a['not_sent']==a['not_delivered']==a['incomplete_attempts']==0
  fields={'schedule_to_send':('send_started_ns','scheduled_ns'),'send_to_write_completed':('write_completed_ns','send_started_ns'),'send_to_delivery':('delivery_ns','send_started_ns'),'schedule_to_delivery':('delivery_ns','scheduled_ns'),'send_to_source_completed':('ack_ns','send_started_ns'),'send_to_full_completion':('completed_ns','send_started_ns')}
  for name,(end,start) in fields.items():
   m=r['latencies'][name]
   if qos==0 and name=='send_to_source_completed':assert m is None;continue
   v=sorted(s[end]-s[start] for s in records if s[end] is not None);assert len(v)==m['count']
   for percentile in (50,95,99):assert v[(percentile*len(v)+99)//100-1]==m[f'p{percentile}_ns']
  checks.append({'qos':qos,'pairs':2500,'connections':5000,'mode':'closed_loop_window1','measured_messages':len(records),'all_six_raw_quantiles_verified':True,'success':True,'report':str(report),'raw':str(raw),'p99_send_to_delivery_ns':r['latencies']['send_to_delivery']['p99_ns'],'offering_window_delivery_tps':r['offering_window_tps']['delivered_unique_per_second'],'command':cmd});print(json.dumps(checks[-1]),flush=True)
except Exception as exc:error=repr(exc);print(error,flush=True)
finally:
 (out/'verification.json').write_text(json.dumps({'scope':'5000-connection correctness acceptance,not a standalone max-throughput benchmark','binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'checks':checks,'error':error},indent=2)+'\n')
if error:raise SystemExit(1)
