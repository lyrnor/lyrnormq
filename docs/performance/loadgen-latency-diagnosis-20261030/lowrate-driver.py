from pathlib import Path
import subprocess,json,time,os
base=Path('/var/log/lyrnormq-loadgen/lowrate-20261030');base.mkdir(parents=True,exist_ok=True);checks=[];error=None
try:
 for qos in (0,1,2):
  report=base/f'q{qos}.json';raw=base/f'q{qos}.jsonl';cmd=['/opt/lyrnormq-loadgen/lyrnormq-loadgen','--addr','172.30.139.129:1883','--qos',str(qos),'--pairs','2500','--inflight','1','--payload-bytes','128','--rate','5000','--warmup-ms','1000','--duration-ms','10000','--drain-timeout-ms','5000','--connect-timeout-ms','10000','--max-messages','5000000','--output',str(report),'--raw',str(raw)]
  start=time.time();p=subprocess.run(cmd,capture_output=True,text=True,timeout=90);stop=time.time();(base/f'q{qos}.stdout').write_text(p.stdout);(base/f'q{qos}.stderr').write_text(p.stderr);assert p.returncode in (0,2),(p.returncode,p.stderr);r=json.loads(report.read_text());samples=[json.loads(x) for x in raw.open()];a=r['accounting'];c=a['counters'];assert c['scheduled']==50000 and c['attempted']==c['write_completed']==c['delivered_unique']==c['completed']==len(samples);assert c['attempted']+a['not_sent']==50000 and a['not_delivered']==a['incomplete_attempts']==0
  assert not r['issues'] or all('scheduled messages were not sent' in x for x in r['issues']),r['issues']
  for name,end,startfield in [('schedule_to_send','send_started_ns','scheduled_ns'),('send_to_delivery','delivery_ns','send_started_ns'),('schedule_to_delivery','delivery_ns','scheduled_ns'),('send_to_full_completion','completed_ns','send_started_ns')]:
   v=sorted(s[end]-s[startfield] for s in samples if s[end] is not None);d=r['latencies'][name];assert len(v)==d['count'];assert v[(99*len(v)+99)//100-1]==d['p99_ns']
  row={'qos':qos,'process_start_unix':start,'process_stop_unix':stop,'offering_window_delivery_tps':r['offering_window_tps']['delivered_unique_per_second'],'p99_send_to_delivery_ms':r['latencies']['send_to_delivery']['p99_ns']/1e6,'p99_schedule_to_delivery_ms':r['latencies']['schedule_to_delivery']['p99_ns']/1e6,'p99_schedule_lag_ms':r['latencies']['schedule_to_send']['p99_ns']/1e6,'sent':c['attempted'],'scheduled':c['scheduled'],'not_sent':a['not_sent'],'report_success':r['success'],'command':cmd};checks.append(row);print(json.dumps(row),flush=True)
except Exception as exc:error=repr(exc);print(error,flush=True)
finally:(base/'verification.json').write_text(json.dumps({'scope':'same5000connections,low offered5000msg/s contrast;not a maximum test;missed deadlines retained','checks':checks,'error':error},indent=2)+'\n')
if error:raise SystemExit(1)
