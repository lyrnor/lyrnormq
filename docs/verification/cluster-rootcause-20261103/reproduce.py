import concurrent.futures,json,os,pathlib,socket,sys,time
os.environ.setdefault('TOKIO_WORKER_THREADS','4')
os.environ.setdefault('RUST_LOG','warn')
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[3]/'tests'))
from cluster_memory_blackbox import CombinedMemoryFleet
from mqtt5_blackbox import connect5
from blackbox import receive,frame
root=pathlib.Path(sys.argv[1]);root.mkdir()
fleet=CombinedMemoryFleet(str(root))
count=int(sys.argv[2]);rate=int(sys.argv[3]);results=[];live=[]
def connect(i,start):
 due=start+i/rate
 time.sleep(max(0,due-time.monotonic()))
 began=time.monotonic();s=None
 try:
  s=socket.create_connection(('127.0.0.1',fleet.brokers[i%3].mqtt),timeout=3);s.settimeout(10)
  s.sendall(connect5(f'diag-{i}',clean=True,properties=[(0x11,0)]))
  h,b=receive(s)
  ok=h==32 and len(b)>=2 and b[1]==0
  if ok:live.append(s)
  else:s.close()
  return {'i':i,'start':began-start,'ms':(time.monotonic()-began)*1000,'ok':ok,'reply':b.hex()}
 except Exception as e:
  if s:s.close()
  return {'i':i,'start':began-start,'ms':(time.monotonic()-began)*1000,'ok':False,'error':repr(e)}
try:
 fleet.start();print('ready',flush=True)
 start=time.monotonic()
 with concurrent.futures.ThreadPoolExecutor(max_workers=min(count,384)) as ex:
  results=list(ex.map(lambda i:connect(i,start),range(count)))
 print(json.dumps({'success':sum(r['ok'] for r in results),'total':count,'results':results}),flush=True)
 time.sleep(1)
 (root/'status.json').write_text(json.dumps(fleet.status()))
finally:
 (root/'results.json').write_text(json.dumps(results,indent=2))
 for s in live:s.close()
 for i,n in enumerate(fleet.brokers):
  if n.output:
   n.output.seek(0);(root/f'node{i+1}.log').write_bytes(n.output.read())
 fleet.stop()
