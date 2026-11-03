#!/usr/bin/env python3
import argparse,os,json,pathlib,subprocess,time,signal,http.client,secrets,sqlite3,sys,platform,hashlib,socket
from cluster_scale_limits import mqtt_options, validate_commands

ROOT=pathlib.Path(os.environ.get('RMQTT_BENCH_ROOT','/opt/rmqtt-cluster-benchmark/20261103'))
BIN=pathlib.Path(os.environ.get('RMQTT_BENCH_BIN',str(ROOT/'rmqtt')))

_last_status = (0, [])
def readstate(root):
    global _last_status
    now = time.monotonic_ns()
    if now - _last_status[0] < 10_000_000_000:
        return _last_status[1]
    out = []
    for i in range(1,4):
        began = time.monotonic_ns()
        try:
            result = subprocess.run([str(BIN), 'cluster-status', '--cluster-config', str(root/f'probe{i}.json')],
                                    capture_output=True, text=True, timeout=5, check=True,
                                    env=dict(os.environ, TOKIO_WORKER_THREADS='1', RUST_LOG='error'))
            status = json.loads(result.stdout)
            out.append({'controller':i, 'applied':status['last_applied'], 'term':status['term'],
                        'leader':status['leader'], 'metadata_client_owners':status['clients'], 'source':'live_status_rpc',
                        'read_start_ns':began, 'read_end_ns':time.monotonic_ns()})
        except (OSError, ValueError, subprocess.SubprocessError, KeyError) as error:
            out.append({'controller':i, 'error':type(error).__name__, 'read_start_ns':began,
                        'read_end_ns':time.monotonic_ns()})
    _last_status = (now, out)
    return out

def statproc(pid):
    text=pathlib.Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()
    return {'pid':pid,'ticks':int(text[11])+int(text[12]),'rss_bytes':int(text[21])*os.sysconf('SC_PAGE_SIZE'),'threads':int(text[17]),'fd':len(list(pathlib.Path(f'/proc/{pid}/fd').iterdir()))}

def start(topology):
    root=ROOT/topology
    if root.exists():raise RuntimeError('topology directory already exists; use new run root, preserve old evidence')
    root.mkdir(parents=True)
    token=ROOT/'peer.token'
    if not token.exists():token.write_text(secrets.token_hex(32));token.chmod(0o600)
    adm=root/'admin.token';adm.write_text(secrets.token_hex(32));adm.chmod(0o600)
    controllers=[{'id':i,'address':f'http://127.0.0.1:{17000+i}'} for i in range(1,4)]
    replicas=[{'id':100+i,'address':f'http://127.0.0.1:{17100+i}'} for i in range(1,4)]
    for i in range(1,4):
        common={'cluster_id':f'rmqtt-control-{ROOT.name}-{topology}-20261103','token_file':str(token),'controllers':controllers,'lease_ms':3000}
        ctrl=dict(common,role='controller',node_id=i,listen=f'127.0.0.1:{17000+i}',data_dir=str(root/f'controller-data{i}'),bootstrap=i==1)
        broker=dict(common,role='broker',node_id=100+i,listen=f'127.0.0.1:{17100+i}',advertise=f'http://127.0.0.1:{17100+i}',data_dir=str(root/f'broker-data{100+i}'),durable={'replicas':replicas})
        (root/f'controller{i}.json').write_text(json.dumps(ctrl,indent=2))
        # Status-only probes must not silently fall back to a different Controller.
        # The target keeps its real endpoint; alternate seeds refuse connections.
        probe_peers=[dict(peer, address=(peer['address'] if peer['id']==i else f"http://127.0.0.1:{peer['id']}")) for peer in controllers]
        probe_peers.sort(key=lambda peer: peer['id']!=i)
        probe=dict(ctrl,controllers=probe_peers)
        (root/f'probe{i}.json').write_text(json.dumps(probe,indent=2))
        (root/f'broker{100+i}.json').write_text(json.dumps(broker,indent=2))
    processes=[]
    def spawn(name,cmd):
        d=root/name;d.mkdir()
        with (d/'process.log').open('wb') as log:
            p=subprocess.Popen(cmd,cwd=d,stdout=log,stderr=subprocess.STDOUT,start_new_session=True,stdin=subprocess.DEVNULL,env=dict(os.environ,TOKIO_WORKER_THREADS='4' if topology=='combined' else '2',RUST_LOG='warn'))
        processes.append({'name':name,'pid':p.pid,'command':cmd,'start_ticks':pathlib.Path(f'/proc/{p.pid}/stat').read_text().rsplit(')',1)[1].split()[19]})
        (root/'processes.json').write_text(json.dumps(processes,indent=2))
    commands=[]
    for i in range(1,4):
        cmd=[str(BIN),'--cluster-config',str(root/f'broker{100+i}.json'),'--listen',f'172.16.240.180:{18830+i}','--admin-listen',f'127.0.0.1:{18760+i}','--admin-token-file',str(adm),'--rules-config',str(root/f'node{i}'/'rules.sqlite'),'--access-control-config',str(root/f'node{i}'/'access.sqlite'),'--auth-rate-per-second','0','--max-handshakes','1024','--max-auth-tasks','128',*mqtt_options(100000)]
        if topology=='combined':cmd+=['--cluster-controller-config',str(root/f'controller{i}.json')]
        commands.append({'name':f'node{i}', 'command':cmd})
    # Verify the entire topology before the first process starts.
    if topology=='combined': validate_commands(commands,100000)
    (root/'limits.json').write_text(json.dumps(mqtt_options(100000)))
    if topology=='separate':
        for i in range(1,4):spawn(f'controller{i}',[str(BIN),'--cluster-config',str(root/f'controller{i}.json')])
    for item in commands:spawn(item['name'],item['command'])
    end=time.monotonic()+60
    while time.monotonic()<end:
        try:
            for i in range(1,4):
                conn=http.client.HTTPConnection('127.0.0.1',18760+i,timeout=2)
                conn.request('GET','/api/v1/overview',headers={'Authorization':'Bearer '+adm.read_text()})
                res=conn.getresponse();body=json.loads(res.read());conn.close()
                if res.status!=200 or not body.get('ready'):raise ValueError('not ready')
            print(json.dumps({'topology':topology,'processes':processes,'controllers':readstate(root)}),flush=True);return
        except (OSError,ValueError,http.client.HTTPException):time.sleep(.2)
    raise RuntimeError('startup deadline, inspect logs')

def stop(topology):
    root=ROOT/topology
    (root/'monitor.stop').touch()
    ps=json.loads((root/'processes.json').read_text())
    def alive(p):
        try:
            stat=pathlib.Path(f"/proc/{p['pid']}/stat").read_text().rsplit(')',1)[1].split()
            if stat[0]=='Z':return False
            if pathlib.Path(f"/proc/{p['pid']}/exe").resolve(strict=True)!=BIN.resolve(strict=True) or stat[19]!=p['start_ticks']:
                raise RuntimeError('PID identity changed; refusing signal')
            return True
        except FileNotFoundError:return False
    for group in [[p for p in ps if p['name'].startswith('node')], [p for p in ps if p['name'].startswith('controller')]]:
        for p in group:
            if alive(p):os.kill(p['pid'],signal.SIGINT)
        end=time.monotonic()+35
        while time.monotonic()<end:
            remaining=[p['pid'] for p in group if alive(p)]
            if not remaining:break
            time.sleep(.2)
        else:raise RuntimeError(f'processes did not exit: {remaining}')
    print('stopped')

def monitor(topology,seconds):
    root=ROOT/topology;ps=json.loads((root/'processes.json').read_text())
    with (root/'resources.jsonl').open('x') as out:
        end=time.monotonic()+seconds
        while time.monotonic()<end and not (root/'monitor.stop').exists():
            row={'wall_ns':time.time_ns(),'monotonic_ns':time.monotonic_ns(),'cpu_hz':os.sysconf('SC_CLK_TCK'),'processes':[],'controllers':readstate(root)}
            for p in ps:
                try:row['processes'].append(dict(name=p['name'],**statproc(p['pid'])))
                except (FileNotFoundError,ProcessLookupError):pass
            # Actual active data connections by local port; excludes LISTEN sockets.
            counts={str(18830+i):0 for i in range(1,4)}
            for line in pathlib.Path('/proc/net/tcp').read_text().splitlines()[1:]:
                parts=line.split();port=str(int(parts[1].split(':')[1],16))
                if parts[3]=='01' and port in counts:counts[port]+=1
            row['mqtt_established']=counts
            row['diskstats']=[line.split() for line in pathlib.Path('/proc/diskstats').read_text().splitlines() if line.split()[2]=='vda']
            out.write(json.dumps(row)+'\n');out.flush();time.sleep(1)

def environment():
    info={'hostname':socket.gethostname(),'platform':platform.platform(),'cpus':os.cpu_count(),'python':platform.python_version(),'meminfo':pathlib.Path('/proc/meminfo').read_text(),'worker_threads':{'combined_each':4,'separate_each':2,'total_each_topology':12},'server_private':'172.16.240.180','clock_hz':os.sysconf('SC_CLK_TCK'),'binary_sha256':hashlib.sha256(BIN.read_bytes()).hexdigest()}
    (ROOT/'environment.json').write_text(json.dumps(info,indent=2));print(json.dumps({k:v for k,v in info.items() if k!='meminfo'}))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['start','stop','monitor','state','environment']);p.add_argument('topology',nargs='?',choices=['combined','separate']);p.add_argument('--seconds',type=int,default=1200);a=p.parse_args()
    if a.action=='start':start(a.topology)
    elif a.action=='stop':stop(a.topology)
    elif a.action=='monitor':monitor(a.topology,a.seconds)
    elif a.action=='state':print(json.dumps(readstate(ROOT/a.topology)))
    else:environment()
