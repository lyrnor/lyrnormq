"""Re-run the failed2000/s cohort with four independent connection probes."""
import hashlib
import json
import pathlib
import shlex
import subprocess
import tarfile
import time

from aggregate import aggregate

SERVER, CLIENT = '47.84.224.186', '47.84.241.200'
BASE = '/opt/rmqtt-multiprocess/20261103'
OUT = pathlib.Path('/private/tmp/rmqtt-multiprocess-results')
LOCAL_PROBE = pathlib.Path('/private/tmp/rmqtt-multi-connections-linux')
BROKER = '/opt/rmqtt-control-batch/20261103/rmqtt'
SERVER_SCRIPT = '/opt/rmqtt-control-batch/20261103/cluster_scale_server.py'
SSH = ['ssh', '-i', '/private/tmp/rmqtt-sg-test-key.pem', '-o', 'BatchMode=yes',
       '-o', 'IdentitiesOnly=yes', '-o', 'StrictHostKeyChecking=yes',
       '-o', 'UserKnownHostsFile=/private/tmp/rmqtt-sg-known-hosts',
       '-o', 'ConnectTimeout=15', '-o', 'ServerAliveInterval=15',
       '-o', 'ServerAliveCountMax=3']


def remote(host, command, timeout=120):
    result = subprocess.run(SSH + ['root@' + host, command], capture_output=True,
                            text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f'{host}: {result.returncode}: {result.stdout[-1500:]} {result.stderr[-1500:]}')
    return result.stdout


def server(action):
    return remote(SERVER, f'ulimit -n 250000; RMQTT_BENCH_ROOT={BASE}/c2000 '
                  f'RMQTT_BENCH_BIN={BROKER} python3 {SERVER_SCRIPT} {action} combined')


def prepare():
    OUT.mkdir(exist_ok=False)
    expected_broker = '87ced3e81da295b4273b2757c8fc4472409d88472e5c77cca77ba472fd28854b'
    if remote(SERVER, 'sha256sum ' + BROKER).split()[0] != expected_broker:
        raise RuntimeError('Broker binary changed')
    digest = hashlib.sha256(LOCAL_PROBE.read_bytes()).hexdigest()
    for host in (SERVER, CLIENT):
        code = f'''from pathlib import Path
import json,os
live=[]
for p in Path('/proc').iterdir():
 if p.name.isdigit():
  try:
   name=(p/'comm').read_text().strip()
   if name.startswith('rmqtt') or name=='connections':live.append(p.name)
  except OSError:pass
print(json.dumps({{'live':live,'cpus':os.cpu_count()}}))
assert not live,'existing workload must not be interrupted'
Path({BASE!r}).mkdir(mode=0o700,parents=True,exist_ok=False)
'''
        (OUT / f'{host}-preflight.json').write_text(remote(host, 'python3 -c ' + shlex.quote(code)))
    for source, name in [(LOCAL_PROBE, 'connections'), (pathlib.Path(__file__).with_name('client.py'), 'client.py')]:
        subprocess.run(['scp', *SSH[1:], str(source), f'root@{CLIENT}:{BASE}/{name}'],
                       check=True, timeout=120)
        expected = hashlib.sha256(source.read_bytes()).hexdigest()
        if remote(CLIENT, 'sha256sum ' + BASE + '/' + name).split()[0] != expected:
            raise RuntimeError('upload checksum mismatch')
    remote(CLIENT, 'chmod 755 ' + BASE + '/connections')
    (OUT / 'deployment.json').write_text(json.dumps(dict(broker_sha256=expected_broker,
        probe_sha256=digest, probe_local_path=str(LOCAL_PROBE), broker_changed=False), indent=2))
    (OUT / 'environment.json').write_text(remote(SERVER, f'RMQTT_BENCH_ROOT={BASE} '
        f'RMQTT_BENCH_BIN={BROKER} python3 {SERVER_SCRIPT} environment'))


def collect():
    selections = [(SERVER, BASE + '/c2000/combined', 'server',
                   ['resources.jsonl', 'processes.json', 'limits.json', 'monitor.log',
                    'node1/process.log', 'node2/process.log', 'node3/process.log']),
                  (CLIENT, BASE + '/client', 'client',
                   ['settings.json', 'commands.json', 'client-resources.json', 'reports.json'] +
                   [f'{i}/{name}' for i in range(4) for name in ('report.json', 'raw.json', 'process.log')])]
    for host, root, label, names in selections:
        code = ('import pathlib,tarfile,sys;root=pathlib.Path(' + repr(root) + ');'
                'archive=tarfile.open(fileobj=sys.stdout.buffer,mode="w|gz");'
                '[archive.add(root/name,arcname=name,recursive=False) for name in '
                + repr(names) + ' if (root/name).is_file()];archive.close()')
        destination = OUT / f'{label}.tar.gz'
        with destination.open('xb') as output:
            result = subprocess.run(SSH + ['root@' + host, 'python3 -c ' + shlex.quote(code)],
                                    stdout=output, stderr=subprocess.PIPE, timeout=180)
        if result.returncode:
            raise RuntimeError(f'evidence copy failed: {host}: {result.stderr.decode()[-1000:]}')
        directory = OUT / label
        directory.mkdir()
        with tarfile.open(destination) as archive:
            archive.extractall(directory, filter='data')


def main():
    prepare()
    active = True
    print(json.dumps({'event': 'start_four_processes', 'rate': 2000, 'target': 10000,
                      'time_ns': time.time_ns()}), flush=True)
    try:
        (OUT / 'start.log').write_text(server('start'))
        code = (f"import subprocess,os,pathlib;r=pathlib.Path('{BASE}/c2000');"
                "f=(r/'combined'/'monitor.log').open('xb');"
                f"subprocess.Popen(['python3','{SERVER_SCRIPT}','monitor','combined','--seconds','300'],"
                f"env=dict(os.environ,RMQTT_BENCH_ROOT=str(r),RMQTT_BENCH_BIN='{BROKER}'),"
                "stdout=f,stderr=f,start_new_session=True,stdin=subprocess.DEVNULL)")
        remote(SERVER, 'python3 -c ' + shlex.quote(code))
        output = remote(CLIENT, f'RMQTT_MULTI_ROOT={BASE}/client RMQTT_MULTI_BIN={BASE}/connections '
                        f'RMQTT_MULTI_RATE=2000 python3 {BASE}/client.py', timeout=240)
        (OUT / 'client-output.json').write_text(output)
        remote(SERVER, f'touch {BASE}/c2000/combined/monitor.stop')
        (OUT / 'state.json').write_text(server('state'))
        (OUT / 'stop.log').write_text(server('stop'))
        active = False
    finally:
        if active:
            (OUT / 'exception-stop.log').write_text(server('stop'))
    collect()
    result = aggregate(OUT / 'client')
    (OUT / 'aggregate.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
