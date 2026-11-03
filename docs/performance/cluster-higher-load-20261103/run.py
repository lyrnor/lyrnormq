"""Finite serial acceptance against the already-deployed, SHA-pinned binaries."""
import json
import pathlib
import shlex
import subprocess
import sys
import time

sys.dont_write_bytecode = True
REPO = pathlib.Path('/Volumes/data/rmqtt')
sys.path.insert(0, str(REPO / 'scripts'))
from cluster_scale_limits import case_passed, validate_environment

SERVER, CLIENT = '47.84.224.186', '47.84.241.200'
BASE = '/opt/rmqtt-higher-load/20261103'
BROKER = '/opt/rmqtt-control-batch/20261103/rmqtt'
SERVER_SCRIPT = '/opt/rmqtt-control-batch/20261103/cluster_scale_server.py'
CONNECTIONS = '/opt/rmqtt-control-batch-load/20261103/connections'
LOADGEN = '/opt/rmqtt-cluster-load/20261103/rmqtt-loadgen'
OUT = pathlib.Path('/private/tmp/rmqtt-higher-load-results')
ADDRS = [f'172.16.240.180:{port}' for port in (18831, 18832, 18833)]
CASES = ['c2000', 'c4000', 'q0', 'q1', 'q2', 'cscale']
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


def server(case, action):
    return remote(SERVER, f'ulimit -n 250000; RMQTT_BENCH_ROOT={BASE}/{case} '
                  f'RMQTT_BENCH_BIN={BROKER} python3 {SERVER_SCRIPT} {action} combined')


def passed(case, result):
    if result.get('exit') != 0 or result.get('timeout'):
        return False
    report = result.get('report') or {}
    if case.startswith('c'):
        count = 10 if case == 'cscale' else 1
        rate = 1000 if case == 'cscale' else int(case[1:])
        steps = report.get('steps') or []
        return bool(report.get('plateau_passed') and report.get('rate') == rate
                    and report.get('endpoints') == ADDRS
                    and report.get('requested_plateaus') == count and len(steps) == count
                    and all(s.get('target') == (i + 1) * 10000
                            and s.get('live_after_hold_ping') == (i + 1) * 10000
                            and s.get('new_scheduled') == s.get('new_attempted')
                            == s.get('new_connack_success') == 10000
                            and s.get('not_started') == 0 and not s.get('errors')
                            and not s.get('ping_errors') for i, s in enumerate(steps)))
    expected = dict(version='V5', qos=int(case[1:]), pairs=5000, payload_bytes=128,
                    rate=10000, duration_ms=60000, warmup_ms=3000,
                    drain_timeout_ms=30000, connect_timeout_ms=30000,
                    io_timeout_ms=10000, setup_concurrency=8, inflight=1,
                    max_messages=2000000, addresses=ADDRS)
    actual = report.get('config') or {}
    return all(actual.get(key) == value for key, value in expected.items()) and case_passed(case, result)


def collect(case):
    # Fixed allowlists exclude all token, private-key and Broker data files.
    selections = [(SERVER, f'{BASE}/{case}/combined',
                   ['resources.jsonl', 'processes.json', 'limits.json', 'monitor.log',
                    'node1/process.log', 'node2/process.log', 'node3/process.log']),
                  (CLIENT, f'{BASE}/{case}',
                   ['report.json', 'client-resources.json', 'process.log', 'raw.jsonl.gz'])]
    for host, root, names in selections:
        code = ('import pathlib,tarfile,sys;root=pathlib.Path(' + repr(root) + ');'
                'archive=tarfile.open(fileobj=sys.stdout.buffer,mode="w|gz");'
                '[archive.add(root/name,arcname=name,recursive=False) for name in '
                + repr(names) + ' if (root/name).is_file()];archive.close()')
        destination = OUT / f'{case}-{"server" if host == SERVER else "client"}.tar.gz'
        with destination.open('xb') as output:
            result = subprocess.run(SSH + ['root@' + host, 'python3 -c ' + shlex.quote(code)],
                                    stdout=output, stderr=subprocess.PIPE, timeout=180)
        if result.returncode:
            raise RuntimeError(f'evidence copy failed: {host}: {result.stderr.decode()[-1500:]}')


def prepare():
    OUT.mkdir(exist_ok=False)
    wanted = [(SERVER, BROKER, '87ced3e81da295b4273b2757c8fc4472409d88472e5c77cca77ba472fd28854b'),
              (CLIENT, CONNECTIONS, 'd0acf9899b9ea9d34fd68c8a5234b4f741b97bef816987b00b49f4ad6a28c2ae'),
              (CLIENT, LOADGEN, 'f955aa4d0843c48f0a689c5ea8ade782910709ae145a23afe77fdf494e19d7ef')]
    for host, path, expected in wanted:
        if remote(host, 'sha256sum ' + shlex.quote(path)).split()[0] != expected:
            raise RuntimeError('binary differs from approved benchmark source')
    for host in (SERVER, CLIENT):
        code = 'from pathlib import Path;Path(' + repr(BASE) + ').mkdir(mode=0o700,parents=True,exist_ok=False)'
        remote(host, 'python3 -c ' + shlex.quote(code))
    client_script = pathlib.Path(__file__).with_name('client.py')
    subprocess.run(['scp', *SSH[1:], str(client_script), f'root@{CLIENT}:{BASE}/client.py'],
                   check=True, timeout=60)
    (OUT / 'environment.json').write_text(remote(SERVER, f'RMQTT_BENCH_ROOT={BASE} '
        f'RMQTT_BENCH_BIN={BROKER} python3 {SERVER_SCRIPT} environment'))


def main():
    prepare()
    active = None
    original_ports = None
    try:
        for case in CASES:
            if case == 'cscale':
                original_ports = remote(CLIENT, 'cat /proc/sys/net/ipv4/ip_local_port_range').strip()
                (OUT / 'original-source-ports.txt').write_text(original_ports + '\n')
                remote(CLIENT, 'sysctl -w net.ipv4.ip_local_port_range="10240 65535"')
            ports = [int(v) for v in remote(CLIENT, 'cat /proc/sys/net/ipv4/ip_local_port_range').split()]
            validate_environment(100000 if case == 'cscale' else 10000, 250000, *ports)
            print(json.dumps({'event': 'begin', 'case': case, 'time_ns': time.time_ns()}), flush=True)
            active = case
            (OUT / f'{case}-start.log').write_text(server(case, 'start'))
            code = (f"import subprocess,os,pathlib;r=pathlib.Path('{BASE}/{case}');"
                    "f=(r/'combined'/'monitor.log').open('xb');"
                    f"subprocess.Popen(['python3','{SERVER_SCRIPT}','monitor','combined','--seconds','700'],"
                    f"env=dict(os.environ,RMQTT_BENCH_ROOT=str(r),RMQTT_BENCH_BIN='{BROKER}'),"
                    "stdout=f,stderr=f,start_new_session=True,stdin=subprocess.DEVNULL)")
            remote(SERVER, 'python3 -c ' + shlex.quote(code))
            raw = remote(CLIENT, f'RMQTT_SCALE_LOAD_ROOT={BASE} python3 {BASE}/client.py {case}', timeout=600)
            (OUT / f'{case}-client.json').write_text(raw)
            result = json.loads(raw)
            report = result.get('report') or {}
            okay = passed(case, result)
            print(json.dumps({'event': 'end', 'case': case, 'passed': okay,
                              'phase': report.get('phase'), 'error': report.get('error'),
                              'steps': report.get('steps'), 'accounting': report.get('accounting'),
                              'offering_window_tps': report.get('offering_window_tps'),
                              'latencies': report.get('latencies'), 'issues': report.get('issues', [])[:3]}), flush=True)
            remote(SERVER, f'touch {BASE}/{case}/combined/monitor.stop')
            (OUT / f'{case}-state.json').write_text(server(case, 'state'))
            (OUT / f'{case}-stop.log').write_text(server(case, 'stop'))
            active = None
            collect(case)
            if not okay:
                print(json.dumps({'event': 'matrix_stopped_on_first_failure', 'case': case}), flush=True)
                break
    finally:
        if active:
            try:
                (OUT / f'{active}-exception-stop.log').write_text(server(active, 'stop'))
                collect(active)
            except Exception as error:
                print('STOP_OR_COLLECTION_FAILED', repr(error), flush=True)
        if original_ports is not None:
            remote(CLIENT, 'sysctl -w net.ipv4.ip_local_port_range=' + shlex.quote(original_ports))
            (OUT / 'restored-source-ports.txt').write_text(remote(CLIENT, 'cat /proc/sys/net/ipv4/ip_local_port_range'))


if __name__ == '__main__':
    main()
