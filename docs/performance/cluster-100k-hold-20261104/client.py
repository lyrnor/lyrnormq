"""Four concurrent, synchronized connection processes on the same load host."""
import hashlib
import json
import os
import pathlib
import resource
import subprocess
import time

ROOT = pathlib.Path(os.environ['LYRNORMQ_MULTI_ROOT'])
BINARY = pathlib.Path(os.environ['LYRNORMQ_MULTI_BIN'])
RATE = int(os.environ.get('LYRNORMQ_MULTI_RATE', '2000'))
ADDRS = [f'172.16.240.180:{port}' for port in (18831, 18832, 18833)]


def sample(pid):
    row = pathlib.Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    return dict(pid=pid, ticks=int(row[11]) + int(row[12]),
                rss_bytes=int(row[21]) * os.sysconf('SC_PAGE_SIZE'))


def main():
    if RATE != 2000:
        raise ValueError('only predeclared aggregate rates are supported')
    ROOT.mkdir(exist_ok=False)
    resource.setrlimit(resource.RLIMIT_NOFILE, (250000, 250000))
    began = time.time_ns()
    start = began // 1000000 + 8000
    ping = start + 370000
    prefix_epoch = began // 1000000000
    settings = dict(instances=4, aggregate_rate=RATE, aggregate_target=100000,
                    per_instance_rate=RATE // 4, per_instance_target=25000,
                    start_unix_ms=start, ping_unix_ms=ping, endpoints=ADDRS,
                    tokio_workers_each=2, max_inflight_each=2048, requested_hold_seconds=300,
                    nofile=list(resource.getrlimit(resource.RLIMIT_NOFILE)),
                    binary_sha256=hashlib.sha256(BINARY.read_bytes()).hexdigest())
    children, logs, commands, samples = [], [], [], []
    timed_out = False
    setup_failed = False
    try:
        for index in range(4):
            directory = ROOT / str(index)
            directory.mkdir()
            # 11characters + six-digit global index matches the old17-byte ID length.
            prefix = f'm{prefix_epoch:08x}{index:02x}'
            command = [str(BINARY), ','.join(ADDRS), str(RATE // 4),
                       str(directory / 'report.json'), '1',
                       '--connections-per-step', '25000', '--client-prefix', prefix,
                       '--index-offset', str(index * 25000), '--start-unix-ms', str(start),
                       '--ping-unix-ms', str(ping), '--raw', str(directory / 'raw.json')]
            output = (directory / 'process.log').open('xb')
            logs.append(output)
            child = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                                     env=dict(os.environ, TOKIO_WORKER_THREADS='2'))
            children.append(child)
            commands.append(dict(index=index, pid=child.pid, prefix=prefix,
                                 index_offset=index * 25000, command=command,
                                 start_ticks=pathlib.Path(f'/proc/{child.pid}/stat').read_text().rsplit(')',1)[1].split()[19]))
        (ROOT / 'settings.json').write_text(json.dumps(settings, indent=2))
        (ROOT / 'commands.json').write_text(json.dumps(commands, indent=2))
        deadline = time.monotonic() + 450
        while any(child.poll() is None for child in children):
            observed = []
            for child in children:
                if child.poll() is None:
                    try:
                        observed.append(sample(child.pid))
                    except (FileNotFoundError, ProcessLookupError):
                        pass
            samples.append(dict(wall_ns=time.time_ns(), monotonic_ns=time.monotonic_ns(),
                                processes=observed))
            if time.time_ns() // 1000000 < start and any(
                    child.returncode not in (None, 0) for child in children):
                setup_failed = True
                break
            if (ROOT / 'abort').exists():
                setup_failed = True
                break
            if time.monotonic() > deadline:
                timed_out = True
                break
            time.sleep(.2)
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
            child.wait()
        for output in logs:
            output.close()
        record = dict(started_ns=began, ended_ns=time.time_ns(),
                      cpu_hz=os.sysconf('SC_CLK_TCK'), samples=samples,
                      timeout=timed_out, setup_failed=setup_failed,
                      exits=[child.returncode for child in children])
        (ROOT / 'client-resources.json').write_text(json.dumps(record, indent=2))
    reports = []
    for index in range(4):
        path = ROOT / str(index) / 'report.json'
        try:
            reports.append(json.loads(path.read_text()))
        except (OSError, ValueError) as error:
            reports.append({'report_error': type(error).__name__})
    (ROOT / 'reports.json').write_text(json.dumps(reports, indent=2))
    print(json.dumps(dict(settings=settings, exits=record['exits'], timeout=timed_out,
                          setup_failed=setup_failed, reports=reports)), flush=True)


if __name__ == '__main__':
    ports = pathlib.Path('/proc/sys/net/ipv4/ip_local_port_range')
    original = ports.read_text()
    changed = False
    try:
        ports.write_text('10000 65535\n')
        changed = True
        main()
    finally:
        if changed:
            active = ports.read_text().strip()
            ports.write_text(original)
            record = dict(original=original.strip(), during=active, restored=ports.read_text().strip())
            (ROOT.parent / 'source-ports.json').write_text(json.dumps(record, indent=2))
            assert record['restored'] == record['original'], 'source port restoration failed'

