"""Explicit nearest-rank latency and sampled process metrics (not allocator peaks)."""
import asyncio
import math
import statistics
import time


def distribution(samples):
    assert samples, 'missing latency samples'
    assert all(math.isfinite(value) and value >= 0 for value in samples)
    values = sorted(samples)
    def percentile(fraction):
        return values[max(0, math.ceil(len(values) * fraction) - 1)] / 1_000_000
    return {'samples': len(values), 'p50_ms': percentile(.5), 'p95_ms': percentile(.95),
            'p99_ms': percentile(.99), 'max_ms': values[-1] / 1_000_000}


def cpu_seconds(value):
    days = 0
    if '-' in value:
        prefix, value = value.split('-', 1)
        days = int(prefix)
    result = 0.0
    for part in value.split(':'):
        result = result * 60 + float(part)
    return days * 86400 + result


async def process_sample(pid, stage):
    process = await asyncio.create_subprocess_exec('ps', '-o', 'rss=,time=', '-p', str(pid),
                                                 stdout=asyncio.subprocess.PIPE,
                                                 stderr=asyncio.subprocess.PIPE)
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 3)
    finally:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await process.wait()
    assert process.returncode == 0, stderr.decode()
    rss, cpu = stdout.decode().split()
    return {'stage': stage, 'monotonic_ns': time.monotonic_ns(), 'rss_bytes': int(rss) * 1024,
            'cpu_seconds': cpu_seconds(cpu)}


class Sampler:
    def __init__(self, pid, interval):
        self.pid, self.interval = pid, interval
        self.samples = []
        self.stop = asyncio.Event()

    async def once(self, stage):
        sample = await process_sample(self.pid, stage)
        self.samples.append(sample)
        return sample

    async def run(self):
        while not self.stop.is_set():
            await self.once('active')
            try:
                await asyncio.wait_for(self.stop.wait(), self.interval)
            except TimeoutError:
                pass


def summarize(runs):
    groups = {}
    for run in runs:
        groups.setdefault((run['case'], run['storage'], run.get('flow_control', 'ack')), []).append(run)
    summary = []
    for (case, storage, flow_control), group in groups.items():
        record = {'case': case, 'storage': storage, 'flow_control': flow_control, 'repetitions': len(group)}
        for name in ('ingress_messages_per_second', 'egress_deliveries_per_second'):
            values = [item[name] for item in group]
            record[name] = {'median': statistics.median(values), 'min': min(values), 'max': max(values)}
        for field in ('delivery_latency', 'ack_latency'):
            record[field] = {}
            for name in ('p50_ms', 'p95_ms', 'p99_ms', 'max_ms'):
                values = [item[field][name] for item in group]
                record[field][name] = {'median': statistics.median(values), 'min': min(values), 'max': max(values)}
        peaks = [item['memory']['sampled_peak_rss_bytes'] for item in group]
        record['sampled_peak_rss_bytes'] = {'median': statistics.median(peaks), 'min': min(peaks), 'max': max(peaks)}
        summary.append(record)
    return summary
