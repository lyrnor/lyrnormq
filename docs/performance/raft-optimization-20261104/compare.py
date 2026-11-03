"""Offline comparison; preserve failed gates and distinguish rounds from calls."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tarfile

HERE = Path(__file__).resolve().parent
BASELINE = HERE.parent / 'raft-chain-20261103'


def module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def summarize(root, before=False):
    read = lambda name: json.loads((root / name).read_text())
    aggregate, resources, chain = map(read, ('aggregate.json', 'resource-summary.json', 'chain-analysis.json'))
    def write_count(node):
        return next((m['count'] for m in node['histograms'].get('metrics', []) if m['metric']=='ControlWrite'), 0)
    leader_name, leader = max(chain['nodes'].items(), key=lambda pair: write_count(pair[1]))
    metrics = {m['metric']:m for m in leader['histograms']['metrics']}
    assert all(m['snapshot_consistent'] for m in metrics.values()), 'inconsistent diagnostic snapshot'
    rounds = metrics.get('ReadIndex' if before else 'ReadIndexRound', {}).get('count')
    correlated = leader['correlated']
    return dict(
        passed=aggregate['passed'], success=aggregate['totals']['new_connack_success'],
        attempted=aggregate['totals']['new_attempted'], skipped=aggregate['totals']['not_started'],
        ping_alive=aggregate['totals']['live_after_hold_ping'],
        entry_rejected=sum(n['admission']['rejected'] for n in chain['nodes'].values()),
        latency_ms={k:aggregate['successful_connect_latency'][k+'_us']/1000 for k in ('mean','p99','max')},
        ramp_seconds=aggregate['global_ramp_and_drain_seconds'],
        broker_cpu_cores=resources['broker']['ramp']['cpu_average_cores'],
        broker_rss_peak_mib=resources['broker']['ramp_and_hold']['simultaneous_sum_rss_peak_mib'],
        client_cpu_cores=resources['client']['ramp']['cpu_average_cores'],
        leader_node=leader_name, logical_readindex_calls=metrics.get('ReadIndex',{}).get('count'),
        actual_readindex_rounds=rounds, append_rpc_count=metrics.get('AppendRpcRoundtrip',{}).get('count'),
        mean_control_write_ms=metrics['ControlWrite']['mean']/1000,
        mean_capacity_wait_ms=metrics['EnqueueWait']['mean']/1000,
        mean_queue_residence_ms=metrics['QueueResidence']['mean']/1000,
        mean_read_group_size=metrics.get('ReadIndexGroupSize',{}).get('mean'),
        correlated_batches=correlated['matched'], unmatched_batches=correlated['unmatched'],
        full64_batches=sum(p['count']==64 for p in correlated['pairs']),
        batch_total_ms={k:correlated['distributions']['total_us'][k]/1000 for k in ('mean','p99')},
        batch_stages_ms={k:v['mean']/1000 for k,v in correlated['distributions'].items() if k.endswith('_us') and v},
        histogram_window_ns=[leader['histograms']['start_wall_ns'],leader['histograms']['end_wall_ns']],
    )


def main(root):
    perf = root / 'perf'
    perf.mkdir(exist_ok=True)
    with tarfile.open(root / 'perf.tar.gz') as archive:
        archive.extractall(perf, filter='data')
    analysis = module(BASELINE / 'analyze.py', 'chain_analysis')
    resources = module(HERE.parent / 'cluster-multiprocess-load-20261103/summarize.py', 'resource_analysis')
    with contextlib.redirect_stdout(io.StringIO()):
        analysis.main(root)
        resources.main(root)
    result = dict(before=summarize(BASELINE, before=True), after=summarize(root),
        note='Same offered load/limits, profiling enabled in both. Success-only latency populations may differ. Histogram padding differs from correlated batch window. Actual rounds before grouping equal logical calls. No maximum capacity or business message throughput claim.')
    (root / 'comparison.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result,indent=2))


if __name__ == '__main__':
    main(Path(sys.argv[1]))
