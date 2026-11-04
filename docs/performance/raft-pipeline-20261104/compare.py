"""Pipeline-aware offline accounting; overlapping batches are not additive work."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent


def module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def intervals(rows):
    points = []
    for row in rows:
        start, end = row['submitted_us'], row['finished_us']
        assert 0 <= start <= end
        points.extend(((start, 1), (end, -1)))
    active, peak, union, summed = 0, 0, 0, 0
    previous = None
    for stamp, delta in sorted(points):
        if previous is not None:
            if active: union += stamp - previous
            summed += active * (stamp - previous)
        active += delta
        peak = max(peak, active)
        previous = stamp
    assert active == 0
    span = max((p[0] for p in points), default=0)-min((p[0] for p in points),default=0)
    return dict(batches=len(rows), union_busy_us=union, sum_batch_us=summed,
                observed_span_us=span, peak_overlap=peak,
                union_fraction_of_observed_span=union/span if span else None,
                note='Union/peak use same-process submit/completion timestamps. Sum is overlapping batch wall time, not utilization or CPU. Completion handling may include FIFO head-of-line wait.')


def main(root):
    prior = module(HERE.parent / 'raft-optimization-20261104/compare.py', 'prior_comparison')
    with contextlib.redirect_stdout(io.StringIO()):
        prior.main(root)
    header = (root/'perf/perf-header.txt').read_text()
    assert '# clockid: monotonic (1)' in header and '# reference time:' in header, 'perf clock evidence missing'
    cpu_path = root/'cpu-analysis.json'
    cpu = json.loads(cpu_path.read_text())
    assert cpu['phase_alignment_valid'], 'capture command and header clock disagree'
    cpu['clock_header_verified'] = True
    cpu['clock_note'] = 'Capture command and perf header both specify CLOCK_MONOTONIC; CLOCK_DATA reference is archived. Cross-host wall alignment remains approximate.'
    cpu_path.write_text(json.dumps(cpu,indent=2)+'\n')
    before = prior.summarize(HERE.parent / 'raft-optimization-20261104')
    after = prior.summarize(root)
    analysis = module(HERE.parent / 'raft-chain-20261103/analyze.py', 'chain_analysis')
    raw = analysis.read_events(root)
    chain = json.loads((root/'chain-analysis.json').read_text())
    start, end = chain['boundaries_ns']['start'], chain['boundaries_ns']['end']
    leader = after['leader_node']
    events = raw[leader]
    batches = [e for e in events if e['event']=='raft_batch_profile' and e.get('success')=='true' and start <= e['wall_ns'] <= end]
    ranges = []
    for batch in batches:
        row = dict(log_index=batch['log_index'])
        for stage in ('Append','ApplyQueued'):
            matches = [e for e in events if e['event']=='raft_storage_trace' and e.get('outcome')=='Success' and e['stage']==stage
                and e.get('first') is not None and e['first'] <= batch['log_index'] <= e['last']
                and batch['submitted_us'] <= e['start_us'] <= e['end_us'] <= batch['finished_us']]
            row[stage] = [dict(first=e['first'],last=e['last'],start_us=e['start_us'],end_us=e['end_us']) for e in matches]
        ranges.append(row)
    overlap = intervals(batches)
    overlap['time_weighted_active_batches'] = overlap['sum_batch_us']/overlap['observed_span_us'] if overlap['observed_span_us'] else None
    overlap['shared_append_batches'] = sum(len(r['Append'])==1 and r['Append'][0]['last']>r['Append'][0]['first'] for r in ranges)
    overlap['shared_apply_batches'] = sum(len(r['ApplyQueued'])==1 and r['ApplyQueued'][0]['last']>r['ApplyQueued'][0]['first'] for r in ranges)
    overlap['range_matches'] = ranges
    hist = {m['metric']:m for m in chain['nodes'][leader]['histograms']['metrics']}
    after['mean_submit_us'] = hist.get('BatchSubmit',{}).get('mean')
    after['window_observations'] = hist.get('BatchWindow')
    after['wal_flush_wait_operations'] = hist.get('WalFlushWait',{}).get('count')
    distributions = chain['nodes'][leader]['correlated']['distributions']
    after['stage_sample_counts'] = {key:value['count'] for key,value in distributions.items() if key.endswith('_us') and value}
    after['watermark_stage_note'] = 'Exact-index SaveCommitted stages cover a subset; do not combine subset means with the full-population five-stage breakdown.'
    comparison = dict(before=before,after=after,pipeline=overlap,
        note='One same-load cohort per version. Raw before/after populations differ if admission fails. Batch wall times may overlap or share WAL/apply ranges; do not sum them as elapsed time, sync counts or CPU. Perf now pins CLOCK_MONOTONIC; old per-phase perf attribution remains invalid.')
    (root/'comparison.json').write_text(json.dumps(comparison,indent=2)+'\n')
    print(json.dumps({k:v for k,v in comparison.items() if k!='pipeline'},indent=2))
    print(json.dumps({k:v for k,v in overlap.items() if k!='range_matches'},indent=2))


if __name__ == '__main__':
    main(Path(sys.argv[1]))
