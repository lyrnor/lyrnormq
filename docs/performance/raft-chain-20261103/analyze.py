"""Offline analysis: correlated batches, cumulative interval histograms, CPU samples."""
import collections
import datetime
import json
import math
from pathlib import Path
import re
import sys


def distribution(values):
    if not values:
        return None
    ordered = sorted(values)
    return dict(count=len(values), mean=sum(values) / len(values),
                p50=ordered[math.ceil(len(values) * .5) - 1],
                p99=ordered[math.ceil(len(values) * .99) - 1], max=max(values), min=min(values))


def fields(line):
    out = dict(re.findall(r'(\w+)=("[^"]*"|Some\(\d+\)|\S+)', line))
    for key, value in list(out.items()):
        if value.startswith('Some('):
            out[key] = int(value[5:-1])
        elif value.isdigit():
            out[key] = int(value)
        elif value == 'None':
            out[key] = None
        else:
            out[key] = value.strip('"')
    out['wall_ns'] = round(datetime.datetime.fromisoformat(line.split()[0]).timestamp() * 1e9)
    return out


def read_events(root):
    by_node = {}
    for path in sorted((root / 'server').glob('node*/process.log')):
        events = []
        for line in path.read_text().splitlines():
            if 'event="raft_' not in line and 'event="handshake_profile"' not in line:
                continue
            event = fields(line)
            if event.get('event') == 'raft_profile':
                event['data'] = json.loads(line.split('data=', 1)[1])
            events.append(event)
        by_node[path.parent.name] = events
    return by_node


def interval_histograms(events, start, end, ping):
    samples = [e for e in events if e['event'] == 'raft_profile']
    before = [e for e in samples if e['wall_ns'] < start]
    after = [e for e in samples if end + 1000000000 < e['wall_ns'] < ping]
    if not before or not after:
        return {'error': 'missing quiet boundary snapshots'}
    a, b = before[-1], after[0]
    old = {v['metric']: v for v in a['data']}
    result = []
    for value in b['data']:
        previous = old.get(value['metric'], {'count': 0, 'sum': 0, 'buckets': [0] * 64})
        count = value['count'] - previous['count']
        if not count:
            continue
        bins = [n - p for n, p in zip(value['buckets'], previous['buckets'])]
        total = value['sum'] - previous['sum']
        consistent = len(bins) == 64 and sum(bins) == count and min(bins) >= 0 and count > 0 and total >= 0
        upper = None
        if consistent:
            accumulated = 0
            for i, n in enumerate(bins):
                accumulated += n
                if accumulated >= math.ceil(count * .99):
                    upper = (1 << i) if i < 63 else (1 << 64) - 1
                    break
        result.append(dict(metric=value['metric'], count=count, mean=total / count if consistent else None,
            p99_upper_bound=upper, snapshot_consistent=consistent,
            process_max_through_end=value['max'], buckets=bins))
    return dict(start_wall_ns=a['wall_ns'], end_wall_ns=b['wall_ns'], metrics=result,
        note='Includes heartbeat activity and boundary padding; nested wall times, not additive CPU. max is cumulative, not interval max.')


def correlate(events, start, end):
    batches = [e for e in events if e['event'] == 'raft_batch_profile'
               and e.get('success') == 'true' and start <= e['wall_ns'] <= end]
    storage = [e for e in events if e['event'] == 'raft_storage_trace' and e.get('outcome') == 'Success']
    pairs = []
    unmatched = 0
    for batch in batches:
        index = batch['log_index']
        def find(stage):
            matches = [e for e in storage if e['stage'] == stage and e['node'] == batch['node'] and e['first'] is not None
                       and e['first'] <= index <= e['last']
                       and batch['submitted_us'] <= e['start_us'] <= e['end_us'] <= batch['finished_us']]
            return matches[0] if len(matches) == 1 else None
        append, apply = find('Append'), find('ApplyQueued')
        if not append or not apply or not (batch['submitted_us'] <= append['start_us'] <= append['end_us'] <= apply['start_us'] <= apply['end_us'] <= batch['finished_us']):
            unmatched += 1
            continue
        row = dict(log_index=index, count=batch['count'], bytes=batch['bytes'],
            total_us=batch['finished_us'] - batch['submitted_us'],
            before_append_us=append['start_us'] - batch['submitted_us'],
            append_us=append['end_us'] - append['start_us'],
            post_append_to_apply_us=apply['start_us'] - append['end_us'],
            apply_us=apply['end_us'] - apply['start_us'],
            after_apply_us=batch['finished_us'] - apply['end_us'],
            command_queue_sum_us=batch['queue_sum_us'], command_queue_max_us=batch['queue_max_us'])
        assert sum(row[k] for k in ('before_append_us','append_us','post_append_to_apply_us','apply_us','after_apply_us')) == row['total_us']
        committed = [e for e in storage if e['node'] == batch['node'] and e['stage'] == 'SaveCommitted'
                     and e['last'] == index and append['end_us'] <= e['start_us'] <= e['end_us'] <= apply['start_us']]
        if len(committed) == 1:
            mark = committed[0]
            row.update(before_watermark_us=mark['start_us']-append['end_us'],
                       watermark_us=mark['end_us']-mark['start_us'],
                       after_watermark_us=apply['start_us']-mark['end_us'])
        pairs.append(row)
    keys = ['total_us','before_append_us','append_us','post_append_to_apply_us','apply_us','after_apply_us','count','bytes','before_watermark_us','watermark_us','after_watermark_us']
    return dict(batches=len(batches), matched=len(pairs), unmatched=unmatched,
        distributions={k: distribution([p[k] for p in pairs if k in p]) for k in keys}, pairs=pairs,
        note='Same-node log-index matching; post_append_to_apply includes replication/commit/apply dispatch, not network alone. Components telescope; do not add nested WAL/apply submetrics again.')


def cpu_samples(root, start, end):
    path = root / 'perf' / 'perf-script.txt'
    if not path.exists():
        return {'error': 'perf script absent'}
    resources = [json.loads(s) for s in (root / 'server/resources.jsonl').read_text().splitlines()]
    offset = resources[0]['wall_ns'] - resources[0]['monotonic_ns']
    begin, finish = (start - offset) / 1e9, (end - offset) / 1e9
    leaves, inclusive = collections.Counter(), collections.Counter()
    samples, weight, stack, sample_weight = 0, 0, [], None
    headers_total, headers_in_window, missing_stack = 0, 0, 0
    def flush():
        nonlocal samples, weight, stack, sample_weight, missing_stack
        if sample_weight is not None and not stack:
            missing_stack += 1
        if sample_weight is not None and stack:
            samples += 1; weight += sample_weight
            leaves[stack[0]] += sample_weight
            for name in set(stack):
                inclusive[name] += sample_weight
        stack, sample_weight = [], None
    for line in path.read_text(errors='replace').splitlines():
        if not line.strip():
            flush(); continue
        match = re.search(r'\s(\d+\.\d+):\s+(\d+)\s+cpu-clock:', line)
        if match:
            headers_total += 1
            flush()
            if begin <= float(match[1]) <= finish:
                headers_in_window += 1
                sample_weight = int(match[2])
        elif sample_weight is not None:
            match = re.match(r'\s*[0-9a-f]+\s+(.*?)\s+\((.*)\)\s*$', line)
            if match:
                name = re.sub(r'\+0x[0-9a-f]+$', '', match[1])
                if name == '[unknown]':
                    name += ' (' + match[2] + ')'
                stack.append(name)
    flush()
    chain_weight = weight
    flat_path = root / 'perf' / 'perf-flat.txt'
    flat_count, flat_missing = 0, 0
    if flat_path.exists():
        leaves = collections.Counter()
        weight = 0
        for line in flat_path.read_text(errors='replace').splitlines():
            header = re.search(r'\s(\d+\.\d+):\s+(\d+)\s+cpu-clock:', line)
            if not header or not begin <= float(header[1]) <= finish:
                continue
            flat_count += 1
            period = int(header[2]); weight += period
            symbol = re.search(r'cpu-clock:\s+[0-9a-f]+\s+(.*?)\s+\((/[^)]*|\[[^]]+\])\)\s*$', line)
            if symbol:
                name = re.sub(r'\+0x[0-9a-f]+$', '', symbol[1])
                if name == '[unknown]': name += ' (' + symbol[2] + ')'
            else:
                name = '[unparsed sampled IP]'; flat_missing += 1
            leaves[name] += period
        assert flat_count == headers_in_window, 'flat/stack sample windows differ'
    else:
        # Never report percentages over only the successfully unwound subset.
        return dict(error='flat sampled-IP evidence needed for incomplete callchains',
                    headers_in_window=headers_in_window, decoded_callchains=samples, missing_stack=missing_stack)
    def top(counter):
        ordered = sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:40]
        return [dict(symbol=s, percent=w / weight * 100, weight=w) for s, w in ordered] if weight else []
    return dict(samples=flat_count, callchain_samples=samples, callchain_cpu_ns=chain_weight, flat_unparsed=flat_missing, headers_total=headers_total, headers_in_window=headers_in_window, missing_stack=missing_stack, sampled_cpu_ns=weight, start_monotonic=begin,end_monotonic=finish,
        leaf=top(leaves), inclusive=top(inclusive), note='Software cpu-clock period-weighted samples in ramp/drain. Leaf uses every sampled IP; inclusive is only decoded callchains and overlaps, so a lower bound. Not wall waiting or exact per-operation CPU. Cross-host alignment uses wall-clock approximation.')


def main(root):
    aggregate = json.loads((root / 'aggregate.json').read_text())
    start = aggregate['settings']['start_unix_ms'] * 1000000
    end = start + round(aggregate['global_ramp_and_drain_seconds'] * 1e9)
    ping = aggregate['settings']['ping_unix_ms'] * 1000000
    events = read_events(root)
    result = dict(boundaries_ns={'start':start,'end':end,'ping':ping},nodes={})
    for node, values in events.items():
        admissions = [e for e in values if e['event']=='handshake_profile' and end < e['wall_ns'] < ping]
        result['nodes'][node] = dict(histograms=interval_histograms(values,start,end,ping),
            correlated=correlate(values,start,end), admission=admissions[-1] if admissions else None)
    (root / 'chain-analysis.json').write_text(json.dumps(result,indent=2)+'\n')
    cpu = cpu_samples(root,start,end)
    (root / 'cpu-analysis.json').write_text(json.dumps(cpu,indent=2)+'\n')
    for node, data in result['nodes'].items():
        print(node,json.dumps({'admission':data['admission'],'chain':data['correlated']['distributions']}))
    print('CPU',json.dumps({k:v for k,v in cpu.items() if k!='inclusive'}))


if __name__ == '__main__':
    main(Path(sys.argv[1]))
