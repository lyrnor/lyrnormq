#!/usr/bin/env python3
"""Historical SQLite state-blob observation probe (before Phase7h).

Do not use to measure current Raft replication lag: periodic checkpoints are not
live applied state. Current scale helper samples authenticated cluster-status RPC.

--leader is an externally established, stable leader's 1-based directory ordinal.
The probe does not discover or validate Raft leadership. Times measure first
observations, include SQLite read/sampling skew, and are not commit/network latency.
"""
import argparse
import bisect
import json
import math
from pathlib import Path
import sqlite3
import time

MAX_SAMPLES = 100000


def validate(directories, leader, duration, interval_ms):
    paths = [Path(p).resolve() for p in directories]
    if len(paths) != 3 or len(set(paths)) != 3 or leader not in (1, 2, 3):
        raise ValueError('three distinct data directories and leader ordinal1..3 required')
    if not math.isfinite(duration) or not .05 <= duration <= 60 or not 1 <= interval_ms <= 1000:
        raise ValueError('duration must be .05..60s; interval must be1..1000ms')
    if math.ceil(duration * 1000 / interval_ms) * 3 > MAX_SAMPLES:
        raise ValueError('configured read sample budget exceeds100000')
    return paths


def distribution(values):
    values = sorted(values)
    if not values:
        return {'count': 0, 'min_ns': None, 'p50_ns': None, 'p95_ns': None, 'p99_ns': None, 'max_ns': None}
    return {'count': len(values), 'min_ns': values[0],
            **{f'p{percent}_ns': values[math.ceil(percent * len(values) / 100)-1]
               for percent in (50, 95, 99)}, 'max_ns': values[-1]}


def summarize(samples, leader):
    """First-observation matching; no interpolation of unseen intermediate advances."""
    result = {}
    for dimension in ('applied_index', 'metadata_revision'):
        streams = {ordinal: [x for x in samples if x['controller'] == ordinal and dimension in x]
                   for ordinal in (1, 2, 3)}
        nonmonotonic = []
        for ordinal, stream in streams.items():
            if any(b[dimension] < a[dimension] for a, b in zip(stream, stream[1:])):
                nonmonotonic.append(ordinal)
        leader_stream = streams[leader]
        advances = [b for a, b in zip(leader_stream, leader_stream[1:]) if b[dimension] > a[dimension]]
        followers = []
        for ordinal in (1, 2, 3):
            if ordinal == leader:
                continue
            stream = streams[ordinal]
            values = [x[dimension] for x in stream]
            matches = []
            for advance in advances:
                # Regressions invalidate monotonic matching rather than fabricate a delay.
                position = bisect.bisect_left(values, advance[dimension]) if ordinal not in nonmonotonic else len(values)
                entry = {'value': advance[dimension], 'leader_read_start_ns': advance['read_start_ns'],
                         'leader_read_end_ns': advance['read_end_ns'], 'observed_delay_ns': None,
                         'follower_already_observed_at_or_before_leader': False,
                         'not_before_leader_bound': False}
                if position < len(stream):
                    observed = stream[position]
                    before = observed['read_end_ns'] <= advance['read_end_ns']
                    entry.update(follower_read_start_ns=observed['read_start_ns'],
                                 follower_read_end_ns=observed['read_end_ns'],
                                 observed_delay_ns=max(0, observed['read_end_ns'] - advance['read_end_ns']),
                                 follower_already_observed_at_or_before_leader=before,
                                 not_before_leader_bound=before)
                matches.append(entry)
            delays = [x['observed_delay_ns'] for x in matches if x['observed_delay_ns'] is not None]
            followers.append({'controller': ordinal, 'observed_advances': len(advances),
                              'matched_advances': len(delays), 'censored_advances': len(matches)-len(delays),
                              'already_observed_before_leader_count': sum(x['not_before_leader_bound'] for x in matches),
                              'observed_delay': distribution(delays), 'matches': matches})
        result[dimension] = {'leader_observations': len(leader_stream), 'leader_advances': len(advances),
                             'intermediate_unobserved_advances_are_not_invented': True,
                             'nonmonotonic_controllers': nonmonotonic, 'followers': followers}
    return result


def sample(directories, leader, duration, interval_ms):
    paths = validate(directories, leader, duration, interval_ms)
    samples, connections = [], {}
    started = time.monotonic_ns()
    deadline = started + int(duration * 1_000_000_000)
    interval_ns = interval_ms * 1_000_000
    next_sample = started
    try:
        while time.monotonic_ns() < deadline and len(samples) + 3 <= MAX_SAMPLES:
            for ordinal, directory in enumerate(paths, 1):
                before = time.monotonic_ns()
                row = {'controller': ordinal, 'read_start_ns': before}
                try:
                    if ordinal not in connections:
                        # mode=ro forbids database creation or mutation; no checkpoint/PRAGMA writes.
                        connections[ordinal] = sqlite3.connect((directory / 'metadata.sqlite').as_uri() + '?mode=ro',
                                                              uri=True, timeout=.02)
                    state = connections[ordinal].execute(
                        "SELECT json_extract(data,'$.last_applied.index'),"
                        "json_extract(data,'$.metadata.revision'),"
                        "json_extract(data,'$.last_applied.leader_id.node_id') FROM meta WHERE key='state'").fetchone()
                    if state is None or any(not isinstance(v, int) or v < 0 for v in state[:2]):
                        raise ValueError('no valid applied state yet')
                    row.update(applied_index=state[0], metadata_revision=state[1], last_log_origin_node=state[2])
                except (sqlite3.Error, OSError, ValueError) as error:
                    row.update(error_type=type(error).__name__, error=str(error))
                row['read_end_ns'] = time.monotonic_ns()
                samples.append(row)
            next_sample += interval_ns
            now = time.monotonic_ns()
            if now > next_sample:
                # No bursts trying to reconstruct missed reads; retain actual timestamps.
                next_sample = now + interval_ns
            time.sleep(max(0, min(next_sample, deadline)-time.monotonic_ns()) / 1_000_000_000)
    finally:
        for connection in connections.values():
            connection.close()
    finished = time.monotonic_ns()
    analysis = summarize(samples, leader)
    read_errors = sum('error_type' in x for x in samples)
    leader_known = all(x['leader_observations'] >= 2 for x in analysis.values())
    monotonic = all(not x['nonmonotonic_controllers'] for x in analysis.values())
    return {'success': read_errors == 0 and leader_known and monotonic,
            'data_directories': [str(p) for p in paths], 'assumed_leader_ordinal': leader,
            'leadership_validation': 'external stable-leader assumption; probe does not establish Raft leader authority or detect every election',
            'semantics': 'First observed leader advance to first follower observation >= value. Includes polling resolution, sequential SQLite read order and read duration. Zero with not_before_leader_bound=true means follower was already observed; it does not establish zero replication delay.',
            'limitations': 'Not pure Raft commit/network latency; no unseen intermediate entries reconstructed. Leases and housekeeping contribute to revisions. Censored advances are not zero-delay observations. Read-only sampling consumes CPU/IO on the same host.',
            'requested_duration_seconds': duration, 'requested_interval_ms': interval_ms,
            'started_monotonic_ns': started, 'finished_monotonic_ns': finished,
            'actual_duration_ns': finished-started, 'sample_limit': MAX_SAMPLES,
            'read_samples': len(samples), 'read_errors': read_errors,
            'leader_observation_available': leader_known, 'analysis': analysis, 'samples': samples}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dirs', required=True)
    parser.add_argument('--leader', type=int, required=True)
    parser.add_argument('--duration-seconds', type=float, default=20)
    parser.add_argument('--interval-ms', type=int, default=10)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    try:
        report = sample(args.data_dirs.split(','), args.leader, args.duration_seconds, args.interval_ms)
    except (ValueError, OSError) as error:
        report = {'success': False, 'error': str(error)}
    with open(args.output, 'x') as output:
        json.dump(report, output, indent=2)
        output.write('\n')
    raise SystemExit(0 if report['success'] else 2)


if __name__ == '__main__':
    main()
