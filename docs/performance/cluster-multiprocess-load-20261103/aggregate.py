"""Merge original observations; shard percentiles are never averaged."""
import json
import pathlib
import sys


def distribution(values):
    if not values:
        return None
    ordered = sorted(values)
    return dict(count=len(values), mean_us=sum(values) / len(values),
                p99_us=ordered[(len(values) * 99 + 99) // 100 - 1], max_us=ordered[-1])


def aggregate(root):
    settings = json.loads((root / 'settings.json').read_text())
    resources = json.loads((root / 'client-resources.json').read_text())
    commands = json.loads((root / 'commands.json').read_text())
    issues, workers, latencies, lags, prefixes = [], [], [], [], set()
    if settings.get('instances') != 4 or settings.get('aggregate_target') != 10000:
        issues.append('unexpected aggregate configuration')
    if settings.get('aggregate_rate') not in (2000, 4000):
        issues.append('unexpected aggregate rate')
    if (settings.get('per_instance_rate', 0) * 4 != settings.get('aggregate_rate')
            or settings.get('per_instance_target') != 2500):
        issues.append('per-instance load does not sum to declared aggregate load')
    if resources.get('timeout') or resources.get('setup_failed') or resources.get('exits') != [0] * 4:
        issues.append('process failure or timeout')
    if len(commands) != 4:
        issues.append('missing process commands')
    for index in range(4):
        if index >= len(commands):
            issues.append(f'worker{index} missing command metadata')
            continue
        try:
            report = json.loads((root / str(index) / 'report.json').read_text())
            raw = json.loads((root / str(index) / 'raw.json').read_text())
            step, samples = report['steps'][0], raw['steps'][0]
        except (OSError, ValueError, KeyError, IndexError) as error:
            issues.append(f'worker{index} missing/invalid report/raw: {type(error).__name__}')
            continue
        prefix = report.get('client_prefix')
        if not prefix or prefix in prefixes:
            issues.append(f'worker{index} duplicate/missing prefix')
        prefixes.add(prefix)
        expected = dict(endpoints=settings['endpoints'], rate=settings['per_instance_rate'],
                        connections_per_step=2500, requested_plateaus=1,
                        index_offset=index * 2500, client_prefix=commands[index]['prefix'],
                        requested_start_unix_ms=settings['start_unix_ms'],
                        requested_ping_unix_ms=settings['ping_unix_ms'],
                        max_concurrent_handshakes=2048, timeout_seconds=15)
        if any(report.get(key) != value or raw.get('settings', {}).get(key) != value
               for key, value in expected.items()):
            issues.append(f'worker{index} configuration mismatch')
        if len(report['steps']) != 1 or len(raw['steps']) != 1 or samples.get('step') != 1:
            issues.append(f'worker{index} unexpected step count')
        observed = samples.get('latency_us', [])
        scheduled = samples.get('schedule_lag_us', [])
        if len(observed) != step.get('new_connack_success') or len(scheduled) != step.get('new_attempted'):
            issues.append(f'worker{index} raw observation count mismatch')
        if samples.get('latency_count') != len(observed) or samples.get('schedule_lag_count') != len(scheduled):
            issues.append(f'worker{index} raw count metadata mismatch')
        if any(not isinstance(value, int) or value < 0 for value in observed + scheduled):
            issues.append(f'worker{index} invalid raw timing')
            continue
        original = distribution(observed)
        if original != step.get('connect_latency'):
            issues.append(f'worker{index} raw latency distribution mismatch')
        original_lag = distribution(scheduled)
        if original_lag != step.get('schedule_lag'):
            issues.append(f'worker{index} raw lag distribution mismatch')
        if (not report.get('plateau_passed') or step.get('target') != 2500
                or step.get('new_scheduled') != 2500 or step.get('new_attempted') != 2500
                or step.get('new_connack_success') != 2500
                or step.get('live_after_hold_ping') != 2500 or step.get('not_started') != 0
                or step.get('errors') or step.get('ping_errors') or step.get('hold_seconds') != 10
                or step.get('intended_rate') != settings['per_instance_rate']
                or not step.get('shared_hold_at_least_10s')):
            issues.append(f'worker{index} incomplete requested load/hold')
        workers.append(dict(index=index, prefix=prefix, **step))
        latencies.extend(observed)
        lags.extend(scheduled)
    latest = max((w['ramp_and_drain_seconds'] for w in workers), default=0)
    shared_hold = (min(w['ping_started_unix_ms'] for w in workers)
                   - max(w['ramp_completed_unix_ms'] for w in workers)) if len(workers) == 4 else None
    if shared_hold is None or shared_hold < 10000:
        issues.append('global simultaneous hold shorter than10s or unverified')
    totals = {key: sum(w[key] for w in workers) for key in
              ['new_scheduled', 'new_attempted', 'new_connack_success', 'not_started', 'live_after_hold_ping']}
    result = dict(passed=not issues, issues=issues, settings=settings, totals=totals,
                  successful_connect_latency=distribution(latencies),
                  actual_task_start_schedule_lag=distribution(lags),
                  global_ramp_and_drain_seconds=latest,
                  completed_per_second=len(latencies) / latest if latest else None,
                  global_simultaneous_hold_ms=shared_hold, workers=workers,
                  metric_notes=['success latency excludes failed/unstarted connections',
                                'p99 computed from merged original observations, not shard percentiles',
                                'global window=max monotonic ramp elapsed anchored at the common start',
                                'four synchronous streams create approximate four-arrival bursts every2ms at2000/s'])
    return result


if __name__ == '__main__':
    root = pathlib.Path(sys.argv[1])
    result = aggregate(root)
    (root / 'aggregate.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result), flush=True)
