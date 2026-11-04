"""CPU-work comparison from raw tick brackets, never extrapolated interval means."""
import contextlib
import datetime
import importlib.util
import io
import json
from pathlib import Path
import sys
import tarfile

HERE = Path(__file__).resolve().parent
BASELINE = HERE.parent / 'raft-pipeline-20261104'


def module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def bracket(root, tail_seconds=0):
    aggregate = json.loads((root/'aggregate.json').read_text())
    rows = [json.loads(s) for s in (root/'server/resources.jsonl').read_text().splitlines()]
    start = aggregate['settings']['start_unix_ms']*1000000
    end = start+round((aggregate['global_ramp_and_drain_seconds']+tail_seconds)*1e9)
    a = max((r for r in rows if r['wall_ns']<=start), key=lambda r:r['wall_ns'])
    b = min((r for r in rows if r['wall_ns']>=end), key=lambda r:r['wall_ns'])
    before = {p['pid']:p['ticks'] for p in a['processes']}
    after = {p['pid']:p['ticks'] for p in b['processes']}
    assert before and before.keys()==after.keys() and a['cpu_hz']==b['cpu_hz']
    ticks = sum(after[p]-t for p,t in before.items()); assert ticks>=0
    seconds = ticks/a['cpu_hz']
    return dict(tail_allowance_seconds=tail_seconds,ticks=ticks,hz=a['cpu_hz'],cpu_seconds=seconds,
        cpu_ms_per_success=seconds*1000/aggregate['totals']['new_connack_success'],
        bracket_seconds=(b['monotonic_ns']-a['monotonic_ns'])/1e9,
        before_padding_ms=(start-a['wall_ns'])/1e6,after_padding_ms=(b['wall_ns']-end)/1e6,
        first_wall_ns=a['wall_ns'],last_wall_ns=b['wall_ns'],
        note='Complete ramp bracket plus disclosed quiet padding; raw same-PID ticks, not exact pure-ramp CPU or an extrapolated mean.')


def resets(root):
    a=json.loads((root/'aggregate.json').read_text());start=a['settings']['start_unix_ms']*1000000
    end=start+round(a['global_ramp_and_drain_seconds']*1e9);ping=a['settings']['ping_unix_ms']*1000000
    result={}
    for p in sorted((root/'server').glob('node*/process.log')):
        node={phase:dict(remote=0,local=0) for phase in ('startup','ramp','hold','cleanup')}
        for line in p.read_text().splitlines():
            kind='remote' if 'remotely-reset pending-accept streams reached limit' in line else 'local' if 'locally-reset streams reached limit' in line else None
            if kind:
                when=round(datetime.datetime.fromisoformat(line.split()[0]).timestamp()*1e9)
                phase='startup' if when<start else 'ramp' if when<end else 'hold' if when<ping else 'cleanup'
                node[phase][kind]+=1
        result[p.parent.name]=node
    return result


def main(root):
    prior=module(BASELINE/'compare.py','pipeline_compare')
    with contextlib.redirect_stdout(io.StringIO()):prior.main(root)
    result=json.loads((root/'comparison.json').read_text())
    result['before']=json.loads((BASELINE/'comparison.json').read_text())['after']
    baseline_raw=root/'baseline';baseline_raw.mkdir(exist_ok=True)
    (baseline_raw/'server').mkdir(exist_ok=True)
    with tarfile.open(BASELINE/'raw/server.tar.gz') as archive:archive.extractall(baseline_raw/'server',filter='data')
    (baseline_raw/'aggregate.json').write_bytes((BASELINE/'aggregate.json').read_bytes())
    result['cpu_work']={'before':bracket(baseline_raw),'after':bracket(root)}
    result['cpu_work_including_tail']={'before':bracket(baseline_raw,3),'after':bracket(root,3)}
    result['h2_reset_warnings']={'before':resets(baseline_raw),'after':resets(root)}
    chain=json.loads((root/'chain-analysis.json').read_text())
    result['quorum_metrics']={node:{m['metric']:{k:m[k] for k in ('count','mean','snapshot_consistent')} for m in v['histograms']['metrics'] if m['metric'].startswith('Quorum')} for node,v in chain['nodes'].items()}
    result['note']='One same-load cohort. Compare actual raw CPU ticks and padding as well as average cores; accepted-tail execution may increase replica work. Latencies are successful connection latencies, not message latency or capacity.'
    (root/'comparison.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k!='pipeline'},indent=2))


if __name__=='__main__':main(Path(sys.argv[1]))
