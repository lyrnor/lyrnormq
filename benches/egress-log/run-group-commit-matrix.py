#!/usr/bin/env python3
"""Bounded Group Commit/segment matrix; existing output is never overwritten."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--binary', type=Path, default=ROOT/'target/release/group_commit')
    parser.add_argument('--queue-dir', type=Path, default=Path('/private/tmp'))
    parser.add_argument('--mode', choices=['direct','concurrent'], default='direct')
    parser.add_argument('--case', choices=['all','single','rules','fanout'], default='all')
    parser.add_argument('--segment-mib', type=int, choices=[1,16,64], nargs='+', default=[1,16,64])
    parser.add_argument('--repeats', type=int, choices=[1,2,3], default=3)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    cases = [('single',4096,1,1),('rules',16384,4,1),('fanout',4096,1,16)]
    if args.case != 'all':
        cases = [case for case in cases if case[0] == args.case]
    manifest = {'platform':platform.platform(), 'mode':args.mode,
                'engine_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
                'binary':str(binary), 'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),
                'source_sha256':hashlib.sha256((ROOT/'benches/egress-log/src/bin/group_commit.rs').read_bytes()).hexdigest(),
                'started':time.strftime('%Y-%m-%dT%H:%M:%S%z'), 'runs':[],
                'method':'New group-width/segment experiment, not the unchanged39-case topology control. Each frame <=64results, width1or4, sync production, callp99 not divided; concurrent separately reports request enqueue->confirmed p99.'}
    for repeat in range(1,args.repeats+1):
        for case,inputs,rules,fanout in cases:
            for segment in args.segment_mib:
                for width in [1,4]:
                    name=f'{args.mode}-{case}-s{segment}-g{width}-{repeat}'
                    command=['/usr/bin/time','-l',str(binary),'--inputs',str(inputs),'--rules',str(rules),
                             '--fanout',str(fanout),'--payload-bytes','1024','--group-width',str(width),
                             '--segment-bytes',str(segment*1048576),'--mode',args.mode,'--queue-dir',str(args.queue_dir)]
                    with (out/f'{name}.json').open('x') as stdout, (out/f'{name}.time').open('x') as stderr:
                        run=subprocess.run(command,stdout=stdout,stderr=stderr,cwd=ROOT,timeout=180)
                    manifest['runs'].append({'name':name,'command':command,'returncode':run.returncode})
                    (out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
                    run.check_returncode()
                    v=json.loads((out/f'{name}.json').read_text())
                    assert v['debug_assertions'] is False
                    assert v['config']=={'inputs':inputs,'rules':rules,'fanout':fanout,'payload_bytes':1024,
                                          'frame_max_results':64,'group_max_frames':width,'segment_bytes':segment*1048576,'mode':args.mode}
                    assert v['stats_after_append']['pending_results']==inputs*rules
                    assert v['stats_after_append']['pending_records']==inputs*rules*fanout
                    assert v['stats_after_append']['pending_bytes']==inputs*rules*1024
                    assert v['actual_append_data_sync_calls'] > 0
                    assert v['frames_per_append_data_sync'] > 0
                    assert v['verified_deliveries']==inputs*rules*fanout
                    assert v['reopen_verified_results']==inputs*rules
                    assert all(v['rss_kib'].values())
                    assert 'maximum resident set size' in (out/f'{name}.time').read_text()
                    if case=='rules' and segment==64:
                        assert v['segment_files_after_append']>=2, 'large case must cross64MiB'
                    if args.mode=='concurrent':
                        assert v['writing']['confirmation_latency']['items']==inputs*rules
                    write=v['writing']['calls']['wall_items_per_second']
                    p99=v['writing']['calls']['api_call_latency_ns']['p99']/1e6
                    print(f'{name}: {write:.0f} results/s, group-callp99={p99:.3f}ms, segments={v["segment_files_after_append"]}, append-data-syncs={v["actual_append_data_sync_calls"]}',flush=True)


if __name__ == '__main__':
    main()
