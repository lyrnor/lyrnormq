import json,pathlib
root=pathlib.Path('/private/tmp/admission-repair-results')
client=json.loads((root/'client-resources.json').read_text())
rows=[json.loads(s) for s in (root/'server-resources.jsonl').read_text().splitlines()]
active=[r for r in rows if client['started_ns'] <= r['wall_ns'] <= client['ended_ns']]
intervals=[]
for a,b in zip(active,active[1:]):
 old={p['pid']:p['ticks'] for p in a['processes']}
 seconds=(b['monotonic_ns']-a['monotonic_ns'])/1e9
 ticks=sum(p['ticks']-old[p['pid']] for p in b['processes'] if p['pid'] in old)
 intervals.append((seconds,ticks/b['cpu_hz']))
cs=client['samples'];duration=(cs[-1]['wall_ns']-cs[0]['wall_ns'])/1e9 if len(cs)>1 else 0
summary={'server_samples':len(rows),'active_samples':len(active),'load_window_s':(client['ended_ns']-client['started_ns'])/1e9,
'broker_cpu_average_cores':sum(t for s,t in intervals)/sum(s for s,t in intervals) if intervals else None,
'broker_cpu_peak_sample_cores':max((t/s for s,t in intervals),default=None),
'broker_cpu_intervals':len(intervals),
'broker_rss_peak_MiB':max((sum(p['rss_bytes'] for p in r['processes']) for r in active),default=0)/1048576,
'broker_fd_peak':max((sum(p['fd'] for p in r['processes']) for r in active),default=0),
'tcp_established_peak':max((sum(r['mqtt_established'].values()) for r in active),default=0),
'client_cpu_average_cores':(cs[-1]['ticks']-cs[0]['ticks'])/client['cpu_hz']/duration if duration else None,
'client_rss_peak_MiB':max((p['rss_bytes'] for p in cs),default=0)/1048576}
(root/'resource-summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2))
