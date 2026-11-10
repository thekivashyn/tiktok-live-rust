"""Measure an explicit real LIVE probe; persist only aggregate diagnostics."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import statistics
from measure_process import once

ROOT=Path(__file__).resolve().parents[1]


def window(samples,start,end):
    selected=[s for s in samples if start<=s['seconds']<=end]
    if not selected:return None
    return {key:{'min_mib':min(s[key] for s in selected)/1048576,
                 'median_mib':statistics.median(s[key] for s in selected)/1048576,
                 'max_mib':max(s[key] for s in selected)/1048576}
            for key in ('working_set_bytes','private_bytes')}


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('username');p.add_argument('--seconds',type=int,default=2700)
    p.add_argument('--cut-interval',type=int,default=600);p.add_argument('--cuts',type=int,default=3)
    p.add_argument('--label',required=True)
    p.add_argument('--report-dir',default='reports/watchdog-investigation')
    a=p.parse_args()
    if not 30<=a.seconds<=14400 or not a.label.replace('-','').replace('.','').isalnum():p.error('invalid duration or label')
    folder=ROOT/a.report_dir;folder.mkdir(parents=True,exist_ok=True)
    path=folder/(a.label+'.json');diagnostic=folder/(a.label+'.progress.ndjson')
    if path.exists() or diagnostic.exists():p.error('use a fresh label')
    binary=ROOT/'target/release/examples/live_soak.exe'
    started=datetime.now(timezone.utc).isoformat()
    binary_hash=hashlib.sha256(binary.read_bytes()).hexdigest()
    result=once([str(binary),a.username,str(a.seconds),str(a.cut_interval),str(a.cuts)],a.seconds+45,False,False,diagnostic_path=diagnostic)
    samples=result['samples'];end=samples[-1]['seconds'];child=result['child_report']
    summary={'exit_code':result['exit_code'],'checks':child['checks'],'events':child['events_consumed'],
             'peak_working_set_mib':result['peak_working_set_bytes']/1048576,
             'peak_private_mib_sampled':result['peak_private_bytes_sampled']/1048576,
             'cpu_seconds':result['cpu_seconds'],'elapsed_seconds':result['wall_seconds'],
             'minute_2':window(samples,60,120),'last_minute':window(samples,max(0,end-60),end),
             'last_five_minutes':window(samples,max(0,end-300),end)}
    document={'started_at_utc':started,'finished_at_utc':datetime.now(timezone.utc).isoformat(),
              'settings':vars(a),'binary_sha256_at_start':binary_hash,'summary':summary,'measurement':result,
              'limits':'Real public LIVE. Probe RAM includes bounded relay and validation state. Sequence/dedupe/field checks cannot prove all TikTok-origin events arrived. No raw chat, sender identities, frames or cookies saved.'}
    path.write_text(json.dumps(document,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'report':str(path),'summary':summary},indent=2),flush=True)
    if result['exit_code'] or not child['passed']:raise SystemExit(1)
