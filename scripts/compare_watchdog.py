"""Alternating old/new receivers with identical status-observer instrumentation."""
from argparse import Namespace
import json
from pathlib import Path
import statistics
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'benchmarks/connector'))
import measure

versions={
    '0.7.1':ROOT/'reports/watchdog-investigation/baseline/instrumented_receiver.exe',
    '0.7.2':ROOT/'target/release/examples/matched_receiver_candidate.exe',
}
original=measure.command
selected=None
def command(impl,mode,*args):
    if impl=='rust' and mode=='receiver':return [str(selected),*map(str,args)]
    return original(impl,mode,*args)
measure.command=command
runs={key:[] for key in versions}
for pair in range(1,4):
    for version in (('0.7.1','0.7.2') if pair%2 else ('0.7.2','0.7.1')):
        selected=versions[version]
        args=Namespace(label=f'watchdog-perf-{version}-{pair}',frames=65536,pace=0,cut=0,stall=0,timeout=180)
        record=measure.socket('rust',args)
        assert record['measurement']['child_report']['version']==version
        runs[version].append(record)
summary={version:{
    'events_per_second':statistics.median(r['summary']['child']['output']['events']/r['summary']['child']['elapsed_seconds'] for r in group),
    'peak_working_set_mib':statistics.median(r['summary']['peak_working_set_mib'] for r in group),
    'peak_private_mib':statistics.median(r['summary']['peak_private_mib'] for r in group),
    'cpu_seconds':statistics.median(r['summary']['cpu_seconds'] for r in group),
} for version,group in runs.items()}
destination=ROOT/'reports/watchdog-investigation/performance-v0.7.2.json'
destination.write_text(json.dumps({'pairs':3,'events_per_run':524289,'summary':summary,'raw_reports':{version:[str(Path('reports/matched-comparison')/(r['settings']['label']+'-rust.json')) for r in group] for version,group in runs.items()},'limits':'Loopback throughput with ACK gating, canonical output and identical status observer. Concurrent paced soaks can add timing noise.'},indent=2)+'\n')
print(json.dumps(summary),flush=True)
