"""Same-corpus measurement. Socket fixture runs in a separate, excluded process."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import time
import zlib

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from measure_process import once
NODE=shutil.which('node')
USE_CANDIDATE=False
CORPUS=ROOT/'reports/matched-corpus'
BENCH=ROOT/'benchmarks/connector'
OUT=ROOT/'reports/matched-comparison'
OUT.mkdir(exist_ok=True)

def command(impl,mode,*args):
    if impl=='rust':return [str(ROOT/f'target/release/examples/matched_{mode}{"_candidate" if mode=="receiver" and USE_CANDIDATE else ""}.exe'),*map(str,args)]
    return [NODE,str(BENCH/f'{"pipeline" if mode=="pipeline" else "receiver"}.mjs'),*map(str,args)]
def summary(run):
    return {'exit_code':run['exit_code'],'peak_working_set_mib':run['peak_working_set_bytes']/1048576,
            'peak_private_mib':run['peak_private_bytes_sampled']/1048576,'cpu_seconds':run['cpu_seconds'],
            'peak_threads':run['peak_threads_sampled'],'child':run['child_report']}
def socket(impl,args):
    prefix=OUT/f'{args.label}-{impl}'
    artifact_paths=[Path(command(impl,'receiver',0)[0]),BENCH/'server.mjs',BENCH/'common.mjs',BENCH/'receiver.mjs',BENCH/'package-lock.json']
    artifacts={str(p.relative_to(ROOT)) if p.is_relative_to(ROOT) else str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in artifact_paths}
    ready=Path(str(prefix)+'.ready.json')
    if ready.exists():raise RuntimeError('Use a fresh run label')
    stderr=open(str(prefix)+'.server.log','wb')
    server=subprocess.Popen([NODE,str(BENCH/'server.mjs'),str(CORPUS/'mixed.bin'),str(prefix),str(args.frames),str(args.pace),str(args.cut),str(args.stall)],stdout=stderr,stderr=stderr,creationflags=subprocess.CREATE_NO_WINDOW)
    try:
        deadline=time.monotonic()+15
        while not ready.exists():
            if server.poll() is not None or time.monotonic()>deadline:raise RuntimeError('Fixture failed to start')
            time.sleep(.05)
        port=json.loads(ready.read_text())['port']
        result=once(command(impl,'receiver',port,args.timeout),args.timeout+10,False,False)
        server.wait(timeout=10)
        fixture=json.loads(Path(str(prefix)+'.server.json').read_text())
        output=result['child_report']['output']
        expected={'events':args.frames*8+1,'serialized_bytes':fixture['expected_bytes'],'gift_quantity':args.frames*3,'crc32':fixture['expected_crc32']}
        checks={key:output[key]==value for key,value in expected.items()}
        checks.update(exit_ok=result['exit_code']==0,fixture_completed=fixture['completed'],fixture_no_failures=not fixture['failures'],socket_cleanup=fixture['active']==0,task_cleanup=fixture['active_tasks']==0,one_socket=fixture['peak_active']==1)
        report={'recorded_at_utc':datetime.now(timezone.utc).isoformat(),'implementation':impl,'artifact_sha256_at_start':artifacts,'corpus_sha256':hashlib.sha256((CORPUS/'mixed.bin').read_bytes()).hexdigest(),'settings':vars(args),'expected':expected,'checks':checks,'fixture':fixture,'summary':summary(result),'measurement':result}
        Path(str(prefix)+'.json').write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps({'implementation':impl,'checks':checks,'summary':summary(result)}),flush=True)
        if not all(checks.values()):raise RuntimeError(f'{impl} socket validation failed')
        return report
    finally:
        if server.poll() is None:server.kill();server.wait(timeout=5)
        stderr.close()
def pipeline(args):
    expected_bytes=(CORPUS/('expected-large.ndjson' if args.large else 'expected.ndjson')).read_bytes();expected_crc=0
    for _ in range(args.passes):expected_crc=zlib.crc32(expected_bytes,expected_crc)
    expected={'events':(16384 if args.large else 8192*8)*args.passes,'gift_quantity':(0 if args.large else 8192*3)*args.passes,'serialized_bytes':len(expected_bytes)*args.passes,'crc32':expected_crc}
    modes={}
    for mode in (('large-gzip',) if args.large else ('plain','mixed','gzip')):
        runs={'rust':[],'node':[]}
        for pair in range(args.pairs):
            for impl in (('rust','node') if pair%2==0 else ('node','rust')):
                run=once(command(impl,'pipeline',CORPUS/f'{mode}.bin',args.passes),120,False,False)
                assert run['exit_code']==0
                assert all(run['child_report']['output'][key]==value for key,value in expected.items())
                runs[impl].append(run)
        medians={impl:{'events_per_second':statistics.median(r['child_report']['events_per_second'] for r in group),'cpu_seconds':statistics.median(r['cpu_seconds'] for r in group),'peak_working_set_mib':statistics.median(r['peak_working_set_bytes'] for r in group)/1048576,'peak_private_mib':statistics.median(r['peak_private_bytes_sampled'] for r in group)/1048576} for impl,group in runs.items()}
        modes[mode]={'summary':medians,'runs':runs,'corpus_sha256':hashlib.sha256((CORPUS/f'{mode}.bin').read_bytes()).hexdigest()}
        print(json.dumps({'mode':mode,'summary':medians,'output_verified':True}),flush=True)
    (OUT/f'{args.label}.json').write_text(json.dumps({'recorded_at_utc':datetime.now(timezone.utc).isoformat(),'settings':vars(args),'expected':expected,'modes':modes,'limits':'Same synthetic wire bytes and canonical JSON+CRC32 work. Connector plus bounded normalization adapter, core types selected. Corpus load and one warm-up pass excluded from throughput but included in process RAM. No socket, TLS, signing or network latency is measured.'},indent=2)+'\n')
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['socket','pipeline']);p.add_argument('--label',required=True)
    p.add_argument('--frames',type=int,default=1000);p.add_argument('--pace',type=int,default=0);p.add_argument('--cut',type=int,default=250);p.add_argument('--stall',type=int,default=750);p.add_argument('--timeout',type=int,default=3600);p.add_argument('--passes',type=int,default=16);p.add_argument('--pairs',type=int,default=3);p.add_argument('--large',action='store_true');p.add_argument('--sequential',action='store_true')
    p.add_argument('--candidate',action='store_true')
    p.add_argument('--reverse',action='store_true')
    args=p.parse_args();USE_CANDIDATE=args.candidate
    if args.mode=='pipeline':pipeline(args)
    elif args.sequential:
        for impl in (('node','rust') if args.reverse else ('rust','node')):socket(impl,args)
    else:
        with ThreadPoolExecutor(max_workers=2) as executor:
            tasks=[executor.submit(socket,impl,args) for impl in ('rust','node')]
            for task in tasks:task.result()
