"""Feed Rust's in-memory wire/metadata audit to the independent Node oracle; save aggregates only."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

ROOT=Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--fixture');parser.add_argument('--user');parser.add_argument('--seconds',type=int,default=120)
    parser.add_argument('--label',required=True)
    args=parser.parse_args()
    if bool(args.fixture)==bool(args.user):parser.error('choose fixture or user')
    if not args.label.replace('-','').replace('.','').isalnum():parser.error('invalid label')
    destination=ROOT/f'reports/profile-like/{args.label}.json'
    if destination.exists():parser.error('use fresh label')
    binary=ROOT/'target/release/examples/metadata_capture.exe'
    command=[str(binary),*(['--fixture',args.fixture] if args.fixture else ['--user',args.user,str(args.seconds)])]
    # Direct OS pipe: no raw event data is retained in a report or temporary file.
    rust=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,creationflags=subprocess.CREATE_NO_WINDOW)
    node=None
    try:
        node=subprocess.Popen([shutil.which('node'),str(ROOT/'benchmarks/connector/metadata-oracle.mjs')],stdin=rust.stdout,stdout=subprocess.PIPE,stderr=subprocess.PIPE,creationflags=subprocess.CREATE_NO_WINDOW)
        rust.stdout.close()
        rust.stdout=None
        output,errors=node.communicate(timeout=args.seconds+60 if args.user else 60)
        _,rust_errors=rust.communicate(timeout=10)
        oracle=json.loads(output) if output else None
        aggregate=next((json.loads(line) for line in rust_errors.decode('utf-8','replace').splitlines() if line.startswith('{')),None)
        passed=rust.returncode==node.returncode==0 and oracle is not None and oracle['passed'] and aggregate is not None and aggregate['completed']
        result={'recorded_at_utc':datetime.now(timezone.utc).isoformat(),'settings':vars(args),'passed':passed,'rust_exit_code':rust.returncode,'oracle_exit_code':node.returncode,'capture':aggregate,'oracle':oracle,'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'raw_content_saved':False,'limits':'Same received payloads decoded independently. User projection is documented; like-specific fields cover the pinned v3 root schema. No independent TikTok publisher delivery oracle.'}
        # Script errors contain static diagnostic text; raw event values are never printed by the oracle.
        if errors:result['oracle_error']=errors.decode('utf-8','replace')[:1200]
        if aggregate is None:result['capture_error']=rust_errors.decode('utf-8','replace')[:1200]
        destination.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
        print(json.dumps(result,indent=2))
        if not passed:raise SystemExit(1)
    finally:
        for process in (rust,node):
            if process and process.poll() is None:process.kill();process.wait(timeout=5)


if __name__=='__main__':main()
