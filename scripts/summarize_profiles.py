"""Verify sender/like parity and completed full-engine LIVE evidence."""
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import re

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'reports/profile-like'
def read(path):return json.loads(path.read_text(encoding='utf-8-sig'))
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    synthetic=read(OUT/'synthetic-final-v0.8.0.json')
    oracle=read(OUT/'nf-gta5vn-oracle-v0.8.0.json')
    assert synthetic['passed'] and synthetic['oracle']['records']==38
    assert oracle['passed'] and oracle['oracle']['likes']==42 and oracle['oracle']['profiles']==121
    assert oracle['binary_sha256']==sha(OUT/'oracle-capture-v0.8.0.exe')
    for report in (synthetic,oracle):
        assert report['capture']['decode_errors']==report['capture']['metadata_incomplete']==0
        assert report['oracle']['mismatches']==0 and len(report['oracle']['like_root_fields'])==12
    live=read(OUT/'nf-gta5vn-live10-v0.8.0.json')
    child=live['measurement']['child_report'];report=child['report']
    assert live['measurement']['exit_code']==0 and child['passed'] and all(child['checks'].values())
    assert live['binary_sha256_at_start']==sha(ROOT/'target/release/examples/live_soak.exe')
    assert live['settings']['username']=='nf.gta5vn' and live['settings']['seconds']==600
    assert live['measurement']['wall_seconds']>=600 and report['version']=='0.8.0'
    assert report['decode_errors']==report['metadata_incomplete_events']==0
    assert report['reconnects']==child['forced_cuts']==3
    coverage=child['profile_coverage']
    assert coverage['like']['like_details']==report['events']['like']>0
    for counts in coverage.values():assert counts['with_user_id']==counts['profiles']
    tests=(OUT/'tests-v0.8.0.log').read_text(encoding='utf-8-sig')
    counts=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed',tests)
    assert sum(int(n) for n,_ in counts)==90 and all(int(n)==0 for _,n in counts)
    recovered=[];waiting=None;epoch=0
    for line in (OUT/'nf-gta5vn-live10-v0.8.0.progress.ndjson').read_text().splitlines():
        if not line.startswith('{'):continue
        row=json.loads(line);status=row['status']
        if status['state']=='retry_wait':waiting=row['elapsed_seconds']
        if status['state']=='receiving' and status['connection_epoch']>epoch:
            if waiting is not None:recovered.append(row['elapsed_seconds']-waiting)
            waiting=None;epoch=status['connection_epoch']
    assert len(recovered)==3
    paths=list((ROOT/'src').rglob('*.rs'))+[ROOT/'Cargo.toml',ROOT/'Cargo.lock',ROOT/'tests/metadata.rs',ROOT/'target/release/tiktok-live-rust.exe',ROOT/'target/release/examples/live_soak.exe',OUT/'oracle-capture-v0.8.0.exe',ROOT/'scripts/generate_metadata_models.py',ROOT/'benchmarks/connector/metadata-oracle.mjs',ROOT/'benchmarks/connector/metadata-schema.json',ROOT/'benchmarks/connector/package-lock.json']
    result={'recorded_at_utc':datetime.now(timezone.utc).isoformat(),'version':'0.8.0','developer':'Tri Nguyen','passed':True,'tests_passed':90,'synthetic':synthetic,'same_payload_live_oracle':oracle,'engine_live':{'summary':live['summary'],'report':report,'checks':child['checks'],'profile_coverage':coverage,'recovery_seconds':recovered,'known_gift_quantity':child['known_gift_quantity']},'limits':['Like root coverage refers to the pinned v3 schema, with an explicit User identity/profile projection.','Unknown protocol fields and unrelated event types are not newly implemented.','Source-absent profile fields stay null; default wire values do not prove field presence.','No claim that all viewer taps or events were delivered by TikTok.','RAM includes the bounded LIVE relay and validation consumer; finite runs do not prove 24/7 stability.','The decoder parity probe and engine soak are separate sequential runs on the same channel.'],'sha256':{str(p.relative_to(ROOT)).replace('\\','/'):sha(p) for p in paths}}
    (OUT/'verification-v0.8.0.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'passed':True,'tests':90,'live':live['summary'],'events':report['events'],'profile_coverage':coverage,'recovery_seconds':recovered},indent=2))


if __name__=='__main__':main()
