"""Verify watchdog regression artifacts and the completed real LIVE run."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'reports/watchdog-investigation'


def read(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verified_socket(label):
    records={kind:read(ROOT/f'reports/matched-comparison/{label}-{kind}.json') for kind in ('rust','node')}
    for record in records.values():
        assert all(record['checks'].values())
        fixture=record['fixture'];child=record['measurement']['child_report']
        assert fixture['active']==fixture['active_tasks']==0 and fixture['peak_active']==1
        assert child.get('decode_errors',child.get('report',{}).get('decode_errors'))==0
    assert records['rust']['measurement']['child_report']['output']==records['node']['measurement']['child_report']['output']
    return {kind:{'summary':r['summary'],'fixture':r['fixture']} for kind,r in records.items()}


def main():
    if (OUT/'verification-v0.7.2.json').exists():
        raise SystemExit('0.7.2 verification already recorded; preserve its original source/binary hashes.')
    pauses={version:[read(OUT/f'pause-{version}-{i}.json') for i in range(1,4)] for version in ('0.7.1','0.7.2','connector')}
    for old,new in zip(pauses['0.7.1'],pauses['0.7.2']):
        assert old['receiver']['report']['last_connection_error']['stage']=='socket_idle'
        assert not old['checks']['complete_output'] and old['receiver']['report']['reconnects']==1
        assert all(new['checks'].values())
        assert new['receiver']['report']['watchdog_ready_reads']>=1
        assert new['receiver']['report']['reconnects']==0
    baseline=verified_socket('watchdog3-repro1')
    fixed=verified_socket('watchdog3-fixed')
    fixed_child=fixed['rust']['summary']['child']
    assert fixed_child['version']=='0.7.2' and fixed_child['idle_seconds']==3
    assert fixed_child['report']['reconnects']==9 and fixed_child['output']['events']==480001
    retry_causes=[r['status']['last_connection_error']['stage'] for r in fixed_child['transitions'] if r['status']['state']=='retry_wait']
    assert retry_causes.count('socket_idle')==3 and retry_causes.count('socket_read')==6
    perf=read(OUT/'performance-v0.7.2.json')
    for paths in perf['raw_reports'].values():
        for name in paths:
            assert all(read(ROOT/name)['checks'].values())
    live=read(OUT/'live-45min-v0.7.2.json')
    child=live['measurement']['child_report']
    assert live['settings']['seconds']==2700 and live['settings']['cuts']==3
    assert live['measurement']['wall_seconds']>=2700
    assert child['passed']==all(child['checks'].values())
    assert live['measurement']['exit_code']==(0 if child['passed'] else 1)
    assert child['report']['version']=='0.7.2'
    assert live['binary_sha256_at_start']==sha(ROOT/'target/release/examples/live_soak.exe')
    progress=[json.loads(line) for line in (OUT/'live-45min-v0.7.2.progress.ndjson').read_text().splitlines() if line.startswith('{')]
    recovery=[];retry=None;epoch=0
    for row in progress:
        status=row['status']
        if status['state']=='retry_wait':retry=row['elapsed_seconds']
        if status['state']=='receiving' and status['connection_epoch']>epoch:
            if retry is not None:
                recovery.append({'connection_epoch':status['connection_epoch'],'retry_wait_to_ready_seconds':row['elapsed_seconds']-retry})
                retry=None
            epoch=status['connection_epoch']
    assert len(recovery)==3 and all(r['retry_wait_to_ready_seconds']>0 for r in recovery)
    test_log=(OUT/'tests-v0.7.2.log').read_text(encoding='utf-8-sig')
    counts=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed',test_log)
    assert sum(int(n) for n,_ in counts)==79 and all(int(n)==0 for _,n in counts)
    paths=[ROOT/'Cargo.toml',ROOT/'Cargo.lock',ROOT/'target/release/tiktok-live-rust.exe',ROOT/'target/release/examples/live_soak.exe']
    paths += [OUT/'baseline/engine.rs', OUT/'baseline/instrumented_receiver.rs', OUT/'baseline/instrumented_receiver.exe', ROOT/'target/release/examples/matched_receiver_candidate.exe']
    paths+=list((ROOT/'src').rglob('*.rs'))
    paths+=[ROOT/'tests/supervisor.rs',ROOT/'examples/live_soak.rs',ROOT/'examples/matched_receiver.rs',ROOT/'scripts/watchdog_pause.py',ROOT/'scripts/measure_live_soak.py',ROOT/'scripts/measure_process.py']
    summary={
        'recorded_at_utc':datetime.now(timezone.utc).isoformat(),'engine':'tiktok-live-rust','version':'0.7.2','developer':'Tri Nguyen',
        'finding':{'confirmed':'An expired prioritized watchdog can discard already-buffered data after receiver task scheduling delay. Reproduced in a virtual-time regression and three Windows process-pause pairs.',
                   'historical_limit':'Original soak1 has no receiver timing/error trace. Its exact historical trigger remains unproven. A normal three-second baseline run passes.',
                   'fix':'One non-waiting read at each expired health deadline; existing decoder/validation handles the message. Same unchanged deadline cannot be probed again. No timeout increase, growing buffer or new production dependency.',
                   'limits':'Single-frame probe is conservative, not an unlimited drain or a guarantee against arbitrary host stalls.'},
        'pause_tests':{version:[{'checks':r['checks'],'pause_seconds':r['pause_seconds'],'receiver_version':r['receiver']['version'],'reconnects':r['receiver'].get('report',r['receiver'])['reconnects'],'events':r['receiver']['output']['events']} for r in group] for version,group in pauses.items()},
        'normal_three_second_baseline':baseline,'three_second_fixed_soak':fixed,
        'loopback_performance':perf,
        'live':{'passed':child['passed'],'failed_checks':[k for k,v in child['checks'].items() if not v],'public_channel':live['settings']['username'],'summary':live['summary'],'receiver':child,'recovery_intervals':recovery,'timing_scope':'retry_wait to first valid webcast batch; includes backoff and handshake, not exact outage or first application-event latency'},
        'validation':{'tests_passed':79,'tests_log':'reports/watchdog-investigation/tests-v0.7.2.log','clippy_log':'reports/watchdog-investigation/clippy-v0.7.2.log'},
        'provenance':{'live_search_usd':0.001,'live_search_call_id':'0fa0a4dae03744688633bb167f5444e8','live_data_path':'Direct guest connection from the local engine, no Euler or search-provider relay'},
        'limits':['LIVE memory includes the bounded relay and validator.','LIVE sequence/dedupe/field/count checks have no independent publisher oracle.','No promise to recover events never delivered during outages.','A finite 45-minute run is not proof of 24/7 stability or absence of all leaks.','Paired performance tests shared the host with paced soaks; small changes can be noise.'],
        'sha256':{str(p.relative_to(ROOT)).replace('\\','/'):sha(p) for p in paths},
    }
    destination=OUT/'verification-v0.7.2.json'
    destination.write_text(json.dumps(summary,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'report':str(destination),'tests_passed':79,'live_summary':live['summary'],'recovery':recovery},indent=2))


if __name__=='__main__':main()
