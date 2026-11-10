"""Verify the final 0.7.3 revision without overwriting the original failed LIVE evidence."""
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


def recovery(path):
    result=[];waiting=None;epoch=0
    for line in path.read_text().splitlines():
        if not line.startswith('{'):continue
        row=json.loads(line);status=row['status']
        if status['state']=='retry_wait':waiting=row['elapsed_seconds']
        if status['state']=='receiving' and status['connection_epoch']>epoch:
            if waiting is not None:result.append(row['elapsed_seconds']-waiting)
            waiting=None;epoch=status['connection_epoch']
    return result


def main():
    original=read(OUT/'live-45min-v0.7.2.json')
    old=original['measurement']['child_report']
    assert original['binary_sha256_at_start']==sha(OUT/'v0.7.2-tested/live_soak.exe')
    assert not old['passed'] and old['report']['decode_errors']==18
    diagnostic=read(OUT/'diagnosis-v0.7.3.json')
    assert diagnostic['last_status']['status']['decode_failures']['chat_empty']>=1
    parity=read(OUT/'empty-chat-parity/result.json')
    assert parity['passed'] and parity['results']['rust']['version']=='0.7.3'
    live=read(OUT/'live-15min-v0.7.3-fixed.json')
    child=live['measurement']['child_report'];report=child['report']
    assert live['measurement']['exit_code']==0 and child['passed'] and all(child['checks'].values())
    assert live['settings']['seconds']==900 and live['measurement']['wall_seconds']>=900
    assert report['version']=='0.7.3' and report['decode_errors']==0
    assert not any(report['decode_failures'].values())
    assert report['empty_chats']>0, 'No live empty-chat case observed in this run'
    assert live['binary_sha256_at_start']==sha(ROOT/'target/release/examples/live_soak.exe')
    intervals=recovery(OUT/'live-15min-v0.7.3-fixed.progress.ndjson')
    assert len(intervals)==3 and report['reconnects']==3 and child['forced_cuts']==3
    paired={kind:read(ROOT/f'reports/matched-comparison/watchdog3-v0.7.3-final-{kind}.json') for kind in ['rust','node']}
    for result in paired.values():
        assert all(result['checks'].values())
        assert result['fixture']['active']==result['fixture']['active_tasks']==0
        assert result['fixture']['peak_active']==1
    assert paired['rust']['measurement']['child_report']['output']==paired['node']['measurement']['child_report']['output']
    rust=paired['rust']['measurement']['child_report']
    assert rust['version']=='0.7.3' and rust['idle_seconds']==3
    assert rust['report']['reconnects']==9 and rust['output']['events']==480001 and rust['report']['decode_errors']==0
    stages=[r['status']['last_connection_error']['stage'] for r in rust['transitions'] if r['status']['state']=='retry_wait']
    assert stages.count('socket_idle')==3 and stages.count('socket_read')==6
    counts=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed',(OUT/'tests-v0.7.3.log').read_text(encoding='utf-8-sig'))
    assert sum(int(n) for n,_ in counts)==82 and all(int(n)==0 for _,n in counts)
    paths=list((ROOT/'src').rglob('*.rs'))+[ROOT/'Cargo.toml',ROOT/'Cargo.lock',ROOT/'tests/core.rs',ROOT/'tests/supervisor.rs',ROOT/'examples/live_soak.rs',ROOT/'target/release/examples/live_soak.exe',ROOT/'target/release/tiktok-live-rust.exe']
    paths += [OUT/'verification-v0.7.2.json',OUT/'diagnosis-v0.7.3.json',OUT/'empty-chat-parity/result.json']
    document={
        'recorded_at_utc':datetime.now(timezone.utc).isoformat(),'version':'0.7.3','developer':'Tri Nguyen','final_checks_passed':True,
        'changes':['One immediately-ready socket read before an expired watchdog, with one probe per unchanged deadline.','Preserve empty-text chat only with nonzero message and sender identity.','Fixed-size decode-failure counters in reports and live status; no raw payload retention.'],
        'tests_passed':82,'prior_watchdog_evidence':'reports/watchdog-investigation/verification-v0.7.2.json',
        'original_live':{'passed':False,'summary':original['summary'],'decode_errors':18,'recovery_seconds':recovery(OUT/'live-45min-v0.7.2.progress.ndjson')},
        'diagnostic':diagnostic,'empty_chat_parity':parity,
        'final_live':{'summary':live['summary'],'report':report,'checks':child['checks'],'known_gift_quantity':child['known_gift_quantity'],'unknown_gift_updates':child['unknown_gift_updates'],'recovery_seconds':intervals,'events_by_connection':child['events_by_connection'],'empty_chat_case_observed':True},
        'final_shared_socket_fixture':{kind:{'checks':result['checks'],'summary':result['summary']} for kind,result in paired.items()},
        'limits':['The original failed soak has no timing trace to establish its exact historical watchdog trigger.','The 18 old LIVE rejections lack per-payload diagnostics; empty chat was confirmed in a later run, not individually for all 18.','The final fixed revision ran 15 minutes after the initial 45-minute discovery run; these are distinct runs, not 60 uninterrupted minutes on the final revision.','Reconnect times measure retry-wait to first valid batch, including backoff and handshake.','LIVE RAM includes the bounded relay and validator.','Output invariants have no independent TikTok publisher oracle; messages never delivered during outages cannot be recovered by this test.','Finite tests cannot establish 24/7 stability or absence of all leaks.','Paced socket-test durations include injected outages and are not maximum processing throughput.'],
        'sha256':{str(p.relative_to(ROOT)).replace('\\','/'):sha(p) for p in paths},
    }
    destination=OUT/'verification-v0.7.3.json'
    destination.write_text(json.dumps(document,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'report':str(destination),'tests':82,'live':live['summary'],'empty_chats':report['empty_chats'],'recovery_seconds':intervals,'events':report['events']},indent=2))


if __name__=='__main__':main()
