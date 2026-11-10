"""Independent synthetic empty-chat corpus, shared byte-for-byte with the connector."""
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import zlib

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'benchmarks/connector'))
from generate import blob, num, env, frame, record, canonical


def main():
    out=ROOT/'reports/watchdog-investigation/empty-chat-parity'
    out.mkdir(exist_ok=True)
    user=9007199254740995
    base=9007199254740993
    def chat(identity,text=None):
        return blob(1,num(2,identity))+blob(2,num(1,user))+(blob(3,text) if text is not None else b'')
    messages=[env('WebcastChatMessage',chat(base),base),
              env('WebcastChatMessage',chat(base),base),
              env('WebcastChatMessage',chat(base+1,''),0),
              env('WebcastChatMessage',chat(base+2),base+2,True),
              env('WebcastChatMessage',chat(base+3,'Xin chào 🎁'),base+3)]
    corpus=out/'corpus.bin'
    with corpus.open('wb') as f:
        for i,message in enumerate(messages):
            payload=blob(1,message)
            record(f,frame(gzip.compress(payload,mtime=0) if i%2 else payload,i+1))
    expected=b''.join(canonical('chat',identity,{'text':text},str(user)) for identity,text in [(base,''),(base+1,''),(base+3,'Xin chào 🎁')])
    (out/'expected.ndjson').write_bytes(expected)
    baseline=ROOT/'reports/watchdog-investigation/v0.7.2-tested/matched_pipeline.exe'
    old=subprocess.run([str(baseline),str(corpus),'1'],capture_output=True,timeout=30)
    assert old.returncode!=0 and b'invalid chat content' in old.stderr
    results={}
    for label,command in [('rust',[str(ROOT/'target/release/examples/matched_pipeline.exe')]),('connector',[shutil.which('node'),str(ROOT/'benchmarks/connector/pipeline.mjs')])]:
        capture=out/f'{label}.ndjson'
        completed=subprocess.run([*command,str(corpus),'1',str(capture)],capture_output=True,check=True,timeout=30)
        result=json.loads(completed.stdout)
        assert capture.read_bytes()==expected
        assert result['output']['events']==3 and result['output']['crc32']==zlib.crc32(expected)
        results[label]=result
    document={'passed':True,'synthetic_only':True,'cases':['absent text with both identities','duplicate','explicit empty text with common-header identity','history','ordinary Unicode chat'],
              'baseline_0_7_2':{'exit_code':old.returncode,'reason':'invalid chat content','sha256':hashlib.sha256(baseline.read_bytes()).hexdigest()},
              'corpus_sha256':hashlib.sha256(corpus.read_bytes()).hexdigest(),'results':results}
    (out/'result.json').write_text(json.dumps(document,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(document,indent=2))


if __name__=='__main__':main()
