"""Bounded large-chat gzip stress case, not typical LIVE traffic."""
import gzip
import hashlib
import json
from generate import OUT, num, blob, env, frame, record, canonical

with (OUT/'large-gzip.bin').open('wb') as wire, (OUT/'expected-large.ndjson').open('wb') as expected:
    for batch in range(128):
        messages=[]
        for offset in range(128):
            identity=9007199254740993+batch*128+offset
            text=f'large chat {batch}:{offset} '+('x'*4000)
            payload=blob(1,num(2,identity))+blob(2,num(1,9007199254740993))+blob(3,text)
            messages.append(env('WebcastChatMessage',payload,identity))
            expected.write(canonical('chat',identity,{'text':text}))
        payload=b''.join(blob(1,m) for m in messages)+blob(5,'-')+num(9,1)
        record(wire,frame(gzip.compress(payload,compresslevel=1,mtime=0),batch+1))
summary={'frames':128,'events':16384,'chat_bytes_approx':4018,'uncompressed_batch_bytes_approx':520000,'synthetic_only':True,
         'sha256':{name:hashlib.sha256((OUT/name).read_bytes()).hexdigest() for name in ('large-gzip.bin','expected-large.ndjson')}}
(OUT/'large-manifest.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary))
