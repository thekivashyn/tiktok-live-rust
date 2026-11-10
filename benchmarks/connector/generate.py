"""Independent synthetic wire generator and canonical output oracle. No captured user data."""
import gzip
import hashlib
import json
from pathlib import Path
import struct
import zlib

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'reports/matched-corpus'
COUNT = 8192  # Exceeds both bounded dedupe/streak windows when cycling.

def varint(n):
    b = bytearray()
    while n > 127:
        b.append((n & 127) | 128)
        n >>= 7
    b.append(n)
    return bytes(b)
def num(tag, n): return varint(tag << 3) + varint(n)
def blob(tag, v):
    if isinstance(v, str): v = v.encode('utf-8')
    return varint(tag << 3 | 2) + varint(len(v)) + v
def env(method, payload, identity, history=False):
    return blob(1, method) + blob(2, payload) + num(3, identity) + num(6, int(history))
def frame(payload, log_id):
    return num(2, log_id) + blob(6, 'pb') + blob(7, 'msg') + blob(8, payload)
def record(file, b): file.write(struct.pack('<I', len(b)) + b)
def canonical(kind, identity, data, user='9007199254740993'):
    return json.dumps([kind, str(identity), user, data], ensure_ascii=False, separators=(',', ':')).encode('utf-8') + b'\n'

def main():
    OUT.mkdir(parents=True, exist_ok=True)
    handles = {mode: (OUT/f'{mode}.bin').open('wb') for mode in ('plain','mixed','gzip')}
    oracle = (OUT/'oracle.bin').open('wb')
    all_expected = (OUT/'expected.ndjson').open('wb')
    crc = 0
    for i in range(1, COUNT+1):
        base = 9007199254740992 + i*16
        user = num(1, 9007199254740993)
        common = lambda identity, key='': blob(1, num(2, identity) + (blob(8, blob(1, key)) if key else b''))
        text = f'fixture {i}: Xin chào 🎁 "quote" \\ newline\n' + ('x'*2048 if i % 64 == 0 else '')
        chat = common(base) + blob(2, user) + blob(3, text)
        messages = [env('WebcastChatMessage', chat, base)]
        expected = canonical('chat', base, {'text':text})
        for offset, count in ((1,1),(2,3)):
            gift = common(base+offset) + num(2,1) + num(5,count) + blob(7,user) + num(9,int(count==3)) + num(11,i) + blob(15,num(11,1)+num(12,1)+blob(16,'Rose')) + blob(45,'8')
            messages.append(env('WebcastGiftMessage', gift, base+offset))
            expected += canonical('gift',base+offset,{'gift_id':'1','group_id':str(i),'repeat_count':count,'repeat_end':count==3,'quantity_delta':1 if count==1 else 2,'gift_type':1,'receiver_id':'8','gift_name':'Rose','diamonds_per_gift':1})
        messages.append(env('WebcastLikeMessage',common(base+3)+num(2,2)+num(3,i*2)+blob(5,user),base+3))
        expected += canonical('like',base+3,{'count':2,'total':str(i*2)})
        for offset, key, action in ((4,'follow',1),(5,'share',3)):
            messages.append(env('WebcastSocialMessage',common(base+offset,key)+blob(2,user)+num(4,action),base+offset))
            expected += canonical(key,base+offset,{'action':str(action),'display_key':key})
        messages.append(env('WebcastMemberMessage',common(base+6)+blob(2,user)+num(10,1),base+6))
        expected += canonical('join',base+6,{'action':1})
        messages.append(env('WebcastRoomUserSeqMessage',common(base+7)+num(3,123),base+7))
        expected += canonical('viewers',base+7,{'count':'123'},None)
        messages += [messages[2], env('WebcastChatMessage',chat,base+8,True)]
        payload = b''.join(blob(1,m) for m in messages) + blob(5,'-') + num(9,1)
        compressed = gzip.compress(payload, compresslevel=1, mtime=0)
        for mode, f in handles.items():
            record(f, frame(compressed if mode=='gzip' or mode=='mixed' and i%2==0 else payload, i))
        record(oracle, expected)
        all_expected.write(expected)
        crc = zlib.crc32(expected,crc)
    # Shared valid positive int64 domain; above 2^63-1 is interpreted as negative by upstream.
    end_id = 9223372036854775807
    end_payload = blob(1,env('WebcastControlMessage',num(2,3),end_id)) + blob(5,'-') + num(9,1)
    (OUT/'end.bin').write_bytes(frame(end_payload, COUNT+1))
    (OUT/'end.ndjson').write_bytes(canonical('stream_end',end_id,{'action':3},None))
    for f in (*handles.values(), oracle, all_expected): f.close()
    manifest = {'frames':COUNT,'events_per_cycle':COUNT*8,'gift_quantity_per_cycle':COUNT*3,'crc32_per_cycle':crc,
                'synthetic_only':True,'message_ids_above_js_safe_integer':True,
                'sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in OUT.iterdir() if p.suffix in ('.bin','.ndjson')},
                'limits':'Core event fixture, not exhaustive TikTok protocol coverage. Includes unicode, large IDs, 2KiB chat bursts, gift updates/finals, duplicates and history.'}
    (OUT/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(manifest,indent=2))

if __name__ == '__main__': main()
