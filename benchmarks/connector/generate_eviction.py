"""Regression oracle: a cumulative gift returns after its bounded state was evicted."""
import hashlib
import json
from generate import OUT, num, blob, env, frame, record, canonical

def gift(identity,group,count,end=False):
    payload=blob(1,num(2,identity))+num(2,1)+num(5,count)+blob(7,num(1,9007199254740993))+num(9,int(end))+num(11,group)+blob(15,num(11,1)+num(12,1)+blob(16,'Rose'))+blob(45,'8')
    return env('WebcastGiftMessage',payload,identity)
def expected(identity,group,count,end,delta):
    return canonical('gift',identity,{'gift_id':'1','group_id':str(group),'repeat_count':count,'repeat_end':end,'quantity_delta':delta,'gift_type':1,'receiver_id':'8','gift_name':'Rose','diamonds_per_gift':1})
with (OUT/'eviction.bin').open('wb') as wire,(OUT/'expected-eviction.ndjson').open('wb') as out:
    for group in range(1,4098):
        identity=9007199254740992+group
        record(wire,frame(blob(1,gift(identity,group,1))+blob(5,'-')+num(9,1),group))
        out.write(expected(identity,group,1,False,1))
    updates=[]
    for step,count,end,delta in ((1,3,False,None),(2,4,False,1),(3,4,True,0)):
        identity=9007199254750000+step
        updates.append(gift(identity,1,count,end))
        out.write(expected(identity,1,count,end,delta))
    updates.append(updates[-1])
    record(wire,frame(b''.join(blob(1,m) for m in updates)+blob(5,'-')+num(9,1),4098))
print(json.dumps({'events':4100,'confirmed_gift_quantity':4098,'unknown_updates':1,'expected_sha256':hashlib.sha256((OUT/'expected-eviction.ndjson').read_bytes()).hexdigest()}))
