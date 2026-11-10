import fs from 'node:fs';
import { crc32 } from 'node:zlib';
import { WebcastDeserializeConfig } from 'tiktok-live-connector';
export const core = ['WebcastChatMessage','WebcastGiftMessage','WebcastLikeMessage','WebcastSocialMessage','WebcastMemberMessage','WebcastRoomUserSeqMessage','WebcastControlMessage'];
WebcastDeserializeConfig.includeMessageTypes = core;
WebcastDeserializeConfig.showBase64OnDecodeError = false;
export function records(path) {
  const data = fs.readFileSync(path), frames=[];
  for (let p=0;p<data.length;) {
    if(p+4>data.length) throw Error('truncated length');
    const length=data.readUInt32LE(p);p+=4;
    if(length>2*1024*1024||p+length>data.length) throw Error('invalid frame');
    frames.push(data.subarray(p,p+length));p+=length;
  }
  return frames;
}
class Window {
  constructor(capacity,map=false){this.capacity=capacity;this.items=map?new Map():new Set();this.order=new Array(capacity);this.next=0;this.size=0;this.evictions=0;}
  has(key){return this.items.has(key);}
  get(key){return this.items.get(key);}
  set(key,value){
    if(!this.items.has(key)){
      if(this.size===this.capacity){this.items.delete(this.order[this.next]);this.evictions++;}else this.size++;
      this.order[this.next]=key;this.next=(this.next+1)%this.capacity;
    }
    if(this.items instanceof Map)this.items.set(key,value);else this.items.add(key);
  }
}
// Application adapter, not an upstream feature. Same bounded windows and canonical output as Rust.
export class Adapter {
  constructor(capture){this.ids=new Window(8192);this.streaks=new Window(4096,true);this.events=0;this.bytes=0;this.gifts=0;this.crc=0;this.kinds={};this.duplicates=0;this.history=0;this.fd=capture?fs.openSync(capture,'w'):null;}
  batch(batch){for(const m of batch.messages??[])this.message(m);}
  message(raw){
    if(raw.isHistory){this.history++;return;}
    if(!core.includes(raw.method))return;
    if(raw.decodeError||!raw.decodedData)throw Error('fixture decode failure');
    const d=raw.decodedData.data,id=raw.msgId!=='0'?raw.msgId:(d.common?.msgId??'0');
    if(id!=='0'&&this.ids.has(id)){this.duplicates++;return;}
    let kind,data;let user=d.user?.id??null;
    switch(raw.method){
      case 'WebcastChatMessage':kind='chat';data={text:d.content};break;
      case 'WebcastGiftMessage':{
        kind='gift';const count=Math.max(d.repeatCount,1),type=d.gift?.type??null,receiver=d.toMemberId??'';
        let delta=null;
        if(type===1&&d.groupId!=='0'&&user&&user!=='0'){
          const key=`${user}|${d.giftId}|${d.groupId}|${receiver}`,unseen=!this.streaks.has(key),prior=this.streaks.get(key)??0;
          delta=Math.max(count-prior,0);this.streaks.set(key,Math.max(count,prior));
          if(unseen&&this.streaks.evictions>0&&count>1)delta=null;
        }else if(type===1){if(d.repeatEnd)delta=count;}else if(type!==null)delta=count;
        data={gift_id:d.giftId,group_id:d.groupId,repeat_count:count,repeat_end:!!d.repeatEnd,quantity_delta:delta,gift_type:type,receiver_id:receiver,gift_name:d.gift?.name??null,diamonds_per_gift:d.gift?.diamondCount??null};
        this.gifts+=delta??0;break;
      }
      case 'WebcastLikeMessage':kind='like';data={count:d.count,total:d.total};break;
      case 'WebcastSocialMessage':{
        const key=d.common?.displayText?.key??'',tokens=key.split('_'),follow=tokens.includes('follow'),share=tokens.includes('share');
        kind=follow&&!share?'follow':share&&!follow?'share':!key&&d.action==='1'?'follow':!key&&d.action==='3'?'share':'social';
        data={action:d.action,display_key:key};break;
      }
      case 'WebcastMemberMessage':if(d.action!==1)return;kind='join';data={action:d.action};break;
      case 'WebcastRoomUserSeqMessage':kind='viewers';user=null;data={count:d.total};break;
      case 'WebcastControlMessage':if(![3,4].includes(d.action))return;kind='stream_end';user=null;data={action:d.action};break;
      default:return;
    }
    if(id!=='0')this.ids.set(id);
    const b=Buffer.from(JSON.stringify([kind,id,user,data])+'\n');
    this.events++;this.bytes+=b.length;this.crc=crc32(b,this.crc);this.kinds[kind]=(this.kinds[kind]??0)+1;
    if(this.fd!==null)fs.writeSync(this.fd,b);
  }
  finish(){if(this.fd!==null){fs.closeSync(this.fd);this.fd=null;}}
  summary(){return {events:this.events,serialized_bytes:this.bytes,gift_quantity:this.gifts,event_counts:this.kinds,crc32:this.crc};}
}
