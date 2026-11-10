import fs from 'node:fs';
import * as proto from 'tiktok-live-proto/v3';
import {schema,model,camel} from './metadata-oracle.mjs';
const output=process.argv[2];if(!output)throw Error('output path required');
const defaults=name=>model(name).decode(new Uint8Array());
function seed(name,depth=0){
  const value=defaults(name);
  for(const [,field,type,marker] of schema[name]){
    const key=camel(field);if(!(key in value))throw Error(`Missing ${name}.${key}`);
    const nested=Object.hasOwn(schema,type);
    let item=nested?(depth>0?seed(type,depth-1):undefined):type==='bool'?true:type==='i32'?7:['i64','u64'].includes(type)?'9007199254740993':type==='url'?'https://example.com/avatar.webp?x=1':`${name}-${key}-Tiếng Việt 🎁`;
    value[key]=marker?.startsWith('map')?{fixture:'value 🎁'}:marker?.startsWith('many')?(item===undefined?[]:[item]):item;
  }
  return value;
}
const user=seed('WireUser',1);user.id='9007199254740993';user.displayId='fixture_handle';user.nickname='Tên hiển thị 🎁';
const rows=[];
function add(method,data){
  const id=String(1000+rows.length);data.common={...proto.CommonMessageData.decode(new Uint8Array()),msgId:id,roomId:'42',createTime:'1793817326',...data.common};
  data.common.msgId=id;
  const bytes=proto[method].encode(data).finish();rows.push({method,msg_id:id,payload_hex:Buffer.from(bytes).toString('hex')});
}
for(const [method,fields] of [['WebcastChatMessage',{content:'hello'}],['WebcastGiftMessage',{giftId:'1',repeatCount:1}],['WebcastLikeMessage',{count:7,total:'9007199254740993'}],['WebcastSocialMessage',{action:'1'}],['WebcastMemberMessage',{action:1}]])add(method,{...proto[method].decode(new Uint8Array()),...fields,user});
add('WebcastLikeMessage',{...seed('LikeDetails',1),count:9,total:'9007199254740995',user});
// Exercise every nested like metadata model independently; top-level tag encoding is upstream's.
const routes={MessageCommon:['common'],Text:['common','displayText'],TextFormat:['common','displayText','defaultFormat'],TextPiece:['common','displayText','pieces',0],TextUser:['common','displayText','pieces',0,'userValue'],TextGift:['common','displayText','pieces',0,'giftValue'],TextHeart:['common','displayText','pieces',0,'heartValue'],TextPattern:['common','displayText','pieces',0,'patternRefValue'],TextImage:['common','displayText','pieces',0,'imageValue'],PatternRef:['common','displayText','pieces',0,'giftValue','nameRef'],Image:['icons',0],ImageContent:['icons',0,'content'],MessageId:['common','dependId'],MessageSei:['common','sei'],SpecifiedText:['specifiedDisplayText',0],LikeEffect:['likeEffect',0],PublicArea:['publicAreaMessageCommon'],CreatorSuccess:['publicAreaMessageCommon','creatorSuccessInfo'],TagItem:['publicAreaMessageCommon','creatorSuccessInfo','tags',0],Topic:['publicAreaMessageCommon','creatorSuccessInfo','topic'],ActionButton:['publicAreaMessageCommon','creatorSuccessInfo','topic','actionButton'],FrequencyControl:['publicAreaMessageCommon','creatorSuccessInfo','frequencyControl'],PortraitInfo:['publicAreaMessageCommon','portraitInfo'],UserMetrics:['publicAreaMessageCommon','portraitInfo','userMetrics',0],PortraitTag:['publicAreaMessageCommon','portraitInfo','portraitTag',0],UserInteraction:['publicAreaMessageCommon','userInteractionInfo'],MessageDisplay:['publicAreaMessageCommon','displayInfo'],OperationInfo:['publicAreaMessageCommon','operationInfo'],OperationState:['publicAreaMessageCommon','operationInfo','initialState']};
function install(name,obj,path,value){
  const [key,...rest]=path;
  const [,field,type,marker]=schema[name].find(([,f])=>camel(f)===key);
  if(!rest.length){obj[key]=value;return;}
  if(marker?.startsWith('many')){obj[key]??=[];obj[key][rest[0]]??=defaults(type);if(rest.length===1)obj[key][rest[0]]=value;else install(type,obj[key][rest[0]],rest.slice(1),value);}
  else {obj[key]??=defaults(type);install(type,obj[key],rest,value);}
}
for(const [name,path] of Object.entries(routes)){
  const data={...defaults('LikeDetails'),count:3,total:'99',user};install('LikeDetails',data,path,seed(name,1));add('WebcastLikeMessage',data);
}
for(const profile of [proto.User.decode(new Uint8Array()),{...user,avatarLarge:undefined,avatarMedium:user.avatarLarge},{...user,id:'0',idStr:'12345'}])add('WebcastChatMessage',{...proto.WebcastChatMessage.decode(new Uint8Array()),content:'profile variants',user:profile});
fs.writeFileSync(output,rows.map(r=>JSON.stringify(r)).join('\n')+'\n');
console.log(JSON.stringify({synthetic_only:true,records:rows.length,like_root_fields:Object.keys(defaults('LikeDetails'))}));
