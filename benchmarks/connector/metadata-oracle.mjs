// Test-only independent decoder. Input stays in pipes; output contains counts/paths, no user data.
import fs from 'node:fs';
import readline from 'node:readline';
import {isDeepStrictEqual} from 'node:util';
import * as proto from 'tiktok-live-proto/v3';
export const schema=JSON.parse(fs.readFileSync(new URL('./metadata-schema.json',import.meta.url),'utf8'));
export const names={Text:'MessageCommonText',ImageContent:'Content',Image:'ImageModel',WireUser:'User',MessageCommon:'CommonMessageData',MessageId:'LiveMessageID',MessageSei:'LiveMessageSEI',TextUser:'TextPieceUser',TextGift:'TextPieceGift',TextHeart:'TextPieceHeart',TextPattern:'TextPiecePatternRef',TextImage:'TextPieceImage',SpecifiedText:'SpecifiedDisplayText',CreatorSuccess:'CreatorSuccessInfo',UserInteraction:'UserInteractionInfo',MessageDisplay:'MessageDisplayInfo',PublicArea:'PublicAreaMessageCommon',LikeDetails:'WebcastLikeMessage'};
export const camel=name=>name.replace('r#','').replace(/_([a-z])/g,(_,c)=>c.toUpperCase());
export const model=name=>proto[names[name]??name];
export function project(name,value){
  if(value==null)return null;
  return Object.fromEntries(schema[name].map(([,field,type,marker])=>{
    const key=camel(field),v=value[key],nested=Object.hasOwn(schema,type);
    if(!(key in value))throw Error(`Unknown field ${name}.${key}`);
    const result=marker?.startsWith('map')?v:marker?.startsWith('many')?(v??[]).map(x=>nested?project(type,x):x):nested?project(type,v):v;
    return [field.replace('r#',''),result];
  }));
}
function avatar(user){
  for(const image of [user.avatarLarge,user.avatarMedium,user.avatarThumb]){
    for(const value of image?.urlList??[]){
      try{const u=new URL(value);if(['https:','http:'].includes(u.protocol)&&u.hostname&&!u.username&&!u.password)return value;}catch{}
    }
  }
  return null;
}
export function profile(user){
  if(!user)return null;
  return {id:user.id!=='0'?user.id:(/^\d+$/.test(user.idStr)&&BigInt(user.idStr)<=18446744073709551615n?BigInt(user.idStr).toString():'0'),username:user.displayId||null,display_name:user.nickname||null,avatar_url:avatar(user),verified:user.verified,sec_uid:user.secUid||null,follow_info:project('FollowInfo',user.followInfo)};
}
function mismatchPath(a,b,path=''){
  if(isDeepStrictEqual(a,b))return null;
  if(a&&b&&typeof a==='object'&&typeof b==='object'){
    for(const key of new Set([...Object.keys(a),...Object.keys(b)])){const p=mismatchPath(a[key],b[key],`${path}.${key}`);if(p)return p;}
  }
  return path||'root';
}
async function main(){
  const actualFields=Object.keys(proto.WebcastLikeMessage.decode(new Uint8Array())).sort();
  const supported=[...schema.LikeDetails.map(([,f])=>camel(f)),'count','total','user'].sort();
  if(!isDeepStrictEqual(actualFields,supported))throw Error('Like root schema coverage changed');
  const report={passed:true,records:0,likes:0,profiles:0,username:0,display_name:0,avatar_url:0,mismatches:0,metadata_incomplete:0,by_kind:{},coverage_by_kind:{},like_fields_nondefault:{},mismatch_paths:[],like_root_fields:actualFields,source:'tiktok-live-proto/v3 used by connector 2.5.0',raw_content_saved:false};
  for await(const line of readline.createInterface({input:process.stdin,crlfDelay:Infinity})){
    if(line.length>600000)throw Error('Audit record too large');
    const r=JSON.parse(line);
    const data=proto[r.method].decode(Buffer.from(r.payload_hex,'hex'));
    const actual={user:profile(data.user),like_details:r.method==='WebcastLikeMessage'?project('LikeDetails',data):null,like_core:r.method==='WebcastLikeMessage'?{count:data.count,total:data.total}:null};
    report.records++;report.by_kind[r.kind]=(report.by_kind[r.kind]??0)+1;
    const coverage=report.coverage_by_kind[r.kind]??={events:0,profiles:0,username:0,display_name:0,avatar_url:0};coverage.events++;
    if(actual.user){report.profiles++;coverage.profiles++;for(const key of ['username','display_name','avatar_url'])if(actual.user[key]){report[key]++;coverage[key]++;}}
    if(r.method==='WebcastLikeMessage'){
      report.likes++;
      const defaults=proto.WebcastLikeMessage.decode(new Uint8Array());
      for(const key of actualFields)if(!isDeepStrictEqual(data[key],defaults[key]))report.like_fields_nondefault[key]=(report.like_fields_nondefault[key]??0)+1;
    }
    if(r.metadata_incomplete)report.metadata_incomplete++;
    if(!isDeepStrictEqual(r.expected,actual)){
      report.mismatches++;if(report.mismatch_paths.length<12)report.mismatch_paths.push({method:r.method,path:mismatchPath(r.expected,actual)});
    }
  }
  report.passed=report.records>0&&report.mismatches===0&&report.metadata_incomplete===0;
  console.log(JSON.stringify(report));
  if(!report.passed)process.exitCode=1;
}
if(process.argv[1]&&new URL(import.meta.url).pathname.replace(/^\//,'').toLowerCase()===process.argv[1].replaceAll('\\','/').toLowerCase())await main();

