import fs from 'node:fs';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { crc32 } from 'node:zlib';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import { WebSocketServer } from 'ws';
import { WebcastPushFrame } from 'tiktok-live-connector';
import { records } from './common.mjs';
const [corpus,prefix,totalText='1000',paceText='0',cutText='0',stallText='0']=process.argv.slice(2);
const total=Number(totalText),pace=Number(paceText),cutEvery=Number(cutText),stallEvery=Number(stallText);
if(!Number.isInteger(total)||total<1||total>1000000||pace<0||pace>100)throw Error('invalid fixture settings');
const frames=records(corpus),oracle=records(path.join(path.dirname(corpus),'oracle.bin'));
const ending=fs.readFileSync(path.join(path.dirname(corpus),'end.bin')),endOutput=fs.readFileSync(path.join(path.dirname(corpus),'end.ndjson'));
const stats={frames:0,connections:0,cuts:0,stalls:0,replays:0,active:0,peak_active:0,active_tasks:0,expected_crc32:0,expected_bytes:0,wire_crc32:0,wire_frames:0,failures:[],completed:false};
const lag=monitorEventLoopDelay({resolution:20});lag.enable();
let pending=null,finalSent=false;const start=performance.now();
const trace=(event,extra={})=>console.error(JSON.stringify({event,elapsed_ms:Math.round(performance.now()-start),frame:stats.frames,...extra}));
const server=new WebSocketServer({host:'127.0.0.1',port:0,perMessageDeflate:false});
const save=()=>fs.writeFileSync(prefix+'.progress.json',JSON.stringify({...stats,max_event_loop_delay_ms:lag.max/1e6,elapsed_seconds:(performance.now()-start)/1000}));
const progress=setInterval(save,5000);
const globalTimeout=setTimeout(()=>{stats.failures.push('fixture global timeout');for(const ws of server.clients)ws.terminate();finish();},3600*1000);
function finish(){clearInterval(progress);clearTimeout(globalTimeout);save();lag.disable();fs.writeFileSync(prefix+'.server.json',JSON.stringify({...stats,max_event_loop_delay_ms:lag.max/1e6,elapsed_seconds:(performance.now()-start)/1000},null,2));server.close();}
server.on('listening',()=>fs.writeFileSync(prefix+'.ready.json',JSON.stringify({port:server.address().port,pid:process.pid})));
server.on('connection',(ws)=>{
  stats.connections++;stats.active++;stats.peak_active=Math.max(stats.peak_active,stats.active);ws._socket.setNoDelay(true);
  const epoch=stats.connections;let lastAck=performance.now();trace('connected',{epoch});
  let entered=false,ack=null;const abort=new AbortController();
  ws.on('error',()=>{});
  ws.on('close',(code)=>{trace('closed',{epoch,code,awaiting_ack:ack?.id??null,since_ack_ms:Math.round(performance.now()-lastAck)});stats.active--;abort.abort();if(ack){ack.reject(Error('socket closed'));ack=null;}});
  ws.on('message',(data)=>{
    try{
      const frame=WebcastPushFrame.decode(data);
      if(frame.payloadType==='ack'&&ack&&frame.logId===ack.id){lastAck=performance.now();const waiter=ack;ack=null;waiter.resolve();}
      if(frame.payloadType==='im_enter_room'&&!entered){entered=true;void drive();}
    }catch{stats.failures.push('invalid client frame');ws.terminate();}
  });
  async function send(data,waitAck=true){
    stats.wire_frames++;stats.wire_crc32=crc32(data,stats.wire_crc32);
    let waiter;
    if(waitAck){const id=WebcastPushFrame.decode(data).logId;waiter=new Promise((resolve,reject)=>{ack={id,resolve,reject};});waiter.catch(()=>{});}
    await new Promise((resolve,reject)=>ws.send(data,error=>error?reject(error):resolve()));
    if(waiter)await waiter;
  }
  async function drive(){
    stats.active_tasks++;
    try{
      if(pending!==null){const replay=pending;pending=null;stats.replays++;await send(frames[replay]);}
      while(stats.frames<total){
        if(pace)await delay(pace,undefined,{signal:abort.signal});
        const index=stats.frames%frames.length;stats.frames++;
        stats.expected_crc32=crc32(oracle[index],stats.expected_crc32);stats.expected_bytes+=oracle[index].length;
        if(stats.frames<total&&((cutEvery&&stats.frames%cutEvery===0)||(stallEvery&&stats.frames%stallEvery===0))){
          pending=index;await send(frames[index],false);
          trace('fault',{epoch,kind:stallEvery&&stats.frames%stallEvery===0?'stall':'cut'});
          if(stallEvery&&stats.frames%stallEvery===0){stats.stalls++;await delay(12000,undefined,{signal:abort.signal});}
          else stats.cuts++;
          ws.terminate();return;
        }
        await send(frames[index]);
      }
      if(!finalSent){finalSent=true;stats.expected_crc32=crc32(endOutput,stats.expected_crc32);stats.expected_bytes+=endOutput.length;await send(ending);stats.completed=true;ws.close();}
    }catch(error){
      // Planned EOF and the receiver's idle watchdog close this peer during injected faults.
      if(pending===null&&!stats.completed)stats.failures.push({reason:'unexpected peer exit',error:error.message,frame:stats.frames,elapsed_ms:Math.round(performance.now()-start)});
      ws.terminate();
    }finally{
      stats.active_tasks--;
      if(stats.completed){
        // Wait for close event bookkeeping; no live connection remains at report time.
        if(stats.active)await new Promise(resolve=>ws.once('close',resolve));
        finish();
      }
    }
  }
});
