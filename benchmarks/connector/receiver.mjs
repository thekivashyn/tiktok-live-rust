import { setTimeout as delay } from 'node:timers/promises';
import { TikTokLiveConnection, RouteConfig, VERSION } from 'tiktok-live-connector';
import { Adapter } from './common.mjs';
const [portText,secondsText='3600',capture]=process.argv.slice(2),port=Number(portText),seconds=Number(secondsText);
const idle=Number(process.env.TLR_BENCH_IDLE_SECONDS??10)*1000;
if(!Number.isInteger(port)||port<1||port>65535)throw Error('invalid loopback port');
// The only replaced route is the offline handshake; no TikTok/Euler traffic is issued.
RouteConfig.fetchSignedWebSocketFromProvider=async()=>({fetchResult:{cursor:'fixture',internalExt:'-',pushServer:`ws://127.0.0.1:${port}/`,routeParams:{},messages:[]},fetchResultCookieHeader:null,fetchResultRoomId:'42'});
const out=new Adapter(capture);let lastBatch=performance.now(),ended=false,decodeErrors=0,connections=0,peakSockets=0,activeSockets=0,connectErrors=0,watchdogs=0;
class Receiver extends TikTokLiveConnection {
  async processProtoMessageFetchResult(batch){
    try {out.batch(batch);lastBatch=performance.now();if(out.kinds.stream_end)ended=true;}
    catch {decodeErrors++;ended=true;}
  }
}
const connection=new Receiver('fixture',{fetchRoomInfoOnConnect:false,processInitialData:false,enableExtendedGiftInfo:false,wsClientOptions:{maxPayload:2*1024*1024,handshakeTimeout:10000},wsConfigOverrides:{DEFAULT_WS_PING_INTERVAL:1000}});
connection.on('error',()=>{});
connection.on('websocketConnected',()=>{activeSockets++;peakSockets=Math.max(peakSockets,activeSockets);connection.wsClient?._socket?.setNoDelay(true);});
connection.on('disconnected',()=>{activeSockets--;});
const start=performance.now();
while(!ended&&performance.now()-start<seconds*1000){
  try{
    await connection.connect('42');connections++;lastBatch=performance.now();
    while(connection.isConnected&&!ended&&performance.now()-start<seconds*1000){
      if(performance.now()-lastBatch>idle){watchdogs++;console.error(JSON.stringify({watchdog:true,elapsed_ms:performance.now()-start,idle_ms:performance.now()-lastBatch,connections}));await connection.disconnect();break;}
      await delay(10);
    }
  }catch{connectErrors++;}
  if(!ended)await delay(1500); // Explicit application reconnect policy, included in soak time.
}
await connection.disconnect();out.finish();
console.log(JSON.stringify({implementation:'connector_with_adapter',version:VERSION,node:process.version,elapsed_seconds:(performance.now()-start)/1000,output:out.summary(),connections,reconnects:Math.max(0,connections-1),connect_errors:connectErrors,decode_errors:decodeErrors,watchdog_disconnects:watchdogs,active_sockets_after_stop:activeSockets,peak_sockets:peakSockets,duplicates:out.duplicates,history_skipped:out.history,ended}));
if(!ended||decodeErrors||activeSockets!==0)process.exitCode=1;
