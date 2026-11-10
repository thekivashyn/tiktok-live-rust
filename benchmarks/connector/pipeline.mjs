import { deserializeWebSocketMessage, VERSION } from 'tiktok-live-connector';
import { records, Adapter } from './common.mjs';
const [path, count, capture]=process.argv.slice(2), passes=Number(count);
if(!Number.isInteger(passes)||passes<1||passes>1024)throw Error('invalid passes');
const frames=records(path);
async function run(n,capture){const out=new Adapter(capture);for(let p=0;p<n;p++)for(const frame of frames){const decoded=await deserializeWebSocketMessage(frame);out.batch(decoded.protoMessageFetchResult);}out.finish();return out;}
await run(1);
const start=performance.now(),out=await run(passes,capture),elapsed=(performance.now()-start)/1000;
console.log(JSON.stringify({implementation:'connector_with_adapter',version:VERSION,node:process.version,frames:frames.length*passes,passes,elapsed_seconds:elapsed,events_per_second:out.events/elapsed,output:out.summary()}));
