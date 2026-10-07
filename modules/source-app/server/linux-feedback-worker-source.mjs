// Fixed PUBLIC synthetic worker. It is embedded in the portable package, not
// a command/script selected from a job body. No model or quality claim.
export const LINUX_FEEDBACK_WORKER_SOURCE=String.raw`
import {readFile,writeFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import net from 'node:net';
const fail=()=>{throw new Error('synthetic_processor_input_denied');};
if(process.platform!=='linux'||process.getuid()!==1000||process.argv.length!==2)fail();
const inputBytes=await readFile('/probe/input.json');if(inputBytes.length>1500000)fail();
const packet=JSON.parse(inputBytes.toString('utf8'));
if(packet.schema!=='soty.synthetic-feedback-worker.v1'||packet.purpose!=='ocr'||packet.budget.cpuMs%1000!==0
  ||packet.budget.cpuMs<1000||packet.budget.cpuMs>10000||packet.budget.wallMs<1||packet.budget.wallMs>60000
  ||!['success','cpu','wall','cancel','scratch','output'].includes(packet.scenario))fail();
const canonical=value=>Array.isArray(value)?'['+value.map(canonical).join(',')+']':value&&typeof value==='object'?'{'+Object.keys(value).sort().map(key=>JSON.stringify(key)+':'+canonical(value[key])).join(',')+'}':JSON.stringify(value);
const digest=value=>createHash('sha256').update(canonical(value)).digest('hex');
if(digest(packet.input)!==packet.inputDigest)fail();
const limits=await readFile('/proc/self/limits','utf8'),seconds=packet.budget.cpuMs/1000;
const cpu=limits.split('\n').find(line=>line.startsWith('Max cpu time'));
if(!new RegExp('^Max cpu time\\s+'+seconds+'\\s+'+seconds+'\\s+seconds\\s*$').test(cpu??''))fail();
if(packet.scenario==='cpu'){let value=0;for(;;)value+=Math.sqrt(value+1);}
if(packet.scenario==='wall'||packet.scenario==='cancel')await new Promise(resolve=>setTimeout(resolve,180000));
if(packet.scenario==='scratch'){for(let n=0;n<200;n++)await writeFile('/scratch/chunk-'+n,Buffer.alloc(32768));fail();}
if(packet.scenario==='output'){process.stdout.write('x'.repeat(40000));process.exit(0);}
const started=performance.now();
const network=await new Promise(resolve=>{const socket=net.connect({host:'198.51.100.1',port:9});socket.setTimeout(500);
  socket.once('connect',()=>{socket.destroy();resolve('CONNECTED');});socket.once('error',error=>resolve(error.code));socket.once('timeout',()=>{socket.destroy();resolve('TIMEOUT');});});
if(!['ENETUNREACH','EHOSTUNREACH','EPERM','EACCES'].includes(network))fail();
await writeFile('/scratch/result','synthetic scratch');
const usage=process.resourceUsage();
process.stdout.write(JSON.stringify({schema:'soty.synthetic-feedback-process-receipt.v1',inputDigest:packet.inputDigest,engineRef:packet.engineRef,
  output:{kind:'transcript',text:'Synthetic local image received; no OCR model or quality claim'},
  metrics:{userCpuMicros:usage.userCPUTime,systemCpuMicros:usage.systemCPUTime,maxRssKiB:usage.maxRSS,wallMs:Math.ceil(performance.now()-started)},
  externalNetworkDenied:true})+'\n');
`;
