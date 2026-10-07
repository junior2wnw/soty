// Private fixed helper transport, never a public plaintext API. The only
// plaintext file lives on this helper's bounded /tmp tmpfs and is removed.
import {createWriteStream,createReadStream,constants,fstatSync} from 'node:fs';
import {lstat,statfs,readFile,unlink,open,readdir} from 'node:fs/promises';
import {Writable,Readable,Duplex} from 'node:stream';
import {once} from 'node:events';
import {sourceColdFailureCode} from './cold-original.mjs';
const check=value=>{if(!value)throw Error('source_cold_guard_refused');};
const CHUNK=65536,MAX=16777216;
const stages=new Set(['preflight','sender_open','send','receiver_open','receive','cleanup','done']);
const own=(value,key)=>value&&Object.getOwnPropertyDescriptor(value,key)?.value;
const flags=['tmpfsPinned','outputWritable','outputNotDuplex','outputConstructed','outputEmitClose','outputAutoDestroy',
  'inputReadable','inputNotDuplex','inputConstructed','inputEmitClose','inputAutoDestroy','targetPinned'];
const same=(a,b)=>a.dev===b.dev&&a.ino===b.ino&&a.uid===b.uid&&a.gid===b.gid&&a.mode===b.mode&&a.nlink===b.nlink;
async function mountId(fd){const value=await readFile('/proc/self/fdinfo/'+fd,'ascii'),match=value.match(/^mnt_id:\s*([1-9]\d*)$/mu);check(match);return BigInt(match[1]);}
async function targetCurrent(target){
  for(const[key,path]of[['namespace','/target/restore'],['dataRoot','/target/restore/data'],['configRoot','/target/restore/config']]){
    const expected=target[key];check(expected?.path===path);const file=await open(path,constants.O_RDONLY|constants.O_DIRECTORY|constants.O_NOFOLLOW);
    try{const actual=await file.stat({bigint:true});check(actual.isDirectory()&&actual.uid===1000n&&(actual.mode&0o7777n)===0o700n
      &&actual.dev===expected.dev&&actual.ino===expected.ino&&await mountId(file.fd)===BigInt(expected.mountId));}finally{await file.close();}
  }
  check((await readdir(target.dataRoot.path)).length===0&&(await readdir(target.configRoot.path)).length===0);
}
const closed=stream=>new Promise(resolve=>{stream.once('close',resolve);stream.on('error',()=>{});});
export function projectColdExtractDiagnostic(value){
  const schema=own(value,'schema'),passed=own(value,'passed'),stage=own(value,'stage'),sendPassed=own(value,'sendPassed'),receivePassed=own(value,'receivePassed'),
    cleanupUnknown=own(value,'cleanupUnknown'),nodeVersion=own(value,'nodeVersion'),profiles=own(value,'profiles'),originalCode=own(value,'code');
  if(schema!=='soty.source-cold-extract.v2'||typeof passed!=='boolean'||!stages.has(stage)||typeof sendPassed!=='boolean'||typeof receivePassed!=='boolean'
    ||typeof cleanupUnknown!=='boolean'||typeof nodeVersion!=='string'||!/^v24\.\d+\.\d+$/u.test(nodeVersion)
    ||own(value,'productionReady')!==false||!profiles||flags.some(key=>typeof own(profiles,key)!=='boolean'))return null;
  const code=originalCode==='none'?'none':sourceColdFailureCode({code:originalCode});if(code!==originalCode)return null;
  return{schema,passed,stage,code,sendPassed,receivePassed,cleanupUnknown,nodeVersion,profiles:Object.fromEntries(flags.map(key=>[key,own(profiles,key)])),productionReady:false};
}
export async function transferColdNativeThroughRam(ports,args,target){
  let output,input,outputClosed,inputClosed,pin,owned=false,sendPassed=false,receivePassed=false,cleanupUnknown=false,stage='preflight',code='none';
  const profiles=Object.fromEntries(flags.map(key=>[key,false]));const file='/tmp/soty-cold-plaintext-'+args.targetId;
  try{
    check(process.platform==='linux'&&process.getuid()===1000&&/^v24\./u.test(process.version)&&/^[a-f0-9]{32}$/u.test(args.targetId)
      &&Number.isSafeInteger(args.limits.plaintextBytes)&&args.limits.plaintextBytes>0&&args.limits.plaintextBytes<=MAX);
    const tmp=await lstat('/tmp',{bigint:true}),fs=await statfs('/tmp',{bigint:true});
    check(tmp.isDirectory()&&!tmp.isSymbolicLink()&&tmp.uid===1000n&&(tmp.mode&0o7777n)===0o700n&&fs.type===0x01021994n
      &&fs.blocks*fs.bsize<=67108864n);profiles.tmpfsPinned=true;await targetCurrent(target);profiles.targetPinned=true;
    stage='sender_open';output=createWriteStream(file,{flags:constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,
      mode:0o600,highWaterMark:CHUNK,autoClose:true,emitClose:true,autoDestroy:true});outputClosed=closed(output);
    await once(output,'ready');owned=true;pin=fstatSync(output.fd,{bigint:true});const observed=await lstat(file,{bigint:true});
    check(pin.isFile()&&pin.uid===1000n&&(pin.mode&0o7777n)===0o600n&&pin.nlink===1n&&pin.dev===tmp.dev&&same(pin,observed));
    pin={...pin,mountId:await mountId(output.fd)};
    Object.assign(profiles,{outputWritable:output instanceof Writable,outputNotDuplex:!(output instanceof Duplex),
      outputConstructed:output._writableState.constructed===true,outputEmitClose:output._writableState.emitClose===true,outputAutoDestroy:output._writableState.autoDestroy===true});
    stage='send';const sent=await ports.send({file:'/probe/backup.enc',privateKeyPem:args.privateKeyPem,expectedSha256:args.expectedSha256,
      expectedManifestSha256:args.expectedManifestSha256,sourceWitness:args.sourceWitness,limits:args.limits,output});
    await outputClosed;check(sent.authenticated===true&&output.closed===true&&output.fd===null);sendPassed=true;
    const ready=await lstat(file,{bigint:true});check(same(pin,ready)&&ready.size<=BigInt(args.limits.plaintextBytes));
    stage='receiver_open';input=createReadStream(file,{flags:constants.O_RDONLY|constants.O_NOFOLLOW,highWaterMark:CHUNK,autoClose:true,emitClose:true});
    inputClosed=closed(input);await once(input,'ready');const opened=fstatSync(input.fd,{bigint:true});check(same(pin,opened)&&await mountId(input.fd)===pin.mountId);
    Object.assign(profiles,{inputReadable:input instanceof Readable,inputNotDuplex:!(input instanceof Duplex),inputConstructed:input._readableState.constructed===true,
      inputEmitClose:input._readableState.emitClose===true,inputAutoDestroy:input._readableState.autoDestroy===true});
    const{archiveBytes,...limits}=args.limits;stage='receive';const received=await ports.extract({input,target,expectedManifestSha256:args.expectedManifestSha256,
      sourceWitness:args.sourceWitness,limits:{...limits,freeSpaceReserveBytes:16777216}});
    await inputClosed;check(received.extracted===true&&received.readbackVerified===true&&input.closed===true&&input.fd===null);receivePassed=true;
  }catch(error){code=sourceColdFailureCode(error);}
  finally{
    for(const[stream,done]of[[input,inputClosed],[output,outputClosed]])if(stream)try{if(!stream.closed)stream.destroy();await done;check(stream.closed===true&&stream.fd===null);}catch{cleanupUnknown=true;}
    if(owned)try{
      const audit=await open(file,constants.O_RDONLY|constants.O_NOFOLLOW);
      try{const actual=await audit.stat({bigint:true}),named=await lstat(file,{bigint:true});
        check(pin&&actual.isFile()&&same(pin,actual)&&same(actual,named)&&await mountId(audit.fd)===pin.mountId);await unlink(file);
      }finally{await audit.close();}
    }catch{cleanupUnknown=true;}
  }
  return projectColdExtractDiagnostic({schema:'soty.source-cold-extract.v2',passed:sendPassed&&receivePassed&&!cleanupUnknown,stage:sendPassed&&receivePassed?'done':stage,
    code,sendPassed,receivePassed,cleanupUnknown,nodeVersion:process.version,profiles,productionReady:false});
}
