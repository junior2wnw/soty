// PUBLIC controlled fixture: real Native3 SQLite/tar/crypto/native fs streams.
// Envelope original is a synthetic unit-contract record, NOT a stopped-Docker
// witness. Full cold separately captures actual stopped before/after original.
import {mkdir,lstat,readdir,open,readFile,stat} from 'node:fs/promises';
import {spawnSync} from 'node:child_process';
import {createHash,randomBytes,generateKeyPairSync} from 'node:crypto';
import {Readable,PassThrough} from 'node:stream';
import {createOrdinaryAppStore} from '../examples/ordinary-app/store.mjs';
import {readOrdinaryFormat3} from '../examples/ordinary-app/reader3.mjs';
import {createOrdinaryNativeRestorePorts} from '../../../deploy/connect/restore-backup.mjs';
import {encryptBackup} from '../../../deploy/connect/backup.mjs';
import {transferColdNativeThroughRam} from './cold-ram-transfer.mjs';
import {SOURCE_COLD_NATIVE_REALM,sourceColdFailureCode} from './cold-original.mjs';
const sha=bytes=>createHash('sha256').update(bytes).digest('hex'),check=value=>{if(!value)throw Error('source_cold_guard_refused');};
const directory='/tmp/soty-cold-stream-probe',database=directory+'/native/native.sqlite',backup='/probe/backup.enc';
const limits={archiveBytes:16777216,plaintextBytes:16777216,fileBytes:4194304,extractedBytes:8388608,entries:64,headers:128,pathBytes:4096,pathDepth:8,
  externalFiles:4,externalBytes:131072,wallMs:30000,idleMs:10000};
const realmId=SOURCE_COLD_NATIVE_REALM,ports=createOrdinaryNativeRestorePorts({realmId});
async function pin(path){const file=await open(path,'r');try{const s=await file.stat({bigint:true}),text=await readFile('/proc/self/fdinfo/'+file.fd,'ascii');
  check(s.isDirectory()&&s.uid===1000n&&(s.mode&0o7777n)===0o700n);return{path,dev:s.dev,ino:s.ino,mountId:Number(text.match(/^mnt_id:\s*([1-9]\d*)$/mu)[1])};}finally{await file.close();}}
let phase='preflight',result,passed=false,duplexOutputDenied=false,duplexInputDenied=false;
try{
  check(process.platform==='linux'&&process.getuid()===1000&&process.version==='v24.15.0');process.umask(0o077);
  const targetRoot=await lstat('/target');check(targetRoot.isDirectory()&&!targetRoot.isSymbolicLink()&&targetRoot.uid===1000&&(targetRoot.mode&0o777)===0o700&&(await readdir('/target')).length===0);
  await mkdir(directory,{mode:0o700});await mkdir(directory+'/native',{mode:0o700});
  const key=randomBytes(32),store=createOrdinaryAppStore({databasePath:database,realmId,key,keyId:'fixture-key',initialize:true,format:3});
  try{store.createResource({id:'stream-fixture',incarnationId:'stream-incarnation',title:'Synthetic',guestEmpty:false});store.db.exec('PRAGMA wal_checkpoint(TRUNCATE)');}finally{store.close();key.fill(0);}
  const reader=readOrdinaryFormat3(database,realmId);check(reader.format===3&&reader.objects===29);
  const files=[];async function scan(path,name){const s=await lstat(path);check(!s.isSymbolicLink());files.push({path:name,type:s.isDirectory()?'directory':'file',size:s.isDirectory()?0:s.size,
    sha256:s.isDirectory()?null:sha(await readFile(path)),uid:s.uid,gid:s.gid,mode:s.mode&0o777});if(s.isDirectory())for(const name2 of(await readdir(path)).sort())await scan(path+'/'+name2,name?name+'/'+name2:name2);}
  await scan(directory,'');files.sort((a,b)=>a.path<b.path?-1:a.path>b.path?1:0);const identity=sha(JSON.stringify({realmId,resource:'stream-fixture'}));
  const inventory={files,stores:[{id:'ordinary-native',required:true,present:true,format:'soty.ordinary-native.v3',identitySha256:identity,paths:['native/native.sqlite']}],external:[]};
  const targetId=randomBytes(16).toString('hex'),manifest={version:1,generationId:targetId,checkpointSha256:sha('independent streams fixture'),inventory};
  const witness={generationId:targetId,checkpointSha256:manifest.checkpointSha256,inventorySha256:sha(JSON.stringify(inventory))};
  const tar=spawnSync('tar',['--format=ustar','-C',directory,'-cf','-','.'],{stdio:['ignore','pipe','ignore'],timeout:10000,maxBuffer:8388608});check(tar.status===0);
  const keys=generateKeyPairSync('rsa',{modulusLength:3072,publicKeyEncoding:{format:'pem',type:'spki'},privateKeyEncoding:{format:'pem',type:'pkcs8'}});
  const metadata={offline:true,dataFormat:'tar',original:{State:{Running:false},Mounts:[{Type:'volume',Destination:'/data'}]},secrets:{},restoreFiles:{},restoreManifest:manifest,
    sourceNativeCheckpoint:{schema:'soty.ordinary-native-checkpoint.v1',realmId,readerFormat:3,readerObjects:29,nativeIdentitySha256:identity}};
  // /probe is a fresh fixture-only bounded RAM mount, not a host packet bind.
  await encryptBackup({output:backup,publicKey:keys.publicKey,metadata,stream:Readable.from([tar.stdout])});tar.stdout.fill(0);
  const input={targetId,privateKeyPem:keys.privateKey,expectedSha256:sha(await readFile(backup)),expectedManifestSha256:sha(JSON.stringify(manifest)),sourceWitness:witness,limits};
  await mkdir('/target/restore',{mode:0o700});await mkdir('/target/restore/data',{mode:0o700});await mkdir('/target/restore/config',{mode:0o700});
  const ns=await stat('/proc/self/ns/mnt',{bigint:true}),target={targetId,mountNamespace:{dev:ns.dev,ino:ns.ino},namespace:await pin('/target/restore'),dataRoot:await pin('/target/restore/data'),configRoot:await pin('/target/restore/config')};
  phase='duplex_negatives';const duplex=new PassThrough({highWaterMark:65536});
  try{await ports.send({file:backup,privateKeyPem:input.privateKeyPem,expectedSha256:input.expectedSha256,expectedManifestSha256:input.expectedManifestSha256,sourceWitness:witness,limits,output:duplex});}
  catch(error){duplexOutputDenied=sourceColdFailureCode(error)==='restore_archive_invalid';}
  const{archiveBytes,...extractLimits}=limits;
  try{await ports.extract({input:duplex,target,expectedManifestSha256:input.expectedManifestSha256,sourceWitness:witness,limits:{...extractLimits,freeSpaceReserveBytes:16777216}});}
  catch(error){duplexInputDenied=sourceColdFailureCode(error)==='restore_archive_invalid';}
  check(duplexOutputDenied&&duplexInputDenied&&duplex.destroyed===false&&(await readdir(target.dataRoot.path)).length===0);duplex.destroy();
  phase='native_streams';result=await transferColdNativeThroughRam(ports,input,target);input.privateKeyPem='';keys.privateKey='';check(result?.passed===true);
  const restored=readOrdinaryFormat3('/target/restore/data/native/native.sqlite',realmId);check(restored.format===3&&restored.objects===29);
  check(sha(await readFile('/target/restore/data/native/native.sqlite'))===sha(await readFile(database)));
  passed=true;phase='done';
}catch(error){console.log(JSON.stringify({schema:'soty.source-native-stream-probe.v1',passed:false,phase,code:sourceColdFailureCode(error),nodeVersion:process.version,
  duplexOutputDenied,duplexInputDenied,...(result?{transfer:result}:{}),syntheticOriginal:true,physicalColdProved:false,productionReady:false}));process.exitCode=1;}
if(passed)console.log(JSON.stringify({schema:'soty.source-native-stream-probe.v1',passed:true,phase,nodeVersion:process.version,duplexOutputDenied,duplexInputDenied,
  transfer:result,syntheticOriginal:true,physicalColdProved:false,productionReady:false}));
