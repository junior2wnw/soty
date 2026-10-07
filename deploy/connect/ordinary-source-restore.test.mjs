import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,readFile,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,dirname,resolve} from 'node:path';
import {spawnSync} from 'node:child_process';
import {createHash,randomBytes,generateKeyPairSync} from 'node:crypto';
import {Readable,Writable} from 'node:stream';
import {createOrdinaryAppStore} from '../../modules/source-app/examples/ordinary-app/store.mjs';
import {readOrdinaryFormat3} from '../../modules/source-app/examples/ordinary-app/reader3.mjs';
import {sourceColdStoppedOriginal,assertSourceColdOriginalUnchanged,sourceColdFailureCode} from '../../modules/source-app/install/cold-original.mjs';
import {encryptBackup} from './backup.mjs';
import {verifyEncryptedBackup} from './verify-backup.mjs';
import {createOrdinaryNativeRestorePorts,inspectRestorableBackup} from './restore-backup.mjs';
const sha=bytes=>createHash('sha256').update(bytes).digest('hex'),realmId='restore-synthetic';
const keys=generateKeyPairSync('rsa',{modulusLength:3072,publicKeyEncoding:{format:'pem',type:'spki'},privateKeyEncoding:{format:'pem',type:'pkcs8'}});
const limits={archiveBytes:16777216,plaintextBytes:16777216,fileBytes:4194304,extractedBytes:8388608,entries:64,headers:128,
  pathBytes:4096,pathDepth:8,externalFiles:4,externalBytes:131072,wallMs:30000,idleMs:10000};
const text=bytes=>bytes.toString('utf8').split('\0')[0],octal=bytes=>parseInt(text(bytes).trim(),8)||0;
function tarInventory(tar){const files=[];for(let at=0;at+512<=tar.length;){const header=tar.subarray(at,at+512);if(header.every(byte=>byte===0))break;
  const size=octal(header.subarray(124,136)),path=text(header.subarray(0,100)).replace(/^\.\//u,'').replace(/^\.$/u,'').replace(/\/$/u,''),type=header[156]===53?'directory':'file';
  const bytes=tar.subarray(at+512,at+512+size);files.push({path,type,size,sha256:type==='directory'?null:sha(bytes),uid:octal(header.subarray(108,116)),gid:octal(header.subarray(116,124)),mode:octal(header.subarray(100,108))});
  at+=512+Math.ceil(size/512)*512;}return files.sort((a,b)=>a.path<b.path?-1:a.path>b.path?1:0);}
function controlledStoppedContainer(){return{Id:'a'.repeat(64),Image:'sha256:'+'b'.repeat(64),RestartCount:0,
  State:{Status:'exited',Running:false,ExitCode:0,OOMKilled:false,StartedAt:'2026-10-07T00:00:00Z',FinishedAt:'2026-10-07T00:00:01Z'},
  Mounts:[{Type:'volume',Name:'own-controlled-volume',Destination:'/data',RW:true}],Config:{User:'1000:1000',WorkingDir:'/app/source-app',Env:['SYNTHETIC=only'],Labels:{fixture:'only'},Entrypoint:['node'],Cmd:['fixture']}};}
async function fixture(t,{realm=realmId,path='native/native.sqlite',empty=false}={}){
  const directory=await mkdtemp(join(tmpdir(),'ordinary-native-restore-'));t.after(async()=>{
    assert.equal(dirname(directory),resolve(tmpdir()));assert.match(directory.split(/[\\/]/u).at(-1),/^ordinary-native-restore-/u);
    await rm(directory,{recursive:true,force:true,maxRetries:3,retryDelay:25});});
  const source=join(directory,'source');await mkdir(dirname(join(source,path)),{recursive:true,mode:0o700});
  const key=randomBytes(32),database=join(source,path),store=createOrdinaryAppStore({databasePath:database,realmId:realm,key,keyId:'fixture-key',initialize:true,format:3});
  try{store.createResource({id:'fixture-resource',incarnationId:'fixture-incarnation',title:'Synthetic',guestEmpty:false});store.db.exec('PRAGMA wal_checkpoint(TRUNCATE)');}finally{store.close();key.fill(0);}
  const reader=readOrdinaryFormat3(database,realm);assert.equal(reader.format,3);assert.equal(reader.objects,29);
  const identity=sha(JSON.stringify({realm,resource:'fixture-resource'}));if(empty)await writeFile(database,Buffer.alloc(0));
  const packed=spawnSync('tar',['--format=ustar','-C',source,'-cf','-','.'],{windowsHide:true,stdio:'pipe',timeout:10000,maxBuffer:8388608});
  assert.equal(packed.status,0,'system tar creates real Native SQLite fixture');const tar=packed.stdout,files=tarInventory(tar);
  const inventory={files,stores:[{id:'ordinary-native',required:true,present:true,format:'soty.ordinary-native.v3',identitySha256:identity,paths:[path]}],external:[]};
  const manifest={version:1,generationId:'c'.repeat(32),checkpointSha256:sha('independent synthetic source checkpoint'),inventory};
  const witness={generationId:manifest.generationId,checkpointSha256:manifest.checkpointSha256,inventorySha256:sha(JSON.stringify(inventory))};
  const original=sourceColdStoppedOriginal(controlledStoppedContainer());let sequence=0;
  const metadata={original,offline:true,dataFormat:'tar',secrets:{},restoreManifest:manifest,restoreFiles:{},
    sourceNativeCheckpoint:{schema:'soty.ordinary-native-checkpoint.v1',realmId:reader.realmId,readerFormat:reader.format,readerObjects:reader.objects,nativeIdentitySha256:identity}};
  return{tar,metadata,async encrypted(meta=metadata,bytes=tar){const file=join(directory,'backup-'+sequence+++'.enc');
    await encryptBackup({output:file,publicKey:keys.publicKey,metadata:meta,stream:Readable.from([bytes])});
    return{file,privateKeyPem:keys.privateKey,expectedSha256:sha(await readFile(file)),expectedManifestSha256:sha(JSON.stringify(meta.restoreManifest)),sourceWitness:witness,limits};}};
}
const denied=async operation=>{await assert.rejects(operation,error=>['restore_archive_invalid','restore_incomplete','restore_authentication_failed'].includes(sourceColdFailureCode(error)));};

test('real encrypted Native3 system tar passes separate Source port; unchanged Root/R0 no-Connect ports deny',async t=>{
  const f=await fixture(t),input=await f.encrypted(),ports=createOrdinaryNativeRestorePorts({realmId});
  const result=await ports.inspect(input);assert.equal(result.authenticated,true);assert.equal(result.inventoryMatched,true);
  assert.equal(result.strictProfile,'soty.ordinary-native.restore.v1');assert.equal(result.sqliteFiles,1);
  await denied(inspectRestorableBackup(input));await assert.rejects(verifyEncryptedBackup(input));
  const chunks=[],output=new Writable({highWaterMark:65536,write(bytes,_encoding,next){chunks.push(Buffer.from(bytes));next();}});
  const sent=await ports.send({...input,output});assert.equal(sent.authenticated,true);assert.ok(Buffer.concat(chunks).length>f.tar.length);
});

test('Source fixed path/missing-or-empty Native/foreign realm/future format reject independently authenticated inputs',async t=>{
  const ports=createOrdinaryNativeRestorePorts({realmId});
  for(const options of [{path:'native/other.sqlite'},{path:'connector-store.sqlite'},{empty:true},{realm:'foreign-synthetic'}]){
    const f=await fixture(t,options);await denied(ports.inspect(await f.encrypted()));}
  const f=await fixture(t),base=f.metadata;
  for(const meta of [{...base,sourceNativeCheckpoint:undefined},{...base,sourceNativeCheckpoint:{...base.sourceNativeCheckpoint,readerFormat:4}},
    {...base,sourceNativeCheckpoint:{...base.sourceNativeCheckpoint,readerObjects:30}},{...base,sourceNativeCheckpoint:{...base.sourceNativeCheckpoint,extra:true}},
    {...base,original:{...base.original,State:{...base.original.State,Running:true}}}])await denied(ports.inspect(await f.encrypted(meta)));
});

test('Source factory/public inputs cannot select another profile/path/callback; tampering still fails shared GCM/tar',async t=>{
  const getter=()=>{throw Error('getter_must_not_run');};
  for(const value of [{realmId,path:'caller'},{realmId,profile:{}},Object.defineProperty({},'realmId',{enumerable:true,get:getter})])
    assert.throws(()=>createOrdinaryNativeRestorePorts(value),error=>error.message!=='getter_must_not_run');
  const f=await fixture(t),ports=createOrdinaryNativeRestorePorts({realmId}),input=await f.encrypted();
  await denied(ports.inspect({...input,profile:{realmId}}));await denied(inspectRestorableBackup({...input,profile:{realmId}}));
  const bytes=await readFile(input.file);bytes[bytes.length-1]^=1;await writeFile(input.file,bytes);
  await denied(ports.inspect({...input,expectedSha256:sha(bytes)}));
  const tar=Buffer.from(f.tar);tar[0]^=1;await denied(ports.inspect(await f.encrypted(f.metadata,tar)));
});

test('stopped Source original is derived and unchanged; running/restarted/mutated originals refuse',()=>{
  const raw=controlledStoppedContainer(),before=sourceColdStoppedOriginal(raw);
  assert.equal(assertSourceColdOriginalUnchanged(before,sourceColdStoppedOriginal(structuredClone(raw))),true);
  for(const delta of [{State:{...raw.State,Running:true}},{RestartCount:1},{State:{...raw.State,Status:'created'}},{Mounts:[]}])
    assert.throws(()=>sourceColdStoppedOriginal({...raw,...delta}));
  assert.throws(()=>assertSourceColdOriginalUnchanged(before,sourceColdStoppedOriginal({...raw,State:{...raw.State,FinishedAt:'2026-10-07T00:00:02Z'}})));
});
