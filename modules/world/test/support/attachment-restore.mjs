// OWN Windows/Linux fixture only. Real SDK encryption, authentication and sender;
// fixed own disk sink is not the protected Linux deployment receiver.
import assert from 'node:assert/strict';
import { createHash, generateKeyPairSync } from 'node:crypto';
import { mkdir, readFile, writeFile, open } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { join } from 'node:path';
import { Readable, Writable } from 'node:stream';
import { DatabaseSync } from 'node:sqlite';
import { encryptBackup } from '../../../../deploy/connect/backup.mjs';
import { inspectRestorableBackup, sendAuthenticatedBackup } from '../../../../deploy/connect/restore-backup.mjs';
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const text=bytes=>bytes.toString('utf8').replace(/\0.*$/su,'');
const octal=bytes=>parseInt(text(bytes).trim()||'0',8);
const LIMITS={archiveBytes:256*1024**2,plaintextBytes:256*1024**2,fileBytes:64*1024**2,extractedBytes:256*1024**2,
  entries:64,headers:128,pathBytes:8192,pathDepth:8,externalFiles:4,externalBytes:4096,wallMs:30000,idleMs:5000};
const allowed=new Set(['','rooms','world',...['connector-store.sqlite','world/world.sqlite'].flatMap(name=>[name,name+'-wal',name+'-shm'])]);
function tarRecords(raw) {
  const rows=[];
  for(let at=0;at+512<=raw.length;) {
    const h=raw.subarray(at,at+512);if(!h.some(Boolean))break;
    assert.equal(octal(h.subarray(148,156)),h.subarray(0,148).reduce((a,b)=>a+b,0)+8*32+h.subarray(156).reduce((a,b)=>a+b,0));
    const type=String.fromCharCode(h[156]),name=text(h.subarray(0,100)).replace(/^\.\//u,'').replace(/\/$/u,'').replace(/^\.$/u,'');
    assert.ok(allowed.has(name)&&['0','5'].includes(type));assert.ok(!rows.some(r=>r.file.path===name));
    const size=octal(h.subarray(124,136)),body=Buffer.from(raw.subarray(at+512,at+512+size));assert.equal(body.length,size);
    rows.push({body,file:{path:name,type:type==='5'?'directory':'file',size,sha256:type==='5'?null:sha(body),
      uid:octal(h.subarray(108,116)),gid:octal(h.subarray(116,124)),mode:octal(h.subarray(100,108))}});
    at+=512+Math.ceil(size/512)*512;
  }
  assert.ok(rows.some(r=>r.file.path==='connector-store.sqlite'));assert.ok(rows.some(r=>r.file.path==='world/world.sqlite'));
  return rows;
}
export async function qualifyOwnAttachmentRestore(seed,output) {
  // All services must already be closed; archive only these two own DBs/directories.
  await mkdir(output,{mode:0o700});
  for(const file of ['connector-store.sqlite','world/world.sqlite']) {
    const db=new DatabaseSync(join(seed,file),{readOnly:true});
    assert.equal(db.prepare('PRAGMA quick_check').get().quick_check,'ok');assert.equal(db.prepare('PRAGMA foreign_key_check').get(),undefined);db.close();
  }
  const tar=process.platform==='win32'?'C:/Windows/System32/tar.exe':'/usr/bin/tar';
  const made=spawnSync(tar,['--format=ustar','-cf','-','.'],{cwd:seed,maxBuffer:64*1024**2,encoding:null});
  assert.equal(made.status,0);const records=tarRecords(made.stdout);
  const files=records.map(r=>r.file).sort((a,b)=>a.path<b.path?-1:a.path>b.path?1:0);
  const inventory={files,stores:[
    {id:'connect',required:true,present:true,format:'soty.connect.fixture',identitySha256:sha('own-connect'),paths:['connector-store.sqlite']},
    {id:'world',required:true,present:true,format:'soty.world.sqlite.v4',identitySha256:sha('own-world'),paths:['world/world.sqlite']},
  ],external:[]};
  const sourceWitness={generationId:'0123456789abcdef0123456789abcdef',checkpointSha256:sha('quiesced-own-chat'),inventorySha256:sha(JSON.stringify(inventory))};
  const restoreManifest={version:1,generationId:sourceWitness.generationId,checkpointSha256:sourceWitness.checkpointSha256,inventory};
  const metadata={offline:true,dataFormat:'tar',original:{State:{Running:false},Mounts:[{Destination:'/data',Type:'volume'}]},secrets:{},restoreFiles:{},restoreManifest};
  const keys=generateKeyPairSync('rsa',{modulusLength:3072}),archive=join(output,'snapshot.enc');
  await encryptBackup({output:archive,publicKey:keys.publicKey.export({type:'spki',format:'pem'}),metadata,stream:Readable.from([made.stdout])});
  const encrypted=await readFile(archive);
  const options={file:archive,privateKeyPem:keys.privateKey.export({type:'pkcs8',format:'pem'}),expectedSha256:sha(encrypted),
    expectedManifestSha256:sha(JSON.stringify(restoreManifest)),sourceWitness,limits:{...LIMITS}};
  const dry=await inspectRestorableBackup(options);assert.equal(dry.inventoryMatched,true);assert.equal(dry.authenticated,true);
  async function receive(input) {
    const chunks=[];let bytes=0;
    const sink=new Writable({highWaterMark:65536,write(chunk,encoding,done){bytes+=chunk.length;assert.ok(bytes<=64*1024**2&&chunk.length<=65536);chunks.push(Buffer.from(chunk));done();}});
    const receipt=await sendAuthenticatedBackup({...input,output:sink});return {receipt,raw:Buffer.concat(chunks),bytes};
  }
  const restored=await receive(options);const metaSize=restored.raw.readUInt32BE(0);
  const entries=tarRecords(restored.raw.subarray(4+metaSize));
  const destination=join(output,'restored');await mkdir(destination,{mode:0o700});
  for(const row of entries) {
    if(row.file.path==='')continue;
    const target=join(destination,...row.file.path.split('/'));
    if(row.file.type==='directory')await mkdir(target,{mode:0o700});
    else {const fd=await open(target,'wx',0o600);try{await fd.writeFile(row.body);await fd.sync();}finally{await fd.close();}}
  }
  for(const row of entries.filter(v=>v.file.type==='file'))assert.equal(sha(await readFile(join(destination,row.file.path))),row.file.sha256);
  const bad=Buffer.from(encrypted);bad[bad.length-1]^=1;const corrupt=join(output,'corrupt.enc');await writeFile(corrupt,bad,{flag:'wx',mode:0o600});
  let writes=0;const never=new Writable({write(chunk,encoding,done){writes++;done();}});
  await assert.rejects(sendAuthenticatedBackup({...options,file:corrupt,expectedSha256:sha(bad),output:never}),error=>error.code==='restore_authentication_failed');assert.equal(writes,0);
  for(const file of ['connector-store.sqlite','world/world.sqlite']) {
    const db=new DatabaseSync(join(destination,file),{readOnly:true});assert.equal(db.prepare('PRAGMA integrity_check').get().integrity_check,'ok');assert.equal(db.prepare('PRAGMA foreign_key_check').get(),undefined);db.close();
  }
  return {directory:destination,receipt:{authenticated:true,readbackVerified:true,corruptArchiveReleasedBytes:0,wholeOwnSeedFiles:files.length,
    plaintextBytes:restored.receipt.plaintextBytes,encryptedBytes:encrypted.length,archiveSha256:options.expectedSha256,
    encryptedSdkLimitBytes:LIMITS.archiveBytes,linuxReceiverQualified:false}};
}
