import assert from 'node:assert/strict';
import { createHash, generateKeyPairSync, sign } from 'node:crypto';
import { createServer } from 'node:http';
import { mkdtemp, mkdir, rm, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createConnectService, digestArgs } from '../../../connect/server/index.mjs';
import { createConnectHandler } from '../../../connect/server/http.mjs';
import { createWorldService } from '../../server/index.mjs';
import { createWorldService as frozen3 } from './world3-frozen/modules/world/server/index.mjs';
import { migrateChatAttachments, ATTACHMENT_LIMITS as L } from '../../server/attachments.mjs';
import { attachmentManifest } from '../../attachment-contract.mjs';
import { qualifyOwnAttachmentRestore } from './attachment-restore.mjs';
const sha=b=>createHash('sha256').update(b).digest('hex'),ORIGIN='https://m02-own.test',PROJECT='m02-attachments-own';
const good=r=>{assert.equal(r.ok,true,r.error?.code);return r;};
const denied=(r,code)=>{assert.equal(r.ok,false);assert.equal(r.error.code,code);assert.ok(!r.data&&!r.attachment);};
function identity(label){const a=generateKeyPairSync('ec',{namedCurve:'prime256v1'}),b=generateKeyPairSync('ec',{namedCurve:'prime256v1'});return {label,key:a.privateKey,publicJwk:a.publicKey.export({format:'jwk'}),encryptionPublicJwk:b.publicKey.export({format:'jwk'})};}
export async function attachmentHttpFixture(t) {
  const root=await mkdtemp(join(tmpdir(),'soty-attachment-own-')),seed=join(root,'seed');await mkdir(join(seed,'world'),{recursive:true});await mkdir(join(seed,'rooms'));
  let now=1800000000000,world,connect,server;const worldPath=join(seed,'world/world.sqlite'),connectPath=join(seed,'connector-store.sqlite');
  const options={databasePath:worldPath,projectId:PROJECT,clock:()=>now};
  const start=writes=>{world=createWorldService({...options,attachmentWrites:writes});connect=createConnectService({databasePath:connectPath,projectId:PROJECT,allowedOrigins:[ORIGIN],clock:()=>now,extensions:[world]});};
  const stop=()=>{connect?.close();world?.close();connect=world=null;};
  t.after(async()=>{if(server)await new Promise(r=>server.close(r));stop();assert.equal(dirname(root),resolve(tmpdir()));assert.match(root.split(/[\\/]/u).at(-1),/^soty-attachment-own-/u);await rm(root,{recursive:true,force:true});});
  let send=input=>connect.handle({...input,origin:ORIGIN});
  const call=async(actor,op,args={})=>{
    const challenge=good(await send({op:'challenge',args:{operation:op,digest:digestArgs(args)}}));
    return send({op,args,proof:{challengeId:challenge.challengeId,publicJwk:actor.publicJwk,signature:sign('sha256',Buffer.from(challenge.message),{key:actor.key,dsaEncoding:'ieee-p1363'}).toString('base64url')}});
  };
  start(false);const actors=[identity('Аня'),identity('Борис'),identity('Вера')];
  for(const actor of actors){actor.accountId=good(await call(actor,'bootstrap',{label:actor.label,encryptionPublicJwk:actor.encryptionPublicJwk})).accountId;good(await call(actor,'world.profile.get'));}
  const group=good(await call(actors[0],'world.community.create',{requestId:'before_migration_group',name:'Свои файлы',joinPolicy:'open'})).community;
  for(const actor of actors.slice(1))good(await call(actor,'world.membership.join',{communityId:group.communityId}));
  good(await call(actors[0],'world.chat.send',{communityId:group.communityId,clientId:'legacy_message_before_migration',text:'До файлов'}));
  assert.equal(world.schemaVersion,3);stop();
  const legacy=frozen3(options);assert.equal(legacy.execute({actor:{accountId:actors[0].accountId,deviceId:'legacy-own-device',label:'legacy'},op:'world.chat.list',args:{communityId:group.communityId}}).messages[0].text,'До файлов');legacy.close();
  assert.throws(()=>createWorldService({...options,attachmentWrites:true}),e=>e.code==='attachment_migration_required');
  const db=new DatabaseSync(worldPath);const before=db.prepare('SELECT * FROM messages').all();assert.deepEqual(migrateChatAttachments(db,PROJECT),{from:3,to:4,writerEnabled:false});db.close();
  const migrated=await qualifyOwnAttachmentRestore(seed,join(root,'migration-restore'));
  const reader=createWorldService({...options,databasePath:join(migrated.directory,'world/world.sqlite')});
  assert.equal(reader.schemaVersion,4);const own={accountId:actors[0].accountId,deviceId:'restored-own-device',label:'restored'};
  assert.equal(reader.execute({actor:own,op:'world.chat.list',args:{communityId:group.communityId,readerEpoch:4}}).messages[0].text,'До файлов');reader.close();
  const check=new DatabaseSync(worldPath,{readOnly:true});assert.deepEqual(check.prepare('SELECT * FROM messages').all(),before);check.close();
  assert.throws(()=>frozen3(options),/world_schema_unsupported/u);
  // Only now enable writes, on an OWN DB whose explicit migration+diskrestore passed.
  start(true);const handler=createConnectHandler({handle:input=>connect.handle(input)});
  server=createServer(async(req,res)=>{if(!await handler(req,res)){res.statusCode=404;res.end();}});
  await new Promise(r=>server.listen(0,'127.0.0.1',r));const endpoint=`http://127.0.0.1:${server.address().port}/api/connect/rpc`;
  send=async input=>{const response=await fetch(endpoint,{method:'POST',headers:{'content-type':'application/json',origin:ORIGIN},body:JSON.stringify({protocol:1,...input})});assert.equal(response.headers.get('cache-control'),'no-store');return response.json();};
  const base=(actor,communityId=group.communityId)=>({communityId,expectedAccountId:actor.accountId});
  let sequence=0;
  const upload=async(actor,bytes,name='отчёт.bin')=>{
    const attachmentId=`own_attachment_${++sequence}`,args={...base(actor),attachmentId};
    good(await call(actor,'world.attachment.begin',{...args,name,size:bytes.length,mediaType:'application/octet-stream'}));
    const chunks=[];
    for(let offset=0,index=0;offset<bytes.length;offset+=L.chunkBytes,index++){
      const part=bytes.subarray(offset,offset+L.chunkBytes);chunks.push({bytes:part.length,sha256:sha(part)});
      good(await call(actor,'world.attachment.chunk',{...args,index,sha256:sha(part),data:part.toString('base64')}));
    }
    return good(await call(actor,'world.attachment.complete',{...args,manifestSha256:sha(attachmentManifest(bytes.length,chunks))})).attachment;
  };
  return {root,seed,options,actors,group,call,base,upload,migrated,stop,worldPath,
    advance:delta=>{now+=delta;},async restore(){stop();return qualifyOwnAttachmentRestore(seed,join(root,'published-restore'));},
    readChunks:async(actor,ref)=>{const chunks=[];for(let index=0;index<ref.chunks;index++){const part=good(await call(actor,'world.attachment.read',{...base(actor),attachmentId:ref.attachmentId,index}));assert.equal(sha(Buffer.from(part.data,'base64')),part.sha256);chunks.push(Buffer.from(part.data,'base64'));}return Buffer.concat(chunks);}};
}
