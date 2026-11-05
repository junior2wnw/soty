import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash,randomUUID} from 'node:crypto';
import {createHttpApp} from '../../server/http-app.js';
import {nativeHttpFixture,nativeIdentity,good} from '../../server/test/support/native-capability-http.mjs';
import {createCanvasVaultClient} from './canvas-vault-client.mjs';
import {createCanvasVaultLocalStore,CANVAS_VAULT_LOCAL_LIMITS} from './canvas-vault-local.mjs';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
import {readCanvasVaultContent,CANVAS_VAULT_READ_LIMITS} from './canvas-vault-bulk-reader.mjs';
import {vaultClientVariant} from './test-support/vault-bulk-client-variant.mjs';

test('actual signed Root50-part Source fails the old1MiB reader and passes the new bounded25MiB reader with an exact file hash',{timeout:60000},async t=>{
  const f=await nativeHttpFixture(t,{appFactory:(dist,options)=>createHttpApp(dist,{...options,allowNativeVaultMigration:true})}),owner=nativeIdentity('Synthetic bulk reader owner'),accountId=(await f.bootstrap(owner)).accountId;
  const call=async(op,args={})=>good(await f.call(owner,op,{expectedAccountId:accountId,...args})),size=25*1024*1024,password='Synthetic bulk reader password 6189';
  const part=index=>{const bytes=Buffer.alloc(512*1024,index+1);Buffer.from('Synthetic exact bounded part '+index).copy(bytes);return bytes;};
  const begin=await call('canvas.vault.upload.begin',{requestId:randomUUID(),password,contentType:'application/octet-stream',size,metadata:{kind:'file',name:'synthetic25MiB.bin'}}),vaultId=begin.current.vaultId;assert.equal(begin.upload.chunks,50);
  for(let index=0;index<50;index++)await call('canvas.vault.upload.chunk',{vaultId,index,bytesBase64:part(index).toString('base64url')});
  await call('canvas.vault.upload.complete',{requestId:randomUUID(),vaultId});
  const object=(await call('canvas.objects.create',{requestId:randomUUID(),binding:{providerId:'soty.vault',ref:vaultId},presentation:{mask:{shape:'rect'}},visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'password'}})).current,objectId=object.objectId;
  const legacy=await vaultClientVariant({mode:'legacy'});let current=true;const local=createCanvasVaultLocalStore(accountId,'soty',{origin:f.origin,indexedDB:indexedNativePort().indexedDB}),client=createCanvasVaultClient({api:{request:call},accountId,projectId:'soty',isCurrent:()=>current,store:local}),old=legacy.create({api:{request:call},accountId,projectId:'soty',isCurrent:()=>current,store:local});
  t.after(()=>{current=false;client.dispose();old.dispose();local.close();legacy.dispose();});await old.unlock(objectId,password);await assert.rejects(old.readContent(objectId),error=>error.code==='vault_wire_size','The unchanged old reader must reproduce the original post-protect failure');
  await client.unlock(objectId,password);const capturedEpoch=client.generation(objectId);
  let reads=0;const result=await readCanvasVaultContent({objectId,isCurrent:()=>current&&client.isUnlocked(objectId)&&client.generation(objectId)===capturedEpoch,expected:{revision:1,contentType:'application/octet-stream',byteLength:size},
    readPart:async request=>{assert.equal(request.objectId,objectId);assert.equal(request.index,reads++);const value=await call('canvas.objects.content',request);assert.equal(Object.hasOwn(value,'objectId'),false,'Current Root DTO binds object identity through the request/current Object context');assert.ok(value.bytesBase64.length<700000,'Each signed RPC remains bounded');return value;}});
  const expected=createHash('sha256');for(let index=0;index<50;index++)expected.update(part(index));const actual=createHash('sha256').update(result.bytes).digest('hex');assert.equal(result.bytes.length,size);assert.equal(actual,expected.digest('hex'));assert.equal(reads,50);assert.equal(result.revision,1);result.bytes.fill(0);
  const integrated=await client.readContent(objectId);assert.equal(integrated.bytes.length,size);assert.equal(createHash('sha256').update(integrated.bytes).digest('hex'),actual,'The canonical SDK itself must use the bounded reader, not only an isolated helper');integrated.bytes.fill(0);
  assert.equal(CANVAS_VAULT_LOCAL_LIMITS.payloadBytes,1024*1024);assert.equal(CANVAS_VAULT_READ_LIMITS.bytes,size);
  const args={expectedAccountId:accountId,requestId:randomUUID(),password,content:{contentType:'application/octet-stream',bytesBase64:Buffer.alloc(1700000).toString('base64url')}},proof=await f.proof(owner,'canvas.vault.create',args),body=JSON.stringify({protocol:1,...proof}),response=await f.http('/api/connect/rpc',{method:'POST',headers:{origin:f.origin,'content-length':Buffer.byteLength(body)},body});
  assert.equal(response.status,413,'No global RPC increase is needed or allowed for bulk reading');assert.equal(response.body.ok,false);
  t.diagnostic(`Root parts=${reads}, decodedBytes=${size}, sha256=${actual}; synthetic temporary fixture only`);
});
