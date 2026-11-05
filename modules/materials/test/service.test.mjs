import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync,readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve,dirname} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {createHash} from 'node:crypto';
import sharp from 'sharp';
import {createMaterialsService} from '../server/index.mjs';
import {MATERIAL_LIMITS,materialManifest} from '../contract.mjs';
import {prepareMaterialUpload,uploadMaterial,readMaterialBlob} from '../browser/client.mjs';
const alice={accountId:'account_alice',deviceId:'device_alice'},bob={accountId:'account_bobby',deviceId:'device_bobby'};
const sha=value=>createHash('sha256').update(value).digest('hex');
const fail=(promise,code)=>assert.rejects(promise,error=>error.code===code);
let serial=0;
async function fixture(t,options={}) {
  const base=resolve(tmpdir()),directory=mkdtempSync(join(base,'soty-materials-test-')),databasePath=join(directory,'materials.sqlite');let now=1000;
  const service=createMaterialsService({databasePath,projectId:'materials-test',clock:()=>now++,...options});
  t.after(async()=>{await service.close();assert.equal(dirname(resolve(directory)),base);assert.ok(directory.startsWith(join(base,'soty-materials-test-')));rmSync(directory,{recursive:true,force:true});});
  const call=(op,args={},actor=alice,extra={})=>service.execute({op:`materials.${op}`,args:{expectedAccountId:actor.accountId,...args},actor,...extra});
  const api={request:(method,args)=>service.execute({op:method,args,actor:alice})};
  return {service,call,api,databasePath,clock:value=>{now=value;}};
}
async function png(width=3,height=2) {return sharp({create:{width,height,channels:3,background:'#45a4c1'}}).png().toBuffer();}
function uploadArgs(bytes,overrides={}) {
  const materialId=`material_${++serial}`,chunks=[];
  for(let at=0;at<bytes.length;at+=MATERIAL_LIMITS.chunkBytes){const part=bytes.subarray(at,at+MATERIAL_LIMITS.chunkBytes);chunks.push({bytes:part.length,sha256:sha(part)});}
  return {materialId,mutationId:`begin_${materialId}`,name:'Фото.png',kind:'image',mediaType:'image/png',size:bytes.length,fileSha256:sha(bytes),...overrides,chunks,manifestSha256:sha(materialManifest(bytes.length,chunks))};
}
async function stage(call,bytes,overrides={}) {
  const input=uploadArgs(bytes,overrides),{chunks,manifestSha256,...begin}=input;await call('begin',begin);
  for(let index=0;index<chunks.length;index++)await call('chunk',{materialId:input.materialId,index,sha256:chunks[index].sha256,data:bytes.subarray(index*MATERIAL_LIMITS.chunkBytes,(index+1)*MATERIAL_LIMITS.chunkBytes).toString('base64')});
  return {...input,commit:{materialId:input.materialId,mutationId:`commit_${input.materialId}`,manifestSha256}};
}

test('pending bytes have no content read; durable commit is owner scoped and survives reopening',async t=>{
  const {call,service,databasePath}=await fixture(t),bytes=await png(),input=await stage(call,bytes);
  assert.deepEqual((await call('list')).materials,[]);
  await fail(call('get',{materialId:input.materialId}),'materials_material_not_found');
  await fail(call('read',{materialId:input.materialId,index:0}),'materials_material_not_found');
  const committed=await call('commit',input.commit);assert.equal(committed.material.width,3);assert.equal(committed.material.height,2);
  assert.equal((await call('list')).materials.length,1);assert.equal((await call('read',{materialId:input.materialId,index:0})).data,bytes.toString('base64'));
  assert.deepEqual((await call('list',{},bob)).materials,[]);
  for(const op of ['get','status','read','trash','restore','cancel'])await fail(call(op,{materialId:input.materialId,...(op==='read'?{index:0}:['trash','restore','cancel'].includes(op)?{mutationId:'foreign_mutation',expectedRevision:1}:{})},bob),'materials_material_not_found');
  await fail(call('list',{expectedAccountId:alice.accountId},bob),'materials_account_changed');
  await fail(call('list',{accountId:bob.accountId}),'materials_invalid_arguments');
  await service.close();const reopened=createMaterialsService({databasePath,projectId:'materials-test'});
  try {const after=await reopened.execute({op:'materials.get',args:{expectedAccountId:alice.accountId,materialId:input.materialId},actor:alice});assert.deepEqual(after,committed);assert.equal((await reopened.execute({op:'materials.commit',args:{expectedAccountId:alice.accountId,...input.commit},actor:alice})).replayed,true);}
  finally {await reopened.close();}
});
test('all mutation and chunk retries are idempotent; altered payload and stale state are rejected',async t=>{
  const {call}=await fixture(t),bytes=await png(),input=await stage(call,bytes),{chunks,manifestSha256,commit,...begin}=input;
  assert.equal((await call('begin',begin)).replayed,true);
  await fail(call('begin',{...begin,name:'Другое.png'}),'materials_mutation_reused');
  assert.equal((await call('chunk',{materialId:input.materialId,index:0,sha256:sha(bytes),data:bytes.toString('base64')})).replayed,true);
  const wrong=Buffer.from(bytes);wrong[wrong.length-1]^=1;
  await fail(call('chunk',{materialId:input.materialId,index:0,sha256:sha(wrong),data:wrong.toString('base64')}),'materials_chunk_reused');
  await call('commit',input.commit);assert.equal((await call('commit',input.commit)).replayed,true);
  await fail(call('commit',{...input.commit,manifestSha256:'0'.repeat(64)}),'materials_mutation_reused');
  const trash={materialId:input.materialId,mutationId:'trash_material',expectedRevision:1};assert.equal((await call('trash',trash)).revision,2);assert.equal((await call('trash',trash)).replayed,true);
  await fail(call('read',{materialId:input.materialId,index:0}),'materials_material_not_found');
  assert.equal((await call('list',{bucket:'trashed'})).materials.length,1);assert.equal((await call('list')).materials.length,0);
  await fail(call('restore',{materialId:input.materialId,mutationId:'restore_stale',expectedRevision:1}),'materials_revision_conflict');
  assert.equal((await call('restore',{materialId:input.materialId,mutationId:'restore_material',expectedRevision:2})).revision,3);
  assert.equal((await call('read',{materialId:input.materialId,index:0})).data,bytes.toString('base64'));
});
test('tampered/incomplete data, forged MIME, SVG, truncated image and decoded image bounds never commit',async t=>{
  const {call}=await fixture(t),bytes=await png();const args=uploadArgs(bytes);const {chunks,manifestSha256,...begin}=args;await call('begin',begin);
  await fail(call('chunk',{materialId:args.materialId,index:0,sha256:sha(bytes),data:bytes.toString('base64')+'\n'}),'materials_chunk_invalid');
  await fail(call('commit',{materialId:args.materialId,mutationId:'commit_incomplete',manifestSha256}),'materials_upload_incomplete');
  for(const [file,overrides,code] of [[bytes,{mediaType:'image/jpeg'},'materials_image_bounds'],[bytes.subarray(0,40),{},'materials_media_invalid'],[Buffer.from('<svg><script/></svg>'),{},'materials_media_invalid']]) {
    const input=await stage(call,file,overrides);await fail(call('commit',input.commit),code);
  }
  await fail(call('begin',{...begin,materialId:'material_svg_1',mutationId:'mutation_svg_1',mediaType:'image/svg+xml'}),'materials_media_type');
  const huge=await sharp({create:{width:8193,height:1,channels:3,background:'red'}}).png().toBuffer();const oversized=await stage(call,huge);await fail(call('commit',oversized.commit),'materials_image_bounds');
  assert.equal((await call('list')).materials.length,0);
});
test('file and manifest integrity are independent of chunk hashes',async t=>{
  const {call}=await fixture(t),bytes=await png();
  const wrongFile=await stage(call,bytes,{fileSha256:'0'.repeat(64)});await fail(call('commit',wrongFile.commit),'materials_file_hash');
  const wrongManifest=await stage(call,bytes);await fail(call('commit',{...wrongManifest.commit,manifestSha256:'0'.repeat(64)}),'materials_manifest_hash');
  await call('commit',wrongManifest.commit);assert.equal((await call('list')).materials.length,1);
});
test('quota reservations include pending and trash; cancellation cannot resurrect an identity',async t=>{
  const bytes=await png(),{call}=await fixture(t,{limits:{accountBytes:bytes.length*2,pending:1}}),first=await stage(call,bytes);
  const next=uploadArgs(bytes),{chunks,manifestSha256,...begin}=next;
  await fail(call('begin',begin),'materials_pending_quota');await call('commit',first.commit);
  await call('trash',{materialId:first.materialId,mutationId:'trash_reservation',expectedRevision:1});
  await call('begin',begin);
  await fail(call('begin',{...begin,materialId:'material_third',mutationId:'begin_third'}),'materials_storage_quota');
  assert.equal((await call('cancel',{materialId:next.materialId,mutationId:'cancel_upload',expectedRevision:0})).state,'cancelled');
  await fail(call('begin',{...begin,mutationId:'begin_resurrect'}),'materials_upload_closed');
});
test('expiry releases reservation; revocation and SQL failure leave no visible half committed data',async t=>{
  const {call,clock,databasePath}=await fixture(t,{limits:{pending:1}}),bytes=await png(),first=await stage(call,bytes);clock(1000+MATERIAL_LIMITS.pendingMs+100);
  const second=await stage(call,bytes);assert.equal((await call('status',{materialId:first.materialId})).state,'expired');
  let calls=0;await fail(call('commit',second.commit,alice,{guard:()=>++calls<3}),'authentication_required');assert.equal((await call('list')).materials.length,0);
  const db=new DatabaseSync(databasePath);db.exec("CREATE TRIGGER material_test_fail BEFORE UPDATE OF state ON materials WHEN NEW.state='active' BEGIN SELECT RAISE(ABORT,'forced_commit_failure'); END");
  await assert.rejects(call('commit',second.commit),/forced_commit_failure/u);assert.equal((await call('list')).materials.length,0);assert.equal((await call('status',{materialId:second.materialId})).state,'pending');
  db.exec('DROP TRIGGER material_test_fail');db.close();await call('commit',second.commit);assert.equal((await call('list')).materials.length,1);
});
test('read detects stored corruption; project and schema mismatches are rejected without takeover',async t=>{
  const {call,databasePath}=await fixture(t),bytes=await png(),input=await stage(call,bytes);await call('commit',input.commit);
  const db=new DatabaseSync(databasePath);db.prepare('UPDATE material_chunks SET bytes=? WHERE owner_id=? AND material_id=?').run(Buffer.alloc(bytes.length),alice.accountId,input.materialId);db.close();
  await fail(call('read',{materialId:input.materialId,index:0}),'materials_storage_corrupt');
  assert.throws(()=>createMaterialsService({databasePath,projectId:'other-project'}),error=>error.code==='materials_project_mismatch');
});
test('browser SDK uploads real bytes, retries unknown ACK, verifies reload read and rejects forged response',async t=>{
  const {api,call}=await fixture(t),bytes=await png(),intent=await prepareMaterialUpload(new Blob([bytes],{type:'image/png'}),{kind:'image',name:'../Photo.png'});
  let lost=true;const flaky={request:async(method,args)=>{const result=await api.request(method,args);if(method==='materials.commit'&&lost){lost=false;throw Error('network_lost_ack');}return result;}};
  await assert.rejects(uploadMaterial(flaky,{accountId:alice.accountId},intent),/network_lost_ack/u);
  const material=await uploadMaterial(api,{accountId:alice.accountId},intent);assert.equal((await call('list')).materials.length,1);assert.equal(material.name,'Photo.png');
  const restored=await readMaterialBlob(api,{accountId:alice.accountId,materialId:material.materialId});assert.deepEqual(Buffer.from(await restored.blob.arrayBuffer()),bytes);
  const forged={request:async(method,args)=>{const result=await api.request(method,args);return method==='materials.read'?{...result,data:Buffer.alloc(bytes.length).toString('base64')}:result;}};
  await fail(readMaterialBlob(forged,{accountId:alice.accountId,materialId:material.materialId}),'materials_chunk_invalid');
  let current=true;const switched={request:async(method,args)=>{const result=await api.request(method,args);current=false;return result;}};
  await fail(readMaterialBlob(switched,{accountId:alice.accountId,materialId:material.materialId,isCurrent:()=>current}),'materials_account_changed');
});
test('the same durable upload path stores and reloads scoped video WebM, including codec-qualified MIME',async t=>{
  const {api,call}=await fixture(t),bytes=readFileSync(new URL('./fixtures/video.webm',import.meta.url)),intent=await prepareMaterialUpload(new Blob([bytes],{type:'video/webm; codecs="vp8,opus"'}),{kind:'video',name:'Test video.webm'});
  const material=await uploadMaterial(api,{accountId:alice.accountId},intent);assert.equal(material.kind,'video');assert.equal(material.width,96);assert.equal(material.height,64);
  const loaded=await readMaterialBlob(api,{accountId:alice.accountId,materialId:material.materialId});assert.deepEqual(Buffer.from(await loaded.blob.arrayBuffer()),bytes);assert.equal((await call('list')).materials.length,1);
});
test('negative controls prove the owner boundary and no-pending-read checks can catch a broken implementation',async()=>{
  const url=new URL('../server/index.mjs',import.meta.url),source=readFileSync(url,'utf8');
  const imports=source.replace(/from '(\.\.?\/[^']+)'/gu,(_match,path)=>`from '${new URL(path,url).href}'`).replaceAll('import.meta.url',JSON.stringify(url.href));
  const changed=imports.replace("const rowFor=(owner,materialId)=>get('SELECT * FROM materials WHERE owner_id=? AND id=?',owner,materialId);","const rowFor=(owner,materialId)=>get('SELECT * FROM materials WHERE id=?',materialId);");
  assert.notEqual(changed,imports);
  const module=await import(`data:text/javascript;base64,${Buffer.from(changed).toString('base64')}`),service=module.createMaterialsService({databasePath:':memory:',projectId:'negative-control'});
  const call=(op,args={},actor=alice)=>service.execute({op:`materials.${op}`,args:{expectedAccountId:actor.accountId,...args},actor});
  try {const item=await stage(call,await png());await call('commit',item.commit);
    await assert.rejects(async()=>{await fail(call('get',{materialId:item.materialId},bob),'materials_material_not_found');},error=>error.code==='ERR_ASSERTION');
  }finally{await service.close();}
  const pendingMutant=imports.replace("check(row&&['active','trashed'].includes(row.state),'materials_material_not_found');","check(row,'materials_material_not_found');");assert.notEqual(pendingMutant,imports);
  const pendingModule=await import(`data:text/javascript;base64,${Buffer.from(pendingMutant).toString('base64')}`),pending=pendingModule.createMaterialsService({databasePath:':memory:',projectId:'negative-pending'});
  const pendingCall=(op,args={})=>pending.execute({op:`materials.${op}`,args:{expectedAccountId:alice.accountId,...args},actor:alice});
  try {const item=await stage(pendingCall,await png());await assert.rejects(async()=>{await fail(pendingCall('get',{materialId:item.materialId}),'materials_material_not_found');},error=>error.code==='ERR_ASSERTION');}
  finally{await pending.close();}
});
