import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';
import { pngFixture } from '../../modules/world/test/avatar-fixtures.mjs';
import { prepareMaterialUpload, uploadMaterial, readMaterialBlob } from '../../modules/materials/browser/client.mjs';

test('real signed Root: photo, audio and video SDK bytes persist; outsiders and unfinished uploads are denied', {timeout:20000}, async t=>{
  const f=await nativeHttpFixture(t),owner=nativeIdentity('Material owner'),other=nativeIdentity('Other person');
  const ownerId=(await f.bootstrap(owner)).accountId,otherId=(await f.bootstrap(other)).accountId;
  const api={request:async(op,args)=>good(await f.call(owner,op,args))},context={accountId:ownerId,isCurrent:()=>true};
  const fixtures=[
    {kind:'image',name:'Synthetic photo',type:'image/png',bytes:pngFixture()},
    {kind:'audio',name:'Synthetic voice',type:'audio/webm',bytes:readFileSync(new URL('../../modules/materials/test/fixtures/tone.webm',import.meta.url))},
    {kind:'video',name:'Synthetic video',type:'video/webm',bytes:readFileSync(new URL('../../modules/materials/test/fixtures/video.webm',import.meta.url))},
  ];
  const receipts=[];
  for(const item of fixtures){
    const intent=await prepareMaterialUpload(new Blob([item.bytes],{type:item.type}),{kind:item.kind,name:item.name});
    const material=await uploadMaterial(api,context,intent);assert.equal(material.kind,item.kind);receipts.push({material,item});
    const restored=await readMaterialBlob(api,{...context,materialId:material.materialId});
    assert.deepEqual(Buffer.from(await restored.blob.arrayBuffer()),item.bytes);
    assert.equal((await f.call(other,'materials.get',{expectedAccountId:otherId,materialId:material.materialId})).ok,false);
    assert.equal((await f.call(other,'materials.read',{expectedAccountId:otherId,materialId:material.materialId,index:0})).ok,false);
  }
  assert.equal(good(await f.call(other,'materials.list',{expectedAccountId:otherId})).materials.length,0);
  await f.restart();
  for(const {material,item}of receipts){const restored=await readMaterialBlob(api,{...context,materialId:material.materialId});assert.deepEqual(Buffer.from(await restored.blob.arrayBuffer()),item.bytes);}
  assert.equal(good(await api.request('materials.list',{expectedAccountId:ownerId})).materials.length,3);
  const intent=await prepareMaterialUpload(new Blob([pngFixture()],{type:'image/png'}),{kind:'image',name:'Pending photo'});
  good(await f.call(owner,'materials.begin',{expectedAccountId:ownerId,materialId:intent.materialId,mutationId:intent.mutationId,
    name:intent.name,kind:intent.kind,mediaType:intent.mediaType,size:intent.size,fileSha256:intent.fileSha256}));
  assert.equal((await f.call(owner,'materials.read',{expectedAccountId:ownerId,materialId:intent.materialId,index:0})).ok,false);
});

test('real signed Root: note writes and completed materials invalidate only their owner', {timeout:20000},async t=>{
  const f=await nativeHttpFixture(t),owner=nativeIdentity('Owner'),other=nativeIdentity('Other'),ownerId=(await f.bootstrap(owner)).accountId,otherId=(await f.bootstrap(other)).accountId;
  const wait=(actor,accountId,cursor)=>f.call(actor,'platform.live.wait',{expectedAccountId:accountId,waitMs:0,...(cursor?{cursor}:{})});
  let own=good(await wait(owner,ownerId)),outside=good(await wait(other,otherId));
  good(await f.call(owner,'notes.put',{expectedAccountId:ownerId,noteId:'real_change',mutationId:'first_change',expectedRevision:0,title:'Inline note',body:'Real content',items:[],color:'plain',pinned:false,state:'active'}));
  own=good(await wait(owner,ownerId,own.cursor));assert.equal(own.changed,true);assert.equal(good(await wait(other,otherId,outside.cursor)).changed,false);
  const api={request:async(op,args)=>good(await f.call(owner,op,args))},context={accountId:ownerId,isCurrent:()=>true};
  const intent=await prepareMaterialUpload(new Blob([pngFixture()],{type:'image/png'}),{kind:'image',name:'Live photo'});
  await uploadMaterial(api,context,intent);assert.equal(good(await wait(owner,ownerId,own.cursor)).changed,true);assert.equal(good(await wait(other,otherId,outside.cursor)).changed,false);
});
