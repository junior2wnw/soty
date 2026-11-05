import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync,readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve,dirname} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import sharp from 'sharp';
import {createMaterialsService} from '../server/index.mjs';
import {prepareMaterialUpload,uploadMaterial} from '../browser/client.mjs';
import {createCanvasObjectsService} from '../../canvas-objects/server.mjs';
const owner={accountId:'account_material_owner',deviceId:'device_material_owner'},viewer={accountId:'account_material_viewer',deviceId:'device_material_viewer'};
async function fixture(t,createMaterials=createMaterialsService) {
  const base=resolve(tmpdir()),directory=mkdtempSync(join(base,'soty-materials-authority-')),databasePath=join(directory,'materials.sqlite'),active=new Set([owner.deviceId,viewer.deviceId]);let objects,leaked,hook;
  const materials=createMaterials({databasePath,projectId:'object_materials',verifyObjectAuthority:(context,consume)=>{
    if(hook)hook(context);return objects.withObjectAuthority(context,proof=>consume({providerId:proof.providerId,ownerId:proof.ownerId,ref:proof.ref,right:proof.right}));
  }});
  const provider={providerId:'soty.materials',withBindingAuthority(request,callback){return materials.native.withSourceAuthority({actor:request.actor,materialId:request.ref,...(request.objectOwnerId?{ownerId:request.objectOwnerId}:{}),mode:request.mode},callback);},project(context){leaked=context;return {schema:'test.material.v1',...materials.native.readMetadata({context}),...materials.native.readChunk({context,index:0})};}};
  objects=createCanvasObjectsService({databasePath:join(directory,'objects.sqlite'),projectId:'object_materials',actorActive:actor=>active.has(actor.deviceId),withActorAuthorityFence:(_actor,callback)=>callback(),providers:[provider]});
  t.after(async()=>{objects.close();await materials.close();assert.equal(dirname(resolve(directory)),base);assert.ok(directory.startsWith(join(base,'soty-materials-authority-')));rmSync(directory,{recursive:true,force:true});});
  const api={request:(op,args)=>materials.execute({op,args,actor:owner})};
  const bytes=await sharp({create:{width:2,height:2,channels:3,background:'purple'}}).png().toBuffer(),intent=await prepareMaterialUpload(new Blob([bytes],{type:'image/png'}),{kind:'image',name:'Synthetic.png'}),material=await uploadMaterial(api,{accountId:owner.accountId},intent);
  const call=(op,args,actor=owner)=>objects.execute({op:`canvas.objects.${op}`,args:{expectedAccountId:actor.accountId,...args},actor});
  const create=(extra={})=>call('create',{requestId:crypto.randomUUID(),binding:{providerId:'soty.materials',ref:material.materialId},presentation:{mask:{shape:'rect'}},visibility:{audience:'public',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'none'},...extra}).current;
  return {materials,objects,bytes,material,call,create,active,databasePath,get context(){return leaked;},hook:fn=>{hook=fn;}};
}
test('real ObjectService branded frame permits public material bytes without a fake owner actor; expired/copied contexts and old owner RPC remain closed',async t=>{
  const f=await fixture(t),object=f.create(),result=f.objects.project({actor:viewer,objectIds:[object.objectId]});
  assert.equal(result.items[0].available,true);assert.equal(result.items[0].view.data,f.bytes.toString('base64'));assert.equal(result.items[0].view.material.materialId,f.material.materialId);
  await assert.rejects(f.materials.execute({op:'materials.read',args:{expectedAccountId:viewer.accountId,materialId:f.material.materialId,index:0},actor:viewer}),error=>error.code==='materials_material_not_found');
  for(const context of [f.context,{...f.context},JSON.parse(JSON.stringify(f.context))])assert.throws(()=>f.materials.native.readChunk({context,index:0}),error=>error.code==='canvas_object_authority_required');
  await assert.rejects(f.materials.execute({op:'materials.native.readChunk',args:{expectedAccountId:owner.accountId,context:f.context,index:0},actor:owner}),error=>error.code==='unsupported_operation');
});
test('private/hidden/revoked source or object cannot borrow a prior public projection to read bytes',async t=>{
  const f=await fixture(t),object=f.create();f.objects.project({actor:viewer,objectIds:[object.objectId]});const context=f.context;
  f.call('update',{objectId:object.objectId,requestId:crypto.randomUUID(),expectedRevision:1,visibility:{audience:'private',accountIds:[],communityIds:[]}});
  assert.equal(f.objects.project({actor:viewer,objectIds:[object.objectId]}).items[0].available,false);
  assert.throws(()=>f.materials.native.readMetadata({context}),error=>error.code==='canvas_object_authority_required');
  const second=f.create();f.active.delete(viewer.deviceId);assert.throws(()=>f.objects.project({actor:viewer,objectIds:[second.objectId]}),error=>error.code==='canvas_authentication_required');
});
test('source binding create is own-active only; no password seal or asynchronous callback is invented',async t=>{
  const f=await fixture(t);
  assert.throws(()=>f.materials.native.withSourceAuthority({actor:viewer,materialId:f.material.materialId,ownerId:owner.accountId,mode:'create'},()=>true),error=>error.code==='materials_owner_required');
  assert.throws(()=>f.materials.native.withSourceAuthority({actor:owner,materialId:f.material.materialId,mode:'create'},async()=>true),error=>error.code==='materials_async_authority');
  assert.throws(()=>f.create({protection:{kind:'password'}}),error=>error.code==='canvas_plaintext_password_forbidden');
  const sql=new DatabaseSync(f.databasePath);sql.prepare("UPDATE materials SET state='trashed',revision=revision+1 WHERE owner_id=? AND id=?").run(owner.accountId,f.material.materialId);sql.close();
  assert.throws(()=>f.materials.native.withSourceAuthority({actor:owner,materialId:f.material.materialId,mode:'create'},()=>true),error=>error.code==='materials_material_not_found');
});
test('a source changed inside a synchronous callback is detected before its result is returned',async t=>{
  const f=await fixture(t);
  assert.throws(()=>f.materials.native.withSourceAuthority({actor:owner,materialId:f.material.materialId,mode:'create'},()=>{
    const db=new DatabaseSync(f.databasePath);db.prepare("UPDATE materials SET state='trashed',revision=revision+1 WHERE owner_id=? AND id=?").run(owner.accountId,f.material.materialId);db.close();return 'must not escape';
  }),error=>error.code==='materials_source_changed');
});
test('native reads fail closed without a verifier and require a fresh current viewer check before returning bytes',async t=>{
  const noAuthority=createMaterialsService({databasePath:':memory:',projectId:'closed_port'});try{assert.throws(()=>noAuthority.native.readMetadata({context:{ownerId:owner.accountId,ref:'material_forged'}}),error=>error.code==='materials_object_authority_required');}finally{await noAuthority.close();}
  const f=await fixture(t),object=f.create();let checks=0;
  f.hook(()=>{if(++checks===3)f.active.delete(viewer.deviceId);});
  assert.throws(()=>f.objects.project({actor:viewer,objectIds:[object.objectId]}),error=>error.code==='canvas_authentication_required');
  assert.ok(checks>=3);
});
test('negative control detects a port that trusts JSON authority claims instead of the live branded context',async t=>{
  const url=new URL('../server/index.mjs',import.meta.url),source=readFileSync(url,'utf8'),changed=source.replace('function objectScope(context) {','function objectScope(context) { return {ownerId:context.ownerId,materialId:context.ref};');assert.notEqual(changed,source);
  const absolute=changed.replace(/from '(\.\.?\/[^']+)'/gu,(_match,path)=>`from '${new URL(path,url).href}'`).replaceAll('import.meta.url',JSON.stringify(url.href));
  const broken=await import(`data:text/javascript;base64,${Buffer.from(absolute).toString('base64')}`),f=await fixture(t,broken.createMaterialsService),object=f.create();f.objects.project({actor:viewer,objectIds:[object.objectId]});
  const shouldDeny=()=>assert.throws(()=>f.materials.native.readChunk({context:{...f.context},index:0}),error=>error.code==='canvas_object_authority_required');
  assert.throws(shouldDeny,error=>error.code==='ERR_ASSERTION');
});
