import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createCanvasObjectsService} from './server.mjs';
import {CanvasObjectError} from './contract.mjs';

const owner={accountId:'representation_owner',deviceId:'owner_device'},other={accountId:'representation_other',deviceId:'other_device'};
const app={kind:'app',id:'app-'+'a'.repeat(32)};
function fixture(t,{factory=createCanvasObjectsService}={}){
 const calls=[],issued=new WeakSet([owner,other]),active=new Set([owner.deviceId,other.deviceId]);let serial=0,service,onRelation;
 const provider=(providerId,ownerRepresents)=>({providerId,...(ownerRepresents===undefined?{}:{ownerRepresents}),
  withBindingAuthority(request,callback){calls.push('source-authority');return callback({allowed:true,protection:request.ref==='sealed'?{kind:'password',sealed:true}:{kind:'none'}});},
  project(){calls.push('content-project');return{kind:'fixture',title:'fresh source'};}});
 const widget=provider('fixture.widget',request=>{calls.push('owner-relation');assert.equal(request.ownerId,owner.accountId);return onRelation?onRelation(request):[app];});
 service=factory({databasePath:':memory:',projectId:'representation_test',actorActive:actor=>active.has(actor.deviceId),
  withActorAuthorityFence:(actor,callback)=>{assert.ok(issued.has(actor),'only original issued actor enters the transport fence');return callback();},
  withProtectionAuthority:({object},callback)=>callback({kind:object.protection.kind,unlocked:false}),providers:[widget,provider('fixture.native',[]),provider('fixture.unknown')]});
 t.after(()=>service.close());
 const command=(op,args)=>service.execute({actor:owner,op:'canvas.objects.'+op,args:{expectedAccountId:owner.accountId,requestId:'cmd_'+(++serial),...args}});
 const create=({providerId='fixture.widget',locked=false,hidden=false,visibility='private'}={})=>command('create',{binding:{providerId,ref:locked?'sealed':'plain'},presentation:{mask:{shape:'rect'}},visibility:{audience:visibility,accountIds:[],communityIds:[]},hidden:{owner:hidden,others:false},protection:{kind:locked?'password':'none'}}).current;
 return{service,calls,create,command,active,setRelation(callback){onRelation=callback;}};
}

test('batch owns representation metadata alongside content and declares Native non-app without extra source reads',t=>{
 const f=fixture(t),widget=f.create(),native=f.create({providerId:'fixture.native'});f.calls.length=0;
 const items=f.service.project({actor:owner,objectIds:[widget.objectId,native.objectId]}).items;
 assert.deepEqual(items[0].ownerRepresents,[app]);assert.deepEqual(items[1].ownerRepresents,[]);
 assert.equal(f.calls.filter(call=>call==='owner-relation').length,1);
 assert.equal(f.calls.filter(call=>call==='content-project').length,2,'ordinary authorized content projection is unchanged');
});

test('locked own and hidden own relation never invokes content/source authority/decrypt',t=>{
 const f=fixture(t),locked=f.create({locked:true}),hidden=f.create({hidden:true}),native=f.create({providerId:'fixture.native',locked:true});f.calls.length=0;
 const items=f.service.project({actor:owner,objectIds:[locked.objectId,hidden.objectId,native.objectId]}).items;
 assert.deepEqual(items.map(item=>item.ownerRepresents),[[app],[app],[]]);
 assert.equal(items[0].view.state,'locked');assert.equal(items[1].available,false);assert.equal(items[1].hidden,true);
 assert.deepEqual(f.calls,['owner-relation','owner-relation']);
});

test('foreign and anonymous projection never calls owner metadata or adds its property',t=>{
 const f=fixture(t),publicObject=f.create({visibility:'public'}),privateObject=f.create();f.calls.length=0;
 for(const actor of [other,null]){
  const items=f.service.project({...(actor?{actor}:{}),objectIds:[publicObject.objectId,privateObject.objectId]}).items;
  assert.equal(items[0].available,true);assert.deepEqual(items[1],{objectId:privateObject.objectId,available:false});
  assert.ok(items.every(item=>!Object.hasOwn(item,'ownerRepresents')));
 }
 assert.equal(f.calls.includes('owner-relation'),false);
});

test('removed returns declared-empty; missing provider/permission remains unknown rather than absence',t=>{
 const f=fixture(t),removed=f.create(),unknown=f.create({providerId:'fixture.unknown'}),missing=f.create();
 f.command('remove',{objectId:removed.objectId,expectedRevision:removed.revision});
 let items=f.service.project({actor:owner,objectIds:[removed.objectId,unknown.objectId]}).items;
 assert.deepEqual(items[0].ownerRepresents,[]);assert.equal(Object.hasOwn(items[1],'ownerRepresents'),false);
 f.setRelation(()=>undefined);assert.equal(Object.hasOwn(f.service.project({actor:owner,objectIds:[missing.objectId]}).items[0],'ownerRepresents'),false);
 f.setRelation(()=>{throw new CanvasObjectError('canvas_provider_access_denied',403);});
 assert.equal(Object.hasOwn(f.service.project({actor:owner,objectIds:[missing.objectId]}).items[0],'ownerRepresents'),false);
 f.service.unregisterProvider('fixture.widget');items=f.service.project({actor:owner,objectIds:[missing.objectId]}).items;
 assert.equal(Object.hasOwn(items[0],'ownerRepresents'),false);
});

test('owner metadata callback cannot publish after Object revision or actor authority changes',t=>{
 for(const revokeActor of [false,true]){
  const f=fixture(t),object=f.create({hidden:true});
  f.setRelation(()=>{if(revokeActor)f.active.delete(owner.deviceId);else f.command('update',{objectId:object.objectId,expectedRevision:object.revision,presentation:{mask:{shape:'ellipse'}}});return[app];});
  assert.throws(()=>f.service.project({actor:owner,objectIds:[object.objectId]}),error=>['canvas_object_changed','canvas_authentication_required'].includes(error.code));
 }
});

test('owner representation refs are bounded validated detached DTOs',t=>{
 const f=fixture(t),object=f.create({hidden:true}),refs=[{...app}];f.setRelation(()=>refs);
 const value=f.service.project({actor:owner,objectIds:[object.objectId]}).items[0];refs[0].id='changed';assert.deepEqual(value.ownerRepresents,[app]);
 for(const invalid of [[{kind:'app',id:app.id,launchUrl:'https://invalid/'}],Array.from({length:17},()=>app),Promise.resolve([app])]){
  f.setRelation(()=>invalid);assert.throws(()=>f.service.project({actor:owner,objectIds:[object.objectId]}));
 }
});

test('negative owner-gate control leaks a private relation and fails the foreign-wire criterion',async t=>{
 const url=new URL('./server.mjs',import.meta.url),source=readFileSync(url,'utf8'),needle="if(!actor||before?.ownerId!==actor.accountId)return undefined;";
 assert.equal(source.split(needle).length,2,'one actual owner gate');
 const changed=source.replace(needle,'if(!actor)return undefined;').replace(/from '(\.\/.+?)'/g,(_match,path)=>`from '${new URL(path,url).href}'`);
 const {createCanvasObjectsService:factory}=await import('data:text/javascript;base64,'+Buffer.from(changed).toString('base64'));
 const f=fixture(t,{factory}),object=f.create({visibility:'public'});
 f.setRelation(()=>[app]);
 const leaked=f.service.project({actor:other,objectIds:[object.objectId]}).items[0];
 assert.throws(()=>assert.equal(Object.hasOwn(leaked,'ownerRepresents'),false),{code:'ERR_ASSERTION'});
});
