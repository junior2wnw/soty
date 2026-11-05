import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {mkdtempSync,rmSync,realpathSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,basename} from 'node:path';
import {createCanvasObjectsService} from './server.mjs';
import {validateCanvasMask,CanvasObjectError} from './contract.mjs';

const owner={accountId:'owner',deviceId:'owner_device'},friend={accountId:'friend',deviceId:'friend_device'},outside={accountId:'outside',deviceId:'outside_device'};
const policy=audience=>({audience,accountIds:[],communityIds:[]}),code=value=>error=>{assert.equal(error.code,value);return true;};
function fixture(t){
 const parent=realpathSync(tmpdir()),folder=mkdtempSync(join(parent,'soty-canvas-objects-')),databasePath=join(folder,'objects.sqlite');let service,serial=0,observerFailure=false;
 const active=new Set([owner.deviceId,friend.deviceId,outside.deviceId]),changes=[],calls=[],sources=new Map([
  ['note',{ownerId:owner.accountId,public:true,text:'secret native text',password:false}],['widget',{ownerId:owner.accountId,public:false,text:'actual widget',viewers:new Set([friend.accountId]),password:false}],['sealed',{ownerId:owner.accountId,public:true,password:true}]]),groups=new Set();
 const provider=providerId=>({providerId,withBindingAuthority(request,callback){calls.push({type:'authority',providerId,ref:request.ref});const source=sources.get(request.ref),allowedNow=()=>Boolean(source&&(source.public||source.ownerId===request.actor?.accountId||source.viewers?.has(request.actor?.accountId))),allowed=allowedNow();const result=callback({allowed,protection:source?.password?{kind:'password',sealed:true}:{kind:'none'}});if(allowed&&!allowedNow())throw new CanvasObjectError('canvas_provider_access_denied',403);return result;},project(context){calls.push({type:'project',providerId,ref:context.ref});assert.throws(()=>service.withObjectAuthority({...context},()=>{}),code('canvas_object_authority_required'));return service.withObjectAuthority(context,proof=>({schema:'fixture.provider.v1',text:sources.get(proof.ref).text,instance:proof.objectId}));}});
 const issued=new WeakSet([owner,friend,outside]);let fenceDepth=0;
 const options={databasePath,projectId:'canvas_tests',actorActive:actor=>active.has(actor.deviceId),withActorAuthorityFence:(actor,callback)=>{if(!issued.has(actor))throw new CanvasObjectError('connect_authority_actor_invalid',403);assert.equal(fenceDepth,0,'the original Connect actor fence must not be nested');assert.ok(active.has(actor.deviceId));fenceDepth++;try{return callback();}finally{fenceDepth--;}},providers:[provider('native.note.v1'),provider('app.widget.v1')],canShareCommunity:()=>true,canAccessCommunity:(actor,id)=>groups.has(actor.accountId+':'+id),onChange:change=>{changes.push(change);if(observerFailure)throw new Error('observer_failed');}};
 const open=()=>{service=createCanvasObjectsService(options);};open();
 t.after(()=>{service.close();const actual=realpathSync(folder);assert.equal(dirname(actual).toLowerCase(),parent.toLowerCase());assert.ok(basename(actual).startsWith('soty-canvas-objects-'));rmSync(actual,{recursive:true,force:true});});
 const call=(actor,op,args={})=>service.execute({actor,op:'canvas.objects.'+op,args:{expectedAccountId:actor.accountId,...args}});
 const mutate=(actor,op,args={})=>call(actor,op,{requestId:'cmd_'+(++serial),...args});
 const create=(extra={})=>mutate(owner,'create',{binding:{providerId:'native.note.v1',ref:'note'},presentation:{mask:{shape:'rect'},fontSize:16},visibility:policy('private'),hidden:{owner:false,others:false},protection:{kind:'none'},...extra});
 const sql=callback=>{const db=new DatabaseSync(databasePath);try{return callback(db);}finally{db.close();}};
 return {call,mutate,create,active,changes,calls,sources,groups,sql,provider,get service(){return service;},restart(){service.close();open();},observerFailure(){observerFailure=true;}};
}

test('two providers keep content in their source, stable instances/CAS/receipts survive reopening and owner isolation',t=>{
 const f=fixture(t),first=f.create({requestId:'stable_create'}),objectId=first.current.objectId;
 assert.equal(first.replayed,false);assert.equal(f.call(owner,'get',{objectId}).object.binding.ref,'note');assert.throws(()=>f.call(friend,'get',{objectId}),code('canvas_owner_required'));
 const widget=f.create({binding:{providerId:'app.widget.v1',ref:'widget'}});assert.notEqual(widget.current.objectId,objectId);assert.equal(f.call(owner,'list').objects.length,2);assert.equal(f.call(friend,'list').objects.length,0);
 const updated=f.mutate(owner,'update',{objectId,expectedRevision:1,presentation:{mask:{shape:'ellipse'},fontSize:24}});assert.equal(updated.current.revision,2);
 assert.throws(()=>f.mutate(owner,'update',{objectId,expectedRevision:1,hidden:{owner:true,others:false}}),code('canvas_revision_conflict'));
 f.restart();const replay=f.create({requestId:'stable_create'});assert.equal(replay.replayed,true);assert.deepEqual(replay.receipt,first.receipt);assert.deepEqual(replay.current,updated.current);
 assert.throws(()=>f.create({requestId:'stable_create',hidden:{owner:true,others:false}}),code('canvas_request_conflict'));
 assert.doesNotMatch(f.sql(db=>JSON.stringify(db.prepare('SELECT * FROM canvas_objects').all())),/secret native text|actual widget|width|height|\bslot\b/u);
});

test('private/hidden objects are excluded before any provider lookup or resolver; public source changes remain live without layout copies',t=>{
 const f=fixture(t),objectId=f.create().current.objectId;f.calls.length=0;
 assert.deepEqual(f.service.project({objectIds:[objectId]}).items,[{objectId,available:false}]);assert.equal(f.calls.length,0,'private filter must run before the provider authority/resolver');
 f.mutate(owner,'update',{objectId,expectedRevision:1,visibility:policy('public')});const visible=f.service.project({objectIds:[objectId]}).items[0];assert.equal(visible.view.text,'secret native text');assert.equal('binding' in visible,false);
 f.sources.get('note').text='fresh source text';assert.equal(f.service.project({objectIds:[objectId]}).items[0].view.text,'fresh source text');
 f.mutate(owner,'update',{objectId,expectedRevision:2,hidden:{owner:false,others:true}});f.calls.length=0;assert.equal(f.service.project({objectIds:[objectId]}).items[0].available,false);assert.equal(f.calls.length,0);
 assert.equal(f.service.project({actor:owner,objectIds:[objectId]}).items[0].available,true);
 f.mutate(owner,'update',{objectId,expectedRevision:3,hidden:{owner:true,others:false}});f.calls.length=0;assert.equal(f.service.project({actor:owner,objectIds:[objectId]}).items[0].available,false);assert.equal(f.calls.length,0);assert.equal(f.call(owner,'get',{objectId}).object.hidden.owner,true,'owner still has management/restore access');
});

test('fresh viewer/source grants and community membership can independently revoke projection',t=>{
 const f=fixture(t),objectId=f.create({binding:{providerId:'app.widget.v1',ref:'widget'},visibility:{audience:'restricted',accountIds:[friend.accountId],communityIds:[]}}).current.objectId;
 assert.equal(f.service.project({actor:friend,objectIds:[objectId]}).items[0].available,true);assert.equal(f.service.project({actor:outside,objectIds:[objectId]}).items[0].available,false);
 f.sources.get('widget').viewers.delete(friend.accountId);assert.equal(f.service.project({actor:friend,objectIds:[objectId]}).items[0].available,false,'a saved object policy cannot override the provider current ACL');
 f.mutate(owner,'update',{objectId,expectedRevision:1,visibility:{audience:'restricted',accountIds:[],communityIds:['group_a']}});f.sources.get('widget').viewers.add(friend.accountId);f.groups.add(friend.accountId+':group_a');assert.equal(f.service.project({actor:friend,objectIds:[objectId]}).items[0].available,true);
 f.groups.clear();f.calls.length=0;assert.equal(f.service.project({actor:friend,objectIds:[objectId]}).items[0].available,false);assert.equal(f.calls.length,0);
 f.active.delete(friend.deviceId);assert.throws(()=>f.service.project({actor:friend,objectIds:[objectId]}),code('canvas_authentication_required'));
});

test('an unknown future provider fails closed, then works after host registration without changing the object schema',t=>{
 const f=fixture(t);assert.throws(()=>f.create({binding:{providerId:'future.physics.v8',ref:'note'}}),code('canvas_provider_unavailable'));
 f.service.registerProvider(f.provider('future.physics.v8'));const objectId=f.create({binding:{providerId:'future.physics.v8',ref:'note'},visibility:policy('public')}).current.objectId;
 assert.equal(f.service.project({objectIds:[objectId]}).items[0].view.text,'secret native text');f.service.unregisterProvider('future.physics.v8');assert.equal(f.service.project({objectIds:[objectId]}).items[0].available,false);assert.equal(f.call(owner,'get',{objectId}).object.binding.providerId,'future.physics.v8');
});

test('actual failed SQLite receipt write rolls object creation back; the same intent passes after the fault is removed',t=>{
 const f=fixture(t);f.sql(db=>db.exec("CREATE TRIGGER fail_canvas_receipt BEFORE INSERT ON canvas_commands BEGIN SELECT RAISE(ABORT,'canvas_write_fault'); END"));
 assert.throws(()=>f.create({requestId:'retryable_object'}),/canvas_write_fault/u);assert.equal(f.call(owner,'list').objects.length,0);assert.equal(f.changes.length,0);
 f.sql(db=>db.exec('DROP TRIGGER fail_canvas_receipt'));const accepted=f.create({requestId:'retryable_object'});assert.equal(accepted.current.revision,1);assert.equal(f.changes.length,1);assert.equal(f.create({requestId:'retryable_object'}).replayed,true);
 f.observerFailure();const removed=f.mutate(owner,'remove',{objectId:accepted.current.objectId,expectedRevision:1});assert.equal(removed.current.state,'removed');f.restart();assert.equal(f.call(owner,'get',{objectId:accepted.current.objectId}).object.state,'removed');assert.equal(f.service.project({objectIds:[accepted.current.objectId]}).items[0].available,false);
 assert.equal(f.mutate(owner,'restore',{objectId:accepted.current.objectId,expectedRevision:2}).current.state,'active');
});

test('plaintext cannot acquire a password flag; only the authoritative sealed provider can yield a locked projection',t=>{
 const f=fixture(t);assert.throws(()=>f.create({protection:{kind:'password'}}),code('canvas_plaintext_password_forbidden'));assert.equal(f.call(owner,'list').objects.length,0);
 const objectId=f.create({binding:{providerId:'native.note.v1',ref:'sealed'},protection:{kind:'password'},visibility:policy('public')}).current.objectId;f.calls.length=0;
 const projected=f.service.project({objectIds:[objectId]}).items[0];assert.deepEqual(projected.view,{schema:'soty.canvas-object-lock.v1',state:'locked'});assert.equal(f.calls.filter(value=>value.type==='project').length,0,'locked content is never resolved and blurred after loading');assert.equal('binding' in projected,false);
 f.sources.get('sealed').password=false;f.calls.length=0;assert.equal(f.service.project({objectIds:[objectId]}).items[0].view.state,'locked');assert.equal(f.calls.length,0,'a locked projection now stops before ANY provider authority lookup');
 assert.throws(()=>f.create({binding:{providerId:'native.note.v1',ref:'sealed'},protection:{kind:'password'}}),code('canvas_plaintext_password_forbidden'));
});

test('object authority is scoped and ephemeral; a JSON copy or a saved context never delegates source authority',t=>{
 const f=fixture(t);let captured;const provider=f.provider('context.v1');const original=provider.project;provider.project=context=>{captured=context;return original(context);};f.service.registerProvider(provider);
 const objectId=f.create({binding:{providerId:'context.v1',ref:'note'},visibility:policy('public')}).current.objectId;f.service.project({objectIds:[objectId]});
 assert.throws(()=>f.service.withObjectAuthority(captured,()=>true),code('canvas_object_authority_required'));assert.throws(()=>f.service.withObjectAuthority(JSON.parse(JSON.stringify(captured)),()=>true),code('canvas_object_authority_required'));
});

test('polygon mask bounds, degenerate/crossing paths and duplicate points reject; normalized masks detach',()=>{
 const good={shape:'polygon',points:[{x:0,y:0},{x:1,y:0},{x:.6,y:.5},{x:1,y:1},{x:0,y:1}]};assert.deepEqual(validateCanvasMask(good),good);
 for(const mask of [{shape:'polygon',points:[{x:0,y:0},{x:.5,y:.5},{x:1,y:1}]},{shape:'polygon',points:[{x:0,y:0},{x:1,y:1},{x:0,y:1},{x:1,y:0}]},{shape:'polygon',points:[{x:0,y:0},{x:1,y:0},{x:1,y:1},{x:0,y:0}]},{shape:'rect',points:[]},{shape:'polygon',points:[{x:0,y:0},{x:Infinity,y:0},{x:1,y:1}]}])assert.throws(()=>validateCanvasMask(mask));
 assert.deepEqual(validateCanvasMask({shape:'ellipse'}),{shape:'ellipse'});const copy=validateCanvasMask(good);copy.points[0].x=.2;assert.equal(good.points[0].x,0);
});

test('the real issued actor crosses its Connect fence, a copied tuple does not, and signed projection never nests the fence',t=>{
 const f=fixture(t),objectId=f.create().current.objectId;assert.equal(f.call(owner,'project',{objectIds:[objectId]}).items[0].available,true);
 assert.throws(()=>f.call({...owner},'list'),code('connect_authority_actor_invalid'));
 assert.throws(()=>f.service.project({actor:{...owner},objectIds:[objectId]}),code('connect_authority_actor_invalid'));
 assert.equal(f.service.project({actor:owner,objectIds:[objectId]}).items[0].available,true);
});

test('fresh content frames read 512KiB binary/text without serializing a context, and expire the proof immediately',t=>{
 const f=fixture(t),objectId=f.create().current.objectId;let context;
 const result=f.service.withObjectRead({actor:owner,objectId},proof=>{context=proof;return f.service.withObjectAuthority(proof,()=>({bytes:Buffer.alloc(512*1024,7),type:'application/octet-stream'}));});
 assert.equal(result.bytes.length,512*1024);assert.equal(result.bytes[0],7);assert.throws(()=>f.service.withObjectAuthority(context,()=>{}),code('canvas_object_authority_required'));
 assert.throws(()=>f.service.withObjectAuthority(JSON.parse(JSON.stringify(context)),()=>{}),code('canvas_object_authority_required'));
 assert.throws(()=>f.service.withObjectRead({actor:{...owner},objectId},()=>Buffer.from('no')),code('connect_authority_actor_invalid'));
 assert.throws(()=>f.service.withObjectRead({actor:owner,objectId,ownerId:owner.accountId},()=>''));
 const text=f.service.withObjectRead({actor:owner,objectId},()=>({text:'x'.repeat(512*1024)}));assert.equal(text.text.length,512*1024);
 assert.throws(()=>f.service.withObjectRead({actor:owner,objectId},()=>Buffer.alloc(1024*1024+1)),code('canvas_content_too_large'));
 assert.equal(f.service.withObjectRead({actor:owner,objectId},()=>Buffer.from('ok')).toString(),'ok');
});

test('locked/private/revoked content never calls the reader; revocation during a read discards its result',t=>{
 const f=fixture(t),privateId=f.create().current.objectId;let calls=0;
 assert.throws(()=>f.service.withObjectRead({objectId:privateId},()=>{calls++;return 'leak';}),code('canvas_object_unavailable'));assert.equal(calls,0);
 const locked=f.create({binding:{providerId:'native.note.v1',ref:'sealed'},protection:{kind:'password'},visibility:policy('public')}).current.objectId;
 assert.throws(()=>f.service.withObjectRead({objectId:locked},()=>{calls++;return 'leak';}),code('canvas_object_locked'));assert.equal(calls,0);
 const widget=f.create({binding:{providerId:'app.widget.v1',ref:'widget'},visibility:{audience:'restricted',accountIds:[friend.accountId],communityIds:[]}}).current.objectId;
 assert.throws(()=>f.service.withObjectRead({actor:friend,objectId:widget},()=>{f.sources.get('widget').viewers.delete(friend.accountId);return Buffer.from('must not escape');}),code('canvas_object_unavailable'));
 assert.throws(()=>f.service.withObjectRead({actor:friend,objectId:widget},()=>{calls++;return 'leak';}),code('canvas_object_unavailable'));assert.equal(calls,0);
});

test('source changes query actual bindings without rewriting geometry/object revisions or broadcasting public-transition hints',t=>{
 const f=fixture(t),objectId=f.create({visibility:policy('public')}).current.objectId,privateId=f.create().current.objectId;f.changes.length=0;
 const before=f.call(owner,'get',{objectId}).object;f.sources.get('note').text='current content';
 assert.deepEqual(f.service.notifySourceChanged({providerId:'native.note.v1',ref:'note',ownerId:owner.accountId}),{notified:2});assert.equal(f.changes.length,2);assert.ok(f.changes.every(change=>change.publicTransition===false));assert.ok(f.changes.every(change=>!JSON.stringify(change).includes('current content')));assert.deepEqual(f.call(owner,'get',{objectId}).object,before);
 assert.equal(f.service.project({objectIds:[privateId]}).items[0].available,false,'a source hint cannot bypass object read filters');assert.equal(f.service.project({objectIds:[objectId]}).items[0].view.text,'current content');
 assert.equal(f.service.notifySourceChanged({providerId:'native.note.v1',ref:'note',ownerId:outside.accountId}).notified,0);
 f.mutate(owner,'remove',{objectId,expectedRevision:1});f.changes.length=0;assert.equal(f.service.notifySourceChanged({providerId:'native.note.v1',ref:'note',ownerId:owner.accountId}).notified,1);
});

test('only the owner gets the content-free hidden marker, before provider resolution; outsiders learn no hidden state',t=>{
 const f=fixture(t),objectId=f.create({hidden:{owner:true,others:false}}).current.objectId;f.calls.length=0;
 assert.deepEqual(f.service.project({actor:owner,objectIds:[objectId]}).items,[{objectId,available:false,hidden:true}]);assert.equal(f.calls.length,0);
 assert.deepEqual(f.service.project({actor:outside,objectIds:[objectId]}).items,[{objectId,available:false}]);assert.deepEqual(f.service.project({objectIds:[objectId]}).items,[{objectId,available:false}]);assert.equal(f.calls.length,0);
});
