import test from 'node:test';
import assert from 'node:assert/strict';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
import {mkdtempSync,rmSync,readFileSync} from 'node:fs';
import {resolve,join,dirname} from 'node:path';
import {tmpdir} from 'node:os';
import sharp from 'sharp';
import {createNotesService} from '../../modules/notes/server/index.mjs';
import {createMaterialsService} from '../../modules/materials/server/index.mjs';
import {createCanvasObjectsService} from '../../modules/canvas-objects/server.mjs';
import {createCanvasWidgetsService,createCanvasWidgetProvider} from '../../modules/canvas-widgets/server.mjs';
import {applyFieldCommand} from './unified-field-state.mjs';
import {createCanvasCreateFlow,createCanvasCreateStore,CANVAS_CREATE_LIMITS} from './canvas-create-flow.mjs';

const actor={accountId:'account_creator',deviceId:'device_creator'},appId='app-'+'a'.repeat(32);
const until=async predicate=>{for(let i=0;i<100;i++){if(predicate())return;await new Promise(resolve=>setImmediate(resolve));}assert.fail('state did not settle');};
function journal(){const rows=new Map();return{rows,fail:false,async get(id){return structuredClone(rows.get(id)??null);},async list(){return [...rows.values()].map(value=>structuredClone(value));},async put(record,expected){if(this.fail)throw Error('disk_full');const current=rows.get(record.operationId);if((current?.version??0)!==expected)return false;rows.set(record.operationId,structuredClone(record));return true;},async remove(id){rows.delete(id);}};}
function fixture(t,{store=journal(),lostAt,placementStatus='saved'}={}){
  const base=resolve(tmpdir()),directory=mkdtempSync(join(base,'soty-create-flow-'));let objects,current=true,lost=true,placeCalls=0,readyCalls=0;
  const verify=(context,consume)=>objects.withObjectAuthority(context,proof=>consume({providerId:proof.providerId,ownerId:proof.ownerId,ref:proof.ref,right:proof.right}));
  const notes=createNotesService({databasePath:join(directory,'notes.sqlite'),projectId:'create-tests',verifyCanvasObjectAuthority:verify}),materials=createMaterialsService({databasePath:join(directory,'materials.sqlite'),projectId:'create-tests',verifyObjectAuthority:verify});
  const widgets=createCanvasWidgetsService({databasePath:join(directory,'widgets.sqlite'),projectId:'create-tests',actorActive:()=>current,withActorAuthorityFence:(_actor,callback)=>callback(),
    withAppSourceAuthority:(request,callback)=>callback({app:{appId:request.appId,name:'Настоящий instance тестового source',status:'ready',ownerAccountId:'other_app_owner'},
      entry:{appId:request.appId,domainId:request.domainId??'app-main',path:request.path??'/',origin:'http://127.0.0.1:3031'},target:{revision:1,digest:'a'.repeat(64),profile:'http.v1'},runtimeAllowed:true})});
  const sourceScope=request=>({actor:request.actor,ownerId:request.objectOwnerId,mode:request.mode});
  objects=createCanvasObjectsService({databasePath:join(directory,'objects.sqlite'),projectId:'create-tests',actorActive:()=>current,withActorAuthorityFence:(_actor,callback)=>callback(),providers:[
    {providerId:'soty.notes',withBindingAuthority:(request,callback)=>notes.canvas.withSourceAuthority({...sourceScope(request),noteId:request.ref},callback),project:context=>notes.canvas.readMetadata({context})},
    {providerId:'soty.materials',withBindingAuthority:(request,callback)=>materials.native.withSourceAuthority({...sourceScope(request),materialId:request.ref},callback),project:context=>materials.native.readMetadata({context})},
    createCanvasWidgetProvider({widgets,withObjectAuthority:(context,callback)=>objects.withObjectAuthority(context,callback)}),
  ]});
  const calls=[],placements=new Map();let field={schema:'soty.field.v1',contexts:[{contextId:'personal',title:'Моё',x:0,y:0}],shortcuts:[]};
  const api={async request(op,args){
    calls.push({op,args:structuredClone(args)});
    if(['notes.put','materials.begin','canvas.widgets.create','canvas.objects.create'].includes(op))assert.ok((await store.list()).some(value=>value.stage!=='complete'),'creation intent must be durably journaled before mutation');
    const service=op.startsWith('notes.')?notes:op.startsWith('materials.')?materials:op.startsWith('canvas.widgets.')?widgets:objects;
    const result=await service.execute({op,args,actor});if(op===lostAt&&lost){lost=false;throw Error('NETWORK_LOST_ACK');}return result;
  }};
  const options={api,accountId:actor.accountId,projectId:'create-tests',isCurrent:()=>current,store,
    onObjectReady:async objectId=>{readyCalls++;assert.equal(objects.project({actor,objectIds:[objectId]}).items[0].available,true);},
    place:async(objectId,size)=>{placeCalls++;if(placements.has(objectId))return placements.get(objectId);assert.equal(objects.project({actor,objectIds:[objectId]}).items[0].available,true);
      const shortcutId=crypto.randomUUID();field=applyFieldCommand(field,{type:'add-shortcut',shortcutId,entity:{kind:'object',id:objectId},contextId:'personal',placement:{kind:'free',x:0,y:0,...size}}).document;
      const ack={shortcutId,status:placementStatus,localDurable:placementStatus!=='unsafe'};placements.set(objectId,ack);if(lostAt==='placement'&&lost){lost=false;throw Error('NETWORK_LOST_ACK');}return ack;}};
  t.after(async()=>{objects.close();widgets.close();notes.close();await materials.close();assert.equal(dirname(resolve(directory)),base);assert.ok(directory.startsWith(join(base,'soty-create-flow-')));rmSync(directory,{recursive:true,force:true});});
  return{api,store,options,calls,notes,materials,objects,widgets,placements,get field(){return field;},get placeCalls(){return placeCalls;},get readyCalls(){return readyCalls;},changeAccount(){current=false;}};
}

test('actual note→ObjectService→canonical placement survives a new controller with no source text in layout or completed journal',async t=>{
  const f=fixture(t),flow=createCanvasCreateFlow(f.options),result=await flow.startNote({title:'Идея',body:'Частный текст прямо на полотне'});
  assert.equal(result.status,'saved');assert.equal(f.placements.size,1);assert.equal(f.readyCalls,1);assert.equal((await flow.listPending()).length,0);
  const note=f.notes.execute({op:'notes.list',args:{expectedAccountId:actor.accountId},actor}).notes[0];assert.equal(note.title,'Идея');assert.doesNotMatch(JSON.stringify(f.field),/Частный текст|body|data:|blob:/u);
  const record=await f.store.get(result.operationId);assert.equal(record.sourceIntent,null);assert.equal(record.stage,'complete');flow.dispose();
  const again=createCanvasCreateFlow(f.options),count=f.calls.length;assert.deepEqual(await again.resume(result.operationId),result);assert.equal(f.calls.length,count);again.dispose();
});
for(const lostAt of ['notes.put','canvas.objects.create','placement'])test(`lost ${lostAt} ACK resumes exact identities after controller reload and creates one source/object/placement`,async t=>{
  const f=fixture(t,{lostAt}),first=createCanvasCreateFlow(f.options);let operationId;
  await assert.rejects(first.startNote({body:'Не дублировать'}),error=>{operationId=error.operationId;return error.code==='NETWORK_LOST_ACK';});first.dispose();
  const next=createCanvasCreateFlow(f.options),result=await next.resume(operationId);assert.equal(result.objectId,[...f.placements.keys()][0]);assert.equal(f.placements.size,1);assert.equal(f.field.shortcuts.length,1);
  assert.equal(f.notes.execute({op:'notes.list',args:{expectedAccountId:actor.accountId},actor}).notes.length,1);assert.equal(f.objects.execute({op:'canvas.objects.list',args:{expectedAccountId:actor.accountId},actor}).objects.length,1);
  const original=f.calls.filter(call=>call.op===lostAt);if(original.length>1)assert.deepEqual(original[0].args,original[1].args);next.dispose();
});
test('valid image/audio/video use real chunk storage; lost media commit ACK resumes without duplicate bytes or object',async t=>{
  const f=fixture(t,{lostAt:'materials.commit'}),image=await sharp({create:{width:3,height:2,channels:3,background:'green'}}).png().toBuffer(),first=createCanvasCreateFlow(f.options);let operationId;
  await assert.rejects(first.startFile(new Blob([image],{type:'image/png'})),error=>{operationId=error.operationId;return error.code==='NETWORK_LOST_ACK';});first.dispose();
  const next=createCanvasCreateFlow(f.options);await next.resume(operationId);
  const audio=readFileSync(new URL('../../modules/materials/test/fixtures/tone.webm',import.meta.url)),video=readFileSync(new URL('../../modules/materials/test/fixtures/video.webm',import.meta.url));
  await next.startFile(new Blob([audio],{type:'audio/webm;codecs=opus'}));await next.startFile(new Blob([video],{type:'video/webm;codecs="vp8,opus"'}));
  const list=await f.materials.execute({op:'materials.list',args:{expectedAccountId:actor.accountId},actor});assert.equal(list.materials.length,3);assert.deepEqual(new Set(list.materials.map(item=>item.kind)),new Set(['image','audio','video']));assert.equal(f.field.shortcuts.length,3);
  assert.ok((await f.store.list()).every(value=>value.sourceIntent===null));next.dispose();
});
test('foreign app widget source uses the same durable pipeline and exact entry; lost widget ACK never creates another instance',async t=>{
  const f=fixture(t,{lostAt:'canvas.widgets.create'}),first=createCanvasCreateFlow(f.options);let operationId;
  await assert.rejects(first.startWidget({appId,entry:{domainId:'named-entry',path:'/dashboard?mode=week'}}),error=>{operationId=error.operationId;return error.code==='NETWORK_LOST_ACK';});first.dispose();
  const next=createCanvasCreateFlow(f.options),result=await next.resume(operationId),widgets=f.widgets.execute({op:'canvas.widgets.list',args:{expectedAccountId:actor.accountId},actor}).widgets;
  assert.equal(widgets.length,1);assert.equal(widgets[0].appId,appId);assert.deepEqual(widgets[0].entry,{domainId:'named-entry',path:'/dashboard?mode=week'});
  assert.equal(f.objects.execute({op:'canvas.objects.get',args:{expectedAccountId:actor.accountId,objectId:result.objectId},actor}).object.binding.providerId,'soty.apps');next.dispose();
});
test('failed initial journal write sends no mutation and retries the same in-memory intent only after storage recovers',async t=>{
  const f=fixture(t);f.store.fail=true;const flow=createCanvasCreateFlow(f.options);let operationId;
  await assert.rejects(flow.startNote({body:'До сети — надёжный черновик'}),error=>{operationId=error.operationId;return error.localDurable===false&&error.code==='canvas_create_local_unavailable';});assert.equal(f.calls.length,0);
  assert.equal((await flow.listPending())[0].localDurable,false);f.store.fail=false;const result=await flow.resume(operationId);assert.equal(result.operationId,operationId);assert.equal(f.field.shortcuts.length,1);flow.dispose();
});
test('unknown/unsupported MIME, oversized files and unsafe widget entry fail before any network or journal mutation',async t=>{
  const f=fixture(t),flow=createCanvasCreateFlow(f.options);
  for(const [blob,code]of [[new Blob(['x']),'canvas_create_media_type_unknown'],[new Blob(['<svg/>'],{type:'image/svg+xml'}),'canvas_create_media_type_unsupported'],[new Blob([new Uint8Array(MATERIAL_FILE_LIMIT+1)],{type:'video/webm'}),'canvas_create_file_size']])await assert.rejects(flow.startFile(blob),error=>error.code===code);
  await assert.rejects(flow.startWidget({appId,entry:{domainId:'main',path:'//outside.test/'}}),error=>error.code==='canvas_create_entry_invalid');assert.equal(f.calls.length,0);assert.equal(f.store.rows.size,0);flow.dispose();
});
const MATERIAL_FILE_LIMIT=25*1024*1024;
test('pending placement is successful only with its existing canonical durable outbox; unsafe placement stays recoverable',async t=>{
  const f=fixture(t,{placementStatus:'pending'}),flow=createCanvasCreateFlow(f.options),result=await flow.startNote({body:'Локально надёжно'});assert.equal(result.status,'pending');assert.equal(result.localDurable,true);flow.dispose();
  const bad=fixture(t,{placementStatus:'unsafe'}),unsafe=createCanvasCreateFlow(bad.options);await assert.rejects(unsafe.startNote({body:'Проверить обещание'}),error=>error.code==='canvas_create_placement_not_durable');assert.equal((await unsafe.listPending())[0].stage,'placement');unsafe.dispose();
});
test('account switch and a mismatched account journal cannot continue an old source into another viewer',async t=>{
  const f=fixture(t,{lostAt:'notes.put'}),flow=createCanvasCreateFlow(f.options);let operationId;await assert.rejects(flow.startNote({body:'Моё'}),error=>{operationId=error.operationId;return true;});flow.dispose();
  const other=createCanvasCreateFlow({...f.options,accountId:'account_other'}),before=f.calls.length;await assert.rejects(other.resume(operationId),error=>error.code==='canvas_create_scope_changed');assert.equal(f.calls.length,before);other.dispose();
  f.changeAccount();const stale=createCanvasCreateFlow(f.options);await assert.rejects(stale.resume(operationId),error=>error.code==='canvas_create_account_changed');assert.equal(f.calls.length,before);stale.dispose();
});
test('two controllers resume a shared journal concurrently without creating duplicate source/object placements or regressing a completed record',async t=>{
  const f=fixture(t,{lostAt:'notes.put'}),first=createCanvasCreateFlow(f.options);let operationId;await assert.rejects(first.startNote({body:'Одна операция'}),error=>{operationId=error.operationId;return true;});first.dispose();
  const a=createCanvasCreateFlow(f.options),b=createCanvasCreateFlow(f.options),[one,two]=await Promise.all([a.resume(operationId),b.resume(operationId)]);assert.equal(one.objectId,two.objectId);assert.equal(one.shortcutId,two.shortcutId);assert.equal(f.placements.size,1);assert.equal((await f.store.get(operationId)).stage,'complete');a.dispose();b.dispose();
});
test('closing during a committed source with delayed ACK keeps the prior durable intent for reload, without continuing stale placement',async t=>{
  const f=fixture(t),api=f.options.api;let release,started=false;
  const flow=createCanvasCreateFlow({...f.options,api:{async request(op,args){const value=await api.request(op,args);if(op==='notes.put'){started=true;await new Promise(resolve=>{release=resolve;});}return value;}}});
  const creating=flow.startNote({body:'Закрыли окно во время сохранения'});await until(()=>started);const operationId=[...f.store.rows.keys()][0];flow.dispose();release();
  await assert.rejects(creating,error=>error.code==='canvas_create_account_changed');assert.equal(f.placeCalls,0);assert.equal((await f.store.get(operationId)).stage,'source');
  const recovered=createCanvasCreateFlow(f.options);await recovered.resume(operationId);assert.equal(f.field.shortcuts.length,1);assert.equal(f.notes.execute({op:'notes.list',args:{expectedAccountId:actor.accountId},actor}).notes.length,1);recovered.dispose();
});
test('negative control: bypassing the durable journal cannot pass the initial-storage-failure network oracle',async t=>{
  const url=new URL('./canvas-create-flow.mjs',import.meta.url),source=readFileSync(url,'utf8'),broken=source.replace('current();await persist(record,0);return resume(record.operationId);',"current();volatile.set(record.operationId,record);await api.request('notes.put',{...record.sourceIntent,expectedAccountId:accountId});return resume(record.operationId);");assert.notEqual(broken,source);
  const absolute=broken.replace(/from '(\.\.?\/[^']+)'/gu,(_match,path)=>`from '${new URL(path,url).href}'`),module=await import(`data:text/javascript;base64,${Buffer.from(absolute).toString('base64')}`);
  let network=0;const store=journal();store.fail=true;const flow=module.createCanvasCreateFlow({api:{async request(){network++;throw Error('sent_without_journal');}},accountId:actor.accountId,projectId:'create-tests',isCurrent:()=>true,place:async()=>{},store});
  await assert.rejects(flow.startNote({body:'Нельзя отправлять'}));assert.throws(()=>assert.equal(network,0),error=>error.code==='ERR_ASSERTION');flow.dispose();
});

/** Deterministic IndexedDB transaction port. A put request does not imply transaction COMMIT. */
function indexedPort({gateFirstWrite=false}={}){
  const port=indexedNativePort();if(gateFirstWrite)port.hold('soty-canvas-create-v1','operations');
  return{indexedDB:port.indexedDB,get rows(){return new Map(port.rows('soty-canvas-create-v1','operations').map(row=>[row.key,row]));},get gated(){return Boolean(port.held);},release(){port.release();}};
}
function localRecord({kind='note',blob,createdAt=1}={}){return{schema:'soty.canvas-create.v1',operationId:crypto.randomUUID(),accountId:actor.accountId,projectId:'create-tests',kind,createdAt,version:1,stage:'source',objectRequestId:crypto.randomUUID(),
  sourceIntent:blob?{blob,materialId:crypto.randomUUID()}:{noteId:crypto.randomUUID(),mutationId:crypto.randomUUID(),title:'',body:'Локальный текст'},sourceRef:null,objectId:null,presentation:{mask:{shape:'rect'},fontSize:16},width:340,height:300,placement:null,intentHash:'a'.repeat(64)};}
test('the production IndexedDB journal waits for transaction commit before network; Blob scope and CAS survive store reopen',async t=>{
  const port=indexedPort({gateFirstWrite:true}),store=createCanvasCreateStore(actor.accountId,'create-tests',{origin:'http://localhost',indexedDB:port.indexedDB}),f=fixture(t,{store}),flow=createCanvasCreateFlow(f.options);
  const creating=flow.startNote({body:'Коммит сначала'});await until(()=>port.gated);assert.equal(f.calls.length,0);assert.equal(port.rows.size,0);port.release();const result=await creating;assert.equal(result.status,'saved');flow.dispose();store.close();
  const reopened=createCanvasCreateStore(actor.accountId,'create-tests',{origin:'http://localhost',indexedDB:port.indexedDB});assert.equal((await reopened.get(result.operationId)).stage,'complete');
  const record=localRecord({kind:'image',blob:new Blob(['owned bytes'],{type:'image/png'})});assert.equal(await reopened.put(record,0),true);assert.equal(await reopened.put({...record,version:2},0),false);
  assert.equal(await(await reopened.get(record.operationId)).sourceIntent.blob.text(),'owned bytes');
  const other=createCanvasCreateStore('account_other','create-tests',{origin:'http://localhost',indexedDB:port.indexedDB});assert.deepEqual(await other.list(),[]);other.close();
  const project=createCanvasCreateStore(actor.accountId,'different-project',{origin:'http://localhost',indexedDB:port.indexedDB});assert.deepEqual(await project.list(),[]);project.close();reopened.close();
});
test('the production journal enforces total Blob quota before committing another file and trims only completed receipts',async()=>{
  const port=indexedPort(),store=createCanvasCreateStore(actor.accountId,'create-tests',{origin:'http://localhost',indexedDB:port.indexedDB}),one=localRecord({kind:'image',blob:new Blob([new Uint8Array(14*1024*1024)],{type:'image/png'})});
  assert.equal(await store.put(one,0),true);await assert.rejects(store.put(localRecord({kind:'image',blob:new Blob([new Uint8Array(14*1024*1024)],{type:'image/png'})}),0),error=>error.code==='canvas_create_journal_quota');assert.equal((await store.list()).length,1);await store.remove(one.operationId);
  const pending=localRecord();await store.put(pending,0);
  for(let index=0;index<34;index++){const value=localRecord({createdAt:index+2});await store.put({...value,stage:'complete',sourceIntent:null,sourceRef:crypto.randomUUID(),objectId:crypto.randomUUID(),placement:{shortcutId:crypto.randomUUID(),status:'saved',localDurable:true}},0);}
  const values=await store.list();assert.equal(values.filter(value=>value.stage==='complete').length,CANVAS_CREATE_LIMITS.completed);assert.ok(values.some(value=>value.operationId===pending.operationId));store.close();
});
test('negative transaction control catches a journal promise resolving on put request before durable COMMIT',async()=>{
  const url=new URL('./canvas-create-flow.mjs',import.meta.url),source=readFileSync(url,'utf8'),needle='store.put({scope,key:key(record.operationId),value:record});wrote=true;',broken=source.replace(needle,needle+'resolve(true);');assert.notEqual(broken,source);
  const absolute=broken.replace(/from '(\.\.?\/[^']+)'/gu,(_match,path)=>`from '${new URL(path,url).href}'`),module=await import(`data:text/javascript;base64,${Buffer.from(absolute).toString('base64')}`),port=indexedPort({gateFirstWrite:true});
  const store=module.createCanvasCreateStore(actor.accountId,'create-tests',{origin:'http://localhost',indexedDB:port.indexedDB});let resolved=false;
  const writing=store.put(localRecord(),0).then(()=>{resolved=true;});await until(()=>port.gated);assert.equal(port.rows.size,0);
  assert.throws(()=>assert.equal(resolved,false),error=>error.code==='ERR_ASSERTION');port.release();await writing;store.close();
});
