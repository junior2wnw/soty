import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {createAppConnectFlow,createAppConnectJournal} from './app-connect-flow.mjs';
import {createCanvasCreateFlow,createCanvasCreateStore,validateCanvasCreateRecord} from './canvas-create-flow.mjs';
import {createAppConnectJournal as legacyJournal} from './test-fixtures/app-connect-journal-v2.mjs';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
import {RUNTIME_PROFILE} from '../../modules/apps/server/schema.mjs';

const accountId='connected_canvas_owner',projectId='connected-canvas',appId='app-'+ 'a'.repeat(32),domainId='dom_'+ 'b'.repeat(32);
const input={name:'Connected one time',address:'localhost:3033/current',hostDeviceId:'owned_host',connectorId:'owned_connector',audience:'private',resultKind:'canvas'};
const clone=value=>structuredClone(value),failure=code=>Object.assign(new Error(code),{code});
const local=()=>{const rows=new Map(),queues=new Map();return{rows,storage:{getItem:key=>rows.get(key)??null,setItem:(key,value)=>rows.set(key,value)},locks:{request(key,callback){const value=(queues.get(key)??Promise.resolve()).catch(()=>{}).then(callback);queues.set(key,value.catch(()=>{}));return value;}}};};
function fixture(t,{loseAt,blockLocal=false,holdRegistration=false,storeFactory=createCanvasCreateStore}={}){
 const own=local(),port=indexedNativePort(),stores=[],controllers=[],calls=[],widgets=new Map(),objects=new Map(),placements=new Map();let active=true,registered=null,lost=false,blocked=blockLocal;
 let releaseRegistration,enterRegistration;const registrationEntered=new Promise(resolve=>{enterRegistration=resolve;});
 const registrationGate=holdRegistration?new Promise(resolve=>{releaseRegistration=resolve;}):null;
 let publication={audience:'private',launchPolicy:'restricted',listed:false,policyEpoch:1,activeDomainIds:[]};
 const createStore=()=>{const store=storeFactory(accountId,projectId,{origin:'https://connect-canvas.invalid',indexedDB:port.indexedDB});stores.push(store);return store;};
 const api={async request(op,args){
  calls.push({op,args:clone(args)});assert.equal(args.expectedAccountId,accountId);
  if(['apps.register','apps.publication.update','canvas.widgets.create','canvas.objects.create'].includes(op)){
   const pending=createAppConnectJournal({accountId,...own}).read(),rows=port.rows('soty-canvas-create-v1','operations').map(row=>row.value);
   assert.ok(pending&&rows.some(row=>row.operationId===pending.id&&row.connection?.requestId===pending.id),'linked Canvas operation must COMMIT before the first remote mutation');
  }
  let result;
  if(op==='apps.register'){enterRegistration();registered??={id:appId,name:args.name,ownerAccountId:accountId,hostDeviceId:args.hostDeviceId,port:args.port,entryPath:args.entryPath,grants:args.grants};result={app:clone(registered)};}
  else if(op==='apps.list')result={apps:registered?[clone(registered)]:[]};
  else if(op==='apps.inspect')result={schema:'soty.app-inspection.v1',app:{id:appId,name:input.name,state:'enabled'},source:{hostDeviceId:input.hostDeviceId,connectorId:input.connectorId,port:3033,entryPath:'/current',revision:1,digest:'c'.repeat(64),profile:RUNTIME_PROFILE},publication:clone(publication),addresses:{aliases:[{id:domainId,state:'bound'}]},actions:{canReserveName:true}};
  else if(op==='apps.directory.resolve')result={items:[{ref:{kind:'app',id:appId},available:true,app:{id:appId,name:input.name,entry:{appId,domainId,path:'/current'}}}]};
  else if(op==='apps.publication.update'){
   publication={...publication,audience:args.audience,launchPolicy:args.launchPolicy,listed:args.listed,activeDomainIds:args.activeDomainIds,policyEpoch:args.expectedPolicyEpoch+1};
   result={requestId:args.requestId,replayed:false,current:{appId,...clone(publication)},receipt:{schema:'soty.app-publication-receipt.v2',namespace:'apps.publication.update.v2',appId,audience:args.audience,policyEpoch:publication.policyEpoch,targetRevision:1,targetDigest:'c'.repeat(64),profile:RUNTIME_PROFILE,launchPolicy:args.launchPolicy,listed:args.listed,activeDomainIds:args.activeDomainIds,exposureAck:args.exposureAck??null,requestKeyHash:createHash('sha256').update(args.requestId).digest('hex')}};
  }else if(op==='canvas.widgets.create'){if(!widgets.has(args.requestId))widgets.set(args.requestId,{widgetId:'widget-'+crypto.randomUUID(),ownerId:accountId,appId:args.appId,state:'active'});result={current:clone(widgets.get(args.requestId))};}
  else if(op==='canvas.objects.create'){if(!objects.has(args.requestId))objects.set(args.requestId,{objectId:'object_'+crypto.randomUUID(),ownerId:accountId,state:'active',binding:clone(args.binding),visibility:clone(args.visibility)});result={current:clone(objects.get(args.requestId))};}
  else throw Error('Unexpected '+op);
  if(op==='apps.register'&&registrationGate)await registrationGate;
  if(loseAt===op&&!lost){lost=true;throw failure('lost_ack');}return result;
 }};
 const create=()=>{
  const actual=createStore(),store=blocked?{...actual,async put(){throw failure('disk_unavailable');}}:actual;
  const canvas=createCanvasCreateFlow({api,accountId,projectId,store,isCurrent:()=>active,place:async(objectId,size)=>{
   if(!placements.has(objectId))placements.set(objectId,{shortcutId:'shortcut_'+crypto.randomUUID(),status:'saved',localDurable:true,size});
   if(loseAt==='placement'&&!lost){lost=true;throw failure('lost_ack');}return placements.get(objectId);
  }});controllers.push(canvas);
  const journal=createAppConnectJournal({accountId,...own,randomId:()=>crypto.randomUUID()});
  const flow=createAppConnectFlow({api,accountId,journal,...own,isCurrent:()=>active,createPlacement(){assert.fail('Canvas intent must not create a legacy app shortcut');},canvas:{projectId,
   prepare:requestId=>canvas.prepareConnectedWidget(requestId,{mask:{shape:'hex'}}),
   complete:result=>canvas.completeConnectedWidget({requestId:result.requestId,appId:result.app.id}),
   settle:requestId=>canvas.settleConnectedWidget(requestId),
  }});return{flow,journal,canvas};
 };
 t.after(()=>{controllers.forEach(flow=>flow.dispose());stores.forEach(store=>store.close());});
 return{create,createStore,calls,widgets,objects,placements,port,own,registrationEntered,unblock(){blocked=false;},releaseRegistration:()=>releaseRegistration?.(),retire(){active=false;}};
}

test('one connect confirmation durably creates exactly one private widget/object/placement and reopens without re-selection',async t=>{
 const f=fixture(t),first=f.create(),result=await first.flow.run(input);assert.equal(result.resultKind,'canvas');assert.equal(result.creation.operationId,result.requestId);assert.equal(result.creation.localDurable,true);assert.equal(f.widgets.size,1);assert.equal(f.objects.size,1);assert.equal(f.placements.size,1);
 assert.equal([...f.objects.values()][0].visibility.audience,'private');assert.equal(first.journal.read().complete,true);assert.equal(first.journal.read().canvasSettled,true);
 assert.equal([...f.port.rows('soty-canvas-create-v1','operations')][0].value.presentation.mask.shape,'hex');
 const again=await f.create().flow.run(input);assert.deepEqual(again.creation,result.creation);assert.equal(f.widgets.size,1);assert.equal(f.objects.size,1);assert.equal(f.placements.size,1);
});
for(const loseAt of ['apps.register','canvas.widgets.create','canvas.objects.create','placement'])test(`reload after ${loseAt} lost ACK continues the original linked IDs, never the chooser or a second placement`,async t=>{
 const f=fixture(t,{loseAt}),first=f.create();await assert.rejects(first.flow.run(input));const requestId=first.journal.read().id;const result=await f.create().flow.run(input);assert.equal(result.requestId,requestId);assert.equal(result.creation.operationId,requestId);assert.equal(f.widgets.size,1);assert.equal(f.objects.size,1);assert.equal(f.placements.size,1);assert.equal(f.calls.filter(call=>call.op==='apps.register').length,1);
});
test('a failed Canvas local commit blocks registration, and retry uses the same connect request',async t=>{
 const f=fixture(t,{blockLocal:true}),first=f.create();await assert.rejects(first.flow.run(input));assert.equal(f.calls.length,0);const id=first.journal.read().id;f.unblock();const result=await f.create().flow.run(input);assert.equal(result.requestId,id);assert.equal(f.widgets.size,1);
});
test('public Source consent remains separate from a private Canvas object; the existing publication receipt is checked',async t=>{
 const f=fixture(t),result=await f.create().flow.run({...input,audience:'public'});assert.equal(result.audience,'public');const publication=f.calls.find(value=>value.op==='apps.publication.update');assert.equal(publication.args.listed,true);assert.equal(publication.args.exposureAck.scope,'whole-port');assert.equal([...f.objects.values()][0].visibility.audience,'private');
});

async function assertLinkedCommitHeld(f,promise){
 const settled=promise.then(()=>{throw Error('Connect settled before the held transaction COMMIT');});
 await Promise.race([f.port.whenHeld(),settled]);
 // The hold is reached after hashing; let already-queued caller continuations expose an early acknowledgement.
 await Promise.race([new Promise(resolve=>setImmediate(resolve)),settled]);
 assert.equal(f.port.held,1);assert.equal(f.calls.length,0);assert.equal(f.port.rows('soty-canvas-create-v1','operations').length,0);
}
test('an IndexedDB put request is insufficient: registration waits for the actual linked transaction COMMIT',async t=>{
 const f=fixture(t);f.port.hold('soty-canvas-create-v1','operations');const controller=f.create(),promise=controller.flow.run(input);
 await assertLinkedCommitHeld(f,promise);
 f.port.release();assert.equal((await promise).resultKind,'canvas');assert.equal(f.widgets.size,1);
});
test('the same held-COMMIT oracle rejects a store that resolves put on request success before transaction completion',async t=>{
 const url=new URL('./canvas-create-flow.mjs',import.meta.url),current=readFileSync(url,'utf8');
 const early=current.replace('store.put({scope,key:key(record.operationId),value:record});wrote=true;',
  'const earlyPutRequest=store.put({scope,key:key(record.operationId),value:record});wrote=true;earlyPutRequest.onsuccess=()=>resolve(wrote);');assert.notEqual(early,current);
 const code=early.replace(/from '(\.?\.?\/[^']+)'/gu,(_match,path)=>"from '"+new URL(path,url).href+"'"),broken=await import('data:text/javascript;base64,'+Buffer.from(code).toString('base64'));
 const f=fixture(t,{storeFactory:broken.createCanvasCreateStore});f.port.hold('soty-canvas-create-v1','operations');const promise=f.create().flow.run(input);
 try{await assert.rejects(assertLinkedCommitHeld(f,promise),/linked Canvas operation must COMMIT before the first remote mutation/u);}
 finally{f.port.release();await promise.catch(()=>{});}
 assert.equal(f.calls.filter(call=>call.op==='apps.register').length,1);assert.equal(f.widgets.size,0);assert.equal(f.objects.size,0);assert.equal(f.placements.size,0);
});
test('late old-account registration ACK may settle its own journal, but creates no widget/object/placement or new-account UI',async t=>{
 const f=fixture(t,{holdRegistration:true}),controller=f.create(),promise=controller.flow.run(input);
 await Promise.race([f.registrationEntered,promise.then(()=>{throw Error('Connect settled before registration entered');})]);assert.equal(f.calls.at(-1).op,'apps.register');f.retire();f.releaseRegistration();await assert.rejects(promise,{code:'field_account_changed'});assert.equal(controller.journal.read().appId,appId);assert.equal(f.widgets.size,0);assert.equal(f.objects.size,0);assert.equal(f.placements.size,0);
});
test('a lost final Connect journal ACK pins the complete Canvas result through normal completed-record pruning',async t=>{
 const f=fixture(t),set=f.own.storage.setItem;let reject=true;
 f.own.storage.setItem=(key,text)=>{const value=JSON.parse(text);if(reject&&value.schema===4&&value.complete){reject=false;throw failure('test_final_ack_disk_full');}return set(key,text);};
 const controller=f.create();await assert.rejects(controller.flow.run(input),{code:'app_connect_storage_unavailable'});const link=controller.journal.read().id,store=f.createStore();assert.equal((await store.get(link)).stage,'settling');assert.equal((await store.get(link)).connection.settled,false);
 // Rollback control: a writer that refuses the new record schema still has
 // the historical complete-only GC. It must not discard an unacknowledged link.
 const url=new URL('./canvas-create-flow.mjs',import.meta.url),current=readFileSync(url,'utf8'),oldAdmission=current.replace('[SCHEMA,CONNECTION_SCHEMA].includes(value.schema)','value.schema===SCHEMA');assert.notEqual(oldAdmission,current);
 const code=oldAdmission.replace(/from '(\.?\.?\/[^']+)'/gu,(_match,path)=>"from '"+new URL(path,url).href+"'"),legacy=await import('data:text/javascript;base64,'+Buffer.from(code).toString('base64'));
 const oldWriter=legacy.createCanvasCreateStore(accountId,projectId,{origin:'https://connect-canvas.invalid',indexedDB:f.port.indexedDB});t.after(()=>oldWriter.close());assert.throws(()=>legacy.validateCanvasCreateRecord(awaitableRow(),accountId,projectId));
 function awaitableRow(){return f.port.rows('soty-canvas-create-v1','operations').find(row=>row.value.operationId===link).value;}
 for(let index=0;index<34;index++){
  const record={schema:'soty.canvas-create.v1',operationId:'completed_'+index,accountId,projectId,kind:'note',createdAt:Date.now()+index+10000,version:1,stage:'complete',objectRequestId:'request_'+index,sourceIntent:null,sourceRef:'note_'+index,objectId:'object_'+index,presentation:{mask:{shape:'rect'},fontSize:16},width:340,height:300,placement:{shortcutId:'shortcut_'+index,status:'saved',localDurable:true},intentHash:'d'.repeat(64)};
  assert.equal(await oldWriter.put(record,0),true);
 }
 assert.equal((await store.get(link)).connection.settled,false,'ordinary completed GC cannot discard the unacknowledged link');const result=await f.create().flow.run(input);assert.equal(result.requestId,link);assert.equal(f.widgets.size,1);assert.equal(f.objects.size,1);assert.equal(f.placements.size,1);assert.equal(f.create().journal.read().canvasSettled,true);
 assert.equal(await store.get(link),null,'the old completed result may be pruned only after the durable Connect ACK');const replay=await f.create().flow.run(input);assert.deepEqual(replay.creation,result.creation);assert.equal(f.widgets.size,1);
});
test('a v1 native draft remains byte-for-byte unchanged when the new reader opens its existing IndexedDB store',async t=>{
 const f=fixture(t),store=f.createStore(),record={schema:'soty.canvas-create.v1',operationId:'old_note_operation',accountId,projectId,kind:'note',createdAt:1,version:1,stage:'source',objectRequestId:'old_note_object_request',sourceIntent:{noteId:'old_note_id',mutationId:'old_note_mutation',title:'Retained',body:'Retained private local draft'},sourceRef:null,objectId:null,presentation:{mask:{shape:'ellipse'},fontSize:19},width:340,height:300,placement:null,intentHash:'e'.repeat(64)};
 assert.equal(await store.put(record,0),true);const before=JSON.stringify(f.port.rows('soty-canvas-create-v1','operations'));assert.deepEqual(validateCanvasCreateRecord(await f.createStore().get(record.operationId),accountId,projectId),record);assert.equal(JSON.stringify(f.port.rows('soty-canvas-create-v1','operations')),before);assert.equal(f.calls.length,0);
});
test('legacy journals and Canvas records are read without reissue; unknown future formats and old readers fail closed',async t=>{
 for(const source of [{...input,resultKind:undefined},{...input,resultKind:undefined,address:'192.168.1.2:3033/current'},{...input,resultKind:'selection'}]){
  const own=local(),journal=createAppConnectJournal({accountId,...own}),record=await journal.begin(source),raw=[...own.rows.values()][0];assert.ok([1,2,3].includes(record.schema));assert.deepEqual(createAppConnectJournal({accountId,...own}).read(),record);assert.equal([...own.rows.values()][0],raw);
 }
 const f=fixture(t,{loseAt:'canvas.widgets.create'}),controller=f.create();await assert.rejects(controller.flow.run(input));const raw=f.own.storage.getItem('soty.app-connect.v1:'+accountId),record=controller.journal.read();assert.equal(record.schema,4);assert.throws(()=>legacyJournal({accountId,...f.own}).read(),{code:'app_connect_journal_corrupt'});assert.equal(f.own.storage.getItem('soty.app-connect.v1:'+accountId),raw);
 const row=f.port.rows('soty-canvas-create-v1','operations')[0].value;assert.equal(row.schema,'soty.canvas-create.v2');assert.equal(validateCanvasCreateRecord(row,accountId,projectId).connection.requestId,record.id);
 const known=JSON.parse(raw);known.schema=5;f.own.storage.setItem('soty.app-connect.v1:'+accountId,JSON.stringify(known));const unknown=f.own.storage.getItem('soty.app-connect.v1:'+accountId);assert.throws(()=>controller.journal.read(),{code:'app_connect_journal_corrupt'});assert.equal(f.own.storage.getItem('soty.app-connect.v1:'+accountId),unknown);
 assert.throws(()=>validateCanvasCreateRecord({...row,schema:'soty.canvas-create.v3'},accountId,projectId));assert.equal(row.schema,'soty.canvas-create.v2');
});
