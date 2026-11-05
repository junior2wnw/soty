import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createCanvasCreateStore,createCanvasCreateFlow} from './canvas-create-flow.mjs';
import {createNativeDraftStore} from '../../modules/notes/browser/native-local.mjs';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';

const accountId='creation_fence_owner',projectId='soty',origin='https://creation-fence.example';
const makeRecord=()=>({schema:'soty.canvas-create.v1',operationId:crypto.randomUUID(),accountId,projectId,kind:'note',createdAt:1,version:1,stage:'source',objectRequestId:crypto.randomUUID(),
 sourceIntent:{noteId:crypto.randomUUID(),mutationId:crypto.randomUUID(),expectedRevision:0,title:'Synthetic lost ACK',body:'Synthetic old creation body',items:[],color:'plain',pinned:false,state:'active'},sourceRef:null,objectId:null,presentation:{mask:{shape:'rect'},fontSize:16},width:340,height:300,placement:null,intentHash:'b'.repeat(64)});
async function fixture(){
 const port=indexedNativePort(),store=createCanvasCreateStore(accountId,projectId,{origin,indexedDB:port.indexedDB}),drafts=createNativeDraftStore(accountId,projectId,{origin,indexedDB:port.indexedDB});
 const row=makeRecord(),source={providerId:'soty.notes',ref:row.sourceIntent.noteId};
 const seal=async()=>{await drafts.native.mark({...source,operationId:'source_conversion',state:'sealed'});if(store.sealSource)await store.sealSource(source,'source_conversion');};
 return{port,store,drafts,row,source,seal};
}
test('a sealed Native Source rejects late old creation writes even under a new operation ID',async()=>{
 const f=await fixture();try{
  await f.seal();assert.ok(await f.store.nativeGate(f.source));
  await assert.rejects(f.store.put(f.row,0),{code:'canvas_create_native_recovery_pending'});
  await assert.rejects(f.store.put({...f.row,operationId:crypto.randomUUID()},0),{code:'canvas_create_native_recovery_pending'});
  assert.deepEqual(await f.store.list(),[]);
 }finally{f.store.close();}
});
test('retiring the disk copy cannot let a retained old flow resurrect its plaintext before the network fence',async()=>{
 const f=await fixture();let captured;
 const flow=createCanvasCreateFlow({accountId,projectId,isCurrent:()=>true,store:f.store,place:async()=>{throw Error('unexpected placement');},api:{async request(_op,args){captured=args;throw Object.assign(Error('lost ACK'),{code:'network_error'});}}});
 try{
  await assert.rejects(flow.startNote({body:'Synthetic retained creation draft'}),{code:'network_error'});
  const [row]=await f.store.list(),source={providerId:'soty.notes',ref:captured.noteId};
  await f.drafts.native.mark({...source,operationId:'source_conversion',state:'sealed'});if(f.store.sealSource)await f.store.sealSource(source,'source_conversion');
  await f.store.remove(row.operationId); // Control for an exact retirement after encrypted COMMIT, not a product recovery implementation.
  await assert.rejects(flow.resume(row.operationId));
  assert.equal(await f.store.get(row.operationId),null,'the Source fence must precede local re-persistence, not merely the next RPC');
 }finally{flow.dispose();f.store.close();}
});
test('retirement is atomic with its Source fence, exact versions and every matching operation',async()=>{
 const f=await fixture();try{
  await f.store.put(f.row,0);const newer={...f.row,version:2,stage:'object',sourceIntent:null,sourceRef:f.source.ref};await f.store.put(newer,1);
  const fence=await f.store.sealSource(f.source,'source_conversion');
  assert.equal(await f.store.retireSourceIntents(f.source,'wrong_epoch',[newer]),false);
  assert.equal(await f.store.retireSourceIntents(f.source,fence.epoch,[f.row]),false);
  assert.deepEqual(await f.store.get(f.row.operationId),newer);
  f.port.hold('soty-canvas-create-v1','operations');let resolved=false;
  const retiring=f.store.retireSourceIntents(f.source,fence.epoch,[newer]).then(value=>{resolved=true;return value;});
  for(let turn=0;turn<100&&!f.port.held;turn++)await new Promise(resolve=>setImmediate(resolve));
  assert.equal(f.port.held,1);assert.equal(resolved,false);assert.equal(f.port.rows('soty-canvas-create-v1','operations').length,1);
  f.port.release();assert.equal(await retiring,true);assert.equal(await f.store.get(f.row.operationId),null);
  assert.equal((await f.store.sourceFence(f.source)).state,'sealed');
  assert.equal(await f.store.cancelSourceFence(f.source,fence.epoch),false);
  await assert.rejects(f.store.put(f.row,0),{code:'canvas_create_native_recovery_pending'});
 }finally{f.store.close();}
});
test('an operation cannot escape a sealed Source by changing its binding or by reusing a version',async()=>{
 const f=await fixture();try{
  await f.store.put(f.row,0);await f.seal();
  const escaped={...f.row,version:2,stage:'object',sourceIntent:null,sourceRef:crypto.randomUUID()};
  await assert.rejects(f.store.put(escaped,1),{code:'canvas_create_journal_conflict'});
  assert.deepEqual(await f.store.get(f.row.operationId),f.row);
 }finally{f.store.close();}
});
test('a blocked schema2 upgrade has no old-reader fallback and preserves the v1 operation',async()=>{
 const f=await fixture();f.store.close();
 const opened=await new Promise((resolve,reject)=>{const req=f.port.indexedDB.open('soty-canvas-create-v1',1);req.onupgradeneeded=()=>{const store=req.result.createObjectStore('operations',{keyPath:'key'});store.createIndex('scope','scope');};req.onsuccess=()=>resolve(req.result);req.onerror=()=>reject(req.error);});
 const scope=JSON.stringify([origin,projectId,accountId]);await new Promise((resolve,reject)=>{const tx=opened.transaction('operations','readwrite');tx.objectStore('operations').put({scope,key:JSON.stringify([scope,f.row.operationId]),value:f.row});tx.oncomplete=resolve;tx.onerror=tx.onabort=reject;});
 opened.onversionchange=()=>{};
 const store=createCanvasCreateStore(accountId,projectId,{origin,indexedDB:f.port.indexedDB});
 try{
  await assert.rejects(store.sealSource(f.source,'source_conversion'),{code:'canvas_create_local_unavailable'});
  assert.equal(f.port.rows('soty-canvas-create-v1','operations')[0].value.sourceIntent.body,f.row.sourceIntent.body);
  opened.close();const gate=await store.sealSource(f.source,'source_conversion');assert.ok(gate.epoch);assert.deepEqual(await store.get(f.row.operationId),f.row);
 }finally{opened.close();store.close();}
});
test('committed retirement from another store scrubs the retained live CreationFlow instead of leaving a ghost pending copy',async()=>{
 const f=await fixture(),other=createCanvasCreateStore(accountId,projectId,{origin,indexedDB:f.port.indexedDB});let calls=0,current=true;
 const flow=createCanvasCreateFlow({accountId,projectId,isCurrent:()=>current,store:f.store,place:async()=>{throw Error('unexpected placement');},api:{async request(){calls++;throw Object.assign(Error('lost ACK'),{code:'network_error'});}}});
 try{
  await assert.rejects(flow.startNote({body:'Synthetic only old creation copy'}));
  const [row]=await other.list(),source={providerId:'soty.notes',ref:row.sourceIntent.noteId};
  const fence=await other.sealSource(source,'source_conversion');
  assert.equal((await flow.listPending()).length,1,'pending data remains before authenticated retirement');
  current=false;assert.equal(await other.retireSourceIntents(source,fence.epoch,[row]),true);current=true; // Store contract only; coordinator must first authenticate cipher COMMIT.
  assert.deepEqual(await flow.listPending(),[],'a proven retired Source cannot remain as a plaintext RAM creation intent');
  await assert.rejects(flow.resume(row.operationId),{code:'canvas_create_journal_missing'});assert.equal(calls,1);
 }finally{flow.dispose();other.close();f.store.close();}
});
async function forgedRetirementScenario(createStore=createCanvasCreateStore,createFlow=createCanvasCreateFlow){
 const port=indexedNativePort(),previousFactory=globalThis.indexedDB,previousChannel=globalThis.BroadcastChannel,channels=[];
 class Channel{constructor(name){this.name=name;channels.push(this);}postMessage(data){for(const channel of channels)if(channel!==this&&channel.name===this.name&&!channel.closed)queueMicrotask(()=>channel.onmessage?.({data:structuredClone(data)}));}close(){this.closed=true;}}
 globalThis.indexedDB=port.indexedDB;globalThis.BroadcastChannel=Channel;
 const store=createStore(accountId,projectId,{origin,indexedDB:port.indexedDB});let captured;
 const failingStore={...store,async put(value){captured=structuredClone(value);throw Error('synthetic local quota');}};
 const flow=createFlow({accountId,projectId,isCurrent:()=>true,store:failingStore,place:async()=>{throw Error('unexpected placement');},api:{async request(){throw Error('initial quota must prevent network');}}});
 let sender;
 try{
  await assert.rejects(flow.startNote({body:'Synthetic must survive a forged hint'}));const row=captured,source={providerId:'soty.notes',ref:row.sourceIntent.noteId};assert.deepEqual(await store.list(),[]);
  const fence=await store.sealSource(source,'source_conversion');sender=new Channel('soty-canvas-creation:'+JSON.stringify([origin,projectId,accountId]));
  sender.postMessage({...source,epoch:fence.epoch});for(let turn=0;turn<8;turn++)await new Promise(resolve=>setImmediate(resolve));
  assert.equal((await flow.listPending()).length,1,'a hint before actual sealed retirement must not discard the only old creation copy');
  assert.equal(await store.get(row.operationId),null,'failed initial COMMIT left only the live private intent');
 }finally{flow.dispose();store.close();sender?.close();globalThis.indexedDB=previousFactory;globalThis.BroadcastChannel=previousChannel;}
}
test('a cross-tab hint cannot retire live RAM before the actual Source transaction is sealed',()=>forgedRetirementScenario());
test('negative control: trusting a cross-tab hint fails the same retained-copy oracle',async()=>{
 const url=new URL('./canvas-create-flow.mjs',import.meta.url),source=readFileSync(url,'utf8'),needle="if(!closed&&gate?.state==='sealed'&&gate.epoch===source.epoch)emitRetired";
 assert.ok(source.includes(needle));const broken=source.replace(needle,'if(!closed)emitRetired').replace(/from '(\.\.?\/[^']+)'/gu,(_match,path)=>`from '${new URL(path,url).href}'`);
 const module=await import(`data:text/javascript;base64,${Buffer.from(broken).toString('base64')}`);
 await assert.rejects(forgedRetirementScenario(module.createCanvasCreateStore,module.createCanvasCreateFlow),/a hint before actual sealed retirement/);
});
