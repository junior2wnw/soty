import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createNativeDraftStore,nativeDraftDirty} from '../browser/native-local.mjs';
import {indexedNativePort} from './indexed-native-port.mjs';

const accountId='native_cache_owner',projectId='soty',origin='https://soty.example',noteId='note_original_123';
const draft=(body='Synthetic unsaved secret',branchId='branch_original_123',savedAt=1)=>({note:{noteId,title:'Private title',body,items:[],preview:body,pinned:false,color:'plain',state:'active',revision:7,createdAt:1,updatedAt:1},branchId,generation:1,committedGeneration:0,pending:null,conflict:false,savedAt});
const storeFor=(port,account=accountId)=>createNativeDraftStore(account,projectId,{origin,indexedDB:port.indexedDB});
const turn=()=>new Promise(resolve=>setImmediate(resolve));
const until=async predicate=>{for(let index=0;index<100;index++){if(predicate())return;await turn();}assert.fail('Expected IDB boundary not reached');};

test('production v2 draft store hides quarantined branches, preserves them until conditional purge, and rejects late plaintext writes',async()=>{
  const port=indexedNativePort(),store=storeFor(port),value=draft();await store.put(value);assert.equal((await store.list()).length,1);
  const gate=await store.native.mark({providerId:'soty.notes',ref:noteId,operationId:'convert_123'});assert.deepEqual(await store.list(),[]);
  assert.equal((await store.native.inspect(noteId))[0].note.body,value.note.body);
  const late={...value,savedAt:2,generation:2,note:{...value.note,body:'Newer private character'}};await store.put(late);
  assert.equal(await store.native.finalize({providerId:'soty.notes',ref:noteId,epoch:gate.epoch,expected:[value]}),false,'Old snapshots cannot delete a newer independently queued branch');
  assert.equal((await store.native.inspect(noteId))[0].note.body,late.note.body);
  assert.equal(await store.native.finalize({providerId:'soty.notes',ref:noteId,epoch:gate.epoch,expected:[late]}),true);
  await assert.rejects(store.put(value),/notes_note_sealed/u);assert.deepEqual(await store.native.inspect(noteId),[]);
  const reopened=storeFor(port);assert.deepEqual(await reopened.list(),[]);assert.equal((await reopened.native.gate('soty.notes',noteId)).state,'sealed');
  assert.equal(await storeFor(port,'other_cache_owner').native.gate('soty.notes',noteId),null);
});

test('a pending gate waits for durable COMMIT and cannot become a complete fence on abort',async()=>{
  const port=indexedNativePort(),store=storeFor(port);await store.put(draft());port.hold('soty-notes-drafts-v1','nativeProtection');let resolved=false;
  const marking=store.native.mark({providerId:'soty.notes',ref:noteId,operationId:'convert_456'}).then(value=>{resolved=true;return value;});await until(()=>port.held);
  assert.equal(resolved,false);assert.equal(port.rows('soty-notes-drafts-v1','nativeProtection').length,0);port.release();const gate=await marking;
  port.abort('soty-notes-drafts-v1','nativeProtection');await assert.rejects(store.native.finalize({providerId:'soty.notes',ref:noteId,epoch:gate.epoch,expected:await store.native.inspect(noteId)}));
  assert.equal((await store.native.inspect(noteId)).length,1);assert.equal((await store.native.gate('soty.notes',noteId)).state,'pending');
});

test('a real schema1 draft row survives upgrade; an old reader that will not close blocks protection instead of falling back',async()=>{
  const port=indexedNativePort(),request=port.indexedDB.open('soty-notes-drafts-v1',1);request.onupgradeneeded=()=>{const store=request.result.createObjectStore('drafts',{keyPath:'key'});store.createIndex('scope','scope');};
  const legacy=await new Promise((resolve,reject)=>{request.onsuccess=()=>resolve(request.result);request.onerror=reject;});
  const scope=JSON.stringify([origin,projectId,accountId]),row={...draft(),scope,key:JSON.stringify([scope,noteId,'branch_original_123']),bytes:300};
  await new Promise((resolve,reject)=>{const tx=legacy.transaction('drafts','readwrite');tx.objectStore('drafts').put(row);tx.oncomplete=resolve;tx.onabort=reject;});
  const blocked=storeFor(port);await assert.rejects(blocked.list(),/notes_local_unavailable/u);assert.equal(port.databases.get('soty-notes-drafts-v1').version,1);
  legacy.close();const compatible=storeFor(port);assert.equal((await compatible.list())[0].note.body,row.note.body);assert.equal(port.databases.get('soty-notes-drafts-v1').version,2);
  assert.equal(nativeDraftDirty(row),true);
});

test('all in-process aliases are suspended together; cancellation restores access only to a pending unchanged gate',async()=>{
  const port=indexedNativePort(),one=storeFor(port),two=storeFor(port),seen=[];
  const register=(store,label)=>store.native.register(noteId,{suspend(){seen.push(label);},async flush(){},async flushLocal(){},snapshot:()=>draft(),releaseRecovered(){},onGate:state=>seen.push(label+':'+state)});
  const offOne=register(one,'one'),offTwo=register(two,'two');one.native.suspend(noteId);assert.deepEqual(seen.slice(0,2),['one','two']);
  const gate=await one.native.mark({providerId:'soty.notes',ref:noteId,operationId:'convert_alias'});assert.ok(seen.includes('one:pending')&&seen.includes('two:pending'));
  assert.equal(await two.native.cancel({providerId:'soty.notes',ref:noteId,epoch:'stale'}),false);assert.equal(await two.native.cancel({providerId:'soty.notes',ref:noteId,epoch:gate.epoch}),true);
  offOne();offTwo();
});

test('negative control: bypassing the late-write fence fails the original no-plaintext-after-seal criterion',async()=>{
  const url=new URL('../browser/native-local.mjs',import.meta.url),source=readFileSync(url,'utf8'),broken=source.replace("check(fence.result?.state!=='sealed','notes_note_sealed');",'');assert.notEqual(broken,source);
  const module=await import(`data:text/javascript;base64,${Buffer.from(broken).toString('base64')}`),port=indexedNativePort(),store=module.createNativeDraftStore(accountId,projectId,{origin,indexedDB:port.indexedDB});
  const gate=await store.native.mark({providerId:'soty.notes',ref:noteId,operationId:'convert_broken'});await store.native.finalize({providerId:'soty.notes',ref:noteId,epoch:gate.epoch});await store.put(draft());
  assert.equal((await store.native.inspect(noteId)).length,1);assert.throws(()=>assert.equal(port.rows('soty-notes-drafts-v1','drafts').length,0));
});
