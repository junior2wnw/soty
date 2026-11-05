import test from 'node:test';
import assert from 'node:assert/strict';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
import {canvasVaultProfile,deriveCanvasVaultKey,sealCanvasVaultRecord,openCanvasVaultRecord,createCanvasVaultLocalStore,CANVAS_VAULT_LOCAL_LIMITS} from './canvas-vault-local.mjs';

const accountId='creation_recovery_owner',projectId='soty',slot='creation_recovery_object',password='Synthetic recovery-only password';
async function fixture(){
 const port=indexedNativePort(),store=createCanvasVaultLocalStore(accountId,projectId,{origin:'https://synthetic.example',indexedDB:port.indexedDB});
 const profile=await store.ensureProfile(canvasVaultProfile(accountId,projectId,slot)),session=await deriveCanvasVaultKey(profile,password);
 const record=(id,purpose,payload,key=session,version=1)=>sealCanvasVaultRecord(key,{schema:'soty.vault.local-record.v1',accountId,projectId,slot,recordId:id,purpose,version},payload);
 return{port,store,profile,session,record};
}
test('encrypted recovery uses its separate bounded allowance without raising ordinary draft/create limits',async()=>{
 const f=await fixture(),payload={schema:'synthetic.recovery.part',bytes:'x'.repeat(700000)};
 try{
  for(let index=0;index<10;index++)assert.equal(await f.store.put(await f.record('recovery_'+index,'recovery',payload),0),true);
  const recoveries=await f.store.list(slot);assert.equal(recoveries.length,10);
  assert.ok(new TextEncoder().encode(JSON.stringify(recoveries)).length>CANVAS_VAULT_LOCAL_LIMITS.totalBytes);
  assert.equal(CANVAS_VAULT_LOCAL_LIMITS.totalBytes,8*1024*1024);assert.equal(CANVAS_VAULT_LOCAL_LIMITS.payloadBytes,1024*1024);
  for(let index=0;index<8;index++)assert.equal(await f.store.put(await f.record('ordinary_'+index,'draft',payload),0),true);
  await assert.rejects(f.store.put(await f.record('ordinary_8','create',payload),0),{code:'vault_local_quota'});
  assert.equal((await f.store.list(slot)).length,18);
  const rows=JSON.stringify(f.port.rows('soty-canvas-vault-local-v1','cipher'));assert.ok(!rows.includes(payload.bytes.slice(0,300)));
 }finally{f.store.close();}
});
test('recovery and ordinary records rotate together in the same atomic profile COMMIT',async()=>{
 const f=await fixture();try{
  for(const [id,purpose]of [['recovery_a','recovery'],['ordinary_a','draft']])assert.equal(await f.store.put(await f.record(id,purpose,{body:id+' synthetic text'}),0),true);
  const before=await f.store.list(slot),candidate=canvasVaultProfile(accountId,projectId,slot),nextKey=await deriveCanvasVaultKey(candidate,password+' next'),next=[];
  for(const record of before)next.push(await f.record(record.recordId,record.purpose,await openCanvasVaultRecord(f.session,record),nextKey,record.version+1));
  f.port.hold('soty-canvas-vault-local-v1');let resolved=false;
  const change=f.store.replaceProfile(f.profile.profileId,candidate,before,next).then(value=>{resolved=true;return value;});
  for(let turn=0;turn<100&&!f.port.held;turn++)await new Promise(resolve=>setImmediate(resolve));
  assert.equal(f.port.held,1);assert.equal(resolved,false,'a request success is not the profile COMMIT');
  assert.equal(f.port.rows('soty-canvas-vault-local-v1','cipher').find(row=>row.value.schema==='soty.vault.local-key.v1').value.profileId,f.profile.profileId);
  f.port.release();assert.equal(await change,true);const after=await f.store.list(slot);assert.equal(after.length,2);
  for(const record of after){assert.equal(record.cipher.profileId,candidate.profileId);assert.match((await openCanvasVaultRecord(nextKey,record)).body,/synthetic text/u);await assert.rejects(openCanvasVaultRecord(f.session,record));}
 }finally{f.store.close();}
});
test('recovery cannot overwrite another bucket at the same record identity or commit a stale profile rotation',async()=>{
 const f=await fixture();try{
  const original=await f.record('fixed_record','draft',{body:'ordinary'});assert.equal(await f.store.put(original,0),true);
  await assert.rejects(f.store.put(await f.record('fixed_record','recovery',{body:'replacement'},f.session,2),1),{code:'vault_local_scope_changed'});
  const expected=await f.store.list(slot),candidate=canvasVaultProfile(accountId,projectId,slot),nextKey=await deriveCanvasVaultKey(candidate,password+' next'),next=[];
  for(const row of expected)next.push(await f.record(row.recordId,row.purpose,await openCanvasVaultRecord(f.session,row),nextKey,row.version+1));
  assert.equal(await f.store.put(await f.record('late_recovery','recovery',{body:'later'}),0),true);
  assert.equal(await f.store.replaceProfile(f.profile.profileId,candidate,expected,next),false);
  assert.equal((await f.store.list(slot)).length,2);assert.equal((await f.store.ensureProfile(candidate)).profileId,f.profile.profileId);
 }finally{f.store.close();}
});
test('same-version replacements, changed-slot records and malformed rotation sets cannot defeat exact recovery CAS',async()=>{
 const f=await fixture();try{
  const original=await f.record('exact_record','recovery',{body:'original'});assert.equal(await f.store.put(original,0),true);
  await assert.rejects(f.store.put(await f.record('exact_record','recovery',{body:'changed'},f.session,1),1),{code:'vault_local_scope_changed'});
  const differentProfile=await f.store.ensureProfile(canvasVaultProfile(accountId,projectId,'different_object')),differentKey=await deriveCanvasVaultKey(differentProfile,password);
  const changedSlot=await sealCanvasVaultRecord(differentKey,{schema:'soty.vault.local-record.v1',accountId,projectId,slot:'different_object',recordId:'exact_record',purpose:'recovery',version:2},{body:'moved'});
  await assert.rejects(f.store.put(changedSlot,1),{code:'vault_local_scope_changed'});
  const before=await f.store.list(slot),candidate=canvasVaultProfile(accountId,projectId,slot),nextKey=await deriveCanvasVaultKey(candidate,password+' next');
  const next=await f.record('exact_record','recovery',{body:'original'},nextKey,2);
  await assert.rejects(f.store.replaceProfile(f.profile.profileId,candidate,before,[next,next]),{code:'vault_local_scope_changed'});
  await assert.rejects(f.store.replaceProfile(f.profile.profileId,candidate,before,[await f.record('unknown_record','recovery',{body:'other'},nextKey,2)]),{code:'vault_local_scope_changed'});
  await assert.rejects(f.store.replaceProfile(f.profile.profileId,candidate,before,[await f.record('exact_record','draft',{body:'original'},nextKey,2)]),{code:'vault_local_scope_changed'});
  assert.deepEqual(await f.store.get('exact_record'),original);assert.equal((await f.store.ensureProfile(candidate)).profileId,f.profile.profileId);
 }finally{f.store.close();}
});
test('rotation cannot omit an archived recovery; only its exact acknowledged rotation intent may retire',async()=>{
 const f=await fixture();try{
  assert.equal(await f.store.put(await f.record('kept_recovery','recovery',{body:'only old copy'}),0),true);
  assert.equal(await f.store.put(await f.record('current_rotation','rotate',{requestId:'exact_rotation'}),0),true);
  const before=await f.store.list(slot),candidate=canvasVaultProfile(accountId,projectId,slot),key=await deriveCanvasVaultKey(candidate,password+' next');
  await assert.rejects(f.store.replaceProfile(f.profile.profileId,candidate,before,[]),{code:'vault_local_scope_changed'});
  const recovery=before.find(value=>value.recordId==='kept_recovery'),rotation=before.find(value=>value.recordId==='current_rotation');
  const next=[await f.record(recovery.recordId,recovery.purpose,await openCanvasVaultRecord(f.session,recovery),key,2)];
  await assert.rejects(f.store.replaceProfile(f.profile.profileId,candidate,before,next),{code:'vault_local_scope_changed'});
  await assert.rejects(f.store.replaceProfile(f.profile.profileId,candidate,before,next,{retire:[{recordId:rotation.recordId,version:rotation.version+1}]}),{code:'vault_local_scope_changed'});
  assert.equal(await f.store.replaceProfile(f.profile.profileId,candidate,before,next,{retire:[{recordId:rotation.recordId,version:rotation.version}]}),true);
  assert.equal((await f.store.list(slot)).length,1);assert.equal((await openCanvasVaultRecord(key,await f.store.get(recovery.recordId))).body,'only old copy');
 }finally{f.store.close();}
});
test('schema2 excludes an old omit-capable writer before recovery while preserving existing v1 ciphertext',async()=>{
 const port=indexedNativePort(),origin='https://synthetic.example',scope=JSON.stringify([origin,projectId,accountId]);
 const profile=canvasVaultProfile(accountId,projectId,slot),session=await deriveCanvasVaultKey(profile,password),record=await sealCanvasVaultRecord(session,{schema:'soty.vault.local-record.v1',accountId,projectId,slot,recordId:'old_cipher',purpose:'draft',version:1},{body:'preserved existing cipher'});
 const old=await new Promise((resolve,reject)=>{const req=port.indexedDB.open('soty-canvas-vault-local-v1',1);req.onupgradeneeded=()=>{const store=req.result.createObjectStore('cipher',{keyPath:'key'});store.createIndex('scope','scope');};req.onsuccess=()=>resolve(req.result);req.onerror=()=>reject(req.error);});
 await new Promise((resolve,reject)=>{const tx=old.transaction('cipher','readwrite'),store=tx.objectStore('cipher');for(const[kind,id,value]of [['profile',slot,profile],['record',record.recordId,record]])store.put({scope,key:JSON.stringify([scope,kind,id]),value});tx.oncomplete=resolve;tx.onerror=tx.onabort=reject;});
 old.onversionchange=()=>{};const store=createCanvasVaultLocalStore(accountId,projectId,{origin,indexedDB:port.indexedDB});
 try{
  await assert.rejects(store.list(slot),{code:'vault_local_unavailable'});assert.equal(port.rows('soty-canvas-vault-local-v1','cipher').length,2);
  old.close();assert.equal((await openCanvasVaultRecord(session,(await store.list(slot))[0])).body,'preserved existing cipher');
  await assert.rejects(new Promise((resolve,reject)=>{const req=port.indexedDB.open('soty-canvas-vault-local-v1',1);req.onsuccess=()=>{req.result.close();resolve();};req.onerror=()=>reject(req.error);}),/VersionError/u);
 }finally{old.close();store.close();}
});
