import test from 'node:test';
import assert from 'node:assert/strict';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
import {createCanvasVaultLocalStore,canvasVaultProfile,deriveCanvasVaultKey,sealCanvasVaultRecord,openCanvasVaultRecord} from './canvas-vault-local.mjs';
import {canvasVaultScopeSlot} from './canvas-vault-scope.mjs';
const accountId='source_store_owner',projectId='source_store_project',password='Synthetic previous local password',nextPassword='Synthetic next local password';
const source={schema:'soty.vault.local-scope.v1',kind:'source',id:'vault_actual_source'};
async function fixture(){const port=indexedNativePort(),store=createCanvasVaultLocalStore(accountId,projectId,{origin:'https://source-store.example',indexedDB:port.indexedDB});
 const profile=slot=>canvasVaultProfile(accountId,projectId,slot),record=async(key,id,purpose='recovery',version=1,body='Exact retained recovery')=>sealCanvasVaultRecord(key,{schema:'soty.vault.local-record.v1',accountId,projectId,slot:key.profile.slot,recordId:id,purpose,version},{body});
 return{port,store,profile,record,async canonical(value=source,objectId='object_A'){const slot=await canvasVaultScopeSlot(value,projectId),state=await store.ensureScopedProfile(value,profile(slot),{objectId});return{...state,key:await deriveCanvasVaultKey(state.profile,password)};}};
}
test('one Source profile admits new C without a new Object profile and rejects old-slot invention',async()=>{const f=await fixture();try{
 const a=await f.canonical(),c=await f.canonical(source,'object_C');assert.equal(a.profile.profileId,c.profile.profileId);assert.equal(a.profile.slot,c.profile.slot);
 await assert.rejects(f.store.ensureProfile(f.profile('late_object_C')),{code:'vault_local_scope_required'});
 const other=await f.canonical({...source,id:'vault_other_source'},'object_D');assert.notEqual(other.profile.profileId,a.profile.profileId);
 const meta=await f.store.inspectScopes();assert.equal(meta.inventory.profiles.length,0);assert.equal(meta.profiles.length,2);assert.equal(f.port.rows('soty-canvas-vault-local-v1','cipher').length,2,'control/link/inventory rows do not consume cipher/content budget');
}finally{f.store.close();}});
test('Source rekey atomically blocks dirty B and every late old-key writer, then commits every recovery row',async()=>{const f=await fixture();try{
 const a=await f.canonical(),history=await f.record(a.key,'history_B'),draft=await f.record(a.key,'dirty_B','draft');await f.store.put(history,0);await f.store.put(draft,0);
 const intent=await f.record(a.key,'rotation_original_A','rotate');await assert.rejects(f.store.beginSourceRekey(a.profile.slot,a.profile.profileId,intent,{inventoryGeneration:a.inventory.generation}),{code:'vault_local_draft_pending'});
 await f.store.remove(draft.recordId,draft.version);const admission=await f.store.beginSourceRekey(a.profile.slot,a.profile.profileId,intent,{inventoryGeneration:a.inventory.generation});
 const c=await f.canonical(source,'object_C');assert.equal(c.control.phase,'rekeying');assert.equal(c.profile.profileId,a.profile.profileId);
 await assert.rejects(f.store.put(await f.record(c.key,'late_C'),0),{code:'vault_local_rotation_pending'});await assert.rejects(f.store.remove(history.recordId,1),{code:'vault_local_rotation_pending'});
 const profile=f.profile(a.profile.slot),key=await deriveCanvasVaultKey(profile,nextPassword),next=await f.record(key,history.recordId,'recovery',2);
 const expected=await f.store.list(a.profile.slot);await assert.rejects(f.store.commitSourceRekey(admission,profile,expected,[],{retire:[{recordId:intent.recordId,version:1}]}),{code:'vault_local_scope_changed'},'missing B is not a successful complete migration');
 assert.equal(await f.store.commitSourceRekey({...admission,generation:admission.generation+1},profile,expected,[next],{retire:[{recordId:intent.recordId,version:1}]}),false);
 assert.equal(await f.store.commitSourceRekey(admission,profile,expected,[next],{retire:[{recordId:intent.recordId,version:1}]}),true);
 assert.equal((await openCanvasVaultRecord(key,await f.store.get(history.recordId))).body,'Exact retained recovery');await assert.rejects(f.store.put(await f.record(a.key,'old_profile_after_commit'),0),{code:'vault_local_scope_changed'});
}finally{f.store.close();}});
test('closed legacy migration includes removed aliases and uses complete profile/record/destination CAS',async()=>{const f=await fixture();try{
 const oldProfiles=await Promise.all(['old_A','removed_B'].map(slot=>f.store.ensureProfile(f.profile(slot)))),oldKeys=await Promise.all(oldProfiles.map(value=>deriveCanvasVaultKey(value,password)));
 const records=await Promise.all(oldKeys.map((key,index)=>f.record(key,'legacy_history_'+index)));for(const value of records)await f.store.put(value,0);
 const state=await f.canonical(),from=oldProfiles.map((value,index)=>({slot:value.slot,profileId:value.profileId,expected:[records[index]]})),next=await Promise.all(records.map(async(value,index)=>{const{cipher,...header}=value;return sealCanvasVaultRecord(state.key,{...header,slot:state.profile.slot,version:2},await openCanvasVaultRecord(oldKeys[index],value));}));
 const args={inventoryGeneration:state.inventory.generation,objectIds:['old_A','removed_B']};
 await assert.rejects(f.store.migrateScopes(state.profile.slot,from,{profileId:state.profile.profileId,expected:[]},next.slice(0,1),args),{code:'vault_local_scope_changed'});
 assert.equal(await f.store.migrateScopes(state.profile.slot,[{...from[0],expected:[]},from[1]],{profileId:state.profile.profileId,expected:[]},next,args),false);
 f.port.abort('soty-canvas-vault-local-v1','cipher');await assert.rejects(f.store.migrateScopes(state.profile.slot,from,{profileId:state.profile.profileId,expected:[]},next,args),{code:'vault_local_unavailable'});assert.equal((await f.store.get(records[1].recordId)).slot,'removed_B');
 assert.equal(await f.store.migrateScopes(state.profile.slot,from,{profileId:state.profile.profileId,expected:[]},next,args),true);
 assert.equal((await f.store.inspectScopes()).inventory.profiles.every(value=>value.state==='migrated'),true);for(const value of records)assert.equal((await openCanvasVaultRecord(state.key,await f.store.get(value.recordId))).body,'Exact retained recovery');
 assert.equal((await f.store.ensureProfile(f.profile('removed_B'))).profileId,state.profile.profileId);
}finally{f.store.close();}});
test('v3 refuses a still-open v2 writer and preserves ciphertext before old v2 reopen becomes impossible',async()=>{const port=indexedNativePort(),name='soty-canvas-vault-local-v1';
 const legacy=await new Promise((resolve,reject)=>{const request=port.indexedDB.open(name,2);request.onupgradeneeded=()=>{const table=request.result.createObjectStore('cipher',{keyPath:'key'});table.createIndex('scope','scope');};request.onsuccess=()=>resolve(request.result);request.onerror=()=>reject(request.error);});legacy.onversionchange=()=>{};
 const store=createCanvasVaultLocalStore(accountId,projectId,{origin:'https://source-store.example',indexedDB:port.indexedDB});await assert.rejects(store.inspectScopes(),{code:'vault_local_unavailable'});assert.equal(port.databases.get(name).version,2);legacy.close();
 assert.equal((await store.inspectScopes()).profiles.length,0);assert.equal(port.databases.get(name).version,3);
 await assert.rejects(new Promise((resolve,reject)=>{const request=port.indexedDB.open(name,2);request.onsuccess=()=>resolve();request.onerror=()=>reject(request.error);}),/VersionError/u);store.close();
});
