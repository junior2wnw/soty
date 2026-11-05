import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {independentVaultSdkFixture,acceptancePassword} from './test-support/independent-vault-sdk.mjs';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
import {createCanvasVaultClient,canvasVaultNoteContent} from './canvas-vault-client.mjs';
import {createCanvasVaultLocalStore,canvasVaultProfile,deriveCanvasVaultKey,sealCanvasVaultRecord,openCanvasVaultRecord} from './canvas-vault-local.mjs';
import {seedVaultLegacyV2} from './test-support/vault-legacy-v2.mjs';
import {canvasVaultScopeSlot} from './canvas-vault-scope.mjs';
const nextPassword='Source rotation synthetic next password 20261104';
const document=body=>canvasVaultNoteContent({title:'Canonical content',body,revision:1,createdAt:1,updatedAt:1});
const objectArgs=binding=>({requestId:randomUUID(),binding,presentation:{mask:{shape:'rect'},fontSize:16},visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'password'}});

test('actual Root new alias C during A rotation shares the durable Source fence instead of creating an old-password Object profile',async t=>{
 const f=await independentVaultSdkFixture(t);let reached,release;const entering=new Promise(resolve=>{reached=resolve;}),gate=new Promise(resolve=>{release=resolve;}),calls=[];
 const a=createCanvasVaultClient({api:{async request(operation,args){calls.push({operation,args});if(operation==='canvas.vault.rotate'){reached();await gate;}return f.raw(operation,args);}},accountId:f.accountId,projectId:f.projectId,store:f.cipher,isCurrent:()=>true});t.after(()=>a.dispose());
 await a.unlock(f.a.objectId,acceptancePassword);const rotating=a.rotate(f.a.objectId,{oldPassword:acceptancePassword,newPassword:nextPassword,requestId:'source_new_C_original_A'});await entering;
 const binding=(await f.raw('canvas.objects.get',{objectId:f.a.objectId})).object.binding,c=(await f.raw('canvas.objects.create',objectArgs(binding))).current,cSdk=f.client('C');
 await assert.rejects(cSdk.unlock(c.objectId,acceptancePassword),{code:'vault_local_rotation_pending'});assert.equal(cSdk.isUnlocked(c.objectId),false);
 const before=await f.cipher.inspectScopes();assert.equal(before.profiles.length,1);assert.equal(before.controls[0].phase,'rekeying');assert.equal(before.profiles.some(value=>value.slot===c.objectId),false);assert.equal(before.inventory.profiles.length,0);
 release();await rotating;await cSdk.unlock(c.objectId,nextPassword);assert.equal((await cSdk.readNote(c.objectId)).body,'Initial Source revision one');assert.equal((await f.cipher.inspectScopes()).profiles.length,1);
 assert.equal(calls.filter(value=>value.operation==='canvas.vault.rotate').length,1);assert.equal(calls.find(value=>value.operation==='canvas.vault.rotate').args.objectId,f.a.objectId);
});

test('offline canonical profile reopens only after fresh Source auth plus explicit prior-local-password recovery; no server rotate/write occurs',async t=>{
 const f=await independentVaultSdkFixture(t),offlinePort=indexedNativePort(),offline=createCanvasVaultLocalStore(f.accountId,f.projectId,{origin:f.http.origin,indexedDB:offlinePort.indexedDB});t.after(()=>offline.close());
 const b=f.client('offline-B',{store:offline});await b.unlock(f.b.objectId,acceptancePassword);const oldProfile=(await offline.inspectScopes()).profiles[0],oldKey=await deriveCanvasVaultKey(oldProfile,acceptancePassword),recordId='offline_exact_history',body='Exact prior-device private recovery history';
 await offline.put(await sealCanvasVaultRecord(oldKey,{schema:'soty.vault.local-record.v1',accountId:f.accountId,projectId:f.projectId,slot:oldProfile.slot,recordId,purpose:'recovery',version:1},{body}),0);b.dispose();
 const a=f.client('online-A');await a.unlock(f.a.objectId,acceptancePassword);await a.rotate(f.a.objectId,{oldPassword:acceptancePassword,newPassword:nextPassword,requestId:'offline_source_rotate_once'});
 const reopened=f.client('offline-B-reopened',{store:offline});await assert.rejects(reopened.unlock(f.b.objectId,nextPassword),{code:'vault_local_password_or_data_invalid'});
 const before=await offline.get(recordId),mutations=f.calls.filter(value=>['canvas.vault.rotate','canvas.vault.write'].includes(value.operation)).length;
 await assert.rejects(reopened.recoverLocalPassword(f.b.objectId,{currentSourcePassword:'Wrong current Source password',priorLocalPassword:acceptancePassword}),{code:'password_container_unavailable'});
 await assert.rejects(reopened.recoverLocalPassword(f.b.objectId,{currentSourcePassword:nextPassword,priorLocalPassword:'Wrong prior local password'}),{code:'vault_local_password_or_data_invalid'});assert.deepEqual(await offline.get(recordId),before);
 const result=await reopened.recoverLocalPassword(f.b.objectId,{currentSourcePassword:nextPassword,priorLocalPassword:acceptancePassword});assert.equal(result.knownSource,'ready');assert.equal(result.complete,true);
 const newProfile=(await offline.inspectScopes()).profiles[0],newKey=await deriveCanvasVaultKey(newProfile,nextPassword);assert.notEqual(newProfile.profileId,oldProfile.profileId);assert.equal((await openCanvasVaultRecord(newKey,await offline.get(recordId))).body,body);
 assert.equal(f.calls.filter(value=>['canvas.vault.rotate','canvas.vault.write'].includes(value.operation)).length,mutations);assert.equal((await reopened.readNote(f.b.objectId)).body,'Initial Source revision one');assert.equal(offlinePort.databases.has('soty-notes-drafts-v1'),false);
});

test('unknown legacy Source-stage op stays explicit pending; prior password resolves the real receipt and migrates without replay or blocking unrelated Source',async t=>{
 const f=await independentVaultSdkFixture(t),prior='Older operation local password 20261031',otherPrior='Other unknown operation password 20261030',sourceRequestId=randomUUID(),content=document('Actual Source with a lost create ACK');
 const created=await f.raw('canvas.vault.create',{requestId:sourceRequestId,password:prior,content}),witness=(await f.raw('canvas.objects.create',objectArgs(created.binding))).current;
 await f.raw('canvas.vault.rotate',{requestId:randomUUID(),objectId:witness.objectId,expectedRevision:1,oldPassword:prior,newPassword:acceptancePassword});
 const port=indexedNativePort(),origin=f.http.origin,operationId=randomUUID(),otherId=randomUUID(),sourceScope={schema:'soty.vault.local-scope.v1',kind:'source',id:created.binding.ref},profiles=[canvasVaultProfile(f.accountId,f.projectId,operationId),canvasVaultProfile(f.accountId,f.projectId,otherId)];
 const make=async(index,password)=>{const profile=profiles[index],key=await deriveCanvasVaultKey(profile,password),payload={schema:'soty.vault.create.v1',operationId:profile.slot,sourceRequestId:index===0?sourceRequestId:randomUUID(),objectRequestId:randomUUID(),kind:'note',createdAt:1,stage:'source',content,sourceRef:null,objectId:null,placement:null,presentation:{mask:{shape:'rect'},fontSize:16}};
  return sealCanvasVaultRecord(key,{schema:'soty.vault.local-record.v1',accountId:f.accountId,projectId:f.projectId,slot:profile.slot,recordId:profile.slot,purpose:'create',version:1},payload);};
 const records=[await make(0,prior),await make(1,otherPrior)];await seedVaultLegacyV2({indexedDB:port.indexedDB,origin,accountId:f.accountId,projectId:f.projectId,profiles,records});
 const store=createCanvasVaultLocalStore(f.accountId,f.projectId,{origin,indexedDB:port.indexedDB});t.after(()=>store.close());const sdk=f.client('unknown-legacy',{store});
 await sdk.unlock(f.a.objectId,acceptancePassword);const initial=await sdk.localRecoveryState(f.a.objectId);assert.equal(initial.knownSource,'ready');assert.equal(initial.complete,false);assert.equal(initial.needsPriorLocalPassword,true);assert.equal(initial.unresolvedLegacy.length,2);
 const rotated=await sdk.rotate(f.a.objectId,{oldPassword:acceptancePassword,newPassword:nextPassword,requestId:'unknown_other_source_allowed'});assert.equal(rotated.localRecovery.knownSource,'ready');assert.equal(rotated.localRecovery.complete,false);
 await sdk.unlock(witness.objectId,acceptancePassword);const original=(await openCanvasVaultRecord(await deriveCanvasVaultKey(profiles[0],prior),records[0]));
 const mutations=f.calls.filter(value=>['canvas.vault.create','canvas.vault.rotate','canvas.vault.write'].includes(value.operation)).length;
 const recovered=await sdk.recoverLocalPassword(witness.objectId,{currentSourcePassword:acceptancePassword,priorLocalPassword:prior});assert.equal(recovered.knownSource,'ready');assert.equal(recovered.complete,false);assert.equal(recovered.unresolvedLegacy.some(value=>value.slot===otherId),true);
 const slot=await canvasVaultScopeSlot(sourceScope,f.projectId),profile=(await store.inspectScopes()).profiles.find(value=>value.slot===slot),key=await deriveCanvasVaultKey(profile,acceptancePassword),migrated=await store.get(operationId),payload=await openCanvasVaultRecord(key,migrated);
 assert.equal(migrated.slot,slot);assert.equal(payload.stage,'object');assert.equal(payload.sourceRef,created.binding.ref);assert.equal(payload.content,null);for(const field of['operationId','sourceRequestId','objectRequestId'])assert.equal(payload[field],original[field]);
 assert.equal((await store.get(otherId)).slot,otherId,'undecipherable unrelated history is retained verbatim');assert.equal(f.calls.filter(value=>['canvas.vault.create','canvas.vault.rotate','canvas.vault.write'].includes(value.operation)).length,mutations,'recovery never mutates/replays a Source');
 assert.equal((await sdk.readNote(witness.objectId)).body,'Actual Source with a lost create ACK');
});

test('SDK finishes original A lost-ACK rotation after A removal through a read-only current B witness, never rotateB',async t=>{
 const f=await independentVaultSdkFixture(t),calls=[];let lose=true;const sdk=createCanvasVaultClient({api:{async request(operation,args){calls.push({operation,args:structuredClone(args)});const value=await f.raw(operation,args);if(operation==='canvas.vault.rotate'&&lose){lose=false;throw Object.assign(new Error('network_timeout'),{code:'network_timeout'});}return value;}},accountId:f.accountId,projectId:f.projectId,store:f.cipher,isCurrent:()=>true});t.after(()=>sdk.dispose());
 await sdk.unlock(f.a.objectId,acceptancePassword);await sdk.unlock(f.b.objectId,acceptancePassword);const requestId='original_A_removed_after_commit';
 await assert.rejects(sdk.rotate(f.a.objectId,{oldPassword:acceptancePassword,newPassword:nextPassword,requestId}),{code:'network_timeout'});
 await f.raw('canvas.objects.remove',{requestId:randomUUID(),objectId:f.a.objectId,expectedRevision:f.a.revision});
 const result=await sdk.rotate(f.b.objectId,{oldPassword:acceptancePassword,newPassword:nextPassword,requestId});assert.equal(result.replayed,true);assert.equal(result.receipt.revision,2);assert.equal(result.localRecovery.complete,true);
 const rotations=calls.filter(value=>value.operation==='canvas.vault.rotate');assert.equal(rotations.length,1);assert.equal(rotations[0].args.objectId,f.a.objectId);assert.equal(rotations[0].args.requestId,requestId);assert.equal(calls.some(value=>value.operation==='canvas.vault.rotateReceipt'&&value.args.commandId===requestId&&value.args.witnessObjectId===f.b.objectId),true);
 assert.equal((await sdk.readNote(f.b.objectId)).body,'Initial Source revision one');assert.equal((await f.cipher.list()).some(value=>value.purpose==='rotate'),false);
});

test('definite Source CAS refusal plus fresh missing-command proof releases only its rejected intent; transport uncertainty is not cancellation',async t=>{
 const f=await independentVaultSdkFixture(t);let inject=true;const calls=[],sdk=createCanvasVaultClient({api:{async request(operation,args){calls.push({operation,args:structuredClone(args)});
  if(operation==='canvas.vault.rotate'&&inject){inject=false;await f.raw('canvas.vault.write',{objectId:f.b.objectId,requestId:'external_authorized_body_change',expectedRevision:1,content:document('Concurrent authoritative body at revision two')});}
  return f.raw(operation,args);}},accountId:f.accountId,projectId:f.projectId,store:f.cipher,isCurrent:()=>true});t.after(()=>sdk.dispose());
 await sdk.unlock(f.a.objectId,acceptancePassword);await sdk.unlock(f.b.objectId,acceptancePassword);const args={oldPassword:acceptancePassword,newPassword:nextPassword,requestId:'definite_refusal_same_request'};
 await assert.rejects(sdk.rotate(f.a.objectId,args),{code:'vault_revision_conflict'});assert.equal((await f.cipher.inspectScopes()).controls[0].phase,'ready');assert.equal((await f.cipher.list()).some(value=>value.purpose==='rotate'),false);
 assert.equal(sdk.isUnlocked(f.a.objectId),true);assert.equal((await sdk.readNote(f.a.objectId)).body,'Concurrent authoritative body at revision two');
 const result=await sdk.rotate(f.a.objectId,args);assert.equal(result.receipt.revision,3);assert.equal((await sdk.readNote(f.a.objectId)).body,'Concurrent authoritative body at revision two');
 const rotations=calls.filter(value=>value.operation==='canvas.vault.rotate');assert.equal(rotations.length,2);assert.equal(rotations[0].args.requestId,rotations[1].args.requestId);assert.deepEqual(rotations.map(value=>value.args.expectedRevision),[1,2]);
});
