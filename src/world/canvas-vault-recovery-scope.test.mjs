import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,createHash} from 'node:crypto';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
import {createCanvasVaultLocalStore,canvasVaultProfile,deriveCanvasVaultKey,sealCanvasVaultRecord} from './canvas-vault-local.mjs';
import {canvasVaultScopeSlot,createCanvasVaultRecoveryScope} from './canvas-vault-scope.mjs';
import {archiveCreationIntents,readCreationRecoveries,verifyCreationRecoveries} from './canvas-creation-recovery.mjs';
import {archiveNativeNoteDrafts,readNativeNoteRecoveries} from './canvas-native-recovery.mjs';
const accountId='scoped_recovery_owner',projectId='soty',password='Synthetic scope recovery password',scope={schema:'soty.vault.local-scope.v1',kind:'source',id:'vault_scoped_actual_source'};
const source={providerId:'soty.notes',ref:'scoped_original_note_source',ownerId:accountId};
async function fixture(){const port=indexedNativePort(),store=createCanvasVaultLocalStore(accountId,projectId,{origin:'https://scoped-recovery.example',indexedDB:port.indexedDB}),slot=await canvasVaultScopeSlot(scope,projectId),state=await store.ensureScopedProfile(scope,canvasVaultProfile(accountId,projectId,slot));
 const session=await deriveCanvasVaultKey(state.profile,password),current={a:true,b:true};
 const options=(name)=>{const objectId='scoped_object_'+name;return{session,store,accountId,projectId,objectId,source,isCurrent:()=>current[name],cryptoScope:createCanvasVaultRecoveryScope({objectId,source:scope,profile:state.profile,isCurrent:()=>current[name]})};};
 return{port,store,state,session,current,a:options('a'),b:options('b')};}
function noteRecord(body){const intent={noteId:source.ref,mutationId:randomUUID(),expectedRevision:0,title:'Retained title',body,items:[],color:'plain',pinned:false,state:'active'};
 return{schema:'soty.canvas-create.v1',operationId:randomUUID(),accountId,projectId,kind:'note',createdAt:1,version:1,stage:'source',objectRequestId:randomUUID(),sourceIntent:intent,sourceRef:null,objectId:null,presentation:{mask:{shape:'rect'},fontSize:16},width:340,height:300,placement:null,intentHash:createHash('sha256').update(JSON.stringify(intent)).digest('hex')};}
function nativeDraft(body){return{note:{noteId:source.ref,title:'Original',body,preview:body,items:[],color:'plain',pinned:false,state:'active',revision:37,createdAt:1,updatedAt:1},branchId:randomUUID(),generation:1,committedGeneration:0,pending:null,conflict:false,savedAt:1};}

test('Source-scoped Creation recovery keeps each origin Object and exact Source; JSON slot/token cannot bypass the default foreign-object guard',async()=>{const f=await fixture();try{
 const a=noteRecord('Only origin A recovery'),b=noteRecord('Only origin B recovery'),receiptA=await archiveCreationIntents({...f.a,records:[a]}),receiptB=await archiveCreationIntents({...f.b,records:[b]});
 assert.equal(receiptA.objectId,f.a.objectId);assert.equal(receiptB.objectId,f.b.objectId);assert.notEqual(receiptA.archiveId,receiptB.archiveId);
 assert.deepEqual((await readCreationRecoveries(f.a)).flatMap(value=>value.records.map(record=>record.sourceIntent.body)),['Only origin A recovery']);assert.deepEqual((await readCreationRecoveries(f.b)).flatMap(value=>value.records.map(record=>record.sourceIntent.body)),['Only origin B recovery']);
 await assert.rejects(verifyCreationRecoveries({...f.a,records:[b],receipt:receiptA}));
 await assert.rejects(readCreationRecoveries({...f.a,objectId:f.b.objectId}),{code:'vault_local_scope_changed'});
 await assert.rejects(readCreationRecoveries({...f.a,cryptoScope:structuredClone(f.a.cryptoScope)}),{code:'vault_local_scope_changed'});
 const {cryptoScope,...plain}=f.a;await assert.rejects(readCreationRecoveries({...plain,cryptoSlot:f.state.profile.slot}),{code:'vault_local_creation_scope'});
 f.current.b=false;await assert.rejects(readCreationRecoveries(f.b),{code:'canvas_object_locked'});
}finally{f.store.close();}});

test('native-note recovery preserves origin Object while aliases share a crypto slot and does not reveal a revoked B snapshot',async()=>{const f=await fixture();try{
 const a=nativeDraft('Original A note branch'),b=nativeDraft('Original B note branch');await archiveNativeNoteDrafts({...f.a,drafts:[a]});await archiveNativeNoteDrafts({...f.b,drafts:[b]});
 assert.deepEqual((await readNativeNoteRecoveries(f.a)).map(value=>value.draft.note.body),['Original A note branch']);assert.deepEqual((await readNativeNoteRecoveries(f.b)).map(value=>value.draft.note.body),['Original B note branch']);
 const {cryptoScope,...plain}=f.b;await assert.rejects(readNativeNoteRecoveries(plain),{code:'vault_native_recovery_scope'});f.current.b=false;await assert.rejects(readNativeNoteRecoveries(f.b),{code:'canvas_object_locked'});
}finally{f.store.close();}});

test('durable Source rekey phase blocks archive verification even while profile ID and old key are still unchanged',async()=>{const f=await fixture();try{
 const record=noteRecord('Private interrupted archive'),receipt=await archiveCreationIntents({...f.a,records:[record]});await archiveNativeNoteDrafts({...f.a,drafts:[nativeDraft('Private native branch')]});
 const intent=await sealCanvasVaultRecord(f.session,{schema:'soty.vault.local-record.v1',accountId,projectId,slot:f.state.profile.slot,recordId:'phase_local_rekey_original',purpose:'rotate',version:1},{schema:'synthetic.local-rekey'});
 await f.store.beginSourceRekey(f.state.profile.slot,f.state.profile.profileId,intent,{inventoryGeneration:f.state.inventory.generation,mode:'password-recovery'});
 assert.equal((await f.store.inspectScopes()).controls[0].profileId,f.state.profile.profileId,'a profile ID check alone is insufficient');
 await assert.rejects(verifyCreationRecoveries({...f.a,records:[record],receipt}),{code:'vault_local_rotation_pending'});await assert.rejects(readNativeNoteRecoveries(f.a),{code:'vault_local_rotation_pending'});
 await assert.rejects(archiveCreationIntents({...f.a,records:[noteRecord('Not acknowledged during rekey')]}),{code:'vault_local_rotation_pending'});
}finally{f.store.close();}});
