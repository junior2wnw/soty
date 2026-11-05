import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {createCanvasVaultLocalStore,canvasVaultProfile,deriveCanvasVaultKey} from './canvas-vault-local.mjs';
import {archiveNativeNoteDrafts,readNativeNoteRecoveries} from './canvas-native-recovery.mjs';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
const accountId='recovery_owner',projectId='soty',objectId='recovery_object_123',noteId='recovery_note_123',password='Synthetic recovery password45';
const source={providerId:'soty.notes',ref:noteId,ownerId:accountId},secret='Synthetic draft secret from before conversion 987';
const draft={note:{noteId,title:'Private original title',body:secret,items:[],preview:secret,color:'plain',pinned:false,state:'active',revision:37,createdAt:1,updatedAt:2},branchId:'original_branch_123',generation:2,committedGeneration:0,
  pending:{mutationId:'original_mutation_123',expectedRevision:37,generation:1,document:{title:'Original',body:secret,items:[],color:'plain',pinned:false,state:'active'}},conflict:true,savedAt:3};
async function fixture(){const port=indexedNativePort(),options={origin:'https://soty.example',indexedDB:port.indexedDB},store=createCanvasVaultLocalStore(accountId,projectId,options),profile=await store.ensureProfile(canvasVaultProfile(accountId,projectId,objectId)),session=await deriveCanvasVaultKey(profile,password);return{port,options,store,profile,session};}
const turn=()=>new Promise(resolve=>setImmediate(resolve));
const until=async predicate=>{for(let index=0;index<200;index++){if(predicate())return;await turn();}assert.fail('Expected encrypted COMMIT boundary');};

test('actual Vault AEAD archive survives reopen and preserves old pending/conflict bytes without a plaintext fingerprint',async()=>{
  const f=await fixture(),receipt=await archiveNativeNoteDrafts({...f,accountId,projectId,objectId,source,drafts:[draft]});assert.equal(receipt.records.length,1);
  const stored=JSON.stringify(await f.store.list()),fingerprint=createHash('sha256').update(secret).digest('hex');assert.doesNotMatch(stored,new RegExp(secret));assert.equal(stored.includes(draft.note.title),false);assert.equal(stored.includes(fingerprint),false);assert.equal(stored.includes(password),false);
  f.store.close();const reopened=createCanvasVaultLocalStore(accountId,projectId,f.options),key=await deriveCanvasVaultKey(f.profile,password);
  const recovered=await readNativeNoteRecoveries({session:key,store:reopened,objectId,accountId});assert.deepEqual(recovered[0].draft,draft);assert.deepEqual(recovered[0].source,source);
  const same=await archiveNativeNoteDrafts({session:key,store:reopened,accountId,projectId,objectId,source,drafts:[draft]});assert.deepEqual(same,receipt);assert.equal((await reopened.list()).length,1);reopened.close();
});

test('encrypted recovery cannot acknowledge before COMMIT; quota/abort leaves no archive receipt',async()=>{
  const f=await fixture();f.port.hold('soty-canvas-vault-local-v1');let done=false;
  const writing=archiveNativeNoteDrafts({...f,accountId,projectId,objectId,source,drafts:[draft]}).then(value=>{done=true;return value;});await until(()=>f.port.held);
  assert.equal(done,false);assert.ok(f.port.databases.get('soty-canvas-vault-local-v1').stores.has('cipher'));
  assert.equal(f.port.rows('soty-canvas-vault-local-v1','cipher').filter(row=>row.value?.schema==='soty.vault.local-record.v1').length,0);f.port.release();await writing;
  f.port.abort('soty-canvas-vault-local-v1');await assert.rejects(archiveNativeNoteDrafts({...f,accountId,projectId,objectId,source,drafts:[{...draft,savedAt:4,generation:3}]}));
  assert.equal((await readNativeNoteRecoveries({...f,objectId,accountId}))[0].draft.savedAt,3);f.store.close();
});

test('an intentionally premature archive acknowledgement violates the same durable-COMMIT oracle',async()=>{
  const url=new URL('./canvas-native-recovery.mjs',import.meta.url),sourceCode=readFileSync(url,'utf8');
  // The new final phase/profile read is another real IDB barrier. An actually
  // premature adapter must omit BOTH waits; merely removing put's await can
  // no longer produce an early receipt through the unchanged phase read.
  const noPut=sourceCode.replace("vaultCheck(await store.put(value,expected),'vault_local_conflict');",'void store.put(value,expected);'),broken=noPut.replace('await currentProfile(store,session);scope.active();return{','scope.active();return{');assert.notEqual(noPut,sourceCode);assert.notEqual(broken,noPut);
  const absolute=broken.replace(/from '(\.\/[^']+)'/gu,(_match,path)=>`from '${new URL(path,import.meta.url).href}'`),module=await import(`data:text/javascript;base64,${Buffer.from(absolute).toString('base64')}`);
  const f=await fixture();f.port.hold('soty-canvas-vault-local-v1');let done=false;
  const writing=module.archiveNativeNoteDrafts({...f,accountId,projectId,objectId,source,drafts:[draft]}).then(()=>{done=true;});await until(()=>f.port.held);
  assert.equal(done,true);assert.throws(()=>assert.equal(done,false),'The unchanged receipt-before-COMMIT oracle must detect this broken adapter');f.port.release();await writing;await turn();f.store.close();
});

test('wrong password, changed owner and same-version content collision fail without a fallback',async()=>{
  const f=await fixture();await archiveNativeNoteDrafts({...f,accountId,projectId,objectId,source,drafts:[draft]});const wrong=await deriveCanvasVaultKey(f.profile,password+'wrong');
  await assert.rejects(readNativeNoteRecoveries({session:wrong,store:f.store,objectId,accountId}),error=>error.code==='vault_local_password_or_data_invalid');
  await assert.rejects(archiveNativeNoteDrafts({...f,accountId,projectId,objectId,source:{...source,ownerId:'other_owner'},drafts:[draft]}));
  await assert.rejects(archiveNativeNoteDrafts({...f,accountId,projectId,objectId,source,drafts:[{...draft,note:{...draft.note,body:'collision'}}]}),error=>error.code==='vault_local_conflict');f.store.close();
});
