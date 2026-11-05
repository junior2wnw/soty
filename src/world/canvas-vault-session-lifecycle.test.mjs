import test from 'node:test';
import assert from 'node:assert/strict';
import {createNoteSession} from '../../modules/notes/browser/state.mjs';
import {hasRetainedNativeDrafts} from '../../modules/notes/browser/native-local.mjs';
import {CanvasVaultError,createCanvasVaultMemoryCache} from './canvas-vault-local.mjs';
import {independentVaultSdkFixture,acceptancePassword,noteApi} from './test-support/independent-vault-sdk.mjs';

for(const disposed of[false,true])test(`protected ${disposed?'disposed':'active'} failed-COMMIT session is distinguished from a harmless refreshed Object grant`,async t=>{
 const f=await independentVaultSdkFixture(t);let fail=true;const store={...f.cipher,async put(record,version){if(fail&&record.purpose==='draft')throw new CanvasVaultError('vault_local_quota');return f.cipher.put(record,version);}},sdk=f.client('lifecycle',{store});
 await sdk.unlock(f.a.objectId,acceptancePassword);const note=await sdk.readNote(f.a.objectId),drafts=sdk.draftStore(f.a.objectId),editor=createNoteSession({api:noteApi(sdk,f.a.objectId),accountId:f.accountId,store:drafts,note,cache:createCanvasVaultMemoryCache(),debounceMs:60000}),text='Actual private RAM before reauthentication';
 try{
  editor.edit({body:text});assert.equal(editor.state().note.body,text);await assert.rejects(editor.flushLocal(),{code:'vault_local_quota'});assert.equal(hasRetainedNativeDrafts(),true);
  if(disposed){editor.dispose();await new Promise(resolve=>setImmediate(resolve));assert.equal(drafts.native.sessions(note.noteId)[0].isDisposed(),true);assert.equal(sdk.isUnlocked(f.a.objectId),true,'disposing a surface is different from losing its SDK grant');}
  else await sdk.unlock(f.a.objectId,acceptancePassword);
  fail=false;await sdk.unlock(f.a.objectId,acceptancePassword);
  if(disposed){assert.equal(hasRetainedNativeDrafts(),false);assert.equal((await sdk.draftStore(f.a.objectId).list()).some(value=>value.note.body===text),true);}
  else{editor.edit({body:'Active caret/branch still accepts the next actual edit'});assert.equal(editor.state().note.body,'Active caret/branch still accepts the next actual edit');assert.equal(editor.state().localProtection,undefined);await editor.flushLocal();assert.equal(hasRetainedNativeDrafts(),false);}
  assert.equal(f.port.databases.has('soty-notes-drafts-v1'),false);assert.equal(f.port.databases.has('soty-notes-cache-v1'),false);
 }finally{fail=false;editor.dispose();await editor.flushLocal();}
});
