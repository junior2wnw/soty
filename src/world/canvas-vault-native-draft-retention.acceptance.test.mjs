import test from 'node:test';
import assert from 'node:assert/strict';
import {createNoteSession} from '../../modules/notes/browser/state.mjs';
import {hasRetainedNativeDrafts,flushRetainedNativeDrafts} from '../../modules/notes/browser/native-local.mjs';
import {CanvasVaultError,createCanvasVaultMemoryCache} from './canvas-vault-local.mjs';
import {independentVaultSdkFixture,acceptancePassword,noteApi,until} from './test-support/independent-vault-sdk.mjs';

test('failed protected DraftStore COMMIT survives NoteSession disposal/relock in an explicit private retained registry, never plaintext Native IDB',async t=>{
  const f=await independentVaultSdkFixture(t),onlyRam='ONLY_RAM_PROTECTED_DRAFT_20261104_PRIVATE';let failDraft=true;
  const faulty={...f.cipher,async put(record,version){if(failDraft&&record.purpose==='draft')throw new CanvasVaultError('vault_local_quota');return f.cipher.put(record,version);}};
  const sdk=f.client('retained',{store:faulty});await sdk.unlock(f.a.objectId,acceptancePassword);const initial=await sdk.readNote(f.a.objectId),drafts=sdk.draftStore(f.a.objectId);
  const editor=createNoteSession({api:noteApi(sdk,f.a.objectId),accountId:f.accountId,note:initial,store:drafts,cache:createCanvasVaultMemoryCache(),debounceMs:60000});
  t.after(()=>{editor.dispose();for(const port of drafts.native?.sessions?.(initial.noteId)??[])port.releaseRecovered(port.snapshot().generation);});
  editor.edit({body:onlyRam});await assert.rejects(editor.flushLocal(),{code:'vault_local_quota'});assert.equal(editor.state().localDurable,false);assert.equal(editor.hasUnsavedChanges(),true);
  editor.dispose();await until(()=>editor.state().localError==='vault_local_quota');await sdk.relock(f.a.objectId);await new Promise(resolve=>setImmediate(resolve));
  const serialized=JSON.stringify([...f.port.databases].map(([name,value])=>({name,stores:[...value.stores].map(([store,table])=>({store,rows:[...table.rows.values()]}))})));
  assert.equal(serialized.includes(onlyRam),false);assert.equal(f.port.databases.has('soty-notes-drafts-v1'),false);assert.equal(f.port.databases.has('soty-notes-cache-v1'),false);
  assert.equal(hasRetainedNativeDrafts(),true,'shell must still warn after the protected editor is gone and its cipher COMMIT failed');
  const retained=drafts.native.sessions(initial.noteId);assert.equal(retained.length,1);assert.equal(retained[0].snapshot().note.body,onlyRam);assert.equal(retained[0].hasUnsavedChanges(),true);
  await assert.rejects(flushRetainedNativeDrafts(),'locked/failed storage must not pretend the RAM branch is durable');
  assert.equal(hasRetainedNativeDrafts(),true);assert.equal(JSON.stringify(f.port.rows('soty-canvas-vault-local-v1','cipher')).includes(onlyRam),false);
  // Explicit same-account unlock/recovery may make the branch durable later;
  // the test never writes it into an ordinary Notes store as a fallback.
  failDraft=false;await sdk.unlock(f.a.objectId,acceptancePassword);await flushRetainedNativeDrafts();assert.equal(hasRetainedNativeDrafts(),false);
  assert.equal((await sdk.draftStore(f.a.objectId).list()).some(row=>row.note.body===onlyRam),true);
});
