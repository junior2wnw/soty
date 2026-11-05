import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {createNoteSession} from '../../modules/notes/browser/state.mjs';
import {createCanvasVaultMemoryCache} from './canvas-vault-local.mjs';
import {independentVaultSdkFixture,acceptancePassword,noteApi} from './test-support/independent-vault-sdk.mjs';

test('signed Root lost-ACK write returns the original receipt revision, not a later current Source revision',async t=>{
  const f=await independentVaultSdkFixture(t),a=f.client('A'),b=f.client('B');await a.unlock(f.a.objectId,acceptancePassword);
  const first=await a.readNote(f.a.objectId),original={...first,mutationId:randomUUID(),expectedRevision:1,body:'Writer A original revision two'};
  f.loseWriteAck('A');await assert.rejects(a.writeNote(f.a.objectId,original),{code:'network_timeout'});
  await b.unlock(f.b.objectId,acceptancePassword);const second=await b.readNote(f.b.objectId);assert.equal(second.revision,2);
  await b.writeNote(f.b.objectId,{...second,mutationId:randomUUID(),expectedRevision:2,body:'Writer B authoritative revision three'});
  await a.unlock(f.a.objectId,acceptancePassword);const replay=await a.writeNote(f.a.objectId,original),actual=f.outcomes.filter(value=>value.label==='A');
  assert.deepEqual(actual.map(value=>[value.receiptRevision,value.currentRevision]),[[2,2],[2,3]],'real Root distinguishes immutable receipt from current Source');
  const requests=f.calls.filter(value=>value.label==='A'&&value.operation==='canvas.vault.write');assert.deepEqual(requests[1].args,requests[0].args,'replay keeps the entire original request, including API bytes/timestamp');
  assert.equal(replay.revision,2,'ACK confirms the exact writer-A mutation, never the current writer-B body');
});

test('actual NoteSession cannot silently rebase stale writer A onto B after lost-ACK replay',async t=>{
  const f=await independentVaultSdkFixture(t),a=f.client('A'),b=f.client('B');await a.unlock(f.a.objectId,acceptancePassword);const initial=await a.readNote(f.a.objectId);
  const editor=createNoteSession({api:noteApi(a,f.a.objectId),accountId:f.accountId,note:initial,store:a.draftStore(f.a.objectId),cache:createCanvasVaultMemoryCache(),debounceMs:60000});t.after(()=>editor.dispose());
  f.loseWriteAck('A');editor.edit({body:'Writer A branch before lost ACK'});await editor.flush();assert.equal(editor.state().dirty,true);assert.equal(editor.state().localDurable,true);assert.equal(editor.state().error,'network_timeout');
  await b.unlock(f.b.objectId,acceptancePassword);const revisionTwo=await b.readNote(f.b.objectId);assert.equal(revisionTwo.revision,2);
  const bBody='Keep B revision three; stale A must conflict';await b.writeNote(f.b.objectId,{...revisionTwo,mutationId:randomUUID(),expectedRevision:2,body:bBody});
  await a.unlock(f.a.objectId,acceptancePassword);await editor.retry();const replayRevision=editor.state().note.revision;
  const original=f.calls.filter(value=>value.label==='A'&&value.operation==='canvas.vault.write');assert.deepEqual(original[1].args,original[0].args);
  editor.edit({body:'Stale A continuation must not overwrite B'});await editor.flush();
  await b.unlock(f.b.objectId,acceptancePassword);const authoritative=await b.readNote(f.b.objectId);
  assert.deepEqual({body:authoritative.body,revision:authoritative.revision,conflict:editor.state().conflict,replayRevision},{body:bBody,revision:3,conflict:true,replayRevision:2},'durable replay ACK must not bless stale A text as current revision3');
});
