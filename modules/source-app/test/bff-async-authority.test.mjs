import test from 'node:test';
import assert from 'node:assert/strict';
import { createOrdinaryHttpFixture } from './support/ordinary-http.mjs';

test('actual Source2 BFF OIDC/Native SQL: async current fences outside TX, sync captured Native proof only in final remembered/link/consume transaction', { timeout: 20000 }, async t => {
  const f = await createOrdinaryHttpFixture(t, { embedOidc: true, asyncNativeAuthority: true }), [realm] = f.realms;
  const start = await realm.beginNative(), callback = (await realm.completeOidc(await realm.authorizeNative(start))).callback;
  await realm.finishNative(callback); assert.equal(realm.finalChecks, 3); assert.ok(realm.asyncChecks >= 3);
  assert.equal((await realm.request('/api/embed/context')).status, 200);
  const sent = await realm.request('/api/embed/feedback', { method:'POST',data:{requestId:'async-native-feedback-0001',body:'Synthetic only',attachments:[]} });
  assert.equal(sent.status,200); assert.equal(realm.finalChecks,4);
  let entered, release, held = false; const reached = new Promise(done => { entered=done; }), gate = new Promise(done => { release=done; });
  realm.beforeAsyncCheck=async()=>{if(!held){held=true;entered();await gate;}};
  const pending=realm.request('/api/embed/context');await reached;realm.store.revokeNativeSession(realm.nativeToken);release();
  assert.equal((await pending).status,403);assert.equal(realm.store.db.prepare('SELECT count(*) AS n FROM native_tickets').get().n,1);
});
