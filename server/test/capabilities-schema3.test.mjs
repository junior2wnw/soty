import test from 'node:test';
import assert from 'node:assert/strict';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';

test('actual default-off Caps3 host preserves native HTTP and signed owner history without an OAuth key', async t => {
  const f = await nativeHttpFixture(t, { capabilitiesVersion: 3 });
  const owner = nativeIdentity('Владелец schema3'), account = await f.bootstrap(owner);
  const caller = await f.issue(owner, account.accountId);
  const registryId = f.app.locals.capabilitiesService.registryId;
  const input = { title: 'Черновик schema3', body: 'Точный текст е\u0301 / é / 中文', idempotencyKey: 'host-schema3-draft' };
  const created = await f.http('/api/capabilities/v1/notes/drafts', {
    method: 'POST', token: caller.token, body: JSON.stringify(input),
  });
  assert.equal(created.status, 201);
  assert.equal(created.body.invocation.status, 'succeeded');
  const invocationId = created.body.invocation.invocationId;

  // The host receives neither migration admission nor OAuth configuration/key.
  await f.restart({ enabled: false });
  assert.equal(f.app.locals.capabilitiesService.schemaVersion, 3);
  assert.equal(f.app.locals.capabilitiesService.registryId, registryId);
  assert.ok(f.app.locals.nativeRecovery, 'reader3 fallback retains the recovery scheduler');
  assert.equal((await f.http('/api/capabilities/v1/status')).body.notesCreateEnabled, false);

  const history = await f.http(`/api/capabilities/v1/invocations/${invocationId}`, { token: caller.token });
  assert.equal(history.status, 200);
  assert.deepEqual(history.body.invocation, created.body.invocation);
  assert.deepEqual(history.body.result, created.body.result);
  assert.equal(history.headers['cache-control'], 'no-store');
  for (const privateValue of [input.title, input.body, caller.token]) {
    assert.equal(JSON.stringify(history.body).includes(privateValue), false);
  }
  const note = good(await f.call(owner, 'notes.get', {
    expectedAccountId: account.accountId, noteId: created.body.result.noteId,
  })).note;
  assert.equal(note.title, input.title);
  assert.equal(note.body, input.body);
  const ownerHistory = good(await f.call(owner, 'access.invocations.list', { expectedAccountId: account.accountId }));
  assert.equal(ownerHistory.invocations.length, 1);
  assert.equal(ownerHistory.invocations[0].invocationId, invocationId);
  const fresh = await f.http('/api/capabilities/v1/notes/drafts', {
    method: 'POST', token: caller.token, body: JSON.stringify({ ...input, idempotencyKey: 'host-schema3-disabled' }),
  });
  assert.equal(fresh.status, 503);
  assert.equal(f.sql(f.capsFile, db => db.prepare('SELECT count(*) AS n FROM cap_invocations').get().n), 1);
  assert.equal(f.sql(f.notesFile, db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n), 1);
});
