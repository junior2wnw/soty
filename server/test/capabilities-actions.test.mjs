import test from 'node:test';
import assert from 'node:assert/strict';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';
import { validateCapabilityAudience } from '../capabilities-actions.js';
const CREATE = '/api/capabilities/v1/notes/drafts', HISTORY = '/api/capabilities/v1/invocations/';
const input = { title: 'Точный черновик 🚀', body: 'Первая строка\nе\u0301 и é\n中文 👩🏽‍💻', idempotencyKey: 'exact-native-http-key' };
const post = (f, token, value = input, options = {}) => f.http(CREATE, { method: 'POST', token, body: JSON.stringify(value), ...options });
const noSecrets = (response, strings) => {
  const wire = JSON.stringify(response.body);
  for (const secret of strings) assert.equal(wire.includes(secret), false, 'response excludes private content');
  assert.equal(response.headers['cache-control'], 'no-store');
  assert.equal(response.headers['x-content-type-options'], 'nosniff');
  assert.equal(response.headers['access-control-allow-origin'], undefined);
};

test('real signed owner, external HTTP create/get/replay and Notes read share one exact private result', async t => {
  const f = await nativeHttpFixture(t), owner = nativeIdentity('Автор'), account = await f.bootstrap(owner);
  const identity = await f.issue(owner, account.accountId);
  const ready = await f.http('/api/capabilities/v1/status');
  assert.deepEqual(ready.body, { notesCreateEnabled: true, audience: f.origin });
  const first = await post(f, identity.token);
  assert.equal(first.status, 201); assert.equal(first.body.reused, false);
  assert.equal(first.body.invocation.status, 'succeeded'); assert.equal(first.body.invocation.effectState, 'committed');
  const { noteId, revision, url } = first.body.result, invocationId = first.body.invocation.invocationId;
  assert.equal(revision, 1); assert.equal(url, `${f.origin}/#notes/${noteId}`);
  assert.equal(first.headers.location, `${f.origin}${HISTORY}${invocationId}`);
  const note = good(await f.call(owner, 'notes.get', { expectedAccountId: account.accountId, noteId })).note;
  assert.equal(note.title, input.title); assert.equal(note.body, input.body);
  noSecrets(first, [input.title, input.body, identity.token, account.accountId]);
  const replay = await post(f, identity.token);
  assert.equal(replay.status, 200); assert.equal(replay.body.reused, true);
  assert.deepEqual(replay.body.invocation, first.body.invocation); assert.deepEqual(replay.body.result, first.body.result);
  const history = await f.http(HISTORY + invocationId, { token: identity.token });
  assert.equal(history.status, 200); assert.deepEqual(history.body.invocation, first.body.invocation);
  assert.deepEqual(history.body.result, first.body.result); noSecrets(history, [input.title, input.body, identity.token]);
  const conflict = await post(f, identity.token, { ...input, body: 'Другое содержание' });
  assert.equal(conflict.status, 409); assert.equal(conflict.body.error.code, 'invocation_request_conflict');
  assert.deepEqual(f.sql(f.capsFile, db => ({ ...db.prepare('SELECT reserved_amount,spent_amount FROM cap_budgets').get() })),
    { reserved_amount: 0, spent_amount: 1 });
  assert.equal(f.sql(f.notesFile, db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n), 1);
  assert.equal(f.sql(f.capsFile, db => db.prepare('SELECT input_json FROM cap_invocations').get().input_json), 'null');
});

test('historical HTTP replay survives edits, purge, restart and disabled execution without resurrecting a note', async t => {
  const f = await nativeHttpFixture(t), owner = nativeIdentity('Автор'), account = await f.bootstrap(owner);
  const identity = await f.issue(owner, account.accountId), first = await post(f, identity.token);
  assert.equal(first.status, 201);
  const { noteId } = first.body.result, invocationId = first.body.invocation.invocationId;
  const change = { expectedAccountId: account.accountId, noteId, mutationId: 'human_edit_after_agent', expectedRevision: 1,
    title: 'Правка человеком', body: 'Новый приватный текст', items: [], pinned: false, color: 'plain', state: 'trashed' };
  good(await f.call(owner, 'notes.put', change));
  good(await f.call(owner, 'notes.purge', { expectedAccountId: account.accountId, noteId,
    mutationId: 'human_purge_after_agent', expectedRevision: 2 }));
  await f.restart({ enabled: false });
  assert.equal((await f.http('/api/capabilities/v1/status')).body.notesCreateEnabled, false);
  const replay = await post(f, identity.token);
  assert.equal(replay.status, 200); assert.deepEqual(replay.body.invocation, first.body.invocation);
  assert.deepEqual(replay.body.result, first.body.result);
  assert.equal((await f.http(HISTORY + invocationId, { token: identity.token })).status, 200);
  const fresh = await post(f, identity.token, { ...input, idempotencyKey: 'disabled-new-http-key' });
  assert.equal(fresh.status, 503);
  const missing = await f.call(owner, 'notes.get', { expectedAccountId: account.accountId, noteId });
  assert.equal(missing.error.code, 'notes_note_not_found');
  assert.equal(f.sql(f.notesFile, db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n), 1);
  assert.equal(f.sql(f.capsFile, db => db.prepare('SELECT count(*) AS n FROM cap_invocations').get().n), 1);
});

test('foreign account, sibling grant, wrong audience and revoked credentials cannot read native history', async t => {
  const f = await nativeHttpFixture(t), alice = nativeIdentity('Алиса'), bob = nativeIdentity('Боб');
  const a = await f.bootstrap(alice), b = await f.bootstrap(bob);
  const original = await f.issue(alice, a.accountId), second = await f.issue(bob, b.accountId);
  const first = await post(f, original.token), id = first.body.invocation.invocationId;
  const sibling = await f.issue(alice, a.accountId, { principalId: original.principalId });
  const wrongAudience = await f.issue(alice, a.accountId, { principalId: original.principalId, grantId: original.grantId, audience: `${f.origin}/other` });
  for (const token of [second.token, sibling.token]) {
    const denied = await f.http(HISTORY + id, { token }); assert.equal(denied.status, 404);
    assert.equal(JSON.stringify(denied.body).includes(id), false);
  }
  assert.equal((await f.http(HISTORY + id, { token: wrongAudience.token })).status, 401);
  good(await f.call(alice, 'access.grants.revoke', { expectedAccountId: a.accountId, grantId: original.grantId }));
  const denied = await f.http(HISTORY + id, { token: original.token });
  assert.ok([401, 403].includes(denied.status)); assert.equal(JSON.stringify(denied.body).includes(id), false);
  assert.ok([401, 403].includes((await post(f, original.token)).status));
  assert.equal(f.sql(f.notesFile, db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n), 1);
});

test('actual private HTTP rejects noncanonical routes, Host, auth ambiguity and malformed inputs before a new ledger row', async t => {
  const f = await nativeHttpFixture(t), owner = nativeIdentity('Автор'), account = await f.bootstrap(owner);
  const identity = await f.issue(owner, account.accountId);
  const cases = [
    { path: CREATE + '?', status: 400 }, { path: CREATE + '?x=1', status: 400 },
    { path: CREATE.replace('/notes/', '/%6eotes/'), status: 400 },
    { path: CREATE + '/', status: 404 },
    { headers: { host: 'unexpected.invalid' }, status: 400 },
    { headers: { origin: 'https://foreign.invalid' }, status: 403 },
    { headers: { authorization: [`Bearer ${identity.token}`, `Bearer ${identity.token}`] }, status: 401 },
    { headers: { authorization: 'Basic invalid' }, status: 401 },
    { headers: { 'content-type': 'text/plain' }, status: 415 },
    { headers: { 'content-encoding': 'gzip' }, status: 415 },
    { body: Buffer.from([0xc0, 0xaf]), status: 400 },
    { body: JSON.stringify({ ...input, body: '\ud800' }), status: 400 },
    { body: JSON.stringify({ ...input, accountId: account.accountId }), status: 400 },
    { body: '{"title":"x","ti\\u0074le":"y","body":"z","idempotencyKey":"request-key"}', status: 400 },
    { body: JSON.stringify({ ...input, title: '😀'.repeat(81) }), status: 400 },
    { body: JSON.stringify({ ...input, body: '中'.repeat(87371) + 'x'.repeat(9), title: '' }), status: 413 },
    { body: ' '.repeat(2 * 1024 * 1024 + 1), status: 413 },
  ];
  for (const { path = CREATE, status, ...options } of cases) {
    const response = await f.http(path, { method: 'POST', token: identity.token, body: JSON.stringify(input), ...options });
    assert.equal(response.status, status, path); noSecrets(response, [input.title, input.body, identity.token]);
  }
  assert.equal((await f.http(CREATE, { token: identity.token })).status, 405);
  assert.equal((await f.http(CREATE, { method: 'OPTIONS' })).status, 405);
  assert.equal((await post(f, undefined)).status, 401);
  assert.equal(f.sql(f.capsFile, db => db.prepare('SELECT count(*) AS n FROM cap_invocations').get().n), 0);
  assert.equal((await post(f, identity.token)).status, 201);
});

test('default-off and mixed native stores keep ordinary Notes available without silently migrating', async t => {
  for (const [notesVersion, capabilitiesVersion] of [[1, 1], [1, 2], [2, 1]]) {
    const f = await nativeHttpFixture(t, { notesVersion, capabilitiesVersion });
    const owner = nativeIdentity('Автор'), account = await f.bootstrap(owner), identity = await f.issue(owner, account.accountId);
    assert.equal((await f.http('/api/capabilities/v1/status')).body.notesCreateEnabled, false);
    assert.equal((await post(f, identity.token)).status, 503);
    assert.equal(f.sql(f.notesFile, db => db.prepare('PRAGMA user_version').get().user_version), notesVersion);
    assert.equal(f.sql(f.capsFile, db => db.prepare('PRAGMA user_version').get().user_version), capabilitiesVersion);
    assert.deepEqual(good(await f.call(owner, 'notes.list', { expectedAccountId: account.accountId })).notes, []);
  }
});

test('trusted execution configuration requires an explicit allowed canonical audience', () => {
  assert.equal(validateCapabilityAudience(), '');
  assert.equal(validateCapabilityAudience({ audience: 'https://soty.test/', shellOrigins: ['https://soty.test'], enabled: true }), 'https://soty.test');
  for (const options of [{ enabled: true }, { enabled: 'true' },
    { audience: 'http://public.test', shellOrigins: ['http://public.test'] },
    { audience: 'https://other.test', shellOrigins: ['https://soty.test'] },
    { audience: 'https://soty.test/path', shellOrigins: ['https://soty.test'] }]) {
    assert.throws(() => validateCapabilityAudience(options), { code: 'capability_configuration_invalid' });
  }
});
