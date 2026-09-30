import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { oauthNativeFixture, nativeIdentity, good, digest } from './support/oauth-native-http.mjs';

const CREATE = '/api/capabilities/v1/notes/drafts', HISTORY = '/api/capabilities/v1/invocations/';
const input = { title: 'Общий результат е\u0301 / é', body: 'Сначала агент\nПотом человек 👩🏽‍💻', idempotencyKey: 'actual-oauth-result' };
const post = (f, token, value = input) => f.http(CREATE, { method: 'POST', token, body: JSON.stringify(value) });
const rowCount = (f, table) => f.sql(f.capsFile, db => db.prepare(`SELECT count(*) AS n FROM ${table}`).get().n);
const noContent = response => {
  for (const value of [input.title, input.body]) assert.equal(JSON.stringify(response.body).includes(value), false, 'create-only response excludes note content');
  assert.equal(response.headers['cache-control'], 'no-store');
};

test('actual signed two-account OAuth and refresh share one native result without crossing account or connection boundaries', async t => {
  const f = await oauthNativeFixture(t), alice = nativeIdentity('Алиса'), bob = nativeIdentity('Боб');
  const a = await f.bootstrap(alice), b = await f.bootstrap(bob), session = f.browser();
  const first = await f.connect(alice, a.accountId, { session });
  const other = await f.connect(bob, b.accountId, { session }); // Real Provider account switch and XSRF autoform.
  const made = await post(f, first.tokens.access_token);
  assert.equal(made.status, 201); noContent(made);
  assert.equal(made.body.invocation.status, 'succeeded');
  const invocationId = made.body.invocation.invocationId;
  assert.equal((await f.http(HISTORY + invocationId, { token: other.tokens.access_token })).status, 404);
  const sibling = await f.connect(alice, a.accountId, { session });
  assert.ok(first.flow.connectionId !== sibling.flow.connectionId, 'same profile has two independently consented connections');
  const occupied = await post(f, sibling.tokens.access_token);
  assert.equal(occupied.status, 409); assert.deepEqual(occupied.body, { error: { code: 'invocation_request_conflict' } });
  assert.equal(rowCount(f, 'cap_invocations'), 1);
  const original = good(await f.call(alice, 'notes.get', { expectedAccountId: a.accountId, noteId: made.body.result.noteId })).note;
  assert.equal(original.title, input.title); assert.equal(original.body, input.body);
  good(await f.call(alice, 'notes.put', { expectedAccountId: a.accountId, noteId: original.noteId, mutationId: 'human-after-real-oauth',
    expectedRevision: 1, title: 'Правка владельца', body: 'Новый закрытый текст', items: [], pinned: false, color: 'plain', state: 'active' }));
  const rotated = f.tokens(await f.refresh(first.flow, first.tokens));
  assert.ok(digest(rotated.access_token) !== digest(first.tokens.access_token), 'refresh mints a distinct access credential');
  const replay = await post(f, rotated.access_token);
  assert.equal(replay.status, 200); assert.equal(replay.body.reused, true);
  assert.deepEqual(replay.body.result, made.body.result); noContent(replay);
  assert.equal(JSON.stringify(replay.body).includes('Новый закрытый текст'), false);
  const history = await f.http(HISTORY + invocationId, { token: rotated.access_token });
  assert.equal(history.status, 200); noContent(history);
  assert.equal(f.sql(f.notesFile, db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n), 1);
  const owned = good(await f.call(alice, 'oauth.connections.list', { expectedAccountId: a.accountId }));
  assert.equal(owned.connections.length, 2); assert.equal(owned.connections.find(x => x.id === first.flow.connectionId).budget.spent, 1);
  const projection = good(await f.call(alice, 'access.principals.list', { expectedAccountId: a.accountId }));
  assert.equal(projection.principals.filter(x => x.managedBy === 'oauth').length, 2);
  for (const file of [f.capsFile, f.capsFile + '-wal'].filter(existsSync)) {
    const bytes = readFileSync(file);
    for (const secret of [first.flow.code, first.tokens.access_token, first.tokens.refresh_token, rotated.access_token]) {
      assert.equal(bytes.includes(Buffer.from(secret)), false, 'capability storage contains no raw code or bearer');
    }
  }
});

test('actual Provider rejects wrong resource and PKCE before consuming code; rotated refresh reuse revokes only its family', async t => {
  const f = await oauthNativeFixture(t), actor = nativeIdentity('Владелец'), account = await f.bootstrap(actor);
  const flow = await f.complete(await f.decide(await f.begin(), actor, account.accountId));
  const wrongResource = await f.exchange(flow, { resource: f.origin + '/mcp' });
  assert.equal(wrongResource.status, 400); assert.equal(wrongResource.body.error, 'invalid_target');
  const wrongPkce = await f.exchange(flow, { code_verifier: 'b'.repeat(43) });
  assert.equal(wrongPkce.status, 400); assert.equal(wrongPkce.body.error, 'invalid_grant');
  const original = f.tokens(await f.exchange(flow));
  const sibling = await f.connect(actor, account.accountId);
  const badRefresh = await f.refresh(flow, original, { resource: f.origin + '/mcp' });
  assert.equal(badRefresh.status, 400); assert.equal(badRefresh.body.error, 'invalid_target');
  const rotated = f.tokens(await f.refresh(flow, original));
  const reused = await f.refresh(flow, original);
  assert.equal(reused.status, 400); assert.equal(reused.body.error, 'invalid_grant');
  assert.ok([401, 403].includes((await post(f, rotated.access_token)).status));
  assert.equal((await post(f, sibling.tokens.access_token)).status, 201);
  const connections = good(await f.call(actor, 'oauth.connections.list', { expectedAccountId: account.accountId })).connections;
  assert.equal(connections.find(x => x.id === flow.connectionId).active, false);
  assert.equal(connections.find(x => x.id === sibling.flow.connectionId).active, true);
  assert.equal(rowCount(f, 'cap_invocations'), 1);
});

test('actual issued OAuth bearer survives AS-off keyless restart for own read/replay, while new execution and later revoked access fail', async t => {
  const f = await oauthNativeFixture(t), actor = nativeIdentity('Владелец'), account = await f.bootstrap(actor);
  const linked = await f.connect(actor, account.accountId);
  const made = await post(f, linked.tokens.access_token); assert.equal(made.status, 201);
  const invocationId = made.body.invocation.invocationId;
  await f.keyless();
  assert.equal(f.app.locals.oauthStatus.enabled, false);
  assert.equal(f.app.locals.capabilitiesApiStatus().notesCreateEnabled, false);
  assert.equal((await f.http('/.well-known/oauth-protected-resource')).status, 200);
  assert.equal((await f.refresh(linked.flow, linked.tokens)).status, 503);
  const read = await f.http(HISTORY + invocationId, { token: linked.tokens.access_token });
  assert.equal(read.status, 200); noContent(read);
  const replay = await post(f, linked.tokens.access_token);
  assert.equal(replay.status, 200); assert.equal(replay.body.reused, true); assert.deepEqual(replay.body.result, made.body.result);
  assert.equal((await post(f, linked.tokens.access_token, { ...input, idempotencyKey: 'off-new-execution' })).status, 503);
  good(await f.call(actor, 'oauth.connections.revoke', { expectedAccountId: account.accountId, connectionId: linked.flow.connectionId }));
  assert.ok([401, 403].includes((await f.http(HISTORY + invocationId, { token: linked.tokens.access_token })).status));
  assert.equal(rowCount(f, 'cap_invocations'), 1);
});

test('foreign signed account and wrong completion cannot approve or switch a pending decision; explicit denial creates no connection', async t => {
  const f = await oauthNativeFixture(t), alice = nativeIdentity('Алиса'), bob = nativeIdentity('Боб');
  const a = await f.bootstrap(alice), b = await f.bootstrap(bob), pending = await f.begin();
  const context = pending.context;
  const forged = await f.call(bob, 'oauth.connections.approve', { expectedAccountId: a.accountId,
    interactionId: context.interactionId, browserNonce: context.browserNonce, contextDigest: context.contextDigest });
  assert.equal(forged.ok, false); assert.equal(rowCount(f, 'cap_oauth_connections'), 0);
  const denied = await f.decide(pending, alice, a.accountId, { approve: false });
  assert.equal((await pending.session.request(pending.pathname + '/complete', { fields: { expectedAccountId: b.accountId } })).status, 403);
  const callback = await f.complete(denied);
  assert.equal(callback.error, 'access_denied'); assert.equal(callback.code, null);
  assert.equal(rowCount(f, 'cap_oauth_connections'), 0); assert.equal(rowCount(f, 'cap_credentials'), 0);
});

test('expired actual Provider resume returns a usable static error without reflecting authorization data', async t => {
  const f = await oauthNativeFixture(t);
  const response = await f.wire.request('/oauth/authorize/expired_authorization_fixture');
  assert.equal(response.status, 400); assert.equal(response.location, null);
  assert.equal(response.headers.get('cache-control'), 'no-store');
  assert.ok(response.text.includes('Запрос подключения недоступен') && response.text.includes('<a href="/">Открыть Соты</a>'));
  assert.equal(response.text.includes('expired_authorization_fixture'), false);
  assert.equal(/<script\b|<form\b|\son[a-z]+=/iu.test(response.text), false);
  const style = /<style>([\s\S]+?)<\/style>/u.exec(response.text)?.[1]; assert.ok(style);
  const hash = createHash('sha256').update(style).digest('base64');
  const csp = response.headers.get('content-security-policy');
  assert.ok(csp.includes(`style-src 'sha256-${hash}'`) && csp.includes("script-src 'none'") && csp.includes("form-action 'none'"));
  assert.equal(rowCount(f, 'cap_oauth_connections'), 0); assert.equal(rowCount(f, 'cap_credentials'), 0);
});
