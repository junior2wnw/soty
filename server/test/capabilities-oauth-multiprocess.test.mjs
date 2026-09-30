import test from 'node:test';
import assert from 'node:assert/strict';
import { twoASFixture, digest, expectTokens, expectInvalidGrant } from './support/oauth-as-processes.mjs';

const pending = promise => promise.catch(() => ({ transportLost: true }));
const immutable = state => state.credentials.map(({ id, created_at, expires_at }) => ({ id, created_at, expires_at }));
const revoked = state => {
  assert.equal(state.state, 'revoked'); assert.ok(Number.isSafeInteger(state.revokedAt));
  assert.ok(Number.isSafeInteger(state.rootRevokedAt));
  assert.ok(state.credentials.every(row => Number.isSafeInteger(row.revoked_at)), 'no active right survives family revoke');
};
const operations = (worker, method, model, status, token) => worker.events.filter(event => event.kind === 'operation'
  && event.method === method && event.model === model && event.status === status
  && (token === undefined || event.idHash === digest(token))).length;
const notesCount = f => f.sql('notes', db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n);
const point = (point, method, model, token) => ({ point, method, model, ...(token ? { hash: digest(token) } : {}) });

test('two actual AS processes read one code before consume; reuse commits revoke before the winner can issue late AT', { timeout: 30000 }, async t => {
  const f = await twoASFixture(t), flow = await f.complete('a', await f.begin('a'));
  const sibling = await f.connection('b'), a = f.workers.get('a'), b = f.workers.get('b');
  await a.arm([point('a-found', 'find', 'AuthorizationCode', flow.code), point('a-consumed', 'consume', 'AuthorizationCode', flow.code)]);
  await b.arm([point('b-found', 'find', 'AuthorizationCode', flow.code)]);
  const first = pending(f.exchange('a', flow)), second = pending(f.exchange('b', flow));
  await Promise.all([a.wait('a-found'), b.wait('b-found')]);
  f.assertLocksReleased(); assert.equal(f.source('AuthorizationCode', flow.code).consumed_at, null);
  a.release(); await a.wait('a-consumed');
  f.assertLocksReleased(); assert.ok(Number.isSafeInteger(f.source('AuthorizationCode', flow.code).consumed_at));
  b.release(); expectInvalidGrant(await second); revoked(f.state(flow.connectionId));
  a.release(); expectInvalidGrant(await first);
  await Promise.all([a.waitOperation('consume', 'AuthorizationCode', 'consumed', flow.code),
    b.waitOperation('consume', 'AuthorizationCode', 'invalid_grant', flow.code), a.waitOperation('upsert', 'AccessToken', 'refused')]);
  assert.equal(operations(a, 'consume', 'AuthorizationCode', 'consumed', flow.code) + operations(b, 'consume', 'AuthorizationCode', 'consumed', flow.code), 1);
  assert.equal(operations(b, 'consume', 'AuthorizationCode', 'invalid_grant', flow.code), 1);
  assert.equal(operations(a, 'upsert', 'AccessToken', 'refused'), 1, 'actual late issuance reached and was refused');
  assert.equal(f.state(flow.connectionId).credentials.length, 0); assert.equal(notesCount(f), 0);
  await f.restart('a'); revoked(f.state(flow.connectionId));
  assert.equal((await f.post('a', sibling.tokens.access_token, 'code-race-sibling')).status, 201);
  assert.equal(notesCount(f), 1);
});

test('two actual AS processes read one RT; the completed token response does not survive a later durable reuse revoke', { timeout: 30000 }, async t => {
  const f = await twoASFixture(t), own = await f.connection('a'), sibling = await f.connection('b');
  const a = f.workers.get('a'), b = f.workers.get('b'), source = own.tokens.refresh_token;
  await a.arm([point('a-found', 'find', 'RefreshToken', source)]);
  await b.arm([point('b-found', 'find', 'RefreshToken', source)]);
  const first = pending(f.refresh('a', own.flow, own.tokens)), second = pending(f.refresh('b', own.flow, own.tokens));
  await Promise.all([a.wait('a-found'), b.wait('b-found')]); f.assertLocksReleased();
  a.release(); const issued = expectTokens(await first);
  assert.equal((await f.bearer('a', issued.access_token)).status, 404, 'fresh AT authenticates but cannot invent an Invocation');
  const snapshot = immutable(f.state(own.flow.connectionId));
  b.release(); expectInvalidGrant(await second); revoked(f.state(own.flow.connectionId));
  await Promise.all([a.waitOperation('consume', 'RefreshToken', 'consumed', source),
    b.waitOperation('consume', 'RefreshToken', 'invalid_grant', source)]);
  assert.equal(operations(a, 'consume', 'RefreshToken', 'consumed', source) + operations(b, 'consume', 'RefreshToken', 'consumed', source), 1);
  assert.equal(operations(b, 'consume', 'RefreshToken', 'invalid_grant', source), 1);
  await f.restart('b'); revoked(f.state(own.flow.connectionId));
  assert.deepEqual(immutable(f.state(own.flow.connectionId)), snapshot, 'original expiry/identity never extended');
  assert.ok([401, 403].includes((await f.bearer('b', issued.access_token)).status));
  assert.equal((await f.post('b', sibling.tokens.access_token, 'rt-race-sibling')).status, 201);
  assert.equal(notesCount(f), 1);
});

test('kill after actual Grant/link COMMIT resumes the signed decision on the second AS with exactly the same authority', { timeout: 30000 }, async t => {
  const f = await twoASFixture(t), flow = await f.begin('a'), a = f.workers.get('a');
  await a.arm([point('grant-committed', 'upsert', 'Grant')]);
  const request = pending(flow.session.request('a', flow.pathname + '/complete', { fields: { expectedAccountId: f.accountId } }));
  const observed = await a.wait('grant-committed'); f.assertLocksReleased();
  const before = f.state(flow.connectionId);
  assert.ok(before.providerGrantId === observed.token, 'committed provider link names the actual saved Grant');
  const identities = [before.clientId, before.principalId, before.rootGrantId, before.providerGrantId];
  assert.equal(before.artifacts.Grant, 1); assert.equal(before.credentials.length, 0);
  await a.kill(); const lost = await request;
  assert.ok(lost.status === 502 || lost.transportLost === true, 'no successful completion response was delivered');
  await f.restart('a');
  const completed = await f.complete('b', flow), tokens = expectTokens(await f.exchange('b', completed));
  const after = f.state(flow.connectionId);
  assert.deepEqual([after.clientId, after.principalId, after.rootGrantId, after.providerGrantId], identities);
  for (const table of ['cap_oauth_connections', 'cap_clients', 'cap_principals', 'cap_grants', 'cap_budgets']) {
    assert.equal(f.sql('caps', db => db.prepare(`SELECT count(*) AS n FROM ${table}`).get().n), 1, 'exactly one ' + table);
  }
  assert.equal(after.artifacts.Grant, 1); assert.equal(after.credentials.length, 1);
  assert.equal((await f.post('a', tokens.access_token, 'grant-crash-recovered')).status, 201);
  assert.equal(notesCount(f), 1);
});

// These are the actual 9.12.2 orders: code -> AT -> RT; RT rotation -> RT -> AT.
// Each pause is AFTER a real port returned from COMMIT and BEFORE Provider can
// advance to its next await/save or send the HTTP response. No expiry is faked.
const crashes = [
  { name: 'code consume before AT', kind: 'code', method: 'consume', model: 'AuthorizationCode', at: 0, rt: 0 },
  { name: 'code AT triple COMMIT before RT/response', kind: 'code', method: 'upsert', model: 'AccessToken', at: 1, rt: 0 },
  { name: 'RT consume before successor RT', kind: 'refresh', method: 'consume', model: 'RefreshToken', at: 1, rt: 1 },
  { name: 'successor RT COMMIT before AT', kind: 'refresh', method: 'upsert', model: 'RefreshToken', at: 1, rt: 2 },
  { name: 'refresh AT triple COMMIT before token response', kind: 'refresh', method: 'upsert', model: 'AccessToken', at: 2, rt: 2 },
];
for (const scenario of crashes) test('actual AS process kill: ' + scenario.name, { timeout: 30000 }, async t => {
  const f = await twoASFixture(t);
  const own = scenario.kind === 'code' ? { flow: await f.complete('a', await f.begin('a')) } : await f.connection('a');
  const source = scenario.kind === 'code' ? own.flow.code : own.tokens.refresh_token;
  const sourceModel = scenario.kind === 'code' ? 'AuthorizationCode' : 'RefreshToken';
  const a = f.workers.get('a');
  await a.arm([point('committed', scenario.method, scenario.model, scenario.method === 'consume' ? source : undefined)]);
  const attempt = () => scenario.kind === 'code' ? f.exchange('a', own.flow) : f.refresh('a', own.flow, own.tokens);
  const request = pending(attempt()), observed = await a.wait('committed');
  f.assertLocksReleased(); assert.ok(Number.isSafeInteger(f.source(sourceModel, source).consumed_at));
  const before = f.state(own.flow.connectionId), snapshot = immutable(before);
  assert.equal(before.state, 'active');
  assert.equal(before.artifacts.AccessToken ?? 0, scenario.at); assert.equal(before.artifacts.RefreshToken ?? 0, scenario.rt);
  assert.equal(before.credentials.length, scenario.at, 'AT artifact + credential + link committed atomically');
  assert.equal(before.invocations, 0); assert.equal(notesCount(f), 0);
  await a.kill(); const lost = await request;
  assert.ok(lost.status === 502 || lost.transportLost === true, 'token response was not delivered');
  await f.restart('a');
  assert.deepEqual(immutable(f.state(own.flow.connectionId)), snapshot);
  assert.equal(f.state(own.flow.connectionId).state, 'active', 'crash alone is not a fabricated revoke or rollback');
  if (scenario.model === 'AccessToken') {
    // This token is known only to test IPC, not recovered from an OAuth retry.
    assert.equal((await f.bearer('b', observed.token)).status, 404, 'committed lost AT is still a real right before revoke');
  }
  const retry = scenario.kind === 'code' ? await f.exchange('b', own.flow) : await f.refresh('b', own.flow, own.tokens);
  expectInvalidGrant(retry); revoked(f.state(own.flow.connectionId));
  assert.deepEqual(immutable(f.state(own.flow.connectionId)), snapshot);
  if (scenario.model === 'AccessToken') assert.ok([401, 403].includes((await f.bearer('a', observed.token)).status));
  assert.equal(f.state(own.flow.connectionId).invocations, 0); assert.equal(notesCount(f), 0);
});
