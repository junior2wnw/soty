import test from 'node:test';
import assert from 'node:assert/strict';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';
import { parseDelegationJson } from '../capabilities-ingress.js';
import { buildCapabilitiesOpenApi } from '../capabilities-openapi.js';
import { SERVICE_DELEGATION_PATH as DERIVE, SERVICE_DELEGATION_BODY_BYTES } from '../capabilities-http-contract.js';

const CREATE = '/api/capabilities/v1/notes/drafts', HISTORY = '/api/capabilities/v1/invocations/';
async function parent(f, owner, accountId, { allow = true, audience = f.origin, budget = 2 } = {}) {
  const principal = good(await f.call(owner, 'access.principals.create', { expectedAccountId: accountId,
    label: 'Родитель', clientLabel: 'Внешний сервис' })).principal;
  const grant = good(await f.call(owner, 'access.grants.issue', { expectedAccountId: accountId, principalId: principal.id,
    capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }], resources: ['notes:new'], effects: ['create'],
    recipients: ['soty:notes'], expiresAt: Date.now() + 3600000, allowDelegation: allow, maxDepth: allow ? 1 : 0,
    budget: { unit: 'invocations', limit: budget } })).grant;
  const issued = good(await f.call(owner, 'access.credentials.issue', { expectedAccountId: accountId, grantId: grant.id, audience }));
  return { principal, grant, ...issued };
}
const derive = (f, token, value = { label: 'Помощник', expiresAt: Date.now() + 600000 }, options = {}) =>
  f.http(DERIVE, { method: 'POST', token, body: JSON.stringify(value), ...options });
const create = (f, token, key) => f.http(CREATE, { method: 'POST', token,
  body: JSON.stringify({ title: 'От помощника', body: 'Точная приватная работа\n🚀 е\u0301', idempotencyKey: key }) });

test('actual signed owner and headless child share budget but not private history; key and grant revocation differ', async t => {
  const f = await nativeHttpFixture(t), owner = nativeIdentity('Владелец'), account = await f.bootstrap(owner);
  const issuer = await parent(f, owner, account.accountId);
  const issued = await derive(f, issuer.token);
  assert.equal(issued.status, 201); assert.deepEqual(Object.keys(issued.body).sort(), ['credential', 'grant', 'principal', 'token']);
  assert.equal(issued.headers['cache-control'], 'no-store'); assert.equal(issued.headers['referrer-policy'], 'no-referrer');
  assert.equal(issued.headers['access-control-allow-origin'], undefined); assert.equal(issued.headers['retry-after'], undefined);
  const child = issued.body;
  assert.ok(typeof child.token === 'string' && /^soty_cap_[A-Za-z0-9_-]{43}$/u.test(child.token), 'one-shot secret has the required shape');
  assert.equal(child.grant.parentGrantId, issuer.grant.id); assert.equal(child.grant.rootGrantId, issuer.grant.id);
  assert.equal(child.grant.allowDelegation, false); assert.equal(child.grant.maxDepth, 0); assert.equal(child.grant.depth, 1);
  assert.equal(child.credential.audience, f.origin); assert.ok(child.credential.expiresAt <= issuer.credential.expiresAt);
  assert.notEqual(child.principal.id, issuer.principal.id); assert.notEqual(child.principal.clientId, issuer.principal.clientId);
  const own = await create(f, issuer.token, 'parent-private-history'), first = await create(f, child.token, 'child-exact-private-work');
  assert.equal(own.status, 201); assert.equal(first.status, 201);
  const replay = await create(f, child.token, 'child-exact-private-work');
  assert.equal(replay.status, 200); assert.equal(replay.body.reused, true);
  assert.equal(replay.body.invocation.invocationId, first.body.invocation.invocationId);
  const id = first.body.invocation.invocationId;
  assert.equal((await f.http(HISTORY + id, { token: issuer.token })).status, 404);
  assert.equal((await f.http(HISTORY + own.body.invocation.invocationId, { token: child.token })).status, 404);
  const note = good(await f.call(owner, 'notes.get', { expectedAccountId: account.accountId, noteId: first.body.result.noteId })).note;
  assert.equal(note.body, 'Точная приватная работа\n🚀 е\u0301');
  const history = good(await f.call(owner, 'access.invocations.list', { expectedAccountId: account.accountId, limit: 20 }));
  assert.ok(history.invocations.some(value => value.invocationId === id), 'the owner sees the child invocation in existing history');
  const events = good(await f.call(owner, 'access.events.list', { expectedAccountId: account.accountId, limit: 50 })).events;
  const event = events.find(value => value.objectId === child.grant.id && value.kind === 'access.grants.derive');
  assert.equal(event.actorType, 'service'); assert.equal(event.actorId, issuer.principal.id);
  assert.equal(JSON.stringify(events).includes(child.token), false, 'owner audit excludes the one-shot secret');
  good(await f.call(owner, 'access.credentials.revoke', { expectedAccountId: account.accountId, credentialId: issuer.credential.id }));
  assert.equal((await derive(f, issuer.token)).status, 401);
  assert.equal((await f.http(HISTORY + id, { token: child.token })).status, 200, 'revoking only the issuer key does not revoke the child grant');
  good(await f.call(owner, 'access.grants.revoke', { expectedAccountId: account.accountId, grantId: issuer.grant.id }));
  assert.ok([401, 403].includes((await f.http(HISTORY + id, { token: child.token })).status));
  assert.deepEqual(f.sql(f.capsFile, db => ({ ...db.prepare('SELECT reserved_amount,spent_amount FROM cap_budgets').get() })),
    { reserved_amount: 0, spent_amount: 2 });
  assert.equal(f.sql(f.capsFile, db => db.prepare('SELECT count(*) AS n FROM cap_budgets').get().n), 1);
  assert.equal(f.sql(f.notesFile, db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n), 2);
});

test('derive remains available with native execution disabled, and rejects nondelegable or wrong-resource parents without issuance', async t => {
  const f = await nativeHttpFixture(t, { enabled: false }), owner = nativeIdentity('Владелец'), account = await f.bootstrap(owner);
  const allowed = await parent(f, owner, account.accountId), denied = await parent(f, owner, account.accountId, { allow: false });
  const mcp = await parent(f, owner, account.accountId, { audience: `${f.origin}/mcp` });
  const before = f.sql(f.capsFile, db => db.prepare('SELECT count(*) AS n FROM cap_grants').get().n);
  assert.equal((await derive(f, denied.token)).status, 403);
  assert.equal((await derive(f, mcp.token)).status, 401);
  assert.equal(f.sql(f.capsFile, db => db.prepare('SELECT count(*) AS n FROM cap_grants').get().n), before);
  const issued = await derive(f, allowed.token);
  assert.equal(issued.status, 201);
  assert.equal((await create(f, issued.body.token, 'disabled-native-helper')).status, 503);
  assert.equal(f.sql(f.notesFile, db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n), 0);
});

test('strict service issuance ingress rejects authority injection, duplicate keys, Unicode and raw body overflow before mutation', async t => {
  const f = await nativeHttpFixture(t), owner = nativeIdentity('Владелец'), account = await f.bootstrap(owner);
  const issuer = await parent(f, owner, account.accountId), expiry = Date.now() + 600000;
  const cases = [
    { body: JSON.stringify({ label: 'x', expiresAt: expiry, parentGrantId: issuer.grant.id }), status: 400 },
    { body: `{"label":"x","la\\u0062el":"y","expiresAt":${expiry}}`, status: 400 },
    { body: JSON.stringify({ label: '\ud800', expiresAt: expiry }), status: 400 },
    { body: JSON.stringify({ label: 'x', expiresAt: String(expiry) }), status: 400 },
    { body: JSON.stringify({ label: 'x', expiresAt: 1 }), status: 400 },
    { body: ' '.repeat(SERVICE_DELEGATION_BODY_BYTES + 1), status: 413 },
    { body: Buffer.from([0xff]), status: 400 },
    { body: '{}', headers: { 'content-type': 'text/plain' }, status: 415 },
    { body: '{}', headers: { origin: 'https://foreign.invalid' }, status: 403 },
    { body: '{}', headers: { host: 'foreign.invalid' }, status: 400 },
  ];
  for (let index = 0; index < cases.length; index++) {
    const result = await f.http(DERIVE, { method: 'POST', token: issuer.token, ...cases[index] });
    assert.equal(result.status, cases[index].status, `closed case ${index}`);
    assert.equal(Object.hasOwn(result.body, 'token'), false, 'failure contains no secret');
    assert.equal(result.headers['retry-after'], undefined);
  }
  assert.equal(f.sql(f.capsFile, db => db.prepare('SELECT count(*) AS n FROM cap_grants').get().n), 1);
  assert.equal(f.sql(f.capsFile, db => db.prepare('SELECT count(*) AS n FROM cap_credentials').get().n), 1);
});

test('flat scalar parser and OpenAPI preserve the exact issuance contract and one-shot recovery rule', () => {
  const parsed = parseDelegationJson(' {"expiresAt":1e12,"la\\u0062el":"Помощник"}\n');
  assert.deepEqual({ ...parsed }, { expiresAt: 1000000000000, label: 'Помощник' });
  for (const raw of ['[]', '{"label":"x","expiresAt":null}', '{"label":{},"expiresAt":1}',
    '{"label":"x","expiresAt":1.5}', '{"label":"x","expiresAt":9007199254740992}',
    '{"label":"x","expiresAt":1,}', '\ufeff{"label":"x","expiresAt":1}']) {
    assert.throws(() => parseDelegationJson(raw), error => error.code === 'invalid_input');
  }
  const doc = buildCapabilitiesOpenApi(), operation = doc.paths[DERIVE].post;
  assert.deepEqual(Object.keys(doc.paths[DERIVE]), ['post']);
  assert.equal(operation.operationId, 'deriveNotesServiceGrant');
  assert.deepEqual(operation.security, [{ CapabilityBearer: [] }]);
  assert.deepEqual(doc.components.schemas.ServiceDelegationRequest.required, ['label', 'expiresAt']);
  assert.equal(doc.components.schemas.ServiceDelegationRequest.additionalProperties, false);
  assert.equal(doc.components.schemas.ServiceDelegationRequest['x-soty-max-raw-bytes'], 16384);
  assert.equal(operation.responses[503].headers, undefined); assert.equal(operation.responses[429].headers, undefined);
  assert.match(operation.description, /never automatically repeat issuance/u);
  assert.equal(doc.components.schemas.ServiceDelegationResponse.properties.token['x-soty-one-shot-secret'], true);
});
