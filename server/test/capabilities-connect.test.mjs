import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { generateKeyPairSync, sign } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { createHttpApp } from '../http-app.js';
import { digestArgs } from '../../modules/connect/server/index.mjs';

function identity(label) {
  const signing = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { label, signing: signing.privateKey, publicJwk: signing.publicKey.export({ format: 'jwk' }),
    encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) };
}
function good(result) { assert.equal(result.ok, true, result.error?.code); return result; }
function denied(result) { assert.equal(result.ok, false); assert.equal(typeof result.error?.code, 'string'); return result.error.code; }

async function fixture(t) {
  const parent = resolve(tmpdir());
  const directory = mkdtempSync(join(parent, 'soty-capabilities-http-'));
  let app;
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const origin = `http://127.0.0.1:${server.address().port}`;
  t.after(async () => {
    server.closeAllConnections();
    await new Promise(done => server.close(done));
    await app?.locals.closeServices();
    assert.equal(dirname(resolve(directory)), parent);
    assert.ok(resolve(directory).startsWith(join(parent, 'soty-capabilities-http-')));
    rmSync(directory, { recursive: true, force: true });
  });
  app = createHttpApp(resolve('dist'), { dataDir: directory, connectOrigins: [origin],
    appOriginTemplate: '', gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' } });
  async function send(input, requestOrigin = origin) {
    const response = await fetch(`${origin}/api/connect/rpc`, { method: 'POST',
      headers: { 'content-type': 'application/json', ...(requestOrigin ? { origin: requestOrigin } : {}) },
      body: JSON.stringify({ protocol: 1, ...input }) });
    assert.equal(response.headers.get('cache-control'), 'no-store');
    return response.json();
  }
  async function proof(actor, op, args) {
    const challenge = good(await send({ op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
    return { op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: actor.signing, dsaEncoding: 'ieee-p1363' }).toString('base64url') } };
  }
  const call = async (actor, op, args = {}) => send(await proof(actor, op, args));
  const bootstrap = async actor => good(await call(actor, 'bootstrap', { label: actor.label, encryptionPublicJwk: actor.encryptionPublicJwk }));
  return { service: app.locals.capabilitiesService, origin, send, proof, call, bootstrap };
}

test('real application signed RPC keeps service clients account-scoped and revocation survives a retained actor', async t => {
  const f = await fixture(t);
  const alice = identity('Алиса'), bob = identity('Боб');
  const a = await f.bootstrap(alice), b = await f.bootstrap(bob);
  const principal = good(await f.call(alice, 'access.principals.create', {
    expectedAccountId: a.accountId, label: 'Редактор', clientLabel: 'Локальная проверка',
  })).principal;
  assert.equal(principal.accountId, a.accountId);
  assert.deepEqual(good(await f.call(bob, 'access.principals.list', { expectedAccountId: b.accountId })).principals, []);
  denied(await f.call(bob, 'access.principals.list', { expectedAccountId: a.accountId }));
  denied(await f.call(bob, 'access.principals.revoke', { expectedAccountId: b.accountId, principalId: principal.id }));
  const grant = good(await f.call(alice, 'access.grants.issue', {
    expectedAccountId: a.accountId, principalId: principal.id,
    capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }],
    resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'],
    expiresAt: Date.now() + 3_600_000, allowDelegation: false, maxDepth: 0,
    budget: { unit: 'invocations', limit: 3 },
  })).grant;
  const credential = good(await f.call(alice, 'access.credentials.issue', {
    expectedAccountId: a.accountId, grantId: grant.id, audience: f.origin,
  }));
  assert.equal(typeof credential.token, 'string');
  const actor = f.service.authenticateCredential({ token: credential.token, audience: f.origin });
  assert.ok(actor && Object.isFrozen(actor));
  assert.throws(() => f.service.authenticateCredential({ token: credential.token, audience: `${f.origin}/elsewhere` }));
  const before = good(await f.call(alice, 'access.grants.list', { expectedAccountId: a.accountId }));
  assert.equal(before.grants.length, 1);
  assert.equal(before.grants[0].budget.remaining, 3);
  assert.equal(JSON.stringify(before).includes(credential.token), false, 'list must never echo a credential');
  const history = good(await f.call(alice, 'access.invocations.list', { expectedAccountId: a.accountId }));
  assert.deepEqual(history.invocations, []);
  denied(await f.call(bob, 'access.invocations.list', { expectedAccountId: a.accountId }));
  const audit = good(await f.call(alice, 'access.events.list', { expectedAccountId: a.accountId, limit: 1 }));
  assert.equal(audit.events.length, 1);
  assert.equal(JSON.stringify(audit).includes(credential.token), false, 'audit must not contain a credential');
  assert.deepEqual(good(await f.call(bob, 'access.events.list', { expectedAccountId: b.accountId })).events, []);
  denied(await f.call(bob, 'access.credentials.issue', { expectedAccountId: b.accountId, grantId: grant.id, audience: f.origin }));
  good(await f.call(alice, 'access.grants.revoke', { expectedAccountId: a.accountId, grantId: grant.id }));
  assert.throws(() => f.service.authenticateCredential({ token: credential.token, audience: f.origin }));
  assert.throws(() => f.service.authorize({ actor, capabilityId: 'notes.createDraft', version: 1,
    resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'] }));
  assert.deepEqual(good(await f.call(bob, 'access.grants.list', { expectedAccountId: b.accountId })).grants, []);
});

test('a real issuer device revoke invalidates credentials and signed management rejects tampering or missing Origin', async t => {
  const f = await fixture(t);
  const owner = identity('Владелец'), phone = identity('Телефон');
  const account = await f.bootstrap(owner);
  const enrollment = good(await f.call(phone, 'enrollment.start', { label: phone.label, encryptionPublicJwk: phone.encryptionPublicJwk }));
  good(await f.call(owner, 'enrollment.approve', { requestId: enrollment.requestId,
    wrappedKey: { schema: 'test.encrypted', ciphertext: 'opaque-test-data' } }));
  const joined = good(await f.call(phone, 'enrollment.finish', { requestId: enrollment.requestId, expectedAccountId: account.accountId }));
  const principal = good(await f.call(phone, 'access.principals.create', { expectedAccountId: account.accountId, label: 'Клиент телефона' })).principal;
  const args = { expectedAccountId: account.accountId, principalId: principal.id,
    capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }],
    resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'],
    expiresAt: Date.now() + 3_600_000, allowDelegation: false, maxDepth: 0,
    budget: { unit: 'invocations', limit: 1 } };
  const signed = await f.proof(phone, 'access.grants.issue', args);
  signed.args = { ...args, budget: { unit: 'invocations', limit: 1000 } };
  assert.equal(denied(await f.send(signed)), 'challenge_mismatch');
  denied(await f.send({ op: 'challenge', args: { operation: 'access.grants.issue', digest: digestArgs(args) } }, ''));
  const grant = good(await f.call(phone, 'access.grants.issue', args)).grant;
  const credential = good(await f.call(phone, 'access.credentials.issue', {
    expectedAccountId: account.accountId, grantId: grant.id, audience: f.origin,
  }));
  assert.ok(f.service.authenticateCredential({ token: credential.token, audience: f.origin }));
  good(await f.call(owner, 'device.revoke', { deviceId: joined.deviceId }));
  assert.throws(() => f.service.authenticateCredential({ token: credential.token, audience: f.origin }));
  assert.equal(denied(await f.call(phone, 'access.principals.list', { expectedAccountId: account.accountId })), 'device_revoked');
  assert.equal(good(await f.call(owner, 'access.principals.list', { expectedAccountId: account.accountId })).principals.length, 1);
});

test('the real signed app route accepts the account-bound UI and rejects an old view after an account change', async t => {
  const f = await fixture(t);
  const alice = identity('App owner'), bob = identity('Another account');
  const a = await f.bootstrap(alice), b = await f.bootstrap(bob);
  assert.deepEqual(good(await f.call(alice, 'apps.devices', { expectedAccountId: a.accountId })).devices, []);
  assert.deepEqual(good(await f.call(alice, 'apps.list', { expectedAccountId: a.accountId })).apps, []);
  assert.deepEqual(good(await f.call(bob, 'apps.devices', { expectedAccountId: b.accountId })).devices, []);
  assert.equal(denied(await f.call(bob, 'apps.devices', { expectedAccountId: a.accountId })), 'authentication_required');
  assert.equal(denied(await f.call(bob, 'apps.revoke', { expectedAccountId: a.accountId, appId: 'app-00000000000000000000000000000000' })), 'authentication_required');
  assert.equal(denied(await f.call(alice, 'apps.devices', { expectedAccountId: a.accountId, extra: true })), 'unexpected_argument');
});
