import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { performance } from 'node:perf_hooks';
import { createHumanIdentityHostProfile } from '../profile.mjs';
import { createHumanIdentityService } from '../service.mjs';
import { createOAuthIngress, HUMAN_USERINFO_HTTP_LIMITS } from '../../../server/capabilities-oauth-ingress.js';
import { environment } from './support/renewal-fixture.mjs';

const random = () => randomBytes(32).toString('base64url');
const privateJwk = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
Object.assign(privateJwk, { kid: 'userinfo-budget-fixture', alg: 'RS256', use: 'sig' });
const throws = (action, code) => assert.throws(action, error => error.code === code);

function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-userinfo-budget-')), databasePath = join(directory, 'identity.sqlite');
  let clock = Date.now(), active = true, fenceHook, fences = 0;
  const origin = 'http://127.0.0.1:19191', options = { enabled: true, issuer: origin + '/human-identity',
    registryId: 'REG.soty', environmentId: 'fixture', clients: ['alpha', 'beta'].map((id, index) => ({ id,
      label: id, redirectUri: `http://127.0.0.1:${19292 + index}/callback`, clientSecret: random() })),
    jwks: { keys: [privateJwk] }, cookieKeys: [random()], artifactKey: randomBytes(32), artifactKeyId: 'fixture' };
  const profile = createHumanIdentityHostProfile(options, { shellOrigins: [origin] });
  const service = createHumanIdentityService({ databasePath, profile, now: () => clock, actorActive: () => active,
    withAuthorityFence(callback) { fences++; fenceHook?.(); return callback(); } });
  t.after(() => { service.close(); assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.match(basename(directory), /^soty-userinfo-budget-/u); rmSync(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 }); });
  function token({ accountId = 'account.fixture', deviceId = 'device.fixture', clientId = 'alpha', grantId, ttl = 300, scope = 'openid profile', claims = {} } = {}) {
    const epoch = Math.floor(clock / 1000);
    if (!grantId) {
      const interactionId = random(), browserNonce = random(), parameters = { client_id: clientId, redirect_uri: profile.client(clientId).redirectUri,
        response_type: 'code', scope: 'openid profile', state: random(), nonce: random(), code_challenge_method: 'S256', code_challenge: random() };
      service.sdk.upsert({ model: 'Interaction', id: interactionId, payload: { jti: interactionId, kind: 'Interaction', params: parameters }, expiresIn: 300, browserNonce });
      const context = service.prepareInteraction({ interactionId, browserNonce, parameters });
      service.execute({ op: 'identity.human.approve', actor: { accountId, deviceId }, args: { expectedAccountId: accountId,
        interactionId, browserNonce, csrf: context.csrf, decision: 'approve', requestId: 'decision.' + random() } });
      const approvedBinding = service.readApprovedInteraction(context); grantId = random();
      service.sdk.upsert({ model: 'Grant', id: grantId, expiresIn: 600, approvedBinding,
        payload: { jti: grantId, kind: 'Grant', accountId, clientId, iat: epoch, exp: epoch + 600 } });
    }
    const value = random(); service.sdk.upsert({ model: 'AccessToken', id: value, expiresIn: ttl,
      payload: { jti: value, kind: 'AccessToken', accountId, clientId, grantId, scope, iat: epoch, exp: epoch + ttl, ...claims } });
    return { value, grantId };
  }
  const request = (value, change = {}) => ({ method: 'GET', originalUrl: '/human-identity/userinfo', url: '/userinfo',
    rawHeaders: ['Authorization', 'Bearer ' + value], socket: { remoteAddress: 'same-real-peer' }, ...change });
  const ingress = change => createOAuthIngress({ userinfoBudget: service.userinfoBudget, ...change });
  const enter = (guard, req, reference = service.userinfoBudget.capture(req)) => guard.enter(req, {}, reference);
  return { service, token, request, ingress, enter, databasePath, profile,
    set active(value) { active = value; }, advance(ms) { clock += ms; },
    set fenceHook(value) { fenceHook = value; }, get fences() { return fences; } };
}

test('a verified AT gets an opaque request-only ref; client/account budgets share the physical peer without sharing rate windows', t => {
  const f = fixture(t), guard = f.ingress({ limits: { attempts: 1 }, userinfoLimits: { attempts: 2 } });
  const alpha = f.token(), beta = f.token({ clientId: 'beta' }), other = f.token({ accountId: 'other-account' });
  const req = f.request(alpha.value), ref = f.service.userinfoBudget.capture(req), start = f.fences;
  assert.equal(Object.isFrozen(ref), true); assert.equal(JSON.stringify(ref), '{}');
  f.enter(guard, req, ref).release(); assert.ok(f.fences > start, 'enter repeats the actual authority fence');
  f.enter(guard, f.request(alpha.value)).release();
  throws(() => f.enter(guard, f.request(alpha.value)), 'rate_limit');
  f.enter(guard, f.request(beta.value)).release(); f.enter(guard, f.request(other.value)).release();
  f.enter(guard, f.request(random())).release();
  throws(() => f.enter(guard, f.request(random())), 'rate_limit');
  // Spending an invalid-token peer window never spends the other valid client's window.
  f.enter(guard, f.request(beta.value)).release();
});

test('a token-shaped artifact with the wrong model payload, jti, audience or expired exp cannot mint a userinfo rate ref', t => {
  const f = fixture(t);
  for (const claims of [{ kind: 'RefreshToken' }, { jti: random() }, { aud: 'other-resource' }, { exp: 1 }]) {
    const at = f.token({ claims }); assert.equal(f.service.userinfoBudget.capture(f.request(at.value)), undefined);
  }
});

test('AT replacement, a new grant and another active device do not reset an account/client budget', t => {
  const f = fixture(t), guard = f.ingress({ userinfoLimits: { attempts: 1 } }), first = f.token();
  f.enter(guard, f.request(first.value)).release();
  const rotation = f.token({ grantId: first.grantId }), newGrant = f.token(), newDevice = f.token({ deviceId: 'backup-device' });
  for (const value of [rotation, newGrant, newDevice]) throws(() => f.enter(guard, f.request(value.value)), 'rate_limit');
});

test('fake/reused refs and changed request, method, bearer or service never bypass an exhausted raw-peer window', t => {
  const f = fixture(t), guard = f.ingress({ limits: { attempts: 1 } }), at = f.token();
  f.enter(guard, f.request(random())).release();
  const req = f.request(at.value), ref = f.service.userinfoBudget.capture(req);
  throws(() => f.enter(guard, req, Object.freeze({})), 'rate_limit');
  throws(() => f.enter(guard, f.request(at.value), ref), 'rate_limit');
  f.enter(guard, req, ref).release(); throws(() => f.enter(guard, req, ref), 'rate_limit');
  for (const change of [q => { q.method = 'POST'; }, q => { q.originalUrl = '/human-identity/token'; },
    q => { q.rawHeaders[1] = 'Bearer ' + random(); }]) {
    const q = f.request(at.value), captured = f.service.userinfoBudget.capture(q); change(q);
    throws(() => f.enter(guard, q, captured), 'rate_limit');
  }
  const foreign = fixture(t), foreignToken = foreign.token(), foreignReq = foreign.request(foreignToken.value);
  throws(() => guard.enter(foreignReq, {}, foreign.service.userinfoBudget.capture(foreignReq)), 'rate_limit');
  throws(() => createOAuthIngress({ userinfoBudget: { capture() {} } }), 'oauth_configuration_invalid');
});

test('current device, grant, client generation and expiry are rechecked after capture, including revocation inside the host fence', t => {
  for (const damage of ['device', 'grant', 'client', 'expiry', 'inside-fence']) {
    const f = fixture(t), guard = f.ingress({ limits: { attempts: 1 } }), at = f.token({ ttl: 2 });
    f.enter(guard, f.request(random())).release();
    const req = f.request(at.value), ref = f.service.userinfoBudget.capture(req); assert.ok(ref);
    if (damage === 'device') f.active = false;
    else if (damage === 'inside-fence') f.fenceHook = () => { f.active = false; };
    else if (damage === 'expiry') f.advance(3000);
    else if (damage === 'grant') f.service.sdk.revokeByGrantId(at.grantId);
    else { const db = new DatabaseSync(f.databasePath); try { db.prepare("UPDATE human_identity_client_heads SET generation=generation+1,updated_at=updated_at+1 WHERE client_id='alpha'").run(); } finally { db.close(); } }
    throws(() => f.enter(guard, req, ref), 'rate_limit');
    assert.equal(f.service.userinfoBudget.capture(f.request(at.value)), undefined);
  }
});

test('malformed/duplicate/oversized bearer, query token, non-userinfo and non-openid tokens keep legacy allocation', t => {
  const f = fixture(t), at = f.token(), noScope = f.token({ scope: 'profile' });
  for (const request of [f.request(at.value, { originalUrl: '/human-identity/token' }),
    f.request(at.value, { originalUrl: '/human-identity/userinfo?access_token=' + at.value }),
    f.request(at.value, { originalUrl: '/human-identity/userinfo?unused=x' }),
    f.request(at.value, { method: 'DELETE' }), f.request(at.value, { method: 'POST' }),
    f.request(at.value, { rawHeaders: ['authorization', 'Bearer ' + at.value, 'Authorization', 'Bearer ' + at.value] }),
    f.request(at.value, { rawHeaders: ['Authorization', 'Bearer ' + 'a'.repeat(257)] }),
    f.request(at.value, { rawHeaders: ['Authorization', 'Bearer  ' + at.value] }),
    f.request(at.value, { rawHeaders: ['Authorization', 'Bearer ' + at.value + ' '] }), f.request(noScope.value)]) {
    assert.equal(f.service.userinfoBudget.capture(request), undefined);
    const guard = f.ingress({ limits: { attempts: 1 } }); f.enter(guard, request).release();
    throws(() => f.enter(guard, request), 'rate_limit');
  }
});

test('authenticated allocation retains global active 16 and physical peer 4, with idempotent release', t => {
  const f = fixture(t), at = f.token(), guard = f.ingress(), held = [];
  t.after(() => held.forEach(lease => lease.release()));
  for (let index = 0; index < 16; index++) held.push(f.enter(guard, f.request(at.value, { socket: { remoteAddress: 'peer-' + Math.floor(index / 4) } })));
  throws(() => f.enter(guard, f.request(at.value, { socket: { remoteAddress: 'fifth-peer' } })), 'temporarily_unavailable');
  held[0].release(); held[0].release();
  throws(() => f.enter(guard, f.request(at.value, { socket: { remoteAddress: 'peer-1' } })), 'temporarily_unavailable');
  f.enter(guard, f.request(at.value, { socket: { remoteAddress: 'peer-0' } })).release();
});

test('host authenticated ceiling spans clients/accounts and retains its window across AT replacement', t => {
  const f = fixture(t), guard = f.ingress({ userinfoLimits: { hostAttempts: 3 } });
  for (const at of [f.token(), f.token({ clientId: 'beta' }), f.token({ accountId: 'other-account' })]) f.enter(guard, f.request(at.value)).release();
  throws(() => f.enter(guard, f.request(f.token({ accountId: 'third-account' }).value)), 'rate_limit');
  f.enter(guard, f.request(random())).release(); // Host authenticated cap does not invent an anonymous grant or global logout.
});

test('slot saturation never evicts a live key; bounded expired compaction releases slots without retaining successful results', t => {
  let clock = 1000; t.mock.method(performance, 'now', () => clock);
  const f = fixture(t), guard = f.ingress({ userinfoLimits: { slots: 2, attempts: 2 } });
  const alpha = f.token(), beta = f.token({ clientId: 'beta' }), third = f.token({ accountId: 'other-account' });
  f.enter(guard, f.request(alpha.value)).release(); const held = f.enter(guard, f.request(beta.value));
  throws(() => f.enter(guard, f.request(third.value)), 'temporarily_unavailable');
  f.enter(guard, f.request(alpha.value)).release();
  clock += 60001;
  f.enter(guard, f.request(third.value)).release();
  held.release();
  // A rotated AT uses the same still-current client/account slot, not a new one.
  f.enter(guard, f.request(f.token({ grantId: third.grantId, accountId: 'other-account' }).value)).release();
});

test('lowering the legacy peer window cannot enlarge the authenticated fixed 600/60s allocation', t => {
  let clock = 1000; t.mock.method(performance, 'now', () => clock);
  const f = fixture(t), guard = f.ingress({ limits: { windowMs: 10, attempts: 1 }, userinfoLimits: { attempts: 1 } }), at = f.token();
  f.enter(guard, f.request(at.value)).release(); clock += 11;
  throws(() => f.enter(guard, f.request(at.value)), 'rate_limit');
  f.enter(guard, f.request(random())).release(); clock += 11; f.enter(guard, f.request(random())).release();
  clock += 60000; f.enter(guard, f.request(at.value)).release();
});

test('closed quota maxima cannot be enlarged, disabled, made fractional or given caller-derived options', () => {
  assert.deepEqual(HUMAN_USERINFO_HTTP_LIMITS, { attempts: 600, hostAttempts: 9600, slots: 2048 });
  for (const userinfoLimits of [{ attempts: 601 }, { hostAttempts: 9601 }, { slots: 2049 }, { attempts: 0 }, { slots: 1.5 }, { account: 'payload' }]) {
    throws(() => createOAuthIngress({ userinfoLimits }), 'oauth_configuration_invalid');
  }
});

test('actual signed Connect and maintained Provider allow >120 same-peer userinfo while isolating real clients/users and denying revoked bearer', async t => {
  const f = await environment(t);
  await f.login(f.rps[0], f.actor, f.wire, undefined, 86400);
  const alpha = f.rps[0].verificationFixture(), sub = f.actor.account.accountId;
  const send = token => f.wire.request(f.issuer + '/userinfo', { cookies: false, headers: { authorization: 'Bearer ' + token } });
  for (let index = 0; index < 125; index++) {
    const response = await send(alpha.accessToken); assert.equal(response.status, 200); assert.equal(response.body.sub === sub, true);
  }
  await f.login(f.rps[1], f.actor, f.wire, undefined, 86400);
  const beta = f.rps[1].verificationFixture(); assert.equal((await send(beta.accessToken)).status, 200);
  await f.login(f.rps[0], f.outsider, f.wire, undefined, 86400);
  const other = f.rps[0].verificationFixture(); assert.equal((await send(other.accessToken)).body.sub === f.outsider.account.accountId, true);
  const backup = await f.backupDevice(); await backup.client.revokeDevice(f.actor.account.deviceId);
  assert.equal([401, 403].includes((await send(alpha.accessToken)).status), true);
  assert.equal([401, 403].includes((await send(beta.accessToken)).status), true);
  assert.equal((await send(other.accessToken)).status, 200);
  for (let index = 0; index < 130; index++) {
    const invalid = await send(random());
    if (invalid.status === 429) { assert.equal((await send(other.accessToken)).status, 200); return; }
    assert.equal([401, 403].includes(invalid.status), true);
  }
  assert.fail('unknown tokens must exhaust only the original raw-peer120 window');
});

test('real Provider exact600 quota persists through confidential refresh and new grant without changing the public GET-only allowlist', async t => {
  const f = await environment(t);
  await f.login(f.rps[0], f.actor, f.wire, undefined, 86400);
  await f.login(f.rps[1], f.actor, f.wire, undefined, 86400);
  const alpha = f.rps[0].verificationFixture(), beta = f.rps[1].verificationFixture();
  const send = (token, fields) => f.wire.request(f.issuer + '/userinfo', { cookies: false,
    headers: { authorization: 'Bearer ' + token }, ...(fields ? { fields, originHeader: null } : {}) });
  // The actual RP callback has already spent exactly one Alpha userinfo entry.
  for (let index = 1; index < 600; index++) assert.equal((await send(alpha.accessToken)).status, 200);
  assert.equal((await send(alpha.accessToken)).status, 429);
  const rotated = await f.refresh(f.rps[0], alpha.refreshToken); assert.equal(rotated.status, 200);
  assert.equal(typeof rotated.body.access_token === 'string', true);
  assert.equal((await send(rotated.body.access_token)).status, 429);
  const flow = await f.begin(f.rps[0]); await f.approve(flow, f.actor, { stayInAppSeconds: 86400 });
  const finished = await f.complete(flow), newGrant = await f.exchangeCode(finished); assert.equal(newGrant.status, 200);
  assert.equal((await send(newGrant.body.access_token)).status, 429);
  assert.equal((await send(beta.accessToken, {})).status, 400);
  assert.equal((await send(beta.accessToken)).status, 200);
});
