import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, generateKeyPairSync, randomBytes, randomUUID } from 'node:crypto';
import { mkdtempSync, realpathSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { createServer } from 'node:http';
import { DatabaseSync } from 'node:sqlite';
import express from 'express';
import { Provider, errors } from 'oidc-provider';
import { createOAuthIngress } from '../capabilities-oauth-ingress.js';

// C1a protocol seam only: real pinned Provider, HTTP and SQLite. The fixture
// seeds codes in three cases and uses synthetic consent in the fourth. It is NOT Connect consent,
// production adapter, encrypted artifact store or external CLI acceptance.
const SCOPE = 'notes.createDraft';
const digest = value => createHash('sha256').update(value).digest('hex');
const epoch = () => Math.floor(Date.now() / 1000);
const verifier = () => randomBytes(32).toString('base64url');
const challenge = value => createHash('sha256').update(value).digest('base64url');
const CLIENTS = ['fixture-codex', 'fixture-opencode'];
const REDIRECT = 'http://127.0.0.1:19876/mcp/oauth/callback';

async function fixture(t, { bounded = false } = {}) {
  const parent = realpathSync(tmpdir()), root = realpathSync(mkdtempSync(join(parent, 'soty-oauth-provider-')));
  const marker = randomUUID(); writeFileSync(join(root, 'test-owner'), marker, { flag: 'wx' });
  const database = join(root, 'provider.sqlite');
  const dbs = [], servers = [], observations = [];
  const first = new DatabaseSync(database); dbs.push(first);
  first.exec(`PRAGMA journal_mode=WAL; CREATE TABLE artifacts(
    model TEXT NOT NULL,id TEXT NOT NULL,payload TEXT NOT NULL,grant_id TEXT,expires INTEGER NOT NULL,
    consumed INTEGER,PRIMARY KEY(model,id));
    CREATE TABLE families(id TEXT PRIMARY KEY,revoked INTEGER NOT NULL DEFAULT 0);
    CREATE INDEX artifacts_grant ON artifacts(grant_id);`);
  t.after(async () => {
    await Promise.all(servers.map(server => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); })));
    for (const db of dbs) db.close();
    assert.equal(dirname(root), parent); assert.equal(readFileSync(join(root, 'test-owner'), 'utf8'), marker);
    rmSync(root, { recursive: true });
  });
  let waitForRead;
  const key = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
  key.kid = 'fixture-key'; key.alg = 'RS256'; key.use = 'sig';
  const resources = ['https://fixture.invalid', 'https://fixture.invalid/mcp'];
  const familyActive = id => first.prepare('SELECT revoked FROM families WHERE id=?').get(id)?.revoked === 0;

  async function endpoint(db, sharedIssuer) {
    let app;
    const server = createServer((req, res) => app(req, res)); servers.push(server);
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    const origin = `http://127.0.0.1:${server.address().port}`, issuer = sharedIssuer ?? `${origin}/oauth`;
    class Adapter {
      constructor(model) { this.model = model; }
      async upsert(id, payload, ttl) {
        const current = Provider.ctx;
        observations.push({ kind: 'upsert', model: this.model, keys: Object.keys(payload).sort(),
          hasContext: Boolean(current), ttlFinite: Number.isFinite(ttl), expFinite: Number.isSafeInteger(payload.exp),
          client: current?.oidc?.client?.clientId ?? null, route: current?.oidc?.route ?? null });
        assert.ok(Number.isSafeInteger(payload.exp) && payload.exp > epoch(), 'checked finite payload expiry');
        db.exec('BEGIN IMMEDIATE');
        try {
          if (this.model === 'Grant') {
            db.prepare('INSERT INTO families(id) VALUES(?) ON CONFLICT DO NOTHING').run(id);
          }
          const family = this.model === 'Grant' ? id
            : ['AuthorizationCode', 'RefreshToken', 'AccessToken'].includes(this.model) ? payload.grantId : undefined;
          if (family && db.prepare('SELECT revoked FROM families WHERE id=?').get(family)?.revoked !== 0) {
            throw new errors.InvalidGrant('connection unavailable');
          }
          db.prepare(`INSERT INTO artifacts(model,id,payload,grant_id,expires) VALUES(?,?,?,?,?)
            ON CONFLICT(model,id) DO UPDATE SET payload=excluded.payload,expires=excluded.expires`).run(
            this.model, digest(id), JSON.stringify(payload), family ?? null, payload.exp);
          db.exec('COMMIT');
        } catch (error) { db.exec('ROLLBACK'); throw error; }
      }
      async find(id) {
        const row = db.prepare('SELECT * FROM artifacts WHERE model=? AND id=?').get(this.model, digest(id));
        if (!row || row.expires <= epoch()) return undefined;
        if (row.grant_id && db.prepare('SELECT revoked FROM families WHERE id=?').get(row.grant_id)?.revoked !== 0) return undefined;
        const payload = JSON.parse(row.payload);
        if (row.consumed !== null) payload.consumed = row.consumed;
        if (waitForRead) await waitForRead(this.model, digest(id));
        return payload;
      }
      async findByUid(uid) {
        const row = db.prepare("SELECT payload,consumed FROM artifacts WHERE model=? AND json_extract(payload,'$.uid')=? AND expires>?")
          .get(this.model, uid, epoch());
        if (!row) return undefined;
        return { ...JSON.parse(row.payload), ...(row.consumed === null ? {} : { consumed: row.consumed }) };
      }
      async findByUserCode() { return undefined; }
      async consume(id) {
        const ctx = Provider.ctx;
        observations.push({ kind: 'consume', model: this.model, hasContext: Boolean(ctx),
          route: ctx?.oidc?.route, client: ctx?.oidc?.client?.clientId });
        assert.ok(ctx?.oidc?.params && CLIENTS.includes(ctx.oidc.client?.clientId), 'public adapter has verified request context');
        let reused = false;
        db.exec('BEGIN IMMEDIATE');
        try {
          const row = db.prepare('SELECT * FROM artifacts WHERE model=? AND id=?').get(this.model, digest(id));
          if (!row) throw new errors.InvalidGrant('source unavailable');
          const payload = JSON.parse(row.payload), resource = ctx.oidc.params.resource;
          if (payload.clientId !== ctx.oidc.client.clientId) throw new errors.InvalidGrant('client mismatch');
          if (resource !== undefined && (typeof resource !== 'string' || resource !== payload.resource)) {
            throw new errors.InvalidTarget('resource mismatch');
          }
          const result = db.prepare('UPDATE artifacts SET consumed=? WHERE model=? AND id=? AND consumed IS NULL')
            .run(epoch(), this.model, digest(id));
          reused = result.changes !== 1;
          if (reused) db.prepare('UPDATE families SET revoked=1 WHERE id=?').run(row.grant_id);
          db.exec('COMMIT');
        } catch (error) { db.exec('ROLLBACK'); throw error; }
        if (reused) throw new errors.InvalidGrant('source already used');
      }
      async destroy(id) {
        observations.push({ kind: 'destroy', model: this.model });
        db.exec('BEGIN IMMEDIATE');
        try {
          const row = db.prepare('SELECT grant_id FROM artifacts WHERE model=? AND id=?').get(this.model, digest(id));
          if (row?.grant_id) db.prepare('UPDATE families SET revoked=1 WHERE id=?').run(row.grant_id);
          db.prepare('DELETE FROM artifacts WHERE model=? AND id=?').run(this.model, digest(id));
          db.exec('COMMIT');
        } catch (error) { db.exec('ROLLBACK'); throw error; }
      }
      async revokeByGrantId(id) {
        observations.push({ kind: 'revokeByGrantId', model: this.model });
        db.prepare('UPDATE families SET revoked=1 WHERE id=?').run(id);
      }
    }
    const provider = new Provider(issuer, {
      adapter: Adapter, clients: CLIENTS.map(client_id => ({ client_id, redirect_uris: [REDIRECT],
        application_type: 'native', token_endpoint_auth_method: 'none',
        grant_types: ['authorization_code', 'refresh_token'], response_types: ['code'] })),
      jwks: { keys: [key] }, cookies: { keys: [randomBytes(32).toString('base64url')] },
      responseTypes: ['code'], scopes: [], claims: {}, pkce: { required: () => true },
      findAccount: async (_ctx, id) => id === 'fixture-owner' ? { accountId: id, claims: async () => ({ sub: id }) } : undefined,
      features: { devInteractions: { enabled: false }, registration: { enabled: false },
        clientIdMetadataDocument: { enabled: false }, userinfo: { enabled: false }, introspection: { enabled: false },
        revocation: { enabled: true, allowedPolicy: (_ctx, client, token) => client.clientId === token.clientId }, resourceIndicators: { enabled: true,
          getResourceServerInfo(_ctx, resource) {
            if (!resources.includes(resource)) throw new errors.InvalidTarget();
            return { scope: SCOPE, accessTokenTTL: 300, accessTokenFormat: 'opaque' };
          } } },
      ttl: { AccessToken: 300, AuthorizationCode: 60, Grant: 3600, RefreshToken: 3600, Interaction: 600, Session: 600 },
      issueRefreshToken: () => true, rotateRefreshToken: true, revokeGrantPolicy: () => true,
      expiresWithSession: () => false,
      routes: { authorization: '/authorize', token: '/token', revocation: '/revoke' },
      interactions: { url: (_ctx, interaction) => `${issuer}/interaction/${interaction.uid}` },
    });
    provider.on('server_error', () => { observations.push({ kind: 'server_error' }); });
    app = express();
    if (bounded) {
      const ingress = createOAuthIngress();
      provider.use(async (ctx, next) => {
        let lease;
        try {
          lease = ingress.enter(ctx.req, ctx.res);
          if (ctx.method === 'POST') ctx.req.body = await lease.readForm();
          await next();
        } catch {
          ctx.status = 400; ctx.body = { error: 'invalid_request' };
        } finally { lease?.release(); }
      });
    }
    // Synthetic consent for exercising the library's Session/Interaction/code
    // serialization. This route exists only in this disposable test server.
    app.get('/oauth/interaction/:uid', async (req, res, next) => {
      try {
        const details = await provider.interactionDetails(req, res);
        assert.equal(details.uid, req.params.uid);
        const grant = new provider.Grant({ accountId: 'fixture-owner', clientId: details.params.client_id });
        grant.addResourceScope(details.params.resource, SCOPE);
        const grantId = await grant.save();
        await provider.interactionFinished(req, res, { login: { accountId: 'fixture-owner', remember: false }, consent: { grantId } });
      } catch (error) { next(error); }
    });
    app.use('/oauth', provider.callback());
    app.use((_error, _req, res, _next) => res.status(500).json({ error: 'fixture_interaction_failed' }));
    const send = async (path, fields) => {
      const response = await fetch(`${origin}/oauth${path}`, { method: 'POST', headers: {
        Host: new URL(issuer).host, 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams(fields) });
      const raw = await response.text();
      return { status: response.status, body: raw ? JSON.parse(raw) : null };
    };
    const seed = async ({ clientId = CLIENTS[0], resource = resources[1] } = {}) => {
      const client = await provider.Client.find(clientId);
      const grant = new provider.Grant({ accountId: 'fixture-owner', clientId }); grant.addResourceScope(resource, SCOPE);
      const grantId = await grant.save(), secret = verifier();
      const code = new provider.AuthorizationCode({ accountId: 'fixture-owner', client, grantId,
        resource, scope: SCOPE, redirectUri: REDIRECT, codeChallenge: challenge(secret), codeChallengeMethod: 'S256' });
      const value = await code.save();
      return { grantId, value, secret, clientId, resource };
    };
    const exchange = (seed, extra = {}) => send('/token', { grant_type: 'authorization_code', client_id: seed.clientId,
      code: seed.value, code_verifier: seed.secret, redirect_uri: REDIRECT, resource: seed.resource, ...extra });
    return { origin, issuer, provider, seed, exchange, send };
  }
  const primary = await endpoint(first);
  return { ...primary, observations, db: first, familyActive,
    consumed(model, value) { return first.prepare('SELECT consumed FROM artifacts WHERE model=? AND id=?').get(model, digest(value))?.consumed; },
    async second() { const db = new DatabaseSync(database); dbs.push(db); return endpoint(db, primary.issuer); },
    barrier(value) {
      const target = digest(value); let arrived = 0, release;
      const ready = new Promise(resolve => { release = resolve; });
      waitForRead = async (model, id) => { if (model !== 'RefreshToken' || id !== target) return;
        arrived++; if (arrived === 2) { waitForRead = null; release(); } await ready; };
    },
  };
}

test('pinned Provider exposes parsed resource before consume and keeps wrong-resource source usable', async t => {
  const f = await fixture(t), seeded = await f.seed();
  const discovery = await (await fetch(`${f.issuer}/.well-known/openid-configuration`)).json();
  assert.equal(discovery.issuer, f.issuer); assert.deepEqual(discovery.code_challenge_methods_supported, ['S256']);
  assert.equal(discovery.authorization_response_iss_parameter_supported, true);
  const wrongPkce = await f.exchange(seeded, { code_verifier: verifier() });
  assert.equal(wrongPkce.status, 400); assert.equal(wrongPkce.body.error, 'invalid_grant');
  assert.equal(f.consumed('AuthorizationCode', seeded.value), null);
  const wrong = await f.exchange(seeded, { resource: 'https://fixture.invalid' });
  assert.equal(wrong.status, 400); assert.equal(wrong.body.error, 'invalid_target');
  assert.equal(f.consumed('AuthorizationCode', seeded.value), null);
  const issued = await f.exchange(seeded);
  assert.equal(issued.status, 200); assert.ok(typeof issued.body.refresh_token === 'string', 'issued refresh');
  const refresh = resource => f.send('/token', { grant_type: 'refresh_token', client_id: seeded.clientId,
    refresh_token: issued.body.refresh_token, resource });
  const wrongRefresh = await refresh('https://fixture.invalid');
  assert.equal(wrongRefresh.status, 400); assert.equal(wrongRefresh.body.error, 'invalid_target');
  assert.equal(f.consumed('RefreshToken', issued.body.refresh_token), null);
  const rotated = await refresh(seeded.resource);
  assert.equal(rotated.status, 200); assert.ok(rotated.body.refresh_token !== issued.body.refresh_token, 'rotated refresh');
  assert.ok(f.observations.filter(row => row.kind === 'consume').every(row => row.hasContext && row.route === 'token'));
  assert.ok(f.observations.filter(row => row.kind === 'upsert').every(row => row.expFinite));
  assert.ok(!f.observations.some(row => row.kind === 'server_error'));
});

test('pinned Provider token revocation is family-wide and preserves a sibling connection', async t => {
  const f = await fixture(t), first = await f.seed(), sibling = await f.seed();
  const a = await f.exchange(first), b = await f.exchange(sibling);
  assert.equal(a.status, 200); assert.equal(b.status, 200);
  const revoked = await f.send('/revoke', { client_id: first.clientId, token: a.body.access_token, token_type_hint: 'access_token' });
  assert.equal(revoked.status, 200); assert.equal(f.familyActive(first.grantId), false); assert.equal(f.familyActive(sibling.grantId), true);
  assert.ok(f.observations.some(row => row.kind === 'revokeByGrantId' && row.model === 'RefreshToken'));
  assert.ok(f.observations.some(row => row.kind === 'destroy' && row.model === 'Grant'));
  const expiredFamily = await f.send('/token', { grant_type: 'refresh_token', client_id: first.clientId, refresh_token: a.body.refresh_token });
  assert.equal(expiredFamily.status, 400); assert.equal(expiredFamily.body.error, 'invalid_grant');
  const healthy = await f.send('/token', { grant_type: 'refresh_token', client_id: sibling.clientId, refresh_token: b.body.refresh_token });
  assert.equal(healthy.status, 200);
});

test('two Provider/SQLite writers consume once; losing refresh revokes family before late token upsert', async t => {
  const f = await fixture(t), second = await f.second(), seeded = await f.seed();
  const issued = await f.exchange(seeded); assert.equal(issued.status, 200);
  f.barrier(issued.body.refresh_token);
  const fields = { grant_type: 'refresh_token', client_id: seeded.clientId, refresh_token: issued.body.refresh_token, resource: seeded.resource };
  const responses = await Promise.all([f.send('/token', fields), second.send('/token', fields)]);
  assert.ok(responses.every(item => item.status === 200 || item.status === 400));
  assert.ok(responses.filter(item => item.status === 200).length <= 1);
  assert.ok(responses.some(item => item.status === 400 && item.body.error === 'invalid_grant'));
  assert.equal(f.familyActive(seeded.grantId), false);
  const client = await f.provider.Client.find(seeded.clientId);
  const late = new f.provider.AccessToken({ accountId: 'fixture-owner', client, grantId: seeded.grantId });
  await assert.rejects(late.save(), error => error instanceof errors.InvalidGrant);
  assert.ok(!f.observations.some(row => row.kind === 'server_error'));
});

test('actual authorize and synthetic owner consent expose only the six admitted storage models', async t => {
  const f = await fixture(t), secret = verifier(), state = randomUUID(), resource = 'https://fixture.invalid/mcp';
  const jar = new Map();
  const browserGet = async url => {
    const response = await fetch(url, { redirect: 'manual', headers: { Cookie: [...jar].map(([name, value]) => `${name}=${value}`).join('; ') } });
    for (const cookie of response.headers.getSetCookie()) {
      const pair = cookie.split(';', 1)[0], split = pair.indexOf('='); jar.set(pair.slice(0, split), pair.slice(split + 1));
    }
    assert.ok([302, 303].includes(response.status), 'authorization flow redirects');
    const location = response.headers.get('location'); assert.ok(typeof location === 'string', 'redirect is present');
    await response.body?.cancel(); return new URL(location, url);
  };
  const params = new URLSearchParams({ client_id: CLIENTS[1], response_type: 'code', redirect_uri: REDIRECT,
    scope: SCOPE, resource, code_challenge: challenge(secret), code_challenge_method: 'S256', state });
  let url = await browserGet(`${f.issuer}/authorize?${params}`);
  for (let step = 0; step < 5 && url.origin === f.origin; step++) url = await browserGet(url);
  assert.equal(url.origin + url.pathname, REDIRECT, 'registered callback');
  assert.equal(url.searchParams.get('state'), state); assert.equal(url.searchParams.get('iss'), f.issuer);
  assert.ok(url.searchParams.has('code') && !url.searchParams.has('error'), 'real code returned');
  const issued = await f.send('/token', { grant_type: 'authorization_code', client_id: CLIENTS[1],
    redirect_uri: REDIRECT, code: url.searchParams.get('code'), code_verifier: secret, resource });
  assert.equal(issued.status, 200);
  const writes = f.observations.filter(row => row.kind === 'upsert');
  assert.deepEqual([...new Set(writes.map(row => row.model))].sort(),
    ['AccessToken', 'AuthorizationCode', 'Grant', 'Interaction', 'RefreshToken', 'Session']);
  assert.ok(writes.every(row => row.expFinite));
  assert.ok(!f.observations.some(row => row.kind === 'server_error'));
  t.diagnostic(JSON.stringify(Object.fromEntries(writes.map(row => [row.model, { fields: row.keys, ttlFinite: row.ttlFinite }]))));
});

test('bounded upstream forms work with the actual Provider and reject duplicate resources before code consumption', async t => {
  const f = await fixture(t, { bounded: true }), seeded = await f.seed();
  const fields = new URLSearchParams({ grant_type: 'authorization_code', client_id: seeded.clientId,
    code: seeded.value, code_verifier: seeded.secret, redirect_uri: REDIRECT, resource: seeded.resource });
  fields.append('resource', seeded.resource);
  const duplicate = await fetch(`${f.issuer}/token`, { method: 'POST', body: fields });
  assert.equal(duplicate.status, 400); assert.equal((await duplicate.json()).error, 'invalid_request');
  assert.equal(f.consumed('AuthorizationCode', seeded.value), null);
  const issued = await f.exchange(seeded); assert.equal(issued.status, 200);
  const refresh = await f.send('/token', { grant_type: 'refresh_token', client_id: seeded.clientId,
    refresh_token: issued.body.refresh_token, resource: seeded.resource });
  assert.equal(refresh.status, 200);
  const revoked = await f.send('/revoke', { client_id: seeded.clientId, token: refresh.body.access_token });
  assert.equal(revoked.status, 200); assert.equal(f.familyActive(seeded.grantId), false);
  assert.ok(!f.observations.some(row => row.kind === 'server_error'));
});

test('fresh consent in one browser keeps sibling grants independent of the short AS session', async t => {
  const f = await fixture(t), jar = new Map(), clientId = CLIENTS[1], resource = 'https://fixture.invalid/mcp';
  const authorize = async () => {
    const secret = verifier(), state = randomUUID(); let interactions = 0;
    const params = new URLSearchParams({ client_id: clientId, response_type: 'code', redirect_uri: REDIRECT,
      scope: SCOPE, resource, code_challenge: challenge(secret), code_challenge_method: 'S256', state });
    let url = new URL(`${f.issuer}/authorize?${params}`);
    for (let step = 0; step < 6 && url.origin === f.origin; step++) {
      if (url.pathname.startsWith('/oauth/interaction/')) interactions++;
      const response = await fetch(url, { redirect: 'manual', headers: {
        Cookie: [...jar].map(([name, value]) => `${name}=${value}`).join('; ') } });
      for (const cookie of response.headers.getSetCookie()) {
        const pair = cookie.split(';', 1)[0], split = pair.indexOf('='); jar.set(pair.slice(0, split), pair.slice(split + 1));
      }
      assert.ok([302, 303].includes(response.status), 'real authorization redirect');
      const target = response.headers.get('location'); assert.ok(target); await response.body?.cancel(); url = new URL(target, url);
    }
    assert.equal(interactions, 1, 'fresh consent on each flow without client prompt parameter');
    assert.equal(url.origin + url.pathname, REDIRECT); assert.equal(url.searchParams.get('state'), state);
    assert.equal(url.searchParams.get('iss'), f.issuer); assert.ok(url.searchParams.has('code'));
    const issued = await f.send('/token', { grant_type: 'authorization_code', client_id: clientId,
      redirect_uri: REDIRECT, code: url.searchParams.get('code'), code_verifier: secret, resource });
    assert.equal(issued.status, 200); return issued.body;
  };
  const first = await authorize(), sibling = await authorize();
  const refresh = token => f.send('/token', { grant_type: 'refresh_token', client_id: clientId, refresh_token: token, resource });
  const a = await refresh(first.refresh_token), b = await refresh(sibling.refresh_token);
  t.diagnostic(JSON.stringify({ sessionProfile: true, firstError: a.body.error, siblingError: b.body.error,
    familyStates: f.db.prepare('SELECT revoked FROM families').all().map(row => row.revoked),
    destroyedModels: f.observations.filter(row => row.kind === 'destroy').map(row => row.model) }));
  assert.equal(a.status, 200, 'a later consent must not replace an earlier family'); assert.equal(b.status, 200);
  const sessions = f.db.prepare("SELECT payload FROM artifacts WHERE model='Session'").all();
  assert.ok(sessions.length > 0, 'real browser session persisted');
  for (const row of sessions) {
    const session = await f.provider.Session.find(JSON.parse(row.payload).jti); if (session) await session.destroy();
  }
  assert.equal(f.db.prepare("SELECT count(*) AS n FROM artifacts WHERE model='Session'").get().n, 0);
  const afterLogout = await refresh(a.body.refresh_token), other = await refresh(b.body.refresh_token);
  assert.equal(afterLogout.status, 200, 'connection survives deletion of the short AS session'); assert.equal(other.status, 200);
  await f.send('/revoke', { client_id: clientId, token: afterLogout.body.access_token });
  assert.equal((await refresh(afterLogout.body.refresh_token)).status, 400);
  assert.equal((await refresh(other.body.refresh_token)).status, 200, 'sibling survives family revoke');
});
