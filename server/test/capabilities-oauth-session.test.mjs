import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, generateKeyPairSync, randomBytes } from 'node:crypto';
import { createServer } from 'node:http';
import { DatabaseSync } from 'node:sqlite';
import { readFileSync } from 'node:fs';
import express from 'express';
import { Provider, errors } from 'oidc-provider';

// Actual pinned Provider and HTTP flow, adapted from the existing synthetic
// provider seam fixture. This is not a production adapter or Connect consent.
// Only Date is controlled; no direct Session.save/resetIdentifier/persist calls.
const CLIENT = 'fixture-session-native', REDIRECT = 'http://127.0.0.1:19876/mcp/oauth/callback';
const RESOURCE = 'https://session-fixture.invalid/mcp', SCOPE = 'notes.createDraft';
const EPOCH_MS = 1_800_000_000_000;
const epoch = () => Math.floor(Date.now() / 1000);
const sha = value => createHash('sha256').update(value).digest('hex');
const pkce = value => createHash('sha256').update(value).digest('base64url');
const key = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
Object.assign(key, { kid: 'session-fixture-key', alg: 'RS256', use: 'sig' });

async function fixture(t, { bounded = true } = {}) {
  assert.equal(JSON.parse(readFileSync(new URL('../../node_modules/oidc-provider/package.json', import.meta.url))).version, '9.12.2');
  t.mock.timers.enable({ apis: ['Date'], now: EPOCH_MS });
  const advance = seconds => t.mock.timers.tick(seconds * 1000);
  const db = new DatabaseSync(':memory:');
  db.exec(`CREATE TABLE artifacts(model TEXT NOT NULL,id_hash TEXT NOT NULL,payload TEXT NOT NULL,
    expires INTEGER NOT NULL,created_at INTEGER NOT NULL,consumed INTEGER,PRIMARY KEY(model,id_hash));`);
  const events = [], jar = new Map();
  let app, acceptedClockStep = 0;
  const server = createServer((req, res) => app(req, res));
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => {
    await new Promise(resolve => { server.closeAllConnections(); server.close(resolve); });
    db.close();
  });
  const origin = `http://127.0.0.1:${server.address().port}`, issuer = origin + '/oauth';

  class Adapter {
    constructor(model) { this.model = model; }
    async upsert(id, payload, ttl) {
      assert.ok(Number.isSafeInteger(payload.iat) && Number.isSafeInteger(payload.exp), 'finite artifact times');
      if (bounded && this.model === 'Session') {
        assert.ok(payload.exp <= payload.iat + 600, 'Session keeps the original iat window, including a new jti');
      }
      events.push({ kind: 'upsert', model: this.model, idHash: sha(id), uidHash: payload.uid && sha(payload.uid),
        iat: payload.iat, exp: payload.exp, ttl, fields: Object.keys(payload).sort() });
      db.prepare(`INSERT INTO artifacts(model,id_hash,payload,expires,created_at) VALUES(?,?,?,?,?)
        ON CONFLICT(model,id_hash) DO UPDATE SET payload=excluded.payload,expires=excluded.expires`)
        .run(this.model, sha(id), JSON.stringify(payload), payload.exp, payload.iat);
    }
    async find(id) {
      const row = db.prepare('SELECT * FROM artifacts WHERE model=? AND id_hash=?').get(this.model, sha(id));
      if (!row || row.expires <= epoch()) return undefined;
      return { ...JSON.parse(row.payload), ...(row.consumed === null ? {} : { consumed: row.consumed }) };
    }
    async findByUid(uid) {
      const row = db.prepare("SELECT * FROM artifacts WHERE model=? AND json_extract(payload,'$.uid')=? AND expires>?")
        .get(this.model, uid, epoch());
      return row && JSON.parse(row.payload);
    }
    async findByUserCode() { return undefined; }
    async destroy(id) {
      events.push({ kind: 'destroy', model: this.model, idHash: sha(id) });
      db.prepare('DELETE FROM artifacts WHERE model=? AND id_hash=?').run(this.model, sha(id));
    }
    async consume(id) {
      const changed = db.prepare('UPDATE artifacts SET consumed=? WHERE model=? AND id_hash=? AND consumed IS NULL')
        .run(epoch(), this.model, sha(id));
      if (changed.changes !== 1) throw new errors.InvalidGrant('source unavailable');
    }
    async revokeByGrantId(id) {
      db.prepare("DELETE FROM artifacts WHERE json_extract(payload,'$.grantId')=?").run(id);
    }
  }

  const sessionTTL = (_ctx, session) => {
    const remaining = (session.iat ?? epoch()) + 600 - epoch();
    if (remaining <= 0) throw new errors.SessionNotFound('short session expired');
    return remaining;
  };
  const provider = new Provider(issuer, {
    adapter: Adapter, clients: [{ client_id: CLIENT, redirect_uris: [REDIRECT], application_type: 'native',
      token_endpoint_auth_method: 'none', grant_types: ['authorization_code', 'refresh_token'], response_types: ['code'] }],
    jwks: { keys: [key] }, cookies: { keys: [randomBytes(32).toString('base64url')] },
    responseTypes: ['code'], scopes: [], claims: {}, pkce: { required: () => true },
    findAccount: async (_ctx, id) => id === 'fixture-owner' ? { accountId: id, claims: async () => ({ sub: id }) } : undefined,
    features: { devInteractions: { enabled: false }, registration: { enabled: false },
      clientIdMetadataDocument: { enabled: false }, userinfo: { enabled: false }, introspection: { enabled: false },
      resourceIndicators: { enabled: true, getResourceServerInfo(_ctx, resource) {
        if (resource !== RESOURCE) throw new errors.InvalidTarget();
        return { scope: SCOPE, accessTokenTTL: 300, accessTokenFormat: 'opaque' };
      } } },
    ttl: { AccessToken: 300, AuthorizationCode: 60, Grant: 3600, RefreshToken: 3600, Interaction: 600,
      Session: bounded ? sessionTTL : 600 },
    issueRefreshToken: () => true, rotateRefreshToken: true, expiresWithSession: () => false,
    routes: { authorization: '/authorize', token: '/token' },
    interactions: { url: (_ctx, interaction) => `${issuer}/interaction/${interaction.uid}` },
    renderError(ctx, _out, error) { ctx.status = error.statusCode ?? 400; ctx.body = { error: error.error }; },
  });
  provider.on('server_error', () => events.push({ kind: 'server_error' }));
  provider.on('authorization.accepted', () => {
    if (acceptedClockStep) { const step = acceptedClockStep; acceptedClockStep = 0; advance(step); }
  });
  app = express();
  app.get('/oauth/interaction/:uid', async (req, res, next) => {
    try {
      const interaction = await provider.interactionDetails(req, res);
      assert.ok(interaction.uid === req.params.uid, 'interaction route binding');
      events.push({ kind: 'consent', uidHash: sha(interaction.uid), prompt: interaction.prompt.name,
        reasons: [...interaction.prompt.reasons], fields: Object.keys(interaction).sort() });
      const grant = new provider.Grant({ accountId: 'fixture-owner', clientId: CLIENT });
      grant.addResourceScope(RESOURCE, SCOPE);
      const grantId = await grant.save();
      await provider.interactionFinished(req, res, { login: { accountId: 'fixture-owner', remember: false }, consent: { grantId } });
    } catch (error) { next(error); }
  });
  app.use('/oauth', provider.callback());
  app.use((_error, _req, res, _next) => res.status(500).json({ error: 'fixture_failed' }));

  function storeCookies(response, url) {
    for (const raw of response.headers.getSetCookie()) {
      const [pair, ...attributes] = raw.split(';'), split = pair.indexOf('=');
      const attrs = Object.fromEntries(attributes.map(part => {
        const index = part.indexOf('='); return index === -1 ? [part.trim().toLowerCase(), true]
          : [part.slice(0, index).trim().toLowerCase(), part.slice(index + 1).trim()];
      }));
      const cookie = { name: pair.slice(0, split), value: pair.slice(split + 1), path: attrs.path ?? url.pathname.replace(/[^/]*$/u, ''),
        expires: attrs['max-age'] !== undefined ? Date.now() + Number(attrs['max-age']) * 1000
          : attrs.expires ? Date.parse(attrs.expires) : Infinity };
      const id = cookie.name + '\0' + cookie.path;
      if (!cookie.value || cookie.expires <= Date.now()) jar.delete(id); else jar.set(id, cookie);
    }
  }
  async function get(input) {
    const url = new URL(input); assert.equal(url.origin, origin, 'only task-local HTTP');
    const cookie = [...jar.values()].filter(value => value.expires > Date.now()
      && (url.pathname === value.path || url.pathname.startsWith(value.path.endsWith('/') ? value.path : value.path + '/')))
      .sort((a, b) => b.path.length - a.path.length).map(value => value.name + '=' + value.value).join('; ');
    const response = await fetch(url, { redirect: 'manual', headers: { Cookie: cookie } });
    storeCookies(response, url);
    const location = response.headers.get('location'), body = await response.text();
    return { status: response.status, url: location ? new URL(location, url) : null,
      error: response.headers.get('content-type')?.includes('application/json') ? JSON.parse(body)?.error : undefined };
  }
  function request({ prompt } = {}) {
    const secret = randomBytes(32).toString('base64url'), state = randomBytes(24).toString('base64url');
    const params = new URLSearchParams({ client_id: CLIENT, response_type: 'code', redirect_uri: REDIRECT,
      resource: RESOURCE, scope: SCOPE, code_challenge: pkce(secret), code_challenge_method: 'S256', state });
    if (prompt !== undefined) params.set('prompt', prompt);
    return { secret, state, url: new URL(`${issuer}/authorize?${params}`) };
  }
  async function finish(flow, first) {
    let response = first ?? await get(flow.url);
    for (let i = 0; i < 5 && response.url?.origin === origin; i++) response = await get(response.url);
    return response;
  }
  async function send(fields) {
    const response = await fetch(issuer + '/token', { method: 'POST', body: new URLSearchParams(fields) });
    return { status: response.status, body: await response.json() };
  }
  async function authorize() {
    const flow = request(), result = await finish(flow);
    assert.equal(result.url?.origin + result.url?.pathname, REDIRECT, 'registered callback reached without requesting it');
    assert.ok(result.url.searchParams.get('state') === flow.state && result.url.searchParams.get('iss') === issuer, 'response binding');
    assert.ok(result.url.searchParams.has('code') && !result.url.searchParams.has('error'), 'real code returned');
    const issued = await send({ grant_type: 'authorization_code', client_id: CLIENT, redirect_uri: REDIRECT,
      resource: RESOURCE, code: result.url.searchParams.get('code'), code_verifier: flow.secret });
    assert.equal(issued.status, 200); return issued.body;
  }
  const sessions = () => db.prepare("SELECT id_hash,created_at,payload FROM artifacts WHERE model='Session'").all()
    .map(row => ({ idHash: row.id_hash, createdAt: row.created_at, payload: JSON.parse(row.payload) }));
  return { db, events, get, request, finish, authorize, sessions, advance,
    crossExpiryDuringAccepted(seconds) { acceptedClockStep = seconds; },
    refresh(refreshToken) { return send({ grant_type: 'refresh_token', client_id: CLIENT, refresh_token: refreshToken, resource: RESOURCE }); } };
}

test('actual repeated authorize proves fixed Session600 slides expiry and reset retains original uid/iat', async t => {
  const f = await fixture(t, { bounded: false });
  await f.authorize();
  const first = f.sessions()[0]; assert.equal(first.payload.exp - first.payload.iat, 600);
  f.advance(10);
  const flow = f.request(), begun = await f.get(flow.url);
  assert.ok(begun.url?.pathname.startsWith('/oauth/interaction/'), 'new interaction required');
  const resaved = f.sessions()[0];
  assert.ok(resaved.idHash === first.idHash, 'ordinary authorize resaves the existing identifier');
  assert.equal(resaved.payload.iat, first.payload.iat);
  assert.equal(resaved.payload.exp, first.payload.exp + 10, 'counterexample: fixed TTL extends original window');
  const start = f.events.length, completed = await f.finish(flow, begun);
  assert.ok(completed.url?.searchParams.has('code'), 'HTTP resume actually completed');
  const reset = f.sessions()[0];
  assert.ok(reset.idHash !== first.idHash, 'HTTP resume resetIdentifier occurred');
  assert.ok(reset.payload.uid === first.payload.uid, 'uid stays stable');
  assert.equal(reset.payload.iat, first.payload.iat);
  assert.equal(reset.payload.exp - first.payload.iat, 610);
  const tail = f.events.slice(start), destroyed = tail.findIndex(event => event.kind === 'destroy' && event.idHash === first.idHash);
  const saved = tail.findIndex(event => event.kind === 'upsert' && event.idHash === reset.idHash);
  assert.ok(destroyed >= 0 && saved > destroyed, 'old Session destroyed before replacement upsert');
  assert.equal(reset.payload.authorizations[CLIENT].persistsLogout, true);
  t.diagnostic('Fixed600 control: resave/reset at +10s produced original-window length610s; this is the rejected configuration.');
});

test('remaining original TTL preserves600 across real resets and fresh consent; refresh survives expired Session', async t => {
  const f = await fixture(t), firstTokens = await f.authorize(), first = f.sessions()[0];
  f.advance(100); const secondTokens = await f.authorize(), second = f.sessions()[0];
  assert.ok(first.idHash !== second.idHash && first.payload.uid === second.payload.uid, 'real same-session reset');
  assert.equal(second.createdAt, first.createdAt); assert.equal(second.payload.iat, first.payload.iat);
  assert.equal(second.payload.exp, first.payload.exp);
  const consents = f.events.filter(event => event.kind === 'consent');
  assert.equal(consents.length, 2); assert.ok(consents[0].uidHash !== consents[1].uidHash, 'each authorize gets a fresh interaction');
  assert.ok(consents[1].reasons.includes('native_client_prompt'), 'native default policy supplies fresh consent');
  const noPrompt = f.request({ prompt: 'none' }), denied = await f.finish(noPrompt);
  assert.equal(denied.url?.searchParams.get('error'), 'interaction_required');
  assert.equal(denied.url?.searchParams.has('code'), false);
  assert.equal(f.events.filter(event => event.kind === 'consent').length, 2);
  f.advance(501);
  assert.equal((await f.refresh(firstTokens.refresh_token)).status, 200, 'old family independent of expired Session');
  assert.equal((await f.refresh(secondTokens.refresh_token)).status, 200, 'sibling family remains independent');
  await f.authorize();
  const third = f.sessions().find(row => row.payload.exp > epoch());
  assert.ok(third.payload.uid !== first.payload.uid, 'expired artifact cannot restore the old Session');
  assert.equal(third.payload.iat, epoch()); assert.equal(third.payload.exp - third.payload.iat, 600);
  assert.ok(f.events.filter(event => event.kind === 'consent').at(-1).reasons.includes('no_session'));
  for (const { payload } of f.sessions()) assert.equal(payload.authorizations[CLIENT].persistsLogout, true);
  const tokenRows = f.db.prepare("SELECT payload FROM artifacts WHERE model IN ('AuthorizationCode','RefreshToken','AccessToken')").all();
  assert.ok(tokenRows.length > 0);
  for (const row of tokenRows) assert.notEqual(JSON.parse(row.payload).expiresWithSession, true);
  assert.equal(f.events.some(event => event.kind === 'server_error'), false);
  t.diagnostic(JSON.stringify({ models: [...new Set(f.events.filter(event => event.kind === 'upsert').map(event => event.model))].sort(),
    originalWindowSeconds: 600, elapsedTime: 'controlled Date only, not600s wall clock',
    interactionFields: [...new Set(f.events.filter(event => event.kind === 'upsert' && event.model === 'Interaction').flatMap(event => event.fields))].sort() }));
});

test('crossing original expiry inside an actual resumed request produces controlled expiry, never a renewed Session', async t => {
  const f = await fixture(t), tokens = await f.authorize(), first = f.sessions()[0];
  f.advance(599);
  const flow = f.request(), begun = await f.get(flow.url);
  assert.ok(begun.url?.pathname.startsWith('/oauth/interaction/'));
  f.crossExpiryDuringAccepted(2);
  const completed = await f.finish(flow, begun);
  // A validated native redirect receives the controlled OAuth error via query;
  // authorization_error_handler uses renderError only when redirect is unsafe.
  assert.equal(completed.status, 303);
  assert.equal(completed.url?.origin + completed.url?.pathname, REDIRECT);
  assert.equal(completed.url.searchParams.get('error'), 'invalid_request');
  assert.equal(completed.url.searchParams.has('code'), false);
  assert.ok(completed.url.searchParams.get('state') === flow.state, 'error response preserves request binding');
  for (const row of f.sessions()) assert.ok(row.payload.exp <= first.payload.iat + 600, 'no Math.max(1) lifetime extension');
  assert.equal((await f.refresh(tokens.refresh_token)).status, 200, 'Session save expiry does not revoke accepted family');
  await f.authorize();
  const fresh = f.sessions().find(row => row.payload.exp > epoch());
  assert.ok(fresh.payload.uid !== first.payload.uid, 'next authorization starts a fresh consent-bound Session');
  assert.equal(fresh.payload.exp - fresh.payload.iat, 600);
  assert.equal(f.events.some(event => event.kind === 'server_error'), false);
});
