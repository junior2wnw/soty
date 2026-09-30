import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, generateKeyPairSync, randomBytes } from 'node:crypto';
import { createServer } from 'node:http';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, basename } from 'node:path';
import express from 'express';
import { AccessError } from '../../modules/capabilities/server/validation.mjs';
import { createOAuthHostProfile } from '../capabilities-oauth-profile.js';
import { attachCapabilitiesOAuth } from '../capabilities-oauth.js';

// Actual production host/router/wrapper and pinned Provider over loopback HTTP.
// This ephemeral SQLite adapter and direct owner decisions are SYNTHETIC seam
// ports: no encrypted domain storage, signed Connect consent or external CLI.
// All codes/tokens are produced by real HTTP authorization; none are seeded.
const CLIENT = 'soty-codex-cli', REDIRECT = 'http://127.0.0.1:19876/callback';
const SCOPE = 'notes.createDraft', epoch = () => Math.floor(Date.now() / 1000);
const sha = value => createHash('sha256').update(value).digest('hex');
// The maintained Provider may reorder JSON object properties when loading and
// resaving a Session. Compare all parsed fields, preserving arrays and identity,
// instead of treating a different serialization order as an authority change.
const canonical = value => Array.isArray(value) ? value.map(canonical)
  : value && typeof value === 'object' ? Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]))
    : value;
const pkce = value => createHash('sha256').update(value).digest('base64url');
const key = generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey.export({ format: 'jwk' });
Object.assign(key, { kid: 'account-switch-fixture', alg: 'RS256', use: 'sig' });

async function fixture(t, { closedConfirmation = false } = {}) {
  assert.equal(JSON.parse(readFileSync(new URL('../../node_modules/oidc-provider/package.json', import.meta.url))).version, '9.12.2');
  const parent = realpathSync(tmpdir()), directory = realpathSync(mkdtempSync(join(parent, 'soty-oauth-account-switch-')));
  const owner = randomBytes(16).toString('hex');
  writeFileSync(join(directory, 'test-owner'), owner, { flag: 'wx' });
  writeFileSync(join(directory, 'index.html'), '<!doctype html><title>Synthetic consent fixture</title><main>Consent seam</main>');
  const db = new DatabaseSync(':memory:');
  db.exec(`CREATE TABLE artifacts(model TEXT NOT NULL,id_hash TEXT NOT NULL,payload TEXT NOT NULL,
    family TEXT,expires INTEGER NOT NULL,consumed INTEGER,PRIMARY KEY(model,id_hash));
    CREATE TABLE families(id TEXT PRIMARY KEY,account_id TEXT NOT NULL,revoked INTEGER NOT NULL DEFAULT 0);`);
  const proposals = new Map(), contexts = new WeakMap(), jar = new Map(), events = [];
  let app;
  const server = createServer((req, res) => app(req, res));
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  t.after(async () => {
    await new Promise(done => { server.closeAllConnections(); server.close(done); });
    db.close();
    assert.equal(dirname(directory), parent); assert.match(basename(directory), /^soty-oauth-account-switch-/u);
    assert.equal(readFileSync(join(directory, 'test-owner'), 'utf8'), owner);
    rmSync(directory, { recursive: true });
  });
  const origin = `http://127.0.0.1:${server.address().port}`, issuer = origin + '/oauth', resource = origin + '/mcp';
  const profile = createOAuthHostProfile({ enabled: true, issuer, jwks: { keys: [key] },
    cookieKeys: [randomBytes(32).toString('base64url')], artifactKey: randomBytes(32), artifactKeyId: 'synthetic-only' },
  { audience: origin, shellOrigins: [origin] });
  const familyActive = id => db.prepare('SELECT revoked FROM families WHERE id=?').get(id)?.revoked === 0;
  const familyRevoke = id => {
    db.prepare('UPDATE families SET revoked=1 WHERE id=?').run(id);
    db.prepare('DELETE FROM artifacts WHERE family=?').run(id);
  };
  const store = {
    upsert({ model, id, payload, stagedGrant }) {
      assert.ok(Number.isSafeInteger(payload.exp) && payload.exp > epoch(), 'finite library expiry');
      let family = null;
      if (model === 'Grant') {
        const proposal = contexts.get(stagedGrant);
        assert.ok(proposal?.decision === 'approved', 'fixture only admits an explicit synthetic decision');
        if (proposal.providerGrantId && proposal.providerGrantId !== id) throw new AccessError('oauth_grant_conflict');
        proposal.providerGrantId = id; family = id;
        db.prepare('INSERT INTO families(id,account_id) VALUES(?,?) ON CONFLICT DO NOTHING').run(id, payload.accountId);
      } else if (['AuthorizationCode', 'RefreshToken', 'AccessToken'].includes(model)) family = payload.grantId;
      if (family && !familyActive(family)) throw new AccessError('access_denied');
      db.prepare(`INSERT INTO artifacts(model,id_hash,payload,family,expires) VALUES(?,?,?,?,?)
        ON CONFLICT(model,id_hash) DO UPDATE SET payload=excluded.payload,expires=excluded.expires`)
        .run(model, sha(id), JSON.stringify(payload), family, payload.exp);
      events.push({ kind: 'upsert', model, account: payload.accountId ?? null });
    },
    find({ model, id }) {
      const row = db.prepare('SELECT * FROM artifacts WHERE model=? AND id_hash=?').get(model, sha(id));
      if (!row || row.expires <= epoch() || (row.family && !familyActive(row.family))) return undefined;
      return { ...JSON.parse(row.payload), ...(row.consumed === null ? {} : { consumed: row.consumed }) };
    },
    findByUid({ uid }) {
      const row = db.prepare("SELECT payload FROM artifacts WHERE model='Session' AND json_extract(payload,'$.uid')=? AND expires>?")
        .get(uid, epoch());
      return row && JSON.parse(row.payload);
    },
    consume({ model, id }) {
      const row = db.prepare('SELECT family FROM artifacts WHERE model=? AND id_hash=?').get(model, sha(id));
      if (!row || (row.family && !familyActive(row.family))) return { status: 'invalid_grant' };
      const changed = db.prepare('UPDATE artifacts SET consumed=? WHERE model=? AND id_hash=? AND consumed IS NULL')
        .run(epoch(), model, sha(id));
      if (changed.changes !== 1) { if (row.family) familyRevoke(row.family); return { status: 'invalid_grant' }; }
      return { status: 'consumed' };
    },
    destroy({ model, id }) {
      const row = db.prepare('SELECT family FROM artifacts WHERE model=? AND id_hash=?').get(model, sha(id));
      events.push({ kind: 'destroy', model });
      if (row?.family) familyRevoke(row.family);
      else db.prepare('DELETE FROM artifacts WHERE model=? AND id_hash=?').run(model, sha(id));
    },
    revokeByGrantId({ providerGrantId }) { familyRevoke(providerGrantId); },
  };
  const presentation = proposal => ({ interactionId: proposal.id, contextDigest: 'a'.repeat(64),
    clientProfile: CLIENT, resource, scope: SCOPE, durationMs: 3600000, budgetLimit: 2,
    expiresAt: proposal.expiresAt, checkedAt: Date.now(), decision: proposal.decision,
    decidedAccountId: proposal.accountId ?? null });
  const lookup = ({ interactionId, browserNonce }) => {
    const proposal = proposals.get(interactionId);
    if (!proposal || proposal.nonce !== browserNonce) throw new AccessError('access_denied');
    return proposal;
  };
  const oauth = {
    readiness: () => ({ available: true }), // Explicit test facade, not production readiness.
    artifactStore: store,
    prepareInteraction({ interactionId, browserNonce }) {
      let proposal = proposals.get(interactionId);
      if (!proposal) {
        const payload = store.find({ model: 'Interaction', id: interactionId });
        assert.ok(payload?.params.client_id === CLIENT, 'real Provider Interaction required');
        proposal = { id: interactionId, nonce: browserNonce, decision: 'pending', expiresAt: payload.exp * 1000 };
        proposals.set(interactionId, proposal);
      }
      assert.ok(proposal.nonce === browserNonce, 'same browser binding');
      return presentation(proposal);
    },
    readInteraction(args) { return presentation(lookup(args)); },
    beginGrantBinding(args) {
      const proposal = lookup(args); assert.equal(proposal.decision, 'approved');
      const context = Object.freeze({}); contexts.set(context, proposal);
      return { context, providerGrantId: proposal.providerGrantId,
        connection: { accountId: proposal.accountId, staticClientId: CLIENT, resource, expiresAt: Date.now() + 3600000 } };
    },
    endGrantBinding(context) { contexts.delete(context); },
  };
  app = express();
  app.use((_req, res, next) => {
    res.set('Content-Security-Policy', "default-src 'self'; base-uri 'none'; object-src 'none'; script-src 'self'; form-action 'self'"); next();
  });
  if (closedConfirmation) app.post('/oauth/session/end/confirm', (_req, res) => {
    res.status(400).set('Cache-Control', 'no-store').json({ error: 'fixture_closed_confirmation' });
  });
  attachCapabilitiesOAuth(app, { profile, service: { oauth }, distDir: directory });

  function saveCookies(response, url) {
    for (const raw of response.headers.getSetCookie()) {
      const [pair, ...attributes] = raw.split(';'), equal = pair.indexOf('=');
      const attrs = Object.fromEntries(attributes.map(part => {
        const split = part.indexOf('='); return split < 0 ? [part.trim().toLowerCase(), true]
          : [part.slice(0, split).trim().toLowerCase(), part.slice(split + 1).trim()];
      }));
      const cookie = { name: pair.slice(0, equal), value: pair.slice(equal + 1),
        path: attrs.path ?? url.pathname.replace(/[^/]*$/u, ''),
        expires: attrs['max-age'] !== undefined ? Date.now() + Number(attrs['max-age']) * 1000
          : attrs.expires ? Date.parse(attrs.expires) : Infinity };
      const id = cookie.name + '\0' + cookie.path;
      if (!cookie.value || cookie.expires <= Date.now()) jar.delete(id); else jar.set(id, cookie);
    }
  }
  async function request(input, { fields, originHeader = origin, cookies = true } = {}) {
    const url = new URL(input, origin); assert.equal(url.origin, origin, 'never request the native callback or another server');
    const cookie = [...jar.values()].filter(item => item.expires > Date.now()
      && (url.pathname === item.path || url.pathname.startsWith(item.path.endsWith('/') ? item.path : item.path + '/')))
      .sort((a, b) => b.path.length - a.path.length).map(item => item.name + '=' + item.value).join('; ');
    const response = await fetch(url, { redirect: 'manual', signal: AbortSignal.timeout(4000),
      ...(fields ? { method: 'POST', body: new URLSearchParams(fields) } : {}),
      headers: { ...(cookies ? { Cookie: cookie } : {}), ...(fields && originHeader ? { Origin: originHeader, 'Sec-Fetch-Site': 'same-origin' } : {}) } });
    if (cookies) saveCookies(response, url);
    const location = response.headers.get('location');
    return { status: response.status, headers: response.headers, text: await response.text(),
      location: location && new URL(location, url) };
  }
  async function begin(accountId) {
    const verifier = randomBytes(32).toString('base64url'), state = randomBytes(24).toString('base64url');
    const query = new URLSearchParams({ client_id: CLIENT, redirect_uri: REDIRECT, response_type: 'code', resource,
      scope: SCOPE, code_challenge_method: 'S256', code_challenge: pkce(verifier), state });
    const authorize = await request('/oauth/authorize?' + query);
    assert.ok([302, 303].includes(authorize.status) && authorize.location?.origin === origin, 'fresh real authorization reaches consent');
    const interaction = authorize.location.pathname; assert.match(interaction, /^\/oauth\/interaction\/[A-Za-z0-9_-]+$/u);
    const html = await request(interaction); assert.equal(html.status, 200);
    const context = await request(interaction + '/context'); assert.equal(context.status, 200);
    const uid = interaction.split('/').at(-1), proposal = proposals.get(uid);
    assert.equal(proposal.decision, 'pending', 'no remembered consent even with a prior AS Session');
    proposal.decision = 'approved'; proposal.accountId = accountId; // Test-only synthetic owner decision.
    const completed = await request(interaction + '/complete', { fields: { expectedAccountId: accountId } });
    assert.equal(completed.status, 303, 'complete stores real Provider interaction result');
    assert.ok(completed.location?.origin === origin, 'resume stays on issuer');
    return { verifier, state, uid, accountId, response: await request(completed.location) };
  }
  async function exchange(flow, response = flow.response) {
    assert.ok([302, 303].includes(response.status), 'authorization reaches a callback redirect');
    const callback = response.location;
    assert.ok(callback?.origin + callback?.pathname === REDIRECT, 'registered callback returned without following it');
    assert.ok(callback.searchParams.get('state') === flow.state && callback.searchParams.get('iss') === issuer, 'callback binds state and issuer');
    assert.ok(callback.searchParams.has('code') && !callback.searchParams.has('error'), 'real code produced');
    const result = await request('/oauth/token', { cookies: false, originHeader: null, fields: { grant_type: 'authorization_code', client_id: CLIENT,
      redirect_uri: REDIRECT, resource, code: callback.searchParams.get('code'), code_verifier: flow.verifier } });
    const failureCode = result.status === 200 ? '' : JSON.parse(result.text).error;
    assert.equal(result.status, 200, 'actual token exchange succeeds; safe error=' +
      (typeof failureCode === 'string' && /^[a-z_]{1,40}$/u.test(failureCode) ? failureCode : 'unknown'));
    const tokens = JSON.parse(result.text);
    assert.ok(typeof tokens.access_token === 'string' && typeof tokens.refresh_token === 'string', 'real opaque tokens returned');
    assert.ok(store.find({ model: 'AccessToken', id: tokens.access_token })?.accountId === flow.accountId, 'token belongs to the explicitly approved fixture account');
    return tokens;
  }
  const refresh = async tokens => {
    const response = await request('/oauth/token', { cookies: false, originHeader: null, fields: { grant_type: 'refresh_token', client_id: CLIENT,
      resource, refresh_token: tokens.refresh_token } });
    return { status: response.status, tokens: JSON.parse(response.text) };
  };
  const sessionState = () => db.prepare("SELECT id_hash,payload FROM artifacts WHERE model='Session' ORDER BY id_hash").all()
    .map(row => ({ id_hash: row.id_hash, payload: canonical(JSON.parse(row.payload)) }));
  const familyState = () => db.prepare('SELECT account_id,revoked FROM families ORDER BY account_id').all();
  return { origin, issuer, request, begin, exchange, refresh, sessionState, familyState, events };
}

function autoform(response, origin) {
  assert.equal(response.status, 200, 'changing the remembered AS account uses the real Provider autoform');
  const action = /<form method="post" action="([^"]+)">/u.exec(response.text)?.[1];
  assert.ok(action === origin + '/oauth/session/end/confirm', 'only the internal confirmation route is used');
  const fields = Object.fromEntries([...response.text.matchAll(/<input type="hidden" name="([^"]+)" value="([^"]*)"\/>/gu)]
    .map(match => [match[1], match[2]]));
  assert.deepEqual(Object.keys(fields).sort(), ['logout', 'xsrf']);
  assert.equal(fields.logout, 'yes'); assert.ok(typeof fields.xsrf === 'string' && fields.xsrf.length > 10);
  return { action, fields };
}

test('actual host A-to-B switch rejects forged confirmation, then preserves family A and issues a distinct B token', async t => {
  const f = await fixture(t), first = await f.begin('fixture-owner-A'), tokensA = await f.exchange(first);
  const second = await f.begin('fixture-owner-B'), form = autoform(second.response, f.origin);
  const beforeSession = sha(JSON.stringify(f.sessionState())), beforeFamilies = f.familyState();
  const wrongOrigin = await f.request(form.action, { fields: form.fields, originHeader: 'https://untrusted.invalid' });
  assert.equal(wrongOrigin.status, 403);
  assert.equal(sha(JSON.stringify(f.sessionState())), beforeSession); assert.deepEqual(f.familyState(), beforeFamilies);
  const badXsrf = await f.request(form.action, { fields: { ...form.fields, xsrf: 'incorrect-fixture-xsrf' } });
  assert.equal(badXsrf.status, 400);
  assert.equal(sha(JSON.stringify(f.sessionState())), beforeSession, 'bad XSRF must not replace or remove the AS Session');
  assert.deepEqual(f.familyState(), beforeFamilies, 'bad confirmation cannot revoke either family');
  const confirmed = await f.request(form.action, { fields: form.fields });
  assert.equal(confirmed.status, 303, 'valid library XSRF must reach the internal confirmation handler');
  assert.ok(confirmed.location?.origin === f.origin, 'logout confirmation resumes this same issuer');
  const resumed = await f.request(confirmed.location), tokensB = await f.exchange(second, resumed);
  assert.ok(tokensB.access_token !== tokensA.access_token, 'distinct account authorization produces a distinct token');
  const refreshed = await f.refresh(tokensA); assert.equal(refreshed.status, 200, 'switching account must not revoke the earlier family');
  assert.deepEqual(f.familyState().map(row => [row.account_id, row.revoked]), [['fixture-owner-A', 0], ['fixture-owner-B', 0]]);
  assert.ok(f.events.some(event => event.kind === 'destroy' && event.model === 'Session'), 'actual Provider destroyed the old Session');
  const policy = second.response.headers.get('content-security-policy') ?? '';
  const directives = Object.fromEntries(policy.split(';').filter(Boolean).map(item => {
    const [name, ...values] = item.trim().split(/\s+/u); return [name, values];
  }));
  assert.deepEqual(directives['form-action'], ["'self'", REDIRECT], 'resume autoform only expands to the checked callback');
  assert.ok(directives['script-src']?.some(value => /^'sha256-[A-Za-z0-9+/]+=*'$/u.test(value)), 'preserve the maintained Provider inline script hash');
  assert.equal(second.response.headers.get('cache-control'), 'no-store');
});

test('explicit closed-confirmation fault reproduces account-switch failure without misreporting a B callback', async t => {
  const f = await fixture(t, { closedConfirmation: true }), first = await f.begin('fixture-owner-A');
  const tokensA = await f.exchange(first), second = await f.begin('fixture-owner-B'), form = autoform(second.response, f.origin);
  const before = sha(JSON.stringify(f.sessionState())), response = await f.request(form.action, { fields: form.fields });
  assert.equal(response.status, 400); assert.equal(response.location, null);
  assert.deepEqual(JSON.parse(response.text), { error: 'fixture_closed_confirmation' });
  assert.equal(sha(JSON.stringify(f.sessionState())), before); assert.equal((await f.refresh(tokensA)).status, 200);
});

test('internal account switching leaves public RP logout and unsupported protocol features disabled', async t => {
  const f = await fixture(t);
  const response = await f.request('/.well-known/oauth-authorization-server/oauth'); assert.equal(response.status, 200);
  const metadata = JSON.parse(response.text);
  assert.equal(Object.hasOwn(metadata, 'end_session_endpoint'), false, 'public RP logout remains disabled');
  assert.deepEqual({
    pushed_authorization_request_endpoint: Object.hasOwn(metadata, 'pushed_authorization_request_endpoint'),
    dpop_signing_alg_values_supported: Object.hasOwn(metadata, 'dpop_signing_alg_values_supported'),
    request_parameter_supported: metadata.request_parameter_supported === true,
  }, { pushed_authorization_request_endpoint: false, dpop_signing_alg_values_supported: false, request_parameter_supported: false },
  'discovery must not advertise PAR, DPoP or request objects outside the fixed C1 profile');
  for (const pathname of ['/oauth/session/end', '/oauth/session/end/success', '/oauth/session/end/confirm']) {
    const denied = await f.request(pathname);
    assert.ok([400, 404, 405].includes(denied.status), 'no public GET logout route');
    assert.equal(denied.location, null); assert.equal(denied.headers.get('cache-control'), 'no-store');
  }
  assert.deepEqual(f.sessionState(), []); assert.deepEqual(f.familyState(), []);
});
