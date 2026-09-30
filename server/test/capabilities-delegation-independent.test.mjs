import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import express from 'express';
import { attachCapabilitiesActions } from '../capabilities-actions.js';
import { createCapabilitiesService } from '../../modules/capabilities/server/index.mjs';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';

const DERIVE = '/api/capabilities/v1/grants/derive';
const RESPONSE_LIMIT = 65536;

// Reuse only the established real HTTP/signed Connect/store setup. No author
// delegation fixture, fabricated actor, SQL authority rows or replacement
// issuance result is used by these independent transport cases.
async function fixture(t, { audience } = {}) {
  const f = await nativeHttpFixture(t, { enabled: false });
  const identity = nativeIdentity('Independent headless delegation owner');
  const { accountId } = await f.bootstrap(identity);
  const owner = (op, args = {}) => f.call(identity, op, { expectedAccountId: accountId, ...args });
  const { principal } = good(await owner('access.principals.create', { label: 'Independent parent client' }));
  const { grant } = good(await owner('access.grants.issue', { principalId: principal.id,
    capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }], resources: ['notes:new'],
    effects: ['create'], recipients: ['soty:notes'], expiresAt: Date.now() + 3600000,
    allowDelegation: true, maxDepth: 1, budget: { unit: 'invocations', limit: 5 } }));
  const issued = good(await owner('access.credentials.issue', { grantId: grant.id, audience: audience ?? f.origin }));
  const snapshot = () => f.sql(f.capsFile, db => Object.fromEntries(
    ['clients', 'principals', 'grants', 'credentials', 'budgets', 'invocations'].map(name =>
      [name, db.prepare(`SELECT count(*) AS n FROM cap_${name}`).get().n])));
  return { ...f, owner, accountId, principal, grant, issued, snapshot,
    // Preserve the fixture's live getter instead of copying its current value.
    get app() { return f.app; } };
}

function wire(origin, path, { method = 'POST', token, body, headers = {}, allowReset = false } = {}) {
  const endpoint = new URL(origin);
  return new Promise((resolve, reject) => {
    let settled = false, bytes = 0, timer;
    const req = request({ hostname: endpoint.hostname, port: Number(endpoint.port), method, path, agent: false,
      headers: { ...(token ? { Authorization: `Bearer ${token}` } : {}),
        ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}), ...headers } }, res => {
      const chunks = [];
      res.on('data', chunk => {
        bytes += chunk.length;
        if (bytes > RESPONSE_LIMIT) { finish(new Error('independent_response_limit')); req.destroy(); return; }
        chunks.push(chunk);
      });
      res.once('error', () => finish(new Error('independent_response_error')));
      res.once('end', () => {
        try {
          const text = Buffer.concat(chunks).toString('utf8');
          finish(null, { status: res.statusCode, headers: res.headers, bytes, text,
            value: text ? JSON.parse(text) : null });
        } catch { finish(new Error('independent_non_json_response')); }
      });
    });
    function finish(error, value) {
      if (settled) return; settled = true; clearTimeout(timer);
      if (error) reject(error); else resolve(value);
    }
    timer = setTimeout(() => { finish(new Error('independent_http_deadline')); req.destroy(); }, 5000);
    req.once('error', error => {
      if (allowReset && error.code === 'ECONNRESET' && bytes === 0)
        finish(null, { status: null, headers: {}, bytes: 0, text: '', value: null, reset: true });
      else finish(new Error('independent_http_transport_error'));
    });
    req.end(body);
  });
}

function checkedPrivate(reply, secrets = []) {
  assert.equal(reply.headers['cache-control'], 'no-store');
  assert.equal(reply.headers['referrer-policy'], 'no-referrer');
  assert.equal(reply.headers['x-content-type-options'], 'nosniff');
  assert.equal(reply.headers.etag, undefined);
  assert.equal(reply.headers['retry-after'], undefined, 'one-shot issuance never recommends automatic retry');
  assert.ok(reply.bytes <= RESPONSE_LIMIT);
  for (const secret of secrets) assert.equal(reply.text.includes(secret), false, 'private input or bearer is not reflected');
}

function checkedIssued(reply, f, label, audience = f.origin) {
  assert.equal(reply.status, 201);
  checkedPrivate(reply, [f.issued.token]);
  assert.deepEqual(Object.keys(reply.value).sort(), ['credential', 'grant', 'principal', 'token']);
  const { principal, grant, credential, token } = reply.value;
  assert.equal(typeof token, 'string');
  assert.ok(/^soty_cap_[A-Za-z0-9_-]{43}$/u.test(token), 'one new opaque credential is returned');
  assert.equal(principal.label, label); assert.notEqual(principal.id, f.principal.id);
  assert.equal(grant.parentGrantId, f.grant.id); assert.equal(grant.rootGrantId, f.grant.rootGrantId);
  assert.equal(grant.principalId, principal.id); assert.equal(grant.clientId, principal.clientId);
  assert.equal(grant.allowDelegation, false); assert.equal(grant.maxDepth, 0);
  assert.deepEqual(grant.capabilities, [{ capabilityId: 'notes.createDraft', version: 1 }]);
  assert.deepEqual(grant.resources, ['notes:new']); assert.deepEqual(grant.effects, ['create']);
  assert.deepEqual(grant.recipients, ['soty:notes']);
  assert.ok(credential.expiresAt <= f.issued.credential.expiresAt);
  assert.equal(credential.grantId, grant.id); assert.equal(credential.audience, audience);
  assert.deepEqual(Object.keys(credential).sort(), ['audience', 'createdAt', 'expiresAt', 'grantId', 'id']);
  return reply.value;
}

test('independent lost derive response leaves one owner-visible child and truthful service audit without automatic replacement', { timeout: 15000 }, async t => {
  const f = await fixture(t), before = f.snapshot(), target = new URL(f.origin);
  let posts = 0, upstreamStatus = null, responseBytes = 0;
  const sockets = new Set(), upstreamRequests = new Set();
  // A real loopback response-loss proxy. It forwards one submitted request to
  // the production router and discards the actual response bytes; it never
  // reads a token value, returns a replacement key or fakes a COMMIT.
  const proxy = createServer((req, res) => {
    if (req.method !== 'POST' || req.url !== DERIVE) { res.writeHead(404).end(); return; }
    posts++;
    const upstream = request({ hostname: target.hostname, port: Number(target.port), method: 'POST', path: DERIVE,
      headers: { Authorization: `Bearer ${f.issued.token}`, 'Content-Type': 'application/json',
        ...(req.headers['content-length'] ? { 'Content-Length': req.headers['content-length'] } : {}) } }, response => {
      upstreamStatus = response.statusCode;
      response.on('data', chunk => {
        responseBytes += chunk.length;
        if (responseBytes > RESPONSE_LIMIT) { upstream.destroy(); res.destroy(); }
      });
      response.once('error', () => res.destroy());
      response.once('end', () => res.destroy());
    });
    upstreamRequests.add(upstream); upstream.once('close', () => upstreamRequests.delete(upstream));
    upstream.setTimeout(4000, () => upstream.destroy());
    upstream.once('error', () => res.destroy()); req.pipe(upstream);
  });
  proxy.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await new Promise(resolve => proxy.listen(0, '127.0.0.1', resolve));
  try {
    const lost = await wire(`http://127.0.0.1:${proxy.address().port}`, DERIVE,
      { body: JSON.stringify({ label: 'Ответ потерян', expiresAt: Date.now() + 300000 }), allowReset: true });
    assert.equal(lost.reset, true); assert.equal(lost.bytes, 0); assert.equal(upstreamStatus, 201);
    assert.equal(posts, 1); assert.ok(responseBytes > 0 && responseBytes <= RESPONSE_LIMIT);
    const after = f.snapshot();
    for (const name of ['clients', 'principals', 'grants', 'credentials']) assert.equal(after[name], before[name] + 1);
    assert.equal(after.budgets, before.budgets); assert.equal(after.invocations, 0);
    const grants = good(await f.owner('access.grants.list')).grants;
    const children = grants.filter(row => row.parentGrantId === f.grant.id);
    assert.equal(children.length, 1); assert.equal(children[0].rootGrantId, f.grant.rootGrantId);
    const principals = good(await f.owner('access.principals.list')).principals;
    assert.equal(principals.filter(row => row.id === children[0].principalId).length, 1);
    const events = good(await f.owner('access.events.list')).events;
    const issued = events.filter(row => row.objectType === 'grant' && row.objectId === children[0].id);
    assert.equal(issued.length, 1); assert.equal(issued[0].actorType, 'service');
    assert.equal(issued[0].actorId, f.principal.id);
    assert.equal(JSON.stringify({ grants, principals, events }).includes('soty_cap_'), false, 'owner metadata does not recover plaintext');
    good(await f.owner('access.grants.revoke', { grantId: children[0].id }));
    const final = good(await f.owner('access.grants.list')).grants;
    assert.equal(typeof final.find(row => row.id === children[0].id).revokedAt, 'number');
    assert.equal(final.find(row => row.id === f.grant.id).revokedAt, null);
    assert.equal(posts, 1, 'read and safety revoke never retry issuance');
    assert.deepEqual(f.snapshot(), after);
    t.diagnostic(JSON.stringify({ posts, upstreamStatus, discardedResponseBytes: responseBytes,
      childCount: children.length, serviceAuditCount: issued.length, replacementRequests: 0 }));
  } finally {
    for (const req of upstreamRequests) req.destroy();
    for (const socket of sockets) socket.destroy();
    await new Promise(resolve => proxy.close(resolve));
  }
});

test('independent derive wire rejects aliases and authority inputs before issuing a strict Unicode leaf', { timeout: 15000 }, async t => {
  const f = await fixture(t), before = f.snapshot(), expiresAt = Date.now() + 300000;
  const body = JSON.stringify({ label: 'Strict leaf', expiresAt });
  for (const path of [`${DERIVE}?`, `${DERIVE}/`, DERIVE.replace('/derive', '/%64erive'), DERIVE.replace('/grants/', '/%67rants/')]) {
    const denied = await wire(f.origin, path, { token: f.issued.token, body });
    assert.ok([400, 404].includes(denied.status), 'non-canonical targets are refused');
    checkedPrivate(denied, [f.issued.token]);
  }
  const invalidBodies = [
    ...['parentGrantId', 'accountId', 'principalId', 'scope', 'audience', 'budget', 'allowDelegation', 'idempotencyKey']
      .map(key => JSON.stringify({ label: 'Strict leaf', expiresAt, [key]: 'not-client-authority' })),
    `{"label":"a","labe\\u006c":"b","expiresAt":${expiresAt}}`,
    JSON.stringify({ label: '\ud800', expiresAt }),
    JSON.stringify({ label: '🐝'.repeat(51), expiresAt }),
    JSON.stringify({ label: 'Strict leaf', expiresAt: String(expiresAt) }),
  ];
  for (const payload of invalidBodies) {
    const denied = await wire(f.origin, DERIVE, { token: f.issued.token, body: payload });
    assert.equal(denied.status, 400); checkedPrivate(denied, [f.issued.token, 'not-client-authority']);
  }
  const mcp = good(await f.owner('access.credentials.issue', { grantId: f.grant.id, audience: `${f.origin}/mcp` }));
  const wrongAudience = await wire(f.origin, DERIVE, { token: mcp.token, body });
  assert.equal(wrongAudience.status, 401); checkedPrivate(wrongAudience, [mcp.token]);
  const beforeSuccess = f.snapshot();
  assert.deepEqual(beforeSuccess, { ...before, credentials: before.credentials + 1 });
  const label = '🐝'.repeat(50), created = await wire(f.origin, DERIVE,
    { token: f.issued.token, body: JSON.stringify({ label, expiresAt }) });
  checkedIssued(created, f, label);
  assert.equal(f.snapshot().grants, before.grants + 1);
  t.diagnostic(JSON.stringify({ aliasCases: 4, invalidBodies: invalidBodies.length, wrongAudience: 401, created: 1, labelUtf16Units: label.length }));
});

test('independent parent credential revoked during body read cannot use the earlier authenticated actor to derive', { timeout: 15000 }, async t => {
  const f = await fixture(t), real = f.app.locals.capabilitiesService, before = f.snapshot();
  let signalAuthenticated, authenticationCalls = 0;
  const authenticated = new Promise(resolve => { signalAuthenticated = resolve; });
  const front = express();
  attachCapabilitiesActions(front, { audience: f.origin, service: Object.freeze({ ...real,
    authenticateCredential(args) {
      const actor = real.authenticateCredential(args); authenticationCalls++; signalAuthenticated(); return actor;
    } }) });
  front.use(f.app); f.setFront(front);
  const payload = JSON.stringify({ label: 'Read was held', expiresAt: Date.now() + 300000 });
  const target = new URL(f.origin); let req, timeout;
  try {
    const result = new Promise((resolve, reject) => {
      req = request({ hostname: target.hostname, port: Number(target.port), path: DERIVE, method: 'POST', agent: false,
        headers: { Authorization: `Bearer ${f.issued.token}`, 'Content-Type': 'application/json',
          'Content-Length': String(Buffer.byteLength(payload)) } }, res => {
        let text = '', bytes = 0;
        res.on('data', chunk => { bytes += chunk.length; if (bytes > RESPONSE_LIMIT) req.destroy(); else text += chunk.toString('utf8'); });
        res.once('error', () => reject(new Error('independent_held_response_error')));
        res.once('end', () => {
          try { resolve({ status: res.statusCode, headers: res.headers, bytes, text, value: JSON.parse(text) }); }
          catch { reject(new Error('independent_held_non_json')); }
        });
      });
      req.once('error', () => reject(new Error('independent_held_transport_error')));
      timeout = setTimeout(() => req.destroy(), 5000);
      req.write(payload.slice(0, -1));
    });
    // This transparent observer fires only after genuine authentication. No
    // actor, Connect fence, body bytes or credential state is fabricated.
    await Promise.race([authenticated, result.then(() => { throw new Error('derive_did_not_wait_for_body'); })]);
    good(await f.owner('access.credentials.revoke', { credentialId: f.issued.credential.id }));
    req.end(payload.slice(-1));
    const denied = await result;
    assert.equal(denied.status, 401); checkedPrivate(denied, [f.issued.token]);
    assert.deepEqual(f.snapshot(), before);
    assert.equal(authenticationCalls, 1);
    t.diagnostic(JSON.stringify({ authenticatedBeforeRevoke: true, status: denied.status, newChildren: 0 }));
  } finally { clearTimeout(timeout); req?.destroy(); f.setFront(null); }
});

test('independent HTTPS derive boundary works without OAuth metadata or native effect readiness', { timeout: 15000 }, async t => {
  const audience = 'https://delegation-independent.invalid', f = await fixture(t, { audience });
  const connect = f.app.locals.connectService;
  const service = createCapabilitiesService({ databasePath: f.capsFile, projectId: 'soty',
    actorActive: actor => connect.isActorActive(actor) === true,
    delegation: { audience, withAuthorityFence: action => connect.withAuthorityFence(action) } });
  let authenticationCalls = 0;
  const observed = Object.freeze({ ...service, authenticateCredential(args) {
    authenticationCalls++; return service.authenticateCredential(args);
  } });
  const setFront = trusted => {
    const front = express(); front.set('trust proxy', trusted ? 'loopback' : false);
    attachCapabilitiesActions(front, { service: observed, audience, resourceMetadata: null });
    front.use(f.app); f.setFront(front);
  };
  const body = JSON.stringify({ label: 'TLS boundary child', expiresAt: Date.now() + 300000 });
  const headers = { Host: new URL(audience).host, 'X-Forwarded-Proto': 'https', Forwarded: 'proto=https;host=forged.invalid' };
  try {
    const before = f.snapshot(); setFront(false);
    const denied = await wire(f.origin, DERIVE, { token: f.issued.token, headers, body });
    assert.equal(denied.status, 403); checkedPrivate(denied, [f.issued.token, 'forged.invalid']);
    assert.equal(authenticationCalls, 0); assert.deepEqual(f.snapshot(), before);
    assert.equal(service.oauth, undefined); assert.equal(service.nativeNotes, null);
    setFront(true);
    const accepted = await wire(f.origin, DERIVE, { token: f.issued.token, headers, body });
    checkedIssued(accepted, f, 'TLS boundary child', audience);
    assert.equal(f.snapshot().grants, before.grants + 1);
    t.diagnostic(JSON.stringify({ untrusted: denied.status, trustedLoopbackProxy: accepted.status,
      oauthConfigured: false, nativePortPresent: false, childCount: 1 }));
  } finally { f.setFront(null); service.close(); }
});
