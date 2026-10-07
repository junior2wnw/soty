import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { randomBytes, createHmac } from 'node:crypto';
import { environment } from '../../human-identity/test/support/renewal-fixture.mjs';
import { createSourceAppBff } from '../server/bff.mjs';
import { createSourceNativeAuthorityPort } from '../server/native-authority.mjs';
import { STANDARD_SELECTED_SOURCE } from '../server/standard-profile.mjs';
import { digest } from '../server/wire.mjs';
import { selectedResourceProfile } from '../../apps/scoped-embed/resource-profile.mjs';
import { createResourceSourceProofSigner } from '../../apps/scoped-embed/resource-proof.mjs';

const opaque = () => randomBytes(32).toString('base64url');
test('actual maintained Root OIDC + controlled private bridge establishes Basic Source session; Native revoke and body actor claims deny', { timeout: 15000 }, async t => {
  const root = await environment(t, { renewal: false });
  let bff, context, live = true, linked = false;
  const sockets = new Set(), key = randomBytes(32);
  const source = createServer((req, res) => bff.handleRequest(req, res).then(handled => { if (!handled) res.writeHead(404).end(); }));
  const ipc = createServer(async (req, res) => {
    const parts = []; for await (const part of req) parts.push(part);
    const input = JSON.parse(Buffer.concat(parts).toString('utf8'));
    const body = JSON.stringify({ nonce: input.nonce, context });
    res.writeHead(200, { 'content-type': 'application/json', 'x-soty-source-mac': createHmac('sha256', key).update('authority-result\0' + body).digest('base64url') }); res.end(body);
  });
  for (const server of [source, ipc]) {
    server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
    await new Promise(done => server.listen(0, '127.0.0.1', done));
  }
  t.after(async () => { bff?.close(); sockets.forEach(socket => socket.destroy()); await Promise.all([source, ipc].map(server => new Promise(done => server.close(done)))); });
  const embedOrigin = 'http://127.0.0.1:' + source.address().port, nativeOrigin = 'http://localhost:' + source.address().port;
  let client;
  await root.configureClients(clients => { client = { ...clients[0], version: 2, redirectUri: nativeOrigin + '/soty/callback' }; return [client]; });
  const profile = selectedResourceProfile({ schema: 'soty.selected-human-embed.v2', appId: 'app-' + 'a'.repeat(32),
    connector: { linkId: 'fixture_link', hostDeviceId: 'fixture_host', connectorId: 'fixture_connector' }, target: { revision: 1, digest: 'a'.repeat(64) },
    sourceProfile: STANDARD_SELECTED_SOURCE, resource: { registryId: 'soty', environmentId: 'production', tenantId: root.actor.account.accountId,
      appId: 'app-' + 'a'.repeat(32), resourceId: 'fixture:resource', selection: { kind: 'soty.resource.v1', nativeId: 'selected', incarnationId: 'fixture_incarnation' } },
    issuer: root.issuer, clientId: client.id, embedOrigin, nativeOrigin, parentOrigin: root.origin });
  context = { schema: 'soty.verified-launch-continuation.v2', reference: { id: opaque(), version: 1, digest: 'd'.repeat(64) },
    profileDigest: profile.digest, appId: profile.appId, sourceProfile: profile.sourceProfile, resource: profile.resource,
    rootPrincipal: { accountId: root.actor.account.accountId, deviceId: root.actor.account.deviceId },
    humanPrincipal: { issuer: root.issuer, subject: root.actor.account.accountId, clientId: client.id,
      clientProfileDigest: root.profile.publicClients[0].profileDigest, clientGeneration: 1 },
    entry: { domainId: 'fixture_domain', origin: embedOrigin }, target: profile.target, policyEpoch: 1, expiresAt: Date.now() + 300000 };
  const interactions = new Map(), sessions = new Map(), completions = new Map(), nonces = new Set();
  // Controlled RAM fixture only; durable encrypted Source storage is the next
  // ordinary-app gate. This is not a production/installed subject-bridge claim.
  const storage = {
    consumeNonce: async value => { if (nonces.has(value)) return false; nonces.add(value); return true; },
    createInteraction: async record => { interactions.set(record.idHash, structuredClone(record)); },
    getInteraction: async hash => structuredClone(interactions.get(hash) ?? null),
    claimInteraction: async (hash, revision, intent) => { const row = interactions.get(hash); if (row?.revision !== revision || row.phase !== 'pending') return false;
      row.phase = 'claimed'; row.revision++; row.protocolIntent = structuredClone(intent); return true; },
    claimCallback: async (hash, revision) => { const row = interactions.get(hash); if (row?.revision !== revision || row.phase !== 'claimed') return false;
      row.phase = 'exchanging'; row.revision++; return true; },
    completeInteraction: async (input, final) => { const row = interactions.get(input.idHash); if (row?.revision !== input.revision || row.phase !== 'exchanging') return false;
      final(); row.phase = 'completed'; row.revision++; row.completionToken = input.completionToken;
      sessions.set(input.session.idHash, structuredClone(input.session)); completions.set(input.idHash, input.session.idHash);
      completions.set(input.session.completionHash, input.session.idHash); return true; },
    readSession: async hash => structuredClone(sessions.get(hash) ?? null),
    readCompletion: async hash => structuredClone(sessions.get(completions.get(hash)) ?? null),
    consumeCompletion: async (hash, sessionHash, final) => { if (completions.get(hash) !== sessionHash) return null; final(); completions.delete(hash); return { token: sessions.get(sessionHash).cookieToken }; },
    readTokenProof: async session => ({ accessToken: session.accessToken, expiresAt: session.expiresAt }),
    revokeSession: async hash => { sessions.get(hash).active = false; },
  };
  const native = createSourceNativeAuthorityPort({ capture: async binding => {
    assert.equal(live, true); assert.equal(binding.identity.subject, root.actor.account.accountId);
    if (binding.operation !== 'link') assert.equal(linked, true); return {}; },
    withCurrent(_native, _binding, final) { assert.equal(live, true); return final(); },
    linkVerifiedIdentity() { linked = true; return true; }, read: async (_proof, _binding, args) => ({ selected: 'real-maintained-OIDC-fixture', requestId: args.requestId }) });
  bff = createSourceAppBff({ profile, transportKey: key, connectorPort: ipc.address().port, storage, native,
    rp: { issuer: root.issuer, clientId: client.id, clientSecret: client.clientSecret, redirectUri: client.redirectUri } });
  const signer = createResourceSourceProofSigner({ profile, key });
  const jars = new Map();
  async function request(origin, path, { method = 'GET', data, form, signed = false, requestOrigin = origin } = {}) {
    const jar = jars.get(origin) ?? new Map(); jars.set(origin, jar);
    const cookie = [...jar].map(([name, value]) => name + '=' + value).join('; ');
    const body = form ? Buffer.from(new URLSearchParams(form).toString()) : data ? Buffer.from(JSON.stringify(data)) : Buffer.alloc(0);
    const response = await fetch(origin + path, { method, redirect: 'manual', headers: { ...(cookie ? { cookie } : {}),
      ...(method === 'POST' ? { origin: requestOrigin, 'content-type': form ? 'application/x-www-form-urlencoded' : 'application/json' } : {}),
      ...(signed ? signer.headers({ context, method, path, body, cookie }) : {}) }, ...(method === 'POST' ? { body } : {}) });
    for (const header of response.headers.getSetCookie()) { const pair = header.split(';')[0], split = pair.indexOf('='); jar.set(pair.slice(0, split), pair.slice(split + 1)); }
    const text = await response.text(); let value; try { value = JSON.parse(text); } catch {}
    return { status: response.status, text, value, location: response.headers.get('location') };
  }
  assert.equal((await request(embedOrigin, '/api/embed/login', { method: 'POST', data: {}, signed: true, requestOrigin: 'https://foreign.invalid' })).status, 403);
  const start = await request(embedOrigin, '/api/embed/login', { method: 'POST', data: {}, signed: true }); assert.equal(start.status, 200);
  const handoff = new URL(start.value.nativeUrl), nativePage = await request(nativeOrigin, handoff.pathname + handoff.search); assert.equal(nativePage.status, 200);
  const form = Object.fromEntries([...nativePage.text.matchAll(/name="([^"]+)" value="([^"]*)"/gu)].map(match => [match[1], match[2]])); form.consent = 'yes';
  assert.equal((await request(nativeOrigin, '/soty/authorize', { method: 'POST', form: { ...form, csrf: opaque() } })).status, 403);
  assert.equal((await request(nativeOrigin, '/soty/authorize', { method: 'POST', form, requestOrigin: 'https://foreign.invalid' })).status, 403);
  const authorize = await request(nativeOrigin, '/soty/authorize', { method: 'POST', form }); assert.equal(authorize.status, 303);
  const rootAuthorize = await root.wire.request(authorize.location), interaction = await root.wire.request(rootAuthorize.location);
  assert.equal(interaction.status, 200);
  const current = await root.wire.request(rootAuthorize.location.href + '/context'), flow = { rp: { redirectUri: client.redirectUri }, session: root.wire, location: rootAuthorize.location, context: current.body };
  await root.approve(flow); const finished = await root.complete(flow);
  const wrongState = new URL(finished.callback); wrongState.searchParams.set('state', opaque());
  assert.equal((await request(nativeOrigin, wrongState.pathname + wrongState.search)).status, 403);
  const duplicatedState = new URL(finished.callback); duplicatedState.searchParams.append('state', duplicatedState.searchParams.get('state'));
  assert.equal((await request(nativeOrigin, duplicatedState.pathname + duplicatedState.search)).status, 403);
  assert.equal(linked, false); assert.equal(sessions.size, 0);
  const callback = await request(nativeOrigin, finished.callback.pathname + finished.callback.search); assert.equal(callback.status, 303);
  const embeddedCallback = new URL(callback.location), linkedReply = await request(embedOrigin, embeddedCallback.pathname + embeddedCallback.search, { signed: true }); assert.equal(linkedReply.status, 200);
  const locator = jars.get(embedOrigin).get('soty_rp_link'), completed = await request(embedOrigin, '/api/embed/complete-link?intent=' + locator, { signed: true }); assert.equal(completed.status, 200);
  const read = await request(embedOrigin, '/api/embed/query', { method: 'POST', data: { requestId: 'fixture-query-0001', input: {} }, signed: true }); assert.equal(read.status, 200);
  assert.equal(read.value.data.selected, 'real-maintained-OIDC-fixture');
  assert.equal((await request(embedOrigin, '/api/embed/query', { method: 'POST', data: { requestId: 'fixture-query-0002', input: {}, actor: 'owner' }, signed: true })).status, 400);
  assert.equal((await request(embedOrigin, '/api/embed/invoke', { method: 'POST', data: { requestId: 'fixture-write-0001', input: {} }, signed: true })).status, 503);
  const originalReference = context.reference; context.reference = { ...originalReference, id: opaque() };
  assert.equal((await request(embedOrigin, '/api/embed/query', { method: 'POST', data: { requestId: 'fixture-query-0004', input: {} }, signed: true })).status, 401);
  context.reference = originalReference;
  live = false;
  assert.equal((await request(embedOrigin, '/api/embed/query', { method: 'POST', data: { requestId: 'fixture-query-0003', input: {} }, signed: true })).status, 503);
});
