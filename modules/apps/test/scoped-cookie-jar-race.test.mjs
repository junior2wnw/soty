import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { randomBytes } from 'node:crypto';
import { createLocalScopedEmbedBroker } from '../scoped-embed/local-broker.mjs';
import { selectedResourceProfile } from '../scoped-embed/resource-profile.mjs';
import { HIVE_SELECTED_SOURCE } from '../scoped-embed/resource-route-adapters.mjs';
import { createResourceSourceProofVerifier } from '../scoped-embed/resource-proof.mjs';

const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };

// Real loopback HTTP and MAC validation; this Source only models cookie
// transport. It does not represent Native authentication or project grants.
async function fixture(t, { clock = Date.now, lifetime = 300000 } = {}) {
  const profile = selectedResourceProfile({ schema: 'soty.selected-human-embed.v2', appId: 'app-' + 'a'.repeat(32),
    connector: { linkId: 'link', hostDeviceId: 'device', connectorId: 'connector' }, target: { revision: 1, digest: '1'.repeat(64) },
    sourceProfile: HIVE_SELECTED_SOURCE,
    resource: { registryId: 'soty', tenantId: 'owner', appId: 'app-' + 'a'.repeat(32), environmentId: 'production', resourceId: 'locator',
      selection: { kind: 'hive.project.v1', nativeId: ' Native / ID ', incarnationId: 'scope' } },
    issuer: 'https://root.test/human-identity', clientId: 'hive.rp', embedOrigin: 'https://hive.root.test', nativeOrigin: 'https://hive.native.test', parentOrigin: 'https://root.test' });
  const context = { schema: 'soty.verified-launch-continuation.v2', reference: { id: randomBytes(32).toString('base64url'), version: 1, digest: '9'.repeat(64) },
    profileDigest: profile.digest, appId: profile.appId, sourceProfile: profile.sourceProfile, resource: profile.resource,
    rootPrincipal: { accountId: 'owner', deviceId: 'root-device' }, humanPrincipal: { issuer: profile.issuer, subject: 'fixture-sub', clientId: profile.clientId,
      clientProfileDigest: '3'.repeat(64), clientGeneration: 1 }, entry: { domainId: 'domain', origin: profile.embedOrigin }, target: profile.target,
    policyEpoch: 1, expiresAt: clock() + lifetime };
  const key = randomBytes(32), tokens = [randomBytes(32).toString('base64url'), randomBytes(32).toString('base64url')];
  const verifier = createResourceSourceProofVerifier({ profile, key, consumeNonce: () => true, clock });
  let current = structuredClone(context), active = true, binding = true, held = null, issue = 0;
  let observed = 0;
  const source = createServer(async (req, res) => {
    try {
      const parts = []; for await (const chunk of req) parts.push(chunk);
      await verifier.verify(req, { body: Buffer.concat(parts) }); observed++;
      const seen = req.headers.cookie ?? '';
      if (held) { const gate = held; held = null; gate.entered.resolve(); await gate.release.promise; }
      if (req.url === '/api/embed/session-continue') {
        const value = JSON.parse(Buffer.concat(parts).toString('utf8'));
        const deleting = value.requestId === 'delete_fixture_request';
        const token = deleting ? '' : tokens[issue++ % tokens.length];
        res.setHeader('set-cookie', 'soty_rp_session=' + token + '; HttpOnly; Path=/; SameSite=Lax; Max-Age=' + (deleting ? 0 : 300));
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify(deleting ? { schema: 'soty.source-session-continuation.v1', ready: false, reason: 'login_required' }
          : { schema: 'soty.source-session-continuation.v1', ready: true, sessionExpiresAt: context.expiresAt,
            accessExpiresAt: context.expiresAt, renewable: true, receiptDigest: '4'.repeat(64) }));
      } else {
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify({ hasCookie: Boolean(seen), latestCookie: seen === 'soty_rp_session=' + tokens[(issue - 1 + tokens.length) % tokens.length] }));
      }
    } catch { res.writeHead(403).end('{}'); }
  });
  source.listen(0, '127.0.0.1'); await once(source, 'listening');
  const broker = createLocalScopedEmbedBroker({ profile, localPort: source.address().port, key, clock,
    readAuthority: async () => { if (!active) throw Object.assign(new Error('revoked'), { code: 'scoped_embed_authority_changed' }); return current; },
    assertBinding: () => binding });
  t.after(async () => { broker.close(); source.closeAllConnections(); await new Promise(done => source.close(done)); });
  const read = (ctx = context) => broker.dispatch({ context: ctx, method: 'GET', path: '/api/embed/project' });
  const change = (requestId = 'issue_fixture_request', ctx = context) => broker.dispatch({ context: ctx, method: 'POST', path: '/api/embed/session-continue',
    headers: { origin: profile.embedOrigin }, body: Buffer.from(JSON.stringify({ requestId })) });
  return { broker, context, read, change, observed: () => observed,
    hold() { const gate = { entered: deferred(), release: deferred() }; held = gate; return gate; },
    revoke() { active = false; }, retire() { binding = false; }, replace(value) { current = value; } };
}

test('older cookie-free GET cannot erase the cookie committed by a concurrent real HTTP continuation', async t => {
  const f = await fixture(t), gate = f.hold(), old = f.read(); await gate.entered.promise;
  const ack = await f.change(); assert.equal(ack.auth.ack.ready, true); gate.release.resolve(); await old;
  const result = JSON.parse((await f.read()).body);
  assert.equal(result.hasCookie, true); assert.equal(result.latestCookie, true);
});

test('older GET cannot restore a replaced cookie', async t => {
  const f = await fixture(t); await f.change();
  const gate = f.hold(), old = f.read(); await gate.entered.promise;
  await f.change(); gate.release.resolve(); await old;
  assert.equal(JSON.parse((await f.read()).body).latestCookie, true);
});

test('older GET cannot undo an explicit Source deletion', async t => {
  const f = await fixture(t); await f.change();
  const deletion = f.hold(), beforeDelete = f.read(); await deletion.entered.promise;
  const ack = await f.change('delete_fixture_request'); assert.equal(ack.auth.ack.ready, false);
  deletion.release.resolve(); await beforeDelete;
  assert.equal(JSON.parse((await f.read()).body).hasCookie, false);
});

for (const [label, mutate] of [
  ['revocation', f => f.revoke()], ['retired binding', f => f.retire()],
  ['profile change', f => f.replace({ ...f.context, profileDigest: 'f'.repeat(64) })],
  ['reference replacement', f => f.replace({ ...f.context, reference: { ...f.context.reference, digest: 'e'.repeat(64) } })],
]) test('held continuation cannot install cookies after ' + label, async t => {
  const f = await fixture(t), gate = f.hold(), pending = f.change();
  const denied = assert.rejects(pending); await gate.entered.promise; mutate(f); gate.release.resolve(); await denied;
  const next = { ...f.context, reference: { ...f.context.reference, id: randomBytes(32).toString('base64url') } };
  f.replace(next);
  if (label === 'profile change' || label === 'reference replacement') {
    assert.equal(JSON.parse((await f.read(next)).body).hasCookie, false);
  } else await assert.rejects(f.read(next));
});

test('forget retires a pending read and continuation even while the trusted Root binding stays current', async t => {
  for (const mode of ['read', 'continue']) {
    const f = await fixture(t); await f.change();
    const gate = f.hold(), pending = mode === 'read' ? f.read() : f.change();
    const denied = assert.rejects(pending, { code: 'scoped_embed_authority_changed' });
    await gate.entered.promise; f.broker.forget(f.context.reference);
    // A fresh request may create a new empty head; the retired request cannot
    // mutate it, even with the same still-current reference and valid cookies.
    assert.equal(JSON.parse((await f.read()).body).hasCookie, false);
    gate.release.resolve(); await denied;
    assert.equal(JSON.parse((await f.read()).body).hasCookie, false);
  }
});

test('same reference ID with changed immutable context cannot borrow its prior cookie', async t => {
  const f = await fixture(t); await f.change(); const before = f.observed();
  const next = { ...f.context, reference: { ...f.context.reference, digest: 'e'.repeat(64) } };
  f.replace(next); await assert.rejects(f.read(next), { code: 'scoped_embed_authority_changed' });
  assert.equal(f.observed(), before, 'denied before Source HTTP');
});

test('final trusted Root deadline retains the jar across its prior deadline without extending any Source cookie', async t => {
  let now = 100000; const f = await fixture(t, { clock: () => now, lifetime: 5000 }); await f.change();
  const gate = f.hold(), pending = f.read(); await gate.entered.promise;
  const next = { ...f.context, expiresAt: now + 10000 }; f.replace(next); now += 7000;
  gate.release.resolve(); await pending;
  assert.equal(JSON.parse((await f.read(next)).body).hasCookie, true);
  now += 300000; f.replace({ ...next, expiresAt: now + 5000 });
  assert.equal(JSON.parse((await f.read({ ...next, expiresAt: now + 5000 })).body).hasCookie, false,
    'a current Root context never extends the Source cookie expiry');
});
