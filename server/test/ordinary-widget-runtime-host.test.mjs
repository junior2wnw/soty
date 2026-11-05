import test from 'node:test';
import assert from 'node:assert/strict';
import { request } from 'node:http';
import WebSocket from 'ws';
import { createInvitationFixture } from '../../modules/invitations/test/http-fixture.mjs';

const password = 'Own isolated test password 2026!';
const code = expected => error => { assert.equal(error.code, expected); return true; };
function http(port, origin, path, { method = 'GET', cookie, body, requestOrigin = origin, headers = {} } = {}) {
  return new Promise((resolve, reject) => {
    const req = request({ hostname: '127.0.0.1', port, method, path, headers: { host: new URL(origin).host,
      ...(requestOrigin ? { origin: requestOrigin } : {}), ...(cookie ? { cookie } : {}),
      ...(body ? { 'content-type': 'application/json' } : {}), ...headers } }, res => {
      const chunks = []; res.on('data', bytes => chunks.push(bytes)); res.on('error', reject);
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks).toString() }));
    }); req.on('error', reject); req.end(body ? JSON.stringify(body) : undefined);
  });
}
async function fixture(t, options) {
  const f = await createInvitationFixture(options); t.after(() => f.close());
  const port = f.server.address().port;
  const call = (op, args = {}, actor = f.owner) => f.call(actor, op, { expectedAccountId: actor.account.accountId, ...args });
  async function placement(name) {
    const widget = (await call('canvas.widgets.create', { requestId: 'create_' + name, appId: f.appId })).current;
    const object = (await call('canvas.objects.create', { requestId: 'object_' + name,
      binding: { providerId: 'soty.apps', ref: widget.widgetId }, presentation: { mask: { shape: 'rect' } },
      visibility: { audience: 'public', accountIds: [], communityIds: [] }, hidden: { owner: false, others: false }, protection: { kind: 'none' } })).current;
    await call('canvas.vault.protect', { requestId: 'protect_' + name, objectId: object.objectId, expectedObjectRevision: object.revision, password });
    await call('canvas.vault.unlock', { objectId: object.objectId, password });
    return { widget, object, name };
  }
  const W1 = await placement('one'), W2 = await placement('two'), jar = new Map();
  async function launch(placement, actor = f.owner) {
    const result = await call('apps.launch', { appId: f.appId,
      ...(placement ? { ...placement.widget.entry, instanceId: placement.widget.widgetId } : {}) }, actor);
    const url = new URL(result.launchUrl), origin = result.runtimeOrigin ?? result.entry.origin;
    assert.equal(url.origin, origin, 'the actual boot origin must equal the declared runtime origin');
    return { result, url, origin };
  }
  async function admit(placement) {
    const launched = await launch(placement);
    const response = await http(port, launched.origin, '/_soty/session', { method: 'POST', body: { ticket: launched.url.hash.slice(1) } });
    assert.equal(response.status, 200, 'legitimate own fixture must mint its own host cookie');
    const cookie = response.headers['set-cookie'][0].split(';')[0]; jar.set(launched.origin, cookie);
    const issued = JSON.parse(response.body);
    const checked = await http(port, launched.origin, '/_soty/session', { cookie, headers: { 'x-soty-boot-check': issued.sessionCheck } });
    assert.equal(checked.status, 200); assert.equal(JSON.parse(checked.body).sessionCheck === issued.sessionCheck, true);
    assert.equal((await http(port, launched.origin, '/_soty/session', { cookie, headers: { 'x-soty-boot-check': 'x'.repeat(43) } })).status, 403);
    return launched;
  }
  const read = launched => http(port, launched.origin, '/', { cookie: jar.get(launched.origin) });
  const relock = placement => call('canvas.vault.relock', { requestId: 'relock_' + placement.name, objectId: placement.object.objectId, scope: 'device' });
  return { ...f, port, call, W1, W2, jar, launch, admit, read, relock };
}

test('actual Root HTTP: locking the last admitted widget cannot poison independent main or surviving widget', { timeout: 30000 }, async t => {
  const f = await fixture(t), main = await f.admit(), w1 = await f.admit(f.W1), w2 = await f.admit(f.W2);
  assert.equal((await f.read(main)).status, 200); assert.equal((await f.read(w1)).status, 200); assert.equal((await f.read(w2)).status, 200);
  await f.relock(f.W2);
  assert.equal((await f.read(main)).status, 200, 'independently admitted main must survive W2 relock');
  assert.equal((await f.read(w1)).status, 200, 'W1 must retain its own admitted host session');
  assert.equal((await f.read(w2)).status, 403);
  assert.notEqual(w1.origin, w2.origin); assert.notEqual(main.origin, w1.origin);
  assert.equal(w1.result.entry.origin, main.result.entry.origin, 'canonical entry stays canonical');
});

test('actual Root HTTP: an independently allowed main cookie cannot stand in for a locked widget', { timeout: 30000 }, async t => {
  const f = await fixture(t), w1 = await f.admit(f.W1), w2 = await f.admit(f.W2), main = await f.admit();
  await f.relock(f.W2);
  assert.equal((await f.read(w2)).status, 403, 'W2 must not inherit the last main holder');
  assert.equal((await f.read(main)).status, 200); assert.equal((await f.read(w1)).status, 200);
  assert.equal((await http(f.port, w1.origin, '/')).status, 401, 'internal runtime host always requires its own cookie');
});

test('actual Root HTTP: held tickets, host swaps, cross-origin writes and app revocation cannot mint or reuse widget authority', { timeout: 30000 }, async t => {
  const f = await fixture(t), held = await f.launch(f.W2), w1 = await f.admit(f.W1);
  await f.relock(f.W2);
  assert.equal((await http(f.port, held.origin, '/_soty/session', { method: 'POST', body: { ticket: held.url.hash.slice(1) } })).status, 403);
  const wrongHost = await f.launch(f.W1), w2Origin = held.origin;
  assert.equal((await http(f.port, w2Origin, '/_soty/session', { method: 'POST', body: { ticket: wrongHost.url.hash.slice(1) } })).status, 403);
  // Only isolated fixture-issued credentials stay in RAM for these negative HTTP inputs.
  assert.equal((await http(f.port, w2Origin, '/', { cookie: f.jar.get(w1.origin) })).status, 403);
  const forged = new URL(w1.origin); forged.hostname = forged.hostname.replace(/-[a-f0-9]{24}\./u, '-' + '0'.repeat(24) + '.');
  assert.equal((await http(f.port, forged.origin, '/', { cookie: f.jar.get(w1.origin) })).status, 403);
  assert.equal((await http(f.port, w1.origin, '/api/items', { method: 'POST', requestOrigin: w2Origin, cookie: f.jar.get(w1.origin), body: { text: 'must not dispatch' } })).status, 403);
  const canonical = await f.call('apps.entry.get', { appId: f.appId });
  assert.equal(canonical.entry.origin, w1.result.entry.origin);
  await f.call('apps.revoke', { appId: f.appId });
  assert.notEqual((await f.read(w1)).status, 200);
});

test('actual Root: different legitimate viewers get distinct runtime storage origins without app-domain ownership', { timeout: 30000 }, async t => {
  const f = await fixture(t);
  await f.call('apps.update', { appId: f.appId, grants: { accountIds: [f.member.account.accountId], communityIds: [] } });
  await f.call('canvas.vault.unlock', { objectId: f.W1.object.objectId, password }, f.member);
  const domainsBefore = await f.call('apps.domains.get', { appId: f.appId });
  const owner = await f.launch(f.W1), member = await f.launch(f.W1, f.member);
  assert.notEqual(owner.origin, member.origin); assert.equal(owner.result.entry.origin, member.result.entry.origin);
  assert.match(new URL(owner.origin).hostname, /^app-w[a-f0-9]{32}-[a-f0-9]{24}\.legacy\.localhost$/u);
  const ownerBoot = await http(f.port, owner.origin, '/_soty/session', { method: 'POST', body: { ticket: owner.url.hash.slice(1) } });
  const memberBoot = await http(f.port, member.origin, '/_soty/session', { method: 'POST', body: { ticket: member.url.hash.slice(1) } });
  assert.equal(ownerBoot.status, 200); assert.equal(memberBoot.status, 200);
  const memberCookie = memberBoot.headers['set-cookie'][0].split(';')[0];
  assert.equal((await http(f.port, member.origin, '/', { cookie: memberCookie })).status, 200);
  assert.equal((await http(f.port, owner.origin, '/', { cookie: memberCookie })).status, 403);
  await assert.rejects(f.call('apps.domains.claim', { appId: f.appId, slug: 'must-not-own', requestId: 'viewer_domain', expectedDomainsRevision: 0 }, f.member), code('apps_owner_required'));
  assert.equal((await f.call('apps.domains.get', { appId: f.appId })).domains.length, domainsBefore.domains.length);
});

test('actual Root: a public app alias never gives an internal widget host anonymous fallback or public reset', { timeout: 30000 }, async t => {
  const f = await fixture(t), alias = await f.call('apps.domains.claim', { appId: f.appId, slug: 'own-public-fixture', requestId: 'own_alias', expectedDomainsRevision: 0 });
  const publication = await f.call('apps.publication.get', { appId: f.appId });
  await f.call('apps.publication.update', { appId: f.appId, requestId: 'own_publish', expectedPolicyEpoch: publication.policyEpoch,
    expectedTargetRevision: publication.activeTargetRevision, launchPolicy: 'anyone', listed: false, activeDomainIds: [alias.receipt.domainId],
    exposureAck: { scope: 'whole-port', targetRevision: publication.target.revision, targetDigest: publication.target.digest, profile: publication.target.profile } });
  const widget = (await f.call('canvas.widgets.create', { requestId: 'public_widget', appId: f.appId, entry: { domainId: alias.receipt.domainId, path: '/' } })).current;
  const launched = await f.launch({ widget });
  assert.equal(launched.result.entry.origin, alias.receipt.origin);
  assert.equal((await http(f.port, alias.receipt.origin, '/')).status, 200);
  assert.equal((await http(f.port, launched.origin, '/')).status, 401);
  assert.equal((await http(f.port, launched.origin, '/_soty/session', { method: 'DELETE' })).status, 401);
  assert.equal((await http(f.port, launched.origin, '/_soty/relay-identity-config')).status, 401);
});

test('actual Root WS: browser runtime origin is checked, upstream canonical origin retained, relock closes only its own stream', { timeout: 30000 }, async t => {
  const f = await fixture(t), w1 = await f.admit(f.W1), w2 = await f.admit(f.W2), observedOrigins = [];
  f.sample.server.on('upgrade', req => observedOrigins.push(req.headers.origin));
  const sockets = [];
  t.after(() => sockets.forEach(ws => ws.terminate()));
  async function socket(launch) {
    const ws = new WebSocket(`ws://127.0.0.1:${f.port}/live`, { headers: { host: new URL(launch.origin).host,
      origin: launch.origin, cookie: f.jar.get(launch.origin) } }); sockets.push(ws);
    await new Promise((resolve, reject) => { ws.once('message', resolve); ws.once('error', reject); }); return ws;
  }
  const left = await socket(w1), right = await socket(w2);
  assert.equal(observedOrigins.length === 2 && observedOrigins[0] === w1.result.entry.origin && observedOrigins[1] === w2.result.entry.origin,
    true, 'already-checked internal Host must not rewrite the upstream ordinary WS contract');
  const closed = new Promise(resolve => right.once('close', resolve)); await f.relock(f.W2); await closed;
  assert.equal(left.readyState, 1); assert.equal(right.readyState, 3);
  const echoed = new Promise((resolve, reject) => { left.once('message', resolve); left.once('error', reject); });
  left.send('own surviving W1'); assert.equal((await echoed).toString(), 'own surviving W1');
});

test('actual Root TLS admission: only issued nonexpired current runtime hosts qualify, never arbitrary known-widget hashes', { timeout: 30000 }, async t => {
  // Actual HTTP authority endpoint/SDK/Root/Widgets/Vault; this fixture makes no TLS handshake claim.
  const f = await fixture(t, { appScheme: 'https' }), first = await f.launch(f.W1), service = f.app.locals.appsService;
  const hostname = new URL(first.origin).hostname;
  assert.equal(service.allowsTlsDomain(hostname), true, 'fresh signed launch issued the host before its first TLS boot');
  const unknown = hostname.replace(/-[a-f0-9]{24}\./u, '-' + '0'.repeat(24) + '.');
  assert.equal((await http(f.port, f.origin, '/api/apps/tls-allow?domain=' + encodeURIComponent(hostname))).status, 204);
  assert.equal((await http(f.port, f.origin, '/api/apps/tls-allow?domain=' + encodeURIComponent(unknown))).status, 403);
  assert.equal(service.allowsTlsDomain(unknown), false, 'known widget identity does not authorize unbounded certificate addresses');
  const response = await http(f.port, first.origin, '/_soty/session', { method: 'POST', body: { ticket: first.url.hash.slice(1) } });
  assert.equal(response.status, 200); assert.equal(service.allowsTlsDomain(hostname), true, 'issued session remains after the consumed ticket');
  await f.relock(f.W1); assert.equal(service.allowsTlsDomain(hostname), false, 'retired runtime cannot keep dynamic TLS admission');
});
