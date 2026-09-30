import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { Worker } from 'node:worker_threads';
import WebSocket, { WebSocketServer } from 'ws';
import { createAppsService } from '../server/index.mjs';
import { createAppInspection } from '../server/inspection.mjs';
import { createDomainRegistry } from '../server/domains.mjs';
import { createPublicationRegistry } from '../server/publications.mjs';
import { createLocalAppsRuntime } from '../../../scripts/agent-modules/local-apps.mjs';

// Independent C1 fixture: actual service operations create all account/device/app
// records; a production connector talks to a deliberately unimpressive HTTP404
// process and real WS echo endpoint. A responding source must not become a claim
// that this process is a correct application. No author fixture is imported.
const owner = Object.freeze({ accountId: 'settings-owner', deviceId: 'settings-owner-browser' });
const member = Object.freeze({ accountId: 'settings-member', deviceId: 'settings-member-browser' });
const foreign = Object.freeze({ accountId: 'settings-foreign', deviceId: 'settings-foreign-browser' });
const community = 'settings-community';
const identity = Object.freeze({ linkId: 'settings-link', hostDeviceId: 'host:alpha', connectorId: 'connector:beta', name: 'C1 test source' });
const delay = ms => new Promise(done => setTimeout(done, ms));
const hash = value => createHash('sha256').update(value).digest('hex');
async function until(check, label, timeout = 3500) {
  const deadline = Date.now() + timeout;
  while (!check()) { if (Date.now() >= deadline) throw new Error(`Timeout: ${label}`); await delay(5); }
}
function rejectsCode(code) { return error => { assert.equal(error?.code, code); return true; }; }
const stableSettings = value => ({ app: value.app, publication: value.publication, addresses: value.addresses });

async function environment(t, { namedOnly = false, entryPath = '/board?tag=a%2Bb#item', expectObservation = true, grants = { accountIds: [member.accountId], communityIds: [community] } } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-settings-independent-'));
  const databasePath = join(directory, 'registry.sqlite');
  const connections = new Set(), clients = new Set(), peers = new Set(), services = [];
  const activeActors = new Set([owner.deviceId, member.deviceId, foreign.deviceId]);
  const admins = new Set([owner.accountId]), members = new Set([owner.accountId, member.accountId]);
  const heldProbes = new Set();
  let service, clock = 1_800_000_000_000, requestId = 0, holdProbes = false, failProbes = false, probeCount = 0;
  const captureConnections = server => server.on('connection', socket => { connections.add(socket); socket.once('close', () => connections.delete(socket)); });
  const upstream = createServer((req, res) => {
    if (req.method === 'HEAD') {
      probeCount++;
      if (failProbes) { req.socket.destroy(); return; }
      if (holdProbes) { heldProbes.add(res); res.once('close', () => heldProbes.delete(res)); return; }
    }
    res.writeHead(404, { 'content-type': 'text/plain' }); res.end('This endpoint responds but is not an application.');
  });
  captureConnections(upstream);
  const upstreamWs = new WebSocketServer({ server: upstream });
  upstreamWs.on('connection', socket => {
    peers.add(socket); socket.on('error', () => {}); socket.once('close', () => peers.delete(socket));
    socket.on('message', data => socket.send(data.toString()));
  });
  const gateway = createServer((req, res) => { if (!service?.handleRequest(req, res)) { res.writeHead(404); res.end('outside application router'); } });
  captureConnections(gateway);
  gateway.on('upgrade', (req, socket, head) => { if (!service?.handleUpgrade(req, socket, head)) socket.destroy(); });
  await Promise.all([new Promise(done => upstream.listen(0, '127.0.0.1', done)), new Promise(done => gateway.listen(0, '127.0.0.1', done))]);
  const gatewayPort = gateway.address().port, upstreamPort = upstream.address().port;
  const shellOrigin = `http://127.0.0.1:${gatewayPort}`;
  const token = randomBytes(32).toString('base64url');
  const config = {
    databasePath, shellOrigins: [shellOrigin, 'http://localhost:5171'],
    appOriginTemplate: namedOnly ? '' : `http://{appId}.legacy.localhost:${gatewayPort}`,
    namedAppZone: `http://named.localhost:${gatewayPort}`,
    now: () => clock,
    actorActive: actor => [owner, member, foreign].some(value => value.accountId === actor?.accountId && value.deviceId === actor?.deviceId) && activeActors.has(actor.deviceId),
    canAccessCommunity: (accountId, id) => id === community && members.has(accountId),
    isGroupAdmin: (accountId, id) => id === community && admins.has(accountId),
    activeCommunityIds: accountId => members.has(accountId) ? [community] : [],
    authenticateConnector: async auth => auth.linkId === identity.linkId && auth.deviceId === identity.hostDeviceId
      && auth.connectorId === identity.connectorId && auth.token === token,
    accessAuditMs: 25,
  };
  service = createAppsService(config); services.push(service);
  const runtime = createLocalAppsRuntime({
    randomSecret: () => randomBytes(32).toString('base64url'), digest: hash,
    createWebSocket: url => new WebSocket(url), httpRequest: request,
    encodeBase64: bytes => Buffer.from(bytes).toString('base64'), decodeBase64: value => Buffer.from(value, 'base64'), now: () => clock,
  }, { identity, token, serverUrl: shellOrigin });
  t.after(async () => {
    runtime.stop();
    for (const socket of clients) socket.terminate();
    for (const socket of peers) socket.terminate();
    for (const item of services) item.close();
    for (const socket of connections) socket.destroy();
    upstreamWs.close();
    await Promise.all([new Promise(done => gateway.close(done)), new Promise(done => upstream.close(done))]);
    assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.match(directory.split(/[\\/]/u).at(-1), /^soty-settings-independent-/u);
    await rm(directory, { recursive: true, force: true });
  });
  runtime.start(); await until(() => runtime.status().connected, 'production connector authenticated');
  const call = (op, args = {}, actor = owner, instance = service) => instance.execute({ op, args: { expectedAccountId: actor.accountId, ...args }, actor });
  const claim = await runtime.claim();
  call('apps.claim', { hostDeviceId: claim.hostDeviceId, connectorId: claim.connectorId, claimCode: claim.claimCode });
  const app = call('apps.register', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId,
    name: 'Private settings specimen', port: upstreamPort, entryPath, grants }).app;
  const inspect = (actor = owner, instance = service) => call('apps.inspect', { appId: app.id }, actor, instance);
  if (expectObservation) await until(() => inspect().source.observation.state === 'responding', 'real HTTP404 connector observation');
  function reserve(slug = 'first-address') {
    const before = inspect();
    const result = call('apps.domains.claim', { appId: app.id, slug, expectedDomainsRevision: before.addresses.revision, requestId: `claim-${++requestId}` });
    return inspect().addresses.aliases.find(item => item.id === result.receipt.domainId);
  }
  function publicationArgs({ policy = 'anyone', domains = [], listed = false } = {}) {
    const current = inspect();
    return { appId: app.id, requestId: `publication-${++requestId}`, expectedPolicyEpoch: current.publication.policyEpoch,
      expectedTargetRevision: current.source.revision, launchPolicy: policy, listed, activeDomainIds: domains.map(item => item.id),
      ...(policy === 'anyone' ? { exposureAck: { scope: 'whole-port', targetRevision: current.source.revision, targetDigest: current.source.digest, profile: current.source.profile } } : {}) };
  }
  const publish = options => call('apps.publication.update', publicationArgs(options));
  function otherService(overrides = {}) { const next = createAppsService({ ...config, ...overrides }); services.push(next); return next; }
  const http = (domain, path, { method = 'GET', body, cookie, origin = domain.origin } = {}) => new Promise((done, reject) => {
    const bytes = body === undefined ? undefined : Buffer.from(JSON.stringify(body));
    const req = request({ hostname: '127.0.0.1', port: gatewayPort, path, method,
      headers: { Host: new URL(domain.origin).host, Origin: origin,
        ...(cookie ? { Cookie: cookie } : {}), ...(bytes ? { 'Content-Type': 'application/json', 'Content-Length': bytes.length } : {}) } }, res => {
      const chunks = []; res.on('data', chunk => chunks.push(chunk)); res.once('end', () => done({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks).toString() }));
      res.once('aborted', () => reject(new Error('Unexpected truncated response')));
    });
    req.setTimeout(3000, () => req.destroy(new Error('HTTP test timeout'))); req.once('error', reject); req.end(bytes);
  });
  function launch(domain, actor = owner) { return call('apps.launch', { appId: app.id, domainId: domain.id }, actor); }
  async function exchange(domain, launched) {
    const result = await http(domain, '/_soty/session', { method: 'POST', body: { ticket: new URL(launched.launchUrl).hash.slice(1) } });
    assert.equal(result.status, 200);
    const cookie = result.headers['set-cookie']?.[0]?.split(';')[0]; assert.ok(cookie);
    return cookie;
  }
  async function websocket(domain, cookie) {
    const socket = new WebSocket(`ws://127.0.0.1:${gatewayPort}/echo`, { headers: { Host: new URL(domain.origin).host, Origin: domain.origin, Cookie: cookie } });
    clients.add(socket); socket.on('error', () => {}); socket.once('close', () => clients.delete(socket));
    await new Promise((done, reject) => { socket.once('open', done); socket.once('error', reject); }); return socket;
  }
  async function echo(socket, value) {
    return new Promise((done, reject) => {
      const timer = setTimeout(() => reject(new Error('Echo timeout')), 2500);
      socket.once('message', data => { clearTimeout(timer); done(data.toString()); }); socket.send(value);
    });
  }
  return { config, databasePath, shellOrigin, app, inspect, call, reserve, publish, publicationArgs, otherService, runtime,
    admins, members, activeActors, http, launch, exchange, websocket, echo,
    advance: milliseconds => { clock += milliseconds; }, time: () => clock, probeCount: () => probeCount,
    failProbes: () => { failProbes = true; },
    holdProbes: () => { holdProbes = true; }, releaseProbes: () => { holdProbes = false; for (const res of heldProbes) { res.writeHead(404); res.end(); } },
  };
}

test('C1 owner-only projection does not expose settings to a granted member, public visitor or wrong account', async t => {
  const f = await environment(t), address = f.reserve(); f.publish({ domains: [address] });
  const actual = f.inspect(); assert.equal(actual.app.name, 'Private settings specimen');
  assert.equal(actual.source.hostDeviceId, identity.hostDeviceId);
  assert.doesNotMatch(JSON.stringify(actual), /linkId|connectorKey|identity_json|runtimeReady|runtimeMode|exposureAck|__Host-|launchUrl/u);
  for (const actor of [member, foreign]) assert.throws(() => f.inspect(actor), rejectsCode('apps_owner_required'));
  assert.throws(() => f.call('apps.inspect', { appId: f.app.id, expectedAccountId: foreign.accountId }), rejectsCode('authentication_required'));
  f.activeActors.delete(owner.deviceId);
  assert.throws(() => f.inspect(), rejectsCode('apps_authentication_required'));
});

test('C1 real HTTP404 means responding, expires precisely at45s, and cannot survive a connector restart', async t => {
  const f = await environment(t), first = f.inspect(), initial = first.source.observation;
  assert.equal(first.checkedAt, f.time());
  assert.deepEqual(initial, { state: 'responding', observedAt: f.time(), freshUntil: f.time() + 45000, evidence: 'connector-v2-observation' });
  f.advance(44999); const nearlyExpired = f.inspect();
  assert.equal(nearlyExpired.source.observation.state, 'responding');
  assert.equal(nearlyExpired.checkedAt, f.time()); assert.equal(nearlyExpired.source.observation.freshUntil - nearlyExpired.checkedAt, 1);
  f.advance(1); const stale = f.inspect();
  assert.deepEqual(stale.source.observation, { ...initial, state: 'unknown' }); assert.equal(stale.checkedAt, f.time());
  f.runtime.stop(); await until(() => f.inspect().source.observation.state === 'offline', 'channel disconnect');
  assert.deepEqual(f.inspect().source.observation, { state: 'offline', observedAt: null, freshUntil: null, evidence: 'connector-offline' });
  f.holdProbes(); const oldCount = f.probeCount(); f.runtime.start();
  await until(() => f.probeCount() > oldCount, 'fresh connection probe held');
  assert.deepEqual(f.inspect().source.observation, { state: 'unknown', observedAt: null, freshUntil: null, evidence: 'not-observed' });
  f.releaseProbes(); await until(() => f.inspect().source.observation.state === 'responding', 'new observation');
  assert.equal(f.inspect().source.observation.observedAt, f.time());
});

test('C1 an actual failed process probe is unreachable rather than offline, and never removes the owner preview action', async t => {
  const f = await environment(t);
  f.runtime.stop(); await until(() => f.inspect().source.observation.state === 'offline', 'old connection closed');
  f.failProbes(); f.runtime.start(); await until(() => f.inspect().source.observation.state === 'unreachable', 'fresh probe connection failed');
  const current = f.inspect();
  assert.equal(current.source.observation.evidence, 'connector-v2-observation');
  assert.equal(current.source.observation.observedAt, f.time());
  assert.equal(current.actions.canPreview, true); assert.equal(current.actions.canPublish, true);
  f.advance(45000); assert.equal(f.inspect().source.observation.state, 'unknown');
});

test('C1 inspect preserves full private hash-SPA links, and public alias claim remains inactive until explicitly enabled', async t => {
  const f = await environment(t), a = f.reserve(), b = f.reserve('second-address');
  f.publish({ domains: [a] });
  const third = f.reserve('third-address'), current = f.inspect();
  const canonical = new URL(current.addresses.canonical.shareUrl);
  assert.equal(canonical.origin, f.shellOrigin);
  const query = canonical.hash.slice(canonical.hash.indexOf('?') + 1);
  assert.equal(new URLSearchParams(query).get('path'), '/board?tag=a%2Bb#item');
  assert.ok(canonical.hash.startsWith(`#launch/${f.app.id}/${current.addresses.canonical.id}?`));
  assert.equal(current.addresses.aliases.find(item => item.id === a.id).shareUrl, a.origin + '/board?tag=a%2Bb#item');
  for (const address of [b, third]) {
    const item = current.addresses.aliases.find(value => value.id === address.id);
    assert.equal(item.active, false); assert.equal(item.shareUrl, null);
    assert.equal((await f.http(item, '/')).status, 503);
  }
  f.publish({ policy: 'restricted', domains: [a] });
  const privateUrl = new URL(f.inspect().addresses.aliases.find(item => item.id === a.id).shareUrl);
  assert.equal(privateUrl.origin, f.shellOrigin);
  assert.equal(new URLSearchParams(privateUrl.hash.slice(privateUrl.hash.indexOf('?') + 1)).get('path'), '/board?tag=a%2Bb#item');
});

test('C1 named-only preview survives disabled new claims, while retained aliases do not grant a new claim', async t => {
  const f = await environment(t, { namedOnly: true, entryPath: '/#/dashboard' });
  assert.equal(f.inspect().addresses.canonical, null); assert.equal(f.inspect().actions.canPreview, false);
  const address = f.reserve(); f.publish({ policy: 'restricted', domains: [address] });
  assert.equal(f.inspect().actions.canPreview, true);
  const disabled = f.otherService({ namedAppZone: '' }), current = f.inspect(owner, disabled);
  assert.equal(current.addresses.claimOrigin, null); assert.equal(current.actions.canReserveName, false);
  assert.equal(current.actions.canPreview, true); assert.ok(current.addresses.aliases[0].shareUrl);
  assert.equal(new URLSearchParams(new URL(current.addresses.aliases[0].shareUrl).hash.split('?')[1]).get('path'), '/#/dashboard');
  assert.throws(() => f.call('apps.domains.claim', { appId: f.app.id, slug: 'disabled-claim', requestId: 'disabled-claim-id', expectedDomainsRevision: current.addresses.revision }, owner, disabled));
});

test('C1 CAS conflict changes neither grants, app revision nor publication epoch; omitted grants remain intact', async t => {
  const f = await environment(t), before = f.inspect();
  f.call('apps.update', { appId: f.app.id, expectedRevision: before.app.revision, name: 'Winning name' });
  const winner = stableSettings(f.inspect());
  assert.throws(() => f.call('apps.update', { appId: f.app.id, expectedRevision: before.app.revision,
    name: 'Losing name', grants: { accountIds: [], communityIds: [] } }), rejectsCode('app_revision_conflict'));
  assert.deepEqual(stableSettings(f.inspect()), winner);
  assert.deepEqual(winner.app.grants, before.app.grants); assert.equal(winner.publication.policyEpoch, before.publication.policyEpoch);
  for (const expectedRevision of [0, -1, 1.1, '1', null]) assert.throws(() => f.call('apps.update', { appId: f.app.id, expectedRevision, name: 'Never written' }), rejectsCode('invalid_app_revision'));
  assert.deepEqual(stableSettings(f.inspect()), winner);
});

test('C1 name-only save remains possible after losing group administration, but explicitly resubmitted grants require current authority', async t => {
  const f = await environment(t), before = f.inspect(); f.admins.delete(owner.accountId);
  f.call('apps.update', { appId: f.app.id, expectedRevision: before.app.revision, name: 'Renamed after role change' });
  const renamed = f.inspect(); assert.deepEqual(renamed.app.grants, before.app.grants);
  assert.equal(renamed.publication.policyEpoch, before.publication.policyEpoch);
  assert.throws(() => f.call('apps.update', { appId: f.app.id, expectedRevision: renamed.app.revision, name: 'Rejected', grants: renamed.app.grants }), rejectsCode('apps_community_admin_required'));
  assert.deepEqual(stableSettings(f.inspect()), stableSettings(renamed));
});

test('C1 two actual worker writers cannot overwrite one another using the same owner revision', { timeout: 15000 }, async t => {
  const f = await environment(t), before = f.inspect(), gate = new Int32Array(new SharedArrayBuffer(4));
  const intents = [
    { appId: f.app.id, expectedRevision: before.app.revision, name: 'Concurrent rename' },
    { appId: f.app.id, expectedRevision: before.app.revision, grants: { accountIds: [], communityIds: [] } },
  ];
  const jobs = intents.map(args => {
    const worker = new Worker(`
      const { parentPort, workerData } = require('node:worker_threads');
      (async () => {
        const { createAppsService } = await import(workerData.module);
        const service = createAppsService({ ...workerData.config, actorActive: () => true, isGroupAdmin: () => true });
        parentPort.postMessage({ ready: true });
        Atomics.wait(new Int32Array(workerData.gate), 0, 0);
        let result;
        try { service.execute({ op: 'apps.update', actor: workerData.actor, args: workerData.args }); result = { ok: true }; }
        catch (error) { result = { ok: false, code: error.code }; }
        finally { service.close(); }
        parentPort.postMessage(result);
      })().catch(error => { parentPort.postMessage({ fatal: error.message }); });
    `, { eval: true, workerData: {
      module: new URL('../server/index.mjs', import.meta.url).href, actor: owner, args, gate: gate.buffer,
      config: { databasePath: f.databasePath, shellOrigins: f.config.shellOrigins, appOriginTemplate: f.config.appOriginTemplate, namedAppZone: f.config.namedAppZone },
    } });
    t.after(() => worker.terminate());
    let readyDone, resultDone, readyFailed, resultFailed;
    const ready = new Promise((done, reject) => { readyDone = done; readyFailed = reject; });
    const result = new Promise((done, reject) => { resultDone = done; resultFailed = reject; });
    // Both promises are observed before waiting for the release barrier.
    const failure = error => { readyFailed(error); resultFailed(error); };
    result.catch(() => {});
    worker.on('error', failure); worker.on('message', value => { if (value.ready) readyDone(); else if (value.fatal) failure(new Error(value.fatal)); else resultDone(value); });
    return { ready, result };
  });
  await Promise.all(jobs.map(item => item.ready)); Atomics.store(gate, 0, 1); Atomics.notify(gate, 0, 2);
  const results = await Promise.all(jobs.map(item => item.result));
  assert.equal(results.filter(item => item.ok).length, 1);
  assert.equal(results.filter(item => item.code === 'app_revision_conflict').length, 1);
  const current = f.inspect(), winner = results.findIndex(item => item.ok);
  assert.equal(current.app.revision, before.app.revision + 1);
  assert.equal(current.app.name, winner === 0 ? 'Concurrent rename' : before.app.name);
  assert.deepEqual(current.app.grants, winner === 1 ? { accountIds: [], communityIds: [] } : before.app.grants);
  assert.equal(current.publication.policyEpoch, before.publication.policyEpoch + (winner === 1 ? 1 : 0));
});

test('C1 rename preserves an already issued ticket and live WS, while a real grants change closes access', async t => {
  const f = await environment(t), canonical = f.inspect().addresses.canonical;
  const ticket = f.launch(canonical, member), cookie = await f.exchange(canonical, f.launch(canonical, member));
  const socket = await f.websocket(canonical, cookie); assert.equal(await f.echo(socket, 'before-rename'), 'before-rename');
  f.call('apps.update', { appId: f.app.id, expectedRevision: f.inspect().app.revision, name: 'Name is not a permission' });
  assert.equal(await f.echo(socket, 'after-rename'), 'after-rename'); await f.exchange(canonical, ticket);
  f.call('apps.update', { appId: f.app.id, expectedRevision: f.inspect().app.revision, grants: { accountIds: [], communityIds: [] } });
  await until(() => socket.readyState === WebSocket.CLOSED, 'revoked member stream closes');
  assert.equal((await f.http(canonical, '/_soty/session', { cookie })).status, 403);
});

test('C1 revoked app remains inspectable by its owner but has no active action or share URL', async t => {
  const f = await environment(t), address = f.reserve(), pending = f.publicationArgs({ domains: [address] });
  f.call('apps.publication.update', pending);
  f.call('apps.revoke', { appId: f.app.id });
  const replay = f.call('apps.publication.update', pending);
  assert.equal(replay.replayed, true); assert.equal(replay.receipt.launchPolicy, 'anyone'); assert.equal(replay.current.appState, 'revoked');
  const current = f.inspect(); assert.equal(current.app.state, 'revoked');
  assert.ok(current.addresses.canonical.origin); assert.equal(current.addresses.canonical.shareUrl, null);
  assert.ok(current.addresses.aliases.every(item => item.shareUrl === null && !item.active));
  assert.ok(Object.values(current.actions).every(value => value === false));
  assert.deepEqual(current.source.observation, { state: 'unknown', observedAt: null, freshUntil: null, evidence: 'not-observed' });
});

test('C1 unsafe historical entry path cannot become a share link but does not block emergency restrict', async t => {
  const f = await environment(t, { entryPath: '/x/..//not-a-safe-launch', expectObservation: false }), address = f.reserve();
  f.publish({ domains: [address], listed: true });
  const current = f.inspect();
  assert.equal(f.runtime.status().connected, true);
  assert.equal(f.probeCount(), 0, 'an unsafe binding is refused before any upstream HEAD');
  assert.deepEqual(current.source.observation, { state: 'unknown', observedAt: null, freshUntil: null, evidence: 'not-observed' });
  assert.equal(current.source.entryPath, '/x/..//not-a-safe-launch');
  assert.equal(current.addresses.canonical.shareUrl, null); assert.equal(current.addresses.aliases[0].shareUrl, null);
  assert.equal(current.actions.canPreview, false); assert.equal(current.actions.canEdit, true); assert.equal(current.actions.canPublish, true);
  assert.equal((await f.http(address, '/')).status, 503);
  assert.equal(f.probeCount(), 0);
  f.publish({ policy: 'restricted', domains: [], listed: false });
  assert.equal(f.inspect().publication.launchPolicy, 'restricted'); assert.equal(f.inspect().publication.listed, false);
});

test('C1 claim success remains visible after a publication conflict, and retired aliases stay unavailable', async t => {
  const f = await environment(t), address = f.reserve(), stale = f.publicationArgs({ domains: [address] });
  f.call('apps.update', { appId: f.app.id, expectedRevision: f.inspect().app.revision, grants: { accountIds: [], communityIds: [] } });
  assert.throws(() => f.call('apps.publication.update', stale), rejectsCode('app_publication_revision_conflict'));
  let current = f.inspect(); assert.equal(current.addresses.aliases[0].state, 'bound'); assert.equal(current.addresses.aliases[0].active, false);
  f.call('apps.domains.retire', { appId: f.app.id, domainId: address.id, expectedDomainsRevision: current.addresses.revision, requestId: 'retire-after-partial' });
  current = f.inspect(); assert.equal(current.addresses.aliases[0].state, 'tombstone'); assert.equal(current.addresses.aliases[0].shareUrl, null);
  assert.equal((await f.http(address, '/')).status, 410);
});

test('C1 replay reports historical success separately from current restriction, and pruned intent cannot reapply itself', async t => {
  const f = await environment(t), address = f.reserve(), first = f.publicationArgs({ domains: [address] });
  const original = f.call('apps.publication.update', first);
  f.publish({ policy: 'restricted', domains: [address] });
  const replay = f.call('apps.publication.update', first);
  assert.equal(replay.replayed, true); assert.equal(replay.receipt.launchPolicy, 'anyone'); assert.equal(replay.current.launchPolicy, 'restricted');
  assert.equal(replay.receipt.policyEpoch, original.receipt.policyEpoch);
  for (let i = 0; i < 64; i++) f.publish({ policy: 'restricted', domains: [address] });
  const before = stableSettings(f.inspect());
  assert.throws(() => f.call('apps.publication.update', first), rejectsCode('app_publication_revision_conflict'));
  assert.deepEqual(stableSettings(f.inspect()), before);
});

test('C1 owner inspection sees one WAL snapshot despite a second service changing policy between subordinate reads', async t => {
  const f = await environment(t), address = f.reserve(), other = f.otherService();
  const before = f.inspect(), change = f.publicationArgs({ domains: [address] });
  const db = new DatabaseSync(f.databasePath); db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000');
  try {
  const assertActor = actor => { assert.equal(actor.accountId, owner.accountId); return true; };
  const publications = createPublicationRegistry({ db, assertActor, canUse: () => true });
  const domains = createDomainRegistry({ db, assertActor, legacyTemplate: f.config.appOriginTemplate, namedAppZone: f.config.namedAppZone,
    shellOrigins: f.config.shellOrigins, onRetireInTransaction: publications.retireInTransaction, onPolicyChanged: publications.notifyChanged });
  let mutated = false;
  const inspection = createAppInspection({ db, assertActor, publications, shellOrigin: f.shellOrigin,
    namedAppZone: f.config.namedAppZone, nameClaimsEnabled: true, inspectSource: () => null,
    domains: { execute(input) {
      if (!mutated) {
        mutated = true;
        f.call('apps.update', { appId: f.app.id, expectedRevision: before.app.revision, name: 'Newer snapshot name' }, owner, other);
        f.call('apps.domains.claim', { appId: f.app.id, slug: 'newer-snapshot-address', expectedDomainsRevision: before.addresses.revision, requestId: 'snapshot-second-alias' }, owner, other);
        f.call('apps.publication.update', change, owner, other);
      }
      return domains.execute(input);
    } },
  });
  const snapshot = inspection.read(owner, { appId: f.app.id });
  assert.equal(snapshot.publication.policyEpoch, before.publication.policyEpoch);
  assert.equal(snapshot.publication.launchPolicy, 'restricted'); assert.equal(snapshot.addresses.aliases[0].active, false);
  assert.deepEqual(snapshot.app, before.app); assert.deepEqual(snapshot.addresses, before.addresses);
  const after = f.inspect(); assert.equal(after.publication.launchPolicy, 'anyone'); assert.equal(after.addresses.aliases.find(item => item.id === address.id).active, true);
  assert.equal(after.app.name, 'Newer snapshot name'); assert.equal(after.addresses.aliases.length, before.addresses.aliases.length + 1);
  } finally { db.close(); }
});
