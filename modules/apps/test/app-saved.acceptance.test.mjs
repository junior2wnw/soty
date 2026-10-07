import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { spawn } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import WebSocket from 'ws';
import { createAppsService } from '../server/index.mjs';
import { createWorldService } from '../../world/server/index.mjs';

// Independent D1 fixture. A real WebSocket claim and service registration create
// apps; the connector is then disconnected. Saved authority must be independent
// of live HEAD/binding readiness. No author's fixture or current-DDL SQL seeding.
const owner = Object.freeze({ accountId: 'saved_owner', deviceId: 'saved_owner_browser', label: 'App author' });
const secondOwner = Object.freeze({ accountId: 'saved_owner_two', deviceId: 'saved_owner_two_browser', label: 'Second author' });
const thirdOwner = Object.freeze({ accountId: 'saved_owner_three', deviceId: 'saved_owner_three_browser', label: 'Third author' });
const reader = Object.freeze({ accountId: 'saved_reader', deviceId: 'saved_reader_browser', label: 'Reader' });
const readerPhone = Object.freeze({ ...reader, deviceId: 'saved_reader_phone' });
const stranger = Object.freeze({ accountId: 'saved_stranger', deviceId: 'saved_stranger_browser', label: 'Other account' });
const actors = [owner, secondOwner, thirdOwner, reader, readerPhone, stranger];
const actorKey = actor => JSON.stringify([actor.accountId, actor.deviceId]);
const hash = value => createHash('sha256').update(value).digest('hex');
const pause = ms => new Promise(done => setTimeout(done, ms));
const code = expected => error => { assert.equal(error?.code, expected); return true; };
async function until(check, label, timeout = 5000) {
  const end = Date.now() + timeout;
  do { const value = check(); if (value) return value; await pause(5); } while (Date.now() < end);
  assert.fail(`Timed out: ${label}`);
}
function safeEntry(entry) {
  assert.deepEqual(Object.keys(entry).sort(), ['appId', 'domainId', 'origin', 'path', 'label', 'savedRevision', 'updatedAt', 'current'].sort());
  if (entry.current) assert.deepEqual(Object.keys(entry.current).sort(), ['name', 'status', 'canManage'].sort());
  assert.doesNotMatch(JSON.stringify(entry), /connectorId|connectorKey|hostDeviceId|linkId|grants|bio|interests|launchUrl|claimCode|sessionCheck/u);
}

async function environment(t, { fence = true } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-saved-independent-'));
  const databasePath = join(directory, 'apps.sqlite'), worldPath = join(directory, 'world.sqlite');
  const world = createWorldService({ databasePath: worldPath, projectId: 'saved_acceptance' });
  let service, serial = 0, clock = 1_800_000_000_000;
  const active = new Set(actors.map(actorKey)), services = [], children = new Set(), sockets = new Set();
  const devices = new Map(), connectorTokens = new Map();
  const gateway = createServer((_req, res) => res.writeHead(404).end());
  gateway.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  gateway.on('upgrade', (req, socket, head) => { if (!service?.handleUpgrade(req, socket, head)) socket.destroy(); });
  await new Promise(done => gateway.listen(0, '127.0.0.1', done));
  const origin = `http://127.0.0.1:${gateway.address().port}`;
  const config = {
    databasePath, appOriginTemplate: `http://{appId}.legacy.localhost:${gateway.address().port}`,
    namedAppZone: `http://named.localhost:${gateway.address().port}`, shellOrigins: [origin], now: () => clock,
    actorActive: actor => active.has(actorKey(actor)),
    canAccessCommunity: (accountId, communityId) => world.canAccessCommunity(accountId, communityId),
    isGroupAdmin: (accountId, communityId) => world.isGroupAdmin(accountId, communityId),
    activeCommunityIds: accountId => world.activeCommunityIds(accountId),
    ...(fence ? { withAuthorityFence: callback => world.withCommunityAuthorityFence(callback) } : {}),
    authenticateConnector: async auth => connectorTokens.get(JSON.stringify([auth.linkId, auth.deviceId, auth.connectorId])) === auth.token,
  };
  function open(overrides = {}) { const instance = createAppsService({ ...config, ...overrides }); services.push(instance); return instance; }
  service = open();
  t.after(async () => {
    for (const child of children) if (child.exitCode === null) child.kill();
    await Promise.all([...children].map(child => child.finished));
    for (const instance of services) instance.close(); world.close();
    for (const socket of sockets) socket.destroy();
    await new Promise(done => gateway.close(done));
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-saved-independent-/u);
    await rm(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });
  const call = (op, args = {}, actor = owner, instance = service) => instance.execute({ op, args: { expectedAccountId: actor.accountId, ...args }, actor });
  async function device(actor = owner) {
    if (devices.has(actor.accountId)) return devices.get(actor.accountId);
    const identity = { linkId: `link_${actor.accountId}`, hostDeviceId: `host_${actor.accountId}`, connectorId: `connector_${actor.accountId}` };
    const token = randomBytes(32).toString('base64url'), claimCode = randomBytes(32).toString('base64url');
    connectorTokens.set(JSON.stringify([identity.linkId, identity.hostDeviceId, identity.connectorId]), token);
    const ws = new WebSocket(`${origin.replace('http:', 'ws:')}/api/apps/channel`), frames = [];
    ws.on('message', raw => frames.push(JSON.parse(raw.toString()))); ws.on('error', () => {});
    await new Promise((done, reject) => { ws.once('open', done); ws.once('error', reject); });
    ws.send(JSON.stringify({ type: 'auth', schema: 'soty.apps-channel.v1', ...identity, token, name: 'Private source label' }));
    await until(() => frames.some(frame => frame.type === 'ready'), 'connector authenticated');
    ws.send(JSON.stringify({ type: 'claim', claimDigest: hash(claimCode) }));
    await until(() => frames.some(frame => frame.type === 'claim-ready'), 'explicit claim acknowledged');
    call('apps.claim', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, claimCode }, actor);
    const closed = new Promise(done => ws.once('close', done)); ws.close(); await closed;
    await until(() => call('apps.devices', {}, actor).devices.every(value => !value.online), 'source disconnected');
    devices.set(actor.accountId, identity); return identity;
  }
  async function app({ actor = owner, name = 'Original saved title', grants = { accountIds: [reader.accountId] }, entryPath = '/initial' } = {}) {
    const identity = await device(actor);
    const registered = call('apps.register', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId,
      port: 12000 + ++serial, name, entryPath, grants }, actor).app;
    await until(() => call('apps.inspect', { appId: registered.id }, actor).source.observation.state === 'offline',
      'server processed the source close, not only the client close event');
    return registered;
  }
  const inspect = (appId, actor = owner) => call('apps.inspect', { appId }, actor);
  function alias(appId, slug, actor = owner) {
    const before = inspect(appId, actor);
    const result = call('apps.domains.claim', { appId, slug, requestId: `claim_${++serial}`, expectedDomainsRevision: before.addresses.revision }, actor);
    return inspect(appId, actor).addresses.aliases.find(value => value.id === result.receipt.domainId);
  }
  function publish(appId, aliases, policy = 'anyone', actor = owner) {
    const before = inspect(appId, actor);
    return call('apps.publication.update', { appId, requestId: `publish_${++serial}`, expectedPolicyEpoch: before.publication.policyEpoch,
      expectedTargetRevision: before.source.revision, activeDomainIds: aliases.map(value => value.id), launchPolicy: policy, listed: false,
      ...(policy === 'anyone' ? { exposureAck: { scope: 'whole-port', targetRevision: before.source.revision, targetDigest: before.source.digest, profile: before.source.profile } } : {}) }, actor);
  }
  function retire(appId, domainId, actor = owner) {
    return call('apps.domains.retire', { appId, domainId, expectedDomainsRevision: inspect(appId, actor).addresses.revision, requestId: `retire_${++serial}` }, actor);
  }
  const get = (appId, actor = reader, instance = service) => call('apps.saved.get', { appId }, actor, instance);
  const list = (args = {}, actor = reader, instance = service) => call('apps.saved.list', args, actor, instance);
  const saveArgs = (appId, extra = {}, actor = reader) => ({ appId, saved: true, expectedRevision: get(appId, actor).revision, requestId: `saved_${++serial}`, ...extra });
  const save = (appId, extra = {}, actor = reader, instance = service) => call('apps.saved.set', saveArgs(appId, extra, actor), actor, instance);
  const remove = (appId, actor = reader) => call('apps.saved.set', { appId, saved: false, expectedRevision: get(appId, actor).revision, requestId: `remove_${++serial}` }, actor);
  function sql(fn, file = databasePath) { const db = new DatabaseSync(file); try { return fn(db); } finally { db.close(); } }
  return { directory, databasePath, worldPath, origin, config, world, call, app, inspect, alias, publish, retire, get, list, saveArgs, save, remove, open, sql, children, active,
    reopen() { service.close(); service = open(); return service; }, advance(ms) { clock += ms; } };
}

test('D1 independent save keeps the exact public alias, query and fragment across another device and reopen', async t => {
  const f = await environment(t), app = await f.app({ grants: {} }), address = f.alias(app.id, 'public-a');
  f.publish(app.id, [address]);
  const path = '/board?tag=a%2Bb#item', intent = f.saveArgs(app.id, { domainId: address.id, path });
  assert.equal(f.get(app.id).entry, null);
  const accepted = f.call('apps.saved.set', intent, reader);
  assert.equal(accepted.replayed, false); assert.equal(accepted.receipt.saved, true);
  const entry = accepted.current.entry; safeEntry(entry);
  assert.equal(entry.domainId, address.id); assert.equal(entry.origin, address.origin); assert.equal(entry.path, path);
  assert.equal(entry.current.canManage, false); assert.equal(entry.label, 'Original saved title');
  assert.equal(f.get(app.id, readerPhone).revision, accepted.current.revision);
  assert.deepEqual(f.get(app.id, readerPhone).entry, entry);
  assert.deepEqual(f.get(app.id, stranger), { revision: 0, entry: null });
  f.reopen(); assert.deepEqual(f.get(app.id, readerPhone).entry, entry);
  assert.equal(f.sql(db => db.prepare('SELECT COUNT(*) AS n FROM profiles').get().n, f.worldPath), 0, 'saving must not create World profiles');
});

test('D1 exact entry authority does not fall back to public aliases, other apps or newly claimed inactive names', async t => {
  const f = await environment(t), appA = await f.app({ grants: {} }), appB = await f.app({ actor: secondOwner, grants: {} });
  const a = f.alias(appA.id, 'alpha'), b = f.alias(appB.id, 'bravo', secondOwner);
  f.publish(appA.id, [a]); f.publish(appB.id, [b], 'anyone', secondOwner);
  const inactive = f.alias(appA.id, 'not-published');
  for (const extra of [{}, { domainId: b.id }, { domainId: inactive.id }]) {
    assert.throws(() => f.save(appA.id, extra), code('app_unavailable'));
    assert.equal(f.get(appA.id).revision, 0);
  }
  f.save(appA.id, { domainId: a.id });
  f.publish(appA.id, [], 'restricted');
  const unavailable = f.get(appA.id).entry; safeEntry(unavailable); assert.equal(unavailable.current, null);
  assert.equal(unavailable.domainId, a.id); assert.equal(unavailable.origin, a.origin);
  assert.throws(() => f.save(appA.id, { domainId: a.id }), code('app_unavailable'));
});

test('D1 lost access freezes the personal label without exposing later private metadata; retired origin is never substituted', async t => {
  const f = await environment(t), app = await f.app({ grants: {} });
  const first = f.alias(app.id, 'first-entry'), other = f.alias(app.id, 'second-entry'); f.publish(app.id, [first, other]);
  f.save(app.id, { domainId: first.id, path: '/#/private-choice' });
  f.publish(app.id, [], 'restricted');
  f.call('apps.update', { appId: app.id, name: 'PRIVATE-NEW-NAME-NOT-FOR-READER' });
  const unavailable = f.get(app.id).entry; safeEntry(unavailable);
  assert.equal(unavailable.current, null); assert.equal(unavailable.label, 'Original saved title');
  assert.doesNotMatch(JSON.stringify(f.list()), /PRIVATE-NEW-NAME/u);
  f.publish(app.id, [first, other]); f.retire(app.id, first.id);
  const retired = f.get(app.id).entry; assert.equal(retired.current, null); assert.equal(retired.origin, first.origin);
  assert.notEqual(retired.origin, other.origin);
  f.call('apps.revoke', { appId: app.id });
  assert.equal(f.remove(app.id).current.entry, null);
  assert.equal(f.list().entries.length, 0);
});

test('D1 dynamic group authority is real World state; leaving or losing owner authority cannot refresh a saved entry', async t => {
  const f = await environment(t);
  const group = f.world.execute({ op: 'world.community.create', args: { requestId: 'saved_group', name: 'Original group', joinPolicy: 'open' }, actor: owner }).community;
  const communityId = group.communityId;
  f.world.execute({ op: 'world.membership.join', args: { communityId }, actor: reader });
  f.world.execute({ op: 'world.membership.join', args: { communityId }, actor: secondOwner });
  const app = await f.app({ grants: { communityIds: [communityId] } });
  assert.ok(f.save(app.id).current.entry.current);
  f.world.execute({ op: 'world.membership.leave', args: { communityId }, actor: reader });
  assert.equal(f.get(app.id).entry.current, null);
  assert.throws(() => f.save(app.id), code('app_unavailable'));
  f.world.execute({ op: 'world.membership.join', args: { communityId }, actor: reader });
  assert.ok(f.get(app.id).entry.current);
  f.world.execute({ op: 'world.membership.transfer', args: { communityId, profileId: secondOwner.accountId, requestId: 'transfer_saved_group' }, actor: owner });
  f.world.execute({ op: 'world.membership.role', args: { communityId, profileId: owner.accountId, role: 'member' }, actor: secondOwner });
  assert.equal(f.world.isGroupAdmin(owner.accountId, communityId), false);
  assert.equal(f.get(app.id).entry.current, null);
  assert.equal(f.remove(app.id).current.entry, null);
});

test('D1 lost ACK replays historical receipt separately from current removal and rejects changed intent', async t => {
  const f = await environment(t), app = await f.app();
  const intent = f.saveArgs(app.id, { path: '/first?x=1#read' }), accepted = f.call('apps.saved.set', intent, reader);
  const removed = f.remove(app.id); assert.equal(removed.current.entry, null);
  f.reopen();
  const replay = f.call('apps.saved.set', intent, readerPhone);
  assert.equal(replay.replayed, true); assert.deepEqual(replay.receipt, accepted.receipt);
  assert.equal(replay.current.entry, null); assert.equal(replay.current.revision, removed.current.revision);
  assert.throws(() => f.call('apps.saved.set', { ...intent, path: '/different' }, reader), code('apps_saved_request_conflict'));
  assert.equal(f.get(app.id).entry, null);
  assert.throws(() => f.call('apps.saved.set', { ...intent, requestId: 'new-stale-save' }, reader), code('apps_saved_revision_conflict'));
});

test('D1 quota200 does not block remove, exact replay or no-op, and pruned retries cannot resurrect removed entries', { timeout: 30000 }, async t => {
  const f = await environment(t), apps = [];
  for (let i = 0; i < 201; i++) apps.push(await f.app({ actor: [owner, secondOwner, thirdOwner][Math.floor(i / 80)], name: `Quota specimen ${i}` }));
  const firstIntent = f.saveArgs(apps[0].id); f.call('apps.saved.set', firstIntent, reader);
  for (const app of apps.slice(1, 200)) f.save(app.id);
  const head = f.get(apps[0].id).revision;
  assert.throws(() => f.save(apps[200].id), code('apps_saved_capacity'));
  assert.equal(f.get(apps[0].id).revision, head);
  const noOpIntent = f.saveArgs(apps[1].id), noOp = f.call('apps.saved.set', noOpIntent, reader);
  assert.equal(noOp.current.revision, head + 1, 'accepted no-op must still advance the account head');
  assert.equal(f.call('apps.saved.set', noOpIntent, readerPhone).replayed, true);
  f.remove(apps[0].id); f.save(apps[200].id);
  assert.equal(f.get(apps[0].id).entry, null);
  assert.throws(() => f.call('apps.saved.set', firstIntent, readerPhone), code('apps_saved_revision_conflict'));
  f.reopen();
  assert.throws(() => f.call('apps.saved.set', firstIntent, reader), code('apps_saved_revision_conflict'));
  let cursor, count = 0;
  do { const page = f.list({ limit: 50, ...(cursor ? { cursor } : {}) }); count += page.entries.length; cursor = page.nextCursor; } while (cursor);
  assert.equal(count, 200);
  assert.equal(f.get(apps[0].id).entry, null);
});

test('D1 cursor belongs to one account revision and long paths stay within the actual response byte budget', { timeout: 20000 }, async t => {
  const f = await environment(t), apps = [];
  const path = '/' + 'p'.repeat(8000) + '#item';
  for (let i = 0; i < 40; i++) { const app = await f.app({ name: `Page ${i}` }); apps.push(app); f.save(app.id, { path }); }
  const first = f.list({ limit: 50 });
  assert.ok(first.entries.length > 0 && first.entries.length < 40); assert.ok(first.nextCursor);
  assert.ok(Buffer.byteLength(JSON.stringify(first)) <= 256 * 1024);
  assert.deepEqual(first.entries.map(value => value.savedRevision), [...first.entries.map(value => value.savedRevision)].sort((a, b) => b - a));
  const second = f.list({ limit: 50, cursor: first.nextCursor });
  assert.equal(second.revision, first.revision);
  assert.equal(new Set([...first.entries, ...second.entries].map(value => value.appId)).size, 40);
  for (const entry of [...first.entries, ...second.entries]) safeEntry(entry);
  assert.throws(() => f.list({ cursor: first.nextCursor }, stranger), code('invalid_saved_cursor'));
  f.remove(apps[0].id);
  assert.throws(() => f.list({ cursor: first.nextCursor }), code('apps_saved_cursor_expired'));
  assert.throws(() => f.list({ limit: 51 }));
});

test('D1 actor spoofing, revoked installations and invalid entry paths leave the library unchanged', async t => {
  const f = await environment(t), app = await f.app();
  for (const path of ['https://external.example/', '//external.example/', '/%2fother', '/_soty/boot', '/x/../_soty/session', '/%5cother', '/bad\npath', '/' + 'я'.repeat(2000)]) {
    assert.throws(() => f.save(app.id, { path })); assert.equal(f.get(app.id).revision, 0);
  }
  assert.throws(() => f.call('apps.saved.get', { appId: app.id, expectedAccountId: stranger.accountId }, reader), code('authentication_required'));
  assert.throws(() => f.call('apps.saved.set', { ...f.saveArgs(app.id), accountId: stranger.accountId }, reader));
  assert.throws(() => f.call('apps.saved.set', { appId: app.id, saved: false, domainId: 'unexpected', expectedRevision: 0, requestId: 'bad_remove' }, reader));
  assert.equal(f.get(app.id).revision, 0);
  f.active.delete(actorKey(reader));
  assert.throws(() => f.get(app.id), code('apps_authentication_required'));
  assert.equal(f.get(app.id, readerPhone).revision, 0);
});

test('D1 missing, asynchronous or deferred host fence cannot silently admit new operations; legacy functions still work', async t => {
  const f = await environment(t, { fence: false }), app = await f.app();
  assert.equal(f.call('apps.list').apps.length, 1);
  for (const [op, args] of [['apps.saved.get', { appId: app.id }], ['apps.saved.list', {}],
    ['apps.saved.set', { appId: app.id, saved: true, expectedRevision: 0, requestId: 'missing_fence' }]]) {
    assert.throws(() => f.call(op, args, reader), code('apps_authority_fence_required'));
  }
  let called = 0;
  const asynchronous = f.open({ withAuthorityFence: async callback => { called++; return callback(); } });
  assert.throws(() => f.get(app.id, reader, asynchronous), code('apps_authority_fence_required'));
  assert.equal(called, 0);
  let captured;
  const deferred = f.open({ withAuthorityFence: callback => { captured = callback; return undefined; } });
  assert.throws(() => f.call('apps.saved.set', { appId: app.id, saved: true, expectedRevision: 0, requestId: 'delayed_fence' }, reader, deferred), code('apps_authority_fence_invalid'));
  assert.throws(() => captured(), code('apps_authority_fence_invalid'));
  assert.equal(f.sql(db => db.prepare('SELECT count(*) AS n FROM app_saved_heads').get().n), 0);
  const restored = f.open({ withAuthorityFence: callback => f.world.withCommunityAuthorityFence(callback) });
  assert.deepEqual(f.get(app.id, reader, restored), { revision: 0, entry: null });
});

test('D1 malformed post-commit host result cannot duplicate the saved effect on an exact retry', async t => {
  const f = await environment(t), app = await f.app();
  const intent = f.saveArgs(app.id);
  const malformed = f.open({ withAuthorityFence: callback => f.world.withCommunityAuthorityFence(() => {
    const result = callback(); return Promise.resolve(result);
  }) });
  // The known World fence detects an async callback result. Apps already committed;
  // this is unknown acknowledgement, not a fictional cross-database rollback.
  assert.throws(() => f.call('apps.saved.set', intent, reader, malformed), code('world_authority_callback_async'));
  const replay = f.call('apps.saved.set', intent, reader);
  assert.equal(replay.replayed, true); assert.equal(replay.receipt.revision, 1); assert.equal(replay.current.revision, 1);
  assert.equal(f.list().entries.length, 1);
});

const writerSource = String.raw`
  import { writeFileSync, existsSync } from 'node:fs';
  import { join } from 'node:path';
  const c = JSON.parse(process.argv[1]);
  const mark = (name, value) => writeFileSync(join(c.directory, name + '-' + c.tag), JSON.stringify(value));
  const { createWorldService } = await import(c.worldModule);
  const { createAppsService } = await import(c.appsModule);
  let world, apps;
  try {
    world = createWorldService({ databasePath: c.worldPath, projectId: 'saved_acceptance' });
    apps = createAppsService({ ...c.config, actorActive: actor => actor.accountId === c.actor.accountId && actor.deviceId === c.actor.deviceId,
      canAccessCommunity: (account, group) => world.canAccessCommunity(account, group), isGroupAdmin: (account, group) => world.isGroupAdmin(account, group),
      withAuthorityFence: callback => world.withCommunityAuthorityFence(callback) });
    mark('ready', {});
    const end = Date.now() + 5000;
    while (!existsSync(join(c.directory, 'start-writers'))) {
      if (Date.now() > end) throw new Error('start_timeout'); Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 5);
    }
    try { mark('done', { value: apps.execute({ op: 'apps.saved.set', args: c.args, actor: c.actor }) }); }
    catch (error) { mark('done', { error: { code: error.code, status: error.status } }); }
  } catch (error) { mark('failed', { code: error.code || error.message }); process.exitCode = 1; }
  finally { apps?.close(); world?.close(); }
`;

function writer(f, tag, actor, args) {
  const config = { databasePath: f.databasePath, appOriginTemplate: f.config.appOriginTemplate, namedAppZone: f.config.namedAppZone, shellOrigins: f.config.shellOrigins };
  const env = Object.fromEntries(['SystemRoot', 'WINDIR', 'TEMP', 'TMP'].filter(key => process.env[key] !== undefined).map(key => [key, process.env[key]]));
  const child = spawn(process.execPath, ['--input-type=module', '-e', writerSource, JSON.stringify({ config, directory: f.directory,
    worldPath: f.worldPath, tag, actor, args, worldModule: new URL('../../world/server/index.mjs', import.meta.url).href,
    appsModule: new URL('../server/index.mjs', import.meta.url).href })], { windowsHide: true, env, stdio: ['ignore', 'ignore', 'pipe'] });
  let stderr = ''; child.stderr.on('data', bytes => { stderr = (stderr + bytes.toString()).slice(-8000); });
  child.finished = new Promise((done, reject) => { child.once('error', reject); child.once('exit', status => done({ status, stderr })); });
  f.children.add(child); return child;
}
async function marker(f, name) {
  await until(() => existsSync(join(f.directory, name)), name);
  return JSON.parse(readFileSync(join(f.directory, name), 'utf8'));
}

function savedShape(f) {
  return f.sql(db => ({
    heads: db.prepare('SELECT count(*) AS n FROM app_saved_heads').get().n,
    entries: db.prepare('SELECT count(*) AS n FROM app_saved_entries').get().n,
    receipts: db.prepare('SELECT count(*) AS n FROM app_saved_receipts').get().n,
    revisions: db.prepare('SELECT revision FROM app_saved_heads ORDER BY account_id').all().map(row => row.revision),
  }));
}

test('D1 two real processes racing different apps with one account revision admit exactly one desired state', { timeout: 15000 }, async t => {
  const f = await environment(t), a = await f.app(), b = await f.app();
  // Capture each desired intent once. A known pre-commit busy response is
  // retryable, but never means the stale desired state passed the CAS.
  const args = [f.saveArgs(a.id), f.saveArgs(b.id)], actors = [reader, readerPhone];
  const original = JSON.stringify(args);
  const first = writer(f, 'a', actors[0], args[0]), second = writer(f, 'b', actors[1], args[1]);
  await Promise.all([marker(f, 'ready-a'), marker(f, 'ready-b')]); writeFileSync(join(f.directory, 'start-writers'), 'go');
  const outcomes = await Promise.all([marker(f, 'done-a'), marker(f, 'done-b')]);
  assert.equal(outcomes.filter(value => value.value).length, 1);
  assert.equal(outcomes.filter(value => value.error).length, 1);
  assert.equal((await first.finished).status, 0); assert.equal((await second.finished).status, 0);
  f.reopen(); assert.deepEqual(savedShape(f), { heads: 1, entries: 1, receipts: 1, revisions: [1] });
  const loser = outcomes.findIndex(value => value.error);
  let denied = outcomes[loser];
  if (denied.error.code === 'world_authority_busy') {
    // Both original writers have settled before a fresh real process retries
    // exactly the losing actor/body/requestId/expectedRevision, without a new
    // intent or a wider authority-fence deadline.
    const retry = writer(f, 'retry', actors[loser], args[loser]);
    await marker(f, 'ready-retry'); denied = await marker(f, 'done-retry');
    assert.equal((await retry.finished).status, 0);
  }
  assert.equal(JSON.stringify(args), original);
  assert.equal(denied.error?.code, 'apps_saved_revision_conflict');
  f.reopen(); assert.equal(f.list().revision, 1); assert.equal(f.list().entries.length, 1);
  assert.deepEqual(savedShape(f), { heads: 1, entries: 1, receipts: 1, revisions: [1] });
});

for (const heldFile of ['world', 'apps']) test('D1 known ' + heldFile + ' contention leaves no effect and the same stale intent still conflicts in a fresh process', { timeout: 15000 }, async t => {
  const f = await environment(t), a = await f.app(), b = await f.app();
  const winner = f.saveArgs(a.id), loser = f.saveArgs(b.id), original = JSON.stringify(loser);
  const holder = new DatabaseSync(heldFile === 'world' ? f.worldPath : f.databasePath);
  holder.exec('BEGIN IMMEDIATE');
  try { assert.throws(() => f.call('apps.saved.set', loser, readerPhone), code(heldFile === 'world' ? 'world_authority_busy' : 'apps_saved_busy')); }
  finally { holder.exec('ROLLBACK'); holder.close(); }
  assert.deepEqual(savedShape(f), { heads: 0, entries: 0, receipts: 0, revisions: [] });
  assert.equal(f.call('apps.saved.set', winner, reader).current.revision, 1);
  assert.deepEqual(savedShape(f), { heads: 1, entries: 1, receipts: 1, revisions: [1] });
  const retry = writer(f, 'retry', readerPhone, loser);
  await marker(f, 'ready-retry'); writeFileSync(join(f.directory, 'start-writers'), 'go');
  const denied = await marker(f, 'done-retry');
  assert.equal((await retry.finished).status, 0);
  assert.equal(JSON.stringify(loser), original);
  assert.equal(denied.error?.code, 'apps_saved_revision_conflict');
  f.reopen(); assert.deepEqual(savedShape(f), { heads: 1, entries: 1, receipts: 1, revisions: [1] });
});
