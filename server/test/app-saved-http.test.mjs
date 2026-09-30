import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { spawn } from 'node:child_process';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import WebSocket from 'ws';
import { createHttpApp } from '../http-app.js';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';

// Production HTTP composition and independently enrolled signed clients. Only
// client IDB is replaced with a memory adapter. No test registry or auth bypass
// stands in for Connect. Interposed callbacks below retain the real World fence.
const pause = ms => new Promise(done => setTimeout(done, ms));
const sleepWord = new Int32Array(new SharedArrayBuffer(4));
const code = expected => error => { assert.equal(error?.code, expected); return true; };
async function until(check, label, timeout = 5000) {
  const deadline = Date.now() + timeout;
  do { const value = await check(); if (value) return value; await pause(5); } while (Date.now() < deadline);
  assert.fail(`Timed out: ${label}`);
}
function storage() {
  let value = null;
  return {
    async read() { return structuredClone(value); },
    async claim(candidate) { if (!value) value = structuredClone(candidate); return structuredClone(value); },
    async compareAndSwap(revision, candidate) {
      assert.equal(value.localRevision, revision); value = structuredClone(candidate); return structuredClone(value);
    },
  };
}
async function environment(t) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-saved-http-independent-'));
  const dataDir = join(directory, 'data'), dist = join(directory, 'dist');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Independent saved API test</title>');
  let app, serial = 0;
  const sockets = new Set(), clients = [], children = new Set();
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const port = server.address().port, origin = `http://127.0.0.1:${port}`;
  app = createHttpApp(dist, { dataDir, connectOrigins: [origin], appOriginTemplate: `http://{appId}.legacy.localhost:${port}`,
    namedAppZone: `http://named.localhost:${port}` });
  server.on('upgrade', (req, socket, head) => { if (!app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  t.after(async () => {
    for (const child of children) if (child.exitCode === null) child.kill();
    await Promise.all([...children].map(child => child.finished));
    for (const client of clients) client.dispose(); await app.locals.closeServices();
    for (const socket of sockets) socket.destroy(); await new Promise(done => server.close(done));
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-saved-http-independent-/u);
    await rm(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });
  function client() {
    const value = createClientWithStorage({ projectId: 'soty', endpoint: `${origin}/api/connect/rpc`,
      fetch: (url, options) => fetch(url, { ...options, headers: { ...options.headers, origin } }),
    }, storage()); clients.push(value); return value;
  }
  const owner = client(), ownerAccount = await owner.bootstrap('Saved owner');
  const reader = client(), readerAccount = await reader.bootstrap('Saved reader');
  const foreign = client(), foreignAccount = await foreign.bootstrap('Foreign reader');
  const identity = { linkId: 'saved_http_link_1234567890123456789012345678', hostDeviceId: 'saved_http_host', connectorId: 'saved_http_connector' };
  const token = randomBytes(32).toString('base64url'), claimCode = randomBytes(32).toString('base64url');
  const registered = await fetch(`${origin}/api/connectors/register`, { method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
    body: JSON.stringify({ linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, scope: 'Dev', protocol: 2, capabilities: ['apps'] }) });
  assert.equal((await registered.json()).ok, true);
  const ws = new WebSocket(`${origin.replace('http:', 'ws:')}/api/apps/channel`), frames = [];
  ws.on('message', raw => frames.push(JSON.parse(raw.toString()))); ws.on('error', () => {});
  await new Promise((done, reject) => { ws.once('open', done); ws.once('error', reject); });
  ws.send(JSON.stringify({ type: 'auth', schema: 'soty.apps-channel.v1', ...identity, token, name: 'Owner private device' }));
  await until(() => frames.some(frame => frame.type === 'ready'), 'real connector auth');
  ws.send(JSON.stringify({ type: 'claim', claimDigest: createHash('sha256').update(claimCode).digest('hex') }));
  await until(() => frames.some(frame => frame.type === 'claim-ready'), 'claim receipt');
  const ids = { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId };
  await owner.extension('apps.claim', { ...ids, claimCode });
  const closed = new Promise(done => ws.once('close', done)); ws.close(); await closed;
  async function create(grants = {}) {
    return (await owner.extension('apps.register', { ...ids, port: 21000 + ++serial, name: 'Remember this title', entryPath: '/initial', grants })).app;
  }
  async function named(appId) {
    let snapshot = await owner.extension('apps.inspect', { appId });
    const claimed = await owner.extension('apps.domains.claim', { appId, slug: `saved-${++serial}`, requestId: `claim_${serial}`,
      expectedDomainsRevision: snapshot.addresses.revision });
    snapshot = await owner.extension('apps.inspect', { appId });
    const address = snapshot.addresses.aliases.find(value => value.id === claimed.receipt.domainId);
    await owner.extension('apps.publication.update', { appId, requestId: `publish_${++serial}`, expectedPolicyEpoch: snapshot.publication.policyEpoch,
      expectedTargetRevision: snapshot.source.revision, activeDomainIds: [address.id], launchPolicy: 'anyone', listed: false,
      exposureAck: { scope: 'whole-port', targetRevision: snapshot.source.revision, targetDigest: snapshot.source.digest, profile: snapshot.source.profile } });
    return address;
  }
  const paths = { connect: join(dataDir, 'connect', 'accounts.sqlite'), world: join(dataDir, 'world', 'world.sqlite'), apps: join(dataDir, 'apps', 'registry.sqlite') };
  function inspect(file, fn) { const db = new DatabaseSync(file, { readOnly: true }); try { return fn(db); } finally { db.close(); } }
  return { directory, dataDir, paths, app, owner, reader, foreign, ownerAccount, readerAccount, foreignAccount, create, named, inspect, children,
    async secondReader() {
      const phone = client(), invitation = await phone.startEnrollment('Second saved device');
      await reader.approveEnrollment(invitation.requestId, readerAccount.accountId);
      await phone.previewEnrollment(invitation.requestId);
      const joined = await phone.finishEnrollment(invitation.requestId, readerAccount.accountId);
      assert.equal(joined.accountId, readerAccount.accountId); return phone;
    },
  };
}

test('D1 production signed save works before World profile provisioning and a second enrolled device restores the same entry', { timeout: 20000 }, async t => {
  const f = await environment(t), app = await f.create(), address = await f.named(app.id);
  assert.equal(f.inspect(f.paths.world, db => db.prepare('SELECT COUNT(*) AS n FROM profiles').get().n), 0);
  await assert.rejects(f.reader.extension('apps.saved.set', { appId: app.id, saved: true, expectedRevision: 0, requestId: 'private_canonical_denied' }), error => {
    assert.equal(error.code, 'app_unavailable'); assert.equal(error.status, 400); return true;
  });
  const pending = { expectedAccountId: f.readerAccount.accountId, appId: app.id, saved: true, expectedRevision: 0,
    requestId: 'signed_exact_entry', domainId: address.id, path: '/#/dashboard?tag=a%2Bb' };
  const accepted = await f.reader.extension('apps.saved.set', pending);
  assert.equal(accepted.current.entry.origin, address.origin); assert.equal(accepted.current.entry.path, pending.path);
  assert.doesNotMatch(JSON.stringify(accepted), /Owner private device|connectorId|hostDeviceId|grants|bio|interests|claimCode/u);
  const phone = await f.secondReader();
  assert.deepEqual(await phone.extension('apps.saved.get', { expectedAccountId: f.readerAccount.accountId, appId: app.id }), { ok: true, ...accepted.current });
  assert.deepEqual(await f.foreign.extension('apps.saved.get', { appId: app.id }), { ok: true, revision: 0, entry: null });
  await assert.rejects(f.reader.extension('apps.saved.get', { expectedAccountId: f.foreignAccount.accountId, appId: app.id }), error => {
    assert.equal(error.code, 'authentication_required'); assert.equal(error.status, 400); return true;
  });
  assert.equal(f.inspect(f.paths.world, db => db.prepare('SELECT COUNT(*) AS n FROM profiles').get().n), 0);
});

test('D1 signed composition holds Connect then World before Apps; errors before or after Apps COMMIT have distinct retry effects', { timeout: 20000 }, async t => {
  const f = await environment(t), app = await f.create({ accountIds: [f.readerAccount.accountId] });
  const world = f.app.locals.worldService, original = world.withCommunityAuthorityFence;
  let checked = 0;
  function writerAvailable(file) {
    const db = new DatabaseSync(file);
    try { db.exec('PRAGMA busy_timeout=1; BEGIN IMMEDIATE; ROLLBACK'); return true; }
    catch (error) { assert.equal(Number(error.errcode) & 255, 5); return false; }
    finally { db.close(); }
  }
  world.withCommunityAuthorityFence = callback => original(() => {
    checked++;
    assert.equal(writerAvailable(f.paths.connect), false, 'verified actor transaction is still held');
    assert.equal(writerAvailable(f.paths.world), false, 'World authority is held before Apps');
    assert.equal(writerAvailable(f.paths.apps), true, 'the saved registry has not taken its writer lock yet');
    return callback();
  });
  t.after(() => { world.withCommunityAuthorityFence = original; });
  const intent = { appId: app.id, saved: true, expectedRevision: 0, requestId: 'signed_after_commit' };
  assert.equal((await f.reader.extension('apps.saved.get', { appId: app.id })).revision, 0);
  assert.ok(checked > 0);
  world.withCommunityAuthorityFence = callback => original(() => { throw new Error('synthetic failure before Apps call'); });
  await assert.rejects(f.reader.extension('apps.saved.set', intent));
  world.withCommunityAuthorityFence = original;
  assert.deepEqual(await f.reader.extension('apps.saved.get', { appId: app.id }), { ok: true, revision: 0, entry: null });
  world.withCommunityAuthorityFence = callback => original(() => { callback(); throw new Error('synthetic lost result after Apps commit'); });
  await assert.rejects(f.reader.extension('apps.saved.set', intent));
  world.withCommunityAuthorityFence = original;
  const replay = await f.reader.extension('apps.saved.set', intent);
  assert.equal(replay.replayed, true); assert.equal(replay.receipt.revision, 1); assert.equal(replay.current.revision, 1);
  const removed = await f.reader.extension('apps.saved.set', { appId: app.id, saved: false, expectedRevision: 1, requestId: 'signed_remove' });
  assert.equal(removed.current.entry, null);
  const staleAck = await f.reader.extension('apps.saved.set', intent);
  assert.equal(staleAck.receipt.saved, true); assert.equal(staleAck.current.entry, null); assert.equal(staleAck.current.revision, 2);
});

test('D1 occupied Apps writer returns a bounded busy refusal, releases World authority and admits the same intent after release', { timeout: 20000 }, async t => {
  const f = await environment(t), app = await f.create({ accountIds: [f.readerAccount.accountId] });
  const blocker = new DatabaseSync(f.paths.apps);
  const pending = { appId: app.id, saved: true, expectedRevision: 0, requestId: 'retry_after_apps_busy' };
  const dataBytes = () => [f.paths.apps, f.paths.apps + '-wal'].map(file => existsSync(file) ? readFileSync(file).toString('base64') : null);
  let blocked = true;
  blocker.exec('BEGIN IMMEDIATE'); const before = dataBytes();
  try {
    await assert.rejects(f.reader.extension('apps.saved.set', pending), error => {
      assert.equal(error.code, 'apps_saved_busy'); assert.equal(error.status, 503); return true;
    });
    assert.deepEqual(dataBytes(), before, 'failed admission added no Apps pages or WAL frames');
    const independentWorld = new DatabaseSync(f.paths.world);
    try { independentWorld.exec('PRAGMA busy_timeout=1; BEGIN IMMEDIATE; ROLLBACK'); }
    finally { independentWorld.close(); }
    blocker.exec('ROLLBACK'); blocked = false;
  } finally { if (blocked) blocker.exec('ROLLBACK'); blocker.close(); }
  assert.deepEqual(await f.reader.extension('apps.saved.get', { appId: app.id }), { ok: true, revision: 0, entry: null });
  const accepted = await f.reader.extension('apps.saved.set', pending);
  assert.equal(accepted.replayed, false); assert.equal(accepted.current.revision, 1);
  assert.equal((await f.reader.extension('apps.saved.set', pending)).replayed, true);
});

test('D1 occupied World authority is a transient signed HTTP refusal and performs no Apps write', { timeout: 20000 }, async t => {
  const f = await environment(t), app = await f.create({ accountIds: [f.readerAccount.accountId] });
  const blocker = new DatabaseSync(f.paths.world), intent = { appId: app.id, saved: true, expectedRevision: 0, requestId: 'retry_after_world_busy' };
  blocker.exec('BEGIN IMMEDIATE');
  try {
    await assert.rejects(f.reader.extension('apps.saved.set', intent), error => {
      assert.equal(error.code, 'world_authority_busy'); assert.equal(error.status, 503); return true;
    });
    assert.equal(f.inspect(f.paths.apps, db => db.prepare('SELECT count(*) AS n FROM app_saved_heads').get().n), 0);
  } finally { blocker.exec('ROLLBACK'); blocker.close(); }
  const accepted = await f.reader.extension('apps.saved.set', intent);
  assert.equal(accepted.replayed, false); assert.equal(accepted.current.revision, 1);
});

const membershipWriter = String.raw`
  import { existsSync, writeFileSync } from 'node:fs';
  import { join } from 'node:path';
  const c = JSON.parse(process.argv[1]), mark = (name, value = {}) => writeFileSync(join(c.directory, name), JSON.stringify(value));
  const { createWorldService } = await import(c.worldModule); let world;
  try {
    world = createWorldService({ databasePath: c.path, projectId: 'soty' }); mark('writer-ready');
    const end = Date.now() + 8000;
    while (!existsSync(join(c.directory, 'writer-start'))) {
      if (Date.now() > end) throw new Error('start_timeout'); Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 5);
    }
    mark('writer-attempt');
    world.execute({ op: 'world.membership.remove', args: { communityId: c.communityId, profileId: c.memberId }, actor: c.owner });
    mark('writer-done', { allowed: world.canAccessCommunity(c.memberId, c.communityId) });
  } catch (error) { mark('writer-failed', { code: error.code || error.message }); process.exitCode = 1; }
  finally { world?.close(); }
`;
function startWriter(f, communityId) {
  const env = Object.fromEntries(['SystemRoot', 'WINDIR', 'TEMP', 'TMP'].filter(key => process.env[key] !== undefined).map(key => [key, process.env[key]]));
  const child = spawn(process.execPath, ['--input-type=module', '-e', membershipWriter, JSON.stringify({ directory: f.directory,
    path: f.paths.world, communityId, owner: { accountId: f.ownerAccount.accountId, deviceId: f.ownerAccount.deviceId, label: 'Saved owner' },
    memberId: f.readerAccount.accountId, worldModule: new URL('../../modules/world/server/index.mjs', import.meta.url).href })],
  { windowsHide: true, env, stdio: ['ignore', 'ignore', 'pipe'] });
  let stderr = ''; child.stderr.on('data', bytes => { stderr = (stderr + bytes.toString()).slice(-8000); });
  child.finished = new Promise((done, reject) => { child.once('error', reject); child.once('exit', status => done({ status, stderr })); });
  f.children.add(child); return child;
}

test('D1 actual signed Apps save and second-process World removal cannot interleave authority and commit', { timeout: 20000 }, async t => {
  const f = await environment(t);
  const created = await f.owner.extension('world.community.create', { requestId: 'signed_saved_group', name: 'Shared entry', joinPolicy: 'open' });
  const communityId = created.community.communityId;
  await f.reader.extension('world.membership.join', { communityId });
  const app = await f.create({ communityIds: [communityId] });
  const child = startWriter(f, communityId);
  await until(() => existsSync(join(f.directory, 'writer-ready')), 'independent World process ready');
  const world = f.app.locals.worldService, original = world.canAccessCommunity;
  let held = false;
  world.canAccessCommunity = (accountId, id) => {
    if (!held && accountId === f.readerAccount.accountId && id === communityId) {
      held = true; writeFileSync(join(f.directory, 'writer-start'), 'start');
      const end = Date.now() + 3000;
      while (!existsSync(join(f.directory, 'writer-attempt'))) {
        if (Date.now() > end) assert.fail('world writer never reached the real mutation');
        Atomics.wait(sleepWord, 0, 0, 5);
      }
      Atomics.wait(sleepWord, 0, 0, 180);
      assert.equal(existsSync(join(f.directory, 'writer-done')), false, 'membership writer must wait while Apps authorizes and commits');
    }
    return original(accountId, id);
  };
  t.after(() => { world.canAccessCommunity = original; });
  const intent = { appId: app.id, saved: true, expectedRevision: 0, requestId: 'signed_concurrent_save' };
  const accepted = await f.reader.extension('apps.saved.set', intent);
  assert.equal(held, true); assert.equal(accepted.receipt.saved, true);
  assert.equal((await child.finished).status, 0);
  assert.deepEqual(JSON.parse(readFileSync(join(f.directory, 'writer-done'), 'utf8')), { allowed: false });
  const current = await f.reader.extension('apps.saved.get', { appId: app.id });
  assert.equal(current.entry.current, null);
  await assert.rejects(f.reader.extension('apps.saved.set', { ...intent, expectedRevision: 1, requestId: 'after_real_revoke' }), code('app_unavailable'));
  const replay = await f.reader.extension('apps.saved.set', intent);
  assert.equal(replay.replayed, true); assert.equal(replay.current.entry.current, null);
});
