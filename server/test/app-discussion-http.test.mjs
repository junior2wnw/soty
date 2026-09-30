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
async function entry(f, appId, address) {
  const chosen = address ?? (await f.owner.extension('apps.inspect', { appId })).addresses.canonical;
  return { appId, domainId: chosen.id, path: '/discussion?tag=a%2Bb#message' };
}
const context = (client, scope) => client.extension('apps.discussion.context', scope);
const sendArgs = (scope, conversationId, requestId, body) => ({ ...scope, conversationId, requestId, body });

test('D2 real signed first-use discussion preserves exact public entry without World provisioning or source readiness', { timeout: 20000 }, async t => {
  const f = await environment(t), app = await f.create(), address = await f.named(app.id), scope = await entry(f, app.id, address);
  const profileCount = () => f.inspect(f.paths.world, db => db.prepare('SELECT count(*) AS n FROM profiles').get().n);
  assert.equal(profileCount(), 0);
  await assert.rejects(context(f.reader, { appId: app.id }), error => { assert.equal(error.status, 400); return true; });
  const initial = await context(f.reader, scope); assert.equal(initial.context.audience, 'public'); assert.deepEqual(initial.messages, []);
  const intent = { ...sendArgs(scope, initial.context.conversationId, 'signed-native-first', 'Message from a new Connect account'), expectedAccountId: f.readerAccount.accountId };
  const accepted = await f.reader.extension('apps.discussion.send', intent);
  assert.equal(accepted.replayed, false); assert.equal(accepted.message.author.accountId, f.readerAccount.accountId);
  assert.equal(accepted.message.author.label, 'Discussion reader');
  const phone = await f.secondReader();
  assert.equal((await context(phone, scope)).messages[0].id, accepted.receipt.id);
  assert.deepEqual((await phone.extension('apps.discussion.send', intent)).receipt, accepted.receipt);
  await assert.rejects(f.reader.extension('apps.discussion.send', { ...intent, expectedAccountId: f.foreignAccount.accountId }), error => {
    assert.equal(error.code, 'authentication_required'); assert.equal(error.status, 400); return true;
  });
  assert.equal(profileCount(), 0);
  assert.doesNotMatch(JSON.stringify(await context(f.reader, scope)), /Owner private device|connectorId|hostDeviceId|bio|interests|communityIds|grants|claimCode/u);
});

test('D2 signed lost result after Apps commit replays the own receipt after closure without returning private history', { timeout: 20000 }, async t => {
  const f = await environment(t), app = await f.create({ accountIds: [f.readerAccount.accountId] }), scope = await entry(f, app.id);
  const initial = await context(f.reader, scope), intent = sendArgs(scope, initial.context.conversationId, 'signed-loss-after-commit', 'Keep this body private');
  const world = f.app.locals.worldService, original = world.withCommunityAuthorityFence;
  function writerAvailable(file) {
    const db = new DatabaseSync(file); try { db.exec('PRAGMA busy_timeout=1; BEGIN IMMEDIATE; ROLLBACK'); return true; }
    catch (error) { assert.equal(Number(error.errcode) & 255, 5); return false; } finally { db.close(); }
  }
  let entered = false;
  world.withCommunityAuthorityFence = callback => original(() => {
    entered = true;
    assert.equal(writerAvailable(f.paths.connect), false); assert.equal(writerAvailable(f.paths.world), false);
    assert.equal(writerAvailable(f.paths.apps), true, 'D2 takes Apps only after the authority fence');
    callback(); throw new Error('Synthetic response loss after committed discussion send');
  });
  try { await assert.rejects(f.reader.extension('apps.discussion.send', intent)); }
  finally { world.withCommunityAuthorityFence = original; }
  assert.equal(entered, true);
  await f.owner.extension('apps.revoke', { appId: app.id });
  const replay = await f.reader.extension('apps.discussion.send', intent);
  assert.equal(replay.replayed, true); assert.equal(replay.message, null); assert.equal(replay.ownCurrent.removed, false);
  assert.doesNotMatch(JSON.stringify(replay), /Keep this body private|messages|currentConversation/u);
  await assert.rejects(f.reader.extension('apps.discussion.send', { ...intent, body: 'Changed on retry' }), error => {
    assert.equal(error.code, 'apps_discussion_request_conflict'); assert.equal(error.status, 400); return true;
  });
  const administrative = await context(f.owner, { appId: app.id, conversationId: initial.context.conversationId, administrative: true });
  assert.equal(administrative.context.entry, null); assert.equal(administrative.context.canPost, false); assert.equal(administrative.messages.length, 1);
  await f.reader.extension('apps.discussion.remove', { appId: app.id, conversationId: initial.context.conversationId, messageId: replay.receipt.id });
  const removed = await f.reader.extension('apps.discussion.send', intent);
  assert.deepEqual(removed.receipt, replay.receipt); assert.equal(removed.ownCurrent.removed, true);
});

test('D2 real signed Apps and World busy refusals are503 with no Apps effect and exact retry after release', { timeout: 20000 }, async t => {
  const f = await environment(t), app = await f.create({ accountIds: [f.readerAccount.accountId] }), scope = await entry(f, app.id);
  const conversationId = (await context(f.reader, scope)).context.conversationId;
  for (const [which, expectedCode] of [['apps', 'apps_discussion_busy'], ['world', 'world_authority_busy']]) {
    const intent = sendArgs(scope, conversationId, `signed-busy-${which}`, `After ${which} is released`);
    const blocker = new DatabaseSync(f.paths[which]); blocker.exec('BEGIN IMMEDIATE');
    const bytes = () => [f.paths.apps, f.paths.apps + '-wal'].map(path => existsSync(path) ? readFileSync(path).toString('base64') : null);
    const before = bytes();
    try {
      await assert.rejects(f.reader.extension('apps.discussion.send', intent), error => {
        assert.equal(error.code, expectedCode); assert.equal(error.status, 503); return true;
      });
      assert.deepEqual(bytes(), before, 'failed admission must not add message/counter/WAL writes');
      if (which === 'apps') {
        const probe = new DatabaseSync(f.paths.world); try { probe.exec('PRAGMA busy_timeout=1; BEGIN IMMEDIATE; ROLLBACK'); } finally { probe.close(); }
      }
    } finally { blocker.exec('ROLLBACK'); blocker.close(); }
    const accepted = await f.reader.extension('apps.discussion.send', intent);
    assert.equal(accepted.replayed, false); assert.equal((await f.reader.extension('apps.discussion.send', intent)).replayed, true);
  }
  assert.equal((await context(f.reader, scope)).messages.length, 2);
});

test('D2 signed rate refusal is429, while replay, own erase and emergency restriction remain usable', { timeout: 20000 }, async t => {
  const f = await environment(t), app = await f.create({ accountIds: [f.readerAccount.accountId] }), scope = await entry(f, app.id);
  const conversationId = (await context(f.reader, scope)).context.conversationId, intents = [];
  let refusal = null;
  for (let i = 0; i < 30; i++) {
    const intent = sendArgs(scope, conversationId, `burst-${i}`, `Burst ${i}`); intents.push(intent);
    try { assert.equal((await f.reader.extension('apps.discussion.send', intent)).replayed, false); }
    catch (error) { refusal = error; break; }
  }
  assert.ok(refusal, 'bounded real HTTP burst reached rate admission');
  assert.equal(refusal.code, 'apps_discussion_rate_limited'); assert.equal(refusal.status, 429);
  const repeated = await f.reader.extension('apps.discussion.send', intents[0]); assert.equal(repeated.replayed, true);
  await f.reader.extension('apps.discussion.remove', { appId: app.id, conversationId, messageId: repeated.receipt.id });
  await f.owner.extension('apps.revoke', { appId: app.id });
  assert.equal((await f.reader.extension('apps.discussion.send', intents[0])).ownCurrent.removed, true);
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
    path: f.paths.world, communityId, owner: { accountId: f.ownerAccount.accountId, deviceId: f.ownerAccount.deviceId, label: 'Discussion owner' },
    memberId: f.readerAccount.accountId, worldModule: new URL('../../modules/world/server/index.mjs', import.meta.url).href })],
  { windowsHide: true, env, stdio: ['ignore', 'ignore', 'pipe'] });
  let stderr = ''; child.stderr.on('data', bytes => { stderr = (stderr + bytes.toString()).slice(-8000); });
  child.finished = new Promise((done, reject) => { child.once('error', reject); child.once('exit', status => done({ status, stderr })); });
  f.children.add(child); return child;
}

test('D2 actual signed send and independent World revocation cannot split historical authority from the committed effect', { timeout: 20000 }, async t => {
  const f = await environment(t);
  const group = await f.owner.extension('world.community.create', { requestId: 'discussion-fenced-group', name: 'Discussion authority', joinPolicy: 'open' });
  const communityId = group.community.communityId;
  await f.reader.extension('world.membership.join', { communityId });
  const app = await f.create({ communityIds: [communityId] }), scope = await entry(f, app.id);
  const conversationId = (await context(f.reader, scope)).context.conversationId;
  const child = startWriter(f, communityId);
  await until(() => existsSync(join(f.directory, 'writer-ready')), 'real independent World writer initialized');
  const world = f.app.locals.worldService, original = world.appCommunityAuthority;
  let held = false;
  world.appCommunityAuthority = (accountId, ownerAccountId, ids) => {
    if (!held && accountId === f.readerAccount.accountId && ids.includes(communityId)) {
      held = true; writeFileSync(join(f.directory, 'writer-start'), 'start');
      const end = Date.now() + 3000;
      while (!existsSync(join(f.directory, 'writer-attempt'))) {
        if (Date.now() > end) assert.fail('membership mutation did not start'); Atomics.wait(sleepWord, 0, 0, 5);
      }
      Atomics.wait(sleepWord, 0, 0, 180);
      assert.equal(existsSync(join(f.directory, 'writer-done')), false, 'writer must wait while discussion authority and commit are fenced');
    }
    return original(accountId, ownerAccountId, ids);
  };
  const intent = sendArgs(scope, conversationId, 'signed-fenced-effect', 'Authorized before removal committed');
  let accepted;
  try { accepted = await f.reader.extension('apps.discussion.send', intent); }
  finally { world.appCommunityAuthority = original; }
  assert.equal(held, true); assert.equal(accepted.replayed, false); assert.equal((await child.finished).status, 0);
  assert.equal(JSON.parse(readFileSync(join(f.directory, 'writer-done'), 'utf8')).allowed, false);
  await assert.rejects(context(f.reader, scope));
  await assert.rejects(f.reader.extension('apps.discussion.send', { ...intent, requestId: 'new-after-revoke' }));
  const replay = await f.reader.extension('apps.discussion.send', intent);
  assert.equal(replay.replayed, true); assert.equal(replay.message, null); assert.deepEqual(replay.receipt, accepted.receipt);
});
async function environment(t) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-discussion-http-independent-'));
  const dataDir = join(directory, 'data'), dist = join(directory, 'dist');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Independent discussion API test</title>');
  let app, serial = 0;
  const sockets = new Set(), clients = [], children = new Set();
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const port = server.address().port, origin = `http://127.0.0.1:${port}`;
  t.after(async () => {
    for (const child of children) if (child.exitCode === null) child.kill();
    await Promise.all([...children].map(child => child.finished));
    for (const client of clients) client.dispose(); await app?.locals.closeServices();
    for (const socket of sockets) socket.destroy(); await new Promise(done => server.close(done));
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-discussion-http-independent-/u);
    await rm(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });
  app = createHttpApp(dist, { dataDir, connectOrigins: [origin], appOriginTemplate: `http://{appId}.legacy.localhost:${port}`,
    namedAppZone: `http://named.localhost:${port}` });
  server.on('upgrade', (req, socket, head) => { if (!app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  function client() {
    const value = createClientWithStorage({ projectId: 'soty', endpoint: `${origin}/api/connect/rpc`,
      fetch: (url, options) => fetch(url, { ...options, headers: { ...options.headers, origin } }),
    }, storage()); clients.push(value); return value;
  }
  const owner = client(), ownerAccount = await owner.bootstrap('Discussion owner');
  const reader = client(), readerAccount = await reader.bootstrap('Discussion reader');
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
      const phone = client(), invitation = await phone.startEnrollment('Second discussion device');
      await reader.approveEnrollment(invitation.requestId, readerAccount.accountId);
      await phone.previewEnrollment(invitation.requestId);
      const joined = await phone.finishEnrollment(invitation.requestId, readerAccount.accountId);
      assert.equal(joined.accountId, readerAccount.accountId); return phone;
    },
  };
}
