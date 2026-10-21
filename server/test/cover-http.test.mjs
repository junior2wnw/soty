import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { randomBytes } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import sharp from 'sharp';
import { createHttpApp } from '../http-app.js';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { digestArgs } from '../../modules/connect/server/index.mjs';
import { appCoverOperations } from '../../modules/apps/server/cover.mjs';

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
  const directory = await mkdtemp(join(tmpdir(), 'soty-cover-http-'));
  const dataDir = join(directory, 'data'), dist = join(directory, 'dist');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Cover HTTP pilot</title>');
  let app; const sockets = new Set(), clients = [];
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const origin = `http://127.0.0.1:${server.address().port}`;
  const open = () => createHttpApp(dist, { env: {}, gonka: {}, dataDir, connectOrigins: [origin],
    appOriginTemplate: `http://{appId}.legacy.localhost:${server.address().port}`, namedAppZone: `http://named.localhost:${server.address().port}`,
    discoveryOrigin: '', appHosting: { domainProfile: 'separate-site', retainedNamedAppZones: [] } });
  t.after(async () => {
    for (const client of clients) client.dispose(); await app?.locals.closeServices();
    for (const socket of sockets) socket.destroy(); await new Promise(done => server.close(done));
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-cover-http-/u);
    await rm(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });
  app = open();
  function client(vault = storage()) {
    const value = createClientWithStorage({ projectId: 'soty', endpoint: `${origin}/api/connect/rpc`,
      fetch: (url, options) => fetch(url, { ...options, headers: { ...options.headers, origin } }),
    }, vault); clients.push(value); return value;
  }
  const ownVault = storage(), owner = client(ownVault), account = await owner.bootstrap('Synthetic cover owner');
  const foreign = client(), foreignAccount = await foreign.bootstrap('Synthetic foreign viewer');
  return { dataDir, origin, owner, account, foreign, foreignAccount, get app() { return app; },
    async reopen() { await app.locals.closeServices(); app = open(); return client(ownVault); } };
}

test('actual signed HTTP profile cover survives service and device-client reload with private access enforced', { timeout: 30000 }, async t => {
  const f = await environment(t), scope = { expectedAccountId: f.account.accountId, targetId: f.account.accountId };
  const profile = (await f.owner.extension('world.profile.get', {})).profile;
  assert.equal(profile.avatarWriteVersion, 1, 'capability follows the installed Connect registry');
  const initial = await f.owner.extension('world.profile.cover.status', scope);
  const id = () => `cover1.${initial.serverTime}.${randomBytes(16).toString('hex')}`;
  const bytes = await sharp({ create: { width: 80, height: 60, channels: 3, background: '#734789' } }).png().toBuffer();
  const prepared = await f.owner.extension('world.profile.cover.prepare', { ...scope, requestId: id(),
    expectedTargetRevision: initial.scope.targetRevision, expectedPolicyEpoch: initial.scope.policyEpoch,
    expectedCoverRevision: initial.scope.coverRevision, mime: 'image/png', dataBase64: bytes.toString('base64'),
    crop: { x: .5, y: .5, zoom: 1 } });
  const commit = { ...scope, requestId: id(), draftId: prepared.draft.id };
  const saved = await f.owner.extension('world.profile.cover.commit', commit);
  const image = await f.owner.extension('world.profile.cover.read', scope);
  assert.equal(image.current.mode, 'custom'); assert.ok(image.image.dataBase64.length > 10);
  assert.ok((await f.owner.extension('world.profile.get', {})).profile.avatarRevision > profile.avatarRevision);
  await assert.rejects(f.foreign.extension('world.profile.cover.read', { ...scope, expectedAccountId: f.foreignAccount.accountId }));
  const unsigned = await fetch(`${f.origin}/api/connect/rpc`, { method: 'POST', headers: { 'content-type': 'application/json', origin: f.origin },
    body: JSON.stringify({ op: 'world.profile.cover.read', args: scope }) });
  assert.equal((await unsigned.json()).ok, false);
  const reloaded = await f.reopen();
  const after = await reloaded.extension('world.profile.cover.read', scope);
  assert.equal(after.image.dataBase64, image.image.dataBase64); assert.deepEqual(after.current, image.current);
  const replay = await reloaded.extension('world.profile.cover.commit', commit);
  assert.equal(replay.replayed, true); assert.deepEqual(replay.receipt, saved.receipt);
});

test('fresh HTTP Apps store installs all actual signed cover operations', { timeout: 30000 }, async t => {
  const f = await environment(t), extension = f.app.locals.appsService.coverExtension;
  assert.deepEqual([...extension.operations], [...appCoverOperations]);
  for (const operation of appCoverOperations) {
    const result = await f.app.locals.connectService.handle({ origin: f.origin, op: 'challenge', args: { operation, digest: digestArgs({}) } });
    assert.equal(result.ok, true, result.error?.code);
  }
  await assert.rejects(f.owner.extension('apps.cover.status', { expectedAccountId: f.account.accountId, appId: 'app-' + 'f'.repeat(32) }),
    error => error.code !== 'unsupported_operation');
  const db = new DatabaseSync(join(f.dataDir, 'apps', 'registry.sqlite'), { readOnly: true });
  try { assert.equal(db.prepare('PRAGMA user_version').get().user_version, 12); }
  finally { db.close(); }
});
