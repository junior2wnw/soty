import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { randomBytes, createHash } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import WebSocket from 'ws';
import { createHttpApp } from '../http-app.js';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { createFieldDirectory } from '../../src/world/field-directory.ts';

const code = expected => error => { assert.equal(error?.code, expected); return true; };
const until = async (check, label) => { const end = Date.now() + 5000; while (!check()) { if (Date.now() > end) assert.fail(label); await new Promise(done => setTimeout(done, 5)); } };
function localIdentityStore() {
  let value = null;
  return { async read() { return structuredClone(value); }, async claim(candidate) { value ??= structuredClone(candidate); return structuredClone(value); },
    async compareAndSwap(revision, candidate) { assert.equal(value.localRevision, revision); value = structuredClone(candidate); return structuredClone(value); } };
}
async function environment(t) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-field-signed-http-')), dataDir = join(directory, 'data'), dist = join(directory, 'dist');
  await mkdir(dist); await writeFile(join(dist, 'index.html'), '<!doctype html><title>Isolated field API acceptance</title>');
  const clients = [], sockets = new Set(); let app, serial = 0, phase = 'start-listener';
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const port = server.address().port, origin = `http://127.0.0.1:${port}`;
  app = createHttpApp(dist, { dataDir, connectOrigins: [origin], appOriginTemplate: `http://{appId}.legacy.localhost:${port}`, namedAppZone: `http://named.localhost:${port}` });
  server.on('upgrade', (req, socket, head) => { if (!app.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  t.after(async () => {
    if (t.signal.aborted) t.diagnostic(`field fixture cancelled during ${phase}`);
    clients.forEach(client => client.dispose());
    // Match production shutdown: stop ingress and owned sockets before stores.
    // A bounded test failure must not leave a live callback against closed SQL.
    const stopped = new Promise(done => server.close(done));
    sockets.forEach(socket => socket.destroy());
    await stopped; await app.locals.closeServices();
    await rm(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });
  async function client(label) {
    const value = createClientWithStorage({ projectId: 'soty', endpoint: `${origin}/api/connect/rpc`,
      fetch: (url, options) => fetch(url, { ...options, headers: { ...options.headers, origin } }) }, localIdentityStore());
    clients.push(value); phase = 'signed-client-bootstrap'; return { client: value, account: await value.bootstrap(label) };
  }
  const owner = await client('Автор'), member = await client('Участник'), outsider = await client('Внешний');
  phase = 'native-profiles';
  for (const actor of [owner, member, outsider]) await actor.client.extension('world.profile.get', {}, { expectedAccountId: actor.account.accountId });
  const identity = { linkId: 'field_test_link_0123456789012345678901234567', hostDeviceId: 'field_private_host', connectorId: 'field_private_connector' };
  const token = randomBytes(32).toString('base64url'), claimCode = randomBytes(32).toString('base64url');
  phase = 'connector-register';
  const response = await fetch(`${origin}/api/connectors/register`, { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
    body: JSON.stringify({ linkId: identity.linkId, deviceId: identity.hostDeviceId, connectorId: identity.connectorId, scope: 'Dev', protocol: 2, capabilities: ['apps'] }) });
  assert.equal((await response.json()).ok, true);
  const ws = new WebSocket(`${origin.replace('http:', 'ws:')}/api/apps/channel`), frames = [];
  ws.on('message', raw => frames.push(JSON.parse(raw.toString()))); ws.on('error', () => {});
  phase = 'connector-websocket-open';
  await until(() => ws.readyState === WebSocket.OPEN, 'connector websocket did not open');
  phase = 'connector-authenticate';
  ws.send(JSON.stringify({ type: 'auth', schema: 'soty.apps-channel.v1', ...identity, token, name: 'Только мой ноутбук' }));
  await until(() => frames.some(frame => frame.type === 'ready'), 'connector did not authenticate');
  phase = 'connector-claim-ready';
  ws.send(JSON.stringify({ type: 'claim', claimDigest: createHash('sha256').update(claimCode).digest('hex') }));
  await until(() => frames.some(frame => frame.type === 'claim-ready'), 'claim was not acknowledged');
  const ids = { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId };
  phase = 'signed-connector-claim';
  await owner.client.extension('apps.claim', { ...ids, claimCode });
  phase = 'connector-websocket-close'; ws.close();
  await until(() => ws.readyState === WebSocket.CLOSED, 'connector websocket did not close');
  const call = (actor, op, args = {}) => { phase = `signed-${op}`; return actor.client.extension(op, args, { expectedAccountId: actor.account.accountId }); };
  const field = (actor, op, args = {}) => call(actor, op, { expectedAccountId: actor.account.accountId, ...args });
  const create = async (name, grants = {}) => (await call(owner, 'apps.register', { ...ids, name, grants, port: 23000 + ++serial, entryPath: '/initial' })).app;
  async function publish(id, listed) {
    let current = await call(owner, 'apps.inspect', { appId: id });
    const address = await call(owner, 'apps.domains.claim', { appId: id, slug: `field-${++serial}`, requestId: `claim_${serial}`, expectedDomainsRevision: current.addresses.revision });
    current = await call(owner, 'apps.inspect', { appId: id });
    await call(owner, 'apps.publication.update', { appId: id, requestId: `publish_${++serial}`, expectedPolicyEpoch: current.publication.policyEpoch,
      expectedTargetRevision: current.source.revision, activeDomainIds: [address.receipt.domainId], launchPolicy: 'anyone', listed,
      exposureAck: { scope: 'whole-port', targetRevision: current.source.revision, targetDigest: current.source.digest, profile: current.source.profile } });
    return address.receipt.domainId;
  }
  return { app, owner, member, outsider, client, call, field, create, publish, clients, ids };
}
const safeApp = value => {
  assert.deepEqual(Object.keys(value).sort(), ['id', 'name', 'ownerAccountId', 'state', 'createdAt', 'updatedAt', 'access', 'canManage', 'entry'].sort());
  assert.doesNotMatch(JSON.stringify(value), /hostDeviceId|connectorId|connectorKey|deviceName|grants|claimCode|launchUrl|Только мой ноутбук/u);
};

test('signed native own devices remain private and the real Directory adapter uses the strict legacy empty payload', { timeout: 20000 }, async t => {
  const f = await environment(t), operations = [];
  assert.equal((await f.call(f.owner, 'apps.devices')).devices.length, 1);
  assert.deepEqual((await f.call(f.member, 'apps.devices')).devices, []);
  const directory = createFieldDirectory({ accountId: f.owner.account.accountId, api: { request(op, args) {
    operations.push({ op, keys: Object.keys(args) }); return f.call(f.owner, op, args);
  } } }); t.after(() => directory.dispose());
  const mine = await directory.loadMine({ limit: 60 }), device = mine.items.find(value => value.entity.kind === 'device');
  assert.equal(mine.status, 'ready'); assert.equal(device.title, 'Только мой ноутбук'); assert.equal(device.source, 'owner');
  assert.match(device.entity.id, /^device-[a-f0-9]{64}$/u); assert.deepEqual(operations.find(value => value.op === 'apps.devices').keys, []);
  const search = await directory.search({ query: 'ноутбук' }); assert.equal(search.items.some(value => value.entity.kind === 'device'), false);
});

test('signed apps directory unions current grants and explicit public listing; unlisted apps and private groups remain absent to outsiders', { timeout: 20000 }, async t => {
  const f = await environment(t), group = (await f.call(f.owner, 'world.community.create', { name: 'Приватная студия', joinPolicy: 'invite', requestId: 'private_group_001' })).community;
  await f.call(f.owner, 'world.membership.invite', { communityId: group.communityId, profileId: f.member.account.accountId });
  await f.call(f.member, 'world.membership.join', { communityId: group.communityId });
  const privateApp = await f.create('Личный проект', { communityIds: [group.communityId] });
  const publicApp = await f.create('Музыка общая'); await f.publish(publicApp.id, true);
  const unlisted = await f.create('Музыка по ссылке'); await f.publish(unlisted.id, false);
  const memberPage = await f.field(f.member, 'apps.directory.search'); memberPage.apps.forEach(safeApp);
  assert.deepEqual(new Set(memberPage.apps.map(value => value.id)), new Set([privateApp.id, publicApp.id]));
  assert.equal(memberPage.apps.find(value => value.id === privateApp.id).access, 'granted');
  const publicPage = await f.field(f.outsider, 'apps.directory.search'); publicPage.apps.forEach(safeApp);
  assert.deepEqual(publicPage.apps.map(value => value.id), [publicApp.id]);
  assert.equal(publicPage.apps[0].access, 'public'); assert.equal(publicPage.apps[0].canManage, false);
  assert.deepEqual((await f.field(f.outsider, 'world.directory.search', { query: 'студия' })).communities, []);
  assert.equal((await f.field(f.member, 'world.directory.search', { query: 'студия' })).communities[0].communityId, group.communityId);
  assert.deepEqual((await f.field(f.member, 'apps.directory.search', { scope: 'mine' })).apps.map(value => value.id), [privateApp.id]);
  await f.call(f.owner, 'world.membership.remove', { communityId: group.communityId, profileId: f.member.account.accountId });
  assert.equal((await f.field(f.member, 'apps.directory.resolve', { appIds: [privateApp.id] })).items[0].available, false);
  assert.equal((await f.field(f.member, 'apps.directory.search')).apps.some(value => value.id === privateApp.id), false);
  await assert.rejects(f.call(f.member, 'apps.directory.search'), code('apps_directory_account_required'));
  await assert.rejects(f.field(f.member, 'apps.directory.search', { expectedAccountId: f.owner.account.accountId }), code('authentication_required'));
});

test('signed directory cursors preserve keyset pagination while names change and reject account/scope switching', { timeout: 20000 }, async t => {
  const f = await environment(t);
  const records = [];
  for (const name of ['Музыка Один', 'Музыка Два', 'Музыка Три', 'Музыка Четыре']) records.push(await f.create(name, { accountIds: [f.member.account.accountId] }));
  const first = await f.field(f.member, 'apps.directory.search', { query: 'МУЗ', limit: 2 });
  assert.equal(first.apps.length, 2); assert.ok(first.nextCursor);
  await assert.rejects(f.field(f.outsider, 'apps.directory.search', { query: 'МУЗ', limit: 2, cursor: first.nextCursor }), code('invalid_directory_cursor'));
  await assert.rejects(f.field(f.member, 'apps.directory.search', { query: 'МУЗ', scope: 'mine', cursor: first.nextCursor }), code('invalid_directory_cursor'));
  await f.call(f.owner, 'apps.update', { appId: first.apps[0].id, name: 'Иное название' });
  const second = await f.field(f.member, 'apps.directory.search', { query: 'МУЗ', limit: 2, cursor: first.nextCursor });
  assert.equal(second.apps.length, 2); assert.equal(second.apps.some(value => first.apps.some(before => before.id === value.id)), false);
  assert.deepEqual((await f.field(f.member, 'apps.directory.search', { query: '!!!' })).apps, []);
});

test('signed personal field CAS is restored on another enrolled device and cannot adopt another account', { timeout: 20000 }, async t => {
  const f = await environment(t);
  const group = (await f.call(f.owner, 'world.community.create', { name: 'Доступная группа', joinPolicy: 'open', requestId: 'shortcut_is_not_join' })).community;
  const privateApp = await f.create('Не выданное приложение');
  const document = { schema: 'soty.field.v1', contexts: [{ contextId: 'home', title: 'Личное', x: 10, y: 25 }], shortcuts: [
    { shortcutId: 'group-ref', entity: { kind: 'community', id: group.communityId }, contextId: 'home', slot: [0, 0] },
    { shortcutId: 'app-ref', entity: { kind: 'app', id: privateApp.id }, contextId: 'home', slot: [40, 0] }
  ] };
  const first = await f.field(f.member, 'world.field.put', { expectedRevision: 0, requestId: 'signed_field_001', document });
  const replay = await f.field(f.member, 'world.field.put', { expectedRevision: 0, requestId: 'signed_field_001', document });
  assert.equal(first.receipt.revision, 1); assert.equal(replay.replayed, true); assert.deepEqual(first.receipt, replay.receipt);
  assert.equal((await f.call(f.member, 'world.community.get', { communityId: group.communityId })).community.membership, null);
  await assert.rejects(f.call(f.member, 'world.chat.list', { communityId: group.communityId }), code('community_membership_required'));
  assert.equal((await f.field(f.member, 'apps.directory.resolve', { appIds: [privateApp.id] })).items[0].available, false);
  const { client: phone } = await f.client('Второе устройство');
  const enrollment = await phone.startEnrollment('Телефон'); await f.member.client.approveEnrollment(enrollment.requestId, f.member.account.accountId);
  await phone.previewEnrollment(enrollment.requestId); await phone.finishEnrollment(enrollment.requestId, f.member.account.accountId);
  const restored = await phone.extension('world.field.get', { expectedAccountId: f.member.account.accountId }, { expectedAccountId: f.member.account.accountId });
  assert.deepEqual(restored.document, document); assert.equal(restored.revision, 1);
  await assert.rejects(f.field(f.outsider, 'world.field.get', { expectedAccountId: f.member.account.accountId }), code('field_account_changed'));
  await assert.rejects(f.member.client.extension('world.field.get', { expectedAccountId: f.member.account.accountId }, { expectedAccountId: f.outsider.account.accountId }), code('ACTIVE_PROFILE_CHANGED'));
  assert.equal((await f.field(f.outsider, 'world.field.get')).revision, 0);
});

test('signed accepted private contact appears only in mine and saved resolve fallback; blocking immediately removes that authority', { timeout: 20000 }, async t => {
  const f = await environment(t);
  const card = await f.member.client.card(), request = await f.owner.client.requestContact(card.cardId);
  const before = await f.member.client.contacts(); assert.equal(before.requests.incoming[0].requestId, request.requestId);
  await f.member.client.acceptContact(request.requestId);
  const directory = createFieldDirectory({ accountId: f.owner.account.accountId, api: { request: (op, args) => f.call(f.owner, op, args) } }); t.after(() => directory.dispose());
  const mine = await directory.loadMine({ limit: 60 }), known = mine.items.find(value => value.entity.kind === 'person');
  assert.equal(known.entity.id, f.member.account.accountId); assert.equal(known.title, 'Участник'); assert.equal(known.description, 'В контактах'); assert.equal(known.online, undefined);
  const raw = directory.getRecord(known.entity); assert.equal(raw.kind, 'contact'); assert.equal(raw.peerAccountId, f.member.account.accountId);
  assert.equal(raw.bio, undefined); assert.equal(raw.avatarUrl, undefined);
  assert.equal((await directory.search({ query: 'Участник', kinds: ['person'] })).items.length, 0);
  assert.equal((await f.field(f.owner, 'world.directory.resolve', { entities: [known.entity] })).items[0].available, false);
  const saved = await directory.resolve([known.entity]); assert.equal(saved.items[0].title, 'Участник'); assert.deepEqual(saved.unavailable, []);
  await f.owner.client.blockContact(f.member.account.accountId);
  const removed = await directory.resolve([known.entity]); assert.equal(removed.items.length, 0); assert.deepEqual(removed.unavailable, [known.entity]);
  assert.equal(directory.getRecord(known.entity), null); assert.equal((await directory.loadMine({ limit: 60 })).items.some(value => value.entity.id === known.entity.id), false);
});
