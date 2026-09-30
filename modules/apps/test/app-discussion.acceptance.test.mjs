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

// Independent D2 service fixture: real SQLite/World authority and WebSocket
// claim, with the source deliberately offline. Signed-wire coverage is separate.
const owner = Object.freeze({ accountId: 'discussion_owner', deviceId: 'owner_browser', label: 'Author label' });
const reader = Object.freeze({ accountId: 'discussion_reader', deviceId: 'reader_browser', label: 'Reader label' });
const phone = Object.freeze({ ...reader, deviceId: 'reader_phone' });
const other = Object.freeze({ accountId: 'discussion_other', deviceId: 'other_browser', label: 'Other label' });
const later = Object.freeze({ accountId: 'discussion_later', deviceId: 'later_browser', label: 'Later label' });
const actors = [owner, reader, phone, other, later];
const key = value => JSON.stringify([value.accountId, value.deviceId]);
const sha = value => createHash('sha256').update(value).digest('hex');
const pause = ms => new Promise(done => setTimeout(done, ms));
const code = expected => error => { assert.equal(error?.code, expected); return true; };
async function until(check, label, timeout = 5000) {
  const end = Date.now() + timeout;
  do { const value = check(); if (value) return value; await pause(5); } while (Date.now() < end);
  assert.fail(`Timed out: ${label}`);
}
function safe(value) {
  assert.doesNotMatch(JSON.stringify(value), /connectorId|connectorKey|hostDeviceId|linkId|claimCode|sessionCheck|grants_json|communityIds|bio|interests/u);
}
function messageShape(message) {
  assert.deepEqual(Object.keys(message).sort(), ['id', 'conversationId', 'author', 'body', 'replyTo', 'createdAt', 'removedAt', 'canRemove'].sort());
  assert.deepEqual(Object.keys(message.author).sort(), ['accountId', 'label']); safe(message);
}

async function environment(t, overrides = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-discussion-independent-'));
  const databasePath = join(directory, 'apps.sqlite'), worldPath = join(directory, 'world.sqlite');
  const world = createWorldService({ databasePath: worldPath, projectId: 'discussion_acceptance' });
  let service, serial = 0, clock = 1_800_000_000_000;
  const active = new Set(actors.map(key)), services = [], sockets = new Set(), children = new Set();
  const gateway = createServer((_req, res) => res.writeHead(404).end());
  gateway.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  gateway.on('upgrade', (req, socket, head) => { if (!service?.handleUpgrade(req, socket, head)) socket.destroy(); });
  await new Promise(done => gateway.listen(0, '127.0.0.1', done));
  const origin = `http://127.0.0.1:${gateway.address().port}`;
  const token = randomBytes(32).toString('base64url');
  const config = {
    databasePath, appOriginTemplate: `http://{appId}.legacy.localhost:${gateway.address().port}`,
    namedAppZone: `http://named.localhost:${gateway.address().port}`, shellOrigins: [origin], now: () => clock,
    actorActive: actor => active.has(key(actor)),
    canAccessCommunity: (accountId, communityId) => world.canAccessCommunity(accountId, communityId),
    isGroupAdmin: (accountId, communityId) => world.isGroupAdmin(accountId, communityId),
    activeCommunityIds: accountId => world.activeCommunityIds(accountId),
    withAuthorityFence: callback => world.withCommunityAuthorityFence(callback),
    readCommunityAuthority: (actor, ownerAccountId, ids) => world.appCommunityAuthority(actor.accountId, ownerAccountId, ids),
    authenticateConnector: async auth => auth.token === token,
    ...overrides,
  };
  function open(extra = {}) { const result = createAppsService({ ...config, ...extra }); services.push(result); return result; }
  t.after(async () => {
    for (const child of children) if (child.exitCode === null) child.kill();
    await Promise.all([...children].map(child => child.finished));
    for (const instance of services) instance.close(); world.close();
    for (const socket of sockets) socket.destroy(); await new Promise(done => gateway.close(done));
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-discussion-independent-/u);
    await rm(directory, { recursive: true, force: true, maxRetries: 3, retryDelay: 50 });
  });
  service = open();
  const call = (op, args = {}, actor = owner, instance = service) => instance.execute({ op, args: { expectedAccountId: actor.accountId, ...args }, actor });
  const identity = { linkId: 'discussion_link', hostDeviceId: 'discussion_host', connectorId: 'discussion_connector' };
  const claimCode = randomBytes(32).toString('base64url');
  const ws = new WebSocket(`${origin.replace('http:', 'ws:')}/api/apps/channel`), frames = [];
  ws.on('message', bytes => frames.push(JSON.parse(bytes.toString()))); ws.on('error', () => {});
  await new Promise((done, reject) => { ws.once('open', done); ws.once('error', reject); });
  ws.send(JSON.stringify({ type: 'auth', schema: 'soty.apps-channel.v1', ...identity, token, name: 'Private source label' }));
  await until(() => frames.some(frame => frame.type === 'ready'), 'real connector authentication');
  ws.send(JSON.stringify({ type: 'claim', claimDigest: sha(claimCode) }));
  await until(() => frames.some(frame => frame.type === 'claim-ready'), 'claim challenge');
  call('apps.claim', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, claimCode });
  const closed = new Promise(done => ws.once('close', done)); ws.close(); await closed;
  await until(() => call('apps.devices').devices.every(device => !device.online), 'offline source');
  const inspect = id => call('apps.inspect', { appId: id });
  function app(grants = { accountIds: [reader.accountId] }) {
    return call('apps.register', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId,
      port: 15000 + ++serial, name: `Discussion specimen ${serial}`, entryPath: '/start', grants }).app;
  }
  const entry = (id, address = inspect(id).addresses.canonical, path = '/thread?tag=a%2Bb#anchor') => ({ appId: id, domainId: address.id, path });
  function alias(id) {
    const result = call('apps.domains.claim', { appId: id, slug: `discussion-${++serial}`, requestId: `claim_${serial}`,
      expectedDomainsRevision: inspect(id).addresses.revision });
    return inspect(id).addresses.aliases.find(value => value.id === result.receipt.domainId);
  }
  function publish(id, aliases, launchPolicy = 'anyone') {
    const before = inspect(id);
    return call('apps.publication.update', { appId: id, requestId: `publish_${++serial}`, expectedPolicyEpoch: before.publication.policyEpoch,
      expectedTargetRevision: before.source.revision, activeDomainIds: aliases.map(value => value.id), launchPolicy, listed: false,
      ...(launchPolicy === 'anyone' ? { exposureAck: { scope: 'whole-port', targetRevision: before.source.revision,
        targetDigest: before.source.digest, profile: before.source.profile } } : {}) });
  }
  const context = (scope, actor = reader, instance = service) => call('apps.discussion.context', scope, actor, instance);
  const archives = (scope, actor = reader, instance = service) => call('apps.discussion.archives', scope, actor, instance);
  const sendArgs = (scope, conversationId, body, extra = {}) => ({ ...scope, conversationId, requestId: `message_${++serial}`, body, ...extra });
  function send(scope, conversationId, body, actor = reader, extra = {}) {
    clock += 2500; return call('apps.discussion.send', sendArgs(scope, conversationId, body, extra), actor);
  }
  function group(name, members = []) {
    const communityId = world.execute({ op: 'world.community.create', args: { name, joinPolicy: 'open', requestId: `group_${++serial}` }, actor: owner }).community.communityId;
    for (const actor of members) world.execute({ op: 'world.membership.join', args: { communityId }, actor });
    return communityId;
  }
  function sql(fn, file = databasePath) { const db = new DatabaseSync(file); try { return fn(db); } finally { db.close(); } }
  return { directory, databasePath, worldPath, world, config, children, call, app, inspect, entry, alias, publish, context, archives, sendArgs, send, group, sql, active, open,
    advance(ms = 2500) { clock += ms; }, reopen(extra = {}) { service.close(); service = open(extra); return service; } };
}

test('D2 first use is native app discussion with minimal author identity, no World profile and no live source', async t => {
  const f = await environment(t), app = f.app(), scope = f.entry(app.id);
  await until(() => f.inspect(app.id).source.observation.state === 'offline', 'server processed the disconnected source');
  assert.equal(f.sql(db => db.prepare('SELECT count(*) AS n FROM profiles').get().n, f.worldPath), 0);
  const empty = f.context(scope); safe(empty); assert.deepEqual(empty.messages, []);
  assert.equal(empty.context.audience, 'shared'); assert.equal(empty.context.canPost, true);
  const accepted = f.send(scope, empty.context.conversationId, 'First native message');
  assert.equal(accepted.replayed, false); messageShape(accepted.message);
  assert.equal(accepted.message.author.accountId, reader.accountId); assert.equal(accepted.message.author.label, reader.label);
  assert.deepEqual(f.context(scope, phone).messages.map(value => value.body), ['First native message']);
  assert.equal(f.sql(db => db.prepare('SELECT count(*) AS n FROM profiles').get().n, f.worldPath), 0);
  assert.equal(f.inspect(app.id).source.observation.state, 'offline');
  f.call('apps.update', { appId: app.id, name: 'Renamed without changing readers' });
  assert.equal(f.context(scope).context.conversationId, empty.context.conversationId);
  f.call('apps.update', { appId: app.id, grants: { accountIds: [reader.accountId] } });
  assert.equal(f.context(scope).context.conversationId, empty.context.conversationId);
  const profile = f.world.execute({ op: 'world.profile.get', args: {}, actor: reader }).profile;
  f.world.execute({ op: 'world.profile.update', args: { expectedRevision: profile.revision,
    displayName: 'PRIVATE NEW PROFILE NAME', bio: 'PRIVATE BIO THAT IS NOT A CHAT DTO', discoverable: false, showMemberships: false }, actor: reader });
  const laterRead = f.context(scope); assert.equal(laterRead.messages[0].author.label, reader.label);
  assert.doesNotMatch(JSON.stringify(laterRead), /PRIVATE NEW PROFILE NAME|PRIVATE BIO/u);
});

test('D2 returning to public creates a distinct audience; a grant-based canonical entry still clearly posts to public', async t => {
  const f = await environment(t), app = f.app(), canonical = f.entry(app.id), address = f.alias(app.id), scope = f.entry(app.id, address);
  const first = f.context(canonical).context.conversationId;
  f.send(canonical, first, 'PRIVATE ONE'); f.publish(app.id, [address]);
  const publicOne = f.context(scope, other).context.conversationId; assert.notEqual(publicOne, first);
  const ownerPublic = f.context(canonical, owner); assert.equal(ownerPublic.context.audience, 'public');
  f.send(scope, publicOne, 'PUBLIC ONE', other);
  assert.throws(() => f.context({ ...scope, conversationId: first }, other));
  assert.equal(f.context({ ...scope, conversationId: first }).context.canPost, false);
  f.publish(app.id, [], 'restricted');
  const privateTwo = f.context(canonical).context.conversationId;
  assert.notEqual(privateTwo, first); assert.notEqual(privateTwo, publicOne);
  f.send(canonical, privateTwo, 'PRIVATE TWO'); f.publish(app.id, [address]);
  const publicTwo = f.context(scope, other).context.conversationId;
  assert.equal(new Set([first, publicOne, privateTwo, publicTwo]).size, 4);
  const visible = f.archives(scope, other); safe(visible);
  assert.deepEqual(visible.entries.map(value => value.conversationId), [publicOne]);
  assert.equal(visible.nextCursor, null, 'hidden archives must not create empty continuation pages');
  f.publish(app.id, [address]); assert.equal(f.context(scope, other).context.conversationId, publicTwo, 'policy no-op is not a new audience');
  f.call('apps.update', { appId: app.id, grants: { accountIds: [reader.accountId, later.accountId] } });
  assert.notEqual(f.context(scope, other).context.conversationId, publicTwo, 'actual grant changes rotate even with anyone enabled');
});

test('D2 restricted archives require current grant plus original predicate, not an identical grant path', async t => {
  const f = await environment(t), app = f.app(), scope = f.entry(app.id);
  const direct = f.context(scope).context.conversationId; f.send(scope, direct, 'OLD DIRECT ONLY');
  const nextGroup = f.group('New group', [reader, other]);
  f.call('apps.update', { appId: app.id, grants: { communityIds: [nextGroup] } });
  assert.equal(f.context({ ...scope, conversationId: direct }).messages[0].body, 'OLD DIRECT ONLY');
  assert.throws(() => f.context({ ...scope, conversationId: direct }, other));
  const originalGroup = f.group('Original historical group', [reader]);
  f.call('apps.update', { appId: app.id, grants: { communityIds: [originalGroup] } });
  const oldGroupConversation = f.context(scope).context.conversationId;
  const groupMessage = f.send(scope, oldGroupConversation, 'OLD GROUP ONLY');
  f.call('apps.update', { appId: app.id, grants: { accountIds: [reader.accountId, other.accountId, later.accountId] } });
  assert.throws(() => f.context({ ...scope, conversationId: oldGroupConversation }, later));
  f.world.execute({ op: 'world.membership.join', args: { communityId: originalGroup }, actor: later });
  assert.equal(f.context({ ...scope, conversationId: oldGroupConversation }, later).messages[0].body, 'OLD GROUP ONLY');
  f.world.execute({ op: 'world.membership.leave', args: { communityId: originalGroup }, actor: later });
  assert.throws(() => f.context({ ...scope, conversationId: oldGroupConversation }, later));
  f.world.execute({ op: 'world.membership.join', args: { communityId: originalGroup }, actor: other });
  f.world.execute({ op: 'world.membership.transfer', args: { communityId: originalGroup, profileId: other.accountId, requestId: 'discussion_transfer' }, actor: owner });
  f.world.execute({ op: 'world.membership.role', args: { communityId: originalGroup, profileId: owner.accountId, role: 'member' }, actor: other });
  assert.throws(() => f.context({ ...scope, conversationId: oldGroupConversation }), 'lost publisher group authority invalidates historical group access');
  assert.throws(() => f.call('apps.discussion.remove', { appId: app.id, conversationId: oldGroupConversation, messageId: groupMessage.receipt.id }, other),
    'the World community owner does not become app-discussion moderator');
  assert.equal(f.context(scope).context.canPost, true, 'independent current direct grant still permits the new conversation');
});

test('D2 accepted replay is own immutable metadata after ACL loss; global request identity cannot retarget a post', async t => {
  const f = await environment(t), app = f.app(), scope = f.entry(app.id), first = f.context(scope).context.conversationId;
  const intent = f.sendArgs(scope, first, 'PRIVATE LOST ACK BODY');
  const accepted = f.call('apps.discussion.send', intent, reader);
  const address = f.alias(app.id); f.publish(app.id, [address]);
  f.call('apps.update', { appId: app.id, grants: {} });
  const publicScope = f.entry(app.id, address), current = f.context(publicScope, other).context.conversationId;
  assert.throws(() => f.context(scope));
  const replay = f.call('apps.discussion.send', intent, phone);
  assert.equal(replay.replayed, true); assert.deepEqual(replay.receipt, accepted.receipt); assert.equal(replay.message, null); safe(replay);
  assert.deepEqual(Object.keys(replay.receipt).sort(), ['id', 'conversationId', 'createdAt'].sort());
  assert.doesNotMatch(JSON.stringify(replay), /PRIVATE LOST ACK BODY|currentConversation|messages/u);
  const otherApp = f.app();
  for (const changed of [{ body: 'changed' }, { conversationId: current }, { ...publicScope, conversationId: current },
    { domainId: address.id }, { path: '/different' }, { appId: otherApp.id }, { replyTo: accepted.receipt.id }]) {
    assert.throws(() => f.call('apps.discussion.send', { ...intent, ...changed }, reader), code('apps_discussion_request_conflict'));
  }
  assert.throws(() => f.call('apps.discussion.send', { ...intent, requestId: 'never-accepted-old-generation' }, reader));
  const erased = f.call('apps.discussion.remove', { appId: app.id, conversationId: first, messageId: accepted.receipt.id }, reader);
  assert.equal(erased.removed, true);
  f.reopen();
  const afterDelete = f.call('apps.discussion.send', intent, reader);
  assert.deepEqual(afterDelete.receipt, accepted.receipt); assert.deepEqual(afterDelete.ownCurrent, { removed: true }); assert.equal(afterDelete.message, null);
  assert.throws(() => f.call('apps.discussion.send', { ...intent, body: 'resurrection' }, reader), code('apps_discussion_request_conflict'));
  assert.deepEqual(f.context(publicScope, other).messages, []);
});

test('D2 retired exact entries never fall back; owner administrative archive can redact after app revoke but cannot post', async t => {
  const f = await environment(t), app = f.app({}), a = f.alias(app.id), b = f.alias(app.id);
  f.publish(app.id, [a, b]); const scope = f.entry(app.id, a), conversationId = f.context(scope, other).context.conversationId;
  const accepted = f.send(scope, conversationId, 'Owner can redact without restoring runtime', other);
  f.call('apps.domains.retire', { appId: app.id, domainId: a.id, requestId: 'retire-original-entry', expectedDomainsRevision: f.inspect(app.id).addresses.revision });
  for (const actor of [owner, other]) assert.throws(() => f.context({ ...scope, conversationId }, actor));
  assert.equal(f.context(f.entry(app.id, b), other).messages.length, 1);
  const admin = { appId: app.id, conversationId, administrative: true };
  let archived = f.context(admin, owner); assert.equal(archived.context.entry, null); assert.equal(archived.context.canPost, false); assert.equal(archived.context.ownerAdministrative, true);
  assert.throws(() => f.context(admin, other)); assert.throws(() => f.context({ ...admin, domainId: b.id }, owner));
  f.call('apps.revoke', { appId: app.id });
  archived = f.context(admin, owner); assert.equal(archived.messages.length, 1); assert.equal(archived.context.canPost, false);
  assert.throws(() => f.send(f.entry(app.id, b), conversationId, 'must not post after revoke', owner));
  assert.throws(() => f.call('apps.discussion.remove', { appId: app.id, conversationId, messageId: accepted.receipt.id }, reader));
  assert.equal(f.call('apps.discussion.remove', { appId: app.id, conversationId, messageId: accepted.receipt.id }, owner).removed, true);
  const tombstone = f.context(admin, owner).messages[0]; assert.equal(tombstone.body, null); assert.ok(tombstone.removedAt); messageShape(tombstone);
  f.active.delete(key(owner)); assert.throws(() => f.context(admin, owner), 'revoked owner installation gets no administrative bypass');
});

test('D2 replies stay in their conversation and never retain a copied deleted body or World history', async t => {
  const f = await environment(t), communityId = f.group('Independent community history', [reader]);
  f.world.execute({ op: 'world.chat.send', args: { communityId, clientId: 'world-only-message', text: 'WORLD SECRET MUST STAY IN WORLD' }, actor: reader });
  const app = f.app({ communityIds: [communityId] }), scope = f.entry(app.id), current = f.context(scope);
  assert.deepEqual(current.messages, []);
  const first = f.send(scope, current.context.conversationId, 'Body that will disappear');
  const reply = f.send(scope, current.context.conversationId, 'A native reply', reader, { replyTo: first.receipt.id });
  const app2 = f.app(), scope2 = f.entry(app2.id), conversation2 = f.context(scope2).context.conversationId;
  assert.throws(() => f.send(scope2, conversation2, 'foreign reply', reader, { replyTo: first.receipt.id }));
  f.call('apps.discussion.remove', { appId: app.id, conversationId: current.context.conversationId, messageId: first.receipt.id }, reader);
  const snapshot = f.context(scope); safe(snapshot);
  assert.doesNotMatch(JSON.stringify(snapshot), /Body that will disappear|WORLD SECRET MUST STAY IN WORLD/u);
  assert.equal(snapshot.messages.find(value => value.id === reply.receipt.id).replyTo, first.receipt.id);
});

test('D2 deletion of a message outside the initial tail arrives through the snapshot change cursor', async t => {
  const f = await environment(t), app = f.app(), scope = f.entry(app.id), first = f.context(scope);
  const oldest = f.send(scope, first.context.conversationId, 'Old message once visible');
  for (let i = 0; i < 39; i++) f.send(scope, first.context.conversationId, `Later message ${i}`);
  const initial = f.context(scope); assert.ok(!initial.messages.some(value => value.id === oldest.receipt.id)); assert.ok(initial.historyCursor);
  f.call('apps.discussion.remove', { appId: app.id, conversationId: first.context.conversationId, messageId: oldest.receipt.id }, owner);
  const changes = f.call('apps.discussion.changes', { ...scope, conversationId: first.context.conversationId, cursor: initial.changeCursor }, reader);
  assert.equal(changes.resetRequired, false);
  const removed = changes.changes.find(value => value.type === 'removed' && value.message.id === oldest.receipt.id);
  assert.ok(removed); assert.equal(removed.message.body, null); assert.ok(removed.message.removedAt);
  const old = f.call('apps.discussion.history', { ...scope, conversationId: first.context.conversationId, cursor: initial.historyCursor }, reader);
  assert.equal(old.messages.find(value => value.id === oldest.receipt.id).body, null);
  assert.throws(() => f.call('apps.discussion.changes', { ...scope, conversationId: first.context.conversationId, cursor: initial.changeCursor }, other));
});

test('D2 actual UTF-8 JSON pages stay bounded and cursors cannot cross actors, conversations or restart snapshots', { timeout: 30000 }, async t => {
  const f = await environment(t), app = f.app({ accountIds: [reader.accountId, other.accountId] }), scope = f.entry(app.id);
  const before = f.context(scope), conversationId = before.context.conversationId;
  for (let i = 0; i < 50; i++) f.send(scope, conversationId, '漢'.repeat(3990) + String(i).padStart(4, '0'));
  const first = f.context(scope); assert.ok(Buffer.byteLength(JSON.stringify(first)) <= 256 * 1024); assert.ok(first.historyCursor);
  const all = [...first.messages]; let cursor = first.historyCursor;
  while (cursor) {
    const page = f.call('apps.discussion.history', { ...scope, conversationId, cursor, limit: 50 }, reader);
    assert.ok(Buffer.byteLength(JSON.stringify(page)) <= 256 * 1024); assert.equal(page.resetRequired, false);
    all.push(...page.messages); cursor = page.nextCursor;
  }
  assert.equal(all.length, 50); assert.equal(new Set(all.map(value => value.id)).size, 50);
  const changePage = f.call('apps.discussion.changes', { ...scope, conversationId, cursor: before.changeCursor, limit: 100 }, reader);
  assert.ok(Buffer.byteLength(JSON.stringify(changePage)) <= 256 * 1024); assert.equal(changePage.hasMore, true);
  assert.throws(() => f.call('apps.discussion.history', { ...scope, conversationId, cursor: first.historyCursor }, other));
  assert.throws(() => f.call('apps.discussion.history', { ...scope, conversationId, cursor: first.historyCursor }, phone));
  assert.throws(() => f.call('apps.discussion.history', { ...scope, path: '/a-different-entry', conversationId, cursor: first.historyCursor }, reader));
  const otherApp = f.app(), otherScope = f.entry(otherApp.id), otherConversation = f.context(otherScope).context.conversationId;
  assert.throws(() => f.call('apps.discussion.history', { ...otherScope, conversationId: otherConversation, cursor: first.historyCursor }, reader));
  f.reopen();
  const afterRestart = f.call('apps.discussion.changes', { ...scope, conversationId, cursor: before.changeCursor }, reader);
  assert.equal(afterRestart.resetRequired, true); assert.deepEqual(afterRestart.changes, []);
  const resetHistory = f.call('apps.discussion.history', { ...scope, conversationId, cursor: first.historyCursor }, reader);
  assert.equal(resetHistory.resetRequired, true); assert.deepEqual(resetHistory.messages, []);
  assert.ok(f.context(scope).messages.length > 0, 'reset is not a claim that the conversation is empty');
});

test('D2 body validation, exact actor scope and missing host fence fail without any accepted post', async t => {
  const f = await environment(t), app = f.app(), scope = f.entry(app.id), conversationId = f.context(scope).context.conversationId;
  for (const body of ['', ' '.repeat(3), 'a'.repeat(4001), '\ud800', ['coerced'], { body: 'wrong' }]) {
    assert.throws(() => f.call('apps.discussion.send', f.sendArgs(scope, conversationId, body), reader));
  }
  assert.throws(() => f.call('apps.discussion.send', { ...f.sendArgs(scope, conversationId, 'wrong account'), expectedAccountId: other.accountId }, reader));
  assert.throws(() => f.call('apps.discussion.context', { ...scope, origin: 'https://attacker.invalid' }, reader));
  const missing = f.open({ withAuthorityFence: undefined });
  assert.throws(() => f.context(scope, reader, missing));
  assert.ok(f.call('apps.inspect', { appId: app.id }, owner, missing), 'legacy owner inspection remains available');
  assert.deepEqual(f.context(scope).messages, []);
});

test('D2 retained change windows report reset without pretending older deleted content is still authoritative', async t => {
  const f = await environment(t, { discussionLimits: { changesRetained: 4 } }), app = f.app(), scope = f.entry(app.id);
  const first = f.context(scope), id = first.context.conversationId;
  const oldest = f.send(scope, id, 'Will be removed after the old cursor');
  const previouslyLoaded = f.context(scope);
  for (let i = 0; i < 5; i++) f.send(scope, id, `New message ${i}`);
  f.call('apps.discussion.remove', { appId: app.id, conversationId: id, messageId: oldest.receipt.id }, reader);
  const reset = f.call('apps.discussion.changes', { ...scope, conversationId: id, cursor: previouslyLoaded.changeCursor }, reader);
  assert.equal(reset.resetRequired, true); assert.deepEqual(reset.changes, []); assert.equal(reset.hasMore, false);
  const fresh = f.context(scope); assert.equal(fresh.messages.find(value => value.id === oldest.receipt.id).body, null);
  assert.doesNotMatch(JSON.stringify(fresh), /Will be removed after the old cursor/u);
  const retainedBefore = f.sql(db => db.prepare('SELECT count(*) AS n FROM app_discussion_changes WHERE conversation_id=?').get(id).n);
  assert.equal(retainedBefore, 4);
  f.reopen({ discussionLimits: { changesRetained: 1 } });
  assert.equal(f.context(scope).messages.length, fresh.messages.length, 'lower admission settings do not make earlier valid data corrupt');
  assert.equal(f.sql(db => db.prepare('SELECT count(*) AS n FROM app_discussion_changes WHERE conversation_id=?').get(id).n), retainedBefore,
    'read/reopen does not silently purge the prior retained window');
  const nextCursor = f.context(scope).changeCursor, remaining = fresh.messages.find(value => value.body === 'New message 0');
  f.call('apps.discussion.remove', { appId: app.id, conversationId: id, messageId: remaining.id }, reader);
  const last = f.call('apps.discussion.changes', { ...scope, conversationId: id, cursor: nextCursor }, reader);
  assert.equal(last.resetRequired, false); assert.equal(last.changes.length, 1); assert.equal(last.changes[0].message.id, remaining.id);
  // Browser draft preservation is deliberately a D3 gate, not simulated here.
});

test('D2 message quotas include tombstones and cannot block exact replay, owner redaction or emergency closure', async t => {
  const f = await environment(t, { discussionLimits: { messagesPerApp: 2, messages: 3 } });
  const app = f.app(), app2 = f.app(), scope = f.entry(app.id), scope2 = f.entry(app2.id);
  const id = f.context(scope).context.conversationId, id2 = f.context(scope2).context.conversationId;
  const firstIntent = f.sendArgs(scope, id, 'First');
  const first = f.call('apps.discussion.send', firstIntent, reader);
  f.send(scope, id, 'Second'); f.send(scope2, id2, 'Third globally');
  assert.throws(() => f.send(scope, id, 'Per-app overflow'), code('apps_discussion_capacity'));
  assert.throws(() => f.send(scope2, id2, 'Global overflow'), code('apps_discussion_capacity'));
  assert.equal(f.call('apps.discussion.send', firstIntent, phone).replayed, true);
  f.call('apps.discussion.remove', { appId: app.id, conversationId: id, messageId: first.receipt.id }, owner);
  assert.throws(() => f.send(scope, id, 'A tombstone is not a reusable client identity slot'), code('apps_discussion_capacity'));
  const address = f.alias(app.id); f.publish(app.id, [address]); f.publish(app.id, [], 'restricted');
  f.call('apps.revoke', { appId: app.id });
  const admin = f.context({ appId: app.id, conversationId: id, administrative: true }, owner);
  assert.equal(admin.messages.length, 2); assert.equal(admin.context.canPost, false);
  f.reopen({ discussionLimits: { messages: 1, messagesPerApp: 1 } });
  assert.equal(f.context({ appId: app.id, conversationId: id, administrative: true }, owner).messages.length, 2);
  const replay = f.call('apps.discussion.send', firstIntent, reader);
  assert.deepEqual(replay.receipt, first.receipt); assert.equal(replay.ownCurrent.removed, true);
  f.call('apps.discussion.remove', { appId: app.id, conversationId: id, messageId: admin.messages.find(value => value.body === 'Second').id }, owner);
});

test('D2 empty audience churn consumes one head, while materialized and body bounds do not obstruct restriction', async t => {
  const f = await environment(t, { discussionLimits: { heads: 2, conversations: 2, conversationsPerApp: 1, bodyBytes: 12, bodyBytesPerApp: 8 } });
  const app = f.app(), scope = f.entry(app.id), first = f.context(scope).context.conversationId;
  for (let i = 0; i < 8; i++) f.call('apps.update', { appId: app.id, grants: { accountIds: i % 2 ? [reader.accountId] : [reader.accountId, later.accountId] } });
  assert.notEqual(f.context(scope).context.conversationId, first);
  const id = f.context(scope).context.conversationId;
  assert.throws(() => f.send(scope, id, '漢'.repeat(3)), code('apps_discussion_capacity'));
  const accepted = f.send(scope, id, '12345678');
  assert.equal(f.context(scope).messages.length, 1, 'failed body admission did not materialize an extra message');
  const app2 = f.app(), scope2 = f.entry(app2.id), second = f.context(scope2).context.conversationId;
  assert.throws(() => f.send(scope2, second, '12345'), code('apps_discussion_capacity'));
  f.send(scope2, second, '1234');
  const app3 = f.app(); assert.throws(() => f.context(f.entry(app3.id)), code('apps_discussion_capacity'));
  f.call('apps.discussion.remove', { appId: app.id, conversationId: id, messageId: accepted.receipt.id }, reader);
  f.send(scope, id, '1234');
  const address = f.alias(app.id); f.publish(app.id, [address]);
  const publicScope = f.entry(app.id, address), next = f.context(publicScope).context.conversationId;
  assert.throws(() => f.send(publicScope, next, 'x'), code('apps_discussion_capacity'));
  f.publish(app.id, [], 'restricted'); f.call('apps.revoke', { appId: app.id });
  assert.equal(f.context({ appId: app.id, conversationId: id, administrative: true }, owner).context.canPost, false);
});

test('D2 private archive density does not create observable empty pages or hidden continuation hints', { timeout: 30000 }, async t => {
  const f = await environment(t), app = f.app(), canonical = f.entry(app.id), address = f.alias(app.id);
  for (let i = 0; i < 210; i++) {
    f.call('apps.update', { appId: app.id, grants: { accountIds: i % 2 ? [reader.accountId] : [reader.accountId, later.accountId] } });
    const id = f.context(canonical).context.conversationId;
    f.send(canonical, id, `Never public ${i}`);
  }
  f.publish(app.id, [address]); const scope = f.entry(app.id, address);
  const hidden = f.archives({ ...scope, limit: 1 }, other);
  assert.deepEqual(hidden, { entries: [], nextCursor: null, resetRequired: false });
  const publicId = f.context(scope, other).context.conversationId; f.send(scope, publicId, 'One visible prior conversation', other);
  f.publish(app.id, [], 'restricted'); f.publish(app.id, [address]);
  const visible = f.archives({ ...scope, limit: 1 }, other);
  assert.equal(visible.entries.length, 1); assert.equal(visible.entries[0].conversationId, publicId);
  assert.equal(visible.nextCursor, null); safe(visible);
});

test('D2 opaque archive cursor size does not disclose unmaterialized audience generation digits', async t => {
  const f = await environment(t), app = f.app(), address = f.alias(app.id), scope = f.entry(app.id, address);
  f.publish(app.id, [address]); let id = f.context(scope, other).context.conversationId;
  f.send(scope, id, 'Visible first archive', other);
  f.publish(app.id, [], 'restricted'); f.publish(app.id, [address]);
  id = f.context(scope, other).context.conversationId; f.send(scope, id, 'Visible second archive', other);
  f.publish(app.id, [], 'restricted'); f.publish(app.id, [address]);
  const before = f.archives({ ...scope, limit: 1 }, other); assert.ok(before.nextCursor);
  for (let i = 0; i < 105; i++) f.call('apps.update', { appId: app.id,
    grants: { accountIds: i % 2 ? [reader.accountId] : [reader.accountId, later.accountId] } });
  const after = f.archives({ ...scope, limit: 1 }, other); assert.deepEqual(after.entries, before.entries);
  assert.equal(after.nextCursor.length, before.nextCursor.length, 'ciphertext length must not encode hidden generation digit count');
});

const duplicateWriter = String.raw`
  import { existsSync, writeFileSync } from 'node:fs';
  import { join } from 'node:path';
  const c = JSON.parse(process.argv[1]), mark = (name, value = {}) => writeFileSync(join(c.directory, c.label + '-' + name), JSON.stringify(value));
  const { createAppsService } = await import(c.appsModule);
  const { createWorldService } = await import(c.worldModule);
  let world, apps;
  try {
    world = createWorldService({ databasePath: c.worldPath, projectId: 'discussion_acceptance' });
    apps = createAppsService({ databasePath: c.databasePath, appOriginTemplate: c.appOriginTemplate, namedAppZone: c.namedAppZone,
      shellOrigins: c.shellOrigins, now: () => 1800000090000, actorActive: () => true,
      canAccessCommunity: (a, id) => world.canAccessCommunity(a, id), isGroupAdmin: (a, id) => world.isGroupAdmin(a, id),
      withAuthorityFence: fn => world.withCommunityAuthorityFence(fn),
      readCommunityAuthority: (a, owner, ids) => world.appCommunityAuthority(a.accountId, owner, ids) });
    mark('ready'); const end = Date.now() + 10000;
    while (!existsSync(join(c.directory, 'writers-start'))) {
      if (Date.now() > end) throw new Error('writer_start_timeout'); Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 5);
    }
    let result;
    for (let i = 0; i < 20; i++) {
      try { result = apps.execute({ op: 'apps.discussion.send', args: c.args, actor: c.actor }); break; }
      catch (error) { if (!['apps_discussion_busy', 'world_authority_busy'].includes(error.code)) throw error; Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 20); }
    }
    if (!result) throw new Error('writer_never_admitted'); mark('done', result);
  } catch (error) { mark('failed', { code: error.code || error.message }); process.exitCode = 1; }
  finally { apps?.close(); world?.close(); }
`;
function worker(f, label, args) {
  const config = { label, directory: f.directory, databasePath: f.databasePath, worldPath: f.worldPath,
    appOriginTemplate: f.config.appOriginTemplate, namedAppZone: f.config.namedAppZone, shellOrigins: f.config.shellOrigins,
    args: { ...args, expectedAccountId: reader.accountId }, actor: reader,
    appsModule: new URL('../server/index.mjs', import.meta.url).href, worldModule: new URL('../../world/server/index.mjs', import.meta.url).href };
  const env = Object.fromEntries(['SystemRoot', 'WINDIR', 'TEMP', 'TMP'].filter(name => process.env[name] !== undefined).map(name => [name, process.env[name]]));
  const child = spawn(process.execPath, ['--input-type=module', '-e', duplicateWriter, JSON.stringify(config)], { windowsHide: true, env, stdio: ['ignore', 'ignore', 'pipe'] });
  let stderr = ''; child.stderr.on('data', bytes => { stderr = (stderr + bytes.toString()).slice(-4000); });
  child.finished = new Promise((done, reject) => { child.once('error', reject); child.once('exit', status => done({ status, stderr })); });
  f.children.add(child); return child;
}

test('D2 two actual SQLite writers admitting the same intent produce one durable message and the same receipt', { timeout: 20000 }, async t => {
  const f = await environment(t), app = f.app(), scope = f.entry(app.id), id = f.context(scope).context.conversationId;
  const intent = f.sendArgs(scope, id, 'Exactly one cross-process post');
  const a = worker(f, 'a', intent); await until(() => existsSync(join(f.directory, 'a-ready')), 'writer A open');
  const b = worker(f, 'b', intent); await until(() => existsSync(join(f.directory, 'b-ready')), 'writer B open');
  writeFileSync(join(f.directory, 'writers-start'), 'go');
  assert.equal((await a.finished).status, 0); assert.equal((await b.finished).status, 0);
  const resultA = JSON.parse(readFileSync(join(f.directory, 'a-done'), 'utf8')), resultB = JSON.parse(readFileSync(join(f.directory, 'b-done'), 'utf8'));
  assert.deepEqual(resultA.receipt, resultB.receipt); assert.notEqual(resultA.replayed, resultB.replayed);
  const current = f.context(scope); assert.equal(current.messages.length, 1); assert.equal(current.messages[0].body, intent.body);
});
