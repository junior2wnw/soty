import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createECDH, generateKeyPairSync, sign, randomBytes } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import webPush from 'web-push';
import { createRootStartup } from '../startup.js';
import { digestArgs } from '../../modules/connect/server/index.mjs';

const good = value => { assert.equal(value.ok, true, value.error?.code); return value; };
async function fixture(t, notifications) {
  const directory = await mkdtemp(path.join(tmpdir(), 'soty-core-installation-'));
  const dataDir = path.join(directory, 'data'), dist = path.join(directory, 'dist');
  await mkdir(dist); await writeFile(path.join(dist, 'index.html'), '<title>Owned core fixture</title>');
  const origin = 'https://fixture.soty.invalid', roots = [];
  t.after(async () => {
    for (const item of roots.reverse()) await item.close();
    assert.equal(path.dirname(directory), path.resolve(tmpdir())); assert.ok(path.basename(directory).startsWith('soty-core-installation-'));
    await rm(directory, { recursive: true, force: true, maxRetries: 3 });
  });
  const open = async enabled => {
    const root = await createRootStartup({ rootDir: directory,
      env: { HOST: '127.0.0.1', PORT: '0', DATA_DIR: dataDir, SOTY_DIST_DIR: dist, SOTY_WORLD7_MIGRATION: enabled ? '1' : '0' },
      httpOptions: { connectOrigins: [origin], ...(notifications ? { notifications } : {}) } });
    roots.push(root); await root.start(); return root;
  };
  let root = await open(false);
  const rpc = async (actor, op, args = {}) => {
    const endpoint = 'http://127.0.0.1:' + root.server.address().port + '/api/connect/rpc';
    const post = value => fetch(endpoint, { method: 'POST', headers: { Origin: origin, 'content-type': 'application/json' },
      body: JSON.stringify({ protocol: 1, ...value }) }).then(reply => reply.json());
    const challenge = good(await post({ op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
    return post({ op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.jwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: actor.key, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  };
  const actor = async () => {
    const pair = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), value = { key: pair.privateKey, jwk: pair.publicKey.export({ format: 'jwk' }) };
    Object.assign(value, good(await rpc(value, 'bootstrap', { label: 'Owned core fixture', encryptionPublicJwk: value.jwk }))); return value;
  };
  const own = (actor, op, args = {}) => rpc(actor, op, { expectedAccountId: actor.accountId, ...args });
  return { directory, dataDir, dist, actor, rpc, own, get root() { return root; },
    async reopen(enabled) { await root.close(); root = await open(enabled); return root; } };
}
function subscription() {
  const key = createECDH('prime256v1'); key.generateKeys();
  return { endpoint: 'https://fcm.googleapis.com/fcm/send/' + randomBytes(18).toString('base64url'), expirationTime: null,
    keys: { p256dh: key.getPublicKey().toString('base64url'), auth: randomBytes(16).toString('base64url') } };
}

test('actual stock Root mounts signed notification status while OFF and leaves World6 and push storage untouched', async t => {
  const f = await fixture(t), alice = await f.actor();
  const status = good(await f.own(alice, 'notifications.status'));
  assert.equal(status.configured, false); assert.equal(status.enabled, false); assert.equal(status.publicKey, undefined);
  assert.equal(existsSync(path.join(f.dataDir, 'push', 'push.sqlite')), false);
  assert.equal(f.root.app.locals.worldService.schemaVersion, 6);
  const profile = good(await f.rpc(alice, 'world.profile.get')).profile;
  assert.equal(profile.chatAttachmentsVersion, undefined);
  const response = await fetch('http://127.0.0.1:' + f.root.server.address().port + '/api/apps/capabilities');
  assert.deepEqual((await response.json()).notifications, { configured: false });
});

test('actual Root message commit reaches push; a signed click survives read/mute but not leave or another account', async t => {
  const deliveries = [], f = await fixture(t, { configuration: { enabled: true, ...webPush.generateVAPIDKeys(), subject: 'mailto:fixture@example.invalid' },
    sendNotification: async (_subscription, payload) => { deliveries.push(JSON.parse(payload)); } });
  const alice = await f.actor(), bob = await f.actor();
  good(await f.own(bob, 'notifications.subscribe', { subscription: subscription() }));
  const room = good(await f.rpc(alice, 'world.community.create', { requestId: 'core_root_room', name: 'Owned room' })).community;
  good(await f.rpc(bob, 'world.membership.join', { communityId: room.communityId }));
  const send = { communityId: room.communityId, clientId: 'core_root_message', text: 'Private text stays out of push' };
  const message = good(await f.rpc(alice, 'world.chat.send', send)).message;
  await f.root.app.locals.pushService.drain(); assert.equal(deliveries.length, 1);
  assert.deepEqual(Object.keys(deliveries[0]).sort(), ['bindingId', 'notificationId', 'schema']);
  const notificationId = deliveries[0].notificationId;
  good(await f.rpc(bob, 'world.chat.read', { communityId: room.communityId, throughSeq: message.seq }));
  good(await f.rpc(bob, 'world.membership.preferences', { communityId: room.communityId, muted: true }));
  assert.deepEqual(good(await f.own(bob, 'notifications.open', { notificationId })).target, { kind: 'room', communityId: room.communityId });
  assert.equal((await f.own(alice, 'notifications.open', { notificationId })).ok, false);
  good(await f.rpc(alice, 'world.chat.send', send)); await f.root.app.locals.pushService.drain(); assert.equal(deliveries.length, 1);
  good(await f.rpc(bob, 'world.membership.leave', { communityId: room.communityId }));
  assert.equal((await f.own(bob, 'notifications.open', { notificationId })).ok, false);
  await f.root.close();
  assert.throws(() => f.root.app.locals.pushService.execute({ op: 'notifications.status', actor: bob,
    args: { expectedAccountId: bob.accountId } }), { code: 'push_actor_denied' });
});

test('actual Root migrates World6 only under the explicit flag and the compatible default reader reopens World7', async t => {
  const f = await fixture(t), alice = await f.actor();
  const room = good(await f.rpc(alice, 'world.community.create', { requestId: 'core_root_attachment_room', name: 'Owned file room' })).community;
  const scope = { expectedAccountId: alice.accountId, communityId: room.communityId };
  assert.equal(good(await f.rpc(alice, 'world.attachment.capabilities', scope)).writable, false);
  await f.reopen(true);
  assert.equal(f.root.app.locals.worldService.schemaVersion, 7);
  assert.equal(good(await f.rpc(alice, 'world.profile.get')).profile.chatAttachmentsVersion, 1);
  assert.equal(good(await f.rpc(alice, 'world.attachment.capabilities', scope)).writable, true);
  await f.reopen(false);
  assert.equal(f.root.app.locals.worldService.schemaVersion, 7);
  assert.equal(good(await f.rpc(alice, 'world.attachment.capabilities', scope)).readable, true);
  const db = new DatabaseSync(path.join(f.dataDir, 'world', 'world.sqlite'), { readOnly: true });
  try { assert.equal(db.prepare('PRAGMA user_version').get().user_version, 7);
    assert.equal(db.prepare('SELECT COUNT(*) AS n FROM world_chat_attachments').get().n, 0); }
  finally { db.close(); }
});
