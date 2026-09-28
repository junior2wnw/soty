import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, sign } from 'node:crypto';
import { createServer } from 'node:http';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { createConnectService, digestArgs } from '../../connect/server/index.mjs';
import { createConnectHandler } from '../../connect/server/http.mjs';
import { createWorldService } from '../server/index.mjs';
import { asDataUrl, pngFixture } from './avatar-fixtures.mjs';

const ORIGIN = 'https://world.test';
function identity(label) {
  const signing = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { label, signing: signing.privateKey, publicJwk: signing.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) };
}
function fixture(t) {
  const base = resolve(tmpdir()); const directory = mkdtempSync(join(base, 'soty-world-connect-test-'));
  const options = { projectId: 'world-connect-test', clock: () => 1_800_000_000_000 };
  const world = createWorldService({ ...options, databasePath: join(directory, 'world.sqlite') });
  const connect = createConnectService({ ...options, databasePath: join(directory, 'connect.sqlite'), allowedOrigins: [ORIGIN], extensions: [world],
    canRequestContact: (actorId, targetId) => world.canRequestContact(actorId, targetId) });
  t.after(() => {
    connect.close(); world.close(); assert.equal(dirname(resolve(directory)), base);
    assert.ok(resolve(directory).startsWith(join(base, 'soty-world-connect-test-'))); rmSync(directory, { recursive: true, force: true });
  });
  const send = input => connect.handle({ ...input, origin: ORIGIN });
  return { world, connect, send };
}
async function proof(send, actor, op, args = {}) {
  const challenge = await send({ op: 'challenge', args: { operation: op, digest: digestArgs(args) } });
  assert.equal(challenge.ok, true, challenge.error?.code);
  return { op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
    signature: sign('sha256', Buffer.from(challenge.message, 'utf8'), { key: actor.signing, dsaEncoding: 'ieee-p1363' }).toString('base64url') } };
}
const call = async (send, actor, op, args = {}) => send(await proof(send, actor, op, args));
function good(result) { assert.equal(result.ok, true, result.error?.code); return result; }
function error(result, code) { assert.equal(result.ok, false); assert.equal(result.error.code, code); }
async function boot(send, actor) { return good(await call(send, actor, 'bootstrap', { label: actor.label, encryptionPublicJwk: actor.encryptionPublicJwk })); }

test('two independently signed accounts complete discovery, request, approval, chat and revocation without a shared room secret', async t => {
  const { send, world } = fixture(t); const alice = identity('Алексей'); const bob = identity('Маша');
  const a = await boot(send, alice); const b = await boot(send, bob); assert.notEqual(a.accountId, b.accountId);
  good(await call(send, alice, 'world.profile.get')); good(await call(send, bob, 'world.profile.get'));
  good(await call(send, alice, 'world.profile.update', { expectedRevision: 1, discoverable: true }));
  const created = good(await call(send, alice, 'world.community.create', { requestId: 'real_create_001', name: 'Фотоклуб', joinPolicy: 'request' }));
  const communityId = created.community.communityId;
  const found = good(await call(send, bob, 'world.discovery.search', { query: 'фотоклуб' }));
  assert.equal(found.communities[0].communityId, communityId);
  assert.equal(good(await call(send, bob, 'world.membership.join', { communityId })).community.membership.state, 'requested');
  error(await call(send, bob, 'world.chat.list', { communityId }), 'community_membership_required');
  good(await call(send, alice, 'world.membership.decide', { communityId, profileId: b.accountId, accept: true }));
  good(await call(send, bob, 'world.chat.send', { communityId, clientId: 'signed_message_001', text: 'Галерея открылась' }));
  const result = good(await call(send, alice, 'world.chat.list', { communityId }));
  assert.equal(result.messages[0].author.profileId, b.accountId); assert.equal(result.messages[0].text, 'Галерея открылась');
  good(await call(send, alice, 'world.membership.ban', { communityId, profileId: b.accountId }));
  assert.equal(world.canAccessCommunity(b.accountId, communityId), false);
  error(await call(send, bob, 'world.chat.list', { communityId }), 'community_membership_required');
  error(await call(send, bob, 'world.chat.send', { communityId, clientId: 'signed_message_002', text: 'Denied' }), 'community_membership_required');
  assert.equal(good(await call(send, alice, 'world.chat.list', { communityId })).messages.length, 1);
});

test('world authority comes from verified active installation; payload spoofing, changed signatures, replay and revoked devices fail', async t => {
  const { send } = fixture(t); const alice = identity('Алексей'); const bob = identity('Маша'); const phone = identity('Телефон');
  const a = await boot(send, alice); const b = await boot(send, bob);
  error(await call(send, alice, 'world.profile.get', { accountId: b.accountId }), 'invalid_arguments');
  error(await call(send, phone, 'world.profile.get'), 'authentication_required');
  const signed = await proof(send, alice, 'world.profile.get');
  const tampered = { ...signed, proof: { ...signed.proof, publicJwk: bob.publicJwk } };
  error(await send(tampered), 'invalid_signature');
  good(await send(signed)); error(await send(signed), 'challenge_consumed');
  const enrollment = good(await call(send, phone, 'enrollment.start', { label: phone.label, encryptionPublicJwk: phone.encryptionPublicJwk }));
  good(await call(send, alice, 'enrollment.approve', { requestId: enrollment.requestId, wrappedKey: { schema: 'test.encrypted', ciphertext: 'opaque-test-data' } }));
  const joined = good(await call(send, phone, 'enrollment.finish', { requestId: enrollment.requestId, expectedAccountId: a.accountId }));
  assert.equal(good(await call(send, phone, 'world.profile.get')).profile.profileId, a.accountId);
  good(await call(send, alice, 'device.revoke', { deviceId: joined.deviceId }));
  error(await call(send, phone, 'world.profile.get'), 'device_revoked');
});

test('Connect account contact request enforces World audience policy and does not export contact cards', async t => {
  const { send } = fixture(t); const alice = identity('Алексей'); const bob = identity('Маша');
  await boot(send, alice); const b = await boot(send, bob);
  good(await call(send, alice, 'world.profile.get')); good(await call(send, bob, 'world.profile.get'));
  const rejected = await call(send, alice, 'contacts.requestAccount', { accountId: b.accountId });
  assert.equal(rejected.ok, false);
  good(await call(send, bob, 'world.profile.update', { expectedRevision: 1, discoverable: true, contactPolicy: 'nobody' }));
  assert.equal((await call(send, alice, 'contacts.requestAccount', { accountId: b.accountId })).ok, false);
  good(await call(send, bob, 'world.profile.update', { expectedRevision: 2, contactPolicy: 'everyone' }));
  const request = good(await call(send, alice, 'contacts.requestAccount', { accountId: b.accountId }));
  assert.equal(request.status, 'pending'); assert.equal(Object.hasOwn(request, 'cardId'), false);
  const profile = good(await call(send, alice, 'world.profile.view', { profileId: b.accountId }));
  assert.equal(Object.hasOwn(profile.profile, 'cardId'), false); assert.equal(profile.canRequestContact, true);
});

test('actual HTTP adapter carries two signed users through community chat and returns fixed authorization errors', async t => {
  const { connect } = fixture(t); const handler = createConnectHandler(connect);
  const server = createServer((req, res) => { handler(req, res).catch(() => { res.statusCode = 500; res.end(); }); });
  await new Promise(resolveListen => server.listen(0, '127.0.0.1', resolveListen));
  t.after(() => new Promise(resolveClose => server.close(resolveClose)));
  const endpoint = `http://127.0.0.1:${server.address().port}/api/connect/rpc`;
  const send = async input => {
    const response = await fetch(endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json', Origin: ORIGIN }, body: JSON.stringify({ protocol: 1, ...input }) });
    assert.equal(response.headers.get('cache-control'), 'no-store'); return response.json();
  };
  const alice = identity('Алексей'); const bob = identity('Маша'); const account = await boot(send, alice); await boot(send, bob);
  const raster = asDataUrl(pngFixture());
  good(await call(send, alice, 'world.profile.avatar.set', { expectedRevision: 1, avatarUrl: raster, thumbnailUrl: raster }));
  error(await call(send, bob, 'world.profile.avatar.read', { profileId: account.accountId }), 'profile_not_found');
  good(await call(send, alice, 'world.profile.update', { expectedRevision: 2, discoverable: true }));
  assert.equal(good(await call(send, bob, 'world.profile.avatars', { profileIds: [account.accountId] })).avatars[0].avatarUrl, raster);
  good(await call(send, alice, 'world.profile.update', { expectedRevision: 3, discoverable: false }));
  assert.equal(good(await call(send, bob, 'world.profile.avatars', { profileIds: [account.accountId] })).avatars.length, 0);
  const group = good(await call(send, alice, 'world.community.create', { requestId: 'http_create_001', name: 'HTTP группа' })).community;
  error(await call(send, bob, 'world.chat.list', { communityId: group.communityId }), 'community_membership_required');
  good(await call(send, bob, 'world.membership.join', { communityId: group.communityId }));
  good(await call(send, bob, 'world.chat.send', { communityId: group.communityId, clientId: 'http_send_001', text: 'Общее приложение работает' }));
  assert.equal(good(await call(send, alice, 'world.chat.list', { communityId: group.communityId })).messages[0].text, 'Общее приложение работает');
});
