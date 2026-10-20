import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, sign } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { createConnectService, digestArgs } from '../../connect/server/index.mjs';
import { createWorldService } from '../server/index.mjs';
import { asDataUrl, pngFixture, jpegFixture } from './avatar-fixtures.mjs';

const PROJECT = 'signed-world-audiences', ORIGIN = 'https://world-audiences.test';
const image = asDataUrl(jpegFixture, 'image/jpeg'), thumbnail = asDataUrl(pngFixture());
const good = result => { assert.equal(result.ok, true, result.error?.code); return result; };
const error = (result, code) => { assert.equal(result.ok, false); assert.equal(result.error.code, code); };
function identity(label) {
  const signing = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { label, signing: signing.privateKey, publicJwk: signing.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) };
}
function minimal(value, accountId) {
  assert.deepEqual(Object.keys(value).sort(), ['avatarColor', 'avatarRevision', 'displayName', 'profileId', 'revision']);
  assert.equal(value.profileId, accountId);
}
function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-world-audience-connect-'));
  const worldPath = join(directory, 'world.sqlite'), connectPath = join(directory, 'connect.sqlite');
  let world, connect, beforeWorld = null;
  const open = () => {
    world = createWorldService({ databasePath: worldPath, projectId: PROJECT,
      withCurrentContacts: (actor, callback) => connect.withCurrentContacts(actor, view => { beforeWorld?.(view); return callback(view); }) });
    connect = createConnectService({ databasePath: connectPath, projectId: PROJECT, allowedOrigins: [ORIGIN], extensions: [world],
      canRequestContact: (actorId, targetId) => world.canRequestContact(actorId, targetId) });
  };
  open();
  t.after(() => { connect.close(); world.close(); rmSync(directory, { recursive: true, force: true }); });
  const send = input => connect.handle({ ...input, origin: ORIGIN });
  async function call(actor, op, args = {}) {
    const challenge = good(await send({ op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
    return send({ op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message, 'utf8'), { key: actor.signing, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  }
  const w = (actor, op, args = {}) => call(actor, 'world.' + op, args);
  const update = async (actor, values) => good(await w(actor, 'profile.update', { expectedRevision: good(await w(actor, 'profile.get')).profile.revision, ...values })).profile;
  const boot = async actor => {
    const reply = good(await call(actor, 'bootstrap', { label: actor.label, encryptionPublicJwk: actor.encryptionPublicJwk }));
    good(await w(actor, 'profile.get')); return reply;
  };
  const request = async (sender, recipient) => good(await call(sender, 'contacts.request', { cardId: good(await call(recipient, 'card.get')).cardId }));
  const accept = async (sender, recipient) => good(await call(recipient, 'contacts.accept', { requestId: (await request(sender, recipient)).requestId }));
  const resolve = async (actor, own, target) => good(await w(actor, 'directory.resolve', { expectedAccountId: own.accountId, entities: [{ kind: 'person', id: target.accountId }] })).items[0];
  const search = async (actor, own, args = {}) => good(await w(actor, 'directory.search', { expectedAccountId: own.accountId, kind: 'people', ...args }));
  return { call, w, boot, update, request, accept, resolve, search, worldPath, connectPath,
    get world() { return world; }, beforeWorld(callback) { beforeWorld = callback; },
    reopen() { connect.close(); world.close(); open(); },
  };
}

test('real signed current contacts, pending acceptance, removal, block and restart fence every full profile/directory/avatar read', async t => {
  const f = fixture(t), owner = identity('Владелец'), friend = identity('Друг'), outsider = identity('Внешний');
  const a = await f.boot(owner), b = await f.boot(friend); await f.boot(outsider);
  await f.update(owner, { audience: 'friends', displayName: 'Уникальный профиль', bio: 'Только текущим друзьям', interests: ['точность'] });
  good(await f.w(owner, 'profile.avatar.set', { expectedRevision: good(await f.w(owner, 'profile.get')).profile.revision, avatarUrl: image, thumbnailUrl: thumbnail }));
  const deniedNow = async () => {
    error(await f.w(friend, 'profile.view', { profileId: a.accountId }), 'profile_not_found');
    assert.equal((await f.resolve(friend, b, a)).available, false);
    error(await f.w(friend, 'profile.avatar.read', { profileId: a.accountId }), 'profile_not_found');
    assert.deepEqual(good(await f.w(friend, 'profile.avatars', { profileIds: [a.accountId] })).avatars, []);
    assert.equal((await f.search(friend, b, { query: 'Уникальный' })).people.length, 0);
  };
  const allowedNow = async () => {
    assert.equal(good(await f.w(friend, 'profile.view', { profileId: a.accountId })).profile.bio, 'Только текущим друзьям');
    const resolved = await f.resolve(friend, b, a); assert.equal(resolved.access, 'friends'); assert.deepEqual(resolved.person.interests, ['точность']);
    assert.equal(good(await f.w(friend, 'profile.avatar.read', { profileId: a.accountId })).avatarUrl, image);
    assert.equal(good(await f.w(friend, 'profile.avatars', { profileIds: [a.accountId] })).avatars[0].avatarUrl, thumbnail);
    for (const scope of ['all', 'mine']) assert.deepEqual((await f.search(friend, b, { scope, query: 'Уникальный' })).people.map(person => person.profileId), [a.accountId]);
    assert.equal((await f.search(friend, b, { scope: 'public', query: 'Уникальный' })).people.length, 0);
  };
  await deniedNow(); const pending = await f.request(owner, friend); await deniedNow();
  const active = good(await f.call(friend, 'contacts.accept', { requestId: pending.requestId })); await allowedNow();
  error(await f.w(outsider, 'profile.view', { profileId: a.accountId }), 'profile_not_found');
  assert.equal(good(await f.w(friend, 'discovery.search', { query: 'Уникальный' })).totals.people, 0);
  good(await f.call(friend, 'contacts.remove', { relationshipId: active.relationshipId })); await deniedNow();
  await f.accept(owner, friend); await allowedNow();
  good(await f.call(owner, 'contacts.block', { peerAccountId: b.accountId })); await deniedNow();
  good(await f.call(owner, 'contacts.unblock', { peerAccountId: b.accountId })); await deniedNow();
  await f.accept(owner, friend); f.reopen(); await allowedNow();
  await f.update(owner, { audience: 'public' });
  assert.deepEqual((await f.search(friend, b, { scope: 'mine', query: 'Уникальный' })).people.map(person => person.profileId), [a.accountId]);
  await f.update(owner, { audience: 'link' });
  for (const scope of ['all', 'mine', 'public']) assert.deepEqual((await f.search(friend, b, { scope, query: 'Уникальный' })).people, []);
  good(await f.w(friend, 'profile.view', { profileId: a.accountId }));
  await f.update(owner, { audience: 'private' }); await deniedNow();
});

test('same-group nonfriends and former friends get minimal new messages and roster identities, never hidden profile fields or full avatars', async t => {
  const f = fixture(t), owner = identity('Автор'), reader = identity('Участник');
  const a = await f.boot(owner), b = await f.boot(reader);
  await f.update(owner, { audience: 'friends', bio: 'Приватное описание автора', interests: ['тайна'] });
  good(await f.w(owner, 'profile.avatar.set', { expectedRevision: good(await f.w(owner, 'profile.get')).profile.revision, avatarUrl: image, thumbnailUrl: thumbnail }));
  const communityId = good(await f.w(owner, 'community.create', { requestId: 'signed_minimal_group', name: 'Общая группа' })).communityId;
  good(await f.w(reader, 'membership.join', { communityId }));
  const first = { communityId, clientId: 'signed_minimal_first', text: 'Первое' };
  minimal(good(await f.w(owner, 'chat.send', first)).message.author, a.accountId);
  minimal(good(await f.w(owner, 'chat.send', first)).message.author, a.accountId);
  error(await f.w(reader, 'profile.view', { profileId: a.accountId }), 'profile_not_found');
  const rosterOnly = await f.resolve(reader, b, a); assert.equal(rosterOnly.access, 'member'); minimal(rosterOnly.person, a.accountId);
  const relation = await f.accept(owner, reader);
  assert.equal(good(await f.w(reader, 'profile.view', { profileId: a.accountId })).profile.bio, 'Приватное описание автора');
  good(await f.call(reader, 'contacts.remove', { relationshipId: relation.relationshipId }));
  minimal(good(await f.w(owner, 'chat.send', { communityId, clientId: 'signed_minimal_after_remove', text: 'Новое после удаления' })).message.author, a.accountId);
  const messages = good(await f.w(reader, 'chat.list', { communityId })).messages;
  assert.equal(messages.length, 2); for (const message of messages) minimal(message.author, a.accountId);
  for (const member of good(await f.w(reader, 'membership.list', { communityId })).members) minimal(member.profile, member.profile.profileId);
  error(await f.w(reader, 'profile.view', { profileId: a.accountId }), 'profile_not_found');
  error(await f.w(reader, 'profile.avatar.read', { profileId: a.accountId, communityId }), 'profile_not_found');
  assert.equal(good(await f.w(reader, 'profile.avatars', { profileIds: [a.accountId], communityId })).avatars[0].avatarUrl, thumbnail);
  assert.deepEqual(good(await f.w(reader, 'profile.avatars', { profileIds: [a.accountId] })).avatars, []);
});

test('genuine Connect writer fence precedes the World transaction; a returned contact view expires and is not cached for later reads', async t => {
  const f = fixture(t), owner = identity('Автор'), friend = identity('Друг');
  const a = await f.boot(owner), b = await f.boot(friend); await f.update(owner, { audience: 'friends' }); await f.accept(owner, friend);
  const worldProbe = new DatabaseSync(f.worldPath), connectProbe = new DatabaseSync(f.connectPath);
  worldProbe.exec('PRAGMA busy_timeout=0'); connectProbe.exec('PRAGMA busy_timeout=0');
  let captured = null, entered = 0;
  f.beforeWorld(view => {
    entered++; captured = view;
    assert.throws(() => connectProbe.exec('BEGIN IMMEDIATE'), error => (Number(error.errcode) & 255) === 5);
    worldProbe.exec('BEGIN IMMEDIATE; ROLLBACK');
    assert.equal(view.accountId, b.accountId); assert.equal(view.has(a.accountId), true);
  });
  try {
    good(await f.w(friend, 'profile.view', { profileId: a.accountId })); assert.equal(entered, 1);
    assert.throws(() => captured.has(a.accountId), error => error.code === 'connect_contact_authority_expired');
    f.beforeWorld(null);
    good(await f.call(friend, 'contacts.block', { peerAccountId: a.accountId }));
    error(await f.w(friend, 'profile.view', { profileId: a.accountId }), 'profile_not_found');
  } finally { f.beforeWorld(null); connectProbe.close(); worldProbe.close(); }
});

test('signed World calls and host session DTOs retain device/account authority after enrollment, revocation and mismatched copies', async t => {
  const f = fixture(t), owner = identity('Владелец'), friend = identity('Друг'), phone = identity('Телефон');
  const a = await f.boot(owner), b = await f.boot(friend); await f.update(owner, { audience: 'friends' }); await f.accept(owner, friend);
  const enrollment = good(await f.call(phone, 'enrollment.start', { label: phone.label, encryptionPublicJwk: phone.encryptionPublicJwk }));
  good(await f.call(friend, 'enrollment.approve', { requestId: enrollment.requestId, wrappedKey: { schema: 'test.encrypted', ciphertext: 'synthetic-owned-fixture' } }));
  const mobile = good(await f.call(phone, 'enrollment.finish', { requestId: enrollment.requestId, expectedAccountId: b.accountId }));
  good(await f.w(phone, 'profile.view', { profileId: a.accountId }));
  error(await f.w(friend, 'profile.view', { profileId: a.accountId, accountId: a.accountId }), 'invalid_arguments');
  assert.throws(() => f.world.execute({ actor: { accountId: a.accountId, deviceId: b.deviceId, label: 'Подмена' }, op: 'world.profile.view', args: { profileId: a.accountId } }),
    error => error.code === 'connect_authority_actor_mismatch');
  good(await f.call(friend, 'device.revoke', { deviceId: mobile.deviceId }));
  error(await f.w(phone, 'profile.view', { profileId: a.accountId }), 'device_revoked');
  assert.throws(() => f.world.execute({ actor: { accountId: b.accountId, deviceId: mobile.deviceId, label: phone.label }, op: 'world.profile.view', args: { profileId: a.accountId } }),
    error => error.code === 'device_revoked');
  good(await f.w(friend, 'profile.view', { profileId: a.accountId }));
});

test('directory continuation rechecks current contacts instead of retaining a prior authorized candidate page', async t => {
  const f = fixture(t), viewer = identity('Наблюдатель'), left = identity('Слева'), right = identity('Справа');
  const own = await f.boot(viewer), a = await f.boot(left), b = await f.boot(right);
  await f.update(left, { audience: 'friends', displayName: 'Маркер левый' });
  await f.update(right, { audience: 'friends', displayName: 'Маркер правый' });
  const ra = await f.accept(left, viewer), rb = await f.accept(right, viewer);
  const first = await f.search(viewer, own, { query: 'Маркер', limit: 1 });
  assert.equal(first.people.length, 1); assert.equal(typeof first.nextCursor, 'string');
  const unseen = first.people[0].profileId === a.accountId ? { account: b, relation: rb } : { account: a, relation: ra };
  good(await f.call(viewer, 'contacts.remove', { relationshipId: unseen.relation.relationshipId }));
  const next = await f.search(viewer, own, { query: 'Маркер', limit: 1, cursor: first.nextCursor });
  assert.deepEqual(next.people, []); assert.equal(next.nextCursor, null);
  assert.equal((await f.resolve(viewer, own, unseen.account)).available, false);
  const current = await f.search(viewer, own, { query: 'Маркер' });
  assert.deepEqual(current.people.map(person => person.profileId), [first.people[0].profileId]);
});
