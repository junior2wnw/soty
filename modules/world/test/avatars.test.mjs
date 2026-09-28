import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { createWorldService, SCHEMA_VERSION } from '../server/index.mjs';
import { removeDiscoveryProjection } from './schema-fixtures.mjs';
import { validateAvatar, AVATAR_LIMITS } from '../server/avatars.mjs';
import { pngFixture, pngChunk, asDataUrl, jpegFixture, webpFixture } from './avatar-fixtures.mjs';

const image = asDataUrl(pngFixture());
const alice = { accountId: 'acct_avatar_alice', deviceId: 'dev_alice', label: 'Алексей' };
const bob = { accountId: 'acct_avatar_bob', deviceId: 'dev_bob', label: 'Маша' };
const outsider = { accountId: 'acct_avatar_outside', deviceId: 'dev_outside', label: 'Ирина' };
const limits = { maximumBytes: AVATAR_LIMITS.imageBytes, maximumPixels: AVATAR_LIMITS.imagePixels };
function denied(fn, code) { assert.throws(fn, error => error.code === code, code); }
function fixture(t) {
  const base = resolve(tmpdir()), dir = mkdtempSync(join(base, 'soty-avatars-test-')), path = join(dir, 'world.sqlite'); const services = [];
  const open = () => { const service = createWorldService({ databasePath: path, projectId: 'avatars' }); services.push(service); return service; };
  const service = open(); const call = (actor, op, args = {}) => service.execute({ actor, op: 'world.' + op, args });
  [alice, bob, outsider].forEach(actor => call(actor, 'profile.get'));
  t.after(() => { services.forEach(item => item.close()); assert.equal(dirname(resolve(dir)), base); assert.ok(resolve(dir).startsWith(join(base, 'soty-avatars-test-'))); rmSync(dir, { recursive: true, force: true }); });
  const set = (actor, avatarUrl = image, thumbnailUrl = image) => call(actor, 'profile.avatar.set', { expectedRevision: call(actor, 'profile.get').profile.revision, avatarUrl, thumbnailUrl });
  const update = (actor, values) => call(actor, 'profile.update', { expectedRevision: call(actor, 'profile.get').profile.revision, ...values });
  return { service, path, open, call, set, update };
}

test('raster avatars accept bounded PNG/JPEG/WebP with matching MIME and dimensions', () => {
  assert.deepEqual({ width: validateAvatar(image, limits).width, height: validateAvatar(image, limits).height }, { width: 2, height: 2 });
  assert.equal(validateAvatar(asDataUrl(jpegFixture, 'image/jpeg'), limits).width, 2);
  assert.equal(validateAvatar(asDataUrl(webpFixture, 'image/webp'), limits).width, 1);
});

test('avatar validation rejects external URLs, SVG, mismatched signatures, corrupt data, animation and oversized dimensions/bytes', () => {
  for (const invalid of ['https://tracker.invalid/avatar.png', 'data:image/svg+xml;base64,PHN2Zy8+', 'data:text/html;base64,PGgxLz4=']) denied(() => validateAvatar(invalid, limits), 'invalid_avatar_mime');
  denied(() => validateAvatar(asDataUrl(jpegFixture), limits), 'invalid_avatar_data');
  denied(() => validateAvatar(image + '=', limits), 'invalid_avatar_data');
  const corrupt = pngFixture(); corrupt[corrupt.length - 1] ^= 1;
  denied(() => validateAvatar(asDataUrl(corrupt), limits), 'invalid_avatar_data');
  denied(() => validateAvatar(asDataUrl(pngFixture(513, 1)), limits), 'avatar_dimensions');
  denied(() => validateAvatar(asDataUrl(Buffer.alloc(AVATAR_LIMITS.imageBytes + 1)), limits), 'avatar_too_large');
  const original = pngFixture(); const animated = Buffer.concat([original.subarray(0, 33), pngChunk('acTL', Buffer.alloc(8)), original.subarray(33)]);
  denied(() => validateAvatar(asDataUrl(animated), limits), 'invalid_avatar_data');
  const spoofed = Buffer.from(webpFixture); spoofed[0] |= 128;
  denied(() => validateAvatar(asDataUrl(spoofed, 'image/webp'), limits), 'invalid_avatar_data');
  const meta = Buffer.concat([jpegFixture.subarray(0, 2), Buffer.from([255, 225, 0, 6, 69, 120, 105, 102]), jpegFixture.subarray(2)]);
  denied(() => validateAvatar(asDataUrl(meta, 'image/jpeg'), limits), 'invalid_avatar_data');
});

test('avatar bytes never expand ordinary profile/discovery/chat projections; hidden reads and batches are authorized afresh', t => {
  const f = fixture(t); assert.equal(f.call(alice, 'profile.get').profile.avatarRevision, null);
  const changed = f.set(alice).profile; assert.equal(changed.avatarRevision, 2); assert.equal(Object.hasOwn(changed, 'avatarUrl'), false);
  assert.equal(f.call(alice, 'profile.avatar.read', { profileId: alice.accountId }).avatarUrl, image);
  denied(() => f.call(bob, 'profile.avatar.read', { profileId: alice.accountId }), 'profile_not_found');
  assert.equal(f.call(bob, 'profile.avatars', { profileIds: [alice.accountId] }).avatars.length, 0);
  f.update(alice, { discoverable: true });
  assert.equal(f.call(bob, 'profile.avatars', { profileIds: [alice.accountId] }).avatars[0].avatarRevision, 2);
  const search = f.call(bob, 'discovery.search'); assert.equal(JSON.stringify(search).includes('base64'), false);
  f.update(alice, { discoverable: false });
  assert.equal(f.call(bob, 'profile.avatars', { profileIds: [alice.accountId] }).avatars.length, 0);
  denied(() => f.call(bob, 'profile.avatar.read', { profileId: alice.accountId }), 'profile_not_found');
  denied(() => f.call(alice, 'profile.avatar.set', { expectedRevision: changed.revision, avatarUrl: image, thumbnailUrl: image }), 'revision_conflict');
});

test('hidden avatars are readable only in an authorized existing group context, including historical authors', t => {
  const f = fixture(t); f.set(bob);
  const communityId = f.call(alice, 'community.create', { requestId: 'avatars_group', name: 'Фотоклуб' }).communityId;
  f.call(bob, 'membership.join', { communityId }); f.call(bob, 'chat.send', { communityId, clientId: 'avatar_history', text: 'Снимок' });
  assert.equal(JSON.stringify(f.call(alice, 'chat.list', { communityId })).includes('base64'), false);
  assert.equal(f.call(alice, 'profile.avatars', { profileIds: [bob.accountId], communityId }).avatars.length, 1);
  denied(() => f.call(outsider, 'profile.avatars', { profileIds: [bob.accountId], communityId }), 'community_membership_required');
  f.call(bob, 'membership.leave', { communityId });
  assert.equal(f.call(alice, 'profile.avatars', { profileIds: [bob.accountId], communityId }).avatars.length, 1);
  denied(() => f.call(bob, 'profile.avatars', { profileIds: [alice.accountId], communityId }), 'community_membership_required');
});

test('avatar thumbnail batches and writes are bounded; deletion and restart preserve the exact state', t => {
  const f = fixture(t);
  denied(() => f.set(alice, image, asDataUrl(pngFixture(193, 1))), 'avatar_dimensions');
  assert.equal(f.call(alice, 'profile.get').profile.revision, 1);
  denied(() => f.set(alice, image, asDataUrl(Buffer.alloc(AVATAR_LIMITS.thumbnailBytes + 1))), 'avatar_too_large');
  denied(() => f.set(alice, image, null), 'invalid_avatar_data');
  denied(() => f.call(alice, 'profile.avatars', { profileIds: Array(25).fill(alice.accountId) }), 'invalid_avatar_batch');
  f.set(alice, asDataUrl(jpegFixture, 'image/jpeg'), image); f.service.close();
  const reopened = f.open(); const full = reopened.execute({ actor: alice, op: 'world.profile.avatar.read', args: { profileId: alice.accountId } });
  assert.equal(full.avatarUrl, asDataUrl(jpegFixture, 'image/jpeg'));
  const thumb = reopened.execute({ actor: alice, op: 'world.profile.avatars', args: { profileIds: [alice.accountId] } }); assert.equal(thumb.avatars[0].avatarUrl, image);
  reopened.execute({ actor: alice, op: 'world.profile.avatar.set', args: { expectedRevision: 2, avatarUrl: null, thumbnailUrl: null } });
  assert.equal(reopened.execute({ actor: alice, op: 'world.profile.avatar.read', args: { profileId: alice.accountId } }).avatarUrl, null);
});

test('schema v1 upgrades additively and active-community lookup changes immediately after leave/archive', t => {
  const f = fixture(t); const communityId = f.call(alice, 'community.create', { requestId: 'migration_group', name: 'Сохраняется' }).communityId;
  f.call(bob, 'membership.join', { communityId }); assert.deepEqual(f.service.activeCommunityIds(bob.accountId), [communityId]);
  f.call(bob, 'membership.leave', { communityId }); assert.deepEqual(f.service.activeCommunityIds(bob.accountId), []);
  f.service.close(); const db = new DatabaseSync(f.path);
  removeDiscoveryProjection(db);
  db.exec("DROP TABLE profile_avatars; ALTER TABLE profiles DROP COLUMN avatar_revision; PRAGMA user_version=1; UPDATE world_meta SET value='1' WHERE key='schema_version';"); db.close();
  const migrated = f.open(); assert.equal(migrated.schemaVersion, SCHEMA_VERSION);
  assert.equal(migrated.execute({ actor: alice, op: 'world.profile.get' }).profile.avatarRevision, null);
  assert.equal(migrated.execute({ actor: alice, op: 'world.community.get', args: { communityId } }).community.name, 'Сохраняется');
  migrated.execute({ actor: alice, op: 'world.community.archive', args: { communityId, expectedRevision: 1 } });
  assert.deepEqual(migrated.activeCommunityIds(alice.accountId), []);
});
