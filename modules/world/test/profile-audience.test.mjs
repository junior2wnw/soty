import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { createWorldService, SCHEMA_VERSION } from '../server/index.mjs';
import { migrateWorld as migrateV3 } from './support/legacy-world-schema-v3.mjs';
import { asDataUrl, pngFixture, jpegFixture } from './avatar-fixtures.mjs';

const PROJECT = 'world-audience-test';
const alice = Object.freeze({ accountId: 'audience_alice', deviceId: 'audience_alice_device', label: 'Алиса' });
const bob = Object.freeze({ accountId: 'audience_bob', deviceId: 'audience_bob_device', label: 'Борис' });
const eve = Object.freeze({ accountId: 'audience_eve', deviceId: 'audience_eve_device', label: 'Ева' });
const image = asDataUrl(jpegFixture, 'image/jpeg'), thumbnail = asDataUrl(pngFixture());
const identityKeys = ['avatarColor', 'avatarRevision', 'displayName', 'profileId', 'revision'];
const denied = (callback, code) => assert.throws(callback, error => error.code === code);
function minimal(value, accountId) {
  assert.deepEqual(Object.keys(value).sort(), identityKeys);
  assert.equal(value.profileId, accountId);
  assert.equal(Object.hasOwn(value, 'bio'), false); assert.equal(Object.hasOwn(value, 'interests'), false);
}
function fixture(t, options = {}) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-world-audience-')), databasePath = join(directory, 'world.sqlite');
  let service = createWorldService({ databasePath, projectId: PROJECT, ...options });
  const call = (actor, op, args = {}) => service.execute({ actor, op: 'world.' + op, args });
  const update = (actor, args) => call(actor, 'profile.update', { expectedRevision: call(actor, 'profile.get').profile.revision, ...args }).profile;
  const resolve = (actor, target) => call(actor, 'directory.resolve', { expectedAccountId: actor.accountId, entities: [{ kind: 'person', id: target.accountId }] }).items[0];
  const search = (actor, args = {}) => call(actor, 'directory.search', { expectedAccountId: actor.accountId, kind: 'people', ...args });
  for (const actor of [alice, bob, eve]) call(actor, 'profile.get');
  t.after(() => { service.close(); rmSync(directory, { recursive: true, force: true }); });
  return { databasePath, call, update, resolve, search,
    close() { service.close(); },
    reopen() { service.close(); service = createWorldService({ databasePath, projectId: PROJECT, ...options }); },
  };
}
function snapshot(db) {
  const names = db.prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name").all();
  return Object.fromEntries(names.map(({ name }) => [name, db.prepare(`SELECT * FROM "${name.replaceAll('"', '""')}"`).all()
    .map(row => JSON.stringify(Object.fromEntries(Object.entries(row).map(([key, value]) => [key, value instanceof Uint8Array ? Buffer.from(value).toString('hex') : value])))).sort()]));
}

test('World3 migrates to the unambiguous audience format5, preserving legacy identities, revisions, avatars and public index sequence', t => {
  const directory = mkdtempSync(join(tmpdir(), 'soty-world-audience-migration-')), databasePath = join(directory, 'world.sqlite');
  let service; t.after(() => { service?.close(); rmSync(directory, { recursive: true, force: true }); });
  const db = new DatabaseSync(databasePath); migrateV3(db, PROJECT);
  const insert = db.prepare('INSERT INTO profiles(account_id,display_name,bio,interests,search_text,discoverable,revision,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)');
  insert.run(alice.accountId, alice.label, 'Сохранённое описание', '["лес"]', 'алиса лес', 0, 17, 101, 202);
  insert.run(bob.accountId, bob.label, 'Публичное описание', '["фото"]', 'борис фото', 1, 23, 102, 203);
  db.prepare('INSERT INTO profile_avatars(account_id,mime,image,thumbnail_mime,thumbnail) VALUES(?,?,?,?,?)')
    .run(alice.accountId, 'image/jpeg', jpegFixture, 'image/png', pngFixture());
  const before = db.prepare('SELECT * FROM profiles ORDER BY account_id').all().map(row => ({ ...row }));
  const avatars = db.prepare('SELECT * FROM profile_avatars').all();
  const sequence = db.prepare('SELECT * FROM world_directory ORDER BY seq').all(); db.close();
  service = createWorldService({ databasePath, projectId: PROJECT, allowAudienceMigration: true }); assert.equal(SCHEMA_VERSION, 5); assert.equal(service.schemaVersion, 5);
  const observed = new DatabaseSync(databasePath, { readOnly: true });
  try {
    assert.equal(observed.prepare('PRAGMA user_version').get().user_version, 5);
    assert.equal(observed.prepare("SELECT value FROM world_meta WHERE key='schema_version'").get().value, '5');
    assert.deepEqual(observed.prepare('SELECT * FROM profiles ORDER BY account_id').all().map(({ audience, ...row }) => row), before);
    assert.deepEqual(observed.prepare('SELECT account_id,audience FROM profiles ORDER BY account_id').all().map(row => ({ ...row })),
      [{ account_id: alice.accountId, audience: 'private' }, { account_id: bob.accountId, audience: 'public' }]);
    assert.deepEqual(observed.prepare('SELECT * FROM profile_avatars').all(), avatars);
    assert.deepEqual(observed.prepare('SELECT * FROM world_directory ORDER BY seq').all(), sequence);
    const column = observed.prepare('PRAGMA table_info(profiles)').all().at(-1);
    assert.deepEqual({ name: column.name, type: column.type, notnull: column.notnull, dflt_value: column.dflt_value, pk: column.pk },
      { name: 'audience', type: 'TEXT', notnull: 1, dflt_value: "'private'", pk: 0 });
  } finally { observed.close(); }
  service.close(); service = null;
  const old = new DatabaseSync(databasePath), accepted = snapshot(old);
  try { assert.throws(() => migrateV3(old, PROJECT), /world_schema_unsupported/u); assert.deepEqual(snapshot(old), accepted); }
  finally { old.close(); }
  service = createWorldService({ databasePath, projectId: PROJECT });
  assert.equal(service.execute({ actor: alice, op: 'world.profile.get' }).profile.bio, 'Сохранённое описание');
});

test('Root audience reader rejects reserved attachment format4 without relabelling or modifying its store', t => {
  const directory = mkdtempSync(join(tmpdir(), 'soty-world-reserved-four-')), databasePath = join(directory, 'world.sqlite');
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const db = new DatabaseSync(databasePath); migrateV3(db, PROJECT);
  db.exec("CREATE TABLE chat_attachments(id TEXT PRIMARY KEY); CREATE TABLE chat_attachment_chunks(id TEXT PRIMARY KEY); PRAGMA user_version=4; UPDATE world_meta SET value='4' WHERE key='schema_version'");
  const before = snapshot(db); db.close();
  assert.throws(() => createWorldService({ databasePath, projectId: PROJECT }), /world_schema_unsupported/u);
  const after = new DatabaseSync(databasePath, { readOnly: true });
  try { assert.equal(after.prepare('PRAGMA user_version').get().user_version, 4); assert.deepEqual(snapshot(after), before); }
  finally { after.close(); }
});

test('World5 admission verifies the exact audience column and public-index trigger, not only the numeric label', async t => {
  for (const [name, sql] of [
    ['missing public trigger', 'DROP TRIGGER world_profile_directory_ai'],
    ['legacy public trigger', "DROP TRIGGER world_profile_directory_au; CREATE TRIGGER world_profile_directory_au AFTER UPDATE OF discoverable ON profiles BEGIN SELECT 1; END"],
    ['wrong column', 'ALTER TABLE profiles RENAME COLUMN audience TO unsupported_audience'],
    ['unexpected profile writer', 'CREATE TRIGGER unexpected_profile_writer AFTER UPDATE ON profiles BEGIN SELECT 1; END'],
    ['reserved attachment fragment', 'CREATE TABLE chat_attachments(id TEXT PRIMARY KEY)'],
    ['contradictory legacy projection', "UPDATE profiles SET discoverable=1 WHERE account_id='audience_alice'"],
  ]) await t.test(name, tt => {
    const f = fixture(tt); f.close(); const db = new DatabaseSync(f.databasePath); db.exec(sql); const before = snapshot(db); db.close();
    assert.throws(() => createWorldService({ databasePath: f.databasePath, projectId: PROJECT }), /world_schema_metadata_mismatch/u);
    const after = new DatabaseSync(f.databasePath, { readOnly: true });
    try { assert.deepEqual(snapshot(after), before); } finally { after.close(); }
  });
});

test('owner full profile and exact signed link are distinct from public discovery; missing contact authority never admits friends', t => {
  const f = fixture(t);
  f.call(alice, 'profile.avatar.set', { expectedRevision: 1, avatarUrl: image, thumbnailUrl: thumbnail });
  for (const audience of ['private', 'friends', 'link', 'public']) {
    const own = f.update(alice, { audience, bio: 'Аудитория описание', interests: ['уникальное'] });
    assert.equal(own.audience, audience); assert.equal(own.discoverable, audience === 'public');
    assert.equal(f.call(alice, 'profile.view', { profileId: alice.accountId }).profile.bio, own.bio);
    assert.equal(f.resolve(alice, alice).access, 'owner');
    if (['link', 'public'].includes(audience)) {
      assert.equal(f.call(bob, 'profile.view', { profileId: alice.accountId }).profile.bio, own.bio);
      assert.equal(f.resolve(bob, alice).access, audience);
      assert.equal(f.call(bob, 'profile.avatar.read', { profileId: alice.accountId }).avatarUrl, image);
      assert.equal(f.call(bob, 'profile.avatars', { profileIds: [alice.accountId] }).avatars[0].avatarUrl, thumbnail);
    } else {
      denied(() => f.call(bob, 'profile.view', { profileId: alice.accountId }), 'profile_not_found');
      assert.equal(f.resolve(bob, alice).available, false);
      denied(() => f.call(bob, 'profile.avatar.read', { profileId: alice.accountId }), 'profile_not_found');
      assert.deepEqual(f.call(bob, 'profile.avatars', { profileIds: [alice.accountId] }).avatars, []);
    }
    for (const query of ['Аудитория', alice.accountId, 'уникальное']) {
      const global = f.call(bob, 'discovery.search', { query });
      assert.equal(global.people.some(person => person.profileId === alice.accountId), audience === 'public');
      assert.equal(f.search(bob, { query }).people.some(person => person.profileId === alice.accountId), audience === 'public');
    }
    assert.equal(f.call(bob, 'discovery.search').totals.people, Number(audience === 'public'));
  }
});

test('profile CAS, legacy booleans and consistent double fields survive restart; conflicting fields write nothing', t => {
  const f = fixture(t), first = f.update(alice, { audience: 'friends', bio: 'Сохраняется' });
  denied(() => f.call(alice, 'profile.update', { expectedRevision: first.revision - 1, audience: 'public' }), 'revision_conflict');
  denied(() => f.call(alice, 'profile.update', { expectedRevision: first.revision, audience: 'friends', discoverable: true }), 'profile_audience_conflict');
  denied(() => f.call(alice, 'profile.update', { expectedRevision: first.revision, audience: 'strangers' }), 'invalid_choice');
  assert.deepEqual(f.call(alice, 'profile.get').profile, first);
  f.reopen(); assert.deepEqual(f.call(alice, 'profile.get').profile, first);
  assert.equal(f.update(alice, { bio: 'Другое описание' }).audience, 'friends');
  assert.equal(f.update(alice, { discoverable: true }).audience, 'public');
  assert.equal(f.update(alice, { discoverable: false }).audience, 'private');
  assert.equal(f.update(alice, { audience: 'link', discoverable: false }).audience, 'link');
  assert.equal(f.update(alice, { audience: 'public', discoverable: true }).audience, 'public');
});

test('public FTS and exact global counts contain only public rows while owner directory remains available', t => {
  const f = fixture(t);
  f.update(alice, { audience: 'public', displayName: 'Общий маркер' });
  f.update(bob, { audience: 'link', displayName: 'Общий маркер' });
  f.update(eve, { audience: 'friends', displayName: 'Общий маркер' });
  const db = new DatabaseSync(f.databasePath, { readOnly: true });
  try {
    assert.deepEqual(db.prepare("SELECT entity_id FROM world_directory WHERE kind='person'").all().map(row => row.entity_id), [alice.accountId]);
    assert.equal(db.prepare("SELECT n FROM world_directory_counts WHERE kind='person'").get().n, 1);
    assert.deepEqual(db.prepare("SELECT d.entity_id FROM world_directory_fts JOIN world_directory d ON d.seq=world_directory_fts.rowid WHERE world_directory_fts MATCH 'маркер*'").all().map(row => row.entity_id), [alice.accountId]);
  } finally { db.close(); }
  assert.deepEqual(f.search(bob, { query: 'маркер', scope: 'public' }).people.map(person => person.profileId), [alice.accountId]);
  assert.deepEqual(f.search(bob, { query: 'маркер', scope: 'mine' }).people.map(person => person.profileId), [bob.accountId]);
});

test('chat send, replay, list, removed-message replay and every roster state disclose only explicit minimal identity', t => {
  const f = fixture(t);
  f.update(alice, { discoverable: true, bio: 'Описание владельца', interests: ['владелец'] });
  f.update(bob, { discoverable: false, bio: 'Секрет Бориса', interests: ['скрыто'] });
  f.update(eve, { discoverable: false, bio: 'Секрет Евы', interests: ['закрыто'] });
  const group = f.call(alice, 'community.create', { requestId: 'audience_minimal_group', name: 'Клуб' }).community;
  const communityId = group.communityId; f.call(bob, 'membership.join', { communityId });
  const args = { communityId, clientId: 'audience_minimal_message', text: 'Сообщение' };
  const sent = f.call(bob, 'chat.send', args).message; minimal(sent.author, bob.accountId);
  minimal(f.call(bob, 'chat.send', args).message.author, bob.accountId);
  minimal(f.call(alice, 'chat.list', { communityId }).messages[0].author, bob.accountId);
  for (const member of f.call(alice, 'membership.list', { communityId }).members) minimal(member.profile, member.profile.profileId);
  f.call(alice, 'membership.invite', { communityId, profileId: eve.accountId });
  minimal(f.call(alice, 'membership.list', { communityId, state: 'invited' }).members[0].profile, eve.accountId);
  f.call(alice, 'membership.ban', { communityId, profileId: bob.accountId });
  minimal(f.call(alice, 'membership.list', { communityId, state: 'banned' }).members[0].profile, bob.accountId);
  f.call(alice, 'chat.remove', { communityId, messageId: sent.messageId });
  f.call(alice, 'membership.unban', { communityId, profileId: bob.accountId }); f.call(bob, 'membership.join', { communityId });
  const replay = f.call(bob, 'chat.send', args).message; assert.equal(replay.removed, true); assert.equal(replay.text, ''); minimal(replay.author, bob.accountId);
  const requestGroup = f.call(alice, 'community.create', { requestId: 'audience_request_group', name: 'Заявки', joinPolicy: 'request' }).community;
  f.call(eve, 'membership.join', { communityId: requestGroup.communityId });
  minimal(f.call(alice, 'membership.list', { communityId: requestGroup.communityId, state: 'requested' }).members[0].profile, eve.accountId);
  const preview = f.call(eve, 'community.get', { communityId }).community.previewMembers;
  assert.equal(preview.length, 1); minimal(preview[0], alice.accountId);
  assert.equal(f.call(bob, 'profile.get').profile.bio, 'Секрет Бориса');
});

test('community authorization grants only a thumbnail, including historical authors, and cannot open a hidden full image', t => {
  const f = fixture(t);
  f.call(bob, 'profile.avatar.set', { expectedRevision: 1, avatarUrl: image, thumbnailUrl: thumbnail });
  const communityId = f.call(alice, 'community.create', { requestId: 'audience_image_group', name: 'Изображения' }).communityId;
  f.call(bob, 'membership.join', { communityId }); f.call(bob, 'chat.send', { communityId, clientId: 'audience_image_author', text: 'История' });
  denied(() => f.call(alice, 'profile.avatar.read', { profileId: bob.accountId, communityId }), 'profile_not_found');
  assert.equal(f.call(alice, 'profile.avatars', { profileIds: [bob.accountId], communityId }).avatars[0].avatarUrl, thumbnail);
  assert.equal(f.call(bob, 'profile.avatar.read', { profileId: bob.accountId }).avatarUrl, image);
  f.call(bob, 'membership.leave', { communityId });
  assert.equal(f.call(alice, 'profile.avatars', { profileIds: [bob.accountId], communityId }).avatars[0].avatarUrl, thumbnail);
  denied(() => f.call(alice, 'profile.avatar.read', { profileId: bob.accountId, communityId }), 'profile_not_found');
  denied(() => f.call(eve, 'profile.avatars', { profileIds: [bob.accountId], communityId }), 'community_membership_required');
  assert.deepEqual(f.call(alice, 'profile.avatars', { profileIds: [bob.accountId] }).avatars, []);
  f.update(bob, { audience: 'link' }); assert.equal(f.call(alice, 'profile.avatar.read', { profileId: bob.accountId }).avatarUrl, image);
});
