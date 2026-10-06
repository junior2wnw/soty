import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createWorldService } from '../server/index.mjs';
import { migrateWorld } from '../server/schema.mjs';

const owner = { accountId: 'field_owner', deviceId: 'field_owner_browser', label: 'Автор' };
const member = { accountId: 'field_member', deviceId: 'field_member_browser', label: 'Участник' };
const other = { accountId: 'field_other', deviceId: 'field_other_browser', label: 'Внешний' };
const hidden = { accountId: 'field_hidden', deviceId: 'field_hidden_browser', label: 'Скрытый' };
const phone = { ...member, deviceId: 'field_member_phone' };
const document = x => ({ schema: 'soty.field.v1', contexts: [{ contextId: 'my-context', title: 'Личное', x, y: 0 }],
  shortcuts: [{ shortcutId: 'shortcut-notes', entity: { kind: 'builtin', id: 'notes' }, contextId: 'my-context', slot: [0, 0] }] });
const denies = (callback, expected) => assert.throws(callback, error => error.code === expected);
function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'soty-field-backend-')), databasePath = join(directory, 'world.sqlite');
  let now = 1_800_000_000_000, serial = 0;
  let service = createWorldService({ databasePath, projectId: 'field-tests', clock: () => now });
  const call = (actor, op, args = {}) => service.execute({ actor, op, args });
  const directoryCall = (actor, op, args = {}) => call(actor, op, { expectedAccountId: actor.accountId, ...args });
  const group = (name, joinPolicy = 'invite') => { now += 61_000; return call(owner, 'world.community.create', { name, joinPolicy, requestId: `field_create_${++serial}` }).community; };
  for (const actor of [owner, member, other, hidden]) call(actor, 'world.profile.get');
  t.after(() => { service.close(); rmSync(directory, { recursive: true, force: true }); });
  return { databasePath, call, directoryCall, group, advance: () => { now += 10; }, reopen() { service.close(); service = createWorldService({ databasePath, projectId: 'field-tests', clock: () => now }); }, close: () => service.close() };
}

test('field CAS, immutable request identity and reload preserve personal document independently of groups/grants', t => {
  const f = fixture(t), first = f.directoryCall(member, 'world.field.get');
  assert.equal(first.revision, 0); assert.deepEqual(first.document.contexts, []);
  const args = { expectedRevision: 0, requestId: 'field_intent_001', document: document(24) };
  const accepted = f.directoryCall(member, 'world.field.put', args);
  assert.equal(accepted.receipt.revision, 1); assert.equal(accepted.replayed, false);
  assert.equal(f.directoryCall(owner, 'world.field.get').revision, 0);
  assert.deepEqual(f.call(member, 'world.community.list').communities, []);
  denies(() => f.directoryCall(member, 'world.field.put', { ...args, requestId: 'field_intent_other', document: document(99) }), 'field_revision_conflict');
  f.advance(); const second = f.directoryCall(member, 'world.field.put', { expectedRevision: 1, requestId: 'field_intent_002', document: document(42) });
  const replay = f.directoryCall(member, 'world.field.put', args);
  assert.equal(replay.replayed, true); assert.deepEqual(replay.receipt, accepted.receipt); assert.deepEqual(replay.current, second.current);
  denies(() => f.directoryCall(member, 'world.field.put', { ...args, document: document(100) }), 'field_request_conflict');
  denies(() => f.directoryCall(member, 'world.field.put', { ...args, expectedRevision: 1 }), 'field_request_conflict');
  f.reopen(); assert.deepEqual(f.directoryCall(member, 'world.field.get'), second.current);
  assert.deepEqual(f.directoryCall(member, 'world.field.put', args).receipt, accepted.receipt);
});

test('positions only, account fence and invalid hash never mutate accepted layout', t => {
  const f = fixture(t); f.directoryCall(member, 'world.field.put', { expectedRevision: 0, requestId: 'field_valid_001', document: document(8) });
  const before = f.directoryCall(member, 'world.field.get');
  denies(() => f.directoryCall(member, 'world.field.get', { expectedAccountId: owner.accountId }), 'field_account_changed');
  denies(() => f.call(member, 'world.field.get', { accountId: member.accountId }), 'invalid_arguments');
  denies(() => f.directoryCall(member, 'world.field.put', { expectedRevision: 1, requestId: 'field_invalid_hash', document: document(9), contentHash: '0'.repeat(64) }), 'field_content_hash_mismatch');
  denies(() => f.directoryCall(member, 'world.field.put', { expectedRevision: 1, requestId: 'field_invalid_payload', document: { ...document(9), grants: { accountIds: [other.accountId] } } }), 'field_document_invalid');
  assert.deepEqual(f.directoryCall(member, 'world.field.get'), before);
});

test('additive tables are preserved by the unchanged World v3 migrator and receipts cannot be changed/replaced/deleted', t => {
  const f = fixture(t), accepted = f.directoryCall(member, 'world.field.put', { expectedRevision: 0, requestId: 'field_old_reader', document: document(72) });
  f.close(); const db = new DatabaseSync(f.databasePath);
  try {
    assert.equal(db.prepare('PRAGMA user_version').get().user_version, 3);
    const rows = db.prepare('SELECT * FROM world_field_receipts').all();
    migrateWorld(db, 'field-tests');
    assert.deepEqual(db.prepare('SELECT * FROM world_field_receipts').all(), rows);
    assert.equal(db.prepare('SELECT document_json FROM world_field_documents').get().document_json, JSON.stringify(document(72)));
    for (const statement of ["UPDATE world_field_receipts SET content_hash='0'", 'DELETE FROM world_field_receipts',
      'INSERT OR REPLACE INTO world_field_receipts SELECT * FROM world_field_receipts']) assert.throws(() => db.exec(statement), /field_receipt_immutable/u);
  } finally { db.close(); }
  f.reopen(); assert.deepEqual(f.directoryCall(member, 'world.field.get'), accepted.current);
});

test('future field epoch or corrupt hash refuses reopen without replacing saved rows', t => {
  const f = fixture(t); f.directoryCall(member, 'world.field.put', { expectedRevision: 0, requestId: 'field_future_001', document: document(51) }); f.close();
  const db = new DatabaseSync(f.databasePath);
  db.prepare("UPDATE world_meta SET value='99' WHERE key='field_schema_version'").run();
  denies(() => f.reopen(), 'field_schema_unsupported');
  assert.equal(db.prepare('SELECT revision FROM world_field_documents').get().revision, 1);
  db.prepare("UPDATE world_meta SET value='1' WHERE key='field_schema_version'").run();
  db.prepare("UPDATE world_field_documents SET revision=2,content_hash=?").run('0'.repeat(64)); db.close();
  denies(() => f.reopen(), 'field_storage_corrupt');
});

test('private joined/invited groups are searched only for the current actor; invitation does not open chat', t => {
  const f = fixture(t), group = f.group('Студия музыки');
  f.call(owner, 'world.membership.invite', { communityId: group.communityId, profileId: member.accountId });
  const invited = f.directoryCall(member, 'world.directory.search', { query: 'СТУДИЯ', kind: 'communities' });
  assert.equal(invited.communities[0].membership.state, 'invited'); assert.equal(invited.communities[0].permissions.canWrite, false);
  denies(() => f.call(member, 'world.chat.list', { communityId: group.communityId }), 'community_membership_required');
  assert.deepEqual(f.directoryCall(other, 'world.directory.search', { query: 'Студия' }).communities, []);
  assert.deepEqual(f.directoryCall(other, 'world.directory.resolve', { entities: [{ kind: 'community', id: group.communityId }] }).items,
    [{ ref: { kind: 'community', id: group.communityId }, available: false }]);
  f.call(member, 'world.membership.join', { communityId: group.communityId });
  assert.equal(f.directoryCall(member, 'world.directory.search', { scope: 'mine' }).communities[0].permissions.canWrite, true);
  assert.deepEqual(f.directoryCall(member, 'world.directory.search', { scope: 'public' }).communities, []);
  f.call(member, 'world.membership.leave', { communityId: group.communityId });
  assert.deepEqual(f.directoryCall(member, 'world.directory.search').communities, []);
});

test('saved hidden person resolves minimal current roster projection, never global search or hidden group hints', t => {
  const f = fixture(t), group = f.group('Приватная команда');
  for (const actor of [member, hidden]) { f.call(owner, 'world.membership.invite', { communityId: group.communityId, profileId: actor.accountId }); f.call(actor, 'world.membership.join', { communityId: group.communityId }); }
  const profile = f.call(hidden, 'world.profile.get').profile;
  f.call(hidden, 'world.profile.update', { expectedRevision: profile.revision, bio: 'Не для агрегатора', interests: ['тайное'] });
  assert.deepEqual(f.directoryCall(member, 'world.directory.search', { query: 'Скрытый', kind: 'people' }).people, []);
  const ref = { kind: 'person', id: hidden.accountId };
  const resolved = f.directoryCall(member, 'world.directory.resolve', { entities: [ref] }).items[0];
  assert.equal(resolved.available, true); assert.equal(resolved.access, 'member');
  assert.deepEqual(Object.keys(resolved.person).sort(), ['profileId', 'displayName', 'avatarColor', 'avatarRevision', 'revision'].sort());
  assert.doesNotMatch(JSON.stringify(resolved), /Не для агрегатора|тайное|Приватная команда|communityId/u);
  assert.deepEqual(f.directoryCall(other, 'world.directory.resolve', { entities: [ref] }).items, [{ ref, available: false }]);
  f.call(owner, 'world.membership.remove', { communityId: group.communityId, profileId: hidden.accountId });
  assert.deepEqual(f.directoryCall(member, 'world.directory.resolve', { entities: [ref] }).items, [{ ref, available: false }]);
});

test('directory seek cursors survive deletions, reject cross-account/device/scope and keep punctuation literal', t => {
  const f = fixture(t);
  for (let index = 0; index < 5; index++) f.group(`Публичная музыка ${index}`, 'open');
  const first = f.directoryCall(member, 'world.directory.search', { query: 'муз', kind: 'communities', limit: 2 });
  assert.equal(first.communities.length, 2); assert.ok(first.nextCursor);
  for (const actor of [other, phone]) denies(() => f.directoryCall(actor, 'world.directory.search', { query: 'муз', kind: 'communities', limit: 2, cursor: first.nextCursor }), 'invalid_directory_cursor');
  denies(() => f.directoryCall(member, 'world.directory.search', { query: 'муз', kind: 'communities', scope: 'mine', cursor: first.nextCursor }), 'invalid_directory_cursor');
  f.call(owner, 'world.community.archive', { communityId: first.communities[0].communityId, expectedRevision: first.communities[0].revision });
  const second = f.directoryCall(member, 'world.directory.search', { query: 'муз', kind: 'communities', limit: 2, cursor: first.nextCursor });
  assert.equal(second.communities.length, 2); assert.equal(second.communities.some(group => first.communities.some(before => before.communityId === group.communityId)), false);
  f.group('Личная музыка'); assert.deepEqual(f.directoryCall(owner, 'world.directory.search', { query: '!!!' }).communities, []);
});
