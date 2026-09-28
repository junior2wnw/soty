import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { createWorldService, SCHEMA_VERSION } from '../server/index.mjs';
import { removeDiscoveryProjection } from './schema-fixtures.mjs';
import { asDataUrl, pngFixture } from './avatar-fixtures.mjs';

const owner = { accountId: 'acct_directory_owner', deviceId: 'dev_directory_owner', label: 'Владелец' };
const reader = { accountId: 'acct_directory_reader', deviceId: 'dev_directory_reader', label: 'Наблюдатель' };
const personIds = result => result.people.map(profile => profile.profileId);
const communityIds = result => result.communities.map(group => group.communityId);
const denied = (fn, code) => assert.throws(fn, error => error.code === code, code);

function fixture(t) {
  const base = resolve(tmpdir()), dir = mkdtempSync(join(base, 'soty-directory-test-'));
  const databasePath = join(dir, 'world.sqlite'), services = [];
  let now = 1_800_000_000_000, serial = 0;
  const open = () => { const service = createWorldService({ databasePath, projectId: 'directory-tests', clock: () => now }); services.push(service); return service; };
  let service = open();
  const call = (actor, operation, args = {}) => service.execute({ actor, op: 'world.' + operation, args });
  [owner, reader].forEach(actor => call(actor, 'profile.get'));
  const update = (actor, fields) => call(actor, 'profile.update', { expectedRevision: call(actor, 'profile.get').profile.revision, ...fields }).profile;
  const person = (label, fields = {}) => {
    const actor = { accountId: `acct_directory_${++serial}`, deviceId: `dev_directory_${serial}`, label };
    call(actor, 'profile.get'); update(actor, { discoverable: true, ...fields }); return actor;
  };
  const group = (name, fields = {}) => call(owner, 'community.create', { requestId: `directory_group_${++serial}`, name, ...fields }).community;
  const changeGroup = (group, fields) => call(owner, 'community.update', { communityId: group.communityId,
    expectedRevision: call(owner, 'community.get', { communityId: group.communityId }).community.revision, ...fields }).community;
  const search = args => call(reader, 'discovery.search', args);
  t.after(() => {
    services.forEach(item => item.close());
    assert.equal(dirname(resolve(dir)), base); assert.ok(resolve(dir).startsWith(join(base, 'soty-directory-test-')));
    rmSync(dir, { recursive: true, force: true });
  });
  return { get service() { return service; }, databasePath, call, update, person, group, changeGroup, search,
    open: () => { service = open(); return service; }, advance: milliseconds => { now += milliseconds; } };
}

test('visibility, invitation-only policy and archive immediately remove search rows, FTS hits and public counts', t => {
  const f = fixture(t), photographer = f.person('Фотограф Ирина', { bio: 'Уфа прогулки' });
  const publicGroup = f.group('Фотографы Уфы'), requestGroup = f.group('Фотография природы', { joinPolicy: 'request' });
  const privateGroup = f.group('Фотография семьи', { joinPolicy: 'invite' });
  assert.deepEqual(personIds(f.search({ query: 'фотог' })), [photographer.accountId]);
  assert.deepEqual(new Set(communityIds(f.search({ query: 'фотог' }))), new Set([publicGroup.communityId, requestGroup.communityId]));
  assert.deepEqual(f.search({}).totals, { people: 1, communities: 2 });
  assert.equal(f.search({ query: privateGroup.communityId }).communities.length, 0);
  f.update(photographer, { discoverable: false });
  f.changeGroup(publicGroup, { joinPolicy: 'invite' });
  f.call(owner, 'community.archive', { communityId: requestGroup.communityId, expectedRevision: requestGroup.revision });
  assert.deepEqual(f.search({ query: 'фотог' }).totals, { people: 0, communities: 0 });
  assert.deepEqual(f.search({}).totals, { people: 0, communities: 0 });
  for (const query of [photographer.accountId, publicGroup.communityId, requestGroup.communityId]) {
    const result = f.search({ query }); assert.equal(result.people.length + result.communities.length, 0);
  }
  // Even a group's owner does not expose an invitation-only group in the public directory.
  assert.equal(f.call(owner, 'discovery.search', { query: 'фотог' }).communities.length, 0);
  f.update(photographer, { discoverable: true }); f.changeGroup(publicGroup, { joinPolicy: 'open' });
  assert.deepEqual(f.search({ query: 'фотог' }).totals, { people: 1, communities: 1 });
  const db = new DatabaseSync(f.databasePath);
  try {
    assert.equal(db.prepare("SELECT COUNT(*) AS n FROM world_directory_fts WHERE world_directory_fts MATCH 'search_text : (\"фотог\"*)'").get().n, 2);
    assert.deepEqual(Object.fromEntries(db.prepare('SELECT kind,n FROM world_directory_counts').all().map(row => [row.kind, row.n])), { person: 1, community: 1 });
  } finally { db.close(); }
});

test('a rename replaces FTS terms without changing its keyset position or double counting', t => {
  const f = fixture(t), person = f.person('Дальний фотограф', { interests: ['Природа'] });
  const group = f.group('Дальний фотоклуб');
  const db = new DatabaseSync(f.databasePath);
  try {
    const previous = db.prepare('SELECT seq FROM world_directory WHERE entity_id=?').get(person.accountId).seq;
    f.update(person, { displayName: 'Близкий музыкант', interests: ['Музыка'] });
    f.changeGroup(group, { name: 'Близкий музыкальный клуб' });
    assert.equal(db.prepare('SELECT seq FROM world_directory WHERE entity_id=?').get(person.accountId).seq, previous);
    assert.equal(f.search({ query: 'дальн' }).people.length + f.search({ query: 'дальн' }).communities.length, 0);
    assert.equal(f.search({ query: 'природ' }).people.length, 0);
    assert.deepEqual(f.search({ query: 'музык' }).totals, { people: 1, communities: 1 });
    assert.deepEqual(f.search({}).totals, { people: 1, communities: 1 });
  } finally { db.close(); }
});

test('deleting previous-page rows never skips the next keyset page, with and without FTS', t => {
  for (const query of ['', 'ключ']) {
    const f = fixture(t), people = Array.from({ length: 7 }, (_, index) => f.person(`Ключ ${index}`));
    const first = f.search({ query, kind: 'people', limit: 2 });
    assert.deepEqual(personIds(first), people.slice(0, 2).map(person => person.accountId));
    assert.ok(first.nextCursor);
    f.update(people[0], { discoverable: false }); f.update(people[1], { discoverable: false });
    const second = f.search({ query, kind: 'people', limit: 2, cursor: first.nextCursor });
    assert.deepEqual(personIds(second), people.slice(2, 4).map(person => person.accountId));
    f.update(people[2], { discoverable: false });
    const third = f.search({ query, kind: 'people', limit: 2, cursor: second.nextCursor });
    assert.deepEqual(personIds(third), people.slice(4, 6).map(person => person.accountId));
    const last = f.search({ query, kind: 'people', limit: 2, cursor: third.nextCursor });
    assert.deepEqual(personIds(last), [people[6].accountId]); assert.equal(last.nextCursor, null);
  }
});

test('cursor scope, version and sequence are validated independently of user search syntax', t => {
  const f = fixture(t); ['Поиск Один', 'Поиск Два', 'Поиск Три'].forEach(label => f.person(label));
  const cursor = f.search({ query: 'поиск', kind: 'people', limit: 1 }).nextCursor;
  assert.ok(cursor);
  denied(() => f.search({ query: 'другой', kind: 'people', cursor }), 'invalid_cursor');
  denied(() => f.search({ query: 'поиск', kind: 'all', cursor }), 'invalid_cursor');
  const decoded = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
  for (const edit of [{ scope: 'tampered' }, { version: 2 }, { after: -1 }, { after: 1.5 }, { after: Number.MAX_SAFE_INTEGER + 1 }]) {
    const changed = Buffer.from(JSON.stringify({ ...decoded, ...edit })).toString('base64url');
    denied(() => f.search({ query: 'поиск', kind: 'people', cursor: changed }), 'invalid_cursor');
  }
  for (const value of ['not_json', Buffer.from('null').toString('base64url'), 'a'.repeat(1024), 12]) {
    denied(() => f.search({ query: 'поиск', kind: 'people', cursor: value }), 'invalid_cursor');
  }
});

test('Cyrillic prefixes are case folded, punctuation remains literal and FTS operators cannot broaden results', t => {
  const f = fixture(t), first = f.person('Фотография Ёлки', { bio: 'Уфа утро' });
  const literal = f.person('Фото OR Уфа'), other = f.person('Уфа вечером');
  assert.deepEqual(personIds(f.search({ query: 'ЁЛ' })), [first.accountId]);
  assert.deepEqual(new Set(personIds(f.search({ query: 'ФОТО' }))), new Set([first.accountId, literal.accountId]));
  assert.deepEqual(new Set(personIds(f.search({ query: 'фото уф' }))), new Set([first.accountId, literal.accountId]));
  assert.deepEqual(personIds(f.search({ query: 'фото OR уф' })), [literal.accountId]);
  assert.deepEqual(new Set(personIds(f.search({ query: '"фото"* -уф' }))), new Set([first.accountId, literal.accountId]));
  for (const query of ['NEAR(фото уфа)', 'person:фото', '*', '%', "' OR 1=1 --"]) assert.equal(f.search({ query }).people.length, 0, query);
  assert.equal(f.search({ query: other.accountId }).people[0].profileId, other.accountId);
});

test('popular FTS totals are explicitly lower bounds while exact directory counters remain exact', t => {
  const f = fixture(t), db = new DatabaseSync(f.databasePath), count = 1005;
  try {
    db.exec('PRAGMA foreign_keys=ON; BEGIN IMMEDIATE');
    const profile = db.prepare('INSERT INTO profiles(account_id,display_name,search_text,discoverable,created_at,updated_at) VALUES(?,?,?,1,1,1)');
    const community = db.prepare("INSERT INTO communities(id,owner_id,name,search_text,join_policy,created_at,updated_at) VALUES(?,?,?,?,'open',1,1)");
    const membership = db.prepare("INSERT INTO memberships(community_id,account_id,role,state,joined_at,updated_at) VALUES(?,?,'owner','active',1,1)");
    for (let index = 0; index < count; index++) {
      const name = `Каталог ${index}`; profile.run(`acct_bulk_${index}`, name, name.toLowerCase());
      community.run(`group_bulk_${index}`, owner.accountId, name, name.toLowerCase()); membership.run(`group_bulk_${index}`, owner.accountId);
    }
    db.exec('COMMIT');
    const result = f.search({ query: 'катал', limit: 20 });
    assert.deepEqual(result.totals, { people: 1000, communities: 1000, peopleExact: false, communitiesExact: false });
    assert.equal(result.people.length + result.communities.length, 20); assert.ok(result.nextCursor);
    assert.deepEqual(f.search({ query: 'катал', kind: 'people' }).totals, { people: 1000, communities: 0, peopleExact: false });
    assert.deepEqual(f.search({ query: 'катал', kind: 'communities' }).totals, { people: 0, communities: 1000, communitiesExact: false });
    assert.deepEqual(f.search({}).totals, { people: count, communities: count });
    // Exact boundary is truthful: 1000 matches are exact; a 1001st match makes it a lower bound.
    db.exec("UPDATE communities SET state='archived' WHERE CAST(substr(id,12) AS INTEGER)>=1000; UPDATE profiles SET discoverable=0 WHERE CAST(substr(account_id,11) AS INTEGER)>=1000");
    assert.deepEqual(f.search({ query: 'катал' }).totals, { people: 1000, communities: 1000 });
  } finally { db.close(); }
});

test('directory and FTS query plans use their indexes and do not allocate temporary order-by trees', t => {
  const f = fixture(t); f.person('Проверка индекса'); const db = new DatabaseSync(f.databasePath);
  try {
    const plans = [
      ['SELECT * FROM world_directory WHERE seq>? ORDER BY seq LIMIT ?', [0, 31], /INTEGER PRIMARY KEY/],
      ['SELECT * FROM world_directory WHERE kind=? AND seq>? ORDER BY seq LIMIT ?', ['person', 0, 31], /world_directory_kind_seq/],
      ['SELECT * FROM world_directory WHERE entity_id=?', ['acct_directory_1'], /world_directory_entity/],
      ['SELECT * FROM world_directory WHERE kind=? AND entity_id=?', ['person', 'acct_directory_1'], /INDEX/],
      ['SELECT d.* FROM world_directory_fts JOIN world_directory d ON d.seq=world_directory_fts.rowid WHERE world_directory_fts MATCH ? AND world_directory_fts.rowid>? ORDER BY world_directory_fts.rowid LIMIT ?', ['kind : person AND search_text : ("пров"*)', 0, 31], /VIRTUAL TABLE INDEX/],
      ['SELECT COUNT(*) AS n FROM (SELECT 1 FROM world_directory_fts WHERE world_directory_fts MATCH ? LIMIT ?)', ['kind : person AND search_text : ("пров"*)', 1001], /VIRTUAL TABLE INDEX/],
    ];
    for (const [sql, args, expected] of plans) {
      const details = db.prepare('EXPLAIN QUERY PLAN ' + sql).all(...args).map(row => row.detail).join('\n');
      assert.match(details, expected, sql); assert.doesNotMatch(details, /USE TEMP B-TREE|SCAN world_directory\b/, sql);
    }
  } finally { db.close(); }
});

test('v2 database upgrades to v3 without changing profiles, avatars, groups, chat or receipts; reopening is idempotent', t => {
  const f = fixture(t); f.update(owner, { discoverable: true, displayName: 'Миграция Ирина', bio: 'Сохраняется' });
  const image = asDataUrl(pngFixture());
  f.call(owner, 'profile.avatar.set', { expectedRevision: f.call(owner, 'profile.get').profile.revision, avatarUrl: image, thumbnailUrl: image });
  const publicGroup = f.group('Миграция общий'), privateGroup = f.group('Миграция личный', { joinPolicy: 'invite' });
  f.call(owner, 'chat.send', { communityId: publicGroup.communityId, clientId: 'migration_message', text: 'История сохраняется' });
  f.service.close(); const db = new DatabaseSync(f.databasePath);
  const tables = ['profiles', 'profile_avatars', 'communities', 'memberships', 'messages', 'receipts', 'world_audit'];
  const before = Object.fromEntries(tables.map(table => [table, db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all()]));
  removeDiscoveryProjection(db);
  db.exec("PRAGMA user_version=2; UPDATE world_meta SET value='2' WHERE key='schema_version'"); db.close();
  const migrated = f.open(); assert.equal(SCHEMA_VERSION, 3); assert.equal(migrated.schemaVersion, 3);
  assert.deepEqual(f.search({ query: 'миграц' }).totals, { people: 1, communities: 1 });
  assert.equal(f.search({ query: privateGroup.communityId }).communities.length, 0);
  assert.equal(f.call(owner, 'profile.avatar.read', { profileId: owner.accountId }).avatarUrl, image);
  const reopenedDb = new DatabaseSync(f.databasePath);
  let sequence;
  try {
    for (const table of tables) assert.deepEqual(reopenedDb.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(), before[table], table);
    sequence = reopenedDb.prepare('SELECT seq,kind,entity_id FROM world_directory ORDER BY seq').all();
    assert.equal(reopenedDb.prepare('PRAGMA foreign_key_check').get(), undefined);
    assert.equal(reopenedDb.prepare('PRAGMA user_version').get().user_version, 3);
  } finally { reopenedDb.close(); }
  migrated.close(); f.open();
  assert.deepEqual(f.search({}).totals, { people: 1, communities: 1 });
  const again = new DatabaseSync(f.databasePath);
  try { assert.deepEqual(again.prepare('SELECT seq,kind,entity_id FROM world_directory ORDER BY seq').all(), sequence); }
  finally { again.close(); }
});
