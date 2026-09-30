import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, mkdir, readFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createHistoricalAppsV5 } from './apps-v5.fixture.mjs';
import { seedHistoricalRollbackV4 } from './apps-v4.fixture.mjs';
import { seedHistoricalPublicationV3 } from './apps-v3.fixture.mjs';
import { migrateAppsSchema as oldAppsV5Migrator } from './fixtures/apps-v5/schema.mjs';
import { migrateAppsSchema } from '../../modules/apps/server/schema.mjs';
import { readStorageFormat } from './storage-probe.mjs';
import { assertStorageCompatible, currentStorageReaders, storageReaderLabel } from './storage-guard.mjs';

const normalizedSql = sql => sql.split(/('(?:[^']|'')*')/gu).map((part, index) => index % 2 ? part
  : part.replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase()).join('');
const objects = db => db.prepare("SELECT type,name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()
  .map(row => ({ ...row, sql: normalizedSql(row.sql) }));
const format = apps => ({ ok: true, schema: 'soty.storage-format.v3', rooms: 'empty', apps, notes: 'empty', capabilities: 'empty' });
const image = readers => ({ Id: 'sha256:' + '6'.repeat(64), Config: { Labels: { [storageReaderLabel]: readers } } });
const oldReaders = '{"version":3,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5],"notes":[1],"capabilities":[1]}}';
const appId = letter => `app-${letter.repeat(32)}`;
const liveDomain = 'dom_' + 'c'.repeat(32), retiredDomain = 'dom_' + 'd'.repeat(32);
const discussionTables = ['app_discussion_heads', 'app_discussion_conversations', 'app_discussion_messages',
  'app_discussion_changes', 'app_discussion_usage', 'app_discussion_rates'];
const discussionGuards = ['app_discussion_head_no_delete', 'app_discussion_head_no_replace', 'app_discussion_head_lineage',
  'app_discussion_conversation_immutable', 'app_discussion_conversation_no_delete', 'app_discussion_conversation_no_replace',
  'app_discussion_message_immutable', 'app_discussion_message_no_delete', 'app_discussion_message_no_replace',
  'app_discussion_message_redaction', 'app_discussion_change_immutable', 'app_discussion_usage_no_delete', 'app_discussion_usage_no_replace'];

async function directory(t) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'soty-apps6-reader-'));
  t.after(async () => {
    assert.equal(path.dirname(path.resolve(root)), path.resolve(os.tmpdir()));
    assert.match(path.basename(root), /^soty-apps6-reader-/u);
    await rm(root, { recursive: true, force: true });
  });
  await mkdir(path.join(root, 'apps'));
  return { root, filename: path.join(root, 'apps', 'registry.sqlite') };
}

function seedHistoricalApps(db) {
  createHistoricalAppsV5(db);
  db.exec('PRAGMA foreign_keys=ON');
  db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run('connector-owner', 'account-owner', '{"synthetic":true}', 'Fixture device', 1);
  for (const [letter, state, grants] of [['a', 'enabled', { accountIds: ['account-reader'], communityIds: ['community-original'] }],
    ['b', 'revoked', { accountIds: [], communityIds: [] }], ['c', 'enabled', { accountIds: ['account-private'], communityIds: [] }]]) {
    const id = appId(letter);
    db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(id, 'account-owner', 'connector-owner',
      `Historical ${letter}`, 9001, '/board?tag=a%2Bb#item', JSON.stringify(grants), state, 3, 1, 3);
    for (const account of grants.accountIds) db.prepare("INSERT INTO local_app_grants VALUES (?,'account',?)").run(id, account);
    for (const group of grants.communityIds) db.prepare("INSERT INTO local_app_grants VALUES (?,'community',?)").run(id, group);
    db.prepare('INSERT INTO app_domain_heads VALUES (?,0)').run(id);
    seedHistoricalPublicationV3(db, id);
    if (letter === 'a') seedHistoricalRollbackV4(db, id); else db.prepare('INSERT INTO app_source_heads VALUES (?,1)').run(id);
  }
  db.exec("INSERT INTO app_domain_zones VALUES ('zone_historical','named','https://{slug}.apps.example','apps.example','https','',1)");
  db.prepare("INSERT INTO app_domains VALUES (?,'zone_historical','live.apps.example','https://live.apps.example','live',?,'account-owner','alias','bound',1,NULL)").run(liveDomain, appId('a'));
  db.prepare("INSERT INTO app_domains VALUES (?,'zone_historical','retired.apps.example','https://retired.apps.example','retired',?,'account-owner','alias','tombstone',1,2)").run(retiredDomain, appId('b'));
  db.prepare("INSERT INTO app_domain_receipts VALUES ('account-owner','historical-domain-key','historical-domain-hash','retire',?,1,2)").run(retiredDomain);
  db.prepare('INSERT INTO app_publication_domains VALUES (?,?,?)').run(appId('a'), liveDomain, 'account-owner');
  const target = db.prepare('SELECT digest,profile FROM app_runtime_targets WHERE app_id=? AND revision=1').get(appId('a'));
  const ack = JSON.stringify({ scope: 'whole-port', targetRevision: 1, targetDigest: target.digest, profile: target.profile });
  db.prepare("UPDATE app_publications SET launch_policy='anyone',listed=1,exposure_ack_revision=1,exposure_ack_json=? WHERE app_id=?").run(ack, appId('a'));
  db.prepare('INSERT INTO app_publication_receipts VALUES (?,?,?,?,?,?,?)').run('account-owner', 'historical-policy-key',
    'historical-policy-hash', appId('a'), 4, '{"synthetic":"historical-policy"}', 4);
  db.prepare('INSERT INTO app_saved_heads VALUES (?,3)').run('account-reader');
  for (const [letter, domain, label, revision] of [['a', liveDomain, 'My saved public app', 2], ['b', retiredDomain, 'My unavailable app', 3]]) {
    const origin = letter === 'a' ? 'https://live.apps.example' : 'https://retired.apps.example';
    db.prepare('INSERT INTO app_saved_entries VALUES (?,?,?,?,?,?,?,8)').run('account-reader', appId(letter), domain,
      origin, '/board?tag=a%2Bb#item', label, revision);
    db.prepare('INSERT INTO app_saved_receipts VALUES (?,?,?,?,1,?,8)').run('account-reader', String(revision).repeat(64), 'f'.repeat(64), appId(letter), revision);
  }
  assert.equal(oldAppsV5Migrator(db).migrated, false, 'the seeded database is accepted by the actual historical5 implementation');
}

// Synthetic persisted rows exercise all six new projections. This is storage
// evidence, not a claim that discussion authorization or the signed API passed.
function seedDiscussion(db) {
  const conversation = 'conv_' + 'a'.repeat(32), first = 'msg_' + 'a'.repeat(32), removed = 'msg_' + 'b'.repeat(32);
  const grants = { accountIds: ['account-reader'], communityIds: ['community-original'] };
  const audience = createHash('sha256').update(JSON.stringify(['soty.app-discussion.audience.v1', 'account-owner', 'anyone',
    grants.accountIds, grants.communityIds])).digest('hex');
  const body = 'Synthetic discussion text', bytes = Buffer.byteLength(body);
  db.exec('BEGIN');
  db.prepare('INSERT INTO app_discussion_heads VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)')
    .run(appId('a'), conversation, 1, 'account-owner', 'anyone', JSON.stringify(grants), audience, 2, bytes, 1, 10, 15000, 12);
  db.prepare('INSERT INTO app_discussion_conversations VALUES (?,?,?,?,?,?,?,?,?,?)')
    .run(conversation, appId('a'), 1, 'account-owner', 'anyone', JSON.stringify(grants), audience, 10, 2, 3);
  db.prepare('INSERT INTO app_discussion_messages VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)')
    .run(first, appId('a'), conversation, 1, 'account-reader', 'Fixture author', '1'.repeat(64), '2'.repeat(64), body, bytes, null, 10, null, null);
  db.prepare('INSERT INTO app_discussion_messages VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)')
    .run(removed, appId('a'), conversation, 2, 'account-reader', 'Fixture author', '3'.repeat(64), '4'.repeat(64), null, 0, first, 11, 12, 'account-owner');
  db.prepare('INSERT INTO app_discussion_changes VALUES (?,1,?,?,10)').run(conversation, first, 'message');
  db.prepare('INSERT INTO app_discussion_changes VALUES (?,2,?,?,11)').run(conversation, removed, 'message');
  db.prepare('INSERT INTO app_discussion_changes VALUES (?,3,?,?,12)').run(conversation, removed, 'removed');
  db.prepare('UPDATE app_discussion_usage SET head_count=1,conversation_count=1,message_count=2,body_bytes=? WHERE id=1').run(bytes);
  db.prepare('INSERT INTO app_discussion_rates VALUES (?,12,18000)').run('account-reader');
  db.exec('COMMIT');
}

test('literal historical Apps5 matches the committed old migrator including its frozen launch-path dependency', async () => {
  const provenance = JSON.parse(await readFile(new URL('./fixtures/apps-v5/provenance.json', import.meta.url), 'utf8'));
  assert.equal(provenance.commit, '3794f01febe3c01e2d3d107be1f03dbf4b023b3c');
  assert.deepEqual(Object.keys(provenance.files).sort(), ['domain-policy.mjs', 'launch-path.mjs', 'protocol.mjs', 'schema.mjs']);
  for (const [name, entry] of Object.entries(provenance.files)) {
    const source = await readFile(new URL(`./fixtures/apps-v5/${name}`, import.meta.url), 'utf8');
    assert.equal(createHash('sha256').update(source.replaceAll('\r\n', '\n')).digest('hex'), entry.sha256, name);
    assert.equal(entry.sourcePath, `modules/apps/server/${name}`);
  }
  const literal = new DatabaseSync(':memory:'), actual = new DatabaseSync(':memory:');
  try {
    createHistoricalAppsV5(literal); oldAppsV5Migrator(actual);
    assert.equal(objects(literal).length, 38);
    assert.deepEqual(objects(literal), objects(actual));
    assert.equal(literal.prepare('PRAGMA user_version').get().user_version, 5);
    assert.equal(literal.prepare("SELECT value FROM apps_meta WHERE key='schema'").get().value, 'soty.apps-registry.v5');
    assert.equal(oldAppsV5Migrator(literal).migrated, false);
  } finally { literal.close(); actual.close(); }
});

test('Apps5 main plus committed Apps6 WAL preserves saved entries and makes exact old5 code refuse without writes', async t => {
  const { root, filename } = await directory(t), db = new DatabaseSync(filename);
  try {
    seedHistoricalApps(db);
    db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0');
    const mainBefore = await readFile(filename);
    assert.equal(mainBefore.readUInt32BE(60), 5);
    assert.deepEqual(await readStorageFormat(root), format(5));
    const oldObjects = objects(db);
    const preserved = db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name<>'apps_meta' AND name NOT GLOB 'sqlite_*' ORDER BY name").all().map(row => row.name);
    const oldRows = new Map(preserved.map(name => [name, db.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all()]));
    const migrated = migrateAppsSchema(db);
    assert.equal(migrated.schema, 'soty.apps-registry.v6'); assert.equal(migrated.migrated, true);
    for (const [name, rows] of oldRows) assert.deepEqual(db.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all(), rows, name);
    const nextObjects = new Map(objects(db).map(row => [row.name, row]));
    assert.equal(nextObjects.size - oldObjects.length, 25, 'six tables, six indexes and thirteen guards are added');
    for (const object of oldObjects) assert.deepEqual(nextObjects.get(object.name), object, object.name);
    for (const table of discussionTables.filter(name => name !== 'app_discussion_usage')) {
      assert.equal(db.prepare(`SELECT count(*) AS n FROM ${table}`).get().n, 0, 'migration never opens or imports conversations');
    }
    assert.deepEqual({ ...db.prepare('SELECT * FROM app_discussion_usage').get() }, { id: 1, head_count: 0, conversation_count: 0, message_count: 0, body_bytes: 0 });
    seedDiscussion(db);
    const walBefore = await readFile(filename + '-wal');
    assert.ok(walBefore.length > 32); assert.deepEqual(await readFile(filename), mainBefore);
    const observed = await readStorageFormat(root);
    assert.deepEqual(observed, format(6));
    assert.doesNotMatch(JSON.stringify(observed), /account-|Historical|Synthetic discussion|My saved|apps\.example|9001|community-original/u);
    assert.throws(() => assertStorageCompatible(image(oldReaders), observed), /storage_reader_incompatible/u);
    assertStorageCompatible(image(currentStorageReaders), observed);
    const oldDb = new DatabaseSync(filename);
    try {
      assert.throws(() => oldAppsV5Migrator(oldDb), error => error.code === 'apps_schema_unsupported');
      assert.equal(oldDb.isTransaction, false);
      assert.equal(oldDb.prepare('PRAGMA user_version').get().user_version, 6);
    } finally { oldDb.close(); }
    assert.deepEqual(await readFile(filename), mainBefore, 'old code and host probe never checkpoint the active writer');
    assert.deepEqual(await readFile(filename + '-wal'), walBefore, 'old code and host probe never append, truncate or rewrite WAL');
    for (const [name, rows] of oldRows) assert.deepEqual(db.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all(), rows, name);
  } finally { db.close(); }
  assert.deepEqual(await readStorageFormat(root), format(6));
  const reopened = new DatabaseSync(filename);
  try {
    assert.equal(migrateAppsSchema(reopened).migrated, false);
    assert.equal(reopened.prepare('SELECT count(*) AS n FROM app_saved_entries').get().n, 2);
    assert.equal(reopened.prepare('SELECT count(*) AS n FROM app_saved_receipts').get().n, 2);
    assert.equal(reopened.prepare('SELECT required_binding_version FROM app_source_heads WHERE app_id=?').get(appId('a')).required_binding_version, 2);
    assert.equal(reopened.prepare('SELECT active_target_revision FROM app_publications WHERE app_id=?').get(appId('a')).active_target_revision, 1);
    assert.equal(reopened.prepare('SELECT launch_policy FROM app_publications WHERE app_id=?').get(appId('c')).launch_policy, 'restricted');
    assert.equal(reopened.prepare('SELECT state FROM local_apps WHERE id=?').get(appId('b')).state, 'revoked');
    assert.deepEqual({ ...reopened.prepare('SELECT count(*) AS n,sum(removed_at IS NOT NULL) AS removed FROM app_discussion_messages').get() }, { n: 2, removed: 1 });
    assert.equal(reopened.prepare('SELECT count(*) AS n FROM app_discussion_changes').get().n, 3);
  } finally { reopened.close(); }
});

test('Apps6 refuses every missing discussion table, essential projection and version6 marker over Apps5', async t => {
  const mutations = [...discussionTables.map(name => `DROP TABLE ${name}`),
    ...[['heads', 'current_id'], ['heads', 'audience_hash'], ['heads', 'body_bytes'], ['conversations', 'grants_json'],
      ['conversations', 'message_seq'], ['conversations', 'change_seq'], ['messages', 'author_account_id'], ['messages', 'intent_hash'],
      ['messages', 'body'], ['messages', 'removed_at'], ['changes', 'message_id'], ['changes', 'kind'],
      ['usage', 'message_count'], ['rates', 'credit']].map(([table, column]) => `ALTER TABLE app_discussion_${table} RENAME COLUMN ${column} TO old_${column}`),
    'CREATE TABLE unexpected_discussion_data(value TEXT)', 'CREATE VIEW unexpected_discussion_view AS SELECT 1'];
  for (const sql of mutations) {
    const { root, filename } = await directory(t), db = new DatabaseSync(filename);
    try { migrateAppsSchema(db); db.exec('PRAGMA foreign_keys=OFF'); db.exec(sql); } finally { db.close(); }
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u, sql);
  }
  const { root, filename } = await directory(t), db = new DatabaseSync(filename);
  try { createHistoricalAppsV5(db); db.exec("UPDATE apps_meta SET value='soty.apps-registry.v6' WHERE key='schema'; PRAGMA user_version=6"); }
  finally { db.close(); }
  await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u);
});

test('Apps6 requires all thirteen exact discussion guards plus inherited saved and source guards', async t => {
  for (const name of [...discussionGuards, 'app_saved_head_no_downgrade', 'app_source_head_no_downgrade']) {
    for (const variant of ['missing', 'changed-body', 'changed-table', 'changed-literal']) {
      const { root, filename } = await directory(t), db = new DatabaseSync(filename);
      try {
        migrateAppsSchema(db);
        const record = db.prepare('SELECT sql,tbl_name FROM sqlite_schema WHERE name=?').get(name);
        db.exec(`DROP TRIGGER ${name}`);
        if (variant === 'changed-body') db.exec(`CREATE TRIGGER ${name} BEFORE UPDATE ON ${record.tbl_name} BEGIN SELECT 1; END`);
        if (variant === 'changed-table') db.exec(record.sql.replace(` ON ${record.tbl_name} `, ' ON local_apps '));
        if (variant === 'changed-literal') db.exec(record.sql.replace(/RAISE\(ABORT,'([^']+)'\)/u, (_, message) => `RAISE(ABORT,'${message.toUpperCase()}')`));
      } finally { db.close(); }
      await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u, `${name}:${variant}`);
    }
  }
});

test('Apps6 does not accept a weakened lineage or redaction guard under its old name', async t => {
  for (const [name, before, after] of [
    ['app_discussion_head_lineage', 'NEW.grants_json<>OLD.grants_json', '0'],
    ['app_discussion_head_lineage', 'NEW.generation<>OLD.generation+1', 'NEW.generation<OLD.generation+1'],
    ['app_discussion_message_redaction', 'OLD.removed_at IS NOT NULL OR ', ''],
    ['app_discussion_message_redaction', 'NEW.body IS NOT NULL OR ', ''],
  ]) {
    const { root, filename } = await directory(t), db = new DatabaseSync(filename);
    try {
      migrateAppsSchema(db);
      const { sql } = db.prepare('SELECT sql FROM sqlite_schema WHERE name=?').get(name);
      assert.ok(sql.includes(before)); db.exec(`DROP TRIGGER ${name}`); db.exec(sql.replace(before, after));
    } finally { db.close(); }
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u, `${name}:${before}`);
  }
});
