import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { createHistoricalAppsV5 } from '../../../deploy/connector/apps-v5.fixture.mjs';
import { seedHistoricalPublicationV3 } from '../../../deploy/connector/apps-v3.fixture.mjs';
import { seedHistoricalRollbackV4 } from '../../../deploy/connector/apps-v4.fixture.mjs';
import { migrateAppsSchema, inspectAppsSchema } from '../server/schema.mjs';
import { createDiscussionRegistry, syncDiscussionAudienceInTransaction } from '../server/discussions.mjs';

const app = `app-${'a'.repeat(32)}`, retired = `app-${'b'.repeat(32)}`, domain = `dom_${'d'.repeat(32)}`;
const owner = { accountId: 'owner', deviceId: 'browser', label: 'Author' };
const snapshot = db => ({ version: db.prepare('PRAGMA user_version').get().user_version,
  definitions: db.prepare("SELECT type,name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all(),
  rows: Object.fromEntries(db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*'").all()
    .map(({ name }) => [name, db.prepare(`SELECT * FROM ${name} ORDER BY rowid`).all()])) });
function oldDatabase(t) {
  const db = new DatabaseSync(':memory:'); db.exec('PRAGMA foreign_keys=ON'); createHistoricalAppsV5(db); t.after(() => db.close());
  db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run('link|host|connector', owner.accountId,
    '{"linkId":"link","hostDeviceId":"host","connectorId":"connector"}', 'Device', 1);
  for (const [id, state, port] of [[app, 'enabled', 8000], [retired, 'revoked', 8001]]) {
    db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(id, owner.accountId, 'link|host|connector', 'Historical app', port, '/#/board',
      '{"accountIds":["friend"],"communityIds":["private_group"]}', state, 4, 1, 2);
    db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)').run(id, 'account', 'friend');
    db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)').run(id, 'community', 'private_group');
    db.prepare('INSERT INTO app_domain_heads VALUES (?,1)').run(id); seedHistoricalPublicationV3(db, id);
  }
  seedHistoricalRollbackV4(db, app); db.prepare('INSERT INTO app_source_heads VALUES (?,1)').run(retired);
  db.exec("INSERT INTO app_domain_zones VALUES ('named','named','https://{slug}.apps.example','apps.example','https','',1)");
  db.prepare("INSERT INTO app_domains VALUES (?,'named','kept.apps.example','https://kept.apps.example','kept',?,'owner','alias','bound',1,NULL)").run(domain, app);
  db.prepare("INSERT INTO app_domains VALUES (?,'named','retired.apps.example','https://retired.apps.example','retired',?,'owner','alias','tombstone',1,2)")
    .run(`dom_${'e'.repeat(32)}`, retired);
  db.prepare('INSERT INTO app_saved_heads VALUES (?,2)').run(owner.accountId);
  db.prepare('INSERT INTO app_saved_entries VALUES (?,?,?,?,?,?,?,?)').run(owner.accountId, app, domain, 'https://kept.apps.example', '/saved#old', 'Old saved label', 1, 2);
  db.prepare('INSERT INTO app_saved_receipts VALUES (?,?,?,?,?,?,?)').run(owner.accountId, 'a'.repeat(64), 'b'.repeat(64), app, 1, 1, 2);
  return db;
}
function populated(t) {
  const db = oldDatabase(t); migrateAppsSchema(db);
  const model = createDiscussionRegistry({ db, assertActor: () => true, withAuthorityFence: fn => fn(), canUse: () => true,
    now: () => 100, authorLabel: actor => actor.label, readCommunityAuthority: () => [],
    resolveEntry: input => ({ appId: app, domainId: domain, origin: 'https://kept.apps.example', path: input.path ?? '/' }) });
  const scope = { appId: app, domainId: domain, path: '/thread' };
  const run = (op, args) => model.execute({ op: `apps.discussion.${op}`, args, actor: owner });
  const context = run('context', scope).context;
  const result = run('send', { ...scope, conversationId: context.conversationId, requestId: 'message', body: 'Private body' });
  return { db, run, context, result };
}

test('literal historical5 migrates to6 preserving every existing row and lazily creating no conversation/head', t => {
  const db = oldDatabase(t), before = snapshot(db); assert.equal(inspectAppsSchema(db), 'v5');
  assert.equal(migrateAppsSchema(db).schema, 'soty.apps-registry.v6'); assert.equal(inspectAppsSchema(db), 'v6');
  const after = snapshot(db);
  for (const [table, rows] of Object.entries(before.rows)) if (table !== 'apps_meta') assert.deepEqual(after.rows[table], rows, table);
  const added = Object.keys(after.rows).filter(name => !(name in before.rows)); assert.equal(added.length, 6);
  for (const name of added) if (name !== 'app_discussion_usage') assert.deepEqual(after.rows[name], []);
  assert.deepEqual({ ...after.rows.app_discussion_usage[0] }, { id: 1, head_count: 0, conversation_count: 0, message_count: 0, body_bytes: 0 });
  assert.equal(migrateAppsSchema(db).migrated, false); assert.deepEqual(snapshot(db), after);
});

test('failed last discussion guard installation rolls back all6 objects, usage and marker', t => {
  const db = oldDatabase(t), before = snapshot(db), original = db.exec;
  db.exec = function (sql) { if (sql.includes('CREATE TRIGGER app_discussion_usage_no_replace')) throw new Error('last_guard_failed'); return original.call(this, sql); };
  assert.throws(() => migrateAppsSchema(db), /last_guard_failed/u); db.exec = original;
  assert.deepEqual(snapshot(db), before); assert.equal(inspectAppsSchema(db), 'v5');
  assert.equal(migrateAppsSchema(db).migrated, true);
});

test('known descriptor/fingerprint guards prevent replacement, deletion and resurrection while allowing one tombstone', t => {
  const f = populated(t), before = snapshot(f.db), conv = f.context.conversationId, message = f.result.receipt.id;
  for (const sql of ["DELETE FROM app_discussion_heads", "UPDATE app_discussion_heads SET current_id='conv_" + '0'.repeat(32) + "'",
    "UPDATE app_discussion_conversations SET mode='anyone'", 'DELETE FROM app_discussion_conversations',
    "UPDATE app_discussion_messages SET intent_hash='" + 'f'.repeat(64) + "'", 'DELETE FROM app_discussion_messages',
    "UPDATE app_discussion_messages SET body='different'", 'DELETE FROM app_discussion_usage',
    'INSERT OR REPLACE INTO app_discussion_messages SELECT * FROM app_discussion_messages',
    'INSERT OR REPLACE INTO app_discussion_conversations SELECT * FROM app_discussion_conversations',
    'INSERT OR REPLACE INTO app_discussion_heads SELECT * FROM app_discussion_heads']) {
    assert.throws(() => f.db.exec(sql)); assert.deepEqual(snapshot(f.db), before);
  }
  f.run('remove', { appId: app, conversationId: conv, messageId: message });
  const removed = snapshot(f.db); assert.equal(migrateAppsSchema(f.db).migrated, false);
  assert.throws(() => f.db.exec("UPDATE app_discussion_messages SET body='resurrect',body_bytes=9,removed_at=NULL,removed_by=NULL"));
  assert.deepEqual(snapshot(f.db), removed);
});

test('reopen rejects broken usage, missing head and missed policy rotation without backfilling or repairs', t => {
  const mutations = [
    db => db.exec('UPDATE app_discussion_usage SET message_count=0'),
    db => db.exec('UPDATE app_discussion_heads SET message_count=0'),
    db => db.exec('UPDATE app_discussion_conversations SET change_seq=7'),
    db => db.exec('DELETE FROM app_discussion_changes'),
    db => db.exec("UPDATE local_apps SET grants_json='{\"accountIds\":[],\"communityIds\":[]}' WHERE id='" + app + "'"),
    db => { const ddl = db.prepare("SELECT sql FROM sqlite_schema WHERE name='app_discussion_head_no_delete'").get().sql;
      db.exec('PRAGMA foreign_keys=OFF; DROP TRIGGER app_discussion_head_no_delete; DELETE FROM app_discussion_heads'); db.exec(ddl); db.exec('PRAGMA foreign_keys=ON'); },
  ];
  for (const mutate of mutations) {
    const { db } = populated(t); mutate(db); const before = snapshot(db);
    assert.throws(() => migrateAppsSchema(db), { code: 'apps_registry_corrupt' }); assert.deepEqual(snapshot(db), before);
  }
});

test('future7 or missing/changed known guards are refused before persistent state changes', t => {
  for (const mutate of [db => db.exec("UPDATE apps_meta SET value='soty.apps-registry.v7' WHERE key='schema'; PRAGMA user_version=7"),
    db => db.exec('DROP TRIGGER app_discussion_head_lineage'), db => db.exec('DROP INDEX app_discussion_message_request'),
    db => db.exec('DROP TRIGGER app_discussion_message_redaction; CREATE TRIGGER app_discussion_message_redaction BEFORE UPDATE ON app_discussion_messages BEGIN SELECT 1; END')]) {
    const { db } = populated(t); mutate(db); const before = snapshot(db);
    assert.throws(() => migrateAppsSchema(db), { code: 'apps_schema_unsupported' }); assert.deepEqual(snapshot(db), before);
  }
});

test('rotated private history remains immutable and valid across reopen, including rollback to the original audience', t => {
  const f = populated(t), first = f.context.conversationId;
  f.db.exec('BEGIN IMMEDIATE'); f.db.prepare('UPDATE local_apps SET grants_json=? WHERE id=?').run('{"accountIds":[],"communityIds":[]}', app);
  syncDiscussionAudienceInTransaction(f.db, app, 101); f.db.exec('COMMIT');
  const second = f.db.prepare('SELECT current_id FROM app_discussion_heads').get().current_id;
  f.db.exec('BEGIN IMMEDIATE'); f.db.prepare('UPDATE local_apps SET grants_json=? WHERE id=?').run('{"accountIds":["friend"],"communityIds":["private_group"]}', app);
  syncDiscussionAudienceInTransaction(f.db, app, 102); f.db.exec('COMMIT');
  const third = f.db.prepare('SELECT current_id FROM app_discussion_heads').get().current_id;
  assert.notEqual(first, second); assert.notEqual(first, third);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM app_discussion_conversations').get().n, 1);
  const before = snapshot(f.db); assert.equal(migrateAppsSchema(f.db).migrated, false); assert.deepEqual(snapshot(f.db), before);
});
