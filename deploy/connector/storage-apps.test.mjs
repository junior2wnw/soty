import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, readdir, writeFile, rm, lstat, symlink } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { migrateAppsSchema } from '../../modules/apps/server/schema.mjs';
import { createRoomStore } from '../../server/room-store.js';
import { readStorageFormat } from './storage-probe.mjs';
import { assertStorageCompatible, currentStorageReaders, storageReaderLabel } from './storage-guard.mjs';
import { createHistoricalAppsV2 } from './apps-v2.fixture.mjs';
import { createHistoricalAppsV3, seedHistoricalPublicationV3 } from './apps-v3.fixture.mjs';
import { createHistoricalAppsV4, seedHistoricalRollbackV4 } from './apps-v4.fixture.mjs';
import { migrateAppsSchema as migrateHistoricalAppsV5 } from './fixtures/apps-v5/schema.mjs';

const format = (apps, rooms = 'empty') => ({ ok: true, schema: 'soty.storage-format.v3', rooms, apps, notes: 'empty', capabilities: 'empty' });
const image = readers => ({ Id: 'sha256:' + '1'.repeat(64), Config: { Labels: { [storageReaderLabel]: readers ?? currentStorageReaders } } });
const filename = root => path.join(root, 'apps', 'registry.sqlite');
const privateAppId = 'app-' + 'a'.repeat(32);

function seedPrivateApp(db) {
  db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run('connector-private', 'account-private', '{"synthetic":"private"}', 'Private device', 1);
  db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(privateAppId, 'account-private', 'connector-private',
    'Private project', 9001, '/', '{"accountIds":["account_guest"],"communityIds":[]}', 'enabled', 7, 1, 2);
  db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)').run(privateAppId, 'account', 'account_guest');
}

async function directory(t) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'soty-app-format-'));
  t.after(async () => {
    assert.equal(path.dirname(path.resolve(root)), path.resolve(os.tmpdir()));
    assert.match(path.basename(root), /^soty-app-format-/u);
    await rm(root, { recursive: true, force: true });
  });
  return root;
}

// Historical v1 fixture, independent of the new migration. The production
// probe never imports this application source or any candidate code.
async function v1(root, version = 1) {
  await mkdir(path.join(root, 'apps'));
  const db = new DatabaseSync(filename(root));
  try {
    db.exec(`CREATE TABLE apps_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
      INSERT INTO apps_meta VALUES ('schema','soty.apps-registry.v1');
      CREATE TABLE app_devices (connector_key TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,identity_json TEXT NOT NULL,name TEXT NOT NULL,created_at INTEGER NOT NULL);
      CREATE TABLE local_apps (id TEXT PRIMARY KEY,owner_account_id TEXT NOT NULL,connector_key TEXT NOT NULL REFERENCES app_devices(connector_key),name TEXT NOT NULL,port INTEGER NOT NULL,entry_path TEXT NOT NULL,grants_json TEXT NOT NULL,state TEXT NOT NULL,revision INTEGER NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
      CREATE INDEX local_apps_owner ON local_apps(owner_account_id);
      CREATE TABLE local_app_grants (app_id TEXT NOT NULL REFERENCES local_apps(id) ON DELETE CASCADE,kind TEXT NOT NULL CHECK(kind IN ('account','community')),principal_id TEXT NOT NULL,PRIMARY KEY(app_id,kind,principal_id));
      CREATE INDEX local_app_grants_principal ON local_app_grants(kind,principal_id,app_id);`);
    db.exec('PRAGMA user_version=' + version);
    seedPrivateApp(db);
  } finally { db.close(); }
}

async function v2(root, version = 2) {
  await mkdir(path.join(root, 'apps'));
  const db = new DatabaseSync(filename(root));
  try {
    if (version === 4) createHistoricalAppsV4(db); else if (version === 3) createHistoricalAppsV3(db); else createHistoricalAppsV2(db);
    seedPrivateApp(db);
    db.prepare('INSERT INTO app_domain_heads VALUES (?,2)').run(privateAppId);
    db.exec("INSERT INTO app_domain_zones VALUES ('zone_test','named','https://{slug}.apps.example','apps.example','https','',1)");
    db.prepare("INSERT INTO app_domains VALUES ('dom_retired','zone_test','retired.apps.example','https://retired.apps.example','retired',?,?,'alias','tombstone',1,2)")
      .run(privateAppId, 'account-private');
    if (version >= 3) {
      seedHistoricalPublicationV3(db, privateAppId);
      const target = db.prepare('SELECT digest,profile FROM app_runtime_targets WHERE app_id=?').get(privateAppId);
      const ack = JSON.stringify({ scope: 'whole-port', targetRevision: 1, targetDigest: target.digest, profile: target.profile });
      db.prepare("INSERT INTO app_domains VALUES ('dom_live','zone_test','live.apps.example','https://live.apps.example','live',?,?,'alias','bound',3,NULL)")
        .run(privateAppId, 'account-private');
      db.prepare('INSERT INTO app_publication_domains VALUES (?,?,?)').run(privateAppId, 'dom_live', 'account-private');
      db.prepare("UPDATE app_publications SET launch_policy='anyone',listed=1,policy_epoch=4,exposure_ack_revision=1,exposure_ack_json=?,updated_at=4 WHERE app_id=?")
        .run(ack, privateAppId);
      db.prepare("INSERT INTO app_publication_receipts VALUES (?,?,?,?,?,?,?)")
        .run('account-private', 'historical-request', 'historical-intent', privateAppId, 4, '{"synthetic":"historical-publication"}', 4);
      db.prepare("INSERT INTO app_domain_receipts VALUES (?,?,?,'retire','dom_retired',2,2)")
        .run('account-private', 'historical-domain-request', 'historical-domain-intent');
      if (version === 4) seedHistoricalRollbackV4(db, privateAppId);
    }
  } finally { db.close(); }
}

async function v3(root) {
  await v2(root, 3);
}

async function v4(root) {
  await v2(root, 4);
}

async function v5(root) {
  await v4(root);
  const db = new DatabaseSync(filename(root));
  try { migrateHistoricalAppsV5(db); } finally { db.close(); }
}

async function v6(root) {
  await v5(root);
  const db = new DatabaseSync(filename(root));
  try { migrateAppsSchema(db); } finally { db.close(); }
}

test('Apps absent or genuinely empty directory is explicit and no store is created by the probe', async t => {
  const root = await directory(t);
  assert.deepEqual(await readStorageFormat(root), format('empty'));
  assert.deepEqual(await readdir(root), []);
  await mkdir(path.join(root, 'apps'));
  assert.deepEqual(await readStorageFormat(root), format('empty'));
  assert.deepEqual(await readdir(path.join(root, 'apps')), []);
  await writeFile(path.join(root, 'apps', 'registry.sqlite.bak'), 'unreviewed evidence');
  await assert.rejects(readStorageFormat(root), /storage_format_unreadable/);
});

test('historical Apps v1 through v5 plus current v6 are readable without disclosing or rewriting data', async t => {
  for (const version of [0, 1, 2, 3, 4, 5, 6]) {
    const root = await directory(t);
    if (version === 6) await v6(root); else if (version === 5) await v5(root); else if (version === 4) await v4(root); else if (version === 3) await v3(root); else if (version === 2) await v2(root); else await v1(root, version);
    const before = await readFile(filename(root));
    const observed = await readStorageFormat(root);
    assert.deepEqual(observed, format(version >= 2 ? version : 1));
    assertStorageCompatible(image(), observed);
    assert.doesNotMatch(JSON.stringify(observed), /account-private|connector-private|Private project|9001/);
    assert.deepEqual(await readFile(filename(root)), before);
  }
});

test('rooms and Apps formats coexist without one masking the other', async t => {
  const root = await directory(t); await v1(root);
  await writeFile(path.join(root, 'room_1234567890123456.json'), '{"auth":"fixture"}');
  assert.deepEqual(await readStorageFormat(root), format(1, 1));
  const rooms = createRoomStore(root); rooms.close();
  assert.deepEqual(await readStorageFormat(root), format(1, 2));
  const db = new DatabaseSync(filename(root));
  try { migrateAppsSchema(db); } finally { db.close(); }
  assert.deepEqual(await readStorageFormat(root), format(6, 2));
});

for (const previous of [1, 2, 3, 4, 5]) test(`committed real Apps v${previous} to v6 migration is observed in WAL without checkpointing or weakening old data`, async t => {
  const root = await directory(t); if (previous === 5) await v5(root); else if (previous === 4) await v4(root); else if (previous === 3) await v3(root); else if (previous === 2) await v2(root); else await v1(root);
  const db = new DatabaseSync(filename(root));
  try {
    db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0');
    const before = await readFile(filename(root));
    assert.equal(before.readUInt32BE(60), previous);
    assert.deepEqual(await readStorageFormat(root), format(previous));
    const preserved = ['app_devices', 'local_apps', 'local_app_grants', ...(previous >= 2
      ? ['app_domain_zones', 'app_domain_heads', 'app_domains', 'app_domain_receipts'] : []), ...(previous >= 3
      ? ['app_runtime_targets', 'app_publications', 'app_publication_domains', 'app_publication_receipts'] : []), ...(previous >= 4
      ? ['app_source_heads', 'app_source_receipts'] : []), ...(previous >= 5 ? ['app_saved_heads', 'app_saved_entries', 'app_saved_receipts'] : [])];
    const rows = Object.fromEntries(preserved.map(table => [table, db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all()]));
    const originalDdl = db.prepare("SELECT name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all();
    migrateAppsSchema(db);
    assert.ok((await lstat(filename(root) + '-wal')).size > 32);
    assert.deepEqual(await readFile(filename(root)), before, 'the migration is still in the WAL');
    const observed = await readStorageFormat(root);
    assert.deepEqual(observed, format(6));
    const oldReader = JSON.stringify({ version: 3, readers: { rooms: [1, 2], apps: [1, 2, 3, 4, 5], notes: [1], capabilities: [1] } });
    assert.throws(() => assertStorageCompatible(image(oldReader), observed), /storage_reader_incompatible/);
    assertStorageCompatible(image(), observed);
    assert.deepEqual(await readFile(filename(root)), before, 'the format probe did not checkpoint or rewrite data');
    for (const table of preserved) assert.deepEqual(db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(), rows[table]);
    for (const item of originalDdl) assert.equal(db.prepare('SELECT sql FROM sqlite_schema WHERE name=?').get(item.name).sql, item.sql, item.name);
    if (previous < 3) {
      assert.deepEqual({ ...db.prepare('SELECT launch_policy,listed,policy_epoch,active_target_revision,exposure_ack_revision FROM app_publications').get() },
        { launch_policy: 'restricted', listed: 0, policy_epoch: 1, active_target_revision: 1, exposure_ack_revision: null });
      assert.equal(db.prepare('SELECT count(*) AS n FROM app_publication_domains').get().n, 0);
    }
    assert.deepEqual({ ...db.prepare('SELECT * FROM app_source_heads').get() }, { app_id: privateAppId, required_binding_version: previous >= 4 ? 2 : 1 });
    assert.equal(db.prepare('SELECT count(*) AS n FROM app_source_receipts').get().n, previous >= 4 ? 2 : 0);
    for (const table of ['app_saved_heads', 'app_saved_entries', 'app_saved_receipts']) assert.equal(db.prepare(`SELECT count(*) AS n FROM ${table}`).get().n, 0);
    assert.equal(db.prepare('SELECT count(*) AS n FROM app_discussion_heads').get().n, 0, 'migration never opens discussions for the owner');
    assert.deepEqual({ ...db.prepare('SELECT * FROM app_discussion_usage').get() }, { id: 1, head_count: 0, conversation_count: 0, message_count: 0, body_bytes: 0 });
  } finally { db.close(); }
  assert.deepEqual(await readStorageFormat(root), format(6));
});

test('future Apps v9 in WAL refuses even while the main file is accepted v6', async t => {
  const root = await directory(t); await v6(root);
  const db = new DatabaseSync(filename(root));
  try {
    db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0');
    const before = await readFile(filename(root)); assert.equal(before.readUInt32BE(60), 6);
    db.exec("BEGIN; UPDATE apps_meta SET value='soty.apps-registry.v9' WHERE key='schema'; PRAGMA user_version=9; COMMIT");
    await assert.rejects(readStorageFormat(root), /storage_format_unknown/);
    assert.deepEqual(await readFile(filename(root)), before);
  } finally { db.close(); }
});

test('Apps zero, garbage, orphan journals, directories and unmarked SQLite never count as empty', async t => {
  for (const variant of ['zero', 'garbage', 'orphan-wal', 'orphan-shm', 'orphan-journal', 'directory', 'unmarked']) {
    const root = await directory(t); await mkdir(path.join(root, 'apps'));
    const file = filename(root);
    if (variant === 'zero') await writeFile(file, '');
    if (variant === 'garbage') await writeFile(file, Buffer.alloc(4096, 7));
    if (variant.startsWith('orphan-')) await writeFile(file + '-' + variant.slice(7), Buffer.alloc(100));
    if (variant === 'directory') await mkdir(file);
    if (variant === 'unmarked') { const db = new DatabaseSync(file); db.exec('CREATE TABLE unknown(value TEXT)'); db.close(); }
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/, variant);
  }
  const root = await directory(t); await writeFile(path.join(root, 'apps'), 'not a directory');
  await assert.rejects(readStorageFormat(root), /storage_format_unreadable/);
});

test('both Apps marker and user_version must agree, including rejection of a declared future reader', async t => {
  for (const [schema, version] of [['soty.apps-registry.v1', 2], ['soty.apps-registry.v1', 3],
    ['soty.apps-registry.v2', 0], ['soty.apps-registry.v2', 1], ['soty.apps-registry.v2', 3],
    ['soty.apps-registry.v3', 0], ['soty.apps-registry.v3', 1], ['soty.apps-registry.v3', 2], ['soty.apps-registry.v3', 4],
    ['soty.apps-registry.v4', 0], ['soty.apps-registry.v4', 1], ['soty.apps-registry.v4', 2], ['soty.apps-registry.v4', 3],
    ['soty.apps-registry.v4', 5], ['soty.apps-registry.v5', 0], ['soty.apps-registry.v5', 1], ['soty.apps-registry.v5', 2],
    ['soty.apps-registry.v5', 3], ['soty.apps-registry.v5', 4], ['soty.apps-registry.v5', 6],
    ...[0, 1, 2, 3, 4, 5, 7].map(version => ['soty.apps-registry.v6', version]),
    ...[0, 1, 2, 3, 4, 5, 6, 8].map(version => ['soty.apps-registry.v7', version]),
    ...[0, 1, 2, 3, 4, 5, 6, 7, 8, 9].map(version => ['soty.apps-registry.v8', version]), ['soty.apps-registry.v9', 9], ['unknown', 1]]) {
    const root = await directory(t); await v1(root);
    const db = new DatabaseSync(filename(root));
    try { db.prepare("UPDATE apps_meta SET value=? WHERE key='schema'").run(schema); db.exec('PRAGMA user_version=' + version); } finally { db.close(); }
    await assert.rejects(readStorageFormat(root), /storage_format_unknown/);
  }
});

test('a known Apps7 marker over Apps6 remains unreadable without its actual scoped schema', async t => {
  const root = await directory(t); await v6(root);
  const db = new DatabaseSync(filename(root));
  try {
    db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0');
    const before = await readFile(filename(root));
    db.exec("BEGIN; UPDATE apps_meta SET value='soty.apps-registry.v7' WHERE key='schema'; PRAGMA user_version=7; COMMIT");
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/);
    assert.deepEqual(await readFile(filename(root)), before);
  } finally { db.close(); }
});

test('Apps required tables, projections and the pinned origin marker must actually exist', async t => {
  const cases = [
    [1, 'DROP TABLE local_app_grants'],
    [1, 'ALTER TABLE local_apps RENAME COLUMN grants_json TO previous_grants_json'],
    [1, 'CREATE TABLE app_domains(id TEXT)'],
    [1, 'DROP TABLE local_app_grants; CREATE VIEW local_app_grants AS SELECT 1 AS app_id,2 AS kind,3 AS principal_id'],
    [2, 'DROP TABLE app_domain_receipts'],
    [2, 'ALTER TABLE app_domains RENAME COLUMN owner_account_id TO previous_owner_account_id'],
    [2, "DELETE FROM apps_meta WHERE key='legacy_origin_template'"],
    [2, "DELETE FROM apps_meta WHERE key='schema'"],
    [3, 'DROP TABLE app_publication_receipts'],
    [3, 'DROP TABLE app_publication_domains'],
    [3, 'DROP TABLE app_publications'],
    [3, 'ALTER TABLE app_runtime_targets RENAME COLUMN digest TO previous_digest'],
    [3, 'ALTER TABLE app_publications RENAME COLUMN exposure_ack_json TO previous_ack'],
    [3, 'ALTER TABLE app_publication_domains RENAME COLUMN owner_account_id TO previous_owner'],
    [3, 'ALTER TABLE app_publication_receipts RENAME COLUMN committed_epoch TO previous_epoch'],
    [3, "DELETE FROM apps_meta WHERE key='legacy_origin_template'"],
    [3, "UPDATE apps_meta SET value='soty.apps-registry.v4' WHERE key='schema'; PRAGMA user_version=4"],
    [4, 'DROP TABLE app_source_receipts'],
    [4, 'DROP TABLE app_source_heads'],
    [4, 'ALTER TABLE app_source_heads RENAME COLUMN required_binding_version TO previous_binding_version'],
    [4, 'ALTER TABLE app_source_receipts RENAME COLUMN committed_epoch TO previous_epoch'],
    [4, 'ALTER TABLE app_source_receipts RENAME COLUMN intent_hash TO previous_intent_hash'],
  ];
  for (const [version, mutation] of cases) {
    const root = await directory(t); if (version === 4) await v4(root); else if (version === 3) await v3(root); else if (version === 2) await v2(root); else await v1(root);
    const db = new DatabaseSync(filename(root));
    // This case deliberately creates an invalid database shape; live writers
    // keep foreign keys enabled. Disabling only here lets DROP reach the probe.
    try { db.exec('PRAGMA foreign_keys=OFF'); db.exec(mutation); } finally { db.close(); }
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/, mutation);
  }
});

test('Apps v4 requires the exact table and body of every sticky floor and target replacement guard', async t => {
  const guards = ['app_runtime_target_no_update', 'app_runtime_target_no_delete', 'app_source_head_no_downgrade',
    'app_source_head_no_delete', 'app_source_head_no_replace_downgrade', 'app_runtime_target_no_replace'];
  for (const name of guards) {
    for (const kind of ['missing', 'wrong-body', 'wrong-table']) {
      const root = await directory(t); await v4(root);
      const db = new DatabaseSync(filename(root));
      try {
        const before = db.prepare('SELECT sql,tbl_name FROM sqlite_schema WHERE name=?').get(name);
        db.exec(`DROP TRIGGER ${name}`);
        if (kind === 'wrong-body') db.exec(`CREATE TRIGGER ${name} BEFORE UPDATE ON ${before.tbl_name} BEGIN SELECT 1; END`);
        if (kind === 'wrong-table') db.exec(before.sql.replace(` ON ${before.tbl_name} `, ' ON local_apps '));
      } finally { db.close(); }
      await assert.rejects(readStorageFormat(root), /storage_format_unreadable/, `${name}:${kind}`);
    }
  }
});

test('Apps v3 recognizes only both frozen immutable-target triggers, not their names alone', async t => {
  const mutations = [
    'DROP TRIGGER app_runtime_target_no_update',
    'DROP TRIGGER app_runtime_target_no_delete',
    'CREATE TRIGGER unknown_guard BEFORE UPDATE ON local_apps BEGIN SELECT 1; END',
    'CREATE VIEW unknown_view AS SELECT 1',
    'DROP TRIGGER app_runtime_target_no_update; CREATE TRIGGER app_runtime_target_no_update BEFORE UPDATE ON app_runtime_targets BEGIN SELECT 1; END',
    "DROP TRIGGER app_runtime_target_no_update; CREATE TRIGGER app_runtime_target_no_update BEFORE UPDATE ON local_apps BEGIN SELECT RAISE(ABORT,'app_runtime_target_immutable'); END",
    "DROP TRIGGER app_runtime_target_no_delete; CREATE TRIGGER app_runtime_target_no_delete BEFORE DELETE ON app_runtime_targets BEGIN SELECT RAISE(ABORT,'APP_RUNTIME_TARGET_IMMUTABLE'); END",
  ];
  for (const mutation of mutations) {
    const root = await directory(t); await v3(root);
    const db = new DatabaseSync(filename(root));
    try { db.exec(mutation); } finally { db.close(); }
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/, mutation);
  }
  for (const version of [1, 2]) {
    const root = await directory(t); if (version === 1) await v1(root); else await v2(root);
    const db = new DatabaseSync(filename(root));
    try { db.exec('CREATE TRIGGER app_runtime_target_no_update BEFORE UPDATE ON local_apps BEGIN SELECT 1; END'); } finally { db.close(); }
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/);
  }
});

test('Apps directory symlink and nonregular journal are refused before opening data outside the checked root', async t => {
  const root = await directory(t), target = path.join(root, 'outside'); await mkdir(target);
  await symlink(target, path.join(root, 'apps'), process.platform === 'win32' ? 'junction' : 'dir');
  await assert.rejects(readStorageFormat(root), /storage_format_unreadable/);
  const nested = await directory(t); await v2(nested);
  await mkdir(filename(nested) + '-wal');
  await assert.rejects(readStorageFormat(nested), /storage_format_unreadable/);
});

test('Apps database and each journal symlink refuse even when the target would be readable', async t => {
  for (const suffix of ['', '-wal', '-shm', '-journal']) {
    const root = await directory(t); await v2(root);
    const target = path.join(root, 'outside.sqlite'); await writeFile(target, await readFile(filename(root)));
    if (suffix === '') await rm(filename(root));
    try { await symlink(target, filename(root) + suffix, 'file'); }
    catch (error) { if (['EPERM', 'EACCES', 'ENOTSUP'].includes(error.code)) { t.skip('file symlinks need platform permission; Linux canary remains required'); return; } throw error; }
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/, suffix || 'main');
  }
});
