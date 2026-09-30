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

const format = (apps, rooms = 'empty') => ({ ok: true, schema: 'soty.storage-format.v2', rooms, apps });
const image = readers => ({ Id: 'sha256:' + '1'.repeat(64), Config: { Labels: { [storageReaderLabel]: readers ?? currentStorageReaders } } });
const filename = root => path.join(root, 'apps', 'registry.sqlite');

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
    db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run('connector-private', 'account-private', '{"synthetic":"private"}', 'Private device', 1);
    db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run('app-' + 'a'.repeat(32), 'account-private', 'connector-private',
      'Private project', 9001, '/', '{"accountIds":[],"communityIds":[]}', 'enabled', 7, 1, 2);
  } finally { db.close(); }
}

async function v2(root) {
  await v1(root);
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

test('Apps marker with historical user_version 0 or 1 and migrated v2 are independently readable without data disclosure', async t => {
  for (const version of [0, 1, 2]) {
    const root = await directory(t);
    if (version === 2) await v2(root); else await v1(root, version);
    const before = await readFile(filename(root));
    const observed = await readStorageFormat(root);
    assert.deepEqual(observed, format(version === 2 ? 2 : 1));
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
  assert.deepEqual(await readStorageFormat(root), format(2, 2));
});

test('committed real Apps migration in WAL is observed while the main file still contains v1', async t => {
  const root = await directory(t); await v1(root);
  const db = new DatabaseSync(filename(root));
  try {
    db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0');
    const before = await readFile(filename(root));
    assert.equal(before.readUInt32BE(60), 1);
    assert.deepEqual(await readStorageFormat(root), format(1));
    migrateAppsSchema(db);
    assert.ok((await lstat(filename(root) + '-wal')).size > 32);
    assert.deepEqual(await readFile(filename(root)), before, 'the migration is still in the WAL');
    const observed = await readStorageFormat(root);
    assert.deepEqual(observed, format(2));
    const oldReader = JSON.stringify({ version: 2, readers: { rooms: [1, 2], apps: [1] } });
    assert.throws(() => assertStorageCompatible(image(oldReader), observed), /storage_reader_incompatible/);
    assertStorageCompatible(image(), observed);
    assert.deepEqual(await readFile(filename(root)), before, 'the format probe did not checkpoint or rewrite data');
    const privateApp = db.prepare('SELECT state,grants_json,revision FROM local_apps').get();
    assert.deepEqual({ ...privateApp }, { state: 'enabled', grants_json: '{"accountIds":[],"communityIds":[]}', revision: 7 });
  } finally { db.close(); }
  assert.deepEqual(await readStorageFormat(root), format(2));
});

test('unknown future Apps version in WAL refuses even while the main file is accepted v2', async t => {
  const root = await directory(t); await v2(root);
  const db = new DatabaseSync(filename(root));
  try {
    db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0');
    const before = await readFile(filename(root)); assert.equal(before.readUInt32BE(60), 2);
    db.exec("BEGIN; UPDATE apps_meta SET value='soty.apps-registry.v3' WHERE key='schema'; PRAGMA user_version=3; COMMIT");
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
    ['soty.apps-registry.v2', 0], ['soty.apps-registry.v2', 1], ['soty.apps-registry.v2', 3], ['soty.apps-registry.v3', 3], ['unknown', 1]]) {
    const root = await directory(t); await v1(root);
    const db = new DatabaseSync(filename(root));
    try { db.prepare("UPDATE apps_meta SET value=? WHERE key='schema'").run(schema); db.exec('PRAGMA user_version=' + version); } finally { db.close(); }
    await assert.rejects(readStorageFormat(root), /storage_format_unknown/);
  }
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
  ];
  for (const [version, mutation] of cases) {
    const root = await directory(t); if (version === 2) await v2(root); else await v1(root);
    const db = new DatabaseSync(filename(root));
    try { db.exec(mutation); } finally { db.close(); }
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/, mutation);
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
