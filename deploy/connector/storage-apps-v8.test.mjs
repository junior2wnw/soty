import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { migrateAppsSchema } from '../../modules/apps/server/schema.mjs';
import { readStorageFormat } from './storage-probe.mjs';
import { assertStorageCompatible, currentStorageReaders, storageReaderLabel } from './storage-guard.mjs';
function image(readers) { return { Id: 'sha256:' + 'a'.repeat(64), Config: { Labels: { [storageReaderLabel]: readers } } }; }
async function store(t, version = 8) {
  const root = await mkdtemp(join(tmpdir(), 'soty-apps8-')); await mkdir(join(root, 'apps'));
  const db = new DatabaseSync(join(root, 'apps', 'registry.sqlite'));
  t.after(async () => { db.close(); assert.equal(dirname(resolve(root)), resolve(tmpdir())); assert.match(basename(root), /^soty-apps8-/u); await rm(root, { recursive: true, force: true }); });
  migrateAppsSchema(db, { allowScopedEmbedMigration: true, allowSelectedResourceMigration: version === 8 }); return { root, db };
}
test('standalone probe recognizes exact Apps8 and reader7 image is refused; compatible fallback8 reads old and new formats', async t => {
  const { root } = await store(t), format = await readStorageFormat(root);
  assert.deepEqual(format, { ok: true, schema: 'soty.storage-format.v3', rooms: 'empty', apps: 8, notes: 'empty', capabilities: 'empty' });
  assert.equal(assertStorageCompatible(image(currentStorageReaders), format).apps, 8);
  const old = JSON.parse(currentStorageReaders); old.readers.apps = old.readers.apps.filter(value => value !== 8);
  assert.throws(() => assertStorageCompatible(image(JSON.stringify(old)), format), { code: 'storage_reader_incompatible' });
  for (const apps of [1,2,3,4,5,6,7]) assert.equal(assertStorageCompatible(image(currentStorageReaders), { ...format, apps }).apps, apps);
});
test('Apps8 probe rejects marker-swapped7 layout and a missing immutable admission guard, including WAL', async t => {
  const { root, db } = await store(t); db.exec('PRAGMA journal_mode=WAL;PRAGMA wal_autocheckpoint=0');
  db.exec('DROP TRIGGER app_scoped_admission_no_replace');
  await assert.rejects(readStorageFormat(root), { code: 'storage_format_unreadable' });
});
test('known marker8 on older table CHECK is rejected instead of being treated as merely future data', async t => {
  const { root, db } = await store(t, 7);
  db.exec("UPDATE apps_meta SET value='soty.apps-registry.v8' WHERE key='schema';PRAGMA user_version=8");
  await assert.rejects(readStorageFormat(root), { code: 'storage_format_unreadable' });
});
