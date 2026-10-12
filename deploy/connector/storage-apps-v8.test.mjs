import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { STANDARD_ROOT_WORKSPACE_SOURCE } from '../../modules/source-app/server/standard-profile.mjs';
import { selectedResourceProfile } from '../../modules/apps/scoped-embed/resource-profile.mjs';
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

const canonical = value => Array.isArray(value) ? '[' + value.map(canonical).join(',') + ']'
  : value && typeof value === 'object' ? '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}' : JSON.stringify(value);
const digest = value => createHash('sha256').update(value).digest('hex');
async function workspaceStore(t, mutate = () => {}) {
  const { root, db } = await store(t), appId = 'app-' + 'a'.repeat(32), owner = 'owner', key = 'link|host|connector';
  db.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(key, owner, '{"linkId":"link","hostDeviceId":"host","connectorId":"connector"}', 'Source', 1);
  db.prepare('INSERT INTO local_apps VALUES(?,?,?,?,?,?,?,?,?,?,?)').run(appId, owner, key, 'Source', 5350, '/embed', '{"accountIds":[],"communityIds":[]}', 'enabled', 1, 1, 1);
  const targetDigest = digest(JSON.stringify(['soty.runtime-target.v1', appId, 2, owner, key, 5350, '/embed', 'soty.selected-human-embed.v2']));
  db.prepare('INSERT INTO app_runtime_targets VALUES(?,?,?,?,?,?,?,?,?)').run(appId, 2, owner, key, 5350, '/embed', 'soty.selected-human-embed.v2', targetDigest, 1);
  const profile = { schema: 'soty.selected-human-embed.v2', appId, connector: { linkId: 'link', hostDeviceId: 'host', connectorId: 'connector' },
    target: { revision: 2, digest: targetDigest }, sourceProfile: { ...STANDARD_ROOT_WORKSPACE_SOURCE },
    resource: { registryId: 'registry', tenantId: owner, appId, environmentId: 'test', resourceId: 'workspace',
      selection: { kind: 'soty.workspace.v1', nativeId: 'native-workspace', incarnationId: 'incarnation' } },
    issuer: 'https://shell.example/human-identity', clientId: 'client', embedOrigin: 'https://workspace.example', nativeOrigin: 'https://workspace.example', parentOrigin: 'https://shell.example' };
  mutate(profile); const bytes = canonical(profile);
  db.prepare('INSERT INTO app_scoped_embed_admissions VALUES(?,?,?,?,?,?)').run(appId, 2, targetDigest, digest(bytes), bytes, 1);
  return { root, profile };
}

test('standalone host reader accepts canonical exact-origin Root workspace2 admission', async t => {
  const { root, profile } = await workspaceStore(t);
  assert.equal(selectedResourceProfile(profile).nativeOrigin, profile.embedOrigin);
  assert.equal((await readStorageFormat(root)).apps, 8);
});

for (const [name, mutate] of [
  ['wrong digest', value => { value.sourceProfile.digest = '0'.repeat(64); }],
  ['workspace1', value => { value.sourceProfile.version = 1; }],
  ['resource profile', value => { value.sourceProfile.id = 'soty.standard-resource'; }],
  ['non-workspace kind', value => { value.resource.selection.kind = 'example.project.v1'; }],
  ['same host other port', value => { value.nativeOrigin = 'https://workspace.example:8443'; }],
  ['parent equals embed', value => { value.parentOrigin = value.embedOrigin; }],
]) test(`exact-origin exception refuses ${name} with valid canonical admission hashes`, async t => {
  const { root, profile } = await workspaceStore(t, mutate);
  assert.throws(() => selectedResourceProfile(profile));
  await assert.rejects(readStorageFormat(root), { code: 'storage_format_unreadable' });
});
