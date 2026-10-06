import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { mkdtemp, mkdir, readFile, writeFile, rm, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { readStorageFormat } from './storage-probe.mjs';
import { snapshotStorage } from './storage-snapshot.mjs';
import { assertStorageCompatible, checkedStorageFormat, currentStorageReaders, storageReaderLabel,
  guardStorageStart, requireStorageStartReceipt, storageReaders } from './storage-guard.mjs';
import { initializeHumanIdentitySchema } from '../../modules/human-identity/schema.mjs';

// Literal independent vectors seed the stores. The candidate initializer is used only for explicit conformance.
const vector = new URL('./fixtures/human-identity-v1/', import.meta.url);
const sql = await readFile(new URL('identity.sql', vector), 'utf8');
const lineage = 'soty.human-identity.sqlite.v1', profile = 'oidc-provider-9.12.2-human-v1';
const issuer = 'https://soty.example/human-identity';
const v3 = { ok: true, schema: 'soty.storage-format.v3', rooms: 'empty', apps: 'empty', notes: 'empty', capabilities: 'empty' };
const v4 = { ...v3, schema: 'soty.storage-format.v4', appRegistration: 1, feedback: 1 };
const v5 = { ...v4, schema: 'soty.storage-format.v5', humanIdentity: 1 };
const reader3 = '{"version":3,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6],"notes":[1,2],"capabilities":[1,2,3]}}';
const reader4 = '{"version":4,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6],"notes":[1,2],"capabilities":[1,2,3],"appRegistration":[1],"feedback":[1]}}';
const image = (label = currentStorageReaders) => ({ Id: 'sha256:' + '1'.repeat(64), Config: { Labels: { [storageReaderLabel]: label } } });
const sha = value => createHash('sha256').update(value).digest('hex');
const normalized = value => value.split(/('(?:[^']|'')*')/gu).map((part, index) => index % 2 ? part
  : part.replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase()).join('');
const layout = db => db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()
  .map(row => [row.type, row.name, row.tbl_name, normalized(row.sql)]);
async function directory(t) {
  const root = await mkdtemp(join(tmpdir(), 'soty-human-format-'));
  t.after(async () => { assert.equal(dirname(resolve(root)), resolve(tmpdir())); assert.match(basename(root), /^soty-human-format-/u);
    await rm(root, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 }); }); return root;
}
async function seed(root, { registryId = 'REG.soty', environmentId = 'production', issue = issuer } = {}) {
  await mkdir(join(root, 'human-identity'), { recursive: true }); const path = join(root, 'human-identity', 'identity.sqlite'), db = new DatabaseSync(path);
  try {
    db.exec(sql + '\nPRAGMA user_version=1');
    const insert = db.prepare('INSERT INTO human_identity_meta VALUES(?,?)');
    for (const [key, value] of Object.entries({ lineage, registry_id: registryId, environment_id: environmentId, issuer: issue, profile })) insert.run(key, value);
  } finally { db.close(); } return path;
}
async function seedUniversal(root, kind, registryId = 'REG.soty', environmentId = 'production') {
  const name = kind === 'feedback' ? 'feedback' : 'app-registration', table = kind === 'feedback' ? 'feedback_meta' : 'registration_metadata';
  const entry = await readFile(new URL(`./fixtures/universal-stores-v1/${name}.sql`, import.meta.url), 'utf8');
  await mkdir(join(root, name)); const db = new DatabaseSync(join(root, name, kind === 'feedback' ? 'feedback.sqlite' : 'registry.sqlite'));
  try { db.exec(entry + '\nPRAGMA user_version=1');
    const insert = db.prepare(`INSERT INTO ${table} VALUES(?,?)`);
    for (const pair of [['lineage', kind === 'feedback' ? 'soty.feedback.sqlite.v1' : 'soty.app-registration.v1'], ['registry_id', registryId], ['environment_id', environmentId]]) insert.run(...pair);
  } finally { db.close(); }
}
function mutate(path, fn) { const db = new DatabaseSync(path); try { fn(db); } finally { db.close(); } }
function metadata(db, key, value) {
  const guards = db.prepare("SELECT name,sql FROM sqlite_schema WHERE type='trigger' AND tbl_name='human_identity_meta'").all();
  for (const guard of guards) db.exec(`DROP TRIGGER ${guard.name}`);
  db.prepare('UPDATE human_identity_meta SET value=? WHERE key=?').run(value, key); for (const guard of guards) db.exec(guard.sql);
}

test('frozen HumanIdentity v1 vector pins every table/index/conditional guard independently and matches only the unreleased candidate', async () => {
  const receipt = JSON.parse(await readFile(new URL('provenance.json', vector), 'utf8')), literal = new DatabaseSync(':memory:'), actual = new DatabaseSync(':memory:');
  try {
    literal.exec(sql); assert.equal(sha(sql), receipt.ddlSha256); assert.equal(sha(JSON.stringify(layout(literal))), receipt.layoutSha256); assert.equal(layout(literal).length, receipt.objects);
    assert.equal(receipt.kind, 'reviewed-uncommitted-U2-candidate'); assert.equal(receipt.sourceBase, '698a75f82ba9780494e10e1bf060781e94095b5e');
    initializeHumanIdentitySchema(actual, { enabled: true, registryId: 'REG.soty', environmentId: 'production', issuer }); assert.deepEqual(layout(actual), layout(literal));
    const probe = await readFile(new URL('./storage-probe.mjs', import.meta.url), 'utf8'); assert.ok(probe.includes(receipt.layoutSha256));
    assert.doesNotMatch(probe, /from\s+['"][^'"]*modules\//u);
  } finally { literal.close(); actual.close(); }
});

test('only an installed human store raises the profile to seven-store v5; absence/empty preserves exact v3/v4', async t => {
  const root = await directory(t); await mkdir(join(root, 'human-identity')); assert.deepEqual(await readStorageFormat(root), v3);
  await seedUniversal(root, 'feedback'); assert.deepEqual(await readStorageFormat(root), { ...v4, appRegistration: 'empty' });
  await seedUniversal(root, 'registration'); const path = await seed(root), before = sha(await readFile(path));
  assert.deepEqual(await readStorageFormat(root), v5); assert.equal(sha(await readFile(path)), before);
  const only = await directory(t); await seed(only); assert.deepEqual(await readStorageFormat(only), { ...v5, appRegistration: 'empty', feedback: 'empty' });
});

test('old v3/v4 readers refuse v5 evidence, while current v5 reads all historical manifests without downgrading receipts', () => {
  for (const label of [reader3, reader4]) assert.throws(() => assertStorageCompatible(image(label), v5), /storage_reader_incompatible/u);
  for (const value of [v3, v4, v5]) assert.deepEqual(assertStorageCompatible(image(), value), value);
  assertStorageCompatible(image(reader4), v4); assertStorageCompatible(image(reader3), v3);
  assert.deepEqual(Object.keys(storageReaders(image())).sort(), ['appRegistration', 'apps', 'capabilities', 'feedback', 'humanIdentity', 'notes', 'rooms']);
  for (const changed of [undefined, [], [3], ['1'], [1, 1]]) {
    const manifest = JSON.parse(currentStorageReaders); if (changed === undefined) delete manifest.readers.humanIdentity; else manifest.readers.humanIdentity = changed;
    assert.throws(() => storageReaders(image(JSON.stringify(manifest))), /storage_reader_unknown/u);
  }
  for (const value of [undefined, 0, 3, '1', null]) assert.throws(() => checkedStorageFormat({ ...v5, humanIdentity: value }), /storage_probe_invalid/u);
  const receipt = { ...v5, schema: 'soty.storage-start.v5', containerId: '2'.repeat(64), image: image().Id, mountSha256: '3'.repeat(64) }; delete receipt.ok;
  requireStorageStartReceipt(receipt, receipt.containerId);
  assert.throws(() => requireStorageStartReceipt({ ...receipt, schema: 'soty.storage-start.v4' }, receipt.containerId), /storage_start_guard_missing/u);
});

test('missing/altered tables, guards, eligibility indexes and extra objects fail closed without repairing a store', async t => {
  for (const damage of ['DROP TABLE human_identity_decisions', 'DROP INDEX human_identity_artifact_expiry', 'DROP INDEX human_identity_decision_uid',
    'DROP TRIGGER human_identity_decisions_no_delete', "DROP TRIGGER human_identity_decisions_no_delete;CREATE TRIGGER human_identity_decisions_no_delete BEFORE DELETE ON human_identity_decisions BEGIN SELECT 1;END",
    'CREATE TABLE extra(value TEXT)', 'CREATE VIEW hidden AS SELECT 1']) {
    const root = await directory(t), path = await seed(root); mutate(path, db => db.exec(damage)); const before = sha(await readFile(path));
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u); assert.equal(sha(await readFile(path)), before);
  }
});

test('human metadata binds lineage/protocol/issuer and agrees with other installed store scope; future schema and malformed issuer are not empty', async t => {
  for (const [key, value] of [['lineage', 'soty.other.v1'], ['profile', 'future-provider'], ['issuer', 'https://soty.example/human-identity?x=1'],
    ['issuer', 'http://external.example/human-identity'], ['registry_id', '../escape'], ['environment_id', 'x'.repeat(513)]]) {
    const root = await directory(t), path = await seed(root); mutate(path, db => metadata(db, key, value)); await assert.rejects(readStorageFormat(root), /storage_format_unknown/u);
  }
  const future = await directory(t), path = await seed(future); mutate(path, db => db.exec('PRAGMA user_version=3')); await assert.rejects(readStorageFormat(future), /storage_format_unknown/u);
  for (const changes of [{ registryId: 'other' }, { environmentId: 'development' }]) {
    const root = await directory(t); await seedUniversal(root, 'feedback'); await seed(root, changes); await assert.rejects(readStorageFormat(root), /storage_format_unknown/u);
  }
});

test('probe output never decrypts or enumerates SDK artifacts/decisions, while private snapshot preserves encrypted bytes and committed WAL', async t => {
  const root = await directory(t), path = await seed(root), sentinel = 'SYNTHETIC_PRIVATE_SDK_PAYLOAD', db = new DatabaseSync(path);
  try {
    db.exec('PRAGMA journal_mode=WAL;PRAGMA wal_autocheckpoint=0');
    db.prepare('INSERT INTO human_identity_artifacts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)').run('Interaction', 'a'.repeat(64), Buffer.alloc(100, 29), 'b'.repeat(64), sentinel,
      null, null, 'client', null, null, 'c'.repeat(64), 9000000000, null, 1);
    const before = await readFile(path + '-wal'), result = await readStorageFormat(root); assert.equal(JSON.stringify(result).includes(sentinel), false);
    assert.equal(JSON.stringify(result).includes('REG.soty'), false); assert.deepEqual(await readFile(path + '-wal'), before);
    const targetParent = await directory(t), target = join(targetParent, 'snapshot'); await snapshotStorage(root, target);
    assert.deepEqual(await readFile(join(target, 'human-identity', 'identity.sqlite-wal')), before); assert.deepEqual(await readStorageFormat(target), result);
    await assert.rejects(snapshotStorage(root, join(targetParent, 'too-small'), 100), /storage_format_unreadable/u);
  } finally { db.close(); }
});

test('orphan human journals, unexpected files and a directory junction are retained as refusal evidence', async t => {
  const orphan = await directory(t); await mkdir(join(orphan, 'human-identity')); await writeFile(join(orphan, 'human-identity', 'identity.sqlite-wal'), 'evidence');
  await assert.rejects(readStorageFormat(orphan), /storage_format_unreadable/u);
  const root = await directory(t); await seed(root); await writeFile(join(root, 'human-identity', 'unexpected'), 'evidence');
  const targetParent = await directory(t), target = join(targetParent, 'snapshot'); await snapshotStorage(root, target);
  assert.equal(await readFile(join(target, 'human-identity', 'unexpected'), 'utf8'), 'evidence'); await assert.rejects(readStorageFormat(target), /storage_format_unreadable/u);
  const link = await directory(t), outside = await directory(t);
  try { await symlink(outside, join(link, 'human-identity'), process.platform === 'win32' ? 'junction' : 'dir'); }
  catch (error) { if (process.platform === 'win32' && error.code === 'EPERM') { t.skip('Windows link privilege unavailable; Linux release test required'); return; } throw error; }
  await assert.rejects(readStorageFormat(link), /storage_format_unreadable/u); await assert.rejects(snapshotStorage(link, join(targetParent, 'link-copy')), /storage_format_unreadable/u);
});

test('actual guard emits a v5 START receipt and refuses a v4 image before START even if its container copied a v5 label', async t => {
  const root = await directory(t); await seed(root);
  const runtime = { Id: '2'.repeat(64), Image: image().Id, State: { Running: false }, Config: { Env: ['DATA_DIR=/data'], Labels: { [storageReaderLabel]: currentStorageReaders } },
    Mounts: [{ Type: 'volume', Name: 'synthetic-data', Source: '/docker/volumes/synthetic-data', Destination: '/data', RW: true }] };
  const context = label => ({ engine: { async inspect() { return structuredClone(runtime); }, async image() { return image(label); },
    async request(_method, url) { return url.startsWith('/volumes/') ? { Name: 'synthetic-data', Driver: 'local', Scope: 'local', Mountpoint: '/docker/volumes/synthetic-data', Options: {} } : []; } },
    async probe() { return readStorageFormat(root); } });
  const receipt = await guardStorageStart(context(currentStorageReaders), runtime); assert.equal(receipt.schema, 'soty.storage-start.v5'); assert.equal(receipt.humanIdentity, 1);
  requireStorageStartReceipt(receipt, runtime.Id); await assert.rejects(guardStorageStart(context(reader4), runtime), /storage_reader_incompatible/u);
  const docker = await readFile(new URL('../../Dockerfile', import.meta.url), 'utf8'); const label = /LABEL io\.soty\.storage\.readers="(.+)"/u.exec(docker)?.[1]?.replace(/\\"/gu, '"');
  assert.equal(label, currentStorageReaders); assert.match(docker, /ARG LEGACY_UNIVERSAL_MODE=0/u); assert.match(docker, /server\/universal-mode\.js/u);
});
