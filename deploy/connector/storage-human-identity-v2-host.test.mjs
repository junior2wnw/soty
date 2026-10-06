import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, mkdir, readFile, rm, writeFile, realpath } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { tmpdir } from 'node:os';
import { join, dirname, basename } from 'node:path';
import { readStorageFormat } from './storage-probe.mjs';
import { snapshotStorage } from './storage-snapshot.mjs';
import { assertStorageCompatible, currentStorageReaders, storageReaderLabel } from './storage-guard.mjs';

const sql = await readFile(new URL('./fixtures/human-identity-v2/identity.sql', import.meta.url), 'utf8');
const oldReaders = '{"version":5,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6],"notes":[1,2],"capabilities":[1,2,3],"appRegistration":[1],"feedback":[1],"humanIdentity":[1]}}';
const image = readers => ({ Id: 'sha256:' + '1'.repeat(64), Config: { Labels: { [storageReaderLabel]: readers } } });
async function fixture(t) {
  const parent = await realpath(tmpdir()), root = await realpath(await mkdtemp(join(parent, 'soty-human-v2-host-'))), owner = randomUUID();
  await writeFile(join(root, 'owner'), owner, { flag: 'wx' });
  t.after(async () => {
    db.close();
    assert.equal(await realpath(root), root); assert.equal(dirname(root), parent); assert.match(basename(root), /^soty-human-v2-host-/u);
    assert.equal(await readFile(join(root, 'owner'), 'utf8'), owner);
    await rm(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  const data = join(root, 'data'); await mkdir(join(data, 'human-identity'), { recursive: true });
  const file = join(data, 'human-identity', 'identity.sqlite'), db = new DatabaseSync(file);
  db.function('human_identity_gc_epoch', () => 0); db.exec(sql + '\nPRAGMA user_version=2');
  const insert = db.prepare('INSERT INTO human_identity_meta VALUES(?,?)');
  for (const pair of Object.entries({ lineage: 'soty.human-identity.sqlite.v2', registry_id: 'REG.soty', environment_id: 'production',
    issuer: 'https://soty.example/human-identity', profile: 'oidc-provider-9.12.2-human-v1' })) insert.run(...pair);
  return { root, data, file, db };
}

test('the actual inline host probe recognizes literal schema2 and refuses a frozen v1-only fallback', async t => {
  const f = await fixture(t), before = await readFile(f.file);
  const format = await readStorageFormat(f.data);
  assert.deepEqual(format, { ok: true, schema: 'soty.storage-format.v5', rooms: 'empty', apps: 'empty', notes: 'empty',
    capabilities: 'empty', appRegistration: 'empty', feedback: 'empty', humanIdentity: 2 });
  assert.deepEqual(await readFile(f.file), before);
  assert.throws(() => assertStorageCompatible(image(oldReaders), format), /storage_reader_incompatible/u);
  assert.deepEqual(assertStorageCompatible(image(currentStorageReaders), format), format);
  f.db.exec('DROP INDEX human_identity_artifact_retention');
  const damaged = await readFile(f.file);
  await assert.rejects(readStorageFormat(f.data), /storage_format_unreadable/u);
  assert.deepEqual(await readFile(f.file), damaged);
});

test('a bounded seven-store snapshot preserves schema2 consumed-token evidence in committed WAL without disclosure', async t => {
  const f = await fixture(t); f.db.exec('PRAGMA journal_mode=WAL');
  const sentinel = 'SYNTHETIC_PRIVATE_RENEWAL_KEY';
  f.db.prepare('INSERT INTO human_identity_artifacts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)').run('RefreshToken', 'a'.repeat(64),
    Buffer.alloc(80, 29), 'b'.repeat(64), sentinel, null, null, 'fixture-client', null, null, 'c'.repeat(64), 2000, 1000, 1, 86401);
  const wal = await readFile(f.file + '-wal'), format = await readStorageFormat(f.data), snapshot = join(f.root, 'snapshot');
  assert.equal(JSON.stringify(format).includes(sentinel), false); assert.equal(JSON.stringify(format).includes('REG.soty'), false);
  await snapshotStorage(f.data, snapshot); assert.deepEqual(await readFile(join(snapshot, 'human-identity', 'identity.sqlite-wal')), wal);
  assert.deepEqual(await readStorageFormat(snapshot), format);
  const reader = new DatabaseSync(join(snapshot, 'human-identity', 'identity.sqlite'), { readOnly: true });
  try {
    const preserved = reader.prepare('SELECT consumed_at,retain_until,payload_cipher FROM human_identity_artifacts').get();
    assert.equal(preserved.consumed_at, 1000); assert.equal(preserved.retain_until, 86401); assert.deepEqual(Buffer.from(preserved.payload_cipher), Buffer.alloc(80, 29));
  } finally { reader.close(); }
  assert.deepEqual(await readFile(f.file + '-wal'), wal);
});
