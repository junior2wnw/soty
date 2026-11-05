import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { migrateDatabase } from './test-support/connect-v5-schema-330afe.mjs';
import { readConnectFormat, readStorageFormat } from './storage-probe.mjs';
import { assertStorageCompatible, checkedStorageFormat, currentStorageReaders, storageReaders } from './storage-guard.mjs';
import { snapshotStorage } from './storage-snapshot.mjs';

// Public immutable serving source, not a candidate initializer or production DATA.
const SERVING_COMMIT = '330afe853dd8b40c13faa4d7256828b583d4ee3d';
const SCHEMA_SHA = '5cc5e16a5dd271c2b7501fd90f873e5e99fefbc13eb799bae8ddecebb7245ce9';
const SERVING_IMAGE = 'sha256:c5821dd11eb84b5467ff3c6826716dfc30b30a4ecdcff91a9266327825d47588';
const servingReaders = {
  version: 7,
  readers: { rooms: [1, 2], apps: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14], notes: [1, 2], capabilities: [1, 2, 3], appRegistration: [1], feedback: [1], humanIdentity: [1, 2, 3], connect: [3, 4, 5], world: [6, 7] },
};
const image = (readers = servingReaders) => ({ Id: SERVING_IMAGE, Config: { Labels: { 'io.soty.storage.readers': JSON.stringify(readers) } } });
const sha = bytes => createHash('sha256').update(bytes).digest('hex');

async function fixture(t, version = 5) {
  const workspace = await mkdtemp(path.join(tmpdir(), 'soty-connect5-knowledge-'));
  const root = path.join(workspace, 'data');
  t.after(async () => {
    const absolute = path.resolve(workspace);
    assert.equal(path.dirname(absolute), path.resolve(tmpdir()));
    assert.match(path.basename(absolute), /^soty-connect5-knowledge-/u);
    await rm(absolute, { recursive: true, force: true });
  });
  await mkdir(path.join(root, 'connect'), { recursive: true });
  const filename = path.join(root, 'connect', 'accounts.sqlite');
  const db = new DatabaseSync(filename);
  try {
    migrateDatabase(db, 'soty', { supportedVersion: version });
    if (version === 5) db.prepare('INSERT INTO credential_security VALUES(1,?)').run('synthetic-retained-data');
  } finally { db.close(); }
  return { root, workspace, filename, async edit(sql) { const db = new DatabaseSync(filename); try { db.exec(sql); } finally { db.close(); } } };
}

test('frozen Connect5 initializer is byte-exact serving commit source', async () => {
  assert.match(SERVING_COMMIT, /^[a-f0-9]{40}$/u);
  assert.equal(sha(await readFile(new URL('./test-support/connect-v5-schema-330afe.mjs', import.meta.url))), SCHEMA_SHA);
});

test('actual serving reader metadata accepts verified Connect5 without relabelling the image', () => {
  const original = JSON.stringify(servingReaders);
  assert.deepEqual(storageReaders(image()), servingReaders.readers);
  assert.equal(JSON.stringify(servingReaders), original);
});

test('actual serving Connect5 schema is read-only recognized; candidate4 remains denied', async t => {
  const f = await fixture(t), before = await readFile(f.filename);
  assert.equal(await readConnectFormat(f.root), 5);
  const format = await readStorageFormat(f.root);
  assert.equal(format.connect, 5);
  assertStorageCompatible(image(), format);
  const previous = structuredClone(servingReaders); previous.readers.connect = [3, 4];
  assert.deepEqual(previous.readers.connect, [3, 4]);
  assert.throws(() => assertStorageCompatible(image(previous), format), /storage_reader_incompatible/u);
  assert.deepEqual(JSON.parse(currentStorageReaders).readers.connect, [3, 4, 5]);
  assertStorageCompatible(image(JSON.parse(currentStorageReaders)), format);
  assert.deepEqual(await readFile(f.filename), before);
});

test('existing snapshot copies Connect5 byte-exact without migration or a new storage helper', async t => {
  const f = await fixture(t), before = await readFile(f.filename);
  const target = path.join(f.workspace, 'snapshot');
  await snapshotStorage(f.root, target);
  assert.equal(await readConnectFormat(target), 5);
  assert.deepEqual(await readFile(path.join(target, 'connect', 'accounts.sqlite')), before);
  assert.deepEqual(await readFile(f.filename), before);
});

for (const version of [3, 4]) test(`Connect${version} remains recognized without rewriting`, async t => {
  const f = await fixture(t, version), before = await readFile(f.filename);
  assert.equal(await readConnectFormat(f.root), version);
  assert.deepEqual(await readFile(f.filename), before);
});

test('unknown Connect6 data and metadata are rejected, including a claimed future reader', async t => {
  const f = await fixture(t);
  await f.edit("PRAGMA user_version=6; UPDATE connect_meta SET value='6' WHERE key='schema_version'");
  const before = await readFile(f.filename);
  await assert.rejects(readConnectFormat(f.root), /storage_format_unknown/u);
  const claim = structuredClone(servingReaders); claim.readers.connect.push(6);
  assert.throws(() => storageReaders(image(claim)), /storage_reader_unknown/u);
  assert.throws(() => checkedStorageFormat({ ok: true, schema: 'soty.storage-format.v7', rooms: 'empty', apps: 'empty', notes: 'empty', capabilities: 'empty', appRegistration: 'empty', feedback: 'empty', humanIdentity: 'empty', connect: 6, world: 'empty' }), /storage_probe_invalid/u);
  assert.deepEqual(await readFile(f.filename), before);
});

for (const [name, sql] of [
  ['wrong project', "UPDATE connect_meta SET value='other-project' WHERE key='project_id'"],
  ['wrong lineage', "UPDATE connect_meta SET value='other-lineage' WHERE key='schema_lineage'"],
  ['mismatched version', "UPDATE connect_meta SET value='4' WHERE key='schema_version'"],
  ['old minimum reader', "UPDATE connect_meta SET value='1' WHERE key='min_reader'"],
  ['extra metadata', "INSERT INTO connect_meta VALUES('future','value')"],
]) test(`Connect5 rejects ${name} without changing source bytes`, async t => {
  const f = await fixture(t); await f.edit(sql);
  const before = await readFile(f.filename);
  await assert.rejects(readConnectFormat(f.root), /storage_format_unknown/u);
  assert.deepEqual(await readFile(f.filename), before);
});

for (const [name, sql] of [
  ['dropped credential index', 'DROP INDEX credential_lookup'],
  ['changed credential index predicate', 'DROP INDEX credential_lookup; CREATE UNIQUE INDEX credential_lookup ON credential_methods(kind,lookup) WHERE enabled=0'],
  ['unexpected table', 'CREATE TABLE unexpected(value)'],
  ['malformed admission policy', 'UPDATE platform_admission SET revision=-1'],
]) test(`Connect5 rejects ${name} as unreadable, preserving source bytes`, async t => {
  const f = await fixture(t); await f.edit(sql);
  const before = await readFile(f.filename);
  await assert.rejects(readConnectFormat(f.root), /storage_format_unreadable/u);
  assert.deepEqual(await readFile(f.filename), before);
});

test('legacy reader metadata remains closed and rejects duplicate, future-store and typed claims', () => {
  for (const change of [
    v => { v.readers.connect = [3, 4, 5, 5]; },
    v => { v.readers.connect = ['5']; },
    v => { v.readers.connect = []; },
    v => { v.readers.futureStore = [1]; },
    v => { v.allowUnknown = true; },
  ]) {
    const claim = structuredClone(servingReaders); change(claim);
    assert.throws(() => storageReaders(image(claim)), /storage_reader_unknown/u);
  }
});
