import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { migrateDatabase } from '../../modules/connect/server/schema.mjs';
import { readConnectFormat, readStorageFormat } from './storage-probe.mjs';
import { assertStorageCompatible, checkedStorageFormat, currentStorageReaders, storageReaderLabel } from './storage-guard.mjs';

// Frozen additive fixture also runs in the compatibility bridge, whose writer is still schema4.
const additive5 = `
  CREATE TABLE credential_methods (
    account_id TEXT NOT NULL REFERENCES accounts(id), kind TEXT NOT NULL CHECK(kind IN('password','phrase')),
    id TEXT NOT NULL UNIQUE, enabled INTEGER NOT NULL CHECK(enabled IN(0,1)), revision INTEGER NOT NULL,
    login TEXT, lookup TEXT, salt TEXT, verifier TEXT, wrapped_key TEXT,
    request_id TEXT NOT NULL, digest TEXT NOT NULL, updated_at INTEGER NOT NULL,
    PRIMARY KEY(account_id,kind)
  );
  CREATE UNIQUE INDEX credential_lookup ON credential_methods(kind,lookup) WHERE enabled=1;
  CREATE TABLE credential_logins (
    device_id TEXT PRIMARY KEY REFERENCES installations(id), credential_id TEXT NOT NULL,
    account_id TEXT NOT NULL REFERENCES accounts(id), kind TEXT NOT NULL, lookup TEXT NOT NULL,
    digest TEXT NOT NULL, result_json TEXT NOT NULL, created_at INTEGER NOT NULL
  );
  CREATE TABLE credential_security (id INTEGER PRIMARY KEY CHECK(id=1), decoy_key TEXT NOT NULL);
  PRAGMA user_version=5;
  UPDATE connect_meta SET value='5' WHERE key='schema_version';
`;

async function fixture(t) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'soty-connect5-reader-'));
  t.after(async () => {
    assert.equal(path.dirname(path.resolve(root)), path.resolve(os.tmpdir()));
    assert.match(path.basename(root), /^soty-connect5-reader-/u);
    await rm(root, { recursive: true, force: true });
  });
  await mkdir(path.join(root, 'connect'));
  const file = path.join(root, 'connect', 'accounts.sqlite');
  const db = new DatabaseSync(file);
  try { migrateDatabase(db, 'soty', { supportedVersion: 4 }); db.exec(additive5); }
  finally { db.close(); }
  return { root, file, change(sql) { const db = new DatabaseSync(file); try { db.exec(sql); } finally { db.close(); } } };
}
const image = readers => ({ Id: 'sha256:' + 'a'.repeat(64), Config: { Labels: { [storageReaderLabel]: JSON.stringify(readers) } } });

test('the additive Connect5 fixture is independently recognized without changing its bytes; original reader4 is fenced', async t => {
  const f = await fixture(t), before = await readFile(f.file);
  assert.equal(await readConnectFormat(f.root), 5);
  const observed = await readStorageFormat(f.root);
  assert.equal(observed.connect, 5);
  const current = JSON.parse(currentStorageReaders);
  assertStorageCompatible(image(current), observed);
  const previous = structuredClone(current); previous.readers.connect = [3, 4];
  assert.throws(() => assertStorageCompatible(image(previous), observed), { code: 'storage_reader_incompatible' });
  assert.deepEqual(await readFile(f.file), before);
});

test('Connect5 cannot be smuggled through a historical six-store probe or reader declaration', async t => {
  const f = await fixture(t), observed = await readStorageFormat(f.root);
  const previous = { ...observed, schema: 'soty.storage-format.v6' }; delete previous.world;
  assert.throws(() => checkedStorageFormat(previous), { code: 'storage_probe_invalid' });
  const readers = JSON.parse(currentStorageReaders); readers.version = 6; delete readers.readers.world;
  assert.throws(() => assertStorageCompatible(image(readers), observed), { code: 'storage_reader_unknown' });
});

test('Connect5 preserves the frozen DDL boundary against missing indexes, extra columns/tables and a false schema4 marker', async t => {
  for (const sql of [
    'DROP INDEX credential_lookup',
    'ALTER TABLE credential_logins ADD COLUMN unexpected TEXT',
    'CREATE TABLE unexpected(value)',
    "PRAGMA user_version=4; UPDATE connect_meta SET value='4' WHERE key='schema_version'",
  ]) {
    const f = await fixture(t); f.change(sql);
    await assert.rejects(readConnectFormat(f.root), { code: 'storage_format_unreadable' });
  }
});

test('Connect5 metadata, admission policy and future versions still fail closed', async t => {
  for (const sql of [
    "UPDATE connect_meta SET value='1' WHERE key='min_reader'",
    "INSERT INTO connect_meta VALUES('unexpected','1')",
    'DELETE FROM platform_admission',
    "PRAGMA user_version=6; UPDATE connect_meta SET value='6' WHERE key='schema_version'",
  ]) {
    const f = await fixture(t); f.change(sql);
    await assert.rejects(readConnectFormat(f.root), /storage_format_(unknown|unreadable)/u);
  }
});
