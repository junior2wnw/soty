import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { randomBytes } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createOrdinaryAppStore } from '../examples/ordinary-app/store.mjs';
import { readOrdinaryFormat1 } from '../examples/ordinary-app/reader.mjs';

test('literal independent Source reader accepts exact new Native format1; unexpected schema is refused before writer/start without mutation', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-ordinary-reader-')); t.after(() => rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 30 }));
  const options = { databasePath: join(directory, 'source.sqlite'), realmId: 'board', key: randomBytes(32), keyId: 'fixture-key', initialize: true };
  const source = createOrdinaryAppStore(options); source.createResource({ id: 'selected', incarnationId: 'one', title: 'Synthetic' }); source.close();
  assert.equal(readOrdinaryFormat1(options.databasePath, 'board').objects, 18);
  assert.throws(() => readOrdinaryFormat1(options.databasePath, 'other'), error => error.code === 'ordinary_source_reader_refused');
  const db = new DatabaseSync(options.databasePath); db.exec('CREATE TABLE unexpected_shadow_acl(id TEXT PRIMARY KEY)');
  const before = db.prepare('SELECT type,name,sql FROM sqlite_master ORDER BY type,name').all(); db.close();
  assert.throws(() => createOrdinaryAppStore({ ...options, initialize: false }), error => error.code === 'ordinary_source_reader_refused');
  const read = new DatabaseSync(options.databasePath, { readOnly: true }); assert.deepEqual(read.prepare('SELECT type,name,sql FROM sqlite_master ORDER BY type,name').all(), before); read.close();
});
