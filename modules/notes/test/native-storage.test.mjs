import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, rmSync, readFileSync, writeFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { createHistoricalNotesV1 } from '../../../deploy/connector/notes-v1.fixture.mjs';
import { migrateNotes as historicalMigrate } from '../../../deploy/connector/fixtures/notes-v1/schema.mjs';
import { inspectNotesSchema, migrateNotes, SUPPORTED_SCHEMA_VERSIONS } from '../server/schema-v2.mjs';
import { createNotesService } from '../server/index.mjs';
import { hash } from '../server/validation.mjs';

const PROJECT = 'notes-native-storage-test';
const OWNER = Object.freeze({ accountId: 'account_native_owner', deviceId: 'device_native_owner' });
const sourceId = 'a'.repeat(32), invocationId = 'inv_native_storage';
const suffix = hash(JSON.stringify(['soty.native-note.v1', sourceId, invocationId]));
const nativeId = `n_${suffix}`, mutationId = `m_${suffix}`;
const notesDigest = '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204';
const sha = value => createHash('sha256').update(value).digest('hex');
const fileHashes = file => Object.fromEntries(['', '-wal'].map(suffix => [suffix || 'main', existsSync(file + suffix) ? sha(readFileSync(file + suffix)) : null]));
const code = expected => error => error.code === expected;
function fixture(t) {
  const base = resolve(tmpdir()), directory = mkdtempSync(join(base, 'soty-native-notes-'));
  const file = join(directory, 'notes.sqlite'), handles = new Set();
  const open = () => { const db = new DatabaseSync(file); handles.add(db); return db; };
  const close = db => { if (handles.delete(db)) db.close(); };
  t.after(() => {
    for (const db of handles) db.close();
    assert.equal(dirname(resolve(directory)), base); assert.match(basename(directory), /^soty-native-notes-/u);
    rmSync(directory, { recursive: true, force: true });
  });
  return { file, open, close };
}
function domainSnapshot(db) {
  return ['notes_meta','note_accounts','notes','note_receipts','notes_fts'].map(table => {
    const rows = db.prepare(`SELECT * FROM ${table}`).all().map(row => ({ ...row }));
    if (table === 'notes_meta') return rows.filter(row => row.key === 'project_id');
    return rows;
  });
}
function call(service, op, args) {
  return service.execute({ op: `notes.${op}`, actor: OWNER, args: { expectedAccountId: OWNER.accountId, ...args } });
}
function put(noteId, mutation, expectedRevision = 0, overrides = {}) {
  return { noteId, mutationId: mutation, expectedRevision, title: 'Личное', body: 'Точный текст 😀',
    items: [], color: 'plain', pinned: false, state: 'active', ...overrides };
}

test('new and literal historical v1 stay v1 by default; migration admission is a strict boolean', t => {
  const f = fixture(t), db = f.open();
  assert.deepEqual(SUPPORTED_SCHEMA_VERSIONS, [1, 2]);
  for (const value of [null, 0, 1, '', 'true', [], {}]) {
    assert.throws(() => migrateNotes(db, PROJECT, { allowNativeMigration: value }), code('notes_invalid_arguments'));
    assert.equal(db.prepare('PRAGMA user_version').get().user_version, 0);
  }
  assert.deepEqual(migrateNotes(db, PROJECT), { schemaVersion: 1, registryId: null });
  assert.equal(db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name='note_native_creates'").get().n, 0);
  const memory = new DatabaseSync(':memory:'); t.after(() => memory.close());
  createHistoricalNotesV1(memory, PROJECT);
  assert.deepEqual(migrateNotes(memory, PROJECT), { schemaVersion: 1, registryId: null });
});

test('explicit v1 to v2 preserves real Notes, search, usage and receipts; reopening never changes registry identity', t => {
  const f = fixture(t); let db = f.open(); createHistoricalNotesV1(db, PROJECT); f.close(db);
  const service = createNotesService({ databasePath: f.file, projectId: PROJECT, clock: () => 100 });
  call(service, 'put', put('note_live_one', 'mutation_first'));
  call(service, 'put', put('note_trash_one', 'mutation_second', 0, { state: 'trashed' }));
  call(service, 'put', put('note_deleted_one', 'mutation_third', 0, { state: 'trashed' }));
  call(service, 'purge', { noteId: 'note_deleted_one', mutationId: 'mutation_purge', expectedRevision: 1 });
  service.close(); db = f.open();
  const before = domainSnapshot(db), migrated = migrateNotes(db, PROJECT, { allowNativeMigration: true });
  assert.equal(migrated.schemaVersion, 2); assert.match(migrated.registryId, /^[a-f0-9]{32}$/u);
  assert.deepEqual(domainSnapshot(db), before);
  f.close(db); db = f.open();
  assert.deepEqual(migrateNotes(db, PROJECT), migrated);
  assert.deepEqual(migrateNotes(db, PROJECT, { allowNativeMigration: true }), migrated);
  assert.equal(db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n, 0, 'migration must not invent native evidence');
  assert.throws(() => migrateNotes(db, 'other-project'), code('notes_project_mismatch'));
});

test('migration rollback leaves the historical layout and identity absent after a fault before COMMIT', t => {
  const f = fixture(t), db = f.open(); createHistoricalNotesV1(db, PROJECT);
  const before = domainSnapshot(db);
  const wrapper = { prepare: db.prepare.bind(db), get isTransaction() { return db.isTransaction; },
    exec(sql) { if (sql === 'COMMIT') throw new Error('test_commit_fault'); return db.exec(sql); } };
  assert.throws(() => migrateNotes(wrapper, PROJECT, { allowNativeMigration: true }), /test_commit_fault/u);
  assert.deepEqual(inspectNotesSchema(db, PROJECT), { schemaVersion: 1, registryId: null });
  assert.deepEqual(domainSnapshot(db), before);
  assert.equal(db.prepare("SELECT count(*) AS n FROM notes_meta WHERE key='registry_id'").get().n, 0);
  assert.equal(migrateNotes(db, PROJECT, { allowNativeMigration: true }).schemaVersion, 2);
});

test('unknown version and partial schema fail before a journal-mode write', t => {
  for (const damage of ['future', 'index', 'trigger']) {
    const f = fixture(t), db = f.open();
    migrateNotes(db, PROJECT, { allowNativeMigration: true });
    if (damage === 'future') db.exec('PRAGMA user_version=3');
    if (damage === 'index') db.exec('DROP INDEX notes_owner_order');
    if (damage === 'trigger') db.exec("DROP TRIGGER note_native_create_no_update; CREATE TRIGGER note_native_create_no_update BEFORE UPDATE ON note_native_creates BEGIN SELECT 1; END");
    db.exec('PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE');
    const before = fileHashes(f.file);
    assert.throws(() => migrateNotes(db, PROJECT, { allowNativeMigration: true }),
      code(damage === 'future' ? 'notes_schema_unsupported' : 'notes_schema_metadata_mismatch'));
    assert.equal(db.prepare('PRAGMA journal_mode').get().journal_mode, 'delete');
    assert.deepEqual(fileHashes(f.file), before);
  }
});

test('proof and registry metadata guards reject replacement/deletion; a tombstone retains creation evidence', t => {
  const f = fixture(t); let db = f.open();
  migrateNotes(db, PROJECT, { allowNativeMigration: true }); f.close(db);
  const service = createNotesService({ databasePath: f.file, projectId: PROJECT, clock: () => 200 });
  // Storage fixture only: ordinary Notes create + explicit immutable evidence seed, not a B2 executor proof.
  call(service, 'put', put(nativeId, mutationId));
  db = f.open();
  const inputDigest = hash(JSON.stringify({ body: 'Точный текст 😀', title: 'Личное' }));
  const insert = db.prepare('INSERT INTO note_native_creates VALUES(?,?,?,?,?,?,?,?,?)');
  insert.run(sourceId, invocationId, OWNER.accountId, nativeId, mutationId, inputDigest, notesDigest, 1, 200);
  const original = { ...db.prepare('SELECT * FROM note_native_creates').get() };
  assert.throws(() => db.exec('UPDATE note_native_creates SET revision=1'), /notes_native_proof_immutable/u);
  assert.throws(() => db.exec('DELETE FROM note_native_creates'), /notes_native_proof_immutable/u);
  assert.throws(() => db.prepare('INSERT OR REPLACE INTO note_native_creates VALUES(?,?,?,?,?,?,?,?,?)')
    .run(...Object.values(original)), /notes_native_proof_immutable/u);
  assert.throws(() => db.exec("UPDATE notes_meta SET value='new' WHERE key='registry_id'"), /notes_identity_immutable/u);
  assert.throws(() => db.exec("DELETE FROM notes_meta WHERE key='registry_id'"), /notes_identity_immutable/u);
  assert.throws(() => db.exec("INSERT OR REPLACE INTO notes_meta VALUES('registry_id','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')"), /notes_identity_immutable/u);
  for (let revision = 1; revision <= 34; revision++) call(service, 'put', put(nativeId, `mutation_edit_${revision}`, revision, { body: `Human edit ${revision}` }));
  call(service, 'put', put(nativeId, 'mutation_trash', 35, { state: 'trashed' }));
  call(service, 'purge', { noteId: nativeId, mutationId: 'mutation_remove', expectedRevision: 36 });
  assert.deepEqual({ ...db.prepare('SELECT * FROM note_native_creates').get() }, original);
  assert.equal(db.prepare('SELECT count(*) AS n FROM note_receipts WHERE mutation_id=?').get(mutationId).n, 0);
  assert.equal(db.prepare('SELECT state FROM notes WHERE id=?').get(nativeId).state, 'deleted');
  db.exec('PRAGMA foreign_keys=ON');
  assert.throws(() => db.prepare('DELETE FROM notes WHERE id=?').run(nativeId), /FOREIGN KEY/u);
  service.close(); f.close(db); db = f.open();
  assert.equal(migrateNotes(db, PROJECT).schemaVersion, 2);
  assert.equal(db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n, 1);
});

test('missing mandatory registry identity on v2 is corruption and is never regenerated', t => {
  const f = fixture(t), db = f.open(); migrateNotes(db, PROJECT, { allowNativeMigration: true });
  const sql = db.prepare("SELECT sql FROM sqlite_schema WHERE name='notes_identity_no_delete'").get().sql;
  db.exec('DROP TRIGGER notes_identity_no_delete');
  db.exec("DELETE FROM notes_meta WHERE key='registry_id'");
  db.exec(sql);
  assert.throws(() => migrateNotes(db, PROJECT, { allowNativeMigration: true }), code('notes_schema_metadata_mismatch'));
  assert.equal(db.prepare("SELECT count(*) AS n FROM notes_meta WHERE key='registry_id'").get().n, 0);
});

test('real historical reader refuses committed v2 WAL with unchanged main/WAL while the writer owns the WAL', t => {
  const f = fixture(t), writer = f.open(); createHistoricalNotesV1(writer, PROJECT);
  writer.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA wal_checkpoint(TRUNCATE)');
  assert.equal(readFileSync(f.file).readUInt32BE(60), 1);
  migrateNotes(writer, PROJECT, { allowNativeMigration: true });
  assert.equal(readFileSync(f.file).readUInt32BE(60), 1, 'main is still genuinely v1');
  const before = fileHashes(f.file), old = f.open();
  assert.throws(() => historicalMigrate(old, PROJECT), /notes_schema_unsupported/u);
  f.close(old);
  assert.deepEqual(fileHashes(f.file), before);
});

test('historical DELETE-mode refusal records the real legacy pragma side effect instead of claiming untouched bytes', t => {
  const f = fixture(t), db = f.open(); migrateNotes(db, PROJECT, { allowNativeMigration: true });
  db.exec('PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE');
  const before = fileHashes(f.file);
  assert.throws(() => historicalMigrate(db, PROJECT), /notes_schema_unsupported/u);
  assert.equal(db.prepare('PRAGMA user_version').get().user_version, 2);
  assert.equal(db.prepare('PRAGMA journal_mode').get().journal_mode, 'wal');
  assert.notDeepEqual(fileHashes(f.file), before, 'legacy code performs a persistent mode change before refusing');
});

test('real historical reader refuses checkpointed v2 main without rewriting it', t => {
  const f = fixture(t), db = f.open(); migrateNotes(db, PROJECT, { allowNativeMigration: true });
  db.exec('PRAGMA wal_checkpoint(TRUNCATE)'); assert.equal(readFileSync(f.file).readUInt32BE(60), 2);
  const before = fileHashes(f.file), old = f.open();
  assert.throws(() => historicalMigrate(old, PROJECT), /notes_schema_unsupported/u); f.close(old);
  assert.deepEqual(fileHashes(f.file), before);
});

test('a real malformed freelist with the exact known layout is refused before v2 data writes', t => {
  const f = fixture(t); let db = f.open(); createHistoricalNotesV1(db, PROJECT); f.close(db);
  const bytes = readFileSync(f.file);
  const freePages = bytes.readUInt32BE(36); assert.ok(freePages < 0xffffffff);
  // SQLite fileformat §1.3.8: offset 36 is the freelist count, offset 32 its trunk.
  // This preserves sqlite_schema and every domain row, unlike a fake error hook.
  bytes.writeUInt32BE(freePages + 1, 36); writeFileSync(f.file, bytes);
  db = f.open(); assert.equal(inspectNotesSchema(db, PROJECT).schemaVersion, 1);
  assert.throws(() => migrateNotes(db, PROJECT, { allowNativeMigration: true }), code('notes_storage_corrupt'));
  assert.equal(db.prepare('PRAGMA user_version').get().user_version, 1);
  assert.equal(db.prepare("SELECT count(*) AS n FROM notes_meta WHERE key='registry_id'").get().n, 0);
});

test('DELETE-mode reopen and explicit migration retain the plain integrity check without blocking checkpoint', t => {
  const f = fixture(t); let db = f.open(); createHistoricalNotesV1(db, PROJECT);
  const statements = [], wrapper = { get isTransaction() { return db.isTransaction; }, exec: sql => db.exec(sql),
    prepare(sql) { statements.push(sql); return db.prepare(sql); } };
  migrateNotes(wrapper, PROJECT, { allowNativeMigration: true });
  assert.equal(statements.filter(sql => sql === 'PRAGMA quick_check').length, 1, 'one preserved integrity pass');
  db.exec('PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE'); f.close(db); db = f.open();
  statements.length = 0;
  assert.equal(migrateNotes(wrapper, PROJECT).schemaVersion, 2);
  assert.equal(statements.filter(sql => sql === 'PRAGMA quick_check').length, 1);
  assert.equal(statements.filter(sql => sql === 'SELECT * FROM notes ORDER BY rowid').length, 1, 'no duplicate body scan');
  db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
});
