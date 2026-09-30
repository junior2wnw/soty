import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { existsSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createNotesService } from '../server/index.mjs';
import { inspectNotesSchema, migrateNotes } from '../server/schema.mjs';

const sha = value => createHash('sha256').update(value).digest('hex');
const fault = code => error => error?.code === code;
const OWNER = Object.freeze({ accountId: 'acct_native_review', deviceId: 'device_native_review' });
const TIME = 1_780_000_000_000;
const historicalUrl = new URL('../../../deploy/connector/fixtures/notes-v1/schema.mjs', import.meta.url);
const historicalSha = 'da75ffc702fb0ee4db9878780d374f53bca0d68b46e91eafb56038b45d1e0dfa';

async function fixture(t) {
  assert.equal(sha(readFileSync(historicalUrl)), historicalSha);
  const old = await import(historicalUrl.href);
  const parent = realpathSync(tmpdir()), root = mkdtempSync(path.join(parent, 'soty-native-notes-independent-'));
  const marker = path.join(root, '.owner'), nonce = randomBytes(20).toString('hex');
  writeFileSync(marker, nonce);
  const databasePath = path.join(root, 'notes.sqlite'), handles = new Set();
  t.after(() => {
    for (const handle of [...handles].reverse()) handle.close();
    assert.equal(realpathSync(root), root); assert.equal(path.dirname(root), parent);
    assert.match(path.basename(root), /^soty-native-notes-independent-[A-Za-z0-9_-]+$/u);
    assert.equal(readFileSync(marker, 'utf8'), nonce);
    rmSync(root, { recursive: true, force: true });
  });
  const track = value => { handles.add(value); return value; };
  const close = value => { value.close(); handles.delete(value); };
  const raw = () => track(new DatabaseSync(databasePath));
  const db = raw(); old.migrateNotes(db, 'soty');
  db.exec('PRAGMA wal_autocheckpoint=0');
  const open = (options = {}) => track(createNotesService({ databasePath, projectId: 'soty', clock: () => TIME, ...options }));
  return { root, databasePath, db, old, raw, open, close };
}
const doc = (body, state = 'active') => ({ title: 'Историческая записка', body, items: [], color: 'plain', pinned: false, state });
const request = (service, op, args, actor = OWNER) => service.execute({ op, actor, args: { expectedAccountId: actor.accountId, ...args } });
function snapshot(db) {
  return Object.fromEntries([
    ['notes', 'SELECT * FROM notes ORDER BY account_id,id'],
    ['accounts', 'SELECT * FROM note_accounts ORDER BY account_id'],
    ['fts', 'SELECT rowid,scope,title,body,items FROM notes_fts ORDER BY rowid'],
    ['receipts', 'SELECT * FROM note_receipts ORDER BY account_id,note_id,mutation_id'],
  ].map(([key, sql]) => [key, db.prepare(sql).all()]));
}
function fileProof(file) {
  const proof = {};
  for (const suffix of ['', '-wal']) {
    try { proof[suffix] = sha(readFileSync(file + suffix)); }
    catch (error) { if (error.code !== 'ENOENT') throw error; proof[suffix] = null; }
  }
  return proof;
}
function populateHistorical(db) {
  const body = 'Сохранённый текст 🐝 e\u0301';
  for (const [account, noteId, state] of [
    [OWNER.accountId, 'note_seed_active', 'active'], [OWNER.accountId, 'note_seed_trashed', 'trashed'],
    [OWNER.accountId, 'note_seed_deleted', 'deleted'], ['acct_other_native', 'note_seed_other', 'archived'],
  ]) {
    const value = state === 'deleted' ? { ...doc('', 'active'), title: '', state: 'deleted' } : doc(body, state);
    const bytes = state === 'deleted' ? 0 : Buffer.byteLength(JSON.stringify(value));
    db.prepare('INSERT OR IGNORE INTO note_accounts(account_id) VALUES(?)').run(account);
    const rowid = db.prepare(`INSERT INTO notes(account_id,id,title,body,items,preview,color,pinned,state,revision,bytes,created_at,updated_at)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(account, noteId, value.title, value.body, '[]', value.body,
      'plain', 0, state, state === 'deleted' ? 3 : 1, bytes, TIME, TIME).lastInsertRowid;
    if (state !== 'deleted') db.prepare('INSERT INTO notes_fts(rowid,scope,title,body,items) VALUES(?,?,?,?,?)')
      .run(rowid, sha(account), value.title, value.body, '');
    db.prepare('UPDATE note_accounts SET bytes=bytes+?,identities=identities+1,active=active+?,archived=archived+?,trashed=trashed+? WHERE account_id=?')
      .run(bytes, Number(state === 'active'), Number(state === 'archived'), Number(state === 'trashed'), account);
    if (state === 'active') db.prepare('INSERT INTO note_receipts VALUES(?,?,?,?,?,?)').run(account, noteId,
      'mut_seed_initial', sha(JSON.stringify([0, { ...value, bytes }])), JSON.stringify({ noteId, revision: 1, updatedAt: TIME }), 1);
  }
  return body;
}

test('historical populated Notes v1 survives default reopen and explicit v2 migration with usable checkpoints', async t => {
  const f = await fixture(t), original = populateHistorical(f.db), before = snapshot(f.db);
  f.db.exec('PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE'); f.close(f.db);
  const baseline = f.open(); assert.equal(baseline.schemaVersion, 1); assert.equal(baseline.registryId, null);
  assert.deepEqual(baseline.supportedSchemaVersions, [1, 2]); assert.equal(baseline.native, undefined);
  const inspection = f.raw(); assert.deepEqual(snapshot(inspection), before);
  assert.equal(inspection.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get().busy, 0);
  assert.equal(request(baseline, 'notes.get', { noteId: 'note_seed_active' }).note.body, original);
  assert.equal(request(baseline, 'notes.list', { query: 'Сохранённый' }).notes.length, 1);
  const replay = request(baseline, 'notes.put', { noteId: 'note_seed_active', mutationId: 'mut_seed_initial',
    expectedRevision: 0, ...doc(original) });
  assert.equal(replay.replayed, true); assert.equal(replay.revision, 1);
  f.close(baseline); inspection.exec('PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE'); f.close(inspection);
  const upgraded = f.open({ allowNativeMigration: true }), check = f.raw();
  assert.equal(upgraded.schemaVersion, 2); assert.match(upgraded.registryId, /^[a-f0-9]{32}$/u);
  assert.deepEqual(snapshot(check), before); assert.equal(check.prepare('SELECT count(*) AS n FROM note_native_creates').get().n, 0);
  assert.equal(check.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get().busy, 0);
  const registryId = upgraded.registryId; f.close(upgraded);
  const reopened = f.open(); assert.equal(reopened.registryId, registryId); assert.equal(reopened.schemaVersion, 2);
  assert.throws(() => request(reopened, 'notes.get', { noteId: 'note_seed_other' }), fault('notes_note_not_found'));
  assert.throws(() => request(reopened, 'notes.get', { noteId: 'note_seed_deleted' }), fault('notes_note_not_found'));
  t.diagnostic(`Node ${process.versions.node}, SQLite ${process.versions.sqlite}; historical schema SHA checked`);
});

test('native create proof outlives ordinary receipt trimming, edits, trash and purge without resurrection', async t => {
  const f = await fixture(t), service = f.open({ allowNativeMigration: true });
  const sourceStore = '8'.repeat(32), invocation = 'inv_native_independent', suffix = sha(JSON.stringify(['soty.native-note.v1', sourceStore, invocation]));
  const noteId = `n_${suffix}`, mutationId = `m_${suffix}`, input = doc('Первый личный текст 🐝');
  request(service, 'notes.put', { noteId, mutationId, expectedRevision: 0, ...input });
  // This is an explicit persisted future-format proof, not a native execution claim.
  f.db.prepare('INSERT INTO note_native_creates VALUES(?,?,?,?,?,?,?,?,?)').run(sourceStore, invocation, OWNER.accountId,
    noteId, mutationId, sha(JSON.stringify({ body: input.body, title: input.title })),
    '95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204', 1, TIME);
  const proof = f.db.prepare('SELECT * FROM note_native_creates').get();
  for (let revision = 1; revision <= 34; revision++) request(service, 'notes.put', { noteId,
    mutationId: `mut_native_edit_${revision}`, expectedRevision: revision, ...doc(`Изменённый текст ${revision}`) });
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM note_receipts WHERE note_id=?').get(noteId).n, 32);
  assert.equal(f.db.prepare('SELECT 1 FROM note_receipts WHERE mutation_id=?').get(mutationId), undefined);
  request(service, 'notes.put', { noteId, mutationId: 'mut_native_trashed', expectedRevision: 35, ...doc('Последний текст', 'trashed') });
  request(service, 'notes.purge', { noteId, mutationId: 'mut_native_purged', expectedRevision: 36 });
  assert.deepEqual(f.db.prepare('SELECT * FROM note_native_creates').get(), proof);
  assert.equal(f.db.prepare('SELECT state FROM notes WHERE id=?').get(noteId).state, 'deleted');
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM notes_fts').get().n, 0);
  assert.throws(() => request(service, 'notes.put', { noteId, mutationId, expectedRevision: 0, ...input }), fault('notes_note_deleted'));
  assert.throws(() => f.db.exec('INSERT OR REPLACE INTO note_native_creates SELECT * FROM note_native_creates'), /notes_native_proof_immutable/u);
  assert.throws(() => f.db.prepare('DELETE FROM note_native_creates WHERE invocation_id=?').run(invocation), /notes_native_proof_immutable/u);
  f.close(service); const reopened = f.open();
  assert.equal(reopened.schemaVersion, 2); assert.deepEqual(f.db.prepare('SELECT * FROM note_native_creates').get(), proof);
  assert.throws(() => request(reopened, 'notes.get', { noteId }), fault('notes_note_not_found'));
});

test('future, partial and missing-identity Notes layouts refuse before WAL transition and never repair', async t => {
  for (const kind of ['future', 'guard', 'registry', 'unknown']) {
    const f = await fixture(t); migrateNotes(f.db, 'soty', { allowNativeMigration: true });
    if (kind === 'future') f.db.exec("UPDATE notes_meta SET value='soty.notes.sqlite.v3' WHERE key='lineage'; PRAGMA user_version=3");
    if (kind === 'guard') f.db.exec('DROP TRIGGER note_native_create_no_replace');
    if (kind === 'unknown') f.db.exec('CREATE TABLE sqliteExtra(value TEXT)');
    if (kind === 'registry') {
      const guard = f.db.prepare("SELECT sql FROM sqlite_schema WHERE name='notes_identity_no_delete'").get().sql;
      f.db.exec('DROP TRIGGER notes_identity_no_delete'); f.db.exec("DELETE FROM notes_meta WHERE key='registry_id'"); f.db.exec(guard);
    }
    f.db.exec('PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE');
    const before = fileProof(f.databasePath);
    assert.throws(() => f.open({ allowNativeMigration: true }), fault(kind === 'future' ? 'notes_schema_unsupported' : 'notes_schema_metadata_mismatch'));
    assert.deepEqual(fileProof(f.databasePath), before);
    assert.equal(f.db.prepare('PRAGMA journal_mode').get().journal_mode, 'delete');
  }
});

test('known Notes layout with inconsistent accounting refuses migration without rewriting user rows', async t => {
  const f = await fixture(t); populateHistorical(f.db);
  f.db.prepare('UPDATE note_accounts SET bytes=bytes+1 WHERE account_id=?').run(OWNER.accountId);
  const before = snapshot(f.db);
  assert.throws(() => f.open({ allowNativeMigration: true }), fault('notes_storage_corrupt'));
  assert.deepEqual(snapshot(f.db), before);
  assert.equal(f.db.prepare('PRAGMA user_version').get().user_version, 1);
  assert.equal(f.db.prepare("SELECT value FROM notes_meta WHERE key='registry_id'").get(), undefined);
  assert.equal(f.db.prepare("SELECT 1 FROM sqlite_schema WHERE name='note_native_creates'").get(), undefined);
});

test('historical Notes migrator refuses committed v2 WAL rather than trusting the v1 main header', async t => {
  const f = await fixture(t); populateHistorical(f.db);
  f.db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
  assert.equal(readFileSync(f.databasePath).readUInt32BE(60), 1);
  const version2 = migrateNotes(f.db, 'soty', { allowNativeMigration: true });
  assert.equal(version2.schemaVersion, 2); assert.equal(readFileSync(f.databasePath).readUInt32BE(60), 1);
  assert.ok(readFileSync(f.databasePath + '-wal').length > 32);
  const before = fileProof(f.databasePath), oldConnection = new DatabaseSync(f.databasePath);
  try { assert.throws(() => f.old.migrateNotes(oldConnection, 'soty'), /notes_schema_unsupported/u); }
  finally { oldConnection.close(); }
  assert.deepEqual(fileProof(f.databasePath), before);
  assert.deepEqual(inspectNotesSchema(f.db, 'soty'), version2);
});

test('migration options are strict before creating any new path', async t => {
  const f = await fixture(t);
  for (const [at, value] of [null, 1, 'true', {}, []].entries()) {
    const target = path.join(f.root, `not-created-${at}`, 'notes.sqlite');
    assert.throws(() => createNotesService({ databasePath: target, projectId: 'soty', allowNativeMigration: value }), fault('notes_invalid_arguments'));
    assert.equal(existsSync(path.dirname(target)), false);
  }
});
