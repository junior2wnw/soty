import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, mkdir, readFile, readdir, writeFile, rm, symlink } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createHistoricalNotesV1 } from './notes-v1.fixture.mjs';
import { createHistoricalCapabilitiesV1 } from './capabilities-v1.fixture.mjs';
import { migrateNotes } from './fixtures/notes-v1/schema.mjs';
import { initializeCapabilitiesSchema } from './fixtures/capabilities-v1/schema.mjs';
import { readStorageFormat } from './storage-probe.mjs';
import { assertStorageCompatible, currentStorageReaders, storageReaderLabel, guardStorageStart } from './storage-guard.mjs';

const commit = '6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e';
const hash = value => createHash('sha256').update(value).digest('hex');
const normalizedSql = sql => sql.split(/('(?:[^']|'')*')/gu).map((part, index) => index % 2 ? part
  : part.replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase()).join('');
const objects = db => db.prepare("SELECT type,name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()
  .map(row => ({ ...row, sql: normalizedSql(row.sql) }));
const format = (notes = 'empty', capabilities = 'empty') => ({ ok: true, schema: 'soty.storage-format.v3', rooms: 'empty', apps: 'empty', notes, capabilities });
const stores = {
  notes: { main: 'notes.sqlite', meta: 'notes_meta', create: createHistoricalNotesV1, actual: db => migrateNotes(db, 'soty'), count: 12,
    digest: 'da75ffc702fb0ee4db9878780d374f53bca0d68b46e91eafb56038b45d1e0dfa' },
  capabilities: { main: 'capabilities.sqlite', meta: 'cap_metadata', create: createHistoricalCapabilitiesV1, actual: initializeCapabilitiesSchema, count: 20,
    digest: '959fdb4938d6b0ebc9359817b001f0eed08be70e437083648e9ac259c56229ad' },
};
const file = (root, store) => path.join(root, store, stores[store].main);
const put = (db, table, values) => db.prepare(`INSERT INTO ${table} (${Object.keys(values).join(',')}) VALUES (${Object.keys(values).map(() => '?').join(',')})`).run(...Object.values(values));

function seedNotes(db) {
  db.prepare('INSERT INTO note_accounts(account_id) VALUES (?)').run('fixture-owner');
  let total = 0;
  for (const [index, state] of ['active', 'archived', 'trashed', 'deleted'].entries()) {
    const doc = { title: state === 'deleted' ? '' : 'Synthetic café', body: state === 'deleted' ? '' : 'Приватный пример ' + state,
      items: [], color: 'plain', pinned: false, state };
    const bytes = state === 'deleted' ? 0 : Buffer.byteLength(JSON.stringify(doc)); total += bytes;
    db.prepare('INSERT INTO notes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)').run(index + 1, 'fixture-owner', 'fixture-note-' + index,
      doc.title, doc.body, '[]', doc.body, doc.color, 0, state, index + 1, bytes, 100, 101 + index);
    if (state !== 'deleted') db.prepare('INSERT INTO notes_fts(rowid,scope,title,body,items) VALUES (?,?,?,?,?)')
      .run(index + 1, hash('fixture-owner'), doc.title, doc.body, '');
    put(db, 'note_receipts', { account_id: 'fixture-owner', note_id: 'fixture-note-' + index, mutation_id: 'fixture-mutation-' + index,
      digest: hash('mutation-' + index), result: JSON.stringify({ id: 'fixture-note-' + index, revision: index + 1, state }), revision: index + 1 });
  }
  db.prepare('UPDATE note_accounts SET bytes=?,identities=4,active=1,archived=1,trashed=1 WHERE account_id=?').run(total, 'fixture-owner');
}

function seedCapabilities(db) {
  put(db, 'cap_contracts', { capability_id: 'notes.createDraft', version: 1, digest: hash('contract') });
  put(db, 'cap_clients', { id: 'client-fixture', account_id: 'fixture-owner', label: 'Synthetic client', state: 'active', policy_epoch: 1, created_at: 100, revoked_at: null });
  put(db, 'cap_principals', { id: 'principal-fixture', account_id: 'fixture-owner', client_id: 'client-fixture', kind: 'service', label: 'Synthetic principal', state: 'active', creator_device_id: 'device-fixture', created_at: 100, revoked_at: null });
  for (const child of [false, true]) put(db, 'cap_grants', { id: child ? 'grant-child' : 'grant-root', account_id: 'fixture-owner', client_id: 'client-fixture', principal_id: 'principal-fixture',
    parent_id: child ? 'grant-root' : null, root_id: 'grant-root', creator_device_id: 'device-fixture', capabilities_json: '["notes.createDraft@1"]', resources_json: '[]', effects_json: '[]', recipients_json: '[]',
    allow_delegation: Number(!child), max_depth: 1, depth: Number(child), not_before: 100, expires_at: 10000, policy_epoch: 1, created_at: 100, revoked_at: null });
  put(db, 'cap_credentials', { id: 'credential-fixture', digest: hash('synthetic-not-a-secret'), account_id: 'fixture-owner', client_id: 'client-fixture', principal_id: 'principal-fixture',
    grant_id: 'grant-child', audience: 'synthetic-audience', expires_at: 10000, created_at: 100, revoked_at: null });
  put(db, 'cap_audit', { id: 'audit-fixture', account_id: 'fixture-owner', kind: 'grant', object_type: 'grant', object_id: 'grant-child', actor_type: 'device', actor_id: 'device-fixture', created_at: 100 });
  put(db, 'cap_budgets', { root_grant_id: 'grant-root', unit: 'invocations', limit_amount: 10, reserved_amount: 1, spent_amount: 1 });
  for (const committed of [false, true]) {
    const id = committed ? 'invocation-committed' : 'invocation-pending', reservation = 'reservation-' + id;
    put(db, 'cap_budget_reservations', { id: reservation, invocation_id: id, attempt_id: 'attempt-' + id, root_grant_id: 'grant-root', unit: 'invocations', amount: 1,
      actual_amount: committed ? 1 : null, disposition: committed ? 'spent' : 'reserved', request_digest: hash(id), created_at: 101, updated_at: 102 });
    put(db, 'cap_invocations', { id, account_id: 'fixture-owner', client_id: 'client-fixture', principal_id: 'principal-fixture', grant_id: 'grant-child', root_grant_id: 'grant-root', policy_epoch: 1,
      capability_id: 'notes.createDraft', capability_version: 1, capability_digest: hash('contract'), request_key: 'key-' + id, request_digest: hash(id), internal_request_id: 'internal-' + id,
      input_json: '{"synthetic":"private input"}', target_json: '{}', authorization_json: '{}', status: committed ? 'succeeded' : 'accepted', effect_state: committed ? 'committed' : 'none',
      cancel_requested: 0, effects_json: '[]', reservation_id: reservation, job_id: committed ? 'job-fixture' : null, created_at: 101, updated_at: 102, completed_at: committed ? 102 : null });
    put(db, 'cap_dispatch_intents', { invocation_id: id, internal_request_id: 'internal-' + id, state: committed ? 'bound' : 'pending', created_at: 101, updated_at: 102 });
    if (committed) put(db, 'cap_receipts', { invocation_id: id, value_json: '{"synthetic":"own receipt"}', digest: hash('receipt'), created_at: 102 });
  }
}

async function directory(t) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'soty-notes-caps-reader-'));
  t.after(async () => {
    assert.equal(path.dirname(path.resolve(root)), path.resolve(os.tmpdir()));
    assert.match(path.basename(root), /^soty-notes-caps-reader-/u);
    await rm(root, { recursive: true, force: true });
  });
  return root;
}

async function database(root, store, { seed = false } = {}) {
  await mkdir(path.join(root, store), { recursive: true });
  const db = new DatabaseSync(file(root, store));
  try {
    db.exec('PRAGMA foreign_keys=ON'); stores[store].create(db);
    if (seed) (store === 'notes' ? seedNotes : seedCapabilities)(db);
    return db;
  } catch (error) { db.close(); throw error; }
}

async function persistentBytes(root, store) {
  const result = {};
  for (const suffix of ['', '-wal']) {
    try { result[suffix] = hash(await readFile(file(root, store) + suffix)); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
  }
  return result;
}

for (const [store, spec] of Object.entries(stores)) test(`${store}: literal v1 matches every SQL object of the exact historical migrator`, async () => {
  const provenance = JSON.parse(await readFile(new URL(`./fixtures/${store}-v1/provenance.json`, import.meta.url), 'utf8'));
  const source = await readFile(new URL(`./fixtures/${store}-v1/schema.mjs`, import.meta.url), 'utf8');
  assert.equal(provenance.commit, commit); assert.deepEqual(Object.keys(provenance.files), ['schema.mjs']);
  assert.equal(provenance.files['schema.mjs'].sourcePath, `modules/${store}/server/schema.mjs`);
  assert.equal(provenance.files['schema.mjs'].sha256, spec.digest);
  assert.equal(hash(source.replaceAll('\r\n', '\n')), spec.digest);
  const literal = new DatabaseSync(':memory:'), actual = new DatabaseSync(':memory:');
  try {
    spec.create(literal); spec.actual(actual);
    assert.equal(objects(literal).length, spec.count); assert.deepEqual(objects(literal), objects(actual));
    (store === 'notes' ? seedNotes : seedCapabilities)(literal); spec.actual(literal);
    assert.deepEqual(literal.prepare('PRAGMA foreign_key_check').all(), []);
    assert.equal(literal.prepare('PRAGMA user_version').get().user_version, 1);
  } finally { literal.close(); actual.close(); }
});

test('only absent or actually empty Notes/Capabilities directories are empty and never initialized', async t => {
  const root = await directory(t);
  assert.deepEqual(await readStorageFormat(root), format());
  for (const store of Object.keys(stores)) await mkdir(path.join(root, store));
  await mkdir(path.join(root, 'world')); await writeFile(path.join(root, 'world', 'out-of-scope'), 'synthetic');
  assert.deepEqual(await readStorageFormat(root), format());
  for (const store of Object.keys(stores)) assert.deepEqual(await readdir(path.join(root, store)), []);
});

test('populated cold v1 stores are independent, keep FTS/receipts/ledger bytes and expose only four format fields', async t => {
  for (const selection of [['notes'], ['capabilities'], ['notes', 'capabilities']]) {
    const root = await directory(t);
    for (const store of selection) (await database(root, store, { seed: true })).close();
    const before = await Promise.all(selection.map(store => persistentBytes(root, store)));
    const observed = await readStorageFormat(root);
    assert.deepEqual(observed, format(selection.includes('notes') ? 1 : 'empty', selection.includes('capabilities') ? 1 : 'empty'));
    assertStorageCompatible({ Id: 'sha256:' + '1'.repeat(64), Config: { Labels: { [storageReaderLabel]: currentStorageReaders } } }, observed);
    assert.deepEqual(await Promise.all(selection.map(store => persistentBytes(root, store))), before);
    assert.doesNotMatch(JSON.stringify(observed), /fixture|private|credential|input|receipt/u);
    if (selection.includes('notes')) {
      const db = new DatabaseSync(file(root, 'notes'), { readOnly: true });
      try { assert.equal(db.prepare("SELECT count(*) AS n FROM notes_fts WHERE notes_fts MATCH 'cafe'").get().n, 3); }
      finally { db.close(); }
    }
  }
});

for (const [store, spec] of Object.entries(stores)) test(`${store}: committed initialization in WAL is read while raw main still has version0`, async t => {
  const root = await directory(t); await mkdir(path.join(root, store));
  const db = new DatabaseSync(file(root, store));
  try {
    db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA synchronous=FULL');
    spec.create(db); (store === 'notes' ? seedNotes : seedCapabilities)(db);
    assert.equal((await readFile(file(root, store))).readUInt32BE(60), 0);
    assert.equal(db.prepare('PRAGMA user_version').get().user_version, 1);
    const before = await persistentBytes(root, store);
    assert.ok(before['-wal']);
    assert.deepEqual(await readStorageFormat(root), format(store === 'notes' ? 1 : 'empty', store === 'capabilities' ? 1 : 'empty'));
    assert.deepEqual(await persistentBytes(root, store), before);
  } finally { db.close(); }
});

// Original bridge case: an unknown lineage in a version2 WAL. Reader2 also
// tests a genuine v2 layout against the unmodified historical bridge parser.
for (const [store, spec] of Object.entries(stores)) test(`${store}: unknown marker committed only in WAL refuses before a START receipt and remains unchanged`, async t => {
  const root = await directory(t), db = await database(root, store, { seed: true });
  try {
    db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0');
    assert.equal((await readFile(file(root, store))).readUInt32BE(60), 1);
    db.exec(`BEGIN IMMEDIATE; PRAGMA user_version=2; UPDATE ${spec.meta} SET value='unrecognized-future' WHERE key='lineage'; COMMIT`);
    const before = await persistentBytes(root, store);
    const runtime = { Id: '1'.repeat(64), Image: 'sha256:' + '2'.repeat(64), State: { Running: false }, Config: { Env: ['DATA_DIR=/data'] },
      Mounts: [{ Type: 'volume', RW: true, Name: 'synthetic-data', Source: '/volumes/synthetic-data/_data', Destination: '/data' }] };
    let inspected = 0;
    const context = { engine: {
      inspect: async () => { inspected++; return runtime; },
      image: async () => ({ Id: runtime.Image, Config: { Labels: { [storageReaderLabel]: currentStorageReaders } } }),
      request: async (_method, route) => route === '/containers/json' ? [] : { Name: 'synthetic-data', Driver: 'local', Scope: 'local', Options: null, Mountpoint: runtime.Mounts[0].Source },
    }, probe: () => readStorageFormat(root) };
    await assert.rejects(guardStorageStart(context, runtime), /storage_format_unknown/u);
    assert.equal(inspected, 1, 'no successful post-probe state or START receipt');
    assert.deepEqual(await persistentBytes(root, store), before);
    assert.equal(db.prepare('PRAGMA user_version').get().user_version, 2);
  } finally { db.close(); }
});

for (const [store, spec] of Object.entries(stores)) test(`${store}: unknown marker, missing metadata and unsupported header are not fresh storage`, async t => {
  for (const mutation of [`PRAGMA user_version=0`, `PRAGMA user_version=2`, `PRAGMA user_version=99`,
    `UPDATE ${spec.meta} SET value='wrong' WHERE key='lineage'`, `DELETE FROM ${spec.meta} WHERE key='lineage'`,
    ...(store === 'notes' ? ["UPDATE notes_meta SET value='another-project' WHERE key='project_id'", "DELETE FROM notes_meta WHERE key='project_id'"] : [])]) {
    const root = await directory(t), db = await database(root, store);
    try { db.exec(mutation); } finally { db.close(); }
    const before = await persistentBytes(root, store);
    await assert.rejects(readStorageFormat(root), /storage_format_unknown/u);
    assert.deepEqual(await persistentBytes(root, store), before);
  }
});

for (const store of Object.keys(stores)) test(`${store}: directory/main/sidecar corruption and unexpected entries fail closed`, async t => {
  for (const variant of ['root-file', 'zero', 'short', 'garbage', 'orphan-wal', 'orphan-shm', 'orphan-journal', 'orphan-backup', 'main-directory', 'sidecar-directory', 'extra-file']) {
    const root = await directory(t), directoryPath = path.join(root, store), filename = file(root, store);
    if (variant === 'root-file') await writeFile(directoryPath, 'not a directory');
    else {
      await mkdir(directoryPath);
      if (variant === 'zero') await writeFile(filename, '');
      if (variant === 'short') await writeFile(filename, 'SQLite format 3\0');
      if (variant === 'garbage') await writeFile(filename, Buffer.alloc(4096, 7));
      if (variant.startsWith('orphan-')) await writeFile(filename + (variant === 'orphan-backup' ? '.bak' : '-' + variant.slice(7)), 'evidence');
      if (variant === 'main-directory') await mkdir(filename);
      if (variant === 'sidecar-directory' || variant === 'extra-file') {
        (await database(root, store)).close();
        if (variant === 'sidecar-directory') await mkdir(filename + '-wal'); else await writeFile(path.join(directoryPath, 'unknown.bin'), 'evidence');
      }
    }
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u, variant);
  }
});

for (const store of Object.keys(stores)) test(`${store}: unexpected schema objects, missing columns/indexes and replaced definitions fail closed`, async t => {
  const table = store === 'notes' ? 'note_accounts' : 'cap_contracts';
  const mutations = [
    `CREATE TABLE unknown_future(value TEXT)`, `CREATE VIEW unknown_view AS SELECT * FROM ${table}`,
    `CREATE TRIGGER unknown_hook AFTER INSERT ON ${table} BEGIN SELECT 1; END`, `CREATE INDEX unknown_index ON ${table}(${store === 'notes' ? 'bytes' : 'digest'})`,
    `ALTER TABLE ${table} ADD COLUMN future TEXT`, `ALTER TABLE ${table} RENAME COLUMN ${store === 'notes' ? 'bytes' : 'digest'} TO missing_projection`,
    store === 'notes' ? 'DROP INDEX note_receipts_trim' : 'DROP INDEX cap_dispatch_pending',
    store === 'notes' ? 'DROP INDEX notes_owner_order; CREATE INDEX notes_owner_order ON notes(account_id,state,pinned ASC,updated_at DESC,id ASC)' : 'DROP INDEX cap_grants_root; CREATE INDEX cap_grants_root ON cap_grants(root_id)',
    store === 'notes' ? 'DROP TABLE note_receipts' : 'DROP TABLE cap_receipts',
    ...(store === 'notes' ? [
      "DROP TABLE notes_fts; CREATE VIRTUAL TABLE notes_fts USING fts5(scope,title,body,items,tokenize='porter',prefix='2 3')",
      "DROP TABLE notes_fts; CREATE VIRTUAL TABLE notes_fts USING fts5(scope,title,body,items,tokenize='unicode61 remove_diacritics 2',prefix='2')",
      'DROP TABLE notes_fts; CREATE TABLE notes_fts(scope,title,body,items)',
    ] : ['DROP TABLE cap_contracts; CREATE TABLE cap_contracts(capability_id TEXT NOT NULL,version INTEGER NOT NULL,digest TEXT NOT NULL,PRIMARY KEY(capability_id,version))']),
  ];
  for (const mutation of mutations) {
    const root = await directory(t), db = await database(root, store);
    try { db.exec(mutation); } finally { db.close(); }
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u);
  }
});

test('directory junctions and file/sidecar symlinks are not trusted store paths', async t => {
  for (const store of Object.keys(stores)) {
    const root = await directory(t), target = path.join(root, 'target'); await mkdir(target);
    await symlink(target, path.join(root, store), process.platform === 'win32' ? 'junction' : 'dir');
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u);
  }
  await t.test('main and sidecar file symlinks', async t => {
    for (const store of Object.keys(stores)) for (const suffix of ['', '-wal', '-shm', '-journal']) {
      const root = await directory(t); (await database(root, store)).close();
      const target = path.join(root, 'target-file'); await writeFile(target, Buffer.alloc(4096));
      if (suffix === '') await rm(file(root, store));
      try { await symlink(target, file(root, store) + suffix, 'file'); }
      catch (error) { if (process.platform === 'win32' && error.code === 'EPERM') return t.skip('Windows file symlink permission unavailable; Linux gate remains required'); throw error; }
      await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u);
    }
  });
});

test('a real exclusive SQLite writer causes a bounded unreadable refusal, not empty initialization', async t => {
  const root = await directory(t), db = await database(root, 'capabilities', { seed: true });
  try {
    db.exec('PRAGMA journal_mode=DELETE; BEGIN EXCLUSIVE');
    const before = await persistentBytes(root, 'capabilities');
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u);
    assert.deepEqual(await persistentBytes(root, 'capabilities'), before);
  } finally { db.exec('ROLLBACK'); db.close(); }
  assert.deepEqual(await readStorageFormat(root), format('empty', 1));
});

for (const store of Object.keys(stores)) test(`${store}: cold committed WAL after an exited writer is read without checkpointing`, async t => {
  const root = await directory(t); await mkdir(path.join(root, store));
  const module = new URL(`./${store}-v1.fixture.mjs`, import.meta.url).href;
  const create = store === 'notes' ? 'createHistoricalNotesV1' : 'createHistoricalCapabilitiesV1';
  const script = `import { DatabaseSync } from 'node:sqlite'; import { createHash } from 'node:crypto';
    import { ${create} as create } from ${JSON.stringify(module)};
    const hash=${hash.toString()}, put=${put.toString()}, seed=${store === 'notes' ? seedNotes.toString() : seedCapabilities.toString()};
    const db=new DatabaseSync(process.argv[1]); db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA synchronous=FULL');
    create(db); db.exec('BEGIN IMMEDIATE'); seed(db); db.exec('COMMIT');
    process.stdout.write('ready\\n'); setInterval(()=>{},1000);`;
  const child = spawn(process.execPath, ['--input-type=module', '-e', script, file(root, store)], { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
  const exited = once(child, 'exit');
  try {
    await new Promise((resolve, reject) => {
      let text = '';
      const timeout = setTimeout(() => reject(new Error('synthetic_writer_ready_timeout')), 5000);
      const finish = fn => value => { clearTimeout(timeout); child.stdout.removeAllListeners('data'); child.removeListener('error', error); child.removeListener('exit', earlyExit); fn(value); };
      const error = finish(reject), earlyExit = finish(() => reject(new Error('synthetic_writer_exited_before_ready')));
      child.once('error', error); child.once('exit', earlyExit);
      child.stdout.on('data', chunk => { text += chunk.toString('utf8'); if (text === 'ready\n') finish(resolve)(); else if (text.length > 32) error(new Error('synthetic_writer_invalid_ready')); });
    });
    child.kill('SIGKILL'); await exited;
    assert.equal((await readFile(file(root, store))).readUInt32BE(60), 0);
    const before = await persistentBytes(root, store), names = await readdir(path.join(root, store));
    assert.ok(before['-wal']); assert.ok(names.includes(stores[store].main + '-shm'));
    assert.deepEqual(await readStorageFormat(root), format(store === 'notes' ? 1 : 'empty', store === 'capabilities' ? 1 : 'empty'));
    assert.deepEqual(await persistentBytes(root, store), before);
    assert.deepEqual(await readdir(path.join(root, store)), names);
  } finally {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    await exited; // Confirm worker handles are gone before fixture cleanup on Windows.
  }
});

test('the trusted probe imports only builtins and never a Notes/Capabilities fixture, schema or migrator', async () => {
  const source = await readFile(new URL('./storage-probe.mjs', import.meta.url), 'utf8');
  const imports = [...source.matchAll(/^import .* from '([^']+)'/gmu)].map(match => match[1]);
  assert.deepEqual(imports, ['node:fs/promises', 'node:path', 'node:sqlite']);
  assert.doesNotMatch(source, /import\s*\(|migrateNotes|initializeCapabilitiesSchema|fixtures\//u);
});
