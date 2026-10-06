import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { mkdtemp, mkdir, readFile, writeFile, rm, unlink, symlink } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, resolve, basename } from 'node:path';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { readStorageFormat } from './storage-probe.mjs';
import { snapshotStorage } from './storage-snapshot.mjs';
import { assertStorageCompatible, checkedStorageFormat, currentStorageReaders, storageReaderLabel,
  guardStorageStart, requireStorageStartReceipt, storageReaders } from './storage-guard.mjs';
import { initializeRegistrationSchema } from '../../modules/app-contract/server/schema.mjs';
import { initializeFeedbackSchema } from '../../modules/feedback/server/schema.mjs';

// Seed only literal reviewed fixtures; actual candidate imports above are used solely in one conformance test.
const fixtureRoot = new URL('./fixtures/universal-stores-v1/', import.meta.url);
const formats = {
  appRegistration: { directory: 'app-registration', filename: 'registry.sqlite', metadata: 'registration_metadata',
    lineage: 'soty.app-registration.v1', sql: readFileSync(new URL('app-registration.sql', fixtureRoot), 'utf8') },
  feedback: { directory: 'feedback', filename: 'feedback.sqlite', metadata: 'feedback_meta',
    lineage: 'soty.feedback.sqlite.v1', sql: readFileSync(new URL('feedback.sql', fixtureRoot), 'utf8') },
};
const legacy = { ok: true, schema: 'soty.storage-format.v3', rooms: 'empty', apps: 'empty', notes: 'empty', capabilities: 'empty' };
const format = (appRegistration = 1, feedback = 1) => ({ ...legacy, schema: 'soty.storage-format.v4', appRegistration, feedback });
const reader3 = '{"version":3,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6],"notes":[1,2],"capabilities":[1,2,3]}}';
const image = (label = currentStorageReaders) => ({ Id: 'sha256:' + '1'.repeat(64), Config: { Labels: { [storageReaderLabel]: label } } });
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const normalizedSql = sql => sql.split(/('(?:[^']|'')*')/gu).map((part, index) => index % 2 ? part
  : part.replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase()).join('');
const layout = db => db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()
  .map(row => [row.type, row.name, row.tbl_name, normalizedSql(row.sql)]);

async function directory(t) {
  const root = await mkdtemp(join(tmpdir(), 'soty-universal-storage-'));
  t.after(async () => {
    assert.equal(dirname(resolve(root)), resolve(tmpdir())); assert.match(basename(root), /^soty-universal-storage-/u);
    await rm(root, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
  });
  return root;
}
async function seed(root, kind, { registryId = 'REG.soty', environmentId = 'production' } = {}) {
  const entry = formats[kind], directory = join(root, entry.directory); await mkdir(directory, { recursive: true });
  const filename = join(directory, entry.filename), db = new DatabaseSync(filename);
  try {
    db.exec(entry.sql + '\nPRAGMA user_version=1;');
    const insert = db.prepare(`INSERT INTO ${entry.metadata}(key,value) VALUES(?,?)`);
    for (const [key, value] of [['lineage', entry.lineage], ['registry_id', registryId], ['environment_id', environmentId]]) insert.run(key, value);
  } finally { db.close(); }
  return filename;
}
function mutate(filename, callback) { const db = new DatabaseSync(filename); try { callback(db); } finally { db.close(); } }
function changeMetadata(db, table, key, value) {
  const guards = db.prepare("SELECT name,sql FROM sqlite_schema WHERE type='trigger' AND tbl_name=?").all(table);
  for (const guard of guards) db.exec(`DROP TRIGGER ${guard.name}`);
  db.prepare(`UPDATE ${table} SET value=? WHERE key=?`).run(value, key);
  for (const guard of guards) db.exec(guard.sql);
}
async function persistentHash(filename) {
  const values = [];
  for (const suffix of ['', '-wal']) {
    try { values.push([suffix, sha(await readFile(filename + suffix))]); }
    catch (error) { if (error.code !== 'ENOENT') throw error; values.push([suffix, null]); }
  }
  return values;
}

test('literal U1 store fixtures pin independent normalized layouts and match the actual candidate formats', async () => {
  const provenance = JSON.parse(await readFile(new URL('provenance.json', fixtureRoot), 'utf8'));
  assert.equal(provenance.sourceBase, '698a75f82ba9780494e10e1bf060781e94095b5e');
  assert.equal(provenance.kind, 'reviewed-uncommitted-U1-candidate');
  for (const kind of Object.keys(formats)) {
    const entry = formats[kind], actual = new DatabaseSync(':memory:'), literal = new DatabaseSync(':memory:');
    try {
      literal.exec(entry.sql);
      const expected = provenance.stores[entry.directory];
      assert.equal(sha(entry.sql), expected.ddlSha256); assert.equal(sha(JSON.stringify(layout(literal))), expected.layoutSha256);
      assert.equal(layout(literal).length, expected.objects);
      if (kind === 'appRegistration') initializeRegistrationSchema(actual, { registryId: 'REG.soty', environmentId: 'production' });
      else initializeFeedbackSchema(actual, 'REG.soty', 'production');
      assert.deepEqual(layout(actual), layout(literal));
    } finally { actual.close(); literal.close(); }
  }
  const source = await readFile(new URL('./storage-probe.mjs', import.meta.url), 'utf8');
  assert.doesNotMatch(source, /from\s+['"][^'"]*modules\//u);
  for (const expected of Object.values(provenance.stores)) assert.ok(source.includes(expected.layoutSha256));
});

test('absent and empty new stores preserve exact legacy v3, while either installed store produces six-store v4', async t => {
  const root = await directory(t); assert.deepEqual(await readStorageFormat(root), legacy);
  for (const entry of Object.values(formats)) await mkdir(join(root, entry.directory));
  assert.deepEqual(await readStorageFormat(root), legacy);
  const file = await seed(root, 'appRegistration'), before = await persistentHash(file);
  assert.deepEqual(await readStorageFormat(root), format(1, 'empty')); assert.deepEqual(await persistentHash(file), before);
  const feedback = await seed(root, 'feedback'), secondBefore = await persistentHash(feedback);
  assert.deepEqual(await readStorageFormat(root), format()); assert.deepEqual(await persistentHash(feedback), secondBefore);
  const onlyFeedback = await directory(t); await seed(onlyFeedback, 'feedback'); assert.deepEqual(await readStorageFormat(onlyFeedback), format('empty', 1));
});

test('v3 images cannot start new stores even when a container copies v4 labels; v4 reads all historical combinations', () => {
  for (const pair of [[1, 1], [1, 'empty'], ['empty', 1]]) assert.throws(() => assertStorageCompatible(image(reader3), format(...pair)), /storage_reader_incompatible/u);
  for (const rooms of ['empty', 1, 2]) for (const apps of ['empty', 1, 2, 3, 4, 5, 6]) {
    for (const notes of ['empty', 1, 2]) for (const capabilities of ['empty', 1, 2, 3]) {
      assertStorageCompatible(image(), { ...legacy, rooms, apps, notes, capabilities });
    }
  }
  assert.deepEqual(Object.keys(storageReaders(image())).sort(), ['appRegistration', 'apps', 'capabilities', 'feedback', 'humanIdentity', 'notes', 'rooms']);
});

test('v4 format/readers and start receipts are closed, typed and never silently downgraded to v3', () => {
  const declared = JSON.parse(currentStorageReaders);
  for (const store of ['appRegistration', 'feedback']) {
    for (const value of [undefined, [], [1, 1], [2], ['1'], 1]) {
      const changed = structuredClone(declared);
      if (value === undefined) delete changed.readers[store]; else changed.readers[store] = value;
      assert.throws(() => assertStorageCompatible(image(JSON.stringify(changed)), format()), /storage_reader_unknown/u);
    }
    for (const value of [undefined, null, 0, 2, '1']) assert.throws(() => checkedStorageFormat({ ...format(), [store]: value }), /storage_probe_invalid/u);
  }
  assert.throws(() => checkedStorageFormat({ ...format(), schema: legacy.schema }), /storage_probe_invalid/u);
  assert.throws(() => checkedStorageFormat({ ...legacy, schema: 'soty.storage-format.v4' }), /storage_probe_invalid/u);
  const receipt = { schema: 'soty.storage-start.v4', containerId: '2'.repeat(64), image: image().Id,
    mountSha256: '3'.repeat(64), rooms: 'empty', apps: 'empty', notes: 'empty', capabilities: 'empty', appRegistration: 1, feedback: 1 };
  requireStorageStartReceipt(receipt, receipt.containerId);
  for (const value of [{ ...receipt, feedback: undefined }, { ...receipt, appRegistration: 2 }, { ...receipt, schema: 'soty.storage-start.v3' },
    { ...receipt, allow: true }]) assert.throws(() => requireStorageStartReceipt(value, receipt.containerId), /storage_start_guard_missing/u);
});

test('missing table, index or guard, a no-op guard, and additive objects all fail independent format recognition', async t => {
  const damage = {
    appRegistration: ['DROP TABLE registration_feedback_outbox', 'DROP INDEX registration_heads_owner',
      'DROP TRIGGER registration_heads_no_delete',
      "DROP TRIGGER registration_heads_no_delete; CREATE TRIGGER registration_heads_no_delete BEFORE DELETE ON registration_heads BEGIN SELECT 1; END"],
    feedback: ['DROP TABLE feedback_attachments', 'DROP INDEX feedback_ticket_page', 'DROP TRIGGER feedback_meta_no_update',
      "DROP TRIGGER feedback_meta_no_update; CREATE TRIGGER feedback_meta_no_update BEFORE UPDATE ON feedback_meta BEGIN SELECT 1; END"],
  };
  for (const kind of Object.keys(formats)) for (const sql of [...damage[kind], 'CREATE TABLE unexpected_payload(value TEXT)', 'CREATE VIEW unexpected_projection AS SELECT 1 AS value']) {
    const root = await directory(t), filename = await seed(root, kind); mutate(filename, db => db.exec(sql));
    const before = await persistentHash(filename);
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u, kind + ':' + sql);
    assert.deepEqual(await persistentHash(filename), before);
  }
});

test('same projection with weakened CHECK/FK/STRICT table DDL is rejected, not accepted by column names alone', async t => {
  for (const kind of Object.keys(formats)) for (const variant of ['strict', 'constraint']) {
    const root = await directory(t), filename = await seed(root, kind);
    mutate(filename, db => {
      const table = kind === 'appRegistration' ? 'registration_heads' : variant === 'strict' ? 'feedback_installations' : 'feedback_attachments';
      const tableSql = db.prepare("SELECT sql FROM sqlite_schema WHERE type='table' AND name=?").get(table).sql;
      const attached = db.prepare("SELECT sql FROM sqlite_schema WHERE tbl_name=? AND type IN ('trigger','index') AND sql IS NOT NULL").all(table);
      const weakened = variant === 'strict' ? tableSql.replace(/\)\s*STRICT$/u, ')')
        : kind === 'appRegistration' ? tableSql.replace('CHECK(revision BETWEEN 1 AND 1000000)', '')
          : tableSql.replace('REFERENCES feedback_tickets(id)', '');
      assert.notEqual(weakened, tableSql);
      db.exec(`DROP TABLE ${table}; ${weakened};`);
      for (const row of attached) db.exec(row.sql);
    });
    await assert.rejects(readStorageFormat(root), /storage_format_unreadable/u);
  }
});

test('lineage/key/size damage and swapped registry/environment metadata fail without disclosing identities', async t => {
  for (const kind of Object.keys(formats)) {
    for (const [key, value] of [['lineage', 'soty.some-other-store.v1'], ['registry_id', '../escape'], ['environment_id', 'x'.repeat(129)]]) {
      const root = await directory(t), filename = await seed(root, kind);
      mutate(filename, db => changeMetadata(db, formats[kind].metadata, key, value));
      await assert.rejects(readStorageFormat(root), /storage_format_unknown/u);
    }
    const root = await directory(t), filename = await seed(root, kind);
    mutate(filename, db => db.prepare(`INSERT INTO ${formats[kind].metadata}(key,value) VALUES('extra','value')`).run());
    await assert.rejects(readStorageFormat(root), /storage_format_unknown/u);
  }
  for (const changes of [{ registryId: 'wrong-registry' }, { environmentId: 'development' }]) {
    const root = await directory(t); await seed(root, 'appRegistration'); await seed(root, 'feedback', changes);
    await assert.rejects(readStorageFormat(root), /storage_format_unknown/u);
  }
});

test('new format recognition never enumerates tickets, media BLOBs, descriptors or provider proofs', async t => {
  const root = await directory(t), filename = await seed(root, 'feedback'), sentinel = 'SYNTHETIC_PRIVATE_CONTENT_NEVER_IN_PROBE';
  mutate(filename, db => {
    db.prepare('INSERT INTO feedback_installations VALUES (?,?,?,?,?,?,?)').run('installation', 'key', 'REG.soty', 'tenant', 'app', 'production', 1);
    db.prepare('INSERT INTO feedback_tickets VALUES (?,?,?,?,?,?,?,?,?,?)').run('ticket', 'installation', 'app', 'reporter', 'owner', sentinel, 'open', 1, 1, 1);
    db.prepare('INSERT INTO feedback_attachments VALUES (?,?,?,?,?,?,?,?)').run('attachment', 'ticket', 1, 'audio', 'audio.webm', 'audio/webm', 32768, Buffer.alloc(32768, 73));
    db.prepare('INSERT INTO feedback_provider_receipts VALUES (?,?,?,?)').run('proof', 'installation', JSON.stringify({ body: sentinel }), 1);
  });
  const before = await persistentHash(filename), result = await readStorageFormat(root);
  assert.deepEqual(result, format('empty', 1)); assert.equal(JSON.stringify(result).includes(sentinel), false);
  assert.equal(JSON.stringify(result).includes('REG.soty'), false); assert.deepEqual(await persistentHash(filename), before);
});

test('future versions, orphan journals and unexpected files are evidence, never interpreted as empty stores', async t => {
  for (const kind of Object.keys(formats)) {
    const root = await directory(t), filename = await seed(root, kind);
    mutate(filename, db => db.exec('PRAGMA user_version=99')); await assert.rejects(readStorageFormat(root), /storage_format_unknown/u);
    const orphan = await directory(t), entry = formats[kind]; await mkdir(join(orphan, entry.directory));
    await writeFile(join(orphan, entry.directory, entry.filename + '-wal'), 'orphan');
    await assert.rejects(readStorageFormat(orphan), /storage_format_unreadable/u);
    const extra = await directory(t); await seed(extra, kind); await writeFile(join(extra, entry.directory, 'unexpected'), 'evidence');
    await assert.rejects(readStorageFormat(extra), /storage_format_unreadable/u);
  }
});

function hostGuard(root, label = currentStorageReaders) {
  const runtime = { Id: '2'.repeat(64), Image: image().Id, State: { Running: false }, Config: { Env: ['DATA_DIR=/data'],
    Labels: { [storageReaderLabel]: currentStorageReaders } },
    Mounts: [{ Type: 'volume', Name: 'synthetic-data', Source: '/docker/volumes/synthetic-data', Destination: '/data', RW: true }] };
  let probes = 0;
  const engine = {
    async inspect() { return structuredClone(runtime); }, async image() { return image(label); },
    async request(_method, url) { return url.startsWith('/volumes/')
      ? { Name: 'synthetic-data', Driver: 'local', Scope: 'local', Mountpoint: '/docker/volumes/synthetic-data', Options: {} } : []; },
  };
  const context = { engine, async probe() { probes++; return readStorageFormat(root); } };
  return { runtime, context, get probes() { return probes; } };
}
test('real-format guard records v4 evidence and rejects absent reader declarations before candidate START', async t => {
  const root = await directory(t); await seed(root, 'appRegistration'); await seed(root, 'feedback');
  const current = hostGuard(root), receipt = await guardStorageStart(current.context, current.runtime);
  assert.equal(receipt.schema, 'soty.storage-start.v4'); assert.equal(receipt.appRegistration, 1); assert.equal(receipt.feedback, 1);
  requireStorageStartReceipt(receipt, current.runtime.Id);
  const old = hostGuard(root, reader3); await assert.rejects(guardStorageStart(old.context, old.runtime), /storage_reader_incompatible/u);
  const noFeedback = JSON.parse(currentStorageReaders); delete noFeedback.readers.feedback;
  const missing = hostGuard(root, JSON.stringify(noFeedback));
  await assert.rejects(guardStorageStart(missing.context, missing.runtime), /storage_reader_unknown/u); assert.equal(missing.probes, 0);
});

async function coldWal(t, kind, unknown) {
  const root = await directory(t), filename = await seed(root, kind);
  const config = { filename, metadata: formats[kind].metadata, unknown };
  const code = `import {DatabaseSync} from 'node:sqlite';const c=JSON.parse(process.argv[1]),db=new DatabaseSync(c.filename);
    db.exec('PRAGMA journal_mode=WAL;PRAGMA wal_autocheckpoint=0;PRAGMA synchronous=FULL');
    db.prepare('INSERT INTO '+c.metadata+'(key,value) VALUES(?,?)').run('cold_wal_evidence','private-synthetic');
    ${unknown ? "db.exec('PRAGMA user_version=99');" : ''}
    process.stdout.write('ready');setInterval(()=>{},1000);`;
  const child = spawn(process.execPath, ['--input-type=module', '-e', code, JSON.stringify(config)], { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
  t.after(() => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); });
  const ready = await Promise.race([once(child.stdout, 'data'), once(child, 'exit').then(() => { throw new Error('fixture exited'); })]);
  assert.equal(String(ready[0]), 'ready'); const ended = once(child, 'exit'); child.kill('SIGKILL'); await ended;
  await unlink(filename + '-shm'); assert.equal((await readFile(filename)).readUInt32BE(60), 1);
  return { root, filename };
}
test('private snapshot retains both new dirs and committed cold WAL, preserving source bytes and rejection evidence', async t => {
  for (const kind of Object.keys(formats)) {
    const f = await coldWal(t, kind, true), before = await persistentHash(f.filename);
    const targetParent = await directory(t), target = join(targetParent, 'snapshot'); await snapshotStorage(f.root, target);
    const copied = join(target, formats[kind].directory, formats[kind].filename);
    assert.deepEqual(await persistentHash(copied), before);
    await assert.rejects(readStorageFormat(target), /storage_format_unknown/u);
    assert.deepEqual(await persistentHash(f.filename), before);
  }
});

test('snapshot includes registration and private media store, enforces aggregate size and preserves unexpected evidence', async t => {
  const source = await directory(t), registry = await seed(source, 'appRegistration'), feedback = await seed(source, 'feedback');
  const targetParent = await directory(t), target = join(targetParent, 'complete');
  const before = await Promise.all([persistentHash(registry), persistentHash(feedback)]); await snapshotStorage(source, target);
  assert.deepEqual(await readStorageFormat(target), format());
  assert.deepEqual(await Promise.all([persistentHash(registry), persistentHash(feedback)]), before);
  await assert.rejects(snapshotStorage(source, join(targetParent, 'too-small'), 1), /storage_format_unreadable/u);
  await writeFile(join(source, 'feedback', 'orphan-evidence'), 'synthetic');
  const retained = join(targetParent, 'retained'); await snapshotStorage(source, retained);
  assert.equal(await readFile(join(retained, 'feedback', 'orphan-evidence'), 'utf8'), 'synthetic');
  await assert.rejects(readStorageFormat(retained), /storage_format_unreadable/u);
});

test('new stores cannot use a symlink/junction to escape the volume in probe or snapshot', async t => {
  const source = await directory(t), targetParent = await directory(t), outside = join(targetParent, 'outside'); await mkdir(outside);
  try { await symlink(outside, join(source, 'feedback'), process.platform === 'win32' ? 'junction' : 'dir'); }
  catch (error) { if (process.platform === 'win32' && error.code === 'EPERM') { t.skip('Requires Windows link privileges; Linux release suite verifies'); return; } throw error; }
  await assert.rejects(readStorageFormat(source), /storage_format_unreadable/u);
  await assert.rejects(snapshotStorage(source, join(targetParent, 'snapshot')), /storage_format_unreadable/u);
});
