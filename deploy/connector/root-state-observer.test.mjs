import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, readdir, rm, lstat, chmod, chown, link, symlink, unlink, utimes } from 'node:fs/promises';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { tmpdir } from 'node:os';
import { Worker } from 'node:worker_threads';
import { observeRootState, ROOT_STATE_OBSERVER_LIMITS } from './root-state-observer.mjs';
import { FEEDBACK_CATALOG_ADMISSION, captureFeedbackCatalogAdmission } from './feedback-catalog-admission.mjs';

const linux = { skip: process.platform !== 'linux' && 'Linux private tmpfs contract; no test bypass' };
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const q = text => '"' + text.replaceAll('"', '""') + '"';
async function fixture(t) {
  const root = await mkdtemp('/dev/shm/soty-root-state-test-');
  assert.equal(path.dirname(root), '/dev/shm'); assert.match(path.basename(root), /^soty-root-state-test-/);
  t.after(() => rm(root, { recursive: true, force: true }));
  const source = path.join(root, 'source'), ramRoot = path.join(root, 'ram');
  await mkdir(source, { mode: 0o700 }); await mkdir(ramRoot, { mode: 0o700 });
  return { root, source, ramRoot };
}
const observe = (f, limits) => observeRootState({ source: f.source, ramRoot: f.ramRoot, ...(limits ? { limits } : {}) });
async function database(f, name = 'unknown.bin', sql = 'CREATE TABLE records(nonce INTEGER); INSERT INTO records VALUES(0)') {
  const file = path.join(f.source, name); await mkdir(path.dirname(file), { recursive: true });
  const db = new DatabaseSync(file); try { db.exec(sql); } finally { db.close(); }
  return file;
}
// Fixture mutations must really change schema_version/shadow state; the actual
// observer always keeps defensive:true on its independent read-only RAM copy.
function edit(file, sql) { const db = new DatabaseSync(file, { defensive: false }); try { db.exec(sql); } finally { db.close(); } }
async function physical(root, relative = '.') {
  const file = path.join(root, relative), s = await lstat(file, { bigint: true });
  if (s.isDirectory()) {
    const rows = [[relative, s.mode, s.uid, s.gid]];
    for (const name of (await readdir(file)).sort()) rows.push(...await physical(root, relative === '.' ? name : relative + '/' + name));
    return rows;
  }
  return [[relative, s.mode, s.uid, s.gid, s.size, s.ino, s.mtimeNs, s.ctimeNs, sha(await readFile(file))]];
}
async function coldWal(t, { shm = false } = {}) {
  const f = await fixture(t), file = await database(f, 'world/state.sqlite', `
    PRAGMA journal_mode=WAL; PRAGMA user_version=1;
    CREATE TABLE auth_nonce(id INTEGER PRIMARY KEY, nonce TEXT);
    INSERT INTO auth_nonce VALUES(1,'before');
  `);
  const baseline = await observe(f);
  const code = `import{DatabaseSync}from'node:sqlite';const db=new DatabaseSync(${JSON.stringify(file)});
    db.exec("PRAGMA wal_autocheckpoint=0;PRAGMA synchronous=FULL;BEGIN;UPDATE auth_nonce SET nonce='committed';PRAGMA user_version=2;COMMIT");
    process.stdout.write('ready');setInterval(()=>{},1000);`;
  const child = spawn(process.execPath, ['--input-type=module', '-e', code], { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
  t.after(() => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); });
  const ready = await Promise.race([once(child.stdout, 'data'), once(child, 'exit').then(() => { throw Error('synthetic WAL child failed'); })]);
  assert.equal(String(ready[0]), 'ready');
  const ended = once(child, 'exit'); child.kill('SIGKILL'); await ended;
  if (!shm) await unlink(file + '-shm');
  assert.equal((await readFile(file)).readUInt32BE(60), 1);
  assert.ok((await lstat(file + '-wal')).size > 32);
  return { ...f, file, baseline };
}

test('standalone import exports only an observer and bounded constants; unsupported host refuses safely', async () => {
  const exports = await import('./root-state-observer.mjs');
  assert.deepEqual(Object.keys(exports).sort(), ['ROOT_STATE_OBSERVER_LIMITS', 'observeRootState']);
  assert.equal(Object.isFrozen(ROOT_STATE_OBSERVER_LIMITS), true);
  if (process.platform !== 'linux') await assert.rejects(observeRootState({ source: '/data', ramRoot: '/tmp' }), { code: 'root_state_unavailable' });
});

test('all nine Root stores AND an unknown SQLite canary and unknown ordinary files participate', linux, async t => {
  const f = await fixture(t), files = [];
  for (const name of ['rooms-v2.sqlite', 'apps/registry.sqlite', 'notes/notes.sqlite', 'capabilities/grants.sqlite',
    'app-registration/registry.sqlite', 'feedback/feedback.sqlite', 'human-identity/identity.sqlite', 'connect/state.sqlite', 'world/world.sqlite', 'new-store/canary.bin']) {
    files.push(await database(f, name));
  }
  const canary = path.join(f.source, 'unlisted-canary'); await writeFile(canary, 'PRIVATE_SENTINEL');
  const before = await physical(f.source), baseline = await observe(f);
  assert.equal(baseline.sqliteDatabases, 10); assert.equal(baseline.tables, 10); assert.equal(baseline.files, 11);
  assert.equal(baseline.rows, 10); assert.match(baseline.treeSha256, /^[a-f0-9]{64}$/);
  assert.doesNotMatch(JSON.stringify(baseline), /world|registry|nonce|PRIVATE_SENTINEL|CREATE|source|\/dev\/shm/);
  assert.deepEqual(await physical(f.source), before);
  for (const file of files) {
    edit(file, 'UPDATE records SET nonce=1'); assert.notEqual((await observe(f)).treeSha256, baseline.treeSha256);
    edit(file, 'UPDATE records SET nonce=0'); assert.equal((await observe(f)).treeSha256, baseline.treeSha256);
  }
  await writeFile(canary, 'PRIVATE_SENTINE2'); assert.notEqual((await observe(f)).treeSha256, baseline.treeSha256);
  assert.deepEqual(await readdir(f.ramRoot), []);
});

test('real FTS shadow tables, WITHOUT ROWID, generated fields, quoted identifiers and __proto__ are all observed', linux, async t => {
  const f = await fixture(t), file = await database(f, 'generic', `
    CREATE TABLE "ordinary WITHOUT ROWID"(a,b,c);
    INSERT INTO "ordinary WITHOUT ROWID"(rowid,a,b,c) VALUES(7,1,'same',x'00ff');
    CREATE TABLE ${q('quoted"pk')}(a TEXT,b INTEGER,v BLOB,PRIMARY KEY(a,b)) WITHOUT ROWID;
    INSERT INTO ${q('quoted"pk')} VALUES('key',2,x'0102');
    CREATE TABLE __proto__(v); INSERT INTO __proto__ VALUES('all');
    CREATE TABLE generated(a,b GENERATED ALWAYS AS(a+1) STORED); INSERT INTO generated(a) VALUES(2);
    CREATE VIRTUAL TABLE search USING fts5(content); INSERT INTO search(rowid,content) VALUES(3,'hello world');
  `);
  const baseline = await observe(f); assert.ok(baseline.tables >= 9); assert.ok(baseline.rows > 5);
  edit(file, 'UPDATE "ordinary WITHOUT ROWID" SET rowid=8');
  assert.notEqual((await observe(f)).treeSha256, baseline.treeSha256);
  edit(file, 'UPDATE "ordinary WITHOUT ROWID" SET rowid=7');
  assert.equal((await observe(f)).treeSha256, baseline.treeSha256);
  edit(file, `UPDATE ${q('quoted"pk')} SET v=x'0103'`);
  assert.notEqual((await observe(f)).treeSha256, baseline.treeSha256);
  const beforeShadow = await observe(f), db = new DatabaseSync(file, { defensive: false });
  const visible = db.prepare('SELECT rowid,content FROM search').all();
  db.exec("INSERT INTO search_idx(segid,term,pgno) VALUES(999,x'00',1)");
  assert.deepEqual(db.prepare('SELECT rowid,content FROM search').all(), visible); db.close();
  assert.notEqual((await observe(f)).treeSha256, beforeShadow.treeSha256);
});

test('every unknown shadow-like table matters even if application selectors would omit it', linux, async t => {
  const f = await fixture(t), file = await database(f, 'db', `
    CREATE TABLE auth_nonce(n INTEGER); INSERT INTO auth_nonce VALUES(0);
    CREATE TABLE feedback(v); INSERT INTO feedback VALUES('original');
    CREATE TABLE jobs(v); INSERT INTO jobs VALUES('queued');
    CREATE TABLE future_shadow(a TEXT,b INTEGER,v,PRIMARY KEY(a,b)) WITHOUT ROWID;
    INSERT INTO future_shadow VALUES('term',1,x'fe');
  `);
  const baseline = await observe(f);
  for (const sql of ['UPDATE auth_nonce SET n=1', "UPDATE feedback SET v='edited'", "UPDATE jobs SET v='running'", "UPDATE future_shadow SET v=x'ff'"]) {
    edit(file, sql); assert.notEqual((await observe(f)).treeSha256, baseline.treeSha256);
  }
});

test('int64 neighbours/extrema, BLOB versus TEXT/NULL, IEEE REAL and raw invalid UTF-8 TEXT remain distinct', linux, async t => {
  const f = await fixture(t), file = await database(f, 'typed', 'CREATE TABLE typed(k TEXT PRIMARY KEY,v)');
  const db = new DatabaseSync(file), insert = db.prepare('INSERT INTO typed VALUES(?,?)');
  insert.run('big', 9007199254740992n); insert.run('max', 9223372036854775807n); insert.run('min', -9223372036854775808n);
  insert.run('blob', Buffer.from([0, 255])); insert.run('text', 'same'); insert.run('nil', null); insert.run('float', 0);
  db.exec("INSERT INTO typed VALUES('invalid-text',CAST(x'80' AS TEXT))"); db.close();
  const baseline = await observe(f);
  const changes = [['big', 9007199254740993n], ['max', 9223372036854775806n], ['min', -9223372036854775807n],
    ['blob', Buffer.from([0, 254])], ['text', Buffer.from('same')], ['nil', Buffer.alloc(0)], ['float', Number.MIN_VALUE], ['float', Infinity], ['float', -Infinity]];
  for (const [key, value] of changes) {
    const d = new DatabaseSync(file); d.prepare('UPDATE typed SET v=? WHERE k=?').run(value, key); d.close();
    assert.notEqual((await observe(f)).treeSha256, baseline.treeSha256);
  }
  const before = (await observe(f)).treeSha256;
  edit(file, "UPDATE typed SET v=CAST(x'81' AS TEXT) WHERE k='invalid-text'");
  assert.notEqual((await observe(f)).treeSha256, before);
});

test('full schema includes indexes/views/triggers and persistent PRAGMAs', linux, async t => {
  const f = await fixture(t), file = await database(f), baseline = await observe(f);
  const cases = ['CREATE INDEX idx ON records(nonce)', 'CREATE VIEW view_all AS SELECT * FROM records',
    'CREATE TRIGGER changed AFTER INSERT ON records BEGIN SELECT 1; END',
    'PRAGMA application_id=16777217', 'PRAGMA user_version=25', 'PRAGMA schema_version=100'];
  let previous = baseline.treeSha256;
  for (const sql of cases) {
    edit(file, sql); const current = (await observe(f)).treeSha256;
    assert.notEqual(current, previous); previous = current;
  }
});

test('committed WAL is read on RAM only; absence of original SHM remains absence', linux, async t => {
  const f = await coldWal(t), before = await physical(f.source), current = await observe(f);
  assert.notEqual(current.treeSha256, f.baseline.treeSha256);
  assert.deepEqual(await physical(f.source), before);
  await assert.rejects(lstat(f.file + '-shm'), { code: 'ENOENT' });
  assert.deepEqual(await readdir(f.ramRoot), []);
});

test('checkpoint/TRUNCATE/close changes physical DB-WAL-SHM, but not the complete logical tree', linux, async t => {
  const f = await coldWal(t, { shm: true }), raw = await physical(f.source), before = await observe(f);
  assert.deepEqual(await physical(f.source), raw);
  edit(f.file, 'PRAGMA wal_checkpoint(TRUNCATE)');
  assert.notDeepEqual(await physical(f.source), raw);
  assert.deepEqual(await observe(f), before);
});

test('a later committed WAL mutation cannot reuse the previous state digest', linux, async t => {
  const f = await coldWal(t), before = await observe(f);
  edit(f.file, "UPDATE auth_nonce SET nonce='second committed value'");
  assert.notEqual((await observe(f)).treeSha256, before.treeSha256);
});

test('orphan sidecars are ordinary raw files, never silently excluded', linux, async t => {
  const f = await fixture(t); await database(f);
  const orphan = path.join(f.source, 'not-a-database-wal'); await writeFile(orphan, 'first');
  const before = await observe(f); await writeFile(orphan, 'other');
  assert.notEqual((await observe(f)).treeSha256, before.treeSha256);
});

test('corrupt WAL and hot journals refuse; unused source SHM cache cannot veto a valid database', linux, async t => {
  const f = await coldWal(t), wal = await readFile(f.file + '-wal'), valid = await observe(f);
  const corrupted = Buffer.from(wal); corrupted[24] ^= 1; await writeFile(f.file + '-wal', corrupted);
  const raw = await physical(f.source);
  await assert.rejects(observe(f), { code: 'root_state_sqlite_invalid' });
  assert.deepEqual(await physical(f.source), raw);
  await writeFile(f.file + '-wal', wal); await writeFile(f.file + '-shm', Buffer.alloc(32768));
  const cached = await physical(f.source); assert.deepEqual(await observe(f), valid); assert.deepEqual(await physical(f.source), cached);
  await unlink(f.file + '-shm'); await writeFile(f.file + '-journal', 'unknown hot journal');
  await assert.rejects(observe(f), { code: 'root_state_sqlite_invalid' });
  assert.deepEqual(await readdir(f.ramRoot), []);
});

test('file/directory modes and special permission bits matter; sidecars must retain DB ownership/mode', linux, async t => {
  const f = await fixture(t), file = await database(f), canary = path.join(f.source, 'permissions');
  await writeFile(canary, 'same'); await chmod(canary, 0o600); const before = await observe(f);
  await chmod(canary, 0o640); assert.notEqual((await observe(f)).treeSha256, before.treeSha256);
  await chmod(canary, 0o600); await chmod(f.source, 0o1700);
  assert.notEqual((await observe(f)).treeSha256, before.treeSha256);
  await chmod(f.source, 0o700); await chmod(file, 0o640);
  assert.notEqual((await observe(f)).treeSha256, before.treeSha256);
  const w = await coldWal(t); await chmod(w.file + '-wal', 0o600); await chmod(w.file, 0o640);
  await assert.rejects(observe(w), { code: 'root_state_sqlite_invalid' });
});

test('POSIX file and directory uid/gid are part of equality', linux, async t => {
  if (process.geteuid() !== 0) { t.skip('requires isolated synthetic root helper to change uid/gid'); return; }
  const f = await fixture(t), file = await database(f), baseline = await observe(f);
  await chown(file, 12345, 12346); assert.notEqual((await observe(f)).treeSha256, baseline.treeSha256);
  await chown(file, 0, 0); assert.equal((await observe(f)).treeSha256, baseline.treeSha256);
  await chown(f.source, 12345, 12346); assert.notEqual((await observe(f)).treeSha256, baseline.treeSha256);
  await chown(f.source, 0, 0);
});

test('links, linked source ancestors, hardlinks and FIFOs are refused', linux, async t => {
  const f = await fixture(t), file = await database(f), linked = path.join(f.source, 'linked');
  await link(file, linked); await assert.rejects(observe(f), { code: 'root_state_invalid' }); await unlink(linked);
  await symlink(file, linked); await assert.rejects(observe(f), { code: 'root_state_invalid' }); await unlink(linked);
  const alias = path.join(f.root, 'alias'); await symlink(f.source, alias);
  await assert.rejects(observeRootState({ source: alias, ramRoot: f.ramRoot }), { code: 'root_state_invalid' });
  const fifo = spawn('mkfifo', [linked], { stdio: 'ignore' }); assert.equal((await once(fifo, 'exit'))[0], 0);
  await assert.rejects(observe(f), { code: 'root_state_invalid' });
});

test('only private owned tmpfs, disjoint from source, is accepted; no disk fallback', linux, async t => {
  const f = await fixture(t); await database(f);
  await chmod(f.ramRoot, 0o750); await assert.rejects(observe(f), { code: 'root_state_ram_invalid' }); await chmod(f.ramRoot, 0o700);
  await assert.rejects(observeRootState({ source: f.source, ramRoot: f.root }), { code: 'root_state_invalid' });
  await assert.rejects(observeRootState({ source: f.source, ramRoot: '/tmp' }), { code: 'root_state_ram_invalid' });
  const alias = path.join(f.root, 'ram-alias'); await symlink(f.ramRoot, alias);
  await assert.rejects(observeRootState({ source: f.source, ramRoot: alias }), { code: 'root_state_ram_invalid' });
});

test('entry/byte/depth/row/value/logical/time limits refuse instead of truncating', linux, async t => {
  const f = await fixture(t); await database(f, 'nested/a/db');
  for (const limits of [{ entries: 1 }, { bytes: 20 }, { depth: 1 }, { rows: 1 }, { valueBytes: 1 }, { logicalBytes: 1 }, { wallMs: 1 }]) {
    await assert.rejects(observe(f, limits), error => ['root_state_limit', 'root_state_timeout'].includes(error.code));
    assert.deepEqual(await readdir(f.ramRoot), []);
  }
  await assert.rejects(observe(f, { bytes: ROOT_STATE_OBSERVER_LIMITS.bytes + 1 }), { code: 'root_state_invalid' });
});

test('a concurrent metadata mutation during SQLite work is detected by the final complete source fence', linux, async t => {
  const f = await fixture(t); await database(f);
  const canary = path.join(f.source, 'race'); await writeFile(canary, 'same');
  let changed = 0;
  const timer = setInterval(() => { void utimes(canary, new Date(), new Date(Date.now() + ++changed)).catch(() => {}); }, 2);
  try { await assert.rejects(observe(f), { code: 'root_state_changed' }); }
  finally { clearInterval(timer); }
});

test('malformed schemas and SQLite-looking invalid files return sanitized errors and clean RAM', linux, async t => {
  const f = await fixture(t), file = await database(f, 'PRIVATE_SENTINEL.sqlite');
  const db = new DatabaseSync(file, { defensive: false });
  db.exec("PRAGMA writable_schema=ON; UPDATE sqlite_schema SET sql='CREATE VIRTUAL TABLE records USING unavailable_PRIVATE_SENTINEL' WHERE name='records'; PRAGMA schema_version=999"); db.close();
  await assert.rejects(observe(f), error => {
    assert.equal(error.code, 'root_state_sqlite_invalid');
    assert.doesNotMatch(String(error.stack), /PRIVATE_SENTINEL|\/dev\/shm|CREATE|records/); return true;
  });
  assert.deepEqual(await readdir(f.ramRoot), []);
  await unlink(file);
  const truncated = Buffer.alloc(100); Buffer.from('SQLite format 3\0').copy(truncated);
  await writeFile(file, truncated);
  const raw = await physical(f.source);
  await assert.rejects(observe(f), { code: 'root_state_sqlite_invalid' });
  assert.deepEqual(await physical(f.source), raw);
});

test('foreign-key violations fail closed without exposing offending rows or leaking RAM handles', linux, async t => {
  const f = await fixture(t); await database(f, 'canary', `
    PRAGMA foreign_keys=OFF; CREATE TABLE parent(id PRIMARY KEY);
    CREATE TABLE child(id REFERENCES parent(id)); INSERT INTO child VALUES('PRIVATE_SENTINEL');
  `);
  const raw = await physical(f.source);
  await assert.rejects(observe(f), error => {
    assert.equal(error.code, 'root_state_sqlite_invalid');
    assert.doesNotMatch(String(error.stack), /PRIVATE_SENTINEL|child|parent|canary/); return true;
  });
  assert.deepEqual(await physical(f.source), raw); assert.deepEqual(await readdir(f.ramRoot), []);
});

// These tests exercise the same read-only SQLite worker on every host. They do
// not replace the Linux FD/tmpfs envelope tests above or spoof process.platform.
const admission = phase => ({ phase, rowsSha256: FEEDBACK_CATALOG_ADMISSION.rowsSha256 });
const catalogDDL = `CREATE TABLE cap_contracts(
      capability_id TEXT NOT NULL, version INTEGER NOT NULL, digest TEXT NOT NULL,
      PRIMARY KEY(capability_id,version)
    ) STRICT;
  CREATE TABLE cap_grants(id TEXT PRIMARY KEY, resources_json TEXT NOT NULL) STRICT;
  INSERT INTO cap_contracts VALUES('notes.createDraft',1,'old-contract');
  INSERT INTO cap_grants VALUES('existing-grant','["notes:new"]');
  CREATE TABLE body(value BLOB); INSERT INTO body VALUES(x'000102');`;
async function catalogFixture(t) {
  const root = await mkdtemp(path.join(tmpdir(), 'soty-catalog-admission-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const file = path.join(root, 'capabilities.sqlite');
  edit(file, catalogDDL); return file;
}
function addCatalog(file) {
  const db = new DatabaseSync(file);
  try {
    const insert = db.prepare('INSERT INTO cap_contracts(capability_id,version,digest) VALUES(?,?,?)');
    for (const row of FEEDBACK_CATALOG_ADMISSION.rows) insert.run(row.capability_id, row.version, row.digest);
  } finally { db.close(); }
}
async function sqliteProbe(file, { phase, relativePath = FEEDBACK_CATALOG_ADMISSION.path, limits } = {}) {
  const worker = new Worker(new URL('./root-state-observer.mjs', import.meta.url), {
    workerData: { kind: 'soty.root-state-observation.v1', file, wal: false,
      cap: { ...ROOT_STATE_OBSERVER_LIMITS, ...limits },
      ...(phase ? { feedbackCatalogAdmission: { relativePath, input: admission(phase) } } : {}) },
    env: {}, execArgv: [], stdout: true, stderr: true,
    resourceLimits: { maxOldGenerationSizeMb: 128, maxYoungGenerationSizeMb: 16, stackSizeMb: 4 },
  });
  worker.stdout.resume(); worker.stderr.resume();
  let result;
  const timer = setTimeout(() => { void worker.terminate(); }, 10_000);
  try {
    await new Promise((resolve, reject) => {
      worker.once('message', value => { result = value; });
      worker.once('error', reject);
      worker.once('exit', code => code === 0 ? resolve() : reject(Error('synthetic SQLite worker failed')));
    });
    assert.equal(typeof result?.ok, 'boolean'); return result;
  } finally { clearTimeout(timer); await worker.terminate(); }
}

test('catalog admission is a fixed exact policy, not a caller-provided ignore list', () => {
  const policy = FEEDBACK_CATALOG_ADMISSION;
  assert.equal(sha(JSON.stringify(policy.rows)), policy.rowsSha256);
  assert.equal(policy.rows.length, 7); assert.equal(new Set(policy.rows.map(row => row.capability_id)).size, 7);
  assert.equal(Object.isFrozen(policy.rows), true); assert.ok(policy.rows.every(Object.isFrozen));
  for (const value of [undefined, null, [], {}, { phase: 'after' },
    { ...admission('after'), rowsSha256: '0'.repeat(64) }, { ...admission('after'), phase: 'any' },
    { ...admission('after'), path: policy.path }, { ...admission('after'), rows: policy.rows },
    { ...admission('after'), [Symbol('hidden')]: true }]) {
    assert.throws(() => captureFeedbackCatalogAdmission(value), { code: 'root_state_catalog_admission_invalid' });
  }
  let calls = 0;
  assert.throws(() => captureFeedbackCatalogAdmission({ get phase() { calls++; return 'after'; }, rowsSha256: policy.rowsSha256 }),
    { code: 'root_state_catalog_admission_invalid' });
  assert.equal(calls, 0);
  assert.deepEqual(captureFeedbackCatalogAdmission(admission('before')), admission('before'));
});

test('real SQLite keeps raw evidence and normalizes only the exact seven absent-to-present tuples', async t => {
  const file = await catalogFixture(t), before = await sqliteProbe(file), initial = await sqliteProbe(file, { phase: 'before' });
  assert.equal(before.ok, true); assert.equal(initial.ok, true);
  // Captured with the byte-pinned qualified a30a91a8 worker, independently of
  // this implementation; preserve its raw framing, typing and record counts.
  assert.deepEqual(before, { ok: true, sha256: 'cec04eeb3fe4f24a8fbc83115930d19eeca8719a360a7360cd45fb2a590d904c',
    tables: 3, rows: 3, records: 25, logicalBytes: 2448 });
  assert.equal(initial.sha256, before.sha256); assert.equal(initial.normalizedSha256, before.sha256);
  assert.equal(initial.catalogAdmission.beforeAbsent, true); assert.equal(initial.catalogAdmission.addedRows, 0);
  addCatalog(file);
  const raw = await sqliteProbe(file), after = await sqliteProbe(file, { phase: 'after' });
  assert.deepEqual(raw, { ok: true, sha256: 'f9e328de2c6e4c155957378294e68354d1fba7fcc0b51a856ef84446ebaf7ce0',
    tables: 3, rows: 10, records: 32, logicalBytes: 4375 });
  assert.equal(after.ok, true); assert.equal(after.sha256, raw.sha256);
  assert.notEqual(after.sha256, before.sha256); assert.equal(after.normalizedSha256, before.sha256);
  assert.equal(after.rows, before.rows + 7); assert.equal(after.normalizedRows, before.rows);
  assert.equal(after.records, before.records + 7); assert.ok(after.logicalBytes > before.logicalBytes);
  assert.deepEqual(Object.keys(raw).sort(), ['ok', 'sha256', 'tables', 'rows', 'records', 'logicalBytes'].sort());
  assert.deepEqual(after.catalogAdmission, { schema: FEEDBACK_CATALOG_ADMISSION.schema, phase: 'after',
    rowsSha256: FEEDBACK_CATALOG_ADMISSION.rowsSha256, tableSchemaSha256: FEEDBACK_CATALOG_ADMISSION.tableSchemaSha256,
    beforeAbsent: false, afterExact: true, addedRows: 7 });
  assert.deepEqual(await sqliteProbe(file, { phase: 'before' }), { ok: false, code: 'root_state_catalog_admission_invalid' });
});

test('missing, altered, extra-version or wrong-schema approved tuples and a wrong path refuse', async t => {
  for (const mutation of [
    "DELETE FROM cap_contracts WHERE capability_id='apps.feedback.reply'",
    "UPDATE cap_contracts SET digest='altered' WHERE capability_id='apps.feedback.reply'",
    "INSERT INTO cap_contracts VALUES('apps.feedback.reply',2,'other-version')",
    'CREATE INDEX extra_contract_index ON cap_contracts(digest)',
  ]) {
    const file = await catalogFixture(t); addCatalog(file); edit(file, mutation);
    assert.deepEqual(await sqliteProbe(file, { phase: 'after' }), { ok: false, code: 'root_state_catalog_admission_invalid' });
  }
  const file = await catalogFixture(t); addCatalog(file);
  assert.deepEqual(await sqliteProbe(file, { phase: 'after', relativePath: 'different/capabilities.sqlite' }),
    { ok: false, code: 'root_state_catalog_admission_invalid' });
});

test('normalization never hides an eighth row, existing contract, grant, body, other schema or row identity', async t => {
  for (const mutation of [
    "INSERT INTO cap_contracts VALUES('apps.feedback.eighth',1,'unapproved')",
    "UPDATE cap_contracts SET digest='changed-old' WHERE capability_id='notes.createDraft'",
    "UPDATE cap_grants SET resources_json='[\"different:resource\"]'",
    "UPDATE body SET value=x'000103'",
    'CREATE INDEX extra_body_index ON body(value)',
    "UPDATE cap_contracts SET rowid=99 WHERE capability_id='notes.createDraft'",
  ]) {
    const file = await catalogFixture(t), before = await sqliteProbe(file); addCatalog(file); edit(file, mutation);
    const after = await sqliteProbe(file, { phase: 'after' });
    assert.equal(after.ok, true, mutation); assert.match(after.normalizedSha256, /^[a-f0-9]{64}$/);
    assert.notEqual(after.normalizedSha256, before.sha256, mutation);
  }
});

test('raw row limits still count the approved metadata instead of giving normalization a bypass', async t => {
  const file = await catalogFixture(t), before = await sqliteProbe(file); addCatalog(file);
  const after = await sqliteProbe(file); assert.equal(after.records, before.records + 7);
  assert.deepEqual(await sqliteProbe(file, { phase: 'after', limits: { rows: after.records - 1 } }),
    { ok: false, code: 'root_state_limit' });
});

test('the catalog-only service reader preserves the seven metadata rows and all existing schema/rows', async t => {
  // In the bounded old-image qualification this import is the actual old
  // reader. The candidate catalog's independent digest proof pins the tuples.
  const { createCapabilitiesService } = await import('../../modules/capabilities/server/index.mjs');
  const root = await mkdtemp(path.join(tmpdir(), 'soty-catalog-reader-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const file = path.join(root, 'capabilities.sqlite');
  const owner = Object.freeze({ accountId: 'catalog-owner', deviceId: 'catalog-device' }), clock = 1_800_000_000_000;
  const input = { databasePath: file, projectId: 'catalog-reader', clock: () => clock,
    actorActive: actor => actor.accountId === owner.accountId && actor.deviceId === owner.deviceId };
  const call = (service, op, args = {}) => service.execute({ actor: owner, op: 'access.' + op,
    args: { expectedAccountId: owner.accountId, ...args } });
  const initial = createCapabilitiesService(input);
  const principal = call(initial, 'principals.create', { label: 'Existing synthetic client' }).principal;
  call(initial, 'grants.issue', { principalId: principal.id, capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }],
    resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'], expiresAt: clock + 10_000,
    allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit: 4 } });
  const rights = call(initial, 'grants.list'); initial.close(); const before = await sqliteProbe(file);
  addCatalog(file); const after = await sqliteProbe(file, { phase: 'after' });
  assert.equal(after.ok, true); assert.equal(after.normalizedSha256, before.sha256);
  const reopened = createCapabilitiesService(input);
  try { assert.deepEqual(call(reopened, 'grants.list'), rights); } finally { reopened.close(); }
  assert.deepEqual(await sqliteProbe(file, { phase: 'after' }), after);
});

test('explicit full-tree evidence preserves default/raw semantics and every other file and path', linux, async t => {
  const f = await fixture(t), file = await database(f, FEEDBACK_CATALOG_ADMISSION.path, catalogDDL);
  const config = path.join(f.source, 'config.json'); await writeFile(config, 'synthetic-config'); await chmod(config, 0o640);
  const input = phase => ({ source: f.source, ramRoot: f.ramRoot, feedbackCatalogAdmission: admission(phase) });
  const before = await observe(f), admittedBefore = await observeRootState(input('before'));
  assert.deepEqual(admittedBefore.raw, before); assert.deepEqual(admittedBefore.normalized, before);
  addCatalog(file); const raw = await observe(f), after = await observeRootState(input('after'));
  assert.deepEqual(after.raw, raw); assert.deepEqual(after.normalized, before);
  assert.notEqual(raw.treeSha256, before.treeSha256); assert.equal(after.admission.afterExact, true);
  assert.deepEqual(await readdir(f.ramRoot), []);
  await writeFile(config, 'modified-config');
  assert.notEqual((await observeRootState(input('after'))).normalized.treeSha256, before.treeSha256);
  await writeFile(config, 'synthetic-config'); await chmod(config, 0o600);
  assert.notEqual((await observeRootState(input('after'))).normalized.treeSha256, before.treeSha256);
  const other = await fixture(t); await database(other, 'other/capabilities.sqlite', catalogDDL);
  await assert.rejects(observeRootState({ source: other.source, ramRoot: other.ramRoot, feedbackCatalogAdmission: admission('before') }),
    { code: 'root_state_catalog_admission_invalid' });
  assert.deepEqual(await readdir(other.ramRoot), []);
});
