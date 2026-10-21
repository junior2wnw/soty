import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, readdir, rm, lstat, chmod, chown, link, symlink, unlink, utimes } from 'node:fs/promises';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { observeRootState, ROOT_STATE_OBSERVER_LIMITS } from './root-state-observer.mjs';

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

test('corrupt WAL/SHM and nonempty rollback journals refuse without partial success or source mutations', linux, async t => {
  const f = await coldWal(t), wal = await readFile(f.file + '-wal');
  const corrupted = Buffer.from(wal); corrupted[24] ^= 1; await writeFile(f.file + '-wal', corrupted);
  const raw = await physical(f.source);
  await assert.rejects(observe(f), { code: 'root_state_sqlite_invalid' });
  assert.deepEqual(await physical(f.source), raw);
  await writeFile(f.file + '-wal', wal); await writeFile(f.file + '-shm', Buffer.alloc(32768));
  await assert.rejects(observe(f), { code: 'root_state_sqlite_invalid' });
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
