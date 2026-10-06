import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile, rm, unlink, symlink, open, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve, dirname, basename } from 'node:path';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { DatabaseSync } from 'node:sqlite';
import { createHistoricalNotesV1 } from './notes-v1.fixture.mjs';
import { snapshotStorage } from './storage-snapshot.mjs';
import { readStorageFormat } from './storage-probe.mjs';

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), 'soty-cold-snapshot-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const source = join(root, 'source'); await mkdir(source);
  return { root, source, target: join(root, 'private') };
}
const digest = value => createHash('sha256').update(value).digest('hex');
async function hashes(file) { return [digest(await readFile(file)), digest(await readFile(file + '-wal'))]; }
async function coldWal(t, unknown = false) {
  const f = await fixture(t); await mkdir(join(f.source, 'notes'));
  const file = join(f.source, 'notes', 'notes.sqlite');
  const db = new DatabaseSync(file); createHistoricalNotesV1(db); db.close();
  const upgrade = new URL('./notes-v2.fixture.mjs', import.meta.url).href;
  const code = `import { DatabaseSync } from 'node:sqlite'; import { upgradeHistoricalNotesV2 } from ${JSON.stringify(upgrade)}; const db=new DatabaseSync(${JSON.stringify(file)}); db.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA synchronous=FULL'); upgradeHistoricalNotesV2(db); ${unknown ? "db.exec('PRAGMA user_version=99');" : ''} process.stdout.write('ready'); setInterval(()=>{},1000);`;
  const child = spawn(process.execPath, ['--input-type=module', '-e', code], { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
  t.after(() => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); });
  const ready = await Promise.race([once(child.stdout, 'data'), once(child, 'exit').then(() => { throw Error('fixture exited'); })]);
  assert.equal(String(ready[0]), 'ready'); const ended = once(child, 'exit'); child.kill('SIGKILL'); await ended;
  await unlink(file + '-shm');
  assert.equal((await readFile(file)).readUInt32BE(60), 1);
  return { ...f, file };
}

test('cold main1/WAL2 without SHM is read normally from a private snapshot; source main and WAL stay byte-identical', async t => {
  const f = await coldWal(t); const before = await hashes(f.file);
  await snapshotStorage(f.source, f.target);
  const formats = await readStorageFormat(f.target);
  assert.equal(formats.notes, 2); assert.equal(formats.rooms, 'empty');
  assert.deepEqual(await hashes(f.file), before);
  await assert.rejects(readFile(f.file + '-shm'), { code: 'ENOENT' });
});

test('an unknown committed WAL format never falls back to the older main-file format', async t => {
  const f = await coldWal(t, true); const before = await hashes(f.file);
  await snapshotStorage(f.source, f.target);
  await assert.rejects(readStorageFormat(f.target), /storage_format_unknown/);
  assert.deepEqual(await hashes(f.file), before);
});

test('the size budget is aggregate; unexpected store files and orphan journals retain rejection evidence', async t => {
  const f = await fixture(t); await mkdir(join(f.source, 'apps'));
  await writeFile(join(f.source, 'apps', 'unexpected'), 'evidence');
  await writeFile(join(f.source, 'rooms-v2.sqlite-wal'), 'orphan');
  await assert.rejects(snapshotStorage(f.source, f.target, 10), /storage_format_unreadable/);
  const target = join(f.root, 'complete'); await snapshotStorage(f.source, target);
  assert.equal(await readFile(join(target, 'apps', 'unexpected'), 'utf8'), 'evidence');
  await assert.rejects(readStorageFormat(target), /storage_format_unreadable/);
});

test('legacy room JSON is copied before migration and excluded only when a primary room database exists', async t => {
  const f = await fixture(t), name = 'a'.repeat(24) + '.json'; await writeFile(join(f.source, name), '{}');
  await snapshotStorage(f.source, f.target); assert.equal((await readStorageFormat(f.target)).rooms, 1);
  await writeFile(join(f.source, 'rooms-v2.sqlite'), 'invalid database');
  const target = join(f.root, 'primary'); await snapshotStorage(f.source, target);
  await assert.rejects(readFile(join(target, name)), { code: 'ENOENT' });
  await assert.rejects(readStorageFormat(target), /storage_format_unreadable/);
});

test('a symlink inside a store is refused rather than dereferenced', async t => {
  const f = await fixture(t); await mkdir(join(f.source, 'notes')); const other = join(f.root, 'other'); await writeFile(other, 'private');
  try { await symlink(other, join(f.source, 'notes', 'notes.sqlite')); }
  catch (e) { if (process.platform === 'win32' && e.code === 'EPERM') { t.skip('Windows requires symlink privilege; verified in Linux'); return; } throw e; }
  await assert.rejects(snapshotStorage(f.source, f.target), /storage_format_unreadable/);
});

test('the reviewed 192MiB default copies new-store growth above48MiB, rejects one aggregate byte over, and preserves source evidence', async t => {
  const f = await fixture(t), MiB = 1024 * 1024;
  assert.equal(dirname(resolve(f.root)), resolve(tmpdir())); assert.match(basename(f.root), /^soty-cold-snapshot-/u);
  const registration = join(f.source, 'app-registration'), feedback = join(f.source, 'feedback');
  await mkdir(registration); await mkdir(feedback);
  const files = [join(registration, 'registry.sqlite'), join(feedback, 'feedback.sqlite')];
  // Synthetic sparse files exercise logical byte budgets without allocating media buffers or reading user data.
  for (const file of files) {
    const handle = await open(file, 'wx');
    try { await handle.truncate(96 * MiB); } finally { await handle.close(); }
  }
  await snapshotStorage(f.source, f.target);
  assert.deepEqual(await Promise.all(['app-registration/registry.sqlite', 'feedback/feedback.sqlite']
    .map(file => stat(join(f.target, file)).then(value => value.size))), [96 * MiB, 96 * MiB]);
  const handle = await open(files[1], 'r+');
  try { await handle.truncate(96 * MiB + 1); } finally { await handle.close(); }
  await assert.rejects(snapshotStorage(f.source, join(f.root, 'over-budget')), /storage_format_unreadable/u);
  assert.deepEqual(await Promise.all(files.map(file => stat(file).then(value => value.size))), [96 * MiB, 96 * MiB + 1]);
});
