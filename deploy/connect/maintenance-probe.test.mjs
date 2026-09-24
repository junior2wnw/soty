import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, readFile, writeFile, stat, unlink } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { probe } from './maintenance-probe.mjs';

async function fixture(jobs = []) {
  const dataDir = await mkdtemp(path.join(os.tmpdir(), 'soty-maintenance-'));
  const database = path.join(dataDir, 'connector-store.sqlite');
  const db = new DatabaseSync(database); db.exec('CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT);CREATE TABLE jobs(id TEXT PRIMARY KEY,value TEXT);');
  db.prepare('INSERT INTO meta VALUES (?,?)').run('schema', 'soty.connector-sqlite.v1');
  for (let i = 0; i < jobs.length; i++) db.prepare('INSERT INTO jobs VALUES (?,?)').run('private-job-' + i, JSON.stringify(jobs[i]));
  db.close();
  return { dataDir, database, transaction: 'a'.repeat(32), sync: process.platform === 'win32' ? async () => {} : undefined };
}

test('lightweight SQLite probe counts pending and corrupt missing status without returning IDs', async () => {
  const f = await fixture([{ status: 'queued' }, { status: 'running' }, { status: 'succeeded' }, {}, { status: 'cancelled' }]);
  const before = await readFile(f.database), result = await probe(f);
  assert.deepEqual(result, { ok: true, schema: 'soty.connect.maintenance.v1', count: 3, maintenance: false, owned: false });
  assert.ok(!JSON.stringify(result).includes('private-job')); assert.deepEqual(await readFile(f.database), before);
  await assert.rejects(probe({ ...f, verb: 'enter' }), /probe_active_jobs/);
});

test('maintenance marker is transaction-owned, durable, idempotent and never replaces external maintenance', async () => {
  const f = await fixture(); const before = await readFile(f.database);
  assert.equal((await probe({ ...f, verb: 'enter' })).owned, true);
  assert.equal((await probe({ ...f, verb: 'enter' })).maintenance, true);
  await assert.rejects(probe({ ...f, transaction: 'b'.repeat(32), verb: 'leave' }), /probe_external_maintenance/);
  assert.equal((await probe(f)).owned, true);
  assert.equal((await probe({ ...f, verb: 'leave' })).maintenance, false);
  assert.deepEqual(await readFile(f.database), before);
  await writeFile(path.join(f.dataDir, 'connector-maintenance.json'), JSON.stringify({ schema: 'soty.connector-maintenance.v1', operator: true }));
  await assert.rejects(probe({ ...f, verb: 'enter' }), /probe_external_maintenance/);
  await assert.rejects(probe({ ...f, verb: 'leave' }), /probe_external_maintenance/);
  assert.equal((await probe(f)).owned, false);
});

test('missing SQLite never falls back to JSON or creates a new database and legacy rollback intent blocks reads', async () => {
  const f = await fixture(); await unlink(f.database); await writeFile(path.join(f.dataDir, 'connector-store.json'), '{"jobs":[]}');
  await assert.rejects(probe(f), error => error.code === 'ENOENT');
  await assert.rejects(stat(f.database), error => error.code === 'ENOENT');
  await writeFile(path.join(f.dataDir, 'connector-rollback.json'), '{}');
  await assert.rejects(probe(f), /legacy_rollback_pending/);
});
