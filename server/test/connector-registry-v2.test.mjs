import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createConnectorStore } from '../connector-store.js';
import { readConnectorRegistry, readConnectorState } from '../connector-registry.js';

const sha = value => createHash('sha256').update(value).digest('hex');
const base = resolve(tmpdir());
async function fixture(t) {
  const directory = await mkdtemp(join(base, 'soty-registry-v2-'));
  const store = createConnectorStore(directory, { automaticJobActive: () => true }); await store.ready;
  const token = 'fixture-token-' + 'x'.repeat(40);
  const host = { linkId: 'registry-fixture-link-12345678901234567890', deviceId: 'host-fixture', connectorId: 'connector-fixture' };
  const connector = { ...host, token };
  const registration = { ...host, scope: 'CurrentUser', capabilities: ['agent', 'soty.auto-owner-check.v1'], agent: { id: 'opencode', provider: 'gonka', available: true } };
  assert.equal((await store.register(registration, token)).ok, true);
  assert.equal((await store.poll(connector, 0, undefined, { automaticOwnerGuard: true })).ok, true);
  t.after(async () => {
    await store.close();
    assert.equal(dirname(resolve(directory)), base); assert.match(basename(directory), /^soty-registry-v2-/u);
    await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 30 });
  });
  const access = requestId => ({ ownerAccountId: 'fixture-owner', sourceActorDeviceId: 'fixture-user-device',
    guard: () => true, connectorId: host.connectorId, expectedDeviceId: host.deviceId, requestId, requestIntent: sha(requestId) });
  const create = (requestId, autoRegister = false) => store.createJob({ linkId: host.linkId, deviceId: host.deviceId,
    kind: 'agent', input: { kind: 'agent', text: 'Fixture request', output: 'local-app', ...(autoRegister ? { autoRegister: true } : {}) } }, access(requestId));
  return { directory, store, host, connector, token, access, create };
}

async function persistedBytes(directory) {
  return Promise.all(['connector-store.sqlite', 'connector-store.sqlite-wal'].map(async file => {
    // SQLite may create an empty WAL sidecar while opening a read-only WAL DB.
    try { const bytes = await readFile(join(directory, file)); return bytes.length ? sha(bytes) : null; }
    catch (error) { if (error.code === 'ENOENT') return null; throw error; }
  }));
}

test('read-only registry reads v1, protected v2 and v1 again without changing data or exposing unrelated results', async t => {
  const f = await fixture(t), ordinary = await f.create('manual-task');
  assert.equal(ordinary.ok, true); assert.equal((await f.store.lease(f.connector)).jobs[0].id, ordinary.job.id);
  assert.equal((await f.store.finishJob(f.connector, ordinary.job.id, { ok: true, exitCode: 0, text: 'fixture-result' })).ok, true);
  const before = await readConnectorRegistry(f.directory, { jobId: ordinary.job.id });
  assert.equal(before.jobs.find(job => job.id === ordinary.job.id).result.text, 'fixture-result');
  assert.equal((await f.store.poll(f.connector, 0, undefined, { automaticOwnerGuard: true })).ok, true);
  const auto = await f.create('automatic-task', true); assert.equal(auto.ok, true);
  assert.ok(auto.job?.id);
  const dbPath = join(f.directory, 'connector-store.sqlite');
  const readOnly = new DatabaseSync(dbPath, { readOnly: true });
  try {
    assert.equal(readOnly.prepare("SELECT value FROM meta WHERE key='schema'").get().value, 'soty.connector-sqlite.v2');
    assert.throws(() => readOnly.prepare("UPDATE meta SET value=value WHERE key='schema'").run(), /read.?only/iu);
  } finally { readOnly.close(); }
  const bytes = await persistedBytes(f.directory);
  const registry = await readConnectorRegistry(f.directory, { jobId: auto.job.id });
  const full = await readConnectorState(f.directory);
  assert.deepEqual(await persistedBytes(f.directory), bytes, 'read-only adapters must not change database or WAL contents');
  assert.equal(registry.jobs.find(job => job.id === ordinary.job.id).result, null, 'unselected result is not returned');
  assert.equal(registry.jobs.find(job => job.id === auto.job.id).result, null);
  assert.equal(full.jobs.find(job => job.id === ordinary.job.id).result.text, 'fixture-result');
  assert.equal(full.jobs.find(job => job.id === auto.job.id).input.autoRegister, true);
  assert.equal(JSON.stringify(registry).includes(f.token), false);
  assert.equal(JSON.stringify(registry).includes('tokenHash'), false);
  assert.equal((await f.store.cancelJob(f.host.linkId, auto.job.id, { ...f.access('automatic-task'), ownerAccountId: 'fixture-owner' })).job.status, 'cancelled');
  const v1 = new DatabaseSync(dbPath, { readOnly: true });
  try { assert.equal(v1.prepare("SELECT value FROM meta WHERE key='schema'").get().value, 'soty.connector-sqlite.v1'); }
  finally { v1.close(); }
  const restored = await readConnectorRegistry(f.directory, { jobId: ordinary.job.id });
  assert.deepEqual(restored.jobs.find(job => job.id === ordinary.job.id), before.jobs.find(job => job.id === ordinary.job.id));
  assert.equal((await readConnectorState(f.directory)).jobs.find(job => job.id === auto.job.id).status, 'cancelled');
});

test('unknown connector database schema stays refused and unchanged by read-only adapters', async t => {
  const f = await fixture(t), dbPath = join(f.directory, 'connector-store.sqlite');
  await f.store.close();
  const db = new DatabaseSync(dbPath);
  try { db.prepare("UPDATE meta SET value='soty.connector-sqlite.v99' WHERE key='schema'").run(); }
  finally { db.close(); }
  const bytes = await persistedBytes(f.directory);
  await assert.rejects(readConnectorRegistry(f.directory), /Unsupported connector registry schema/u);
  await assert.rejects(readConnectorState(f.directory), /Unsupported connector registry schema/u);
  assert.deepEqual(await persistedBytes(f.directory), bytes);
  const verify = new DatabaseSync(dbPath, { readOnly: true });
  try { assert.equal(verify.prepare("SELECT value FROM meta WHERE key='schema'").get().value, 'soty.connector-sqlite.v99'); }
  finally { verify.close(); }
});
