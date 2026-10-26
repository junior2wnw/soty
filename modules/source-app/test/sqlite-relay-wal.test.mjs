import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createSqliteRelayNonceStore } from '../server/sqlite-relay-nonces.mjs';

async function fixture(t) {
  const parent = resolve(tmpdir()), directory = await mkdtemp(join(parent, 'soty-nonce-wal-'));
  t.after(async () => {
    assert.equal(dirname(directory), parent); assert.match(basename(directory), /^soty-nonce-wal-/u);
    await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 30 });
  });
  const path = join(directory, 'nonce.sqlite'), initial = createSqliteRelayNonceStore({ databasePath: path });
  const prior = randomBytes(24).toString('base64url'), priorExpiry = Date.now() + 10000;
  assert.equal(initial.consume(prior, priorExpiry), true); initial.close();
  // A store left between the old publication and WAL setup is compatible and
  // retains its original schema; another process can already hold a read lock.
  const old = new DatabaseSync(path);
  assert.equal(old.prepare('PRAGMA journal_mode=DELETE').get().journal_mode, 'delete'); old.close();
  return { path, prior, priorExpiry };
}
async function holdRead(t, path, autoReleaseMs) {
  const script = `import {DatabaseSync} from 'node:sqlite';
    const db=new DatabaseSync(process.argv[1]); db.exec('BEGIN'); db.prepare('SELECT * FROM soty_nonce_meta').get();
    process.stdout.write('locked\\n');
    const release=()=>{ db.exec('COMMIT');db.close();process.exit(0); };
    ${autoReleaseMs ? `setTimeout(release,${autoReleaseMs});` : "process.stdin.once('data',release);"}`;
  const child = spawn(process.execPath, ['--input-type=module', '-e', script, path],
    { stdio: ['pipe', 'pipe', 'pipe'], windowsHide: true, env: { ...process.env, NODE_NO_WARNINGS: '1' } });
  let stderr = ''; child.stderr.on('data', data => { stderr += data; });
  const exit = new Promise((done, reject) => { child.once('error', reject); child.once('exit', code => done(code)); });
  t.after(() => { if (child.exitCode === null && child.signalCode === null) child.kill(); });
  await new Promise((done, reject) => {
    let text = ''; child.stdout.on('data', data => { text += data; if (text.includes('locked\n')) done(); });
    child.once('error', reject); child.once('exit', code => reject(Error(`holder exited ${code}: ${stderr}`)));
  });
  return { child, exit };
}
const id = () => randomBytes(24).toString('base64url');

test('an immediate SQLite BUSY on WAL transition is retried without accepting a replay', async t => {
  const f = await fixture(t), original = DatabaseSync.prototype.prepare;
  let attempts = 0;
  DatabaseSync.prototype.prepare = function(sql) {
    if (sql === 'PRAGMA journal_mode=WAL' && ++attempts === 1) return { get() {
      throw Object.assign(new Error('database is locked'), { code: 'ERR_SQLITE_ERROR', errcode: 261 });
    } };
    return original.call(this, sql);
  };
  try {
    const store = createSqliteRelayNonceStore({ databasePath: f.path, busyTimeoutMs: 1000 });
    const value = id(), expiry = Date.now() + 10000;
    try { assert.equal(attempts, 2); assert.equal(store.consume(value, expiry), true); assert.equal(store.consume(value, expiry), false); }
    finally { store.close(); }
  } finally { DatabaseSync.prototype.prepare = original; }
});

test('other SQLite errors and a zero retry budget still fail closed', async t => {
  const f = await fixture(t), original = DatabaseSync.prototype.prepare;
  let attempts = 0, errcode = 11;
  DatabaseSync.prototype.prepare = function(sql) {
    if (sql === 'PRAGMA journal_mode=WAL') { attempts++; return { get() {
      throw Object.assign(new Error('synthetic SQLite failure'), { code: 'ERR_SQLITE_ERROR', errcode });
    } }; }
    return original.call(this, sql);
  };
  try {
    assert.throws(() => createSqliteRelayNonceStore({ databasePath: f.path, busyTimeoutMs: 1000 }), { code: 'soty_identity_store_unavailable' });
    assert.equal(attempts, 1, 'non-busy SQLite failures must not be retried');
    attempts = 0; errcode = 5;
    assert.throws(() => createSqliteRelayNonceStore({ databasePath: f.path, busyTimeoutMs: 0 }), { code: 'soty_identity_store_unavailable' });
    assert.equal(attempts, 1, 'zero budget must fail immediately');
    attempts = 0;
    const started = performance.now();
    assert.throws(() => createSqliteRelayNonceStore({ databasePath: f.path, busyTimeoutMs: 35 }), { code: 'soty_identity_store_unavailable' });
    assert.ok(attempts > 1 && performance.now() - started < 5000, 'retries must stop within the bounded startup budget');
  } finally { DatabaseSync.prototype.prepare = original; }
});

test('startup waits for a brief reader before changing an older store to WAL', { timeout: 10000 }, async t => {
  const f = await fixture(t), holder = await holdRead(t, f.path, 250);
  const value = id(), expiry = Date.now() + 10000; let store, holderCode;
  try {
    store = createSqliteRelayNonceStore({ databasePath: f.path, busyTimeoutMs: 1000 });
    assert.equal(store.consume(f.prior, f.priorExpiry), false, 'an older consumed nonce must remain consumed');
    assert.equal(store.consume(value, expiry), true); assert.equal(store.consume(value, expiry), false);
    holderCode = await holder.exit;
  } finally {
    store?.close();
    if (holderCode === undefined) {
      if (holder.child.exitCode === null && holder.child.signalCode === null) holder.child.kill();
      await holder.exit;
    }
  }
  assert.equal(holderCode, 0);
  const reopened = createSqliteRelayNonceStore({ databasePath: f.path });
  assert.equal(reopened.consume(value, expiry), false); reopened.close();
});

test('an extended read lock fails closed within the configured budget and permits later recovery', { timeout: 10000 }, async t => {
  const f = await fixture(t), holder = await holdRead(t, f.path, 0);
  let holderCode;
  try {
    assert.throws(() => createSqliteRelayNonceStore({ databasePath: f.path, busyTimeoutMs: 40 }),
      { code: 'soty_identity_store_unavailable', status: 503 });
  } finally { holder.child.stdin.end('release'); holderCode = await holder.exit; }
  assert.equal(holderCode, 0);
  const store = createSqliteRelayNonceStore({ databasePath: f.path });
  const value = id(), expiry = Date.now() + 10000;
  assert.equal(store.consume(value, expiry), true); assert.equal(store.consume(value, expiry), false); store.close();
});
