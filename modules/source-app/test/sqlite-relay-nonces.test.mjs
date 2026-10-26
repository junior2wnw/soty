import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { spawn, spawnSync } from 'node:child_process';
import { mkdtemp, readFile, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { DatabaseSync } from 'node:sqlite';

const nonce = () => randomBytes(24).toString('base64url');
const moduleUrl = process.env.SOTY_SOURCE_TEST_SDK_DIRECTORY
  ? pathToFileURL(join(process.env.SOTY_SOURCE_TEST_SDK_DIRECTORY, 'dist', 'server.mjs')).href
  : new URL('../server/sqlite-relay-nonces.mjs', import.meta.url).href;
const { createSqliteRelayNonceStore } = await import(moduleUrl);
const { createSqliteRelayIdentity } = await import(process.env.SOTY_SOURCE_TEST_SDK_DIRECTORY
  ? moduleUrl : new URL('../server/sqlite-relay-identity.mjs', import.meta.url).href);
const env = { ...process.env, NODE_NO_WARNINGS: '1' };

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-nonce-')), closers = [];
  t.after(async () => { for (const close of closers) close(); await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 30 }); });
  return { directory, path: join(directory, 'nonce.sqlite'), close: callback => closers.push(callback) };
}

test('the single identity module rejects invalid configuration before creating storage', async t => {
  const f = await fixture(t);
  assert.throws(() => createSqliteRelayIdentity({ issuer: 'http://untrusted.example/human-identity',
    appId: 'app-' + 'a'.repeat(32), databasePath: f.path }), { code: 'soty_identity_invalid' });
  await assert.rejects(stat(f.path), { code: 'ENOENT' });
});

test('consumption survives an actual process restart', async t => {
  const f = await fixture(t), id = nonce(), expiresAt = Date.now() + 10000;
  const store = createSqliteRelayNonceStore({ databasePath: f.path });
  assert.equal(store.consume(id, expiresAt), true); store.close();
  const script = `import {createSqliteRelayNonceStore} from ${JSON.stringify(moduleUrl)};
    const store=createSqliteRelayNonceStore({databasePath:process.argv[1]});
    process.stdout.write(JSON.stringify(store.consume(process.argv[2],Number(process.argv[3]))));store.close();`;
  const child = spawnSync(process.execPath, ['--input-type=module', '-e', script, f.path, id, String(expiresAt)], { env, encoding: 'utf8', windowsHide: true });
  assert.equal(child.status, 0, child.stderr); assert.equal(child.stdout, 'false');
  const reopened = createSqliteRelayNonceStore({ databasePath: f.path });
  assert.equal(reopened.consume(id, expiresAt), false); reopened.close();
});

test('two independent processes cold-start and race on one nonce; only one wins', { timeout: 10000 }, async t => {
  const f = await fixture(t);
  const id = nonce(), expiresAt = Date.now() + 10000;
  const script = `import {createSqliteRelayNonceStore} from ${JSON.stringify(moduleUrl)};
    process.stdout.write('READY\\n');process.stdin.once('data',()=>{let store;try{store=createSqliteRelayNonceStore({databasePath:process.argv[1]});process.stdout.write(JSON.stringify(store.consume(process.argv[2],Number(process.argv[3]))));}
    catch(error){process.stderr.write(error.code||'failure');process.exitCode=1;}finally{store?.close();}});`;
  function worker() {
    const child = spawn(process.execPath, ['--input-type=module', '-e', script, f.path, id, String(expiresAt)], { env, windowsHide: true });
    let output = '', errors = '';
    const ready = new Promise((resolve, reject) => {
      child.stdout.on('data', data => { output += data.toString(); if (output.includes('READY\n')) resolve(); });
      child.once('error', reject); child.once('exit', code => { if (!output.includes('READY\n')) reject(Error('worker did not start: ' + code)); });
    });
    child.stderr.on('data', data => { errors += data.toString(); });
    const done = new Promise((resolve, reject) => { child.once('error', reject); child.once('exit', code => resolve({ code, output, errors })); });
    return { child, ready, done };
  }
  const a = worker(), b = worker();
  await Promise.all([a.ready, b.ready]); a.child.stdin.end('go'); b.child.stdin.end('go');
  const results = await Promise.all([a.done, b.done]);
  assert.deepEqual(results.map(value => value.code), [0, 0], JSON.stringify(results));
  assert.deepEqual(results.map(value => value.output.trim().split('\n').at(-1)).sort(), ['false', 'true']);
});

test('quota, write lock and clock rollback fail closed without losing accepted nonces', async t => {
  const f = await fixture(t); let now = Date.now();
  const store = createSqliteRelayNonceStore({ databasePath: f.path, maxItems: 1, busyTimeoutMs: 30, clock: () => now });
  f.close(() => store.close());
  const one = nonce(), two = nonce(), three = nonce(), expiresAt = now + 10000;
  assert.equal(store.consume(one, expiresAt), true);
  assert.throws(() => store.consume(two, expiresAt), { code: 'soty_identity_store_unavailable', status: 503 });
  assert.equal(store.consume(one, expiresAt), false);
  now += 10001; assert.equal(store.consume(two, now + 10000), true);
  now -= 20000; assert.throws(() => store.consume(three, now + 10000), { code: 'soty_identity_store_unavailable' });
  now += 20000;
  const lock = new DatabaseSync(f.path); lock.exec('BEGIN IMMEDIATE');
  assert.throws(() => store.consume(three, now + 10000), { code: 'soty_identity_store_unavailable' });
  lock.exec('ROLLBACK'); lock.close();
  assert.equal(store.consume(two, now + 10000), false);
  now += 10001; assert.equal(store.consume(three, now + 10000), true);
});

test('a foreign application database is refused without changing its bytes or data', async t => {
  const f = await fixture(t), foreign = new DatabaseSync(f.path);
  foreign.exec("CREATE TABLE app_data (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO app_data VALUES(1,'preserve');"); foreign.close();
  const before = createHash('sha256').update(await readFile(f.path)).digest('hex');
  assert.throws(() => createSqliteRelayNonceStore({ databasePath: f.path }), { code: 'soty_identity_store_unavailable' });
  assert.equal(createHash('sha256').update(await readFile(f.path)).digest('hex'), before);
  const verify = new DatabaseSync(f.path, { readOnly: true });
  assert.equal(verify.prepare('SELECT value FROM app_data WHERE id=1').get().value, 'preserve'); verify.close();
});
