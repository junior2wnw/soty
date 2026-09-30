import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { fork } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createCapabilitiesService } from '../server/index.mjs';
import { initializeCapabilitiesSchema } from '../server/schema.mjs';
import { normalizeOAuthConfiguration } from '../server/oauth-profile.mjs';
import { createOAuthArtifactStore } from '../server/oauth-artifacts.mjs';

// Independent storage/process gate. The key and every identity below are
// synthetic. No Connect actor, Provider adapter, consent or token is produced.
const PROJECT = 'oauth-artifact-independent';
const ORIGIN = 'https://artifact-independent.test';
const NOW = 1_800_000_000_231;
const hash = value => createHash('sha256').update(value).digest('hex');
const id = value => hash(value).slice(0, 32);
const fault = code => error => error?.code === code;
const workerMode = process.argv.includes('--oauth-artifact-independent-worker');

function configuration() {
  return normalizeOAuthConfiguration({ issuer: ORIGIN + '/oauth',
    resources: { http: ORIGIN, mcp: ORIGIN + '/mcp' },
    artifactKey: Buffer.alloc(32, 109), artifactKeyId: 'independent-fixture-key',
    withAuthorityFence: () => { throw new Error('auxiliary storage must not request authority'); },
    isRegisteredRedirect: ({ clientId, redirectUri }) => clientId === 'soty-codex-cli'
      && redirectUri === 'http://127.0.0.1:41763/callback' });
}

function session(name, uid = id(name + '-uid')) {
  const iat = Math.floor(NOW / 1000);
  return { kind: 'Session', jti: id(name), uid, iat, exp: iat + 600 };
}

function openStore(databasePath, storage, clock = () => NOW) {
  const db = new DatabaseSync(databasePath);
  db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=4321');
  let closed = false;
  const store = createOAuthArtifactStore({ db, projectId: PROJECT, ...storage, clock,
    configuration: configuration(), ensureOpen() { assert.equal(closed, false); },
    transaction(action, { busyMs } = {}) {
      assert.equal(db.isTransaction, false);
      const previous = db.prepare('PRAGMA busy_timeout').get().timeout;
      try {
        if (busyMs !== undefined) db.exec(`PRAGMA busy_timeout=${busyMs}`);
        db.exec('BEGIN IMMEDIATE');
        const value = action();
        assert.ok(!value || typeof value.then !== 'function');
        db.exec('COMMIT');
        return value;
      } catch (error) {
        if (db.isTransaction) db.exec('ROLLBACK');
        throw error;
      } finally { db.exec(`PRAGMA busy_timeout=${previous}`); }
    } });
  return { db, store, close() { if (!closed) { store.close(); db.close(); closed = true; } } };
}

function fixture(t) {
  const parent = realpathSync(tmpdir());
  const directory = mkdtempSync(path.join(parent, 'soty-oauth-artifact-independent-'));
  const owner = randomBytes(24).toString('hex'), marker = path.join(directory, '.owner');
  writeFileSync(marker, owner);
  const databasePath = path.join(directory, 'capabilities.sqlite');
  const initial = new DatabaseSync(databasePath);
  let storage;
  try {
    initializeCapabilitiesSchema(initial, { projectId: PROJECT, allowNativeMigration: true });
    storage = initializeCapabilitiesSchema(initial, { projectId: PROJECT, allowOAuthMigration: true });
  } finally { initial.close(); }
  const handles = new Set(), children = new Set();
  function open() { const handle = openStore(databasePath, storage); handles.add(handle); return handle; }
  t.after(async () => {
    for (const child of children) {
      if (!child.exitObserved) child.process.kill();
      await child.closed;
    }
    for (const handle of handles) handle.close();
    const actual = realpathSync(directory);
    assert.equal(actual, directory); assert.equal(path.dirname(actual), parent);
    assert.match(path.basename(actual), /^soty-oauth-artifact-independent-[A-Za-z0-9_-]+$/u);
    assert.equal(readFileSync(marker, 'utf8'), owner);
    rmSync(actual, { recursive: true });
  });
  return { databasePath, storage, open, children };
}

const put = (store, payload) => store.upsert({ model: 'Session', id: payload.jti, payload });
const rows = db => db.prepare('SELECT * FROM cap_oauth_artifacts ORDER BY model,id_hash').all();
function noAuthority(db) {
  for (const table of ['cap_oauth_connections', 'cap_oauth_interactions', 'cap_oauth_credentials',
    'cap_principals', 'cap_grants', 'cap_credentials', 'cap_invocations']) {
    assert.equal(db.prepare(`SELECT count(*) AS n FROM ${table}`).get().n, 0, table);
  }
  assert.equal(db.prepare("SELECT count(*) AS n FROM cap_oauth_artifacts WHERE model!='Session'").get().n, 0);
}

function childWriter(f, payload) {
  const environment = { NODE_NO_WARNINGS: '1' };
  // Only runtime paths are inherited; no original auth/config/environment dump.
  for (const name of ['SystemRoot', 'SYSTEMROOT', 'WINDIR', 'TEMP', 'TMP']) {
    if (process.env[name] !== undefined) environment[name] = process.env[name];
  }
  const child = fork(fileURLToPath(import.meta.url), ['--oauth-artifact-independent-worker'], {
    execPath: process.execPath, execArgv: [], env: environment,
    stdio: ['ignore', 'ignore', 'pipe', 'ipc'], windowsHide: true });
  const entry = { process: child, exitObserved: false, closed: undefined };
  f.children.add(entry);
  const messages = [], waiters = [];
  let stderrBytes = 0, failure;
  child.stderr.on('data', chunk => { stderrBytes += chunk.length; });
  child.on('message', message => {
    if (message?.type === 'fatal') failure = new Error('independent child failed: ' + message.code);
    const waiter = waiters.shift();
    if (waiter) failure ? waiter.reject(failure) : waiter.resolve(message);
    else messages.push(message);
  });
  child.on('error', error => { failure = error; for (const waiter of waiters.splice(0)) waiter.reject(error); });
  child.on('exit', (code, signal) => {
    entry.exitObserved = true;
    if (code !== 0 || signal) failure ||= new Error('independent child exited without completion');
    for (const waiter of waiters.splice(0)) waiter.reject(failure || new Error('child ended before response'));
  });
  entry.closed = new Promise(resolve => child.once('close', (code, signal) => resolve({ code, signal, stderrBytes })));
  const next = () => failure ? Promise.reject(failure) : messages.length ? Promise.resolve(messages.shift())
    : new Promise((resolve, reject) => waiters.push({ resolve, reject }));
  const timeout = setTimeout(() => { if (!entry.exitObserved) child.kill(); }, 10000);
  entry.closed.finally(() => clearTimeout(timeout));
  child.send({ type: 'open', databasePath: f.databasePath, storage: f.storage, payload });
  return { pid: child.pid, next, start() { child.send({ type: 'go' }); }, closed: entry.closed };
}

async function race(f, payloads) {
  const children = payloads.map(payload => childWriter(f, payload));
  assert.equal(new Set(children.map(child => child.pid)).size, 2);
  assert.ok(children.every(child => child.pid !== process.pid));
  for (const response of await Promise.all(children.map(child => child.next()))) assert.equal(response.type, 'ready');
  children.forEach(child => child.start());
  const outcomes = await Promise.all(children.map(child => child.next()));
  for (const outcome of outcomes) {
    assert.equal(outcome.type, 'result'); assert.equal(outcome.busyTimeout, 4321);
    assert.equal(outcome.transactionOpen, false);
  }
  for (const closed of await Promise.all(children.map(child => child.closed))) {
    assert.deepEqual(closed, { code: 0, signal: null, stderrBytes: 0 });
  }
  return outcomes;
}

if (workerMode) {
  process.once('message', message => {
    let handle;
    try {
      assert.equal(message.type, 'open');
      handle = openStore(message.databasePath, message.storage);
      process.once('message', command => {
        let outcome;
        try {
          assert.equal(command.type, 'go');
          put(handle.store, message.payload);
          outcome = { type: 'result', accepted: true };
        } catch (error) {
          outcome = { type: 'result', accepted: false, code: error?.code || 'unclassified' };
        }
        outcome.busyTimeout = handle.db.prepare('PRAGMA busy_timeout').get().timeout;
        outcome.transactionOpen = handle.db.isTransaction;
        handle.close();
        process.send(outcome, () => process.disconnect());
      });
      process.send({ type: 'ready' });
    } catch (error) {
      handle?.close();
      process.send({ type: 'fatal', code: error?.code || 'unclassified' }, () => process.disconnect());
    }
  });
} else {
  test('two OS writers competing for one Session UID retain exactly one ID without authority', { timeout: 15000 }, async t => {
    const f = fixture(t), { db, store } = f.open();
    const uid = id('contended-uid'), payloads = [session('writer-a', uid), session('writer-b', uid)];
    const outcomes = await race(f, payloads);
    assert.equal(outcomes.filter(result => result.accepted).length, 1);
    const loser = outcomes.findIndex(result => !result.accepted);
    assert.ok(['oauth_invalid_artifact', 'oauth_storage_busy'].includes(outcomes[loser].code));
    // A transient contender must also fail on the now-committed UID, not later
    // create a second row. This explicit test retry is not a production retry.
    assert.throws(() => put(store, payloads[loser]), fault('oauth_invalid_artifact'));
    const winner = payloads[outcomes.findIndex(result => result.accepted)];
    assert.equal(rows(db).length, 1);
    assert.deepEqual(store.findByUid({ uid }), winner);
    assert.equal(store.find({ model: 'Session', id: payloads[loser].jti }), undefined);
    noAuthority(db);
  });

  test('two OS writers at the final auxiliary slot cannot overfill or evict existing ciphertext', { timeout: 15000 }, async t => {
    const f = fixture(t), { db, store } = f.open();
    for (let i = 0; i < 1023; i++) put(store, session('retained-' + i));
    const before = rows(db), previousIds = new Set(before.map(row => row.id_hash));
    const payloads = [session('final-slot-a'), session('final-slot-b')];
    const outcomes = await race(f, payloads);
    assert.equal(outcomes.filter(result => result.accepted).length, 1);
    const loser = outcomes.findIndex(result => !result.accepted);
    assert.ok(['oauth_quota_exceeded', 'oauth_storage_busy'].includes(outcomes[loser].code));
    assert.throws(() => put(store, payloads[loser]), fault('oauth_quota_exceeded'));
    const after = rows(db);
    assert.equal(after.length, 1024);
    assert.deepEqual(after.filter(row => previousIds.has(row.id_hash)), before);
    const winner = payloads[outcomes.findIndex(result => result.accepted)];
    assert.deepEqual(store.find({ model: 'Session', id: winner.jti }), winner);
    noAuthority(db);
  });

  test('persisted authentic ciphertext transplanted to another ID fails only at decrypting use, without repair', t => {
    const f = fixture(t), first = f.open();
    const a = { ...session('sealed-a'), state: { privateMarker: 'original-private-A' } };
    const b = { ...session('sealed-b'), state: { privateMarker: 'original-private-B' } };
    put(first.store, a); put(first.store, b);
    const validB = first.db.prepare('SELECT * FROM cap_oauth_artifacts WHERE id_hash=?').get(hash(b.jti));
    // Synthetic persisted corruption, preserving real DDL and valid AEAD bytes.
    // Both cipher and plaintext digest move together; AAD must still reject it.
    first.db.prepare('UPDATE cap_oauth_artifacts SET payload_cipher=?,payload_digest=? WHERE id_hash=?')
      .run(validB.payload_cipher, validB.payload_digest, hash(a.jti));
    const damaged = rows(first.db);
    first.close();
    const baseline = createCapabilitiesService({ databasePath: f.databasePath, projectId: PROJECT, actorActive: () => true });
    try { assert.equal(baseline.schemaVersion, 3); assert.equal(baseline.oauth, undefined); }
    finally { baseline.close(); }
    const { db, store } = f.open();
    for (const readOrReplace of [() => store.find({ model: 'Session', id: a.jti }),
      () => store.findByUid({ uid: a.uid }), () => put(store, a),
      () => store.destroy({ model: 'Session', id: a.jti })]) {
      assert.throws(readOrReplace, fault('capabilities_storage_corrupt'));
    }
    assert.deepEqual(store.find({ model: 'Session', id: b.jti }), b);
    assert.deepEqual(store.cleanup({ limit: 1 }), { artifactsDeleted: 0, interactionsDeleted: 0, credentialsDeleted: 0 });
    assert.deepEqual(rows(db), damaged);
    noAuthority(db);
  });
}
