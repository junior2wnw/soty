import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createSqliteSourceStorage } from '../server/index.mjs';
import { createOrdinaryAppStore } from '../examples/ordinary-app/store.mjs';
import { readOrdinaryFormat1 } from '../examples/ordinary-app/reader.mjs';
import { digest, syncResult } from '../server/wire.mjs';

const record = (idHash = 'interaction') => ({ idHash, revision: 0, phase: 'pending', expiresAt: 9000, context: { synthetic: true } });
const session = { idHash: 'session', completionHash: 'completion', cookieToken: 'synthetic-cookie', accessToken: 'synthetic-access', accessExpiresAt: 8000, expiresAt: 6000 };
async function exchanging(storage, id = 'interaction') {
  await storage.createInteraction(record(id));
  assert.equal(await storage.claimInteraction(id, 0, { synthetic: true }), true);
  assert.equal(await storage.claimCallback(id, 1), true);
}
const completion = () => ({ idHash: 'interaction', revision: 2, completionToken: 'synthetic-completion', session });

test('ordinary store preserves literal reader, original AAD and persisted session/token deadlines across reopen', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'source-storage-extraction-'));
  let app; t.after(async () => { app?.close(); await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 30 }); });
  const options = { databasePath: join(directory, 'app.sqlite'), realmId: 'fixture', key: randomBytes(32), keyId: 'fixture', clock: () => 1000, initialize: true };
  app = createOrdinaryAppStore(options);
  app.createResource({ id: 'app-space', incarnationId: 'one', title: 'Synthetic' }); app.createPrincipal('owner');
  const nativeSessionHash = digest(app.createNativeSession('owner'));
  await exchanging(app.storage);
  assert.equal(await app.storage.completeInteraction(completion(), () => ({ principalId: 'owner', resourceId: 'app-space', nativeSessionHash })), true);
  const row = app.db.prepare('SELECT * FROM source_sessions').get();
  const bytes = Buffer.from(row.cipher, 'base64'), decipher = createDecipheriv('aes-256-gcm', options.key, bytes.subarray(0, 12));
  decipher.setAAD(Buffer.from(JSON.stringify(['soty.ordinary-source.v1', 'fixture', 'Session', 'session', 0, 'fixture'])));
  decipher.setAuthTag(bytes.subarray(12, 28));
  assert.deepEqual(JSON.parse(Buffer.concat([decipher.update(bytes.subarray(28)), decipher.final()])), session);
  app.db.prepare('UPDATE source_sessions SET expires_at=4000 WHERE id_hash=?').run('session');
  app.close(); assert.equal(readOrdinaryFormat1(options.databasePath, 'fixture').format, 1);
  app = createOrdinaryAppStore({ ...options, initialize: false });
  assert.equal((await app.storage.readSession('session')).expiresAt, 4000);
  assert.deepEqual(await app.storage.readTokenProof(session), { accessToken: 'synthetic-access', expiresAt: 8000 });
  assert.equal(app.db.prepare('SELECT cipher FROM source_sessions').get().cipher, row.cipher);
  assert.deepEqual(await app.storage.consumeCompletion('completion', 'session', () => {}), { token: 'synthetic-cookie' });
  assert.deepEqual(await app.storage.consumeCompletion('completion', 'session', () => {}), { token: 'synthetic-cookie' });
  await app.storage.revokeSession('session');
  await assert.rejects(app.storage.readTokenProof(session), { code: 'authentication_required' });
});

// Independent host owns Native tables and Source DDL. These are synthetic Native
// account/session FKs, not an initialization or migration of a real HIVE DB.
function nativeHost(t) {
  const db = new DatabaseSync(':memory:'); t.after(() => db.close());
  db.exec(`PRAGMA foreign_keys=ON;
    CREATE TABLE hive_accounts(id TEXT PRIMARY KEY);
    CREATE TABLE hive_account_sessions(token_hash TEXT PRIMARY KEY,account_id TEXT NOT NULL REFERENCES hive_accounts(id));
    CREATE TABLE source_namespaces(id TEXT PRIMARY KEY);
    CREATE TABLE native_audit(id TEXT PRIMARY KEY);
    INSERT INTO hive_accounts VALUES('owner'); INSERT INTO hive_account_sessions VALUES('native-session','owner'); INSERT INTO source_namespaces VALUES('app-space');`);
  let now = 1000, closed = false;
  const key = randomBytes(32), current = () => { assert.equal(closed, false, 'host closed'); };
  const aad = (model, id, revision) => Buffer.from(JSON.stringify(['independent-native-source', model, id, revision]));
  const options = { database: db, current, keyId: 'host-key', clock: () => now,
    transaction(action) { current(); db.exec('BEGIN IMMEDIATE'); try { const out = syncResult(action()); db.exec('COMMIT'); return out; } catch (e) { db.exec('ROLLBACK'); throw e; } },
    encrypt(model, id, revision, value) { current(); const iv = randomBytes(12), c = createCipheriv('aes-256-gcm', key, iv); c.setAAD(aad(model, id, revision)); const encrypted = Buffer.concat([c.update(JSON.stringify(value)), c.final()]); return Buffer.concat([iv, c.getAuthTag(), encrypted]).toString('base64'); },
    decrypt(model, id, revision, value, keyId) { current(); assert.equal(keyId, 'host-key'); const b = Buffer.from(value, 'base64'), c = createDecipheriv('aes-256-gcm', key, b.subarray(0, 12)); c.setAAD(aad(model, id, revision)); c.setAuthTag(b.subarray(12, 28)); return JSON.parse(Buffer.concat([c.update(b.subarray(28)), c.final()])); } };
  const before = db.prepare('SELECT * FROM sqlite_master').all(), storage = createSqliteSourceStorage(options);
  assert.deepEqual(db.prepare('SELECT * FROM sqlite_master').all(), before, 'construction must not own DDL');
  db.exec(`CREATE TABLE source_interactions(id_hash TEXT PRIMARY KEY,revision INTEGER,phase TEXT,expires_at INTEGER,cipher TEXT,key_id TEXT);
    CREATE TABLE source_sessions(id_hash TEXT PRIMARY KEY,native_session_hash TEXT REFERENCES hive_account_sessions(token_hash),principal_id TEXT REFERENCES hive_accounts(id),resource_id TEXT REFERENCES source_namespaces(id),active INTEGER,expires_at INTEGER,cipher TEXT,key_id TEXT);
    CREATE TABLE source_completions(id_hash TEXT PRIMARY KEY,interaction_hash TEXT UNIQUE REFERENCES source_interactions(id_hash),session_hash TEXT REFERENCES source_sessions(id_hash),consumed INTEGER);
    CREATE TABLE source_nonces(nonce TEXT PRIMARY KEY,expires_at INTEGER);`);
  return { db, storage, options, setNow: value => { now = value; }, closePort: () => { closed = true; } };
}
const link = () => ({ principalId: 'owner', nativeSessionHash: 'native-session', resourceId: 'app-space' });

test('host transaction rolls back Native linking and Source completion together; missing Native FK cannot create a Source session', async t => {
  const { db, storage } = nativeHost(t); await exchanging(storage);
  await assert.rejects(storage.completeInteraction(completion(), () => { db.prepare('INSERT INTO native_audit VALUES(?)').run('attempt'); return { ...link(), nativeSessionHash: 'foreign' }; }), /FOREIGN KEY/u);
  assert.equal(db.prepare('SELECT count(*) n FROM native_audit').get().n, 0);
  assert.equal(db.prepare('SELECT count(*) n FROM source_sessions').get().n, 0);
  assert.equal((await storage.getInteraction('interaction')).phase, 'exchanging');
  assert.equal(await storage.completeInteraction(completion(), () => { db.prepare('INSERT INTO native_audit VALUES(?)').run('committed'); return link(); }), true);
  assert.equal(db.prepare('SELECT count(*) n FROM native_audit').get().n, 1);
  assert.equal(await storage.completeInteraction(completion(), () => { throw new Error('must not repeat'); }), false);
  await assert.rejects(storage.consumeCompletion('completion', 'session', () => { db.prepare('INSERT INTO native_audit VALUES(?)').run('denied'); throw new Error('revoked'); }), /revoked/u);
  assert.equal(db.prepare('SELECT consumed FROM source_completions').get().consumed, 0);
  assert.equal(db.prepare('SELECT count(*) n FROM native_audit').get().n, 1);
  assert.equal((await storage.readCompletion('interaction')).idHash, 'session');
  assert.equal(db.prepare('PRAGMA foreign_key_check').all().length, 0);
});

test('CAS, synchronous final callback, expiry and host lifetime remain enforced', async t => {
  const { db, storage, setNow, closePort } = nativeHost(t);
  await storage.createInteraction(record());
  await assert.rejects(storage.claimInteraction('interaction', 0, {}, () => { db.prepare('INSERT INTO native_audit VALUES(?)').run('rolled-back'); return Promise.resolve({}); }));
  assert.equal(db.prepare('SELECT count(*) n FROM native_audit').get().n, 0);
  assert.equal((await storage.getInteraction('interaction')).revision, 0);
  assert.deepEqual(await Promise.all([storage.claimInteraction('interaction', 0, {}), storage.claimInteraction('interaction', 0, {})]), [true, false]);
  assert.equal(await storage.claimCallback('interaction', 0), false);
  setNow(9000); assert.equal(await storage.claimCallback('interaction', 1), false);
  closePort(); await assert.rejects(storage.readSession('missing'), /host closed/u);
  assert.equal(db.prepare('SELECT 1 n').get().n, 1, 'host still owns the connection');
});

test('nonce duplicate/expiry/capacity and interaction capacity retain bounded SQL behavior', async t => {
  const { db, storage, setNow } = nativeHost(t);
  assert.equal(await storage.consumeNonce('one', 2000), true); assert.equal(await storage.consumeNonce('one', 2000), false);
  setNow(2000); assert.equal(await storage.consumeNonce('one', 9000), true);
  db.exec('BEGIN'); for (let i = 1; i < 4096; i++) db.prepare('INSERT INTO source_nonces VALUES(?,9000)').run(String(i)); db.exec('COMMIT');
  assert.equal(await storage.consumeNonce('overflow', 9000), false);
  for (let i = 0; i < 256; i++) await storage.createInteraction(record(String(i)));
  await assert.rejects(storage.createInteraction(record('overflow')), { code: 'ordinary_source_capacity' });
  assert.equal(db.prepare('SELECT count(*) n FROM source_interactions').get().n, 256);
});
