import test from 'node:test';
import assert from 'node:assert/strict';
import { Agent, createServer, request } from 'node:http';
import { generateKeyPairSync, randomBytes, sign } from 'node:crypto';
import { mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createHttpApp } from '../http-app.js';
import { startNativeRecovery } from '../capabilities-recovery.js';
import { createNotesService } from '../../modules/notes/server/index.mjs';
import { createCapabilitiesService } from '../../modules/capabilities/server/index.mjs';
import { digestArgs } from '../../modules/connect/server/index.mjs';

const CREATE = '/api/capabilities/v1/notes/drafts';
const HISTORY = '/api/capabilities/v1/invocations/';
const good = value => { assert.equal(value.ok, true, value.error?.code); return value; };

// Own full-host fixture: no author fixture, fake signature, replaced handler or
// authorization callback. The only effect fault is an explicit temporary SQL
// trigger in this fixture's Caps database, after actual Notes COMMIT.
async function host(t) {
  const parent = realpathSync(tmpdir()), prefix = 'soty-native-http-independent-';
  const directory = realpathSync(mkdtempSync(path.join(parent, prefix)));
  const marker = randomBytes(20).toString('hex'), markerFile = path.join(directory, '.owner');
  writeFileSync(markerFile, marker, { flag: 'wx' });
  const capsFile = path.join(directory, 'capabilities', 'capabilities.sqlite');
  const notesFile = path.join(directory, 'notes', 'notes.sqlite');
  const sockets = new Set(), agent = new Agent({ keepAlive: true, maxSockets: 1 });
  let app;
  const server = createServer((req, res) => app ? app(req, res) : res.writeHead(503).end());
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  t.after(async () => {
    agent.destroy(); for (const socket of sockets) socket.destroy();
    await new Promise(resolve => server.close(resolve));
    await app?.locals.closeServices();
    assert.equal(realpathSync(directory), directory); assert.equal(path.dirname(directory), parent);
    assert.ok(path.basename(directory).startsWith(prefix)); assert.equal(readFileSync(markerFile, 'utf8'), marker);
    rmSync(directory, { recursive: true });
  });
  createNotesService({ databasePath: notesFile, projectId: 'soty', allowNativeMigration: true }).close();
  createCapabilitiesService({ databasePath: capsFile, projectId: 'soty', allowNativeMigration: true,
    actorActive: () => false }).close();
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const origin = `http://127.0.0.1:${server.address().port}`;
  app = createHttpApp(path.resolve('dist'), { dataDir: directory, connectOrigins: [origin], capabilityAudience: origin,
    nativeNotesEnabled: true, discoveryOrigin: origin, appOriginTemplate: '', namedAppZone: '',
    gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' } });
  // Tests drive a real recovery page at a chosen seam; no automatic timer may
  // hide the unknown state before the independent wire observation.
  app.locals.nativeRecovery.close();

  function wire(target, { method = 'GET', body, headers = {}, token, unfinished = false } = {}) {
    return new Promise((resolve, reject) => {
      const req = request(origin + target, { method, agent, headers: {
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
        ...(body === undefined ? {} : { 'Content-Type': 'application/json' }), ...headers,
      } }, res => {
        const chunks = []; let bytes = 0;
        res.on('data', chunk => {
          bytes += chunk.length;
          if (bytes > 262144) { req.destroy(); reject(new Error('independent_response_limit')); return; }
          chunks.push(chunk);
        });
        res.once('error', reject);
        res.once('end', () => {
          try {
            const text = Buffer.concat(chunks).toString('utf8');
            resolve({ status: res.statusCode, headers: res.headers, text, bytes, value: text ? JSON.parse(text) : null });
          } catch { reject(new Error('independent_invalid_json_response')); }
          finally { if (unfinished) req.destroy(); }
        });
      });
      req.setTimeout(4000, () => req.destroy(new Error('independent_http_timeout')));
      req.once('error', reject);
      if (unfinished) req.write(body); else req.end(body);
    });
  }
  const signing = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  async function signed(op, args = {}) {
    const rpc = async value => {
      const reply = await wire('/api/connect/rpc', { method: 'POST', headers: { Origin: origin },
        body: JSON.stringify({ protocol: 1, ...value }) });
      assert.equal(reply.status, 200); return good(reply.value);
    };
    const challenge = await rpc({ op: 'challenge', args: { operation: op, digest: digestArgs(args) } });
    return rpc({ op, args, proof: { challengeId: challenge.challengeId,
      publicJwk: signing.publicKey.export({ format: 'jwk' }),
      signature: sign('sha256', Buffer.from(challenge.message), { key: signing.privateKey,
        dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  }
  const account = await signed('bootstrap', { label: 'Independent HTTP owner',
    encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) });
  const owner = (op, args = {}) => signed(op, { expectedAccountId: account.accountId, ...args });
  const { principal } = await owner('access.principals.create', { label: 'Independent external client' });
  const { grant } = await owner('access.grants.issue', { principalId: principal.id,
    capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }], resources: ['notes:new'],
    effects: ['create'], recipients: ['soty:notes'], expiresAt: Date.now() + 3600000,
    budget: { unit: 'invocations', limit: 10 } });
  const credential = () => owner('access.credentials.issue', { grantId: grant.id, audience: origin });
  const identity = await credential();
  function sql(store, query) {
    const db = new DatabaseSync(store === 'notes' ? notesFile : capsFile, { readOnly: true });
    try { return db.prepare(query).all().map(row => ({ ...row })); } finally { db.close(); }
  }
  function receiptFault(enabled) {
    const db = new DatabaseSync(capsFile);
    try { db.exec(enabled
      ? "CREATE TRIGGER independent_receipt_fault BEFORE INSERT ON cap_receipts BEGIN SELECT RAISE(ABORT,'independent_secret_receipt_failure'); END;"
      : 'DROP TRIGGER independent_receipt_fault'); } finally { db.close(); }
  }
  return { app, origin, wire, owner, credential, identity, sql, receiptFault };
}

function privateReply(reply, secrets = []) {
  assert.equal(reply.headers['cache-control'], 'no-store');
  assert.equal(reply.headers['x-content-type-options'], 'nosniff');
  assert.equal(reply.headers['referrer-policy'], 'no-referrer');
  assert.equal(reply.headers['access-control-allow-origin'], undefined);
  assert.equal(reply.headers.etag, undefined); assert.ok(reply.bytes <= 65536);
  for (const value of secrets) assert.equal(reply.text.includes(value), false, 'private/internal value is absent');
}

test('wire unknown after actual Notes COMMIT and Caps receipt failure is resolved only by proof, with a fresh read after credential revoke', { timeout: 15000 }, async t => {
  const f = await host(t), input = { title: 'Неизвестный результат 🐝', body: 'Точный текст e\u0301\nи новая строка',
    idempotencyKey: 'independent-wire-receipt-fault' };
  const secrets = [input.title, input.body, f.identity.token, 'independent_secret_receipt_failure', 'INSERT INTO'];
  f.receiptFault(true);
  const pending = await f.wire(CREATE, { method: 'POST', token: f.identity.token, body: JSON.stringify(input) });
  assert.equal(pending.status, 202); privateReply(pending, secrets);
  assert.equal(pending.value.reused, false); assert.equal(pending.value.invocation.effectState, 'unknown');
  assert.equal(pending.value.invocation.status, 'accepted');
  assert.equal(pending.value.result, undefined); assert.equal(pending.value.invocation.receipt, undefined);
  const id = pending.value.invocation.invocationId;
  assert.equal(pending.headers.location, f.origin + HISTORY + id);
  assert.equal(f.sql('notes', 'SELECT count(*) AS n FROM note_native_creates')[0].n, 1);
  assert.equal(f.sql('caps', 'SELECT count(*) AS n FROM cap_receipts')[0].n, 0);
  assert.deepEqual(f.sql('caps', 'SELECT reserved_amount,spent_amount FROM cap_budgets'), [{ reserved_amount: 1, spent_amount: 0 }]);
  const stored = f.sql('notes', 'SELECT title,body FROM notes')[0];
  assert.equal(stored.title, input.title); assert.equal(stored.body, input.body);

  const read = await f.wire(HISTORY + id, { token: f.identity.token, headers: { 'If-None-Match': '*' } });
  assert.equal(read.status, 200); privateReply(read, secrets);
  assert.deepEqual(read.value.invocation, pending.value.invocation);
  assert.equal(f.sql('caps', 'SELECT count(*) AS n FROM cap_receipts')[0].n, 0, 'GET does not reconcile or execute');
  await f.owner('access.credentials.revoke', { credentialId: f.identity.credential.id });
  const denied = await f.wire(HISTORY + id, { token: f.identity.token });
  assert.equal(denied.status, 401); assert.deepEqual(denied.value, { error: { code: 'authorization_required' } });
  privateReply(denied, [...secrets, id]);
  f.receiptFault(false);

  let tick, executions = 0;
  const coordinator = f.app.locals.capabilitiesService.nativeNotes;
  const recovery = startNativeRecovery({ coordinator: {
    reconcilePage: args => coordinator.reconcilePage(args),
    execute() { executions++; throw new Error('unexpected_autoexecution'); },
  }, timers: {
    setTimeout(callback) { tick = callback; return { unref() {} }; },
    clearTimeout() { tick = null; },
  } });
  t.after(() => recovery.close()); tick();
  assert.equal(recovery.status().checked, 1); assert.equal(recovery.status().committed, 1);
  assert.equal(recovery.status().unavailable, false); assert.equal(executions, 0);
  recovery.close();
  assert.equal((await f.wire(HISTORY + id, { token: f.identity.token })).status, 401);
  const renewed = await f.credential();
  const complete = await f.wire(HISTORY + id, { token: renewed.token });
  assert.equal(complete.status, 200); assert.equal(complete.value.invocation.status, 'succeeded');
  assert.equal(complete.value.invocation.effectState, 'committed'); privateReply(complete, [...secrets, renewed.token]);
  assert.equal(complete.value.result.revision, 1);
  assert.equal(complete.value.result.url, `${f.origin}/#notes/${complete.value.result.noteId}`);
  const replay = await f.wire(CREATE, { method: 'POST', token: renewed.token, body: JSON.stringify(input) });
  assert.equal(replay.status, 200); assert.equal(replay.value.reused, true);
  assert.deepEqual(replay.value.invocation, complete.value.invocation);
  assert.equal(f.sql('notes', 'SELECT count(*) AS n FROM note_native_creates')[0].n, 1);
  assert.deepEqual(f.sql('caps', 'SELECT reserved_amount,spent_amount FROM cap_budgets'), [{ reserved_amount: 0, spent_amount: 1 }]);
  assert.equal(f.sql('caps', 'SELECT input_json FROM cap_invocations')[0].input_json, 'null');
});

test('early auth failure closes an incomplete upload, and the same client then receives canonical uncached private responses', { timeout: 15000 }, async t => {
  const f = await host(t);
  const early = await f.wire(CREATE, { method: 'POST', unfinished: true, body: '{"title":',
    headers: { 'Content-Length': '1000000', 'X-Forwarded-Host': 'untrusted.invalid' } });
  assert.equal(early.status, 401); assert.equal(early.headers.connection, 'close');
  assert.deepEqual(early.value, { error: { code: 'authorization_required' } }); privateReply(early);
  assert.equal(f.sql('caps', 'SELECT count(*) AS n FROM cap_invocations')[0].n, 0);
  const payload = { title: '', body: 'Сохранить после раннего отказа', idempotencyKey: 'independent-after-early-rejection' };
  const accepted = await f.wire(CREATE, { method: 'POST', token: f.identity.token,
    headers: { 'X-Forwarded-Host': 'untrusted.invalid', 'X-Forwarded-Proto': 'https' }, body: JSON.stringify(payload) });
  assert.equal(accepted.status, 201); privateReply(accepted, [payload.body, f.identity.token, 'untrusted.invalid']);
  const id = accepted.value.invocation.invocationId;
  assert.equal(accepted.headers.location, f.origin + HISTORY + id);
  assert.equal(accepted.value.result.url, `${f.origin}/#notes/${accepted.value.result.noteId}`);
  const head = await f.wire(HISTORY + id, { method: 'HEAD', token: f.identity.token });
  assert.equal(head.status, 405); assert.equal(head.headers.allow, 'GET'); assert.equal(head.text, ''); privateReply(head);
  const get = await f.wire(HISTORY + id, { token: f.identity.token, headers: { 'If-None-Match': '*' } });
  assert.equal(get.status, 200); privateReply(get, [payload.body, f.identity.token]);
  assert.deepEqual(get.value.invocation, accepted.value.invocation);
  assert.equal(f.sql('notes', 'SELECT count(*) AS n FROM note_native_creates')[0].n, 1);
});
