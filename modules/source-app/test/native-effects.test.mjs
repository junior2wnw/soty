import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createSourceNativeAuthorityPort, createNativeAuthorityRuntime, isSourceNativeCommitPort } from '../server/native-authority.mjs';
import { check, digest } from '../server/wire.mjs';

const binding = { identity: { issuer: 'https://issuer.test/human-identity', subject: 'synthetic-reporter' },
  resource: { selection: { kind: 'soty.resource.v1', nativeId: 'selected', incarnationId: 'one' } }, operation: 'execute' };
const input = { requestId: 'fixture-effect-0001', input: { title: 'Synthetic empty resource item' } };

async function fixture(t, { beforeCommit, afterCommit } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-source-effect-'));
  let db = new DatabaseSync(join(directory, 'native.sqlite'));
  db.exec(`CREATE TABLE grant_state (subject TEXT PRIMARY KEY, resource TEXT NOT NULL, live INTEGER NOT NULL, support INTEGER NOT NULL);
    INSERT INTO grant_state VALUES ('synthetic-reporter', 'selected', 1, 0);
    CREATE TABLE effects (request_id TEXT PRIMARY KEY, input_digest TEXT NOT NULL, resource TEXT NOT NULL, revision INTEGER NOT NULL);
    CREATE TABLE receipts (request_id TEXT PRIMARY KEY, input_digest TEXT NOT NULL, receipt_json TEXT NOT NULL);`);
  const proofs = new WeakSet(); let calls = 0;
  const port = createSourceNativeAuthorityPort({
    capture: async () => { const proof = Object.freeze({}); proofs.add(proof); return proof; },
    withCurrent(proof, scope, final) {
      const grant = db.prepare('SELECT * FROM grant_state WHERE subject = ?').get(scope.identity.subject);
      check(proofs.has(proof) && grant?.live === 1 && grant.resource === scope.resource.selection.nativeId, 'source_app_native_access_denied', 403);
      return final();
    },
    execute: async (_proof, scope, args, final) => {
      calls++; await beforeCommit?.(); assert.equal(isSourceNativeCommitPort(final), true);
      let result;
      db.exec('BEGIN IMMEDIATE');
      try {
        // This is the Native commit boundary, not an SDK pre/post assertion.
        result = final.commit(() => {
          const expected = digest(args.input), old = db.prepare('SELECT * FROM receipts WHERE request_id = ?').get(args.requestId);
          check(!old || old.input_digest === expected, 'source_app_request_conflict', 409);
          if (old) return JSON.parse(old.receipt_json);
          const receipt = { requestId: args.requestId, inputDigest: expected, outcome: 'committed', revision: 1 };
          db.prepare('INSERT INTO effects VALUES (?, ?, ?, ?)').run(args.requestId, expected, scope.resource.selection.nativeId, 1);
          db.prepare('INSERT INTO receipts VALUES (?, ?, ?)').run(args.requestId, expected, JSON.stringify(receipt)); return receipt;
        });
        db.exec('COMMIT');
      } catch (error) { db.exec('ROLLBACK'); throw error; }
      await afterCommit?.(); return result;
    },
    readProof: async (_proof, scope, args) => {
      const effect = db.prepare('SELECT * FROM effects WHERE request_id = ? AND resource = ?').get(args.requestId, scope.resource.selection.nativeId);
      check(!effect || effect.input_digest === args.inputDigest, 'source_app_request_conflict', 409);
      const row = db.prepare('SELECT * FROM receipts WHERE request_id = ?').get(args.requestId);
      return row ? JSON.parse(row.receipt_json) : { requestId: args.requestId, inputDigest: args.inputDigest, outcome: 'not_applied' };
    },
    feedback: {
      context: async () => ({}), list: async () => [], get: async () => ({}), submit: async () => ({}),
      reply: async (_proof, scope, _args, final) => {
        db.exec('BEGIN IMMEDIATE');
        try {
          const row = db.prepare('SELECT support FROM grant_state WHERE subject = ?').get(scope.identity.subject);
          check(row?.support === 1, 'source_app_native_support_denied', 403);
          const result = final.commit(() => {
            return { replied: true };
          }); db.exec('COMMIT'); return result;
        } catch (error) { db.exec('ROLLBACK'); throw error; }
      },
    },
  });
  const runtime = createNativeAuthorityRuntime(port), proof = await runtime.capture(binding);
  t.after(async () => { runtime.close(); db.close(); await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 30 }); });
  return { runtime, proof, revoke() { db.prepare('UPDATE grant_state SET live = 0').run(); },
    count(table) { assert.ok(['effects', 'receipts'].includes(table)); return db.prepare('SELECT count(*) AS n FROM ' + table).get().n; },
    calls: () => calls, restart() { db.close(); db = new DatabaseSync(join(directory, 'native.sqlite')); } };
}

test('actual Native SQLite fence: revoke after async preparation prevents effect and receipt at commit', async t => {
  let reached, release; const entered = new Promise(done => { reached = done; }), gate = new Promise(done => { release = done; });
  const f = await fixture(t, { beforeCommit: async () => { reached(); await gate; } });
  const pending = f.runtime.call(f.proof, 'execute', input); await entered; f.revoke(); release();
  await assert.rejects(pending, error => error.code === 'source_app_native_access_denied' && error.status === 403);
  assert.equal(f.count('effects'), 0); assert.equal(f.count('receipts'), 0);
});

test('actual Native SQLite lost ACK after COMMIT is unknown; readonly receipt survives restart without apply retry', async t => {
  const f = await fixture(t, { afterCommit() { throw new Error('synthetic lost response after Native COMMIT'); } });
  await assert.rejects(f.runtime.call(f.proof, 'execute', input), error => error.code === 'source_app_effect_unknown' && error.status === 503);
  assert.equal(f.count('effects'), 1); assert.equal(f.count('receipts'), 1); f.restart();
  const receipt = await f.runtime.call(f.proof, 'readProof', { requestId: input.requestId, inputDigest: digest(input.input) });
  assert.equal(receipt.outcome, 'committed'); assert.equal(f.calls(), 1); assert.equal(f.count('effects'), 1);
  await assert.rejects(f.runtime.call(f.proof, 'readProof', { requestId: input.requestId, inputDigest: 'a'.repeat(64) }), error => error.code === 'source_app_request_conflict');
});

test('post-COMMIT Native revoke is unknown, never a false rollback or private receipt under revoked access', async t => {
  let f; f = await fixture(t, { afterCommit() { f.revoke(); } });
  await assert.rejects(f.runtime.call(f.proof, 'execute', input), error => error.code === 'source_app_effect_unknown');
  assert.equal(f.count('effects'), 1); assert.equal(f.count('receipts'), 1);
  await assert.rejects(f.runtime.call(f.proof, 'readProof', { requestId: input.requestId, inputDigest: digest(input.input) }), error => error.status === 403);
});

test('Source support authority is separate from Native participant or Root App ownership', async t => {
  const f = await fixture(t);
  await assert.rejects(f.runtime.feedback(f.proof, 'reply', { requestId: 'fixture-reply-0001', ticketId: 'one', body: 'Synthetic reply', expectedRevision: 1 }),
    error => error.code === 'source_app_native_support_denied' && error.status === 403);
  assert.equal(f.count('effects'), 0); assert.equal(f.count('receipts'), 0);
});

test('mutators require one operation-local synchronous commit port; late, double and thenable callbacks cannot add effects', async () => {
  for (const mode of ['missing', 'late', 'double', 'thenable', 'reentrant']) {
    let later, effects = 0;
    const runtime = createNativeAuthorityRuntime(createSourceNativeAuthorityPort({ capture: async () => ({}), withCurrent(_native, _scope, final) { return final(); },
      execute: async (_native, _scope, _args, port) => {
        later = port;
        if (mode === 'missing' || mode === 'late') return {};
        return port.commit(() => {
          if (mode === 'reentrant') return port.commit(() => { effects++; return {}; });
          if (mode === 'thenable') return Promise.resolve({});
          effects++;
          if (mode === 'double') { try { port.commit(() => { effects++; return {}; }); } catch {} }
          return {};
        });
      } }));
    const proof = await runtime.capture(binding); await assert.rejects(runtime.call(proof, 'execute', input));
    const beforeLate = effects; assert.throws(() => later.commit(() => { effects++; return {}; })); assert.equal(effects, beforeLate);
    assert.equal(effects, mode === 'double' ? 1 : 0); runtime.close();
  }
  assert.equal(isSourceNativeCommitPort({ commit() {} }), false);
});
