import test from 'node:test';
import assert from 'node:assert/strict';
import { request } from 'node:http';
import express from 'express';
import { attachCapabilitiesActions } from '../capabilities-actions.js';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';

const CREATE = '/api/capabilities/v1/notes/drafts';
const input = { title: 'Долговечный результат', body: 'Точный текст после потери связи', idempotencyKey: 'durable-http-result-01' };
const post = (f, token) => f.http(CREATE, { method: 'POST', token, body: JSON.stringify(input) });
const noteCount = f => f.sql(f.notesFile, db => db.prepare('SELECT count(*) AS n FROM note_native_creates').get().n);

test('a real socket lost after the committed effect retries the original HTTP key without a second Note or charge', async t => {
  const f = await nativeHttpFixture(t), owner = nativeIdentity('Автор'), account = await f.bootstrap(owner);
  const identity = await f.issue(owner, account.accountId);
  let dropped = false;
  const intercept = (req, res) => {
    if (req.url !== CREATE || dropped) return;
    const end = res.end;
    res.end = function(...args) {
      assert.equal(noteCount(f), 1, 'the real effect is already committed before the response is lost');
      dropped = true; res.end = end; req.socket.destroy(); return this;
    };
  };
  f.server.prependListener('request', intercept);
  await assert.rejects(post(f, identity.token), error => error.code === 'ECONNRESET');
  f.server.removeListener('request', intercept);
  assert.equal(dropped, true);
  const retry = await post(f, identity.token);
  assert.equal(retry.status, 200); assert.equal(retry.body.reused, true);
  assert.equal(retry.body.invocation.status, 'succeeded'); assert.equal(noteCount(f), 1);
  const note = good(await f.call(owner, 'notes.get', { expectedAccountId: account.accountId, noteId: retry.body.result.noteId })).note;
  assert.equal(note.body, input.body);
  assert.deepEqual(f.sql(f.capsFile, db => ({ ...db.prepare('SELECT reserved_amount,spent_amount FROM cap_budgets').get() })),
    { reserved_amount: 0, spent_amount: 1 });
});

test('a real signed revoke while the HTTP body is arriving is rechecked before admission', async t => {
  const f = await nativeHttpFixture(t), owner = nativeIdentity('Автор'), account = await f.bootstrap(owner);
  const identity = await f.issue(owner, account.accountId);
  const text = JSON.stringify(input);
  let received;
  const bodyStarted = new Promise(resolve => { received = resolve; });
  const watch = req => { if (req.url === CREATE) req.once('data', received); };
  f.server.prependListener('request', watch);
  let req;
  const response = new Promise((resolve, reject) => {
    req = request(f.origin + CREATE, { method: 'POST', headers: { authorization: `Bearer ${identity.token}`,
      'content-type': 'application/json', 'content-length': Buffer.byteLength(text) } }, res => {
      const parts = []; res.on('data', chunk => parts.push(chunk)); res.once('error', reject);
      res.once('end', () => resolve({ status: res.statusCode, body: JSON.parse(Buffer.concat(parts)) }));
    });
    req.once('error', reject); req.write(text.slice(0, 10));
  });
  await bodyStarted;
  good(await f.call(owner, 'access.grants.revoke', { expectedAccountId: account.accountId, grantId: identity.grantId }));
  req.end(text.slice(10)); const denied = await response;
  f.server.removeListener('request', watch);
  assert.ok([401, 403].includes(denied.status)); assert.deepEqual(Object.keys(denied.body), ['error']);
  assert.equal(noteCount(f), 0);
  assert.equal(f.sql(f.capsFile, db => db.prepare('SELECT count(*) AS n FROM cap_invocations').get().n), 0);
});

test('the HTTP response reauthorizes after a real effect and signed revoke rather than disclosing the actorless executor result', async t => {
  const f = await nativeHttpFixture(t), owner = nativeIdentity('Автор'), account = await f.bootstrap(owner);
  const identity = await f.issue(owner, account.accountId), service = f.app.locals.capabilitiesService;
  const preparedRevoke = await f.proof(owner, 'access.grants.revoke', { expectedAccountId: account.accountId, grantId: identity.grantId });
  let revoke, executorResult, reads = 0;
  const front = express();
  attachCapabilitiesActions(front, { audience: f.origin, service: {
    authenticateCredential: service.authenticateCredential,
    nativeNotes: { ...service.nativeNotes,
      execute(args) {
        executorResult = service.nativeNotes.execute(args);
        // Connect's signed grant operation commits synchronously before its
        // Promise is returned. This seam is outside the finished effect fence.
        revoke = f.app.locals.connectService.handle({ ...preparedRevoke, origin: f.origin });
        assert.equal(f.sql(f.capsFile, db => db.prepare('SELECT revoked_at FROM cap_grants WHERE id=?').get(identity.grantId).revoked_at) > 0, true);
        return executorResult;
      },
      get(args) { reads++; return service.nativeNotes.get(args); },
    },
  } });
  front.use((req, res) => f.app(req, res)); f.setFront(front);
  const denied = await post(f, identity.token);
  good(await revoke);
  assert.equal(executorResult.outcome, 'committed'); assert.equal(reads, 1);
  assert.ok([401, 403].includes(denied.status)); assert.deepEqual(Object.keys(denied.body), ['error']);
  assert.equal(denied.headers['cache-control'], 'no-store');
  const wire = JSON.stringify(denied.body);
  assert.equal(wire.includes(executorResult.invocation.invocationId), false);
  assert.equal(noteCount(f), 1);
  const noteId = executorResult.invocation.receipt.artifacts[0].id;
  assert.equal(good(await f.call(owner, 'notes.get', { expectedAccountId: account.accountId, noteId })).note.body, input.body);
});

test('an exception after the real Caps receipt COMMIT still returns a freshly read durable success', async t => {
  const f = await nativeHttpFixture(t), owner = nativeIdentity('Автор'), account = await f.bootstrap(owner);
  const identity = await f.issue(owner, account.accountId), service = f.app.locals.capabilitiesService;
  const front = express(); let reads = 0;
  attachCapabilitiesActions(front, { audience: f.origin, service: { authenticateCredential: service.authenticateCredential,
    nativeNotes: { ...service.nativeNotes,
      execute(args) { service.nativeNotes.execute(args); throw new Error('after_committed_receipt_test'); },
      get(args) { reads++; return service.nativeNotes.get(args); },
    } } });
  front.use((req, res) => f.app(req, res)); f.setFront(front);
  const success = await post(f, identity.token);
  assert.equal(success.status, 201); assert.equal(success.body.invocation.status, 'succeeded');
  assert.equal(reads, 1); assert.equal(noteCount(f), 1);
  assert.equal((await post(f, identity.token)).body.reused, true); assert.equal(noteCount(f), 1);
});
