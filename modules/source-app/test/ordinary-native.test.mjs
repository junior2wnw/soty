import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { mkdtemp, rm, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createOrdinaryAppStore } from '../examples/ordinary-app/store.mjs';
import { createOrdinaryAppNativePort } from '../examples/ordinary-app/native.mjs';
import { createNativeAuthorityRuntime } from '../server/native-authority.mjs';
import { digest } from '../server/wire.mjs';
import { feedbackOutput } from '../shared/feedback-wire.mjs';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

async function fixture(t, { realm = 'board', beforeCommit, emptyGuest = false } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-ordinary-native-')), key = randomBytes(32), options = { databasePath: join(directory, 'source.sqlite'), realmId: realm, key, keyId: 'fixture-key', initialize: true };
  let store = createOrdinaryAppStore(options), runtime;
  store.createResource({ id: 'selected', incarnationId: 'one', title: 'Synthetic resource', guestEmpty: emptyGuest });
  if (!emptyGuest) {
    store.createPrincipal('native-owner'); store.createPrincipal('native-participant');
    store.grant('selected', 'native-owner', 'owner'); store.grant('selected', 'native-participant', 'participant');
  }
  const sessions = emptyGuest ? {} : { owner: store.createNativeSession('native-owner'), participant: store.createNativeSession('native-participant') };
  function createRuntime() { runtime = createNativeAuthorityRuntime(createOrdinaryAppNativePort({ store, resourceId: 'selected', incarnationId: 'one', beforeCommit, allowEmptyGuest: emptyGuest })); }
  createRuntime(); t.after(async () => { runtime.close(); store.close(); await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 40 }); });
  const binding = subject => ({ identity: { issuer: 'https://issuer.test/human-identity', subject }, rootPrincipal: { accountId: 'root-app-owner', deviceId: 'root-device' },
    humanPrincipal: { issuer: 'https://issuer.test/human-identity', subject, clientId: 'fixture' }, resource: { selection: { kind: 'soty.resource.v1', nativeId: 'selected', incarnationId: 'one' } }, semanticDigest: 'a'.repeat(64), operation: 'link' });
  async function legacy(which) {
    const selected = binding('oidc-' + which), proof = await runtime.capture(selected, { headers: { cookie: 'ordinary_native_' + realm + '=' + sessions[which] } });
    store.tx(() => runtime.commitIdentity(proof, selected.identity)); return proof;
  }
  return { get store() { return store; }, get runtime() { return runtime; }, sessions, binding, legacy, options,
    restart() { runtime.close(); store.close(); store = createOrdinaryAppStore({ ...options, initialize: false }); createRuntime(); } };
}

test('sealed token expiry stays separate from the shortened Source deadline; fresh expiry and revoke still deny', async t => {
  const f = await fixture(t), originalEnd = Date.now() + 300000, tokenEnd = originalEnd + 300000, sourceEnd = Date.now() + 120000;
  for (const legacy of [false, true]) {
    const idHash = digest('synthetic-source-session-' + legacy);
    const payload = { idHash, accessToken: 'synthetic-access-token', expiresAt: originalEnd, ...(legacy ? {} : { accessExpiresAt: tokenEnd }) };
    f.store.tx(() => f.store.db.prepare('INSERT INTO source_sessions VALUES(?,?,?,?,?,?,?,?)').run(idHash, digest(f.sessions.owner),
      'native-owner', 'selected', 1, sourceEnd, f.store.encrypt('Session', idHash, 0, payload), 'fixture-key'));
    f.restart();
    const session = await f.store.storage.readSession(idHash);
    assert.equal(session.expiresAt, sourceEnd);
    const token = await f.store.storage.readTokenProof(session);
    assert.equal(token.expiresAt, legacy ? originalEnd : tokenEnd);
    assert.ok(token.expiresAt > session.expiresAt);
    f.store.tx(() => f.store.db.prepare('UPDATE source_sessions SET expires_at=? WHERE id_hash=?').run(Date.now() - 1, idHash));
    await assert.rejects(f.store.storage.readTokenProof(session), error => error.code === 'authentication_required');
    f.store.tx(() => f.store.db.prepare('UPDATE source_sessions SET expires_at=?,active=0 WHERE id_hash=?').run(sourceEnd, idHash));
    await assert.rejects(f.store.storage.readTokenProof(session), error => error.code === 'authentication_required');
  }
});

test('actual example Native SQL: Root App owner cannot become Native support; feedback receipt/media and exact writes survive restart', async t => {
  const f = await fixture(t), participant = await f.legacy('participant'), owner = await f.legacy('owner');
  const write = { requestId: 'ordinary-item-0001', input: { operation: 'items.create', title: 'Synthetic item' } };
  assert.equal((await f.runtime.call(participant, 'execute', write)).outcome, 'committed');
  assert.equal((await f.runtime.call(participant, 'execute', write)).replayed, true);
  assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_items').get().n, 1);
  const png = await readFile(new URL('../../feedback/test/fixtures/chrome.png', import.meta.url));
  const submit = { requestId: 'ordinary-feedback-0001', body: 'Synthetic private issue', attachments: [{ kind: 'image', name: 'fixture.png', mimeType: 'image/png', dataBase64: png.toString('base64') }] };
  const received = feedbackOutput('submit', await f.runtime.feedback(participant, 'submit', submit));
  assert.equal(received.ticket.canManage, false);
  await assert.rejects(f.runtime.feedback(participant, 'status', { requestId: 'ordinary-status-0001', ticketId: received.ticket.id, expectedRevision: 1, status: 'ready_to_check' }), error => error.code === 'ordinary_native_support_denied');
  const ready = await f.runtime.feedback(owner, 'status', { requestId: 'ordinary-status-0002', ticketId: received.ticket.id, expectedRevision: 1, status: 'ready_to_check' });
  assert.equal(ready.ticket.status, 'ready_to_check');
  await assert.rejects(f.runtime.feedback(owner, 'accept', { requestId: 'ordinary-accept-0001', ticketId: received.ticket.id, expectedRevision: 2 }), error => error.code === 'ordinary_native_reporter_denied');
  const acceptedArgs = { requestId: 'ordinary-accept-0002', ticketId: received.ticket.id, expectedRevision: 2 };
  assert.equal((await f.runtime.feedback(participant, 'accept', acceptedArgs)).ticket.status, 'resolved');
  assert.equal((await f.runtime.feedback(participant, 'accept', acceptedArgs)).replayed, true);
  f.restart(); const freshParticipant = await f.legacy('participant');
  const receipt = await f.runtime.call(freshParticipant, 'readProof', { requestId: write.requestId, input: { inputDigest: digest(write.input) } });
  assert.equal(receipt.outcome, 'committed'); assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_items').get().n, 1);
  const retained = (await f.runtime.feedback(freshParticipant, 'get', { ticketId: received.ticket.id })).ticket;
  assert.equal(retained.status, 'resolved'); assert.equal(retained.attachments[0].dataBase64 === submit.attachments[0].dataBase64, true);
});

test('two actual OS Native writers share one Source SQL authority and exact receipt; duplicate request commits once', async t => {
  const f = await fixture(t); await f.legacy('participant');
  const config = { options: { databasePath: f.options.databasePath, realmId: 'board', keyId: 'fixture-key' }, keyBase64: f.options.key.toString('base64'),
    nativeToken: f.sessions.participant, binding: f.binding('oidc-participant'), args: { requestId: 'ordinary-two-os-0001', input: { operation: 'items.create', title: 'Once across two Native processes' } } };
  const execute = () => new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [fileURLToPath(new URL('./support/ordinary-native-worker.mjs', import.meta.url))], { windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] });
    let output = '', errorBytes = 0; child.stdout.on('data', part => { output += part; }); child.stderr.on('data', part => { errorBytes += part.length; }); child.once('error', reject);
    child.once('exit', code => resolve({ code, result: JSON.parse(output), errorBytes })); child.stdin.end(JSON.stringify(config));
  });
  const results = await Promise.all([execute(), execute()]); assert.ok(results.every(result => result.code === 0), 'worker safe status=' + results.map(result => result.code).join(','));
  assert.deepEqual(results.map(result => result.result.replayed).sort(), [false, true]);
  assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_items').get().n, 1); assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_receipts').get().n, 1);
});

test('actual Source selected Native ACL checks at final SQL commit, not an async pre/post illusion', async t => {
  let reached, release; const entered = new Promise(done => { reached = done; }), gate = new Promise(done => { release = done; });
  const f = await fixture(t, { beforeCommit: async operation => { if (operation === 'items.create') { reached(); await gate; } } }), participant = await f.legacy('participant');
  const pending = f.runtime.call(participant, 'execute', { requestId: 'ordinary-held-0001', input: { operation: 'items.create', title: 'Denied at final commit' } });
  await entered; f.store.revokeMembership('selected', 'native-participant'); release();
  await assert.rejects(pending, error => error.code === 'ordinary_native_access_denied');
  assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_items').get().n, 0);
  assert.equal(f.store.db.prepare('SELECT count(*) AS n FROM native_receipts').get().n, 0);
});

test('existing Native link needs real Native session, conflicting immutable issuer/sub is never auto-merged; guest only creates empty app-owned resource', async t => {
  const f = await fixture(t); await assert.rejects(f.runtime.capture(f.binding('missing-native-proof'), { headers: {} }), error => error.status === 403);
  const participant = await f.legacy('participant'), ownerBinding = f.binding('oidc-participant');
  const owner = await f.runtime.capture(ownerBinding, { headers: { cookie: 'ordinary_native_board=' + f.sessions.owner } });
  assert.throws(() => f.store.tx(() => f.runtime.commitIdentity(owner, ownerBinding.identity)), error => error.code === 'ordinary_native_link_conflict');
  assert.equal((await f.runtime.call(participant, 'read', { requestId: 'ordinary-read-0001', input: { operation: 'items.list' } })).items.length, 0);
  const guest = await fixture(t, { realm: 'new-empty', emptyGuest: true }), guestBinding = guest.binding('oidc-new-guest');
  const proof = await guest.runtime.capture(guestBinding, { headers: {} });
  const linked = guest.store.tx(() => guest.runtime.commitIdentity(proof, guestBinding.identity, { createEmptyGuest: true }));
  guest.runtime.withCurrent(proof, () => true); assert.equal(typeof linked.principalId, 'string');
  assert.equal(guest.store.db.prepare('SELECT count(*) AS n FROM native_principals').get().n, 1);
  assert.equal(guest.store.db.prepare('SELECT count(*) AS n FROM native_items').get().n, 0);
  await assert.rejects(guest.runtime.capture(guest.binding('another-guest'), { headers: {} }), error => error.code === 'ordinary_guest_existing_resource_denied');
});

test('private queue keyset pages retain every ticket and preserve reporter/owner isolation', async t => {
  const f = await fixture(t), participant = await f.legacy('participant'), owner = await f.legacy('owner');
  for (let i = 0; i < 23; i++) await f.runtime.feedback(participant, 'submit', {
    requestId: 'paging-participant-' + String(i).padStart(4, '0'), body: 'Synthetic participant ticket ' + i, attachments: [] });
  await f.runtime.feedback(owner, 'submit', { requestId: 'paging-owner-ticket-0001', body: 'Synthetic owner-only ticket', attachments: [] });
  const first = feedbackOutput('list', await f.runtime.feedback(participant, 'list', { limit: 20 }));
  assert.equal(first.tickets.length, 20); assert.equal(typeof first.nextCursor, 'string');
  const last = feedbackOutput('list', await f.runtime.feedback(participant, 'list', { limit: 20, cursor: first.nextCursor }));
  assert.equal(last.tickets.length, 3); assert.equal(last.nextCursor, null);
  assert.equal(new Set([...first.tickets, ...last.tickets].map(value => value.id)).size, 23);
  assert.ok([...first.tickets, ...last.tickets].every(value => value.body.startsWith('Synthetic participant')));
  const all = feedbackOutput('list', await f.runtime.feedback(owner, 'list', { limit: 50 })); assert.equal(all.tickets.length, 24);
  await assert.rejects(f.runtime.feedback(participant, 'list', { limit: 20, cursor: 'invalid_cursor' }), error => error.code === 'ordinary_native_cursor_invalid');
});
test('long private threads stay out of the 64KiB list while detail retains the original text and replies', async t => {
  const f = await fixture(t), participant = await f.legacy('participant'), owner = await f.legacy('owner');
  let first;
  for (let i = 0; i < 9; i++) {
    const result = await f.runtime.feedback(participant, 'submit', { requestId: 'long-list-report-' + String(i).padStart(4, '0'), body: 'Ж'.repeat(8000), attachments: [] });
    if (!first) first = result.ticket.id;
    await f.runtime.feedback(owner, 'reply', { requestId: 'long-list-reply-' + String(i).padStart(4, '0'), ticketId: result.ticket.id, expectedRevision: 1, body: 'Я'.repeat(8000) });
  }
  const page = feedbackOutput('list', await f.runtime.feedback(owner, 'list', { limit: 20 }));
  assert.equal(page.tickets.length, 9); assert.ok(Buffer.byteLength(JSON.stringify({ ok: true, data: page })) < 65536);
  assert.ok(page.tickets.every(value => Array.from(value.body).length === 200 && value.messages.length === 0));
  const detail = feedbackOutput('get', await f.runtime.feedback(owner, 'get', { ticketId: first }));
  assert.equal(detail.ticket.body, 'Ж'.repeat(8000)); assert.equal(detail.ticket.messages[0].body, 'Я'.repeat(8000));
});
