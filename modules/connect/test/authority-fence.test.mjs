import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomBytes, sign } from 'node:crypto';
import { mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Worker } from 'node:worker_threads';
import { DatabaseSync } from 'node:sqlite';
import { createConnectService, digestArgs } from '../server/index.mjs';

const PROJECT = 'authority-fence-test', ORIGIN = 'https://authority.test';
const code = expected => error => error?.code === expected;
function fixture(t, extensions = []) {
  const parent = realpathSync(tmpdir()), directory = mkdtempSync(path.join(parent, 'soty-authority-fence-'));
  const nonce = randomBytes(16).toString('hex'); writeFileSync(path.join(directory, 'owned'), nonce, { flag: 'wx' });
  const databasePath = path.join(directory, 'connect.sqlite');
  const service = createConnectService({ databasePath, projectId: PROJECT, allowedOrigins: [ORIGIN], extensions });
  const workers = [];
  t.after(async () => {
    for (const worker of workers) await worker.terminate();
    service.close();
    const actual = realpathSync(directory);
    assert.equal(path.dirname(actual), parent); assert.match(path.basename(actual), /^soty-authority-fence-/u);
    assert.equal(readFileSync(path.join(actual, 'owned'), 'utf8'), nonce);
    rmSync(actual, { recursive: true });
  });
  async function worker(mode) {
    const shared = new SharedArrayBuffer(8), flags = new Int32Array(shared);
    const value = new Worker(new URL('./support/authority-writer.mjs', import.meta.url), {
      workerData: { mode, databasePath, projectId: PROJECT, origin: ORIGIN, flags: shared },
    });
    workers.push(value); await message(value, 'ready'); return { worker: value, flags };
  }
  return { service, databasePath, worker };
}
function message(worker, phase) {
  return new Promise((resolve, reject) => {
    const cleanup = () => { clearTimeout(timer); worker.off('message', onMessage); worker.off('error', onError); worker.off('exit', onExit); };
    const onMessage = value => { if (value.phase === phase) { cleanup(); resolve(value); } };
    const onError = error => { cleanup(); reject(error); };
    const onExit = () => { cleanup(); reject(new Error('writer_exited_before_' + phase)); };
    const timer = setTimeout(() => { cleanup(); reject(new Error('writer_timeout_' + phase)); }, 5000);
    worker.on('message', onMessage); worker.once('error', onError); worker.once('exit', onExit);
  });
}
function identity() {
  const keys = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { privateKey: keys.privateKey, publicJwk: keys.publicKey.export({ format: 'jwk' }),
    encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) };
}
function good(value) { assert.equal(value.ok, true, value.error?.code); return value; }
async function signed(service, actor, op, args = {}) {
  const challenge = good(await service.handle({ op: 'challenge', args: { operation: op, digest: digestArgs(args) }, origin: ORIGIN }));
  return { op, args, origin: ORIGIN, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
    signature: sign('sha256', Buffer.from(challenge.message), { key: actor.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url') } };
}
const call = async (service, actor, op, args) => good(await service.handle(await signed(service, actor, op, args)));
const bootstrap = (service, actor) => call(service, actor, 'bootstrap', { label: 'Synthetic owner', encryptionPublicJwk: actor.encryptionPublicJwk });
async function enroll(service, owner, phone) {
  const start = await call(service, phone, 'enrollment.start', { label: 'Synthetic phone', encryptionPublicJwk: phone.encryptionPublicJwk });
  const status = await call(service, owner, 'status', {});
  await call(service, owner, 'enrollment.approve', { requestId: start.requestId, wrappedKey: { schema: 'test.encrypted', ciphertext: 'synthetic' } });
  return call(service, phone, 'enrollment.finish', { requestId: start.requestId, expectedAccountId: status.accountId });
}

test('host fence excludes a real signed revoke until its synchronous observation completes', async t => {
  const f = fixture(t), owner = identity(), phone = identity();
  await bootstrap(f.service, owner); const actor = await enroll(f.service, owner, phone);
  const request = await signed(f.service, owner, 'device.revoke', { deviceId: actor.deviceId });
  const peer = await f.worker('revoke'), completion = message(peer.worker, 'result');
  const result = f.service.withAuthorityFence(() => {
    peer.worker.postMessage(request);
    if (Atomics.load(peer.flags, 0) === 0) Atomics.wait(peer.flags, 0, 0, 2000);
    assert.equal(Atomics.load(peer.flags, 0), 1, 'the independent writer actually attempted the signed request');
    assert.equal(Atomics.wait(peer.flags, 1, 0, 100), 'timed-out');
    assert.equal(f.service.isActorActive(actor), true);
    return 'observed-before-revoke';
  });
  assert.equal(result, 'observed-before-revoke'); good((await completion).result);
  assert.equal(f.service.withAuthorityFence(() => f.service.isActorActive(actor)), false,
    'a revoke committed first is visible inside the next fence');
});

test('busy acquisition never runs the callback and restores the normal signed writer wait', async t => {
  const f = fixture(t), owner = identity(); await bootstrap(f.service, owner);
  const request = await signed(f.service, owner, 'profile.rename', { label: 'After contention' });
  const peer = await f.worker('lock'), released = message(peer.worker, 'released');
  peer.worker.postMessage({ release: true });
  let calls = 0; const started = performance.now();
  assert.throws(() => f.service.withAuthorityFence(() => { calls++; }), code('connect_authority_busy'));
  assert.equal(calls, 0); assert.ok(performance.now() - started < 2500);
  // The lock remains longer than100ms, so an incorrectly retained short policy fails this real write.
  good(await f.service.handle(request)); await released;
  assert.equal(f.service.withAuthorityFence(() => 'available'), 'available');
});

test('nested mutation and close cannot damage an outer fence or consume a signed proof', async t => {
  const f = fixture(t), owner = identity(); await bootstrap(f.service, owner);
  const request = await signed(f.service, owner, 'profile.rename', { label: 'Still authorized' });
  let denied;
  assert.equal(f.service.withAuthorityFence(() => {
    assert.throws(() => f.service.withAuthorityFence(() => assert.fail()), code('connect_transaction_nested'));
    assert.throws(() => f.service.close(), code('connect_transaction_nested'));
    denied = f.service.handle(request);
    return 42;
  }), 42);
  assert.equal((await denied).error.code, 'connect_transaction_nested');
  good(await f.service.handle(request));
});

test('a fence called by a signed extension cannot roll back the outer proof or action transaction', async t => {
  let service;
  const extension = { operations: new Set(['test.nested']), execute() {
    assert.throws(() => service.withAuthorityFence(() => assert.fail()), code('connect_transaction_nested'));
    assert.throws(() => service.close(), code('connect_transaction_nested'));
    return { nestedRefused: true };
  } };
  const f = fixture(t, [extension]); service = f.service;
  const owner = identity(); await bootstrap(service, owner);
  const request = await signed(service, owner, 'test.nested');
  assert.equal(good(await service.handle(request)).nestedRefused, true);
  assert.equal((await service.handle(request)).error.code, 'challenge_consumed');
  good(await call(service, owner, 'profile.rename', { label: 'Outer commit preserved' }));
});

test('async callbacks are refused before invocation and returned promises cannot count as success', async t => {
  const f = fixture(t); let called = false;
  for (const action of [async () => { called = true; }, async function* () { called = true; }]) {
    assert.throws(() => f.service.withAuthorityFence(action), code('connect_authority_callback_invalid'));
  }
  assert.equal(called, false);
  assert.throws(() => f.service.withAuthorityFence(() => Promise.reject(new Error('synthetic async failure'))), code('connect_authority_callback_async'));
  assert.throws(() => f.service.withAuthorityFence(() => ({ then(_resolve, reject) { reject(new Error('synthetic thenable')); } })), code('connect_authority_callback_async'));
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(f.service.withAuthorityFence(() => 'recovered'), 'recovered');
  const original = new Error('primary callback');
  assert.throws(() => f.service.withAuthorityFence(() => { throw original; }), error => error === original);
  assert.equal(f.service.withAuthorityFence(() => 1), 1);
});

test('cleanup errors preserve the primary callback failure and the store fails closed until rollback', t => {
  const f = fixture(t), exec = DatabaseSync.prototype.exec, original = new Error('primary');
  let connection;
  try {
    DatabaseSync.prototype.exec = function(sql) {
      if (sql === 'ROLLBACK' || sql === 'PRAGMA busy_timeout=5000') { connection = this; throw new Error('synthetic cleanup failure'); }
      return exec.call(this, sql);
    };
    assert.throws(() => f.service.withAuthorityFence(() => { throw original; }), error => error === original);
  } finally { DatabaseSync.prototype.exec = exec; }
  assert.throws(() => f.service.withAuthorityFence(() => assert.fail()), code('connect_transaction_nested'));
  assert.ok(connection?.isTransaction); exec.call(connection, 'ROLLBACK; PRAGMA busy_timeout=5000');
  assert.equal(f.service.withAuthorityFence(() => 1), 1);
});

test('closed services reject the host fence without invoking supplied work', t => {
  const f = fixture(t); f.service.close(); let called = false;
  assert.throws(() => f.service.withAuthorityFence(() => { called = true; }), code('service_closed'));
  assert.equal(called, false); f.service.close();
});

test('private actor fence joins only the exact active signed dispatcher actor; old host fence remains non-nestable', async t => {
  let service; const retained = new Map(); let calls = 0;
  const extension = { operations: new Set(['test.subject']), execute({ actor, args }) {
    if (args.mode === 'different') {
      assert.throws(() => service.withActorAuthorityFence(retained.get(args.deviceId), () => assert.fail()), code('connect_authority_actor_mismatch'));
      return { denied: true };
    }
    retained.set(actor.deviceId, actor);
    assert.throws(() => service.withActorAuthorityFence({ ...actor }, () => assert.fail()), code('connect_authority_actor_invalid'));
    assert.throws(() => service.withAuthorityFence(() => assert.fail()), code('connect_transaction_nested'));
    return { observed: service.withActorAuthorityFence(actor, () => { calls++; return actor.accountId; }) };
  } };
  const f = fixture(t, [extension]); service = f.service;
  const owner = identity(), phone = identity(), foreign = identity();
  const a = await bootstrap(service, owner), b = await bootstrap(service, foreign);
  const enrolled = await enroll(service, owner, phone);
  assert.equal((await call(service, owner, 'test.subject')).observed, a.accountId);
  assert.equal((await call(service, phone, 'test.subject')).observed, a.accountId);
  assert.equal(calls, 2);
  assert.equal((await call(service, foreign, 'test.subject', { mode: 'different', deviceId: a.deviceId })).denied, true);
  assert.equal((await call(service, phone, 'test.subject', { mode: 'different', deviceId: a.deviceId })).denied, true,
    'even another signed installation of the same account cannot join with the old actor reference');
  const actor = retained.get(a.deviceId), mobileActor = retained.get(enrolled.deviceId);
  assert.equal(service.withActorAuthorityFence(actor, () => 'fresh'), 'fresh');
  for (const value of [{ ...actor }, JSON.parse(JSON.stringify(actor)), { accountId: b.accountId, deviceId: a.deviceId }]) {
    assert.throws(() => service.withActorAuthorityFence(value, () => assert.fail()), code('connect_authority_actor_invalid'));
  }
  service.withAuthorityFence(() => {
    assert.throws(() => service.withActorAuthorityFence(actor, () => assert.fail()), code('connect_transaction_nested'));
  });
  service.withActorAuthorityFence(actor, () => {
    assert.throws(() => service.withActorAuthorityFence(actor, () => assert.fail()), code('connect_authority_reentrant'));
    assert.throws(() => service.withActorAuthorityFence(mobileActor, () => assert.fail()), code('connect_authority_reentrant'));
  });
  let asyncCalls = 0;
  assert.throws(() => service.withActorAuthorityFence(actor, async () => { asyncCalls++; }), code('connect_authority_callback_invalid'));
  assert.equal(asyncCalls, 0);
  assert.throws(() => service.withActorAuthorityFence(actor, () => Promise.reject(new Error('synthetic only'))), code('connect_authority_callback_async'));
  await call(service, owner, 'device.revoke', { deviceId: enrolled.deviceId });
  assert.throws(() => service.withActorAuthorityFence(mobileActor, () => assert.fail()), code('device_revoked'));
  // Independent committed ownership change cannot turn an old branded object
  // into authority for the new account, even though isActorActive is field-based.
  const db = new DatabaseSync(f.databasePath);
  try { db.prepare('UPDATE installations SET account_id=? WHERE id=?').run(b.accountId, a.deviceId); } finally { db.close(); }
  assert.throws(() => service.withActorAuthorityFence(actor, () => assert.fail()), code('connect_authority_actor_mismatch'));
});


async function currentContactRequest(service, sender, recipient) {
  const card = await call(service, recipient, 'card.get', {});
  return call(service, sender, 'contacts.request', { cardId: card.cardId });
}
async function currentContactAccept(service, sender, recipient) {
  const request = await currentContactRequest(service, sender, recipient);
  return call(service, recipient, 'contacts.accept', { requestId: request.requestId });
}

test('current contacts require reciprocal active admission and observe removal, block and a new acceptance', async t => {
  const f = fixture(t), owner = identity(), friend = identity(), stranger = identity();
  const a = await bootstrap(f.service, owner), b = await bootstrap(f.service, friend), c = await bootstrap(f.service, stranger);
  const read = () => f.service.withCurrentContacts(a, contacts => ({ ids: contacts.accountIds(), has: contacts.has(b.accountId) }));
  assert.deepEqual(read(), { ids: [], has: false });
  const pending = await currentContactRequest(f.service, owner, friend);
  assert.deepEqual(read(), { ids: [], has: false }, 'an invitation is not a friendship');
  const active = await call(f.service, friend, 'contacts.accept', { requestId: pending.requestId });
  assert.deepEqual(read(), { ids: [b.accountId], has: true });
  f.service.withCurrentContacts(b, contacts => { assert.equal(contacts.has(a.accountId), true); assert.equal(contacts.has(c.accountId), false); });
  await call(f.service, owner, 'contacts.remove', { relationshipId: active.relationshipId });
  assert.deepEqual(read(), { ids: [], has: false });
  await currentContactAccept(f.service, owner, friend);
  await call(f.service, friend, 'contacts.block', { peerAccountId: a.accountId });
  assert.deepEqual(read(), { ids: [], has: false });
  await call(f.service, friend, 'contacts.unblock', { peerAccountId: a.accountId });
  assert.deepEqual(read(), { ids: [], has: false }, 'unblock does not restore the previous relationship');
  await currentContactAccept(f.service, owner, friend);
  assert.deepEqual(read(), { ids: [b.accountId], has: true });
});

test('host current-contact views expire and cannot be retained, widened or nested for another owner', async t => {
  const f = fixture(t), owner = identity(), friend = identity();
  const a = await bootstrap(f.service, owner), b = await bootstrap(f.service, friend);
  await currentContactAccept(f.service, owner, friend);
  let retained;
  const result = f.service.withAuthorityFence(() => f.service.withCurrentContacts(Object.freeze({ accountId: a.accountId, deviceId: a.deviceId }), contacts => {
    retained = contacts; assert.ok(Object.isFrozen(contacts)); assert.ok(Object.isFrozen(contacts.accountIds()));
    assert.deepEqual(Object.keys(contacts).sort(), ['accountId', 'accountIds', 'deviceId', 'has']);
    assert.throws(() => f.service.withCurrentContacts(b, () => assert.fail()), code('connect_authority_actor_mismatch'));
    return f.service.withCurrentContacts(a, nested => { assert.equal(nested, contacts); return nested.has(b.accountId); });
  }));
  assert.equal(result, true);
  assert.throws(() => retained.has(b.accountId), code('connect_contact_authority_expired'));
  assert.throws(() => retained.accountIds(), code('connect_contact_authority_expired'));
  assert.throws(() => f.service.withCurrentContacts({ accountId: b.accountId, deviceId: a.deviceId }, () => assert.fail()), code('connect_authority_actor_mismatch'));
  assert.equal(f.service.withCurrentContacts(a, contacts => contacts.has(b.accountId)), true);
});

test('current-contact coordinator joins the signed actor and rejects a foreign actor even under the host witness', async t => {
  let service; const retained = new Map(); let peerAccountId;
  const extension = { operations: new Set(['test.currentContacts']), execute({ actor, args }) {
    retained.set(actor.deviceId, actor);
    if (args.foreignDeviceId) {
      const foreign = retained.get(args.foreignDeviceId);
      assert.ok(foreign);
      assert.throws(() => service.withCurrentContacts(foreign, () => assert.fail()), code('connect_authority_actor_mismatch'));
      assert.throws(() => service.withCurrentContacts({ accountId: foreign.accountId, deviceId: foreign.deviceId }, () => assert.fail()), code('connect_authority_actor_mismatch'));
      return { denied: true };
    }
    assert.throws(() => service.withActorAuthorityFence({ ...actor }, () => assert.fail()), code('connect_authority_actor_invalid'));
    return { friend: service.withActorAuthorityFence(actor, () => service.withCurrentContacts(actor, contacts => contacts.has(peerAccountId))) };
  } };
  const f = fixture(t, [extension]); service = f.service;
  const owner = identity(), friend = identity(), foreign = identity();
  const a = await bootstrap(service, owner), b = await bootstrap(service, friend); await bootstrap(service, foreign);
  peerAccountId = b.accountId; await currentContactAccept(service, owner, friend);
  assert.equal((await call(service, owner, 'test.currentContacts', {})).friend, true);
  assert.equal((await call(service, foreign, 'test.currentContacts', { foreignDeviceId: a.deviceId })).denied, true);
  service.withActorAuthorityFence(retained.get(a.deviceId), () => {
    assert.equal(service.withCurrentContacts(retained.get(a.deviceId), contacts => contacts.has(b.accountId)), true);
    assert.throws(() => service.withCurrentContacts(b, () => assert.fail()), code('connect_authority_actor_mismatch'),
      'the live generic host witness must never override the original retained issued actor');
  });
  service.withAuthorityFence(() => {
    assert.equal(service.withCurrentContacts(Object.freeze({ accountId: a.accountId, deviceId: a.deviceId }), contacts => contacts.has(b.accountId)), true,
      'a freshly validated host session DTO need not be an issued Connect object');
    assert.throws(() => service.withActorAuthorityFence({ ...retained.get(a.deviceId) }, () => assert.fail()), code('connect_authority_actor_invalid'));
  });
});

test('current-contact callbacks reject async work and errors clear only their own contact frame', async t => {
  const f = fixture(t), owner = identity(), friend = identity();
  const a = await bootstrap(f.service, owner), b = await bootstrap(f.service, friend);
  await currentContactAccept(f.service, owner, friend);
  let invoked = 0, late;
  assert.throws(() => f.service.withCurrentContacts(a, async () => { invoked++; }), code('connect_authority_callback_invalid'));
  assert.equal(invoked, 0);
  assert.throws(() => f.service.withCurrentContacts(a, contacts => {
    late = Promise.resolve().then(() => contacts.has(b.accountId)); return late;
  }), code('connect_authority_callback_async'));
  await assert.rejects(late, code('connect_contact_authority_expired'));
  const expected = new Error('synthetic current-contact failure');
  assert.throws(() => f.service.withAuthorityFence(() => f.service.withCurrentContacts(a, () => { throw expected; })), error => error === expected);
  assert.equal(f.service.withCurrentContacts(a, contacts => contacts.has(b.accountId)), true);
  f.service.withCurrentContacts(a, contacts => {
    assert.throws(() => f.service.withCurrentContacts(a, () => { throw expected; }), error => error === expected);
    assert.equal(contacts.has(b.accountId), true, 'a failed nested callback cannot expire its parent observation');
  });
  f.service.close();
  assert.throws(() => f.service.withCurrentContacts(a, () => { invoked++; }), code('service_closed'));
  assert.equal(invoked, 0);
});

test('current contacts reload from the real store and reject a revoked or reassigned installation', async t => {
  const f = fixture(t), owner = identity(), friend = identity(), phone = identity(), foreign = identity();
  const a = await bootstrap(f.service, owner), b = await bootstrap(f.service, friend), c = await bootstrap(f.service, foreign);
  const mobile = await enroll(f.service, owner, phone); await currentContactAccept(f.service, owner, friend);
  const reloaded = createConnectService({ databasePath: f.databasePath, projectId: PROJECT, allowedOrigins: [ORIGIN] });
  try {
    assert.equal(reloaded.withCurrentContacts(a, contacts => contacts.has(b.accountId)), true);
    await call(f.service, owner, 'device.revoke', { deviceId: mobile.deviceId });
    assert.throws(() => reloaded.withCurrentContacts(mobile, () => assert.fail()), code('device_revoked'));
    const db = new DatabaseSync(f.databasePath);
    try { db.prepare('UPDATE installations SET account_id=? WHERE id=?').run(c.accountId, a.deviceId); } finally { db.close(); }
    assert.throws(() => reloaded.withCurrentContacts(a, () => assert.fail()), code('connect_authority_actor_mismatch'));
  } finally { reloaded.close(); }
});

test('current-contact observation excludes a real signed relationship removal until it has ended', async t => {
  const f = fixture(t), owner = identity(), friend = identity();
  const a = await bootstrap(f.service, owner), b = await bootstrap(f.service, friend);
  const relation = await currentContactAccept(f.service, owner, friend);
  const request = await signed(f.service, friend, 'contacts.remove', { relationshipId: relation.relationshipId });
  const peer = await f.worker('revoke'), completion = message(peer.worker, 'result');
  const result = f.service.withCurrentContacts(a, contacts => {
    peer.worker.postMessage(request);
    if (Atomics.load(peer.flags, 0) === 0) Atomics.wait(peer.flags, 0, 0, 2000);
    assert.equal(Atomics.load(peer.flags, 0), 1, 'the separate Connect writer attempted the actual signed removal');
    assert.equal(Atomics.wait(peer.flags, 1, 0, 100), 'timed-out');
    assert.equal(contacts.has(b.accountId), true); return contacts.accountIds();
  });
  assert.deepEqual(result, [b.accountId]); good((await completion).result);
  assert.equal(f.service.withCurrentContacts(a, contacts => contacts.has(b.accountId)), false);
});


test('real capability owner and snapshot DTOs join the host fence without becoming issued Connect actors', async t => {
  const { createCapabilitiesService } = await import('../../capabilities/server/index.mjs');
  const f = fixture(t), owner = identity(), friend = identity();
  const a = await bootstrap(f.service, owner), b = await bootstrap(f.service, friend);
  await currentContactAccept(f.service, owner, friend);
  const caps = createCapabilitiesService({ databasePath: path.join(path.dirname(f.databasePath), 'caps.sqlite'),
    projectId: PROJECT, actorActive: actor => f.service.isActorActive(actor) });
  try {
    const capCall = (op, args) => caps.execute({ op: 'access.' + op, actor: a, args: { expectedAccountId: a.accountId, ...args } });
    const principal = capCall('principals.create', { label: 'Synthetic existing agent' }).principal;
    const grant = capCall('grants.issue', { principalId: principal.id, capabilities: [{ capabilityId: 'notes.createDraft', version: 1 }],
      resources: ['notes:new'], effects: ['create'], recipients: ['soty:notes'], expiresAt: Date.now() + 600000,
      allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit: 2 } }).grant;
    const issued = capCall('credentials.issue', { grantId: grant.id, audience: ORIGIN });
    const actor = caps.authenticateCredential({ token: issued.token, audience: ORIGIN });
    const authorization = caps.authorize({ actor, capabilityId: 'notes.createDraft', version: 1, action: 'read' });
    let observed = 0, retained;
    const read = subject => {
      observed++; assert.ok(Object.isFrozen(subject)); assert.equal(subject.accountId, a.accountId); assert.equal(subject.deviceId, a.deviceId);
      assert.throws(() => f.service.withActorAuthorityFence(subject, () => assert.fail()), code('connect_authority_actor_invalid'));
      return f.service.withCurrentContacts(subject, contacts => { retained = contacts; return contacts.has(b.accountId); });
    };
    assert.equal(f.service.withAuthorityFence(() => caps.withOwnerAuthority({ actor }, read)), true);
    assert.equal(f.service.withAuthorityFence(() => caps.withSnapshotOwnerAuthority({ authorization }, read)), true);
    assert.equal(observed, 2);
    assert.throws(() => retained.has(b.accountId), code('connect_contact_authority_expired'));
    capCall('grants.revoke', { grantId: grant.id });
    assert.throws(() => f.service.withAuthorityFence(() => caps.withOwnerAuthority({ actor }, read)), code('access_denied'));
    assert.throws(() => f.service.withAuthorityFence(() => caps.withSnapshotOwnerAuthority({ authorization }, read)), code('access_denied'));
    assert.equal(observed, 2, 'revoked capability authority never enters the contact callback');
  } finally { caps.close(); }
});
