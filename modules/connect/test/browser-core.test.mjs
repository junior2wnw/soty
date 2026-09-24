import test from 'node:test';
import assert from 'node:assert/strict';
import { createClientWithStorage } from '../browser/client.mjs';
import {
  ConnectError, canonicalJson, createInstallation, cryptoApi, sha256, base64url, unbase64url,
  wrapEnrollmentRoot, unwrapEnrollmentRoot, sealLocalRoot, openLocalRoot, verifyInstallation,
} from '../browser/crypto.mjs';
import { validateState } from '../browser/storage.mjs';
import { createConnectService, canonicalJson as serverCanonicalJson, digestArgs } from '../server/index.mjs';

const projectId = 'connect-browser-test';
const origin = 'https://browser.test';
const endpoint = origin + '/rpc';
const scope = { projectId, endpoint };

/** Synchronous atomic commit models storage transactions; it does not claim IndexedDB coverage. */
function memoryStorage(initial = null) {
  let value = initial === null ? null : structuredClone(initial);
  return {
    writes: 0,
    readError: null,
    writeError: null,
    read() {
      if (this.readError) throw this.readError;
      return Promise.resolve(value === null ? null : structuredClone(value));
    },
    claim(candidate) {
      if (this.writeError) throw this.writeError;
      if (value === null) { validateState(candidate, scope); value = structuredClone(candidate); this.writes++; }
      return Promise.resolve(structuredClone(value));
    },
    compareAndSwap(expectedRevision, candidate) {
      if (this.writeError) throw this.writeError;
      if (value?.localRevision !== expectedRevision) throw new ConnectError('LOCAL_CONFLICT', 'Concurrent write');
      validateState(candidate, scope); value = structuredClone(candidate); this.writes++;
      return Promise.resolve(structuredClone(value));
    },
    inspect() { return value === null ? null : structuredClone(value); },
    corrupt(mutator) { mutator(value); },
  };
}

function fixture(t) {
  const service = createConnectService({ databasePath: ':memory:', projectId, allowedOrigins: [origin] });
  t.after(() => service.close());
  const traffic = [];
  let responseMutation = null, dropOperation = null;
  const fetcher = async (_url, options) => {
    const body = JSON.parse(options.body);
    assert.equal(body.protocol, 1, 'browser transport must declare the HTTP protocol version');
    traffic.push(structuredClone(body));
    let result = await service.handle({ ...body, origin });
    if (responseMutation) result = await responseMutation(body, result) || result;
    if (dropOperation === body.op && result.ok) { dropOperation = null; throw new TypeError('Network response lost after commit'); }
    return new Response(JSON.stringify(result), { status: result.ok ? 200 : 400, headers: { 'Content-Type': 'application/json' } });
  };
  return {
    service, traffic, fetcher,
    client(store = memoryStorage(), extra = {}) { return { store, client: createClientWithStorage({ projectId, endpoint, fetch: fetcher, ...extra }, store) }; },
    mutateResponse(fn) { responseMutation = fn; },
    loseResponse(op) { dropOperation = op; },
  };
}
async function finishConfirmed(client, requestId) {
  const preview = await client.previewEnrollment(requestId);
  assert.ok(preview.account);
  return client.finishEnrollment(requestId, preview.account.accountId);
}

test('browser/server canonical JSON, digest, key identity and native nonextractable keys agree', async t => {
  const value = { z: ['Ж', -0, true, null], a: { b: 2, a: 1 } };
  assert.equal(canonicalJson(value), serverCanonicalJson(value));
  assert.equal(await sha256(canonicalJson(value)), digestArgs(value));
  for (const invalid of [{ x: undefined }, [NaN], new Date(), [Infinity], [1, , 2]]) assert.throws(() => canonicalJson(invalid));
  const a = await createInstallation('A'), b = await createInstallation('B');
  assert.notEqual(a.deviceId, b.deviceId);
  assert.notDeepEqual(a.encryptionPublicJwk, b.encryptionPublicJwk);
  for (const key of [a.signingPrivateKey, a.encryptionPrivateKey, a.storageKey]) {
    assert.equal(key.extractable, false);
    await assert.rejects(cryptoApi().subtle.exportKey('jwk', key));
  }
  await verifyInstallation(a);
  const f = fixture(t), { client } = f.client();
  const result = await client.bootstrap('Браузер');
  assert.match(result.deviceId, /^dev_/);
  assert.equal((await client.status()).accountId, result.accountId);
});

test('concurrent empty tabs atomically claim the same installation and root before bootstrap', async t => {
  const f = fixture(t), store = memoryStorage();
  const a = f.client(store).client, b = f.client(store).client;
  const results = await Promise.all([a.bootstrap('Первый'), b.bootstrap('Второй')]);
  assert.equal(results[0].accountId, results[1].accountId);
  assert.equal(results[0].deviceId, results[1].deviceId);
  assert.equal(store.inspect().installations.length, 1);
  await a.saveVault({ work: 'на месте' });
  assert.deepEqual((await b.loadVault()).payload, { work: 'на месте' });
});

test('storage read/write failures and future/corrupt schemas never create replacement accounts', async t => {
  const f = fixture(t), { store, client } = f.client();
  store.readError = new ConnectError('STORAGE_READ_FAILED', 'unavailable');
  await assert.rejects(client.bootstrap('A'), { code: 'STORAGE_READ_FAILED' });
  assert.equal(store.writes, 0); assert.equal(f.traffic.length, 0);
  store.readError = null; store.writeError = new ConnectError('STORAGE_ABORTED', 'abort');
  await assert.rejects(client.bootstrap('A'), { code: 'STORAGE_ABORTED' });
  assert.equal(f.traffic.length, 0);
  store.writeError = null; await client.bootstrap('A');
  const before = f.traffic.length;
  store.corrupt(state => { state.schema = 'connect.browser-state.v99'; });
  await assert.rejects(client.bootstrap('A'), { code: 'UNSUPPORTED_LOCAL_STATE' });
  assert.equal(f.traffic.length, before);
  store.corrupt(state => { state.schema = 'connect.browser-state.v1'; state.installations[0].rootEnvelope = null; });
  await assert.rejects(client.bootstrap('A'), { code: 'CORRUPT_LOCAL_STATE' });
  assert.equal(f.traffic.length, before);
});

test('a corrupt local root or mismatched saved key pair fails before network bootstrap', async t => {
  const f = fixture(t), { store, client } = f.client();
  await client.bootstrap('A');
  const before = f.traffic.length;
  const good = store.inspect();
  store.corrupt(state => { state.installations[0].rootEnvelope.ciphertext = base64url(new Uint8Array(48)); });
  await assert.rejects(client.bootstrap('A'), { code: 'DECRYPT_FAILED' });
  assert.equal(f.traffic.length, before);
  const other = await createInstallation('Other');
  const corrupt = memoryStorage(good);
  corrupt.corrupt(state => { state.installations[0].encryptionPrivateKey = other.encryptionPrivateKey; });
  await assert.rejects(f.client(corrupt).client.status(), { code: 'CORRUPT_LOCAL_STATE' });
  assert.equal(f.traffic.length, before);
});

test('reader preserves additive extension data and refuses a future reader epoch', async t => {
  const f = fixture(t), { store, client } = f.client();
  await client.bootstrap('A');
  store.corrupt(state => { state.extensions = { futureOptionalFeature: { values: ['preserve', 1], enabled: true } }; });
  await client.saveVault({ saved: true });
  assert.deepEqual(store.inspect().extensions, { futureOptionalFeature: { values: ['preserve', 1], enabled: true } });
  const before = f.traffic.length;
  store.corrupt(state => { state.minReaderEpoch = 2; });
  await assert.rejects(client.bootstrap('A'), { code: 'UNSUPPORTED_LOCAL_STATE' });
  assert.equal(f.traffic.length, before);
});

test('a relabelled account or wrong proof challenge cannot silently change local account identity', async t => {
  const f = fixture(t), { client } = f.client();
  const initial = await client.bootstrap('A');
  f.mutateResponse((body, result) => body.op === 'bootstrap' && result.ok ? { ...result, accountId: 'acc_attacker' } : result);
  await assert.rejects(client.bootstrap('A'), { code: 'ACCOUNT_MISMATCH' });
  assert.equal((await client.getLocalState()).accountId, initial.accountId);
  f.mutateResponse((body, result) => body.op === 'challenge' && result.ok ? { ...result, message: canonicalJson({ ...JSON.parse(result.message), operation: 'recovery.use' }) } : result);
  const requests = f.traffic.length;
  await assert.rejects(client.status(), { code: 'INVALID_CHALLENGE' });
  assert.equal(f.traffic.length, requests + 1); // No signature was sent.
});

test('enrollment creates new keys, preserves prior profile, transfers only the encrypted root, and is retryable', async t => {
  const f = fixture(t), a = f.client(), b = f.client();
  const first = await a.client.bootstrap('Анна');
  await a.client.saveVault({ songs: ['песня'], draft: 'личное' });
  const previous = await b.client.bootstrap('Другой профиль');
  await b.client.saveVault({ draft: 'остался здесь' });
  const request = await b.client.startEnrollment('Ноутбук');
  assert.equal((await b.client.getLocalState()).accountId, previous.accountId);
  const inspected = await a.client.inspectEnrollment(request.requestId);
  assert.notEqual(inspected.deviceId, first.deviceId);
  assert.notEqual(inspected.deviceId, previous.deviceId);
  f.loseResponse('enrollment.approve');
  await assert.rejects(a.client.approveEnrollment(request.requestId), { code: 'NETWORK_ERROR' });
  await f.client(a.store).client.approveEnrollment(request.requestId);
  const preview = await b.client.previewEnrollment(request.requestId);
  assert.equal(preview.account.accountId, first.accountId);
  assert.equal(preview.account.label, 'Анна');
  assert.equal(preview.source.deviceId, first.deviceId);
  assert.equal((await b.client.getLocalState()).accountId, previous.accountId);
  f.loseResponse('enrollment.finish');
  await assert.rejects(b.client.finishEnrollment(request.requestId, preview.account.accountId), { code: 'NETWORK_ERROR' });
  assert.equal((await b.client.getLocalState()).accountId, previous.accountId);
  const reloaded = f.client(b.store).client;
  const second = await finishConfirmed(reloaded, request.requestId);
  assert.equal(second.accountId, first.accountId);
  assert.notEqual(second.deviceId, first.deviceId);
  assert.equal((await reloaded.getLocalState()).profiles.length, 2);
  assert.deepEqual((await reloaded.loadVault()).payload, { songs: ['песня'], draft: 'личное' });
  assert.equal((await reloaded.finishEnrollment(request.requestId, first.accountId)).alreadyCompleted, true);
  await reloaded.switchProfile(previous.accountId);
  assert.deepEqual((await reloaded.loadVault()).payload, { draft: 'остался здесь' });
  const approve = f.traffic.find(request => request.op === 'enrollment.approve');
  assert.equal(JSON.stringify(approve).includes('privateKey'), false);
  assert.equal(JSON.stringify(approve).includes('личное'), false);
});

test('a recipient previews an unexpected approving account without activating it or exposing its current data', async t => {
  const f = fixture(t), attacker = f.client().client, recipient = f.client().client;
  const wrong = await attacker.bootstrap('Незнакомый профиль');
  const current = await recipient.bootstrap('Мой профиль');
  await recipient.saveVault({ private: 'stays with my account' });
  const request = await recipient.startEnrollment('Мой экран');
  assert.equal((await recipient.previewEnrollment(request.requestId)).status, 'pending');
  await attacker.approveEnrollment(request.requestId);
  await assert.rejects(recipient.finishEnrollment(request.requestId, wrong.accountId), { code: 'ENROLLMENT_PREVIEW_REQUIRED' });
  const preview = await recipient.previewEnrollment(request.requestId);
  assert.equal(preview.account.accountId, wrong.accountId);
  assert.equal(preview.source.deviceId, wrong.deviceId);
  assert.equal(Object.hasOwn(preview, 'wrappedKey'), false);
  assert.equal((await recipient.getLocalState()).accountId, current.accountId);
  const before = f.traffic.length;
  await assert.rejects(recipient.finishEnrollment(request.requestId, current.accountId), { code: 'ENROLLMENT_PREVIEW_REQUIRED' });
  assert.equal(f.traffic.length, before);
  assert.deepEqual((await recipient.loadVault()).payload, { private: 'stays with my account' });
  assert.equal((await attacker.loadVault()).payload, null);
});

test('profile changes before approval, snapshot upload and after recipient preview cannot cross account boundaries', async t => {
  const f = fixture(t), source = f.client().client, additional = f.client().client, recipient = f.client(), candidate = f.client().client;
  const sourceAccount = await source.bootstrap('Source');
  const first = await recipient.client.bootstrap('First local');
  const second = await additional.bootstrap('Second local');
  const saved = await recipient.client.startEnrollment('Second here');
  await additional.approveEnrollment(saved.requestId); await finishConfirmed(recipient.client, saved.requestId);
  await recipient.client.switchProfile(first.accountId);
  const otherTab = f.client(recipient.store).client;
  const outgoing = await candidate.startEnrollment('Candidate');
  const shown = await recipient.client.inspectEnrollment(outgoing.requestId);
  await otherTab.switchProfile(second.accountId);
  let before = f.traffic.length;
  await assert.rejects(recipient.client.approveEnrollment(outgoing.requestId, shown.accountId), { code: 'ACTIVE_PROFILE_CHANGED' });
  await assert.rejects(recipient.client.saveVault({ private: 'First snapshot' }, undefined, first.accountId), { code: 'ACTIVE_PROFILE_CHANGED' });
  await assert.rejects(recipient.client.loadVault(first.accountId), { code: 'ACTIVE_PROFILE_CHANGED' });
  assert.equal(f.traffic.length, before);
  assert.equal((await additional.loadVault()).payload, null);

  await otherTab.switchProfile(first.accountId);
  const incoming = await recipient.client.startEnrollment('Source here');
  await source.approveEnrollment(incoming.requestId);
  await recipient.client.previewEnrollment(incoming.requestId);
  await otherTab.switchProfile(second.accountId);
  before = f.traffic.length;
  await assert.rejects(recipient.client.finishEnrollment(incoming.requestId, sourceAccount.accountId), { code: 'ACTIVE_PROFILE_CHANGED' });
  assert.equal(f.traffic.length, before);

  await recipient.client.previewEnrollment(incoming.requestId);
  let release, started;
  const waiting = new Promise(resolve => { started = resolve; });
  const barrier = new Promise(resolve => { release = resolve; });
  f.mutateResponse(async (body, result) => { if (body.op === 'enrollment.finish') { started(); await barrier; } return result; });
  const rejected = assert.rejects(recipient.client.finishEnrollment(incoming.requestId, sourceAccount.accountId), { code: 'ACTIVE_PROFILE_CHANGED' });
  await waiting; await otherTab.switchProfile(first.accountId); release(); await rejected;
  assert.equal((await recipient.client.getLocalState()).accountId, first.accountId);
  assert.equal((await recipient.client.getLocalState()).pendingEnrollment.requestId, incoming.requestId);
  f.mutateResponse(null);
  assert.equal((await recipient.client.previewEnrollment(incoming.requestId)).status, 'finished');
  await recipient.client.finishEnrollment(incoming.requestId, sourceAccount.accountId);
  assert.equal((await recipient.client.getLocalState()).profiles.length, 3);
  assert.equal((await recipient.client.getLocalState()).accountId, sourceAccount.accountId);
});

test('expired local enrollment requires an explicit restart and retains existing profiles and keys', async t => {
  const f = fixture(t), { client, store } = f.client();
  const current = await client.bootstrap('Current');
  const expired = await client.startEnrollment('Screen');
  store.corrupt(state => { state.pendingEnrollment.expiresAt = Date.now() - 1; });
  const before = f.traffic.length;
  await assert.rejects(client.startEnrollment('Screen'), { code: 'ENROLLMENT_EXPIRED' });
  assert.equal(f.traffic.length, before);
  await client.discardPendingEnrollment();
  const next = await client.startEnrollment('Screen');
  assert.notEqual(next.requestId, expired.requestId);
  assert.equal((await client.getLocalState()).accountId, current.accountId);
  assert.equal(store.inspect().installations.length, 3);
});

test('ECDH transfer rejects wrong project, account, request and recipient bindings', async () => {
  const recipient = await createInstallation('Recipient'), other = await createInstallation('Other');
  const raw = cryptoApi().getRandomValues(new Uint8Array(32));
  const binding = { projectId, accountId: 'acc_source', requestId: 'en_request', encryptionPublicJwk: recipient.encryptionPublicJwk };
  const envelope = await wrapEnrollmentRoot(raw, binding);
  assert.deepEqual(await unwrapEnrollmentRoot(envelope, recipient, binding), raw);
  for (const changed of [{ ...binding, projectId: 'wrong' }, { ...binding, accountId: 'wrong' }, { ...binding, requestId: 'wrong' }]) {
    await assert.rejects(unwrapEnrollmentRoot(envelope, recipient, changed));
  }
  await assert.rejects(unwrapEnrollmentRoot(envelope, other, binding));
  const local = await sealLocalRoot(recipient, projectId, raw);
  assert.deepEqual(await openLocalRoot({ ...recipient, rootEnvelope: local }, projectId), raw);
  await assert.rejects(openLocalRoot({ ...recipient, rootEnvelope: local }, 'wrong'));
});

test('backup CAS rejects stale writers and authenticated envelopes reject account/revision relabelling', async t => {
  const f = fixture(t), a = f.client();
  await a.client.bootstrap('A');
  await a.client.saveVault({ version: 1 });
  const stale = f.client(memoryStorage(a.store.inspect()));
  await a.client.saveVault({ version: 2 });
  await assert.rejects(stale.client.saveVault({ version: 'lost update' }), { code: 'revision_conflict' });
  assert.deepEqual(await stale.client.loadVault(), { revision: 2, payload: { version: 2 } });
  f.mutateResponse((body, result) => body.op === 'vault.get' ? { ...result, revision: result.revision + 1 } : result);
  await assert.rejects(a.client.loadVault(), { code: 'ENVELOPE_MISMATCH' });
  assert.equal((await a.client.getLocalState()).current.vaultRevision, 2);
  f.mutateResponse((body, result) => body.op === 'vault.get' ? { ...result, revision: 0, envelope: null } : result);
  await assert.rejects(a.client.loadVault(), { code: 'VAULT_ROLLBACK' });
});

test('recovery kit opens only its account key and loses no old profile on one-use network retry', async t => {
  const f = fixture(t), source = f.client(), receiver = f.client();
  const account = await source.client.bootstrap('Source');
  await source.client.saveVault({ draft: 'recovered private content' });
  const kit = await source.client.prepareRecovery();
  assert.equal((await source.client.status()).recovery.verified, false);
  assert.equal(JSON.stringify(kit).includes('privateKey'), false);
  assert.equal(JSON.stringify(kit).includes('signing'), false);
  assert.equal(kit.accountId, account.accountId);
  await source.client.confirmRecovery(kit);
  assert.equal((await source.client.status()).recovery.verified, true);
  const previous = await receiver.client.bootstrap('Existing');
  const before = f.traffic.length;
  await assert.rejects(receiver.client.recover({ ...kit, secret: base64url(new Uint8Array(32)) }, 'Recovered'), { code: 'DECRYPT_FAILED' });
  assert.equal(f.traffic.length, before);
  f.loseResponse('recovery.use');
  await assert.rejects(receiver.client.recover(kit, 'Recovered'), { code: 'NETWORK_ERROR' });
  assert.equal((await receiver.client.getLocalState()).accountId, previous.accountId);
  const client = f.client(receiver.store).client;
  const recovered = await client.recover(kit, 'Recovered');
  assert.equal(recovered.accountId, account.accountId);
  assert.notEqual(recovered.deviceId, account.deviceId);
  assert.deepEqual((await client.loadVault()).payload, { draft: 'recovered private content' });
  assert.equal((await client.getLocalState()).profiles.length, 2);
  assert.equal((await client.recover(kit, 'Recovered')).alreadyCompleted, true);
  assert.equal(JSON.stringify(await client.getLocalState()).includes(kit.secret), false);
  await assert.rejects(f.client().client.recover(kit, 'Attacker'));
});

test('unconfirmed replacement recovery does not deactivate the previously verified kit', async t => {
  const f = fixture(t), { client } = f.client();
  const original = await client.bootstrap('Original');
  const oldKit = await client.prepareRecovery(); await client.confirmRecovery(oldKit);
  await client.prepareRecovery();
  const other = f.client().client;
  assert.equal((await other.recover(oldKit, 'Replacement phone')).accountId, original.accountId);
});

test('contact scan never authenticates as the peer or silently accepts the reciprocal request', async t => {
  const f = fixture(t), a = f.client().client, b = f.client().client;
  const accountA = await a.bootstrap('Анна'), accountB = await b.bootstrap('Борис');
  const cardA = await a.card(), cardB = await b.card();
  const guest = f.client().client;
  assert.equal((await guest.resolveCard(cardA.cardId)).label, 'Анна');
  assert.equal((await guest.getLocalState()).current, null);
  const outgoing = await b.requestContact(cardA.cardId);
  await a.requestContact(cardB.cardId);
  assert.equal((await a.contacts()).contacts.length, 0);
  assert.equal((await b.contacts()).contacts.length, 0);
  const incoming = (await a.contacts()).requests.incoming.find(item => item.requestId === outgoing.requestId);
  const relationship = await a.acceptContact(incoming.requestId);
  assert.equal((await b.contacts()).contacts[0].peerAccountId, accountA.accountId);
  assert.equal((await a.getLocalState()).accountId, accountA.accountId);
  assert.equal((await b.getLocalState()).accountId, accountB.accountId);
  await a.removeContact(relationship.relationshipId);
  assert.equal((await b.contacts()).contacts.length, 0);
  await a.blockContact(accountB.accountId);
  await assert.rejects(b.requestContact(cardA.cardId));
  await a.rotateCard(); await assert.rejects(guest.resolveCard(cardA.cardId));
});

test('device revocation cannot regenerate identity by bootstrap and does not revoke other devices', async t => {
  const f = fixture(t), a = f.client().client, b = f.client().client;
  const first = await a.bootstrap('A');
  const request = await b.startEnrollment('B'); await a.approveEnrollment(request.requestId);
  const second = await finishConfirmed(b, request.requestId);
  await b.revokeDevice(second.deviceId);
  await assert.rejects(b.bootstrap('B again'), { code: 'DEVICE_REVOKED' });
  assert.equal((await a.status()).accountId, first.accountId);
});

test('onState exposes safe committed state and observes errors without failing committed operations', async t => {
  const f = fixture(t), states = [];
  const { client, store } = f.client(memoryStorage(), { onState: state => { states.push(state); } });
  const initial = await client.bootstrap('A');
  const kit = await client.prepareRecovery();
  assert.equal(JSON.stringify(states).includes(kit.secret), false);
  assert.equal(JSON.stringify(states).includes('privateKey'), false);
  let reported;
  const errorReported = new Promise(resolve => { reported = resolve; });
  const failedObserver = f.client(store, {
    onState: async () => { throw new Error('observer failed'); },
    onError: error => { reported(error); },
  }).client;
  assert.equal((await failedObserver.bootstrap('A')).accountId, initial.accountId);
  assert.equal((await errorReported).code, 'NOTIFICATION_FAILED');
  assert.equal((await failedObserver.getLocalState()).notificationError.code, 'NOTIFICATION_FAILED');
  assert.equal((await client.getLocalState()).accountId, initial.accountId);
});

test('onState can await client reads without deadlocking the serialized operation queue', { timeout: 2_000 }, async t => {
  const f = fixture(t);
  let observed;
  const stateRead = new Promise(resolve => { observed = resolve; });
  const { client } = f.client(memoryStorage(), {
    onState: async state => {
      const status = await client.status();
      observed({ state, status });
    },
  });
  const initial = await client.bootstrap('A');
  const result = await stateRead;
  assert.equal(result.state.accountId, initial.accountId);
  assert.equal(result.status.accountId, initial.accountId);
});

test('a late vault response after another tab switches profile never reaches the new profile', async t => {
  const f = fixture(t), source = f.client().client, receiver = f.client();
  const accountA = await source.bootstrap('A'); await source.saveVault({ secret: 'Only account A' });
  const accountB = await receiver.client.bootstrap('B'); await receiver.client.saveVault({ secret: 'Only account B' });
  const request = await receiver.client.startEnrollment('A on another device');
  await source.approveEnrollment(request.requestId); await finishConfirmed(receiver.client, request.requestId);
  assert.equal((await receiver.client.getLocalState()).accountId, accountA.accountId);
  let release, started;
  const waiting = new Promise(resolve => { started = resolve; });
  const barrier = new Promise(resolve => { release = resolve; });
  f.mutateResponse(async (body, result) => {
    if (body.op === 'vault.get') { started(); await barrier; }
    return result;
  });
  const pending = receiver.client.loadVault();
  // Attach rejection handling before allowing the deliberately delayed response through.
  const rejected = assert.rejects(pending, { code: 'ACTIVE_PROFILE_CHANGED' });
  await waiting;
  const otherTab = f.client(receiver.store).client;
  await otherTab.switchProfile(accountB.accountId);
  release(); await rejected;
  assert.equal((await receiver.client.getLocalState()).accountId, accountB.accountId);
  f.mutateResponse(null);
  assert.deepEqual((await receiver.client.loadVault()).payload, { secret: 'Only account B' });
});

test('cross-tab change notifications contain only revisions and re-read durable state', async t => {
  const f = fixture(t), store = memoryStorage(), channels = new Set(), messages = [];
  const createChannel = name => {
    const channel = {
      name, onmessage: null,
      postMessage(data) {
        messages.push(structuredClone(data));
        for (const other of channels) if (other !== channel && other.name === name) queueMicrotask(() => other.onmessage?.({ data: structuredClone(data) }));
      },
      close() { channels.delete(channel); },
    };
    channels.add(channel); return channel;
  };
  let observed;
  const changed = new Promise(resolve => { observed = resolve; });
  const a = f.client(store, { createChannel }).client;
  const b = f.client(store, { createChannel, onState: state => { if (state.accountId) observed(state); } }).client;
  t.after(() => { a.dispose(); b.dispose(); });
  const account = await a.bootstrap('A');
  assert.equal((await changed).accountId, account.accountId);
  assert.deepEqual(Object.keys(messages[0]).sort(), ['localRevision', 'projectId', 'schema']);
  assert.equal(JSON.stringify(messages).includes(account.accountId), false);
  b.dispose(); await assert.rejects(b.getLocalState(), { code: 'CLIENT_DISPOSED' });
});
