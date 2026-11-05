import test from 'node:test';
import assert from 'node:assert/strict';
import { watchDeviceEnrollment } from '../ui/device-enrollment.mjs';
import { createClientWithStorage } from '../browser/client.mjs';
import { createConnectService } from '../server/index.mjs';
import { ConnectError } from '../browser/crypto.mjs';

const tick = () => new Promise(resolve => setImmediate(resolve));
async function until(check) { const end = Date.now() + 5000; while (!check()) { assert.ok(Date.now() < end, 'enrollment completed within its bound'); await tick(); } }
function deferred() { let resolve, reject; const promise = new Promise((done, fail) => { resolve = done; reject = fail; }); return { promise, resolve, reject }; }
const request = { requestId: 'enroll_' + 'r'.repeat(32), expiresAt: Date.now() + 60_000 };
const approved = () => ({ ...request, status: 'approved', account: { accountId: 'previous-account', label: 'Мой аккаунт' } });
function fixture(t, change = {}) {
  let state = { accountId: 'automatic-account', deviceId: 'automatic-device', localRevision: 1, pendingEnrollment: null, ...change.state };
  const calls = { starts: 0, previews: 0, finishes: 0, discards: 0, shown: [], errors: [], completed: [] };
  const client = { getLocalState: async () => structuredClone(state),
    async startEnrollment() { calls.starts++; const value = change.start ? change.start() : request; state.pendingEnrollment = value; state.localRevision++; return value; },
    async previewEnrollment(id) { calls.previews++; return change.preview ? change.preview(id) : approved(); },
    async discardPendingEnrollmentIfCurrent(id, expected, { signal }) {
      assert.equal(signal.aborted, false); assert.equal(id, state.pendingEnrollment.requestId);
      assert.equal(expected.localRevision, state.localRevision); assert.equal(expected.deviceId, state.deviceId);
      calls.discards++; state = { ...state, pendingEnrollment: null, localRevision: state.localRevision + 1 }; return structuredClone(state);
    },
    async finishEnrollment(id, accountId, options) {
      calls.finishes++;
      if (change.finish) return change.finish(id, accountId, options);
      assert.equal(id, request.requestId); assert.equal(accountId, 'previous-account'); assert.equal(options.signal.aborted, false);
      state = { ...state, accountId, deviceId: 'new-device', localRevision: state.localRevision + 1 };
      return { accountId, deviceId: state.deviceId };
    },
  };
  if (change.oldSdk) delete client.discardPendingEnrollmentIfCurrent;
  const handle = watchDeviceEnrollment({ client, label: 'New device', isCurrent: () => true,
    onRequest: value => { calls.shown.push(value.requestId); return change.onRequest?.(value); }, onWaiting() {},
    onComplete: result => calls.completed.push(result), onError: error => calls.errors.push(error) });
  t.after(() => handle.stop());
  return { handle, calls, state: () => state, update: values => { state = { ...state, ...values }; } };
}

test('the locally requested QR opens the approving account without any recipient-side action', async t => {
  const f = fixture(t); await until(() => f.calls.completed.length === 1);
  assert.equal(f.calls.starts, 1); assert.equal(f.calls.previews, 1); assert.equal(f.calls.finishes, 1);
  assert.equal(f.state().accountId, 'previous-account'); assert.equal(f.calls.errors.length, 0);
});

test('waiting is single-flight and never finishes an unapproved request', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const f = fixture(t, { preview: () => ({ ...request, status: 'pending', account: null }) });
  await until(() => f.calls.previews === 1); await tick();
  t.mock.timers.tick(1200); await until(() => f.calls.previews === 2); await tick();
  assert.equal(f.calls.finishes, 0); assert.equal(f.calls.errors.length, 0);
});

test('a transient network timeout checks the same QR again before finishing the approval', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] }); let attempts = 0;
  const f = fixture(t, { preview: () => { if (++attempts === 1) throw Object.assign(Error('Timeout'), { code: 'NETWORK_TIMEOUT' }); return approved(); } });
  await until(() => f.calls.previews === 1); await tick();
  assert.equal(f.calls.errors.length, 0); t.mock.timers.tick(1200);
  await until(() => f.calls.completed.length === 1);
  assert.equal(f.calls.starts, 1); assert.equal(f.calls.previews, 2); assert.equal(f.calls.finishes, 1);
});

test('closing the QR before a late approval arrives cannot open its account', async t => {
  const response = deferred(), f = fixture(t, { preview: () => response.promise });
  await until(() => f.calls.previews === 1); f.handle.stop(); response.resolve(approved());
  await tick(); await tick(); assert.equal(f.calls.finishes, 0); assert.equal(f.calls.completed.length, 0);
});

test('a changed revision catches an A to B to A transition while the approval read is pending', async t => {
  const response = deferred(), f = fixture(t, { preview: () => response.promise });
  await until(() => f.calls.previews === 1);
  f.update({ localRevision: f.state().localRevision + 2 }); response.resolve(approved());
  await until(() => f.calls.errors.length === 1);
  assert.equal(f.calls.errors[0].code, 'ACTIVE_PROFILE_CHANGED'); assert.equal(f.calls.finishes, 0);
});

test('an expired pending QR stops and is never silently replaced', async t => {
  const f = fixture(t, { preview: () => ({ ...request, expiresAt: Date.now() - 1, status: 'pending', account: null }) });
  await until(() => f.calls.errors.length === 1);
  assert.equal(f.calls.errors[0].code, 'ENROLLMENT_EXPIRED'); assert.equal(f.calls.starts, 1); assert.equal(f.calls.finishes, 0); assert.equal(f.calls.discards, 0);
});

const expiredRequest = () => ({ requestId: 'enroll_' + 'e'.repeat(32), expiresAt: Date.now() - 1 });
const expiredError = () => Object.assign(Error('Expired'), { code: 'enrollment_expired' });
test('reopening a known expired saved QR replaces it once and completes the fresh request', async t => {
  const saved = expiredRequest(), f = fixture(t, { state: { pendingEnrollment: saved },
    preview: id => { if (id === saved.requestId) throw expiredError(); return approved(); } });
  await until(() => f.calls.completed.length === 1 || f.calls.errors.length);
  assert.deepEqual(f.calls.errors, []); assert.equal(f.calls.discards, 1); assert.equal(f.calls.starts, 1);
  assert.equal(f.calls.shown.at(-1), request.requestId); assert.equal(f.calls.finishes, 1);
});

test('a QR first observed as valid is not automatically replaced when it expires while visible', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] }); let reads = 0;
  const f = fixture(t, { state: { pendingEnrollment: request }, preview: () => {
    if (++reads > 1) throw expiredError(); return { ...request, status: 'pending', account: null };
  } });
  await until(() => f.calls.previews === 1); await tick(); t.mock.timers.tick(1200);
  await until(() => f.calls.errors.length === 1);
  assert.equal(f.calls.errors[0].code, 'enrollment_expired'); assert.equal(f.calls.discards, 0); assert.equal(f.calls.starts, 0);
});

test('unknown saved preview outcomes retain the same pending QR without replacing it', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] }); const saved = expiredRequest();
  const f = fixture(t, { state: { pendingEnrollment: saved }, preview: () => { throw Object.assign(Error('Unknown'), { code: 'NETWORK_ERROR' }); } });
  await until(() => f.calls.previews === 1); await tick(); t.mock.timers.tick(1200);
  await until(() => f.calls.previews === 2); await tick();
  assert.equal(f.calls.starts, 0); assert.equal(f.calls.discards, 0); assert.equal(f.calls.finishes, 0);
  assert.equal(f.state().pendingEnrollment.requestId, saved.requestId); assert.deepEqual(f.calls.errors, []);
});

test('reopened expiry is renewed after a network timeout once the first authoritative preview arrives', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] }); const saved = expiredRequest(); let reads = 0;
  const f = fixture(t, { state: { pendingEnrollment: saved }, preview: id => {
    if (++reads === 1) throw Object.assign(Error('Timeout'), { code: 'NETWORK_TIMEOUT' });
    if (id === saved.requestId) throw expiredError(); return approved();
  } });
  await until(() => f.calls.previews === 1); await tick();
  assert.equal(f.calls.discards, 0); assert.equal(f.calls.starts, 0);
  t.mock.timers.tick(1200); await until(() => f.calls.completed.length || f.calls.errors.length);
  assert.deepEqual(f.calls.errors, []); assert.equal(f.calls.discards, 1); assert.equal(f.calls.starts, 1);
});

test('reopening a lost start acknowledgement reconciles its ID before a known expired request is replaced', async t => {
  const saved = expiredRequest(); let starts = 0;
  const f = fixture(t, { state: { pendingEnrollment: { requestId: null, expiresAt: null, label: 'New device' } },
    start: () => ++starts === 1 ? saved : request,
    preview: id => { if (id === saved.requestId) throw expiredError(); return approved(); } });
  await until(() => f.calls.completed.length || f.calls.errors.length);
  assert.deepEqual(f.calls.errors, []); assert.equal(f.calls.discards, 1); assert.equal(f.calls.starts, 2);
  assert.equal(f.calls.shown.at(-1), request.requestId);
});

for (const action of ['close', 'profile-switch']) test(`${action} during the saved expiry preview cannot discard or replace a request`, async t => {
  const response = deferred(), f = fixture(t, { state: { pendingEnrollment: expiredRequest() }, preview: () => response.promise });
  await until(() => f.calls.previews === 1);
  if (action === 'close') f.handle.stop(); else f.update({ localRevision: f.state().localRevision + 2 });
  response.reject(expiredError()); await tick(); await tick();
  assert.equal(f.calls.discards, 0); assert.equal(f.calls.starts, 0); assert.equal(f.calls.completed.length, 0);
  if (action === 'profile-switch') assert.equal(f.calls.errors[0].code, 'ACTIVE_PROFILE_CHANGED');
});

test('an older SDK without the conditional discard keeps the expired request for explicit user action', async t => {
  const saved = expiredRequest(), f = fixture(t, { state: { pendingEnrollment: saved }, oldSdk: true, preview: () => { throw expiredError(); } });
  await until(() => f.calls.errors.length === 1);
  assert.equal(f.calls.errors[0].code, 'enrollment_expired'); assert.equal(f.state().pendingEnrollment.requestId, saved.requestId);
  assert.equal(f.calls.starts, 0); assert.equal(f.calls.discards, 0);
});

test('a saved finished request is recovered even after QR expiry without starting another request', async t => {
  let state = { accountId: 'automatic-account', deviceId: 'automatic-device', localRevision: 7,
    pendingEnrollment: { ...request, expiresAt: Date.now() - 1 } };
  let starts = 0, completed;
  const client = { getLocalState: async () => state, startEnrollment: async () => { starts++; throw Error('must reuse saved request'); },
    previewEnrollment: async id => { assert.equal(id, request.requestId); return { ...approved(), expiresAt: Date.now() - 1, status: 'finished' }; },
    finishEnrollment: async () => { state = { ...state, accountId: 'previous-account', deviceId: 'recovered-device' }; return state; } };
  const handle = watchDeviceEnrollment({ client, label: 'New device', isCurrent: () => true, onRequest() {}, onWaiting() {},
    onComplete: value => { completed = value; }, onError: error => { throw error; } });
  t.after(() => handle.stop()); await until(() => completed);
  assert.equal(starts, 0); assert.equal(completed.accountId, 'previous-account');
});

test('closing during an in-flight finish aborts local activation and never reports completion', async t => {
  let signal; const response = deferred();
  const f = fixture(t, { finish: (_id, _account, options) => { signal = options.signal; return response.promise; } });
  await until(() => signal); f.handle.stop(); assert.equal(signal.aborted, true);
  response.resolve({ accountId: 'previous-account', deviceId: 'new-device' }); await tick(); await tick();
  assert.equal(f.calls.completed.length, 0);
});

function signedFixture(t) {
  const projectId = 'expiry-entry-real', origin = 'https://expiry.test', endpoint = origin + '/rpc';
  let now = Date.now();
  const service = createConnectService({ databasePath: ':memory:', projectId, allowedOrigins: [origin], clock: () => now });
  const clients = []; t.after(() => { clients.forEach(client => client.dispose()); service.close(); });
  // Atomic storage port with a held pre-commit boundary; not a native IndexedDB implementation.
  const storage = () => {
    let state = null, next;
    return { read: async () => structuredClone(state), claim: async value => structuredClone(state ||= value),
      holdNext() { next = { entered: deferred(), release: deferred(), done: deferred() }; return next; },
      async compareAndSwap(revision, value, options) {
        const held = next; next = null;
        if (held) { held.entered.resolve(options); await held.release.promise; }
        try {
          if (state.localRevision !== revision) throw new ConnectError('LOCAL_CONFLICT', 'Concurrent write');
          options?.beforeCommit?.(); state = structuredClone(value); return structuredClone(state);
        } finally { held?.done.resolve(); }
      },
    };
  };
  const client = store => {
    const value = createClientWithStorage({ projectId, endpoint, fetch: async (_url, options) => {
      const reply = await service.handle({ ...JSON.parse(options.body), origin });
      return new Response(JSON.stringify(reply), { status: reply.ok ? 200 : 400, headers: { 'Content-Type': 'application/json' } });
    } }, store); clients.push(value); return value;
  };
  return { storage, client, expire() { now += 300_001; } };
}

test('conditional SDK discard retains every key/profile and returns only its committed revision', async t => {
  const f = signedFixture(t), store = f.storage(), client = f.client(store);
  await client.bootstrap('Automatic'); const pending = await client.startEnrollment('New device');
  const snapshot = await client.getLocalState(), raw = await store.read();
  const clearing = client.discardPendingEnrollmentIfCurrent(pending.requestId, snapshot);
  snapshot.localRevision += 100; // SDK admission captures the displayed primitives, not a mutable caller object.
  const result = await clearing;
  assert.equal(result.localRevision, raw.localRevision + 1); assert.equal(result.pendingEnrollment, null);
  assert.deepEqual((await store.read()).installations, raw.installations);
});

test('genuine SDK CAS retry cannot discard a different request created by another tab', { timeout: 5000 }, async t => {
  const f = signedFixture(t), store = f.storage(), client = f.client(store), other = f.client(store);
  await client.bootstrap('Automatic'); const first = await client.startEnrollment('New device'), snapshot = await client.getLocalState();
  const held = store.holdNext(), clearing = client.discardPendingEnrollmentIfCurrent(first.requestId, snapshot);
  await held.entered.promise; await other.discardPendingEnrollment(); const replacement = await other.startEnrollment('New device');
  held.release.resolve(); await assert.rejects(clearing, { code: 'ACTIVE_PROFILE_CHANGED' });
  assert.notEqual(replacement.requestId, first.requestId);
  assert.equal((await client.getLocalState()).pendingEnrollment.requestId, replacement.requestId);
  assert.equal((await client.getLocalState()).accountId, snapshot.accountId);
});

test('closing a reopened expired QR before the SDK storage commit retains its pending request', { timeout: 5000 }, async t => {
  const f = signedFixture(t), store = f.storage(), client = f.client(store);
  await client.bootstrap('Automatic'); const old = await client.startEnrollment('New device'), before = await store.read(); f.expire();
  const held = store.holdNext(), errors = [], completed = [];
  const handle = watchDeviceEnrollment({ client, label: 'New device', isCurrent: () => true, onRequest() {}, onWaiting() {},
    onError: error => errors.push(error), onComplete: value => completed.push(value) }); t.after(() => handle.stop());
  const options = await held.entered.promise; handle.stop(); assert.equal(options.signal.aborted, true);
  held.release.resolve(); await held.done.promise; await tick(); await tick();
  assert.equal((await client.getLocalState()).pendingEnrollment.requestId, old.requestId);
  assert.deepEqual(await store.read(), before); assert.deepEqual(errors, []); assert.deepEqual(completed, []);
});

test('real authoritative expiry on reopen creates one fresh QR while preserving the original automatic profile', { timeout: 5000 }, async t => {
  const f = signedFixture(t), store = f.storage(), client = f.client(store);
  const automatic = await client.bootstrap('Automatic'), old = await client.startEnrollment('New device'); f.expire();
  const shown = [], errors = [];
  const handle = watchDeviceEnrollment({ client, label: 'New device', isCurrent: () => true,
    onRequest: value => shown.push(value), onWaiting() {}, onError: error => errors.push(error), onComplete() {} }); t.after(() => handle.stop());
  await until(() => shown.some(value => value.requestId !== old.requestId) || errors.length);
  assert.deepEqual(errors, []); assert.equal(shown.length, 2); assert.notEqual(shown[1].requestId, old.requestId);
  const state = await client.getLocalState(); assert.equal(state.accountId, automatic.accountId);
  assert.equal(state.pendingEnrollment.requestId, shown[1].requestId); assert.equal(state.profiles.length, 1);
});

test('real signed QR approval opens the same account on both devices and retains the automatic profile and its data', async t => {
  const projectId = 'device-entry-real', origin = 'https://device.test', endpoint = origin + '/rpc';
  const service = createConnectService({ databasePath: ':memory:', projectId, allowedOrigins: [origin] });
  const clients = []; t.after(() => { for (const client of clients) client.dispose(); service.close(); });
  const client = () => {
    let state = null;
    const storage = { read: async () => structuredClone(state), claim: async value => structuredClone(state ||= value),
      compareAndSwap: async (revision, value, options) => { if (state.localRevision !== revision) throw new ConnectError('LOCAL_CONFLICT', 'Conflict'); options?.beforeCommit?.(); return structuredClone(state = value); } };
    const value = createClientWithStorage({ projectId, endpoint, fetch: async (_url, options) => {
      const reply = await service.handle({ ...JSON.parse(options.body), origin });
      return new Response(JSON.stringify(reply), { status: reply.ok ? 200 : 400, headers: { 'Content-Type': 'application/json' } });
    } }, storage); clients.push(value); return value;
  };
  const old = client(), recipient = client(), account = await old.bootstrap('My original account');
  const automatic = await recipient.bootstrap('My automatic account');
  await old.saveVault({ marker: 'original data' }); await recipient.saveVault({ marker: 'automatic data' });
  let completed, failure;
  const handle = watchDeviceEnrollment({ client: recipient, label: 'New device', isCurrent: () => true,
    onRequest: async value => { await old.approveEnrollment(value.requestId, account.accountId); }, onWaiting() {},
    onComplete: result => { completed = result; }, onError: error => { failure = error; } });
  t.after(() => handle.stop()); await until(() => completed || failure); assert.equal(failure, undefined);
  assert.equal(completed.accountId, account.accountId); assert.notEqual(completed.deviceId, account.deviceId);
  assert.equal((await old.status()).accountId, account.accountId);
  assert.equal((await recipient.status()).accountId, account.accountId);
  assert.equal((await recipient.loadVault()).payload.marker, 'original data');
  assert.equal((await recipient.getLocalState()).profiles.length, 2);
  await recipient.switchProfile(automatic.accountId);
  assert.equal((await recipient.loadVault()).payload.marker, 'automatic data');
});
