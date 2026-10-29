import test from 'node:test';
import assert from 'node:assert/strict';
import { watchDeviceEnrollment } from '../ui/device-enrollment.mjs';
import { createClientWithStorage } from '../browser/client.mjs';
import { createConnectService } from '../server/index.mjs';
import { ConnectError } from '../browser/crypto.mjs';

const tick = () => new Promise(resolve => setImmediate(resolve));
async function until(check) { const end = Date.now() + 5000; while (!check()) { assert.ok(Date.now() < end, 'enrollment completed within its bound'); await tick(); } }
function deferred() { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; }
const request = { requestId: 'enroll_' + 'r'.repeat(32), expiresAt: Date.now() + 60_000 };
const approved = () => ({ ...request, status: 'approved', account: { accountId: 'previous-account', label: 'Мой аккаунт' } });
function fixture(t, change = {}) {
  let state = { accountId: 'automatic-account', deviceId: 'automatic-device', localRevision: 1, pendingEnrollment: null };
  const calls = { starts: 0, previews: 0, finishes: 0, errors: [], completed: [] };
  const client = { getLocalState: async () => structuredClone(state),
    async startEnrollment() { calls.starts++; state.pendingEnrollment = request; state.localRevision++; return request; },
    async previewEnrollment() { calls.previews++; return change.preview ? change.preview() : approved(); },
    async finishEnrollment(id, accountId, options) {
      calls.finishes++;
      if (change.finish) return change.finish(id, accountId, options);
      assert.equal(id, request.requestId); assert.equal(accountId, 'previous-account'); assert.equal(options.signal.aborted, false);
      state = { ...state, accountId, deviceId: 'new-device', localRevision: state.localRevision + 1 };
      return { accountId, deviceId: state.deviceId };
    },
  };
  const handle = watchDeviceEnrollment({ client, label: 'New device', isCurrent: () => true,
    onRequest: change.onRequest || (() => {}), onWaiting() {},
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
  assert.equal(f.calls.errors[0].code, 'ENROLLMENT_EXPIRED'); assert.equal(f.calls.starts, 1); assert.equal(f.calls.finishes, 0);
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
