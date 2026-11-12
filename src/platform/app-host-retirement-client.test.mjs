import test from 'node:test';
import assert from 'node:assert/strict';
import { createRootHostClient } from '../core/app-host-retirement-client.mjs';
import { createAppHostRetirement } from './app-host-retirement.mjs';

const owner = { accountId: 'account-A', deviceId: 'device-A' }, appId = 'app-' + 'a'.repeat(32);
const state = scope => ({ ...scope, current: { ...scope, revoked: false } });
const session = (revision = 1) => ({ schema: 'soty.app-host-session.v1', frameId: 'F'.repeat(43),
  lease: String.fromCharCode(64 + revision).repeat(43), revision, expiresAt: 86401000 });
const reply = (revision = 1) => ({ hostSession: session(revision), hostSessionExpiresAt: 100000, entry: { appId } });
const deferred = () => { let resolve; const promise = new Promise(yes => { resolve = yes; }); return { promise, resolve }; };
function fixture({ retirement, request = async () => reply(), initial = owner } = {}) {
  let selected = state(initial), cleanups = 0;
  const events = [], privateReplies = new WeakSet(), cleanup = async () => { cleanups++; events.push('cleanup'); };
  const raw = { getLocalState: async () => selected, dispose() { events.push('dispose'); },
    async extension(op, args, context) {
      events.push({ op, args, context });
      const value = await request(op, args, context);
      if (value && typeof value === 'object') privateReplies.add(value);
      return value;
    },
    appSlotCleanup: value => privateReplies.has(value) ? cleanup : null,
  };
  const root = createRootHostClient(raw, retirement ? { retirement } : {});
  root.observe(selected);
  return { ...root, events, raw, cleanups: () => cleanups,
    select(scope) { selected = state(scope); root.observe(selected); },
    changeDurableOnly(scope) { selected = state(scope); } };
}
const context = { expectedAccountId: owner.accountId };
const launch = f => f.client.extension('apps.launch', { appId, hostSession: true }, context);

test('managed launch persists a fixed obligation before returning to its iframe caller', async () => {
  const events = [], retirement = { async beforeLaunch(scope, current) { assert.deepEqual(scope, owner); assert(current()); events.push('before'); return true; },
    record(scope, value) { assert.deepEqual(scope, owner); assert.deepEqual(value, { appId, hostSession: session() }); events.push('record'); return () => events.push('retired'); } };
  const f = fixture({ retirement, request: async () => { events.push('rpc'); return reply(); } });
  const result = await launch(f); events.push('caller');
  assert.deepEqual(events, ['before', 'rpc', 'record', 'caller']);
  const cleanup = f.client.appSlotCleanup(result); assert.equal(cleanup, f.client.appSlotCleanup(result));
  assert.equal(f.client.appSlotCleanup(structuredClone(result)), null, 'a JSON copy acquires no cleanup capability');
  await cleanup(); assert.deepEqual(events, ['before', 'rpc', 'record', 'caller', 'retired']); assert.equal(f.cleanups(), 1);
});

test('lifecycle IO cannot observe caller mutations made after SDK argument capture', async () => {
  const gate = deferred();
  const retirement = { beforeLaunch: () => gate.promise, record: () => () => {} };
  const f = fixture({ retirement }), args = { appId, hostSession: true }, suppliedContext = { ...context };
  const pending = f.client.extension('apps.launch', args, suppliedContext);
  args.appId = 'app-' + 'b'.repeat(32); suppliedContext.expectedAccountId = 'other-account';
  gate.resolve(true); await pending;
  assert.equal(f.events[0].args.appId, appId); assert.equal(f.events[0].context.expectedAccountId, owner.accountId);
});

test('persist failure retires only the returned reservation and never exposes it for iframe admission', async () => {
  const failure = Object.assign(Error('private storage failed'), { code: 'app_host_retirement_unavailable' });
  const f = fixture({ retirement: { beforeLaunch: async () => false, record() { throw failure; } } });
  await assert.rejects(launch(f), error => error === failure); assert.equal(f.cleanups(), 1);
});

test('Source, external popups and unrelated parallel reads preserve their existing SDK path', async () => {
  let before = 0, records = 0;
  const source = { runtimeProfile: 'soty.selected-human-embed.v2', scopedCloseHandle: 'S'.repeat(43), entry: { appId } };
  const f = fixture({ retirement: { beforeLaunch: async () => { before++; return false; }, record() { records++; } }, request: async () => source });
  const a = await launch(f); assert.equal(a, source); assert.equal(f.client.appSlotCleanup(a), f.raw.appSlotCleanup(a));
  await f.client.extension('apps.launch', { appId }, context);
  await f.client.extension('apps.relay.identity', { appId }, context);
  assert.equal(before, 1); assert.equal(records, 0); assert.equal(f.events.length, 3);
});

test('a pending ordinary retirement cannot block a currently verified Source session', async () => {
  const pending = Object.assign(Error('unknown close'), { code: 'app_host_retirement_pending' });
  const source = { runtimeProfile: 'soty.selected-human-embed.v2', scopedCloseHandle: 'S'.repeat(43), entry: { appId } };
  const f = fixture({ retirement: { beforeLaunch: async () => { throw pending; }, record() { assert.fail('Source has no host proof'); } },
    request: async op => op === 'apps.authentication.get' ? { schema: 'soty.app-authentication-status.v1', appId, transport: 'source' } : source });
  assert.equal(await launch(f), source);
  assert.deepEqual(f.events.map(c => c.op), ['apps.authentication.get', 'apps.launch']);
});

test('transport diagnosis never bypasses an unknown host retirement or admits a changed Source target', async () => {
  for (const changedAfterRead of [false, true]) {
    const pending = Object.assign(Error('unknown close'), { code: 'app_host_retirement_pending' });
    const f = fixture({ retirement: { beforeLaunch: async () => { throw pending; }, record() { assert.fail('no new host admission'); } },
      request: async op => op === 'apps.authentication.get' ? { schema: 'soty.app-authentication-status.v1', appId, transport: changedAfterRead ? 'source' : 'relay' } : reply() });
    await assert.rejects(launch(f), error => error === pending);
    assert.equal(f.events.filter(c => c.op === 'apps.launch').length, changedAfterRead ? 1 : 0);
    assert.equal(f.cleanups(), changedAfterRead ? 1 : 0, 'a post-diagnosis ordinary reservation never reaches an iframe');
  }
});

test('a generation crossed A→B→A cannot persist or admit a late old-window reply', async () => {
  const gate = deferred(); let records = 0;
  const f = fixture({ retirement: { beforeLaunch: async () => true, record() { records++; return () => {}; } }, request: () => gate.promise });
  const pending = launch(f); await new Promise(resolve => setImmediate(resolve));
  f.select({ accountId: 'account-B', deviceId: 'device-B' }); f.select(owner); gate.resolve(reply());
  await assert.rejects(pending, { code: 'ACTIVE_PROFILE_CHANGED' }); assert.equal(records, 0); assert.equal(f.cleanups(), 1);
});

test('same-person profile display updates do not retire an otherwise current launch', async () => {
  const gate = deferred(); let records = 0;
  const f = fixture({ retirement: { beforeLaunch: async () => true, record() { records++; return () => {}; } }, request: () => gate.promise });
  const pending = launch(f); await new Promise(resolve => setImmediate(resolve));
  f.observe({ ...state(owner), label: 'Changed profile label' }); gate.resolve(reply());
  await pending; assert.equal(records, 1); assert.equal(f.cleanups(), 0);
});

test('a durable device or account change with a delayed observer cannot mislabel a newly signed reply', async () => {
  for (const [next, suppliedContext] of [[{ ...owner, deviceId: 'device-B' }, context],
    [{ accountId: 'account-B', deviceId: 'device-B' }, undefined]]) {
    let records = 0, f;
    f = fixture({ retirement: { beforeLaunch: async () => true, record() { records++; return () => {}; } },
      request: async () => { f.changeDurableOnly(next); return reply(); } });
    await assert.rejects(f.client.extension('apps.launch', { appId, hostSession: true }, suppliedContext), { code: 'ACTIVE_PROFILE_CHANGED' });
    assert.equal(records, 0); assert.equal(f.cleanups(), 1);
  }
});

test('a known renewal candidate is recorded before caller can send its confirm CAS', async () => {
  const seen = [], f = fixture({ retirement: { beforeLaunch: async () => true,
    record(scope, value) { seen.push(value.hostSession.revision); return () => {}; } },
    request: async op => op === 'apps.launch' ? reply() : reply(2) });
  await launch(f);
  await f.client.extension('apps.session.renew', { appId, frameId: session().frameId, lease: session().lease, revision: 1 }, context);
  assert.deepEqual(seen, [1, 2]);
  await f.client.extension('apps.session.confirm', { appId, frameId: session().frameId, lease: session(2).lease, revision: 2 }, context);
  assert.deepEqual(seen, [1, 2, 2]);
});

test('only a matching closed signed confirmation or current context may compact older known revisions', async () => {
  const flags = [], requestId = 'fixture-confirm-000001';
  let response = reply();
  const f = fixture({ retirement: { beforeLaunch: async () => true,
    record(_scope, _value, confirmed) { flags.push(confirmed); return () => {}; } }, request: async () => response });
  await launch(f);
  const args = { appId, frameId: session().frameId, lease: session(2).lease, revision: 2, requestId };
  response = { ...reply(2), schema: 'soty.app-host-session-confirmation.v1', confirmed: true, requestId };
  await f.client.extension('apps.session.confirm', args, context);
  response = { ...response, ready: true };
  await f.client.extension('apps.session.confirm', args, context);
  response = { ...reply(2), schema: 'soty.app-host-session-confirmation.v1', confirmed: true, requestId: 'different-confirm-0001' };
  await f.client.extension('apps.session.confirm', args, context);
  response = { ...reply(2), schema: 'soty.app-host-session-context.v1', state: 'current' };
  await f.client.extension('apps.session.context', args, context);
  response = { ...response, state: 'renew-required' };
  await f.client.extension('apps.session.context', args, context);
  assert.deepEqual(flags, [false, true, false, false, true, false]);
});

function port() {
  const values = new Map(), held = new Map();
  const storage = { getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value) };
  function locks() {
    const owned = [];
    return { request(name, options, callback) {
      assert.equal(options.ifAvailable, true); assert.equal(options.steal, undefined);
      if (held.has(name)) return Promise.resolve(callback(null));
      return new Promise((resolve, reject) => {
        const token = { reject }; held.set(name, token); owned.push([name, token]);
        Promise.resolve(callback({ name })).then(resolve, reject);
      });
    }, terminate() { for (const [name, token] of owned) { if (held.get(name) === token) held.delete(name); token.reject(Error('document terminated')); } } };
  }
  return { storage, locks, values };
}
async function priorDocument(p) {
  const locks = p.locks(), retirement = createAppHostRetirement({ storage: p.storage, locks, close: async () => ({ closed: true }) });
  await retirement.beforeLaunch(owner, () => true); retirement.record(owner, { appId, hostSession: session() }); locks.terminate();
}
function recoveryFixture(p, request) {
  let selected = state(owner);
  const calls = [], raw = { getLocalState: async () => selected, appSlotCleanup: () => null, dispose() {},
    extension: async (op, args, ctx) => { calls.push({ op, args, ctx }); return request(op, args); } };
  const root = createRootHostClient(raw, { view: { sessionStorage: p.storage, navigator: { locks: p.locks() } } }); root.observe(selected);
  return { ...root, calls, select(scope) { selected = state(scope); root.observe(selected); } };
}

test('an exact signed close ACK with optional Root ok removes the old obligation before launch', async () => {
  const p = port(); await priorDocument(p);
  const f = recoveryFixture(p, async op => op === 'apps.session.close' ? { ok: true, closed: true } : reply());
  await launch(f); assert.deepEqual(f.calls.map(c => c.op), ['apps.session.close', 'apps.launch']);
  assert.deepEqual(f.calls[0].args, { appId, frameId: session().frameId, lease: session().lease, revision: 1 });
});

test('only explicit terminal fixed-proof denial is reconciled as stale; unknown network remains pending', async () => {
  for (const code of ['app_host_session_closed', 'NETWORK_ERROR', 'DEVICE_REVOKED']) {
    const p = port(); await priorDocument(p);
    const f = recoveryFixture(p, async op => { if (op === 'apps.session.close') throw Object.assign(Error(code), { code }); return reply(); });
    if (code === 'app_host_session_closed') { await launch(f); assert.equal(f.calls.length, 2); }
    else {
      await assert.rejects(launch(f), { code: 'app_host_retirement_pending' });
      assert.deepEqual(f.calls.map(c => c.op), ['apps.session.close', 'apps.authentication.get']);
    }
  }
});

test('identity change during a close ACK retains its obligation and prevents the new allocation', async () => {
  const p = port(); await priorDocument(p); let f;
  f = recoveryFixture(p, async op => { assert.equal(op, 'apps.session.close'); f.select({ accountId: 'account-B', deviceId: 'device-B' }); return { closed: true }; });
  await assert.rejects(launch(f), { code: 'app_host_retirement_pending' }); assert.equal(f.calls.length, 1);
  assert.equal(JSON.parse([...p.values.values()][0]).records.length, 1);
});
