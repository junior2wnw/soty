import test from 'node:test';
import assert from 'node:assert/strict';
import { createClientWithStorage } from '../browser/client.mjs';
import { ConnectError } from '../browser/crypto.mjs';
import { validateState } from '../browser/storage.mjs';
import { createConnectService } from '../server/index.mjs';

const projectId = 'extension-identity-test', origin = 'https://identity.test', endpoint = `${origin}/rpc`;
const scope = { projectId, endpoint };
const turn = () => new Promise(resolve => setImmediate(resolve));
const deferred = () => { let resolve; const promise = new Promise(yes => { resolve = yes; }); return { promise, resolve }; };

// Each read returns its own transaction snapshot. The one-shot hook models a
// different tab committing after that snapshot was taken, before it resolves.
function memoryStorage() {
  let value = null, afterSnapshot = null;
  const copy = () => value === null ? null : structuredClone(value);
  return {
    async read() {
      const result = copy(), hook = afterSnapshot; afterSnapshot = null;
      if (hook) await hook();
      return result;
    },
    async claim(candidate) { if (value === null) { validateState(candidate, scope); value = structuredClone(candidate); } return copy(); },
    async compareAndSwap(expected, candidate) {
      if (value?.localRevision !== expected) throw new ConnectError('LOCAL_CONFLICT', 'Concurrent write');
      validateState(candidate, scope); value = structuredClone(candidate); return copy();
    },
    inspect: copy,
    nextRead(hook) { afterSnapshot = hook; },
  };
}
function channelBus() {
  const channels = new Set();
  return () => {
    const channel = { onmessage: null,
      postMessage(data) { for (const other of channels) if (other !== channel) queueMicrotask(() => other.onmessage?.({ data: structuredClone(data) })); },
      close() { channels.delete(channel); },
    };
    channels.add(channel); return channel;
  };
}
async function fixture(t) {
  const operations = [], traffic = [], clients = [];
  const service = createConnectService({ databasePath: ':memory:', projectId, allowedOrigins: [origin], extensions: [{
    operations: new Set(['world.profile.get', 'world.community.list', 'world.echo', 'agent.turn', 'agent.cancel', 'voice.agent.turn']),
    execute({ op, args, actor }) { operations.push({ op, accountId: actor.accountId, args }); return { accountId: actor.accountId, text: args.text ?? null }; },
  }] });
  let afterResponse = null;
  const fetcher = async (_url, options) => {
    const body = JSON.parse(options.body); traffic.push(body.op);
    const response = await service.handle({ ...body, origin });
    if (afterResponse) await afterResponse(body, response, options);
    return new Response(JSON.stringify(response), { status: response.ok ? 200 : 400 });
  };
  const client = (store, options = {}) => {
    const value = createClientWithStorage({ projectId, endpoint, fetch: fetcher, ...options }, store);
    clients.push(value); return value;
  };
  t.after(() => { clients.forEach(value => value.dispose()); service.close(); });
  const store = memoryStorage(), second = memoryStorage();
  const a = await client(store).bootstrap('A'), b = await client(second).bootstrap('B');
  const merged = store.inspect(); merged.installations.push(...second.inspect().installations); merged.localRevision++;
  await store.compareAndSwap(merged.localRevision - 1, merged);
  const createChannel = channelBus(), observed = [];
  const primary = client(store, { createChannel, onState: state => observed.push(state.accountId) });
  const other = client(store, { createChannel });
  traffic.length = 0;
  return { store, primary, other, a: a.accountId, b: b.accountId, operations, traffic, observed,
    afterResponse(callback) { afterResponse = callback; },
  };
}

test('real signed legacy calls reproduce the coalesced A → B → A transaction-snapshot race', async t => {
  const f = await fixture(t);
  f.afterResponse(body => {
    if (body.op === 'world.profile.get') f.store.nextRead(() => f.other.switchProfile(f.b));
    if (body.op === 'world.community.list') f.store.nextRead(() => f.other.switchProfile(f.a));
  });
  const before = await f.primary.getLocalState();
  const [profile, communities] = await Promise.all([
    f.primary.extension('world.profile.get'), f.primary.extension('world.community.list'),
  ]);
  const after = await f.primary.getLocalState(); await turn();
  assert.equal(before.accountId, f.a); assert.equal(profile.accountId, f.a);
  assert.equal(communities.accountId, f.b); assert.equal(after.accountId, f.a);
  assert.equal(f.observed.includes(f.b), false, 'queued state notices can miss the intermediate account');
});

test('expected account rejects a read queued behind a legacy read before any B challenge or signature', async t => {
  const f = await fixture(t), context = { expectedAccountId: f.a };
  f.afterResponse(body => { if (body.op === 'world.profile.get') f.store.nextRead(() => f.other.switchProfile(f.b)); });
  const results = await Promise.allSettled([
    // A legacy read keeps this deliberate transaction-snapshot queue boundary.
    f.primary.extension('world.profile.get', {}),
    f.primary.extension('world.community.list', {}, context),
  ]);
  assert.equal(results[0].status, 'fulfilled'); assert.equal(results[0].value.accountId, f.a);
  assert.equal(results[1].status, 'rejected'); assert.equal(results[1].reason.code, 'ACTIVE_PROFILE_CHANGED');
  assert.deepEqual(f.traffic, ['challenge', 'world.profile.get']);
  assert.equal(f.operations.length, 1); assert.equal(f.operations[0].accountId, f.a);
  await f.other.switchProfile(f.a); assert.equal((await f.primary.getLocalState()).accountId, f.a);
});

test('queued context and payload are captured before a caller can mutate them', async t => {
  const f = await fixture(t), entered = deferred(), release = deferred();
  f.store.nextRead(async () => { entered.resolve(); await release.promise; });
  const hold = f.primary.getLocalState(); await entered.promise;
  const context = { expectedAccountId: f.a }, args = { text: 'captured' };
  const pending = f.primary.extension('world.echo', args, context);
  context.expectedAccountId = f.b; args.text = 'changed';
  release.resolve(); await hold;
  const result = await pending;
  assert.equal(result.accountId, f.a); assert.equal(result.text, 'captured');
  assert.deepEqual(f.operations[0].args, { text: 'captured' }, 'client context is never inserted into signed product args');
});

test('expected admission still checks current identity immediately before signing', async t => {
  const f = await fixture(t);
  f.afterResponse(async body => { if (body.op === 'challenge') await f.other.switchProfile(f.b); });
  await assert.rejects(f.primary.extension('world.echo', {}, { expectedAccountId: f.a }), { code: 'ACTIVE_PROFILE_CHANGED' });
  assert.deepEqual(f.traffic, ['challenge']); assert.equal(f.operations.length, 0);
});

test('expected admission still rejects a response after another account is selected', async t => {
  const f = await fixture(t);
  f.afterResponse(async body => { if (body.op === 'world.echo') await f.other.switchProfile(f.b); });
  await assert.rejects(f.primary.extension('world.echo', {}, { expectedAccountId: f.a }), { code: 'ACTIVE_PROFILE_CHANGED' });
  assert.equal(f.operations.length, 1); assert.equal(f.operations[0].accountId, f.a);
});

test('malformed or mismatched context dispatches no challenge; two-argument API remains compatible', async t => {
  const f = await fixture(t);
  for (const context of [null, [], {}, { expectedAccountId: '' }, { expectedAccountId: f.a, extra: true }]) {
    await assert.rejects(f.primary.extension('world.echo', {}, context), { code: 'INVALID_ARGUMENT' });
  }
  await assert.rejects(f.primary.extension('world.echo', {}, { expectedAccountId: f.b }), { code: 'ACTIVE_PROFILE_CHANGED' });
  assert.deepEqual(f.traffic, []);
  assert.equal((await f.primary.extension('world.echo', { text: 'legacy' })).text, 'legacy');
});

test('profile rename rejects an A action queued behind a legacy read before any B challenge in the coalesced ABA race', async t => {
  const f = await fixture(t);
  f.afterResponse(body => { if (body.op === 'world.profile.get') f.store.nextRead(() => f.other.switchProfile(f.b)); });
  const results = await Promise.allSettled([
    f.primary.extension('world.profile.get', {}),
    f.primary.rename('New A', f.a),
  ]);
  assert.equal(results[0].status, 'fulfilled');
  assert.equal(results[1].status, 'rejected'); assert.equal(results[1].reason.code, 'ACTIVE_PROFILE_CHANGED');
  assert.deepEqual(f.traffic, ['challenge', 'world.profile.get']);
  f.afterResponse(null);
  assert.equal((await f.other.card()).label, 'B');
  await f.other.switchProfile(f.a);
  assert.equal((await f.primary.card()).label, 'A');
  assert.deepEqual(f.store.inspect().installations.map(item => item.label), ['A', 'B']);
});

test('profile rename retains the admission fence immediately before signing', async t => {
  const f = await fixture(t);
  f.afterResponse(async body => { if (body.op === 'challenge') await f.other.switchProfile(f.b); });
  await assert.rejects(f.primary.rename('New A', f.a), { code: 'ACTIVE_PROFILE_CHANGED' });
  assert.deepEqual(f.traffic, ['challenge']);
  f.afterResponse(null); assert.equal((await f.other.card()).label, 'B');
});

test('profile rename can commit only A and rejects its late acknowledgement after B is selected', async t => {
  const f = await fixture(t);
  f.afterResponse(async body => { if (body.op === 'profile.rename') await f.other.switchProfile(f.b); });
  await assert.rejects(f.primary.rename('New A', f.a), { code: 'ACTIVE_PROFILE_CHANGED' });
  f.afterResponse(null);
  assert.equal((await f.other.card()).label, 'B');
  assert.equal((await f.other.getLocalState()).label, 'B');
  await f.other.switchProfile(f.a);
  assert.equal((await f.primary.card()).label, 'New A');
  await f.primary.rename(' New A ', f.a);
  assert.equal((await f.primary.getLocalState()).label, 'New A');
});

test('profile rename validates the expected account and preserves the legacy one-argument API', async t => {
  const f = await fixture(t);
  for (const expected of [null, '', {}, f.b]) {
    await assert.rejects(f.primary.rename('New A', expected), { code: expected === f.b ? 'ACTIVE_PROFILE_CHANGED' : 'INVALID_ARGUMENT' });
  }
  assert.deepEqual(f.traffic, []);
  await f.primary.rename(' Legacy A ');
  assert.equal((await f.primary.getLocalState()).label, 'Legacy A');
  assert.equal((await f.primary.card()).label, 'Legacy A');
});


function transportClock(t) {
  let now = 0, next = 0;
  const timers = new Map();
  t.mock.method(globalThis, 'setTimeout', (callback, ms) => {
    const id = ++next; timers.set(id, { callback, at: now + ms }); return id;
  });
  t.mock.method(globalThis, 'clearTimeout', id => timers.delete(id));
  return { pending: () => timers.size, advance(ms) {
    now += ms;
    for (const [id, timer] of [...timers]) if (timer.at <= now) { timers.delete(id); timer.callback(); }
  } };
}
function holdResponse(f, operation) {
  const entered = deferred(), release = deferred();
  let signal;
  f.afterResponse((body, _response, options) => {
    if (body.op !== operation) return;
    signal = options.signal; entered.resolve();
    return new Promise((resolve, reject) => {
      const abort = () => reject(new DOMException('Aborted', 'AbortError'));
      signal.addEventListener('abort', abort, { once: true });
      release.promise.then(() => { signal.removeEventListener('abort', abort); resolve(); });
    });
  });
  return { entered: entered.promise, release: release.resolve, signal: () => signal };
}

test('signed agent turn survives 20 seconds while local state and cancel remain usable; success clears deadline', async t => {
  const f = await fixture(t), clock = transportClock(t), held = holdResponse(f, 'agent.turn');
  const pending = f.primary.extension('agent.turn', { text: 'long turn' }, { expectedAccountId: f.a });
  await held.entered; clock.advance(20_001);
  assert.equal(held.signal().aborted, false);
  assert.equal((await f.primary.getLocalState()).accountId, f.a);
  assert.equal((await f.primary.extension('agent.cancel', {}, { expectedAccountId: f.a })).accountId, f.a);
  clock.advance(69_999); assert.equal(held.signal().aborted, false);
  held.release(); assert.equal((await pending).text, 'long turn');
  assert.equal(clock.pending(), 0);
  clock.advance(120_000); assert.equal(held.signal().aborted, false);
  assert.equal(f.traffic.filter(op => op === 'agent.turn').length, 1);
});

test('signed agent turn aborts at its fixed bound without retries and clears the deadline', async t => {
  const f = await fixture(t), clock = transportClock(t), held = holdResponse(f, 'agent.turn');
  const pending = f.primary.extension('agent.turn', { timeoutMs: 999999999 });
  const rejected = assert.rejects(pending, { code: 'NETWORK_TIMEOUT' });
  await held.entered; clock.advance(119_999); assert.equal(held.signal().aborted, false);
  clock.advance(1); await rejected;
  assert.equal(held.signal().aborted, true); assert.equal(clock.pending(), 0);
  held.release(); await turn();
  assert.equal(f.traffic.filter(op => op === 'agent.turn').length, 1);
  assert.equal((await f.primary.getLocalState()).accountId, f.a);
});

for (const operation of ['challenge', 'world.echo', 'agent.cancel', 'voice.agent.turn']) {
  test(`${operation} retains 20-second deadline even with a caller timeout argument`, async t => {
    const f = await fixture(t), clock = transportClock(t), held = holdResponse(f, operation);
    const pending = f.primary.extension(operation === 'challenge' ? 'agent.turn' : operation, { timeoutMs: 999999999 });
    const rejected = assert.rejects(pending, { code: 'NETWORK_TIMEOUT' });
    await held.entered; clock.advance(19_999); assert.equal(held.signal().aborted, false);
    clock.advance(1); await rejected; assert.equal(clock.pending(), 0);
    held.release(); await turn();
    assert.equal(f.traffic.filter(op => op === operation).length, 1);
    if (operation === 'challenge') assert.equal(f.operations.length, 0);
  });
}
