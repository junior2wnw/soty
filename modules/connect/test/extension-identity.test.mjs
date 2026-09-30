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
    operations: new Set(['world.profile.get', 'world.community.list', 'world.echo']),
    execute({ op, args, actor }) { operations.push({ op, accountId: actor.accountId, args }); return { accountId: actor.accountId, text: args.text ?? null }; },
  }] });
  let afterResponse = null;
  const fetcher = async (_url, options) => {
    const body = JSON.parse(options.body); traffic.push(body.op);
    const response = await service.handle({ ...body, origin });
    if (afterResponse) await afterResponse(body, response);
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

test('expected account rejects the second ABA read before any B challenge or signature', async t => {
  const f = await fixture(t), context = { expectedAccountId: f.a };
  f.afterResponse(body => { if (body.op === 'world.profile.get') f.store.nextRead(() => f.other.switchProfile(f.b)); });
  const results = await Promise.allSettled([
    f.primary.extension('world.profile.get', {}, context),
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
