import test from 'node:test';
import assert from 'node:assert/strict';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { join } from 'node:path';

// Actual repository SDK and Connect. Provider transport is synthetic in this suite.
const root = fileURLToPath(new URL('../../', import.meta.url));
const [{ createClientWithStorage }, { createConnectService }] = await Promise.all([
  import(pathToFileURL(join(root, 'modules/connect/browser/client.mjs')).href),
  import(pathToFileURL(join(root, 'modules/connect/server/index.mjs')).href),
]);
const ORIGIN = 'https://voice-sdk-fixture.invalid';
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const clone = value => value === null ? null : structuredClone(value);
const bounded = async (promise, maximum = 300) => {
  let timer;
  try { return await Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('state_queue_blocked')), maximum); })]); }
  finally { clearTimeout(timer); }
};
function memoryStorage() {
  let state = null;
  return { read: async () => clone(state),
    claim: async candidate => { if (!state) state = clone(candidate); return clone(state); },
    compareAndSwap: async (revision, candidate) => {
      if (state?.localRevision !== revision) throw Object.assign(new Error('LOCAL_CONFLICT'), { code: 'LOCAL_CONFLICT' });
      state = clone(candidate); return clone(state);
    },
    async mergeProfile(other) { const previous = await this.read(); await this.compareAndSwap(previous.localRevision,
      { ...previous, localRevision: previous.localRevision + 1, installations: [...previous.installations, ...other.installations] }); },
  };
}
async function fixture(t, options = {}) {
  let held = null;
  const observed = [], gates = [];
  const extension = { operations: new Set(['voice.orb.status', 'voice.orb.start', 'voice.orb.turn', 'voice.orb.cancel', 'voice.orb.end', 'fixture.slow']),
    async executeAsync({ op, args, actor }) {
      observed.push({ op, args: clone(args), accountId: actor.accountId, deviceId: actor.deviceId });
      if (held?.op === op) { const gate = held; held = null; gate.entered.resolve(); await gate.release.promise; }
      return op === 'voice.orb.start' && options.voiceCleanup ? { schema: 'soty.voice-orb-started.v1', sessionId: 'fixed_owned_session',
        providerSessionId: 'sess_synthetic', expiresAt: Date.now() + 15000, remainingDurationMs: 15000 }
        : { schema: 'synthetic.voice-sdk-result.v1', accountId: actor.accountId, deviceId: actor.deviceId };
    } };
  const connect = createConnectService({ databasePath: ':memory:', projectId: 'soty', allowedOrigins: [ORIGIN], extensions: [extension] });
  const fetcher = async (_url, init) => {
    const request = JSON.parse(init.body), result = await connect.handle({ origin: ORIGIN, ...request });
    return new Response(JSON.stringify(result), { status: 200, headers: { 'content-type': 'application/json' } });
  };
  const storage = memoryStorage(), otherStorage = memoryStorage();
  const sdkOptions = { scopedVoiceCleanup: options.voiceCleanup === true, projectId: 'soty', endpoint: ORIGIN + '/rpc', fetch: fetcher, createChannel: () => ({ close() {}, postMessage() {} }) };
  const client = createClientWithStorage(sdkOptions, storage), second = createClientWithStorage(sdkOptions, otherStorage);
  const alice = await client.bootstrap('SDK Voice Alice'), bob = await second.bootstrap('SDK Voice Bob');
  await storage.mergeProfile(await otherStorage.read());
  t.after(() => { gates.forEach(gate => gate.release.resolve()); client.dispose(); second.dispose(); connect.close(); });
  return { client, alice, bob, observed,
    hold(op) { const gate = { op, entered: deferred(), release: deferred() }; gates.push(gate); held = gate; return gate; },
  };
}
for (const operation of ['voice.orb.start', 'voice.orb.turn', 'voice.orb.end']) test(`actual signed SDK ${operation} releases the state queue and cancel does not wait`, async t => {
  const f = await fixture(t), gate = f.hold(operation);
  const pending = f.client.extension(operation, { requestId: 'synthetic_voice_owned_request' }, { expectedAccountId: f.alice.accountId });
  // A baseline failure must not strand a 60/120-second request or hide a rejection.
  const observedPending = pending.then(value => ({ value }), error => ({ error }));
  await bounded(gate.entered.promise, 1500);
  try {
    assert.equal((await bounded(f.client.getLocalState())).accountId, f.alice.accountId);
    const cancelled = await bounded(f.client.extension('voice.orb.cancel', { sessionId: 'synthetic_owned_session' }, { expectedAccountId: f.alice.accountId }));
    assert.equal(cancelled.accountId, f.alice.accountId);
  } finally { gate.release.resolve(); }
  assert.equal((await observedPending).error, undefined);
  assert.deepEqual(f.observed.map(row => row.op), [operation, 'voice.orb.cancel']);
});

test('actual signed SDK discards an in-flight A→B→A result using durable localRevision', async t => {
  const f = await fixture(t), before = await f.client.getLocalState(), gate = f.hold('voice.orb.turn');
  const pending = f.client.extension('voice.orb.turn', { sessionId: 'synthetic_session', callId: 'synthetic_delegation' }, { expectedAccountId: f.alice.accountId });
  const observedPending = pending.then(value => ({ value }), error => ({ error }));
  await bounded(gate.entered.promise, 1500);
  try { await bounded(f.client.switchProfile(f.bob.accountId)); await bounded(f.client.switchProfile(f.alice.accountId)); }
  finally { gate.release.resolve(); }
  const result = await observedPending;
  assert.equal(result.error?.code, 'ACTIVE_PROFILE_CHANGED');
  const after = await f.client.getLocalState(); assert.equal(after.accountId, f.alice.accountId);
  assert.equal(after.deviceId, before.deviceId); assert.ok(Number.isSafeInteger(after.localRevision));
  assert.ok(after.localRevision > before.localRevision);
});

test('voice controls require the displayed account and JSON mutation cannot repurpose their signed payload', async t => {
  const f = await fixture(t);
  await assert.rejects(f.client.extension('voice.orb.start', {}), error => error.code === 'INVALID_ARGUMENT');
  assert.equal(f.observed.length, 0);
  const gate = f.hold('voice.orb.turn'), args = { sessionId: 'original_session', callId: 'original_task' };
  const pending = f.client.extension('voice.orb.turn', args, { expectedAccountId: f.alice.accountId });
  const observedPending = pending.then(value => ({ value }), error => ({ error }));
  args.sessionId = 'other_session'; args.callId = 'other_task';
  await bounded(gate.entered.promise, 1500); gate.release.resolve(); await observedPending;
  assert.deepEqual(f.observed[0].args, { sessionId: 'original_session', callId: 'original_task' });
});


test('actual SDK owned-close capability is not JSON-copyable, takes no target and ends only original A after B activation', async t => {
  const f = await fixture(t, { voiceCleanup: true });
  const result = await f.client.extension('voice.orb.start', { requestId: 'real_signed_start_one' }, { expectedAccountId: f.alice.accountId });
  const cleanup = f.client.voiceSessionCleanup(result); assert.equal(typeof cleanup, 'function');
  assert.equal(f.client.voiceSessionCleanup(JSON.parse(JSON.stringify(result))), null); assert.equal(f.client.appSlotCleanup(result), null);
  await assert.rejects(cleanup({ sessionId: 'foreign_session' }), error => error.code === 'INVALID_ARGUMENT');
  result.sessionId = 'mutated_target'; await f.client.switchProfile(f.bob.accountId);
  await cleanup(); await cleanup();
  const ends = f.observed.filter(row => row.op === 'voice.orb.end'); assert.equal(ends.length, 1);
  assert.equal(ends[0].accountId, f.alice.accountId); assert.deepEqual(ends[0].args, { sessionId: 'fixed_owned_session' });
  assert.equal((await f.client.getLocalState()).accountId, f.bob.accountId);
});

test('actual SDK automatically closes a late admitted A start before rejecting its result after A→B', async t => {
  const f = await fixture(t, { voiceCleanup: true }), gate = f.hold('voice.orb.start');
  const pending = f.client.extension('voice.orb.start', { requestId: 'late_real_signed_start' }, { expectedAccountId: f.alice.accountId });
  const observedPending = pending.then(value => ({ value }), error => ({ error })); await bounded(gate.entered.promise, 1500);
  try { await bounded(f.client.switchProfile(f.bob.accountId)); } finally { gate.release.resolve(); }
  const result = await observedPending; assert.equal(result.error?.code, 'ACTIVE_PROFILE_CHANGED');
  const ends = f.observed.filter(row => row.op === 'voice.orb.end'); assert.equal(ends.length, 1);
  assert.equal(ends[0].accountId, f.alice.accountId); assert.deepEqual(ends[0].args, { sessionId: 'fixed_owned_session' });
});

test('SDK disposal attempts only previously captured exact voice cleanup and cached callback cannot mint a task', async t => {
  const f = await fixture(t, { voiceCleanup: true });
  const result = await f.client.extension('voice.orb.start', { requestId: 'dispose_real_signed_start' }, { expectedAccountId: f.alice.accountId });
  const cleanup = f.client.voiceSessionCleanup(result); f.client.dispose(); assert.equal(f.client.voiceSessionCleanup(result), null);
  await cleanup(); assert.equal(f.observed.filter(row => row.op === 'voice.orb.end').length, 1);
  assert.equal(f.observed.some(row => row.op === 'voice.orb.turn'), false);
});
