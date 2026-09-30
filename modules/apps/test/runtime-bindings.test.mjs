import test from 'node:test';
import assert from 'node:assert/strict';
import { createRuntimeBindings, RUNTIME_BINDING_LIMITS } from '../server/runtime-bindings.mjs';
import { RUNTIME_PROFILE, runtimeTargetDigest } from '../server/schema.mjs';
import { FRAME_BYTES } from '../server/protocol.mjs';

const owner = { accountId: 'account_A', deviceId: 'browser_A' }, key = 'link_A|host_A|connector_A';
const identity = { linkId: 'link_A', hostDeviceId: 'host_A', connectorId: 'connector_A' };
const appId = number => `app-${number.toString(16).padStart(32, '0')}`;
const id43 = number => number.toString().padStart(43, 'a');
const pins = target => ({ appId: target.appId, revision: target.revision, digest: target.digest, profile: target.profile });
const code = expected => error => error.code === expected;
function target(number = 1, changes = {}) {
  const value = { appId: appId(number), revision: 1, ownerAccountId: owner.accountId, connectorKey: key,
    port: 8000 + number, entryPath: '/#/dashboard', profile: RUNTIME_PROFILE, ...changes };
  return { ...value, digest: runtimeTargetDigest(value) };
}
function decision(value) {
  return { appId: value.appId, requiredBindingVersion: 2, targetRevision: value.revision,
    targetDigest: value.digest, profile: value.profile, route: { connectorKey: value.connectorKey } };
}
function fixture(t, options = {}) {
  let time = 10_000, sequence = 0;
  const scheduled = new Map(), sent = [], invalidations = [], channels = new Map();
  const timers = {
    setTimeout(callback, ms) { const timer = { id: ++sequence, unref() {} }; scheduled.set(timer.id, { callback, at: time + ms }); return timer; },
    clearTimeout(timer) { scheduled.delete(timer.id); },
  };
  function channel(changes = {}) {
    const value = { key, identity: { ...identity }, bindingVersion: 2, channelId: id43(++sequence), observations: new Map(), streams: new Map(),
      ws: { readyState: 1, bufferedAmount: 0, terminated: false, terminate() { this.terminated = true; this.readyState = 3; } }, ...changes };
    channels.set(value.key, value); return value;
  }
  const first = channel();
  const manager = createRuntimeBindings({ channels, timers, now: () => time, blockedPorts: options.blockedPorts,
    send(context, frame) { sent.push({ channel: context, frame: JSON.parse(JSON.stringify(frame)) }); return options.send ? options.send(context, frame) : true; },
    onBindingInvalidated(context, id) { invalidations.push({ channel: context, id }); options.onBindingInvalidated?.(context, id); },
  });
  t.after(() => manager.close());
  const frames = (type, context = first) => sent.filter(item => item.channel === context && item.frame.type === type).map(item => item.frame);
  function ack(set, context = first) { return manager.handleFrame(context, { type: 'binding-ack', channelId: context.channelId, syncId: set.syncId, ...pins(set.target) }); }
  function install(values, context = first) {
    manager.sync(context, values);
    for (const set of frames('binding-set', context)) ack(set, context);
  }
  function prepare(value = target(), changes = {}) {
    const input = { preparationId: id43(++sequence), actor: { ...owner }, target: value, requiredBindingVersion: 2,
      signal: new AbortController().signal, ...changes };
    return { input, promise: manager.prepareTarget(input), frame: frames('target-prepare', channels.get(value.connectorKey)).at(-1) };
  }
  function reply(prepared, changes = {}, context = first) {
    return manager.handleFrame(context, { type: 'target-prepared', channelId: context.channelId,
      nonce: prepared.frame.nonce, ...pins(prepared.frame.target), state: 'responding', httpStatus: 200, ...changes });
  }
  return { manager, channel: first, channels, replacement: channel, frames, sent, invalidations, ack, install, prepare, reply,
    time(value) { time = value; }, tick(ms) {
      time += ms;
      for (;;) {
        const item = [...scheduled].find(([, value]) => value.at <= time); if (!item) break;
        scheduled.delete(item[0]); item[1].callback();
      }
    }, pendingTimers: () => scheduled.size,
  };
}

test('configuration ACK grants exact branded reference but never invents HTTP health', t => {
  const f = fixture(t), value = target(); f.manager.sync(f.channel, [value]);
  assert.throws(() => f.manager.requireBinding(f.channel, decision(value)), code('app_binding_pending'));
  const set = f.frames('binding-set')[0]; assert.equal(set.target.connectorKey, undefined); assert.equal(Buffer.byteLength(JSON.stringify(set)) < FRAME_BYTES, true);
  f.ack(set); const reference = f.manager.requireBinding(f.channel, decision(value));
  assert.equal(Object.isFrozen(reference), true); assert.deepEqual(f.manager.openPins(reference), { channelId: f.channel.channelId, syncId: set.syncId, ...pins(value) });
  assert.deepEqual(f.manager.getState(f.channel, value.appId), { state: 'bound' }); assert.equal(f.channel.observations.size, 0);
  assert.throws(() => f.manager.assertBindingCurrent(f.channel, { ...reference }), code('app_binding_changed'));
  assert.throws(() => f.manager.requireBinding(f.channel, { ...decision(value), targetDigest: 'f'.repeat(64) }), code('app_binding_pending'));
});

test('owner binding projection cannot borrow an earlier target ACK or rejection', t => {
  const f = fixture(t), a = target(), b = target(1, { revision: 2, port: 9100 }); f.install([a]);
  assert.deepEqual(f.manager.getState(f.channel, a.appId, a), { state: 'bound' });
  for (const expected of [b, { ...a, appId: appId(2) }, { ...a, connectorKey: 'different' },
    { ...a, digest: 'f'.repeat(64) }, { ...a, profile: 'unknown-profile' }]) {
    assert.deepEqual(f.manager.getState(f.channel, a.appId, expected), { state: 'pending', reason: 'app_binding_changed' });
  }
  // Inspection is not an operation that replaces or invalidates the old binding.
  assert.ok(f.manager.requireBinding(f.channel, decision(a)));
  f.manager.sync(f.channel, [b]); const set = f.frames('binding-set').at(-1);
  f.manager.handleFrame(f.channel, { type: 'binding-rejected', channelId: f.channel.channelId, syncId: set.syncId,
    ...pins(b), error: 'invalid_app_path' });
  assert.equal(f.manager.getState(f.channel, b.appId, b).state, 'rejected');
  assert.equal(f.manager.getState(f.channel, a.appId, a).state, 'pending');
});

test('unrelated sync preserves reference; pending retransmission keeps its deadline and same syncId', t => {
  const f = fixture(t), a = target(), b = target(2); f.install([a]);
  const reference = f.manager.requireBinding(f.channel, decision(a)); f.manager.sync(f.channel, [a, b]);
  const firstB = f.frames('binding-set').at(-1); f.tick(2000); f.manager.sync(f.channel, [a, b]);
  assert.equal(f.manager.requireBinding(f.channel, decision(a)), reference);
  assert.equal(f.frames('binding-set').at(-1).syncId, firstB.syncId);
  f.tick(3000); assert.deepEqual(f.manager.getState(f.channel, b.appId), { state: 'unavailable', reason: 'app_binding_timeout' });
  f.ack(firstB); assert.throws(() => f.manager.requireBinding(f.channel, decision(b)), code('app_binding_pending'));
  f.manager.sync(f.channel, [a, b]); const retried = f.frames('binding-set').at(-1); assert.notEqual(retried.syncId, firstB.syncId);
  f.ack(retried); assert.equal(f.manager.requireBinding(f.channel, decision(a)), reference);
});

test('A to B to A requires fresh ACK and invalidates only changed application', t => {
  const f = fixture(t), a = target(), neighbour = target(2), b = target(1, { revision: 2, port: 9100 });
  f.install([a, neighbour]); const old = f.manager.requireBinding(f.channel, decision(a)), stable = f.manager.requireBinding(f.channel, decision(neighbour));
  const oldSet = f.frames('binding-set')[0]; f.manager.sync(f.channel, [b, neighbour]);
  assert.throws(() => f.manager.assertBindingCurrent(f.channel, old), code('app_binding_changed'));
  assert.equal(f.manager.requireBinding(f.channel, decision(neighbour)), stable); assert.deepEqual(f.invalidations.map(item => item.id), [a.appId]);
  f.ack(oldSet); assert.throws(() => f.manager.requireBinding(f.channel, decision(b)), code('app_binding_pending'));
  f.ack(f.frames('binding-set').at(-1)); f.manager.sync(f.channel, [a, neighbour]);
  const returned = f.frames('binding-set').at(-1); assert.notEqual(returned.syncId, old.syncId);
  f.ack(oldSet); assert.throws(() => f.manager.requireBinding(f.channel, decision(a)), code('app_binding_pending')); f.ack(returned);
  assert.notEqual(f.manager.requireBinding(f.channel, decision(a)), old);
  f.manager.sync(f.channel, [neighbour]); assert.equal(f.frames('binding-remove').at(-1).bindings[0].syncId, returned.syncId);
  f.manager.sync(f.channel, [a, neighbour]); assert.notEqual(f.frames('binding-set').at(-1).syncId, returned.syncId);
});

test('reconnect rejects old references and late controls before the old close callback', async t => {
  const f = fixture(t), value = target(); f.install([value]); const old = f.manager.requireBinding(f.channel, decision(value));
  const pending = f.prepare(), rejection = assert.rejects(pending.promise, code('app_offline'));
  const replacement = f.replacement(); f.manager.sync(replacement, [value]);
  assert.throws(() => f.manager.assertBindingCurrent(f.channel, old), code('app_binding_changed'));
  f.reply(pending); assert.equal(replacement.observations.size, 0);
  f.manager.drop(f.channel); await rejection; assert.equal(f.channels.get(key), replacement);
  f.manager.handleFrame(f.channel, { type: 'binding-ack', channelId: f.channel.channelId, syncId: old.syncId, ...pins(value) });
  assert.throws(() => f.manager.requireBinding(replacement, decision(value)), code('app_binding_pending'));
  f.ack(f.frames('binding-set', replacement).at(-1), replacement); f.manager.requireBinding(replacement, decision(value));
});

test('100 max-length paths use individual bounded frames and synchronous ACK burst; 101 refuses before mutation', t => {
  const f = fixture(t), values = Array.from({ length: 100 }, (_, index) => target(index + 1, { entryPath: '/' + 'я'.repeat(8191) }));
  f.manager.sync(f.channel, values); const frames = f.frames('binding-set'); assert.equal(frames.length, 100);
  assert.equal(frames.every(frame => Buffer.byteLength(JSON.stringify(frame), 'utf8') <= FRAME_BYTES), true);
  assert.equal(frames.reduce((sum, frame) => sum + Buffer.byteLength(JSON.stringify(frame), 'utf8'), 0) < RUNTIME_BINDING_LIMITS.sendBytes, true);
  for (const frame of frames) f.ack(frame); for (const value of values) f.manager.requireBinding(f.channel, decision(value));
  const before = f.sent.length; assert.throws(() => f.manager.sync(f.channel, [...values, target(101)]), code('app_binding_capacity'));
  assert.equal(f.sent.length, before); f.manager.requireBinding(f.channel, decision(values[0]));
});

test('unsafe historical path and local port refusal disable only their own desired binding', t => {
  const f = fixture(t, { blockedPorts: [9100] }), a = target(), b = target(2); f.install([a, b]);
  const stable = f.manager.requireBinding(f.channel, decision(b)), bad = target(1, { revision: 2, entryPath: '/%2e/_soty/session' });
  f.manager.sync(f.channel, [bad, b]); assert.deepEqual(f.manager.getState(f.channel, a.appId), { state: 'rejected', reason: 'invalid_app_path' });
  assert.equal(f.frames('binding-set').length, 2); assert.equal(f.manager.requireBinding(f.channel, decision(b)), stable);
  f.manager.sync(f.channel, [target(1, { revision: 3, port: 9100 }), b]);
  assert.deepEqual(f.manager.getState(f.channel, a.appId), { state: 'rejected', reason: 'invalid_app_port' });
});

test('correlated binding rejection and observations never become authority for neighbour or stale pins', t => {
  const f = fixture(t), a = target(), b = target(2); f.manager.sync(f.channel, [a, b]); const [setA, setB] = f.frames('binding-set');
  f.manager.handleFrame(f.channel, { type: 'binding-rejected', channelId: f.channel.channelId, syncId: setA.syncId, ...pins(a), error: 'invalid_app_port' });
  assert.deepEqual(f.manager.getState(f.channel, a.appId), { state: 'rejected', reason: 'invalid_app_port' }); f.ack(setA);
  assert.throws(() => f.manager.requireBinding(f.channel, decision(a)), code('app_binding_pending')); f.ack(setB);
  const observation = { type: 'bound-observation', channelId: f.channel.channelId, syncId: setB.syncId, ...pins(b), state: 'responding', httpStatus: 404 };
  f.manager.handleFrame(f.channel, observation); assert.deepEqual(f.channel.observations.get(b.appId), { state: 'ready', at: 10_000,
    targetRevision: b.revision, targetDigest: b.digest, evidence: 'connector-v2-observation', httpStatus: 404 });
  assert.throws(() => f.manager.handleFrame(f.channel, { ...observation, httpStatus: 500 }), code('app_bad_binding_observation'));
  assert.throws(() => f.manager.handleFrame(f.channel, { ...observation, digest: 'f'.repeat(64) }), code('app_bad_binding_frame'));
  assert.throws(() => f.manager.handleFrame(f.channel, { ...observation, channelId: id43(999) }), code('app_bad_binding_frame'));
  assert.throws(() => f.manager.handleFrame(f.channel, { ...observation, extra: 'unexpected' }), code('app_bad_binding_frame'));
  f.manager.handleFrame(f.channel, { ...observation, syncId: id43(998), state: 'unreachable', httpStatus: null });
  assert.equal(f.channel.observations.get(b.appId).state, 'ready');
  f.manager.handleFrame(f.channel, { ...observation, state: 'unreachable', httpStatus: 500 }); assert.equal(f.channel.observations.get(b.appId).state, 'stopped');
});

test('digest tampering fails before any desired-map mutation; legacy mode never receives v2 admission', t => {
  const f = fixture(t), value = target(); f.install([value]); const reference = f.manager.requireBinding(f.channel, decision(value));
  assert.throws(() => f.manager.sync(f.channel, [{ ...value, port: 9999 }]), code('app_invalid_binding_digest'));
  assert.equal(f.manager.requireBinding(f.channel, decision(value)), reference);
  const legacy = f.replacement({ bindingVersion: 1, channelId: undefined });
  assert.throws(() => f.manager.requireBinding(legacy, decision(value)), code('app_source_protocol_required'));
  assert.throws(() => f.manager.prepareTarget({ preparationId: id43(1), actor: owner, target: value, requiredBindingVersion: 2 }), code('app_source_protocol_required'));
});

test('failed send or exact buffered-byte bound revokes prior admission and leaves no queue or timers', t => {
  for (const mode of ['buffer', 'false', 'throw']) {
    let fail = false; const f = fixture(t, { send: () => { if (fail && mode === 'throw') throw new Error('injected'); return !fail || mode !== 'false'; } });
    const value = target(); f.install([value]); const reference = f.manager.requireBinding(f.channel, decision(value));
    fail = true; if (mode === 'buffer') f.channel.ws.bufferedAmount = RUNTIME_BINDING_LIMITS.sendBytes;
    assert.throws(() => f.manager.sync(f.channel, [target(1, { revision: 2 })]), code(mode === 'buffer' ? 'app_binding_backpressure' : 'app_offline'));
    assert.equal(f.channel.ws.terminated, true); assert.equal(f.pendingTimers(), 0);
    assert.throws(() => f.manager.assertBindingCurrent(f.channel, reference), code('app_binding_changed'));
  }
});

test('two preparations for the same app have separate nonce/proof and never change active binding', async t => {
  const f = fixture(t), active = target(), candidate = target(1, { revision: 2, port: 9100 }); f.install([active]);
  const reference = f.manager.requireBinding(f.channel, decision(active)), a = f.prepare(candidate), b = f.prepare(candidate);
  assert.notEqual(a.frame.nonce, b.frame.nonce); assert.notEqual(a.frame.nonce, a.input.preparationId);
  f.reply(b, { httpStatus: 401 }); f.reply(a, { httpStatus: 404 }); const [proofA, proofB] = await Promise.all([a.promise, b.promise]);
  assert.equal(f.manager.verifyPreparedTarget({ ...a.input, evidence: proofA }), true); assert.equal(f.manager.verifyPreparedTarget({ ...b.input, evidence: proofB }), true);
  assert.equal(f.manager.verifyPreparedTarget({ ...a.input, evidence: proofB }), false); assert.equal(f.manager.verifyPreparedTarget({ ...a.input, evidence: { ...proofA } }), false);
  assert.equal(f.manager.verifyPreparedTarget({ ...a.input, evidence: proofA, actor: { ...owner, deviceId: 'other' } }), false);
  assert.equal(f.manager.verifyPreparedTarget({ ...a.input, evidence: proofA, target: { ...candidate, port: 9999 } }), false);
  assert.equal(f.manager.requireBinding(f.channel, decision(active)), reference); assert.equal(f.channel.observations.size, 0);
});

test('preparation captures input and success expires from start, not response; abort and reconnect invalidate proof', async t => {
  const f = fixture(t), source = target(), actor = { ...owner }, controller = new AbortController();
  const prepared = f.prepare(source, { actor, signal: controller.signal }), original = { ...source }; actor.accountId = 'other'; source.port = 9999;
  f.tick(4000); f.reply(prepared); const evidence = await prepared.promise;
  const input = { ...prepared.input, actor: owner, target: original, evidence };
  assert.equal(f.manager.verifyPreparedTarget(input), true); f.time(9999); assert.equal(f.manager.verifyPreparedTarget(input), false);
  f.time(39_999); assert.equal(f.manager.verifyPreparedTarget(input), true); f.time(40_000); assert.equal(f.manager.verifyPreparedTarget(input), false);
  f.time(15_000); controller.abort(); assert.equal(f.manager.verifyPreparedTarget(input), false);
  const another = f.prepare(original); f.reply(another); const second = await another.promise;
  f.replacement(); assert.equal(f.manager.verifyPreparedTarget({ ...another.input, evidence: second }), false);
});

test('four in-flight preparations bound capacity; abort, timeout, drop and close settle all tickets exactly once', async t => {
  const f = fixture(t), controller = new AbortController(), pending = [f.prepare(target(), { signal: controller.signal }), f.prepare(), f.prepare(), f.prepare()];
  assert.throws(() => f.prepare(), code('app_prepare_busy')); const rejectedAbort = assert.rejects(pending[0].promise, code('apps_source_preparation_stale'));
  controller.abort(); await rejectedAbort; const replacement = f.prepare(); f.reply(pending[0]);
  const timed = [...pending.slice(1), replacement].map(item => assert.rejects(item.promise, code('apps_source_probe_timeout')));
  f.tick(5000); await Promise.all(timed); assert.equal(f.pendingTimers(), 0);
  const dropped = f.prepare(), dropResult = assert.rejects(dropped.promise, code('app_offline')); f.manager.drop(f.channel); await dropResult;
  const channel = f.replacement(), closing = f.prepare(), closeResult = assert.rejects(closing.promise, code('app_offline'));
  f.manager.close(); await closeResult; assert.equal(f.pendingTimers(), 0); assert.equal(f.channels.get(key), channel);
});

test('typed preparation rejection and negative HTTP evidence preserve unrelated active apps', async t => {
  for (const error of ['invalid_app_port', 'invalid_app_path', 'unsupported_profile', 'app_prepare_busy']) {
    const f = fixture(t), active = target(); f.install([active]); const reference = f.manager.requireBinding(f.channel, decision(active)), prepared = f.prepare(target(2));
    const rejected = assert.rejects(prepared.promise, code(error));
    f.manager.handleFrame(f.channel, { type: 'target-rejected', channelId: f.channel.channelId, nonce: prepared.frame.nonce, ...pins(prepared.frame.target), error });
    await rejected; assert.equal(f.manager.requireBinding(f.channel, decision(active)), reference);
  }
  for (const status of [500, 599, null]) {
    const f = fixture(t), prepared = f.prepare(), rejected = assert.rejects(prepared.promise, code('app_source_unreachable'));
    f.reply(prepared, { state: 'unreachable', httpStatus: status }); await rejected;
  }
});

test('current-nonce tampering is a protocol fault; late timeout reply cannot resolve a replacement', async t => {
  const f = fixture(t), first = f.prepare(), firstError = assert.rejects(first.promise, code('apps_source_probe_timeout'));
  assert.throws(() => f.reply(first, { digest: 'f'.repeat(64) }), code('app_bad_binding_frame'));
  assert.throws(() => f.reply(first, { state: 'responding', httpStatus: 500 }), code('app_bad_binding_observation'));
  f.tick(5000); await firstError; const second = f.prepare(); f.reply(first); f.reply(second);
  const evidence = await second.promise; assert.equal(f.manager.verifyPreparedTarget({ ...second.input, evidence }), true);
  assert.equal(f.manager.handleFrame(f.channel, { type: 'head' }), false);
});
