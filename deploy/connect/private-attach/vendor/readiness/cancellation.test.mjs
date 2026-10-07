import test from 'node:test';
import assert from 'node:assert/strict';
import { Writable } from 'node:stream';
import { performance } from 'node:perf_hooks';
import { getEventListeners } from 'node:events';
import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fixture } from '../../test/strict-sender-fixture.mjs';
import { sendAuthenticatedBackup } from './staged-test-adapter.mjs';
import { createReadinessBoundary } from './readiness-boundary.mjs';
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const deferred = () => {
  let resolve, reject; const promise = new Promise((done, fail) => { resolve = done; reject = fail; });
  return { promise, resolve, reject };
};
const SPEC = Object.freeze({ transaction: '1234567890abcdef1234567890abcdef', nonce: 'a'.repeat(32),
  image: 'sha256:' + 'b'.repeat(64), receiverSourceSha256: 'c'.repeat(64) });
const warm = spec => ({ ...spec, inputBytes: 0, verifiedBeforeStop: true });
const ready = expected => ({ transaction: expected.transaction, nonce: expected.nonce,
  expectedSha256: expected.expectedSha256, expectedManifestSha256: expected.expectedManifestSha256,
  ...expected.sourceWitness, inputBytes: 0 });
const receipt = encrypted => ({ expectedSha256: encrypted.options.expectedSha256,
  expectedManifestSha256: encrypted.options.expectedManifestSha256, sourceWitness: encrypted.options.sourceWitness });
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
function output(writeHook) {
  let bytes = 0, writes = 0;
  const stream = new Writable({ highWaterMark: 65536, autoDestroy: true, emitClose: true,
    write(chunk, _encoding, callback) { bytes += chunk.length; writes++; if (writeHook) writeHook(callback); else callback(); } });
  return { stream, bytes: () => bytes, writes: () => writes };
}
async function prepared(t, ports = {}, signal) {
  const f = await fixture(t), encrypted = await f.encrypt();
  const boundary = createReadinessBoundary({ prepareBeforeStop: spec => warm(spec), bindAfterAuthentication: expected => ready(expected), ...ports });
  const lease = await boundary.warmBeforeServingStop(SPEC, signal ? { signal } : undefined);
  boundary.noteServingStopped(lease); boundary.captureLaterBackup(lease, receipt(encrypted));
  return { f, encrypted, boundary, lease };
}
function observe(promise) {
  let settled = false;
  const outcome = promise.then(value => { settled = true; return { value }; }, error => { settled = true; return { error }; });
  return { outcome, settled: () => settled };
}
async function promptFailure(observed, code) {
  const result = await Promise.race([observed.outcome, delay(100).then(() => ({ deadline: true }))]);
  assert.equal(result.deadline, undefined, 'local cancellation did not settle inside 100ms');
  assert.equal(result.error?.code, code); return result;
}

test('never-resolving bind locally cancels before original idle/wall, with native close/body0', async t => {
  const entered = deferred(), controller = new AbortController(); let portSignal;
  const f = await prepared(t, { bindAfterAuthentication: (_expected, fence) => {
    portSignal = fence.signal; entered.resolve(); return new Promise(() => {});
  } });
  const collected = output();
  const observed = observe(sendAuthenticatedBackup({ ...f.encrypted.options, output: collected.stream,
    beforeBody: f.boundary.beforeBody(f.lease), signal: controller.signal }));
  t.after(async () => { controller.abort(); await observed.outcome; });
  await entered.promise; const cancelledAt = performance.now(); f.boundary.cancel(f.lease);
  await promptFailure(observed, 'restore_io_failed');
  assert.ok(performance.now() - cancelledAt < 100);
  assert.equal(portSignal.aborted, true); assert.equal(collected.stream.closed, true);
  assert.equal(collected.bytes(), 0); assert.equal(collected.writes(), 0);
});

test('cancel then late ready resolves without reading ready getters or reopening body', async t => {
  const entered = deferred(), bound = deferred(); let reads = 0;
  const lateReady = {};
  for (const key of ['transaction','nonce','expectedSha256','expectedManifestSha256','generationId','checkpointSha256','inventorySha256','inputBytes'])
    Object.defineProperty(lateReady, key, { enumerable: true, get() { reads++; throw Error('must_not_read'); } });
  const f = await prepared(t, { bindAfterAuthentication: () => { entered.resolve(); return bound.promise; } });
  const collected = output(), observed = observe(sendAuthenticatedBackup({ ...f.encrypted.options,
    output: collected.stream, beforeBody: f.boundary.beforeBody(f.lease) }));
  await entered.promise; f.boundary.cancel(f.lease);
  await promptFailure(observed, 'restore_io_failed');
  bound.resolve(lateReady); await delay(10);
  assert.equal(reads, 0); assert.equal(collected.bytes(), 0); assert.equal(collected.stream.closed, true);
});

test('late private rejection and repeated cancellation remain handled without private getters', async t => {
  const entered = deferred(), bound = deferred(), packets = []; let reads = 0;
  const hidden = {};
  for (const key of ['message','code','name','stack','cause','reason']) Object.defineProperty(hidden, key,
    { get() { reads++; throw Error('private_getter_must_not_run'); } });
  const f = await prepared(t, { emit: packet => { packets.push(packet); throw hidden; },
    bindAfterAuthentication: () => { entered.resolve(); return bound.promise; } });
  const collected = output(), observed = observe(sendAuthenticatedBackup({ ...f.encrypted.options,
    output: collected.stream, beforeBody: f.boundary.beforeBody(f.lease) }));
  await entered.promise; f.boundary.cancel(f.lease); f.boundary.cancel(f.lease);
  await promptFailure(observed, 'restore_io_failed');
  bound.reject(hidden); await delay(10);
  assert.equal(reads, 0); assert.equal(collected.bytes(), 0);
  assert.deepEqual(packets.filter(packet => packet.phase === 'cancelled'), [
    { schema: 'soty.restore-readiness-phase.v1', phase: 'cancelled', code: 'admission_closed' }]);
});

test('native cancellation wakes never-resolving warm before a lease or stop admission exists', async () => {
  const entered = deferred(), controller = new AbortController(); let portSignal;
  const boundary = createReadinessBoundary({ prepareBeforeStop: (_spec, control) => {
    portSignal = control.signal; entered.resolve(); return new Promise(() => {});
  }, bindAfterAuthentication: expected => ready(expected) });
  const observed = observe(boundary.warmBeforeServingStop(SPEC, { signal: controller.signal }));
  await entered.promise; controller.abort();
  await promptFailure(observed, 'readiness_cancelled');
  assert.equal(portSignal.aborted, true);
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
});

test('late warm resolution after cancellation cannot publish a lease', async () => {
  const entered = deferred(), pending = deferred(), controller = new AbortController(); let reads = 0;
  const boundary = createReadinessBoundary({ prepareBeforeStop: () => { entered.resolve(); return pending.promise; }, bindAfterAuthentication: expected => ready(expected) });
  const observed = observe(boundary.warmBeforeServingStop(SPEC, { signal: controller.signal }));
  await entered.promise; controller.abort();
  await promptFailure(observed, 'readiness_cancelled');
  const response = {}; Object.defineProperty(response, 'verifiedBeforeStop', { enumerable: true, get() { reads++; throw Error('must_not_read'); } });
  pending.resolve(response); await delay(10);
  assert.equal(reads, 0); assert.equal((await observed.outcome).value, undefined);
});

test('already-aborted warm never invokes a port and never reads abort reason/getters', async () => {
  const controller = new AbortController(); let calls = 0, reads = 0;
  const hidden = {}; Object.defineProperty(hidden, 'message', { get() { reads++; throw Error('must_not_read'); } });
  controller.abort(hidden);
  Object.defineProperty(controller.signal, 'aborted', { get() { reads++; throw Error('must_not_read'); } });
  Object.defineProperty(controller.signal, 'reason', { get() { reads++; throw Error('must_not_read'); } });
  const boundary = createReadinessBoundary({ prepareBeforeStop: () => { calls++; return warm(SPEC); }, bindAfterAuthentication: expected => ready(expected) });
  await assert.rejects(boundary.warmBeforeServingStop(SPEC, { signal: controller.signal }), { code: 'readiness_cancelled' });
  assert.equal(calls, 0); assert.equal(reads, 0);
});

test('spoof signal/extra control deny before starting warm and without invoking getters', async () => {
  let calls = 0, reads = 0;
  const fake = {}; Object.defineProperty(fake, 'aborted', { get() { reads++; throw Error('must_not_read'); } });
  const boundary = createReadinessBoundary({ prepareBeforeStop: () => { calls++; return warm(SPEC); }, bindAfterAuthentication: expected => ready(expected) });
  for (const control of [{ signal: fake }, { signal: null }, { signal: undefined }, { signal: new AbortController().signal, extra: true }])
    await assert.rejects(boundary.warmBeforeServingStop(SPEC, control), { code: 'readiness_signal_invalid' });
  assert.equal(calls, 0); assert.equal(reads, 0);
});

for (const phase of ['warm', 'stopped', 'captured']) test('cancel in ' + phase + ' state permanently denies later admission', async t => {
  const f = await fixture(t), encrypted = await f.encrypt(); let binds = 0;
  const packets = [];
  const boundary = createReadinessBoundary({ prepareBeforeStop: spec => warm(spec), bindAfterAuthentication: expected => { binds++; return ready(expected); }, emit: packet => packets.push(packet) });
  const lease = await boundary.warmBeforeServingStop(SPEC);
  if (phase !== 'warm') boundary.noteServingStopped(lease);
  if (phase === 'captured') boundary.captureLaterBackup(lease, receipt(encrypted));
  const retained = phase === 'captured' ? boundary.beforeBody(lease) : null;
  boundary.cancel(lease);
  assert.throws(() => boundary.noteServingStopped(lease), { code: 'readiness_stage_invalid' });
  assert.throws(() => boundary.captureLaterBackup(lease, receipt(encrypted)), { code: 'readiness_backup_invalid' });
  assert.throws(() => boundary.beforeBody(lease), { code: 'readiness_stage_invalid' });
  if (retained) {
    const collected = output();
    await assert.rejects(sendAuthenticatedBackup({ ...encrypted.options, output: collected.stream, beforeBody: retained }), { code: 'restore_io_failed' });
    assert.equal(collected.bytes(), 0); assert.equal(collected.stream.closed, true);
  }
  assert.equal(binds, 0);
  assert.equal(packets.find(packet => packet.phase === 'cancelled').code, 'admission_closed');
});

test('synchronous cancel inside trusted bind cannot win admission with its ready result', async t => {
  let current;
  const f = await prepared(t, { bindAfterAuthentication: expected => { current.boundary.cancel(current.lease); return ready(expected); } });
  current = f; const collected = output();
  await assert.rejects(sendAuthenticatedBackup({ ...f.encrypted.options, output: collected.stream,
    beforeBody: f.boundary.beforeBody(f.lease) }), { code: 'restore_io_failed' });
  assert.equal(collected.bytes(), 0); assert.equal(collected.stream.closed, true);
});

test('shared native signal cancellation during bind closes local sender and requests port abort', async t => {
  const controller = new AbortController(), entered = deferred(); let portSignal;
  const f = await prepared(t, { bindAfterAuthentication: (_expected, fence) => {
    portSignal = fence.signal; entered.resolve(); return new Promise(() => {});
  } }, controller.signal);
  const collected = output(), observed = observe(sendAuthenticatedBackup({ ...f.encrypted.options,
    output: collected.stream, beforeBody: f.boundary.beforeBody(f.lease), signal: controller.signal }));
  await entered.promise; controller.abort();
  await promptFailure(observed, 'restore_io_failed');
  assert.equal(portSignal.aborted, true); assert.equal(collected.bytes(), 0); assert.equal(collected.stream.closed, true);
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
});

test('after body admission local cancel is not a claim to retract bytes or close borrowed output', async t => {
  const controller = new AbortController(), entered = deferred(), packets = []; let releaseWrite, portSignal;
  const f = await prepared(t, { emit: packet => packets.push(packet), bindAfterAuthentication: (expected, fence) => { portSignal = fence.signal; return ready(expected); } });
  const collected = output(callback => { releaseWrite = callback; entered.resolve(); });
  const observed = observe(sendAuthenticatedBackup({ ...f.encrypted.options, output: collected.stream,
    beforeBody: f.boundary.beforeBody(f.lease), signal: controller.signal }));
  t.after(async () => { controller.abort(); releaseWrite?.(); await observed.outcome; });
  await entered.promise; assert.ok(collected.bytes() > 0);
  f.boundary.cancel(f.lease); await delay(20);
  assert.equal(portSignal.aborted, true); assert.equal(observed.settled(), false);
  assert.equal(collected.stream.closed, false);
  assert.equal(packets.at(-1).code, 'admission_closed');
  assert.throws(() => f.boundary.beforeBody(f.lease), { code: 'readiness_stage_invalid' });
  controller.abort(); releaseWrite(); releaseWrite = null;
  await assert.rejects(observed.outcome.then(result => { if (result.error) throw result.error; }), { code: 'restore_io_failed' });
  assert.equal(collected.stream.closed, true);
});

test('completed admission and failed warm detach the optional external native listener', async t => {
  const controller = new AbortController();
  const f = await prepared(t, {}, controller.signal);
  assert.equal(getEventListeners(controller.signal, 'abort').length, 1);
  const collected = output();
  await sendAuthenticatedBackup({ ...f.encrypted.options, output: collected.stream, beforeBody: f.boundary.beforeBody(f.lease), signal: controller.signal });
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
  let reads = 0;
  const privateError = {}; Object.defineProperty(privateError, 'message', { get() { reads++; throw Error('must_not_read'); } });
  const failing = createReadinessBoundary({ prepareBeforeStop: () => { throw privateError; }, bindAfterAuthentication: expected => ready(expected) });
  await assert.rejects(failing.warmBeforeServingStop(SPEC, { signal: controller.signal }), { code: 'readiness_port_failed' });
  assert.equal(reads, 0); assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
});

test('current Root Core and generic admission remain frozen after readiness port', async () => {
  const pins=JSON.parse(await readFile(new URL('../../current-core-pins.json',import.meta.url),'utf8'));
  assert.deepEqual(pins.productionProfile,{wallMs:120000,idleMs:15000});
  assert.equal(pins.currentCoreRevision,'90335314a663441d34148e8c312e3e4d42350eb7');
  assert.equal(pins.files.length,5);
  for(const file of pins.files) assert.equal(sha(await readFile(new URL('../../'+file.path,import.meta.url))),file.sha256);
  const adapter=await readFile(new URL('../../staged-sender-adapter.mjs',import.meta.url),'utf8');
  assert.match(adapter,/createStagedAuthenticatedBackupSender/u);
  assert.doesNotMatch(adapter,/vendor\/readiness\/staged\/restore-backup/u);
});
