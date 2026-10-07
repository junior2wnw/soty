import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable, Writable, PassThrough } from 'node:stream';
import { constants } from 'node:fs';
import { getEventListeners } from 'node:events';
import { performance } from 'node:perf_hooks';
import { readFile } from 'node:fs/promises';
import { SPEC, RECEIPT, BODY, fence, sha, delay, deferred, environment, manualBegin, mockReadonlyFilesystem, consumeSyntheticBody } from './synthetic-fixture.mjs';
import { bindingFrame, encodeFrame, decodeCanonicalFrame, WireOwner, warmFrame, boundFrame, captureSpec, PRODUCTION_CLOCK, MAX_FRAME_BYTES, beginFrame, beginAckFrame, WARM_LEASE_MS } from './wire-protocol.mjs';
import { prepareWarmForSyntheticFixture } from './linux-warm-preflight.mjs';
import { runWarmReceiver, createWarmControlClientForSyntheticFixture, createWarmReceiverForSyntheticFixture } from './warm-receiver.mjs';

function ackBeginOutput(announcements) {
  return new Writable({ write(chunk, _encoding, callback) {
    if (decodeCanonicalFrame(chunk).schema === 'soty.receiver-control-begin.v1') announcements.write(encodeFrame(beginAckFrame(SPEC)));
    callback();
  } });
}

test('native warm/bind/EOF/body uses separate physical receipt and phase packets', async t => {
  const env = environment(); t.after(env.close);
  const warm = await env.client.warmBeforeServingStop();
  assert.equal(warm.inputBytes, 0); assert.equal(env.counts.receiveCalls, 0);
  assert.equal(env.payload.readableLength, 0); assert.equal(Readable.isDisturbed(env.payload), false);
  const ready = await env.client.bindInsideAuthenticatedHook(RECEIPT, fence(RECEIPT));
  assert.equal(ready.schema, 'soty.receiver-bound.v1'); assert.equal(ready.inputBytes, 0);
  env.payload.end(BODY);
  const physical = await env.rawRun;
  assert.equal(physical.plaintextSha256, sha(BODY)); assert.equal(physical.manifestSha256, RECEIPT.expectedManifestSha256);
  assert.equal(physical.plaintextBytes, BODY.length); assert.equal(physical.readbackVerified, true);
  assert.deepEqual(Object.keys(physical).sort(), ['entries', 'extracted', 'fileBytes', 'manifestSha256', 'plaintextBytes', 'plaintextSha256', 'readbackVerified', 'targetId'].sort());
  assert.equal(env.counts.receiveCalls, 1); assert.ok(env.counts.rechecks >= 3); assert.equal(env.counts.closed, 1);
  assert.deepEqual(env.phases.map(value => value.phase), ['receiver_start', 'warm_read0', 'control_begin_read0', 'control_bound_read0', 'body_receiver_start', 'physical_receipt']);
  for (const event of env.phases) assert.deepEqual(Object.keys(event).sort(), ['bodyStarted', 'code', 'elapsedMs', 'phase', 'schema'].sort());
});

test('warming invokes no payload _read, metadata read or body handler before bind', async t => {
  let pulls = 0;
  const payload = new Readable({ highWaterMark: 65536, read() { pulls++; } });
  const env = environment({ payloadInput: payload }); t.after(env.close);
  await env.client.warmBeforeServingStop(); await delay(20);
  assert.equal(pulls, 0); assert.equal(env.counts.receiveCalls, 0); assert.equal(Readable.isDisturbed(payload), false);
});

test('prequeued payload is refused even when a valid control is waiting', async t => {
  const payload = new PassThrough({ highWaterMark: 65536 }); payload.write(BODY);
  const env = environment({ payloadInput: payload }); t.after(env.close);
  await assert.rejects(env.rawRun, { code: 'warm_payload_not_read0' }); assert.equal(env.counts.receiveCalls, 0);
});

test('control line without EOF never admits body and cancels promptly', async t => {
  const env = environment(); t.after(env.close);
  await env.client.warmBeforeServingStop(); await manualBegin(env); env.control.write(encodeFrame(bindingFrame(SPEC, RECEIPT)));
  await delay(25); assert.equal(env.counts.receiveCalls, 0); assert.equal(Readable.isDisturbed(env.payload), false);
  env.abort.abort(); await assert.rejects(env.rawRun, { code: 'warm_cancelled' });
});

test('canonical control assembled at every byte boundary still binds exact same record', async t => {
  const env = environment(); t.after(env.close); await env.client.warmBeforeServingStop(); await manualBegin(env);
  const bytes = encodeFrame(bindingFrame(SPEC, RECEIPT));
  for (const byte of bytes) env.control.write(Buffer.from([byte])); env.control.end();
  for (let index = 0; index < 200 && !env.phases.some(value => value.phase === 'body_receiver_start'); index++) await delay(1);
  assert.equal(env.counts.receiveCalls, 1); env.payload.end(BODY);
  assert.equal((await env.rawRun).plaintextSha256, sha(BODY));
});

for (const [name, mutate] of [
  ['nonce', value => { value.nonce = 'f'.repeat(32); }],
  ['transaction', value => { value.transaction = 'f'.repeat(32); }],
  ['ciphertext', value => { value.expectedSha256 = 'not-a-digest'; }],
  ['manifest', value => { value.expectedManifestSha256 = 'not-a-digest'; }],
  ['witness generation', value => { value.sourceWitness.generationId = 'f'.repeat(32); }],
  ['source pin', value => { value.sourcePins.restore = 'f'.repeat(64); }],
  ['profile', value => { value.profile = 'legacy'; }],
  ['extra field', value => { value.extra = true; }],
]) test(`untrusted control denies ${name} before body`, async t => {
  let pulls = 0;
  const payload = new Readable({ highWaterMark: 65536, read() { pulls++; } });
  const env = environment({ payloadInput: payload }); t.after(env.close); await env.client.warmBeforeServingStop(); await manualBegin(env);
  const value = structuredClone(bindingFrame(SPEC, RECEIPT)); mutate(value); env.control.end(encodeFrame(value));
  await assert.rejects(env.rawRun); assert.equal(env.counts.receiveCalls, 0); assert.equal(pulls, 0);
});

for (const [name, bytes] of [
  ['duplicate keys', Buffer.from('{"schema":"bad","schema":"soty.receiver-bind.v1"}\n')],
  ['noncanonical whitespace', Buffer.from(' {"schema":"bad"}\n')],
  ['extra frame', Buffer.concat([encodeFrame(bindingFrame(SPEC, RECEIPT)), encodeFrame(bindingFrame(SPEC, RECEIPT))])],
  ['invalid UTF8', Buffer.from([255, 10])],
  ['overlong line', Buffer.alloc(MAX_FRAME_BYTES + 1, 120)],
  ['empty EOF', Buffer.alloc(0)],
  ['unterminated JSON', Buffer.from('{"schema":"soty.receiver-bind.v1"}')],
]) test(`byte-bounded wire rejects ${name} with fixed failure and no body`, async t => {
  const env = environment(); t.after(env.close); await env.client.warmBeforeServingStop(); await manualBegin(env); env.control.end(bytes);
  await assert.rejects(env.rawRun, error => /^warm_[a-z_]+$/u.test(error.code)); assert.equal(env.counts.receiveCalls, 0);
});

test('accessor poison and private telemetry errors are never read', async t => {
  let reads = 0;
  const poison = new Error();
  for (const key of ['message', 'code', 'stack', 'reason']) Object.defineProperty(poison, key, { get() { reads++; throw Error('getter_must_not_run'); } });
  const spec = { ...SPEC }; Object.defineProperty(spec, 'nonce', { get() { reads++; throw poison; }, enumerable: true });
  assert.throws(() => captureSpec(spec), { code: 'warm_record_invalid' });
  const env = environment({ emit() { throw poison; }, preflight: async () => { throw poison; } }); t.after(env.close);
  await assert.rejects(env.rawRun, { code: 'warm_receiver_failed' }); assert.equal(reads, 0); assert.equal(env.counts.receiveCalls, 0);
});

test('private native error packet is sanitized without code/message/reason getter reads', async t => {
  const env = environment(); t.after(env.close); await env.client.warmBeforeServingStop();
  let reads = 0; const privateError = {};
  for (const key of ['code', 'message', 'reason']) Object.defineProperty(privateError, key, { get() { reads++; throw Error(); } });
  env.control.emit('error', privateError);
  await assert.rejects(env.rawRun, { code: 'warm_wire_io_failed' }); assert.equal(reads, 0); assert.equal(env.counts.receiveCalls, 0);
});

test('host fence mismatch never writes a control byte', async t => {
  const env = environment(); t.after(env.close); await env.client.warmBeforeServingStop();
  const other = { ...RECEIPT, expectedSha256: 'f'.repeat(64) };
  await assert.rejects(env.client.bindInsideAuthenticatedHook(RECEIPT, fence(other)), { code: 'warm_authenticated_source_mismatch' });
  assert.equal(env.control.readableLength, 0); assert.equal(env.counts.receiveCalls, 0);
});

test('warm peer cancel wakes missing bound reply rather than waiting for idle', async t => {
  const announcements = new PassThrough({ highWaterMark: 8192 }), controls = ackBeginOutput(announcements);
  const client = createWarmControlClientForSyntheticFixture({ spec: SPEC, announcementInput: announcements, controlOutput: controls,
    signal: undefined, limits: { wallMs: 1000, idleMs: 800 } });
  announcements.write(encodeFrame(warmFrame(SPEC))); await client.warmBeforeServingStop();
  const binding = client.bindInsideAuthenticatedHook(RECEIPT, fence(RECEIPT)); binding.catch(() => {});
  await delay(15); const started = performance.now(); client.cancel();
  await assert.rejects(binding, { code: 'warm_cancelled' }); assert.ok(performance.now() - started < 200);
  await assert.rejects(client.bindInsideAuthenticatedHook(RECEIPT, fence(RECEIPT)), { code: 'warm_stage_invalid' });
  assert.equal(announcements.destroyed, true);
});

test('bound frame without phase-channel EOF is not enough for sender body admission', async () => {
  const announcements = new PassThrough({ highWaterMark: 8192 }), controls = ackBeginOutput(announcements);
  const client = createWarmControlClientForSyntheticFixture({ spec: SPEC, announcementInput: announcements, controlOutput: controls,
    signal: undefined, limits: { wallMs: 150, idleMs: 70 } });
  announcements.write(encodeFrame(warmFrame(SPEC))); await client.warmBeforeServingStop();
  const binding = client.bindInsideAuthenticatedHook(RECEIPT, fence(RECEIPT)); binding.catch(() => {});
  await delay(10); announcements.write(encodeFrame(boundFrame(bindingFrame(SPEC, RECEIPT))));
  await assert.rejects(binding, { code: 'warm_timeout' }); client.cancel();
});

test('source/root mutation across a deferred pre-body recheck prevents body', async t => {
  const entered = deferred(), release = deferred(); let checks = 0;
  const env = environment({ preflight: async (_spec, check) => ({ async recheck() {
    check(); checks++; if (checks === 2) { entered.resolve(); await release.promise; throw Error('private_source_drift'); }
  }, async close() {} }) }); t.after(env.close);
  await env.client.warmBeforeServingStop(); const binding = env.client.bindInsideAuthenticatedHook(RECEIPT, fence(RECEIPT)); binding.catch(() => {});
  await entered.promise; assert.equal(env.counts.receiveCalls, 0); release.resolve();
  await assert.rejects(env.rawRun, { code: 'warm_receiver_failed' }); await assert.rejects(binding); assert.equal(env.counts.receiveCalls, 0);
});

test('native backpressure and cancellation do not settle a borrowed callback early', async () => {
  const held = deferred(), entered = deferred(), control = new PassThrough({ highWaterMark: 8192 }), payload = new PassThrough({ highWaterMark: 65536 });
  let writeCount = 0, calls = 0, callbackHeld;
  const announcements = new Writable({ highWaterMark: 1, write(_chunk, _encoding, callback) {
    writeCount++; if (writeCount === 3) { callbackHeld = callback; entered.resolve(); }
    else { callback(); queueMicrotask(() => {
      if (writeCount === 1) control.write(encodeFrame(beginFrame(SPEC)));
      else if (writeCount === 2) control.end(encodeFrame(bindingFrame(SPEC, RECEIPT)));
    }); }
  } });
  const abort = new AbortController();
  const driver = createWarmReceiverForSyntheticFixture({ preflight: async () => ({ async recheck() {}, async close() {} }),
    receive: async () => { calls++; return {}; }, limits: { wallMs: 1000, idleMs: 500 } });
  let settled = false;
  const running = driver({ spec: SPEC, controlInput: control, payloadInput: payload, announcementOutput: announcements,
    signal: abort.signal, emit() {} });
  const observation = running.then(() => { settled = true; }, error => { settled = true; throw error; }); observation.catch(() => {});
  await entered.promise;
  abort.abort(); await delay(20); assert.equal(settled, false); assert.equal(calls, 0);
  callbackHeld(); await assert.rejects(observation, { code: 'warm_cancelled' }); assert.equal(calls, 0);
  held.resolve();
});

test('EOF with a malformed physical receipt never turns phase success into restore success', async t => {
  const env = environment({ receive: async (control, input) => { await consumeSyntheticBody(control, input); return { ok: true }; } }); t.after(env.close);
  await env.client.warmBeforeServingStop(); await env.client.bindInsideAuthenticatedHook(RECEIPT, fence(RECEIPT)); env.payload.end(BODY);
  await assert.rejects(env.rawRun, { code: 'warm_record_invalid' });
  assert.equal(env.phases.some(value => value.phase === 'physical_receipt'), false);
});

test('body missing EOF cannot produce a physical receipt; native cancel terminates owned input', async t => {
  const env = environment(); t.after(env.close); await env.client.warmBeforeServingStop();
  await env.client.bindInsideAuthenticatedHook(RECEIPT, fence(RECEIPT)); env.payload.write(BODY); await delay(20);
  assert.equal(env.phases.some(value => value.phase === 'physical_receipt'), false);
  env.abort.abort(); await assert.rejects(env.rawRun, { code: 'warm_cancelled' });
});

test('same nonce/control cannot replay once bound and body finishes', async t => {
  const env = environment(); t.after(env.close); await env.client.warmBeforeServingStop();
  await env.client.bindInsideAuthenticatedHook(RECEIPT, fence(RECEIPT)); env.payload.end(BODY); await env.rawRun;
  await assert.rejects(env.client.bindInsideAuthenticatedHook(RECEIPT, fence(RECEIPT)), { code: 'warm_stage_invalid' });
  assert.equal(env.counts.receiveCalls, 1);
});

for (const [name, mutate] of [
  ['cipher digest', value => { value.expectedSha256 = 'f'.repeat(64); }],
  ['manifest digest', value => { value.expectedManifestSha256 = 'f'.repeat(64); }],
  ['checkpoint witness', value => { value.sourceWitness.checkpointSha256 = 'f'.repeat(64); }],
  ['inventory witness', value => { value.sourceWitness.inventorySha256 = 'f'.repeat(64); }],
  ['nonce', value => { value.nonce = 'f'.repeat(32); }],
  ['source pins', value => { value.sourcePins.restore = 'f'.repeat(64); }],
  ['nonzero read0', value => { value.inputBytes = 1; }],
  ['extra field', value => { value.unreviewed = true; }],
]) test(`host rejects bound acknowledgement with different ${name}`, async () => {
  const announcements = new PassThrough({ highWaterMark: 8192 });
  const controls = ackBeginOutput(announcements);
  const client = createWarmControlClientForSyntheticFixture({ spec: SPEC, announcementInput: announcements, controlOutput: controls,
    signal: undefined, limits: { wallMs: 1000, idleMs: 500 } });
  announcements.write(encodeFrame(warmFrame(SPEC))); await client.warmBeforeServingStop();
  const value = structuredClone(boundFrame(bindingFrame(SPEC, RECEIPT))); mutate(value);
  controls.once('finish', () => { announcements.end(encodeFrame(value)); });
  await assert.rejects(client.bindInsideAuthenticatedHook(RECEIPT, fence(RECEIPT)), error => /^warm_[a-z_]+$/u.test(error.code));
  client.cancel();
});

test('unsolicited second phase frame cannot be accepted as a future binding', async () => {
  const announcements = new PassThrough({ highWaterMark: 8192 }), controls = ackBeginOutput(announcements);
  const client = createWarmControlClientForSyntheticFixture({ spec: SPEC, announcementInput: announcements, controlOutput: controls,
    signal: undefined, limits: { wallMs: 1000, idleMs: 500 } });
  announcements.write(Buffer.concat([encodeFrame(warmFrame(SPEC)), encodeFrame(boundFrame(bindingFrame(SPEC, RECEIPT)))]));
  await assert.rejects(client.warmBeforeServingStop(), error => /^warm_[a-z_]+$/u.test(error.code)); client.cancel();
});

test('fence expiry while native acknowledgement is pending cannot admit a later valid reply', async () => {
  const announcements = new PassThrough({ highWaterMark: 8192 }), controls = ackBeginOutput(announcements);
  const client = createWarmControlClientForSyntheticFixture({ spec: SPEC, announcementInput: announcements, controlOutput: controls,
    signal: undefined, limits: { wallMs: 1000, idleMs: 500 } });
  announcements.write(encodeFrame(warmFrame(SPEC))); await client.warmBeforeServingStop();
  let expired = false;
  const actualFenceFixture = { authenticated: RECEIPT, check() { if (expired) throw Error('private_expired_reason'); } };
  const binding = client.bindInsideAuthenticatedHook(RECEIPT, actualFenceFixture); binding.catch(() => {});
  await delay(15); expired = true; announcements.end(encodeFrame(boundFrame(bindingFrame(SPEC, RECEIPT))));
  await assert.rejects(binding, { code: 'warm_fence_closed' }); client.cancel();
});

test('read-only Linux probe logic pins nine public source files and never opens config/data contents', async () => {
  const fs = mockReadonlyFilesystem(), lease = await fs.preflight(SPEC, () => {});
  await lease.recheck(); await lease.close();
  assert.equal(fs.state.opened.filter(value => value.path.startsWith('/operator/root/')).length, 9);
  for (const value of fs.state.opened.filter(value => value.flags !== undefined)) assert.equal(value.flags & (constants.O_WRONLY | constants.O_RDWR), 0);
  assert.equal(fs.state.opened.some(value => value.path.startsWith('/owned/target/config/') || value.path.startsWith('/owned/target/data/')), false);
  assert.equal(fs.state.opened.length, fs.state.closed.length);
});

for (const [name, mutate, code] of [
  ['config tmpfs downgraded', state => { state.mounts.find(row => row.path === '/owned/target/config').type = 'ext4'; }, 'warm_topology_invalid'],
  ['data turned into tmpfs', state => { state.mounts.find(row => row.path === '/owned/target/data').type = 'tmpfs'; }, 'warm_topology_invalid'],
  ['operator source becomes rw', state => { state.mounts.find(row => row.path === '/operator').options = 'rw'; }, 'warm_source_not_readonly'],
  ['root path swapped after warm', state => { state.dirs.set('/owned/target', { ...state.dirs.get('/owned/target'), ino: 999n }); }, 'warm_target_drift'],
  ['namespace replaced', state => { state.namespace.ino = 999n; }, 'warm_target_drift'],
  ['source file path replaced', state => { const path = state.sourceRoot + '/wire-protocol.mjs', file = state.files.get(path); state.files.set(path, { ...file, stat: { ...file.stat, ino: 999n } }); }, 'warm_source_drift'],
  ['target config populated', state => { state.entries.set('/owned/target/config', ['unreviewed']); }, 'warm_target_contaminated'],
]) test(`read-only preflight refuses ${name} on recheck`, async () => {
  const fs = mockReadonlyFilesystem(), lease = await fs.preflight(SPEC, () => {}); mutate(fs.state);
  await assert.rejects(lease.recheck(), { code }); await lease.close();
  assert.equal(fs.state.opened.length, fs.state.closed.length);
});

test('post-await source read drift is rejected and all acquired metadata handles close', async () => {
  const fs = mockReadonlyFilesystem(), path = fs.state.sourceRoot + '/wire-protocol.mjs'; let changed = false;
  fs.state.hook = async (operation, current) => {
    if (operation === 'handle.read' && current === path && !changed) {
      changed = true; fs.state.files.get(path).stat.ctimeNs++;
    }
  };
  await assert.rejects(fs.preflight(SPEC, () => {}), { code: 'warm_source_drift' });
  assert.equal(fs.state.opened.length, fs.state.closed.length);
});

test('already-aborted WireOwner installs no retained signal listener', () => {
  const abort = new AbortController(); abort.abort();
  assert.throws(() => new WireOwner({ signal: abort.signal }), { code: 'warm_cancelled' });
  assert.equal(getEventListeners(abort.signal, 'abort').length, 0);
});

test('current Root Core is frozen and receiver clocks remain 120/15', async () => {
  const pins=JSON.parse(await readFile(new URL('../../current-core-pins.json',import.meta.url),'utf8'));
  assert.deepEqual(pins.productionProfile,{wallMs:120000,idleMs:15000});
  assert.equal(pins.currentCoreRevision,'90335314a663441d34148e8c312e3e4d42350eb7');
  assert.equal(pins.files.length,5);
  for(const file of pins.files) assert.equal(sha(await readFile(new URL('../../'+file.path,import.meta.url))),file.sha256);
  assert.deepEqual(PRODUCTION_CLOCK,{wallMs:120000,idleMs:15000});
  const driver=await readFile(new URL('warm-receiver.mjs',import.meta.url),'utf8');
  assert.match(driver,/receiveStrictBackup\(control, payload\)/u);
});

test('default Linux-only runtime never reads body or emits warm on Windows', { skip: process.platform === 'linux' }, async () => {
  let pulls = 0;
  const control = new PassThrough(), payload = new Readable({ read() { pulls++; } }), announcements = new PassThrough(); let events = [];
  await assert.rejects(runWarmReceiver({ spec: SPEC, controlInput: control, payloadInput: payload,
    announcementOutput: announcements, signal: undefined, emit(value) { events.push(value); } }), { code: 'warm_linux_required' });
  assert.equal(events.some(value => value.phase === 'warm_read0'), false);
  assert.equal(pulls, 0); assert.equal(announcements.readableLength, 0);
});

test('warm wait is independent of control idle; begin-control starts the distinct late phase', async t => {
  const env = environment({ limits: { wallMs: 100, idleMs: 40 }, leaseMs: 700 }); t.after(env.close);
  await env.client.warmBeforeServingStop(); await delay(100);
  assert.equal(env.counts.receiveCalls, 0); assert.equal(env.phases.some(value => value.phase === 'receiver_failed'), false);
  await env.client.bindInsideAuthenticatedHook(RECEIPT, fence(RECEIPT)); env.payload.end(BODY);
  assert.equal((await env.rawRun).plaintextSha256, sha(BODY));
  assert.equal(env.phases.filter(value => value.phase === 'control_begin_read0').length, 1);
});

test('outer warm lease expires while waiting before control and cannot be renewed by frames', async t => {
  const env = environment({ limits: { wallMs: 1000, idleMs: 500 }, leaseMs: 90, clientLeaseMs: 1000 }); t.after(env.close);
  await env.client.warmBeforeServingStop();
  await assert.rejects(env.rawRun, { code: 'warm_lease_expired' });
  env.client.cancel();
  await assert.rejects(env.client.bindInsideAuthenticatedHook(RECEIPT, fence(RECEIPT)), { code: 'warm_stage_invalid' });
  assert.equal(env.client.signal.aborted, true); assert.equal(env.counts.receiveCalls, 0);
});

test('begin-control transition cannot reset the original warm deadline during body', async t => {
  const env = environment({ limits: { wallMs: 1000, idleMs: 500 }, leaseMs: 170 }); t.after(env.close);
  await env.client.warmBeforeServingStop(); await delay(80);
  await env.client.bindInsideAuthenticatedHook(RECEIPT, fence(RECEIPT)); env.payload.write(BODY);
  await assert.rejects(env.rawRun, { code: 'warm_lease_expired' });
  assert.equal(env.phases.some(value => value.phase === 'physical_receipt'), false);
  assert.equal(env.client.signal.aborted, true);
});

test('bind without explicit begin-control is denied before any body call', async t => {
  const env = environment(); t.after(env.close); await env.client.warmBeforeServingStop();
  env.control.end(encodeFrame(bindingFrame(SPEC, RECEIPT)));
  await assert.rejects(env.rawRun, { code: 'warm_record_invalid' }); assert.equal(env.counts.receiveCalls, 0);
});

test('a fake Proxy cannot reenter current-state validation through reflection traps', () => {
  let traps = 0;
  const value = new Proxy({ ...SPEC }, { ownKeys() { traps++; throw Error('private_proxy'); } });
  assert.throws(() => captureSpec(value), { code: 'warm_record_invalid' }); assert.equal(traps, 0);
});

test('an already-aborted host signal cannot create a fresh warm peer', () => {
  const abort = new AbortController(); abort.abort();
  const announcements = new PassThrough(), controls = new PassThrough();
  assert.throws(() => createWarmControlClientForSyntheticFixture({ spec: SPEC, announcementInput: announcements, controlOutput: controls,
    signal: abort.signal, limits: { wallMs: 1000, idleMs: 500 } }), { code: 'warm_cancelled' });
  assert.equal(getEventListeners(abort.signal, 'abort').length, 0);
  announcements.destroy(); controls.destroy();
});

test('physical receipt before deadline cannot bypass a lease expiring during final owned cleanup', async t => {
  const env = environment({ leaseMs: 100, clientLeaseMs: 1000, preflight: async (_spec, check) => ({
    async recheck() { check(); }, async close() { await delay(140); },
  }) }); t.after(env.close);
  await env.client.warmBeforeServingStop(); await env.client.bindInsideAuthenticatedHook(RECEIPT, fence(RECEIPT)); env.payload.end(BODY);
  await assert.rejects(env.rawRun, { code: 'warm_lease_expired' });
  assert.equal(env.phases.some(value => value.phase === 'physical_receipt'), true);
});
