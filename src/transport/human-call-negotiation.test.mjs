import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const { transform } = createRequire(import.meta.url)(process.env.SOTY_ESBUILD_MODULE || 'esbuild');
const source = readFileSync(process.env.SOTY_HUMAN_NEGOTIATION_SOURCE || new URL('../sync.ts', import.meta.url), 'utf8');
const start = source.indexOf('const p2pIceServers:'), end = source.indexOf('function safeRemoteTimeoutMs', start);
assert.ok(start >= 0 && end > start);
// Exact actual-function region, not a rewritten negotiation implementation.
const built = await transform(source.slice(start, end), { loader: 'ts', format: 'esm', target: 'es2024' });
const runtimePath = resolve(process.env.SOTY_HUMAN_NEGOTIATION_OUTPUT || 'output/human-call-negotiation-runtime.mjs');
mkdirSync(dirname(runtimePath), { recursive: true }); writeFileSync(runtimePath, built.code);
const { createHumanCallPorts } = await import(pathToFileURL(runtimePath));
const tick = () => new Promise(resolve => setImmediate(resolve));
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
async function until(check) { for (let n = 0; n < 60; n++) { if (check()) return; await tick(); } throw new Error('controlled_schedule_timeout'); }

// Deterministic native-IO model: real operation-chain ordering and valid RTC
// description transitions. No native RTC, ICE success or Root proof is faked.
class StrictRTC {
  signalingState = 'stable'; connectionState = 'new'; iceConnectionState = 'new';
  localDescription = null; remoteDescription = null; tail = Promise.resolve();
  enqueued = []; executed = []; refusals = []; localApplied = 0; closes = 0;
  offerHold = null; answerHold = null; remoteAnswerHold = null; channels = 0;
  operation(kind, action) {
    this.enqueued.push(kind);
    const result = this.tail.then(async () => {
      this.executed.push({ kind, state: this.signalingState }); this.open(); return action();
    });
    this.tail = result.catch(() => {}); return result;
  }
  open() { if (this.connectionState === 'closed') throw new DOMException('controlled_closed', 'InvalidStateError'); }
  async generateOffer() {
    assert.ok(['stable', 'have-local-offer'].includes(this.signalingState));
    if (this.offerHold) await this.offerHold.promise;
    this.open(); return { type: 'offer', sdp: 'v=0\r\n' };
  }
  async generateAnswer() {
    assert.equal(this.signalingState, 'have-remote-offer');
    if (this.answerHold) await this.answerHold.promise;
    this.open(); return { type: 'answer', sdp: 'v=0\r\n' };
  }
  createOffer() { return this.operation('create-offer', () => this.generateOffer()); }
  createAnswer() { return this.operation('create-answer', () => this.generateAnswer()); }
  setLocalDescription(value) {
    return this.operation('set-local-' + (value?.type || 'auto'), async () => {
      if (value === undefined) value = await (this.signalingState === 'have-remote-offer' ? this.generateAnswer() : this.generateOffer());
      this.open();
      if (value.type === 'rollback') {
        if (this.signalingState === 'stable') {
          this.refusals.push('rollback-stable-invalid-state');
          throw new DOMException('controlled_rollback_stable', 'InvalidStateError');
        }
        this.signalingState = 'stable'; this.localDescription = null; return;
      }
      assert.ok(value.type === 'offer' ? ['stable', 'have-local-offer'].includes(this.signalingState) : this.signalingState === 'have-remote-offer');
      this.localDescription = value; this.localApplied++;
      this.signalingState = value.type === 'offer' ? 'have-local-offer' : 'stable';
    });
  }
  setRemoteDescription(value) {
    return this.operation('set-remote-' + value.type, async () => {
      if (value.type === 'answer' && this.remoteAnswerHold) await this.remoteAnswerHold.promise;
      this.open();
      if (value.type === 'offer' && this.signalingState === 'have-local-offer') {
        this.signalingState = 'stable'; this.localDescription = null;
      }
      assert.ok(value.type === 'offer' ? this.signalingState === 'stable' : this.signalingState === 'have-local-offer');
      this.remoteDescription = value; this.signalingState = value.type === 'offer' ? 'have-remote-offer' : 'stable';
    });
  }
  addIceCandidate() { return this.operation('add-ice-candidate', async () => {}); }
  addTransceiver(kind, options) { assert.equal(kind, 'audio'); assert.equal(options.direction, 'recvonly'); }
  createDataChannel() { this.channels++; return { close() {} }; }
  close() { this.closes++; this.connectionState = 'closed'; }
}
class ControlledSocket {
  readyState = 0; sent = [];
  send(raw) { this.sent.push(JSON.parse(raw)); }
  open() { this.readyState = 1; this.onopen?.(); }
  receive(value) { this.onmessage?.({ data: JSON.stringify({ callId: 'controlled-call', epoch: 1, ...value }) }); }
  close() { this.readyState = 3; this.onclose?.(); }
}
async function fixture(t, { local = 'device-z', peers = ['device-a'] } = {}) {
  const socket = new ControlledSocket(), pcs = [], abort = new AbortController(); let captures = 0, interrupted = 0;
  const scope = { accountId: 'controlled-account', deviceId: local, roomId: 'controlled-room', mode: 'group', generation: 1 };
  const client = { async extension(operation) {
    if (operation === 'call.leave') return { left: true };
    assert.equal(operation, 'call.join');
    // Existing admission-shaped unit IO solely enters actual WeakMap branding.
    // It is not evidence of genuine signed/current access or membership.
    return { schema: 'soty.human-call-admission.v1', purpose: 'join', callId: 'controlled-call', epoch: 1,
      accountId: scope.accountId, deviceId: local, grantRef: 'controlled-reference', expiresAt: Date.now() + 30000 };
  } };
  const ports = createHumanCallPorts({ client, rtcFactory: () => { const pc = new StrictRTC(); pcs.push(pc); return pc; },
    socketFactory: () => socket, acquire: async () => { captures++; throw new Error('capture_forbidden'); },
    playRemote: () => { throw new Error('playback_forbidden'); }, onInterrupted: () => { interrupted++; } });
  t.after(async () => {
    for (const pc of pcs) { pc.offerHold?.resolve(); pc.answerHold?.resolve(); pc.remoteAnswerHold?.resolve(); }
    await ports.close();
  });
  const admission = await ports.host.authorize(scope, 'join', { signal: abort.signal });
  const joined = ports.transport.join({ scope, admission, signal: abort.signal });
  socket.open();
  const roster = ids => [{ accountId: scope.accountId, deviceId: local, microphone: false },
    ...ids.map(deviceId => ({ accountId: 'controlled-peer', deviceId, microphone: false }))];
  socket.receive({ type: 'call.ready', self: { accountId: scope.accountId, deviceId: local }, peers: roster(peers) });
  await joined;
  const receive = (kind, peer = peers[0]) => socket.receive({ type: 'p2p.' + kind, signal: { kind, deviceId: peer, targetDeviceId: local, sdp: 'v=0\r\n' } });
  const count = type => socket.sent.filter(value => value.type === 'p2p.' + type).length;
  const drain = async () => { for (let n = 0; n < 12; n++) { await Promise.all(pcs.map(pc => pc.tail)); await tick(); } };
  return { pcs, socket, ports, receive, count, drain, roster, captures: () => captures, interruptions: () => interrupted };
}

test('pending local createOffer must not prevent polite peer answering an incoming offer', { timeout: 2500 }, async t => {
  const f = await fixture(t), pc = f.pcs[0]; pc.offerHold = deferred(); pc.onnegotiationneeded();
  await until(() => pc.executed.length > 0); assert.equal(pc.signalingState, 'stable');
  f.receive('offer');
  await until(() => pc.enqueued.includes('set-local-rollback') || pc.enqueued.includes('set-remote-offer'));
  assert.equal(pc.signalingState, 'stable'); pc.offerHold.resolve(); await f.drain();
  if (process.env.SOTY_HUMAN_NEGOTIATION_OBSERVATION) writeFileSync(process.env.SOTY_HUMAN_NEGOTIATION_OBSERVATION, JSON.stringify({
    controlledFunctionOnly: true, actualRTC: false, genuineAdmissionProof: false, captures: f.captures(),
    interruptions: f.interruptions(), operations: pc.executed, refusals: pc.refusals, offerSignals: f.count('offer'),
    answerSignals: f.count('answer'), signalingState: pc.signalingState, connectionState: pc.connectionState,
    remoteDescriptionApplied: pc.remoteDescription !== null,
  }, null, 2) + '\n', { flag: 'wx' });
  assert.equal(f.captures(), 0);
  assert.equal(f.count('answer'), 1, 'a pending local offer must yield to the incoming polite offer');
  assert.equal(f.interruptions(), 0); assert.equal(pc.signalingState, 'stable');
});

test('offer arriving during pending remote answer is chained instead of discarded as impolite glare', async t => {
  const f = await fixture(t, { local: 'device-a', peers: ['device-z'] }), pc = f.pcs[0];
  pc.onnegotiationneeded(); await until(() => f.count('offer') === 1);
  pc.remoteAnswerHold = deferred(); f.receive('answer');
  await until(() => pc.executed.some(value => value.kind === 'set-remote-answer'));
  assert.equal(pc.signalingState, 'have-local-offer'); f.receive('offer');
  assert.equal(pc.enqueued.includes('set-remote-offer'), true);
  pc.remoteAnswerHold.resolve(); await f.drain();
  assert.equal(f.count('answer'), 1); assert.equal(pc.signalingState, 'stable');
  assert.equal(f.interruptions(), 0); assert.equal(f.captures(), 0);
});

test('impolite peer still ignores an actual colliding offer and consumes its original answer', async t => {
  const f = await fixture(t, { local: 'device-a', peers: ['device-z'] }), pc = f.pcs[0];
  pc.onnegotiationneeded(); await until(() => f.count('offer') === 1); f.receive('offer'); await tick();
  assert.equal(pc.enqueued.includes('set-remote-offer'), false); assert.equal(f.count('answer'), 0);
  f.receive('answer'); await f.drain(); assert.equal(pc.signalingState, 'stable');
  assert.equal(f.count('offer'), 1); assert.equal(f.interruptions(), 0); assert.equal(f.captures(), 0);
});

test('one native initial negotiation event does not queue a redundant eager second offer', async t => {
  const f = await fixture(t, { local: 'device-a', peers: ['device-z'] }), pc = f.pcs[0];
  pc.onnegotiationneeded(); await until(() => f.count('offer') === 1); f.receive('answer'); await f.drain();
  assert.equal(pc.channels, 1); assert.equal(f.count('offer'), 1);
  assert.equal(pc.signalingState, 'stable'); assert.equal(f.captures(), 0);
});

test('removed peer cannot install or signal a pending answer while the other group peers stay current', async t => {
  const f = await fixture(t, { peers: ['device-a', 'device-b', 'device-c'] }), removed = f.pcs[0];
  removed.answerHold = deferred(); f.receive('offer', 'device-a');
  await until(() => removed.executed.some(value => value.kind === 'set-local-auto'));
  f.socket.receive({ type: 'call.peers', peers: f.roster(['device-b', 'device-c']) });
  removed.answerHold.resolve(); await f.drain();
  assert.equal(f.count('answer'), 0); assert.equal(removed.localApplied, 0); assert.equal(removed.closes, 1);
  assert.equal(f.pcs[1].closes, 0); assert.equal(f.pcs[2].closes, 0);
  f.receive('offer', 'device-b'); await f.drain();
  assert.equal(f.count('answer'), 1); assert.equal(f.interruptions(), 0); assert.equal(f.captures(), 0);
});

test('dispose during atomic local generation closes the PC and rejects late SDP ICE and receiver effects', async t => {
  const f = await fixture(t), pc = f.pcs[0]; pc.offerHold = deferred();
  const lateICE = pc.onicecandidate, lateTrack = pc.ontrack; pc.onnegotiationneeded();
  await until(() => pc.executed.length > 0); await f.ports.close(); pc.offerHold.resolve();
  const track = { kind: 'audio', stops: 0, stop() { this.stops++; } };
  lateICE({ candidate: { toJSON: () => ({ candidate: 'controlled-candidate' }) } }); lateTrack({ track });
  await f.drain(); assert.equal(f.count('offer'), 0); assert.equal(f.count('answer'), 0);
  assert.equal(pc.localApplied, 0); assert.equal(track.stops, 1);
  assert.equal(f.interruptions(), 0); assert.equal(f.captures(), 0);
});

test('three group peers retain independent operation chains and leave closes every peer without capture', async t => {
  const peers = ['device-a', 'device-b', 'device-c'], f = await fixture(t, { peers });
  for (const pc of f.pcs) { pc.offerHold = deferred(); pc.onnegotiationneeded(); }
  await until(() => f.pcs.every(pc => pc.executed.length > 0));
  for (const peer of peers) f.receive('offer', peer);
  f.pcs[1].offerHold.resolve(); await until(() => f.count('answer') === 1);
  assert.equal(f.pcs[0].signalingState, 'stable'); assert.equal(f.pcs[2].signalingState, 'stable');
  f.pcs[0].offerHold.resolve(); f.pcs[2].offerHold.resolve(); await f.drain();
  assert.equal(f.count('answer'), 3); assert.equal(f.pcs.every(pc => pc.signalingState === 'stable'), true);
  assert.equal(f.interruptions(), 0); assert.equal(f.captures(), 0);
  await f.ports.close(); assert.equal(f.pcs.every(pc => pc.closes === 1), true);
});
